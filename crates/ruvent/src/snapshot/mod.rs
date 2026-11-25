//! The operational snapshot. Not a method, and not the recovery journal.
//!
//! The engine copies events on the configured channels and types to a
//! collector as they are registered — claimable and dispatch alike — without
//! waiting. The collector gathers a unit (a count or an age, whichever comes
//! first) and writes it to its own file. Nothing here is replayed or re-sent
//! at restart. A full collector drops copies and a failed write is counted;
//! the server carries on either way. On shutdown the last unit is written.
//!
//! ```text
//! "ruvsnap" 0x01                                   file header
//! [len u32][crc32 u32] [count u32][item]…          one frame per file
//! item = [1][number u32][length u16][UTF-8]        name definition
//!      | [2] envelope body, journal layout         claimable
//!      | [3] envelope body, delivery layout        dispatch
//! ```

use crate::envelope::codec::{self as body, Layout, Malformed, Reader};
use crate::envelope::{Envelope, Kind};
use crate::names::{Name, Pattern};
use bytes::{BufMut, Bytes};
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub const MAGIC: &[u8; 8] = b"ruvsnap\x01";
const NAME: u8 = 1;
const CLAIMABLE: u8 = 2;
const DISPATCH: u8 = 3;

/// Which events are collected, and how they are cut into files.
#[derive(Clone, Debug)]
pub struct SnapshotConfig {
    pub directory: PathBuf,
    /// Channels to collect. Empty means none.
    pub channels: Vec<Pattern>,
    /// Event types to collect on those channels. Empty means all.
    pub types: Vec<String>,
    /// A file is written once this many events are collected…
    pub max_events: usize,
    /// …or this long after the first event of the unit, whichever comes first.
    pub max_age: Duration,
    /// Events the collector may hold before the engine drops (and counts) the next.
    pub buffer: usize,
}

impl SnapshotConfig {
    pub fn new(directory: impl Into<PathBuf>, channels: Vec<Pattern>) -> Self {
        SnapshotConfig { directory: directory.into(), channels, types: Vec::new(), max_events: 4096, max_age: Duration::from_secs(5), buffer: 65_536 }
    }

    pub fn wants(&self, channel: &str, event_type: &str) -> bool {
        self.channels.iter().any(|pattern| pattern.matches(channel)) && (self.types.is_empty() || self.types.iter().any(|wanted| wanted == event_type))
    }
}

/// What the collector has done. Counters only grow.
#[derive(Default, Debug)]
pub struct SnapshotStats {
    /// Copies the engine dropped because the collector's buffer was full.
    pub dropped: AtomicU64,
    /// Events written to snapshot files.
    pub written: AtomicU64,
    /// Files whose write failed; their events are lost.
    pub failed: AtomicU64,
}

/// The collector task's handles.
pub struct Collector {
    pub sender: mpsc::Sender<Envelope>,
    pub stats: Arc<SnapshotStats>,
    pub task: JoinHandle<()>,
}

/// Encodes one unit as a snapshot file.
pub fn encode(events: &[Envelope]) -> Result<Vec<u8>, Malformed> {
    let mut numbers: HashMap<Name, u32> = HashMap::new();
    let mut items = Vec::new();
    let mut count = 0u32;
    for event in events {
        let layout = match event.kind {
            Kind::Claimable => Layout::Journal,
            Kind::Dispatch => Layout::Delivery,
        };
        let mut used = Vec::new();
        for name in event.names(layout) {
            let next = numbers.len() as u32;
            let number = *numbers.entry(name.clone()).or_insert_with(|| {
                let text = name.as_bytes();
                let length = text.len().min(u16::MAX as usize);
                items.put_u8(NAME);
                items.put_u32_le(next);
                items.put_u16_le(length as u16);
                items.put_slice(&text[..length]);
                count += 1;
                next
            });
            used.push(number);
        }
        let mut used = used.into_iter();
        items.put_u8(if event.kind == Kind::Claimable { CLAIMABLE } else { DISPATCH });
        body::encode(event, layout, &mut |_| used.next().unwrap_or_default(), &mut items)?;
        count += 1;
    }
    let mut file = MAGIC.to_vec();
    file.put_u32_le((4 + items.len()) as u32);
    file.put_u32_le(0);
    let start = file.len();
    file.put_u32_le(count);
    file.extend_from_slice(&items);
    let crc = crc32fast::hash(&file[start..]);
    file[start - 4..start].copy_from_slice(&crc.to_le_bytes());
    Ok(file)
}

/// Reads a snapshot file back.
pub fn read(path: &Path) -> io::Result<Vec<Envelope>> {
    let invalid = |message: String| io::Error::new(io::ErrorKind::InvalidData, format!("{}: {message}", path.display()));
    let bytes = Bytes::from(std::fs::read(path)?);
    if !bytes.starts_with(MAGIC) || bytes.len() < MAGIC.len() + 8 {
        return Err(invalid("not a ruvent snapshot".into()));
    }
    let length = u32::from_le_bytes(bytes[8..12].try_into().unwrap_or_default()) as usize;
    let crc = u32::from_le_bytes(bytes[12..16].try_into().unwrap_or_default());
    let body = bytes.slice(16..);
    if body.len() != length || crc32fast::hash(&body) != crc {
        return Err(invalid("damaged snapshot".into()));
    }
    let decode = || -> Result<Vec<Envelope>, Malformed> {
        let mut reader = Reader::new(&body);
        let count = reader.u32()?;
        let mut names: HashMap<u32, Name> = HashMap::new();
        let mut events = Vec::new();
        for _ in 0..count {
            match reader.u8()? {
                NAME => {
                    let number = reader.u32()?;
                    let length = reader.u16()? as usize;
                    let text = std::str::from_utf8(reader.take(length)?).map_err(|_| Malformed::Forbidden("name not UTF-8"))?;
                    names.insert(number, Name::new(text));
                }
                CLAIMABLE => events.push(body::decode(&mut reader, Layout::Journal, &|number| names.get(&number).cloned())?),
                DISPATCH => events.push(body::decode(&mut reader, Layout::Delivery, &|number| names.get(&number).cloned())?),
                _ => return Err(Malformed::Forbidden("unknown snapshot item")),
            }
        }
        reader.finish()?;
        Ok(events)
    };
    decode().map_err(|error| invalid(error.to_string()))
}

async fn write(directory: PathBuf, sequence: u64, events: Vec<Envelope>) -> io::Result<()> {
    tokio::task::spawn_blocking(move || -> io::Result<()> {
        let bytes = encode(&events).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        std::fs::create_dir_all(&directory)?;
        // The wall clock names the file and is read nowhere else.
        let stamp = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|elapsed| elapsed.as_millis()).unwrap_or_default();
        let path = directory.join(format!("snapshot-{stamp}-{sequence:08}.rvs"));
        let temporary = path.with_extension("tmp");
        std::fs::write(&temporary, &bytes)?;
        std::fs::rename(&temporary, &path)
    })
    .await
    .map_err(io::Error::other)?
}

enum Got {
    Event(Box<Envelope>),
    Closed,
    Due,
}

impl Collector {
    pub fn spawn(config: SnapshotConfig) -> Collector {
        let (sender, mut receiver) = mpsc::channel::<Envelope>(config.buffer.max(1));
        let stats = Arc::new(SnapshotStats::default());
        let counters = stats.clone();
        let task = tokio::spawn(async move {
            let mut unit: Vec<Envelope> = Vec::new();
            let mut started = tokio::time::Instant::now();
            let mut sequence = 0u64;
            loop {
                let got = if unit.is_empty() {
                    receiver.recv().await.map_or(Got::Closed, |event| Got::Event(Box::new(event)))
                } else {
                    let left = config.max_age.saturating_sub(started.elapsed());
                    // The unit's age limit: the one place this module waits on the clock.
                    match tokio::time::timeout(left, receiver.recv()).await {
                        Ok(Some(event)) => Got::Event(Box::new(event)),
                        Ok(None) => Got::Closed,
                        Err(_) => Got::Due,
                    }
                };
                let closed = matches!(got, Got::Closed);
                if let Got::Event(event) = got {
                    if unit.is_empty() {
                        started = tokio::time::Instant::now();
                    }
                    unit.push(*event);
                    if unit.len() < config.max_events.max(1) {
                        continue;
                    }
                }
                if !unit.is_empty() {
                    let events = std::mem::take(&mut unit);
                    let count = events.len() as u64;
                    sequence += 1;
                    match write(config.directory.clone(), sequence, events).await {
                        Ok(()) => counters.written.fetch_add(count, Ordering::Relaxed),
                        Err(_) => counters.failed.fetch_add(1, Ordering::Relaxed),
                    };
                }
                if closed {
                    break;
                }
            }
        });
        Collector { sender, stats, task }
    }
}

#[cfg(test)]
mod tests;
