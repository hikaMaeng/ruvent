//! The segment container (2.5): a directory of segment files, a checkpoint
//! file and a lock file.
//!
//! ```text
//! <dir>/lock               held for the log's lifetime: one engine per directory
//! <dir>/0000000001.seg     segments, numbered in one sequence for every lane
//! <dir>/checkpoint.ckpt    the last whole checkpoint (replaced by rename)
//! <dir>/checkpoint.tmp     one being written; removed at open
//! ```
//!
//! The log lane holds frames of records; it rolls to a new segment once the
//! next frame would take a segment past its size (S3). A new segment and a
//! renamed checkpoint are followed by an fsync of the directory, so the file
//! a later write depends on is there after a crash. Segments are never
//! preallocated (S17).
//!
//! At open, the last log segment is read once to its last whole frame: a torn
//! tail — the one write that was in flight — is cut off; a damaged frame with
//! bytes after it stops the open. The log then always continues in a new
//! segment, so the writer never has to learn an old segment's dictionary.
//!
//! A path that is a file — the single-file container of format 3, or a format
//! 2 or older journal — is refused and left as it was, and so is a segment of
//! format 3 (whose envelopes still carried `correlation`). Nothing is migrated (B12).

use super::bodies::SegmentBodies;
use super::checkpoint::CheckpointFile;
use super::codec::{encode_frame, invalid, parse_segment_header, read_frame, segment_header, Defined, Dictionary, Frame, Stream, SEGMENT_HEADER};
use super::recover::LogSource;
use super::spool::Spool;
use super::{Batch, Bodies, Chores, Journal, Lane, Location, Parts, Recovered, SegmentState, Written};
use crate::envelope::ProducerId;
use async_trait::async_trait;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

/// S3: a segment is rolled at this size unless the server says otherwise.
pub const DEFAULT_SEGMENT_BYTES: u64 = 64 << 20;
pub(super) const CHECKPOINT: &str = "checkpoint.ckpt";
pub(super) const CHECKPOINT_TEMP: &str = "checkpoint.tmp";
const LOCK: &str = "lock";

pub(super) fn segment_path(dir: &Path, number: u32) -> PathBuf {
    dir.join(format!("{number:010}.seg"))
}

/// Makes a directory's entries durable. Windows has no handle for that; NTFS
/// journals its metadata.
pub(super) fn sync_directory(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    File::open(dir)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

/// Creates a segment with its header, durably: the file and its directory entry.
pub(super) fn create_segment(dir: &Path, lane: Lane, number: u32, first: u64) -> io::Result<File> {
    let mut file = OpenOptions::new().create_new(true).write(true).open(segment_path(dir, number))?;
    file.write_all(&segment_header(lane, number, first))?;
    file.sync_all()?;
    sync_directory(dir)?;
    Ok(file)
}

/// One segment file found at open.
#[derive(Clone, Copy, Debug)]
pub(super) struct Found {
    pub number: u32,
    pub lane: Lane,
    pub first: u64,
    pub bytes: u64,
}

impl Found {
    fn state(&self) -> SegmentState {
        SegmentState { number: self.number, lane: self.lane, bytes: self.bytes, first: self.first, removed: false }
    }
}

fn list(dir: &Path) -> io::Result<Vec<Found>> {
    let mut found = Vec::new();
    let mut short = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else { continue };
        let Some(stem) = name.strip_suffix(".seg") else { continue };
        let Ok(number) = stem.parse::<u32>() else { continue };
        let mut head = Vec::with_capacity(SEGMENT_HEADER as usize);
        let bytes = std::fs::metadata(&path)?.len();
        File::open(&path)?.take(SEGMENT_HEADER).read_to_end(&mut head)?;
        match parse_segment_header(&head) {
            Some((lane, written, first)) if written == number => found.push(Found { number, lane, first, bytes }),
            None if (head.len() as u64) < SEGMENT_HEADER && magic_prefix(&head) => short.push((number, path)),
            _ => return Err(invalid(format!("{}: not a ruvent segment of format 7 (format 6 and older are refused, not migrated); it was left untouched", path.display()))),
        }
    }
    let highest = found.iter().map(|segment| segment.number).max().unwrap_or(0);
    for (number, path) in short {
        // A header cut short is a segment whose creation did not finish. Only
        // the newest one can be that; it holds nothing.
        if number > highest {
            std::fs::remove_file(&path)?;
        } else {
            return Err(invalid(format!("{}: segment header cut short in the middle of the log", path.display())));
        }
    }
    found.sort_by_key(|segment| segment.number);
    Ok(found)
}

/// Whether a header cut short starts like a segment's.
fn magic_prefix(head: &[u8]) -> bool {
    let magic = super::codec::SEGMENT_MAGIC;
    let shared = head.len().min(magic.len());
    head[..shared] == magic[..shared]
}

/// Reads the last log segment to its last whole frame and cuts a torn tail.
fn trim_tail(dir: &Path, segment: &mut Found) -> io::Result<()> {
    let path = segment_path(dir, segment.number);
    let mut file = File::open(&path)?;
    file.seek(SeekFrom::Start(SEGMENT_HEADER))?;
    let mut stream = Stream::new(BufReader::new(file), SEGMENT_HEADER);
    let mut defined = Defined::default();
    loop {
        match read_frame(&mut stream, segment.bytes, segment.number, &mut defined)? {
            Frame::Whole { .. } => {}
            Frame::End => return Ok(()),
            Frame::Torn { at } => {
                let file = OpenOptions::new().write(true).open(&path)?;
                file.set_len(at)?;
                file.sync_all()?;
                segment.bytes = at;
                return Ok(());
            }
            Frame::Damaged { at, why } => return Err(invalid(format!("{}: damaged at byte {at}: {why}; the server stops rather than drop what follows", path.display()))),
        }
    }
}

/// The segment the log appends to.
struct Current {
    number: u32,
    first: u64,
    file: File,
    bytes: u64,
    dictionary: Dictionary,
}

/// The log lane's writer: owned by the engine's writer task, moved into a
/// blocking call for each append.
struct Writer {
    dir: PathBuf,
    segment_bytes: u64,
    numbers: Arc<AtomicU32>,
    current: Option<Current>,
    /// Producer ids by number, so a new segment can define a number the
    /// ledger defined in an earlier one.
    ///
    /// | key | producer number |
    /// | --- | --- |
    /// | lifetime | defined in the current or the previous segment |
    /// | bound | the producers of two segments |
    /// | cleared by | the writer, at each roll |
    known: HashMap<u32, ProducerId>,
}

impl Writer {
    fn open(&mut self, first: u64, written: &mut Written) -> io::Result<()> {
        let number = self.numbers.fetch_add(1, Ordering::Relaxed);
        let file = create_segment(&self.dir, Lane::Log, number, first)?;
        written.segments.push(SegmentState { number, lane: Lane::Log, bytes: SEGMENT_HEADER, first, removed: false });
        if let Some(old) = self.current.replace(Current { number, first, file, bytes: SEGMENT_HEADER, dictionary: Dictionary::default() }) {
            self.known.retain(|number, _| old.dictionary.producers().contains(number));
        }
        Ok(())
    }

    fn append(&mut self, batch: &Batch) -> io::Result<Written> {
        let mut written = Written::default();
        if self.current.is_none() {
            self.open(batch.first, &mut written)?;
        }
        let mut encoded = {
            let current = self.current.as_mut().ok_or_else(|| io::Error::other("no segment"))?;
            encode_frame(batch, &mut current.dictionary, &mut self.known, current.number, current.bytes)?
        };
        let full = self.current.as_ref().is_some_and(|current| current.bytes > SEGMENT_HEADER && current.bytes + encoded.frame.len() as u64 > self.segment_bytes);
        if full {
            self.open(batch.first, &mut written)?;
            let current = self.current.as_mut().ok_or_else(|| io::Error::other("no segment"))?;
            encoded = encode_frame(batch, &mut current.dictionary, &mut self.known, current.number, current.bytes)?;
        }
        let current = self.current.as_mut().ok_or_else(|| io::Error::other("no segment"))?;
        current.file.write_all(&encoded.frame)?;
        current.file.sync_data()?;
        current.bytes += encoded.frame.len() as u64;
        written.located = encoded.located;
        written.log_bytes = encoded.frame.len() as u64;
        written.segments.retain(|segment| segment.number != current.number);
        written.segments.push(SegmentState { number: current.number, lane: Lane::Log, bytes: current.bytes, first: current.first, removed: false });
        Ok(written)
    }
}

/// The segment container (format 7; formats 3 to 6 had the same container: 3 to 5 with names in the records and policy fields in the envelope, 6 with a claim token in `Claim`).
///
/// Three things keep one write from damaging another, and none is a clock:
/// the lock file; the serial chain (every frame starts where the previous one
/// ended, and recovery stops at a break); and append-only writes, so a crash
/// can only tear the last frame of the last segment.
pub struct SegmentLog {
    writer: Option<Writer>,
    /// Every segment found at open: the first append reports them.
    found: Vec<SegmentState>,
    dir: PathBuf,
    segment_bytes: u64,
    numbers: Arc<AtomicU32>,
    _lock: File,
}

impl SegmentLog {
    /// Opens (or creates) a log in `dir` with segments of [`DEFAULT_SEGMENT_BYTES`].
    pub fn open(dir: impl Into<PathBuf>) -> io::Result<(Self, Recovered)> {
        Self::open_with(dir, DEFAULT_SEGMENT_BYTES)
    }

    /// Opens (or creates) a log in `dir` whose segments roll at `segment_bytes`
    /// (S3). Returns what recovery reads: the checkpoint, then the tail, read
    /// from disk as it is consumed.
    pub fn open_with(dir: impl Into<PathBuf>, segment_bytes: u64) -> io::Result<(Self, Recovered)> {
        let dir = dir.into();
        if dir.is_file() {
            return Err(invalid(format!("{}: a single-file journal (format 2 or older, or format 3's single-file container) is refused, not migrated; it was left untouched", dir.display())));
        }
        std::fs::create_dir_all(&dir)?;
        let lock = OpenOptions::new().create(true).write(true).truncate(false).open(dir.join(LOCK))?;
        lock.try_lock().map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, format!("journal {} is held by another engine", dir.display())))?;
        match std::fs::remove_file(dir.join(CHECKPOINT_TEMP)) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => return Err(error),
            _ => {}
        }
        let mut found = list(&dir)?;
        if let Some(last) = found.iter_mut().filter(|segment| segment.lane == Lane::Log).max_by_key(|segment| segment.number) {
            trim_tail(&dir, last)?;
        }
        let next = found.iter().map(|segment| segment.number).max().map_or(1, |number| number + 1);
        let numbers = Arc::new(AtomicU32::new(next));
        let checkpoint = dir.join(CHECKPOINT);
        let checkpoint = checkpoint.exists().then_some(checkpoint);
        let log: Vec<Found> = found.iter().filter(|segment| segment.lane == Lane::Log).copied().collect();
        let source = LogSource::new(dir.clone(), log, checkpoint);
        let writer = Writer { dir: dir.clone(), segment_bytes, numbers: numbers.clone(), current: None, known: HashMap::new() };
        let found = found.iter().map(Found::state).collect();
        Ok((SegmentLog { writer: Some(writer), found, dir, segment_bytes, numbers, _lock: lock }, Recovered::new(source)))
    }
}

#[async_trait]
impl Journal for SegmentLog {
    async fn append(&mut self, batch: Batch) -> io::Result<Written> {
        let mut writer = self.writer.take().ok_or_else(|| io::Error::other("journal is unusable after an earlier write failure"))?;
        // The writer is put back only on success: after a failed write or
        // fsync the segment's contents are unknown.
        let (writer, result) = tokio::task::spawn_blocking(move || {
            let result = writer.append(&batch);
            (writer, result)
        })
        .await
        .map_err(io::Error::other)?;
        let mut written = result?;
        self.writer = Some(writer);
        if !self.found.is_empty() {
            let mut segments = std::mem::take(&mut self.found);
            segments.append(&mut written.segments);
            written.segments = segments;
        }
        Ok(written)
    }

    fn parts(&mut self) -> Parts {
        let bodies = SegmentBodies::new(self.dir.clone());
        Parts {
            checkpoints: Some(Box::new(CheckpointFile::new(self.dir.clone()))),
            chores: Some(Box::new(SegmentChores { dir: self.dir.clone(), numbers: self.numbers.clone(), segment_bytes: self.segment_bytes, long: None, bodies: bodies.clone() })),
            bodies: Some(Arc::new(bodies)),
            spool: Some(Spool::new(self.dir.clone(), self.numbers.clone(), self.segment_bytes)),
        }
    }
}

/// The long-lived lane's current segment.
struct Long {
    number: u32,
    file: File,
    bytes: u64,
}

/// Relocation into the long-lived lane, and segment removal (2.5).
struct SegmentChores {
    dir: PathBuf,
    numbers: Arc<AtomicU32>,
    segment_bytes: u64,
    long: Option<Long>,
    bodies: SegmentBodies,
}

impl SegmentChores {
    fn seal(&mut self, written: &mut Written) -> io::Result<()> {
        if let Some(long) = &mut self.long {
            long.file.sync_data()?;
            written.segments.push(SegmentState { number: long.number, lane: Lane::Long, bytes: long.bytes, first: 0, removed: false });
        }
        Ok(())
    }
}

impl Chores for SegmentChores {
    fn relocate(&mut self, from: &[Location]) -> io::Result<Written> {
        let mut written = Written::default();
        for (position, at) in from.iter().enumerate() {
            let payload = self.bodies.read(at)?;
            let frame = super::codec::HEADER + payload.len() as u64;
            let roll = self.long.as_ref().is_none_or(|long| long.bytes > SEGMENT_HEADER && long.bytes + frame > self.segment_bytes);
            if roll {
                self.seal(&mut written)?;
                let number = self.numbers.fetch_add(1, Ordering::Relaxed);
                let file = create_segment(&self.dir, Lane::Long, number, 0)?;
                self.long = Some(Long { number, file, bytes: SEGMENT_HEADER });
            }
            let long = self.long.as_mut().ok_or_else(|| io::Error::other("no long-lived segment"))?;
            let mut bytes = Vec::with_capacity(frame as usize);
            bytes.extend_from_slice(&super::codec::body_frame_header(at.len, at.crc));
            bytes.extend_from_slice(&payload);
            long.file.write_all(&bytes)?;
            let offset = long.bytes + super::codec::HEADER;
            long.bytes += frame;
            written.located.push((position as u64, Location { segment: long.number, offset, len: at.len, crc: at.crc }));
        }
        self.seal(&mut written)?;
        Ok(written)
    }

    fn delete(&mut self, segments: &[u32]) -> io::Result<Written> {
        let mut written = Written::default();
        for number in segments {
            if self.long.as_ref().is_some_and(|long| long.number == *number) {
                continue;
            }
            match std::fs::remove_file(segment_path(&self.dir, *number)) {
                Err(error) if error.kind() != io::ErrorKind::NotFound => return Err(error),
                _ => {}
            }
            written.segments.push(SegmentState { number: *number, lane: Lane::default(), bytes: 0, first: 0, removed: true });
        }
        sync_directory(&self.dir)?;
        Ok(written)
    }
}
