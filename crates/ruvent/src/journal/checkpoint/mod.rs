//! The checkpoint file (2.5): the live events' metadata, the producer table
//! and where replay starts, written slice by slice while the ledger runs.
//!
//! ```text
//! "rvckpt\0" 0x06
//! frame = [len u32][crc32 u32][count v][item]…        (crc over count and items)
//! item  = [tag u8] then
//!    1 Head      start serial v, uptime u64, generation v, commit v
//!    2 (unused; was Name: there are no names)
//!    3 Producer  number v, producer id 16, seq v
//!    4 Entry     head, serial v, standing, location?
//!    5 End       entries v, producers v
//! standing = 0 waiting | 1 delayed, until v | 2 claimed, lease ms v, worker 16
//! location? = 0 | 1 location (as in a segment)
//! ```
//!
//! It is written to `checkpoint.tmp`, synced, renamed to `checkpoint.ckpt` and
//! the directory synced: the file recovery reads is always a whole one. A
//! checkpoint without its `End`, or with a frame that fails its CRC, is damage,
//! and recovery stops on it rather than guess.

use super::codec::{invalid, put_head, put_location, Cut, Stream, HEADER};
use super::segment::{sync_directory, CHECKPOINT, CHECKPOINT_TEMP};
use super::{CheckpointHead, Checkpoints, Entry, Part, ProducerEntry, Replay, Standing};
use crate::envelope::codec::put_varint;
use bytes::BufMut;
use std::collections::VecDeque;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 8] = b"rvckpt\0\x07";
const HEAD: u8 = 1;
const PRODUCER: u8 = 3;
const ENTRY: u8 = 4;
const END: u8 = 5;
/// Producer entries per frame.
const PRODUCERS_PER_FRAME: usize = 4096;

/// Writes the checkpoint of a segment log directory.
///
/// | state | key | lifetime | bound | cleared by |
/// | --- | --- | --- | --- | --- |
/// | the file being written | one checkpoint | `begin` ~ `finish`, `abort` | the live metadata and the producer table | this writer |
pub struct CheckpointFile {
    dir: PathBuf,
    file: Option<BufWriter<File>>,
    entries: u64,
    producers: u64,
    bytes: u64,
}

impl CheckpointFile {
    pub fn new(dir: PathBuf) -> Self {
        CheckpointFile { dir, file: None, entries: 0, producers: 0, bytes: 0 }
    }

    fn frame(&mut self, items: &[u8], count: u64) -> io::Result<()> {
        let mut body = Vec::with_capacity(items.len() + 10);
        put_varint(&mut body, count);
        body.extend_from_slice(items);
        let length = u32::try_from(body.len()).map_err(|_| invalid("checkpoint frame exceeds 4 GiB"))?;
        let file = self.file.as_mut().ok_or_else(|| io::Error::other("no checkpoint begun"))?;
        file.write_all(&length.to_le_bytes())?;
        file.write_all(&crc32fast::hash(&body).to_le_bytes())?;
        file.write_all(&body)?;
        self.bytes += HEADER + body.len() as u64;
        Ok(())
    }

    fn entry(&mut self, entry: &Entry, out: &mut Vec<u8>, count: &mut u64) -> io::Result<()> {
        out.put_u8(ENTRY);
        put_head(&entry.event, out)?;
        put_varint(out, entry.serial);
        match &entry.standing {
            Standing::Waiting => out.put_u8(0),
            Standing::Delayed { until } => {
                out.put_u8(1);
                put_varint(out, *until);
            }
            Standing::Claimed { lease_ms, worker } => {
                out.put_u8(2);
                put_varint(out, *lease_ms);
                out.put_slice(worker);
            }
        }
        match &entry.at {
            Some(at) => {
                out.put_u8(1);
                put_location(out, at);
            }
            None => out.put_u8(0),
        }
        *count += 1;
        Ok(())
    }
}

impl Checkpoints for CheckpointFile {
    fn begin(&mut self, head: &CheckpointHead) -> io::Result<()> {
        self.abort();
        let mut file = BufWriter::new(File::create(self.dir.join(CHECKPOINT_TEMP))?);
        file.write_all(MAGIC)?;
        self.file = Some(file);
        (self.entries, self.producers, self.bytes) = (0, 0, MAGIC.len() as u64);
        let mut out = Vec::with_capacity(32);
        out.put_u8(HEAD);
        put_varint(&mut out, head.start);
        out.put_u64_le(head.uptime);
        put_varint(&mut out, u64::from(head.generation));
        put_varint(&mut out, head.commit);
        self.frame(&out, 1)
    }

    fn write(&mut self, part: &Part) -> io::Result<()> {
        match part {
            Part::Producers(producers) => {
                for chunk in producers.chunks(PRODUCERS_PER_FRAME) {
                    let mut out = Vec::with_capacity(chunk.len() * 30);
                    for producer in chunk {
                        out.put_u8(PRODUCER);
                        put_varint(&mut out, u64::from(producer.number));
                        out.put_slice(&producer.id);
                        put_varint(&mut out, producer.seq);
                    }
                    self.producers += chunk.len() as u64;
                    self.frame(&out, chunk.len() as u64)?;
                }
                Ok(())
            }
            Part::Entries(entries) => {
                let mut out = Vec::with_capacity(entries.len() * 96);
                let mut count = 0;
                for entry in entries {
                    self.entry(entry, &mut out, &mut count)?;
                }
                self.entries += entries.len() as u64;
                self.frame(&out, count)
            }
        }
    }

    fn finish(&mut self) -> io::Result<u64> {
        let mut out = Vec::with_capacity(24);
        out.put_u8(END);
        put_varint(&mut out, self.entries);
        put_varint(&mut out, self.producers);
        self.frame(&out, 1)?;
        let file = self.file.take().ok_or_else(|| io::Error::other("no checkpoint begun"))?;
        let file = file.into_inner().map_err(|error| error.into_error())?;
        file.sync_all()?;
        // Closed before the rename: Windows will not replace a file that is open.
        drop(file);
        std::fs::rename(self.dir.join(CHECKPOINT_TEMP), self.dir.join(CHECKPOINT))?;
        sync_directory(&self.dir)?;
        Ok(self.bytes)
    }

    fn abort(&mut self) {
        if self.file.take().is_some() {
            let _ = std::fs::remove_file(self.dir.join(CHECKPOINT_TEMP));
        }
    }
}

/// Reads a checkpoint file item by item, one frame in memory at a time.
pub(super) struct CheckpointReader {
    path: PathBuf,
    file: BufReader<File>,
    left: u64,
    buffered: VecDeque<Replay>,
    counted: (u64, u64),
    ended: bool,
}

fn damaged(path: &Path, why: impl std::fmt::Display) -> io::Error {
    invalid(format!("checkpoint {} is damaged: {why}; the server stops rather than guess", path.display()))
}

impl CheckpointReader {
    pub fn open(path: PathBuf) -> io::Result<Self> {
        let mut file = File::open(&path)?;
        let left = file.metadata()?.len();
        let mut magic = [0u8; 8];
        file.read_exact(&mut magic).map_err(|_| damaged(&path, "shorter than its header"))?;
        if &magic != MAGIC {
            return Err(invalid(format!("{}: not a ruvent checkpoint of format 7 (format 6 and older are refused, not migrated); it was left untouched", path.display())));
        }
        Ok(CheckpointReader { path, file: BufReader::new(file), left: left - 8, buffered: VecDeque::new(), counted: (0, 0), ended: false })
    }

    fn frame(&mut self) -> io::Result<()> {
        if self.left < HEADER {
            return Err(damaged(&self.path, "it ends before its End item"));
        }
        let mut header = [0u8; 8];
        self.file.read_exact(&mut header)?;
        let length = u64::from(u32::from_le_bytes(header[..4].try_into().unwrap_or_default()));
        let crc = u32::from_le_bytes(header[4..].try_into().unwrap_or_default());
        if length > self.left - HEADER {
            return Err(damaged(&self.path, "a frame runs past the end of the file"));
        }
        let mut body = vec![0; length as usize];
        self.file.read_exact(&mut body)?;
        self.left -= HEADER + length;
        if crc32fast::hash(&body) != crc {
            return Err(damaged(&self.path, "CRC mismatch"));
        }
        let mut stream = Stream::new(&body[..], 0);
        stream.enter(length);
        self.items(&mut stream).map_err(|cut| match cut {
            Cut::Io(error) => error,
            Cut::Eof => damaged(&self.path, "an item cut short"),
            Cut::Malformed(why) => damaged(&self.path, why),
        })?;
        if stream.left() != 0 {
            return Err(damaged(&self.path, "bytes left over in a frame"));
        }
        Ok(())
    }

    fn items(&mut self, stream: &mut Stream<&[u8]>) -> Result<(), Cut> {
        let count = stream.varint()?;
        for _ in 0..count {
            match stream.u8()? {
                HEAD => {
                    let head = CheckpointHead { start: stream.varint()?, uptime: stream.u64()?, generation: stream.varint32()?, commit: stream.varint()? };
                    self.buffered.push_back(Replay::Head(head));
                }
                PRODUCER => {
                    self.counted.1 += 1;
                    self.buffered.push_back(Replay::Producer(ProducerEntry { number: stream.varint32()?, id: stream.id16()?, seq: stream.varint()? }));
                }
                ENTRY => {
                    let event = stream.head()?;
                    let serial = stream.varint()?;
                    let standing = match stream.u8()? {
                        0 => Standing::Waiting,
                        1 => Standing::Delayed { until: stream.varint()? },
                        2 => Standing::Claimed { lease_ms: stream.varint()?, worker: stream.id16()? },
                        _ => return Err(Cut::Malformed("unknown standing")),
                    };
                    let at = match stream.u8()? {
                        0 => None,
                        1 => Some(stream.location()?),
                        _ => return Err(Cut::Malformed("location marker")),
                    };
                    self.counted.0 += 1;
                    self.buffered.push_back(Replay::Entry(Entry { event, at, serial, standing }));
                }
                END => {
                    let counts = (stream.varint()?, stream.varint()?);
                    if counts != self.counted {
                        return Err(Cut::Malformed("End counts differ from the items read"));
                    }
                    self.ended = true;
                }
                _ => return Err(Cut::Malformed("unknown checkpoint item")),
            }
        }
        Ok(())
    }

    pub fn next(&mut self) -> io::Result<Option<Replay>> {
        loop {
            if let Some(item) = self.buffered.pop_front() {
                return Ok(Some(item));
            }
            if self.ended {
                return Ok(None);
            }
            self.frame()?;
        }
    }
}

/// Every item of a checkpoint file, in order. For tools and tests; recovery
/// reads it lazily.
pub fn read_checkpoint(path: impl Into<PathBuf>) -> io::Result<Vec<Replay>> {
    let mut reader = CheckpointReader::open(path.into())?;
    let mut items = Vec::new();
    while let Some(item) = reader.next()? {
        items.push(item);
    }
    Ok(items)
}
