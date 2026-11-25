//! Durability. Not a method: the engine writes it, nobody calls it.
//!
//! The journal exists so that a restart can carry on: it holds the claimable
//! events on durable channels still waiting, delayed or claimed, the producer
//! sequence table, the generation and the uptime clock. Dispatch events and
//! events on volatile channels are never written.
//!
//! ## Format 3: a record set and a container
//!
//! The record set ([`Record`]) is what the ledger says; the container is how
//! it reaches the disk: a directory of segments ([`SegmentLog`], 2.5).
//!
//! | record | means |
//! | --- | --- |
//! | `Register` | this registration exists, with this envelope (failures, times and group included) and this root channel; its payload inline, or at a location written earlier (a streamed body) |
//! | `Body` | a payload written inline with no registration; skipped unless something points at it |
//! | `Claim` | this registration is claimed under this token, lease length and credential |
//! | `Remove` | this registration is gone: completed, deleted or expired |
//! | `Reclaim` | its lease ran out: waiting again with this failure count |
//! | `Release` | the worker gave it back: this failure count, visible again at this uptime (or now) |
//! | `Isolate` | it moved to this isolation channel with this failure count and this expiry |
//! | `Relocate` | its payload now lives at this location (the long-lived lane) |
//! | `ProducerDef` | producer number ↔ producer id |
//! | `Producer` | this producer's last applied sequence number |
//! | `Generation` | the server now runs in this generation |
//!
//! Records name registrations by work key, which is unique per registration,
//! so replay never has to look anything up by id. All the net change one
//! operation causes — a completion's removal, all of its follow-ups and the
//! producer's sequence — is written in one frame, and replay keeps whole
//! frames only, so a torn write leaves either all of an operation or none.
//!
//! Every frame carries the accumulated uptime (ms) and its commit number, and
//! every record a serial number. The fuzzy checkpoint splits replay on the
//! serial: a record is applied only if it is newer than what the checkpoint
//! saw of its event (2.5).
//!
//! ## The pieces and who runs them
//!
//! | piece | runs on | writes |
//! | --- | --- | --- |
//! | [`Journal::append`] | the engine's writer task, one frame in flight | the log lane |
//! | [`Checkpoints`] | its own blocking thread | `checkpoint.ckpt` |
//! | [`Chores`] | its own blocking thread | the long-lived lane; removes segments |
//! | [`Bodies`] | blocking reads, any number | nothing |
//! | [`Spool`] | the connection owners | the body lane (streamed follow-up bodies, 2.6) |

mod bodies;
mod checkpoint;
mod codec;
mod recover;
mod segment;
mod spool;

use crate::envelope::{ClaimToken, Digest, Envelope, ProducerId, WorkKey};
use crate::names::Name;
use async_trait::async_trait;
use bytes::Bytes;
use std::io;
use std::sync::Arc;

pub use bodies::SegmentBodies;
pub use checkpoint::{read_checkpoint, CheckpointFile};
pub use segment::{SegmentLog, DEFAULT_SEGMENT_BYTES};
pub use spool::{Spool, SpoolWrite, Spooled};

/// Where a payload lives on disk: a segment, the offset of its first byte, its
/// length and its CRC-32. The CRC lets a reader check the bytes it got back
/// without reading the frame around them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct Location {
    pub segment: u32,
    pub offset: u64,
    pub len: u32,
    pub crc: u32,
}

impl Location {
    /// The payload's digest, as the ledger compares contents (2.1).
    pub fn digest(&self) -> Digest {
        Digest { len: self.len, crc: self.crc }
    }
}

/// The three kinds of segment (2.5).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub enum Lane {
    /// Frames of records, payloads inline. Replayed from the checkpoint's start.
    #[default]
    Log,
    /// Payloads moved out of segments they kept alive (relocation). Never replayed.
    Long,
    /// Payloads streamed from work connections before their registration (2.6). Never replayed.
    Body,
}

/// One segment as the container reports it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentState {
    pub number: u32,
    pub lane: Lane,
    /// Its size in bytes, header included.
    pub bytes: u64,
    /// The serial of its first record (log lane); 0 for the other lanes.
    pub first: u64,
    /// It is gone from the disk.
    pub removed: bool,
}

/// What one write did on disk.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Written {
    /// Payload locations. For an append: by the serial of the `Register` that
    /// carried the payload inline. For a relocation: by position in the request.
    /// A journal that reports locations must hand out a [`Bodies`] reader in
    /// its [`Parts`]: the ledger may drop a located payload from memory.
    pub located: Vec<(u64, Location)>,
    /// Segments created, grown or removed by this write (all of them on the first append).
    pub segments: Vec<SegmentState>,
    /// Bytes added to the log lane: the checkpoint's trigger counts these.
    pub log_bytes: u64,
}

/// One statement about durable state.
///
/// `Register` is the large variant and also the most common one; records are
/// built once and moved into a batch, so boxing it would cost an allocation
/// per registration for nothing.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Record {
    /// A registration. `event` has its work key, the absolute `not_before` and
    /// `expires`, and its failure count; `root` is its root channel. With `at`
    /// the payload was written earlier (streamed) and `event.payload` is empty.
    Register {
        event: Envelope,
        root: Name,
        at: Option<Location>,
    },
    /// A payload with no registration around it.
    Body {
        payload: Bytes,
    },
    Claim {
        key: WorkKey,
        token: ClaimToken,
        lease_ms: u64,
        credential: Name,
    },
    Remove(WorkKey),
    Reclaim {
        key: WorkKey,
        failures: u16,
    },
    /// `visible_at` is absolute uptime; `None` means waiting at once.
    Release {
        key: WorkKey,
        failures: u16,
        visible_at: Option<u64>,
    },
    Isolate {
        key: WorkKey,
        channel: Name,
        failures: u16,
        expires: Option<u64>,
    },
    Relocate {
        key: WorkKey,
        at: Location,
    },
    ProducerDef {
        number: u32,
        id: ProducerId,
    },
    Producer {
        number: u32,
        seq: u64,
    },
    Generation(u32),
}

/// One write: the records of one or more ledger batches, applied in order.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Batch {
    /// Accumulated uptime (ms) when the batch was handed to the writer.
    pub uptime: u64,
    /// The commit number: the engine's flight number for this write.
    pub commit: u64,
    /// The serial number of the first record; the rest follow one by one.
    pub first: u64,
    pub records: Vec<Record>,
}

/// Where an event stood when a checkpoint saw it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Standing {
    Waiting,
    /// Visible again at this uptime.
    Delayed {
        until: u64,
    },
    Claimed {
        token: ClaimToken,
        lease_ms: u64,
        credential: Name,
    },
}

/// One live event in a checkpoint: its metadata, where its payload is, and
/// the serial of its last change (2.5).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// Head, work key, current channel, failures, times; the payload empty.
    pub event: Envelope,
    pub root: Name,
    /// `None` while its registration was not yet on disk when the checkpoint
    /// started: the registration is then in the replayed tail, which supplies it.
    pub at: Option<Location>,
    pub serial: u64,
    pub standing: Standing,
}

/// One producer table entry in a checkpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProducerEntry {
    pub number: u32,
    pub id: ProducerId,
    pub seq: u64,
}

/// What a checkpoint starts with.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CheckpointHead {
    /// The serial of the first record that was not on disk when the scan
    /// began: replay starts here.
    pub start: u64,
    pub uptime: u64,
    pub generation: u32,
    /// The last commit number on disk when the scan began.
    pub commit: u64,
}

/// A piece of a checkpoint, handed from the ledger to the checkpoint writer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Part {
    Producers(Vec<ProducerEntry>),
    Entries(Vec<Entry>),
}

/// One thing recovery reads, in order: the checkpoint (head, producers,
/// entries), then the tail of the log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Replay {
    Head(CheckpointHead),
    Producer(ProducerEntry),
    Entry(Entry),
    /// A log record and its serial. A `Register` read from a segment has an
    /// empty payload and `at` set: payloads are not read at recovery.
    Record {
        serial: u64,
        record: Record,
    },
    /// A segment's own definition of a producer number. Carries no serial.
    ProducerName {
        number: u32,
        id: ProducerId,
    },
}

/// Where the log ended.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct End {
    /// The uptime of the last whole frame (or of the checkpoint): the clock
    /// carries on from here.
    pub uptime: u64,
    /// The serial the next record takes.
    pub serial: u64,
    /// The commit number of the last whole frame.
    pub commit: u64,
}

/// A pull-based reading of what a journal held. It reads from disk as it is
/// consumed, so recovery holds metadata, never the whole log (2.5).
pub trait Source: Send {
    /// The next item; `Ok(None)` at the end. An error is damage in the middle
    /// of the log or the checkpoint: the server must not start on it.
    fn next(&mut self) -> io::Result<Option<Replay>>;

    /// Where the log ended. Valid once [`Source::next`] returned `Ok(None)`.
    fn end(&self) -> End;
}

/// What a journal held when it was opened.
pub struct Recovered {
    source: Box<dyn Source>,
}

impl Default for Recovered {
    fn default() -> Self {
        Recovered::from_replay(Vec::new(), End::default())
    }
}

impl std::fmt::Debug for Recovered {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Recovered")
    }
}

/// A source over items already in memory.
struct Listed {
    items: std::vec::IntoIter<Replay>,
    end: End,
}

impl Source for Listed {
    fn next(&mut self) -> io::Result<Option<Replay>> {
        Ok(self.items.next())
    }

    fn end(&self) -> End {
        self.end
    }
}

impl Recovered {
    pub fn new(source: impl Source + 'static) -> Self {
        Recovered { source: Box::new(source) }
    }

    /// Log records with no checkpoint, numbered so the last one is `end.serial − 1`.
    pub fn from_records(records: Vec<Record>, end: End) -> Self {
        let first = end.serial.saturating_sub(records.len() as u64);
        let items = records.into_iter().enumerate().map(|(at, record)| Replay::Record { serial: first + at as u64, record }).collect();
        Recovered::from_replay(items, end)
    }

    pub fn from_replay(items: Vec<Replay>, end: End) -> Self {
        Recovered::new(Listed { items: items.into_iter(), end })
    }

    pub fn into_source(self) -> Box<dyn Source> {
        self.source
    }
}

/// Writes a checkpoint. Blocking: it runs on a thread of its own.
pub trait Checkpoints: Send + 'static {
    fn begin(&mut self, head: &CheckpointHead) -> io::Result<()>;
    fn write(&mut self, part: &Part) -> io::Result<()>;
    /// Makes the checkpoint durable and the one recovery reads. Returns its size.
    fn finish(&mut self) -> io::Result<u64>;
    /// Drops a checkpoint that was begun; the previous one stays.
    fn abort(&mut self);
}

/// Work on segments beside the writer. Blocking: it runs on a thread of its own.
pub trait Chores: Send + 'static {
    /// Copies each payload to the long-lived lane, durably. `located` answers
    /// by position in `from`.
    fn relocate(&mut self, from: &[Location]) -> io::Result<Written>;
    /// Removes whole segments.
    fn delete(&mut self, segments: &[u32]) -> io::Result<Written>;
}

/// Reads payloads by location. Blocking; shared by any number of reads.
pub trait Bodies: Send + Sync + 'static {
    /// The payload at `at`, checked against its CRC.
    fn read(&self, at: &Location) -> io::Result<Bytes>;
}

/// The pieces of a journal that run beside its writer, taken once at start.
#[derive(Default)]
pub struct Parts {
    pub checkpoints: Option<Box<dyn Checkpoints>>,
    pub chores: Option<Box<dyn Chores>>,
    pub bodies: Option<Arc<dyn Bodies>>,
    pub spool: Option<Spool>,
}

/// Where the engine's records go. One task owns it; writes never overlap.
#[async_trait]
pub trait Journal: Send + 'static {
    /// Writes `batch` as one frame and makes it durable. Any error is
    /// terminal: the engine stops rather than retry, because after a failed
    /// write the medium is in a state no retry can reason about.
    async fn append(&mut self, batch: Batch) -> io::Result<Written>;

    /// Whether anything survives a restart. A journal that keeps nothing makes
    /// every channel volatile: no records, and commit at apply.
    fn durable(&self) -> bool {
        true
    }

    /// The checkpoint writer, the chores, the body reader and the spool, if
    /// this journal has them. Called once, before the first append.
    fn parts(&mut self) -> Parts {
        Parts::default()
    }
}

/// Keeps nothing. For tests and for an engine without a disk.
pub struct Volatile;

#[async_trait]
impl Journal for Volatile {
    async fn append(&mut self, _batch: Batch) -> io::Result<Written> {
        Ok(Written::default())
    }

    fn durable(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests;
