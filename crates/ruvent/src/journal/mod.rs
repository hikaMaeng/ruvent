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
//! it reaches the disk. Today's container is one file ([`FileJournal`]); the
//! segment container replaces it without changing a record.
//!
//! | record | means |
//! | --- | --- |
//! | `Register` | this registration exists, with this envelope (failures, times and group included) and this root channel |
//! | `Body` | a payload written ahead of its registration (a streamed follow-up body); skipped unless a `Register` points at it |
//! | `Claim` | this registration is claimed under this token, lease length and credential |
//! | `Remove` | this registration is gone: completed, deleted or expired |
//! | `Reclaim` | its lease ran out: waiting again with this failure count |
//! | `Release` | the worker gave it back: this failure count, visible again at this uptime (or now) |
//! | `Isolate` | it moved to this isolation channel with this failure count and this expiry |
//! | `Relocate` | its payload now lives at this location |
//! | `ProducerDef` | producer number ↔ producer id |
//! | `Producer` | this producer's last applied sequence number |
//! | `Generation` | the server now runs in this generation |
//!
//! Records name registrations by work key, which is unique per registration,
//! so replay never has to look anything up by id. All the net change one
//! operation causes — a completion's removal, all of its follow-ups and the
//! producer's sequence — is written in one batch, and replay keeps whole
//! batches only, so a torn write leaves either all of an operation or none.
//!
//! Every batch carries the accumulated uptime (ms) and its commit number, and
//! its records carry serial numbers `first..next` — the place the fuzzy
//! checkpoint will use to split replay.

mod codec;
mod file;

use crate::envelope::{ClaimToken, Envelope, ProducerId, WorkKey};
use crate::names::Name;
use async_trait::async_trait;
use bytes::Bytes;
use std::io;

pub use file::FileJournal;

/// Where a payload lives once it is not (only) in memory: a segment, an
/// offset and a length. Filled by the segment container; the single-file
/// container keeps every payload inline and never produces one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct Location {
    pub segment: u32,
    pub offset: u64,
    pub len: u32,
}

/// One statement about durable state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Record {
    /// A registration. `event` has its work key, the absolute `not_before` and
    /// `expires`, and its failure count; `root` is its root channel.
    Register {
        event: Envelope,
        root: Name,
    },
    /// A payload written ahead of the registration that will point at it.
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
    /// The serial number of the first record. A snapshot's records take none.
    pub first: u64,
    pub records: Vec<Record>,
}

/// What a journal held when it was opened.
#[derive(Default, Debug)]
pub struct Recovered {
    pub records: Vec<Record>,
    /// The uptime of the last whole batch: the clock carries on from here.
    pub uptime: u64,
    /// The serial number the next record takes.
    pub serial: u64,
    /// The commit number of the last whole batch.
    pub commit: u64,
}

/// Where the engine's records go. One task owns it; writes never overlap.
#[async_trait]
pub trait Journal: Send + 'static {
    /// Writes `batch` as one frame and makes it durable. Any error is
    /// terminal: the engine stops rather than retry, because after a failed
    /// write the medium is in a state no retry can reason about.
    async fn append(&mut self, batch: Batch) -> io::Result<()>;

    /// Replaces the whole journal with `batch`, a snapshot of the live state.
    /// Atomic: afterwards the journal is either the old one or the new one.
    async fn rewrite(&mut self, batch: Batch) -> io::Result<()>;

    /// Whether anything survives a restart. A journal that keeps nothing makes
    /// every channel volatile: no records, and commit at apply.
    fn durable(&self) -> bool {
        true
    }
}

/// Keeps nothing. For tests and for an engine without a disk.
pub struct Volatile;

#[async_trait]
impl Journal for Volatile {
    async fn append(&mut self, _batch: Batch) -> io::Result<()> {
        Ok(())
    }

    async fn rewrite(&mut self, _batch: Batch) -> io::Result<()> {
        Ok(())
    }

    fn durable(&self) -> bool {
        false
    }
}

pub use codec::{decode_frames, encode_frame, FrameNames, MAGIC};

#[cfg(test)]
mod tests;
