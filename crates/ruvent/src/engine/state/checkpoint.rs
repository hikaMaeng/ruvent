//! The fuzzy checkpoint, the ledger's side (2.5, B25, S4).
//!
//! The ledger walks its slot arena a slice at a time between operation
//! batches and hands each slice to the checkpoint writer; operations keep
//! being applied meanwhile. Every entry carries the serial of its event's last
//! change. Replay starts at the serial of the first record that was not on
//! disk when the walk began, and applies a record only if it is newer than
//! what the entry says — so a change made during the walk is replayed if the
//! walk saw the slot before it, and skipped if after. A key the checkpoint
//! does not have is a registration from the tail, or a record for an event
//! that left before the walk reached it.
//!
//! The walk starts when the log's tail since the last checkpoint passes
//! max(one segment, the last checkpoint × 8), or the cap max(8 GiB, the last
//! checkpoint) — bytes, not records, because the producer table is in the
//! checkpoint too. No timer.

use super::slots::{Index, Place, Slot};
use super::State;
use crate::journal::{CheckpointHead, Entry, Part, ProducerEntry, Standing};
use bytes::Bytes;

/// A walk in progress.
///
/// | key | the one checkpoint being written |
/// | --- | --- |
/// | lifetime | `begin_checkpoint` ~ `checkpointed`, `abort_checkpoint` |
/// | bound | one; the slices in flight are bounded by the engine |
/// | cleared by | the ledger |
#[derive(Debug)]
pub(super) struct Scan {
    pub cursor: usize,
    /// The arena's extent when the walk began. A slot past it was filled
    /// after the walk began, so its registration is in the replayed tail; the
    /// walk does not chase a growing arena.
    pub end: usize,
    pub start: u64,
    /// Log bytes landed when the walk began: the tail after it lands is what came since.
    pub written: u64,
    pub done: bool,
}

impl State {
    /// Whether a checkpoint should start now (S4).
    pub fn checkpoint_due(&self) -> bool {
        if !self.journaled || self.scan.is_some() {
            return false;
        }
        let storage = &self.config.storage;
        let last = self.book.checkpoint_bytes;
        let trigger = storage.segment_bytes.max(last.saturating_mul(storage.checkpoint_tail_multiple));
        let cap = storage.checkpoint_tail_cap.max(last);
        self.book.tail >= trigger.min(cap)
    }

    pub fn scanning(&self) -> bool {
        self.scan.is_some()
    }

    /// Starts a walk. `start` is the serial of the first record not on disk,
    /// `commit` the last commit number on disk. Returns the head and the
    /// producer table (entries whose linger ran out are dropped first).
    pub fn begin_checkpoint(&mut self, start: u64, commit: u64) -> (CheckpointHead, Part) {
        self.producers.expire(self.now);
        let mut producers: Vec<ProducerEntry> = self.producers.entries.iter().filter_map(|(id, entry)| entry.number.map(|number| ProducerEntry { number, id: *id, seq: entry.applied })).collect();
        producers.sort_by_key(|producer| producer.number);
        self.scan = Some(Scan { cursor: 0, end: self.slots.extent(), start, written: self.book.written, done: false });
        (CheckpointHead { start, uptime: self.now, generation: self.generation, commit }, Part::Producers(producers))
    }

    /// The next slice of the walk, or `None` once it has covered the arena.
    pub fn scan_slice(&mut self) -> Option<Part> {
        let step = self.config.storage.checkpoint_slice.max(1);
        let scan = self.scan.as_mut().filter(|scan| !scan.done)?;
        let extent = scan.end;
        let from = scan.cursor;
        let to = (from + step).min(extent);
        scan.cursor = to;
        if from >= extent {
            scan.done = true;
            return None;
        }
        let entries = (from..to).filter_map(|index| self.slots.get(index as Index)).filter(|slot| slot.fixed.durable).map(entry).collect();
        Some(Part::Entries(entries))
    }

    /// The checkpoint is durable: it is where replay starts now.
    pub fn checkpointed(&mut self, bytes: u64) {
        let Some(scan) = self.scan.take() else { return };
        self.book.checkpoint_start = Some(scan.start);
        self.book.checkpoint_bytes = bytes;
        self.book.tail = self.book.written.saturating_sub(scan.written);
        self.book.changed = true;
        self.counters.checkpoints += 1;
    }

    pub fn abort_checkpoint(&mut self) {
        self.scan = None;
    }

    /// The whole checkpoint at once, from a stopped ledger (2.8, B22).
    pub fn full_checkpoint(&mut self, start: u64, commit: u64) -> (CheckpointHead, Vec<Part>) {
        let (head, producers) = self.begin_checkpoint(start, commit);
        let mut parts = vec![producers];
        while let Some(part) = self.scan_slice() {
            parts.push(part);
        }
        (head, parts)
    }
}

/// A durable slot as a checkpoint entry.
fn entry(slot: &Slot) -> Entry {
    let mut event = slot.envelope();
    event.payload = Bytes::new();
    let standing = match &slot.place {
        Place::Waiting | Place::Held => Standing::Waiting,
        Place::Delayed { until } => Standing::Delayed { until: *until },
        Place::Claimed(attempt) => Standing::Claimed { token: attempt.token, lease_ms: attempt.lease_ms, credential: attempt.credential.clone() },
    };
    Entry { event, root: slot.fixed.root, at: slot.at, serial: slot.serial, standing }
}
