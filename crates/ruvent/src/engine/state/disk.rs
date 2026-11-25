//! Disk accounting (2.6, S6), the segment book, the body cache and
//! relocation (2.5).
//!
//! The ledger knows which payload lives in which segment, so it keeps each
//! segment's live count. A segment may go once nothing in it is live, the
//! last checkpoint starts after it (log lane) and no read pins it (the engine
//! checks the pins). A live count only drops once the record that moved the
//! payload away is on disk: before that, a crash would replay a registration
//! pointing into the removed segment.
//!
//! The body cache is the payloads held in memory. A durable payload stays in
//! memory after its registration lands until memory passes the high
//! watermark; then the newest are released — released only, never written,
//! because the segment already holds them (2.5). A cold payload is read on the
//! read path before it goes out, and the next few of its lane are read ahead.

use super::slots::Index;
use super::State;
use crate::journal::{Lane, Location, Record, SegmentState, Written};
use bytes::Bytes;
use std::collections::{BTreeMap, VecDeque};

/// A payload to read ahead into the cache: the next events of a lane whose
/// head went out cold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Warm {
    index: Index,
    generation: u32,
    at: Location,
}

impl Warm {
    pub fn at(&self) -> Location {
        self.at
    }
}

/// One segment in the book.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Segment {
    pub lane: Lane,
    pub bytes: u64,
    /// The serial of its first record (log lane).
    pub first: u64,
    /// The container reported it: lane, size and first serial are real.
    pub known: bool,
    /// Live payloads in it, and their bytes.
    pub live: u64,
    pub live_bytes: u64,
    /// Handed to the engine for removal.
    pub deleting: bool,
}

/// A relocation being gathered or run: the live payloads of one segment.
#[derive(Debug, Default)]
pub(super) struct Relocation {
    pub segment: u32,
    /// The arena index the gathering scan has reached, and where it stops: the
    /// extent when the segment was chosen (a later slot cannot point into a
    /// sealed segment).
    pub cursor: usize,
    pub end: usize,
    pub items: Vec<(Index, u32, Location)>,
    /// Handed to the chores; waiting for `relocated`.
    pub sent: bool,
}

/// The segment book and the disk-side state of the ledger.
///
/// | state | key | scope | lifetime | bound | cleared by |
/// | --- | --- | --- | --- | --- | --- |
/// | segments | segment number | log directory | reported or first referenced ~ reported removed | segments on disk | the ledger, on the container's reports |
/// | frees | (commit, segment) | ledger | a payload leaves ~ that commit lands | durable removals and relocations in flight | the ledger, in `State::landed` |
/// | relocation | one segment | ledger | chosen ~ relocated | one at a time; its live payloads | the ledger |
#[derive(Debug, Default)]
pub(super) struct Book {
    pub segments: BTreeMap<u32, Segment>,
    pub frees: VecDeque<(u64, u32, u64)>,
    /// Where the last whole checkpoint starts, and its size.
    pub checkpoint_start: Option<u64>,
    pub checkpoint_bytes: u64,
    /// Log-lane bytes landed in all, and since the last checkpoint's start.
    pub written: u64,
    pub tail: u64,
    /// Something changed that can make a segment removable.
    pub changed: bool,
    pub relocation: Option<Relocation>,
}

impl Book {
    pub fn live_add(&mut self, at: &Location) {
        let segment = self.segments.entry(at.segment).or_default();
        segment.live += 1;
        segment.live_bytes += u64::from(at.len);
    }

    pub fn live_remove(&mut self, segment: u32, bytes: u64) {
        if let Some(entry) = self.segments.get_mut(&segment) {
            entry.live = entry.live.saturating_sub(1);
            entry.live_bytes = entry.live_bytes.saturating_sub(bytes);
            if entry.live == 0 {
                self.changed = true;
            }
        }
    }

    pub fn report(&mut self, state: &SegmentState) {
        if state.removed {
            self.segments.remove(&state.number);
            return;
        }
        let segment = self.segments.entry(state.number).or_default();
        segment.lane = state.lane;
        segment.bytes = segment.bytes.max(state.bytes);
        if state.lane == Lane::Log && !segment.known {
            segment.first = state.first;
        }
        segment.known = true;
        self.changed = true;
    }

    pub fn used(&self) -> u64 {
        self.segments.values().map(|segment| segment.bytes).sum::<u64>() + self.checkpoint_bytes
    }

    /// The highest number of each lane: the segment still being written.
    fn newest(&self, lane: Lane) -> Option<u32> {
        self.segments.iter().rev().find(|(_, segment)| segment.known && segment.lane == lane).map(|(number, _)| *number)
    }

    /// Whether the segment takes no more writes.
    pub fn sealed(&self, number: u32, segment: &Segment) -> bool {
        segment.known && self.newest(segment.lane).is_some_and(|newest| number < newest)
    }

    /// Segments that may go: nothing live, sealed, and for the log lane wholly
    /// before the last checkpoint's start. Each is handed out once.
    pub fn removable(&mut self) -> Vec<u32> {
        if !std::mem::take(&mut self.changed) {
            return Vec::new();
        }
        let start = self.checkpoint_start;
        let mut found = Vec::new();
        for (number, segment) in &self.segments {
            if segment.live > 0 || segment.deleting || !self.sealed(*number, segment) {
                continue;
            }
            if segment.lane == Lane::Log {
                let next = self.segments.range(number + 1..).find(|(_, next)| next.known && next.lane == Lane::Log).map(|(_, next)| next.first);
                if !(start.is_some() && next.is_some_and(|next| Some(next) <= start)) {
                    continue;
                }
            }
            found.push(*number);
        }
        for number in &found {
            if let Some(segment) = self.segments.get_mut(number) {
                segment.deleting = true;
            }
        }
        found
    }

    /// Bytes held by sealed segments that something live keeps from going.
    pub fn pinned_dead(&self) -> u64 {
        self.segments.iter().filter(|(number, segment)| segment.live > 0 && self.sealed(**number, segment)).map(|(_, segment)| segment.bytes.saturating_sub(segment.live_bytes)).sum()
    }

    /// The sealed segment with the smallest live share, to relocate first.
    pub fn sparsest(&self) -> Option<u32> {
        self.segments
            .iter()
            .filter(|(number, segment)| segment.live > 0 && !segment.deleting && segment.bytes > 0 && self.sealed(**number, segment))
            .min_by(|(_, a), (_, b)| (a.live_bytes as u128 * b.bytes as u128).cmp(&(b.live_bytes as u128 * a.bytes as u128)))
            .map(|(number, _)| *number)
    }
}

impl State {
    /// Disk in use: every segment and the checkpoint.
    pub(super) fn disk_used(&self) -> u64 {
        self.book.used()
    }

    /// S6: the capacity less max(segments × S, fraction × capacity). `None`
    /// when no capacity is configured: the disk watermark is off.
    pub(super) fn disk_budget(&self) -> Option<u64> {
        let storage = &self.config.storage;
        storage.disk_capacity.map(|capacity| {
            let reserve = (storage.disk_reserve_segments.saturating_mul(storage.segment_bytes)).max((capacity as f64 * storage.disk_reserve) as u64);
            capacity.saturating_sub(reserve)
        })
    }

    /// A write landed: segments reported, payload locations for the
    /// registrations it carried, the log's growth. `through` is the serial
    /// after its last record.
    pub fn wrote(&mut self, written: Written, through: u64) {
        for segment in &written.segments {
            if segment.removed {
                self.counters.segments_removed += 1;
            }
            self.book.report(segment);
        }
        self.book.written += written.log_bytes;
        self.book.tail += written.log_bytes;
        let mut located = written.located.into_iter().peekable();
        while let Some(&(serial, index, generation)) = self.unlocated.front() {
            if serial >= through {
                break;
            }
            self.unlocated.pop_front();
            while located.next_if(|(found, _)| *found < serial).is_some() {}
            let Some((_, at)) = located.next_if(|(found, _)| *found == serial) else { continue };
            let Some(slot) = self.slots.current_mut(index, generation) else { continue };
            if slot.at.is_some() {
                continue;
            }
            slot.at = Some(at);
            let cached = !slot.payload.is_empty();
            self.book.live_add(&at);
            if cached {
                self.cache.push_back((index, generation));
            }
        }
        self.tidy_cache();
    }

    /// Drops cache entries of events that left, from the front, and compacts
    /// the queue when stale entries outnumber the events.
    fn tidy_cache(&mut self) {
        while let Some(&(index, generation)) = self.cache.front() {
            if self.slots.current(index, generation).is_some_and(|slot| !slot.cold() && !slot.payload.is_empty()) {
                break;
            }
            self.cache.pop_front();
        }
        if self.cache.len() > 2 * self.slots.len() + 1024 {
            let slots = &self.slots;
            self.cache.retain(|(index, generation)| slots.current(*index, *generation).is_some_and(|slot| !slot.payload.is_empty()));
        }
    }

    /// Releases cached durable payloads, newest first, until managed memory
    /// is at `target` or nothing more can go. Nothing is written (2.5).
    pub(super) fn release_cache(&mut self, target: u64) {
        while self.memory_used() > target {
            let Some((index, generation)) = self.cache.pop_back() else { return };
            let Some(slot) = self.slots.current_mut(index, generation) else { continue };
            if !slot.fixed.durable || slot.at.is_none() || slot.payload.is_empty() {
                continue;
            }
            let len = slot.payload.len() as u64;
            slot.payload = Bytes::new();
            self.slot_bytes = self.slot_bytes.saturating_sub(len);
            self.counters.released += 1;
        }
    }

    /// The payload of a slot that leaves the ledger or moves: its segment's
    /// live count drops once the record saying so is on disk; a spilled
    /// volatile payload's at once (it is journaled nowhere).
    pub(super) fn free_payload(&mut self, at: Option<Location>, durable: bool) {
        let Some(at) = at else { return };
        if durable {
            self.book.frees.push_back((self.carrying, at.segment, u64::from(at.len)));
        } else {
            self.book.live_remove(at.segment, u64::from(at.len));
        }
    }

    /// Frees whose records are now on disk.
    pub(super) fn apply_frees(&mut self, commit: u64) {
        while self.book.frees.front().is_some_and(|(waiting, _, _)| *waiting <= commit) {
            let Some((_, segment, bytes)) = self.book.frees.pop_front() else { break };
            self.book.live_remove(segment, bytes);
        }
    }

    /// The next events of `lane` that are cold, to read ahead (2.5). Nothing
    /// is read ahead while memory is at the high watermark.
    pub(super) fn read_ahead(&mut self, lane: &super::lanes::LaneKey) {
        let wanted = self.config.storage.read_ahead;
        if wanted == 0 || self.memory_used() >= self.high_watermark() {
            return;
        }
        let Some(found) = self.lanes.get(lane) else { return };
        let mut picked = Vec::new();
        for &index in found.queue.values().take(wanted) {
            if self.warming.contains(&index) {
                continue;
            }
            if let Some(slot) = self.slots.get(index).filter(|slot| slot.cold()) {
                if let Some(at) = slot.at {
                    picked.push(Warm { index, generation: self.slots.generation(index), at });
                }
            }
        }
        for warm in picked {
            self.warming.insert(warm.index);
            self.effects.warm.push(warm);
        }
    }

    /// A payload read ahead came back: cached if its event is still there,
    /// still cold, and memory is under the high watermark.
    pub fn warmed(&mut self, warm: Warm, payload: Bytes) {
        self.warming.remove(&warm.index);
        let high = self.high_watermark();
        if self.memory_used() + payload.len() as u64 >= high {
            return;
        }
        let Some(slot) = self.slots.current_mut(warm.index, warm.generation) else { return };
        if !slot.cold() || slot.at != Some(warm.at) {
            return;
        }
        self.slot_bytes += payload.len() as u64;
        slot.payload = payload;
        self.cache.push_back((warm.index, warm.generation));
        self.counters.read_ahead += 1;
    }

    /// Run at the end of every batch: hand out removable segments; choose and
    /// gather a relocation when dead-but-pinned bytes pass the share of the
    /// disk budget (S3).
    pub(super) fn tend_disk(&mut self) {
        let removable = self.book.removable();
        self.effects.delete.extend(removable);
        if self.book.relocation.is_none() {
            let threshold = self.disk_budget().map(|budget| (budget as f64 * self.config.storage.relocate_at) as u64);
            if threshold.is_some_and(|threshold| self.book.pinned_dead() > threshold) {
                if let Some(segment) = self.book.sparsest() {
                    self.book.relocation = Some(Relocation { segment, end: self.slots.extent(), ..Relocation::default() });
                }
            }
        }
        let step = self.config.storage.checkpoint_slice.max(1);
        let Some(relocation) = self.book.relocation.as_mut().filter(|relocation| !relocation.sent) else { return };
        let extent = relocation.end;
        let end = (relocation.cursor + step).min(extent);
        for index in relocation.cursor..end {
            let index = index as Index;
            if let Some(slot) = self.slots.get(index) {
                if let Some(at) = slot.at.filter(|at| at.segment == relocation.segment) {
                    relocation.items.push((index, self.slots.generation(index), at));
                }
            }
        }
        relocation.cursor = end;
        if end < extent {
            return;
        }
        if relocation.items.is_empty() {
            self.book.relocation = None;
            return;
        }
        relocation.sent = true;
        self.effects.relocate = relocation.items.iter().map(|(_, _, at)| *at).collect();
    }

    /// A relocation landed: each payload still where it was gets its new
    /// location and a `Relocate` record; its old segment's live count drops
    /// once that record is on disk.
    pub fn relocated(&mut self, written: Written) {
        let Some(relocation) = self.book.relocation.take() else { return };
        for segment in &written.segments {
            self.book.report(segment);
        }
        for (position, new) in written.located {
            let Some(&(index, generation, old)) = relocation.items.get(position as usize) else { continue };
            let Some(slot) = self.slots.current(index, generation) else { continue };
            if slot.at != Some(old) {
                continue;
            }
            let (durable, key) = (slot.fixed.durable, slot.fixed.key);
            self.free_payload(Some(old), durable);
            if let Some(slot) = self.slots.get_mut(index) {
                slot.at = Some(new);
            }
            self.book.live_add(&new);
            if durable {
                self.record_for(index, Record::Relocate { key, at: new });
            }
            self.counters.relocated += 1;
        }
    }
}
