//! Managed memory, disk, the high watermarks and root-type pause (2.6).
//!
//! A root is the type of the event that started a lineage: a registration
//! is its own root, a follow-up counts to its parent's. Channels are one per
//! request and say nothing about who floods; the type does (plan 9.15).
//!
//! Each resource has a budget (the limit minus the reserve) and one high
//! watermark, measured on what is left once paused roots' shares are taken
//! out, so a paused root cannot push others over it. A root's share of memory
//! is its events' metadata and volatile payloads — what memory cannot shed;
//! its share of disk is its payloads in segments. A paused root cannot grow
//! its share: its producers are told `Paused`, and its events are not assigned
//! from net-growth lanes while its share is above its share at the pause.
//!
//! What is done at the watermark, cheapest first: release cached durable
//! payloads (2.5); pause the biggest root of the resource that is high; past
//! the limit, assign from no net-growth lane at all.

use super::roots::{Notified, Paused, Root};
use super::slots::Slot;
use super::State;
use crate::engine::ops::{Outbound, SessionId};

/// The most root types remembered as "drained before" once they hold
/// nothing (the `drained` set). Past it the set is cleared, which can only
/// make a root look new — an early pause it would have been spared.
pub(super) const DRAINED: usize = 4096;

/// What one event counts for.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Usage {
    /// Managed memory: metadata and the payload while it is in memory.
    pub memory: u64,
    /// Its root's memory share: metadata, and the payload if volatile.
    pub share: u64,
    /// Its root's disk share: the payload, if it is in a segment.
    pub disk: u64,
}

impl State {
    pub(super) fn usage(&self, slot: &Slot) -> Usage {
        let meta = self.config.memory.slot_bytes;
        let held = slot.payload.len() as u64;
        Usage { memory: meta + held, share: meta + if slot.fixed.durable { 0 } else { held }, disk: if slot.fixed.durable || slot.at.is_some() { slot.len() } else { 0 } }
    }

    pub(super) fn account_add(&mut self, root: u16, usage: Usage) {
        self.slot_bytes += usage.memory;
        if !self.roots.contains_key(&root) {
            // A root that drained before it emptied is not one that never drains.
            let ended = u64::from(self.drained.remove(&root));
            self.roots.insert(root, Root { ended, ..Root::default() });
        }
        let entry = self.roots.entry(root).or_default();
        entry.events += 1;
        entry.bytes += usage.share;
        entry.disk += usage.disk;
        if self.grown.last() != Some(&root) {
            self.grown.push(root);
        }
    }

    pub(super) fn account_remove(&mut self, root: u16, usage: Usage) {
        self.slot_bytes = self.slot_bytes.saturating_sub(usage.memory);
        if let Some(entry) = self.roots.get_mut(&root) {
            entry.events = entry.events.saturating_sub(1);
            entry.bytes = entry.bytes.saturating_sub(usage.share);
            entry.disk = entry.disk.saturating_sub(usage.disk);
            if entry.is_empty() {
                if entry.ended > 0 {
                    if self.drained.len() >= DRAINED {
                        self.drained.clear();
                    }
                    self.drained.insert(root);
                }
                self.roots.remove(&root);
            }
        }
    }

    /// Managed memory in use: event metadata and payloads in memory (the
    /// cache included), and the producer table.
    pub(super) fn memory_used(&self) -> u64 {
        self.slot_bytes + self.producers.entries.len() as u64 * self.config.producers.entry_bytes
    }

    pub(super) fn memory_budget(&self) -> u64 {
        let reserve = (self.memory_limit as f64 * self.config.memory.reserve) as u64;
        self.memory_limit.saturating_sub(reserve)
    }

    /// The memory high watermark in bytes, with no root paused.
    pub(super) fn high_watermark(&self) -> u64 {
        (self.memory_budget() as f64 * self.config.memory.high_watermark) as u64
    }

    /// A production connection registered `len` bytes to `root`. A paused root
    /// tells it `Paused` once; past the allowance its reading stops (S14).
    pub(super) fn pause_notice(&mut self, session: SessionId, root: u16, len: u64) {
        let allowance = self.config.pause_allowance;
        let Some(paused) = self.roots.get_mut(&root).and_then(|root| root.paused.as_mut()) else { return };
        match paused.notified.get_mut(&session) {
            None => {
                paused.notified.insert(session, Notified { bytes: 0, allowance: allowance.saturating_add(len), stopped: false });
                self.send(session, Outbound::Paused { event_type: root }, None);
            }
            Some(notified) => {
                notified.bytes = notified.bytes.saturating_add(len);
                if !notified.stopped && notified.bytes > notified.allowance {
                    notified.stopped = true;
                    self.effects.reading.push((session, false));
                }
            }
        }
    }

    /// Pauses `root` for the resource it is high on.
    pub(super) fn pause(&mut self, root: u16, by_disk: bool) {
        let Some(entry) = self.roots.get_mut(&root) else { return };
        if entry.paused.is_some() {
            return;
        }
        entry.paused = Some(Paused { at_bytes: entry.bytes, at_disk: entry.disk, by_disk, ..Paused::default() });
        self.paused.insert(root);
        self.counters.pauses += 1;
    }

    pub(super) fn resume(&mut self, root: u16) {
        self.release_holds(root);
        self.paused.remove(&root);
        let Some(entry) = self.roots.get_mut(&root) else { return };
        let Some(paused) = entry.paused.take() else { return };
        if entry.is_empty() {
            self.roots.remove(&root);
        }
        for (session, notified) in paused.notified {
            self.send(session, Outbound::Resumed { event_type: root }, None);
            if notified.stopped {
                self.effects.reading.push((session, true));
            }
        }
    }

    /// Pauses the biggest unpaused roots of one resource while what is left of
    /// it is at the high watermark of what is left of its budget.
    fn pause_biggest(&mut self, used: u64, budget: u64, share: fn(&Root) -> u64, by_disk: bool) {
        loop {
            let paused: u64 = self.paused.iter().filter_map(|root| self.roots.get(root)).map(share).sum();
            let left_budget = budget.saturating_sub(paused) as f64;
            let left_used = used.saturating_sub(paused) as f64;
            if left_used < self.config.memory.high_watermark * left_budget {
                return;
            }
            let biggest = self.roots.iter().filter(|(_, root)| root.paused.is_none() && share(root) > 0).max_by_key(|(_, root)| share(root)).map(|(kind, _)| *kind);
            let Some(biggest) = biggest else { return };
            self.pause(biggest, by_disk);
        }
    }

    /// Run at the end of every batch: the cache, the limits, the early pause
    /// and the backlog bounds of roots that grew, the high watermarks, and the
    /// resume and hold-release of paused roots.
    pub(super) fn check_memory(&mut self) {
        let high = self.high_watermark();
        if self.memory_used() >= high {
            self.release_cache((high as f64 * self.config.memory.release) as u64);
        }
        let used = self.memory_used();
        let disk_used = self.disk_used();
        let disk_budget = self.disk_budget();
        let disk_limit = self.config.storage.disk_capacity;
        self.over_limit = used >= self.memory_limit || disk_limit.is_some_and(|limit| disk_used >= limit);
        if self.over_limit {
            self.counters.over_limit += 1;
        }
        let budget = self.memory_budget();
        let early = (budget as f64 * self.config.memory.early_pause) as u64;
        let early_disk = disk_budget.map(|budget| (budget as f64 * self.config.memory.early_pause) as u64);
        for root in std::mem::take(&mut self.grown) {
            let Some(entry) = self.roots.get(&root) else { continue };
            if entry.paused.is_some() {
                continue;
            }
            let bounded = self.config.types.settings(root).max_backlog.is_some_and(|bound| entry.events > bound.count || entry.bytes > bound.bytes);
            // A root none of whose lineages ever ended is paused early (S19).
            let never_drained = entry.ended == 0;
            let early_memory = never_drained && entry.bytes >= early.max(1);
            let early_on_disk = never_drained && early_disk.is_some_and(|early| entry.disk >= early.max(1));
            if bounded || early_memory || early_on_disk {
                self.pause(root, early_on_disk && !early_memory);
            }
        }
        self.pause_biggest(used, budget, |root| root.bytes, false);
        if let Some(disk_budget) = disk_budget {
            self.pause_biggest(disk_used, disk_budget, |root| root.disk, true);
        }
        let release = self.config.memory.release;
        for root in self.paused.clone() {
            let (share, at, holds) = match self.roots.get(&root) {
                Some(Root { bytes, disk, paused: Some(paused), .. }) => {
                    if paused.by_disk {
                        (*disk, paused.at_disk, !paused.holds.is_empty())
                    } else {
                        (*bytes, paused.at_bytes, !paused.holds.is_empty())
                    }
                }
                _ => {
                    self.paused.remove(&root);
                    continue;
                }
            };
            if (share as f64) < release * at as f64 {
                self.resume(root);
            } else if holds && share <= at {
                self.release_holds(root);
            }
        }
    }
}
