//! Managed memory, the high watermark and root-channel pause (2.6).
//!
//! The budget is the limit minus the reserve. The watermark is measured on
//! what is left once paused roots' shares are taken out, so a paused root
//! cannot push others over it. A paused root cannot grow its share: its
//! producers are told `Paused`, and its events are not assigned from
//! net-growth lanes while its share is above its share at the pause.

use super::roots::{Notified, Paused, Root};
use super::State;
use crate::engine::ops::{Outbound, SessionId};
use crate::names::Name;

/// The most root channels remembered as "drained before" once they hold
/// nothing (the `drained` set). Past it the set is cleared, which can only
/// make a root look new — an early pause it would have been spared.
pub(super) const DRAINED: usize = 4096;

impl State {
    /// Managed bytes for one event with a payload of `len` bytes.
    pub(super) fn cost(&self, len: u64) -> u64 {
        self.config.memory.slot_bytes + len
    }

    pub(super) fn account_add(&mut self, root: &Name, cost: u64) {
        self.slot_bytes += cost;
        if !self.roots.contains_key(root) {
            // A root that drained before it emptied is not one that never drains.
            let ended = u64::from(self.drained.remove(root));
            self.roots.insert(root.clone(), Root { ended, ..Root::default() });
        }
        let entry = self.roots.entry(root.clone()).or_default();
        entry.events += 1;
        entry.bytes += cost;
        if self.grown.last() != Some(root) {
            self.grown.push(root.clone());
        }
    }

    pub(super) fn account_remove(&mut self, root: &Name, cost: u64) {
        self.slot_bytes = self.slot_bytes.saturating_sub(cost);
        if let Some(entry) = self.roots.get_mut(root) {
            entry.events = entry.events.saturating_sub(1);
            entry.bytes = entry.bytes.saturating_sub(cost);
            if entry.is_empty() {
                if entry.ended > 0 {
                    if self.drained.len() >= DRAINED {
                        self.drained.clear();
                    }
                    self.drained.insert(root.clone());
                }
                self.roots.remove(root);
            }
        }
    }

    /// Managed memory in use: event metadata and payloads, and the producer table.
    pub(super) fn memory_used(&self) -> u64 {
        self.slot_bytes + self.producers.entries.len() as u64 * self.config.producers.entry_bytes
    }

    pub(super) fn memory_budget(&self) -> u64 {
        let reserve = (self.memory_limit as f64 * self.config.memory.reserve) as u64;
        self.memory_limit.saturating_sub(reserve)
    }

    /// A production connection registered `len` bytes to `root`. A paused root
    /// tells it `Paused` once; past the allowance its reading stops (S14).
    pub(super) fn pause_notice(&mut self, session: SessionId, root: &Name, len: u64) {
        let allowance = self.config.pause_allowance;
        let Some(paused) = self.roots.get_mut(root).and_then(|root| root.paused.as_mut()) else { return };
        match paused.notified.get_mut(&session) {
            None => {
                paused.notified.insert(session, Notified { bytes: 0, allowance: allowance.saturating_add(len), stopped: false });
                self.send(session, Outbound::Paused { channel: root.clone() }, None);
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

    pub(super) fn pause(&mut self, root: &Name) {
        let Some(entry) = self.roots.get_mut(root) else { return };
        if entry.paused.is_some() {
            return;
        }
        entry.paused = Some(Paused { at_bytes: entry.bytes, ..Paused::default() });
        self.paused.insert(root.clone());
        self.counters.pauses += 1;
    }

    pub(super) fn resume(&mut self, root: &Name) {
        self.release_holds(root);
        self.paused.remove(root);
        let Some(entry) = self.roots.get_mut(root) else { return };
        let Some(paused) = entry.paused.take() else { return };
        if entry.is_empty() {
            self.roots.remove(root);
        }
        for (session, notified) in paused.notified {
            self.send(session, Outbound::Resumed { channel: root.clone() }, None);
            if notified.stopped {
                self.effects.reading.push((session, true));
            }
        }
    }

    /// Run at the end of every batch: the limit, the early pause and the
    /// backlog bounds of roots that grew, the high watermark, and the resume
    /// and hold-release of paused roots.
    pub(super) fn check_memory(&mut self) {
        let used = self.memory_used();
        self.over_limit = used >= self.memory_limit;
        if self.over_limit {
            self.counters.over_limit += 1;
        }
        let budget = self.memory_budget();
        let early = (budget as f64 * self.config.memory.early_pause) as u64;
        for root in std::mem::take(&mut self.grown) {
            let Some(entry) = self.roots.get(&root) else { continue };
            if entry.paused.is_some() {
                continue;
            }
            let bounded = self.config.channels.settings(&root).max_backlog.is_some_and(|bound| entry.events > bound.count || entry.bytes > bound.bytes);
            // A root none of whose lineages ever ended is paused early (S19).
            if bounded || (entry.ended == 0 && entry.bytes >= early.max(1)) {
                self.pause(&root);
            }
        }
        loop {
            let paused_bytes: u64 = self.paused.iter().filter_map(|root| self.roots.get(root)).map(|root| root.bytes).sum();
            let left_budget = budget.saturating_sub(paused_bytes) as f64;
            let left_used = used.saturating_sub(paused_bytes) as f64;
            if left_used < self.config.memory.high_watermark * left_budget {
                break;
            }
            let biggest = self.roots.iter().filter(|(_, root)| root.paused.is_none() && root.bytes > 0).max_by_key(|(_, root)| root.bytes).map(|(name, _)| name.clone());
            let Some(biggest) = biggest else { break };
            self.pause(&biggest);
        }
        let release = self.config.memory.release;
        for root in self.paused.clone() {
            let (bytes, at, holds) = match self.roots.get(&root) {
                Some(Root { bytes, paused: Some(paused), .. }) => (*bytes, paused.at_bytes, !paused.holds.is_empty()),
                _ => {
                    self.paused.remove(&root);
                    continue;
                }
            };
            if (bytes as f64) < release * at as f64 {
                self.resume(&root);
            } else if holds && bytes <= at {
                self.release_holds(&root);
            }
        }
    }
}
