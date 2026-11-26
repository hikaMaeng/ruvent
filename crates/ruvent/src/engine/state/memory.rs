//! Managed memory and its high watermark (2.5, 2.6).
//!
//! Memory has a budget (the limit minus the reserve) and one high watermark.
//! What is done at the watermark is the one thing that does not block anyone:
//! cached durable payloads are released (paging, 2.5) — their only copy is on
//! disk, and they are read back when they go out. Nobody is paused and
//! nothing is refused: a producer is never held up by the server (plan 9.15).
//! Event metadata stays in memory; how much may wait is the deployment's to
//! size (`depth_target`, reported in `Stats`).

use super::slots::Slot;
use super::State;

/// What one event counts for in managed memory: its metadata and the payload
/// while it is in memory.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Usage {
    pub memory: u64,
}

impl State {
    pub(super) fn usage(&self, slot: &Slot) -> Usage {
        Usage { memory: self.config.memory.slot_bytes + slot.payload.len() as u64 }
    }

    pub(super) fn account_add(&mut self, usage: Usage) {
        self.slot_bytes += usage.memory;
    }

    pub(super) fn account_remove(&mut self, usage: Usage) {
        self.slot_bytes = self.slot_bytes.saturating_sub(usage.memory);
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

    /// The memory high watermark in bytes.
    pub(super) fn high_watermark(&self) -> u64 {
        (self.memory_budget() as f64 * self.config.memory.high_watermark) as u64
    }

    /// Run at the end of every batch: at the high watermark, release cached
    /// payloads down to `release` of it.
    pub(super) fn check_memory(&mut self) {
        let high = self.high_watermark();
        if self.memory_used() >= high {
            self.release_cache((high as f64 * self.config.memory.release) as u64);
        }
    }
}
