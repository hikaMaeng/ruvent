//! The timing wheel: lease ends and visibility times wake the engine (2.4).
//!
//! Entries are never removed early. Each names a slot and its generation, and
//! is checked against the slot when it comes due, so an extension, a
//! completion or a deletion costs nothing here.

use super::slots::Index;
use crate::envelope::ClaimToken;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Due {
    /// The lease of this attempt runs out.
    Lease(ClaimToken),
    /// A delayed event becomes visible.
    Visible,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Entry {
    pub at: u64,
    pub index: Index,
    pub generation: u32,
    pub due: Due,
}

/// The wheel.
///
/// | key | (time, slot, slot generation, what is due) |
/// | --- | --- |
/// | scope | one engine |
/// | lifetime | assignment or delay, until it comes due (checked then) |
/// | bound | claimed + delayed, plus entries made stale by extensions |
/// | cleared by | the ledger, as entries come due (`State::process_due`) |
#[derive(Default)]
pub(super) struct Wheel {
    waking: BinaryHeap<Reverse<Entry>>,
}

impl Wheel {
    pub fn wake_at(&mut self, at: u64, index: Index, generation: u32, due: Due) {
        self.waking.push(Reverse(Entry { at, index, generation, due }));
    }

    /// The earliest time the engine must wake, if any.
    pub fn next(&self) -> Option<u64> {
        self.waking.peek().map(|Reverse(entry)| entry.at)
    }

    pub fn pop_due(&mut self, now: u64) -> Option<Entry> {
        if self.waking.peek().is_some_and(|Reverse(entry)| entry.at <= now) {
            self.waking.pop().map(|Reverse(entry)| entry)
        } else {
            None
        }
    }
}
