//! Lanes and joins.

use super::slots::{Index, JoinId};
use crate::envelope::{Channel, WorkKey};
use std::collections::{BTreeMap, HashSet};

/// A lane's key: (channel, type). Waiting order is per lane, because workers
/// join by type and one lane per channel would let an unsupported type at the
/// head block the rest (2.1).
pub(super) type LaneKey = (Channel, u16);

/// One lane.
///
/// | key | (channel, type) |
/// | --- | --- |
/// | scope | one engine |
/// | lifetime | while it has waiting events or members |
/// | bound | the waiting events of the lane, plus its members |
/// | cleared by | the ledger, when the queue and the members are both empty (`State::tidy`) |
#[derive(Debug, Default)]
pub(super) struct Lane {
    /// Waiting events by work key: the waiting order.
    pub queue: BTreeMap<WorkKey, Index>,
    /// The channel joins that take from this lane, in join order. Joins
    /// without a channel are not stored here (`State::candidates` adds them),
    /// so a lane only they take goes when its queue is empty and a channel per
    /// request leaves no lane behind.
    pub members: Vec<JoinId>,
    /// Where the next round-robin pick starts.
    pub cursor: usize,
}

impl Lane {
    pub fn is_idle(&self) -> bool {
        self.queue.is_empty() && self.members.is_empty()
    }

    pub fn drop_member(&mut self, join: &JoinId) {
        if let Some(at) = self.members.iter().position(|member| member == join) {
            self.members.remove(at);
            if self.cursor > at {
                self.cursor -= 1;
            }
            if self.cursor >= self.members.len() {
                self.cursor = 0;
            }
        }
    }
}

/// One join: a work connection taking a selection of lanes.
///
/// | key | (work connection, the `req` of its `Join`) |
/// | --- | --- |
/// | scope | one engine |
/// | lifetime | `Join` to the connection's close |
/// | bound | the window the worker opened (`count`, `bytes`) |
/// | cleared by | the ledger, at the connection's close |
#[derive(Debug)]
pub(super) struct Join {
    /// `None`: every channel.
    pub channel: Option<Channel>,
    /// Empty: every type.
    pub types: Vec<u16>,
    pub lease_ms: u64,
    /// The window: assignments and payload bytes (`0`: no byte bound) held at once.
    pub count: u32,
    pub bytes: u64,
    /// What it holds now.
    pub claimed: u32,
    pub claimed_bytes: u64,
    /// `Pending` (or `more`) was sent since the last `Window`.
    pub signalled: bool,
    /// Join order, for round robin.
    pub order: u64,
    /// The channel's dispatch events come to this connection.
    pub dispatch: bool,
    /// The lanes it is a member of: a channel join's only. A join without a
    /// channel finds its lanes by type (`State::typed_lanes`).
    pub lanes: Vec<LaneKey>,
    /// The attempts that count against it.
    pub attempts: HashSet<Index>,
}

impl Join {
    /// Whether it selects the lane.
    pub fn selects(&self, lane: &LaneKey) -> bool {
        self.channel.is_none_or(|channel| channel == lane.0) && (self.types.is_empty() || self.types.contains(&lane.1))
    }

    /// Whether it is this selection.
    pub fn is(&self, channel: &Option<Channel>, types: &[u16]) -> bool {
        self.channel == *channel && self.types == types
    }

    /// Whether some lane is selected by both it and this selection.
    pub fn overlaps(&self, channel: &Option<Channel>, types: &[u16]) -> bool {
        let channels = match (&self.channel, channel) {
            (Some(own), Some(other)) => own == other,
            _ => true,
        };
        channels && (self.types.is_empty() || types.is_empty() || types.iter().any(|kind| self.types.contains(kind)))
    }

    pub fn has_room(&self) -> bool {
        self.claimed < self.count && (self.bytes == 0 || self.claimed_bytes < self.bytes)
    }

    /// Whether an event of `len` payload bytes fits. A join holding nothing
    /// takes one event over its byte bound, or a big event would block the
    /// lane for good (2.4, like `fetch.max.bytes`).
    pub fn fits(&self, len: u64) -> bool {
        self.claimed < self.count && (self.bytes == 0 || self.claimed == 0 || self.claimed_bytes + len <= self.bytes)
    }
}
