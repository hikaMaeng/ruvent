//! Lanes, joins and the net-growth counters.

use super::slots::{Index, JoinId};
use crate::envelope::{Channel, WorkKey};
use std::collections::{BTreeMap, HashSet};

/// A lane's key: (channel, type, isolated). Waiting order is per lane,
/// because workers join by type and one lane per channel would let an
/// unsupported type at the head block the rest (2.1). Isolated events wait in
/// a lane of their own, taken only by joins that ask for them (B6).
pub(super) type LaneKey = (Channel, u16, bool);

/// Recent completions of a lane: parents in, follow-ups out (2.6, S19).
///
/// The window is approximate: once `completions` reaches the configured
/// window, every counter is halved, so old completions fade without a ring
/// buffer and the state stays four counters and a count per lane.
#[derive(Clone, Debug, Default)]
pub(super) struct Growth {
    pub completions: u32,
    pub parents: u64,
    pub children: u64,
    pub parent_bytes: u64,
    pub child_bytes: u64,
}

impl Growth {
    pub fn record(&mut self, parent_bytes: u64, children: u64, child_bytes: u64, window: u32) {
        if self.completions >= window.max(1) {
            self.completions /= 2;
            self.parents /= 2;
            self.children /= 2;
            self.parent_bytes /= 2;
            self.child_bytes /= 2;
        }
        self.completions += 1;
        self.parents += 1;
        self.children += children;
        self.parent_bytes += parent_bytes;
        self.child_bytes += child_bytes;
    }

    /// More or bigger follow-ups come out than parents go in.
    pub fn net(&self) -> bool {
        self.children > self.parents || self.child_bytes > self.parent_bytes
    }
}

/// One lane.
///
/// | key | (channel, type, isolated) |
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
    /// B26: its type serializes groups.
    pub serialize: bool,
    pub growth: Growth,
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
    /// Empty: every type. The credential's grant was checked at `Join`.
    pub types: Vec<u16>,
    /// It takes isolated events only; otherwise only the others.
    pub isolated: bool,
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
    /// The channel's dispatch events (of the credential's types) come to this connection.
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
        self.isolated == lane.2 && self.channel.is_none_or(|channel| channel == lane.0) && (self.types.is_empty() || self.types.contains(&lane.1))
    }

    /// Whether it is this selection.
    pub fn is(&self, channel: &Option<Channel>, types: &[u16], isolated: bool) -> bool {
        self.channel == *channel && self.types == types && self.isolated == isolated
    }

    /// Whether some lane is selected by both it and this selection.
    pub fn overlaps(&self, channel: &Option<Channel>, types: &[u16], isolated: bool) -> bool {
        let channels = match (&self.channel, channel) {
            (Some(own), Some(other)) => own == other,
            _ => true,
        };
        self.isolated == isolated && channels && (self.types.is_empty() || types.is_empty() || types.iter().any(|kind| self.types.contains(kind)))
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
