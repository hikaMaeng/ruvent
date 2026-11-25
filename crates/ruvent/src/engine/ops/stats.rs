//! The answer to `Stats` (2.8, B24): values copied out of the ledger when asked.
//! Nothing is reported periodically.

use crate::names::Name;

/// Counters since the engine started. They only grow.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Counters {
    /// Sequenced operations at or below the producer's last applied number, dropped.
    pub retransmits: u64,
    /// Registrations dropped because an active event had the id and the same digest.
    pub duplicates: u64,
    /// Completions, extensions and releases whose token was not the current attempt's.
    pub stale: u64,
    /// Events removed because they expired.
    pub expired: u64,
    /// Events moved to an isolation channel.
    pub isolated: u64,
    /// Leases that ran out.
    pub reclaimed: u64,
    /// Faults of any kind, told or not.
    pub faults: u64,
    /// Batches that ended with managed memory at or past the limit (net-growth lanes were not assigned).
    pub over_limit: u64,
    /// Root channels paused, by any rule.
    pub pauses: u64,
}

/// One channel in the top-K by waiting events.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelStat {
    pub channel: Name,
    /// Waiting events (in a lane or held back by a pause).
    pub waiting: u64,
    /// Age in ms of its oldest waiting event (by registration time).
    pub oldest_ms: u64,
}

/// A (channel, type) with waiting events and no join: nobody will take them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdleLane {
    pub channel: Name,
    pub event_type: Name,
    pub waiting: u64,
}

/// One root channel in the top-K by share.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RootStat {
    pub root: Name,
    /// Events held for the root, in any state.
    pub events: u64,
    /// Managed bytes held for the root.
    pub bytes: u64,
    pub paused: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    /// Events waiting in a lane or held back by a pause.
    pub waiting: u64,
    /// Events delayed until a visibility time.
    pub delayed: u64,
    /// Events claimed.
    pub claimed: u64,
    /// Age in ms of the oldest waiting event.
    pub oldest_ms: u64,
    /// Managed memory in use, the budget (limit − reserve) and the limit, in bytes.
    pub memory_used: u64,
    pub memory_budget: u64,
    pub memory_limit: u64,
    /// Events the budget holds at the configured metadata size, and the target (S22).
    pub depth_capacity: u64,
    pub depth_target: u64,
    pub draining: bool,
    pub counters: Counters,
    pub channels: Vec<ChannelStat>,
    pub idle_lanes: Vec<IdleLane>,
    pub roots: Vec<RootStat>,
}
