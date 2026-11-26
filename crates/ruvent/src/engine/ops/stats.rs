//! The answer to `Stats` (2.8, B24): values copied out of the ledger when asked.
//! Nothing is reported periodically.

use crate::envelope::Channel;

/// Counters since the engine started. They only grow.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Counters {
    /// Sequenced operations at or below the producer's last applied number, dropped.
    pub retransmits: u64,
    /// Registrations dropped because an active event had the id and the same digest.
    pub duplicates: u64,
    /// Completions, extensions and releases whose token was not the current attempt's.
    pub stale: u64,
    /// Leases that ran out.
    pub reclaimed: u64,
    /// Faults of any kind, told or not.
    pub faults: u64,
    /// Cached durable payloads released at the high watermark (2.5).
    pub released: u64,
    /// Cold payloads read ahead into the cache.
    pub read_ahead: u64,
    /// Payloads moved to the long-lived lane.
    pub relocated: u64,
    /// Segments removed.
    pub segments_removed: u64,
    /// Checkpoints made durable.
    pub checkpoints: u64,
}

/// Memory and disk as the connection owners see them: what decides whether a
/// follow-up's body is received to disk (2.6). Copied after every ledger batch.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Pressure {
    /// Managed memory in use, the budget (limit - reserve) and the limit.
    pub memory_used: u64,
    pub memory_budget: u64,
    pub memory_limit: u64,
    /// The memory high watermark in bytes (of the whole budget).
    pub memory_high: u64,
    /// Disk in use (segments and the checkpoint), and the budget and the
    /// capacity when a capacity is configured.
    pub disk_used: u64,
    pub disk_budget: Option<u64>,
    pub disk_limit: Option<u64>,
}

/// One type in the top-K by waiting events.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypeStat {
    pub event_type: u16,
    /// Waiting events (in a lane or held back by a pause).
    pub waiting: u64,
    /// Age in ms of its oldest waiting event (by registration time).
    pub oldest_ms: u64,
}

/// A (channel, type) with waiting events and no join that selects it: nobody will take them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdleLane {
    pub channel: Channel,
    pub event_type: u16,
    pub waiting: u64,
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
    /// Disk in use (segments and the checkpoint), the budget and the capacity
    /// (S6); budget and capacity are 0 when no capacity is configured.
    pub disk_used: u64,
    pub disk_budget: u64,
    pub disk_limit: u64,
    /// Segments on disk.
    pub segments: u64,
    pub draining: bool,
    pub counters: Counters,
    pub types: Vec<TypeStat>,
    pub idle_lanes: Vec<IdleLane>,
}
