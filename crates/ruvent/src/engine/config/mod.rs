//! What the engine is told at start.
//!
//! Nothing here is policy about events — retry counts, back-off and follow-ups
//! belong to the workers — with one exception: the isolation threshold
//! (`isolate_after`, B6), the single place a failure count is compared.
//!
//! Every value of the plan's chapter 8 is a field here with its default, not
//! a constant (v4.6). Channel-level values are [`ChannelSettings`], which a
//! channel pattern can override ([`ChannelConfig`]).

mod channels;
mod limits;

pub use crate::names::Pattern;
pub use crate::snapshot::SnapshotConfig;
pub use channels::{ChannelConfig, ChannelRule, ChannelSettings, MaxBacklog};
pub use limits::{MemoryConfig, ProducerConfig, StorageConfig};
use std::collections::HashMap;
use std::time::Duration;

/// When other connections may see a change relative to the disk.
///
/// Both modes write the same way: one batch at a time, each carrying
/// everything that changed while the previous one was out, and `Committed`
/// goes out after the fsync in both (B2). They differ only in visibility.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Durability {
    /// Visible at once. A crash loses what changed since the last completed
    /// write; producers still hold it, unacknowledged, and send it again.
    #[default]
    Scheduled,
    /// An event on a durable channel is visible to no other connection —
    /// assignment, notice, backlog, explicit claim — before its registration
    /// is on disk (2.7). A completion's dispatch follow-ups and a deletion's
    /// result wait for the completion or deletion to be on disk.
    Strict,
}

/// What a credential may be granted.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Method {
    /// `Register` of a claimable event.
    RegisterClaimable,
    /// `Register` of a dispatch event, from a production or a work connection.
    RegisterDispatch,
    /// `Join` and explicit `Claim`.
    Claim,
    /// `Complete`, `Extend`, `Release`.
    Complete,
    Delete,
    Subscribe,
    Unsubscribe,
    /// `Stats` and `Drain` (2.11).
    Operate,
}

impl Method {
    pub const ALL: [Method; 8] = [Method::RegisterClaimable, Method::RegisterDispatch, Method::Claim, Method::Complete, Method::Delete, Method::Subscribe, Method::Unsubscribe, Method::Operate];
}

/// What one service credential may do.
#[derive(Clone, Debug)]
pub struct Grant {
    pub methods: Vec<Method>,
    pub channels: Vec<Pattern>,
    /// Whether this credential may register on behalf of an end user (`delegate`).
    pub delegate: bool,
}

impl Grant {
    /// Every method on every channel, delegation included.
    pub fn all() -> Self {
        Grant { methods: Method::ALL.to_vec(), channels: vec![Pattern("*".into())], delegate: true }
    }

    pub fn allows(&self, method: Method, channel: &str) -> bool {
        self.methods.contains(&method) && self.channels.iter().any(|pattern| pattern.matches(channel))
    }
}

/// Grants by credential name. `None` in [`EngineConfig::grants`] means every
/// credential may do everything (an embedded engine with in-process workers).
pub type Grants = HashMap<String, Grant>;

/// When the single-file journal is folded into a snapshot: once the file
/// holds more than `min_records` and more than `ratio` times what a snapshot
/// would. The segment container replaces this with the checkpoint triggers
/// of [`StorageConfig`].
#[derive(Clone, Copy, Debug)]
pub struct Compaction {
    pub min_records: u64,
    pub ratio: u64,
}

impl Default for Compaction {
    fn default() -> Self {
        Self { min_records: 50_000, ratio: 4 }
    }
}

#[derive(Clone, Debug)]
pub struct EngineConfig {
    /// Most operations applied between two looks at the journal.
    pub batch_max: usize,
    /// Capacity of the operation inbox. Callers wait when it is full.
    pub inbox: usize,
    /// Capacity of each connection's send buffer. A connection whose buffer is
    /// full when the engine hands it something is disconnected; the
    /// connection owner bounds it in bytes before that (S7).
    pub outbound: usize,
    /// Most backlog entries in one subscription answer.
    pub backlog_page: usize,
    pub durability: Durability,
    pub compaction: Compaction,
    pub grants: Option<Grants>,
    pub snapshot: Option<SnapshotConfig>,
    /// This server's id in `Welcome`. `None`: a fresh one at every start.
    pub server_id: Option<[u8; 16]>,
    /// S2: a producer is told `Committed` once this many of its operations
    /// are confirmable even while later ones still wait for the disk. No timer.
    pub ack_every: u64,
    /// B4, S15: the producer sequence table.
    pub producers: ProducerConfig,
    /// S20: a closed connection's fully written assignments are due at most
    /// this long after the close.
    pub reconnect_grace: Duration,
    /// S10: most follow-ups one completion may register.
    pub max_follow_ups: usize,
    /// S5, S19, S22: managed memory, watermarks and channel pause.
    pub memory: MemoryConfig,
    /// S14: what a production connection may still send to a paused root
    /// channel after `Paused` before its reading stops (the record being read
    /// at the notice is allowed on top).
    pub pause_allowance: u64,
    /// B24: entries in each top-K list of `Stats`.
    pub stats_top: usize,
    /// B6, B9, B17, B26 and volatile channels, by channel pattern.
    pub channels: ChannelConfig,
    /// S3, S4, S6, S17: read by the segment container.
    pub storage: StorageConfig,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            batch_max: 2048,
            inbox: 65_536,
            outbound: 65_536,
            backlog_page: 1024,
            durability: Durability::default(),
            compaction: Compaction::default(),
            grants: None,
            snapshot: None,
            server_id: None,
            ack_every: 256,
            producers: ProducerConfig::default(),
            reconnect_grace: Duration::from_secs(3),
            max_follow_ups: 1024,
            memory: MemoryConfig::default(),
            pause_allowance: 1 << 20,
            stats_top: 16,
            channels: ChannelConfig::default(),
            storage: StorageConfig::default(),
        }
    }
}
