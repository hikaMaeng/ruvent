//! What the engine is told at start. Nothing here is policy about events:
//! retry limits, failure handling and follow-ups belong to the workers.

pub use crate::names::Pattern;
pub use crate::snapshot::SnapshotConfig;
use std::collections::HashMap;

/// When the journal is written relative to answering.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Durability {
    /// Answer at once and write behind, one write at a time, each carrying
    /// everything that changed while the previous one was out. Nothing waits
    /// on a timer. A crash loses what changed since the last completed write.
    #[default]
    Scheduled,
    /// The notices and success answers of a claimable registration, a
    /// completion with its follow-ups, and a deletion are released only
    /// after that change is on disk. Claims and reclaims never wait.
    Strict,
}

/// The seven methods, for grants.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Method {
    RegisterClaimable,
    RegisterDispatch,
    Claim,
    Complete,
    Delete,
    Subscribe,
    Unsubscribe,
}

impl Method {
    pub const ALL: [Method; 7] = [Method::RegisterClaimable, Method::RegisterDispatch, Method::Claim, Method::Complete, Method::Delete, Method::Subscribe, Method::Unsubscribe];
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

/// When the journal is folded into a snapshot: once the file holds more than
/// `min_records` and more than `ratio` times what a snapshot would.
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
    /// full when the engine hands it something is disconnected.
    pub outbound: usize,
    /// Most backlog entries in one subscription answer.
    pub backlog_page: usize,
    pub durability: Durability,
    pub compaction: Compaction,
    pub grants: Option<Grants>,
    pub snapshot: Option<SnapshotConfig>,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self { batch_max: 2048, inbox: 65_536, outbound: 65_536, backlog_page: 1024, durability: Durability::default(), compaction: Compaction::default(), grants: None, snapshot: None }
    }
}
