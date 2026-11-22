//! What the engine keeps, for how long, and what of it reaches the disk.

use crate::envelope::{Envelope, Kind, Text};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

/// action → the queues whose reactors should receive their own copy of that fact.
///
/// Opaque strings on both sides. The engine never learns what any of them mean.
pub type ReactionTable = HashMap<String, Vec<String>>;

/// When the journal is written relative to answering the caller.
#[derive(Clone, Copy, Debug)]
pub enum Durability {
    /// Answer at once; the journal is written behind, at most once per
    /// `window`. The window is not a timer that fires regardless: it starts
    /// when the first unwritten change appears and is the time other changes
    /// have to cancel it out or join it. An idle engine sets no timer at all.
    ///
    /// A crash loses at most what changed since the last completed write.
    Scheduled { window: Duration },
    /// Write and fsync before answering anyone. Nothing acknowledged is ever
    /// lost; throughput is bounded by the disk.
    Strict,
}

impl Default for Durability {
    fn default() -> Self {
        Durability::Scheduled { window: Duration::from_millis(100) }
    }
}

/// Which events are written to the journal. Progress events never are.
#[derive(Clone, Default)]
pub enum Events {
    /// None. Events live in memory for readers and are gone after a restart.
    /// The machine does not need them: the work a fact causes is queued.
    #[default]
    None,
    /// Facts, results, failures and commands on every channel.
    All,
    /// The same, only on channels with one of these prefixes.
    Channels(Vec<String>),
    /// The same, on every channel except those with one of these prefixes.
    Except(Vec<String>),
    /// Whatever this says. Progress events are still never written.
    Custom(Arc<dyn Fn(&Envelope) -> bool + Send + Sync>),
}

impl Events {
    pub(crate) fn wants(&self, event: &Envelope) -> bool {
        if matches!(event.kind, Kind::Progress | Kind::Control) {
            return false;
        }
        let under = |prefixes: &[String]| prefixes.iter().any(|prefix| event.channel.starts_with(prefix.as_str()));
        match self {
            Events::None => false,
            Events::All => true,
            Events::Channels(prefixes) => under(prefixes),
            Events::Except(prefixes) => !under(prefixes),
            Events::Custom(wants) => wants(event),
        }
    }
}

/// What reaches the disk. The default is the least that lets a restart carry
/// on: the work still owed, and how many times each piece has been tried.
#[derive(Clone, Default)]
pub struct Persist {
    pub events: Events,
    /// Also keep settled executions for the deduplication window, so a
    /// transaction resubmitted across a restart is not run again. Off by
    /// default: it turns every completed execution into a disk write.
    pub settled: bool,
    /// Queues whose work is never journaled. A crash drops whatever was on
    /// them; use for work a client will simply ask for again.
    pub transient_queues: HashSet<Text>,
}

/// Bounds on what is kept in memory. Each is enforced at the moment the thing
/// it bounds grows; nothing is swept on a timer.
#[derive(Clone, Debug)]
pub struct Retention {
    /// Events held for readers to resume from.
    pub max_events: usize,
    /// Approximate bytes those events may occupy.
    pub max_bytes: usize,
    /// An event older than this is dropped when the next one is appended.
    pub max_age: Duration,
    /// How long a settled execution is remembered, so the same transaction
    /// sent again is answered instead of rerun.
    pub settled: Duration,
    pub max_settled: usize,
    /// Dead letters kept per queue.
    pub dead_letters: usize,
}

impl Default for Retention {
    fn default() -> Self {
        Self { max_events: 262_144, max_bytes: 256 << 20, max_age: Duration::from_secs(15 * 60), settled: Duration::from_secs(10 * 60), max_settled: 1_000_000, dead_letters: 1024 }
    }
}

/// When the journal is folded into a snapshot: once it holds more than
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

#[derive(Clone)]
pub struct EngineConfig {
    pub reactions: ReactionTable,
    /// How many times an execution may be started before it is failed for good.
    pub max_attempts: u32,
    /// Most operations applied between two looks at the journal and the timers.
    pub batch_max: usize,
    /// Capacity of the operation inbox. Callers wait when it is full; progress
    /// emissions are dropped instead.
    pub inbox: usize,
    pub durability: Durability,
    pub persist: Persist,
    pub retention: Retention,
    pub compaction: Compaction,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            reactions: ReactionTable::new(),
            max_attempts: 5,
            batch_max: 2048,
            inbox: 65_536,
            durability: Durability::default(),
            persist: Persist::default(),
            retention: Retention::default(),
            compaction: Compaction::default(),
        }
    }
}
