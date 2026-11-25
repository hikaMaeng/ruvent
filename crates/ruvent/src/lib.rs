//! ruvent — the event server core.
//!
//! One server is one ledger (0.3). Connections come in three roles (2.2):
//! production (register and delete, numbered by a producer sequence and
//! acknowledged cumulatively once committed), work (join a channel, open a
//! window, receive assignments, complete, extend, release, claim by id) and
//! observation (subscribe, statistics, drain). Claimable events wait in
//! per-(channel, type) lanes until a completion removes them; dispatch events
//! go to the subscribers of their channel and are gone in the same step. Both
//! use one envelope.
//!
//! Retry counts, back-off and follow-ups are the workers' logic; the server
//! keeps four things: lease ends, visibility times, expiries and one failure
//! threshold (`isolate_after`).
//!
//! Journaling is not an operation. The engine journals the claimable events
//! of durable channels and the producer sequence table so a restart carries
//! on, and optionally collects chosen channels into operational snapshot files.

pub mod engine;
pub mod envelope;
pub mod journal;
pub mod methods;
pub mod names;
pub mod snapshot;

pub use async_trait::async_trait;
pub use bytes::Bytes;
pub use engine::config::{ChannelConfig, ChannelRule, ChannelSettings, Compaction, Durability, EngineConfig, Grant, Grants, MaxBacklog, MemoryConfig, Method, ProducerConfig, StorageConfig};
pub use engine::ops::{Assignment, ByeReason, Call, ChannelStat, Counters, FaultKind, Hello, IdleLane, Outbound, Refusal, Role, RootStat, SessionId, Stats, Target, Welcome, DELETED, DELETE_FAILED};
pub use engine::{Closed, Engine, EngineError, Session};
pub use envelope::{ClaimToken, Digest, Envelope, EventId, GroupId, Kind, ProducerId, WorkKey};
pub use journal::{Batch, FileJournal, Journal, Location, Record, Recovered, Volatile};
pub use methods::{client, connect, Backlog, Client, ConnectError, Demux, Inbox, Incoming, Link, Methods};
pub use names::{Name, Pattern};
pub use snapshot::SnapshotConfig;
