//! ruvent — the event server core.
//!
//! One server is one ledger (0.3). Connections come in three roles (2.2):
//! production (register and delete, numbered by a producer sequence and
//! acknowledged cumulatively once committed), work (join a channel, a set of
//! types or both, open a window, receive assignments, complete, extend,
//! release, claim by id) and observation (subscribe, statistics, drain).
//! Claimable events wait in per-(channel, type) lanes until a completion
//! removes them; dispatch events go to the subscribers of their channel and
//! are gone in the same step. Both use one envelope. A channel is a UUIDv7
//! the server reads nothing from; settings and grants are by type.
//!
//! Retry counts, back-off and follow-ups are the workers' logic; the server
//! keeps four things: lease ends, visibility times, expiries and one failure
//! threshold (`isolate_after`).
//!
//! Journaling is not an operation. The engine journals the claimable events
//! of durable types and the producer sequence table so a restart carries on,
//! and optionally collects chosen types into operational snapshot files.

pub mod engine;
pub mod envelope;
pub mod journal;
pub mod methods;
pub mod names;
pub mod snapshot;
pub mod types;

pub use async_trait::async_trait;
pub use bytes::Bytes;
pub use engine::config::{Durability, EngineConfig, Grant, Grants, MaxBacklog, MemoryConfig, Method, ProducerConfig, StorageConfig, TypeConfig, TypeRule, TypeSettings};
pub use engine::ops::{
    delete_outcome, Assignment, ByeReason, Call, Counters, DeleteFailure, FaultKind, Hello, IdleLane, Outbound, Pressure, Refusal, Role, RootStat, SessionId, Stats, TypeStat, Welcome,
};
pub use engine::{Closed, Engine, EngineError, Session};
pub use envelope::{Channel, ClaimToken, Digest, Envelope, EventId, GroupId, Kind, ProducerId, WorkKey, SERVER_TYPE};
pub use journal::{
    Batch, Bodies, CheckpointHead, Checkpoints, Chores, End, Entry, Journal, Lane, Location, Part, Parts, ProducerEntry, Record, Recovered, Replay, SegmentLog, SegmentState, Source, Spool,
    SpoolWrite, Spooled, Standing, Volatile, Written,
};
pub use methods::{client, connect, Backlog, Client, ConnectError, Demux, Inbox, Incoming, Link, Methods};
pub use names::Name;
pub use snapshot::SnapshotConfig;
pub use types::TypeSet;
