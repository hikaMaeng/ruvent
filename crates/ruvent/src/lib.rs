//! ruvent — the event server core.
//!
//! One server is one ledger (0.3). Connections come in three roles (2.2):
//! production (register and delete, numbered by a producer sequence and
//! acknowledged cumulatively once committed), work (join a channel, a set of
//! types or both, open a window, receive assignments, complete, extend,
//! release, claim by id) and observation (subscribe, statistics, drain).
//! Claimable events wait in per-(channel, type) lanes until a completion
//! removes them; dispatch events go to the subscribers of their channel and
//! are gone in the same step. Both use one envelope, which carries only what
//! the server's own work needs (plan 9.15). A channel is a UUIDv7 the server
//! reads nothing from; the type is what workers join by.
//!
//! Retry limits, back-off, dead letters, expiry, ordering and follow-ups are
//! the workers' logic; the server keeps lease ends, visibility times and the
//! failure count, and never acts on the count.
//!
//! Journaling is not an operation. The engine journals every claimable event
//! and the producer sequence table so a restart carries on, and optionally
//! collects chosen types into operational snapshot files.

pub mod engine;
pub mod envelope;
pub mod journal;
pub mod methods;
pub mod names;
pub mod snapshot;

pub use async_trait::async_trait;
pub use bytes::Bytes;
pub use engine::config::{Durability, EngineConfig, Grant, Grants, MemoryConfig, Method, ProducerConfig, StorageConfig};
pub use engine::ops::{delete_outcome, Assignment, ByeReason, Call, Counters, DeleteFailure, FaultKind, Hello, IdleLane, Outbound, Pressure, Refusal, Role, SessionId, Stats, TypeStat, Welcome};
pub use engine::{Closed, Engine, EngineError, Session};
pub use envelope::{Channel, Digest, Envelope, EventId, Kind, ProducerId, WorkKey, WorkerId, SERVER_TYPE};
pub use journal::{
    Batch, Bodies, CheckpointHead, Checkpoints, Chores, End, Entry, Journal, Lane, Location, Part, Parts, ProducerEntry, Record, Recovered, Replay, SegmentLog, SegmentState, Source, Spool,
    SpoolWrite, Spooled, Standing, Volatile, Written,
};
pub use methods::{client, connect, Backlog, Client, ConnectError, Demux, Inbox, Incoming, Link, Methods};
pub use names::Name;
pub use snapshot::SnapshotConfig;
