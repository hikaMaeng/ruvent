//! ruvent — the event server core.
//!
//! The server offers seven methods and nothing else: register a claimable
//! event, register a dispatch event, claim, complete, delete, subscribe,
//! unsubscribe. Claimable events wait in one queue until a completion report
//! removes them; dispatch events are handed to the subscribers of their
//! channel and are gone in the same step. Both use one envelope.
//!
//! The server does not tell workers, proxies and clients apart: each is a
//! connection that calls the methods, in-process ([`methods::connect`]) or
//! over a socket (`ruvent-client`). Retry limits, failure handling and
//! follow-ups are the workers' logic, not the server's.
//!
//! Journaling is not a method. The engine journals the claimable queue so a
//! restart reloads what was still owed, and optionally collects chosen
//! channels into operational snapshot files.

pub mod engine;
pub mod envelope;
pub mod journal;
pub mod methods;
pub mod names;
pub mod snapshot;

pub use async_trait::async_trait;
pub use bytes::Bytes;
pub use engine::config::{Compaction, Durability, EngineConfig, Grant, Grants, Method};
pub use engine::ops::{Call, Outbound, Refusal, Reply, SessionId, DELETED, DELETE_FAILED};
pub use engine::{Closed, Engine, EngineError, Session};
pub use envelope::{Claim, ClaimToken, Envelope, EventId, Kind, WorkKey};
pub use journal::{FileJournal, Journal, Record, Recovered, Volatile};
pub use methods::{connect, Backlog, Client, Demux, Inbox, Incoming, Link, Methods};
pub use names::{Name, Pattern};
pub use snapshot::SnapshotConfig;
