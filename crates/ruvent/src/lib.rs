//! ruvent — an embedded event-sourcing core.
//!
//! Two paths with opposite shapes, owned by one single-writer engine:
//!
//! * **work** travels on leased queues: one message, exactly one consumer at a time;
//! * **observation** is an append-only log: every interested reader, none of them consuming.
//!
//! A reaction table joins the two inside the same commit, so recording a fact and
//! handing out the work that follows from it cannot disagree.
//!
//! ## A framework, not a server
//!
//! ruvent is a service framework in which the unit of a service is not a
//! request and its response but an event and the reactions to it. You register
//! functions against actions with [`App`] and [`Service`]; the queues, the
//! allow-list and the reaction table are derived from what you registered.
//! Transports are plugins, and nothing listens on a port unless you add one.

pub mod app;
#[cfg(doctest)]
mod compile_fail;
pub mod engine;
pub mod envelope;
pub mod gateway;
pub mod journal;
#[cfg(feature = "json")]
pub mod wire;
pub mod worker;

pub use app::{App, Module, Service, Started};
/// Implementing [`Transport`], [`Connection`], [`Journal`], [`Authenticator`] or
/// [`Handler`] by hand needs this attribute; it is re-exported so a plugin
/// does not have to pick a matching version of the crate itself.
pub use async_trait::async_trait;
pub use bytes::Bytes;
pub use engine::{Begin, Closed, Compaction, Delivery, Durability, Engine, EngineConfig, Events, From, Outcome, Persist, ReactionTable, Retention, Stats, Subscription};
#[cfg(feature = "json")]
pub use envelope::json;
pub use envelope::{stream_of, Audience, Command, Emit, Envelope, EventId, Kind, Source, Text};
pub use gateway::{Anonymous, Authenticator, ClientFrame, Connection, Credentials, Gateway, Identity, Open, OwnedSessions, Policy, Proposal, Rejected, ServerFrame, Transport};
pub use journal::{FileJournal, Journal, Record, Recovered, Volatile};
pub use tokio_util::sync::CancellationToken;
pub use worker::{Backoff, Context, Failure, Handler, Lost, Running, Worker, WorkerConfig};
