//! The worker framework: register handlers, and the framework subscribes,
//! claims, runs and completes.
//!
//! It sees only [`ruvent::Methods`] and an [`ruvent::Inbox`], so the same
//! services run in-process ([`ruvent::connect`]) or against a remote server.
//! Every policy the server dropped lives here:
//!
//! | policy | where |
//! | --- | --- |
//! | retry limit | [`Service::max_retries`]: past it the handler does not run; the event is completed |
//! | failure reporting | [`Service::failures`]: only when set, a failure becomes a dispatch event, a follow-up of the completion |
//! | follow-up events | [`Context::emit`] and friends: registered by the completion, in the same step |
//! | long work | [`Service::executor`]: its own permits and a long lease, fed by follow-ups |
//!
//! ## One execution
//!
//! ```text
//! notice ─► permit? ─no─► keep (up to pending_limit; past it, re-read the backlog later)
//!             │yes
//!             ▼
//!          claim(id, tag, lease) ─refused─► permit back
//!             │
//!             ▼
//!          retry > max_retries? ─yes─► complete [+ failure event]
//!             │no
//!             ▼
//!          handler within lease ─timeout─► permit back, nothing completed: the server reclaims it (retry + 1)
//!             │ ok / error / panic
//!             ▼
//!          permit back ─► complete(id, token, follow-ups or failure event)
//! ```
//!
//! The permit returns as soon as the handler ends; the completion is sent
//! without holding it.

mod app;
mod context;
mod handler;
mod lane;
mod service;

pub use app::{App, Running};
pub use context::Context;
pub use service::Service;

use ruvent::Refusal;
use std::fmt;

/// Why a handler did not succeed. Becomes the payload of the failure event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Failure(pub String);

impl Failure {
    pub fn new(reason: impl Into<String>) -> Self {
        Failure(reason.into())
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Failure {}

impl From<&str> for Failure {
    fn from(reason: &str) -> Self {
        Failure::new(reason)
    }
}

impl From<String> for Failure {
    fn from(reason: String) -> Self {
        Failure(reason)
    }
}

/// Why an app stopped or did not start.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerError {
    /// A subscription at start was refused.
    Refused(Refusal),
    /// The connection to the server ended.
    Disconnected,
}

impl fmt::Display for WorkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkerError::Refused(refusal) => write!(f, "subscription refused: {refusal}"),
            WorkerError::Disconnected => f.write_str("the connection to the event server ended"),
        }
    }
}

impl std::error::Error for WorkerError {}
