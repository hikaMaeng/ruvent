//! A service: one channel, handlers by event type, its own permit budget.

use super::handler::{handler, HandlerFn};
use super::{Context, Failure};
use ruvent::Name;
use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

/// The lease when none is set (Q2: 30 seconds).
pub const DEFAULT_LEASE: Duration = Duration::from_secs(30);
/// An executor's lease when none is set.
pub const EXECUTOR_LEASE: Duration = Duration::from_secs(3600);

/// The claimable events of one channel and how to handle them.
pub struct Service {
    pub(crate) channel: Name,
    pub(crate) handlers: HashMap<Name, HandlerFn>,
    pub(crate) permits: usize,
    pub(crate) lease: Duration,
    pub(crate) worker: u32,
    pub(crate) max_retries: u16,
    pub(crate) failures: Option<Name>,
    pub(crate) pending_limit: usize,
}

impl Service {
    /// An event worker: 16 permits, a 30-second lease, at most 5 retries.
    pub fn new(channel: impl Into<Name>) -> Self {
        Service { channel: channel.into(), handlers: HashMap::new(), permits: 16, lease: DEFAULT_LEASE, worker: 0, max_retries: 5, failures: None, pending_limit: 1024 }
    }

    /// A work executor: long jobs handed over as follow-ups, so event workers
    /// return to their pool at once. Its own budget of 4 permits and a one-hour lease.
    pub fn executor(channel: impl Into<Name>) -> Self {
        Service { permits: 4, lease: EXECUTOR_LEASE, ..Service::new(channel) }
    }

    /// Handles events of `event_type`. Notices of types without a handler are not claimed.
    pub fn on<F, Fut>(mut self, event_type: impl Into<Name>, function: F) -> Self
    where
        F: Fn(Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Failure>> + Send + 'static,
    {
        self.handlers.insert(event_type.into(), handler(function));
        self
    }

    /// Executions at once. Each service has its own budget.
    pub fn permits(mut self, permits: usize) -> Self {
        self.permits = permits.max(1);
        self
    }

    /// The claim's lease, and how long a handler may run. A handler that runs
    /// past it is abandoned without a completion; the server reclaims the event.
    pub fn lease(mut self, lease: Duration) -> Self {
        self.lease = lease;
        self
    }

    /// The worker tag written into the claim block. The server never reads it.
    pub fn worker_tag(mut self, tag: u32) -> Self {
        self.worker = tag;
        self
    }

    /// An event reclaimed more often than this is completed without running
    /// the handler (with a failure event if [`Service::failures`] is set).
    pub fn max_retries(mut self, retries: u16) -> Self {
        self.max_retries = retries;
        self
    }

    /// Where failures are reported, as a dispatch event that is a follow-up of
    /// the completion: the failed event's type, its id as correlation, the
    /// reason as payload. Without this, a failure is just a completion.
    pub fn failures(mut self, channel: impl Into<Name>) -> Self {
        self.failures = Some(channel.into());
        self
    }

    /// Notices kept while every permit is taken. Past it they are dropped and
    /// the backlog is read again from the start when a permit is free.
    pub fn pending_limit(mut self, limit: usize) -> Self {
        self.pending_limit = limit.max(1);
        self
    }
}
