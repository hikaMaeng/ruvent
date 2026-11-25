//! A service: one channel, handlers by event type, its own slots.

use super::handler::{handler, HandlerFn};
use super::{Context, Retry};
use ruvent::Name;
use std::collections::BTreeMap;
use std::future::Future;
use std::time::Duration;

/// The claimable events of one channel and how to handle them. Every value
/// is a field with its default (chapter 8).
pub struct Service {
    pub(crate) channel: Name,
    pub(crate) handlers: BTreeMap<Name, HandlerFn>,
    /// P: executions at once over every server (2.4).
    pub(crate) slots: u32,
    /// B7: the lease asked in `Join`. Extended automatically at half.
    pub(crate) lease: Duration,
    /// B7: the longest one run may take. Past it extending stops, the run is
    /// cut off and released as this event's failure (B23: unless it waited
    /// for a paused channel).
    pub(crate) run_limit: Duration,
    /// The byte bound of each window (`0`: none).
    pub(crate) window_bytes: u64,
    /// The delay of a `Retry` that names none, and of a panic.
    pub(crate) retry_delay: Duration,
}

impl Service {
    /// An event worker: 16 slots, a 30-second lease and a 30-second run limit (B7).
    pub fn new(channel: impl Into<Name>) -> Self {
        Service {
            channel: channel.into(),
            handlers: BTreeMap::new(),
            slots: 16,
            lease: Duration::from_secs(30),
            run_limit: Duration::from_secs(30),
            window_bytes: 0,
            retry_delay: Duration::from_secs(1),
        }
    }

    /// An executor: long jobs handed over as follow-ups. 4 slots, the same
    /// 30-second lease (extended), a one-hour run limit (B7).
    pub fn executor(channel: impl Into<Name>) -> Self {
        Service { slots: 4, run_limit: Duration::from_secs(3600), ..Service::new(channel) }
    }

    /// Handles events of `event_type`. The join asks only for the types with a handler.
    pub fn on<F, Fut>(mut self, event_type: impl Into<Name>, function: F) -> Self
    where
        F: Fn(Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Retry>> + Send + 'static,
    {
        self.handlers.insert(event_type.into(), handler(function));
        self
    }

    pub fn slots(mut self, slots: u32) -> Self {
        self.slots = slots.max(1);
        self
    }

    pub fn lease(mut self, lease: Duration) -> Self {
        self.lease = lease;
        self
    }

    pub fn run_limit(mut self, limit: Duration) -> Self {
        self.run_limit = limit;
        self
    }

    pub fn window_bytes(mut self, bytes: u64) -> Self {
        self.window_bytes = bytes;
        self
    }

    pub fn retry_delay(mut self, delay: Duration) -> Self {
        self.retry_delay = delay;
        self
    }
}
