//! A service: one join (a channel, a set of types, or both), handlers by
//! event type, its own slots.

use super::handler::{handler, HandlerFn};
use super::{Context, Retry};
use ruvent::{Channel, Name};
use std::collections::BTreeMap;
use std::future::Future;
use std::time::Duration;

/// What one service's `Join` asks for: the channel (`None`: every channel),
/// the types (empty: every type) and whether isolated events.
#[derive(Clone, Debug)]
pub(crate) struct Selection {
    pub channel: Option<Channel>,
    pub types: Vec<u16>,
    pub isolated: bool,
}

/// The claimable events one join selects and how to handle them. Every
/// value is a field with its default (chapter 8).
///
/// [`Service::new`] joins one channel and takes the types it has handlers
/// for. [`Service::typed`] joins no channel: it takes the types it has
/// handlers for from every channel — a worker for one kind of request on a
/// channel per request (plan 9.14). [`Service::isolated`] makes either take
/// only isolated events (B6), for reprocessing. Two services of one worker
/// may not select the same lane; the server refuses the second join (`Overlap`).
pub struct Service {
    /// `None`: every channel.
    pub(crate) channel: Option<Channel>,
    pub(crate) handlers: BTreeMap<u16, HandlerFn>,
    /// Takes isolated events only; otherwise only the others.
    pub(crate) isolated: bool,
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
    /// An event worker on `channel`: 16 slots, a 30-second lease and a 30-second run limit (B7).
    pub fn new(channel: Channel) -> Self {
        Service { channel: Some(channel), ..Service::typed() }
    }

    /// An event worker on no channel: the types it has handlers for, from
    /// every channel. Same defaults as [`Service::new`].
    pub fn typed() -> Self {
        Service {
            channel: None,
            handlers: BTreeMap::new(),
            isolated: false,
            slots: 16,
            lease: Duration::from_secs(30),
            run_limit: Duration::from_secs(30),
            window_bytes: 0,
            retry_delay: Duration::from_secs(1),
        }
    }

    /// An executor: long jobs handed over as follow-ups. 4 slots, the same
    /// 30-second lease (extended), a one-hour run limit (B7).
    pub fn executor(channel: Channel) -> Self {
        Service { slots: 4, run_limit: Duration::from_secs(3600), ..Service::new(channel) }
    }

    /// Handles events of `event_type`. The join asks only for the types with a handler.
    pub fn on<F, Fut>(mut self, event_type: u16, function: F) -> Self
    where
        F: Fn(Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Retry>> + Send + 'static,
    {
        self.handlers.insert(event_type, handler(function));
        self
    }

    /// Takes only isolated events (B6): a service that reprocesses or
    /// inspects what its types gave up on.
    pub fn isolated(mut self) -> Self {
        self.isolated = true;
        self
    }

    /// Whether this service's join selects the event.
    pub(crate) fn selects(&self, event: &ruvent::Envelope) -> bool {
        self.isolated == event.isolated && self.channel.is_none_or(|own| own == event.channel) && self.handlers.contains_key(&event.event_type)
    }

    /// What the join asks for. A channel service with no handler asks for
    /// every type (and gives each back).
    pub(crate) fn selection(&self) -> Selection {
        Selection { channel: self.channel, types: self.handlers.keys().copied().collect(), isolated: self.isolated }
    }

    /// A name for logs and books: the channel, or the types.
    pub(crate) fn label(&self) -> Name {
        let isolated = if self.isolated { " (isolated)" } else { "" };
        match &self.channel {
            Some(channel) => Name::new(&format!("{channel}{isolated}")),
            None => Name::new(&format!("types {:?}{isolated}", self.handlers.keys().collect::<Vec<_>>())),
        }
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
