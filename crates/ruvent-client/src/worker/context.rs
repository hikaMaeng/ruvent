//! What a handler sees: its event, a way to queue follow-ups, and the methods.

use super::Failure;
use parking_lot::Mutex;
use ruvent::{Bytes, Envelope, EventId, Methods, Name, WorkKey};
use std::sync::Arc;

/// One execution's view. Cloning shares the same follow-up list.
#[derive(Clone)]
pub struct Context {
    event: Arc<Envelope>,
    methods: Arc<dyn Methods>,
    /// Follow-ups, registered by the completion if the handler succeeds.
    ///
    /// | scope | one execution |
    /// | --- | --- |
    /// | bound | what the handler emits |
    /// | cleared by | the framework, when it builds the completion |
    follow_ups: Arc<Mutex<Vec<Envelope>>>,
}

impl Context {
    pub(crate) fn new(event: Envelope, methods: Arc<dyn Methods>) -> Self {
        Context { event: Arc::new(event), methods, follow_ups: Arc::default() }
    }

    /// The claimed envelope, claim block and work key included.
    pub fn event(&self) -> &Envelope {
        &self.event
    }

    pub fn id(&self) -> EventId {
        self.event.id
    }

    pub fn channel(&self) -> &Name {
        &self.event.channel
    }

    pub fn event_type(&self) -> &Name {
        &self.event.event_type
    }

    /// Times the server reclaimed this event after a missed deadline.
    pub fn retry(&self) -> u16 {
        self.event.retry
    }

    /// The registration's work key: the same across reclaims and restarts. Use
    /// it to recognise a re-run of work whose external effect already happened.
    pub fn key(&self) -> Option<WorkKey> {
        self.event.key
    }

    pub fn payload(&self) -> &Bytes {
        &self.event.payload
    }

    pub fn correlation(&self) -> Option<EventId> {
        self.event.correlation
    }

    pub fn reply(&self) -> Option<&Name> {
        self.event.reply.as_ref()
    }

    /// The methods, for calls that should not wait for the completion.
    pub fn methods(&self) -> &Arc<dyn Methods> {
        &self.methods
    }

    /// Queues a follow-up. It gets a fresh UUIDv7 if it has none, and this
    /// event's correlation if it has none. Returns its id.
    pub fn emit(&self, mut event: Envelope) -> EventId {
        if event.id.is_nil() {
            event.id = EventId::new_v7();
        }
        if event.correlation.is_none() {
            event.correlation = self.event.correlation;
        }
        let id = event.id;
        self.follow_ups.lock().push(event);
        id
    }

    /// Queues a claimable follow-up.
    pub fn emit_claimable(&self, channel: impl Into<Name>, event_type: impl Into<Name>, payload: impl Into<Bytes>) -> EventId {
        self.emit(Envelope::claimable(channel, event_type, payload))
    }

    /// Queues a dispatch follow-up.
    pub fn emit_dispatch(&self, channel: impl Into<Name>, event_type: impl Into<Name>, payload: impl Into<Bytes>) -> EventId {
        self.emit(Envelope::dispatch(channel, event_type, payload))
    }

    /// A failure to return: `return Err(context.fail("why"))`.
    pub fn fail(&self, reason: impl Into<String>) -> Failure {
        Failure::new(reason)
    }

    pub(crate) fn take_follow_ups(&self) -> Vec<Envelope> {
        std::mem::take(&mut *self.follow_ups.lock())
    }
}
