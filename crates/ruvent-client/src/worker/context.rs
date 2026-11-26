//! What a handler sees: its event, the worker uuid it is claimed under, a way to
//! queue follow-ups, and a way to emit while running.

use super::{Message, RunKey};
use crate::producer::{Producer, ProducerError, Sent};
use parking_lot::Mutex;
use ruvent::{Bytes, Channel, Envelope, EventId, WorkKey, WorkerId};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// Why an emission did not go out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EmitError {
    /// The work connection this run came from is down: a dispatch event is
    /// one-shot and is not kept (0.3).
    Disconnected,
    /// The worker is gone.
    Closed,
}

/// One execution's view. Cloning shares the same follow-up list.
#[derive(Clone)]
pub struct Context {
    event: Arc<Envelope>,
    key: RunKey,
    messages: mpsc::Sender<Message>,
    producer: Option<Producer>,
    /// Set while a send waits for a paused channel (B23).
    blocked: Arc<AtomicBool>,
    /// Follow-ups, registered by the completion if the handler succeeds.
    ///
    /// | key | position |
    /// | --- | --- |
    /// | scope | one execution |
    /// | lifetime | the run ~ its completion |
    /// | bound | what the handler emits (the server takes at most S10 per completion) |
    /// | cleared by | the worker, when it builds the completion |
    follow_ups: Arc<Mutex<Vec<Envelope>>>,
}

impl Context {
    pub(crate) fn new(event: Envelope, key: RunKey, messages: mpsc::Sender<Message>, producer: Option<Producer>) -> Self {
        Context { event: Arc::new(event), key, messages, producer, blocked: Arc::default(), follow_ups: Arc::default() }
    }

    /// The assigned envelope, work key and failure count included.
    pub fn event(&self) -> &Envelope {
        &self.event
    }

    pub fn id(&self) -> EventId {
        self.event.id
    }

    pub fn channel(&self) -> Channel {
        self.event.channel
    }

    pub fn event_type(&self) -> u16 {
        self.event.event_type
    }

    pub fn payload(&self) -> &Bytes {
        &self.event.payload
    }

    /// Failures counted against this event so far (lease expiries and
    /// `Retry::failed`). Informational: the server isolates, not the SDK.
    pub fn failures(&self) -> u16 {
        self.event.failures
    }

    /// The registration's work key: the same across reclaims and restarts.
    pub fn key(&self) -> Option<WorkKey> {
        self.event.key
    }

    /// The worker uuid the event is claimed under (the claim is this and the
    /// event id; there is no attempt token). A run whose lease ran out may
    /// overlap a later one: protect irreversible effects with a unique
    /// business key or a downstream idempotency key.
    pub fn worker(&self) -> Option<WorkerId> {
        self.event.worker
    }

    /// The server this run was assigned by.
    pub fn server(&self) -> &str {
        &self.key.server
    }

    /// Queues a follow-up, registered with the completion in one step. It
    /// gets a fresh UUIDv7 if it has none. Returns its id.
    pub fn emit(&self, mut event: Envelope) -> EventId {
        if event.id.is_nil() {
            event.id = EventId::new_v7();
        }
        let id = event.id;
        self.follow_ups.lock().push(event);
        id
    }

    pub fn emit_claimable(&self, channel: Channel, event_type: u16, payload: impl Into<Bytes>) -> EventId {
        self.emit(Envelope::claimable(channel, event_type, payload))
    }

    pub fn emit_dispatch(&self, channel: Channel, event_type: u16, payload: impl Into<Bytes>) -> EventId {
        self.emit(Envelope::dispatch(channel, event_type, payload))
    }

    /// A dispatch event sent now, while running, on this run's work
    /// connection (2.2 "실행 중 방출"): it reaches the ledger before this
    /// run's completion, so whoever subscribes its channel sees it before any
    /// follow-up of the completion (one connection keeps the order).
    pub async fn emit_now(&self, event: Envelope) -> Result<(), EmitError> {
        let (done, answer) = oneshot::channel();
        self.messages.send(Message::Emit { key: self.key.clone(), event, done }).await.map_err(|_| EmitError::Closed)?;
        answer.await.map_err(|_| EmitError::Closed)?
    }

    /// Registers `event` through the worker's producer (another lineage, not
    /// a follow-up). While it waits for a paused channel the run is marked:
    /// a run held past its limit by flow control is released as not the
    /// event's fault (B23).
    pub async fn send(&self, event: Envelope) -> Result<Sent, ProducerError> {
        let Some(producer) = &self.producer else { return Err(ProducerError::Closed) };
        producer.send_marked(event, &self.blocked).await
    }

    /// The handler queues a failure answer: `return Err(cx.later(...))`.
    pub fn later(&self, delay: Duration) -> super::Retry {
        super::Retry::later(delay)
    }

    pub(crate) fn blocked(&self) -> bool {
        self.blocked.load(Ordering::Relaxed)
    }

    pub(crate) fn take_follow_ups(&self) -> Vec<Envelope> {
        std::mem::take(&mut *self.follow_ups.lock())
    }
}
