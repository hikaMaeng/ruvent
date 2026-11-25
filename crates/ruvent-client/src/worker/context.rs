//! What a handler sees: its event, the attempt's fencing token, a way to
//! queue follow-ups, and a way to emit while running.

use super::{Message, RunKey};
use crate::producer::{Producer, ProducerError, Sent};
use parking_lot::Mutex;
use ruvent::{Bytes, ClaimToken, Envelope, EventId, Name, WorkKey};
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
    token: ClaimToken,
    key: RunKey,
    messages: mpsc::Sender<Message>,
    producer: Option<Producer>,
    /// B10: how long a result waits for its requester.
    result_keep: Duration,
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
    pub(crate) fn new(event: Envelope, token: ClaimToken, key: RunKey, messages: mpsc::Sender<Message>, producer: Option<Producer>, result_keep: Duration) -> Self {
        Context { event: Arc::new(event), token, key, messages, producer, result_keep, blocked: Arc::default(), follow_ups: Arc::default() }
    }

    /// The assigned envelope, work key and failure count included.
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

    /// The attempt's name, rising monotonically: a fencing token (2.4
    /// "펜싱"). Guard overwriting writes with it; protect irreversible effects
    /// with a unique business key or a downstream idempotency key.
    pub fn token(&self) -> ClaimToken {
        self.token
    }

    /// The server this run was assigned by.
    pub fn server(&self) -> &str {
        &self.key.server
    }

    pub fn correlation(&self) -> Option<EventId> {
        self.event.correlation
    }

    pub fn reply_channel(&self) -> Option<&Name> {
        self.event.reply.as_ref()
    }

    /// Queues a follow-up, registered with the completion in one step. It
    /// gets a fresh UUIDv7 if it has none and this event's correlation if it
    /// has none. Returns its id.
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

    pub fn emit_claimable(&self, channel: impl Into<Name>, event_type: impl Into<Name>, payload: impl Into<Bytes>) -> EventId {
        self.emit(Envelope::claimable(channel, event_type, payload))
    }

    pub fn emit_dispatch(&self, channel: impl Into<Name>, event_type: impl Into<Name>, payload: impl Into<Bytes>) -> EventId {
        self.emit(Envelope::dispatch(channel, event_type, payload))
    }

    /// The request's result (2.8): a claimable follow-up on its reply
    /// channel, with its correlation and the result keep (B10) as `expires`.
    /// `None` when the event asked for no reply.
    pub fn reply(&self, payload: impl Into<Bytes>) -> Option<EventId> {
        let channel = self.event.reply.clone()?;
        let mut result = Envelope::claimable(channel, self.event.event_type.clone(), payload).with_expires(self.result_keep);
        result.correlation = Some(self.event.correlation.unwrap_or(self.event.id));
        Some(self.emit(result))
    }

    /// A dispatch event sent now, while running, on this run's work
    /// connection (2.2 "실행 중 방출"): it reaches the ledger before this
    /// run's completion, so a requester sees it before the result.
    pub async fn emit_now(&self, event: Envelope) -> Result<(), EmitError> {
        let (done, answer) = oneshot::channel();
        self.messages.send(Message::Emit { key: self.key.clone(), event, done }).await.map_err(|_| EmitError::Closed)?;
        answer.await.map_err(|_| EmitError::Closed)?
    }

    /// A stream fragment to the requester (2.8): a dispatch event on the
    /// reply channel with the request's correlation, sent now.
    pub async fn fragment(&self, payload: impl Into<Bytes>) -> Result<(), EmitError> {
        let Some(channel) = self.event.reply.clone() else { return Ok(()) };
        let mut fragment = Envelope::dispatch(channel, self.event.event_type.clone(), payload);
        fragment.correlation = Some(self.event.correlation.unwrap_or(self.event.id));
        self.emit_now(fragment).await
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
