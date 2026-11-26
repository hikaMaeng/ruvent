//! One work connection as its owner (the worker) sees
//! it: the live link, the joins sent on it, and the unacknowledged buffer of
//! its completions (2.3: a work connection numbers its completions under its
//! own producer id).
//!
//! | state | key | scope | lifetime | bound | cleared by |
//! | --- | --- | --- | --- | --- | --- |
//! | live link and joins | join request number ↔ the owner's service index | one connection | welcome ~ disconnection | the services | the owner ([`WorkLine::down`]) |
//! | completion buffer | (server, seq) | work line | handed over ~ `Committed` | S8 bytes; past it completions are held unnumbered (the server frees no window slot until it has them, so at most the window) | `Committed`, `Welcome.resume` |

use crate::buffer::Unacked;
use crate::line::Line;
use ruvent::{Call, Envelope, EventId, Link, Welcome, WorkerId};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

/// A completion, numbered when it enters the buffer.
#[derive(Clone)]
pub(crate) struct Completion {
    pub id: EventId,
    /// The worker uuid the event was claimed under.
    pub worker: WorkerId,
    pub follow_ups: Vec<Envelope>,
}

impl Completion {
    fn call(&self, seq: u64) -> Call {
        Call::Complete { seq, id: self.id, worker: self.worker, follow_ups: self.follow_ups.clone(), spooled: Vec::new() }
    }

    fn bytes(&self) -> u64 {
        64 + self.follow_ups.iter().map(|event| event.payload.len() as u64 + 64).sum::<u64>()
    }
}

pub(crate) struct Live {
    pub epoch: u64,
    pub link: Arc<dyn Link>,
    /// Joins asked and not yet answered: request number → the owner's service index.
    pub asked: HashMap<u32, usize>,
    /// Joins in effect: service index → the join's name (its `Join`'s req), and back.
    pub joins: HashMap<usize, u32>,
    pub services: HashMap<u32, usize>,
}

pub(crate) struct WorkLine {
    _line: Line,
    pub live: Option<Live>,
    buffer: Unacked<Completion>,
    held: VecDeque<Completion>,
    limit: u64,
    next_req: u32,
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

impl WorkLine {
    pub(crate) fn new(line: Line, limit: u64) -> Self {
        WorkLine { _line: line, live: None, buffer: Unacked::new(), held: VecDeque::new(), limit, next_req: 1 }
    }

    /// Welcomed: drop what the server applied, send the rest again (2.9
    /// "서버 재기동": completions kept through a restart go out under their event and worker uuid).
    pub(crate) async fn up(&mut self, epoch: u64, link: Arc<dyn Link>, welcome: &Welcome) {
        let _ = self.buffer.resume(welcome.resume);
        let resend: Vec<Call> = self.buffer.iter().map(|entry| entry.item.call(entry.seq)).collect();
        self.live = Some(Live { epoch, link: link.clone(), asked: HashMap::new(), joins: HashMap::new(), services: HashMap::new() });
        for call in resend {
            if link.send(call).await.is_err() {
                return;
            }
        }
        self.release_held().await;
    }

    pub(crate) fn down(&mut self, epoch: u64) -> bool {
        if self.live.as_ref().is_some_and(|live| live.epoch == epoch) {
            self.live = None;
            return true;
        }
        false
    }

    pub(crate) fn is(&self, epoch: u64) -> bool {
        self.live.as_ref().is_some_and(|live| live.epoch == epoch)
    }

    /// Sends a call that is not numbered, if connected.
    pub(crate) async fn tell(&self, call: Call) {
        if let Some(live) = &self.live {
            let _ = live.link.send(call).await;
        }
    }

    /// Joins `channel` (`None`: every channel) for `types` as `worker`; the answer names `index`.
    pub(crate) async fn join(&mut self, index: usize, worker: WorkerId, selection: crate::worker::Selection, lease: Duration, dispatch: bool) {
        let crate::worker::Selection { channel, types } = selection;
        let req = self.next_req;
        self.next_req = self.next_req.wrapping_add(1).max(1);
        let Some(live) = &mut self.live else { return };
        live.asked.insert(req, index);
        let _ = live.link.send(Call::Join { req, worker, channel, types, lease_ms: millis(lease), dispatch }).await;
    }

    /// The service index a `Joined` or `Refused` answers. With `joined`, the
    /// join is in effect under the name `req`.
    pub(crate) fn answered(&mut self, req: u32, joined: bool) -> Option<usize> {
        let live = self.live.as_mut()?;
        let index = live.asked.remove(&req)?;
        if joined {
            if let Some(old) = live.joins.insert(index, req) {
                live.services.remove(&old);
            }
            live.services.insert(req, index);
        }
        Some(index)
    }

    /// The name of the service's join on this connection, once it is in effect.
    pub(crate) fn join_of(&self, index: usize) -> Option<u32> {
        self.live.as_ref().and_then(|live| live.joins.get(&index).copied())
    }

    /// The service a join name belongs to.
    pub(crate) fn service_of(&self, join: u32) -> Option<usize> {
        self.live.as_ref().and_then(|live| live.services.get(&join).copied())
    }

    /// Numbers and sends a completion, or holds it while the buffer is full.
    pub(crate) async fn complete(&mut self, completion: Completion) {
        if !self.held.is_empty() || self.full(&completion) {
            self.held.push_back(completion);
            return;
        }
        self.number(completion).await;
    }

    async fn number(&mut self, completion: Completion) {
        let bytes = completion.bytes();
        let call = {
            let seq = self.buffer.push(completion.clone(), bytes);
            completion.call(seq)
        };
        if let Some(live) = &self.live {
            let _ = live.link.send(call).await;
        }
    }

    fn full(&self, completion: &Completion) -> bool {
        !self.buffer.is_empty() && self.buffer.bytes().saturating_add(completion.bytes()) > self.limit
    }

    pub(crate) async fn committed(&mut self, seq: u64) {
        self.buffer.ack(seq);
        self.release_held().await;
    }

    async fn release_held(&mut self) {
        while let Some(next) = self.held.front() {
            if self.full(next) {
                return;
            }
            if let Some(completion) = self.held.pop_front() {
                self.number(completion).await;
            }
        }
    }

    /// Nothing numbered is waiting for its acknowledgement and nothing is held.
    pub(crate) fn settled(&self) -> bool {
        self.buffer.is_empty() && self.held.is_empty()
    }
}
