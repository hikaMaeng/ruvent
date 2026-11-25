//! The requester (2.8, 2.9 "요청자"): request–response over one reply
//! channel per instance, `reply.<instance id>`.
//!
//! * Requests go out through a [`Producer`] that may only pick servers whose
//!   reply-channel join is confirmed: a result is registered on the server
//!   that handled the request, so that server must be able to hand it back.
//! * On every server the requester joins its reply channel once per
//!   connection, with dispatch (stream fragments come on the same work
//!   connection as the result, so the ledger's per-connection order keeps
//!   fragments ahead of the result), a short lease (S16) and a wide window.
//! * A result is a claimable event (it waits while the requester is away);
//!   it is handed to its waiter by `correlation` and completed. A result
//!   nobody waits for is completed too.
//! * A waiter ends with its result, a cancel (dropping the [`Stream`]) or its
//!   deadline. **Losing a connection does not end it**: the result is
//!   assigned again once the requester is back. If the work connection to
//!   the request's server changed meanwhile, the fragments in between are
//!   lost and the stream says so ([`Stream::fragments_lost`]).
//! * At the deadline the request may be sent again with the same id and
//!   correlation (within [`RequestOptions::retries`] and the retry budget,
//!   S9): an active original swallows the resend and only a new waiter
//!   stands, so nothing runs twice.
//!
//! State (2.10, SDK):
//!
//! | state | key | scope | lifetime | bound | cleared by |
//! | --- | --- | --- | --- | --- | --- |
//! | reply waiters | `correlation` → waiter, fragment-loss mark | requester | request ~ result, cancel or deadline | requests in progress | SDK ([`Stream`] drop, result) |
//! | reply joins | server | requester | `Joined` ~ disconnection | servers | SDK |
//! | completion buffer | (server, seq) | work line | handed over ~ `Committed` | S8 | SDK |
//! | retry budget | requester | process | the requester's | one counter | SDK (its producer's [`crate::producer::RetryBudget`]) |

use crate::line::backoff::Reconnect;
use crate::line::{Event, Line, Spec};
use crate::link::Connector;
use crate::producer::{Gate, Producer, ProducerConfig, ProducerError};
use crate::servers::{Server, ServerList};
use crate::workline::{Completion, WorkLine};
use parking_lot::Mutex;
use ruvent::{Call, Envelope, EventId, Hello, Name, Outbound};
use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

/// Every value is a field with its default (chapter 8).
#[derive(Clone)]
pub struct RequesterConfig {
    /// The credential an in-process connection runs under.
    pub credential: String,
    pub reconnect: Reconnect,
    /// S16: the lease of a result's assignment.
    pub lease: Duration,
    /// Results assigned at once per server.
    pub window: u32,
    /// S8 for the work lines.
    pub unacked_bytes: u64,
    /// Fragments kept per request before the oldest unread are dropped (and the loss marked).
    pub fragments: usize,
    /// The producer requests go out through.
    pub producer: ProducerConfig,
}

impl Default for RequesterConfig {
    fn default() -> Self {
        RequesterConfig {
            credential: String::new(),
            reconnect: Reconnect::default(),
            lease: Duration::from_secs(5),
            window: 1024,
            unacked_bytes: 64 << 20,
            fragments: 1024,
            producer: ProducerConfig::default(),
        }
    }
}

/// One request's limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestOptions {
    /// The call deadline: how long to wait for the result. Also the
    /// request's `expires` unless it carries one (2.9 "재시도 계약").
    pub deadline: Duration,
    /// Resends at the deadline with the same id and correlation.
    pub retries: u32,
}

impl Default for RequestOptions {
    fn default() -> Self {
        RequestOptions { deadline: Duration::from_secs(30), retries: 0 }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestError {
    /// The deadline passed (after any retries).
    Timeout,
    Producer(ProducerError),
    Closed,
}

impl fmt::Display for RequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequestError::Timeout => f.write_str("the request's deadline passed"),
            RequestError::Producer(error) => write!(f, "{error}"),
            RequestError::Closed => f.write_str("the requester is closed"),
        }
    }
}

impl std::error::Error for RequestError {}

impl From<ProducerError> for RequestError {
    fn from(error: ProducerError) -> Self {
        RequestError::Producer(error)
    }
}

/// What a request's stream yields, in the order the server sent it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Part {
    Fragment(Envelope),
    Result(Envelope),
}

/// A whole answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Response {
    pub result: Envelope,
    pub fragments: Vec<Envelope>,
    /// Some fragments may be missing (2.8).
    pub fragments_lost: bool,
}

struct Waiter {
    fragments: mpsc::Sender<Envelope>,
    result: Option<oneshot::Sender<Envelope>>,
    lost: Arc<AtomicBool>,
    /// The server the request went to, once known.
    server: Option<Server>,
}

struct Shared {
    /// | key | `correlation` |
    /// | --- | --- |
    /// | scope | this requester |
    /// | lifetime | request ~ result, cancel or deadline |
    /// | bound | requests in progress |
    /// | cleared by | the result, [`Stream`]'s drop |
    waiters: Mutex<HashMap<EventId, Waiter>>,
    joined: watch::Sender<BTreeSet<Server>>,
}

/// A requester. Cheap to clone.
#[derive(Clone)]
pub struct Requester {
    inner: Arc<Inner>,
}

struct Inner {
    shared: Arc<Shared>,
    producer: Producer,
    reply: Name,
    fragments: usize,
    task: JoinHandle<()>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Requester {
    /// Starts the reply lines and the producer. Returns at once.
    pub fn start(connector: Arc<dyn Connector>, servers: &dyn ServerList, config: RequesterConfig) -> Requester {
        let instance = EventId::new_v7();
        let reply = Name::new(&format!("reply.{instance}"));
        let mut producer_config = config.producer.clone();
        if producer_config.credential.is_empty() {
            producer_config.credential = config.credential.clone();
        }
        let producer = Producer::start(connector.clone(), servers, producer_config);
        let shared = Arc::new(Shared { waiters: Mutex::default(), joined: watch::channel(BTreeSet::new()).0 });
        let actor = Actor { shared: shared.clone(), connector, hello: Hello::worker(&config.credential), reply: reply.clone(), config: config.clone(), lines: HashMap::new() };
        let task = tokio::spawn(actor.run(servers.watch()));
        Requester { inner: Arc::new(Inner { shared, producer, reply, fragments: config.fragments, task }) }
    }

    /// This instance's reply channel.
    pub fn reply_channel(&self) -> &Name {
        &self.inner.reply
    }

    /// The servers whose reply join is confirmed now.
    pub fn joined(&self) -> BTreeSet<Server> {
        self.inner.shared.joined.borrow().clone()
    }

    /// The producer requests go out through (its retry budget is the requester's).
    pub fn producer(&self) -> &Producer {
        &self.inner.producer
    }

    /// Sends a request and waits for the whole answer.
    pub async fn request(&self, event: Envelope, options: RequestOptions) -> Result<Response, RequestError> {
        let mut stream = self.stream(event, options).await?;
        let mut fragments = Vec::new();
        loop {
            match stream.next().await? {
                Some(Part::Fragment(fragment)) => fragments.push(fragment),
                Some(Part::Result(result)) => return Ok(Response { result, fragments, fragments_lost: stream.fragments_lost() }),
                None => return Err(RequestError::Closed),
            }
        }
    }

    /// Sends a request and returns its stream of fragments and result.
    pub async fn stream(&self, mut event: Envelope, options: RequestOptions) -> Result<Stream, RequestError> {
        if event.id.is_nil() {
            event.id = EventId::new_v7();
        }
        let correlation = *event.correlation.get_or_insert_with(EventId::new_v7);
        event.reply = Some(self.inner.reply.clone());
        if event.expires.is_none() {
            event = event.with_expires(options.deadline);
        }
        let (fragment_tx, fragments) = mpsc::channel(self.inner.fragments.max(1));
        let (result_tx, result) = oneshot::channel();
        let lost = Arc::new(AtomicBool::new(false));
        self.inner.shared.waiters.lock().insert(correlation, Waiter { fragments: fragment_tx, result: Some(result_tx), lost: lost.clone(), server: None });
        let mut stream = Stream {
            inner: self.inner.clone(),
            event: event.clone(),
            correlation,
            fragments,
            result,
            lost,
            options,
            deadline: Instant::now() + options.deadline,
            retries: options.retries,
            done: false,
        };
        let sent = self.inner.producer.send_gated(event, self.gate()).await?;
        stream.sent_to(sent.server);
        Ok(stream)
    }

    fn gate(&self) -> Gate {
        self.inner.shared.joined.subscribe()
    }
}

/// One request's answer as it arrives. Dropping it cancels the waiter.
pub struct Stream {
    inner: Arc<Inner>,
    event: Envelope,
    correlation: EventId,
    fragments: mpsc::Receiver<Envelope>,
    result: oneshot::Receiver<Envelope>,
    lost: Arc<AtomicBool>,
    options: RequestOptions,
    deadline: Instant,
    retries: u32,
    done: bool,
}

impl Stream {
    pub fn correlation(&self) -> EventId {
        self.correlation
    }

    pub fn id(&self) -> EventId {
        self.event.id
    }

    /// Fragments may be missing: the work connection to the request's server changed.
    pub fn fragments_lost(&self) -> bool {
        self.lost.load(Ordering::Relaxed)
    }

    fn sent_to(&mut self, server: Server) {
        if let Some(waiter) = self.inner.shared.waiters.lock().get_mut(&self.correlation) {
            waiter.server = Some(server);
        }
    }

    /// The next fragment, or the result (then `None` after it). Waits until
    /// the deadline; at the deadline sends again if retries are left.
    pub async fn next(&mut self) -> Result<Option<Part>, RequestError> {
        if self.done {
            return Ok(None);
        }
        loop {
            let left = self.deadline.saturating_duration_since(Instant::now());
            let waited = tokio::time::timeout(left, async {
                tokio::select! {
                    biased;
                    Some(fragment) = self.fragments.recv() => Ok(Part::Fragment(fragment)),
                    result = &mut self.result => result.map(Part::Result).map_err(|_| RequestError::Closed),
                }
            })
            .await;
            match waited {
                Ok(Ok(part)) => {
                    if matches!(part, Part::Result(_)) {
                        self.done = true;
                    }
                    return Ok(Some(part));
                }
                Ok(Err(error)) => return Err(error),
                Err(_) => {
                    if self.retries == 0 {
                        return Err(RequestError::Timeout);
                    }
                    self.retries -= 1;
                    let gate = self.inner.shared.joined.subscribe();
                    let sent = self.inner.producer.retry_gated(self.event.clone(), gate).await?;
                    self.sent_to(sent.server);
                    self.deadline = Instant::now() + self.options.deadline;
                }
            }
        }
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        self.inner.shared.waiters.lock().remove(&self.correlation);
    }
}

struct Actor {
    shared: Arc<Shared>,
    connector: Arc<dyn Connector>,
    hello: Hello,
    reply: Name,
    config: RequesterConfig,
    lines: HashMap<Server, WorkLine>,
}

impl Actor {
    async fn run(mut self, mut list: watch::Receiver<BTreeSet<Server>>) {
        let (events, mut incoming) = mpsc::channel::<Event>(1024);
        let mut listed = true;
        // The list's owner may drop it: the servers named last stay.
        let mut watching = true;
        loop {
            if listed {
                let wanted = list.borrow_and_update().clone();
                self.follow_list(&wanted, &events);
                listed = false;
            }
            tokio::select! {
                changed = list.changed(), if watching => match changed {
                    Ok(()) => listed = true,
                    Err(_) => watching = false,
                },
                Some(event) = incoming.recv() => self.event(event).await,
            }
        }
    }

    fn follow_list(&mut self, wanted: &BTreeSet<Server>, events: &mpsc::Sender<Event>) {
        self.lines.retain(|server, _| wanted.contains(server));
        self.shared.joined.send_modify(|joined| joined.retain(|server| wanted.contains(server)));
        for server in wanted {
            if self.lines.contains_key(server) {
                continue;
            }
            let spec = Spec { server: server.clone(), hello: self.hello.clone(), connector: self.connector.clone(), reconnect: self.config.reconnect };
            self.lines.insert(server.clone(), WorkLine::new(Line::spawn(spec, events.clone()), self.config.unacked_bytes));
        }
    }

    async fn event(&mut self, event: Event) {
        match event {
            Event::Up { server, epoch, link, welcome, .. } => {
                let Some(line) = self.lines.get_mut(&server) else { return };
                line.up(epoch, link, &welcome).await;
                line.join(0, self.reply.clone(), Vec::new(), self.config.lease, true).await;
            }
            Event::Message { server, epoch, message, .. } => {
                if !self.lines.get(&server).is_some_and(|line| line.is(epoch)) {
                    return;
                }
                self.outbound(server, *message).await;
            }
            Event::Down { server, epoch, .. } => {
                let Some(line) = self.lines.get_mut(&server) else { return };
                if !line.down(epoch) {
                    return;
                }
                self.shared.joined.send_modify(|joined| {
                    joined.remove(&server);
                });
                // 2.8: fragments sent while this connection was changing are lost.
                for waiter in self.shared.waiters.lock().values() {
                    if waiter.server.as_ref() == Some(&server) {
                        waiter.lost.store(true, Ordering::Relaxed);
                    }
                }
            }
            Event::Fatal { server, .. } => {
                self.shared.joined.send_modify(|joined| {
                    joined.remove(&server);
                });
            }
        }
    }

    async fn outbound(&mut self, server: Server, message: Outbound) {
        let Some(line) = self.lines.get_mut(&server) else { return };
        match message {
            Outbound::Joined { req } => {
                if line.answered(req).is_none() {
                    return;
                }
                line.tell(Call::Window { channel: self.reply.clone(), count: self.config.window, bytes: 0 }).await;
                self.shared.joined.send_modify(|joined| {
                    joined.insert(server);
                });
            }
            Outbound::Refused { req, .. } => {
                line.answered(req);
            }
            Outbound::Assign(assignment) => {
                let correlation = assignment.event.correlation.unwrap_or(assignment.event.id);
                let waiter = self.shared.waiters.lock().get_mut(&correlation).and_then(|waiter| waiter.result.take());
                let (id, token) = (assignment.event.id, assignment.token);
                if let Some(waiter) = waiter {
                    let _ = waiter.send(assignment.event);
                }
                line.complete(Completion { id, token, follow_ups: Vec::new() }).await;
            }
            Outbound::Delivery(fragment) => {
                let Some(correlation) = fragment.correlation else { return };
                let waiters = self.shared.waiters.lock();
                if let Some(waiter) = waiters.get(&correlation) {
                    if waiter.fragments.try_send(fragment).is_err() {
                        waiter.lost.store(true, Ordering::Relaxed);
                    }
                }
            }
            Outbound::Pending { .. } => {
                line.tell(Call::Window { channel: self.reply.clone(), count: self.config.window, bytes: 0 }).await;
            }
            Outbound::Committed { seq } => line.committed(seq).await,
            _ => {}
        }
    }
}
