//! The server's operations as one contract, [`Methods`], with one client
//! implementation, [`Client`], over a [`Link`]: in-process ([`connect`]) or a
//! socket (the `ruvent-client` crate). The server does not know which link a
//! caller used.
//!
//! A [`Client`] is one connection, and a connection has one role (2.2):
//! production calls on a production connection, work calls on a work
//! connection, observation calls on an observation connection. A call the
//! role does not send is refused by the server (`Refusal::Malformed` for an
//! answered call, a `Fault` otherwise).
//!
//! Production and completion are not answered one by one (0.3): `register`,
//! `delete` and `complete` return the sequence number they were sent under,
//! and [`Methods::committed`] waits for the cumulative `Committed` to reach it.

use crate::engine::ops::{Assignment, ByeReason, Call, FaultKind, Hello, Outbound, Refusal, Role, Stats, Welcome};
use crate::engine::{Closed, Engine, Session};
use crate::envelope::{Channel, ClaimToken, Envelope, EventId, Kind, WorkKey};
use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};

/// One page of a channel's waiting backlog.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Backlog {
    /// Notices (head and work key) in work-key order.
    pub notices: Vec<Envelope>,
    /// More are waiting: subscribe again with `after` = the last notice's key.
    pub more: bool,
}

impl Backlog {
    /// The key to continue from.
    pub fn last(&self) -> Option<WorkKey> {
        self.notices.last().and_then(|notice| notice.key)
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// The event server's operations.
#[async_trait]
pub trait Methods: Send + Sync + 'static {
    /// Production: registers an event, numbered by the producer sequence. A
    /// claimable event with a nil id gets a UUIDv7 here, before it leaves.
    /// Work: emits a dispatch event (sequence number 0). Returns the sequence
    /// number and the id.
    async fn register(&self, event: Envelope) -> Result<(u64, EventId), Refusal>;
    /// Production: deletes a waiting event. The outcome is a dispatch event
    /// of type 0 on `result` whose payload names the deleted id ([`crate::delete_outcome`]).
    async fn delete(&self, id: EventId, result: Channel, key: Option<WorkKey>) -> Result<u64, Refusal>;
    /// Waits until the cumulative `Committed` reaches `seq`.
    async fn committed(&self, seq: u64) -> Result<(), Refusal>;
    /// Work: joins `channel` (`None`: every channel) for `types` (empty: all;
    /// not empty without a channel) with this lease length; `isolated` takes
    /// isolated events only (B6). Returns the join's name, the `req` of its `Join`.
    async fn join(&self, channel: Option<Channel>, types: Vec<u16>, isolated: bool, lease: Duration, dispatch: bool) -> Result<u32, Refusal>;
    /// Work: sets the absolute window of the join named `join` (`bytes` 0: no byte bound). Not answered.
    async fn window(&self, join: u32, count: u32, bytes: u64) -> Result<(), Refusal>;
    /// Work: completes the attempt `token` on `id`, registering `follow_ups` in the same step.
    async fn complete(&self, id: EventId, token: ClaimToken, follow_ups: Vec<Envelope>) -> Result<u64, Refusal>;
    /// Work: re-arms the lease. Not answered.
    async fn extend(&self, id: EventId, token: ClaimToken) -> Result<(), Refusal>;
    /// Work: gives the event back, visible after `delay`; `failed` counts a failure. Not answered.
    async fn release(&self, id: EventId, token: ClaimToken, delay: Duration, failed: bool) -> Result<(), Refusal>;
    /// Work: claims the event with this id now.
    async fn claim(&self, id: EventId, lease: Duration) -> Result<Assignment, Refusal>;
    /// Observation: subscribes and returns one page of the channel's waiting backlog after `after`.
    async fn subscribe(&self, channel: Channel, after: Option<WorkKey>) -> Result<Backlog, Refusal>;
    async fn unsubscribe(&self, channel: Channel) -> Result<(), Refusal>;
    /// Observation, operator grant.
    async fn stats(&self) -> Result<Stats, Refusal>;
    /// Observation, operator grant: starts draining the server.
    async fn drain(&self) -> Result<(), Refusal>;
    /// The `Welcome` this connection got.
    fn welcome(&self) -> Option<Welcome>;

    /// `register`, then `committed` for it.
    async fn register_committed(&self, event: Envelope) -> Result<(u64, EventId), Refusal> {
        let (seq, id) = self.register(event).await?;
        if seq > 0 {
            self.committed(seq).await?;
        }
        Ok((seq, id))
    }
}

/// What arrives without being asked for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Incoming {
    /// Work from a window.
    Assign(Assignment),
    /// Claimable work on a subscribed channel: head and work key (the isolated bit set when it was isolated).
    Notice(Envelope),
    /// A dispatch event.
    Delivery(Envelope),
    /// The full window of this join (its name, from `join`) would receive work that waits.
    Pending(u32),
    /// This root type is paused: hold registrations of it.
    Paused(u16),
    Resumed(u16),
    Fault {
        kind: FaultKind,
        detail: &'static str,
    },
    /// A subscription to this channel took effect. Everything that arrived
    /// before this marker was sent before the subscription existed.
    Subscribed(Channel),
    /// The server ended the connection, and why.
    Bye(ByeReason),
    /// The connection ended. Nothing more will arrive.
    Closed,
}

/// The receiving end of a connection's unasked-for traffic.
pub type Inbox = mpsc::Receiver<Incoming>;

/// Carries calls to the server. In-process, or over a socket.
#[async_trait]
pub trait Link: Send + Sync + 'static {
    async fn send(&self, call: Call) -> Result<(), Closed>;
}

type Pending = Arc<Mutex<HashMap<u32, oneshot::Sender<Outbound>>>>;

/// The highest `Committed` so far, and whether the connection ended.
type Acked = (u64, bool);

/// Routes what the server sends.
///
/// `pending` is the table of calls waiting for their answer:
///
/// | key | request number |
/// | --- | --- |
/// | scope | one connection |
/// | lifetime | from the call until its answer, or until the connection ends |
/// | bound | calls in progress on this connection |
/// | cleared by | `Demux::route` (answer) and `Demux::close` (connection gone) |
#[derive(Clone)]
pub struct Demux {
    pending: Pending,
    inbox: mpsc::Sender<Incoming>,
    acked: Arc<watch::Sender<Acked>>,
    welcome: Arc<watch::Sender<Option<Result<Welcome, ByeReason>>>>,
}

impl Demux {
    /// Routes one message from the server, in the order it arrived. Answers
    /// go to their callers; everything else to the inbox. A dropped inbox
    /// discards unasked-for traffic and answers still arrive. Returns whether
    /// the connection is still worth reading.
    pub async fn route(&self, message: Outbound) -> bool {
        let req = match &message {
            Outbound::Joined { req }
            | Outbound::Refused { req, .. }
            | Outbound::Claimed { req, .. }
            | Outbound::Subscribed { req, .. }
            | Outbound::Unsubscribed { req }
            | Outbound::StatsReply { req, .. }
            | Outbound::Draining { req } => Some(*req),
            _ => None,
        };
        if let Some(req) = req {
            // The marker goes into the inbox before the caller hears back, so a
            // caller that waits for it sees everything sent before it first.
            if let Outbound::Subscribed { channel, .. } = &message {
                let _ = self.inbox.send(Incoming::Subscribed(*channel)).await;
            }
            if let Some(waiter) = self.pending.lock().remove(&req) {
                let _ = waiter.send(message);
            }
            return true;
        }
        let incoming = match message {
            Outbound::Welcome(welcome) => {
                self.welcome.send_replace(Some(Ok(welcome)));
                return true;
            }
            Outbound::Committed { seq } => {
                self.acked.send_modify(|acked| acked.0 = acked.0.max(seq));
                return true;
            }
            Outbound::Bye(reason) => {
                self.welcome.send_if_modified(|welcome| {
                    if welcome.is_none() {
                        *welcome = Some(Err(reason));
                        return true;
                    }
                    false
                });
                let _ = self.inbox.send(Incoming::Bye(reason)).await;
                return false;
            }
            Outbound::Assign(assignment) => Incoming::Assign(assignment),
            Outbound::Notice(event) => Incoming::Notice(event),
            Outbound::Delivery(event) => Incoming::Delivery(event),
            Outbound::Pending { join } => Incoming::Pending(join),
            Outbound::Paused { event_type } => Incoming::Paused(event_type),
            Outbound::Resumed { event_type } => Incoming::Resumed(event_type),
            Outbound::Fault { kind, detail } => Incoming::Fault { kind, detail },
            _ => return true,
        };
        let _ = self.inbox.send(incoming).await;
        true
    }

    /// The connection ended: every waiting call fails with `Closed`.
    pub async fn close(&self) {
        self.pending.lock().clear();
        self.acked.send_modify(|acked| acked.1 = true);
        self.welcome.send_if_modified(|welcome| {
            if welcome.is_none() {
                *welcome = Some(Err(ByeReason::Protocol));
                return true;
            }
            false
        });
        let _ = self.inbox.send(Incoming::Closed).await;
    }
}

/// The one implementation of [`Methods`]. The link decides where calls go.
#[derive(Clone)]
pub struct Client {
    link: Arc<dyn Link>,
    role: Role,
    pending: Pending,
    next: Arc<AtomicU32>,
    /// The next sequence number, held while a numbered call is handed to the
    /// link so numbers leave in order.
    sequence: Arc<tokio::sync::Mutex<u64>>,
    acked: watch::Receiver<Acked>,
    welcome: watch::Receiver<Option<Result<Welcome, ByeReason>>>,
}

/// A client of `role` over `link`, the demultiplexer its reader must feed, and the inbox.
pub fn client(link: Arc<dyn Link>, role: Role, inbox: usize) -> (Client, Demux, Inbox) {
    let pending: Pending = Arc::default();
    let (sender, receiver) = mpsc::channel(inbox.max(1));
    let (acked_tx, acked) = watch::channel((0, false));
    let (welcome_tx, welcome) = watch::channel(None);
    let client = Client { link, role, pending: pending.clone(), next: Arc::new(AtomicU32::new(1)), sequence: Arc::new(tokio::sync::Mutex::new(1)), acked, welcome };
    (client, Demux { pending, inbox: sender, acked: Arc::new(acked_tx), welcome: Arc::new(welcome_tx) }, receiver)
}

/// Why a connection was not opened.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectError {
    /// The engine is gone.
    Closed,
    /// The server answered `Hello` with `Bye`.
    Bye(ByeReason),
}

impl Client {
    pub fn role(&self) -> Role {
        self.role
    }

    /// Waits for the connection's `Welcome`, or the `Bye` that came instead.
    pub async fn welcomed(&self) -> Result<Welcome, ConnectError> {
        let mut welcome = self.welcome.clone();
        let outcome = match welcome.wait_for(Option::is_some).await {
            Ok(value) => value.clone(),
            Err(_) => None,
        };
        match outcome {
            Some(Ok(welcome)) => Ok(welcome),
            Some(Err(reason)) => Err(ConnectError::Bye(reason)),
            None => Err(ConnectError::Closed),
        }
    }

    async fn ask(&self, make: impl FnOnce(u32) -> Call) -> Result<Outbound, Refusal> {
        let req = self.next.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = oneshot::channel();
        self.pending.lock().insert(req, sender);
        if self.link.send(make(req)).await.is_err() {
            self.pending.lock().remove(&req);
            return Err(Refusal::Closed);
        }
        match receiver.await {
            Ok(Outbound::Refused { reason, .. }) => Err(reason),
            Ok(answer) => Ok(answer),
            Err(_) => Err(Refusal::Closed),
        }
    }

    async fn tell(&self, call: Call) -> Result<(), Refusal> {
        self.link.send(call).await.map_err(|_| Refusal::Closed)
    }

    /// Hands a numbered call to the link under the next sequence number.
    async fn numbered(&self, make: impl FnOnce(u64) -> Call) -> Result<u64, Refusal> {
        let mut next = self.sequence.lock().await;
        let seq = *next;
        self.link.send(make(seq)).await.map_err(|_| Refusal::Closed)?;
        *next += 1;
        Ok(seq)
    }
}

fn unexpected<T>() -> Result<T, Refusal> {
    Err(Refusal::Malformed)
}

#[async_trait]
impl Methods for Client {
    async fn register(&self, mut event: Envelope) -> Result<(u64, EventId), Refusal> {
        if event.kind == Kind::Claimable && event.id.is_nil() {
            event.id = EventId::new_v7();
        }
        let id = event.id;
        if self.role == Role::Work {
            self.tell(Call::Register { seq: None, event }).await?;
            return Ok((0, id));
        }
        let seq = self.numbered(|seq| Call::Register { seq: Some(seq), event }).await?;
        Ok((seq, id))
    }

    async fn delete(&self, id: EventId, result: Channel, key: Option<WorkKey>) -> Result<u64, Refusal> {
        self.numbered(|seq| Call::Delete { seq, id, result, key }).await
    }

    async fn committed(&self, seq: u64) -> Result<(), Refusal> {
        let mut acked = self.acked.clone();
        let reached = match acked.wait_for(|(acked, closed)| *acked >= seq || *closed).await {
            Ok(value) => value.0 >= seq,
            Err(_) => false,
        };
        if reached {
            Ok(())
        } else {
            Err(Refusal::Closed)
        }
    }

    async fn join(&self, channel: Option<Channel>, types: Vec<u16>, isolated: bool, lease: Duration, dispatch: bool) -> Result<u32, Refusal> {
        match self.ask(|req| Call::Join { req, channel, types, isolated, lease_ms: millis(lease), dispatch }).await? {
            Outbound::Joined { req } => Ok(req),
            _ => unexpected(),
        }
    }

    async fn window(&self, join: u32, count: u32, bytes: u64) -> Result<(), Refusal> {
        self.tell(Call::Window { join, count, bytes }).await
    }

    async fn complete(&self, id: EventId, token: ClaimToken, follow_ups: Vec<Envelope>) -> Result<u64, Refusal> {
        self.numbered(|seq| Call::Complete { seq, id, token, follow_ups, spooled: Vec::new() }).await
    }

    async fn extend(&self, id: EventId, token: ClaimToken) -> Result<(), Refusal> {
        self.tell(Call::Extend { id, token }).await
    }

    async fn release(&self, id: EventId, token: ClaimToken, delay: Duration, failed: bool) -> Result<(), Refusal> {
        self.tell(Call::Release { id, token, delay_ms: millis(delay), failed }).await
    }

    async fn claim(&self, id: EventId, lease: Duration) -> Result<Assignment, Refusal> {
        match self.ask(|req| Call::Claim { req, id, lease_ms: millis(lease) }).await? {
            Outbound::Claimed { assignment, .. } => Ok(assignment),
            _ => unexpected(),
        }
    }

    async fn subscribe(&self, channel: Channel, after: Option<WorkKey>) -> Result<Backlog, Refusal> {
        match self.ask(|req| Call::Subscribe { req, channel, after }).await? {
            Outbound::Subscribed { backlog, more, .. } => Ok(Backlog { notices: backlog, more }),
            _ => unexpected(),
        }
    }

    async fn unsubscribe(&self, channel: Channel) -> Result<(), Refusal> {
        match self.ask(|req| Call::Unsubscribe { req, channel }).await? {
            Outbound::Unsubscribed { .. } => Ok(()),
            _ => unexpected(),
        }
    }

    async fn stats(&self) -> Result<Stats, Refusal> {
        match self.ask(|req| Call::Stats { req }).await? {
            Outbound::StatsReply { stats, .. } => Ok(*stats),
            _ => unexpected(),
        }
    }

    async fn drain(&self) -> Result<(), Refusal> {
        match self.ask(|req| Call::Drain { req }).await? {
            Outbound::Draining { .. } => Ok(()),
            _ => unexpected(),
        }
    }

    fn welcome(&self) -> Option<Welcome> {
        self.welcome.borrow().clone().and_then(Result::ok)
    }
}

struct Local {
    session: Session,
}

#[async_trait]
impl Link for Local {
    async fn send(&self, call: Call) -> Result<(), Closed> {
        self.session.call(call).await
    }
}

/// Connects in-process: the same operations, no socket. Returns once the
/// server answered `Hello`.
pub async fn connect(engine: &Engine, hello: Hello, inbox: usize) -> Result<(Client, Inbox), ConnectError> {
    let role = hello.role;
    let (session, mut outbound) = engine.open(hello).await.map_err(|_| ConnectError::Closed)?;
    let (client, demux, inbox) = client(Arc::new(Local { session }), role, inbox);
    tokio::spawn(async move {
        while let Some(message) = outbound.recv().await {
            if !demux.route(message).await {
                break;
            }
        }
        demux.close().await;
    });
    client.welcomed().await?;
    Ok((client, inbox))
}
