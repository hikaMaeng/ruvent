//! The seven methods as one contract, [`Methods`], with one client
//! implementation, [`Client`], over two links: in-process ([`connect`]) and a
//! socket (the `ruvent-client` crate). Workers and proxies see only the
//! contract; the server does not know which link a caller used.

use crate::engine::ops::{Call, Outbound, Refusal, Reply};
use crate::engine::{Closed, Engine, Session};
use crate::envelope::{ClaimToken, Envelope, EventId, WorkKey};
use crate::names::Name;
use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

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

/// The event server's methods.
#[async_trait]
pub trait Methods: Send + Sync + 'static {
    /// 1. Registers a claimable event. Returns its id and work key.
    async fn register_claimable(&self, event: Envelope) -> Result<(EventId, WorkKey), Refusal>;
    /// 2. Registers a dispatch event: sent to the channel's subscribers now, kept nowhere.
    async fn register_dispatch(&self, event: Envelope) -> Result<EventId, Refusal>;
    /// 3. Claims the event that has this id now, for `lease`. Returns the whole envelope.
    async fn claim(&self, id: EventId, worker: u32, lease: Duration) -> Result<Envelope, Refusal>;
    /// 4. Reports completion — success and failure alike — and registers `follow_ups` in the same step.
    async fn complete(&self, id: EventId, token: ClaimToken, follow_ups: Vec<Envelope>) -> Result<(), Refusal>;
    /// 5. Deletes a waiting event. The outcome is also announced on `result` with correlation `tag`.
    async fn delete(&self, id: EventId, result: Name, tag: EventId, key: Option<WorkKey>) -> Result<bool, Refusal>;
    /// 6. Subscribes to a channel and returns one page of its waiting backlog after `after`.
    async fn subscribe(&self, channel: Name, after: Option<WorkKey>) -> Result<Backlog, Refusal>;
    /// 7. Unsubscribes.
    async fn unsubscribe(&self, channel: Name) -> Result<(), Refusal>;
}

/// What arrives without being asked for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Incoming {
    /// Claimable work on a subscribed channel: head and work key.
    Notice(Envelope),
    /// A dispatch event.
    Delivery(Envelope),
    /// A subscription to this channel took effect. Everything that arrived
    /// before this marker was sent before the subscription existed.
    Subscribed(Name),
    /// The connection ended. Nothing more will arrive.
    Closed,
}

/// The receiving end of a connection's unasked-for traffic.
pub type Inbox = mpsc::Receiver<Incoming>;

/// Carries calls to the server. In-process, or over a socket.
#[async_trait]
pub trait Link: Send + Sync + 'static {
    async fn send(&self, req: u32, call: Call) -> Result<(), Closed>;
}

type Pending = Arc<Mutex<HashMap<u32, oneshot::Sender<Reply>>>>;

/// Calls waiting for their answer.
///
/// | key | request number |
/// | --- | --- |
/// | scope | one connection |
/// | lifetime | from the call until its reply, or until the connection ends |
/// | bound | calls in progress on this connection |
/// | cleared by | `Demux::route` (reply) and `Demux::close` (connection gone) |
#[derive(Clone)]
pub struct Demux {
    pending: Pending,
    inbox: mpsc::Sender<Incoming>,
}

impl Demux {
    /// Routes one message from the server, in the order it arrived. Answers
    /// go to their callers; everything else to the inbox. A dropped inbox
    /// discards unasked-for traffic and answers still arrive. Always true:
    /// the connection stays worth reading until the server ends it.
    pub async fn route(&self, message: Outbound) -> bool {
        match message {
            Outbound::Reply { req, reply } => {
                // The marker goes into the inbox before the caller hears back, so a
                // caller that waits for it sees everything sent before it first.
                if let Reply::Subscribed { channel, .. } = &reply {
                    let _ = self.inbox.send(Incoming::Subscribed(channel.clone())).await;
                }
                if let Some(waiter) = self.pending.lock().remove(&req) {
                    let _ = waiter.send(reply);
                }
            }
            Outbound::Notice(event) => {
                let _ = self.inbox.send(Incoming::Notice(event)).await;
            }
            Outbound::Delivery(event) => {
                let _ = self.inbox.send(Incoming::Delivery(event)).await;
            }
        }
        true
    }

    /// The connection ended: every waiting call fails with `Closed`.
    pub async fn close(&self) {
        self.pending.lock().clear();
        let _ = self.inbox.send(Incoming::Closed).await;
    }
}

/// The one implementation of [`Methods`]. The link decides where calls go.
#[derive(Clone)]
pub struct Client {
    link: Arc<dyn Link>,
    pending: Pending,
    next: Arc<AtomicU32>,
}

/// A client over `link`, the demultiplexer its reader must feed, and the inbox.
pub fn client(link: Arc<dyn Link>, inbox: usize) -> (Client, Demux, Inbox) {
    let pending: Pending = Arc::default();
    let (sender, receiver) = mpsc::channel(inbox.max(1));
    (Client { link, pending: pending.clone(), next: Arc::new(AtomicU32::new(1)) }, Demux { pending, inbox: sender }, receiver)
}

impl Client {
    async fn call(&self, call: Call) -> Result<Reply, Refusal> {
        let req = self.next.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = oneshot::channel();
        self.pending.lock().insert(req, sender);
        if self.link.send(req, call).await.is_err() {
            self.pending.lock().remove(&req);
            return Err(Refusal::Closed);
        }
        match receiver.await {
            Ok(Reply::Refused(refusal)) => Err(refusal),
            Ok(reply) => Ok(reply),
            Err(_) => Err(Refusal::Closed),
        }
    }
}

fn unexpected<T>() -> Result<T, Refusal> {
    Err(Refusal::Malformed)
}

#[async_trait]
impl Methods for Client {
    async fn register_claimable(&self, event: Envelope) -> Result<(EventId, WorkKey), Refusal> {
        match self.call(Call::RegisterClaimable(event)).await? {
            Reply::Registered { id, key } => Ok((id, key)),
            _ => unexpected(),
        }
    }

    async fn register_dispatch(&self, event: Envelope) -> Result<EventId, Refusal> {
        match self.call(Call::RegisterDispatch(event)).await? {
            Reply::Accepted { id } => Ok(id),
            _ => unexpected(),
        }
    }

    async fn claim(&self, id: EventId, worker: u32, lease: Duration) -> Result<Envelope, Refusal> {
        let lease_ms = u64::try_from(lease.as_millis()).unwrap_or(u64::MAX);
        match self.call(Call::Claim { id, worker, lease_ms }).await? {
            Reply::Claimed(event) => Ok(event),
            _ => unexpected(),
        }
    }

    async fn complete(&self, id: EventId, token: ClaimToken, follow_ups: Vec<Envelope>) -> Result<(), Refusal> {
        match self.call(Call::Complete { id, token, follow_ups }).await? {
            Reply::Completed => Ok(()),
            _ => unexpected(),
        }
    }

    async fn delete(&self, id: EventId, result: Name, tag: EventId, key: Option<WorkKey>) -> Result<bool, Refusal> {
        match self.call(Call::Delete { id, result, tag, key }).await? {
            Reply::Deleted { removed } => Ok(removed),
            _ => unexpected(),
        }
    }

    async fn subscribe(&self, channel: Name, after: Option<WorkKey>) -> Result<Backlog, Refusal> {
        match self.call(Call::Subscribe { channel, after }).await? {
            Reply::Subscribed { backlog, more, .. } => Ok(Backlog { notices: backlog, more }),
            _ => unexpected(),
        }
    }

    async fn unsubscribe(&self, channel: Name) -> Result<(), Refusal> {
        match self.call(Call::Unsubscribe { channel }).await? {
            Reply::Unsubscribed => Ok(()),
            _ => unexpected(),
        }
    }
}

struct Local {
    session: Session,
}

#[async_trait]
impl Link for Local {
    async fn send(&self, req: u32, call: Call) -> Result<(), Closed> {
        self.session.call(req, call).await
    }
}

/// Connects in-process under `credential`: the same methods, no socket.
pub async fn connect(engine: &Engine, credential: &str, inbox: usize) -> Result<(Client, Inbox), Closed> {
    let (session, mut outbound) = engine.open(credential).await?;
    let (client, demux, inbox) = client(Arc::new(Local { session }), inbox);
    tokio::spawn(async move {
        while let Some(message) = outbound.recv().await {
            if !demux.route(message).await {
                break;
            }
        }
        demux.close().await;
    });
    Ok((client, inbox))
}
