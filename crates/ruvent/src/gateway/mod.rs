//! The client-facing half, with no protocol in it.
//!
//! A gateway knows three things: who may connect, which commands this
//! deployment accepts, and how to read the log on someone's behalf. It does
//! not know how bytes reach it. A transport — WebSocket, long polling, WebRTC,
//! gRPC — is a plugin that turns its own wire into calls on a gateway.
//!
//! Two calling styles cover every transport:
//!
//! * **streaming** ([`Gateway::serve`]) for a transport with a connection;
//! * **request/response** ([`Gateway::submit`], [`Gateway::poll`]) for one without.
//!
//! Both are possible without server-side session state because a client's
//! position is a number the client holds.

mod policy;

use crate::engine::{Closed, Engine, From, Subscription};
use crate::envelope::{Command, Envelope, EventId, Text};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub use policy::{Anonymous, Open, OwnedSessions};

const MAX_CHANNELS: usize = 32;
const MAX_CHANNEL_NAME: usize = 128;
const STREAM_BATCH: usize = 64;

/// A verified caller. The connection is the only authority on who this is.
#[derive(Clone, Debug)]
pub struct Identity {
    pub user: String,
}

/// What a transport could extract from a handshake or a request.
#[derive(Clone, Debug, Default)]
pub struct Credentials {
    pub token: Option<String>,
    pub peer: Option<SocketAddr>,
}

#[async_trait]
pub trait Authenticator: Send + Sync + 'static {
    /// Returns who this is, or `None` to refuse them.
    async fn authenticate(&self, credentials: &Credentials) -> Option<Identity>;
}

#[async_trait]
impl<F> Authenticator for F
where
    F: Fn(&Credentials) -> Option<Identity> + Send + Sync + 'static,
{
    async fn authenticate(&self, credentials: &Credentials) -> Option<Identity> {
        self(credentials)
    }
}

/// The gateway's judgement calls, made from the envelope alone.
///
/// Which actions exist, and which queue each goes on, is not decided here:
/// that is the route table, built from what the application registered.
pub trait Policy: Send + Sync + 'static {
    /// Rewrites or rejects a client's command.
    fn admit(&self, _identity: &Identity, command: Command) -> Option<Command> {
        Some(command)
    }

    /// Whether this caller may read a channel.
    fn may_read(&self, _identity: &Identity, _channel: &str) -> bool {
        true
    }
}

/// What a client proposes. Identity and placement are decided by the gateway.
#[derive(Clone, Debug)]
pub struct Proposal {
    pub action: Text,
    pub payload: Bytes,
    pub transaction_key: Option<Text>,
    pub channel: Option<Text>,
    pub reply_channel: Option<Text>,
    pub session_id: Option<Text>,
    pub run_id: Option<Text>,
    pub turn_id: Option<Text>,
}

impl Default for Proposal {
    fn default() -> Self {
        Self { action: Text::from(""), payload: Bytes::new(), transaction_key: None, channel: None, reply_channel: None, session_id: None, run_id: None, turn_id: None }
    }
}

#[derive(Clone, Debug)]
pub enum ClientFrame {
    Subscribe { channels: Vec<String>, from: From },
    Event(Proposal),
}

#[derive(Clone, Debug)]
pub enum ServerFrame {
    Ready,
    /// `cursor` is where reading starts; each event's `position` is the next cursor.
    Subscribed {
        channels: Vec<String>,
        cursor: u64,
    },
    Event(Arc<Envelope>),
    Accepted {
        event_id: EventId,
        transaction_key: Text,
    },
    Rejected {
        reason: Rejected,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rejected {
    /// The policy refused the command.
    NotAdmitted,
    /// Too many channels, an over-long name, or none at all.
    InvalidSubscription,
    /// The caller may not read one of the channels.
    Forbidden,
    /// The engine has stopped.
    Unavailable,
}

impl Rejected {
    pub fn as_str(self) -> &'static str {
        match self {
            Rejected::NotAdmitted => "not_admitted",
            Rejected::InvalidSubscription => "invalid_subscription",
            Rejected::Forbidden => "forbidden",
            Rejected::Unavailable => "unavailable",
        }
    }
}

/// One client connection, as a transport presents it.
#[async_trait]
pub trait Connection: Send {
    /// The next frame, or `None` once the peer is gone. Must be cancel-safe:
    /// the gateway drops this future whenever there are events to send.
    async fn recv(&mut self) -> Option<ClientFrame>;
    async fn send(&mut self, frame: ServerFrame) -> io::Result<()>;
}

/// A protocol plugin. It accepts peers on its own wire and drives the gateway
/// with what they say.
#[async_trait]
pub trait Transport: Send + 'static {
    async fn run(self: Box<Self>, gateway: Gateway, shutdown: CancellationToken) -> io::Result<()>;
}

#[derive(Clone)]
pub struct Gateway {
    engine: Engine,
    authenticator: Arc<dyn Authenticator>,
    policy: Arc<dyn Policy>,
    /// action → queue, for every action a client may submit. Being absent
    /// from this table is what "not allowed" means.
    routes: Arc<HashMap<Text, Text>>,
}

impl Gateway {
    pub fn new(engine: Engine, authenticator: Arc<dyn Authenticator>, policy: Arc<dyn Policy>, routes: HashMap<String, String>) -> Self {
        let routes = routes.into_iter().map(|(action, queue)| (Text::from(action), Text::from(queue))).collect();
        Self { engine, authenticator, policy, routes: Arc::new(routes) }
    }

    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    pub async fn authenticate(&self, credentials: &Credentials) -> Option<Identity> {
        self.authenticator.authenticate(credentials).await
    }

    /// Runs every transport until `shutdown` fires, returning the first error.
    pub async fn launch(&self, transports: Vec<Box<dyn Transport>>, shutdown: CancellationToken) -> io::Result<()> {
        let mut running = tokio::task::JoinSet::new();
        for transport in transports {
            running.spawn(transport.run(self.clone(), shutdown.clone()));
        }
        let mut outcome = Ok(());
        while let Some(joined) = running.join_next().await {
            let result = joined.unwrap_or_else(|error| Err(io::Error::other(error)));
            if result.is_err() && outcome.is_ok() {
                outcome = result;
                shutdown.cancel();
            }
        }
        outcome
    }

    /// Accepts one command. Returns once it is durably on its queue.
    pub async fn submit(&self, identity: &Identity, proposal: Proposal) -> Result<(EventId, Text), Rejected> {
        let mut command = Command::new(proposal.action, proposal.channel.unwrap_or_else(|| Text::from("")), proposal.payload);
        if let Some(key) = proposal.transaction_key.filter(|key| !key.is_empty()) {
            command.transaction_key = key;
        }
        command.reply_channel = proposal.reply_channel;
        command.session_id = proposal.session_id;
        command.run_id = proposal.run_id;
        command.turn_id = proposal.turn_id;
        // Stamped here and nowhere else. Whatever the client claimed is gone.
        command.principal = Some(identity.user.as_str().into());

        let queue = self.routes.get(&command.action).ok_or(Rejected::NotAdmitted)?.clone();
        let mut command = self.policy.admit(identity, command).filter(|command| !command.transaction_key.is_empty()).ok_or(Rejected::NotAdmitted)?;
        if command.channel.is_empty() {
            command.channel = command.reply_channel.clone().unwrap_or_else(|| Text::from("events"));
        }
        let accepted = (command.event_id, command.transaction_key.clone());
        self.engine.submit(queue, command).await.map_err(|_: Closed| Rejected::Unavailable)?;
        Ok(accepted)
    }

    fn open(&self, identity: &Identity, channels: Vec<String>, from: From) -> Result<Subscription, Rejected> {
        if channels.is_empty() || channels.len() > MAX_CHANNELS || channels.iter().any(|channel| channel.is_empty() || channel.len() > MAX_CHANNEL_NAME) {
            return Err(Rejected::InvalidSubscription);
        }
        if channels.iter().any(|channel| !self.policy.may_read(identity, channel)) {
            return Err(Rejected::Forbidden);
        }
        Ok(self.engine.subscribe(channels, from))
    }

    /// One read for a transport with no connection: waits up to `wait` for
    /// events after `from` and returns them with the cursor to ask from next.
    pub async fn poll(&self, identity: &Identity, channels: Vec<String>, from: From, max: usize, wait: Duration) -> Result<(Vec<Arc<Envelope>>, u64), Rejected> {
        let mut subscription = self.open(identity, channels, from)?;
        let events = tokio::time::timeout(wait, subscription.next(max.max(1))).await.unwrap_or_default();
        Ok((events, subscription.cursor()))
    }

    /// Drives one connection until the peer leaves.
    pub async fn serve<C: Connection>(&self, mut connection: C, identity: Identity) {
        if connection.send(ServerFrame::Ready).await.is_err() {
            return;
        }
        let mut subscription: Option<Subscription> = None;
        loop {
            tokio::select! {
                frame = connection.recv() => {
                    let reply = match frame {
                        None => return,
                        Some(ClientFrame::Subscribe { channels, from }) => match self.open(&identity, channels.clone(), from) {
                            Ok(opened) => {
                                let cursor = opened.cursor();
                                subscription = Some(opened);
                                ServerFrame::Subscribed { channels, cursor }
                            }
                            Err(reason) => ServerFrame::Rejected { reason },
                        },
                        Some(ClientFrame::Event(proposal)) => match self.submit(&identity, proposal).await {
                            Ok((event_id, transaction_key)) => ServerFrame::Accepted { event_id, transaction_key },
                            Err(reason) => ServerFrame::Rejected { reason },
                        },
                    };
                    if connection.send(reply).await.is_err() {
                        return;
                    }
                }
                events = next(&mut subscription) => {
                    // A slow peer slows only this loop. The log is the buffer;
                    // nothing queues up on its behalf anywhere else.
                    for event in events {
                        if connection.send(ServerFrame::Event(event)).await.is_err() {
                            return;
                        }
                    }
                }
            }
        }
    }
}

async fn next(subscription: &mut Option<Subscription>) -> Vec<Arc<Envelope>> {
    match subscription {
        Some(subscription) => subscription.next(STREAM_BATCH).await,
        None => std::future::pending().await,
    }
}
