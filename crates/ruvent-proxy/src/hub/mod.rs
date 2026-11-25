//! The hub: one task that owns the observation side and the users, and the
//! handles that talk to it.
//!
//! The event servers see, from each proxy, one observation connection each
//! and one subscription per target however many users want it (4장). The
//! hub fans each delivery out to the users whose subscription is in effect
//! on the server it came from at the moment it handles that delivery.
//!
//! | file | role |
//! | --- | --- |
//! | `mod.rs` | [`Proxy`]: start, readiness, users, authentication. What callers hold |
//! | `calls.rs` | registrations ([`Proxy::publish`]) and request–response ([`Proxy::exchange`]): the producer and requester, on the caller's task |
//! | `user.rs` | [`UserHandle`]: one attached user's subscriptions and requests |
//! | `channels/` | the subscription book: pure state, no task, no clock. When a subscription takes effect is decided there |
//! | `task.rs` | the hub task: carries out the book's steps, fans deliveries out, reports health |
//!
//! **When a subscription takes effect.** A user becomes active on a server
//! only when the hub reads that server's confirmation of the subscribe its
//! own join issued (`channels/`, and the observer's tickets). Everything the
//! stream held before that confirmation was sent before the subscription
//! existed for this user and reaches only users already active. So a user
//! never receives an event sent before its subscription took effect — in
//! particular after `0 → 1 → 0 → 1`, nothing of the first subscription leaks
//! into the second. Hooks get no way to subscribe on the hub's observer
//! ([`crate::hooks::UpstreamCalls`]), so every confirmation it reads answers
//! a call it made.
//!
//! **The connections.** Observation goes through the SDK observer, owned by
//! the hub task. Production and request–response go through the SDK
//! requester and its producer, shared by every handle and used on the
//! caller's task; the hub task drops them when it ends. Reconnection,
//! keepalive and the `Bye` rules are the SDK's.
//!
//! **Hooks.** Authentication, subscribe and publish decisions run on the
//! caller's task before anything reaches the hub; only [`crate::Hooks::deliver`]
//! runs inside it, once per event and user.

mod calls;
mod channels;
mod task;
mod user;

pub use channels::UserId;
pub use user::UserHandle;

use crate::auth::{self, AuthAnswer, AuthRequest, Denied, User};
use crate::config::ProxyConfig;
use crate::handshake::Handshake;
use crate::health::Health;
use crate::hooks::{Context, Defaults, Hooks};
use crate::message::{Outcome, Outgoing, Rejection};
use crate::upstream::Upstream;
use parking_lot::RwLock;
use ruvent::{Bytes, Envelope};
use ruvent_client::observer::{Observer, ObserverConfig};
use ruvent_client::producer::ProducerConfig;
use ruvent_client::requester::{RequestError, RequestOptions, Requester, RequesterConfig};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use task::{Command, Hub, Sink, COMMANDS};
use tokio::sync::{mpsc, oneshot, watch};

/// What the handles and the hub task share. The hub task holds it too, but
/// not the command sender, so the task ends once every handle is gone.
struct Core {
    config: ProxyConfig,
    hooks: Arc<dyn Hooks>,
    /// The requester and its producer.
    ///
    /// | key | one per proxy |
    /// | --- | --- |
    /// | lifetime | start ~ the hub task's end |
    /// | bound | one |
    /// | cleared by | the hub task's [`Drop`] |
    requester: RwLock<Option<Requester>>,
    health: watch::Receiver<Health>,
}

/// A handle to a running proxy hub. Cheap to clone. The hub stops when
/// [`Proxy::stop`] is called or when every handle (users' included) is gone.
#[derive(Clone)]
pub struct Proxy {
    commands: mpsc::Sender<Command>,
    core: Arc<Core>,
}

impl Proxy {
    /// Starts the hub with the configured behaviour ([`Defaults`]).
    pub fn start(config: ProxyConfig, upstream: Upstream) -> Proxy {
        Proxy::start_with(config, upstream, Arc::new(Defaults))
    }

    /// Starts the hub and connects to every server of `upstream` in the
    /// background (the SDK reconnects with back-off).
    pub fn start_with(config: ProxyConfig, upstream: Upstream, hooks: Arc<dyn Hooks>) -> Proxy {
        let reconnect = config.reconnect.backoff();
        let credential = upstream.credential.clone();
        let observer_config = ObserverConfig { credential: credential.clone(), reconnect, ..ObserverConfig::default() };
        let (observer, stream) = Observer::start(upstream.connector.clone(), upstream.servers.as_ref(), observer_config);
        let producer = ProducerConfig { credential: credential.clone(), reconnect, ..ProducerConfig::default() };
        let requester_config = RequesterConfig { credential, reconnect, lease: Duration::from_millis(config.exchange.lease_ms), producer, ..RequesterConfig::default() };
        let requester = Requester::start(upstream.connector.clone(), upstream.servers.as_ref(), requester_config);
        let joined = requester.watch_joined();
        let (health_tx, health) = watch::channel(Health::default());
        let (commands, receiver) = mpsc::channel(COMMANDS);
        let core = Arc::new(Core { config, hooks, requester: RwLock::new(Some(requester)), health });
        let hub = Hub::new(core.clone(), receiver, observer, stream, upstream.servers.watch(), joined, health_tx);
        tokio::spawn(hub.run());
        Proxy { commands, core }
    }

    pub fn config(&self) -> &ProxyConfig {
        &self.core.config
    }

    /// Waits until every listed server's connections are up ([`Health`]).
    /// `false` when the hub stopped instead.
    pub async fn ready(&self) -> bool {
        let mut health = self.core.health.clone();
        let ready = health.wait_for(|health| health.ready).await.is_ok();
        ready
    }

    /// Whether every listed server's connections are up right now. `false` once the hub stopped.
    pub fn is_ready(&self) -> bool {
        self.core.health.borrow().ready
    }

    /// The upstream as `/health` shows it, now.
    pub fn health(&self) -> Health {
        self.core.health.borrow().clone()
    }

    /// Stops the hub: every user's stream ends and every upstream connection
    /// is dropped. Later calls on any handle answer "proxy stopped".
    pub async fn stop(&self) {
        let _ = self.commands.send(Command::Stop).await;
    }

    /// The requester (and its producer) while the hub runs.
    pub(crate) fn requester(&self) -> Result<Requester, Rejection> {
        self.core.requester.read().clone().ok_or_else(|| Rejection::unavailable("proxy stopped"))
    }

    fn context(&self) -> Context<'_> {
        Context::new(self)
    }

    /// Who the user of this handshake is: [`Hooks::authenticate`].
    pub async fn authenticate(&self, handshake: &Handshake) -> Result<Arc<User>, Denied> {
        self.core.hooks.authenticate(self.context(), handshake).await.map(Arc::new)
    }

    /// Authenticates a token according to `auth.mode`: the default hook. In
    /// `worker` mode this is one request–response, its deadline `timeout_ms`.
    pub async fn authenticate_by_config(&self, token: Option<&str>) -> Result<User, Denied> {
        let config = &self.core.config;
        if let Some(result) = auth::local(config, token) {
            return result;
        }
        let Some(worker) = config.auth.worker.clone() else { return Err(Denied::Unavailable("auth.worker missing".into())) };
        let Some(token) = token else { return Err(Denied::Missing) };
        let request = serde_json::to_vec(&AuthRequest { token: token.to_owned() }).map_err(|error| Denied::Unavailable(error.to_string()))?;
        let event = Envelope::claimable(worker.channel.as_str(), worker.event_type.as_str(), Bytes::from(request));
        let options = RequestOptions { deadline: Duration::from_millis(worker.timeout_ms), retries: 0 };
        match self.exchange_with(event, options).await {
            Ok(response) => {
                let answer = serde_json::from_slice(&response.result.payload).unwrap_or(AuthAnswer { ok: false, user: None, channels: Vec::new() });
                auth::from_answer(config, answer)
            }
            Err(RequestError::Timeout) => Err(Denied::Timeout),
            Err(error) => Err(Denied::Unavailable(error.to_string())),
        }
    }

    /// Attaches a user whose stream is JSON text: events and results, in
    /// order (the WebSocket and HTTP protocols). It ends when the user is
    /// disconnected (buffer full, a hook failure, detached, or the hub stopped).
    pub async fn attach(&self, user: Arc<User>) -> Result<(UserHandle, mpsc::Receiver<Arc<str>>), String> {
        let (out, events) = mpsc::channel(self.core.config.user_buffer.max(1));
        let handle = self.attach_sink(user, Sink::Text(out)).await?;
        Ok((handle, events))
    }

    /// Attaches a user whose stream carries events as the event server sent
    /// them ([`Outgoing::Event`], shared, the payload never turned into text)
    /// and results as text. For a protocol of your own.
    pub async fn attach_raw(&self, user: Arc<User>) -> Result<(UserHandle, mpsc::Receiver<Outgoing>), String> {
        let (out, events) = mpsc::channel(self.core.config.user_buffer.max(1));
        let handle = self.attach_sink(user, Sink::Raw(out)).await?;
        Ok((handle, events))
    }

    async fn attach_sink(&self, user: Arc<User>, out: Sink) -> Result<UserHandle, String> {
        let (reply, id) = oneshot::channel();
        self.commands.send(Command::Attach { user: user.clone(), out, reply }).await.map_err(|_| "proxy stopped".to_owned())?;
        let id = id.await.map_err(|_| "proxy stopped".to_owned())?;
        Ok(UserHandle::new(id, user, self.clone()))
    }
}

/// A user message that is not JSON of the right shape.
pub fn bad_request(error: impl std::fmt::Display) -> Outcome {
    Outcome::failed(None::<Value>, format!("bad request: {error}"))
}
