//! The hub: one task that owns the upstream subscriptions and the users,
//! and the handles that talk to it.
//!
//! The event server sees one connection per proxy, and one subscription per
//! channel however many users want it. The hub fans each delivery out to the
//! users subscribed at the moment it handles that delivery.
//!
//! | file | role |
//! | --- | --- |
//! | `mod.rs` | [`Proxy`] and [`UserHandle`]: what callers hold. Authentication, publish and subscribe decisions run here, on the caller's task |
//! | `channels/` | the subscription book: pure state, no task, no clock. When a subscription takes effect is decided there |
//! | `task.rs` | the hub task: carries out the book's steps, fans deliveries out, reconnects |
//!
//! **When a subscription takes effect.** A user becomes active only when the
//! hub reads the [`ruvent::Incoming::Subscribed`] marker of the subscribe its
//! own join issued. Everything the inbox held before that marker was sent
//! before the subscription existed and reaches only users already active. So
//! a user never receives an event sent before its subscription took effect —
//! in particular after `0 → 1 → 0 → 1`, nothing of the first subscription
//! leaks into the second. Hooks get no way to subscribe on the hub's
//! connection ([`crate::hooks::UpstreamCalls`]), so every marker it reads is
//! one it asked for.
//!
//! **Hooks.** Authentication, subscribe and publish decisions run on the
//! caller's task before anything reaches the hub; only [`crate::Hooks::deliver`]
//! runs inside it, once per event and user.

mod channels;
mod task;

pub use channels::UserId;

use crate::auth::{self, AuthRequest, Denied, User};
use crate::config::ProxyConfig;
use crate::handshake::Handshake;
use crate::hooks::{Context, Defaults, Hooks};
use crate::message::{Outcome, Publish, PublishKind, Rejection, Request};
use crate::upstream::Connector;
use parking_lot::RwLock;
use ruvent::{Bytes, Envelope, EventId, Methods, Name};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use task::{Command, Hub, COMMANDS};
use tokio::sync::{mpsc, oneshot, watch};

/// What the handles and the hub task share. The hub task holds it too, but
/// not the command sender, so the task ends once every handle is gone.
struct Core {
    config: ProxyConfig,
    hooks: Arc<dyn Hooks>,
    upstream: RwLock<Option<Arc<dyn Methods>>>,
    ready: watch::Receiver<bool>,
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
    pub fn start(config: ProxyConfig, connector: Arc<dyn Connector>) -> Proxy {
        Proxy::start_with(config, connector, Arc::new(Defaults))
    }

    /// Starts the hub and connects upstream in the background (retrying with backoff).
    pub fn start_with(config: ProxyConfig, connector: Arc<dyn Connector>, hooks: Arc<dyn Hooks>) -> Proxy {
        let (commands, receiver) = mpsc::channel(COMMANDS);
        let (ready_tx, ready) = watch::channel(false);
        let core = Arc::new(Core { config, hooks, upstream: RwLock::new(None), ready });
        let hub = Hub::new(core.clone(), receiver, commands.downgrade(), connector, ready_tx);
        tokio::spawn(hub.run());
        Proxy { commands, core }
    }

    pub fn config(&self) -> &ProxyConfig {
        &self.core.config
    }

    /// Waits until the upstream connection is up (and the auth reply channel
    /// is subscribed). `false` when the hub stopped instead.
    pub async fn ready(&self) -> bool {
        let mut ready = self.core.ready.clone();
        let up = ready.wait_for(|up| *up).await.is_ok();
        up
    }

    /// Whether the upstream connection is up right now. `false` once the hub stopped.
    pub fn is_ready(&self) -> bool {
        *self.core.ready.borrow()
    }

    /// Stops the hub: every user's stream ends and the upstream connection is
    /// dropped. Later calls on any handle answer "proxy stopped".
    pub async fn stop(&self) {
        let _ = self.commands.send(Command::Stop).await;
    }

    pub(crate) fn upstream(&self) -> Result<Arc<dyn Methods>, Rejection> {
        self.core.upstream.read().clone().ok_or_else(|| Rejection::unavailable("upstream unavailable"))
    }

    fn context(&self) -> Context<'_> {
        Context::new(self)
    }

    /// Who the user of this handshake is: [`Hooks::authenticate`].
    pub async fn authenticate(&self, handshake: &Handshake) -> Result<Arc<User>, Denied> {
        self.core.hooks.authenticate(self.context(), handshake).await.map(Arc::new)
    }

    /// Authenticates a token according to `auth.mode`: the default hook.
    pub async fn authenticate_by_config(&self, token: Option<&str>) -> Result<User, Denied> {
        let config = &self.core.config;
        if let Some(result) = auth::local(config, token) {
            return result;
        }
        let Some(worker) = config.auth.worker.clone() else { return Err(Denied::Unavailable("auth.worker missing".into())) };
        let Some(token) = token else { return Err(Denied::Missing) };
        let stopped = || Denied::Unavailable("proxy stopped".into());
        let correlation = EventId::new_v7();
        let (reply, answer) = oneshot::channel();
        self.commands.send(Command::AuthWait { correlation, reply }).await.map_err(|_| stopped())?;
        let request = serde_json::to_vec(&AuthRequest { token: token.to_owned() }).map_err(|error| Denied::Unavailable(error.to_string()))?;
        let event = Envelope::claimable(worker.channel.as_str(), worker.event_type.as_str(), Bytes::from(request)).with_correlation(correlation).with_reply(worker.reply_channel.as_str());
        let registered = match self.upstream() {
            Ok(upstream) => upstream.register_claimable(event).await.map_err(|refusal| Denied::Unavailable(refusal.to_string())),
            Err(rejection) => Err(Denied::Unavailable(rejection.message)),
        };
        if let Err(denied) = registered {
            let _ = self.commands.send(Command::AuthCancel { correlation }).await;
            return Err(denied);
        }
        // The answer's deadline: one of the two timers this crate declares.
        match tokio::time::timeout(Duration::from_millis(worker.timeout_ms), answer).await {
            Ok(Ok(answer)) => auth::from_answer(config, answer),
            Ok(Err(_)) => Err(stopped()),
            Err(_) => {
                let _ = self.commands.send(Command::AuthCancel { correlation }).await;
                Err(Denied::Timeout)
            }
        }
    }

    /// Attaches a user. Its stream yields JSON texts — events and results, in
    /// order — and ends when the user is disconnected (buffer full, a hook
    /// failure, detached, or the hub stopped).
    pub async fn attach(&self, user: Arc<User>) -> Result<(UserHandle, mpsc::Receiver<Arc<str>>), String> {
        let (out, events) = mpsc::channel(self.core.config.user_buffer.max(1));
        let (reply, id) = oneshot::channel();
        self.commands.send(Command::Attach { user: user.clone(), out, reply }).await.map_err(|_| "proxy stopped".to_owned())?;
        let id = id.await.map_err(|_| "proxy stopped".to_owned())?;
        Ok((UserHandle { id, user, proxy: self.clone() }, events))
    }

    /// Registers what a user asked for.
    ///
    /// Order: the request's own shape ([`Publish::check`], invalid → 400),
    /// then [`Hooks::publish`] (which may change it), then the registration
    /// as the hook left it.
    pub async fn publish(&self, user: &User, mut publish: Publish) -> Outcome {
        let reference = publish.reference.clone();
        match self.try_publish(user, &mut publish).await {
            Ok(id) => Outcome::ok(reference, Some(id.to_string())),
            Err(rejection) => Outcome::rejected(reference, rejection),
        }
    }

    async fn try_publish(&self, user: &User, publish: &mut Publish) -> Result<EventId, Rejection> {
        publish.check()?;
        self.core.hooks.publish(self.context(), user, publish).await?;
        let delegate = self.core.config.delegate.then_some(user.name.as_str());
        let event = publish.envelope(delegate)?;
        let upstream = self.upstream()?;
        match publish.kind {
            PublishKind::Claimable => upstream.register_claimable(event).await.map(|(id, _)| id),
            PublishKind::Dispatch => upstream.register_dispatch(event).await,
        }
        .map_err(Rejection::from_refusal)
    }
}

/// One attached user. Dropping it detaches the user.
pub struct UserHandle {
    pub id: UserId,
    pub user: Arc<User>,
    proxy: Proxy,
}

impl UserHandle {
    /// Subscribes once [`Hooks::subscribe`] allows it, and returns once the subscription has taken effect.
    pub async fn subscribe(&self, channel: &str) -> Result<(), Rejection> {
        self.proxy.core.hooks.subscribe(self.proxy.context(), &self.user, channel).await?;
        let stopped = || Rejection::unavailable("proxy stopped");
        let (reply, done) = oneshot::channel();
        self.proxy.commands.send(Command::Join { user: self.id, channel: Name::new(channel), reply }).await.map_err(|_| stopped())?;
        done.await.map_err(|_| stopped())?
    }

    pub async fn unsubscribe(&self, channel: &str) {
        let _ = self.proxy.commands.send(Command::Leave { user: self.id, channel: Name::new(channel) }).await;
    }

    /// Puts a result into this user's stream, behind what is already there.
    pub async fn reply(&self, outcome: Outcome) {
        let _ = self.proxy.commands.send(Command::Send { user: self.id, text: outcome.text() }).await;
    }

    /// Handles one user message and puts its result into the stream.
    pub async fn handle(&self, request: Request) {
        let outcome = match request {
            Request::Auth { .. } => Outcome::failed(None, "already authenticated"),
            Request::Publish(publish) => self.proxy.publish(&self.user, publish).await,
            Request::Subscribe { channel, reference } => match self.subscribe(&channel).await {
                Ok(()) => Outcome::ok(reference, None),
                Err(rejection) => Outcome::rejected(reference, rejection),
            },
            Request::Unsubscribe { channel, reference } => {
                self.unsubscribe(&channel).await;
                Outcome::ok(reference, None)
            }
        };
        self.reply(outcome).await;
    }
}

impl Drop for UserHandle {
    fn drop(&mut self) {
        // If the queue is full the hub still drops this user at the next
        // delivery, which finds the stream closed.
        let _ = self.proxy.commands.try_send(Command::Detach { user: self.id });
    }
}

/// A user message that is not JSON of the right shape.
pub fn bad_request(error: impl std::fmt::Display) -> Outcome {
    Outcome::failed(None::<Value>, format!("bad request: {error}"))
}
