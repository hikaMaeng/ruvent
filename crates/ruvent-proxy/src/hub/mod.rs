//! The hub: one task that owns the upstream subscriptions and the users.
//!
//! The event server sees one connection per proxy, and one subscription per
//! channel however many users want it. The hub fans each delivery out to the
//! users subscribed at the moment it handles that delivery.
//!
//! ```text
//! users ──Command──►┐
//! upstream worker ──Refused──► hub (single task) ──try_send──► user streams (full = disconnect)
//! upstream inbox ──Incoming──►┘         │
//!                                       └─ Upstream calls, in order ──► upstream worker ──► server
//! ```
//!
//! **When a subscription takes effect.** Subscribe and unsubscribe go upstream
//! in the order the hub issues them, from one worker. Every user join issues
//! a subscribe (the server keeps one subscription per connection however
//! often it is asked), and the user becomes active only when the hub reads
//! the [`ruvent::Incoming::Subscribed`] marker of that call.
//! Everything the inbox held before that marker was sent before the
//! subscription existed and reaches only users already active. So a user
//! never receives an event sent before its subscription took effect — in
//! particular after `0 → 1 → 0 → 1`, nothing of the first subscription leaks
//! into the second.
//!
//! **Hooks.** Authentication, subscribe and publish decisions run on the
//! caller's task before anything reaches the hub; only [`crate::Hooks::deliver`]
//! runs inside it, once per event and user.

use crate::auth::{self, AuthAnswer, AuthRequest, Denied, User};
use crate::config::ProxyConfig;
use crate::handshake::Handshake;
use crate::hooks::{Context, Defaults, Hooks};
use crate::message::{event_text, Outcome, Publish, PublishKind, Rejection, Request};
use crate::upstream::Connector;
use parking_lot::RwLock;
use ruvent::{Bytes, Envelope, EventId, Inbox, Incoming, Methods, Name, Refusal};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};

/// One attached user, numbered by the hub.
pub type UserId = u64;

/// Commands the hub accepts from users and from itself.
enum Command {
    Attach {
        user: Arc<User>,
        out: mpsc::Sender<Arc<str>>,
        reply: oneshot::Sender<UserId>,
    },
    Join {
        user: UserId,
        channel: Name,
        reply: oneshot::Sender<Result<(), Rejection>>,
    },
    Leave {
        user: UserId,
        channel: Name,
    },
    Detach {
        user: UserId,
    },
    /// Puts a result into the user's ordered stream.
    Send {
        user: UserId,
        text: Arc<str>,
    },
    AuthWait {
        correlation: EventId,
        reply: oneshot::Sender<AuthAnswer>,
    },
    AuthCancel {
        correlation: EventId,
    },
    Connected {
        upstream: Arc<dyn Methods>,
        inbox: Inbox,
    },
}

/// What the upstream worker reports back.
enum Report {
    SubscribeRefused { channel: Name, refusal: Refusal },
}

enum Call {
    Subscribe(Name),
    Unsubscribe(Name),
}

struct Waiter {
    /// The subscribe whose marker activates this user: the channel's `issued` when it joined.
    target: u64,
    user: UserId,
    reply: oneshot::Sender<Result<(), Rejection>>,
}

/// One channel's upstream subscription and its users.
///
/// | key | channel name |
/// | --- | --- |
/// | lifetime | while it has users, waiters, an unanswered subscribe, or is the auth reply channel |
/// | bound | channels in use by attached users, plus one |
/// | cleared by | `Hub::tidy` after a leave, a detach, a marker or a refusal |
#[derive(Default)]
struct Channel {
    active: HashSet<UserId>,
    waiting: Vec<Waiter>,
    /// Subscribes issued on the current upstream connection, and markers (or refusals) seen for them.
    issued: u64,
    seen: u64,
    /// The hub wants an upstream subscription (one was issued and not cancelled).
    subscribed: bool,
    /// The auth reply channel: kept subscribed with no users.
    internal: bool,
}

struct Attached {
    user: Arc<User>,
    out: mpsc::Sender<Arc<str>>,
    channels: HashSet<Name>,
}

struct Shared {
    commands: mpsc::Sender<Command>,
    upstream: RwLock<Option<Arc<dyn Methods>>>,
    config: ProxyConfig,
    hooks: Arc<dyn Hooks>,
    ready: watch::Receiver<bool>,
}

/// A handle to a running proxy hub. Cheap to clone.
#[derive(Clone)]
pub struct Proxy {
    shared: Arc<Shared>,
}

/// Capacity of the hub's command queue. Callers wait when it is full.
const COMMANDS: usize = 4096;

impl Proxy {
    /// Starts the hub with the configured behaviour ([`Defaults`]).
    pub fn start(config: ProxyConfig, connector: Arc<dyn Connector>) -> Proxy {
        Proxy::start_with(config, connector, Arc::new(Defaults))
    }

    /// Starts the hub and connects upstream in the background (retrying with backoff).
    pub fn start_with(config: ProxyConfig, connector: Arc<dyn Connector>, hooks: Arc<dyn Hooks>) -> Proxy {
        let (commands, receiver) = mpsc::channel(COMMANDS);
        let (ready_tx, ready) = watch::channel(false);
        let shared = Arc::new(Shared { commands: commands.clone(), upstream: RwLock::new(None), config: config.clone(), hooks, ready });
        let hub = Hub {
            shared: shared.clone(),
            commands: receiver,
            reports: None,
            calls: None,
            inbox: None,
            channels: HashMap::new(),
            users: HashMap::new(),
            next_user: 1,
            auth_waiters: HashMap::new(),
            reply_channel: config.auth.worker.as_ref().map(|worker| Name::new(&worker.reply_channel)),
            connector,
            ready: ready_tx,
            self_commands: commands,
        };
        tokio::spawn(hub.run());
        Proxy { shared }
    }

    pub fn config(&self) -> &ProxyConfig {
        &self.shared.config
    }

    /// Waits until the upstream connection is up (and the auth reply channel is subscribed).
    pub async fn ready(&self) {
        let mut ready = self.shared.ready.clone();
        let _ = ready.wait_for(|up| *up).await;
    }

    /// Whether the upstream connection is up right now.
    pub fn is_ready(&self) -> bool {
        *self.shared.ready.borrow()
    }

    pub(crate) fn upstream(&self) -> Result<Arc<dyn Methods>, Rejection> {
        self.shared.upstream.read().clone().ok_or_else(|| Rejection::unavailable("upstream unavailable"))
    }

    fn context(&self) -> Context<'_> {
        Context::new(self)
    }

    /// Who the user of this handshake is: [`Hooks::authenticate`].
    pub async fn authenticate(&self, handshake: &Handshake) -> Result<Arc<User>, Denied> {
        self.shared.hooks.authenticate(self.context(), handshake).await.map(Arc::new)
    }

    /// Authenticates a token according to `auth.mode`: the default hook.
    pub async fn authenticate_by_config(&self, token: Option<&str>) -> Result<User, Denied> {
        let config = &self.shared.config;
        if let Some(result) = auth::local(config, token) {
            return result;
        }
        let Some(worker) = config.auth.worker.clone() else { return Err(Denied::Unavailable("auth.worker missing".into())) };
        let Some(token) = token else { return Err(Denied::Missing) };
        let correlation = EventId::new_v7();
        let (reply, answer) = oneshot::channel();
        self.shared.commands.send(Command::AuthWait { correlation, reply }).await.map_err(|_| Denied::Unavailable("proxy stopped".into()))?;
        let request = serde_json::to_vec(&AuthRequest { token: token.to_owned() }).map_err(|error| Denied::Unavailable(error.to_string()))?;
        let event = Envelope::claimable(worker.channel.as_str(), worker.event_type.as_str(), Bytes::from(request)).with_correlation(correlation).with_reply(worker.reply_channel.as_str());
        let registered = match self.upstream() {
            Ok(upstream) => upstream.register_claimable(event).await.map_err(|refusal| Denied::Unavailable(refusal.to_string())),
            Err(rejection) => Err(Denied::Unavailable(rejection.message)),
        };
        if let Err(denied) = registered {
            let _ = self.shared.commands.send(Command::AuthCancel { correlation }).await;
            return Err(denied);
        }
        // The answer's deadline: one of the two timers this crate declares.
        match tokio::time::timeout(Duration::from_millis(worker.timeout_ms), answer).await {
            Ok(Ok(answer)) => auth::from_answer(config, answer),
            Ok(Err(_)) => Err(Denied::Unavailable("proxy stopped".into())),
            Err(_) => {
                let _ = self.shared.commands.send(Command::AuthCancel { correlation }).await;
                Err(Denied::Timeout)
            }
        }
    }

    /// Attaches a user. Its stream yields JSON texts — events and results, in
    /// order — and ends when the user is disconnected (buffer full, or detached).
    pub async fn attach(&self, user: Arc<User>) -> Result<(UserHandle, mpsc::Receiver<Arc<str>>), String> {
        let (out, events) = mpsc::channel(self.shared.config.user_buffer.max(1));
        let (reply, id) = oneshot::channel();
        self.shared.commands.send(Command::Attach { user: user.clone(), out, reply }).await.map_err(|_| "proxy stopped".to_owned())?;
        let id = id.await.map_err(|_| "proxy stopped".to_owned())?;
        Ok((UserHandle { id, user, proxy: self.clone() }, events))
    }

    /// Registers what a user asked for, once [`Hooks::publish`] allows it.
    pub async fn publish(&self, user: &User, mut publish: Publish) -> Outcome {
        let reference = publish.reference.clone();
        let registered = match self.shared.hooks.publish(self.context(), user, &mut publish).await {
            Ok(()) => self.register(user, publish).await,
            Err(rejection) => Err(rejection),
        };
        match registered {
            Ok(id) => Outcome::ok(reference, Some(id.to_string())),
            Err(rejection) => Outcome::rejected(reference, rejection),
        }
    }

    async fn register(&self, user: &User, publish: Publish) -> Result<EventId, Rejection> {
        let payload = Bytes::from(publish.payload.get().as_bytes().to_vec());
        let mut event = match publish.kind {
            PublishKind::Claimable => Envelope::claimable(publish.channel.as_str(), publish.event_type.as_str(), payload),
            PublishKind::Dispatch => Envelope::dispatch(publish.channel.as_str(), publish.event_type.as_str(), payload),
        };
        if let Some(id) = &publish.id {
            event = event.with_id(id.parse().map_err(|_| Rejection::invalid("id is not a UUID"))?);
        }
        if let Some(correlation) = &publish.correlation {
            event = event.with_correlation(correlation.parse().map_err(|_| Rejection::invalid("correlation is not a UUID"))?);
        }
        if let Some(reply) = &publish.reply {
            event = event.with_reply(reply.as_str());
        }
        if self.shared.config.delegate {
            event = event.with_delegate(user.name.as_str());
        }
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
        self.proxy.shared.hooks.subscribe(self.proxy.context(), &self.user, channel).await?;
        let stopped = || Rejection::unavailable("proxy stopped");
        let (reply, done) = oneshot::channel();
        self.proxy.shared.commands.send(Command::Join { user: self.id, channel: Name::new(channel), reply }).await.map_err(|_| stopped())?;
        done.await.map_err(|_| stopped())?
    }

    pub async fn unsubscribe(&self, channel: &str) {
        let _ = self.proxy.shared.commands.send(Command::Leave { user: self.id, channel: Name::new(channel) }).await;
    }

    /// Puts a result into this user's stream, behind what is already there.
    pub async fn reply(&self, outcome: Outcome) {
        let _ = self.proxy.shared.commands.send(Command::Send { user: self.id, text: outcome.text() }).await;
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
        // If the queue is full the hub still drops this user at the next send,
        // which finds the stream gone.
        let _ = self.proxy.shared.commands.try_send(Command::Detach { user: self.id });
    }
}

/// A user message that is not JSON of the right shape.
pub fn bad_request(error: impl std::fmt::Display) -> Outcome {
    Outcome::failed(None::<Value>, format!("bad request: {error}"))
}

struct Hub {
    shared: Arc<Shared>,
    commands: mpsc::Receiver<Command>,
    reports: Option<mpsc::Receiver<Report>>,
    /// Subscribe and unsubscribe calls for the upstream worker. Unbounded, but
    /// each entry stands for one user join or leave already accepted.
    calls: Option<mpsc::UnboundedSender<Call>>,
    inbox: Option<Inbox>,
    channels: HashMap<Name, Channel>,
    users: HashMap<UserId, Attached>,
    next_user: UserId,
    /// Authentications waiting for the worker's answer.
    ///
    /// | key | correlation of the authentication event |
    /// | --- | --- |
    /// | lifetime | until the answer arrives, or the waiting user times out or fails |
    /// | bound | authentications in progress |
    /// | cleared by | the answer, `AuthCancel` |
    auth_waiters: HashMap<EventId, oneshot::Sender<AuthAnswer>>,
    reply_channel: Option<Name>,
    connector: Arc<dyn Connector>,
    ready: watch::Sender<bool>,
    self_commands: mpsc::Sender<Command>,
}

enum Wake {
    Report(Option<Report>),
    Command(Option<Command>),
    Incoming(Option<Incoming>),
}

async fn next_report(reports: &mut Option<mpsc::Receiver<Report>>) -> Option<Report> {
    match reports {
        Some(reports) => reports.recv().await,
        None => std::future::pending().await,
    }
}

async fn next_incoming(inbox: &mut Option<Inbox>) -> Option<Incoming> {
    match inbox {
        Some(inbox) => inbox.recv().await,
        None => std::future::pending().await,
    }
}

impl Hub {
    async fn run(mut self) {
        if let Some(reply) = self.reply_channel.clone() {
            self.channels.insert(reply, Channel { internal: true, ..Channel::default() });
        }
        self.reconnect();
        loop {
            // Reports first: a refusal is always queued before any marker of a later subscribe.
            // Commands before the inbox: an auth waiter is queued before its answer can arrive.
            let wake = tokio::select! {
                biased;
                report = next_report(&mut self.reports) => Wake::Report(report),
                command = self.commands.recv() => Wake::Command(command),
                incoming = next_incoming(&mut self.inbox) => Wake::Incoming(incoming),
            };
            match wake {
                Wake::Report(Some(Report::SubscribeRefused { channel, refusal })) => self.refused(&channel, refusal),
                Wake::Report(None) => self.reports = None,
                Wake::Command(Some(command)) => self.command(command),
                Wake::Command(None) => break,
                Wake::Incoming(Some(Incoming::Delivery(event))) => self.delivery(&event),
                Wake::Incoming(Some(Incoming::Subscribed(channel))) => self.marker(&channel),
                Wake::Incoming(Some(Incoming::Notice(_))) => {}
                Wake::Incoming(Some(Incoming::Closed) | None) => self.lost(),
            }
        }
    }

    /// Connects in the background, waiting longer after each failure.
    fn reconnect(&self) {
        let connector = self.connector.clone();
        let commands = self.self_commands.clone();
        let backoff = self.shared.config.reconnect;
        tokio::spawn(async move {
            let mut wait = Duration::from_millis(backoff.initial_ms.max(1));
            loop {
                match connector.connect().await {
                    Ok((upstream, inbox)) => {
                        let _ = commands.send(Command::Connected { upstream, inbox }).await;
                        return;
                    }
                    Err(_) => {
                        // Reconnect backoff: the other timer this crate declares.
                        tokio::time::sleep(wait).await;
                        wait = (wait * 2).min(Duration::from_millis(backoff.max_ms.max(1)));
                    }
                }
            }
        });
    }

    /// The upstream connection dropped. Events sent meanwhile are lost (Q8(a)).
    fn lost(&mut self) {
        self.inbox = None;
        self.calls = None;
        self.reports = None;
        *self.shared.upstream.write() = None;
        let _ = self.ready.send(false);
        for channel in self.channels.values_mut() {
            channel.issued = 0;
            channel.seen = 0;
            channel.subscribed = false;
        }
        self.reconnect();
    }

    fn connected(&mut self, upstream: Arc<dyn Methods>, inbox: Inbox) {
        let (calls, mut queue) = mpsc::unbounded_channel::<Call>();
        let (report_tx, reports) = mpsc::channel(COMMANDS);
        let worker = upstream.clone();
        // One worker per connection sends subscribe and unsubscribe in issue order.
        tokio::spawn(async move {
            while let Some(call) = queue.recv().await {
                match call {
                    Call::Subscribe(channel) => {
                        if let Err(refusal) = worker.subscribe(channel.clone(), None).await {
                            if report_tx.send(Report::SubscribeRefused { channel, refusal }).await.is_err() {
                                return;
                            }
                        }
                    }
                    Call::Unsubscribe(channel) => {
                        let _ = worker.unsubscribe(channel).await;
                    }
                }
            }
        });
        self.calls = Some(calls);
        self.reports = Some(reports);
        self.inbox = Some(inbox);
        *self.shared.upstream.write() = Some(upstream);
        // Every channel still wanted is subscribed again; waiting users wait for the new marker.
        let wanted: Vec<Name> = self.channels.iter().filter(|(_, channel)| channel.internal || !channel.active.is_empty() || !channel.waiting.is_empty()).map(|(name, _)| name.clone()).collect();
        for name in wanted {
            self.issue(&name);
            if let Some(channel) = self.channels.get_mut(&name) {
                let target = channel.issued;
                for waiter in &mut channel.waiting {
                    waiter.target = target;
                }
            }
        }
        self.mark_ready();
    }

    /// Ready once connected and, in worker auth mode, once the reply channel's subscription took effect.
    fn mark_ready(&self) {
        let reply_ready = match &self.reply_channel {
            Some(reply) => self.channels.get(reply).is_some_and(|channel| channel.subscribed && channel.seen >= channel.issued),
            None => true,
        };
        let _ = self.ready.send(self.calls.is_some() && reply_ready);
    }

    fn issue(&mut self, name: &Name) {
        let Some(channel) = self.channels.get_mut(name) else { return };
        channel.issued += 1;
        channel.subscribed = true;
        if let Some(calls) = &self.calls {
            let _ = calls.send(Call::Subscribe(name.clone()));
        }
    }

    fn command(&mut self, command: Command) {
        match command {
            Command::Attach { user, out, reply } => {
                let id = self.next_user;
                self.next_user += 1;
                self.users.insert(id, Attached { user, out, channels: HashSet::new() });
                let _ = reply.send(id);
            }
            Command::Join { user, channel, reply } => self.join(user, channel, reply),
            Command::Leave { user, channel } => {
                if let Some(attached) = self.users.get_mut(&user) {
                    attached.channels.remove(&channel);
                }
                self.leave(user, &channel);
            }
            Command::Detach { user } => self.detach(user),
            Command::Send { user, text } => self.hand(user, text),
            Command::AuthWait { correlation, reply } => {
                self.auth_waiters.insert(correlation, reply);
            }
            Command::AuthCancel { correlation } => {
                self.auth_waiters.remove(&correlation);
            }
            Command::Connected { upstream, inbox } => self.connected(upstream, inbox),
        }
    }

    fn join(&mut self, user: UserId, name: Name, reply: oneshot::Sender<Result<(), Rejection>>) {
        let Some(attached) = self.users.get_mut(&user) else {
            let _ = reply.send(Err(Rejection::unavailable("user detached")));
            return;
        };
        attached.channels.insert(name.clone());
        let channel = self.channels.entry(name.clone()).or_default();
        if channel.active.contains(&user) {
            let _ = reply.send(Ok(()));
            return;
        }
        // Every join issues its own subscribe, even on a channel already
        // subscribed: the server keeps one subscription per connection, and the
        // marker of this call is the exact point after which the joining user
        // receives events. What the inbox already held stays with the users
        // who were active before.
        self.issue(&name);
        let Some(channel) = self.channels.get_mut(&name) else { return };
        channel.waiting.push(Waiter { target: channel.issued, user, reply });
    }

    fn leave(&mut self, user: UserId, name: &Name) {
        let Some(channel) = self.channels.get_mut(name) else { return };
        channel.active.remove(&user);
        channel.waiting.retain(|waiter| waiter.user != user);
        if channel.active.is_empty() && channel.waiting.is_empty() && !channel.internal && channel.subscribed {
            channel.subscribed = false;
            if let Some(calls) = &self.calls {
                let _ = calls.send(Call::Unsubscribe(name.clone()));
            }
        }
        self.tidy(name);
    }

    fn detach(&mut self, user: UserId) {
        let Some(attached) = self.users.remove(&user) else { return };
        for channel in attached.channels {
            self.leave(user, &channel);
        }
    }

    /// Drops a channel entry nothing needs any more.
    fn tidy(&mut self, name: &Name) {
        if self.channels.get(name).is_some_and(|channel| !channel.internal && channel.active.is_empty() && channel.waiting.is_empty() && channel.seen >= channel.issued && !channel.subscribed) {
            self.channels.remove(name);
        }
    }

    fn hand(&mut self, user: UserId, text: Arc<str>) {
        let full = match self.users.get(&user) {
            Some(attached) => attached.out.try_send(text).is_err(),
            None => false,
        };
        if full {
            // Dropping the hub's sender ends the user's stream: the user's signal it lost events.
            self.detach(user);
        }
    }

    /// A subscribe took effect upstream.
    fn marker(&mut self, name: &Name) {
        let Some(channel) = self.channels.get_mut(name) else { return };
        channel.seen += 1;
        let seen = channel.seen;
        let (ready, still): (Vec<Waiter>, Vec<Waiter>) = std::mem::take(&mut channel.waiting).into_iter().partition(|waiter| waiter.target <= seen);
        channel.waiting = still;
        for waiter in ready {
            channel.active.insert(waiter.user);
            let _ = waiter.reply.send(Ok(()));
        }
        self.tidy(name);
        self.mark_ready();
    }

    fn refused(&mut self, name: &Name, refusal: Refusal) {
        let Some(channel) = self.channels.get_mut(name) else { return };
        channel.seen += 1;
        let seen = channel.seen;
        if seen >= channel.issued {
            channel.subscribed = false;
        }
        let (failed, still): (Vec<Waiter>, Vec<Waiter>) = std::mem::take(&mut channel.waiting).into_iter().partition(|waiter| waiter.target <= seen);
        channel.waiting = still;
        for waiter in failed {
            let rejection = Rejection::from_refusal(refusal);
            let _ = waiter.reply.send(Err(Rejection { message: format!("upstream refused: {}", rejection.message), ..rejection }));
        }
        self.tidy(name);
    }

    fn delivery(&mut self, event: &Envelope) {
        if self.reply_channel.as_ref() == Some(&event.channel) {
            if let Some(waiter) = event.correlation.and_then(|correlation| self.auth_waiters.remove(&correlation)) {
                let answer = serde_json::from_slice(&event.payload).unwrap_or(AuthAnswer { ok: false, user: None, channels: Vec::new() });
                let _ = waiter.send(answer);
            }
            return;
        }
        let Some(channel) = self.channels.get(&event.channel) else { return };
        if channel.active.is_empty() {
            return;
        }
        let text = event_text(event);
        let hooks = &self.shared.hooks;
        let mut full = Vec::new();
        for user in &channel.active {
            if let Some(attached) = self.users.get(user) {
                if !hooks.deliver(&attached.user, event) {
                    continue;
                }
                if attached.out.try_send(text.clone()).is_err() {
                    full.push(*user);
                }
            }
        }
        for user in full {
            self.detach(user);
        }
    }
}
