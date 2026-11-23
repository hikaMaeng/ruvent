//! The hub task: the one place that owns the upstream connection's inbox,
//! the attached users and the subscription book. It turns what arrives —
//! user commands, upstream traffic, the upstream worker's reports — into
//! steps of [`Channels`] and carries them out.
//!
//! ```text
//! users ──Command──►┐
//! upstream worker ──Report──► hub (this task) ──try_send──► user streams (full or closed = detach)
//! upstream inbox ──Incoming──►┘        │
//!                                      └─ Call, in order ──► upstream worker ──► server
//! ```
//!
//! The task ends on [`Command::Stop`] or when every [`super::Proxy`] and
//! user handle is gone. However it ends — a panic included — the hub's
//! [`Drop`] marks the proxy not ready, so `/health` stops answering 200.

use super::channels::{Call, Channels, Step, UserId};
use super::Core;
use crate::auth::{AuthAnswer, User};
use crate::hooks::Hooks;
use crate::message::{event_text, Rejection};
use crate::upstream::Connector;
use ruvent::{Envelope, EventId, Inbox, Incoming, Methods, Name, Refusal};
use std::collections::{HashMap, HashSet};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};

/// Capacity of the hub's command queue. Callers wait when it is full.
pub(super) const COMMANDS: usize = 4096;

/// What the hub accepts from users, from the proxy handle and from itself.
pub(super) enum Command {
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
    /// Ends the hub: every user's stream ends and the upstream connection is dropped.
    Stop,
}

/// What the upstream worker reports back.
enum Report {
    SubscribeRefused { channel: Name, refusal: Refusal },
}

struct Attached {
    user: Arc<User>,
    out: mpsc::Sender<Arc<str>>,
    channels: HashSet<Name>,
}

type Reply = oneshot::Sender<Result<(), Rejection>>;

pub(super) struct Hub {
    core: Arc<Core>,
    commands: mpsc::Receiver<Command>,
    /// For the reconnect task: weak, so the hub never keeps itself alive.
    self_commands: mpsc::WeakSender<Command>,
    reports: Option<mpsc::Receiver<Report>>,
    /// Subscribe and unsubscribe calls for the upstream worker. Unbounded, but
    /// each entry stands for one user join or leave already accepted.
    calls: Option<mpsc::UnboundedSender<Call>>,
    inbox: Option<Inbox>,
    channels: Channels<Reply>,
    /// Attached users.
    ///
    /// | key | `UserId`, numbered by this hub |
    /// | --- | --- |
    /// | lifetime | attach until detach, a full or closed stream, or the hub's end |
    /// | bound | users attached to this proxy |
    /// | cleared by | `Detach`, `Hub::hand`, `Hub::delivery` |
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

/// The deliver hook's answer; `None` when the hook panicked.
fn admits(hooks: &dyn Hooks, user: &User, event: &Envelope) -> Option<bool> {
    catch_unwind(AssertUnwindSafe(|| hooks.deliver(user, event))).ok()
}

impl Hub {
    pub(super) fn new(core: Arc<Core>, commands: mpsc::Receiver<Command>, self_commands: mpsc::WeakSender<Command>, connector: Arc<dyn Connector>, ready: watch::Sender<bool>) -> Hub {
        let reply_channel = core.config.auth.worker.as_ref().map(|worker| Name::new(&worker.reply_channel));
        Hub {
            core,
            commands,
            self_commands,
            reports: None,
            calls: None,
            inbox: None,
            channels: Channels::new(reply_channel.clone()),
            users: HashMap::new(),
            next_user: 1,
            auth_waiters: HashMap::new(),
            reply_channel,
            connector,
            ready,
        }
    }

    pub(super) async fn run(mut self) {
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
                Wake::Report(Some(Report::SubscribeRefused { channel, refusal })) => {
                    let step = self.channels.refused(&channel, &Rejection::from_refusal(refusal));
                    self.apply(step);
                }
                Wake::Report(None) => self.reports = None,
                Wake::Command(Some(Command::Stop) | None) => return,
                Wake::Command(Some(command)) => self.command(command),
                Wake::Incoming(Some(Incoming::Delivery(event))) => self.delivery(&event),
                Wake::Incoming(Some(Incoming::Subscribed(channel))) => {
                    let step = self.channels.marker(&channel);
                    self.apply(step);
                    self.mark_ready();
                }
                Wake::Incoming(Some(Incoming::Notice(_))) => {}
                Wake::Incoming(Some(Incoming::Closed) | None) => self.lost(),
            }
        }
    }

    /// Carries out one step of the book: calls upstream in order, then answers.
    fn apply(&mut self, step: Step<Reply>) {
        if let Some(calls) = &self.calls {
            for call in step.calls {
                let _ = calls.send(call);
            }
        }
        for (reply, result) in step.answers {
            let _ = reply.send(result);
        }
    }

    /// Connects in the background, waiting longer after each failure, until
    /// it connects or the hub is gone.
    fn reconnect(&self) {
        let connector = self.connector.clone();
        let commands = self.self_commands.clone();
        let backoff = self.core.config.reconnect;
        tokio::spawn(async move {
            let mut wait = Duration::from_millis(backoff.initial_ms.max(1));
            while commands.strong_count() > 0 {
                match connector.connect().await {
                    Ok((upstream, inbox)) => {
                        if let Some(commands) = commands.upgrade() {
                            let _ = commands.send(Command::Connected { upstream, inbox }).await;
                        }
                        return;
                    }
                    Err(_) => {
                        // Reconnect backoff: one of the two timers this crate declares.
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
        *self.core.upstream.write() = None;
        let _ = self.ready.send(false);
        self.channels.lost();
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
        *self.core.upstream.write() = Some(upstream);
        let step = self.channels.connected();
        self.apply(step);
        self.mark_ready();
    }

    /// Ready once connected and, in worker auth mode, once the reply channel's subscription took effect.
    fn mark_ready(&self) {
        let reply_ready = self.reply_channel.as_ref().is_none_or(|reply| self.channels.settled(reply));
        let _ = self.ready.send(self.calls.is_some() && reply_ready);
    }

    fn command(&mut self, command: Command) {
        match command {
            Command::Attach { user, out, reply } => {
                let id = self.next_user;
                self.next_user += 1;
                self.users.insert(id, Attached { user, out, channels: HashSet::new() });
                let _ = reply.send(id);
            }
            Command::Join { user, channel, reply } => {
                let Some(attached) = self.users.get_mut(&user) else {
                    let _ = reply.send(Err(Rejection::unavailable("user detached")));
                    return;
                };
                attached.channels.insert(channel.clone());
                let step = self.channels.join(user, channel, reply);
                self.apply(step);
            }
            Command::Leave { user, channel } => {
                if let Some(attached) = self.users.get_mut(&user) {
                    attached.channels.remove(&channel);
                }
                let step = self.channels.leave(user, &channel);
                self.apply(step);
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
            Command::Stop => {}
        }
    }

    /// Drops a user: its stream ends (the hub held the only sender) and it leaves every channel.
    fn detach(&mut self, user: UserId) {
        let Some(attached) = self.users.remove(&user) else { return };
        let step = self.channels.leave_all(user, attached.channels);
        self.apply(step);
    }

    fn hand(&mut self, user: UserId, text: Arc<str>) {
        let failed = self.users.get(&user).is_some_and(|attached| attached.out.try_send(text).is_err());
        if failed {
            self.detach(user);
        }
    }

    fn delivery(&mut self, event: &Envelope) {
        if self.reply_channel.as_ref() == Some(&event.channel) {
            if let Some(waiter) = event.correlation.and_then(|correlation| self.auth_waiters.remove(&correlation)) {
                let answer = serde_json::from_slice(&event.payload).unwrap_or(AuthAnswer { ok: false, user: None, channels: Vec::new() });
                let _ = waiter.send(answer);
            }
            return;
        }
        let mut text = None;
        let mut gone = Vec::new();
        for id in self.channels.audience(&event.channel) {
            let Some(attached) = self.users.get(&id) else { continue };
            // A user that went away is dropped whatever the hook says.
            if attached.out.is_closed() {
                gone.push(id);
                continue;
            }
            match admits(self.core.hooks.as_ref(), &attached.user, event) {
                Some(true) => {}
                Some(false) => continue,
                // The hook could not decide: the user misses this event, and a
                // user that misses an event is disconnected.
                None => {
                    gone.push(id);
                    continue;
                }
            }
            let text = text.get_or_insert_with(|| event_text(event));
            if attached.out.try_send(text.clone()).is_err() {
                gone.push(id);
            }
        }
        for id in gone {
            self.detach(id);
        }
    }
}

impl Drop for Hub {
    fn drop(&mut self) {
        let _ = self.ready.send(false);
        *self.core.upstream.write() = None;
    }
}
