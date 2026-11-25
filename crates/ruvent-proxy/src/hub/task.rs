//! The hub task: the one place that owns the observer's stream, the
//! attached users and the subscription book. It turns what arrives — user
//! commands, the observer's stream, the subscribe worker's reports, changes
//! of the server list and of the reply joins — into steps of [`Channels`]
//! and carries them out.
//!
//! ```text
//! users ──Command──►┐
//! subscribe worker ──Report──► hub (this task) ──try_send──► user streams (full or closed = detach)
//! observer ──Observed──►┘        │      └──► Health (watch) ──► /health
//!                                └─ Call, in order ──► subscribe worker ──► observer ──► every server
//! ```
//!
//! The subscribe worker hands the observer one call at a time, in the order
//! the book issued them, so the observer's tickets are the book's.
//!
//! The task ends on [`Command::Stop`] or when every [`super::Proxy`] and
//! user handle is gone. However it ends — a panic included — the hub's
//! [`Drop`] marks the proxy not ready, so `/health` stops answering 200,
//! and lets go of every upstream connection.

use super::channels::{Call, Channels, Step, UserId};
use super::Core;
use crate::auth::User;
use crate::health::Health;
use crate::hooks::Hooks;
use crate::message::{event_text, Outgoing, Rejection};
use ruvent::{Envelope, Refusal, Target};
use ruvent_client::observer::{Observed, Observer};
use ruvent_client::servers::Server;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

/// Capacity of the hub's command queue. Callers wait when it is full.
pub(super) const COMMANDS: usize = 4096;

/// Where one user's stream goes.
pub(super) enum Sink {
    /// JSON text: events become [`event_text`], made once per event.
    Text(mpsc::Sender<Arc<str>>),
    /// Events as they are; results as text.
    Raw(mpsc::Sender<Outgoing>),
}

impl Sink {
    fn is_closed(&self) -> bool {
        match self {
            Sink::Text(out) => out.is_closed(),
            Sink::Raw(out) => out.is_closed(),
        }
    }

    /// Hands over a result; `false` when the stream is full or closed.
    fn text(&self, text: Arc<str>) -> bool {
        match self {
            Sink::Text(out) => out.try_send(text).is_ok(),
            Sink::Raw(out) => out.try_send(Outgoing::Text(text)).is_ok(),
        }
    }

    /// Hands over an event; the text of it is made at most once across users.
    fn event(&self, event: &Arc<Envelope>, text: &mut Option<Arc<str>>) -> bool {
        match self {
            Sink::Text(out) => out.try_send(text.get_or_insert_with(|| event_text(event)).clone()).is_ok(),
            Sink::Raw(out) => out.try_send(Outgoing::Event(event.clone())).is_ok(),
        }
    }
}

/// What the hub accepts from users, from the proxy handle and from itself.
pub(super) enum Command {
    Attach {
        user: Arc<User>,
        out: Sink,
        reply: oneshot::Sender<UserId>,
    },
    Join {
        user: UserId,
        target: Target,
        reply: oneshot::Sender<Result<(), Rejection>>,
    },
    Leave {
        user: UserId,
        target: Target,
    },
    Detach {
        user: UserId,
    },
    /// Puts a result into the user's ordered stream.
    Send {
        user: UserId,
        text: Arc<str>,
    },
    /// Ends the hub: every user's stream ends and the upstream connections are dropped.
    Stop,
}

/// What the subscribe worker reports back: the answer of one subscribe call.
struct Report {
    target: Target,
    ticket: u64,
    result: Result<(), Refusal>,
}

struct Attached {
    user: Arc<User>,
    out: Sink,
    targets: HashSet<Target>,
}

type Reply = oneshot::Sender<Result<(), Rejection>>;

pub(super) struct Hub {
    core: Arc<Core>,
    commands: mpsc::Receiver<Command>,
    stream: mpsc::Receiver<Observed>,
    reports: mpsc::Receiver<Report>,
    /// Subscribe and unsubscribe calls for the worker. Unbounded, but each
    /// entry stands for one user join or leave already accepted.
    calls: mpsc::UnboundedSender<Call>,
    worker: JoinHandle<()>,
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
    list: watch::Receiver<BTreeSet<Server>>,
    joined: watch::Receiver<BTreeSet<Server>>,
    /// Servers whose observation connection is up (between `Up` and `Gap`).
    ///
    /// | key | server address |
    /// | --- | --- |
    /// | lifetime | `Up` ~ `Gap` |
    /// | bound | servers listed |
    /// | cleared by | `Gap` |
    observing: BTreeSet<Server>,
    health: watch::Sender<Health>,
}

/// What woke the hub. Lives on the stack for one turn of the loop, so the
/// envelope of a delivery is moved, not boxed.
#[allow(clippy::large_enum_variant)]
enum Wake {
    Report(Option<Report>),
    Command(Option<Command>),
    Observed(Option<Observed>),
    List(bool),
    Joined(bool),
}

/// The deliver hook's answer; `None` when the hook panicked.
fn admits(hooks: &dyn Hooks, user: &User, event: &Envelope) -> Option<bool> {
    catch_unwind(AssertUnwindSafe(|| hooks.deliver(user, event))).ok()
}

/// Hands the observer one call at a time, in order, and reports each subscribe's answer.
async fn subscribe_worker(observer: Observer, mut calls: mpsc::UnboundedReceiver<Call>, reports: mpsc::Sender<Report>) {
    while let Some(call) = calls.recv().await {
        match call {
            Call::Subscribe { target, ticket } => {
                let result = observer.subscribe(target.clone()).await;
                if reports.send(Report { target, ticket, result }).await.is_err() {
                    return;
                }
            }
            Call::Unsubscribe(target) => {
                let _ = observer.unsubscribe(target).await;
            }
        }
    }
}

impl Hub {
    pub(super) fn new(
        core: Arc<Core>,
        commands: mpsc::Receiver<Command>,
        observer: Observer,
        stream: mpsc::Receiver<Observed>,
        list: watch::Receiver<BTreeSet<Server>>,
        joined: watch::Receiver<BTreeSet<Server>>,
        health: watch::Sender<Health>,
    ) -> Hub {
        let (calls, queue) = mpsc::unbounded_channel();
        let (report_tx, reports) = mpsc::channel(COMMANDS);
        let worker = tokio::spawn(subscribe_worker(observer, queue, report_tx));
        Hub { core, commands, stream, reports, calls, worker, channels: Channels::new(), users: HashMap::new(), next_user: 1, list, joined, observing: BTreeSet::new(), health }
    }

    pub(super) async fn run(mut self) {
        self.publish_health();
        let (mut listing, mut joining) = (true, true);
        loop {
            // Reports first, commands before the stream: nothing here depends on
            // their order (a join settles on its call's answer and its server's
            // confirmation, read in either order), but a stopping hub stops first.
            let wake = tokio::select! {
                biased;
                report = self.reports.recv() => Wake::Report(report),
                command = self.commands.recv() => Wake::Command(command),
                observed = self.stream.recv() => Wake::Observed(observed),
                changed = self.list.changed(), if listing => Wake::List(changed.is_ok()),
                changed = self.joined.changed(), if joining => Wake::Joined(changed.is_ok()),
            };
            match wake {
                Wake::Report(Some(Report { target, ticket, result })) => {
                    let step = self.channels.answered(&target, ticket, result.map_err(Rejection::from_refusal));
                    self.apply(step);
                }
                // The worker holds the observer; without it there is nothing to watch.
                Wake::Report(None) | Wake::Observed(None) | Wake::Command(Some(Command::Stop) | None) => return,
                Wake::Command(Some(command)) => self.command(command),
                Wake::Observed(Some(observed)) => self.observed(observed),
                Wake::List(alive) => {
                    listing = alive;
                    self.publish_health();
                }
                Wake::Joined(alive) => {
                    joining = alive;
                    self.publish_health();
                }
            }
        }
    }

    /// Carries out one step of the book: calls the observer in order, then answers.
    fn apply(&mut self, step: Step<Reply>) {
        for call in step.calls {
            let _ = self.calls.send(call);
        }
        for (reply, result) in step.answers {
            let _ = reply.send(result);
        }
    }

    fn observed(&mut self, observed: Observed) {
        match observed {
            Observed::Delivery { server, event } => self.delivery(&server, event),
            Observed::Subscribed { server, target, ticket } => {
                let step = self.channels.confirmed(&server, &target, ticket);
                self.apply(step);
            }
            // A fresh connection: nothing is in effect there until it confirms.
            Observed::Up { server, .. } => {
                self.channels.reset(&server);
                self.observing.insert(server);
                self.publish_health();
            }
            // What the server dispatched until the next `Up` is lost (0.3).
            Observed::Gap { server, .. } => {
                self.channels.reset(&server);
                self.observing.remove(&server);
                self.publish_health();
            }
            Observed::Notice { .. } => {}
        }
    }

    fn publish_health(&self) {
        let health = Health::of(&self.list.borrow(), &self.observing, &self.joined.borrow());
        self.health.send_if_modified(|current| {
            let changed = *current != health;
            *current = health;
            changed
        });
    }

    fn command(&mut self, command: Command) {
        match command {
            Command::Attach { user, out, reply } => {
                let id = self.next_user;
                self.next_user += 1;
                self.users.insert(id, Attached { user, out, targets: HashSet::new() });
                let _ = reply.send(id);
            }
            Command::Join { user, target, reply } => {
                let Some(attached) = self.users.get_mut(&user) else {
                    let _ = reply.send(Err(Rejection::unavailable("user detached")));
                    return;
                };
                attached.targets.insert(target.clone());
                let step = self.channels.join(user, target, reply);
                self.apply(step);
            }
            Command::Leave { user, target } => {
                if let Some(attached) = self.users.get_mut(&user) {
                    attached.targets.remove(&target);
                }
                let step = self.channels.leave(user, &target);
                self.apply(step);
            }
            Command::Detach { user } => self.detach(user),
            Command::Send { user, text } => self.hand(user, text),
            Command::Stop => {}
        }
    }

    /// Drops a user: its stream ends (the hub held the only sender) and it leaves every target.
    fn detach(&mut self, user: UserId) {
        let Some(attached) = self.users.remove(&user) else { return };
        let step = self.channels.leave_all(user, attached.targets);
        self.apply(step);
    }

    fn hand(&mut self, user: UserId, text: Arc<str>) {
        let failed = self.users.get(&user).is_some_and(|attached| !attached.out.text(text));
        if failed {
            self.detach(user);
        }
    }

    fn delivery(&mut self, server: &Server, event: Envelope) {
        let audience = self.channels.audience(&event.channel, server);
        if audience.is_empty() {
            return;
        }
        let event = Arc::new(event);
        let mut text = None;
        let mut gone = Vec::new();
        for id in audience {
            let Some(attached) = self.users.get(&id) else { continue };
            // A user that went away is dropped whatever the hook says.
            if attached.out.is_closed() {
                gone.push(id);
                continue;
            }
            match admits(self.core.hooks.as_ref(), &attached.user, &event) {
                Some(true) => {}
                Some(false) => continue,
                // The hook could not decide: the user misses this event, and a
                // user that misses an event is disconnected.
                None => {
                    gone.push(id);
                    continue;
                }
            }
            if !attached.out.event(&event, &mut text) {
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
        self.health.send_replace(Health::default());
        self.worker.abort();
        *self.core.requester.write() = None;
    }
}
