//! The hub task: the one place that owns the observer's stream, the
//! attached members and the subscription book. It turns what arrives —
//! member commands, the observer's stream, the subscribe worker's reports,
//! changes of the server list — into steps of [`Channels`] and carries them
//! out.
//!
//! ```text
//! members ──Command──►┐
//! subscribe worker ──Report──► hub (this task) ──try_send──► member streams (full or closed = detach)
//! observer ──Observed──►┘        │      └──► Health (watch)
//!                                └─ Call, in order ──► subscribe worker ──► observer ──► every server
//! ```
//!
//! The subscribe worker hands the observer one call at a time, in the order
//! the book issued them, so the observer's tickets are the book's.
//!
//! The task ends on [`Command::Stop`] or when every [`super::Fanout`] and
//! member handle is gone. However it ends — a panic included — the hub's
//! [`Drop`] marks the fan-out not ready and lets go of every upstream
//! connection.

use super::channels::{Call, Channels, MemberId, Step};
use super::SubscribeError;
use crate::health::Health;
use ruvent::{Envelope, Refusal, Target};
use ruvent_client::observer::{Observed, Observer};
use ruvent_client::servers::Server;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

/// What the hub accepts from members, from the fan-out handle and from itself.
pub(super) enum Command {
    Attach {
        out: mpsc::Sender<Arc<Envelope>>,
        reply: oneshot::Sender<MemberId>,
    },
    Join {
        member: MemberId,
        target: Target,
        reply: Reply,
    },
    Leave {
        member: MemberId,
        target: Target,
    },
    Detach {
        member: MemberId,
    },
    /// Ends the hub: every member's stream ends and the upstream connections are dropped.
    Stop,
}

/// What the subscribe worker reports back: the answer of one subscribe call.
struct Report {
    target: Target,
    ticket: u64,
    result: Result<(), Refusal>,
}

struct Attached {
    out: mpsc::Sender<Arc<Envelope>>,
    targets: HashSet<Target>,
}

type Reply = oneshot::Sender<Result<(), SubscribeError>>;

pub(super) struct Hub {
    commands: mpsc::Receiver<Command>,
    stream: mpsc::Receiver<Observed>,
    reports: mpsc::Receiver<Report>,
    /// Subscribe and unsubscribe calls for the worker. Unbounded, but each
    /// entry stands for one member join or leave already accepted.
    calls: mpsc::UnboundedSender<Call>,
    worker: JoinHandle<()>,
    channels: Channels<Reply>,
    /// Attached members.
    ///
    /// | key | `MemberId`, numbered by this hub |
    /// | --- | --- |
    /// | lifetime | attach until detach, a full or closed stream, or the hub's end |
    /// | bound | members attached to this fan-out |
    /// | cleared by | `Detach`, `Hub::delivery` |
    members: HashMap<MemberId, Attached>,
    next_member: MemberId,
    list: watch::Receiver<BTreeSet<Server>>,
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
        commands: mpsc::Receiver<Command>,
        observer: Observer,
        stream: mpsc::Receiver<Observed>,
        list: watch::Receiver<BTreeSet<Server>>,
        health: watch::Sender<Health>,
        reports: usize,
    ) -> Hub {
        let (calls, queue) = mpsc::unbounded_channel();
        let (report_tx, report_rx) = mpsc::channel(reports);
        let worker = tokio::spawn(subscribe_worker(observer, queue, report_tx));
        Hub { commands, stream, reports: report_rx, calls, worker, channels: Channels::new(), members: HashMap::new(), next_member: 1, list, observing: BTreeSet::new(), health }
    }

    pub(super) async fn run(mut self) {
        self.publish_health();
        let mut listing = true;
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
            };
            match wake {
                Wake::Report(Some(Report { target, ticket, result })) => {
                    let step = self.channels.answered(&target, ticket, result);
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
        let health = Health::of(&self.list.borrow(), &self.observing);
        self.health.send_if_modified(|current| {
            let changed = *current != health;
            *current = health;
            changed
        });
    }

    fn command(&mut self, command: Command) {
        match command {
            Command::Attach { out, reply } => {
                let id = self.next_member;
                self.next_member += 1;
                self.members.insert(id, Attached { out, targets: HashSet::new() });
                let _ = reply.send(id);
            }
            Command::Join { member, target, reply } => {
                let Some(attached) = self.members.get_mut(&member) else {
                    let _ = reply.send(Err(SubscribeError::Left));
                    return;
                };
                attached.targets.insert(target.clone());
                let step = self.channels.join(member, target, reply);
                self.apply(step);
            }
            Command::Leave { member, target } => {
                if let Some(attached) = self.members.get_mut(&member) {
                    attached.targets.remove(&target);
                }
                let step = self.channels.leave(member, &target);
                self.apply(step);
            }
            Command::Detach { member } => self.detach(member),
            Command::Stop => {}
        }
    }

    /// Drops a member: its stream ends (the hub held the only sender) and it leaves every target.
    fn detach(&mut self, member: MemberId) {
        let Some(attached) = self.members.remove(&member) else { return };
        let step = self.channels.leave_all(member, attached.targets);
        self.apply(step);
    }

    fn delivery(&mut self, server: &Server, event: Envelope) {
        let audience = self.channels.audience(&event.channel, server);
        if audience.is_empty() {
            return;
        }
        let event = Arc::new(event);
        let mut gone = Vec::new();
        for id in audience {
            let Some(attached) = self.members.get(&id) else { continue };
            // Full (too slow) or closed (went away): the hub never waits for one member.
            if attached.out.try_send(event.clone()).is_err() {
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
    }
}
