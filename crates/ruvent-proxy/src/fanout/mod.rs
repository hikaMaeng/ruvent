//! The fan-out: one task that owns the observation side and the members,
//! and the handles that talk to it.
//!
//! The event servers see, from each fan-out, one observation connection
//! each and one subscription per target however many members want it (4장).
//! The hub fans each delivery out to the members whose subscription is in
//! effect on the server it came from at the moment it handles that delivery.
//!
//! | file | role |
//! | --- | --- |
//! | `mod.rs` | [`Fanout`] and [`Member`]: what callers hold |
//! | `channels/` | the subscription book: pure state, no task, no clock. When a subscription takes effect is decided there |
//! | `task.rs` | the hub task: carries out the book's steps, fans deliveries out, reports health |
//!
//! **When a subscription takes effect.** A member becomes active on a server
//! only when the hub reads that server's confirmation of the subscribe its
//! own join issued (`channels/`, and the observer's tickets). Everything the
//! stream held before that confirmation was sent before the subscription
//! existed for this member and reaches only members already active. So a
//! member never receives an event sent before its subscription took effect
//! — in particular after `0 → 1 → 0 → 1`, nothing of the first subscription
//! leaks into the second.
//!
//! **What is not here.** Who a member is, what it may subscribe to, which
//! protocol it speaks, what it publishes and how a request is answered are
//! the program's. It authorizes before [`Member::subscribe`] and registers
//! through its own [`ruvent_client::producer::Producer`]. A request–response
//! is one member: it subscribes the request's own answer channel, registers
//! the request naming that channel in its payload, writes what arrives, and
//! drops the member when the program's end condition is met.

mod channels;
mod task;

pub use channels::MemberId;

use crate::health::Health;
use crate::upstream::Upstream;
use ruvent::{Envelope, Refusal, Target};
use ruvent_client::line::backoff::Reconnect;
use ruvent_client::observer::{Observer, ObserverConfig};
use std::fmt;
use std::sync::Arc;
use task::{Command, Hub};
use tokio::sync::{mpsc, oneshot, watch};

/// Every value is a field with its default (chapter 8).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FanoutConfig {
    /// Reconnection of the observation connections (S20).
    pub reconnect: Reconnect,
    /// Events a member's stream holds unread. The hub never waits for one
    /// member: a member this far behind is detached and its stream ends.
    pub member_buffer: usize,
    /// The hub's command queue. Callers wait while it is full.
    pub commands: usize,
}

impl Default for FanoutConfig {
    fn default() -> Self {
        FanoutConfig { reconnect: Reconnect::default(), member_buffer: 1024, commands: 4096 }
    }
}

/// Why a subscribe did not take effect.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubscribeError {
    /// An event server refused it.
    Refused(Refusal),
    /// The member left the target, or was detached, before it took effect.
    Left,
    /// The fan-out stopped.
    Stopped,
}

impl fmt::Display for SubscribeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SubscribeError::Refused(refusal) => write!(f, "an event server refused the subscription: {refusal:?}"),
            SubscribeError::Left => f.write_str("left before the subscription took effect"),
            SubscribeError::Stopped => f.write_str("the fan-out stopped"),
        }
    }
}

impl std::error::Error for SubscribeError {}

/// A handle to a running fan-out. Cheap to clone. The hub stops when
/// [`Fanout::stop`] is called or when every handle (members' included) is gone.
#[derive(Clone)]
pub struct Fanout {
    commands: mpsc::Sender<Command>,
    health: watch::Receiver<Health>,
    member_buffer: usize,
}

impl Fanout {
    /// Starts the hub and connects to every server of `upstream` in the
    /// background (the SDK reconnects with back-off).
    pub fn start(config: FanoutConfig, upstream: Upstream) -> Fanout {
        let observer_config = ObserverConfig { credential: upstream.credential.clone(), reconnect: config.reconnect, ..ObserverConfig::default() };
        let (observer, stream) = Observer::start(upstream.connector.clone(), upstream.servers.as_ref(), observer_config);
        let (health_tx, health) = watch::channel(Health::default());
        let (commands, receiver) = mpsc::channel(config.commands.max(1));
        let hub = Hub::new(receiver, observer, stream, upstream.servers.watch(), health_tx, config.commands.max(1));
        tokio::spawn(hub.run());
        Fanout { commands, health, member_buffer: config.member_buffer.max(1) }
    }

    /// Waits until every listed server's observation connection is up
    /// ([`Health`]). `false` when the hub stopped instead.
    pub async fn ready(&self) -> bool {
        let mut health = self.health.clone();
        let ready = health.wait_for(|health| health.ready).await.is_ok();
        ready
    }

    /// Whether every listed server's observation connection is up right now. `false` once the hub stopped.
    pub fn is_ready(&self) -> bool {
        self.health.borrow().ready
    }

    /// The upstream, now.
    pub fn health(&self) -> Health {
        self.health.borrow().clone()
    }

    /// [`Fanout::health`] and every later change.
    pub fn watch_health(&self) -> watch::Receiver<Health> {
        self.health.clone()
    }

    /// Stops the hub: every member's stream ends and every upstream
    /// connection is dropped. Later calls on any handle answer [`SubscribeError::Stopped`].
    pub async fn stop(&self) {
        let _ = self.commands.send(Command::Stop).await;
    }

    /// Attaches a member. Its stream carries the events as the event server
    /// sent them, shared between members, the payload untouched. It ends
    /// when the member is detached: dropped, too slow
    /// ([`FanoutConfig::member_buffer`]) or the hub stopped.
    pub async fn attach(&self) -> Result<(Member, mpsc::Receiver<Arc<Envelope>>), SubscribeError> {
        let (out, events) = mpsc::channel(self.member_buffer);
        let (reply, id) = oneshot::channel();
        self.commands.send(Command::Attach { out, reply }).await.map_err(|_| SubscribeError::Stopped)?;
        let id = id.await.map_err(|_| SubscribeError::Stopped)?;
        Ok((Member { id, fanout: self.clone() }, events))
    }
}

/// One attached member. Dropping it detaches the member.
pub struct Member {
    pub id: MemberId,
    fanout: Fanout,
}

impl Member {
    /// Subscribes, and returns once the subscription has taken effect on a
    /// server. A prefix target takes every channel below it at a `.`
    /// boundary. Whether this member may have it is the caller's to decide
    /// first.
    pub async fn subscribe(&self, target: Target) -> Result<(), SubscribeError> {
        let (reply, done) = oneshot::channel();
        self.fanout.commands.send(Command::Join { member: self.id, target, reply }).await.map_err(|_| SubscribeError::Stopped)?;
        done.await.map_err(|_| SubscribeError::Stopped)?
    }

    pub async fn unsubscribe(&self, target: Target) {
        let _ = self.fanout.commands.send(Command::Leave { member: self.id, target }).await;
    }
}

impl Drop for Member {
    fn drop(&mut self) {
        // If the queue is full the hub still drops this member at the next
        // delivery, which finds the stream closed.
        let _ = self.fanout.commands.try_send(Command::Detach { member: self.id });
    }
}
