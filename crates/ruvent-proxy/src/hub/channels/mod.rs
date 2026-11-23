//! The subscription book: which users of each channel are active, and what
//! the hub has to ask upstream. No task, no queue, no clock — each method
//! takes one input of the hub and returns a [`Step`]: the upstream calls to
//! make, in order, and the answers owed to users who were waiting.
//!
//! The book is generic over `R`, the thing a waiting user is answered
//! through (the hub uses a `oneshot::Sender`), so it can be driven and
//! checked without any of the hub's machinery.
//!
//! **When a subscription takes effect.** Every join issues its own
//! subscribe, and the joining user waits for the marker of exactly that
//! subscribe: the `issued` count at join time is the waiter's `target`, and
//! each marker read raises `seen` by one. A user becomes active when `seen`
//! reaches its target. A marker arriving when every issued subscribe has
//! already been seen was not asked for by this book and changes nothing.

use crate::message::Rejection;
use ruvent::Name;
use std::collections::{HashMap, HashSet};

/// One attached user, numbered by the hub.
pub type UserId = u64;

/// A call the hub makes on its upstream connection, in this order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Call {
    Subscribe(Name),
    Unsubscribe(Name),
}

/// What one input to the book asks of the hub.
#[derive(Debug)]
pub struct Step<R> {
    /// Upstream calls, in order. Empty while disconnected.
    pub calls: Vec<Call>,
    /// Waiting users to answer.
    pub answers: Vec<(R, Result<(), Rejection>)>,
}

impl<R> Step<R> {
    fn none() -> Step<R> {
        Step { calls: Vec::new(), answers: Vec::new() }
    }

    fn absorb(&mut self, other: Step<R>) {
        self.calls.extend(other.calls);
        self.answers.extend(other.answers);
    }
}

#[derive(Debug)]
struct Waiter<R> {
    /// The subscribe whose marker activates this user.
    target: u64,
    user: UserId,
    reply: R,
}

#[derive(Debug)]
struct Channel<R> {
    active: HashSet<UserId>,
    waiting: Vec<Waiter<R>>,
    /// Subscribes issued on the current upstream connection, and markers (or refusals) seen for them.
    issued: u64,
    seen: u64,
    /// An upstream subscription is wanted (issued and not cancelled).
    subscribed: bool,
    /// Kept subscribed with no users (the authentication reply channel).
    internal: bool,
}

impl<R> Channel<R> {
    fn new(internal: bool) -> Channel<R> {
        Channel { active: HashSet::new(), waiting: Vec::new(), issued: 0, seen: 0, subscribed: false, internal }
    }

    fn wanted(&self) -> bool {
        self.internal || !self.active.is_empty() || !self.waiting.is_empty()
    }

    /// Nothing needs this entry any more: no users, no outstanding subscribe.
    fn idle(&self) -> bool {
        !self.wanted() && self.seen >= self.issued && !self.subscribed
    }

    /// Counts one marker or refusal; `None` when every issued subscribe was already answered.
    fn answered(&mut self) -> Option<u64> {
        if self.seen >= self.issued {
            return None;
        }
        self.seen += 1;
        Some(self.seen)
    }

    fn due(&mut self, seen: u64) -> Vec<Waiter<R>> {
        let (due, still): (Vec<Waiter<R>>, Vec<Waiter<R>>) = std::mem::take(&mut self.waiting).into_iter().partition(|waiter| waiter.target <= seen);
        self.waiting = still;
        due
    }
}

/// Every channel the hub's users want, and the reply channel.
///
/// | key | channel name |
/// | --- | --- |
/// | lifetime | while it has users, waiters, an unanswered subscribe, or is internal |
/// | bound | channels in use by attached users, plus the internal one |
/// | cleared by | the step that leaves it idle: a leave, a marker, a refusal |
#[derive(Debug)]
pub struct Channels<R> {
    entries: HashMap<Name, Channel<R>>,
    connected: bool,
}

impl<R> Channels<R> {
    /// An empty book, disconnected; `internal` stays subscribed with no users.
    pub fn new(internal: Option<Name>) -> Channels<R> {
        let mut entries = HashMap::new();
        if let Some(name) = internal {
            entries.insert(name, Channel::new(true));
        }
        Channels { entries, connected: false }
    }

    /// The users who receive what arrives on `name` now.
    pub fn audience(&self, name: &Name) -> impl Iterator<Item = UserId> + '_ {
        self.entries.get(name).into_iter().flat_map(|channel| channel.active.iter().copied())
    }

    /// Whether `name` has an upstream subscription that took effect.
    pub fn settled(&self, name: &Name) -> bool {
        self.entries.get(name).is_some_and(|channel| channel.subscribed && channel.issued > 0 && channel.seen >= channel.issued)
    }

    /// Whether the book holds an entry for `name`: the tests' view of its bound.
    #[cfg(test)]
    pub fn contains(&self, name: &Name) -> bool {
        self.entries.contains_key(name)
    }

    /// Wants an upstream subscription to `name` and, when connected, sends one.
    /// Returns the target a joiner waits for. Only subscribes actually sent
    /// are counted: a marker can come back only for those, and
    /// [`Channels::connected`] issues again for whoever waited meanwhile.
    fn issue(&mut self, name: &Name, step: &mut Step<R>) -> u64 {
        let connected = self.connected;
        let Some(channel) = self.entries.get_mut(name) else { return 0 };
        channel.subscribed = true;
        if connected {
            channel.issued += 1;
            step.calls.push(Call::Subscribe(name.clone()));
        }
        channel.issued
    }

    /// `user` wants `name`. Answered at once if already active, else when its subscribe's marker arrives.
    pub fn join(&mut self, user: UserId, name: Name, reply: R) -> Step<R> {
        let mut step = Step::none();
        let channel = self.entries.entry(name.clone()).or_insert_with(|| Channel::new(false));
        if channel.active.contains(&user) {
            step.answers.push((reply, Ok(())));
            return step;
        }
        let target = self.issue(&name, &mut step);
        if let Some(channel) = self.entries.get_mut(&name) {
            channel.waiting.push(Waiter { target, user, reply });
        }
        step
    }

    /// `user` leaves `name`. A join of its still waiting is answered as cancelled.
    pub fn leave(&mut self, user: UserId, name: &Name) -> Step<R> {
        let mut step = Step::none();
        let connected = self.connected;
        let Some(channel) = self.entries.get_mut(name) else { return step };
        channel.active.remove(&user);
        let (cancelled, still): (Vec<Waiter<R>>, Vec<Waiter<R>>) = std::mem::take(&mut channel.waiting).into_iter().partition(|waiter| waiter.user == user);
        channel.waiting = still;
        for waiter in cancelled {
            step.answers.push((waiter.reply, Err(Rejection::unavailable("unsubscribed before the subscription took effect"))));
        }
        if !channel.wanted() && channel.subscribed {
            channel.subscribed = false;
            if connected {
                step.calls.push(Call::Unsubscribe(name.clone()));
            }
        }
        self.tidy(name);
        step
    }

    /// `user` leaves every channel in `names`.
    pub fn leave_all(&mut self, user: UserId, names: impl IntoIterator<Item = Name>) -> Step<R> {
        let mut step = Step::none();
        for name in names {
            step.absorb(self.leave(user, &name));
        }
        step
    }

    /// A subscribe to `name` took effect upstream.
    pub fn marker(&mut self, name: &Name) -> Step<R> {
        let mut step = Step::none();
        let Some(channel) = self.entries.get_mut(name) else { return step };
        let Some(seen) = channel.answered() else { return step };
        for waiter in channel.due(seen) {
            channel.active.insert(waiter.user);
            step.answers.push((waiter.reply, Ok(())));
        }
        self.tidy(name);
        step
    }

    /// The upstream refused a subscribe to `name`.
    pub fn refused(&mut self, name: &Name, rejection: &Rejection) -> Step<R> {
        let mut step = Step::none();
        let Some(channel) = self.entries.get_mut(name) else { return step };
        let Some(seen) = channel.answered() else { return step };
        if seen >= channel.issued {
            channel.subscribed = false;
        }
        for waiter in channel.due(seen) {
            step.answers.push((waiter.reply, Err(Rejection::new(rejection.cause, format!("upstream refused: {}", rejection.message)))));
        }
        self.tidy(name);
        step
    }

    /// The upstream connection dropped: nothing issued on it will be answered.
    pub fn lost(&mut self) {
        self.connected = false;
        for channel in self.entries.values_mut() {
            channel.issued = 0;
            channel.seen = 0;
            channel.subscribed = false;
        }
    }

    /// A new upstream connection: every wanted channel is subscribed again,
    /// and waiting users now wait for the new subscribe's marker.
    pub fn connected(&mut self) -> Step<R> {
        self.connected = true;
        let mut step = Step::none();
        let mut wanted: Vec<Name> = self.entries.iter().filter(|(_, channel)| channel.wanted()).map(|(name, _)| name.clone()).collect();
        wanted.sort();
        for name in wanted {
            let target = self.issue(&name, &mut step);
            if let Some(channel) = self.entries.get_mut(&name) {
                for waiter in &mut channel.waiting {
                    waiter.target = target;
                }
            }
        }
        step
    }

    fn tidy(&mut self, name: &Name) {
        if self.entries.get(name).is_some_and(Channel::idle) {
            self.entries.remove(name);
        }
    }
}

#[cfg(test)]
mod tests;
