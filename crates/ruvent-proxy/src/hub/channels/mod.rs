//! The subscription book: which users of each target (an exact channel, or
//! a `.`-boundary prefix) receive what each server sends, and what the hub
//! has to ask its observer. No task, no queue, no clock — each method takes
//! one input of the hub and returns a [`Step`]: the observer calls to make,
//! in order, and the answers owed to users who were waiting.
//!
//! The book is generic over `R`, the thing a waiting user is answered
//! through (the hub uses a one-shot channel), so it can be driven and
//! checked without any of the hub's machinery.
//!
//! **When a subscription takes effect.** Every join of a user not yet in the
//! target issues its own subscribe, numbered with the next **ticket**: the
//! observer numbers its `subscribe` calls the same way, in the order it
//! handles them, and the hub hands it every call in the order the book
//! issues them. Each server's confirmation comes back as
//! [`Channels::confirmed`] with the ticket of the subscribe it answers — or,
//! for the subscriptions said again at a new connection, the number of calls
//! handled so far. A user becomes active **on that server** at the first
//! confirmation with `ticket ≥` its own: everything after it in the stream
//! was sent after its own subscribe took effect there. A confirmation of an
//! earlier subscribe activates nobody who joined later.
//!
//! **When a join is answered.** Once its subscribe call came back without a
//! refusal ([`Channels::answered`]) and the user is active on at least one
//! server. The observer puts every confirmation in the stream before it
//! answers the call, so a user told "subscribed" who then publishes finds
//! the confirmation ahead of the event. With no server connected the call
//! comes back at once and the user waits for the first server to connect
//! and confirm.
//!
//! **One upstream subscription per target per server.** The observer keeps
//! one subscription per target and says it again at each connection; a
//! second user's subscribe is the same subscription confirmed again, which
//! is what gives that user its own confirmation.

use crate::message::Rejection;
use ruvent::Target;
use ruvent_client::servers::Server;
use std::collections::{HashMap, HashSet};

/// One attached user, numbered by the hub.
pub type UserId = u64;

/// A call the hub makes on its observer, in this order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Call {
    /// The `ticket`-th subscribe call.
    Subscribe {
        target: Target,
        ticket: u64,
    },
    Unsubscribe(Target),
}

/// What one input to the book asks of the hub.
#[derive(Debug)]
pub struct Step<R> {
    /// Observer calls, in order.
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

/// One user in one target.
#[derive(Debug)]
struct Member<R> {
    /// The subscribe call that admits this user.
    ticket: u64,
    /// Servers on which the subscription is in effect for this user.
    on: HashSet<Server>,
    /// The subscribe call came back without a refusal.
    confirmed: bool,
    /// The join was answered "subscribed".
    answered: bool,
    /// Joins of this user still waiting for that answer.
    waiting: Vec<R>,
}

#[derive(Debug)]
struct Entry<R> {
    members: HashMap<UserId, Member<R>>,
}

/// Every target the hub's users want.
///
/// | key | target (channel or prefix) |
/// | --- | --- |
/// | scope | one hub |
/// | lifetime | first join ~ the last member leaves or is refused |
/// | bound | targets in use by attached users; members: their joins |
/// | cleared by | the step that empties it: a leave, a refusal |
///
/// `tickets` counts subscribe calls issued: one counter per hub, never reset.
#[derive(Debug)]
pub struct Channels<R> {
    entries: HashMap<Target, Entry<R>>,
    tickets: u64,
}

impl<R> Default for Channels<R> {
    fn default() -> Self {
        Channels { entries: HashMap::new(), tickets: 0 }
    }
}

impl<R> Channels<R> {
    pub fn new() -> Channels<R> {
        Channels::default()
    }

    /// The users who receive what `server` sends on `channel` now: those of
    /// the exact channel and of every prefix it falls under, each once.
    pub fn audience(&self, channel: &str, server: &Server) -> Vec<UserId> {
        let mut found: Vec<UserId> = Vec::new();
        let mut collect = |target: &Target| {
            if let Some(entry) = self.entries.get(target) {
                found.extend(entry.members.iter().filter(|(_, member)| member.on.contains(server)).map(|(user, _)| *user));
            }
        };
        collect(&Target::Channel(channel.into()));
        for (at, _) in channel.match_indices('.') {
            collect(&Target::Prefix(channel[..at].into()));
        }
        found.sort_unstable();
        found.dedup();
        found
    }

    /// Whether the book holds an entry for `target`: the tests' view of its bound.
    #[cfg(test)]
    pub fn contains(&self, target: &Target) -> bool {
        self.entries.contains_key(target)
    }

    /// `user` wants `target`. Answered at once if its join was already
    /// answered; otherwise when its own subscribe is confirmed.
    pub fn join(&mut self, user: UserId, target: Target, reply: R) -> Step<R> {
        let mut step = Step::none();
        let entry = self.entries.entry(target.clone()).or_insert_with(|| Entry { members: HashMap::new() });
        if let Some(member) = entry.members.get_mut(&user) {
            if member.answered {
                step.answers.push((reply, Ok(())));
            } else {
                member.waiting.push(reply);
            }
            return step;
        }
        self.tickets += 1;
        let ticket = self.tickets;
        entry.members.insert(user, Member { ticket, on: HashSet::new(), confirmed: false, answered: false, waiting: vec![reply] });
        step.calls.push(Call::Subscribe { target, ticket });
        step
    }

    /// `user` leaves `target`. A join of its still waiting is answered as cancelled.
    pub fn leave(&mut self, user: UserId, target: &Target) -> Step<R> {
        let mut step = Step::none();
        let Some(entry) = self.entries.get_mut(target) else { return step };
        if let Some(member) = entry.members.remove(&user) {
            for reply in member.waiting {
                step.answers.push((reply, Err(Rejection::unavailable("unsubscribed before the subscription took effect"))));
            }
        }
        self.tidy(target, &mut step);
        step
    }

    /// `user` leaves every target in `targets`.
    pub fn leave_all(&mut self, user: UserId, targets: impl IntoIterator<Item = Target>) -> Step<R> {
        let mut step = Step::none();
        for target in targets {
            step.absorb(self.leave(user, &target));
        }
        step
    }

    /// `server` confirmed a subscription to `target` sent with `ticket`.
    pub fn confirmed(&mut self, server: &Server, target: &Target, ticket: u64) -> Step<R> {
        let mut step = Step::none();
        let Some(entry) = self.entries.get_mut(target) else { return step };
        for member in entry.members.values_mut().filter(|member| member.ticket <= ticket) {
            member.on.insert(server.clone());
            settle(member, &mut step);
        }
        step
    }

    /// The `ticket`-th subscribe call came back: `Ok` once every connected
    /// server confirmed it, or the first refusal.
    pub fn answered(&mut self, target: &Target, ticket: u64, result: Result<(), Rejection>) -> Step<R> {
        let mut step = Step::none();
        let Some(entry) = self.entries.get_mut(target) else { return step };
        let Some(user) = entry.members.iter().find(|(_, member)| member.ticket == ticket).map(|(user, _)| *user) else { return step };
        match result {
            Ok(()) => {
                if let Some(member) = entry.members.get_mut(&user) {
                    member.confirmed = true;
                    settle(member, &mut step);
                }
            }
            Err(rejection) => {
                if let Some(member) = entry.members.remove(&user) {
                    for reply in member.waiting {
                        step.answers.push((reply, Err(Rejection::new(rejection.cause, format!("upstream refused: {}", rejection.message)))));
                    }
                }
                self.tidy(target, &mut step);
            }
        }
        step
    }

    /// `server`'s observation connection ended or started again: nothing is
    /// in effect there until it confirms again.
    pub fn reset(&mut self, server: &Server) {
        for member in self.entries.values_mut().flat_map(|entry| entry.members.values_mut()) {
            member.on.remove(server);
        }
    }

    /// Drops `target`'s entry once no user wants it, cancelling the upstream subscription.
    fn tidy(&mut self, target: &Target, step: &mut Step<R>) {
        if self.entries.get(target).is_some_and(|entry| entry.members.is_empty()) {
            self.entries.remove(target);
            step.calls.push(Call::Unsubscribe(target.clone()));
        }
    }
}

/// Answers a member's waiting joins once its call is confirmed and it is in effect somewhere.
fn settle<R>(member: &mut Member<R>, step: &mut Step<R>) {
    if member.confirmed && !member.answered && !member.on.is_empty() {
        member.answered = true;
        for reply in member.waiting.drain(..) {
            step.answers.push((reply, Ok(())));
        }
    }
}

#[cfg(test)]
mod tests;
