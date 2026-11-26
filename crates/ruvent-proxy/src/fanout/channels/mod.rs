//! The subscription book: which members of each channel receive what each
//! server sends, and what the hub
//! has to ask its observer. No task, no queue, no clock — each method takes
//! one input of the hub and returns a [`Step`]: the observer calls to make,
//! in order, and the answers owed to members who were waiting.
//!
//! The book is generic over `R`, the thing a waiting member is answered
//! through (the hub uses a one-shot channel), so it can be driven and
//! checked without any of the hub's machinery.
//!
//! **When a subscription takes effect.** Every join of a member not yet in the
//! channel issues its own subscribe, numbered with the next **ticket**: the
//! observer numbers its `subscribe` calls the same way, in the order it
//! handles them, and the hub hands it every call in the order the book
//! issues them. Each server's confirmation comes back as
//! [`Channels::confirmed`] with the ticket of the subscribe it answers — or,
//! for the subscriptions said again at a new connection, the number of calls
//! handled so far. A member becomes active **on that server** at the first
//! confirmation with `ticket ≥` its own: everything after it in the stream
//! was sent after its own subscribe took effect there. A confirmation of an
//! earlier subscribe activates nobody who joined later.
//!
//! **When a join is answered.** Once its subscribe call came back without a
//! refusal ([`Channels::answered`]) and the member is active on at least one
//! server. The observer puts every confirmation in the stream before it
//! answers the call, so a member told "subscribed" who then publishes finds
//! the confirmation ahead of the event. With no server connected the call
//! comes back at once and the member waits for the first server to connect
//! and confirm.
//!
//! **One upstream subscription per channel per server.** The observer keeps
//! one subscription per channel and says it again at each connection; a
//! second member's subscribe is the same subscription confirmed again, which
//! is what gives that member its own confirmation.

use super::SubscribeError;
use ruvent::{Channel, Refusal};
use ruvent_client::servers::Server;
use std::collections::{HashMap, HashSet};

/// One attached member, numbered by the hub.
pub type MemberId = u64;

/// A call the hub makes on its observer, in this order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Call {
    /// The `ticket`-th subscribe call.
    Subscribe {
        channel: Channel,
        ticket: u64,
    },
    Unsubscribe(Channel),
}

/// What one input to the book asks of the hub.
#[derive(Debug)]
pub struct Step<R> {
    /// Observer calls, in order.
    pub calls: Vec<Call>,
    /// Waiting members to answer.
    pub answers: Vec<(R, Result<(), SubscribeError>)>,
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

/// One member in one channel.
#[derive(Debug)]
struct Member<R> {
    /// The subscribe call that admits this member.
    ticket: u64,
    /// Servers on which the subscription is in effect for this member.
    on: HashSet<Server>,
    /// The subscribe call came back without a refusal.
    confirmed: bool,
    /// The join was answered "subscribed".
    answered: bool,
    /// Joins of this member still waiting for that answer.
    waiting: Vec<R>,
}

#[derive(Debug)]
struct Entry<R> {
    members: HashMap<MemberId, Member<R>>,
}

/// Every channel the hub's members want.
///
/// | key | channel (channel or prefix) |
/// | --- | --- |
/// | scope | one hub |
/// | lifetime | first join ~ the last member leaves or is refused |
/// | bound | channels in use by attached members; members: their joins |
/// | cleared by | the step that empties it: a leave, a refusal |
///
/// `tickets` counts subscribe calls issued: one counter per hub, never reset.
#[derive(Debug)]
pub struct Channels<R> {
    entries: HashMap<Channel, Entry<R>>,
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

    /// The members who receive what `server` sends on `channel` now.
    pub fn audience(&self, channel: &Channel, server: &Server) -> Vec<MemberId> {
        let mut found: Vec<MemberId> = match self.entries.get(channel) {
            Some(entry) => entry.members.iter().filter(|(_, member)| member.on.contains(server)).map(|(member, _)| *member).collect(),
            None => Vec::new(),
        };
        found.sort_unstable();
        found
    }

    /// Whether the book holds an entry for `channel`: the tests' view of its bound.
    #[cfg(test)]
    pub fn contains(&self, channel: &Channel) -> bool {
        self.entries.contains_key(channel)
    }

    /// `member` wants `channel`. Answered at once if its join was already
    /// answered; otherwise when its own subscribe is confirmed.
    pub fn join(&mut self, member: MemberId, channel: Channel, reply: R) -> Step<R> {
        let mut step = Step::none();
        let entry = self.entries.entry(channel).or_insert_with(|| Entry { members: HashMap::new() });
        if let Some(member) = entry.members.get_mut(&member) {
            if member.answered {
                step.answers.push((reply, Ok(())));
            } else {
                member.waiting.push(reply);
            }
            return step;
        }
        self.tickets += 1;
        let ticket = self.tickets;
        entry.members.insert(member, Member { ticket, on: HashSet::new(), confirmed: false, answered: false, waiting: vec![reply] });
        step.calls.push(Call::Subscribe { channel, ticket });
        step
    }

    /// `member` leaves `channel`. A join of its still waiting is answered as cancelled.
    pub fn leave(&mut self, member: MemberId, channel: &Channel) -> Step<R> {
        let mut step = Step::none();
        let Some(entry) = self.entries.get_mut(channel) else { return step };
        if let Some(member) = entry.members.remove(&member) {
            for reply in member.waiting {
                step.answers.push((reply, Err(SubscribeError::Left)));
            }
        }
        self.tidy(channel, &mut step);
        step
    }

    /// `member` leaves every channel in `channels`.
    pub fn leave_all(&mut self, member: MemberId, channels: impl IntoIterator<Item = Channel>) -> Step<R> {
        let mut step = Step::none();
        for channel in channels {
            step.absorb(self.leave(member, &channel));
        }
        step
    }

    /// `server` confirmed a subscription to `channel` sent with `ticket`.
    pub fn confirmed(&mut self, server: &Server, channel: &Channel, ticket: u64) -> Step<R> {
        let mut step = Step::none();
        let Some(entry) = self.entries.get_mut(channel) else { return step };
        for member in entry.members.values_mut().filter(|member| member.ticket <= ticket) {
            member.on.insert(server.clone());
            settle(member, &mut step);
        }
        step
    }

    /// The `ticket`-th subscribe call came back: `Ok` once every connected
    /// server confirmed it, or the first refusal.
    pub fn answered(&mut self, channel: &Channel, ticket: u64, result: Result<(), Refusal>) -> Step<R> {
        let mut step = Step::none();
        let Some(entry) = self.entries.get_mut(channel) else { return step };
        let Some(member) = entry.members.iter().find(|(_, member)| member.ticket == ticket).map(|(member, _)| *member) else { return step };
        match result {
            Ok(()) => {
                if let Some(member) = entry.members.get_mut(&member) {
                    member.confirmed = true;
                    settle(member, &mut step);
                }
            }
            Err(refusal) => {
                if let Some(member) = entry.members.remove(&member) {
                    for reply in member.waiting {
                        step.answers.push((reply, Err(SubscribeError::Refused(refusal))));
                    }
                }
                self.tidy(channel, &mut step);
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

    /// Drops `channel`'s entry once no member wants it, cancelling the upstream subscription.
    fn tidy(&mut self, channel: &Channel, step: &mut Step<R>) {
        if self.entries.get(channel).is_some_and(|entry| entry.members.is_empty()) {
            self.entries.remove(channel);
            step.calls.push(Call::Unsubscribe(*channel));
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
