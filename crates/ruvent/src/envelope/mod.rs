//! The one event envelope.
//!
//! Claimable and dispatch events share one layout and differ only in the kind
//! bit of `flags`. Marking a claim fills the claim block of the same envelope,
//! in place, in the engine's resident copy. See
//! `docs/proposals/event-server-redesign.md` §6.0.
//!
//! ```text
//!  fixed head, 27 bytes (every event)
//!  ┌──────────── id 16 ────────────┬─ channel 4 ─┬── type 4 ──┬flags 1┬retry 2┐
//!  work key, 8 bytes (claimable, when the server sends it)
//!  ┌──── generation 4 · arrival 4 ────┐
//!  claim block, 24 bytes (claimed, sent to the claimer)
//!  ┌──── token 8 ────┬ claimer 4 ┬ worker 4 ┬──── deadline 8 ────┐
//!  optional fields (only those whose flag bit is set)
//!  reply 4 · correlation 16 · principal 4 · delegate 4
//!  payload: length 4 + bytes
//! ```
//!
//! The byte layout is [`codec`]. Which of these blocks a record carries is
//! fixed by the record kind, [`codec::Layout`].

pub mod codec;

use crate::names::Name;
use bytes::Bytes;
use std::fmt;
use std::str::FromStr;

/// A 16-byte event id in RFC 9562 octet order. All zero means "the server assigns one".
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct EventId(pub [u8; 16]);

impl EventId {
    pub const NIL: EventId = EventId([0; 16]);

    /// A fresh UUIDv7: 48 bits of milliseconds since the Unix epoch, then random bits.
    pub fn new_v7() -> Self {
        EventId(uuid::Uuid::now_v7().into_bytes())
    }

    pub fn is_nil(&self) -> bool {
        self.0 == [0; 16]
    }

    /// Whether this is a well-formed UUID: version 1 to 8, RFC 9562 variant.
    pub fn is_uuid(&self) -> bool {
        let version = self.0[6] >> 4;
        let variant = self.0[8] >> 6;
        (1..=8).contains(&version) && variant == 0b10
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&uuid::Uuid::from_bytes(self.0).hyphenated(), f)
    }
}

impl fmt::Debug for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl FromStr for EventId {
    type Err = uuid::Error;
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Ok(EventId(uuid::Uuid::parse_str(text)?.into_bytes()))
    }
}

/// What the server assigns to each registration: `(generation, arrival)`.
///
/// It survives reclaims and restarts, a new registration under a reused id
/// gets a new one, and the waiting order of the claimable queue is its
/// numeric order. Field order matters: the derived ordering compares the
/// generation first.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Default)]
pub struct WorkKey {
    pub generation: u32,
    pub arrival: u32,
}

/// Issued fresh at every claim and compared at completion: `(generation, sequence)`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Default)]
pub struct ClaimToken {
    pub generation: u32,
    pub seq: u32,
}

/// The claim block.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Claim {
    pub token: ClaimToken,
    /// Attribution number: names the `(service credential, connection)` the
    /// claim was made under. Never reused within a generation.
    pub claimer: u32,
    /// Whatever the claimer tagged itself with. The server stores it and never reads it.
    pub worker: u32,
    /// Milliseconds on the server's monotonic clock.
    pub deadline: u64,
}

/// What happens to an event: one worker claims it, or it is sent to every subscriber.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Kind {
    /// Waits in the claimable queue until a completion report removes it.
    Claimable,
    /// Handed to the channel's subscribers and gone in the same step.
    Dispatch,
}

/// One event. Cloning copies the head and shares the names and the payload.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Envelope {
    pub id: EventId,
    pub channel: Name,
    /// The event's type, called `type` in the layout.
    pub event_type: Name,
    pub kind: Kind,
    /// Times this event was reclaimed after a missed deadline. Saturates at
    /// 65,535, which then means "65,535 or more".
    pub retry: u16,
    /// Set by the server on claimable events.
    pub key: Option<WorkKey>,
    /// Set while claimed.
    pub claim: Option<Claim>,
    /// Channel for the result.
    pub reply: Option<Name>,
    /// Ties one flow together.
    pub correlation: Option<EventId>,
    /// The registering connection's service credential. Set by the server only.
    pub principal: Option<Name>,
    /// The end user a proxy registered this on behalf of.
    pub delegate: Option<Name>,
    pub payload: Bytes,
}

impl Envelope {
    fn draft(kind: Kind, channel: impl Into<Name>, event_type: impl Into<Name>, payload: impl Into<Bytes>) -> Self {
        Envelope {
            id: EventId::NIL,
            channel: channel.into(),
            event_type: event_type.into(),
            kind,
            retry: 0,
            key: None,
            claim: None,
            reply: None,
            correlation: None,
            principal: None,
            delegate: None,
            payload: payload.into(),
        }
    }

    /// A claimable event to register; the server assigns the id unless [`Envelope::with_id`] sets one.
    pub fn claimable(channel: impl Into<Name>, event_type: impl Into<Name>, payload: impl Into<Bytes>) -> Self {
        Self::draft(Kind::Claimable, channel, event_type, payload)
    }

    /// A dispatch event to register.
    pub fn dispatch(channel: impl Into<Name>, event_type: impl Into<Name>, payload: impl Into<Bytes>) -> Self {
        Self::draft(Kind::Dispatch, channel, event_type, payload)
    }

    pub fn with_id(mut self, id: EventId) -> Self {
        self.id = id;
        self
    }

    pub fn with_reply(mut self, channel: impl Into<Name>) -> Self {
        self.reply = Some(channel.into());
        self
    }

    pub fn with_correlation(mut self, correlation: EventId) -> Self {
        self.correlation = Some(correlation);
        self
    }

    pub fn with_delegate(mut self, user: impl Into<Name>) -> Self {
        self.delegate = Some(user.into());
        self
    }

    pub fn is_claimed(&self) -> bool {
        self.claim.is_some()
    }

    /// The `flags` byte this envelope encodes with, before a layout masks it.
    pub fn flags(&self) -> u8 {
        let mut flags = 0;
        if self.kind == Kind::Dispatch {
            flags |= codec::KIND;
        }
        if self.claim.is_some() {
            flags |= codec::CLAIMED;
        }
        if self.reply.is_some() {
            flags |= codec::REPLY;
        }
        if self.correlation.is_some() {
            flags |= codec::CORRELATION;
        }
        if self.principal.is_some() {
            flags |= codec::PRINCIPAL;
        }
        if self.delegate.is_some() {
            flags |= codec::DELEGATE;
        }
        flags
    }

    /// Every name this envelope carries in `layout`, in encoding order.
    pub fn names(&self, layout: codec::Layout) -> impl Iterator<Item = &Name> {
        let optional = layout.has_optional();
        [Some(&self.channel), Some(&self.event_type)]
            .into_iter()
            .chain([self.reply.as_ref(), self.principal.as_ref(), self.delegate.as_ref()].into_iter().map(move |name| if optional { name } else { None }))
            .flatten()
    }

    /// The same event as a notice: the head and the work key, nothing else.
    pub fn notice(&self) -> Envelope {
        Envelope { claim: None, reply: None, correlation: None, principal: None, delegate: None, payload: Bytes::new(), ..self.clone() }
    }
}

#[cfg(test)]
mod tests;
