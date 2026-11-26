//! The one event envelope.
//!
//! Claimable and dispatch events share one layout and differ only in the kind
//! bit of `flags`. The ledger holds the metadata of a claimable event; the
//! envelope is what goes in and out.
//!
//! ```text
//!  fixed head, 38 bytes (every event)
//!  ┌──── id 16 ────┬── channel 16 ──┬type 2┬flags 2┬failures 2┐
//!  work key (claimable, when the server sends or journals it)
//!  ┌── generation 4 · arrival varint ──┐
//!  optional fields (only those whose flag bit is set)
//!  principal 4 · delegate 4 · not_before varint · expires varint · group 16
//!  payload: length 4 + bytes
//! ```
//!
//! The byte layout is [`codec`]. Which of these blocks a record carries is
//! fixed by the record kind, [`codec::Layout`].
//!
//! ## Why the type is here
//!
//! The envelope holds only what the server acts on. The type is a 16-bit
//! number the server routes claimable work by: a worker joins a channel, a
//! set of types, or both (`Call::Join`), so one worker can take one kind of
//! request from many channels — a channel per request — without knowing the
//! channels in advance. The server never reads what a type means; projects
//! number their own (plan 9.14). Dispatch is routed by channel alone.
//!
//! The type is also what the server's settings and grants are keyed by:
//! which credential may register, claim or read an event, whether it is
//! journaled, when it is isolated, how much of it may wait (plan 9.15).
//! Type 0 is the server's own: delete outcomes ([`crate::delete_outcome`]).
//!
//! ## The channel is an id, not a name
//!
//! A channel is a 16-byte UUIDv7 ([`Channel`]): a stream of events, typically
//! one per request, made by whoever starts the stream. It carries no meaning
//! the server reads — no hierarchy, no prefix, no pattern — so a fresh channel
//! per request costs no dictionary entry and no configuration, and leaves
//! nothing behind once its events are gone. Anything else is refused.
//!
//! ## Times
//!
//! `not_before` and `expires` are milliseconds. In a registration request
//! (and a follow-up) they are **durations from the moment the server applies
//! it**. Everywhere the server holds or sends them — the ledger, the journal,
//! an assignment — they are **absolute values on the server's accumulated
//! uptime clock**, which survives restarts and does not count the time the
//! server was down.

pub mod codec;

use crate::names::Name;
use bytes::Bytes;
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

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

/// A channel: a UUIDv7 in RFC 9562 octet order (see the module's "The
/// channel is an id, not a name"). The server compares it and never reads it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Channel(pub [u8; 16]);

impl Channel {
    /// A fresh channel: a UUIDv7.
    pub fn new() -> Self {
        Channel(uuid::Uuid::now_v7().into_bytes())
    }

    /// Whether the server accepts it: version 7, RFC 9562 variant.
    pub fn is_valid(&self) -> bool {
        self.0[6] >> 4 == 7 && self.0[8] >> 6 == 0b10
    }
}

impl From<EventId> for Channel {
    /// The channel with the same 16 bytes: a request's own id as its stream.
    fn from(id: EventId) -> Self {
        Channel(id.0)
    }
}

impl fmt::Display for Channel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&uuid::Uuid::from_bytes(self.0).hyphenated(), f)
    }
}

impl fmt::Debug for Channel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl FromStr for Channel {
    type Err = uuid::Error;
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Ok(Channel(uuid::Uuid::parse_str(text)?.into_bytes()))
    }
}

/// The type of the server's own events: delete outcomes. No producer may register it.
pub const SERVER_TYPE: u16 = 0;

/// The name of one retransmission buffer: a producer (2.3). 16 bytes.
pub type ProducerId = [u8; 16];

/// A `group` (B26): events of one group on a channel with `serialize_group`
/// are assigned one at a time. The server compares it and never reads it.
pub type GroupId = [u8; 16];

/// What the server assigns to each registration: `(generation, arrival)`.
///
/// It survives reclaims and restarts, a new registration under a reused id
/// gets a new one, and the waiting order of a lane is its numeric order. The
/// generation rises only when the server starts; the arrival never wraps.
/// Field order matters: the derived ordering compares the generation first.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Default)]
pub struct WorkKey {
    pub generation: u32,
    pub arrival: u64,
}

/// The name of one attempt: issued fresh at every assignment and compared at
/// completion, extension and release. Rises monotonically; workers use it as
/// a fencing token.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Default)]
pub struct ClaimToken {
    pub generation: u32,
    pub seq: u64,
}

/// What happens to an event: one worker claims it, or it is sent to every subscriber.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Kind {
    /// Waits until a completion removes it.
    Claimable,
    /// Handed to the channel's subscribers and gone in the same step.
    Dispatch,
}

/// A payload digest: its length and CRC32. "Is this the same content" is
/// decided by it, never by reading the payload twice.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub struct Digest {
    pub len: u32,
    pub crc: u32,
}

impl Digest {
    pub fn of(payload: &[u8]) -> Self {
        Digest { len: u32::try_from(payload.len()).unwrap_or(u32::MAX), crc: crc32fast::hash(payload) }
    }
}

/// One event. Cloning copies the head and shares the names and the payload.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Envelope {
    pub id: EventId,
    pub channel: Channel,
    /// The event's type: a number its project defines, routed by (see the module's "Why the type is here").
    pub event_type: u16,
    pub kind: Kind,
    /// Failures counted against this event: lease expiries and releases that
    /// said "this event failed". Saturates at 65,535.
    pub failures: u16,
    /// Set by the server on claimable events.
    pub key: Option<WorkKey>,
    /// Set by the server: the event reached its type's failure threshold and
    /// waits only for joins that ask for isolated events (B6).
    pub isolated: bool,
    /// The registering connection's service credential. Set by the server only.
    pub principal: Option<Name>,
    /// The end user a proxy registered this on behalf of.
    pub delegate: Option<Name>,
    /// Visible from this time on (see the module's "Times").
    pub not_before: Option<u64>,
    /// Removed, never assigned, from this time on (see the module's "Times").
    pub expires: Option<u64>,
    /// Serialization group (B26).
    pub group: Option<GroupId>,
    pub payload: Bytes,
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

impl Envelope {
    fn draft(kind: Kind, channel: Channel, event_type: u16, payload: impl Into<Bytes>) -> Self {
        Envelope {
            id: EventId::NIL,
            channel,
            event_type,
            kind,
            failures: 0,
            key: None,
            isolated: false,
            principal: None,
            delegate: None,
            not_before: None,
            expires: None,
            group: None,
            payload: payload.into(),
        }
    }

    /// A claimable event to register; the server assigns the id unless [`Envelope::with_id`] sets one.
    pub fn claimable(channel: Channel, event_type: u16, payload: impl Into<Bytes>) -> Self {
        Self::draft(Kind::Claimable, channel, event_type, payload)
    }

    /// A dispatch event to register.
    pub fn dispatch(channel: Channel, event_type: u16, payload: impl Into<Bytes>) -> Self {
        Self::draft(Kind::Dispatch, channel, event_type, payload)
    }

    pub fn with_id(mut self, id: EventId) -> Self {
        self.id = id;
        self
    }

    pub fn with_delegate(mut self, user: impl Into<Name>) -> Self {
        self.delegate = Some(user.into());
        self
    }

    /// Visible only `delay` after the server applies the registration.
    pub fn with_not_before(mut self, delay: Duration) -> Self {
        self.not_before = Some(millis(delay));
        self
    }

    /// Removed if not completed within `life` of the server applying the registration.
    pub fn with_expires(mut self, life: Duration) -> Self {
        self.expires = Some(millis(life));
        self
    }

    pub fn with_group(mut self, group: GroupId) -> Self {
        self.group = Some(group);
        self
    }

    /// The `flags` word this envelope encodes with, before a layout masks it.
    pub fn flags(&self) -> u16 {
        let mut flags = 0;
        for (present, bit) in [
            (self.kind == Kind::Dispatch, codec::KIND),
            (self.isolated, codec::ISOLATED),
            (self.principal.is_some(), codec::PRINCIPAL),
            (self.delegate.is_some(), codec::DELEGATE),
            (self.not_before.is_some(), codec::NOT_BEFORE),
            (self.expires.is_some(), codec::EXPIRES),
            (self.group.is_some(), codec::GROUP),
        ] {
            if present {
                flags |= bit;
            }
        }
        flags
    }

    /// Every name this envelope carries in `layout`, in encoding order: the
    /// credential and the end user, the only names left (the channel is an id).
    pub fn names(&self, layout: codec::Layout) -> impl Iterator<Item = &Name> {
        let optional = layout.has_optional();
        [self.principal.as_ref(), self.delegate.as_ref()].into_iter().filter(move |_| optional).flatten()
    }

    /// The same event as a notice: the head and the work key, nothing else.
    pub fn notice(&self) -> Envelope {
        Envelope { principal: None, delegate: None, not_before: None, expires: None, group: None, payload: Bytes::new(), ..self.clone() }
    }
}

#[cfg(test)]
mod tests;
