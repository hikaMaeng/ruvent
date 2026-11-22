//! The fixed envelope contract. Independent of any payload: the engine routes,
//! orders and deduplicates on these fields and never looks inside `payload`.
//!
//! Text fields are shared (`Arc<str>`). An event derived from another — a
//! result from its command, a reaction from its fact — inherits the stream,
//! the channel, the session and the rest by bumping reference counts, not by
//! copying strings. With events issued and consumed in bulk, that is the
//! difference between a handful of atomic increments per event and a handful
//! of heap allocations.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;
use xxhash_rust::xxh3::Xxh3;

/// Shared, immutable text.
pub type Text = Arc<str>;

/// 128-bit event identity, printed in UUID shape.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EventId([u8; 16]);

impl EventId {
    pub fn random() -> Self {
        Self(*uuid::Uuid::new_v4().as_bytes())
    }

    /// Stable identity for something that may be produced more than once.
    ///
    /// Under at-least-once delivery the same logical event is derived again on
    /// every retry; deriving its id from its meaning makes the retries collapse
    /// into one. The parts are hashed length-delimited, so `["ab", "c"]` and
    /// `["a", "bc"]` are different identities.
    pub fn derive(parts: &[&[u8]]) -> Self {
        let mut hasher = Xxh3::new();
        for part in parts {
            hasher.update(&(part.len() as u32).to_le_bytes());
            hasher.update(part);
        }
        let mut bytes = hasher.digest128().to_be_bytes();
        bytes[6] = (bytes[6] & 0x0f) | 0x80;
        Self(bytes)
    }

    /// The identity of the work a fact causes on one reactor's queue. Unique
    /// per (queue, fact) and identical on a repeat.
    pub fn reaction(queue: &str, fact: EventId) -> Self {
        Self::derive(&[b"react", queue.as_bytes(), &fact.0])
    }

    /// An identity for an event that has no meaning to derive one from. Its
    /// position in the log is already unique for ever, so no hashing is needed.
    pub(crate) fn positional(position: u64) -> Self {
        let mut bytes = [0u8; 16];
        bytes[6] = 0x70;
        bytes[8..].copy_from_slice(&position.to_be_bytes());
        Self(bytes)
    }

    pub fn parse(text: &str) -> Option<Self> {
        uuid::Uuid::parse_str(text).ok().map(|id| Self(*id.as_bytes()))
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        uuid::Uuid::from_bytes(self.0).hyphenated().fmt(f)
    }
}

impl fmt::Debug for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Kind {
    Command,
    Fact,
    Result,
    Progress,
    Failure,
    Control,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Command => "command",
            Kind::Fact => "fact",
            Kind::Result => "result",
            Kind::Progress => "progress",
            Kind::Failure => "failure",
            Kind::Control => "control",
        }
    }
}

/// Who an event is for. `Worker` events are never indexed for client reads at
/// all — not filtered late at a socket, simply absent from every channel log.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
pub enum Audience {
    #[default]
    Client,
    Worker,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Source {
    Client,
    Server,
    Worker,
    Scheduler,
}

impl Source {
    pub fn as_str(self) -> &'static str {
        match self {
            Source::Client => "client",
            Source::Server => "server",
            Source::Worker => "worker",
            Source::Scheduler => "scheduler",
        }
    }
}

/// Canonical stream identity: a session's stream when there is a session,
/// otherwise the channel's.
pub fn stream_of(session_id: Option<&str>, channel: &str) -> Text {
    match session_id {
        Some(session) if !session.is_empty() => format!("session:{session}").into(),
        _ => format!("channel:{channel}").into(),
    }
}

/// A unit of work waiting on a queue. One command is taken by one worker; work
/// for two workers is two commands.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Command {
    pub event_id: EventId,
    pub created_at: u64,
    pub action: Text,
    /// The idempotency unit. Two commands with the same key are the same
    /// event sent twice, and it runs once.
    pub transaction_key: Text,
    pub channel: Text,
    pub audience: Audience,
    pub reply_channel: Option<Text>,
    pub session_id: Option<Text>,
    pub run_id: Option<Text>,
    pub turn_id: Option<Text>,
    pub correlation_id: Option<Text>,
    /// Verified identity stamped by the gateway. Never taken from the wire.
    pub principal: Option<Text>,
    pub payload: Bytes,
}

impl Command {
    pub fn new(action: impl Into<Text>, channel: impl Into<Text>, payload: impl Into<Bytes>) -> Self {
        let event_id = EventId::random();
        Self {
            event_id,
            created_at: 0,
            action: action.into(),
            transaction_key: event_id.to_string().into(),
            channel: channel.into(),
            audience: Audience::Client,
            reply_channel: None,
            session_id: None,
            run_id: None,
            turn_id: None,
            correlation_id: None,
            principal: None,
            payload: payload.into(),
        }
    }

    pub fn transaction_key(mut self, key: impl Into<Text>) -> Self {
        self.transaction_key = key.into();
        self
    }

    pub fn session(mut self, session_id: impl Into<Text>) -> Self {
        self.session_id = Some(session_id.into());
        self
    }

    pub fn reply_to(mut self, channel: impl Into<Text>) -> Self {
        self.reply_channel = Some(channel.into());
        self
    }
}

/// An event with its place in the log.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Envelope {
    pub event_id: EventId,
    pub kind: Kind,
    pub stream: Text,
    /// Position in the whole log, assigned by the engine. Commit order is
    /// total, so this orders events within a stream, within a channel and
    /// across all of them, and is the cursor a reader resumes from. Positions
    /// only ever increase, across restarts too; they are not contiguous.
    pub position: u64,
    pub action: Text,
    pub transaction_key: Text,
    pub channel: Text,
    pub reply_channel: Option<Text>,
    pub session_id: Option<Text>,
    pub run_id: Option<Text>,
    pub turn_id: Option<Text>,
    pub causation: Option<EventId>,
    pub correlation_id: Text,
    pub principal: Option<Text>,
    pub source: Source,
    pub audience: Audience,
    pub created_at: u64,
    pub payload: Bytes,
}

impl Envelope {
    /// A standalone draft. `position` is assigned on append.
    pub fn draft(kind: Kind, action: impl Into<Text>, channel: impl Into<Text>, payload: impl Into<Bytes>) -> Self {
        let event_id = EventId::random();
        let channel = channel.into();
        let key: Text = event_id.to_string().into();
        Self {
            event_id,
            kind,
            stream: stream_of(None, &channel),
            position: 0,
            action: action.into(),
            transaction_key: key.clone(),
            channel,
            reply_channel: None,
            session_id: None,
            run_id: None,
            turn_id: None,
            causation: None,
            correlation_id: key,
            principal: None,
            source: Source::Server,
            audience: Audience::Client,
            created_at: 0,
            payload: payload.into(),
        }
    }

    /// The command event a queued command becomes when work on it begins.
    pub fn from_command(command: &Command) -> Self {
        Self {
            event_id: command.event_id,
            kind: Kind::Command,
            stream: stream_of(command.session_id.as_deref(), &command.channel),
            position: 0,
            action: command.action.clone(),
            transaction_key: command.transaction_key.clone(),
            channel: command.channel.clone(),
            reply_channel: command.reply_channel.clone(),
            session_id: command.session_id.clone(),
            run_id: command.run_id.clone(),
            turn_id: command.turn_id.clone(),
            causation: None,
            correlation_id: command.correlation_id.clone().unwrap_or_else(|| command.transaction_key.clone()),
            principal: command.principal.clone(),
            source: Source::Client,
            audience: command.audience,
            created_at: command.created_at,
            payload: command.payload.clone(),
        }
    }

    /// A follow-up that stays in the cause's stream and lands on its reply
    /// channel, inheriting session, run, turn and correlation identity. Every
    /// inherited field is a shared reference; nothing is copied.
    pub fn follow(cause: &Envelope, event_id: EventId, kind: Kind, action: Text, payload: Bytes, now: u64) -> Self {
        Self {
            event_id,
            kind,
            stream: cause.stream.clone(),
            position: 0,
            action,
            transaction_key: cause.transaction_key.clone(),
            channel: cause.reply_channel.clone().unwrap_or_else(|| cause.channel.clone()),
            reply_channel: cause.reply_channel.clone(),
            session_id: cause.session_id.clone(),
            run_id: cause.run_id.clone(),
            turn_id: cause.turn_id.clone(),
            causation: Some(cause.event_id),
            correlation_id: cause.correlation_id.clone(),
            principal: cause.principal.clone(),
            source: Source::Worker,
            audience: Audience::Client,
            created_at: now,
            payload,
        }
    }
}

#[cfg(feature = "json")]
impl Envelope {
    /// Reads the payload as JSON. The engine never does this; a handler may.
    pub fn json<T: serde::de::DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_slice(&self.payload)
    }
}

/// Serializes a value as a JSON payload.
#[cfg(feature = "json")]
pub fn json<T: Serialize>(value: &T) -> Bytes {
    Bytes::from(serde_json::to_vec(value).expect("payload types serialize without failing"))
}

/// What a handler says while it runs.
#[derive(Clone, Debug)]
pub struct Emit {
    pub action: Text,
    pub payload: Bytes,
    pub kind: Kind,
    /// What makes this the same event on a re-run. Facts have one; a burst of
    /// streamed deltas does not and is fenced by ownership instead.
    pub key: Option<Text>,
    pub audience: Audience,
}
