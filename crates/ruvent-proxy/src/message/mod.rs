//! The user-facing JSON, shared by every protocol.
//!
//! User → proxy:
//!
//! ```json
//! {"op":"auth","token":"…"}
//! {"op":"publish","kind":"claimable"|"dispatch","channel":"…","type":"…","payload":<any JSON>,
//!  "id":"<uuid>"?,"reply":"…"?,"correlation":"<uuid>"?,"ref":<any JSON>?}
//! {"op":"subscribe","channel":"…","ref":<any JSON>?}
//! {"op":"unsubscribe","channel":"…","ref":<any JSON>?}
//! ```
//!
//! Proxy → user:
//!
//! ```json
//! {"op":"event","channel":"…","type":"…","id":"<uuid>","payload":<JSON>,"correlation":"<uuid>"?}
//! {"op":"result","ref":<as sent>,"ok":true|false,"id":"<uuid>"?,"error":"…"?}
//! ```
//!
//! A payload travels as the JSON bytes the user sent. An event payload that
//! is not JSON is shown as a JSON string of its bytes, decoded lossily.

use ruvent::{Bytes, Envelope, EventId, Refusal};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_json::Value;
use std::fmt;
use std::sync::Arc;

/// One message from a user.
#[derive(Debug)]
pub enum Request {
    Auth { token: String },
    Publish(Publish),
    Subscribe { channel: String, reference: Option<Value> },
    Unsubscribe { channel: String, reference: Option<Value> },
}

#[derive(Deserialize)]
struct Head {
    op: String,
}

#[derive(Deserialize)]
struct Token {
    token: String,
}

#[derive(Deserialize)]
struct ChannelRequest {
    channel: String,
    #[serde(rename = "ref", default)]
    reference: Option<Value>,
}

impl Request {
    /// Reads one user message. The `op` field is read first and the message
    /// is then read again as that kind: an internally tagged enum would buffer
    /// the payload and lose its raw JSON.
    pub fn parse(text: &str) -> Result<Request, serde_json::Error> {
        let head: Head = serde_json::from_str(text)?;
        Ok(match head.op.as_str() {
            "auth" => Request::Auth { token: serde_json::from_str::<Token>(text)?.token },
            "publish" => Request::Publish(serde_json::from_str(text)?),
            "subscribe" => {
                let request: ChannelRequest = serde_json::from_str(text)?;
                Request::Subscribe { channel: request.channel, reference: request.reference }
            }
            "unsubscribe" => {
                let request: ChannelRequest = serde_json::from_str(text)?;
                Request::Unsubscribe { channel: request.channel, reference: request.reference }
            }
            other => return Err(serde::de::Error::custom(format!("unknown op `{other}`"))),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PublishKind {
    Claimable,
    Dispatch,
}

/// A registration a user asks for. Also the body of `POST /v1/events`.
///
/// A publish hook may change it before it is registered: the fields are
/// public, and [`Publish::set_payload`] replaces the payload.
#[derive(Debug, Deserialize)]
pub struct Publish {
    pub kind: PublishKind,
    pub channel: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub payload: Box<RawValue>,
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub reply: Option<String>,
    #[serde(default)]
    pub correlation: Option<String>,
    #[serde(rename = "ref", default)]
    pub reference: Option<Value>,
}

impl Publish {
    /// The request's own shape: `id` and `correlation`, when present, are UUIDs.
    pub fn check(&self) -> Result<(), Rejection> {
        self.ids().map(drop)
    }

    fn ids(&self) -> Result<(Option<EventId>, Option<EventId>), Rejection> {
        let parse = |field: &Option<String>, what: &str| field.as_deref().map(str::parse::<EventId>).transpose().map_err(|_| Rejection::invalid(format!("{what} is not a UUID")));
        Ok((parse(&self.id, "id")?, parse(&self.correlation, "correlation")?))
    }

    /// The event this registration becomes, carrying `delegate` when given.
    pub fn envelope(&self, delegate: Option<&str>) -> Result<Envelope, Rejection> {
        let (id, correlation) = self.ids()?;
        let payload = Bytes::from(self.payload.get().as_bytes().to_vec());
        let mut event = match self.kind {
            PublishKind::Claimable => Envelope::claimable(self.channel.as_str(), self.event_type.as_str(), payload),
            PublishKind::Dispatch => Envelope::dispatch(self.channel.as_str(), self.event_type.as_str(), payload),
        };
        if let Some(id) = id {
            event = event.with_id(id);
        }
        if let Some(correlation) = correlation {
            event = event.with_correlation(correlation);
        }
        if let Some(reply) = &self.reply {
            event = event.with_reply(reply.as_str());
        }
        if let Some(delegate) = delegate {
            event = event.with_delegate(delegate);
        }
        Ok(event)
    }

    /// The payload read as `T`.
    pub fn payload_as<T: DeserializeOwned>(&self) -> Result<T, Rejection> {
        serde_json::from_str(self.payload.get()).map_err(|error| Rejection::invalid(format!("payload: {error}")))
    }

    /// Replaces the payload with `value` as JSON.
    pub fn set_payload<T: Serialize>(&mut self, value: &T) -> Result<(), Rejection> {
        let text = serde_json::to_string(value).map_err(|error| Rejection::invalid(format!("payload: {error}")))?;
        self.payload = RawValue::from_string(text).map_err(|error| Rejection::invalid(format!("payload: {error}")))?;
        Ok(())
    }
}

/// Why a request was turned down. A protocol picks its status from it
/// (HTTP: 400, 403, 409, 503); the user sees the message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cause {
    /// The request is wrong in itself.
    Invalid,
    /// The user may not do this.
    Forbidden,
    /// It clashes with what is there (an event with this id is waiting).
    Conflict,
    /// The proxy or the event server cannot take it now.
    Unavailable,
}

/// A request turned down: by a hook, by the proxy, or by the event server.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rejection {
    pub cause: Cause,
    pub message: String,
}

impl Rejection {
    pub fn new(cause: Cause, message: impl Into<String>) -> Self {
        Rejection { cause, message: message.into() }
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Rejection::new(Cause::Invalid, message)
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Rejection::new(Cause::Forbidden, message)
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Rejection::new(Cause::Unavailable, message)
    }

    /// What the event server's refusal means to a user.
    pub fn from_refusal(refusal: Refusal) -> Self {
        // No wildcard: a new refusal is a decision about its status.
        let cause = match refusal {
            Refusal::Duplicate | Refusal::AlreadyClaimed | Refusal::NotClaimed | Refusal::StaleToken => Cause::Conflict,
            Refusal::Forbidden | Refusal::WrongCredential => Cause::Forbidden,
            Refusal::Closed => Cause::Unavailable,
            Refusal::InvalidId | Refusal::Malformed | Refusal::Missing => Cause::Invalid,
        };
        Rejection::new(cause, refusal.to_string().to_lowercase())
    }
}

impl fmt::Display for Rejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Rejection {}

/// The answer to one request.
#[derive(Debug, Serialize, PartialEq)]
pub struct Outcome {
    op: &'static str,
    #[serde(rename = "ref", skip_serializing_if = "Option::is_none")]
    pub reference: Option<Value>,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Why it failed; not sent to the user.
    #[serde(skip)]
    pub cause: Option<Cause>,
}

impl Outcome {
    pub fn ok(reference: Option<Value>, id: Option<String>) -> Self {
        Outcome { op: "result", reference, ok: true, id, error: None, cause: None }
    }

    /// A failure of the request itself ([`Cause::Invalid`]).
    pub fn failed(reference: Option<Value>, error: impl Into<String>) -> Self {
        Outcome::rejected(reference, Rejection::invalid(error))
    }

    pub fn rejected(reference: Option<Value>, rejection: Rejection) -> Self {
        Outcome { op: "result", reference, ok: false, id: None, error: Some(rejection.message), cause: Some(rejection.cause) }
    }

    /// The JSON text sent to the user.
    pub fn text(&self) -> Arc<str> {
        Arc::from(serde_json::to_string(self).unwrap_or_else(|_| r#"{"op":"result","ok":false,"error":"unencodable"}"#.into()))
    }
}

#[derive(Serialize)]
struct EventOut<'a> {
    op: &'static str,
    channel: &'a str,
    #[serde(rename = "type")]
    event_type: &'a str,
    id: String,
    payload: &'a RawValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    correlation: Option<String>,
}

/// The JSON text of a delivered event, made once and shared by every user it goes to.
pub fn event_text(event: &Envelope) -> Arc<str> {
    let payload_text = match std::str::from_utf8(&event.payload) {
        Ok(text) if serde_json::from_str::<&RawValue>(text).is_ok() => text.to_owned(),
        _ => serde_json::to_string(&String::from_utf8_lossy(&event.payload)).unwrap_or_else(|_| "null".into()),
    };
    let Ok(payload) = RawValue::from_string(payload_text) else { return Arc::from(r#"{"op":"event","payload":null}"#) };
    let out = EventOut { op: "event", channel: &event.channel, event_type: &event.event_type, id: event.id.to_string(), payload: &payload, correlation: event.correlation.map(|id| id.to_string()) };
    Arc::from(serde_json::to_string(&out).unwrap_or_default())
}

#[cfg(test)]
mod tests;
