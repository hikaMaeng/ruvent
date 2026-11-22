//! The user-facing JSON, shared by every protocol binary.
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

use ruvent::Envelope;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_json::Value;
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
}

impl Outcome {
    pub fn ok(reference: Option<Value>, id: Option<String>) -> Self {
        Outcome { op: "result", reference, ok: true, id, error: None }
    }

    pub fn failed(reference: Option<Value>, error: impl Into<String>) -> Self {
        Outcome { op: "result", reference, ok: false, id: None, error: Some(error.into()) }
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
mod tests {
    use super::*;
    use ruvent::Bytes;

    #[test]
    fn a_publish_request_parses_with_its_raw_payload() {
        let request = Request::parse(r#"{"op":"publish","kind":"dispatch","channel":"c","type":"t","payload":{"a":[1,2]},"ref":7}"#).unwrap();
        let Request::Publish(publish) = request else { panic!("not a publish") };
        assert_eq!(publish.payload.get(), r#"{"a":[1,2]}"#);
        assert_eq!(publish.reference, Some(Value::from(7)));
        assert!(Request::parse(r#"{"op":"jump"}"#).is_err());
    }

    #[test]
    fn an_event_carries_json_payloads_as_json_and_others_as_strings() {
        let json = Envelope::dispatch("c", "t", Bytes::from_static(br#"{"x":1}"#));
        let text: Value = serde_json::from_str(&event_text(&json)).unwrap();
        assert_eq!(text["payload"]["x"], 1);
        let binary = Envelope::dispatch("c", "t", Bytes::from_static(b"\xffraw"));
        let text: Value = serde_json::from_str(&event_text(&binary)).unwrap();
        assert!(text["payload"].is_string());
    }
}
