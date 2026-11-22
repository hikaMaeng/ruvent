//! JSON framing.
//!
//! The one place a payload is touched, and only to carry it: a payload that is
//! itself JSON is embedded verbatim, without being parsed into a tree and
//! written back out. Anything else travels as a JSON string.
//!
//! Positions are decimal strings. They are 64-bit, and a JSON number stops
//! being exact at 2^53.

use crate::engine::From;
use crate::envelope::{Audience, Envelope};
use crate::gateway::{ClientFrame, Proposal, ServerFrame};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClientDto<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    channels: Option<Vec<String>>,
    cursor: Option<&'a str>,
    from: Option<&'a str>,
    action: Option<String>,
    #[serde(borrow)]
    payload: Option<&'a RawValue>,
    transaction_key: Option<String>,
    channel: Option<String>,
    reply_channel: Option<String>,
    session_id: Option<String>,
    run_id: Option<String>,
    turn_id: Option<String>,
}

/// Reads a start position: a cursor wins, then `"start"`, otherwise now.
pub fn parse_from(cursor: Option<&str>, from: Option<&str>) -> Option<From> {
    match (cursor, from) {
        (Some(cursor), _) => cursor.parse().ok().map(From::Position),
        (None, Some("start")) => Some(From::Start),
        (None, Some("now") | None) => Some(From::Now),
        (None, Some(_)) => None,
    }
}

/// Parses one client frame. `None` means the frame is not one this protocol has.
pub fn decode_client(text: &str) -> Option<ClientFrame> {
    let dto: ClientDto = serde_json::from_str(text).ok()?;
    match dto.kind {
        "subscribe" => Some(ClientFrame::Subscribe { channels: dto.channels?, from: parse_from(dto.cursor, dto.from)? }),
        "event" => Some(ClientFrame::Event(Proposal {
            action: dto.action.filter(|action| !action.is_empty())?.into(),
            payload: Bytes::copy_from_slice(dto.payload?.get().as_bytes()),
            transaction_key: dto.transaction_key.map(Into::into),
            channel: dto.channel.map(Into::into),
            reply_channel: dto.reply_channel.map(Into::into),
            session_id: dto.session_id.map(Into::into),
            run_id: dto.run_id.map(Into::into),
            turn_id: dto.turn_id.map(Into::into),
        })),
        _ => None,
    }
}

#[derive(Serialize)]
#[serde(untagged)]
enum Payload<'a> {
    Raw(&'a RawValue),
    Text(std::borrow::Cow<'a, str>),
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EnvelopeDto<'a> {
    event_id: String,
    kind: &'static str,
    stream_id: &'a str,
    position: String,
    action: &'a str,
    transaction_key: &'a str,
    channel: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    reply_channel: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    run_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    turn_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    causation_event_id: Option<String>,
    correlation_id: &'a str,
    source: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    audience: Option<&'static str>,
    created_at: u64,
    payload: Payload<'a>,
}

fn envelope_dto(event: &Envelope) -> EnvelopeDto<'_> {
    let payload = match std::str::from_utf8(&event.payload).ok().and_then(|text| serde_json::from_str::<&RawValue>(text).ok()) {
        Some(raw) => Payload::Raw(raw),
        None => Payload::Text(String::from_utf8_lossy(&event.payload)),
    };
    EnvelopeDto {
        event_id: event.event_id.to_string(),
        kind: event.kind.as_str(),
        stream_id: &event.stream,
        position: event.position.to_string(),
        action: &event.action,
        transaction_key: &event.transaction_key,
        channel: &event.channel,
        reply_channel: event.reply_channel.as_deref(),
        session_id: event.session_id.as_deref(),
        run_id: event.run_id.as_deref(),
        turn_id: event.turn_id.as_deref(),
        causation_event_id: event.causation.map(|id| id.to_string()),
        correlation_id: &event.correlation_id,
        source: event.source.as_str(),
        audience: (event.audience == Audience::Worker).then_some("worker"),
        created_at: event.created_at,
        payload,
    }
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum ServerDto<'a> {
    Ready,
    Subscribed { channels: &'a [String], cursor: String },
    Event { event: Box<EnvelopeDto<'a>>, cursor: String },
    #[serde(rename_all = "camelCase")]
    Accepted { event_id: String, transaction_key: &'a str },
    Rejected { reason: &'static str },
}

pub fn encode_server(frame: &ServerFrame) -> String {
    let dto = match frame {
        ServerFrame::Ready => ServerDto::Ready,
        ServerFrame::Subscribed { channels, cursor } => ServerDto::Subscribed { channels, cursor: cursor.to_string() },
        ServerFrame::Event(event) => ServerDto::Event { event: Box::new(envelope_dto(event)), cursor: event.position.to_string() },
        ServerFrame::Accepted { event_id, transaction_key } => ServerDto::Accepted { event_id: event_id.to_string(), transaction_key },
        ServerFrame::Rejected { reason } => ServerDto::Rejected { reason: reason.as_str() },
    };
    serde_json::to_string(&dto).expect("server frames contain nothing that cannot be serialized")
}

/// One event as a JSON value, for transports that build their own response bodies.
pub fn encode_event(event: &Envelope) -> String {
    serde_json::to_string(&envelope_dto(event)).expect("an envelope contains nothing that cannot be serialized")
}
