//! The user JSON: parsing, the registration a publish becomes, rejection causes and the texts sent back.

use super::*;
use ruvent::{Bytes, Kind};

fn publish(json: &str) -> Publish {
    serde_json::from_str(json).unwrap()
}

const ID: &str = "0190f5b2-6f1e-7c4a-9d2b-3a4b5c6d7e8f";

// ── parsing user messages ────────────────────────────────────────────────

/// A publish keeps its payload as the exact JSON the user sent.
#[test]
fn a_publish_request_parses_with_its_raw_payload() {
    let request = Request::parse(r#"{"op":"publish","kind":"dispatch","channel":"c","type":"t","payload":{"a":[1, 2]},"ref":7}"#).unwrap();
    let Request::Publish(publish) = request else { panic!("not a publish") };
    assert_eq!(publish.payload.get(), r#"{"a":[1, 2]}"#, "whitespace and order are kept");
    assert_eq!(publish.reference, Some(Value::from(7)));
    assert_eq!(publish.kind, PublishKind::Dispatch);
}

/// Each op parses to its own request; subscribe and unsubscribe keep their ref.
#[test]
fn every_op_parses_to_its_request() {
    assert!(matches!(Request::parse(r#"{"op":"auth","token":"t"}"#).unwrap(), Request::Auth { token } if token == "t"));
    assert!(
        matches!(Request::parse(r#"{"op":"subscribe","channel":"c","ref":"r"}"#).unwrap(), Request::Subscribe { target: Target::Channel(channel), reference: Some(r) } if &*channel == "c" && r == "r")
    );
    assert!(matches!(Request::parse(r#"{"op":"unsubscribe","channel":"c"}"#).unwrap(), Request::Unsubscribe { target: Target::Channel(channel), reference: None } if &*channel == "c"));
}

/// A subscribe may name a prefix instead of a channel: the event server's `.`-boundary prefix target.
#[test]
fn a_subscribe_names_a_channel_or_a_prefix() {
    assert!(matches!(Request::parse(r#"{"op":"subscribe","prefix":"room"}"#).unwrap(), Request::Subscribe { target: Target::Prefix(prefix), .. } if &*prefix == "room"));
    assert!(matches!(Request::parse(r#"{"op":"unsubscribe","prefix":"room","ref":1}"#).unwrap(), Request::Unsubscribe { target: Target::Prefix(prefix), reference: Some(_) } if &*prefix == "room"));
    for text in [r#"{"op":"subscribe","channel":"a","prefix":"b"}"#, r#"{"op":"subscribe","prefix":""}"#, r#"{"op":"unsubscribe"}"#] {
        assert!(Request::parse(text).is_err(), "{text} parsed");
    }
}

/// An unknown op, a missing field, or a message that is not JSON is an error, not a guess.
#[test]
fn malformed_messages_are_errors() {
    for text in [r#"{"op":"jump"}"#, r#"{"channel":"c"}"#, r#"{"op":"subscribe"}"#, r#"{"op":"auth"}"#, "not json", r#"{"op":"publish","kind":"later","channel":"c","type":"t","payload":1}"#] {
        assert!(Request::parse(text).is_err(), "{text} parsed");
    }
}

// ── the registration a publish becomes ───────────────────────────────────

/// A dispatch and a claimable publish become events of that kind on that channel, with the payload bytes as sent.
#[test]
fn a_publish_becomes_an_event_of_its_kind() {
    let dispatch = publish(r#"{"kind":"dispatch","channel":"c","type":"t","payload":{"n":1}}"#).envelope(None).unwrap();
    assert_eq!((dispatch.kind, &*dispatch.channel, &*dispatch.event_type, &dispatch.payload[..]), (Kind::Dispatch, "c", "t", &br#"{"n":1}"#[..]));
    let claimable = publish(r#"{"kind":"claimable","channel":"jobs","type":"run","payload":2}"#).envelope(None).unwrap();
    assert_eq!(claimable.kind, Kind::Claimable);
}

/// id, correlation, reply and delegate are carried over when given, and absent when not.
#[test]
fn optional_fields_are_carried_over() {
    let full = publish(&format!(r#"{{"kind":"dispatch","channel":"c","type":"t","payload":1,"id":"{ID}","correlation":"{ID}","reply":"back"}}"#)).envelope(Some("alice")).unwrap();
    assert_eq!(full.id.to_string(), ID);
    assert_eq!(full.correlation.map(|id| id.to_string()).as_deref(), Some(ID));
    assert_eq!(full.reply.as_deref(), Some("back"));
    assert_eq!(full.delegate.as_deref(), Some("alice"));
    let bare = publish(r#"{"kind":"dispatch","channel":"c","type":"t","payload":1}"#).envelope(None).unwrap();
    assert_eq!((bare.correlation, bare.reply, bare.delegate), (None, None, None));
}

/// A malformed id or correlation is an invalid request, found by `check` before anything else runs.
#[test]
fn malformed_ids_are_invalid_requests() {
    let bad_id = publish(r#"{"kind":"dispatch","channel":"c","type":"t","payload":1,"id":"nope"}"#);
    assert_eq!(bad_id.check(), Err(Rejection::invalid("id is not a UUID")));
    assert_eq!(bad_id.envelope(None).unwrap_err().cause, Cause::Invalid);
    let bad_correlation = publish(r#"{"kind":"dispatch","channel":"c","type":"t","payload":1,"correlation":"7"}"#);
    assert_eq!(bad_correlation.check(), Err(Rejection::invalid("correlation is not a UUID")));
    assert_eq!(publish(&format!(r#"{{"kind":"dispatch","channel":"c","type":"t","payload":1,"id":"{ID}"}}"#)).check(), Ok(()));
}

/// A hook reads the payload as a type and replaces it with another value.
#[test]
fn a_payload_is_read_and_replaced_as_json() {
    let mut request = publish(r#"{"kind":"dispatch","channel":"c","type":"t","payload":{"n":1}}"#);
    let read: Value = request.payload_as().unwrap();
    assert_eq!(read["n"], 1);
    request.set_payload(&serde_json::json!({"n": 2, "by": "hook"})).unwrap();
    let sent: Value = serde_json::from_slice(&request.envelope(None).unwrap().payload).unwrap();
    assert_eq!(sent, serde_json::json!({"n": 2, "by": "hook"}));
    assert_eq!(request.payload_as::<u32>().unwrap_err().cause, Cause::Invalid);
}

// ── rejections ───────────────────────────────────────────────────────────

/// Every refusal of an answered call maps to a cause; the message is its lowercase name.
#[test]
fn every_refusal_maps_to_a_cause() {
    let expected = [
        (Refusal::Forbidden, Cause::Forbidden),
        (Refusal::Missing, Cause::Conflict),
        (Refusal::AlreadyClaimed, Cause::Conflict),
        (Refusal::Malformed, Cause::Invalid),
        (Refusal::Closed, Cause::Unavailable),
    ];
    assert_eq!(expected.len(), Refusal::ALL.len(), "every refusal is listed");
    for (refusal, cause) in expected {
        let rejection = Rejection::from_refusal(refusal);
        assert_eq!(rejection.cause, cause, "{refusal:?}");
        assert_eq!(rejection.message, format!("{refusal:?}").to_lowercase());
    }
}

/// Every fault kind (2.7) maps to a cause; the message is its lowercase name.
#[test]
fn every_fault_maps_to_a_cause() {
    let expected = [
        (FaultKind::Forbidden, Cause::Forbidden),
        (FaultKind::Malformed, Cause::Invalid),
        (FaultKind::InvalidId, Cause::Invalid),
        (FaultKind::IdConflict, Cause::Conflict),
        (FaultKind::FollowUpConflict, Cause::Conflict),
        (FaultKind::SequenceGap, Cause::Unavailable),
    ];
    assert_eq!(expected.len(), FaultKind::ALL.len(), "every kind is listed");
    for (kind, cause) in expected {
        let rejection = Rejection::from_fault(kind);
        assert_eq!((rejection.cause, rejection.message), (cause, format!("{kind:?}").to_lowercase()), "{kind:?}");
    }
}

/// A server refusing the proxy's token or grant is forbidden; every other producer or request failure is unavailable.
#[test]
fn producer_and_request_failures_map_to_a_cause() {
    for reason in ByeReason::ALL.iter().copied() {
        let expected = if matches!(reason, ByeReason::Token | ByeReason::Forbidden) { Cause::Forbidden } else { Cause::Unavailable };
        assert_eq!(Rejection::from_producer(ProducerError::Refused(reason)).cause, expected, "{reason:?}");
        assert_eq!(Rejection::from_request(RequestError::Producer(ProducerError::Refused(reason))).cause, expected);
    }
    for error in [ProducerError::BudgetSpent, ProducerError::Closed] {
        assert_eq!(Rejection::from_producer(error), Rejection::unavailable(error.to_string()));
    }
    assert_eq!(Rejection::from_request(RequestError::Timeout), Rejection::unavailable("the request's deadline passed"));
    assert_eq!(Rejection::from_request(RequestError::Closed).cause, Cause::Unavailable);
}

/// The rejection constructors set their cause, and a rejection shows its message.
#[test]
fn rejection_constructors_set_their_cause() {
    assert_eq!(Rejection::invalid("a").cause, Cause::Invalid);
    assert_eq!(Rejection::forbidden("b").cause, Cause::Forbidden);
    assert_eq!(Rejection::unavailable("c").cause, Cause::Unavailable);
    assert_eq!(Rejection::new(Cause::Conflict, "d").to_string(), "d");
}

// ── texts sent to the user ───────────────────────────────────────────────

/// A result carries op, ref, ok, id and error; the cause stays on the proxy side.
#[test]
fn a_result_text_never_shows_the_cause() {
    let ok: Value = serde_json::from_str(&Outcome::ok(Some(Value::from("r")), Some("i".into())).text()).unwrap();
    assert_eq!(ok, serde_json::json!({"op":"result","ref":"r","ok":true,"id":"i"}));
    let refused = Outcome::rejected(None, Rejection::forbidden("no"));
    assert_eq!(refused.cause, Some(Cause::Forbidden));
    let text: Value = serde_json::from_str(&refused.text()).unwrap();
    assert_eq!(text, serde_json::json!({"op":"result","ok":false,"error":"no"}));
    assert_eq!(Outcome::failed(None, "x").cause, Some(Cause::Invalid));
}

/// A JSON payload is sent as JSON, anything else as a string of its bytes.
#[test]
fn an_event_carries_json_payloads_as_json_and_others_as_strings() {
    let json = Envelope::dispatch("c", "t", Bytes::from_static(br#"{"x":1}"#));
    let text: Value = serde_json::from_str(&event_text(&json)).unwrap();
    assert_eq!((text["op"].clone(), text["channel"].clone(), text["type"].clone(), text["payload"]["x"].clone()), (Value::from("event"), Value::from("c"), Value::from("t"), Value::from(1)));
    assert_eq!(text["id"], json.id.to_string());
    let binary = Envelope::dispatch("c", "t", Bytes::from_static(b"\xffraw"));
    let text: Value = serde_json::from_str(&event_text(&binary)).unwrap();
    assert!(text["payload"].is_string());
}

/// An event's correlation is sent when it has one.
#[test]
fn an_event_shows_its_correlation() {
    let id: EventId = ID.parse().unwrap();
    let event = Envelope::dispatch("c", "t", Bytes::from_static(b"1")).with_correlation(id);
    let text: Value = serde_json::from_str(&event_text(&event)).unwrap();
    assert_eq!(text["correlation"], ID);
    let plain: Value = serde_json::from_str(&event_text(&Envelope::dispatch("c", "t", Bytes::from_static(b"1")))).unwrap();
    assert!(plain.get("correlation").is_none());
}
