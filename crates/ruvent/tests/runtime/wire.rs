//! The JSON frames every bundled transport shares: hostile input never
//! panics, and what goes through comes out unchanged.

use proptest::prelude::*;
use ruvent::wire::json::{decode_client, encode_event, encode_server, parse_from};
use ruvent::{Audience, Bytes, ClientFrame, Envelope, EventId, From, Kind, Rejected, ServerFrame};
use serde_json::{json, Value};
use std::sync::Arc;

fn value() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![Just(Value::Null), any::<bool>().prop_map(Value::from), any::<i64>().prop_map(Value::from), any::<u64>().prop_map(Value::from), ".{0,12}".prop_map(Value::from)];
    leaf.prop_recursive(3, 24, 4, |inner| {
        prop_oneof![prop::collection::vec(inner.clone(), 0..4).prop_map(Value::from), prop::collection::btree_map(".{0,6}", inner, 0..4).prop_map(|map| Value::Object(map.into_iter().collect()))]
    })
}

fn text() -> impl Strategy<Value = Option<String>> {
    prop::option::of(".{0,16}")
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 2048, ..ProptestConfig::default() })]

    /// Whatever a peer sends, decoding returns a frame or `None`. It never
    /// panics, and it never invents an action.
    #[test]
    fn hostile_input_never_panics(input in ".{0,200}") {
        if let Some(ClientFrame::Event(proposal)) = decode_client(&input) {
            prop_assert!(!proposal.action.is_empty());
        }
    }

    /// The same for input that is valid JSON of roughly the right shape.
    #[test]
    fn almost_right_frames_never_panic(kind in prop_oneof![Just("event".to_owned()), Just("subscribe".to_owned()), ".{0,8}"], body in value()) {
        let mut frame = match body { Value::Object(map) => map, other => [("payload".to_owned(), other)].into_iter().collect() };
        frame.insert("type".to_owned(), Value::from(kind));
        let _ = decode_client(&Value::Object(frame).to_string());
    }

    /// A proposal crosses the wire with every field intact, and its payload
    /// arrives as the same JSON — embedded as written, never re-encoded.
    #[test]
    fn a_proposal_crosses_the_wire_intact(action in ".{1,16}", payload in value(), key in text(), channel in text(), reply in text(), session in text(), run in text(), turn in text()) {
        let frame = json!({"type": "event", "action": action, "payload": payload, "transactionKey": key, "channel": channel, "replyChannel": reply, "sessionId": session, "runId": run, "turnId": turn});
        let Some(ClientFrame::Event(proposal)) = decode_client(&frame.to_string()) else { return Err(TestCaseError::fail("a well-formed event frame was refused")) };
        prop_assert_eq!(&*proposal.action, action.as_str());
        prop_assert_eq!(serde_json::from_slice::<Value>(&proposal.payload).unwrap(), payload);
        let field = |value: &Option<ruvent::Text>| value.as_deref().map(str::to_owned);
        prop_assert_eq!((field(&proposal.transaction_key), field(&proposal.channel), field(&proposal.reply_channel)), (key, channel, reply));
        prop_assert_eq!((field(&proposal.session_id), field(&proposal.run_id), field(&proposal.turn_id)), (session, run, turn));
    }

    /// Cursors are 64-bit and travel as decimal strings, so none loses
    /// precision — including every position above 2^53.
    #[test]
    fn every_cursor_survives_the_wire(position in any::<u64>()) {
        prop_assert_eq!(parse_from(Some(&position.to_string()), None), Some(From::Position(position)));
        let frame = json!({"type": "subscribe", "channels": ["a"], "cursor": position.to_string()});
        let Some(ClientFrame::Subscribe { from, .. }) = decode_client(&frame.to_string()) else { return Err(TestCaseError::fail("subscribe refused")) };
        prop_assert_eq!(from, From::Position(position));

        let mut event = Envelope::draft(Kind::Fact, "a", "c", "{}");
        event.position = position;
        let encoded: Value = serde_json::from_str(&encode_server(&ServerFrame::Event(Arc::new(event)))).unwrap();
        let decimal = position.to_string();
        prop_assert_eq!(encoded["cursor"].as_str(), Some(decimal.as_str()));
        prop_assert_eq!(encoded["event"]["position"].as_str(), Some(decimal.as_str()));
    }

    /// An event is always valid JSON. A JSON payload is embedded as itself;
    /// anything else becomes a string.
    #[test]
    fn every_event_encodes_as_json(payload in prop_oneof![value().prop_map(|value| Bytes::from(value.to_string())), prop::collection::vec(any::<u8>(), 0..32).prop_map(Bytes::from)], worker in any::<bool>()) {
        let mut event = Envelope::draft(Kind::Result, "job.result", "room", payload.clone());
        event.causation = Some(EventId::random());
        event.audience = if worker { Audience::Worker } else { Audience::Client };
        let encoded: Value = serde_json::from_str(&encode_event(&event)).expect("an encoded event is JSON");
        match serde_json::from_slice::<Value>(&payload) {
            Ok(parsed) => prop_assert_eq!(&encoded["payload"], &parsed),
            Err(_) => {
                let lossy = String::from_utf8_lossy(&payload);
                prop_assert_eq!(encoded["payload"].as_str(), Some(&*lossy));
            }
        }
        let id = event.event_id.to_string();
        prop_assert_eq!(encoded["eventId"].as_str(), Some(id.as_str()));
        prop_assert_eq!(encoded.get("audience").is_some(), worker, "only worker events say who they are for");
    }
}

/// The frame names are the protocol. A transport in another language reads
/// these strings, so they are fixed here.
#[test]
fn server_frame_names_are_fixed() {
    let kind = |frame: ServerFrame| serde_json::from_str::<Value>(&encode_server(&frame)).unwrap()["type"].as_str().unwrap().to_owned();
    assert_eq!(kind(ServerFrame::Ready), "ready");
    assert_eq!(kind(ServerFrame::Subscribed { channels: vec!["a".into()], cursor: 0 }), "subscribed");
    assert_eq!(kind(ServerFrame::Accepted { event_id: EventId::random(), transaction_key: "k".into() }), "accepted");
    assert_eq!(kind(ServerFrame::Rejected { reason: Rejected::Forbidden }), "rejected");
    let reasons: Vec<&str> = [Rejected::NotAdmitted, Rejected::InvalidSubscription, Rejected::Forbidden, Rejected::Unavailable].map(Rejected::as_str).into();
    assert_eq!(reasons, ["not_admitted", "invalid_subscription", "forbidden", "unavailable"]);
}

/// Where a subscription starts when the client names no cursor.
#[test]
fn a_subscription_without_a_cursor_starts_now_or_at_the_start() {
    assert_eq!(parse_from(None, None), Some(From::Now));
    assert_eq!(parse_from(None, Some("now")), Some(From::Now));
    assert_eq!(parse_from(None, Some("start")), Some(From::Start));
    assert_eq!(parse_from(None, Some("yesterday")), None);
    assert_eq!(parse_from(Some("12"), Some("start")), Some(From::Position(12)), "a cursor wins");
    assert_eq!(parse_from(Some("-1"), None), None);
}

/// A frame with no payload at all is refused; a `null` payload is a payload.
#[test]
fn a_null_payload_is_a_payload_and_a_missing_one_is_refused() {
    let Some(ClientFrame::Event(proposal)) = decode_client(r#"{"type":"event","action":"a","payload":null}"#) else { panic!("a null payload was refused") };
    assert_eq!(&proposal.payload[..], b"null");
    assert!(decode_client(r#"{"type":"event","action":"a"}"#).is_none());
}
