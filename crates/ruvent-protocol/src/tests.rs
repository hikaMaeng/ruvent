use crate::records::{Decoder, Encoder, Message, Side, DELIVERY, NAME, RESET};
use crate::{ProtocolError, DICTIONARY, MAX_FRAME};
use bytes::Bytes;
use proptest::prelude::*;
use ruvent::envelope::codec::Malformed;
use ruvent::{Call, Claim, ClaimToken, Envelope, EventId, Kind, Name, Outbound, Refusal, Reply, WorkKey};

/// Encodes messages, then decodes every frame (length field stripped).
fn trip(messages: &[Message], side: Side, dictionary: usize) -> Result<Vec<Message>, ProtocolError> {
    let mut encoder = Encoder::new(dictionary, MAX_FRAME);
    for message in messages {
        encoder.push(message)?;
    }
    let mut decoder = Decoder::new(side, dictionary, MAX_FRAME);
    let mut out = Vec::new();
    for frame in encoder.take() {
        let length = u32::from_le_bytes(frame[..4].try_into().unwrap()) as usize;
        assert_eq!(length, frame.len() - 4, "the length field counts the bytes after itself");
        out.extend(decoder.decode(frame.slice(4..))?);
    }
    Ok(out)
}

fn frame_bytes(messages: &[Message]) -> Vec<u8> {
    let mut encoder = Encoder::new(DICTIONARY, MAX_FRAME);
    for message in messages {
        encoder.push(message).unwrap();
    }
    encoder.take().concat()
}

#[test]
fn a_hello_frame_has_these_exact_bytes() {
    let bytes = frame_bytes(&[Message::Hello { version: 1, token: "ab".into() }]);
    assert_eq!(bytes, [11, 0, 0, 0, 1, 0, 0, 0, 0x01, 1, 0, 2, 0, b'a', b'b']);
}

#[test]
fn a_claim_call_has_these_exact_bytes() {
    let call = Call::Claim { id: EventId([0x11; 16]), worker: 7, lease_ms: 30_000 };
    let bytes = frame_bytes(&[Message::Call { req: 5, call }]);
    let mut expected = vec![4 + 1 + 4 + 16 + 4 + 8, 0, 0, 0, 1, 0, 0, 0, 0x22, 5, 0, 0, 0];
    expected.extend_from_slice(&[0x11; 16]);
    expected.extend_from_slice(&[7, 0, 0, 0]);
    expected.extend_from_slice(&30_000u64.to_le_bytes());
    assert_eq!(bytes, expected);
}

/// Names are defined in the stream before the record that uses them.
#[test]
fn a_delivery_defines_its_names_first() {
    let event = Envelope::dispatch("c", "t", Bytes::from_static(b"x")).with_id(EventId([0x22; 16]));
    let bytes = frame_bytes(&[Message::Outbound(Outbound::Delivery(event))]);
    let mut expected = vec![0, 0, 0, 0, 3, 0, 0, 0];
    expected.extend_from_slice(&[NAME, 0, 0, 0, 0, 1, 0, b'c']);
    expected.extend_from_slice(&[NAME, 1, 0, 0, 0, 1, 0, b't']);
    expected.push(DELIVERY);
    expected.extend_from_slice(&[0x22; 16]);
    expected.extend_from_slice(&[0, 0, 0, 0, 1, 0, 0, 0, 0x01, 0, 0, 1, 0, 0, 0, b'x']);
    let length = (expected.len() - 4) as u32;
    expected[..4].copy_from_slice(&length.to_le_bytes());
    assert_eq!(bytes, expected);
}

#[test]
fn every_method_and_every_reply_round_trips() {
    let key = WorkKey { generation: 2, arrival: 9 };
    let claimed = Envelope {
        key: Some(key),
        claim: Some(Claim { token: ClaimToken { generation: 2, seq: 1 }, claimer: 3, worker: 4, deadline: 99 }),
        principal: Some("svc".into()),
        ..Envelope::claimable("orders", "create", Bytes::from_static(b"p")).with_id(EventId::new_v7())
    };
    let calls = vec![
        Call::RegisterClaimable(Envelope::claimable("orders", "create", Bytes::from_static(b"{}")).with_reply("r").with_correlation(EventId::new_v7())),
        Call::RegisterDispatch(Envelope::dispatch("feed", "post", Bytes::new()).with_delegate("alice")),
        Call::Claim { id: EventId::new_v7(), worker: 1, lease_ms: 5 },
        Call::Complete {
            id: EventId::new_v7(),
            token: ClaimToken { generation: 1, seq: 2 },
            follow_ups: vec![Envelope::claimable("a", "b", Bytes::new()), Envelope::dispatch("c", "d", Bytes::new())],
        },
        Call::Delete { id: EventId::new_v7(), result: "results".into(), tag: EventId::new_v7(), key: Some(key) },
        Call::Delete { id: EventId::new_v7(), result: "results".into(), tag: EventId::new_v7(), key: None },
        Call::Subscribe { channel: "orders".into(), after: Some(key) },
        Call::Subscribe { channel: "orders".into(), after: None },
        Call::Unsubscribe { channel: "orders".into() },
    ];
    let messages: Vec<Message> = calls.into_iter().enumerate().map(|(req, call)| Message::Call { req: req as u32, call }).collect();
    assert_eq!(trip(&messages, Side::Server, DICTIONARY).unwrap(), messages);

    let mut replies = vec![
        Reply::Registered { id: EventId::new_v7(), key },
        Reply::Accepted { id: EventId::new_v7() },
        Reply::Claimed(claimed.clone()),
        Reply::Completed,
        Reply::Deleted { removed: true },
        Reply::Subscribed { channel: "orders".into(), backlog: vec![claimed.notice()], more: true },
        Reply::Unsubscribed,
    ];
    replies.extend(Refusal::ALL.iter().map(|refusal| Reply::Refused(*refusal)));
    let mut messages: Vec<Message> = replies.into_iter().enumerate().map(|(req, reply)| Message::Outbound(Outbound::Reply { req: req as u32, reply })).collect();
    messages.push(Message::Outbound(Outbound::Notice(claimed.notice())));
    messages.push(Message::Outbound(Outbound::Delivery(Envelope { principal: Some("svc".into()), ..Envelope::dispatch("feed", "post", Bytes::from_static(b"hi")).with_id(EventId::new_v7()) })));
    messages.push(Message::Welcome { version: 1, credential: "svc".into() });
    messages.push(Message::Bye { reason: "bye".into() });
    assert_eq!(trip(&messages, Side::Client, DICTIONARY).unwrap(), messages);
}

#[test]
fn a_record_from_the_wrong_direction_refuses_the_frame() {
    let hello = Message::Hello { version: 1, token: "t".into() };
    assert_eq!(trip(&[hello], Side::Client, DICTIONARY).unwrap_err(), ProtocolError::WrongDirection(0x01));
    let notice = Message::Outbound(Outbound::Reply { req: 1, reply: Reply::Completed });
    assert_eq!(trip(&[notice], Side::Server, DICTIONARY).unwrap_err(), ProtocolError::WrongDirection(0x33));
}

fn decode_raw(side: Side, body: &[u8]) -> Result<Vec<Message>, ProtocolError> {
    Decoder::new(side, DICTIONARY, MAX_FRAME).decode(Bytes::copy_from_slice(body))
}

#[test]
fn unknown_tags_leftover_bytes_and_forbidden_bodies_refuse_the_frame() {
    assert_eq!(decode_raw(Side::Server, &[1, 0, 0, 0, 0x7f]).unwrap_err(), ProtocolError::UnknownTag(0x7f));
    assert_eq!(decode_raw(Side::Server, &[1, 0, 0, 0, 0x11, 0]).unwrap_err(), ProtocolError::Malformed(Malformed::Trailing));
    assert_eq!(decode_raw(Side::Server, &[2, 0, 0, 0, 0x11]).unwrap_err(), ProtocolError::Malformed(Malformed::Short));
    // A registration that claims to be claimed.
    let mut body = vec![3, 0, 0, 0, NAME, 0, 0, 0, 0, 1, 0, b'c', 0x20, 1, 0, 0, 0];
    body.extend_from_slice(&EventId::new_v7().0);
    body.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0, 0x02, 0, 0, 0, 0, 0, 0]);
    assert_eq!(decode_raw(Side::Server, &body).unwrap_err(), ProtocolError::Malformed(Malformed::Forbidden("claimed")));
    // A refusal code nobody defined.
    assert_eq!(decode_raw(Side::Client, &[1, 0, 0, 0, 0x37, 1, 0, 0, 0, 200]).unwrap_err(), ProtocolError::UnknownRefusal(200));
    // Redefining a number without a reset.
    assert_eq!(
        decode_raw(Side::Server, &[2, 0, 0, 0, NAME, 0, 0, 0, 0, 1, 0, b'a', NAME, 0, 0, 0, 0, 1, 0, b'b']).unwrap_err(),
        ProtocolError::Dictionary("a number was defined twice without a reset")
    );
}

#[test]
fn a_frame_over_the_bound_is_refused_on_both_ends() {
    let mut encoder = Encoder::new(DICTIONARY, 64);
    let big = Envelope::dispatch("c", "t", Bytes::from(vec![0u8; 100]));
    assert!(matches!(encoder.push(&Message::Outbound(Outbound::Delivery(big))), Err(ProtocolError::FrameTooLarge(_))));
    assert!(matches!(Decoder::new(Side::Client, DICTIONARY, 64).decode(Bytes::from(vec![0u8; 100])), Err(ProtocolError::FrameTooLarge(100))));
}

/// Many small messages are split into frames that each stay under the bound.
#[test]
fn messages_are_split_across_frames_under_the_bound() {
    let mut encoder = Encoder::new(DICTIONARY, 256);
    let messages: Vec<Message> = (0..50).map(|req| Message::Call { req, call: Call::Claim { id: EventId::new_v7(), worker: 0, lease_ms: 1 } }).collect();
    for message in &messages {
        encoder.push(message).unwrap();
    }
    let frames = encoder.take();
    assert!(frames.len() > 1);
    let mut decoder = Decoder::new(Side::Server, DICTIONARY, 256);
    let mut decoded = Vec::new();
    for frame in frames {
        assert!(frame.len() - 4 <= 256);
        decoded.extend(decoder.decode(frame.slice(4..)).unwrap());
    }
    assert_eq!(decoded, messages);
}

/// After a reset, a number from before it is unknown: it can never be read as a newer name.
#[test]
fn after_a_reset_an_old_number_is_never_read_as_a_new_name() {
    let delivery = |channel: &str| Message::Outbound(Outbound::Delivery(Envelope::dispatch(channel, "t", Bytes::new()).with_id(EventId::new_v7())));
    let mut encoder = Encoder::new(2, MAX_FRAME);
    encoder.push(&delivery("a")).unwrap(); // defines a=0, t=1
    encoder.push(&delivery("b")).unwrap(); // full: reset, then b=0, t=1
    let frame = encoder.take().remove(0);
    assert!(frame.contains(&RESET));
    let mut decoder = Decoder::new(Side::Client, 2, MAX_FRAME);
    let decoded = decoder.decode(frame.slice(4..)).unwrap();
    let channels: Vec<String> = decoded
        .iter()
        .map(|message| match message {
            Message::Outbound(Outbound::Delivery(event)) => event.channel.to_string(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(channels, ["a", "b"]);
    // A record that still uses number 2 (never defined since the reset) is refused.
    let mut stale = vec![1, 0, 0, 0, DELIVERY];
    stale.extend_from_slice(&EventId::new_v7().0);
    stale.extend_from_slice(&[2, 0, 0, 0, 1, 0, 0, 0, 0x01, 0, 0, 0, 0, 0, 0]);
    assert_eq!(decoder.decode(Bytes::from(stale)).unwrap_err(), ProtocolError::Malformed(Malformed::UnknownName(2)));
}

#[test]
fn the_receiver_holds_the_same_bound() {
    let mut body = vec![3, 0, 0, 0];
    for number in 0..3u8 {
        body.extend_from_slice(&[NAME, number, 0, 0, 0, 1, 0, b'a' + number]);
    }
    assert_eq!(Decoder::new(Side::Server, 2, MAX_FRAME).decode(Bytes::from(body)).unwrap_err(), ProtocolError::Dictionary("more definitions than the bound"));
}

fn name() -> impl Strategy<Value = Name> {
    "[a-e]{1,3}".prop_map(Name::from)
}

fn request() -> impl Strategy<Value = Envelope> {
    (any::<bool>(), name(), name(), proptest::option::of(name()), proptest::option::of(any::<[u8; 16]>()), proptest::option::of(name()), proptest::collection::vec(any::<u8>(), 0..16)).prop_map(
        |(dispatch, channel, event_type, reply, correlation, delegate, payload)| Envelope {
            kind: if dispatch { Kind::Dispatch } else { Kind::Claimable },
            reply,
            correlation: correlation.map(EventId),
            delegate,
            ..Envelope::claimable(channel, event_type, Bytes::from(payload))
        },
    )
}

fn call() -> impl Strategy<Value = Call> {
    let key = proptest::option::of((any::<u32>(), any::<u32>()).prop_map(|(generation, arrival)| WorkKey { generation, arrival }));
    prop_oneof![
        request().prop_map(|event| if event.kind == Kind::Claimable { Call::RegisterClaimable(event) } else { Call::RegisterDispatch(event) }),
        (any::<[u8; 16]>(), any::<u32>(), any::<u64>()).prop_map(|(id, worker, lease_ms)| Call::Claim { id: EventId(id), worker, lease_ms }),
        (any::<[u8; 16]>(), any::<(u32, u32)>(), proptest::collection::vec(request(), 0..4)).prop_map(|(id, (generation, seq), follow_ups)| Call::Complete {
            id: EventId(id),
            token: ClaimToken { generation, seq },
            follow_ups
        }),
        (any::<[u8; 16]>(), name(), any::<[u8; 16]>(), key.clone()).prop_map(|(id, result, tag, key)| Call::Delete { id: EventId(id), result, tag: EventId(tag), key }),
        (name(), key).prop_map(|(channel, after)| Call::Subscribe { channel, after }),
        name().prop_map(|channel| Call::Unsubscribe { channel }),
    ]
}

proptest! {
    /// Any sequence of calls survives the trip, with a dictionary small enough to reset often.
    #[test]
    fn calls_round_trip_through_small_dictionaries(calls in proptest::collection::vec(call(), 1..20), dictionary in 12usize..40) {
        let messages: Vec<Message> = calls.into_iter().enumerate().map(|(req, call)| Message::Call { req: req as u32, call }).collect();
        prop_assert_eq!(trip(&messages, Side::Server, dictionary).unwrap(), messages);
    }

    /// No byte string panics either decoder.
    #[test]
    fn arbitrary_bytes_never_panic_the_decoder(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
        let _ = decode_raw(Side::Server, &bytes);
        let _ = decode_raw(Side::Client, &bytes);
    }
}
