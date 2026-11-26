use crate::io::{sniff_v1, v1_bye, Parts, RawRecord, RecordReader, HEADER};
use crate::records::{Body, Decoder, Encoder, Message, Side, ASSIGN, BYE, DELIVERY, HELLO, NAME_DEF, NOTICE, REGISTER, RESET};
use crate::{features, ProtocolError, DICTIONARY, MAX_BODY, VERSION};
use bytes::Bytes;
use proptest::prelude::*;
use ruvent::envelope::codec::Malformed;
use ruvent::{Assignment, ByeReason, Call, Channel, ClaimToken, Counters, Envelope, EventId, FaultKind, Hello, IdleLane, Name, Outbound, Refusal, Role, RootStat, Stats, TypeStat, Welcome, WorkKey};

/// A test channel: a UUIDv7-shaped id derived from a label, the same every time.
pub fn ch(label: &str) -> Channel {
    let fnv = |seed: u64| label.bytes().fold(seed, |hash, byte| (hash ^ u64::from(byte)).wrapping_mul(0x100_0000_01b3));
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&fnv(0xcbf2_9ce4_8422_2325).to_le_bytes());
    bytes[8..].copy_from_slice(&fnv(0x9e37_79b9_7f4a_7c15).to_le_bytes());
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Channel(bytes)
}

/// Splits bytes into records the way [`RecordReader`] does, without a runtime.
fn split(bytes: &[u8]) -> Vec<(RawRecord, Bytes)> {
    let mut at = 0;
    let mut records = Vec::new();
    while at < bytes.len() {
        let tag = bytes[at];
        let head = u16::from_le_bytes([bytes[at + 1], bytes[at + 2]]) as usize;
        let body = u32::from_le_bytes(bytes[at + 3..at + 7].try_into().unwrap()) as usize;
        let start = at + HEADER;
        records.push((RawRecord { tag, head: Bytes::copy_from_slice(&bytes[start..start + head]), body_len: body as u32 }, Bytes::copy_from_slice(&bytes[start + head..start + head + body])));
        at = start + head + body;
    }
    records
}

fn concat(parts: Parts) -> Vec<u8> {
    parts.concat()
}

fn encode(messages: &[Message], dictionary: usize) -> Result<Vec<u8>, ProtocolError> {
    let mut encoder = Encoder::new(dictionary);
    for message in messages {
        encoder.push(message)?;
    }
    Ok(concat(encoder.take()))
}

fn decode(bytes: &[u8], side: Side, dictionary: usize) -> Result<Vec<Message>, ProtocolError> {
    let mut decoder = Decoder::new(side, dictionary);
    let mut out = Vec::new();
    for (raw, body) in split(bytes) {
        let carries = decoder.admit(&raw)?;
        let body = if carries && !body.is_empty() { Body::Bytes(body) } else { Body::None };
        if let Some(message) = decoder.decode(&raw, body)? {
            out.push(message);
        }
    }
    Ok(out)
}

/// Encodes, then decodes, with the handshake record first so the decoder accepts the rest.
fn trip(messages: &[Message], side: Side, dictionary: usize) -> Result<Vec<Message>, ProtocolError> {
    let opener = match side {
        Side::Server => hello(Role::Produce),
        Side::Client => welcome(),
    };
    let mut all = vec![opener];
    all.extend_from_slice(messages);
    let mut decoded = decode(&encode(&all, dictionary)?, side, dictionary)?;
    decoded.remove(0);
    Ok(decoded)
}

fn hello(role: Role) -> Message {
    let hello = Hello { role, credential: Name::new(""), producer: Some([3; 16]), nonce: [4; 16], linger_ms: Some(7_200_000), acks: false, types: vec![17, 0x1234] };
    Message::Hello { version: VERSION, features: features::ALL, token: "secret".into(), hello }
}

fn welcome() -> Message {
    Message::Welcome { version: VERSION, features: features::ALL, welcome: Welcome { server_id: [9; 16], generation: 4, credential: "svc".into(), resume: 41, superseded: true } }
}

fn id(byte: u8) -> EventId {
    EventId([byte; 16])
}

fn assigned(byte: u8, payload: &'static [u8]) -> Envelope {
    let mut event = Envelope::claimable(ch("orders"), 17, Bytes::from_static(payload)).with_id(id(byte)).with_delegate("user-r").with_not_before(std::time::Duration::from_millis(9));
    event.key = Some(WorkKey { generation: 2, arrival: 300 });
    event.failures = 3;
    event.principal = Some("svc".into());
    event
}

fn every_call() -> Vec<Call> {
    let token = ClaimToken { generation: 2, seq: 1 << 40 };
    vec![
        Call::Register {
            seq: Some(1),
            event: Envelope::claimable(ch("orders"), 17, Bytes::from_static(b"payload")).with_id(id(1)).with_expires(std::time::Duration::from_secs(5)).with_group([5; 16]),
        },
        Call::Register { seq: None, event: Envelope::dispatch(ch("feed"), 18, Bytes::from(vec![7u8; 5000])) },
        Call::Delete { seq: 2, id: id(1), result: ch("results"), key: Some(WorkKey { generation: 1, arrival: 9 }) },
        Call::Delete { seq: 3, id: id(1), result: ch("results"), key: None },
        Call::Join { req: 1, channel: Some(ch("orders")), types: vec![17, 23], isolated: false, lease_ms: 30_000, dispatch: true },
        Call::Join { req: 7, channel: None, types: vec![17, u16::MAX], isolated: true, lease_ms: 1, dispatch: false },
        Call::Join { req: 8, channel: Some(ch("orders")), types: Vec::new(), isolated: true, lease_ms: 0, dispatch: false },
        Call::Window { join: 1, count: 16, bytes: 1 << 20 },
        Call::Complete {
            seq: 4,
            id: id(1),
            token,
            follow_ups: vec![Envelope::claimable(ch("next"), 1, Bytes::from_static(b"a")), Envelope::dispatch(ch("feed"), 19, Bytes::new())],
            spooled: Vec::new(),
        },
        Call::Complete { seq: 5, id: id(1), token, follow_ups: Vec::new(), spooled: Vec::new() },
        Call::Extend { id: id(1), token },
        Call::Release { id: id(1), token, delay_ms: 1500, failed: true },
        Call::Claim { req: 2, id: id(1), lease_ms: 5000 },
        Call::Subscribe { req: 3, channel: ch("chat"), after: Some(WorkKey { generation: 1, arrival: 2 }) },
        Call::Subscribe { req: 9, channel: ch("feed"), after: None },
        Call::Unsubscribe { req: 4, channel: ch("feed") },
        Call::Stats { req: 5 },
        Call::Drain { req: 6 },
    ]
}

fn stats() -> Stats {
    Stats {
        waiting: 10,
        delayed: 1,
        claimed: 2,
        oldest_ms: 300,
        memory_used: 1 << 30,
        memory_budget: 2 << 30,
        memory_limit: 3 << 30,
        depth_capacity: 100,
        depth_target: 1000,
        disk_used: 5,
        disk_budget: 6,
        disk_limit: 7,
        segments: 3,
        draining: true,
        counters: Counters {
            retransmits: 1,
            duplicates: 2,
            stale: 3,
            expired: 4,
            isolated: 5,
            reclaimed: 6,
            faults: 7,
            over_limit: 8,
            pauses: 9,
            released: 10,
            read_ahead: 11,
            spilled_bytes: 12,
            relocated: 13,
            segments_removed: 14,
            checkpoints: 15,
        },
        types: vec![TypeStat { event_type: 17, waiting: 9, oldest_ms: 300 }, TypeStat { event_type: u16::MAX, waiting: 1, oldest_ms: 0 }],
        idle_lanes: vec![IdleLane { channel: ch("orders"), event_type: 38, waiting: 4 }],
        roots: vec![RootStat { root: 17, events: 12, bytes: 4096, disk: 8192, paused: true }, RootStat { root: 0, events: 1, bytes: 1, disk: 0, paused: false }],
    }
}

fn every_outbound() -> Vec<Outbound> {
    let assignment = Assignment { event: assigned(1, b"body"), token: ClaimToken { generation: 2, seq: 77 }, lease_ms: 30_000, until: 29_000, more: true };
    let mut notice = assigned(2, b"").notice();
    notice.failures = 0;
    // An isolated event: its notice and its assignment carry the isolated bit (flag bit 9).
    let isolated = Envelope { isolated: true, failures: 5, ..notice.clone() };
    let isolated_assignment = Assignment { event: Envelope { isolated: true, ..assigned(4, b"again") }, ..assignment.clone() };
    vec![
        Outbound::Bye(ByeReason::Superseded),
        Outbound::Fault { kind: FaultKind::SequenceGap, detail: "sequence number skipped ahead" },
        Outbound::Committed { seq: 1 << 33 },
        Outbound::Paused { event_type: 17 },
        Outbound::Resumed { event_type: u16::MAX },
        Outbound::Joined { req: 1 },
        Outbound::Refused { req: 2, reason: Refusal::AlreadyClaimed },
        Outbound::Pending { join: 1 << 20 },
        Outbound::Assign(assignment.clone()),
        Outbound::Assign(isolated_assignment),
        Outbound::Claimed { req: 3, assignment: Assignment { more: false, ..assignment } },
        Outbound::Subscribed { req: 4, channel: ch("orders"), backlog: vec![notice.clone(), isolated.clone()], more: true },
        Outbound::Subscribed { req: 5, channel: ch("chat"), backlog: Vec::new(), more: false },
        Outbound::Unsubscribed { req: 6 },
        Outbound::Notice(notice),
        Outbound::Notice(isolated),
        Outbound::Delivery(Envelope::dispatch(ch("feed"), 3, Bytes::from(vec![1u8; 300])).with_id(id(3)).with_delegate("alice")),
        Outbound::StatsReply { req: 7, stats: Box::new(stats()) },
        Outbound::Draining { req: 8 },
    ]
}

#[test]
fn a_hello_record_has_these_exact_bytes() {
    let hello = Hello { role: Role::Observe, credential: Name::new("ignored"), producer: None, nonce: [0xaa; 16], linger_ms: None, acks: true, types: Vec::new() };
    let bytes = encode(&[Message::Hello { version: 2, features: 1, token: "ab".into(), hello: hello.clone() }], DICTIONARY).unwrap();
    let mut expected = vec![HELLO, 24, 0, 0, 0, 0, 0, 2, 1, 3, 2, b'a', b'b', 0];
    expected.extend_from_slice(&[0xaa; 16]);
    expected.push(0);
    assert_eq!(bytes, expected);
    // Declared types: a count, then each as a little-endian u16.
    let typed = Hello { types: vec![7, 0x1234], ..hello };
    let bytes = encode(&[Message::Hello { version: 2, features: 1, token: "ab".into(), hello: typed }], DICTIONARY).unwrap();
    let mut expected = vec![HELLO, 28, 0, 0, 0, 0, 0, 2, 1, 3, 2, b'a', b'b', 0];
    expected.extend_from_slice(&[0xaa; 16]);
    expected.extend_from_slice(&[2, 7, 0, 0x34, 0x12]);
    assert_eq!(bytes, expected);
}

/// The payload is the record's body, after a head that says how long it is.
/// The channel is the head's 16 octets and the type its u16: no name is
/// defined for either.
#[test]
fn a_register_carries_its_channel_inline_and_its_payload_as_the_body_with_no_names() {
    let event = Envelope::dispatch(ch("c"), 10, Bytes::from_static(b"xyz")).with_id(id(0x22));
    let bytes = encode(&[Message::Call(Call::Register { seq: Some(5), event })], DICTIONARY).unwrap();
    // seq 1 + head 38 (id 16, channel 16, type 2, flags 2, failures 2).
    let mut expected = vec![REGISTER, 39, 0, 3, 0, 0, 0, 5];
    expected.extend_from_slice(&[0x22; 16]);
    expected.extend_from_slice(&ch("c").0);
    expected.extend_from_slice(&[10, 0, 0x01, 0, 0, 0]);
    expected.extend_from_slice(b"xyz");
    assert_eq!(bytes, expected);
}

/// Channels and types never enter the dictionary: only the credential and
/// the end user are names. Every record that names a channel is sent
/// without a single `NameDef`.
#[test]
fn the_dictionary_carries_no_channel_and_no_type() {
    let calls = vec![
        Message::Call(Call::Register { seq: Some(1), event: Envelope::claimable(ch("orders"), 17, Bytes::from_static(b"p")) }),
        Message::Call(Call::Delete { seq: 2, id: id(1), result: ch("results"), key: None }),
        Message::Call(Call::Join { req: 1, channel: Some(ch("orders")), types: vec![17], isolated: false, lease_ms: 1, dispatch: true }),
        Message::Call(Call::Subscribe { req: 2, channel: ch("feed"), after: None }),
        Message::Call(Call::Unsubscribe { req: 3, channel: ch("feed") }),
    ];
    let bytes = encode(&calls, DICTIONARY).unwrap();
    assert!(split(&bytes).iter().all(|(raw, _)| raw.tag != NAME_DEF), "no name was defined");
    let named = encode(&[Message::Call(Call::Register { seq: Some(1), event: Envelope::claimable(ch("orders"), 17, Bytes::new()).with_delegate("user-r") })], DICTIONARY).unwrap();
    assert_eq!(split(&named).iter().filter(|(raw, _)| raw.tag == NAME_DEF).count(), 1, "the end user is the one name");
}

/// A notice written by hand from the layout decodes: the 38-byte head, the
/// isolated bit (9), then the work key.
#[test]
fn a_38_byte_head_with_the_isolated_bit_decodes() {
    let mut bytes = encode(&[welcome()], DICTIONARY).unwrap();
    bytes.extend_from_slice(&[NOTICE, 43, 0, 0, 0, 0, 0]);
    bytes.extend_from_slice(&[0x11; 16]);
    bytes.extend_from_slice(&ch("orders").0);
    bytes.extend_from_slice(&[17, 0, 0x00, 0x02, 3, 0]);
    bytes.extend_from_slice(&[2, 0, 0, 0, 5]);
    let decoded = decode(&bytes, Side::Client, DICTIONARY).unwrap();
    let Message::Outbound(Outbound::Notice(notice)) = &decoded[1] else { panic!("{decoded:?}") };
    assert_eq!((notice.id, notice.channel, notice.event_type, notice.isolated, notice.failures, notice.key), (id(0x11), ch("orders"), 17, true, 3, Some(WorkKey { generation: 2, arrival: 5 })));
    assert_eq!(notice.kind, ruvent::Kind::Claimable);
}

/// Only the server sets the isolated bit: a registration carrying it is refused before it is written.
#[test]
fn a_request_with_the_isolated_bit_is_refused() {
    let mut encoder = Encoder::new(DICTIONARY);
    encoder.push(&hello(Role::Produce)).unwrap();
    let event = Envelope { isolated: true, ..Envelope::claimable(ch("orders"), 17, Bytes::new()) };
    assert_eq!(encoder.push(&Message::Call(Call::Register { seq: Some(1), event })), Err(ProtocolError::Malformed(Malformed::Forbidden("isolated"))));
    let mut bytes = encode(&[hello(Role::Produce)], DICTIONARY).unwrap();
    bytes.extend_from_slice(&[REGISTER, 39, 0, 0, 0, 0, 0, 1]);
    bytes.extend_from_slice(&[0; 16]);
    bytes.extend_from_slice(&ch("orders").0);
    bytes.extend_from_slice(&[17, 0, 0x00, 0x02, 0, 0]);
    assert_eq!(decode(&bytes, Side::Server, DICTIONARY), Err(ProtocolError::Malformed(Malformed::Forbidden("isolated"))));
}

/// Feature bit 5 (prefix subscriptions) is gone with channel names: this build never announces it.
#[test]
fn the_prefix_feature_bit_is_reserved() {
    assert_eq!(features::ALL & (1 << 5), 0);
}

/// Every record of 2.11 round-trips (property: equal after encode and decode).
#[test]
fn every_call_and_every_server_message_round_trips() {
    let calls: Vec<Message> = every_call().into_iter().map(Message::Call).collect();
    assert_eq!(trip(&calls, Side::Server, DICTIONARY).unwrap(), calls);
    let outbound: Vec<Message> = every_outbound().into_iter().map(Message::Outbound).collect();
    assert_eq!(trip(&outbound, Side::Client, DICTIONARY).unwrap(), outbound);
    let opened = decode(&encode(&[hello(Role::Work)], DICTIONARY).unwrap(), Side::Server, DICTIONARY).unwrap();
    assert_eq!(opened, [hello(Role::Work)]);
    let welcomed = decode(&encode(&[welcome()], DICTIONARY).unwrap(), Side::Client, DICTIONARY).unwrap();
    assert_eq!(welcomed, [welcome()]);
}

/// The server's `Outbound::Welcome` goes out with this build's version and features.
#[test]
fn an_engine_welcome_is_sent_with_this_version_and_the_encoders_features() {
    let welcome = Welcome { server_id: [1; 16], generation: 1, credential: "svc".into(), resume: 0, superseded: false };
    let mut encoder = Encoder::new(DICTIONARY).with_features(features::SEQUENCE | features::OPERATE);
    encoder.push(&Message::Outbound(Outbound::Welcome(welcome.clone()))).unwrap();
    let decoded = decode(&concat(encoder.take()), Side::Client, DICTIONARY).unwrap();
    assert_eq!(decoded, [Message::Welcome { version: VERSION, features: features::SEQUENCE | features::OPERATE, welcome }]);
}

/// An assignment carries the lease left when the server starts writing it.
#[test]
fn an_assignment_carries_the_lease_left_at_the_moment_it_is_written() {
    let assignment = Assignment { event: assigned(1, b"p"), token: ClaimToken { generation: 1, seq: 1 }, lease_ms: 30_000, until: 50_000, more: false };
    let mut encoder = Encoder::new(DICTIONARY);
    encoder.push(&Message::Outbound(Outbound::Welcome(Welcome { server_id: [0; 16], generation: 1, credential: "s".into(), resume: 0, superseded: false }))).unwrap();
    encoder.set_now(45_500);
    encoder.push(&Message::Outbound(Outbound::Assign(assignment.clone()))).unwrap();
    let decoded = decode(&concat(encoder.take()), Side::Client, DICTIONARY).unwrap();
    assert_eq!(decoded[1], Message::Outbound(Outbound::Assign(Assignment { until: 4_500, ..assignment })));
}

fn arbitrary_event() -> impl Strategy<Value = Envelope> {
    (any::<[u8; 16]>(), any::<u16>(), proptest::collection::vec(any::<u8>(), 0..600), any::<bool>(), any::<[u8; 16]>(), any::<bool>()).prop_map(|(channel, kind, payload, dispatch, raw, delegate)| {
        let channel = Channel(channel);
        let mut event = if dispatch { Envelope::dispatch(channel, kind, payload) } else { Envelope::claimable(channel, kind, payload) };
        event.id = EventId(raw);
        if delegate {
            event = event.with_delegate(format!("user.{}", kind % 8));
        }
        event
    })
}

proptest! {
    /// Arbitrary registrations and completions round-trip through
    /// dictionaries small enough to reset often.
    #[test]
    fn calls_round_trip_through_small_dictionaries(events in proptest::collection::vec(arbitrary_event(), 1..30), cap in 6usize..12) {
        let mut calls = Vec::new();
        for (index, event) in events.iter().enumerate() {
            calls.push(Message::Call(Call::Register { seq: Some(index as u64 + 1), event: event.clone() }));
        }
        calls.push(Message::Call(Call::Complete { seq: 99, id: id(1), token: ClaimToken::default(), follow_ups: events.iter().take(2).cloned().collect(), spooled: Vec::new() }));
        prop_assert_eq!(trip(&calls, Side::Server, cap).unwrap(), calls);
    }

    /// No input makes the decoder panic; it refuses or decodes.
    #[test]
    fn arbitrary_bytes_never_panic_the_decoder(bytes in proptest::collection::vec(any::<u8>(), 0..400)) {
        for side in [Side::Server, Side::Client] {
            let mut decoder = Decoder::new(side, 8);
            let mut at = 0;
            while at + HEADER <= bytes.len() {
                let head = u16::from_le_bytes([bytes[at + 1], bytes[at + 2]]) as usize;
                let body = u32::from_le_bytes(bytes[at + 3..at + 7].try_into().unwrap()) as usize;
                let end = (at + HEADER + head).min(bytes.len());
                let raw = RawRecord { tag: bytes[at], head: Bytes::copy_from_slice(&bytes[at + HEADER..end]), body_len: body.min(64) as u32 };
                let body_end = (end + raw.body_len as usize).min(bytes.len());
                let _ = decoder.decode(&raw, Body::Bytes(Bytes::copy_from_slice(&bytes[end..body_end])));
                at = body_end.max(at + 1);
            }
        }
    }
}

/// Follow-ups arrive as their own records and leave the decoder as one completion.
#[test]
fn a_completion_is_handed_over_only_when_its_last_follow_up_arrived() {
    let call = Call::Complete {
        seq: 1,
        id: id(1),
        token: ClaimToken::default(),
        follow_ups: vec![Envelope::dispatch(ch("a"), 20, Bytes::from_static(b"1")), Envelope::dispatch(ch("a"), 21, Bytes::from_static(b"2"))],
        spooled: Vec::new(),
    };
    let bytes = encode(&[hello(Role::Work), Message::Call(call.clone())], DICTIONARY).unwrap();
    let mut decoder = Decoder::new(Side::Server, DICTIONARY);
    let mut handed = Vec::new();
    for (raw, body) in split(&bytes) {
        decoder.admit(&raw).unwrap();
        handed.push(decoder.decode(&raw, Body::Bytes(body)).unwrap().is_some());
        if raw.tag == 0x14 {
            assert!(decoder.gathering());
        }
    }
    // hello, complete, follow-up, follow-up: no names (the channel and the type are inline)
    assert_eq!(handed.iter().filter(|done| **done).count(), 2, "the hello and the completion: {handed:?}");
    assert!(!decoder.gathering());
}

#[test]
fn a_record_from_the_wrong_direction_or_out_of_place_is_refused() {
    let welcome_bytes = encode(&[welcome()], DICTIONARY).unwrap();
    assert_eq!(decode(&welcome_bytes, Side::Server, DICTIONARY), Err(ProtocolError::WrongDirection(0x02)));
    let call = encode(&[hello(Role::Work), Message::Call(Call::Stats { req: 1 })], DICTIONARY).unwrap();
    assert_eq!(decode(&call, Side::Client, DICTIONARY), Err(ProtocolError::WrongDirection(HELLO)));
    // Nothing before the hello.
    let early = encode(&[Message::Call(Call::Stats { req: 1 })], DICTIONARY).unwrap();
    assert!(matches!(decode(&early, Side::Server, DICTIONARY), Err(ProtocolError::Handshake(_))));
    // A second hello, and a follow-up outside a completion.
    let twice = encode(&[hello(Role::Work), hello(Role::Work)], DICTIONARY).unwrap();
    assert_eq!(decode(&twice, Side::Server, DICTIONARY), Err(ProtocolError::WrongDirection(HELLO)));
    let mut stray = encode(&[hello(Role::Work)], DICTIONARY).unwrap();
    stray.extend_from_slice(&[0x15, 0, 0, 0, 0, 0, 0]);
    assert_eq!(decode(&stray, Side::Server, DICTIONARY), Err(ProtocolError::WrongDirection(0x15)));
}

#[test]
fn unknown_tags_leftover_bytes_bodies_on_bodiless_records_and_forbidden_heads_are_refused() {
    let opened = encode(&[hello(Role::Produce)], DICTIONARY).unwrap();
    let with = |extra: &[u8]| {
        let mut bytes = opened.clone();
        bytes.extend_from_slice(extra);
        decode(&bytes, Side::Server, DICTIONARY)
    };
    assert_eq!(with(&[0x7f, 0, 0, 0, 0, 0, 0]), Err(ProtocolError::UnknownTag(0x7f)));
    assert_eq!(with(&[0x1b, 2, 0, 0, 0, 0, 0, 1, 9]), Err(ProtocolError::Malformed(Malformed::Trailing)));
    assert_eq!(with(&[0x1b, 1, 0, 2, 0, 0, 0, 1, 9, 9]), Err(ProtocolError::UnexpectedBody(0x1b)));
    // A dispatch event with a visibility time.
    let mut bad = vec![REGISTER, 40, 0, 0, 0, 0, 0, 1];
    bad.extend_from_slice(&[0; 16]);
    bad.extend_from_slice(&ch("c").0);
    bad.extend_from_slice(&[0, 0, 0x41, 0, 0, 0, 5]);
    assert_eq!(with(&bad), Err(ProtocolError::Malformed(Malformed::Forbidden("timing or group on a dispatch event"))));
}

/// A refused message writes nothing, not even the names it would have defined,
/// so the dictionaries stay in step.
#[test]
fn a_refused_message_leaves_no_name_definition_behind() {
    let mut encoder = Encoder::new(DICTIONARY);
    encoder.push(&hello(Role::Produce)).unwrap();
    let bad = Envelope::dispatch(ch("never"), 22, Bytes::new()).with_delegate("someone").with_not_before(std::time::Duration::from_secs(1));
    assert!(encoder.push(&Message::Call(Call::Register { seq: Some(1), event: bad })).is_err());
    let good = Message::Call(Call::Register { seq: Some(1), event: Envelope::dispatch(ch("never"), 22, Bytes::new()).with_delegate("someone") });
    encoder.push(&good).unwrap();
    let decoded = decode(&concat(encoder.take()), Side::Server, DICTIONARY).unwrap();
    assert_eq!(decoded[1], good);
}

#[test]
fn after_a_reset_an_old_number_is_never_read_as_a_new_name() {
    let names: Vec<Message> =
        (0..6).map(|index| Message::Outbound(Outbound::Delivery(Envelope::dispatch(ch("c"), 10, Bytes::new()).with_id(id(index)).with_delegate(format!("user-{index}"))))).collect();
    let bytes = encode(&std::iter::once(welcome()).chain(names.clone()).collect::<Vec<_>>(), 2).unwrap();
    assert!(split(&bytes).iter().filter(|(raw, _)| raw.tag == RESET).count() >= 2);
    assert_eq!(decode(&bytes, Side::Client, 2).unwrap()[1..], names[..]);
}

#[test]
fn the_receiver_holds_the_same_bound() {
    let names: Vec<Message> = (0..3).map(|index| Message::Outbound(Outbound::Delivery(Envelope::dispatch(ch("c"), 10, Bytes::new()).with_delegate(format!("u{index}"))))).collect();
    let bytes = encode(&std::iter::once(welcome()).chain(names).collect::<Vec<_>>(), 8).unwrap();
    assert_eq!(decode(&bytes, Side::Client, 2), Err(ProtocolError::Dictionary("more definitions than the bound")));
}

/// A body past the inline bound is the payload itself, shared, not a copy;
/// a small one shares the head's buffer (S11).
#[test]
fn a_large_body_is_its_own_shared_vector_entry_and_a_small_one_is_inlined() {
    let large = Bytes::from(vec![5u8; 4096]);
    let small = Bytes::from_static(b"tiny");
    let mut encoder = Encoder::new(DICTIONARY);
    encoder.push(&Message::Outbound(Outbound::Delivery(Envelope::dispatch(ch("c"), 10, large.clone())))).unwrap();
    assert!(encoder.shares(&large), "the large payload is an entry of its own, not copied");
    let mut encoder = Encoder::new(DICTIONARY);
    encoder.push(&Message::Outbound(Outbound::Delivery(Envelope::dispatch(ch("c"), 10, small.clone())))).unwrap();
    assert!(!encoder.shares(&small), "a small payload is copied next to its head");
    let mut encoder = Encoder::new(DICTIONARY);
    for payload in [small, large] {
        encoder.push(&Message::Outbound(Outbound::Delivery(Envelope::dispatch(ch("c"), 10, payload)))).unwrap();
    }
    let gathered = encoder.len() as u64;
    let parts = encoder.take();
    assert_eq!((parts.total(), concat(parts).len() as u64), (gathered, gathered));
}

/// Version 1 is refused: a version-2 hello saying 1, and a version-1 hello
/// recognised by its first bytes.
#[test]
fn version_1_is_refused() {
    let mut bytes = encode(&[hello(Role::Produce)], DICTIONARY).unwrap();
    bytes[7] = 1;
    assert_eq!(decode(&bytes, Side::Server, DICTIONARY), Err(ProtocolError::Version(1)));
    let v1_hello = [11, 0, 0, 0, 1, 0, 0, 0, 0x01, 1, 0, 2, 0, b'a', b'b'];
    assert!(sniff_v1(&v1_hello));
    assert!(!sniff_v1(&encode(&[hello(Role::Produce)], DICTIONARY).unwrap()));
    let bye = v1_bye("unsupported protocol version 1");
    assert_eq!(&bye[..9], &[4 + 1 + 2 + 30, 0, 0, 0, 1, 0, 0, 0, BYE]);
}

/// Bodies are read into their own buffers: below the direct threshold copied
/// out of the read buffer, at or past it straight from the stream.
#[tokio::test]
async fn records_are_read_head_first_and_each_body_into_its_own_buffer() {
    let big = Bytes::from((0..10_000u32).map(|n| n as u8).collect::<Vec<u8>>());
    let messages = [
        welcome(),
        Message::Outbound(Outbound::Delivery(Envelope::dispatch(ch("c"), 10, Bytes::from_static(b"small")))),
        Message::Outbound(Outbound::Delivery(Envelope::dispatch(ch("c"), 10, big.clone()))),
        Message::Outbound(Outbound::Committed { seq: 3 }),
    ];
    let bytes = encode(&messages, DICTIONARY).unwrap();
    let (mut client, server) = tokio::io::duplex(1024);
    let writer = tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        client.write_all(&bytes).await.unwrap();
    });
    let mut reader = RecordReader::new(server);
    let mut decoder = Decoder::new(Side::Client, DICTIONARY);
    let mut decoded = Vec::new();
    while decoded.len() < messages.len() {
        let raw = reader.head().await.unwrap().unwrap();
        let body = if decoder.admit(&raw).unwrap() { Body::Bytes(reader.body(raw.body_len).await.unwrap()) } else { Body::None };
        if let Some(message) = decoder.decode(&raw, body).unwrap() {
            decoded.push(message);
        }
    }
    writer.await.unwrap();
    assert_eq!(decoded, messages);
}

/// A body over the bound is refused from its header, before anything is allocated for it.
#[tokio::test]
async fn a_body_over_the_bound_is_refused_from_its_header() {
    let header = [DELIVERY, 0, 0, 0, 0, 0, 2];
    let mut reader = RecordReader::with_limits(&header[..], 4096, MAX_BODY);
    let error = reader.head().await.unwrap_err();
    assert!(error.to_string().contains("exceed the bound"), "{error}");
    let assign = [ASSIGN, 0, 0, 1, 0, 0, 0];
    let mut reader = RecordReader::with_limits(&assign[..], 4096, 0);
    assert!(reader.head().await.is_err());
}

/// A writer that takes a few bytes at a time still gets every byte, in order.
#[tokio::test]
async fn a_partial_write_continues_where_it_stopped() {
    struct Trickle(Vec<u8>);
    impl tokio::io::AsyncWrite for Trickle {
        fn poll_write(mut self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>, buf: &[u8]) -> std::task::Poll<std::io::Result<usize>> {
            let take = buf.len().min(3);
            self.0.extend_from_slice(&buf[..take]);
            std::task::Poll::Ready(Ok(take))
        }
        fn poll_flush(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }
    let messages: Vec<Message> = std::iter::once(welcome()).chain(every_outbound().into_iter().map(Message::Outbound)).collect();
    let mut encoder = Encoder::new(DICTIONARY);
    for message in &messages {
        encoder.push(message).unwrap();
    }
    let mut parts = encoder.take();
    let mut trickle = Trickle(Vec::new());
    parts.write_to(&mut trickle).await.unwrap();
    assert_eq!(parts.written(), parts.total());
    assert_eq!(decode(&trickle.0, Side::Client, DICTIONARY).unwrap(), messages);
}
