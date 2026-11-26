use crate::io::{sniff_v1, v1_bye, Parts, RawRecord, RecordReader, HEADER};
use crate::records::{Body, Decoder, Encoder, Message, Side, ASSIGN, BYE, DELIVERY, EXTEND, HELLO, REGISTER};
use crate::{features, ProtocolError, MAX_BODY, VERSION};
use bytes::Bytes;
use proptest::prelude::*;
use ruvent::envelope::codec::Malformed;
use ruvent::{Assignment, ByeReason, Call, Channel, Counters, Envelope, EventId, FaultKind, Hello, IdleLane, Name, Outbound, Refusal, Role, Stats, TypeStat, Welcome, WorkKey, WorkerId};

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

fn encode(messages: &[Message]) -> Result<Vec<u8>, ProtocolError> {
    let mut encoder = Encoder::new();
    for message in messages {
        encoder.push(message)?;
    }
    Ok(concat(encoder.take()))
}

fn decode(bytes: &[u8], side: Side) -> Result<Vec<Message>, ProtocolError> {
    let mut decoder = Decoder::new(side);
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
fn trip(messages: &[Message], side: Side) -> Result<Vec<Message>, ProtocolError> {
    let opener = match side {
        Side::Server => hello(Role::Produce),
        Side::Client => welcome(),
    };
    let mut all = vec![opener];
    all.extend_from_slice(messages);
    let mut decoded = decode(&encode(&all)?, side)?;
    decoded.remove(0);
    Ok(decoded)
}

fn hello(role: Role) -> Message {
    let hello = Hello { role, credential: Name::new(""), producer: Some([3; 16]), nonce: [4; 16], linger_ms: Some(7_200_000), acks: false };
    Message::Hello { version: VERSION, features: features::ALL, token: "secret".into(), hello }
}

fn welcome() -> Message {
    Message::Welcome { version: VERSION, features: features::ALL, welcome: Welcome { server_id: [9; 16], generation: 4, credential: "svc".into(), resume: 41, superseded: true } }
}

fn id(byte: u8) -> EventId {
    EventId([byte; 16])
}

/// The worker uuid the tests' claims name.
const WORKER: WorkerId = [0x77; 16];

fn assigned(byte: u8, payload: &'static [u8]) -> Envelope {
    let mut event = Envelope::claimable(ch("orders"), 17, Bytes::from_static(payload)).with_id(id(byte)).with_not_before(std::time::Duration::from_millis(9));
    event.key = Some(WorkKey { generation: 2, arrival: 300 });
    event.failures = 3;
    event.worker = Some(WORKER);
    event
}

fn every_call() -> Vec<Call> {
    let worker = WORKER;
    vec![
        Call::Register { seq: Some(1), event: Envelope::claimable(ch("orders"), 17, Bytes::from_static(b"payload")).with_id(id(1)).with_not_before(std::time::Duration::from_secs(5)) },
        Call::Register { seq: None, event: Envelope::dispatch(ch("feed"), 18, Bytes::from(vec![7u8; 5000])) },
        Call::Delete { seq: 2, id: id(1), result: ch("results"), key: Some(WorkKey { generation: 1, arrival: 9 }) },
        Call::Delete { seq: 3, id: id(1), result: ch("results"), key: None },
        Call::Join { req: 1, worker, channel: Some(ch("orders")), types: vec![17, 23], lease_ms: 30_000, dispatch: true },
        Call::Join { req: 7, worker: [0xff; 16], channel: None, types: vec![17, u16::MAX], lease_ms: 1, dispatch: false },
        Call::Join { req: 8, worker: [1; 16], channel: Some(ch("orders")), types: Vec::new(), lease_ms: 0, dispatch: false },
        Call::Window { join: 1, count: 16, bytes: 1 << 20 },
        Call::Complete {
            seq: 4,
            id: id(1),
            worker,
            follow_ups: vec![Envelope::claimable(ch("next"), 1, Bytes::from_static(b"a")), Envelope::dispatch(ch("feed"), 19, Bytes::new())],
            spooled: Vec::new(),
        },
        Call::Complete { seq: 5, id: id(1), worker, follow_ups: Vec::new(), spooled: Vec::new() },
        Call::Extend { id: id(1), worker },
        Call::Release { id: id(1), worker, delay_ms: 1500, failed: true },
        Call::Claim { req: 2, id: id(1), worker, lease_ms: 5000 },
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
        counters: Counters { retransmits: 1, duplicates: 2, stale: 3, reclaimed: 4, faults: 5, released: 6, read_ahead: 7, relocated: 8, segments_removed: 9, checkpoints: 10 },
        types: vec![TypeStat { event_type: 17, waiting: 9, oldest_ms: 300 }, TypeStat { event_type: u16::MAX, waiting: 1, oldest_ms: 0 }],
        idle_lanes: vec![IdleLane { channel: ch("orders"), event_type: 38, waiting: 4 }],
    }
}

fn every_outbound() -> Vec<Outbound> {
    let assignment = Assignment { event: assigned(1, b"body"), lease_ms: 30_000, until: 29_000, more: true };
    let mut notice = assigned(2, b"").notice();
    notice.failures = 0;
    let failed = Envelope { failures: 5, ..notice.clone() };
    // Another worker uuid, a large lease and no `not_before`.
    let other = Assignment { event: Envelope { not_before: None, worker: Some([1; 16]), ..assigned(4, b"again") }, lease_ms: 1 << 40, ..assignment.clone() };
    vec![
        Outbound::Bye(ByeReason::Superseded),
        Outbound::Fault { kind: FaultKind::SequenceGap, detail: "sequence number skipped ahead" },
        Outbound::Committed { seq: 1 << 33 },
        Outbound::Joined { req: 1 },
        Outbound::Refused { req: 2, reason: Refusal::AlreadyClaimed },
        Outbound::Pending { join: 1 << 20 },
        Outbound::Assign(assignment.clone()),
        Outbound::Assign(other),
        Outbound::Claimed { req: 3, assignment: Assignment { more: false, ..assignment } },
        Outbound::Subscribed { req: 4, channel: ch("orders"), backlog: vec![notice.clone(), failed.clone()], more: true },
        Outbound::Subscribed { req: 5, channel: ch("chat"), backlog: Vec::new(), more: false },
        Outbound::Unsubscribed { req: 6 },
        Outbound::Notice(notice),
        Outbound::Notice(failed),
        Outbound::Delivery(Envelope::dispatch(ch("feed"), 3, Bytes::from(vec![1u8; 300])).with_id(id(3))),
        Outbound::StatsReply { req: 7, stats: Box::new(stats()) },
        Outbound::Draining { req: 8 },
    ]
}

#[test]
fn a_hello_record_has_these_exact_bytes() {
    let hello = Hello { role: Role::Observe, credential: Name::new("ignored"), producer: None, nonce: [0xaa; 16], linger_ms: None, acks: true };
    let bytes = encode(&[Message::Hello { version: 2, features: 1, token: "ab".into(), hello }]).unwrap();
    // version, features, role, token, presence, nonce: no type list since version 5.
    let mut expected = vec![HELLO, 23, 0, 0, 0, 0, 0, 2, 1, 3, 2, b'a', b'b', 0];
    expected.extend_from_slice(&[0xaa; 16]);
    assert_eq!(bytes, expected);
}

/// The payload is the record's body, after a head that says how long it is.
/// The channel is the head's 16 octets and the type its u16: no name is
/// defined for either.
#[test]
fn a_register_carries_its_channel_inline_and_its_payload_as_the_body_with_no_names() {
    let event = Envelope::dispatch(ch("c"), 10, Bytes::from_static(b"xyz")).with_id(id(0x22));
    let bytes = encode(&[Message::Call(Call::Register { seq: Some(5), event })]).unwrap();
    // seq 1 + head 38 (id 16, channel 16, type 2, flags 2, failures 2).
    let mut expected = vec![REGISTER, 39, 0, 3, 0, 0, 0, 5];
    expected.extend_from_slice(&[0x22; 16]);
    expected.extend_from_slice(&ch("c").0);
    expected.extend_from_slice(&[10, 0, 0x01, 0, 0, 0]);
    expected.extend_from_slice(b"xyz");
    assert_eq!(bytes, expected);
}

/// Version 5 has no names: no record of any message defines one, and the
/// old dictionary tags (0x08, 0x09) and pause tags (0x21, 0x22) are unknown
/// in both directions.
#[test]
fn no_record_defines_a_name_and_the_retired_tags_are_unknown() {
    let mut messages = vec![hello(Role::Produce)];
    messages.extend(every_call().into_iter().map(Message::Call));
    let bytes = encode(&messages).unwrap();
    assert!(split(&bytes).iter().all(|(raw, _)| !matches!(raw.tag, 0x08 | 0x09)), "no NameDef, no DictReset");
    let mut messages = vec![welcome()];
    messages.extend(every_outbound().into_iter().map(Message::Outbound));
    let bytes = encode(&messages).unwrap();
    assert!(split(&bytes).iter().all(|(raw, _)| !matches!(raw.tag, 0x08 | 0x09 | 0x21 | 0x22)), "no dictionary or pause record");
    for (opener, side) in [(hello(Role::Produce), Side::Server), (welcome(), Side::Client)] {
        for tag in [0x08u8, 0x09, 0x21, 0x22] {
            let mut bytes = encode(std::slice::from_ref(&opener)).unwrap();
            bytes.extend_from_slice(&[tag, 1, 0, 0, 0, 0, 0, 0]);
            assert_eq!(decode(&bytes, side), Err(ProtocolError::UnknownTag(tag)), "tag {tag:#04x} on {side:?}");
        }
    }
}

/// An assignment written by hand from the layout decodes: its head (lease,
/// remaining lease, more), the 38-byte event head, the work key,
/// `not_before` (flag bit 1) and the worker uuid (flag bit 2, 16 octets: no
/// token). The same event head is refused where a worker does not belong.
#[test]
fn a_38_byte_head_with_not_before_and_a_worker_decodes_only_as_an_assignment() {
    let mut head = Vec::new();
    head.extend_from_slice(&[0x11; 16]);
    head.extend_from_slice(&ch("orders").0);
    head.extend_from_slice(&[17, 0, 0x06, 0x00, 3, 0]);
    assert_eq!(head.len(), 38);
    let mut assign = vec![7, 5, 1];
    assign.extend_from_slice(&head);
    assign.extend_from_slice(&[2, 0, 0, 0, 44]);
    assign.push(9);
    assign.extend_from_slice(&[0x77; 16]);
    let mut bytes = encode(&[welcome()]).unwrap();
    bytes.extend_from_slice(&[ASSIGN, assign.len() as u8, 0, 2, 0, 0, 0]);
    bytes.extend_from_slice(&assign);
    bytes.extend_from_slice(b"hi");
    let decoded = decode(&bytes, Side::Client).unwrap();
    let Message::Outbound(Outbound::Assign(assignment)) = &decoded[1] else { panic!("{decoded:?}") };
    let event = &assignment.event;
    assert_eq!((event.id, event.channel, event.event_type, event.failures, event.key, event.not_before), (id(0x11), ch("orders"), 17, 3, Some(WorkKey { generation: 2, arrival: 44 }), Some(9)));
    assert_eq!(event.worker, Some([0x77; 16]));
    assert_eq!((assignment.lease_ms, assignment.until, assignment.more), (7, 5, true));
    assert_eq!(&event.payload[..], b"hi");
    // In a registration: the worker is a field it does not carry.
    let mut bytes = encode(&[hello(Role::Produce)]).unwrap();
    bytes.extend_from_slice(&[REGISTER, 39, 0, 0, 0, 0, 0, 1]);
    let mut request = head.clone();
    request[36] = 0;
    bytes.extend_from_slice(&request);
    assert_eq!(decode(&bytes, Side::Server), Err(ProtocolError::Malformed(Malformed::Forbidden("a field this record does not carry"))));
    // In a delivery: neither `not_before` nor a worker.
    let mut delivery = head.clone();
    delivery[36] = 0;
    delivery[34] = 0x06;
    let mut bytes = encode(&[welcome()]).unwrap();
    bytes.extend_from_slice(&[DELIVERY, 38, 0, 0, 0, 0, 0]);
    bytes.extend_from_slice(&delivery);
    assert_eq!(decode(&bytes, Side::Client), Err(ProtocolError::Malformed(Malformed::Forbidden("a field this record does not carry"))));
}

/// An assignment without a worker uuid is refused before it is written, and
/// a request's worker is never written (only the server sets one).
#[test]
fn an_assignment_needs_a_worker_and_a_request_never_carries_one() {
    let mut encoder = Encoder::new();
    encoder.push(&welcome()).unwrap();
    let bare = Assignment { event: Envelope { worker: None, ..assigned(1, b"p") }, lease_ms: 1, until: 1, more: false };
    assert_eq!(encoder.push(&Message::Outbound(Outbound::Assign(bare))), Err(ProtocolError::Malformed(Malformed::Missing("worker"))));
    let marked = Envelope { worker: Some(WORKER), ..Envelope::claimable(ch("orders"), 17, Bytes::new()) };
    let decoded = trip(&[Message::Call(Call::Register { seq: Some(1), event: marked.clone() })], Side::Server).unwrap();
    assert_eq!(decoded, [Message::Call(Call::Register { seq: Some(1), event: Envelope { worker: None, ..marked } })]);
}

/// A claim request carries the worker uuid, and so does every report on a
/// claim: the event id and that uuid, 16 octets each, with no token.
#[test]
fn claim_requests_and_reports_carry_the_worker_uuid() {
    let worker: WorkerId = [0x42; 16];
    let calls = [
        Message::Call(Call::Join { req: 1, worker, channel: Some(ch("orders")), types: Vec::new(), lease_ms: 30_000, dispatch: false }),
        Message::Call(Call::Claim { req: 2, id: id(1), worker, lease_ms: 5000 }),
        Message::Call(Call::Extend { id: id(1), worker }),
        Message::Call(Call::Release { id: id(1), worker, delay_ms: 0, failed: true }),
    ];
    let mut bytes = encode(&[hello(Role::Work)]).unwrap();
    let start = bytes.len();
    bytes.extend_from_slice(&encode(&calls[2..3]).unwrap());
    // Extend: tag, head 32 (id 16, worker 16), no body.
    assert_eq!(&bytes[start..start + 7], &[EXTEND, 32, 0, 0, 0, 0, 0]);
    assert_eq!(&bytes[start + 7 + 16..start + 7 + 32], &worker);
    for call in calls {
        assert_eq!(trip(std::slice::from_ref(&call), Side::Server).unwrap(), [call]);
    }
}

/// Feature bits of what version 5 dropped — pause (1), prefix subscriptions
/// (5), `group` (6) — are never announced.
#[test]
fn the_retired_feature_bits_are_reserved() {
    assert_eq!(features::ALL & ((1 << 1) | (1 << 5) | (1 << 6)), 0);
    assert_eq!(features::ALL, features::SEQUENCE | features::WINDOW | features::TIMING | features::OPERATE);
}

/// Every record of 2.11 round-trips (property: equal after encode and decode).
#[test]
fn every_call_and_every_server_message_round_trips() {
    let calls: Vec<Message> = every_call().into_iter().map(Message::Call).collect();
    assert_eq!(trip(&calls, Side::Server).unwrap(), calls);
    let outbound: Vec<Message> = every_outbound().into_iter().map(Message::Outbound).collect();
    assert_eq!(trip(&outbound, Side::Client).unwrap(), outbound);
    let opened = decode(&encode(&[hello(Role::Work)]).unwrap(), Side::Server).unwrap();
    assert_eq!(opened, [hello(Role::Work)]);
    let welcomed = decode(&encode(&[welcome()]).unwrap(), Side::Client).unwrap();
    assert_eq!(welcomed, [welcome()]);
}

/// The server's `Outbound::Welcome` goes out with this build's version and features.
#[test]
fn an_engine_welcome_is_sent_with_this_version_and_the_encoders_features() {
    let welcome = Welcome { server_id: [1; 16], generation: 1, credential: "svc".into(), resume: 0, superseded: false };
    let mut encoder = Encoder::new().with_features(features::SEQUENCE | features::OPERATE);
    encoder.push(&Message::Outbound(Outbound::Welcome(welcome.clone()))).unwrap();
    let decoded = decode(&concat(encoder.take()), Side::Client).unwrap();
    assert_eq!(decoded, [Message::Welcome { version: VERSION, features: features::SEQUENCE | features::OPERATE, welcome }]);
}

/// An assignment carries the lease left when the server starts writing it.
#[test]
fn an_assignment_carries_the_lease_left_at_the_moment_it_is_written() {
    let assignment = Assignment { event: assigned(1, b"p"), lease_ms: 60_000, until: 50_000, more: false };
    let mut encoder = Encoder::new();
    encoder.push(&Message::Outbound(Outbound::Welcome(Welcome { server_id: [0; 16], generation: 1, credential: "s".into(), resume: 0, superseded: false }))).unwrap();
    encoder.set_now(45_500);
    encoder.push(&Message::Outbound(Outbound::Assign(assignment.clone()))).unwrap();
    let decoded = decode(&concat(encoder.take()), Side::Client).unwrap();
    assert_eq!(decoded[1], Message::Outbound(Outbound::Assign(Assignment { until: 4_500, ..assignment })));
}

fn arbitrary_event() -> impl Strategy<Value = Envelope> {
    (any::<[u8; 16]>(), any::<u16>(), proptest::collection::vec(any::<u8>(), 0..600), any::<bool>(), any::<[u8; 16]>(), proptest::option::of(any::<u32>())).prop_map(
        |(channel, kind, payload, dispatch, raw, delay)| {
            let channel = Channel(channel);
            let mut event = if dispatch { Envelope::dispatch(channel, kind, payload) } else { Envelope::claimable(channel, kind, payload) };
            event.id = EventId(raw);
            if let (false, Some(delay)) = (dispatch, delay) {
                event = event.with_not_before(std::time::Duration::from_millis(u64::from(delay)));
            }
            event
        },
    )
}

proptest! {
    /// Arbitrary registrations and completions round-trip.
    #[test]
    fn arbitrary_calls_round_trip(events in proptest::collection::vec(arbitrary_event(), 1..30)) {
        let mut calls = Vec::new();
        for (index, event) in events.iter().enumerate() {
            calls.push(Message::Call(Call::Register { seq: Some(index as u64 + 1), event: event.clone() }));
        }
        calls.push(Message::Call(Call::Complete { seq: 99, id: id(1), worker: WORKER, follow_ups: events.iter().take(2).cloned().collect(), spooled: Vec::new() }));
        prop_assert_eq!(trip(&calls, Side::Server).unwrap(), calls);
    }

    /// No input makes the decoder panic; it refuses or decodes.
    #[test]
    fn arbitrary_bytes_never_panic_the_decoder(bytes in proptest::collection::vec(any::<u8>(), 0..400)) {
        for side in [Side::Server, Side::Client] {
            let mut decoder = Decoder::new(side);
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
        worker: WORKER,
        follow_ups: vec![Envelope::dispatch(ch("a"), 20, Bytes::from_static(b"1")), Envelope::dispatch(ch("a"), 21, Bytes::from_static(b"2"))],
        spooled: Vec::new(),
    };
    let bytes = encode(&[hello(Role::Work), Message::Call(call.clone())]).unwrap();
    let mut decoder = Decoder::new(Side::Server);
    let mut handed = Vec::new();
    for (raw, body) in split(&bytes) {
        decoder.admit(&raw).unwrap();
        handed.push(decoder.decode(&raw, Body::Bytes(body)).unwrap().is_some());
        if raw.tag == 0x14 {
            assert!(decoder.gathering());
        }
    }
    // hello, complete, follow-up, follow-up
    assert_eq!(handed.iter().filter(|done| **done).count(), 2, "the hello and the completion: {handed:?}");
    assert!(!decoder.gathering());
}

#[test]
fn a_record_from_the_wrong_direction_or_out_of_place_is_refused() {
    let welcome_bytes = encode(&[welcome()]).unwrap();
    assert_eq!(decode(&welcome_bytes, Side::Server), Err(ProtocolError::WrongDirection(0x02)));
    let call = encode(&[hello(Role::Work), Message::Call(Call::Stats { req: 1 })]).unwrap();
    assert_eq!(decode(&call, Side::Client), Err(ProtocolError::WrongDirection(HELLO)));
    // Nothing before the hello.
    let early = encode(&[Message::Call(Call::Stats { req: 1 })]).unwrap();
    assert!(matches!(decode(&early, Side::Server), Err(ProtocolError::Handshake(_))));
    // A second hello, and a follow-up outside a completion.
    let twice = encode(&[hello(Role::Work), hello(Role::Work)]).unwrap();
    assert_eq!(decode(&twice, Side::Server), Err(ProtocolError::WrongDirection(HELLO)));
    let mut stray = encode(&[hello(Role::Work)]).unwrap();
    stray.extend_from_slice(&[0x15, 0, 0, 0, 0, 0, 0]);
    assert_eq!(decode(&stray, Side::Server), Err(ProtocolError::WrongDirection(0x15)));
}

#[test]
fn unknown_tags_leftover_bytes_bodies_on_bodiless_records_and_forbidden_heads_are_refused() {
    let opened = encode(&[hello(Role::Produce)]).unwrap();
    let with = |extra: &[u8]| {
        let mut bytes = opened.clone();
        bytes.extend_from_slice(extra);
        decode(&bytes, Side::Server)
    };
    assert_eq!(with(&[0x7f, 0, 0, 0, 0, 0, 0]), Err(ProtocolError::UnknownTag(0x7f)));
    assert_eq!(with(&[0x1b, 2, 0, 0, 0, 0, 0, 1, 9]), Err(ProtocolError::Malformed(Malformed::Trailing)));
    assert_eq!(with(&[0x1b, 1, 0, 2, 0, 0, 0, 1, 9, 9]), Err(ProtocolError::UnexpectedBody(0x1b)));
    // A dispatch event with a visibility time.
    let mut bad = vec![REGISTER, 40, 0, 0, 0, 0, 0, 1];
    bad.extend_from_slice(&[0; 16]);
    bad.extend_from_slice(&ch("c").0);
    bad.extend_from_slice(&[0, 0, 0x03, 0, 0, 0, 5]);
    assert_eq!(with(&bad), Err(ProtocolError::Malformed(Malformed::Forbidden("not_before or a worker on a dispatch event"))));
    // A reserved flag bit (3).
    let mut reserved = vec![REGISTER, 39, 0, 0, 0, 0, 0, 1];
    reserved.extend_from_slice(&[0; 16]);
    reserved.extend_from_slice(&ch("c").0);
    reserved.extend_from_slice(&[0, 0, 0x08, 0, 0, 0]);
    assert_eq!(with(&reserved), Err(ProtocolError::Malformed(Malformed::Reserved)));
}

/// A refused message writes nothing: the next one decodes as if it were the first.
#[test]
fn a_refused_message_writes_nothing() {
    let mut encoder = Encoder::new();
    encoder.push(&hello(Role::Produce)).unwrap();
    let bad = Envelope::dispatch(ch("never"), 22, Bytes::new()).with_not_before(std::time::Duration::from_secs(1));
    assert!(encoder.push(&Message::Call(Call::Register { seq: Some(1), event: bad })).is_err());
    let good = Message::Call(Call::Register { seq: Some(1), event: Envelope::dispatch(ch("never"), 22, Bytes::new()) });
    encoder.push(&good).unwrap();
    let decoded = decode(&concat(encoder.take()), Side::Server).unwrap();
    assert_eq!(decoded[1], good);
}

/// A body past the inline bound is the payload itself, shared, not a copy;
/// a small one shares the head's buffer (S11).
#[test]
fn a_large_body_is_its_own_shared_vector_entry_and_a_small_one_is_inlined() {
    let large = Bytes::from(vec![5u8; 4096]);
    let small = Bytes::from_static(b"tiny");
    let mut encoder = Encoder::new();
    encoder.push(&Message::Outbound(Outbound::Delivery(Envelope::dispatch(ch("c"), 10, large.clone())))).unwrap();
    assert!(encoder.shares(&large), "the large payload is an entry of its own, not copied");
    let mut encoder = Encoder::new();
    encoder.push(&Message::Outbound(Outbound::Delivery(Envelope::dispatch(ch("c"), 10, small.clone())))).unwrap();
    assert!(!encoder.shares(&small), "a small payload is copied next to its head");
    let mut encoder = Encoder::new();
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
    let mut bytes = encode(&[hello(Role::Produce)]).unwrap();
    bytes[7] = 1;
    assert_eq!(decode(&bytes, Side::Server), Err(ProtocolError::Version(1)));
    let v1_hello = [11, 0, 0, 0, 1, 0, 0, 0, 0x01, 1, 0, 2, 0, b'a', b'b'];
    assert!(sniff_v1(&v1_hello));
    assert!(!sniff_v1(&encode(&[hello(Role::Produce)]).unwrap()));
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
    let bytes = encode(&messages).unwrap();
    let (mut client, server) = tokio::io::duplex(1024);
    let writer = tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        client.write_all(&bytes).await.unwrap();
    });
    let mut reader = RecordReader::new(server);
    let mut decoder = Decoder::new(Side::Client);
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
    let mut encoder = Encoder::new();
    for message in &messages {
        encoder.push(message).unwrap();
    }
    let mut parts = encoder.take();
    let mut trickle = Trickle(Vec::new());
    parts.write_to(&mut trickle).await.unwrap();
    assert_eq!(parts.written(), parts.total());
    assert_eq!(decode(&trickle.0, Side::Client).unwrap(), messages);
}
