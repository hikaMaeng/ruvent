use super::codec::{self, decode, encode, put_varint, Layout, Malformed, Reader};
use super::*;
use proptest::prelude::*;

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

fn roundtrip(envelope: &Envelope, layout: Layout) -> Result<(Vec<u8>, Envelope), Malformed> {
    let mut out = Vec::new();
    encode(envelope, layout, &mut out)?;
    let bytes = Bytes::from(out.clone());
    let mut reader = Reader::new(&bytes);
    let decoded = decode(&mut reader, layout)?;
    reader.finish()?;
    Ok((out, decoded))
}

fn sample() -> Envelope {
    Envelope::claimable(ch("orders"), 17, Bytes::new()).with_id(EventId([0x11; 16]))
}

const KEY: WorkKey = WorkKey { generation: 3, arrival: 9 };
const WORKER: WorkerId = [0x55; 16];

#[test]
fn the_head_is_38_bytes_and_the_key_adds_four_plus_a_varint() {
    assert_eq!(codec::HEAD, 38, "id 16 + channel 16 + type 2 + flags 2 + failures 2");
    let dispatch = Envelope::dispatch(ch("orders"), 2, Bytes::new());
    assert_eq!(codec::size(&dispatch, Layout::Delivery), 42, "head 38 + payload length 4");
    let waiting = Envelope { key: Some(KEY), ..sample() };
    assert_eq!(codec::size(&waiting, Layout::Journal), 47, "a small arrival is one varint byte");
    assert_eq!(codec::size(&waiting, Layout::Notice), 43);
    let late = Envelope { key: Some(WorkKey { generation: 3, arrival: u64::MAX }), worker: Some(WORKER), ..sample() };
    assert_eq!(codec::size(&late, Layout::Assigned), 56 + 16, "u64::MAX takes ten varint bytes; the worker uuid is 16");
    for (envelope, layout) in [(&dispatch, Layout::Delivery), (&waiting, Layout::Journal), (&late, Layout::Assigned), (&waiting, Layout::Notice)] {
        assert_eq!(roundtrip(envelope, layout).unwrap().0.len(), codec::size(envelope, layout));
    }
}

/// The byte layout, pinned: changing it breaks every peer and every journal.
#[test]
fn an_assigned_event_has_these_exact_bytes() {
    let event = Envelope { failures: 2, key: Some(KEY), not_before: Some(300), worker: Some(WORKER), payload: Bytes::from_static(b"hi"), ..sample() };
    let (bytes, decoded) = roundtrip(&event, Layout::Assigned).unwrap();
    let mut expected = vec![0x11; 16];
    expected.extend_from_slice(&ch("orders").0); // channel: its 16 octets
    expected.extend_from_slice(&[17, 0]); // type 17
    expected.extend_from_slice(&(codec::NOT_BEFORE | codec::WORKER).to_le_bytes());
    expected.extend_from_slice(&[2, 0]); // failures
    expected.extend_from_slice(&[3, 0, 0, 0, 9]); // work key: generation, arrival varint
    expected.extend_from_slice(&[0xac, 0x02]); // not_before 300 as a varint
    expected.extend_from_slice(&[0x55; 16]); // the worker's uuid: the claim
    expected.extend_from_slice(&[2, 0, 0, 0, b'h', b'i']); // payload
    assert_eq!(bytes, expected);
    assert_eq!(decoded.worker, Some(WORKER));
}

#[test]
fn a_dispatch_delivery_has_these_exact_bytes() {
    let event = Envelope::dispatch(ch("c"), 10, Bytes::from_static(b"x")).with_id(EventId([0x22; 16]));
    let (bytes, _) = roundtrip(&event, Layout::Delivery).unwrap();
    let mut expected = vec![0x22; 16];
    expected.extend_from_slice(&ch("c").0);
    expected.extend_from_slice(&[10, 0]);
    expected.extend_from_slice(&codec::KIND.to_le_bytes());
    expected.extend_from_slice(&[0, 0]);
    expected.extend_from_slice(&[1, 0, 0, 0, b'x']);
    assert_eq!(bytes, expected);
}

#[test]
fn layouts_refuse_what_they_must_not_carry() {
    assert_eq!(roundtrip(&sample(), Layout::Delivery).unwrap_err(), Malformed::Forbidden("claimable"));
    assert_eq!(roundtrip(&Envelope::dispatch(ch("c"), 10, Bytes::new()).with_id(EventId::new_v7()), Layout::Assigned).unwrap_err(), Malformed::Forbidden("dispatch"));
    assert_eq!(roundtrip(&sample(), Layout::Journal).unwrap_err(), Malformed::Missing("work key"));
    assert_eq!(roundtrip(&Envelope { key: Some(KEY), ..sample() }, Layout::Assigned).unwrap_err(), Malformed::Missing("worker"), "an assignment names its worker");
    assert_eq!(roundtrip(&Envelope { failures: 1, ..sample() }, Layout::Request).map(|(_, event)| event.failures), Ok(0), "a request never carries failures");
    assert_eq!(roundtrip(&Envelope { worker: Some(WORKER), ..sample() }, Layout::Request).map(|(_, event)| event.worker), Ok(None), "a request never carries a worker");
    assert_eq!(roundtrip(&Envelope { worker: Some(WORKER), key: Some(KEY), ..sample() }, Layout::Journal).map(|(_, event)| event.worker), Ok(None), "the journal keeps a claim as its own record");
}

/// The worker uuid travels only in an assignment; a notice of the same event carries none.
#[test]
fn the_worker_uuid_travels_in_assignments_only() {
    let claimed = Envelope { failures: 4, key: Some(KEY), worker: Some(WORKER), ..sample() };
    let (bytes, decoded) = roundtrip(&claimed, Layout::Assigned).unwrap();
    assert_eq!(decoded.worker, Some(WORKER));
    assert_eq!(u16::from_le_bytes([bytes[34], bytes[35]]) & codec::WORKER, codec::WORKER, "bit 2 of flags");
    assert_eq!(roundtrip(&claimed, Layout::Notice).unwrap().1.worker, None);
    assert!(claimed.notice().worker.is_none());
}

/// A channel is a UUIDv7; the envelope carries any 16 octets and the server checks.
#[test]
fn a_channel_is_valid_only_as_a_uuidv7() {
    assert!(Channel::new().is_valid() && ch("x").is_valid());
    assert!(!Channel::default().is_valid() && !Channel([0xff; 16]).is_valid());
    let mut v4 = Channel::new();
    v4.0[6] = (v4.0[6] & 0x0f) | 0x40;
    assert!(!v4.is_valid(), "another UUID version is refused");
    let channel = Channel::new();
    assert_eq!(channel.to_string().parse::<Channel>().unwrap(), channel);
    let id = EventId::new_v7();
    assert_eq!(Channel::from(id).0, id.0, "a request's own id can be its channel");
}

#[test]
fn not_before_is_claimable_only() {
    let dispatch = Envelope::dispatch(ch("c"), 10, Bytes::new()).with_not_before(std::time::Duration::from_secs(1));
    assert_eq!(roundtrip(&dispatch, Layout::Request).unwrap_err(), Malformed::Forbidden("not_before or a worker on a dispatch event"));
    let timed = sample().with_not_before(std::time::Duration::from_millis(5));
    assert_eq!(roundtrip(&timed, Layout::Request).unwrap().1, timed);
}

fn decode_raw(bytes: Vec<u8>, layout: Layout) -> Result<Envelope, Malformed> {
    let bytes = Bytes::from(bytes);
    let mut reader = Reader::new(&bytes);
    let event = decode(&mut reader, layout)?;
    reader.finish()?;
    Ok(event)
}

fn raw(flags: u16, failures: u16, tail: &[u8]) -> Vec<u8> {
    let mut bytes = vec![0x11; 16];
    bytes.extend_from_slice(&[0x33; 16]);
    bytes.extend_from_slice(&[1, 0]);
    bytes.extend_from_slice(&flags.to_le_bytes());
    bytes.extend_from_slice(&failures.to_le_bytes());
    bytes.extend_from_slice(tail);
    bytes
}

#[test]
fn the_decoder_refuses_forbidden_combinations_reserved_bits_and_leftovers() {
    let payload = [0u8, 0, 0, 0];
    for bit in 3..16 {
        assert_eq!(decode_raw(raw(1 << bit, 0, &payload), Layout::Request).unwrap_err(), Malformed::Reserved, "bit {bit} is reserved (format 5's policy fields are gone)");
    }
    assert_eq!(decode_raw(raw(codec::WORKER, 0, &payload), Layout::Request).unwrap_err(), Malformed::Forbidden("a field this record does not carry"));
    assert_eq!(decode_raw(raw(codec::KIND | codec::NOT_BEFORE, 0, &payload), Layout::Delivery).unwrap_err(), Malformed::Forbidden("not_before or a worker on a dispatch event"));
    assert_eq!(decode_raw(raw(codec::NOT_BEFORE, 0, &[0; 6]), Layout::Notice).unwrap_err(), Malformed::Forbidden("a field this record does not carry"));
    assert_eq!(decode_raw(raw(0, 0, &[3, 0, 0, 0, 9, 0, 0, 0, 0]), Layout::Assigned).unwrap_err(), Malformed::Missing("worker"));
    assert_eq!(decode_raw(raw(0, 1, &payload), Layout::Request).unwrap_err(), Malformed::Forbidden("failures"));
    assert_eq!(decode_raw(raw(codec::KIND, 1, &payload), Layout::Delivery).unwrap_err(), Malformed::Forbidden("failures"));
    assert_eq!(decode_raw(raw(0, 0, &[0, 0, 0, 0, 9]), Layout::Request).unwrap_err(), Malformed::Trailing);
    assert_eq!(decode_raw(raw(0, 0, &[5, 0, 0, 0, 1]), Layout::Request).unwrap_err(), Malformed::Short);
    assert!(decode_raw(raw(0, 0, &payload), Layout::Request).is_ok());
}

#[test]
fn varints_round_trip_and_refuse_overlong_encodings() {
    for value in [0u64, 1, 127, 128, 300, u64::from(u32::MAX), u64::MAX] {
        let mut out = Vec::new();
        put_varint(&mut out, value);
        assert_eq!(out.len(), codec::varint_len(value));
        let bytes = Bytes::from(out);
        let mut reader = Reader::new(&bytes);
        assert_eq!(reader.varint(), Ok(value));
        assert!(reader.finish().is_ok());
    }
    let overlong = Bytes::from(vec![0xff; 11]);
    assert_eq!(Reader::new(&overlong).varint(), Err(Malformed::Overlong));
    let past_u64 = Bytes::from(vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02]);
    assert_eq!(Reader::new(&past_u64).varint(), Err(Malformed::Overlong));
}

#[test]
fn failures_saturate_at_65535() {
    let event = Envelope { failures: u16::MAX, key: Some(KEY), ..sample() };
    assert_eq!(roundtrip(&event, Layout::Journal).unwrap().1.failures, u16::MAX);
    assert_eq!(event.failures.saturating_add(1), u16::MAX);
}

#[test]
fn ids_are_uuids_and_nil_means_unassigned() {
    let id = EventId::new_v7();
    assert!(id.is_uuid() && !id.is_nil());
    assert_eq!(id.0[6] >> 4, 7);
    assert!(!EventId::NIL.is_uuid());
    assert!(!EventId([0xff; 16]).is_uuid());
    assert_eq!(id.to_string().parse::<EventId>().unwrap(), id);
}

fn envelope() -> impl Strategy<Value = Envelope> {
    (
        (any::<[u8; 16]>(), any::<[u8; 16]>(), any::<u16>(), any::<bool>(), any::<u16>(), any::<(u32, u64)>()),
        (proptest::option::of(any::<u64>()), proptest::option::of(any::<[u8; 16]>())),
        proptest::collection::vec(any::<u8>(), 0..64),
    )
        .prop_map(|((id, channel, event_type, dispatch, failures, key), (not_before, worker), payload)| Envelope {
            id: EventId(id),
            channel: Channel(channel),
            event_type,
            kind: if dispatch { Kind::Dispatch } else { Kind::Claimable },
            failures,
            key: Some(WorkKey { generation: key.0, arrival: key.1 }),
            not_before,
            worker,
            payload: Bytes::from(payload),
        })
}

/// What a layout keeps of an envelope, for comparing a round trip.
fn kept(event: &Envelope, layout: Layout) -> Envelope {
    let mut event = event.clone();
    if !layout.has_key() {
        event.key = None;
    }
    if matches!(layout, Layout::Request | Layout::Delivery) {
        event.failures = 0;
    }
    if layout != Layout::Assigned {
        event.worker = None;
    }
    if layout == Layout::Delivery {
        event.not_before = None;
    }
    if layout == Layout::Notice {
        event = event.notice();
    }
    event
}

const LAYOUTS: [Layout; 5] = [Layout::Request, Layout::Notice, Layout::Assigned, Layout::Delivery, Layout::Journal];

proptest! {
    /// Every flag combination either round-trips to what the layout keeps or is refused on encode.
    #[test]
    fn every_envelope_round_trips_through_every_layout_that_accepts_it(event in envelope()) {
        for layout in LAYOUTS {
            match roundtrip(&event, layout) {
                Ok((bytes, decoded)) => {
                    prop_assert_eq!(bytes.len(), codec::size(&event, layout));
                    prop_assert_eq!(decoded, kept(&event, layout));
                }
                Err(Malformed::Forbidden(_)) | Err(Malformed::Missing(_)) => {}
                Err(other) => prop_assert!(false, "{layout:?}: {other}"),
            }
        }
    }

    /// No byte string makes the decoder panic.
    #[test]
    fn arbitrary_bytes_never_panic_the_decoder(bytes in proptest::collection::vec(any::<u8>(), 0..128)) {
        for layout in LAYOUTS {
            let _ = decode_raw(bytes.clone(), layout);
        }
    }
}
