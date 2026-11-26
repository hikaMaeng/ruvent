use super::codec::{self, decode, encode, put_varint, Layout, Malformed, Reader};
use super::*;
use proptest::prelude::*;
use std::collections::HashMap;

/// A dictionary for tests: numbers in first-use order.
#[derive(Default)]
struct Dict {
    numbers: HashMap<Name, u32>,
    names: Vec<Name>,
}

impl Dict {
    fn number(&mut self, name: &Name) -> u32 {
        if let Some(number) = self.numbers.get(name) {
            return *number;
        }
        let number = self.names.len() as u32;
        self.names.push(name.clone());
        self.numbers.insert(name.clone(), number);
        number
    }
}

fn roundtrip(envelope: &Envelope, layout: Layout) -> Result<(Vec<u8>, Envelope), Malformed> {
    let mut dict = Dict::default();
    let mut out = Vec::new();
    encode(envelope, layout, &mut |name| dict.number(name), &mut out)?;
    let bytes = Bytes::from(out.clone());
    let mut reader = Reader::new(&bytes);
    let decoded = decode(&mut reader, layout, &|number| dict.names.get(number as usize).cloned())?;
    reader.finish()?;
    Ok((out, decoded))
}

fn sample() -> Envelope {
    Envelope::claimable("orders", "create", Bytes::new()).with_id(EventId([0x11; 16]))
}

const KEY: WorkKey = WorkKey { generation: 3, arrival: 9 };

#[test]
fn the_head_is_28_bytes_and_the_key_adds_four_plus_a_varint() {
    let dispatch = Envelope::dispatch("orders", "created", Bytes::new());
    assert_eq!(codec::size(&dispatch, Layout::Delivery), 32, "head 28 + payload length 4");
    let waiting = Envelope { key: Some(KEY), ..sample() };
    assert_eq!(codec::size(&waiting, Layout::Journal), 37, "a small arrival is one varint byte");
    assert_eq!(codec::size(&waiting, Layout::Notice), 33);
    let late = Envelope { key: Some(WorkKey { generation: 3, arrival: u64::MAX }), ..sample() };
    assert_eq!(codec::size(&late, Layout::Assigned), 46, "u64::MAX takes ten varint bytes");
    for (envelope, layout) in [(&dispatch, Layout::Delivery), (&waiting, Layout::Journal), (&late, Layout::Assigned), (&waiting, Layout::Notice)] {
        assert_eq!(roundtrip(envelope, layout).unwrap().0.len(), codec::size(envelope, layout));
    }
}

/// The byte layout, pinned: changing it breaks every peer and every journal.
#[test]
fn an_assigned_event_has_these_exact_bytes() {
    let event = Envelope { failures: 2, key: Some(KEY), not_before: Some(300), group: Some([0x44; 16]), payload: Bytes::from_static(b"hi"), ..sample() };
    let (bytes, _) = roundtrip(&event, Layout::Assigned).unwrap();
    let mut expected = vec![0x11; 16];
    expected.extend_from_slice(&[0, 0, 0, 0]); // channel: name 0
    expected.extend_from_slice(&[1, 0, 0, 0]); // type: name 1
    expected.extend_from_slice(&(codec::NOT_BEFORE | codec::GROUP).to_le_bytes());
    expected.extend_from_slice(&[2, 0]); // failures
    expected.extend_from_slice(&[3, 0, 0, 0, 9]); // work key: generation, arrival varint
    expected.extend_from_slice(&[0xac, 0x02]); // not_before 300 as a varint
    expected.extend_from_slice(&[0x44; 16]); // group
    expected.extend_from_slice(&[2, 0, 0, 0, b'h', b'i']); // payload
    assert_eq!(bytes, expected);
}

#[test]
fn a_dispatch_delivery_has_these_exact_bytes() {
    let event = Envelope::dispatch("c", "t", Bytes::from_static(b"x")).with_id(EventId([0x22; 16]));
    let (bytes, _) = roundtrip(&event, Layout::Delivery).unwrap();
    let mut expected = vec![0x22; 16];
    expected.extend_from_slice(&[0, 0, 0, 0, 1, 0, 0, 0]);
    expected.extend_from_slice(&codec::KIND.to_le_bytes());
    expected.extend_from_slice(&[0, 0]);
    expected.extend_from_slice(&[1, 0, 0, 0, b'x']);
    assert_eq!(bytes, expected);
}

#[test]
fn layouts_refuse_what_they_must_not_carry() {
    assert_eq!(roundtrip(&Envelope { principal: Some("svc".into()), ..sample() }, Layout::Request).unwrap_err(), Malformed::Forbidden("principal"));
    assert_eq!(roundtrip(&sample(), Layout::Delivery).unwrap_err(), Malformed::Forbidden("claimable"));
    assert_eq!(roundtrip(&Envelope::dispatch("c", "t", Bytes::new()).with_id(EventId::new_v7()), Layout::Assigned).unwrap_err(), Malformed::Forbidden("dispatch"));
    assert_eq!(roundtrip(&sample(), Layout::Journal).unwrap_err(), Malformed::Missing("work key"));
    assert_eq!(roundtrip(&Envelope { failures: 1, ..sample() }, Layout::Request).map(|(_, event)| event.failures), Ok(0), "a request never carries failures");
}

#[test]
fn timing_and_group_are_claimable_only() {
    for dispatch in [
        Envelope::dispatch("c", "t", Bytes::new()).with_not_before(std::time::Duration::from_secs(1)),
        Envelope::dispatch("c", "t", Bytes::new()).with_expires(std::time::Duration::from_secs(1)),
        Envelope::dispatch("c", "t", Bytes::new()).with_group([1; 16]),
    ] {
        assert_eq!(roundtrip(&dispatch, Layout::Request).unwrap_err(), Malformed::Forbidden("timing or group on a dispatch event"));
    }
    let timed = sample().with_not_before(std::time::Duration::from_millis(5)).with_expires(std::time::Duration::from_secs(60)).with_group([7; 16]);
    assert_eq!(roundtrip(&timed, Layout::Request).unwrap().1, timed);
}

fn decode_raw(bytes: Vec<u8>, layout: Layout) -> Result<Envelope, Malformed> {
    let bytes = Bytes::from(bytes);
    let mut reader = Reader::new(&bytes);
    let event = decode(&mut reader, layout, &|number| (number < 4).then(|| Name::new("n")))?;
    reader.finish()?;
    Ok(event)
}

fn raw(flags: u16, failures: u16, tail: &[u8]) -> Vec<u8> {
    let mut bytes = vec![0x11; 16];
    bytes.extend_from_slice(&[0, 0, 0, 0, 1, 0, 0, 0]);
    bytes.extend_from_slice(&flags.to_le_bytes());
    bytes.extend_from_slice(&failures.to_le_bytes());
    bytes.extend_from_slice(tail);
    bytes
}

#[test]
fn the_decoder_refuses_forbidden_combinations_reserved_bits_and_leftovers() {
    let payload = [0u8, 0, 0, 0];
    assert_eq!(decode_raw(raw(1 << 1, 0, &payload), Layout::Request).unwrap_err(), Malformed::Reserved, "bit 1 was 'claimed' and is reserved now");
    assert_eq!(decode_raw(raw(1 << 2, 0, &payload), Layout::Request).unwrap_err(), Malformed::Reserved, "bit 2 was 'reply' (format 3) and is reserved now");
    assert_eq!(decode_raw(raw(1 << 3, 0, &payload), Layout::Request).unwrap_err(), Malformed::Reserved, "bit 3 was 'correlation' (format 3) and is reserved now");
    assert_eq!(decode_raw(raw(1 << 9, 0, &payload), Layout::Request).unwrap_err(), Malformed::Reserved);
    assert_eq!(decode_raw(raw(1 << 15, 0, &payload), Layout::Request).unwrap_err(), Malformed::Reserved);
    assert_eq!(decode_raw(raw(0, 1, &payload), Layout::Request).unwrap_err(), Malformed::Forbidden("failures"));
    assert_eq!(decode_raw(raw(codec::KIND, 1, &payload), Layout::Delivery).unwrap_err(), Malformed::Forbidden("failures"));
    assert_eq!(decode_raw(raw(codec::PRINCIPAL, 0, &[0; 8]), Layout::Notice).unwrap_err(), Malformed::Forbidden("optional field"));
    assert_eq!(decode_raw(raw(0, 0, &[0, 0, 0, 0, 9]), Layout::Request).unwrap_err(), Malformed::Trailing);
    assert_eq!(decode_raw(raw(0, 0, &[5, 0, 0, 0, 1]), Layout::Request).unwrap_err(), Malformed::Short);
    let mut unknown = raw(0, 0, &payload);
    unknown[16] = 9;
    assert_eq!(decode_raw(unknown, Layout::Request).unwrap_err(), Malformed::UnknownName(9));
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

fn name() -> impl Strategy<Value = Name> {
    "[a-z.]{1,12}".prop_map(Name::from)
}

fn envelope() -> impl Strategy<Value = Envelope> {
    (
        (any::<[u8; 16]>(), name(), name(), any::<bool>(), any::<u16>(), any::<(u32, u64)>()),
        (proptest::option::of(name()), proptest::option::of(name())),
        (proptest::option::of(any::<u64>()), proptest::option::of(any::<u64>()), proptest::option::of(any::<[u8; 16]>())),
        proptest::collection::vec(any::<u8>(), 0..64),
    )
        .prop_map(|((id, channel, event_type, dispatch, failures, key), (principal, delegate), (not_before, expires, group), payload)| Envelope {
            id: EventId(id),
            channel,
            event_type,
            kind: if dispatch { Kind::Dispatch } else { Kind::Claimable },
            failures,
            key: Some(WorkKey { generation: key.0, arrival: key.1 }),
            principal,
            delegate,
            not_before,
            expires,
            group,
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
    if !layout.has_optional() {
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
