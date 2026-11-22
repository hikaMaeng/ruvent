use super::codec::{self, decode, encode, Layout, Malformed, Reader};
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
const CLAIM: Claim = Claim { token: ClaimToken { generation: 3, seq: 1 }, claimer: 2, worker: 7, deadline: 30_000 };

#[test]
fn the_three_sizes_are_31_39_and_63_bytes_plus_payload() {
    let dispatch = Envelope::dispatch("orders", "created", Bytes::new());
    assert_eq!(codec::size(&dispatch, Layout::Delivery), 31);
    let waiting = Envelope { key: Some(KEY), ..sample() };
    assert_eq!(codec::size(&waiting, Layout::Journal), 39);
    let claimed = Envelope { claim: Some(CLAIM), ..waiting.clone() };
    assert_eq!(codec::size(&claimed, Layout::Claimed), 63);
    assert_eq!(codec::size(&waiting, Layout::Notice), 35);
    for (envelope, layout) in [(&dispatch, Layout::Delivery), (&waiting, Layout::Journal), (&claimed, Layout::Claimed), (&waiting, Layout::Notice)] {
        assert_eq!(roundtrip(envelope, layout).unwrap().0.len(), codec::size(envelope, layout));
    }
}

/// The byte layout, pinned: changing it breaks every peer and every journal.
#[test]
fn a_claim_response_has_these_exact_bytes() {
    let event = Envelope { retry: 2, key: Some(KEY), claim: Some(CLAIM), payload: Bytes::from_static(b"hi"), ..sample() };
    let (bytes, _) = roundtrip(&event, Layout::Claimed).unwrap();
    let mut expected = vec![0x11; 16];
    expected.extend_from_slice(&[0, 0, 0, 0]); // channel: name 0
    expected.extend_from_slice(&[1, 0, 0, 0]); // type: name 1
    expected.push(codec::CLAIMED);
    expected.extend_from_slice(&[2, 0]); // retry
    expected.extend_from_slice(&[3, 0, 0, 0, 9, 0, 0, 0]); // work key
    expected.extend_from_slice(&[3, 0, 0, 0, 1, 0, 0, 0]); // token
    expected.extend_from_slice(&[2, 0, 0, 0]); // claimer
    expected.extend_from_slice(&[7, 0, 0, 0]); // worker
    expected.extend_from_slice(&30_000u64.to_le_bytes()); // deadline
    expected.extend_from_slice(&[2, 0, 0, 0, b'h', b'i']); // payload
    assert_eq!(bytes, expected);
}

#[test]
fn a_dispatch_delivery_has_these_exact_bytes() {
    let event = Envelope::dispatch("c", "t", Bytes::from_static(b"x")).with_id(EventId([0x22; 16])).with_correlation(EventId([0x33; 16]));
    let (bytes, _) = roundtrip(&event, Layout::Delivery).unwrap();
    let mut expected = vec![0x22; 16];
    expected.extend_from_slice(&[0, 0, 0, 0, 1, 0, 0, 0]);
    expected.push(codec::KIND | codec::CORRELATION);
    expected.extend_from_slice(&[0, 0]);
    expected.extend_from_slice(&[0x33; 16]);
    expected.extend_from_slice(&[1, 0, 0, 0, b'x']);
    assert_eq!(bytes, expected);
}

#[test]
fn layouts_refuse_what_they_must_not_carry() {
    let claimed = Envelope { key: Some(KEY), claim: Some(CLAIM), ..sample() };
    assert_eq!(roundtrip(&Envelope { principal: Some("svc".into()), ..sample() }, Layout::Request).unwrap_err(), Malformed::Forbidden("principal"));
    assert_eq!(roundtrip(&claimed, Layout::Journal).map(|(_, event)| event.claim), Ok(None), "the journal never writes the claim block");
    assert_eq!(roundtrip(&sample(), Layout::Delivery).unwrap_err(), Malformed::Forbidden("claimable"));
    assert_eq!(roundtrip(&Envelope { key: Some(KEY), ..sample() }, Layout::Claimed).unwrap_err(), Malformed::Forbidden("unclaimed"));
    assert_eq!(roundtrip(&sample(), Layout::Journal).unwrap_err(), Malformed::Missing("work key"));
}

fn decode_raw(bytes: Vec<u8>, layout: Layout) -> Result<Envelope, Malformed> {
    let bytes = Bytes::from(bytes);
    let mut reader = Reader::new(&bytes);
    let event = decode(&mut reader, layout, &|number| (number < 4).then(|| Name::new("n")))?;
    reader.finish()?;
    Ok(event)
}

fn raw(flags: u8, retry: u16, tail: &[u8]) -> Vec<u8> {
    let mut bytes = vec![0x11; 16];
    bytes.extend_from_slice(&[0, 0, 0, 0, 1, 0, 0, 0, flags]);
    bytes.extend_from_slice(&retry.to_le_bytes());
    bytes.extend_from_slice(tail);
    bytes
}

#[test]
fn the_decoder_refuses_forbidden_combinations_reserved_bits_and_leftovers() {
    let payload = [0u8, 0, 0, 0];
    assert_eq!(decode_raw(raw(codec::KIND | codec::CLAIMED, 0, &payload), Layout::Request).unwrap_err(), Malformed::ClaimedDispatch);
    assert_eq!(decode_raw(raw(0x40, 0, &payload), Layout::Request).unwrap_err(), Malformed::Reserved);
    assert_eq!(decode_raw(raw(0x80, 0, &payload), Layout::Request).unwrap_err(), Malformed::Reserved);
    assert_eq!(decode_raw(raw(0, 1, &payload), Layout::Request).unwrap_err(), Malformed::Forbidden("retry"));
    assert_eq!(decode_raw(raw(codec::KIND, 1, &payload), Layout::Delivery).unwrap_err(), Malformed::Forbidden("retry"));
    assert_eq!(decode_raw(raw(codec::REPLY, 0, &[0; 8]), Layout::Notice).unwrap_err(), Malformed::Forbidden("optional field"));
    assert_eq!(decode_raw(raw(0, 0, &[0, 0, 0, 0, 9]), Layout::Request).unwrap_err(), Malformed::Trailing);
    assert_eq!(decode_raw(raw(0, 0, &[5, 0, 0, 0, 1]), Layout::Request).unwrap_err(), Malformed::Short);
    let mut unknown = raw(0, 0, &payload);
    unknown[16] = 9;
    assert_eq!(decode_raw(unknown, Layout::Request).unwrap_err(), Malformed::UnknownName(9));
    assert!(decode_raw(raw(0, 0, &payload), Layout::Request).is_ok());
}

#[test]
fn retry_saturates_at_65535() {
    let event = Envelope { retry: u16::MAX, key: Some(KEY), ..sample() };
    assert_eq!(roundtrip(&event, Layout::Journal).unwrap().1.retry, u16::MAX);
    assert_eq!(event.retry.saturating_add(1), u16::MAX);
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
        any::<[u8; 16]>(),
        name(),
        name(),
        any::<bool>(),
        any::<u16>(),
        any::<(u32, u32)>(),
        proptest::option::of(any::<(u32, u32, u32, u32, u64)>()),
        proptest::option::of(name()),
        proptest::option::of(any::<[u8; 16]>()),
        proptest::option::of(name()),
        proptest::option::of(name()),
        proptest::collection::vec(any::<u8>(), 0..64),
    )
        .prop_map(|(id, channel, event_type, dispatch, retry, key, claim, reply, correlation, principal, delegate, payload)| Envelope {
            id: EventId(id),
            channel,
            event_type,
            kind: if dispatch { Kind::Dispatch } else { Kind::Claimable },
            retry,
            key: Some(WorkKey { generation: key.0, arrival: key.1 }),
            claim: if dispatch { None } else { claim.map(|(generation, seq, claimer, worker, deadline)| Claim { token: ClaimToken { generation, seq }, claimer, worker, deadline }) },
            reply,
            correlation: correlation.map(EventId),
            principal,
            delegate,
            payload: Bytes::from(payload),
        })
}

/// What a layout keeps of an envelope, for comparing a round trip.
fn kept(event: &Envelope, layout: Layout) -> Envelope {
    let mut event = event.clone();
    if !layout.has_key() {
        event.key = None;
    }
    if layout != Layout::Claimed {
        event.claim = None;
    }
    if matches!(layout, Layout::Request | Layout::Delivery) {
        event.retry = 0;
    }
    if !layout.has_optional() {
        event = event.notice();
    }
    event
}

proptest! {
    /// Every flag combination either round-trips to what the layout keeps or is refused on encode.
    #[test]
    fn every_envelope_round_trips_through_every_layout_that_accepts_it(event in envelope()) {
        for layout in [Layout::Request, Layout::Notice, Layout::Claimed, Layout::Delivery, Layout::Journal] {
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
        for layout in [Layout::Request, Layout::Notice, Layout::Claimed, Layout::Delivery, Layout::Journal] {
            let _ = decode_raw(bytes.clone(), layout);
        }
    }
}
