use super::*;
use ruvent::{Assignment, ByeReason, Bytes, Envelope, EventId, WorkKey};

/// The event id an assignment's number names (its last byte).
fn event_id(seq: u64) -> EventId {
    let mut bytes = [0x11; 16];
    bytes[15] = seq as u8;
    EventId(bytes)
}

fn assign(seq: u64) -> Outbound {
    let mut event = Envelope::claimable(ruvent::Channel::new(), 10, Bytes::from_static(b"p")).with_id(event_id(seq));
    event.key = Some(WorkKey::default());
    event.worker = Some([9; 16]);
    Outbound::Assign(Assignment { event, lease_ms: 1000, until: 1000, more: false })
}

fn drain(unsent: &mut Unsent) -> Vec<Outbound> {
    let mut out = Vec::new();
    while let Some((message, weight)) = unsent.pop() {
        unsent.written(weight);
        out.push(message);
    }
    out
}

/// A production connection that does not read holds one acknowledgement:
/// the newest replaces the one still queued (2.2).
#[test]
fn a_newer_committed_replaces_the_queued_one() {
    let mut unsent = Unsent::new(Role::Produce, 0);
    for seq in [3, 7, 5, 9] {
        assert!(!unsent.accept(Outbound::Committed { seq }), "a production connection is never slow");
    }
    unsent.accept(Outbound::Fault { kind: FaultKind::Malformed, detail: "x" });
    unsent.accept(Outbound::Committed { seq: 12 });
    assert_eq!(drain(&mut unsent), [Outbound::Committed { seq: 12 }, Outbound::Fault { kind: FaultKind::Malformed, detail: "x" }]);
    // Once written, the next one is queued anew.
    unsent.accept(Outbound::Committed { seq: 13 });
    assert_eq!(drain(&mut unsent), [Outbound::Committed { seq: 13 }]);
}

#[test]
fn a_fault_is_told_once_per_kind_on_one_connection() {
    let mut unsent = Unsent::new(Role::Work, 1 << 20);
    for _ in 0..3 {
        unsent.accept(Outbound::Fault { kind: FaultKind::IdConflict, detail: "x" });
        unsent.accept(Outbound::Fault { kind: FaultKind::Forbidden, detail: "y" });
    }
    unsent.fault(FaultKind::Forbidden, "z");
    assert_eq!(drain(&mut unsent), [Outbound::Fault { kind: FaultKind::IdConflict, detail: "x" }, Outbound::Fault { kind: FaultKind::Forbidden, detail: "y" }]);
}

/// An observation connection past its byte bound is slow; what it was given
/// stays counted until written (S7).
#[test]
fn an_observer_past_its_byte_bound_is_slow_and_bytes_count_until_written() {
    let mut unsent = Unsent::new(Role::Observe, 13_000);
    let event = Envelope::dispatch(ruvent::Channel::new(), 10, Bytes::from(vec![0u8; 4000]));
    assert!(!unsent.accept(Outbound::Delivery(event.clone())));
    assert!(!unsent.accept(Outbound::Delivery(event.clone())));
    let (_, weight) = unsent.pop().unwrap();
    assert!(!unsent.accept(Outbound::Delivery(event.clone())), "taken is still under the bound");
    assert!(unsent.accept(Outbound::Delivery(event.clone())), "taken but unwritten still counts");
    unsent.written(weight);
    assert!(unsent.counted() < 3 * 4100);
    // A work connection counts dispatches, not its assignments.
    let mut work = Unsent::new(Role::Work, 100);
    for seq in 0..1000 {
        assert!(!work.accept(assign(seq)));
    }
    assert!(work.accept(Outbound::Delivery(event)));
}

/// Dropped assignments come back as their event ids; a slow connection drops everything else too.
#[test]
fn dropping_assignments_returns_their_event_ids_and_keeps_the_rest_in_order() {
    let mut unsent = Unsent::new(Role::Work, u64::MAX);
    unsent.accept(assign(1));
    unsent.accept(Outbound::Committed { seq: 4 });
    unsent.accept(assign(2));
    unsent.accept(Outbound::Pending { join: 1 });
    let ids = unsent.drop_assignments();
    assert_eq!(ids, [event_id(1), event_id(2)]);
    assert_eq!(unsent.len(), 2);
    unsent.accept(assign(3));
    unsent.push(Outbound::Bye(ByeReason::Slow));
    assert_eq!(unsent.drop_all(), [event_id(3)]);
    assert!(unsent.is_empty());
    assert_eq!(unsent.counted(), 0);
}
