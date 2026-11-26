//! Producer sequences, cumulative acknowledgement, instance nonces, digests (2.3).

use super::*;

#[test]
fn a_retransmitted_sequence_is_dropped_and_a_gap_closes_the_connection() {
    let mut h = H::volatile().cast();
    let event = work("work").with_id(id(1));
    h.call(P, Call::Register { seq: Some(1), event: event.clone() });
    h.call(P, Call::Register { seq: Some(1), event: work("work").with_id(id(2)) });
    assert_eq!(h.waiting("work"), 1, "the retransmission was dropped silently");
    assert_eq!(h.s.counters.retransmits, 1);
    let gap = h.call(P, Call::Register { seq: Some(3), event: work("work").with_id(id(3)) });
    assert_eq!(faults(&gap, P), [FaultKind::SequenceGap]);
    assert!(sent(&gap, P).contains(&Outbound::Bye(ByeReason::Protocol)));
    assert!(closed(&gap, P));
    assert_eq!(h.waiting("work"), 1, "nothing after the gap is applied");
}

/// A claimable registration is acknowledged after its batch is on disk, a
/// dispatch event at the end of the ledger batch; cumulatively (2.3, B2).
/// With a journal every claimable event is journaled: no type is volatile.
#[test]
fn committed_waits_for_the_disk_for_claimable_work_and_not_for_dispatch() {
    let mut h = H::durable(false).cast();
    let light = h.produce(P, Envelope::dispatch(ch("feed"), 10, Bytes::new()));
    assert_eq!(committed(&light, P), [1], "a dispatch event commits when applied");
    let durable = h.produce(P, work("work").with_id(id(2)));
    assert!(committed(&durable, P).is_empty(), "a claimable one waits for its batch");
    assert!(h.records.iter().any(|record| matches!(record, Record::Register { event, .. } if event.id == id(2))), "it is journaled");
    let after = h.produce(P, Envelope::dispatch(ch("feed"), 10, Bytes::new()));
    assert!(committed(&after, P).is_empty(), "cumulative: a dispatch behind a claimable one waits too");
    let landed = h.land();
    assert_eq!(committed(&landed, P), [3], "the landing confirms everything up to the last applied");
    let dispatched = h.produce(P, Envelope::dispatch(ch("feed"), 10, Bytes::new()));
    assert_eq!(committed(&dispatched, P), [4]);
}

/// Past N confirmable operations a `Committed` goes out even while later
/// work still waits for the disk; below N it waits for the next trigger (S2).
#[test]
fn committed_goes_out_once_every_n_while_later_work_still_waits() {
    for (every, expected) in [(4, vec![5]), (8, Vec::new())] {
        let mut h = H::with(EngineConfig { ack_every: every, ..config() }, true).cast();
        for byte in 1..=5 {
            h.produce(P, work("work").with_id(id(byte)));
        }
        let first = h.launch();
        h.produce(P, work("work").with_id(id(6)));
        let landed = h.land_through(first);
        assert_eq!(committed(&landed, P), expected, "N = {every}");
        let rest = h.land();
        assert_eq!(committed(&rest, P), [6], "nothing waits any more: the last one goes out");
    }
}

#[test]
fn a_volatile_broadcast_producer_can_turn_acknowledgements_off() {
    let mut h = H::volatile();
    h.open(P, Hello { acks: false, ..hello(Role::Produce, 1) });
    let effects = h.produce(P, Envelope::dispatch(ch("feed"), 10, Bytes::new()));
    assert!(committed(&effects, P).is_empty());
}

/// The same process reconnecting replaces its connection quietly; another
/// instance under the same id pushes the first out with a reason (2.3).
#[test]
fn the_same_process_reconnecting_replaces_quietly_and_another_instance_supersedes() {
    let mut h = H::volatile();
    h.open(10, hello(Role::Produce, 1));
    let again = h.open(11, hello(Role::Produce, 1));
    assert!(closed(&again, 10), "the old connection is closed");
    assert!(!sent(&again, 10).iter().any(|message| matches!(message, Outbound::Bye(_))), "quietly");
    assert!(matches!(sent(&again, 11).as_slice(), [Outbound::Welcome(welcome)] if !welcome.superseded));
    let other = h.open(12, Hello { nonce: [9; 16], ..hello(Role::Produce, 1) });
    assert_eq!(sent(&other, 11), [Outbound::Bye(ByeReason::Superseded)]);
    assert!(closed(&other, 11));
    assert!(matches!(sent(&other, 12).as_slice(), [Outbound::Welcome(welcome)] if welcome.superseded));
}

/// `resume` is the last applied sequence, sent once it is committed (2.3).
#[test]
fn welcome_resumes_at_the_last_applied_sequence_after_it_is_committed() {
    let mut h = H::durable(false).cast();
    h.produce(P, work("work").with_id(id(1)));
    h.produce(P, work("work").with_id(id(2)));
    h.close(P, &[]);
    let reopened = h.open(P, hello(Role::Produce, 1));
    let welcome = reopened.sends.iter().find(|send| send.session == P).expect("welcome");
    assert_eq!(welcome.gate, Some(h.carrying), "held until the producer's last batch is on disk");
    assert!(matches!(&welcome.message, Some(Outbound::Welcome(welcome)) if welcome.resume == 2));
}

#[test]
fn a_duplicate_id_with_the_same_content_is_dropped_and_other_content_is_a_conflict_told_once() {
    let mut h = H::volatile().cast();
    h.produce(P, work("work").with_id(id(1)));
    let same = h.produce(P, work("work").with_id(id(1)));
    assert!(faults(&same, P).is_empty());
    assert_eq!(h.s.counters.duplicates, 1);
    let other = Envelope::claimable(ch("work"), 1, Bytes::from_static(b"other")).with_id(id(1));
    let conflict = h.produce(P, other.clone());
    assert_eq!(faults(&conflict, P), [FaultKind::IdConflict]);
    let again = h.produce(P, other);
    assert!(faults(&again, P).is_empty(), "told once per connection and kind; counted always");
    assert_eq!(h.s.counters.faults, 2);
    assert_eq!(h.waiting("work"), 1);
}

/// Each case is told by its kind; nothing of them is applied. A grant is
/// methods only: the channel and the type never decide (plan 9.15).
#[test]
fn malformed_requests_invalid_ids_and_forbidden_kinds_are_faults() {
    let mut grants = Grants::new();
    grants.insert("svc".into(), Grant { methods: vec![crate::engine::config::Method::RegisterClaimable] });
    let mut h = H::with(EngineConfig { grants: Some(grants), ..config() }, false);
    h.open(P, hello(Role::Produce, 1));
    let claim = crate::envelope::ClaimMark { worker: [2; 16], token: ClaimToken::default(), lease_ms: 1 };
    let cases: Vec<(Envelope, FaultKind)> = vec![
        (Envelope { failures: 1, ..work("pub.x") }, FaultKind::Malformed),
        (Envelope { claim: Some(claim), ..work("pub.x") }, FaultKind::Malformed),
        (Envelope::claimable(ch("pub.x"), crate::envelope::SERVER_TYPE, Bytes::new()), FaultKind::Malformed),
        (work("pub.x").with_id(EventId([0xff; 16])), FaultKind::InvalidId),
        (Envelope::dispatch(ch("pub.x"), 2, Bytes::new()), FaultKind::Forbidden),
    ];
    for (event, kind) in cases {
        let effects = h.produce(P, event);
        assert!(faults(&effects, P).is_empty() || faults(&effects, P) == [kind], "{kind:?}");
        assert_eq!(h.s.sessions[&P].faults & kind.bit(), kind.bit(), "{kind:?} was told");
    }
    assert_eq!(h.s.live(), 0);
}

/// Type 0 is the server's own: no producer registers it, even with every grant.
#[test]
fn registering_the_servers_own_type_is_malformed() {
    let mut h = H::volatile().cast();
    let effects = h.produce(P, Envelope::dispatch(ch("feed"), crate::envelope::SERVER_TYPE, Bytes::new()));
    assert_eq!(faults(&effects, P), [FaultKind::Malformed]);
}

/// A production connection whose credential may register nothing is sent
/// away before `Welcome` (2.7).
#[test]
fn a_producer_that_may_register_nothing_is_refused_before_welcome() {
    let mut grants = Grants::new();
    grants.insert("svc".into(), Grant { methods: vec![crate::engine::config::Method::Subscribe] });
    grants.insert("writer".into(), Grant { methods: vec![crate::engine::config::Method::RegisterDispatch] });
    let mut h = H::with(EngineConfig { grants: Some(grants), ..config() }, false);
    let refused = h.open(P, hello(Role::Produce, 1));
    assert_eq!(sent(&refused, P), [Outbound::Bye(ByeReason::Forbidden)]);
    let accepted = h.open(P + 10, Hello { credential: "writer".into(), ..hello(Role::Produce, 2) });
    assert!(matches!(sent(&accepted, P + 10).as_slice(), [Outbound::Welcome(_)]));
}

#[test]
fn a_call_the_role_does_not_send_is_refused() {
    let mut h = H::volatile().cast();
    let effects = h.call(P, Call::Join { req: 5, channel: Some(ch("work")), types: Vec::new(), lease_ms: 1000, dispatch: false });
    assert_eq!(faults(&effects, P), [FaultKind::Malformed]);
    assert!(sent(&effects, P).contains(&Outbound::Refused { req: 5, reason: Refusal::Malformed }));
    let emit = h.call(W1, Call::Register { seq: None, event: work("work") });
    assert_eq!(faults(&emit, W1), [FaultKind::Malformed], "a work connection emits dispatch events only");
}
