//! Joins, windows, assignment and the worker uuid it is claimed under, leases, release, failure counts, explicit claims (2.4).

use super::*;

#[test]
fn a_window_receives_assignments_round_robin_in_join_order() {
    let mut h = H::volatile().cast();
    h.join(W1, "work", 10);
    h.join(W2, "work", 10);
    let mut order = Vec::new();
    for _ in 0..4 {
        let id = EventId::new_v7();
        let effects = h.produce(P, work("work").with_id(id));
        for session in [W1, W2] {
            if assigned(&effects, session).iter().any(|assignment| assignment.event.id == id) {
                order.push(session);
            }
        }
    }
    assert_eq!(order, [W1, W2, W1, W2], "low load does not pile onto the first worker");
}

/// Assignment, claim and delivery are one step: the payload goes with it,
/// and the envelope carries the uuid of the worker that claimed it — its
/// presence is the claim; there is no token (plan 9.15).
#[test]
fn an_assignment_carries_the_whole_event_and_the_worker_uuid() {
    let mut h = H::volatile().cast();
    h.join(W1, "work", 1);
    h.now = 5_000;
    let effects = h.produce(P, work("work").with_id(id(7)));
    let [assignment] = assigned(&effects, W1).try_into().unwrap();
    assert_eq!((assignment.event.id, assignment.event.payload.as_ref()), (id(7), &b"p"[..]));
    assert_eq!((assignment.lease_ms, assignment.until), (30_000, 35_000), "the lease runs from the assignment");
    assert_eq!(assignment.event.worker, Some(wid(W1)), "the worker uuid its join named");
    let attempt = h.slot(id(7)).unwrap().attempt().unwrap().clone();
    assert_eq!((attempt.worker, attempt.lease_ms), (wid(W1), 30_000), "the ledger holds the same worker");
    assert!(assignment.event.key.is_some());
    assert!(h.slot(id(7)).unwrap().envelope().notice().worker.is_none(), "a notice carries no worker");
}

/// The server counts failures and acts on no count: what to do after so
/// many is the worker's (requirement 6, plan 9.15).
#[test]
fn failures_keep_counting_and_the_server_never_acts_on_them() {
    let mut h = H::volatile().cast();
    h.call(W1, Call::Join { req: 1, worker: wid(W1), channel: Some(ch("work")), types: Vec::new(), lease_ms: 1000, dispatch: false });
    h.call(W1, Call::Window { join: 1, count: 1, bytes: 0 });
    h.produce(P, work("work").with_id(id(1)));
    for round in 1..=7u16 {
        h.advance(1000);
        assert_eq!(h.slot(id(1)).unwrap().failures, round);
        h.call(W1, Call::Window { join: 1, count: 1, bytes: 0 });
    }
    let slot = h.slot(id(1)).unwrap();
    assert_eq!((slot.failures, slot.channel, slot.fixed.event_type), (7, ch("work"), 1), "seven reclaims, the same event where it was");
    let assignment = assigned(&h.advance(0), W1);
    assert!(assignment.is_empty() || assignment[0].event.failures == 7);
    h.slot(id(1)).unwrap().attempt().expect("still assigned like any other event");
    let released = h.call(W1, Call::Release { id: id(1), worker: wid(W1), delay_ms: 0, failed: true });
    assert!(faults(&released, W1).is_empty());
    assert_eq!(h.slot(id(1)).unwrap().failures, 8, "a failed release counts one more");
    assert!(h.slot(id(1)).is_some(), "nothing removes it but a completion or a deletion");
}

#[test]
fn types_are_lanes_and_an_unsupported_type_does_not_block_the_rest() {
    let mut h = H::volatile().cast();
    h.call(W1, Call::Join { req: 1, worker: wid(W1), channel: Some(ch("work")), types: vec![20], lease_ms: 1000, dispatch: false });
    h.call(W1, Call::Window { join: 1, count: 5, bytes: 0 });
    h.produce(P, Envelope::claimable(ch("work"), 37, Bytes::new()).with_id(id(1)));
    let effects = h.produce(P, Envelope::claimable(ch("work"), 20, Bytes::new()).with_id(id(2)));
    assert_eq!(assigned(&effects, W1).len(), 1, "the type-b event behind a type-a one is assigned");
    assert_eq!(h.waiting("work"), 1);
}

/// `more` on the assignment that fills the window; `Pending` once per window
/// (2.4), also to a window said while it is full and work already waits.
#[test]
fn a_full_window_gets_more_on_the_last_assignment_and_pending_once_per_window() {
    let mut h = H::volatile().cast();
    h.call(W1, Call::Join { req: 1, worker: wid(W1), channel: Some(ch("work")), types: Vec::new(), lease_ms: 30_000, dispatch: false });
    for byte in 1..=3 {
        h.produce(P, work("work").with_id(id(byte)));
    }
    let opened = h.call(W1, Call::Window { join: 1, count: 2, bytes: 0 });
    let more: Vec<bool> = assigned(&opened, W1).iter().map(|assignment| assignment.more).collect();
    assert_eq!(more, [false, true], "the one that fills the window says more is waiting");
    let quiet = h.produce(P, work("work").with_id(id(4)));
    assert!(sent(&quiet, W1).is_empty(), "`more` already said it");
    let again = h.call(W1, Call::Window { join: 1, count: 2, bytes: 0 });
    assert_eq!(sent(&again, W1), [Outbound::Pending { join: 1 }], "said while full with work waiting");
    let second = h.produce(P, work("work").with_id(id(5)));
    assert!(sent(&second, W1).is_empty(), "at most one per window");
    h.call(W1, Call::Join { req: 2, worker: wid(W1), channel: Some(ch("late")), types: Vec::new(), lease_ms: 30_000, dispatch: false });
    h.produce(P, work("late").with_id(id(6)));
    let zero = h.call(W1, Call::Window { join: 2, count: 0, bytes: 0 });
    assert_eq!(sent(&zero, W1), [Outbound::Pending { join: 2 }], "a window of 0 said after the work arrived hears it");
}

#[test]
fn a_window_of_zero_refuses_and_a_join_holding_nothing_takes_one_event_over_its_byte_bound() {
    let mut h = H::volatile().cast();
    h.call(W1, Call::Join { req: 1, worker: wid(W1), channel: Some(ch("work")), types: Vec::new(), lease_ms: 30_000, dispatch: false });
    let big = Envelope::claimable(ch("work"), 1, Bytes::from(vec![0; 100]));
    let none = h.produce(P, big.clone().with_id(id(1)));
    assert!(assigned(&none, W1).is_empty(), "no window, no work");
    let opened = h.call(W1, Call::Window { join: 1, count: 5, bytes: 10 });
    assert_eq!(assigned(&opened, W1).len(), 1, "one over the byte bound while holding nothing");
    let next = h.produce(P, big.with_id(id(2)));
    assert!(assigned(&next, W1).is_empty(), "not a second");
}

/// The lease runs from the assignment; when it runs out the event waits again
/// with one more failure and the same id, unclaimed, and the window closes (2.4).
#[test]
fn a_lease_that_runs_out_reclaims_under_the_same_id_and_closes_the_window() {
    let mut h = H::volatile().cast();
    h.call(W1, Call::Join { req: 1, worker: wid(W1), channel: Some(ch("work")), types: Vec::new(), lease_ms: 1000, dispatch: false });
    h.call(W1, Call::Window { join: 1, count: 1, bytes: 0 });
    let first = assigned(&h.produce(P, work("work").with_id(id(1))), W1).remove(0);
    assert_eq!(first.event.worker, Some(wid(W1)));
    assert_eq!(h.s.next_due(), Some(1000), "the wheel wakes at the lease end");
    h.advance(999);
    assert!(h.slot(id(1)).unwrap().attempt().is_some());
    h.advance(1);
    let slot = h.slot(id(1)).expect("the same logical id");
    assert_eq!((slot.failures, slot.attempt().is_some()), (1, false));
    assert_eq!(h.s.joins[&(W1, 1)].count, 0, "the window is closed");
    let again = assigned(&h.join(W2, "work", 1), W2);
    assert_eq!(again[0].event.id, id(1));
    assert_eq!(again[0].event.worker, Some(wid(W2)), "claimed again, under the new worker's uuid");
    assert_eq!(again[0].event.failures, 1);
}

/// A completion that arrives after the lease end but before the reclaim is accepted (2.4).
#[test]
fn a_late_completion_before_the_reclaim_is_accepted_and_after_it_is_dropped() {
    let mut h = H::volatile().cast();
    h.call(W1, Call::Join { req: 1, worker: wid(W1), channel: Some(ch("work")), types: Vec::new(), lease_ms: 1000, dispatch: false });
    h.call(W1, Call::Window { join: 1, count: 2, bytes: 0 });
    let first = assigned(&h.produce(P, work("work").with_id(id(1))), W1).remove(0);
    let second = assigned(&h.produce(P, work("work").with_id(id(2))), W1).remove(0);
    h.now = 5_000;
    // Completed in the same batch before the wheel is looked at: accepted.
    h.complete(W1, &first, Vec::new());
    assert!(h.slot(id(1)).is_none());
    // The second was reclaimed by that batch's end; its completion is stale now.
    h.complete(W1, &second, Vec::new());
    assert!(h.slot(id(2)).is_some());
    assert_eq!(h.s.counters.stale, 1);
}

/// A claim is the event and the worker uuid, and both must match: a report
/// naming the event with another worker's uuid — under the same credential —
/// matches no claim. It is counted stale and changes nothing (plan 9.15).
#[test]
fn a_report_naming_another_worker_is_stale_and_changes_nothing() {
    let mut h = H::volatile().cast();
    h.join(W1, "work", 1);
    let assignment = assigned(&h.produce(P, work("work").with_id(id(1))), W1).remove(0);
    let before = h.slot(id(1)).unwrap().attempt().unwrap().clone();
    let seq = h.next_seq(W2);
    let completed = h.call(W2, Call::Complete { seq, id: id(1), worker: wid(W2), follow_ups: Vec::new(), spooled: Vec::new() });
    assert!(faults(&completed, W2).is_empty(), "late or stale: counted, not told");
    h.now = 5_000;
    h.call(W2, Call::Extend { id: id(1), worker: wid(W2) });
    h.call(W2, Call::Release { id: id(1), worker: wid(W2), delay_ms: 0, failed: true });
    let after = h.slot(id(1)).expect("still held");
    let attempt = after.attempt().expect("still W1's attempt");
    assert_eq!((attempt.worker, attempt.until), (before.worker, before.until), "neither extended nor released");
    assert_eq!(after.failures, 0);
    assert_eq!(h.s.counters.stale, 3, "the completion, the extension and the release");
    let done = h.complete(W1, &assignment, Vec::new());
    assert!(faults(&done, W1).is_empty());
    assert!(h.slot(id(1)).is_none(), "its own worker completes it");
}

/// A closed connection's unwritten assignments go back at once with the same
/// failure count; written ones are due within the reconnect grace (2.4, S20).
#[test]
fn a_closed_connection_returns_unwritten_assignments_at_once_and_pulls_written_ones_to_the_grace() {
    let mut h = H::volatile().cast();
    h.join(W1, "work", 2);
    let unwritten = assigned(&h.produce(P, work("work").with_id(id(1))), W1).remove(0);
    let written = assigned(&h.produce(P, work("work").with_id(id(2))), W1).remove(0);
    h.now = 100;
    h.close(W1, &[unwritten.event.id]);
    let back = h.slot(id(1)).unwrap();
    assert!(matches!(back.place, Place::Waiting) && back.failures == 0, "back at once, failure count unchanged");
    let pulled = h.slot(id(2)).unwrap().attempt().unwrap().until;
    assert_eq!(pulled, 3_100, "min(30 s lease, close + 3 s grace)");
    assert_eq!(written.until, 30_000);
    h.advance(3_000);
    assert!(h.slot(id(2)).unwrap().attempt().is_none(), "due at the grace");
}

/// `Extend` re-arms the lease and moves the attempt to the extending
/// connection's join of the same worker uuid (2.4).
#[test]
fn extend_moves_the_attempt_to_the_extending_connections_join() {
    let mut h = H::volatile().cast();
    h.join(W1, "work", 1);
    assigned(&h.produce(P, work("work").with_id(id(1))), W1).remove(0);
    h.close(W1, &[]);
    h.open(W1 + 10, hello(Role::Work, 2));
    h.join_as(W1 + 10, wid(W1), "work", 1);
    h.now = 2_000;
    h.call(W1 + 10, Call::Extend { id: id(1), worker: wid(W1) });
    let attempt = h.slot(id(1)).unwrap().attempt().unwrap().clone();
    assert_eq!(attempt.until, 32_000, "re-armed at its length from now");
    assert_eq!(attempt.join, Some(h.join_id(W1 + 10, "work")));
    assert_eq!(h.s.joins[&h.join_id(W1 + 10, "work")].claimed, 1, "counted against the new join");
    let next = h.produce(P, work("work").with_id(id(2)));
    assert!(assigned(&next, W1 + 10).is_empty(), "its window of one is taken by the extended attempt");
}

/// `Release{failed = false}` keeps the count, `failed = true` raises it, a delay hides it (2.4).
#[test]
fn release_keeps_or_raises_the_failure_count_and_a_delay_hides_the_event() {
    let mut h = H::volatile().cast();
    h.join(W1, "work", 1);
    let first = assigned(&h.produce(P, work("work").with_id(id(1))), W1).remove(0);
    let again = h.call(W1, Call::Release { id: id(1), worker: wid(W1), delay_ms: 0, failed: false });
    let second = assigned(&again, W1).remove(0);
    assert_eq!((first.event.worker, second.event.worker), (Some(wid(W1)), Some(wid(W1))), "claimed again by the same worker");
    assert_eq!(second.event.failures, 0, "not this event's fault");
    let hidden = h.call(W1, Call::Release { id: id(1), worker: wid(W1), delay_ms: 500, failed: true });
    assert!(assigned(&hidden, W1).is_empty(), "delayed");
    assert_eq!(h.slot(id(1)).unwrap().failures, 1);
    assert_eq!(h.s.next_due(), Some(500));
    let visible = h.advance(500);
    assert_eq!(assigned(&visible, W1)[0].event.id, id(1));
}

/// An explicit claim takes the event from whatever lane it waits in (2.4).
#[test]
fn an_explicit_claim_takes_a_waiting_event_and_refuses_claimed_delayed_or_missing_ones() {
    let mut h = H::volatile().cast();
    h.produce(P, work("work").with_id(id(1)));
    h.produce(P, work("work").with_id(id(2)).with_not_before(Duration::from_secs(5)));
    let claim = |h: &mut H, id| h.call(W1, Call::Claim { req: 9, worker: wid(W1), id, lease_ms: 1000 });
    let taken = claim(&mut h, id(1));
    assert!(matches!(sent(&taken, W1).as_slice(), [Outbound::Claimed { req: 9, assignment }] if assignment.event.id == id(1) && assignment.until == 1000));
    assert_eq!(sent(&claim(&mut h, id(1)), W1), [Outbound::Refused { req: 9, reason: Refusal::AlreadyClaimed }]);
    assert_eq!(sent(&claim(&mut h, id(2)), W1), [Outbound::Refused { req: 9, reason: Refusal::Missing }], "not visible yet");
    assert_eq!(sent(&claim(&mut h, id(3)), W1), [Outbound::Refused { req: 9, reason: Refusal::Missing }]);
}

/// A grant is methods, nothing narrower: with `Claim` every join is allowed,
/// any channel, any type; without it none is (plan 9.15).
#[test]
fn a_join_needs_the_claim_method_and_nothing_narrower() {
    let mut grants = Grants::new();
    grants.insert("svc".into(), Grant { methods: vec![crate::engine::config::Method::Claim] });
    let mut h = H::with(EngineConfig { grants: Some(grants), ..config() }, false);
    h.open(W1, hello(Role::Work, 2));
    let join = |h: &mut H, req, channel: Option<Channel>, types: Vec<u16>| sent(&h.call(W1, Call::Join { req, worker: wid(W1), channel, types, lease_ms: 1000, dispatch: false }), W1);
    assert_eq!(join(&mut h, 3, Some(ch("any")), vec![1]), [Outbound::Joined { req: 3 }], "a type of a channel");
    assert_eq!(join(&mut h, 4, None, vec![9_999]), [Outbound::Joined { req: 4 }], "any type on every channel");
    let mut grants = Grants::new();
    grants.insert("svc".into(), Grant { methods: vec![crate::engine::config::Method::Subscribe] });
    let mut h = H::with(EngineConfig { grants: Some(grants), ..config() }, false);
    h.open(W1, hello(Role::Work, 2));
    assert_eq!(join(&mut h, 8, Some(ch("any")), vec![1]), [Outbound::Refused { req: 8, reason: Refusal::Forbidden }]);
}

/// A join's name (its `req`) names one join: another selection may not take it.
#[test]
fn a_join_name_in_use_is_refused_for_another_selection_and_leaves_nothing_behind() {
    let mut h = H::volatile().cast();
    let join = |h: &mut H, req, types: Vec<u16>| sent(&h.call(W1, Call::Join { req, worker: wid(W1), channel: None, types, lease_ms: 1000, dispatch: false }), W1);
    assert_eq!(join(&mut h, 7, vec![1]), [Outbound::Joined { req: 7 }]);
    assert_eq!(join(&mut h, 7, vec![2]), [Outbound::Refused { req: 7, reason: Refusal::Malformed }], "name 7 holds another selection");
    assert_eq!(join(&mut h, 7, vec![1]), [Outbound::Joined { req: 7 }], "the same selection under its own name changes its terms");
    assert!(!h.s.type_joins.contains_key(&2), "the refused selection left no index entry");
    assert_eq!(h.s.type_joins.get(&1).map(Vec::len), Some(1), "one join for type 1, not two");
    assert_eq!(h.s.sessions.get(&W1).map(|open| open.joins.clone()), Some(vec![7]), "the connection names it once");
}

/// A join names a UUIDv7 channel, a set of types, or both; a type join may
/// not take dispatch events.
#[test]
fn a_join_without_a_valid_selection_is_malformed() {
    let mut h = H::volatile().cast();
    let join = |h: &mut H, req, channel: Option<Channel>, types: Vec<u16>, dispatch| sent(&h.call(W1, Call::Join { req, worker: wid(W1), channel, types, lease_ms: 1000, dispatch }), W1);
    assert_eq!(join(&mut h, 1, None, Vec::new(), false), [Outbound::Refused { req: 1, reason: Refusal::Malformed }], "neither a channel nor a type");
    assert_eq!(join(&mut h, 2, None, vec![1], true), [Outbound::Refused { req: 2, reason: Refusal::Malformed }], "dispatch needs a channel");
    assert_eq!(join(&mut h, 3, Some(Channel([7; 16])), vec![1], false), [Outbound::Refused { req: 3, reason: Refusal::Malformed }], "not a UUIDv7");
    assert_eq!(join(&mut h, 4, Some(Channel::new()), vec![1], false), [Outbound::Joined { req: 4 }]);
}

/// A completion and its follow-ups are one step (2.1).
#[test]
fn a_completion_registers_its_follow_ups_in_the_same_step() {
    let mut h = H::durable(false).cast();
    h.join(W1, "work", 1);
    h.call(O, Call::Subscribe { req: 1, channel: ch("feed"), after: None });
    let assignment = assigned(&h.produce(P, work("work").with_id(id(1))), W1).remove(0);
    let before = h.records.len();
    let effects = h.complete(W1, &assignment, vec![Envelope::claimable(ch("next"), 2, Bytes::new()).with_id(id(2)), Envelope::dispatch(ch("feed"), 19, Bytes::new())]);
    let written: Vec<&Record> = h.records[before..].iter().collect();
    assert!(matches!(written.as_slice(), [Record::Remove(_), Record::Register { event, .. }, ..] if event.event_type == 2), "{written:?}");
    assert!(written.iter().any(|record| matches!(record, Record::Producer { seq: 1, .. })), "the worker's sequence is in the same batch");
    assert!(sent(&effects, O).iter().any(|message| matches!(message, Outbound::Delivery(event) if event.event_type == 19)));
}

/// A follow-up whose id is active: the same content is skipped, other content
/// refuses the whole completion (2.3).
#[test]
fn a_follow_up_with_an_active_id_is_skipped_when_identical_and_refuses_the_completion_when_not() {
    let mut h = H::volatile().cast();
    h.join(W1, "work", 2);
    h.produce(P, work("other").with_id(id(9)));
    let first = assigned(&h.produce(P, work("work").with_id(id(1))), W1).remove(0);
    let conflict = h.complete(W1, &first, vec![Envelope::claimable(ch("other"), 1, Bytes::from_static(b"else")).with_id(id(9)), work("x").with_id(id(3))]);
    assert_eq!(faults(&conflict, W1), [FaultKind::FollowUpConflict]);
    assert!(h.slot(id(1)).is_some() && h.slot(id(3)).is_none(), "nothing happened");
    h.complete(W1, &first, vec![work("other").with_id(id(9)), work("x").with_id(id(3))]);
    assert!(h.slot(id(1)).is_none() && h.slot(id(3)).is_some(), "the identical one was skipped and the rest applied");
    assert_eq!(h.waiting("other"), 1);
}

#[test]
fn a_completion_with_more_follow_ups_than_allowed_is_refused() {
    let mut h = H::with(EngineConfig { max_follow_ups: 2, ..config() }, false).cast();
    h.join(W1, "work", 1);
    let first = assigned(&h.produce(P, work("work").with_id(id(1))), W1).remove(0);
    let effects = h.complete(W1, &first, (0..3).map(|_| work("x")).collect());
    assert_eq!(faults(&effects, W1), [FaultKind::Malformed]);
    assert!(h.slot(id(1)).is_some());
}

/// A claim request without a worker uuid is impossible: a nil uuid in `Join`
/// or `Claim` is refused, `Malformed`, and nothing is claimed (plan 9.15).
#[test]
fn a_join_or_claim_without_a_worker_uuid_is_refused() {
    let mut h = H::volatile().cast();
    h.produce(P, work("work").with_id(id(1)));
    let joined = h.call(W1, Call::Join { req: 1, worker: [0; 16], channel: Some(ch("work")), types: Vec::new(), lease_ms: 1000, dispatch: false });
    assert_eq!(sent(&joined, W1), [Outbound::Refused { req: 1, reason: Refusal::Malformed }]);
    assert!(h.s.sessions[&W1].joins.is_empty(), "no join was made");
    let claimed = h.call(W1, Call::Claim { req: 2, id: id(1), worker: [0; 16], lease_ms: 1000 });
    assert_eq!(sent(&claimed, W1), [Outbound::Refused { req: 2, reason: Refusal::Malformed }]);
    assert!(h.slot(id(1)).unwrap().attempt().is_none(), "still waiting, unclaimed");
}

/// Every report on a claim names the worker uuid: a nil one in `Complete`,
/// `Extend` or `Release` is a fault, `Malformed`, and the claim stands. (A
/// connection is told a fault kind once; each one is counted.)
#[test]
fn a_report_without_a_worker_uuid_is_malformed() {
    let mut h = H::volatile().cast();
    h.join(W1, "work", 1);
    h.produce(P, work("work").with_id(id(1)));
    let seq = h.next_seq(W1);
    let completed = h.call(W1, Call::Complete { seq, id: id(1), worker: [0; 16], follow_ups: Vec::new(), spooled: Vec::new() });
    assert_eq!(faults(&completed, W1), [FaultKind::Malformed]);
    let extended = h.call(W1, Call::Extend { id: id(1), worker: [0; 16] });
    assert!(faults(&extended, W1).is_empty(), "told once per connection");
    let released = h.call(W2, Call::Release { id: id(1), worker: [0; 16], delay_ms: 0, failed: true });
    assert_eq!(faults(&released, W2), [FaultKind::Malformed]);
    h.call(W2, Call::Extend { id: id(1), worker: [0; 16] });
    assert_eq!((h.s.counters.faults, h.s.counters.stale), (4, 0), "each one a fault, none stale");
    let slot = h.slot(id(1)).unwrap();
    assert_eq!((slot.attempt().map(|attempt| attempt.worker), slot.failures), (Some(wid(W1)), 0), "the claim stands");
}

/// A failure report names the event and its worker; when both match, the
/// worker uuid leaves the event and its failure count rises, so another
/// worker may claim it (plan 9.15).
#[test]
fn a_matching_failure_report_removes_the_worker_and_raises_the_count_for_another_worker() {
    let mut h = H::volatile().cast();
    h.join(W1, "work", 1);
    let first = assigned(&h.produce(P, work("work").with_id(id(1))), W1).remove(0);
    assert_eq!((first.event.worker, first.event.failures), (Some(wid(W1)), 0));
    h.call(W1, Call::Window { join: h.join_id(W1, "work").1, count: 0, bytes: 0 });
    let released = h.call(W1, Call::Release { id: id(1), worker: wid(W1), delay_ms: 0, failed: true });
    assert!(faults(&released, W1).is_empty());
    let slot = h.slot(id(1)).unwrap();
    assert_eq!((slot.attempt().is_none(), slot.failures, slot.envelope().worker), (true, 1, None), "no worker, one failure");
    let again = assigned(&h.join(W2, "work", 1), W2);
    assert_eq!(again.len(), 1);
    assert_eq!((again[0].event.id, again[0].event.worker, again[0].event.failures), (id(1), Some(wid(W2)), 1), "another worker claims it");
}

/// A failure report naming the event with a worker uuid that does not hold
/// it matches nothing: counted stale, the count unchanged, the claim kept.
#[test]
fn a_failure_report_with_the_wrong_worker_is_stale_and_keeps_the_claim() {
    let mut h = H::volatile().cast();
    h.join(W1, "work", 1);
    h.produce(P, work("work").with_id(id(1)));
    let released = h.call(W1, Call::Release { id: id(1), worker: wid(W2), delay_ms: 0, failed: true });
    assert!(faults(&released, W1).is_empty(), "counted, not told");
    assert_eq!(h.s.counters.stale, 1);
    let slot = h.slot(id(1)).unwrap();
    assert_eq!((slot.attempt().map(|attempt| attempt.worker), slot.failures), (Some(wid(W1)), 0), "still W1's, no failure counted");
}
