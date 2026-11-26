//! Joins, windows, assignment, leases, release, isolation, groups, explicit claims (2.4).

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

/// Assignment, claim and delivery are one step: the payload goes with it.
#[test]
fn an_assignment_carries_the_whole_event_its_token_and_the_lease_from_now() {
    let mut h = H::volatile().cast();
    h.join(W1, "work", 1);
    h.now = 5_000;
    let effects = h.produce(P, work("work").with_id(id(7)));
    let [assignment] = assigned(&effects, W1).try_into().unwrap();
    assert_eq!((assignment.event.id, assignment.event.payload.as_ref()), (id(7), &b"p"[..]));
    assert_eq!((assignment.lease_ms, assignment.until), (30_000, 35_000), "the lease runs from the assignment");
    assert_eq!(assignment.event.principal.as_deref(), Some("svc"));
    assert!(assignment.event.key.is_some());
}

#[test]
fn types_are_lanes_and_an_unsupported_type_does_not_block_the_rest() {
    let mut h = H::volatile().cast();
    h.call(W1, Call::Join { req: 1, channel: Some(ch("work")), types: vec![20], isolated: false, lease_ms: 1000, dispatch: false });
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
    h.call(W1, Call::Join { req: 1, channel: Some(ch("work")), types: Vec::new(), isolated: false, lease_ms: 30_000, dispatch: false });
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
    h.call(W1, Call::Join { req: 2, channel: Some(ch("late")), types: Vec::new(), isolated: false, lease_ms: 30_000, dispatch: false });
    h.produce(P, work("late").with_id(id(6)));
    let zero = h.call(W1, Call::Window { join: 2, count: 0, bytes: 0 });
    assert_eq!(sent(&zero, W1), [Outbound::Pending { join: 2 }], "a window of 0 said after the work arrived hears it");
}

#[test]
fn a_window_of_zero_refuses_and_a_join_holding_nothing_takes_one_event_over_its_byte_bound() {
    let mut h = H::volatile().cast();
    h.call(W1, Call::Join { req: 1, channel: Some(ch("work")), types: Vec::new(), isolated: false, lease_ms: 30_000, dispatch: false });
    let big = Envelope::claimable(ch("work"), 1, Bytes::from(vec![0; 100]));
    let none = h.produce(P, big.clone().with_id(id(1)));
    assert!(assigned(&none, W1).is_empty(), "no window, no work");
    let opened = h.call(W1, Call::Window { join: 1, count: 5, bytes: 10 });
    assert_eq!(assigned(&opened, W1).len(), 1, "one over the byte bound while holding nothing");
    let next = h.produce(P, big.with_id(id(2)));
    assert!(assigned(&next, W1).is_empty(), "not a second");
}

/// The lease runs from the assignment; when it runs out the event waits again
/// with one more failure, the same id and a new token, and the window closes (2.4).
#[test]
fn a_lease_that_runs_out_reclaims_under_the_same_id_and_closes_the_window() {
    let mut h = H::volatile().cast();
    h.call(W1, Call::Join { req: 1, channel: Some(ch("work")), types: Vec::new(), isolated: false, lease_ms: 1000, dispatch: false });
    h.call(W1, Call::Window { join: 1, count: 1, bytes: 0 });
    let first = assigned(&h.produce(P, work("work").with_id(id(1))), W1).remove(0);
    assert_eq!(h.s.next_due(), Some(1000), "the wheel wakes at the lease end");
    h.advance(999);
    assert!(h.slot(id(1)).unwrap().attempt().is_some());
    h.advance(1);
    let slot = h.slot(id(1)).expect("the same logical id");
    assert_eq!((slot.failures, slot.attempt().is_some()), (1, false));
    assert_eq!(h.s.joins[&(W1, 1)].count, 0, "the window is closed");
    let again = assigned(&h.join(W2, "work", 1), W2);
    assert_eq!(again[0].event.id, id(1));
    assert!(again[0].token > first.token, "a new attempt, a new token");
    assert_eq!(again[0].event.failures, 1);
}

/// A completion that arrives after the lease end but before the reclaim is accepted (2.4).
#[test]
fn a_late_completion_before_the_reclaim_is_accepted_and_after_it_is_dropped() {
    let mut h = H::volatile().cast();
    h.call(W1, Call::Join { req: 1, channel: Some(ch("work")), types: Vec::new(), isolated: false, lease_ms: 1000, dispatch: false });
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

#[test]
fn a_completion_under_another_credential_is_a_fault() {
    let mut h = H::volatile().cast();
    h.open(20, Hello { credential: "other".into(), ..hello(Role::Work, 20) });
    h.join(W1, "work", 1);
    let assignment = assigned(&h.produce(P, work("work").with_id(id(1))), W1).remove(0);
    let seq = h.next_seq(20);
    let effects = h.call(20, Call::Complete { seq, id: id(1), token: assignment.token, follow_ups: Vec::new(), spooled: Vec::new() });
    assert_eq!(faults(&effects, 20), [FaultKind::Forbidden]);
    assert!(h.slot(id(1)).is_some());
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
    h.close(W1, &[unwritten.token]);
    let back = h.slot(id(1)).unwrap();
    assert!(matches!(back.place, Place::Waiting) && back.failures == 0, "back at once, failure count unchanged");
    let pulled = h.slot(id(2)).unwrap().attempt().unwrap().until;
    assert_eq!(pulled, 3_100, "min(30 s lease, close + 3 s grace)");
    assert_eq!(written.until, 30_000);
    h.advance(3_000);
    assert!(h.slot(id(2)).unwrap().attempt().is_none(), "due at the grace");
}

/// `Extend` re-arms the lease and moves the attempt to the extending connection's join (2.4).
#[test]
fn extend_moves_the_attempt_to_the_extending_connections_join() {
    let mut h = H::volatile().cast();
    h.join(W1, "work", 1);
    let assignment = assigned(&h.produce(P, work("work").with_id(id(1))), W1).remove(0);
    h.close(W1, &[]);
    h.open(W1 + 10, hello(Role::Work, 2));
    h.join(W1 + 10, "work", 1);
    h.now = 2_000;
    h.call(W1 + 10, Call::Extend { id: id(1), token: assignment.token });
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
    let again = h.call(W1, Call::Release { id: id(1), token: first.token, delay_ms: 0, failed: false });
    let second = assigned(&again, W1).remove(0);
    assert_eq!(second.event.failures, 0, "not this event's fault");
    let hidden = h.call(W1, Call::Release { id: id(1), token: second.token, delay_ms: 500, failed: true });
    assert!(assigned(&hidden, W1).is_empty(), "delayed");
    assert_eq!(h.slot(id(1)).unwrap().failures, 1);
    assert_eq!(h.s.next_due(), Some(500));
    let visible = h.advance(500);
    assert_eq!(assigned(&visible, W1)[0].event.id, id(1));
}

/// The n-th counted failure isolates; "not this event's fault" never does (2.4, B6).
#[test]
fn the_fifth_counted_failure_isolates_and_releases_without_failure_never_do() {
    let mut h = H::volatile().cast();
    h.join(W1, "work", 1);
    let mut current = assigned(&h.produce(P, work("work").with_id(id(1))), W1).remove(0);
    for _ in 0..10 {
        current = assigned(&h.call(W1, Call::Release { id: id(1), token: current.token, delay_ms: 0, failed: false }), W1).remove(0);
    }
    assert_eq!(h.s.counters.isolated, 0, "ten downstream failures isolate nothing");
    for round in 1..=5 {
        let effects = h.call(W1, Call::Release { id: id(1), token: current.token, delay_ms: 0, failed: true });
        if round < 5 {
            current = assigned(&effects, W1).remove(0);
        }
    }
    let slot = h.slot(id(1)).expect("the same id, isolated");
    assert_eq!((slot.channel, slot.fixed.event_type, slot.isolated, slot.failures), (ch("work"), 1, true, 5), "in place: its channel and type stay");
    assert_eq!(slot.fixed.root, 1, "the root stays");
    assert_eq!(slot.expires, Some(h.now + 7 * 24 * 3600 * 1000), "kept for the type's keep time");
    assert_eq!(h.s.counters.isolated, 1);
    // A lease expiry counts the same as a failed release.
    let mut h = H::volatile().cast();
    h.call(W1, Call::Join { req: 1, channel: Some(ch("work")), types: Vec::new(), isolated: false, lease_ms: 10, dispatch: false });
    h.produce(P, work("work").with_id(id(2)));
    for _ in 0..5 {
        h.call(W1, Call::Window { join: 1, count: 1, bytes: 0 });
        h.advance(10);
    }
    assert!(h.slot(id(2)).unwrap().isolated);
}

/// An isolated event waits only for joins that ask for isolated events; the
/// channel's subscribers hear it with the isolated bit set; failing again it
/// stays isolated, with its expiry and without a second count (B6).
#[test]
fn an_isolated_event_waits_in_place_for_isolated_joins_only() {
    let mut h = H::volatile().cast();
    h.call(O, Call::Subscribe { req: 1, channel: ch("work"), after: None });
    h.call(W1, Call::Join { req: 1, channel: Some(ch("work")), types: Vec::new(), isolated: false, lease_ms: 30_000, dispatch: false });
    h.call(W1, Call::Window { join: 1, count: 1, bytes: 0 });
    let mut current = assigned(&h.produce(P, work("work").with_id(id(1))), W1).remove(0);
    let mut last = Effects::default();
    for round in 1..=5 {
        last = h.call(W1, Call::Release { id: id(1), token: current.token, delay_ms: 0, failed: true });
        if round < 5 {
            current = assigned(&last, W1).remove(0);
        }
    }
    assert!(assigned(&last, W1).is_empty(), "the plain join does not take it");
    let told: Vec<Envelope> = sent(&last, O)
        .into_iter()
        .filter_map(|message| match message {
            Outbound::Notice(event) => Some(event),
            _ => None,
        })
        .collect();
    assert!(matches!(told.as_slice(), [event] if event.id == id(1) && event.isolated && event.channel == ch("work")), "{told:?}");
    let expires = h.slot(id(1)).unwrap().expires;
    // A type join for isolated events of type 1 takes it, with the bit set.
    let taken = h.call(W2, Call::Join { req: 7, channel: None, types: vec![1], isolated: true, lease_ms: 30_000, dispatch: false });
    assert_eq!(sent(&taken, W2), [Outbound::Joined { req: 7 }]);
    let opened = h.call(W2, Call::Window { join: 7, count: 1, bytes: 0 });
    let again = assigned(&opened, W2).remove(0);
    assert!(again.event.isolated && again.event.id == id(1));
    // A fresh event on the same channel goes to the plain join, not the isolated one.
    let fresh = h.produce(P, work("work").with_id(id(2)));
    assert_eq!(assigned(&fresh, W1).len(), 1);
    // Failing again: still isolated, the same expiry, not counted twice.
    h.call(W2, Call::Window { join: 7, count: 0, bytes: 0 });
    h.call(W2, Call::Release { id: id(1), token: again.token, delay_ms: 100, failed: true });
    let slot = h.slot(id(1)).unwrap();
    assert!(slot.isolated);
    assert_eq!((slot.failures, slot.expires, h.s.counters.isolated), (6, expires, 1));
    assert!(matches!(slot.place, Place::Waiting), "back in its isolated lane at once");
}

/// An isolated join and a plain join of one connection on the same channel
/// select different lanes; they do not overlap.
#[test]
fn an_isolated_join_and_a_plain_join_do_not_overlap() {
    let mut h = H::volatile().cast();
    let plain = h.call(W1, Call::Join { req: 1, channel: Some(ch("work")), types: vec![1], isolated: false, lease_ms: 1000, dispatch: false });
    let isolated = h.call(W1, Call::Join { req: 2, channel: None, types: vec![1], isolated: true, lease_ms: 1000, dispatch: false });
    let overlap = h.call(W1, Call::Join { req: 3, channel: None, types: vec![1, 2], isolated: false, lease_ms: 1000, dispatch: false });
    assert_eq!(sent(&plain, W1), [Outbound::Joined { req: 1 }]);
    assert_eq!(sent(&isolated, W1), [Outbound::Joined { req: 2 }]);
    assert_eq!(sent(&overlap, W1), [Outbound::Refused { req: 3, reason: Refusal::Overlap }]);
}

/// B26: for a `serialize_group` type one event of a group on a channel is out at a time.
#[test]
fn serialize_group_assigns_one_event_of_a_group_at_a_time() {
    let mut config = config();
    config.types.rules.insert(0, rule([1], TypeSettings { serialize_group: true, ..TypeSettings::default() }));
    let mut h = H::with(config, false).cast();
    h.join(W1, "orders", 10);
    let first = h.produce(P, work("orders").with_group([1; 16]).with_id(id(1)));
    let second = h.produce(P, work("orders").with_group([1; 16]).with_id(id(2)));
    let other = h.produce(P, work("orders").with_group([2; 16]).with_id(id(3)));
    assert_eq!((assigned(&first, W1).len(), assigned(&second, W1).len(), assigned(&other, W1).len()), (1, 0, 1), "group 1 waits; group 2 goes past it");
    let done = h.complete(W1, &assigned(&first, W1)[0], Vec::new());
    assert_eq!(assigned(&done, W1)[0].event.id, id(2), "the next of group 1 once the first is done");
    // The same type without the setting assigns both at once.
    let mut h = H::volatile().cast();
    h.join(W1, "orders", 10);
    h.produce(P, work("orders").with_group([1; 16]).with_id(id(1)));
    let both = h.produce(P, work("orders").with_group([1; 16]).with_id(id(2)));
    assert_eq!(assigned(&both, W1).len(), 1);
}

/// An explicit claim takes the event from whatever lane it waits in (2.4).
#[test]
fn an_explicit_claim_takes_a_waiting_event_and_refuses_claimed_delayed_or_missing_ones() {
    let mut h = H::volatile().cast();
    h.produce(P, work("work").with_id(id(1)));
    h.produce(P, work("work").with_id(id(2)).with_not_before(Duration::from_secs(5)));
    let claim = |h: &mut H, id| h.call(W1, Call::Claim { req: 9, id, lease_ms: 1000 });
    let taken = claim(&mut h, id(1));
    assert!(matches!(sent(&taken, W1).as_slice(), [Outbound::Claimed { req: 9, assignment }] if assignment.event.id == id(1) && assignment.until == 1000));
    assert_eq!(sent(&claim(&mut h, id(1)), W1), [Outbound::Refused { req: 9, reason: Refusal::AlreadyClaimed }]);
    assert_eq!(sent(&claim(&mut h, id(2)), W1), [Outbound::Refused { req: 9, reason: Refusal::Missing }], "not visible yet");
    assert_eq!(sent(&claim(&mut h, id(3)), W1), [Outbound::Refused { req: 9, reason: Refusal::Missing }]);
}

/// The claim grant covers the types a join asks for; a join for every type
/// needs every type (2.7, plan 9.15). Channels do not matter.
#[test]
fn a_join_is_refused_for_a_type_outside_the_claim_grant() {
    let mut grants = Grants::new();
    grants.insert("svc".into(), Grant { methods: vec![crate::engine::config::Method::Claim], types: TypeSet::from_ranges([(10, 19)]), delegate: false });
    let mut h = H::with(EngineConfig { grants: Some(grants), ..config() }, false);
    h.open(W1, hello(Role::Work, 2));
    let join = |h: &mut H, req, channel: Option<Channel>, types: Vec<u16>| sent(&h.call(W1, Call::Join { req, channel, types, isolated: false, lease_ms: 1000, dispatch: false }), W1);
    assert_eq!(join(&mut h, 3, Some(ch("any")), vec![20]), [Outbound::Refused { req: 3, reason: Refusal::Forbidden }]);
    assert_eq!(join(&mut h, 4, Some(ch("any")), vec![10, 20]), [Outbound::Refused { req: 4, reason: Refusal::Forbidden }], "one ungranted type refuses the join");
    assert_eq!(join(&mut h, 5, Some(ch("any")), Vec::new()), [Outbound::Refused { req: 5, reason: Refusal::Forbidden }], "every type needs every type");
    assert_eq!(join(&mut h, 6, Some(ch("any")), vec![10, 19]), [Outbound::Joined { req: 6 }]);
    assert_eq!(join(&mut h, 7, None, vec![15]), [Outbound::Joined { req: 7 }], "a type join on every channel");
    // Without the claim method nothing is granted.
    let mut grants = Grants::new();
    grants.insert("svc".into(), Grant { methods: vec![crate::engine::config::Method::Subscribe], types: TypeSet::all(), delegate: false });
    let mut h = H::with(EngineConfig { grants: Some(grants), ..config() }, false);
    h.open(W1, hello(Role::Work, 2));
    assert_eq!(join(&mut h, 8, Some(ch("any")), vec![1]), [Outbound::Refused { req: 8, reason: Refusal::Forbidden }]);
}

/// A join's name (its `req`) names one join: another selection may not take it.
#[test]
fn a_join_name_in_use_is_refused_for_another_selection_and_leaves_nothing_behind() {
    let mut h = H::volatile().cast();
    let join = |h: &mut H, req, types: Vec<u16>| sent(&h.call(W1, Call::Join { req, channel: None, types, isolated: false, lease_ms: 1000, dispatch: false }), W1);
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
    let join = |h: &mut H, req, channel: Option<Channel>, types: Vec<u16>, dispatch| sent(&h.call(W1, Call::Join { req, channel, types, isolated: false, lease_ms: 1000, dispatch }), W1);
    assert_eq!(join(&mut h, 1, None, Vec::new(), false), [Outbound::Refused { req: 1, reason: Refusal::Malformed }], "neither a channel nor a type");
    assert_eq!(join(&mut h, 2, None, vec![1], true), [Outbound::Refused { req: 2, reason: Refusal::Malformed }], "dispatch needs a channel");
    assert_eq!(join(&mut h, 3, Some(Channel([7; 16])), vec![1], false), [Outbound::Refused { req: 3, reason: Refusal::Malformed }], "not a UUIDv7");
    assert_eq!(join(&mut h, 4, Some(Channel::new()), vec![1], false), [Outbound::Joined { req: 4 }]);
}

/// A completion and its follow-ups are one step; the follow-ups' root is the parent's (2.1).
#[test]
fn a_completion_registers_its_follow_ups_in_the_same_step_under_the_parents_root() {
    let mut h = H::durable(false).cast();
    h.join(W1, "work", 1);
    h.call(O, Call::Subscribe { req: 1, channel: ch("feed"), after: None });
    let assignment = assigned(&h.produce(P, work("work").with_id(id(1))), W1).remove(0);
    let before = h.records.len();
    let effects = h.complete(W1, &assignment, vec![Envelope::claimable(ch("next"), 2, Bytes::new()).with_id(id(2)), Envelope::dispatch(ch("feed"), 19, Bytes::new())]);
    let written: Vec<&Record> = h.records[before..].iter().collect();
    assert!(matches!(written.as_slice(), [Record::Remove(_), Record::Register { root: 1, event, .. }, ..] if event.event_type == 2), "{written:?}");
    assert!(written.iter().any(|record| matches!(record, Record::Producer { seq: 1, .. })), "the worker's sequence is in the same batch");
    assert!(sent(&effects, O).iter().any(|message| matches!(message, Outbound::Delivery(event) if event.event_type == 19)));
    assert_eq!(h.slot(id(2)).unwrap().fixed.root, 1, "the parent's type is the root");
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
