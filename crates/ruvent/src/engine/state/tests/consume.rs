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
    h.call(W1, Call::Join { req: 1, channel: "work".into(), types: vec!["b".into()], lease_ms: 1000, dispatch: false });
    h.call(W1, Call::Window { channel: "work".into(), count: 5, bytes: 0 });
    h.produce(P, Envelope::claimable("work", "a", Bytes::new()).with_id(id(1)));
    let effects = h.produce(P, Envelope::claimable("work", "b", Bytes::new()).with_id(id(2)));
    assert_eq!(assigned(&effects, W1).len(), 1, "the type-b event behind a type-a one is assigned");
    assert_eq!(h.waiting("work"), 1);
}

/// `more` on the assignment that fills the window; `Pending` once per window
/// (2.4), also to a window said while it is full and work already waits.
#[test]
fn a_full_window_gets_more_on_the_last_assignment_and_pending_once_per_window() {
    let mut h = H::volatile().cast();
    h.call(W1, Call::Join { req: 1, channel: "work".into(), types: Vec::new(), lease_ms: 30_000, dispatch: false });
    for byte in 1..=3 {
        h.produce(P, work("work").with_id(id(byte)));
    }
    let opened = h.call(W1, Call::Window { channel: "work".into(), count: 2, bytes: 0 });
    let more: Vec<bool> = assigned(&opened, W1).iter().map(|assignment| assignment.more).collect();
    assert_eq!(more, [false, true], "the one that fills the window says more is waiting");
    let quiet = h.produce(P, work("work").with_id(id(4)));
    assert!(sent(&quiet, W1).is_empty(), "`more` already said it");
    let again = h.call(W1, Call::Window { channel: "work".into(), count: 2, bytes: 0 });
    assert_eq!(sent(&again, W1), [Outbound::Pending { channel: "work".into() }], "said while full with work waiting");
    let second = h.produce(P, work("work").with_id(id(5)));
    assert!(sent(&second, W1).is_empty(), "at most one per window");
    h.call(W1, Call::Join { req: 2, channel: "late".into(), types: Vec::new(), lease_ms: 30_000, dispatch: false });
    h.produce(P, work("late").with_id(id(6)));
    let zero = h.call(W1, Call::Window { channel: "late".into(), count: 0, bytes: 0 });
    assert_eq!(sent(&zero, W1), [Outbound::Pending { channel: "late".into() }], "a window of 0 said after the work arrived hears it");
}

#[test]
fn a_window_of_zero_refuses_and_a_join_holding_nothing_takes_one_event_over_its_byte_bound() {
    let mut h = H::volatile().cast();
    h.call(W1, Call::Join { req: 1, channel: "work".into(), types: Vec::new(), lease_ms: 30_000, dispatch: false });
    let big = Envelope::claimable("work", "do", Bytes::from(vec![0; 100]));
    let none = h.produce(P, big.clone().with_id(id(1)));
    assert!(assigned(&none, W1).is_empty(), "no window, no work");
    let opened = h.call(W1, Call::Window { channel: "work".into(), count: 5, bytes: 10 });
    assert_eq!(assigned(&opened, W1).len(), 1, "one over the byte bound while holding nothing");
    let next = h.produce(P, big.with_id(id(2)));
    assert!(assigned(&next, W1).is_empty(), "not a second");
}

/// The lease runs from the assignment; when it runs out the event waits again
/// with one more failure, the same id and a new token, and the window closes (2.4).
#[test]
fn a_lease_that_runs_out_reclaims_under_the_same_id_and_closes_the_window() {
    let mut h = H::volatile().cast();
    h.call(W1, Call::Join { req: 1, channel: "work".into(), types: Vec::new(), lease_ms: 1000, dispatch: false });
    h.call(W1, Call::Window { channel: "work".into(), count: 1, bytes: 0 });
    let first = assigned(&h.produce(P, work("work").with_id(id(1))), W1).remove(0);
    assert_eq!(h.s.next_due(), Some(1000), "the wheel wakes at the lease end");
    h.advance(999);
    assert!(h.slot(id(1)).unwrap().attempt().is_some());
    h.advance(1);
    let slot = h.slot(id(1)).expect("the same logical id");
    assert_eq!((slot.failures, slot.attempt().is_some()), (1, false));
    assert_eq!(h.s.joins[&(W1, Name::new("work"))].count, 0, "the window is closed");
    let again = assigned(&h.join(W2, "work", 1), W2);
    assert_eq!(again[0].event.id, id(1));
    assert!(again[0].token > first.token, "a new attempt, a new token");
    assert_eq!(again[0].event.failures, 1);
}

/// A completion that arrives after the lease end but before the reclaim is accepted (2.4).
#[test]
fn a_late_completion_before_the_reclaim_is_accepted_and_after_it_is_dropped() {
    let mut h = H::volatile().cast();
    h.call(W1, Call::Join { req: 1, channel: "work".into(), types: Vec::new(), lease_ms: 1000, dispatch: false });
    h.call(W1, Call::Window { channel: "work".into(), count: 2, bytes: 0 });
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
    assert_eq!(attempt.join, Some((W1 + 10, Name::new("work"))));
    assert_eq!(h.s.joins[&(W1 + 10, Name::new("work"))].claimed, 1, "counted against the new join");
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
    assert_eq!((slot.channel.as_str(), slot.failures), ("isolated.work", 5));
    assert_eq!(slot.fixed.root.as_str(), "work", "the root stays");
    assert_eq!(slot.expires, Some(h.now + 7 * 24 * 3600 * 1000), "kept for the channel's keep time");
    // A lease expiry counts the same as a failed release.
    let mut h = H::volatile().cast();
    h.call(W1, Call::Join { req: 1, channel: "work".into(), types: Vec::new(), lease_ms: 10, dispatch: false });
    h.produce(P, work("work").with_id(id(2)));
    for _ in 0..5 {
        h.call(W1, Call::Window { channel: "work".into(), count: 1, bytes: 0 });
        h.advance(10);
    }
    assert_eq!(h.slot(id(2)).unwrap().channel.as_str(), "isolated.work");
}

#[test]
fn reply_channels_are_never_isolated() {
    let mut h = H::volatile().cast();
    h.join(W1, "reply.me", 1);
    let mut current = assigned(&h.produce(P, work("reply.me").with_id(id(1))), W1).remove(0);
    for _ in 0..8 {
        current = assigned(&h.call(W1, Call::Release { id: id(1), token: current.token, delay_ms: 0, failed: true }), W1).remove(0);
    }
    assert_eq!(h.slot(id(1)).unwrap().channel.as_str(), "reply.me");
}

/// B26: on a `serialize_group` channel one event of a group is out at a time.
#[test]
fn serialize_group_assigns_one_event_of_a_group_at_a_time() {
    let rule = ChannelRule { pattern: Pattern("orders".into()), settings: ChannelSettings { serialize_group: true, ..ChannelSettings::default() } };
    let mut config = config();
    config.channels.rules.insert(0, rule);
    let mut h = H::with(config, false).cast();
    h.join(W1, "orders", 10);
    let first = h.produce(P, work("orders").with_group([1; 16]).with_id(id(1)));
    let second = h.produce(P, work("orders").with_group([1; 16]).with_id(id(2)));
    let other = h.produce(P, work("orders").with_group([2; 16]).with_id(id(3)));
    assert_eq!((assigned(&first, W1).len(), assigned(&second, W1).len(), assigned(&other, W1).len()), (1, 0, 1), "group 1 waits; group 2 goes past it");
    let done = h.complete(W1, &assigned(&first, W1)[0], Vec::new());
    assert_eq!(assigned(&done, W1)[0].event.id, id(2), "the next of group 1 once the first is done");
    // The same channel without the setting assigns both at once.
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

#[test]
fn a_join_is_refused_without_the_claim_grant() {
    let mut grants = Grants::new();
    grants.insert("svc".into(), Grant { methods: vec![crate::engine::config::Method::Claim], channels: vec![Pattern("pub.*".into())], delegate: false });
    let mut h = H::with(EngineConfig { grants: Some(grants), ..config() }, false);
    h.open(W1, hello(Role::Work, 2));
    let refused = h.call(W1, Call::Join { req: 3, channel: "private".into(), types: Vec::new(), lease_ms: 1000, dispatch: false });
    assert_eq!(sent(&refused, W1), [Outbound::Refused { req: 3, reason: Refusal::Forbidden }]);
    let joined = h.call(W1, Call::Join { req: 4, channel: "pub.x".into(), types: Vec::new(), lease_ms: 1000, dispatch: false });
    assert_eq!(sent(&joined, W1), [Outbound::Joined { req: 4 }]);
}

/// A completion and its follow-ups are one step; the follow-ups' root is the parent's (2.1).
#[test]
fn a_completion_registers_its_follow_ups_in_the_same_step_under_the_parents_root() {
    let mut h = H::durable(false).cast();
    h.join(W1, "work", 1);
    h.call(O, Call::Subscribe { req: 1, target: Target::Channel("feed".into()), after: None });
    let assignment = assigned(&h.produce(P, work("work").with_id(id(1))), W1).remove(0);
    let before = h.records.len();
    let effects = h.complete(W1, &assignment, vec![work("next").with_id(id(2)), Envelope::dispatch("feed", "done", Bytes::new())]);
    let written: Vec<&Record> = h.records[before..].iter().collect();
    assert!(matches!(written.as_slice(), [Record::Remove(_), Record::Register { root, .. }, ..] if root.as_str() == "work"), "{written:?}");
    assert!(written.iter().any(|record| matches!(record, Record::Producer { seq: 1, .. })), "the worker's sequence is in the same batch");
    assert!(sent(&effects, O).iter().any(|message| matches!(message, Outbound::Delivery(event) if event.event_type.as_str() == "done")));
    assert_eq!(h.slot(id(2)).unwrap().fixed.root.as_str(), "work");
}

/// A follow-up whose id is active: the same content is skipped, other content
/// refuses the whole completion (2.3).
#[test]
fn a_follow_up_with_an_active_id_is_skipped_when_identical_and_refuses_the_completion_when_not() {
    let mut h = H::volatile().cast();
    h.join(W1, "work", 2);
    h.produce(P, work("other").with_id(id(9)));
    let first = assigned(&h.produce(P, work("work").with_id(id(1))), W1).remove(0);
    let conflict = h.complete(W1, &first, vec![Envelope::claimable("other", "do", Bytes::from_static(b"else")).with_id(id(9)), work("x").with_id(id(3))]);
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
