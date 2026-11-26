//! Subscriptions, backlogs, dispatch, deletion, statistics, drain (2.8).

use super::*;
use crate::engine::config::Method;
use crate::engine::ops::{delete_outcome, DeleteFailure};

fn deliveries(effects: &Effects, session: SessionId) -> usize {
    sent(effects, session).iter().filter(|message| matches!(message, Outbound::Delivery(_))).count()
}

#[test]
fn dispatch_reaches_exactly_the_subscribers_at_the_moment_it_is_registered() {
    let mut h = H::volatile().cast();
    let dispatch = |h: &mut H| h.produce(P, Envelope::dispatch(ch("feed"), 10, Bytes::new()));
    assert_eq!(deliveries(&dispatch(&mut h), O), 0, "nobody subscribed: sent to nobody, kept nowhere");
    h.call(O, Call::Subscribe { req: 1, channel: ch("feed"), after: None });
    assert_eq!(deliveries(&dispatch(&mut h), O), 1);
    h.call(O, Call::Unsubscribe { req: 2, channel: ch("feed") });
    assert_eq!(deliveries(&dispatch(&mut h), O), 0);
    assert_eq!(h.s.live(), 0);
}

/// A channel is a UUIDv7 and nothing else: a subscription, a registration
/// or a deletion naming anything else is refused (plan 9.15). There are no
/// prefixes: two channels are related only by being equal.
#[test]
fn a_channel_that_is_not_a_uuidv7_is_refused_everywhere() {
    let mut h = H::volatile().cast();
    let bad = Channel([7; 16]);
    let mut v4 = Channel([0x11; 16]);
    v4.0[6] = 0x41;
    v4.0[8] = 0x81;
    for (req, channel) in [(1, bad), (2, v4), (3, Channel::default())] {
        assert_eq!(sent(&h.call(O, Call::Subscribe { req, channel, after: None }), O), [Outbound::Refused { req, reason: Refusal::Malformed }]);
    }
    let registered = h.produce(P, Envelope::claimable(bad, 1, Bytes::new()));
    assert_eq!(faults(&registered, P), [FaultKind::Malformed]);
    assert_eq!(h.s.live(), 0);
    let mut h = H::volatile().cast();
    let id = h.register("work");
    let seq = h.next_seq(P);
    let deleted = h.call(P, Call::Delete { seq, id, result: v4, key: None });
    assert_eq!(faults(&deleted, P), [FaultKind::Malformed]);
    assert!(h.slot(id).is_some(), "nothing was deleted");
}

/// Subscriptions, their backlog and dispatch joins hand a connection only the
/// events of the types its credential is granted; the server's own type-0
/// delete outcomes always pass (plan 9.15).
#[test]
fn readers_get_only_the_types_their_credential_is_granted() {
    let mut grants = Grants::new();
    grants.insert("svc".into(), Grant::all());
    grants.insert("narrow".into(), Grant { methods: vec![Method::Subscribe, Method::Claim], types: TypeSet::of([10]), delegate: false });
    let mut h = H::with(EngineConfig { grants: Some(grants), ..config() }, false).cast();
    h.open(20, Hello { credential: "narrow".into(), ..hello(Role::Observe, 20) });
    h.open(21, Hello { credential: "narrow".into(), ..hello(Role::Work, 21) });
    h.produce(P, Envelope::claimable(ch("room"), 10, Bytes::new()).with_id(id(1)));
    h.produce(P, Envelope::claimable(ch("room"), 11, Bytes::new()).with_id(id(2)));
    let backlog = h.call(20, Call::Subscribe { req: 1, channel: ch("room"), after: None });
    let Ok::<[Outbound; 1], _>([Outbound::Subscribed { backlog, .. }]) = sent(&backlog, 20).try_into() else { panic!() };
    assert_eq!(backlog.iter().map(|notice| notice.id).collect::<Vec<_>>(), [id(1)], "the backlog skips type 11");
    h.call(O, Call::Subscribe { req: 1, channel: ch("room"), after: None });
    let joined = h.call(21, Call::Join { req: 1, channel: Some(ch("room")), types: vec![10], isolated: false, lease_ms: 1000, dispatch: true });
    assert_eq!(sent(&joined, 21), [Outbound::Joined { req: 1 }]);
    for (event_type, narrow) in [(10, 1), (11, 0)] {
        let effects = h.produce(P, Envelope::dispatch(ch("room"), event_type, Bytes::new()));
        assert_eq!((deliveries(&effects, 20), deliveries(&effects, 21), deliveries(&effects, O)), (narrow, narrow, 1), "type {event_type}");
    }
    let notice = h.produce(P, Envelope::claimable(ch("room"), 11, Bytes::new()));
    assert!(sent(&notice, 20).is_empty(), "no notice of a type it may not read");
    let seq = h.next_seq(P);
    let outcome = h.call(P, Call::Delete { seq, id: id(2), result: ch("room"), key: None });
    assert_eq!((deliveries(&outcome, 20), deliveries(&outcome, 21)), (1, 1), "a delete outcome is the server's own type");
}

#[test]
fn a_dispatch_join_receives_the_channels_dispatch_events() {
    let mut h = H::volatile().cast();
    h.call(W1, Call::Join { req: 1, channel: Some(ch("answer.me")), types: Vec::new(), isolated: false, lease_ms: 5_000, dispatch: true });
    let chunk = h.call(W2, Call::Register { seq: None, event: Envelope::dispatch(ch("answer.me"), 18, Bytes::new()) });
    assert_eq!(deliveries(&chunk, W1), 1, "a stream chunk emitted on a work connection reaches the requester's work connection");
}

#[test]
fn the_backlog_comes_in_pages_in_work_key_order() {
    let mut h = H::with(EngineConfig { backlog_page: 3, ..config() }, false).cast();
    let ids: Vec<EventId> = (0..7).map(|_| h.register("work")).collect();
    let mut seen = Vec::new();
    let mut after = None;
    loop {
        let effects = h.call(O, Call::Subscribe { req: 1, channel: ch("work"), after });
        let Ok::<[Outbound; 1], _>([Outbound::Subscribed { backlog, more, .. }]) = sent(&effects, O).try_into() else { panic!() };
        assert!(backlog.len() <= 3);
        assert!(backlog.iter().all(|notice| notice.payload.is_empty()), "notices are head and key only");
        seen.extend(backlog.iter().map(|notice| notice.id));
        after = backlog.last().and_then(|notice| notice.key);
        if !more {
            break;
        }
    }
    assert_eq!(seen, ids);
    assert_eq!(h.s.subscribers.exact[&ch("work")], vec![O], "subscribing again does not add a second subscription");
}

#[test]
fn deleting_a_waiting_event_succeeds_once_and_a_claimed_or_missing_one_fails_with_one_notice() {
    let mut h = H::volatile().cast();
    h.call(O, Call::Subscribe { req: 1, channel: ch("out"), after: None });
    let waiting = h.register("work");
    let delete = |h: &mut H, target| {
        let seq = h.next_seq(P);
        h.call(P, Call::Delete { seq, id: target, result: ch("out"), key: None })
    };
    let outcome = |effects: &Effects| -> Vec<(EventId, Result<(), DeleteFailure>)> {
        sent(effects, O)
            .into_iter()
            .filter_map(|message| match message {
                Outbound::Delivery(event) => {
                    assert_eq!((event.event_type, event.channel), (crate::envelope::SERVER_TYPE, ch("out")));
                    delete_outcome(&event.payload)
                }
                _ => None,
            })
            .collect()
    };
    assert_eq!(outcome(&delete(&mut h, waiting)), [(waiting, Ok(()))]);
    assert_eq!(outcome(&delete(&mut h, waiting)), [(waiting, Err(DeleteFailure::Missing))]);
    h.join(W1, "work", 1);
    let claimed = h.register("work");
    assert_eq!(outcome(&delete(&mut h, claimed)), [(claimed, Err(DeleteFailure::Claimed))]);
    assert!(h.slot(claimed).unwrap().attempt().is_some(), "the claim stands");
}

#[test]
fn stats_report_totals_top_types_idle_lanes_and_root_types() {
    let mut h = H::with(EngineConfig { stats_top: 2, ..config() }, false).cast();
    for (channel, event_type, count) in [("a", 3, 3), ("b", 2, 2), ("c", 1, 1)] {
        for _ in 0..count {
            h.produce(P, Envelope::claimable(ch(channel), event_type, Bytes::from_static(b"p")));
        }
    }
    h.join(W1, "c", 1);
    h.now = 50;
    let effects = h.call(O, Call::Stats { req: 4 });
    let Ok::<[Outbound; 1], _>([Outbound::StatsReply { req: 4, stats }]) = sent(&effects, O).try_into() else { panic!() };
    assert_eq!((stats.waiting, stats.claimed, stats.oldest_ms), (5, 1, 50));
    assert_eq!(stats.types.iter().map(|stat| (stat.event_type, stat.waiting)).collect::<Vec<_>>(), [(3, 3), (2, 2)], "top K by waiting");
    assert_eq!(stats.idle_lanes.iter().map(|lane| (lane.channel, lane.event_type)).collect::<Vec<_>>(), [(ch("a"), 3), (ch("b"), 2)], "lanes nobody joined");
    assert_eq!(stats.roots.len(), 2);
    assert_eq!(stats.roots[0].root, 3, "the biggest root type first");
    assert!(stats.depth_capacity > 0 && stats.memory_used > 0);
}

#[test]
fn stats_and_drain_need_the_operator_grant() {
    let mut grants = Grants::new();
    grants.insert("svc".into(), Grant { methods: vec![Method::Subscribe], types: TypeSet::all(), delegate: false });
    let mut h = H::with(EngineConfig { grants: Some(grants), ..config() }, false);
    h.open(O, hello(Role::Observe, 4));
    for call in [Call::Stats { req: 1 }, Call::Drain { req: 1 }] {
        assert_eq!(sent(&h.call(O, call), O), [Outbound::Refused { req: 1, reason: Refusal::Forbidden }]);
    }
    assert!(!h.s.draining());
}

/// Drain: final `Committed` after what was sent is on disk, then
/// `Bye{draining}`; new production connections are sent away; work goes on (2.8).
#[test]
fn drain_sends_new_producers_away_and_closes_open_ones_after_a_final_committed() {
    let mut h = H::durable(false).cast();
    h.join(W1, "work", 1);
    h.produce(P, work("work").with_id(id(1)));
    h.produce(P, work("work").with_id(id(2)));
    let drained = h.call(O, Call::Drain { req: 7 });
    assert_eq!(sent(&drained, O), [Outbound::Draining { req: 7 }]);
    let to_producer: Vec<&Outgoing> = drained.sends.iter().filter(|send| send.session == P).collect();
    assert!(matches!(
        to_producer.as_slice(),
        [Outgoing { message: Some(Outbound::Committed { seq: 2 }), gate: Some(_), .. }, Outgoing { message: Some(Outbound::Bye(ByeReason::Draining)), .. }, Outgoing { message: None, .. }]
    ));
    let late = h.produce(P, work("work").with_id(id(3)));
    assert!(late.sends.is_empty() && h.slot(id(3)).is_none(), "nothing after the drain is applied");
    let newcomer = h.open(30, hello(Role::Produce, 30));
    assert_eq!(sent(&newcomer, 30), [Outbound::Bye(ByeReason::Draining)]);
    let assignment = h.slot(id(1)).unwrap().attempt().unwrap().token;
    let seq = h.next_seq(W1);
    let completed = h.call(W1, Call::Complete { seq, id: id(1), token: assignment, follow_ups: Vec::new(), spooled: Vec::new() });
    assert_eq!(assigned(&completed, W1).len(), 1, "work connections carry on");
}
