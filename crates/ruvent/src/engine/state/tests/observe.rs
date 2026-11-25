//! Subscriptions, backlogs, dispatch, deletion, statistics, drain (2.8).

use super::*;
use crate::engine::ops::DELETED;

fn deliveries(effects: &Effects, session: SessionId) -> usize {
    sent(effects, session).iter().filter(|message| matches!(message, Outbound::Delivery(_))).count()
}

#[test]
fn dispatch_reaches_exactly_the_subscribers_at_the_moment_it_is_registered() {
    let mut h = H::volatile().cast();
    let dispatch = |h: &mut H| h.produce(P, Envelope::dispatch("feed", "t", Bytes::new()));
    assert_eq!(deliveries(&dispatch(&mut h), O), 0, "nobody subscribed: sent to nobody, kept nowhere");
    h.call(O, Call::Subscribe { req: 1, target: Target::Channel("feed".into()), after: None });
    assert_eq!(deliveries(&dispatch(&mut h), O), 1);
    h.call(O, Call::Unsubscribe { req: 2, target: Target::Channel("feed".into()) });
    assert_eq!(deliveries(&dispatch(&mut h), O), 0);
    assert_eq!(h.s.live(), 0);
}

/// A prefix subscription matches at `.` boundaries only, unlike a grant
/// pattern's `starts_with` (4장).
#[test]
fn a_prefix_subscription_matches_at_dot_boundaries_unlike_a_grant_pattern() {
    let mut h = H::volatile().cast();
    h.call(O, Call::Subscribe { req: 1, target: Target::Prefix("user".into()), after: None });
    let count = |h: &mut H, channel: &str| deliveries(&h.produce(P, Envelope::dispatch(channel, "t", Bytes::new())), O);
    assert_eq!(count(&mut h, "user.a"), 1);
    assert_eq!(count(&mut h, "user.a.b"), 1);
    assert_eq!(count(&mut h, "users.a"), 0, "no boundary");
    assert_eq!(count(&mut h, "user"), 0, "the prefix itself is not under it");
    assert!(Pattern("user*".into()).matches("users.a"), "where a grant pattern would match");
    // Subscribed twice (exact and prefix): delivered once.
    h.call(O, Call::Subscribe { req: 2, target: Target::Channel("user.a".into()), after: None });
    assert_eq!(count(&mut h, "user.a"), 1);
}

#[test]
fn a_dispatch_join_receives_the_channels_dispatch_events() {
    let mut h = H::volatile().cast();
    h.call(W1, Call::Join { req: 1, channel: "reply.me".into(), types: Vec::new(), lease_ms: 5_000, dispatch: true });
    let chunk = h.call(W2, Call::Register { seq: None, event: Envelope::dispatch("reply.me", "chunk", Bytes::new()) });
    assert_eq!(deliveries(&chunk, W1), 1, "a stream chunk emitted on a work connection reaches the requester's work connection");
}

#[test]
fn the_backlog_comes_in_pages_in_work_key_order() {
    let mut h = H::with(EngineConfig { backlog_page: 3, ..config() }, false).cast();
    let ids: Vec<EventId> = (0..7).map(|_| h.register("work")).collect();
    let mut seen = Vec::new();
    let mut after = None;
    loop {
        let effects = h.call(O, Call::Subscribe { req: 1, target: Target::Channel("work".into()), after });
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
    assert_eq!(h.s.subscribers.exact["work"], vec![O], "subscribing again does not add a second subscription");
}

#[test]
fn deleting_a_waiting_event_succeeds_once_and_a_claimed_or_missing_one_fails_with_one_notice() {
    let mut h = H::volatile().cast();
    h.call(O, Call::Subscribe { req: 1, target: Target::Channel("out".into()), after: None });
    let waiting = h.register("work");
    let delete = |h: &mut H, target| {
        let seq = h.next_seq(P);
        h.call(P, Call::Delete { seq, id: target, result: "out".into(), tag: id(1), key: None })
    };
    let outcome = |effects: &Effects| -> Vec<String> {
        sent(effects, O)
            .into_iter()
            .filter_map(|message| match message {
                Outbound::Delivery(event) => Some(event.event_type.to_string()),
                _ => None,
            })
            .collect()
    };
    assert_eq!(outcome(&delete(&mut h, waiting)), [DELETED]);
    assert_eq!(outcome(&delete(&mut h, waiting)), ["delete-failed"]);
    h.join(W1, "work", 1);
    let claimed = h.register("work");
    assert_eq!(outcome(&delete(&mut h, claimed)), ["delete-failed"]);
    assert!(h.slot(claimed).unwrap().attempt().is_some(), "the claim stands");
}

#[test]
fn stats_report_totals_top_channels_idle_lanes_and_roots() {
    let mut h = H::with(EngineConfig { stats_top: 2, ..config() }, false).cast();
    for (channel, count) in [("a", 3), ("b", 2), ("c", 1)] {
        for _ in 0..count {
            h.register(channel);
        }
    }
    h.join(W1, "c", 1);
    h.now = 50;
    let effects = h.call(O, Call::Stats { req: 4 });
    let Ok::<[Outbound; 1], _>([Outbound::StatsReply { req: 4, stats }]) = sent(&effects, O).try_into() else { panic!() };
    assert_eq!((stats.waiting, stats.claimed, stats.oldest_ms), (5, 1, 50));
    assert_eq!(stats.channels.iter().map(|stat| (stat.channel.as_str(), stat.waiting)).collect::<Vec<_>>(), [("a", 3), ("b", 2)], "top K by waiting");
    assert_eq!(stats.idle_lanes.iter().map(|lane| lane.channel.as_str()).collect::<Vec<_>>(), ["a", "b"], "lanes nobody joined");
    assert_eq!(stats.roots.len(), 2);
    assert_eq!(stats.roots[0].root.as_str(), "a");
    assert!(stats.depth_capacity > 0 && stats.memory_used > 0);
}

#[test]
fn stats_and_drain_need_the_operator_grant() {
    let mut grants = Grants::new();
    grants.insert("svc".into(), Grant { methods: vec![crate::engine::config::Method::Subscribe], channels: vec![Pattern("*".into())], delegate: false });
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
