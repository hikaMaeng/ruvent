//! Visibility times, expiries and the wheel (2.4, B9), and Strict visibility (2.7).

use super::*;

#[test]
fn not_before_hides_an_event_until_its_time_and_the_wheel_wakes_for_it() {
    let mut h = H::volatile().cast();
    h.join(W1, "work", 5);
    h.now = 1_000;
    let hidden = h.produce(P, work("work").with_id(id(1)).with_not_before(Duration::from_millis(250)));
    assert!(assigned(&hidden, W1).is_empty());
    assert_eq!(h.slot(id(1)).unwrap().fixed.not_before, Some(1_250), "held as an absolute uptime");
    assert_eq!(h.s.next_due(), Some(1_250));
    assert!(assigned(&h.advance(249), W1).is_empty());
    assert_eq!(assigned(&h.advance(1), W1)[0].event.id, id(1));
}

/// Expired events are removed without any consumer, by batches that run
/// anyway; the wheel does not wake for them (2.4, B9).
#[test]
fn an_expired_event_is_never_assigned_and_is_purged_without_any_consumer() {
    let mut h = H::volatile().cast();
    h.produce(P, work("nobody").with_id(id(1)).with_expires(Duration::from_millis(100)));
    assert_eq!(h.s.next_due(), None, "no timer for an expiry");
    h.now = 100;
    h.produce(P, Envelope::dispatch("elsewhere", "t", Bytes::new()));
    assert!(h.slot(id(1)).is_none(), "collected by an unrelated batch");
    assert_eq!(h.s.counters.expired, 1);
    // And one that expired before a join is not assigned to it.
    h.produce(P, work("work").with_id(id(2)).with_expires(Duration::from_millis(10)));
    h.now = 200;
    let joined = h.join(W1, "work", 1);
    assert!(assigned(&joined, W1).is_empty());
    assert!(h.slot(id(2)).is_none());
}

#[test]
fn an_expired_event_is_isolated_instead_where_the_channel_says_so() {
    let mut config = config();
    config.channels.rules.insert(0, ChannelRule { pattern: Pattern("keep".into()), settings: ChannelSettings { isolate_expired: true, ..ChannelSettings::default() } });
    let mut h = H::with(config, false).cast();
    h.produce(P, work("keep").with_id(id(1)).with_expires(Duration::from_millis(10)));
    h.advance(10);
    assert_eq!(h.slot(id(1)).unwrap().channel.as_str(), "isolated.keep");
}

/// Under Strict nothing uncommitted is assigned, noticed, backlogged or
/// claimed by id; all of it becomes visible when the batch lands (2.7).
#[test]
fn under_strict_nothing_uncommitted_is_assigned_noticed_backlogged_or_claimed() {
    let mut h = H::durable(true).cast();
    h.join(W1, "work", 5);
    h.call(O, Call::Subscribe { req: 1, target: Target::Channel("work".into()), after: None });
    let registered = h.produce(P, work("work").with_id(id(1)));
    assert!(assigned(&registered, W1).is_empty(), "not assigned before its batch is on disk");
    let notice = registered.sends.iter().find(|send| send.session == O).expect("a notice, gated");
    assert_eq!(notice.gate, Some(h.carrying));
    let backlog = h.call(O, Call::Subscribe { req: 2, target: Target::Channel("work".into()), after: None });
    assert!(matches!(sent(&backlog, O).as_slice(), [Outbound::Subscribed { backlog, .. }] if backlog.is_empty()), "not in the backlog");
    h.open(W2 + 10, hello(Role::Work, 30));
    let claimed = h.call(W2 + 10, Call::Claim { req: 3, id: id(1), lease_ms: 1000 });
    assert!(claimed.sends.iter().any(|send| send.session == W2 + 10 && send.gate == Some(h.carrying)), "an explicit claim's answer waits for the commit");
    // Land: the remaining event (registered and not claimed) is assigned.
    h.produce(P, work("work").with_id(id(2)));
    let landed = h.land();
    assert_eq!(assigned(&landed, W1).iter().map(|assignment| assignment.event.id).collect::<Vec<_>>(), [id(2)]);
}

#[test]
fn under_scheduled_visibility_does_not_wait_for_the_disk() {
    let mut h = H::durable(false).cast();
    h.join(W1, "work", 5);
    h.call(O, Call::Subscribe { req: 1, target: Target::Channel("work".into()), after: None });
    let registered = h.produce(P, work("work").with_id(id(1)));
    assert_eq!(assigned(&registered, W1).len(), 1);
    assert!(registered.sends.iter().all(|send| send.gate.is_none()));
    assert!(committed(&registered, P).is_empty(), "the acknowledgement still waits for the disk (B2)");
}
