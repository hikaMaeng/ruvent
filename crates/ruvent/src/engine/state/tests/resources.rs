//! Managed memory, the watermark, root-type pause (2.6).
//!
//! A root is the type of the event that began a lineage; the channels here
//! only keep lanes apart.

use super::*;
use crate::engine::config::MaxBacklog;

/// A ledger whose budget is `budget` bytes, with metadata counted as 100 bytes per event.
fn tight(budget: u64) -> H {
    let memory = MemoryConfig { limit: Some(budget * 10 / 9 + 1), slot_bytes: 100, early_pause: 1.0, ..MemoryConfig::default() };
    H::with(EngineConfig { memory, producers: crate::engine::config::ProducerConfig { entry_bytes: 0, ..Default::default() }, ..config() }, false).cast()
}

fn paused(effects: &Effects, session: SessionId) -> Vec<String> {
    sent(effects, session)
        .into_iter()
        .filter_map(|message| match message {
            Outbound::Paused { event_type } => Some(format!("paused {event_type}")),
            Outbound::Resumed { event_type } => Some(format!("resumed {event_type}")),
            _ => None,
        })
        .collect()
}

/// An empty claimable event of `event_type` on the channel labelled `channel`.
fn small(channel: &str, event_type: u16) -> Envelope {
    Envelope::claimable(ch(channel), event_type, Bytes::new())
}

/// At the high watermark the biggest root pauses; below 7/8 of its share at
/// the pause it resumes (2.6, S5).
#[test]
fn the_biggest_root_is_paused_at_the_high_watermark_and_resumed_below_seven_eighths() {
    let mut h = tight(10_000);
    for _ in 0..50 {
        h.produce(P, small("x", 1));
    }
    for _ in 0..30 {
        h.produce(P, small("y", 2));
    }
    assert!(h.s.paused.is_empty(), "80 events × 100 bytes is under 80% of a 10,001-byte budget");
    h.produce(P, small("x", 1));
    assert!(h.s.paused.contains(&1), "the biggest root");
    assert!(!h.s.paused.contains(&2));
    let notice = h.produce(P, small("x", 1));
    assert_eq!(paused(&notice, P), ["paused 1"], "its producer is told when it registers that type");
    h.join(W1, "x", 100);
    let mut resumed = Vec::new();
    let held: Vec<(EventId, ClaimToken)> =
        h.s.joins[&h.join_id(W1, "x")].attempts.iter().filter_map(|index| h.s.slots.get(*index).map(|slot| (slot.fixed.id, slot.attempt().unwrap().token))).collect();
    for assignment in held {
        let seq = h.next_seq(W1);
        let effects = h.call(W1, Call::Complete { seq, id: assignment.0, token: assignment.1, follow_ups: Vec::new(), spooled: Vec::new() });
        resumed.extend(paused(&effects, P));
    }
    assert_eq!(resumed, ["resumed 1"]);
}

/// A paused root's producer is told once; past the allowance its reading stops (S14).
#[test]
fn a_paused_root_tells_its_producers_once_and_stops_reading_past_the_allowance() {
    let mut h = H::with(EngineConfig { pause_allowance: 10, ..config() }, false).cast();
    h.produce(P, small("x", 1));
    h.step(|state| state.pause(1, false));
    let first = h.produce(P, Envelope::claimable(ch("x"), 1, Bytes::from(vec![0; 6])));
    assert_eq!(paused(&first, P), ["paused 1"]);
    let second = h.produce(P, Envelope::claimable(ch("x"), 1, Bytes::from(vec![0; 8])));
    assert!(paused(&second, P).is_empty() && second.reading.is_empty(), "within the allowance (10 + the 6 being read at the notice)");
    let third = h.produce(P, Envelope::claimable(ch("x"), 1, Bytes::from(vec![0; 9])));
    assert_eq!(third.reading, [(P, false)]);
    let other = h.produce(P, small("y", 2));
    assert!(paused(&other, P).is_empty(), "the same connection's other types go on");
    let resumed = h.step(|state| state.resume(1));
    assert_eq!(paused(&resumed, P), ["resumed 1"]);
    assert_eq!(resumed.reading, [(P, true)]);
}

/// A paused root is not assigned from net-growth lanes while its share is
/// above its share at the pause; chain ends still are (2.6).
#[test]
fn a_paused_root_is_not_assigned_from_net_growth_lanes_and_chain_ends_still_are() {
    let mut h = H::volatile().cast();
    h.join(W1, "fan", 10);
    h.join(W2, "leaf", 10);
    // Teach the "fan" lane that it grows: each completion makes two follow-ups.
    for _ in 0..3 {
        let assignment = assigned(&h.produce(P, small("fan", 1)), W1).remove(0);
        let effects = h.complete(W1, &assignment, vec![small("leaf", 2), small("leaf", 2)]);
        for leaf in assigned(&effects, W2) {
            h.complete(W2, &leaf, Vec::new());
        }
    }
    assert!(h.s.lanes[&(ch("fan"), 1, false)].growth.net());
    assert!(!h.s.lanes[&(ch("leaf"), 2, false)].growth.net());
    let (fan, leaf) = (h.join_id(W1, "fan").1, h.join_id(W2, "leaf").1);
    h.call(W1, Call::Window { join: fan, count: 0, bytes: 0 });
    h.call(W2, Call::Window { join: leaf, count: 0, bytes: 0 });
    h.produce(P, small("fan", 1));
    h.produce(P, small("leaf", 2));
    h.step(|state| state.pause(1, false));
    h.produce(P, small("fan", 1));
    h.produce(P, small("leaf", 2));
    // Root type 1 grew past its share at the pause: its fan lane is held; root type 2 is not paused.
    let opened = h.call(W1, Call::Window { join: fan, count: 10, bytes: 0 });
    assert!(assigned(&opened, W1).is_empty(), "net growth of a paused root that grew");
    assert_eq!(h.s.holds.values().map(|held| held.len()).sum::<usize>(), 2, "moved to the hold lane, out of the way");
    let leaves = h.call(W2, Call::Window { join: leaf, count: 10, bytes: 0 });
    assert_eq!(assigned(&leaves, W2).len(), 2, "chain ends drain");
}

/// A root none of whose lineages ended is paused at 1/8 of the budget (S19).
#[test]
fn a_root_that_never_drained_is_paused_at_an_eighth_of_the_budget() {
    let memory = MemoryConfig { limit: Some(10_000 * 10 / 9 + 1), slot_bytes: 100, ..MemoryConfig::default() };
    let mut h = H::with(EngineConfig { memory, producers: crate::engine::config::ProducerConfig { entry_bytes: 0, ..Default::default() }, ..config() }, false).cast();
    for _ in 0..12 {
        h.produce(P, small("nobody", 3));
    }
    assert!(h.s.paused.is_empty(), "1,200 bytes is under 1/8 of 10,000");
    h.produce(P, small("nobody", 3));
    assert!(h.s.paused.contains(&3));
    // A root with a consumer is not.
    h.join(W1, "busy", 100);
    let first = assigned(&h.produce(P, small("busy", 4)), W1).remove(0);
    h.complete(W1, &first, Vec::new());
    for _ in 0..19 {
        h.produce(P, small("busy", 4));
    }
    assert!(!h.s.paused.contains(&4), "it drained once, even though it was empty in between");
}

/// B17: `max_backlog` pauses a root regardless of the watermark.
#[test]
fn max_backlog_pauses_a_root_regardless_of_the_watermark() {
    let mut config = config();
    config.types.rules.insert(0, rule([5], TypeSettings { max_backlog: Some(MaxBacklog { count: 3, bytes: u64::MAX }), ..TypeSettings::default() }));
    let mut h = H::with(config, false).cast();
    for _ in 0..3 {
        h.produce(P, small("bounded", 5));
    }
    assert!(h.s.paused.is_empty());
    h.produce(P, small("bounded", 5));
    assert!(h.s.paused.contains(&5));
}

/// Follow-ups and isolated events count to the root type that began the
/// lineage, so a downstream type's pile-up pauses its upstream producer (2.6).
#[test]
fn follow_ups_and_isolated_events_count_to_the_root_type() {
    let mut h = H::volatile().cast();
    h.join(W1, "a", 1);
    let assignment = assigned(&h.produce(P, small("a", 1)), W1).remove(0);
    h.complete(W1, &assignment, vec![small("b", 2), small("b", 2)]);
    assert_eq!(h.s.roots[&1].events, 2, "the two follow-ups of type 2 are type 1's share");
    assert!(!h.s.roots.contains_key(&2));
    // A paused root holds back registrations of its own type only: a
    // follow-up of type 2 is not a registration of type 2.
    h.step(|state| state.pause(1, false));
    let other = h.produce(P, small("b", 2));
    assert!(paused(&other, P).is_empty(), "type 2 is a root of its own when registered");
    assert_eq!(h.s.roots[&2].events, 1);
}

/// Past the limit (budget + reserve) no net-growth lane is assigned, whatever the root (2.6, step 5).
#[test]
fn past_the_limit_net_growth_lanes_are_not_assigned() {
    let mut h = H::volatile().cast();
    h.join(W1, "fan", 10);
    let assignment = assigned(&h.produce(P, small("fan", 1)), W1).remove(0);
    h.complete(W1, &assignment, vec![small("out", 6), small("out", 6)]);
    h.over_limit_now();
    let held = h.produce(P, small("fan", 1));
    assert!(assigned(&held, W1).is_empty());
    h.s.memory_limit = 1 << 40;
    // Shrinking the limit also paused roots at the watermark; that is not this test.
    for root in h.s.paused.clone() {
        h.step(|state| state.resume(root));
    }
    let fan = h.join_id(W1, "fan").1;
    let opened = h.call(W1, Call::Window { join: fan, count: 10, bytes: 0 });
    assert_eq!(assigned(&opened, W1).len(), 1, "assigned again once under the limit");
}

impl H {
    /// Shrinks the limit under what is in use, so the next batch end is past it.
    fn over_limit_now(&mut self) {
        self.s.memory_limit = 1;
        self.step(|_| {});
        assert!(self.s.over_limit);
    }
}
