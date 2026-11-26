//! A restart rebuilds the ledger from records (2.4, 2.5).

use super::*;
use crate::journal::{End, Part, Recovered, Replay};

/// Replays `records` into a fresh ledger at uptime `uptime`, with the cast connected.
fn restarted(records: Vec<Record>, uptime: u64) -> H {
    let mut s = State::new(&config(), Box::new(V7));
    s.recover(records, uptime, 0, true).unwrap();
    let mut h = H { s, now: uptime, durable: 1, carrying: 2, records: Vec::new(), log: Vec::new(), written: 0, firsts: HashMap::new(), seqs: HashMap::new(), joined: HashMap::new() };
    h.open(W1, hello(Role::Work, 2));
    h
}

/// A restart keeps claims as they were: token, lease length and the claim
/// mark's worker; the lease re-armed at its length; no failure counted (2.4, M8-6).
#[test]
fn a_restart_restores_claims_with_token_lease_and_worker_and_counts_no_failure() {
    let mut h = H::durable(false).cast();
    h.join(W1, "work", 1);
    let assignment = assigned(&h.produce(P, work("work").with_id(id(1))), W1).remove(0);
    h.produce(P, work("work").with_id(id(2)));
    h.now = 10_000;
    let mut after = restarted(h.records.clone(), 10_000);
    assert_eq!(after.s.generation(), h.s.generation() + 1, "the generation rises at a start and only then");
    let slot = after.slot(id(1)).expect("the same id");
    let attempt = slot.attempt().expect("still claimed");
    assert_eq!((attempt.token, attempt.lease_ms, attempt.until, attempt.worker, slot.failures), (assignment.token(), 30_000, 40_000, [2; 16], 0));
    assert!(attempt.join.is_none(), "no join until the worker extends it");
    assert!(h.records.iter().any(|record| matches!(record, Record::Claim { worker, .. } if *worker == [2; 16])), "the claim record carries the worker's uuid");
    // Another worker under the same credential may not take it over.
    after.open(W2, hello(Role::Work, 3));
    let seq = after.next_seq(W2);
    let stolen = after.call(W2, Call::Complete { seq, id: id(1), token: assignment.token(), follow_ups: Vec::new(), spooled: Vec::new() });
    assert_eq!(faults(&stolen, W2), [FaultKind::Forbidden]);
    assert!(after.slot(id(1)).is_some());
    // The worker reconnects, extends what it holds, then opens its window (2.9).
    after.call(W1, Call::Join { req: 1, channel: Some(ch("work")), types: Vec::new(), lease_ms: 30_000, dispatch: false });
    after.call(W1, Call::Extend { id: id(1), token: assignment.token() });
    let opened = after.call(W1, Call::Window { join: 1, count: 1, bytes: 0 });
    assert!(assigned(&opened, W1).is_empty(), "the extended attempt fills its window of one");
    let completed = after.complete(W1, &assignment, Vec::new());
    assert!(after.slot(id(1)).is_none(), "the original worker's completion is accepted");
    assert_eq!(assigned(&completed, W1)[0].event.id, id(2), "and its slot takes the next");
    assert!(assigned(&completed, W1)[0].token().generation > assignment.token().generation);
}

/// `not_before` is an absolute uptime and keeps its remaining time across a
/// restart; the clock does not count the time the server was down (M8-11).
#[test]
fn a_restart_keeps_the_remaining_time_of_not_before() {
    let mut h = H::durable(false).cast();
    h.now = 1_000;
    h.produce(P, work("work").with_id(id(1)).with_not_before(Duration::from_millis(500)));
    h.produce(P, work("work").with_id(id(2)));
    // The server stops at 1,200 and starts again at 1,200 on its own clock.
    let mut after = restarted(h.records.clone(), 1_200);
    assert_eq!(after.s.next_due(), Some(1_500), "300 ms of delay left, as before the stop");
    let joined = after.join(W1, "work", 5);
    assert_eq!(assigned(&joined, W1).iter().map(|assignment| assignment.event.id).collect::<Vec<_>>(), [id(2)], "the delayed one is not visible yet");
    assert!(assigned(&after.advance(299), W1).is_empty());
    let visible = after.advance(1);
    assert_eq!(assigned(&visible, W1)[0].event.id, id(1));
}

#[test]
fn the_producer_table_survives_a_restart_and_resume_is_the_last_journaled_sequence() {
    let mut h = H::durable(false).cast();
    for byte in 1..=3 {
        h.produce(P, work("work").with_id(id(byte)));
    }
    let mut after = restarted(h.records.clone(), 0);
    let welcome = after.open(P, hello(Role::Produce, 1));
    assert!(matches!(sent(&welcome, P).as_slice(), [Outbound::Welcome(welcome)] if welcome.resume == 3));
    let dropped = after.produce(P, work("work").with_id(id(9)));
    assert!(after.slot(id(9)).is_none() && dropped.sends.is_empty(), "sequence 1 again is a retransmission");
    let next = after.call(P, Call::Register { seq: Some(4), event: work("work").with_id(id(4)) });
    assert!(after.slot(id(4)).is_some() && faults(&next, P).is_empty());
}

#[test]
fn a_full_checkpoint_replays_to_the_same_ledger_as_the_records_it_replaced() {
    let mut h = H::durable(false).cast();
    h.call(W1, Call::Join { req: 1, channel: Some(ch("work")), types: Vec::new(), lease_ms: 15, dispatch: false });
    for step in 0..40u8 {
        h.now = u64::from(step) * 10;
        h.produce(P, work("work").with_id(id(step + 1)).with_not_before(Duration::from_millis(u64::from(step % 4) * 5)));
        if step % 3 == 0 {
            h.call(W1, Call::Window { join: 1, count: 2, bytes: 0 });
        }
        if step % 5 == 0 {
            let seq = h.next_seq(P);
            h.call(P, Call::Delete { seq, id: id(step / 2 + 1), result: ch("out"), key: None });
        }
        if step % 7 == 0 {
            if let Some(token) = h.slot(id(step + 1)).and_then(|slot| slot.attempt()).map(|attempt| attempt.token) {
                h.call(W1, Call::Release { id: id(step + 1), token, delay_ms: 1_000, failed: true });
            }
        }
    }
    /// (work key, channel, failures, claim mark's worker, not_before) of every slot.
    type View = Vec<(WorkKey, Channel, u16, Option<[u8; 16]>, Option<u64>)>;
    let view = |state: &State| {
        let mut slots: View = state.slots.iter().map(|(_, slot)| (slot.fixed.key, slot.channel, slot.failures, slot.attempt().map(|attempt| attempt.worker), slot.fixed.not_before)).collect();
        slots.sort();
        slots
    };
    let replayed = restarted(h.records.clone(), h.now);
    let serial = h.s.serial();
    let (head, parts) = h.s.full_checkpoint(serial, h.durable);
    let mut items = vec![Replay::Head(head)];
    for part in parts {
        match part {
            Part::Producers(producers) => items.extend(producers.into_iter().map(Replay::Producer)),
            Part::Entries(entries) => items.extend(entries.into_iter().map(Replay::Entry)),
        }
    }
    let mut folded = State::new(&config(), Box::new(V7));
    folded.recover_from(Recovered::from_replay(items, End { uptime: h.now, serial, commit: 0 }), true).unwrap();
    assert_eq!(view(&replayed.s), view(&folded));
    assert_eq!(view(&replayed.s), view(&h.s), "and both are the ledger as it was");
}

#[test]
fn work_keys_and_tokens_are_u64_past_the_u32_range() {
    let mut h = H::volatile().cast();
    h.s.arrival = u64::from(u32::MAX);
    h.s.claim_seq = u64::from(u32::MAX);
    let generation = h.s.generation();
    h.join(W1, "work", 2);
    let first = assigned(&h.produce(P, work("work").with_id(id(1))), W1).remove(0);
    let second = assigned(&h.produce(P, work("work").with_id(id(2))), W1).remove(0);
    assert!(second.event.key.unwrap().arrival > u64::from(u32::MAX));
    assert!(second.token() > first.token());
    assert_eq!(h.s.generation(), generation, "running out of u32 does not start a generation");
}
