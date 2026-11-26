//! Random operation sequences keep the ledger's bookkeeping consistent.
//!
//! The invariants are what every other rule relies on: an event is in exactly
//! one place; a lane holds exactly the waiting events of its (channel, type);
//! a join's counts are the attempts that name it; an attempt's worker is the
//! worker that took it; the managed bytes are the sum over the events.

use super::*;
use proptest::prelude::*;

#[derive(Clone, Debug)]
enum Step {
    Register(u8),
    Window(u8, u32),
    Complete(u8, bool),
    Release(u8, bool, u64),
    Extend(u8),
    Delete(u8),
    Claim(u8),
    Close(u8),
    Advance(u64),
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        (0..3u8).prop_map(Step::Register),
        (0..2u8, 0..4u32).prop_map(|(worker, count)| Step::Window(worker, count)),
        (0..16u8, any::<bool>()).prop_map(|(at, follow)| Step::Complete(at, follow)),
        (0..16u8, any::<bool>(), 0..30u64).prop_map(|(at, failed, delay)| Step::Release(at, failed, delay)),
        (0..16u8).prop_map(Step::Extend),
        (0..16u8).prop_map(Step::Delete),
        (0..16u8).prop_map(Step::Claim),
        (0..2u8).prop_map(Step::Close),
        (1..40u64).prop_map(Step::Advance),
    ]
}

const CHANNELS: [&str; 3] = ["a", "b", "c"];
/// The type registered on each of [`CHANNELS`]; type 3 is taken by a type join only.
const TYPES: [u16; 3] = [1, 2, 3];
/// The `req` of every worker's type join (type 3, every channel).
const TYPED: u32 = 10;

/// Joins channels `a` and `b` (req 1, 2) and type 3 on every channel.
fn join_all(h: &mut H, session: SessionId) {
    for (at, channel) in CHANNELS[..2].iter().enumerate() {
        h.call(session, Call::Join { req: at as u32 + 1, channel: Some(ch(channel)), types: Vec::new(), lease_ms: 50, dispatch: false });
    }
    h.call(session, Call::Join { req: TYPED, channel: None, types: vec![3], lease_ms: 50, dispatch: false });
}

fn check(state: &State) -> Result<(), TestCaseError> {
    let mut in_lanes = 0usize;
    for (key, lane) in &state.lanes {
        for (work_key, index) in &lane.queue {
            let slot = state.slots.get(*index).expect("a lane names a live slot");
            prop_assert!(matches!(slot.place, Place::Waiting), "a lane holds waiting events only");
            prop_assert_eq!(&slot.lane(), key);
            prop_assert_eq!(slot.fixed.key, *work_key);
            in_lanes += 1;
        }
        prop_assert!(!lane.is_idle(), "an idle lane is dropped");
    }
    let mut waiting = 0usize;
    let mut bytes = 0u64;
    let mut joins: HashMap<JoinId, (u32, u64)> = HashMap::new();
    for (index, slot) in state.slots.iter() {
        bytes += state.usage(slot).memory;
        match &slot.place {
            Place::Waiting => waiting += 1,
            Place::Delayed { .. } => {}
            Place::Claimed(attempt) => {
                prop_assert!(attempt.worker == [2; 16] || attempt.worker == [3; 16], "an attempt names the worker that took it");
                if let Some(join) = &attempt.join {
                    let entry = joins.entry(*join).or_default();
                    entry.0 += 1;
                    entry.1 += slot.len();
                    prop_assert!(state.joins.get(join).is_some_and(|found| found.attempts.contains(&index)));
                    prop_assert_eq!(state.worker_of(join.0), Some(attempt.worker), "an attempt counts against a join of its own worker");
                }
                prop_assert_eq!(slot.envelope().claim.map(|claim| claim.worker), Some(attempt.worker), "the claim mark is the attempt's");
            }
        }
        prop_assert_eq!(state.slots.by_id(&slot.fixed.id), Some(index));
    }
    prop_assert_eq!(waiting, in_lanes, "every waiting event is in its lane");
    prop_assert_eq!(bytes, state.slot_bytes, "managed bytes are the sum over the events");
    for (id, join) in &state.joins {
        prop_assert_eq!((join.claimed, join.claimed_bytes), joins.get(id).copied().unwrap_or_default(), "join {:?}", id);
        prop_assert_eq!(join.claimed as usize, join.attempts.len());
    }
    Ok(())
}

proptest! {
    #[test]
    fn random_operations_keep_the_ledgers_bookkeeping_consistent(steps in proptest::collection::vec(step(), 1..120), strict in any::<bool>()) {
        let config = if strict { super::strict() } else { super::config() };
        let mut h = H::with(config, true).cast();
        let workers = [W1, W2];
        for worker in workers {
            join_all(&mut h, worker);
        }
        let mut ids: Vec<EventId> = Vec::new();
        for step in steps {
            let attempt = |h: &H, at: u8| ids.get(at as usize).and_then(|id| h.slot(*id).and_then(|slot| slot.attempt().map(|attempt| (*id, attempt.token, attempt.join))));
            match step {
                Step::Register(channel) => {
                    let event = Envelope::claimable(ch(CHANNELS[channel as usize]), TYPES[channel as usize], Bytes::from_static(b"p")).with_id(EventId::new_v7());
                    ids.push(event.id);
                    h.produce(P, event);
                }
                Step::Window(worker, count) => {
                    for join in [1, 2, TYPED] {
                        h.call(workers[worker as usize], Call::Window { join, count, bytes: 0 });
                    }
                }
                Step::Complete(at, follow) => {
                    if let Some((id, token, join)) = attempt(&h, at) {
                        let session = join.map_or(W1, |join| join.0);
                        let follow_ups = if follow { vec![work("a"), Envelope::dispatch(ch("feed"), 10, Bytes::new())] } else { Vec::new() };
                        let seq = h.next_seq(session);
                        h.call(session, Call::Complete { seq, id, token, follow_ups, spooled: Vec::new() });
                    }
                }
                Step::Release(at, failed, delay) => {
                    if let Some((id, token, join)) = attempt(&h, at) {
                        h.call(join.map_or(W1, |join| join.0), Call::Release { id, token, delay_ms: delay, failed });
                    }
                }
                Step::Extend(at) => {
                    if let Some((id, token, _)) = attempt(&h, at) {
                        h.call(W2, Call::Extend { id, token });
                    }
                }
                Step::Delete(at) => {
                    if let Some(id) = ids.get(at as usize).copied() {
                        let seq = h.next_seq(P);
                        h.call(P, Call::Delete { seq, id, result: ch("out"), key: None });
                    }
                }
                Step::Claim(at) => {
                    if let Some(id) = ids.get(at as usize).copied() {
                        h.call(W1, Call::Claim { req: 2, id, lease_ms: 30 });
                    }
                }
                Step::Close(worker) => {
                    let session = workers[worker as usize];
                    let unwritten: Vec<ClaimToken> = h.s.slots.iter().filter_map(|(_, slot)| slot.attempt().filter(|attempt| attempt.join.as_ref().is_some_and(|join| join.0 == session)).map(|attempt| attempt.token)).step_by(2).collect();
                    h.close(session, &unwritten);
                    h.open(session, hello(Role::Work, session as u8));
                    join_all(&mut h, session);
                }
                Step::Advance(by) => {
                    h.now += by;
                    h.land();
                }
            }
            check(&h.s)?;
        }
    }
}
