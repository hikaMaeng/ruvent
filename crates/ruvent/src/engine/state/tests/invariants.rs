//! Random operation sequences keep the ledger's bookkeeping consistent.
//!
//! The invariants are what every other rule relies on: an event is in exactly
//! one place; a lane holds exactly the waiting events of its (channel, type);
//! a join's counts are the attempts that name it; the managed bytes and the
//! root shares are the sum over the events; a group's count is its attempts.

use super::*;
use proptest::prelude::*;

#[derive(Clone, Debug)]
enum Step {
    Register(u8, bool),
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
        (0..3u8, any::<bool>()).prop_map(|(channel, grouped)| Step::Register(channel, grouped)),
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

const CHANNELS: [&str; 3] = ["a", "b", "reply.c"];

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
    let mut in_holds = 0usize;
    for held in state.holds.values() {
        for index in held.values() {
            prop_assert!(matches!(state.slots.get(*index).expect("held slot").place, Place::Held));
            in_holds += 1;
        }
    }
    let mut waiting = 0usize;
    let mut held = 0usize;
    let mut bytes = 0u64;
    let mut roots: HashMap<Name, (u64, u64, u64)> = HashMap::new();
    let mut joins: HashMap<JoinId, (u32, u64)> = HashMap::new();
    let mut groups: HashMap<(Name, GroupId), u32> = HashMap::new();
    for (index, slot) in state.slots.iter() {
        let usage = state.usage(slot);
        bytes += usage.memory;
        let root = roots.entry(slot.fixed.root.clone()).or_default();
        root.0 += 1;
        root.1 += usage.share;
        root.2 += usage.disk;
        match &slot.place {
            Place::Waiting => waiting += 1,
            Place::Held => held += 1,
            Place::Delayed { .. } => {}
            Place::Claimed(attempt) => {
                if let Some(join) = &attempt.join {
                    let entry = joins.entry(join.clone()).or_default();
                    entry.0 += 1;
                    entry.1 += slot.len();
                    prop_assert!(state.joins.get(join).is_some_and(|found| found.attempts.contains(&index)));
                }
                if let Some(group) = slot.fixed.group {
                    if state.config.channels.settings(&slot.channel).serialize_group {
                        *groups.entry((slot.channel.clone(), group)).or_default() += 1;
                    }
                }
            }
        }
        prop_assert_eq!(state.slots.by_id(&slot.fixed.id), Some(index));
    }
    prop_assert_eq!(waiting, in_lanes, "every waiting event is in its lane");
    prop_assert_eq!(held, in_holds);
    prop_assert_eq!(bytes, state.slot_bytes, "managed bytes are the sum over the events");
    for (name, root) in &state.roots {
        let (events, root_bytes, disk) = roots.get(name).copied().unwrap_or_default();
        prop_assert_eq!((root.events, root.bytes, root.disk), (events, root_bytes, disk), "root {}", name);
    }
    prop_assert_eq!(roots.len(), state.roots.values().filter(|root| root.events > 0).count());
    for (id, join) in &state.joins {
        prop_assert_eq!((join.claimed, join.claimed_bytes), joins.get(id).copied().unwrap_or_default(), "join {:?}", id);
        prop_assert_eq!(join.claimed as usize, join.attempts.len());
    }
    prop_assert_eq!(&groups, &state.groups);
    Ok(())
}

proptest! {
    #[test]
    fn random_operations_keep_the_ledgers_bookkeeping_consistent(steps in proptest::collection::vec(step(), 1..120), strict in any::<bool>()) {
        let mut config = if strict { super::strict() } else { super::config() };
        config.channels.rules.insert(0, ChannelRule { pattern: Pattern("b".into()), settings: ChannelSettings { serialize_group: true, isolate_after: Some(2), ..ChannelSettings::default() } });
        let mut h = H::with(config, true).cast();
        let workers = [W1, W2];
        for worker in workers {
            for channel in CHANNELS {
                h.call(worker, Call::Join { req: 1, channel: channel.into(), types: Vec::new(), lease_ms: 50, dispatch: false });
            }
        }
        let mut ids: Vec<EventId> = Vec::new();
        for step in steps {
            let attempt = |h: &H, at: u8| ids.get(at as usize).and_then(|id| h.slot(*id).and_then(|slot| slot.attempt().map(|attempt| (*id, attempt.token, attempt.join.clone()))));
            match step {
                Step::Register(channel, grouped) => {
                    let mut event = work(CHANNELS[channel as usize]).with_id(EventId::new_v7());
                    if grouped {
                        event = event.with_group([channel; 16]);
                    }
                    ids.push(event.id);
                    h.produce(P, event);
                }
                Step::Window(worker, count) => {
                    for channel in CHANNELS {
                        h.call(workers[worker as usize], Call::Window { channel: channel.into(), count, bytes: 0 });
                    }
                }
                Step::Complete(at, follow) => {
                    if let Some((id, token, join)) = attempt(&h, at) {
                        let session = join.map_or(W1, |join| join.0);
                        let follow_ups = if follow { vec![work("a"), Envelope::dispatch("feed", "t", Bytes::new())] } else { Vec::new() };
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
                        h.call(P, Call::Delete { seq, id, result: "out".into(), tag: id, key: None });
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
                    for channel in CHANNELS {
                        h.call(session, Call::Join { req: 1, channel: channel.into(), types: Vec::new(), lease_ms: 50, dispatch: false });
                    }
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
