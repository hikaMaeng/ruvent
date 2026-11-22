//! The queue against a model written the obvious way.
//!
//! The real queue keeps a ready line, a heap of deadlines that is never
//! cleaned early, and stale entries it skips when they surface. The model
//! keeps one state per message and recomputes everything from it. Whatever
//! the sequence, both must hand out the same messages in the same order, and
//! the real queue must never ask to be woken later than the model says a
//! message comes back.

use super::*;
use proptest::prelude::*;
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq)]
enum State {
    Ready,
    /// Held until given back.
    Held,
    /// Comes back by itself at this moment.
    Leased(u64),
}

#[derive(Default)]
struct Model {
    messages: BTreeMap<u64, (State, u32)>,
    ready: VecDeque<u64>,
}

impl Model {
    fn put(&mut self, id: u64) {
        self.messages.insert(id, (State::Ready, 0));
        self.ready.push_back(id);
    }

    fn remove(&mut self, id: u64) -> bool {
        self.ready.retain(|ready| *ready != id);
        self.messages.remove(&id).is_some()
    }

    fn promote(&mut self, now: u64) {
        let mut due: Vec<(u64, u64)> = self
            .messages
            .iter()
            .filter_map(|(id, (state, _))| match state {
                State::Leased(at) if *at <= now => Some((*at, *id)),
                _ => None,
            })
            .collect();
        due.sort();
        for (_, id) in due {
            self.messages.get_mut(&id).unwrap().0 = State::Ready;
            self.ready.push_back(id);
        }
    }

    fn take(&mut self, max: usize, visibility: Option<u64>, now: u64) -> Vec<(u64, u32)> {
        self.promote(now);
        let mut taken = Vec::new();
        while taken.len() < max {
            let Some(id) = self.ready.pop_front() else { break };
            let entry = self.messages.get_mut(&id).unwrap();
            entry.0 = visibility.map_or(State::Held, |visibility| State::Leased(now + visibility.max(1)));
            entry.1 += 1;
            taken.push((id, entry.1));
        }
        taken
    }

    fn give_back(&mut self, id: u64, delay: u64, now: u64) -> bool {
        let Some(entry) = self.messages.get_mut(&id) else { return false };
        if entry.0 == State::Ready {
            return false;
        }
        if delay == 0 {
            entry.0 = State::Ready;
            self.ready.push_back(id);
        } else {
            entry.0 = State::Leased(now + delay);
        }
        true
    }

    fn earliest(&self) -> Option<u64> {
        self.messages.values().filter_map(|(state, _)| if let State::Leased(at) = state { Some(*at) } else { None }).min()
    }
}

#[derive(Clone, Debug)]
enum Step {
    Put,
    Take { max: u8, visibility: Option<u8> },
    GiveBack { pick: u16, delay: u8 },
    Remove { pick: u16 },
    Advance { ms: u8 },
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        3 => Just(Step::Put),
        4 => (1..4u8, prop::option::of(0..20u8)).prop_map(|(max, visibility)| Step::Take { max, visibility }),
        3 => (any::<u16>(), prop_oneof![Just(0u8), 1..20u8]).prop_map(|(pick, delay)| Step::GiveBack { pick, delay }),
        1 => any::<u16>().prop_map(|pick| Step::Remove { pick }),
        3 => (0..15u8).prop_map(|ms| Step::Advance { ms }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 4096, ..ProptestConfig::default() })]

    #[test]
    fn the_queue_behaves_like_its_model(steps in prop::collection::vec(step(), 1..200)) {
        let command = Arc::new(Command::new("job", "c", "{}"));
        let (mut queue, mut model) = (Queue::default(), Model::default());
        let (mut now, mut next_id) = (100u64, 1u64);
        // A parked consumer, so the queue reports its deadlines.
        let (reply, _keep) = oneshot::channel();
        queue.waiters.push_back(Waiter { max: 1, visibility: None, deadline: None, reply });

        for step in steps {
            match step {
                Step::Put => {
                    queue.put(next_id, command.clone(), false);
                    model.put(next_id);
                    next_id += 1;
                }
                Step::Take { max, visibility } => {
                    let visibility = visibility.map(u64::from);
                    let real: Vec<(u64, u32)> = queue.take(max as usize, visibility, now).iter().map(|delivery| (delivery.id, delivery.read_count)).collect();
                    prop_assert_eq!(real, model.take(max as usize, visibility, now));
                }
                Step::GiveBack { pick, delay } => {
                    let id = pick as u64 % next_id.max(1);
                    prop_assert_eq!(queue.give_back(id, delay as u64, now), model.give_back(id, delay as u64, now));
                }
                Step::Remove { pick } => {
                    let id = pick as u64 % next_id.max(1);
                    prop_assert_eq!(queue.remove(id).is_some(), model.remove(id));
                }
                Step::Advance { ms } => now += ms as u64,
            }
            // Woken no later than the first message comes back. Earlier is
            // allowed — a stale deadline surfaces once and is dropped.
            if let Some(due) = model.earliest() {
                let asked = queue.next_deadline();
                prop_assert!(asked.is_some_and(|asked| asked <= due), "asked to be woken at {:?}, a message returns at {}", asked, due);
            }
        }
    }
}

#[test]
fn with_nobody_waiting_the_queue_asks_for_no_wakeup() {
    let mut queue = Queue::default();
    queue.put(1, Arc::new(Command::new("job", "c", "{}")), false);
    queue.take(1, Some(10), 0);
    assert_eq!(queue.next_deadline(), None, "an expired deadline is noticed by the next consumer to arrive");
}
