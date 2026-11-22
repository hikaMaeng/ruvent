//! The state machine, driven by random operation sequences.
//!
//! Two kinds of property are checked:
//!
//! * **replay equivalence** — at every journal write, replaying everything
//!   written so far into a fresh state gives back exactly the durable part of
//!   the live state. This is the whole contract of the net-change journal: a
//!   restart carries on from what was written, no more and no less. It is
//!   checked for appended batches, for snapshots, and across restarts;
//! * **invariants after every step** — one owner per execution, no message
//!   held by nobody, a superseded attempt changes nothing, positions only grow.
//!
//! The state is driven directly, synchronously, the way the engine task drives
//! it. No runtime, no disk: thousands of sequences run in well under a second.

use super::*;
use crate::engine::config::{Events, Persist, Retention};
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::time::Duration;

const QUEUES: [&str; 3] = ["a", "b", "t"];
const CHANNELS: [&str; 2] = ["keep.c", "temp.c"];
const FACTS: [&str; 3] = ["fact.x", "fact.y", "fact.z"];

#[derive(Clone, Debug)]
struct Setup {
    settled: bool,
    /// 0 none, 1 all, 2 only `keep.`, 3 all but `keep.`.
    events: u8,
    /// Whether queue `t` is configured as transient.
    transient: bool,
    max_attempts: u32,
    max_events: usize,
    settled_window: u64,
    max_settled: usize,
    dead_letters: usize,
}

fn setup() -> impl Strategy<Value = Setup> {
    (any::<bool>(), 0..4u8, any::<bool>(), 1..4u32, 2..24usize, 0..60u64, 1..6usize, 1..4usize).prop_map(
        |(settled, events, transient, max_attempts, max_events, settled_window, max_settled, dead_letters)| Setup {
            settled,
            events,
            transient,
            max_attempts,
            max_events,
            settled_window,
            max_settled,
            dead_letters,
        },
    )
}

fn persist(setup: &Setup) -> Persist {
    let keep = || vec!["keep.".to_owned()];
    let events = match setup.events {
        0 => Events::None,
        1 => Events::All,
        2 => Events::Channels(keep()),
        _ => Events::Except(keep()),
    };
    let transient_queues: HashSet<Text> = if setup.transient { [Text::from("t")].into() } else { HashSet::new() };
    Persist { events, settled: setup.settled, transient_queues }
}

fn build(setup: &Setup) -> State {
    let reactions: HashMap<Text, Vec<Text>> =
        [("fact.x", vec!["a"]), ("fact.y", vec!["a", "b"]), ("fact.z", vec!["t"])].into_iter().map(|(action, queues)| (Text::from(action), queues.into_iter().map(Text::from).collect())).collect();
    let retention = Retention {
        max_events: setup.max_events,
        max_bytes: usize::MAX,
        max_age: Duration::from_secs(3600),
        settled: Duration::from_millis(setup.settled_window),
        max_settled: setup.max_settled,
        dead_letters: setup.dead_letters,
    };
    State::new(Arc::new(reactions), setup.max_attempts, retention, persist(setup))
}

fn replay(setup: &Setup, journal: &[Record], now: u64) -> State {
    let mut state = build(setup);
    for record in journal {
        state.apply(record, now);
    }
    state
}

#[derive(Clone, Debug)]
enum Step {
    Submit {
        queue: u8,
        key: u8,
        variant: u8,
    },
    /// The very same command again: same event id, a new message.
    Resubmit {
        pick: u16,
    },
    Take {
        queue: u8,
        max: u8,
    },
    Begin {
        pick: u16,
    },
    Finish {
        pick: u16,
        ok: bool,
    },
    Release {
        pick: u16,
        delay: u8,
    },
    Ack {
        pick: u16,
    },
    Nack {
        pick: u16,
        delay: u8,
    },
    Fact {
        pick: u16,
        key: u8,
        fact: u8,
        worker: bool,
    },
    Progress {
        pick: u16,
    },
    Append {
        fact: u8,
        channel: u8,
        key: u8,
    },
    Advance {
        ms: u16,
    },
    Harvest,
    Compact,
    /// A crash: everything not yet harvested is lost, and the next
    /// incarnation starts from the journal.
    Restart,
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        5 => (0..3u8, 0..5u8, 0..4u8).prop_map(|(queue, key, variant)| Step::Submit { queue, key, variant }),
        1 => any::<u16>().prop_map(|pick| Step::Resubmit { pick }),
        5 => (0..3u8, 1..4u8).prop_map(|(queue, max)| Step::Take { queue, max }),
        5 => any::<u16>().prop_map(|pick| Step::Begin { pick }),
        3 => (any::<u16>(), any::<bool>()).prop_map(|(pick, ok)| Step::Finish { pick, ok }),
        2 => (any::<u16>(), 0..3u8).prop_map(|(pick, delay)| Step::Release { pick, delay }),
        1 => any::<u16>().prop_map(|pick| Step::Ack { pick }),
        1 => (any::<u16>(), 0..3u8).prop_map(|(pick, delay)| Step::Nack { pick, delay }),
        2 => (any::<u16>(), 0..4u8, 0..3u8, any::<bool>()).prop_map(|(pick, key, fact, worker)| Step::Fact { pick, key, fact, worker }),
        1 => any::<u16>().prop_map(|pick| Step::Progress { pick }),
        1 => (0..3u8, 0..2u8, 0..4u8).prop_map(|(fact, channel, key)| Step::Append { fact, channel, key }),
        2 => (0..40u16).prop_map(|ms| Step::Advance { ms }),
        2 => Just(Step::Harvest),
        1 => Just(Step::Compact),
        1 => Just(Step::Restart),
    ]
}

/// What the journal is supposed to hold, read out of a state.
#[derive(Debug, PartialEq)]
struct Durable {
    epoch: u64,
    owed: BTreeMap<(String, u64), String>,
    dead: BTreeMap<String, Vec<(u64, EventId)>>,
    executions: BTreeMap<String, String>,
    events: Vec<(u64, EventId)>,
}

/// The durable part of `state`. Events at or before `above` are left out:
/// retention may drop an event from memory after it was written, and a
/// restart may then show it again. Retention is a memory bound, not a promise
/// to delete.
fn durable(state: &State, above: u64) -> Durable {
    let mut owed = BTreeMap::new();
    let mut dead = BTreeMap::new();
    for (name, queue) in &state.queues {
        if state.persist.transient_queues.contains(name) {
            continue;
        }
        for (id, message) in &queue.messages {
            owed.insert((name.to_string(), *id), format!("{}:{}", message.command.event_id, message.command.transaction_key));
        }
        if !queue.dead.is_empty() {
            dead.insert(name.to_string(), queue.dead.iter().map(|(id, command)| (*id, command.event_id)).collect());
        }
    }
    let executions = state
        .executions
        .iter()
        .filter(|(_, execution)| State::wanted(&state.persist, execution))
        .map(|(key, execution)| (key.to_string(), format!("{:?}", (execution.request, execution.fingerprint, execution.attempt, execution.attempts, &execution.done))))
        .collect();
    let events = state.order.iter().filter(|event| event.position > above && state.persist.events.wants(event)).map(|event| (event.position, event.event_id)).collect();
    Durable { epoch: state.epoch, owed, dead, executions, events }
}

struct Claim {
    queue: Text,
    id: u64,
    command: Arc<Envelope>,
    attempt: u64,
}

struct Harness {
    setup: Setup,
    state: State,
    journal: Vec<Record>,
    now: u64,
    /// Messages taken and not yet begun, acknowledged or returned.
    deliveries: Vec<(Text, u64)>,
    /// Every claim ever made in this incarnation, superseded ones included,
    /// so stale attempts keep being exercised.
    claims: Vec<Claim>,
    sent: Vec<(Text, Arc<Command>)>,
}

fn pick(len: usize, pick: u16) -> Option<usize> {
    (len > 0).then(|| pick as usize % len)
}

impl Harness {
    fn new(setup: Setup) -> Self {
        let mut state = build(&setup);
        state.open_epoch();
        let journal = state.harvest();
        Self { setup, state, journal, now: 1_000_000, deliveries: Vec::new(), claims: Vec::new(), sent: Vec::new() }
    }

    /// Everything a stale operation must leave alone.
    fn observe(&self) -> (usize, usize, Durable) {
        (self.state.order.len(), self.state.unwritten_changes(), durable(&self.state, self.state.drop_through))
    }

    fn check_replay(&self, what: &str) -> Result<(), TestCaseError> {
        let replayed = replay(&self.setup, &self.journal, self.now);
        let above = self.state.drop_through;
        prop_assert_eq!(durable(&replayed, above), durable(&self.state, above), "replaying the journal after {} does not give back the durable state", what);
        Ok(())
    }

    fn run(&mut self, step: &Step) -> Result<(), TestCaseError> {
        let now = self.now;
        match *step {
            Step::Submit { queue, key, variant } => {
                let payload: &'static str = if variant % 2 == 0 { "{}" } else { "{\"other\":1}" };
                let mut command = Command::new("job.run", CHANNELS[(variant / 2) as usize % 2], payload).transaction_key(format!("k{key}"));
                command.created_at = now;
                self.state.enqueue(&Text::from(QUEUES[queue as usize]), Arc::new(command));
            }
            Step::Resubmit { pick: at } => {
                if let Some(index) = pick(self.sent.len(), at) {
                    let (queue, command) = self.sent[index].clone();
                    self.state.enqueue(&queue, command);
                }
            }
            Step::Take { queue, max } => {
                let name = Text::from(QUEUES[queue as usize]);
                let taken = self.state.queues.entry(name.clone()).or_default().take(max as usize, None, now);
                for delivery in taken {
                    self.deliveries.push((name.clone(), delivery.id));
                    self.sent.push((name.clone(), delivery.command));
                }
            }
            Step::Begin { pick: at } => {
                let Some(index) = pick(self.deliveries.len(), at) else { return Ok(()) };
                let (queue, id) = self.deliveries.swap_remove(index);
                let queued = self.state.queues.get(&queue).and_then(|owner| owner.messages.get(&id)).map(|message| message.command.clone());
                let begin = self.state.begin(&queue, id, now);
                let still_there = self.state.queues.get(&queue).is_some_and(|owner| owner.messages.contains_key(&id));
                match begin {
                    Begin::Gone => prop_assert!(queued.is_none(), "Gone for a message that exists"),
                    Begin::Claimed { command, attempt, attempts } => {
                        let queued = queued.expect("claimed a message that does not exist");
                        prop_assert_eq!(&command.transaction_key, &queued.transaction_key);
                        prop_assert!(attempts >= 1 && attempts <= self.setup.max_attempts);
                        prop_assert!(still_there, "a claimed message must stay on its queue until it is finished");
                        self.claims.push(Claim { queue, id, command, attempt });
                    }
                    Begin::Exhausted => {
                        prop_assert!(!still_there, "an exhausted message must leave the live queue");
                        prop_assert_eq!(self.state.queues[&queue].dead.back().map(|(dead, _)| *dead), Some(id), "and become the newest dead letter");
                    }
                    Begin::Settled | Begin::Conflict => prop_assert!(!still_there, "{:?} must delete the message", begin),
                    Begin::Duplicate => {}
                }
            }
            Step::Finish { pick: at, ok } => {
                let Some(index) = pick(self.claims.len(), at) else { return Ok(()) };
                let claim = self.claims.swap_remove(index);
                let owned = self.state.owns(&claim.command.transaction_key, claim.attempt);
                let before = self.observe();
                let outcome = if ok { Outcome::Ok(Bytes::from_static(b"\"ok\"")) } else { Outcome::Failed(Bytes::from_static(b"no")) };
                let done = self.state.finish(&claim.command, claim.attempt, outcome, &claim.queue, claim.id, now);
                prop_assert_eq!(done, owned, "finish succeeds exactly when the attempt owns the execution");
                if done {
                    prop_assert!(!self.state.queues[&claim.queue].messages.contains_key(&claim.id), "finishing deletes the message in the same step");
                } else {
                    prop_assert_eq!(self.observe(), before, "a superseded finish changed something");
                }
            }
            Step::Release { pick: at, delay } => {
                let Some(index) = pick(self.claims.len(), at) else { return Ok(()) };
                let claim = &self.claims[index];
                let owned = self.state.owns(&claim.command.transaction_key, claim.attempt);
                let before = self.observe();
                let released = self.state.release(&claim.command.transaction_key, claim.attempt, &claim.queue, claim.id, [0, 5, 20][delay as usize % 3], now);
                prop_assert_eq!(released, owned);
                if !released {
                    prop_assert_eq!(self.observe(), before, "a superseded release changed something");
                }
            }
            Step::Ack { pick: at } => {
                if let Some(index) = pick(self.deliveries.len(), at) {
                    let (queue, id) = self.deliveries.swap_remove(index);
                    self.state.ack(&queue, id);
                }
            }
            Step::Nack { pick: at, delay } => {
                if let Some(index) = pick(self.deliveries.len(), at) {
                    let (queue, id) = self.deliveries.swap_remove(index);
                    prop_assert!(self.state.nack(&queue, id, [0, 5, 20][delay as usize % 3], now), "a held delivery can always be returned");
                }
            }
            Step::Fact { pick: at, key, fact, worker } => {
                let Some(index) = pick(self.claims.len(), at) else { return Ok(()) };
                let audience = if worker { Audience::Worker } else { Audience::Client };
                let emit = Emit { action: FACTS[fact as usize].into(), payload: Bytes::from_static(b"{}"), kind: Kind::Fact, key: Some(format!("f{key}").into()), audience };
                self.emit(index, emit)?;
            }
            Step::Progress { pick: at } => {
                let Some(index) = pick(self.claims.len(), at) else { return Ok(()) };
                let emit = Emit { action: "delta".into(), payload: Bytes::from_static(b"\"x\""), kind: Kind::Progress, key: None, audience: Audience::Client };
                self.emit(index, emit)?;
            }
            Step::Append { fact, channel, key } => {
                let mut draft = Envelope::draft(Kind::Fact, FACTS[fact as usize], CHANNELS[channel as usize], "{}");
                draft.event_id = EventId::derive(&[b"appended", &[fact, channel, key]]);
                self.state.append(draft, now);
            }
            Step::Advance { ms } => self.now += ms as u64,
            Step::Harvest => {
                // The engine writes only when the state says it is dirty, so
                // anything a harvest would write must have made it dirty.
                let dirty = self.state.is_dirty();
                let records = self.state.harvest();
                prop_assert!(dirty || records.is_empty(), "{} records were due but the state did not say it was dirty", records.len());
                // One write states an execution once, in its final state.
                let mut stated = HashSet::new();
                for record in &records {
                    if let Record::Exec { key, .. } = record {
                        prop_assert!(stated.insert(key.clone()), "execution {} stated twice in one write", key);
                    }
                }
                for record in &records {
                    if let Record::Event(event) = record {
                        prop_assert!(event.position > self.state.drop_through, "event at {} was written after retention had dropped it", event.position);
                    }
                }
                self.journal.extend(records);
                prop_assert!(!self.state.is_dirty(), "a harvest leaves nothing unwritten");
                self.check_replay("an appended batch")?;
            }
            Step::Compact => {
                // The compaction trigger compares the journal against this
                // count; it must be what a snapshot actually writes.
                let estimate = self.state.live_records();
                self.journal = self.state.snapshot();
                prop_assert_eq!(self.journal.len() as u64, estimate, "live_records() is not the size of a snapshot");
                prop_assert!(!self.state.is_dirty(), "a snapshot leaves nothing unwritten");
                self.check_replay("a snapshot")?;
            }
            Step::Restart => {
                let mut next = replay(&self.setup, &self.journal, now);
                // What replay staged is what the engine publishes as the
                // readable log after a restart: it must be exactly the log.
                let staged: Vec<u64> = next.staged.iter().map(|event| event.position).filter(|position| *position > next.drop_through).collect();
                let kept: Vec<u64> = next.order.iter().map(|event| event.position).collect();
                prop_assert_eq!(staged, kept, "the log readers see after a restart is not the replayed log");
                next.open_epoch();
                prop_assert!(next.epoch > self.state.epoch, "a new incarnation has a higher epoch");
                // The engine writes the new epoch before it serves anyone, so
                // an incarnation that issued positions always has its epoch on
                // disk. That is what keeps a later one from reusing it.
                self.journal.extend(next.harvest());
                self.state = next;
                self.deliveries.clear();
                self.claims.clear();
            }
        }
        self.state.staged.clear();
        self.invariants()
    }

    fn emit(&mut self, index: usize, emit: Emit) -> Result<(), TestCaseError> {
        let claim = &self.claims[index];
        let owned = self.state.owns(&claim.command.transaction_key, claim.attempt);
        let before = self.observe();
        let written = self.state.emit(&claim.command.clone(), claim.attempt, emit, self.now);
        prop_assert_eq!(written, owned, "an emission is recorded exactly when the attempt owns the execution");
        if !written {
            prop_assert_eq!(self.observe(), before, "a superseded emission changed something");
        }
        Ok(())
    }

    fn invariants(&self) -> Result<(), TestCaseError> {
        // Positions only grow, and the log never holds one event twice.
        let mut seen = HashSet::new();
        for pair in self.state.order.iter().collect::<Vec<_>>().windows(2) {
            prop_assert!(pair[0].position < pair[1].position, "positions out of order");
        }
        for event in &self.state.order {
            prop_assert!(seen.insert(event.event_id), "event {} is in the log twice", event.event_id);
        }
        let bytes: usize = self.state.order.iter().map(|event| cost(event)).sum();
        prop_assert_eq!(self.state.bytes, bytes, "the byte count drifted from the events it counts");
        // The dedup index holds exactly the retained events: no more, or it
        // grows without bound and refuses facts that are no longer there.
        prop_assert_eq!(self.state.ids.len(), self.state.order.len(), "the dedup index and the log disagree in size");
        for event in &self.state.order {
            prop_assert_eq!(self.state.ids.get(&event.event_id).map(|known| known.position), Some(event.position));
        }

        // One owner per execution.
        let mut owners: BTreeMap<&str, usize> = BTreeMap::new();
        for claim in &self.claims {
            if self.state.owns(&claim.command.transaction_key, claim.attempt) {
                *owners.entry(&claim.command.transaction_key).or_default() += 1;
            }
        }
        prop_assert!(owners.values().all(|count| *count == 1), "an execution has two owners: {:?}", owners);

        // A message held without a deadline comes back only when its holder
        // says so. So it must have a holder: a delivery not yet dealt with, or
        // an attempt that still owns its execution. Otherwise it is lost to
        // every consumer until the process restarts.
        let mut holders: BTreeSet<(Text, u64)> = self.deliveries.iter().cloned().collect();
        holders.extend(self.claims.iter().filter(|claim| self.state.owns(&claim.command.transaction_key, claim.attempt)).map(|claim| (claim.queue.clone(), claim.id)));
        for (name, queue) in &self.state.queues {
            for id in queue.held() {
                prop_assert!(holders.contains(&(name.clone(), id)), "message {} on {} is held by nobody", id, name);
            }
        }

        for execution in self.state.executions.values() {
            prop_assert!(execution.attempts <= self.setup.max_attempts);
        }

        // Within a queue, a message id names one thing: a live message or
        // one dead letter. A restart must not hand out an id still in use.
        for (name, queue) in &self.state.queues {
            let mut ids: BTreeSet<u64> = queue.messages.keys().copied().collect();
            for (id, _) in &queue.dead {
                prop_assert!(ids.insert(*id), "id {} on {} names two things", id, name);
            }
        }
        Ok(())
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 2048, ..ProptestConfig::default() })]

    #[test]
    fn the_journal_always_replays_to_the_durable_state(setup in setup(), steps in prop::collection::vec(step(), 1..160)) {
        let mut harness = Harness::new(setup);
        for step in &steps {
            harness.run(step)?;
        }
        harness.run(&Step::Harvest)?;
        harness.run(&Step::Restart)?;
        harness.run(&Step::Harvest)?;
    }
}

// ---------------------------------------------------------------- regressions
//
// Each of these was first found by the property above, then pinned here as
// the smallest sequence that shows it.

fn plain(max_settled: usize) -> Setup {
    Setup { settled: false, events: 0, transient: false, max_attempts: 3, max_events: 16, settled_window: 0, max_settled, dead_letters: 1 }
}

fn submit(harness: &mut Harness, key: u8) {
    harness.run(&Step::Submit { queue: 0, key, variant: 0 }).unwrap();
}

/// Takes the next message on `a` and begins it.
fn take_and_begin(harness: &mut Harness) {
    harness.run(&Step::Take { queue: 0, max: 1 }).unwrap();
    let last = (harness.deliveries.len() - 1) as u16;
    harness.run(&Step::Begin { pick: last }).unwrap();
}

/// The same command submitted twice is two messages carrying one event. The
/// second must be dropped when it is begun while the first runs — not left
/// in flight with nobody holding it.
#[test]
fn a_second_message_carrying_the_same_event_is_dropped_not_stranded() {
    let mut harness = Harness::new(plain(10));
    submit(&mut harness, 0);
    take_and_begin(&mut harness);
    assert_eq!(harness.claims.len(), 1);
    harness.run(&Step::Resubmit { pick: 0 }).unwrap();
    harness.run(&Step::Take { queue: 0, max: 1 }).unwrap();
    harness.run(&Step::Begin { pick: 0 }).expect("the duplicate message must not be stranded");
    assert_eq!(harness.state.queues[&Text::from("a")].messages.len(), 1, "only the message being run is left");
}

/// An execution forgotten when its dedup window ran out, then started again
/// under the same key before the next write. The journal must end up saying
/// the new one exists.
#[test]
fn a_key_started_again_after_it_was_forgotten_survives_a_restart() {
    let mut harness = Harness::new(plain(1));
    submit(&mut harness, 0);
    take_and_begin(&mut harness);
    harness.run(&Step::Harvest).unwrap(); // k0 running, on disk
    harness.run(&Step::Finish { pick: 0, ok: true }).unwrap();
    submit(&mut harness, 1);
    take_and_begin(&mut harness);
    harness.run(&Step::Finish { pick: 0, ok: true }).unwrap(); // k1 settles, k0's record expires
    submit(&mut harness, 0);
    take_and_begin(&mut harness); // k0 again, a new execution
    harness.run(&Step::Harvest).expect("the restarted execution must be in the journal");
}

/// A fact retention dropped and that was then recorded again is in the
/// journal twice. After a restart the log must still hold it once.
///
/// Memory is filled with events from a channel that is not journaled, so the
/// replayed journal has room for both copies.
#[test]
fn an_event_recorded_again_after_retention_dropped_it_is_replayed_once() {
    let mut harness = Harness::new(Setup { events: 2, max_events: 2, ..plain(10) });
    let kept = |key| Step::Append { fact: 2, channel: 0, key };
    let unkept = |key| Step::Append { fact: 2, channel: 1, key };
    harness.run(&kept(0)).unwrap();
    harness.run(&Step::Harvest).unwrap();
    harness.run(&unkept(1)).unwrap();
    harness.run(&unkept(2)).unwrap(); // the kept one is dropped from memory
    harness.run(&kept(0)).unwrap(); // and recorded again under the same id
    harness.run(&Step::Harvest).unwrap();
    harness.run(&Step::Restart).expect("the log must not hold one event twice after a restart");
    assert_eq!(harness.state.order.len(), 1);
}

/// After a restart, a new message never takes the id of a dead letter.
#[test]
fn a_restart_never_reuses_a_dead_letters_id() {
    let mut harness = Harness::new(Setup { max_attempts: 1, ..plain(10) });
    submit(&mut harness, 0);
    take_and_begin(&mut harness);
    harness.run(&Step::Release { pick: 0, delay: 0 }).unwrap();
    take_and_begin(&mut harness); // out of attempts: dead letter
    assert_eq!(harness.state.queues[&Text::from("a")].dead.len(), 1);
    harness.run(&Step::Harvest).unwrap();
    harness.run(&Step::Restart).unwrap();
    submit(&mut harness, 1); // the invariants check the new id against the dead one
    let dead = harness.state.queues[&Text::from("a")].dead[0].0;
    assert!(harness.state.queues[&Text::from("a")].messages.keys().all(|id| *id > dead));
}

/// Attempt numbers only grow, across restarts too. A handler may hand its
/// attempt to the outside world as a fencing token.
#[test]
fn attempt_numbers_only_grow_across_restarts() {
    let mut harness = Harness::new(plain(10));
    submit(&mut harness, 0);
    take_and_begin(&mut harness);
    let before = harness.claims[0].attempt;
    harness.run(&Step::Harvest).unwrap();
    harness.run(&Step::Restart).unwrap();
    take_and_begin(&mut harness);
    assert!(harness.claims[0].attempt > before, "attempt {} after a restart, {before} before", harness.claims[0].attempt);
}

/// An execution that expired while it was waiting to be written, followed by
/// a new one under the same key, is stated once in the next write.
#[test]
fn a_key_listed_twice_is_stated_once() {
    let mut harness = Harness::new(plain(1));
    submit(&mut harness, 0);
    take_and_begin(&mut harness);
    harness.run(&Step::Finish { pick: 0, ok: true }).unwrap(); // k0 listed for the next write
    submit(&mut harness, 1);
    take_and_begin(&mut harness);
    harness.run(&Step::Finish { pick: 0, ok: true }).unwrap(); // k0 expires while listed
    submit(&mut harness, 0);
    take_and_begin(&mut harness); // a new k0, listed again
    harness.run(&Step::Harvest).expect("one write states an execution once");
}

/// After a restart the expiry list can name a key twice: the old settlement
/// and a newer one. The old entry expiring must not take the newer
/// settlement with it.
#[test]
fn an_old_expiry_entry_does_not_take_a_newer_settlement_with_it() {
    let mut harness = Harness::new(Setup { settled: true, settled_window: 10, ..plain(10) });
    let run = |harness: &mut Harness, key| {
        submit(harness, key);
        take_and_begin(harness);
        let last = (harness.claims.len() - 1) as u16;
        harness.run(&Step::Finish { pick: last, ok: true }).unwrap();
    };
    run(&mut harness, 0); // k0 settles at T
    harness.run(&Step::Harvest).unwrap();
    harness.run(&Step::Advance { ms: 20 }).unwrap();
    run(&mut harness, 1); // k0's window has passed: forgotten
    run(&mut harness, 0); // k0 runs again and settles at T+20
    harness.run(&Step::Harvest).unwrap();
    harness.run(&Step::Restart).unwrap(); // replay lists (T, k0) and (T+20, k0)
    harness.run(&Step::Advance { ms: 5 }).unwrap();
    run(&mut harness, 2); // (T, k0) expires; k0 settled at T+20 must stay

    submit(&mut harness, 0);
    harness.run(&Step::Take { queue: 0, max: 1 }).unwrap();
    let claims = harness.claims.len();
    let last = (harness.deliveries.len() - 1) as u16;
    harness.run(&Step::Begin { pick: last }).unwrap();
    assert_eq!(harness.claims.len(), claims, "k0 settled 5 ms ago, inside its window: answered, not run again");
}
