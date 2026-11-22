//! The server's rules, decided against the state machine alone.

use super::*;
use crate::engine::config::{Grant, Pattern};
use proptest::prelude::*;

const A: SessionId = 1;
const B: SessionId = 2;
const OTHER: SessionId = 3;

fn started(config: EngineConfig, ids: Box<dyn IdSource>) -> State {
    let mut state = State::new(&config, ids);
    state.recover(Vec::new()).unwrap();
    state.open(A, &Name::new("svc"));
    state.open(B, &Name::new("svc"));
    state.open(OTHER, &Name::new("other"));
    state.begin(0);
    let _ = state.take();
    state
}

fn fresh() -> State {
    started(EngineConfig::default(), Box::new(V7))
}

/// Applies one call and returns its reply and everything else it produced.
fn run(state: &mut State, session: SessionId, call: Call) -> (Reply, Effects) {
    state.call(session, 7, call);
    let mut effects = state.take();
    let at = effects.sends.iter().position(|send| send.session == session && matches!(send.message, Outbound::Reply { .. })).expect("every call is answered");
    let Outbound::Reply { reply, .. } = effects.sends.remove(at).message else { unreachable!() };
    (reply, effects)
}

fn reply(state: &mut State, session: SessionId, call: Call) -> Reply {
    run(state, session, call).0
}

fn register(state: &mut State, id: EventId) -> (EventId, WorkKey) {
    match reply(state, A, Call::RegisterClaimable(Envelope::claimable("work", "do", Bytes::from_static(b"p")).with_id(id))) {
        Reply::Registered { id, key } => (id, key),
        other => panic!("{other:?}"),
    }
}

fn claim(state: &mut State, session: SessionId, id: EventId, lease_ms: u64) -> Result<Envelope, Refusal> {
    match reply(state, session, Call::Claim { id, worker: 9, lease_ms }) {
        Reply::Claimed(event) => Ok(event),
        Reply::Refused(refusal) => Err(refusal),
        other => panic!("{other:?}"),
    }
}

fn complete(state: &mut State, session: SessionId, id: EventId, token: ClaimToken, follow_ups: Vec<Envelope>) -> Reply {
    reply(state, session, Call::Complete { id, token, follow_ups })
}

fn token(event: &Envelope) -> ClaimToken {
    event.claim.expect("claimed").token
}

fn id(byte: u8) -> EventId {
    let mut bytes = [byte; 16];
    bytes[6] = 0x70 | (byte & 0x0f);
    bytes[8] = 0x80 | (byte & 0x3f);
    EventId(bytes)
}

/// Fresh ids from a list, then UUIDv7.
struct Scripted(std::collections::VecDeque<EventId>);

impl IdSource for Scripted {
    fn next(&mut self) -> EventId {
        self.0.pop_front().unwrap_or_else(EventId::new_v7)
    }
}

fn the_id_now(state: &State, key: WorkKey) -> EventId {
    state.queue[&key].id
}

#[test]
fn of_two_claims_on_one_waiting_event_exactly_one_wins() {
    let mut state = fresh();
    let (id, _) = register(&mut state, EventId::NIL);
    let won = claim(&mut state, A, id, 1000).unwrap();
    assert_eq!(claim(&mut state, B, id, 1000), Err(Refusal::AlreadyClaimed));
    // The loser's report has no effect; the winner's does.
    let mut stale = token(&won);
    stale.seq += 1;
    assert_eq!(complete(&mut state, B, id, stale, Vec::new()), Reply::Refused(Refusal::StaleToken));
    assert_eq!(complete(&mut state, A, id, token(&won), Vec::new()), Reply::Completed);
    assert_eq!(state.live(), 0);
}

#[test]
fn a_claim_answer_carries_the_whole_envelope_with_key_and_claim_block() {
    let mut state = fresh();
    state.begin(500);
    let (id, key) = register(&mut state, EventId::NIL);
    let event = claim(&mut state, A, id, 30_000).unwrap();
    assert_eq!(event.key, Some(key));
    assert_eq!(event.payload, Bytes::from_static(b"p"));
    assert_eq!(event.principal.as_deref(), Some("svc"));
    let claim = event.claim.unwrap();
    assert_eq!((claim.worker, claim.deadline), (9, 30_500), "the deadline is the server's clock plus the lease");
}

#[test]
fn completion_from_another_credential_is_refused_and_the_same_credential_on_a_new_connection_succeeds() {
    let mut state = fresh();
    let (id, _) = register(&mut state, EventId::NIL);
    let event = claim(&mut state, A, id, 1000).unwrap();
    assert_eq!(complete(&mut state, OTHER, id, token(&event), Vec::new()), Reply::Refused(Refusal::WrongCredential));
    // The claiming connection goes away; a new connection under the same credential completes.
    state.close(A);
    state.open(10, &Name::new("svc"));
    assert_eq!(complete(&mut state, 10, id, token(&event), Vec::new()), Reply::Completed);
    assert!(state.attributions.is_empty(), "the attribution goes with its last claim once its connection is gone");
}

#[test]
fn an_id_in_the_claimable_queue_refuses_every_kind_of_registration() {
    let mut state = fresh();
    let (taken, _) = register(&mut state, id(1));
    let reused = |kind: Kind| match kind {
        Kind::Claimable => Envelope::claimable("work", "do", Bytes::new()).with_id(taken),
        Kind::Dispatch => Envelope::dispatch("work", "do", Bytes::new()).with_id(taken),
    };
    assert_eq!(reply(&mut state, A, Call::RegisterClaimable(reused(Kind::Claimable))), Reply::Refused(Refusal::Duplicate));
    assert_eq!(reply(&mut state, A, Call::RegisterDispatch(reused(Kind::Dispatch))), Reply::Refused(Refusal::Duplicate));
    // While claimed, too, and as a follow-up.
    let (parent, _) = register(&mut state, EventId::NIL);
    let held = claim(&mut state, A, parent, 1000).unwrap();
    assert_eq!(claim(&mut state, A, taken, 1000).map(|_| ()), Ok(()));
    assert_eq!(complete(&mut state, A, parent, token(&held), vec![reused(Kind::Claimable)]), Reply::Refused(Refusal::Duplicate));
    assert!(state.ids.contains_key(&parent), "the parent stays claimed when a follow-up is refused");
    // Two follow-ups with one id.
    let twins = vec![Envelope::dispatch("x", "t", Bytes::new()).with_id(id(2)), Envelope::claimable("x", "t", Bytes::new()).with_id(id(2))];
    let (outcome, effects) = run(&mut state, A, Call::Complete { id: parent, token: token(&held), follow_ups: twins });
    assert_eq!(outcome, Reply::Refused(Refusal::Duplicate));
    assert!(effects.records.is_empty() && effects.sends.is_empty(), "a refused completion changes nothing and sends nothing");
    // Once it leaves the queue the id may be used again.
    let (_, effects) = run(&mut state, A, Call::Complete { id: parent, token: token(&held), follow_ups: Vec::new() });
    assert_eq!(effects.records, vec![Record::Remove(state_key_of_removed(&effects))]);
    assert!(matches!(reply(&mut state, A, Call::RegisterClaimable(Envelope::claimable("work", "do", Bytes::new()).with_id(parent))), Reply::Registered { .. }));
}

fn state_key_of_removed(effects: &Effects) -> WorkKey {
    match effects.records.first() {
        Some(Record::Remove(key)) => *key,
        other => panic!("{other:?}"),
    }
}

#[test]
fn reissued_ids_skip_the_old_id_and_every_live_id() {
    let mut state = started(EngineConfig::default(), Box::new(Scripted(std::collections::VecDeque::new())));
    let (live, _) = register(&mut state, id(5));
    let (old, key) = register(&mut state, id(6));
    claim(&mut state, A, old, 10).unwrap();
    // The next fresh ids offered are the reclaimed event's own id and the other live id.
    state.id_source = Box::new(Scripted([old, live, id(7)].into()));
    state.begin(10);
    let _ = reply(&mut state, A, Call::RegisterDispatch(Envelope::dispatch("noise", "t", Bytes::new()).with_id(id(8))));
    assert_eq!(the_id_now(&state, key), id(7));
}

#[test]
fn reclaim_keeps_the_work_key_and_a_new_registration_under_the_old_id_gets_another() {
    let mut state = fresh();
    let (x, first) = register(&mut state, id(0x10));
    let event = claim(&mut state, A, x, 100).unwrap();
    state.begin(100);
    let (_, second) = register(&mut state, EventId::NIL);
    let y = the_id_now(&state, first);
    assert_ne!(y, x, "the reclaimed event has a new id");
    assert_eq!(state.queue[&first].retry, 1);
    // B registers under x, which left the queue with the reclaim.
    let (again, third) = register(&mut state, x);
    assert_eq!(again, x);
    assert!(first != third && second != third, "two registrations under one id have different work keys");
    // The old completion, claim and keyed delete arrive late.
    assert_eq!(complete(&mut state, A, x, token(&event), Vec::new()), Reply::Refused(Refusal::NotClaimed));
    let rewon = claim(&mut state, A, x, 100).unwrap();
    assert_eq!(rewon.key, Some(third), "a claim by id reaches whatever has that id now");
    assert_eq!(complete(&mut state, A, x, token(&event), Vec::new()), Reply::Refused(Refusal::StaleToken));
    let (outcome, _) = run(&mut state, A, Call::Delete { id: y, result: "out".into(), tag: id(1), key: Some(third) });
    assert_eq!(outcome, Reply::Deleted { removed: false });
}

#[test]
fn only_an_external_registration_reclaims_and_only_once_per_batch() {
    let mut state = fresh();
    let (parent, _) = register(&mut state, EventId::NIL);
    let (late, late_key) = register(&mut state, EventId::NIL);
    let held = claim(&mut state, A, parent, 1000).unwrap();
    claim(&mut state, A, late, 10).unwrap();
    state.begin(500);
    // A completion with follow-ups and a deletion do not reclaim.
    assert_eq!(complete(&mut state, A, parent, token(&held), vec![Envelope::claimable("work", "next", Bytes::new())]), Reply::Completed);
    let _ = reply(&mut state, A, Call::Delete { id: id(9), result: "out".into(), tag: id(9), key: None });
    assert!(state.queue[&late_key].claim.is_some(), "still claimed: nothing external registered yet");
    let (_, effects) = run(&mut state, A, Call::RegisterDispatch(Envelope::dispatch("noise", "t", Bytes::new())));
    assert!(state.queue[&late_key].claim.is_none());
    assert!(matches!(effects.records.as_slice(), [Record::Reclaim { retry: 1, .. }]));
    // The second registration of the same batch does not look again.
    let (fresh_id, _) = register(&mut state, EventId::NIL);
    claim(&mut state, A, fresh_id, 0).unwrap();
    let (_, effects) = run(&mut state, A, Call::RegisterDispatch(Envelope::dispatch("noise", "t", Bytes::new())));
    assert!(effects.records.is_empty());
}

#[test]
fn a_thirty_second_claim_expires_after_exactly_thirty_seconds_across_the_u32_millisecond_boundary() {
    let boundary = u64::from(u32::MAX);
    for start in [boundary - 10_000, boundary, boundary + 1] {
        let mut state = fresh();
        state.begin(start);
        let (id, key) = register(&mut state, EventId::NIL);
        claim(&mut state, A, id, 30_000).unwrap();
        state.begin(start + 29_999);
        register(&mut state, EventId::NIL);
        assert!(state.queue[&key].claim.is_some(), "not yet at {start}");
        state.begin(start + 30_000);
        register(&mut state, EventId::NIL);
        assert!(state.queue[&key].claim.is_none(), "expired at {start}");
    }
}

#[test]
fn exhausted_sequences_advance_the_generation_and_gate_the_batch() {
    let mut state = fresh();
    let generation = state.generation;
    state.arrival = u32::MAX;
    state.claim_seq = u32::MAX - 1;
    let (first_id, before) = {
        let (outcome, effects) = run(&mut state, A, Call::RegisterClaimable(Envelope::claimable("work", "do", Bytes::new())));
        assert!(effects.gate_all, "nothing of a batch that advanced the generation leaves before it is durable");
        assert_eq!(effects.records.first(), Some(&Record::Generation(generation + 1)));
        match outcome {
            Reply::Registered { id, key } => (id, key),
            other => panic!("{other:?}"),
        }
    };
    assert_eq!(before, WorkKey { generation: generation + 1, arrival: 0 });
    let event = claim(&mut state, A, first_id, 100).unwrap();
    assert_eq!(token(&event), ClaimToken { generation: generation + 1, seq: 0 });
    let (_, later) = register(&mut state, EventId::NIL);
    assert!(later > before, "FIFO order continues across the generation change");
}

#[test]
fn deleting_a_waiting_event_succeeds_once_and_a_claimed_or_missing_one_fails_with_one_notice() {
    let mut state = fresh();
    let _ = reply(&mut state, B, Call::Subscribe { channel: "out".into(), after: None });
    let (waiting, _) = register(&mut state, EventId::NIL);
    let (claimed, _) = register(&mut state, EventId::NIL);
    claim(&mut state, A, claimed, 1000).unwrap();
    for (target, removed, kind) in [(waiting, true, DELETED), (claimed, false, DELETE_FAILED), (id(3), false, DELETE_FAILED), (waiting, false, DELETE_FAILED)] {
        let tag = EventId::new_v7();
        let (outcome, effects) = run(&mut state, A, Call::Delete { id: target, result: "out".into(), tag, key: None });
        assert_eq!(outcome, Reply::Deleted { removed });
        let notices: Vec<&Envelope> = effects
            .sends
            .iter()
            .filter_map(|send| match &send.message {
                Outbound::Delivery(event) if send.session == B => Some(event),
                _ => None,
            })
            .collect();
        assert_eq!(notices.len(), 1, "exactly one result event");
        assert_eq!((&*notices[0].event_type, notices[0].correlation), (kind, Some(tag)));
        assert_eq!(&notices[0].payload[..16], &target.0);
    }
}

#[test]
fn delete_then_claim_and_claim_then_delete() {
    let mut state = fresh();
    let (first, _) = register(&mut state, EventId::NIL);
    assert_eq!(reply(&mut state, A, Call::Delete { id: first, result: "out".into(), tag: id(1), key: None }), Reply::Deleted { removed: true });
    assert_eq!(claim(&mut state, A, first, 10), Err(Refusal::Missing));
    let (second, key) = register(&mut state, EventId::NIL);
    claim(&mut state, A, second, 10).unwrap();
    assert_eq!(reply(&mut state, A, Call::Delete { id: second, result: "out".into(), tag: id(1), key: None }), Reply::Deleted { removed: false });
    assert!(state.queue[&key].claim.is_some(), "the claim stands");
}

#[test]
fn dispatch_reaches_exactly_the_subscribers_at_the_moment_it_is_registered() {
    let mut state = fresh();
    let dispatch = |state: &mut State| run(state, A, Call::RegisterDispatch(Envelope::dispatch("feed", "t", Bytes::new()))).1;
    let delivered = |effects: &Effects| effects.sends.iter().filter(|send| send.session == B && matches!(send.message, Outbound::Delivery(_))).count();
    assert_eq!(delivered(&dispatch(&mut state)), 0, "nobody subscribed: sent to nobody, kept nowhere");
    let _ = reply(&mut state, B, Call::Subscribe { channel: "feed".into(), after: None });
    assert_eq!(delivered(&dispatch(&mut state)), 1);
    let _ = reply(&mut state, B, Call::Unsubscribe { channel: "feed".into() });
    assert_eq!(delivered(&dispatch(&mut state)), 0);
    assert!(state.queue.is_empty() && state.ids.is_empty());
}

#[test]
fn the_backlog_comes_in_pages_in_work_key_order() {
    let mut state = started(EngineConfig { backlog_page: 3, ..EngineConfig::default() }, Box::new(V7));
    let keys: Vec<WorkKey> = (0..7).map(|_| register(&mut state, EventId::NIL).1).collect();
    let mut seen = Vec::new();
    let mut after = None;
    loop {
        let Reply::Subscribed { backlog, more, .. } = reply(&mut state, B, Call::Subscribe { channel: "work".into(), after }) else { panic!() };
        assert!(backlog.len() <= 3);
        assert!(backlog.iter().all(|notice| notice.payload.is_empty() && notice.claim.is_none()), "notices are head and key only");
        seen.extend(backlog.iter().filter_map(|notice| notice.key));
        after = backlog.last().and_then(|notice| notice.key);
        if !more {
            break;
        }
    }
    assert_eq!(seen, keys);
    assert_eq!(state.subscribers["work"], vec![B], "subscribing again does not add a second subscription");
}

#[test]
fn a_completion_that_arrives_before_the_reclaim_is_accepted_and_after_it_is_refused() {
    // Complete, then register: accepted.
    let mut state = fresh();
    let (id, _) = register(&mut state, EventId::NIL);
    let event = claim(&mut state, A, id, 10).unwrap();
    state.begin(1000);
    assert_eq!(complete(&mut state, A, id, token(&event), Vec::new()), Reply::Completed);
    register(&mut state, EventId::NIL);
    // Register, then complete: the reclaim came first.
    let mut state = fresh();
    let (id, _) = register(&mut state, EventId::NIL);
    let event = claim(&mut state, A, id, 10).unwrap();
    state.begin(1000);
    register(&mut state, EventId::NIL);
    assert_eq!(complete(&mut state, A, id, token(&event), Vec::new()), Reply::Refused(Refusal::Missing));
}

#[test]
fn grants_limit_methods_and_channels_and_delegation() {
    let mut grants = Grants::new();
    grants.insert("svc".into(), Grant { methods: vec![Method::RegisterDispatch, Method::Subscribe], channels: vec![Pattern("pub.*".into())], delegate: false });
    let mut state = started(EngineConfig { grants: Some(grants), ..EngineConfig::default() }, Box::new(V7));
    assert_eq!(reply(&mut state, A, Call::RegisterClaimable(Envelope::claimable("pub.x", "t", Bytes::new()))), Reply::Refused(Refusal::Forbidden));
    assert!(matches!(reply(&mut state, A, Call::RegisterDispatch(Envelope::dispatch("pub.x", "t", Bytes::new()))), Reply::Accepted { .. }));
    assert_eq!(reply(&mut state, A, Call::RegisterDispatch(Envelope::dispatch("priv", "t", Bytes::new()))), Reply::Refused(Refusal::Forbidden));
    assert_eq!(reply(&mut state, A, Call::RegisterDispatch(Envelope::dispatch("pub.x", "t", Bytes::new()).with_delegate("alice"))), Reply::Refused(Refusal::Forbidden));
    assert_eq!(reply(&mut state, OTHER, Call::Subscribe { channel: "pub.x".into(), after: None }), Reply::Refused(Refusal::Forbidden), "no grant, no access");
}

#[test]
fn requests_carrying_server_fields_or_the_wrong_kind_are_malformed() {
    let mut state = fresh();
    let base = || Envelope::claimable("work", "do", Bytes::new());
    for bad in [Envelope { retry: 1, ..base() }, Envelope { principal: Some("me".into()), ..base() }, Envelope { key: Some(WorkKey::default()), ..base() }, Envelope::claimable("", "do", Bytes::new())]
    {
        assert_eq!(reply(&mut state, A, Call::RegisterClaimable(bad)), Reply::Refused(Refusal::Malformed));
    }
    assert_eq!(reply(&mut state, A, Call::RegisterDispatch(base())), Reply::Refused(Refusal::Malformed));
    assert_eq!(reply(&mut state, A, Call::RegisterClaimable(base().with_id(EventId([0xff; 16])))), Reply::Refused(Refusal::InvalidId));
}

#[test]
fn recovery_restores_order_and_reissues_what_was_claimed_with_one_more_retry() {
    let mut state = fresh();
    let keys: Vec<WorkKey> = (0..3).map(|_| register(&mut state, EventId::NIL).1).collect();
    let claimed = the_id_now(&state, keys[1]);
    let event = claim(&mut state, A, claimed, 1000).unwrap();
    let records = state.snapshot();

    let mut restarted = State::new(&EngineConfig::default(), Box::new(V7));
    let startup = restarted.recover(records).unwrap();
    assert_eq!(restarted.generation, state.generation + 1);
    assert!(matches!(startup.as_slice(), [Record::Generation(_), Record::Reclaim { retry: 1, .. }]));
    assert_eq!(restarted.queue.keys().copied().collect::<Vec<_>>(), keys, "same order, same keys");
    assert_ne!(restarted.queue[&keys[1]].id, claimed);
    assert!(restarted.queue.values().all(|event| event.claim.is_none()));
    restarted.open(A, &Name::new("svc"));
    restarted.begin(0);
    let _ = restarted.take();
    assert_eq!(complete(&mut restarted, A, claimed, token(&event), Vec::new()), Reply::Refused(Refusal::Missing), "a report from before the restart has no effect");
    // A later registration queues behind the recovered ones.
    assert!(register(&mut restarted, EventId::NIL).1 > keys[2]);
}

#[test]
fn a_full_snapshot_replays_to_the_same_queue_as_the_records_it_replaced() {
    let mut state = fresh();
    let mut log = Vec::new();
    let mut ids = Vec::new();
    for step in 0..40u64 {
        state.begin(step * 10);
        let (outcome, effects) = run(&mut state, A, Call::RegisterClaimable(Envelope::claimable("work", "do", Bytes::from_static(b"p"))));
        log.extend(effects.records);
        let Reply::Registered { id, .. } = outcome else { panic!("{outcome:?}") };
        ids.push(id);
        if step % 3 == 0 {
            let (_, effects) = run(&mut state, A, Call::Claim { id, worker: 1, lease_ms: 15 });
            log.extend(effects.records);
        }
        if step % 5 == 0 {
            let (_, effects) = run(&mut state, A, Call::Delete { id: ids[step as usize / 2], result: "out".into(), tag: id, key: None });
            log.extend(effects.records);
        }
    }
    let mut replayed = State::new(&EngineConfig::default(), Box::new(V7));
    replayed.recover(std::iter::once(Record::Generation(state.generation)).chain(log).collect()).unwrap();
    let mut folded = State::new(&EngineConfig::default(), Box::new(V7));
    folded.recover(state.snapshot()).unwrap();
    let view = |state: &State| state.queue.iter().map(|(key, event)| (*key, event.retry, event.payload.clone())).collect::<Vec<_>>();
    assert_eq!(view(&replayed), view(&folded));
}

#[test]
fn one_off_reply_channels_leave_nothing_behind() {
    let mut state = fresh();
    for index in 0..5000 {
        let channel = format!("reply.{index}");
        let _ = reply(&mut state, B, Call::Subscribe { channel: channel.as_str().into(), after: None });
        let (id, _) = match reply(&mut state, A, Call::RegisterClaimable(Envelope::claimable(channel.as_str(), "t", Bytes::new()))) {
            Reply::Registered { id, key } => (id, key),
            other => panic!("{other:?}"),
        };
        let event = claim(&mut state, A, id, 100).unwrap();
        complete(&mut state, A, id, token(&event), Vec::new());
        let _ = reply(&mut state, B, Call::Unsubscribe { channel: channel.as_str().into() });
    }
    assert!(state.waiting.is_empty() && state.subscribers.is_empty());
    assert!(state.names.len() <= 2 * 8 + 1024, "the dictionary stays bounded: {}", state.names.len());
}

// ── the reference model ─────────────────────────────────────────────────

/// The lifecycle as the proposal states it, written as plainly as possible,
/// to compare the state machine against.
#[derive(Default, Clone)]
struct Model {
    events: Vec<Modeled>,
}

#[derive(Clone)]
struct Modeled {
    id: EventId,
    retry: u16,
    /// Claim token and deadline while claimed.
    claim: Option<(ClaimToken, u64)>,
}

#[derive(Clone, Debug)]
enum Step {
    Register,
    Claim(usize, u64),
    Complete(usize, bool),
    Delete(usize),
    Advance(u64),
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        Just(Step::Register),
        (0..8usize, 1..50u64).prop_map(|(at, lease)| Step::Claim(at, lease)),
        (0..8usize, any::<bool>()).prop_map(|(at, stale)| Step::Complete(at, stale)),
        (0..8usize).prop_map(Step::Delete),
        (1..40u64).prop_map(Step::Advance),
    ]
}

proptest! {
    /// Random sequences of the methods give the same answers and the same
    /// queue (order, retry counts, claimed or not) as the reference model.
    #[test]
    fn the_state_machine_agrees_with_the_reference_model(steps in proptest::collection::vec(step(), 1..80)) {
        let mut state = fresh();
        let mut model = Model::default();
        let mut now = 0u64;
            let mut known: Vec<EventId> = Vec::new();
        for step in steps {
            state.begin(now);
            match step {
                Step::Register => {
                    // The model reclaims first, like the server, then queues.
                    for event in model.events.iter_mut() {
                        if event.claim.is_some_and(|(_, deadline)| deadline <= now) {
                            event.claim = None;
                            event.retry = event.retry.saturating_add(1);
                        }
                    }
                    let (id, _) = register(&mut state, EventId::NIL);
                    model.events.push(Modeled { id, retry: 0, claim: None });
                    // Ids changed by reclaims are read back by work-key order.
                    for (event, (_, current)) in model.events.iter_mut().zip(state.queue.iter()) {
                        event.id = current.id;
                    }
                    known.push(id);
                }
                Step::Claim(at, lease) => {
                    let Some(&target) = model.events.get(at).map(|event| &event.id) else { continue };
                    let expected_free = model.events[at].claim.is_none();
                    let outcome = claim(&mut state, A, target, lease);
                    prop_assert_eq!(outcome.is_ok(), expected_free);
                    if let Ok(event) = outcome {
                        model.events[at].claim = Some((token(&event), now + lease));
                    }
                }
                Step::Complete(at, stale) => {
                    let Some(event) = model.events.get(at).cloned() else { continue };
                    let Some((mut claimed, _)) = event.claim else {
                        prop_assert_eq!(complete(&mut state, A, event.id, ClaimToken::default(), Vec::new()), Reply::Refused(Refusal::NotClaimed));
                        continue;
                    };
                    if stale {
                        claimed.seq ^= 1;
                    }
                    let outcome = complete(&mut state, A, event.id, claimed, Vec::new());
                    if stale {
                        prop_assert_eq!(outcome, Reply::Refused(Refusal::StaleToken));
                    } else {
                        prop_assert_eq!(outcome, Reply::Completed);
                        model.events.remove(at);
                    }
                }
                Step::Delete(at) => {
                    let Some(event) = model.events.get(at).cloned() else { continue };
                    let outcome = reply(&mut state, A, Call::Delete { id: event.id, result: "out".into(), tag: event.id, key: None });
                    prop_assert_eq!(outcome, Reply::Deleted { removed: event.claim.is_none() });
                    if event.claim.is_none() {
                        model.events.remove(at);
                    }
                }
                Step::Advance(by) => now += by,
            }
            let actual: Vec<(EventId, u16, bool)> = state.queue.values().map(|event| (event.id, event.retry, event.claim.is_some())).collect();
            let expected: Vec<(EventId, u16, bool)> = model.events.iter().map(|event| (event.id, event.retry, event.claim.is_some())).collect();
            prop_assert_eq!(actual, expected);
            prop_assert_eq!(state.ids.len(), state.queue.len());
        }
        let _ = known;
    }
}
