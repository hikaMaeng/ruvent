//! Queues, ownership, reactions, subscriptions and retention, against a
//! volatile engine with the clock under test control.

use ruvent::{Audience, Begin, Bytes, Command, Emit, Engine, EngineConfig, Envelope, EventId, From, Kind, Outcome, Retention};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

const VISIBILITY: Duration = Duration::from_secs(60);
const NOW: Option<Duration> = Some(Duration::ZERO);

fn command(key: &str, reply: &str, payload: &'static str) -> Command {
    Command::new("job.run", "requests", payload).transaction_key(key).reply_to(reply)
}

fn actions(events: &[Arc<Envelope>]) -> Vec<&str> {
    events.iter().map(|event| &*event.action).collect()
}

fn read(engine: &Engine, channel: &str) -> Vec<Arc<Envelope>> {
    engine.subscribe(vec![channel.to_owned()], From::Start).poll(100)
}

#[test]
fn derived_ids_are_stable_and_do_not_run_parts_together() {
    assert_eq!(EventId::derive(&[b"fact", b"session:a", b"k"]), EventId::derive(&[b"fact", b"session:a", b"k"]));
    assert_ne!(EventId::derive(&[b"ab", b"c"]), EventId::derive(&[b"a", b"bc"]), "parts are length-delimited");
    let fact = EventId::random();
    assert_ne!(EventId::reaction("billing", fact), EventId::reaction("shipping", fact), "each reactor is issued its own event");
}

#[tokio::test(start_paused = true)]
async fn a_visibility_deadline_returns_an_unfinished_message_by_itself() {
    let engine = Engine::volatile(EngineConfig::default());
    engine.submit("q", command("t1", "r", "{}")).await.unwrap();

    let first = engine.dequeue("q", 10, Some(VISIBILITY), NOW).await.unwrap();
    assert_eq!((first.len(), first[0].read_count), (1, 1));
    assert!(engine.dequeue("q", 10, Some(VISIBILITY), NOW).await.unwrap().is_empty(), "an in-flight message must not be handed out again");

    // A consumer parked with no limit on its patience is woken by the deadline itself.
    let parked = engine.dequeue("q", 10, Some(VISIBILITY), None).await.unwrap();
    assert_eq!((parked[0].id, parked[0].read_count), (first[0].id, 2));

    assert!(engine.ack("q", parked[0].id).await.unwrap());
    tokio::time::sleep(VISIBILITY * 2).await;
    assert!(engine.dequeue("q", 10, Some(VISIBILITY), NOW).await.unwrap().is_empty(), "an acknowledged message must never come back");
}

#[tokio::test(start_paused = true)]
async fn a_held_message_comes_back_only_when_it_is_given_back() {
    let engine = Engine::volatile(EngineConfig::default());
    engine.submit("q", command("t1", "r", "{}")).await.unwrap();
    let held = engine.dequeue("q", 1, None, NOW).await.unwrap().remove(0);

    tokio::time::sleep(Duration::from_secs(86_400)).await;
    assert!(engine.dequeue("q", 1, None, NOW).await.unwrap().is_empty(), "with no deadline, time alone returns nothing");

    // Given back with a delay: invisible until the delay has passed, then delivered to whoever is parked.
    assert!(engine.nack("q", held.id, Duration::from_secs(30)).await.unwrap());
    assert!(engine.dequeue("q", 1, None, Some(Duration::from_secs(29))).await.unwrap().is_empty());
    assert_eq!(engine.dequeue("q", 1, None, None).await.unwrap()[0].id, held.id);

    // Given back at once.
    assert!(engine.nack("q", held.id, Duration::ZERO).await.unwrap());
    assert_eq!(engine.dequeue("q", 1, None, NOW).await.unwrap()[0].read_count, 3);
}

#[tokio::test(start_paused = true)]
async fn an_idle_engine_and_its_parked_consumers_do_not_wake() {
    let engine = Engine::volatile(EngineConfig::default());
    let parked: Vec<_> = (0..8)
        .map(|index| {
            let engine = engine.clone();
            tokio::spawn(async move { engine.dequeue(format!("q{index}"), 1, None, None).await.unwrap() })
        })
        .collect();
    tokio::task::yield_now().await;
    engine.flush().await.unwrap();
    let before = engine.stats().wakeups;

    tokio::time::sleep(Duration::from_secs(3600)).await;
    assert_eq!(engine.stats().wakeups, before, "an hour with nothing to do must cost no wakeups");

    // And they are woken by the event, not by a later poll.
    engine.submit("q3", command("t1", "r", "{}")).await.unwrap();
    for (index, consumer) in parked.into_iter().enumerate() {
        if index == 3 {
            assert_eq!(consumer.await.unwrap().len(), 1);
        } else {
            consumer.abort();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_consumers_never_share_a_message() {
    const MESSAGES: usize = 20_000;
    const CONSUMERS: usize = 16;
    let engine = Engine::volatile(EngineConfig::default());
    for chunk in 0..20 {
        let engine = engine.clone();
        tokio::spawn(async move {
            for index in 0..MESSAGES / 20 {
                engine.submit("q", command(&format!("t{chunk}-{index}"), "r", "{}")).await.unwrap();
            }
        });
    }

    let seen: Arc<Vec<AtomicU32>> = Arc::new((0..=MESSAGES).map(|_| AtomicU32::new(0)).collect());
    let acked = Arc::new(AtomicUsize::new(0));
    let mut consumers = Vec::new();
    for _ in 0..CONSUMERS {
        let (engine, seen, acked) = (engine.clone(), seen.clone(), acked.clone());
        consumers.push(tokio::spawn(async move {
            while acked.load(Ordering::SeqCst) < MESSAGES {
                for delivery in engine.dequeue("q", 32, None, Some(Duration::from_millis(20))).await.unwrap() {
                    seen[delivery.id as usize].fetch_add(1, Ordering::SeqCst);
                    assert!(engine.ack("q", delivery.id).await.unwrap());
                    acked.fetch_add(1, Ordering::SeqCst);
                }
            }
        }));
    }
    for consumer in consumers {
        consumer.await.unwrap();
    }
    // Ids are issued 1..=MESSAGES; every one was delivered exactly once.
    let deliveries: Vec<u32> = seen.iter().skip(1).map(|count| count.load(Ordering::SeqCst)).collect();
    assert!(deliveries.iter().all(|count| *count == 1), "max deliveries of one message: {:?}", deliveries.iter().max());
    assert_eq!(engine.stats().delivered, MESSAGES as u64);
}

#[tokio::test(start_paused = true)]
async fn one_event_is_run_by_one_worker_and_answered_once() {
    let engine = Engine::volatile(EngineConfig::default());
    engine.submit("q", command("t1", "alice", r#"{"n":1}"#)).await.unwrap();
    let first = engine.dequeue("q", 1, None, NOW).await.unwrap().remove(0);
    let Begin::Claimed { command: claimed, attempt, attempts } = engine.begin("q", first.id).await.unwrap() else { panic!("the first submission must be claimed") };
    assert_eq!((claimed.kind, attempts), (Kind::Command, 1));

    // The same event sent again while it runs is dropped. It does not attach
    // itself to the execution and it is not answered separately.
    engine.submit("q", command("t1", "bob", r#"{"n":1}"#)).await.unwrap();
    let second = engine.dequeue("q", 1, None, NOW).await.unwrap().remove(0);
    assert!(matches!(engine.begin("q", second.id).await.unwrap(), Begin::Duplicate));

    assert!(engine.finish(claimed.clone(), attempt, Outcome::Ok(Bytes::from_static(b"\"done\"")), "q", first.id).await.unwrap());
    assert!(!engine.finish(claimed.clone(), attempt, Outcome::Ok(Bytes::new()), "q", first.id).await.unwrap(), "a finished execution cannot be finished again");

    let answered = read(&engine, "alice");
    assert_eq!(actions(&answered), ["job.run.result"]);
    assert_eq!(&answered[0].payload[..], b"\"done\"");
    assert!(read(&engine, "bob").is_empty(), "one event has one result");

    // After it finished, the same event sent again is told the stored result…
    engine.submit("q", command("t1", "carol", r#"{"n":1}"#)).await.unwrap();
    let late = engine.dequeue("q", 1, None, NOW).await.unwrap().remove(0);
    assert!(matches!(engine.begin("q", late.id).await.unwrap(), Begin::Settled));
    assert_eq!(actions(&read(&engine, "carol")), ["job.run.result"]);

    // …and the same key with different content is a conflict, not a rerun.
    engine.submit("q", command("t1", "dave", r#"{"n":2}"#)).await.unwrap();
    let other = engine.dequeue("q", 1, None, NOW).await.unwrap().remove(0);
    assert!(matches!(engine.begin("q", other.id).await.unwrap(), Begin::Conflict));
    assert_eq!(actions(&read(&engine, "dave")), ["job.run.conflict"]);

    assert!(engine.dequeue("q", 10, None, NOW).await.unwrap().is_empty(), "every message above was settled and deleted");
}

#[tokio::test(start_paused = true)]
async fn a_superseded_attempt_cannot_write_and_attempts_are_bounded() {
    let engine = Engine::volatile(EngineConfig { max_attempts: 2, ..EngineConfig::default() });
    engine.submit("q", command("t1", "r", "{}")).await.unwrap();
    let say = |action: &str| Emit { action: action.into(), payload: Bytes::new(), kind: Kind::Progress, key: None, audience: Audience::Client };

    let first = engine.dequeue("q", 1, None, NOW).await.unwrap().remove(0);
    let Begin::Claimed { command: claimed, attempt: stale, .. } = engine.begin("q", first.id).await.unwrap() else { panic!() };
    assert!(engine.owns("t1", stale).await.unwrap());

    // Given up for a retry in ten seconds: not before, and to whoever is waiting.
    assert!(engine.release("t1", stale, "q", first.id, Duration::from_secs(10)).await.unwrap());
    assert!(engine.dequeue("q", 1, None, Some(Duration::from_secs(9))).await.unwrap().is_empty());
    let retried = engine.dequeue("q", 1, None, None).await.unwrap().remove(0);
    let Begin::Claimed { attempt: current, attempts, .. } = engine.begin("q", retried.id).await.unwrap() else { panic!("a released execution must be runnable again") };
    assert_eq!(attempts, 2);

    // The first attempt can no longer speak, finish or release.
    assert!(!engine.owns("t1", stale).await.unwrap());
    assert!(!engine.emit(claimed.clone(), stale, say("stale")).await.unwrap());
    assert!(!engine.finish(claimed.clone(), stale, Outcome::Ok(Bytes::new()), "q", first.id).await.unwrap());
    assert!(!engine.release("t1", stale, "q", first.id, Duration::ZERO).await.unwrap());
    assert!(engine.emit(claimed.clone(), current, say("current")).await.unwrap());

    // The second attempt fails too. With two attempts allowed, that is the end.
    assert!(engine.release("t1", current, "q", retried.id, Duration::ZERO).await.unwrap());
    let last = engine.dequeue("q", 1, None, NOW).await.unwrap().remove(0);
    assert!(matches!(engine.begin("q", last.id).await.unwrap(), Begin::Exhausted));
    assert_eq!(engine.dead_letters("q").await.unwrap().len(), 1);
    assert!(engine.dequeue("q", 1, None, NOW).await.unwrap().is_empty());

    let events = read(&engine, "r");
    assert_eq!(actions(&events), ["current", "job.run.failure"]);
    assert_eq!(&events[1].payload[..], b"processing_permanent_failure");
}

#[tokio::test(start_paused = true)]
async fn each_reactor_is_issued_its_own_event_in_the_same_step() {
    let reactions = HashMap::from([("order.placed".to_owned(), vec!["billing".to_owned(), "shipping".to_owned()])]);
    let engine = Engine::volatile(EngineConfig { reactions, ..EngineConfig::default() });

    let fact = Envelope::draft(Kind::Fact, "order.placed", "orders", r#"{"id":7}"#);
    let stored = engine.append(vec![fact.clone()]).await.unwrap().remove(0);
    assert_eq!(&*stored.stream, "channel:orders");

    let mut issued = Vec::new();
    for queue in ["billing", "shipping"] {
        let reaction = engine.dequeue(queue, 10, None, NOW).await.unwrap();
        assert_eq!(reaction.len(), 1, "one event on {queue}");
        let work = &reaction[0].command;
        assert_eq!(work.event_id, EventId::reaction(queue, stored.event_id));
        assert_eq!((&*work.action, work.audience, &work.payload[..]), ("order.placed", Audience::Worker, &br#"{"id":7}"#[..]));
        issued.push(work.transaction_key.clone());
        assert!(engine.ack(queue, reaction[0].id).await.unwrap());
    }
    assert_ne!(issued[0], issued[1], "two reactors, two events with their own lifecycles");

    // Recording the same fact again stores nothing and issues nothing.
    engine.append(vec![fact]).await.unwrap();
    assert!(engine.dequeue("billing", 10, None, NOW).await.unwrap().is_empty());
    assert_eq!((engine.stats().events, engine.stats().duplicates, engine.stats().enqueued), (1, 1, 2), "one fact, one duplicate, one message per reactor");
}

#[tokio::test(start_paused = true)]
async fn a_subscription_resumes_from_its_cursor_and_never_sees_worker_traffic() {
    let engine = Engine::volatile(EngineConfig::default());
    let event = |action: &str, channel: &str| Envelope::draft(Kind::Fact, action, channel, "{}");
    engine.append(vec![event("a1", "a"), event("b1", "b"), event("a2", "a")]).await.unwrap();

    let mut history = engine.subscribe(vec!["a".to_owned(), "b".to_owned()], From::Start);
    let mut live = engine.subscribe(vec!["a".to_owned(), "b".to_owned()], From::Now);
    let first = history.poll(2);
    assert_eq!(actions(&first), ["a1", "b1"], "channels are merged in commit order");
    assert!(live.poll(10).is_empty(), "From::Now starts after what already exists");

    let mut hidden = event("secret", "a");
    hidden.audience = Audience::Worker;
    engine.append(vec![hidden, event("b2", "b")]).await.unwrap();

    // A new subscription from the saved cursor continues exactly where the old one stopped.
    let mut resumed = engine.subscribe(vec!["a".to_owned(), "b".to_owned()], From::Position(history.cursor()));
    assert_eq!(actions(&resumed.poll(10)), ["a2", "b2"]);
    assert!(!resumed.lagged());
    assert_eq!(actions(&live.next(10).await), ["b2"]);
    assert_eq!(actions(&engine.read_stream("channel:a", 0, 10)), ["a1", "a2", "secret"], "a stream read sees worker traffic; a subscription does not");
}

#[tokio::test(start_paused = true)]
async fn the_log_is_bounded_as_it_grows_and_a_reader_left_behind_is_told() {
    let retention = Retention { max_events: 4, ..Retention::default() };
    let engine = Engine::volatile(EngineConfig { retention, ..EngineConfig::default() });
    let event = |index: usize| Envelope::draft(Kind::Fact, format!("e{index}"), "room", "{}");

    let first = engine.append(vec![event(0)]).await.unwrap().remove(0);
    let slow = engine.subscribe(vec!["room".to_owned()], From::Position(first.position));
    engine.append((1..10).map(event).collect()).await.unwrap();

    assert_eq!(engine.stats().retained_events, 4);
    assert_eq!(actions(&read(&engine, "room")), ["e6", "e7", "e8", "e9"], "the oldest events are dropped as newer ones arrive");
    assert_eq!(engine.read_stream("channel:room", 0, 100).len(), 4, "the stream index is bounded with it");

    let mut slow = slow;
    assert_eq!(actions(&slow.poll(100)), ["e6", "e7", "e8", "e9"]);
    assert!(slow.lagged(), "events 1 to 5 were dropped before this reader got to them");

    // Age is applied when the next event arrives, not by a sweep.
    tokio::time::sleep(Retention::default().max_age + Duration::from_secs(1)).await;
    assert_eq!(engine.stats().retained_events, 4);
    engine.append(vec![event(10)]).await.unwrap();
    assert_eq!(actions(&read(&engine, "room")), ["e10"]);
}

/// A stopped engine refuses every call, and the refusal says why.
#[tokio::test]
async fn a_stopped_engine_refuses_and_says_why() {
    let engine = Engine::volatile(EngineConfig::default());
    assert!(!engine.is_closed());
    engine.shutdown().await.unwrap();
    assert!(engine.is_closed());
    let refused = engine.submit("q", command("t1", "r", "{}")).await.unwrap_err();
    assert_eq!(refused.to_string(), "engine closed: engine has stopped");
    assert!(engine.dequeue("q", 1, None, NOW).await.is_err());
    assert!(engine.flush().await.is_err());
}

/// Streamed progress never makes a handler wait: when the inbox is full it
/// is dropped, and every drop — and only a drop — is counted.
#[tokio::test(start_paused = true)]
async fn progress_is_dropped_and_counted_only_when_the_inbox_is_full() {
    // On this single-threaded runtime the engine does not run until the test
    // yields, so the inbox fills after exactly its capacity.
    let engine = Engine::volatile(EngineConfig { inbox: 4, ..EngineConfig::default() });
    let command = Arc::new(Envelope::draft(Kind::Command, "job.run", "r", "{}"));
    let say = || Emit { action: "delta".into(), payload: Bytes::new(), kind: Kind::Progress, key: None, audience: Audience::Client };
    let sent: Vec<bool> = (0..10).map(|_| engine.emit_nowait(command.clone(), 1, say())).collect();
    assert_eq!(sent, [true, true, true, true, false, false, false, false, false, false]);
    assert_eq!(engine.stats().progress_dropped, 6);
}

/// A command submitted without a time is stamped by the engine; one with a
/// time keeps it.
#[tokio::test(start_paused = true)]
async fn a_command_without_a_time_is_stamped_on_submission() {
    // Read before the engine reads it: the engine's clock starts there.
    let wall = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
    let engine = Engine::volatile(EngineConfig::default());
    engine.submit("q", command("t1", "r", "{}")).await.unwrap();
    let mut dated = command("t2", "r", "{}");
    dated.created_at = 42;
    engine.submit("q", dated).await.unwrap();
    let delivered = engine.dequeue("q", 2, None, NOW).await.unwrap();
    assert!(delivered[0].command.created_at >= wall && delivered[0].command.created_at < wall + 60_000, "stamped with the engine's clock: {}", delivered[0].command.created_at);
    assert_eq!(delivered[1].command.created_at, 42);
}

/// A reader whose cursor is exactly the last event retention dropped has
/// read that event, so it has missed nothing; one before it has.
#[tokio::test(start_paused = true)]
async fn a_reader_at_the_last_dropped_event_has_missed_nothing() {
    let engine = Engine::volatile(EngineConfig { retention: Retention { max_events: 2, ..Retention::default() }, ..EngineConfig::default() });
    let event = |index: usize| Envelope::draft(Kind::Fact, format!("e{index}"), "room", "{}");
    let stored = engine.append((0..4).map(event).collect()).await.unwrap();
    // e0 and e1 were dropped; e2 and e3 remain.
    let at_last_dropped = engine.subscribe(vec!["room".to_owned()], From::Position(stored[1].position));
    assert!(!at_last_dropped.lagged(), "it read e1, and e2 onward is all there");
    let before_it = engine.subscribe(vec!["room".to_owned()], From::Position(stored[0].position));
    assert!(before_it.lagged(), "it never saw e1");
}

/// A reader waiting on a channel whose every event retention has dropped is
/// still woken by the next event on it.
#[tokio::test(start_paused = true)]
async fn a_reader_is_woken_on_a_channel_retention_emptied() {
    let engine = Engine::volatile(EngineConfig { retention: Retention { max_events: 1, ..Retention::default() }, ..EngineConfig::default() });
    engine.append(vec![Envelope::draft(Kind::Fact, "first", "a", "{}")]).await.unwrap();
    let mut reader = engine.subscribe(vec!["a".to_owned()], From::Now);
    let waiting = tokio::spawn(async move { reader.next(10).await });
    tokio::task::yield_now().await;

    // An event elsewhere pushes channel `a`'s only event out.
    engine.append(vec![Envelope::draft(Kind::Fact, "elsewhere", "b", "{}")]).await.unwrap();
    engine.append(vec![Envelope::draft(Kind::Fact, "second", "a", "{}")]).await.unwrap();
    let woken = tokio::time::timeout(Duration::from_secs(60), waiting).await.expect("the reader was never woken").unwrap();
    assert_eq!(actions(&woken), ["second"]);
}

/// Opening a subscription never unhooks one already waiting on the same
/// channel: every reader is woken by an event, however many there are.
#[tokio::test(start_paused = true)]
async fn a_new_reader_does_not_unhook_one_already_waiting() {
    let engine = Engine::volatile(EngineConfig::default());
    let mut first = engine.subscribe(vec!["a".to_owned()], From::Now);
    let waiting = tokio::spawn(async move { first.next(10).await });
    tokio::task::yield_now().await;

    let mut second = engine.subscribe(vec!["a".to_owned()], From::Now);
    engine.append(vec![Envelope::draft(Kind::Fact, "news", "a", "{}")]).await.unwrap();
    let woken = tokio::time::timeout(Duration::from_secs(60), waiting).await.expect("the first reader was unhooked").unwrap();
    assert_eq!(actions(&woken), ["news"]);
    assert_eq!(actions(&second.next(10).await), ["news"]);
}

/// The log is bounded by bytes as well as by count: an event costs its
/// payload plus a fixed 256 bytes, and the oldest go first once the total
/// would pass the bound.
#[tokio::test(start_paused = true)]
async fn the_log_is_bounded_by_bytes() {
    const EACH: u64 = 256 + 1_000;
    let retention = Retention { max_bytes: 3 * EACH as usize, ..Retention::default() };
    let engine = Engine::volatile(EngineConfig { retention, ..EngineConfig::default() });
    let event = |index: usize| Envelope::draft(Kind::Fact, format!("e{index}"), "room", vec![b'x'; 1_000]);
    engine.append((0..5).map(event).collect()).await.unwrap();
    let stats = engine.stats();
    assert_eq!((stats.retained_events, stats.retained_bytes), (3, 3 * EACH));
    assert_eq!(actions(&read(&engine, "room")), ["e2", "e3", "e4"]);
}

/// A settled execution is remembered for its window, so the same transaction
/// sent again is answered instead of run; after the window — once the next
/// execution settles — it is forgotten and would run again.
#[tokio::test(start_paused = true)]
async fn a_settled_execution_is_remembered_only_for_its_window() {
    let retention = Retention { settled: Duration::from_secs(1), ..Retention::default() };
    let engine = Engine::volatile(EngineConfig { retention, ..EngineConfig::default() });
    let run = |key: &'static str| {
        let engine = engine.clone();
        async move {
            engine.submit("q", command(key, "r", "{}")).await.unwrap();
            let delivery = engine.dequeue("q", 1, None, NOW).await.unwrap().remove(0);
            match engine.begin("q", delivery.id).await.unwrap() {
                Begin::Claimed { command, attempt, .. } => {
                    assert!(engine.finish(command, attempt, Outcome::Ok(Bytes::new()), "q", delivery.id).await.unwrap());
                    "ran"
                }
                Begin::Settled => "answered",
                other => panic!("{other:?}"),
            }
        }
    };
    assert_eq!(run("k0").await, "ran");
    assert_eq!(run("k0").await, "answered", "inside the window");
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(run("k1").await, "ran"); // a settle, which is when expiry is applied
    assert_eq!(run("k0").await, "ran", "the window has passed");
}
