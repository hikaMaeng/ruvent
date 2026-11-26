//! The operations through the in-process client, against a running engine.

use crate::support::*;
use ruvent::{connect, ByeReason, Bytes, Call, ConnectError, Engine, EngineConfig, Envelope, EventId, Hello, Incoming, Methods, Outbound, Refusal, Target, DELETED};
use std::time::Duration;

/// Production, work and observation, each through the contract, do what they say.
#[tokio::test]
async fn the_operations_work_through_the_in_process_client() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let (producer, _) = producer(&engine).await;
    let (worker, mut assignments) = worker(&engine).await;
    let (observer, mut watched) = observer(&engine).await;

    assert!(observer.subscribe(Target::Channel("feed".into()), None).await.unwrap().notices.is_empty());
    worker.join("work".into(), Vec::new(), Duration::from_secs(30), false).await.unwrap();
    worker.window("work".into(), 4, 0).await.unwrap();
    let (seq, id) = producer.register(work("work")).await.unwrap();
    producer.committed(seq).await.unwrap();
    let assignment = next_assignment(&mut assignments).await;
    assert_eq!((assignment.event.id, assignment.event.payload.as_ref()), (id, &b"p"[..]), "assignment, claim and delivery are one step");
    let done = worker.complete(id, assignment.token, vec![Envelope::dispatch("feed", "done", Bytes::new())]).await.unwrap();
    worker.committed(done).await.unwrap();
    assert!(matches!(next_event(&mut watched).await, Some(Incoming::Delivery(event)) if &*event.event_type == "done"));
    worker.register(Envelope::dispatch("feed", "chunk", Bytes::new())).await.unwrap();
    assert!(matches!(next_event(&mut watched).await, Some(Incoming::Delivery(event)) if &*event.event_type == "chunk"), "a worker emits on its work connection");

    observer.subscribe(Target::Channel("out".into()), None).await.unwrap();
    worker.window("work".into(), 0, 0).await.unwrap();
    let (_, doomed) = producer.register(work("work")).await.unwrap();
    let deleted = producer.delete(doomed, "out".into(), None).await.unwrap();
    producer.committed(deleted).await.unwrap();
    assert!(matches!(next_event(&mut watched).await, Some(Incoming::Delivery(event)) if &*event.event_type == DELETED));
    let stats = observer.stats().await.unwrap();
    assert_eq!((stats.waiting, stats.claimed), (0, 0));
    observer.unsubscribe(Target::Channel("feed".into())).await.unwrap();
    producer.register(Envelope::dispatch("feed", "unheard", Bytes::new())).await.unwrap();
    assert!(stays_quiet(&mut watched).await);
}

/// A lease end wakes the engine by itself: the wheel's one timer, no
/// registration needed (2.4).
#[tokio::test(start_paused = true)]
async fn a_lease_end_wakes_the_engine_without_any_registration() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let (producer, _) = producer(&engine).await;
    let (worker, mut assignments) = worker(&engine).await;
    worker.join("work".into(), Vec::new(), Duration::from_secs(1), false).await.unwrap();
    worker.window("work".into(), 1, 0).await.unwrap();
    let (_, id) = producer.register(work("work")).await.unwrap();
    let first = next_assignment(&mut assignments).await;
    tokio::time::advance(Duration::from_millis(1_100)).await;
    let (other, mut others) = connect(&engine, Hello::worker("svc"), 64).await.unwrap();
    other.join("work".into(), Vec::new(), Duration::from_secs(30), false).await.unwrap();
    other.window("work".into(), 1, 0).await.unwrap();
    let again = next_assignment(&mut others).await;
    assert_eq!((again.event.id, again.event.failures), (id, 1), "the same id, one failure counted");
    assert!(again.token > first.token);
    // Its window was closed by the reclaim: it is told work waits (2.4: a
    // `Pending` where it thought it had room means "send `Window` again"), and gets none.
    assert_eq!(next_event(&mut assignments).await, Some(Incoming::Pending("work".into())));
    assert!(stays_quiet(&mut assignments).await);
}

/// Subscribe → dispatch A → unsubscribe → dispatch B, in one batch: only A arrives.
#[tokio::test]
async fn subscriptions_take_effect_in_processing_order_within_a_batch() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let (watching, mut out) = engine.open(Hello::observer("svc")).await.unwrap();
    let (producing, _producing_out) = engine.open(Hello::producer("svc")).await.unwrap();
    assert!(matches!(out.recv().await, Some(Outbound::Welcome(_))));
    watching.call(Call::Subscribe { req: 1, target: Target::Channel("feed".into()), after: None }).await.unwrap();
    producing.call(Call::Register { seq: Some(1), event: Envelope::dispatch("feed", "A", Bytes::new()) }).await.unwrap();
    watching.call(Call::Unsubscribe { req: 2, target: Target::Channel("feed".into()) }).await.unwrap();
    producing.call(Call::Register { seq: Some(2), event: Envelope::dispatch("feed", "B", Bytes::new()) }).await.unwrap();
    let mut seen = Vec::new();
    while seen.len() < 3 {
        match out.recv().await.unwrap() {
            Outbound::Delivery(event) => seen.push(event.event_type.to_string()),
            Outbound::Subscribed { req, .. } | Outbound::Unsubscribed { req } => seen.push(format!("answer {req}")),
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(seen, ["answer 1", "A", "answer 2"]);
    assert!(tokio::time::timeout(QUIET, out.recv()).await.is_err(), "B went to nobody");
}

/// The subscription marker reaches the inbox before the subscribe call returns.
#[tokio::test]
async fn the_subscription_marker_precedes_the_answer() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let (client, mut inbox) = observer(&engine).await;
    client.subscribe(Target::Prefix("feed".into()), None).await.unwrap();
    assert_eq!(inbox.try_recv_marker(), Some(Incoming::Subscribed(Target::Prefix("feed".into()))));
}

trait MarkerProbe {
    fn try_recv_marker(&mut self) -> Option<Incoming>;
}

impl MarkerProbe for ruvent::Inbox {
    /// The marker is already in the inbox when `subscribe` returns, so this
    /// reads without waiting.
    fn try_recv_marker(&mut self) -> Option<Incoming> {
        let waker = std::task::Waker::noop();
        let mut context = std::task::Context::from_waker(waker);
        match self.poll_recv(&mut context) {
            std::task::Poll::Ready(message) => message,
            std::task::Poll::Pending => None,
        }
    }
}

/// After a shutdown every call fails as closed and `stopped` reports success.
#[tokio::test]
async fn after_shutdown_calls_fail_as_closed() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let (client, _) = observer(&engine).await;
    engine.shutdown().await.unwrap();
    assert_eq!(client.stats().await, Err(Refusal::Closed));
    assert!(engine.stopped().await.is_ok());
}

/// A worker's connection closing with assignments its owner never finished
/// writing gives them back at once, failure count unchanged (2.4, M8-2).
#[tokio::test]
async fn a_closing_connection_gives_back_unwritten_assignments_at_once() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let (producer, _) = producer(&engine).await;
    let (session, mut out) = engine.open(Hello::worker("svc")).await.unwrap();
    assert!(matches!(out.recv().await, Some(Outbound::Welcome(_))));
    session.call(Call::Join { req: 1, channel: "work".into(), types: Vec::new(), lease_ms: 30_000, dispatch: false }).await.unwrap();
    session.call(Call::Window { channel: "work".into(), count: 1, bytes: 0 }).await.unwrap();
    producer.register(work("work")).await.unwrap();
    let token = loop {
        if let Some(Outbound::Assign(assignment)) = out.recv().await {
            break assignment.token;
        }
    };
    session.close(vec![token]).await;
    let (other, mut others) = worker(&engine).await;
    other.join("work".into(), Vec::new(), Duration::from_secs(30), false).await.unwrap();
    other.window("work".into(), 1, 0).await.unwrap();
    assert_eq!(next_assignment(&mut others).await.event.failures, 0, "at once, not after the lease or the grace");
}

/// Another instance under the same producer id is told; the first gets
/// `Bye{superseded}`; the same process reconnecting is a quiet replacement,
/// so nobody keeps cutting anybody off (M8-7).
#[tokio::test]
async fn a_second_instance_under_one_producer_id_supersedes_the_first_and_both_are_told() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let hello = Hello::producer("svc");
    let (_first, mut first_inbox) = connect(&engine, hello.clone(), 64).await.unwrap();
    let other = Hello { nonce: EventId::new_v7().0, ..hello.clone() };
    let (second, mut previous) = connect(&engine, other.clone(), 64).await.unwrap();
    assert!(second.welcome().unwrap().superseded, "the newcomer is told it pushed someone out");
    assert_eq!(next_event(&mut first_inbox).await, Some(Incoming::Bye(ByeReason::Superseded)));
    let mut kept = vec![second];
    for _ in 0..3 {
        let (again, inbox) = connect(&engine, other.clone(), 64).await.unwrap();
        assert!(!again.welcome().unwrap().superseded);
        assert_eq!(next_event(&mut previous).await, Some(Incoming::Closed), "the old connection just ends, without a reason");
        kept.push(again);
        previous = inbox;
    }
    let (seq, _) = kept.last().unwrap().register(work("work")).await.unwrap();
    assert_eq!(seq, 1, "a fresh client numbers from one; nothing was applied under this id");
    let refused = connect(&engine, Hello { producer: None, ..hello }, 64).await.err();
    assert_eq!(refused, Some(ConnectError::Bye(ByeReason::Protocol)), "a production connection needs a producer id");
}
