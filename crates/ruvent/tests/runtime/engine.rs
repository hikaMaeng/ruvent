//! The seven methods through the in-process client, against a running engine.

use crate::support::*;
use ruvent::{connect, Bytes, Call, Engine, EngineConfig, Envelope, EventId, Incoming, Methods, Outbound, Refusal, Reply, DELETED};
use std::time::Duration;

/// Each of the seven methods, called through the contract, does what it says.
#[tokio::test]
async fn the_seven_methods_work_through_the_in_process_client() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let (producer, _) = connect(&engine, "svc", 64).await.unwrap();
    let (worker, mut notices) = connect(&engine, "svc", 64).await.unwrap();

    assert!(worker.subscribe("work".into(), None).await.unwrap().notices.is_empty()); // 6
    let (id, key) = producer.register_claimable(work("work")).await.unwrap(); // 1
    let Some(Incoming::Notice(notice)) = next_event(&mut notices).await else { panic!("no notice") };
    assert_eq!((notice.id, notice.key), (id, Some(key)));
    let claimed = worker.claim(id, 3, Duration::from_secs(30)).await.unwrap(); // 3
    worker.complete(id, claimed.claim.unwrap().token, vec![Envelope::dispatch("work", "done", Bytes::new())]).await.unwrap(); // 4
    assert!(matches!(next_event(&mut notices).await, Some(Incoming::Delivery(event)) if &*event.event_type == "done"));
    producer.register_dispatch(Envelope::dispatch("work", "fyi", Bytes::new())).await.unwrap(); // 2
    assert!(matches!(next_event(&mut notices).await, Some(Incoming::Delivery(event)) if &*event.event_type == "fyi"));
    let (doomed, _) = producer.register_claimable(work("work")).await.unwrap();
    assert!(matches!(next_event(&mut notices).await, Some(Incoming::Notice(_))));
    assert!(producer.delete(doomed, "work".into(), EventId::new_v7(), None).await.unwrap()); // 5
    assert!(matches!(next_event(&mut notices).await, Some(Incoming::Delivery(event)) if &*event.event_type == DELETED));
    worker.unsubscribe("work".into()).await.unwrap(); // 7
    producer.register_dispatch(Envelope::dispatch("work", "unheard", Bytes::new())).await.unwrap();
    assert!(stays_quiet(&mut notices).await);
}

/// A missed deadline is noticed at the next registration, with no timer:
/// retry + 1, a new id, and the old completion has no effect.
#[tokio::test(start_paused = true)]
async fn a_missed_deadline_is_reclaimed_by_the_next_registration() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let (client, mut inbox) = connect(&engine, "svc", 64).await.unwrap();
    client.subscribe("work".into(), None).await.unwrap();
    let (id, key) = client.register_claimable(work("work")).await.unwrap();
    let _ = next_event(&mut inbox).await;
    let claimed = client.claim(id, 1, Duration::from_secs(1)).await.unwrap();
    tokio::time::advance(Duration::from_secs(5)).await;
    assert!(stays_quiet(&mut inbox).await, "no registration, no reclaim");
    client.register_dispatch(Envelope::dispatch("elsewhere", "t", Bytes::new())).await.unwrap();
    let Some(Incoming::Notice(again)) = next_event(&mut inbox).await else { panic!("no renewed notice") };
    assert_eq!((again.key, again.retry), (Some(key), 1));
    assert_ne!(again.id, id);
    assert_eq!(client.complete(id, claimed.claim.unwrap().token, Vec::new()).await, Err(Refusal::Missing));
    assert!(client.claim(again.id, 1, Duration::from_secs(1)).await.is_ok());
}

/// Subscribe → dispatch A → unsubscribe → dispatch B, in one batch: only A arrives.
#[tokio::test]
async fn subscriptions_take_effect_in_processing_order_within_a_batch() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let (session, mut out) = engine.open("svc").await.unwrap();
    session.call(1, Call::Subscribe { channel: "feed".into(), after: None }).await.unwrap();
    session.call(2, Call::RegisterDispatch(Envelope::dispatch("feed", "A", Bytes::new()))).await.unwrap();
    session.call(3, Call::Unsubscribe { channel: "feed".into() }).await.unwrap();
    session.call(4, Call::RegisterDispatch(Envelope::dispatch("feed", "B", Bytes::new()))).await.unwrap();
    let mut seen = Vec::new();
    while seen.len() < 5 {
        match out.recv().await.unwrap() {
            Outbound::Delivery(event) => seen.push(event.event_type.to_string()),
            Outbound::Reply { req, .. } => seen.push(format!("reply {req}")),
            Outbound::Notice(_) => panic!("no claimable work here"),
        }
    }
    assert_eq!(seen, ["reply 1", "A", "reply 2", "reply 3", "reply 4"]);
}

/// The subscription marker reaches the inbox before the subscribe call
/// returns, after everything sent before the subscription existed.
#[tokio::test]
async fn the_subscription_marker_precedes_the_answer() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let (client, mut inbox) = connect(&engine, "svc", 64).await.unwrap();
    client.subscribe("feed".into(), None).await.unwrap();
    assert_eq!(inbox.try_recv_marker(), Some(Incoming::Subscribed("feed".into())));
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
    let (client, _) = connect(&engine, "svc", 64).await.unwrap();
    engine.shutdown().await.unwrap();
    assert_eq!(client.register_dispatch(Envelope::dispatch("x", "t", Bytes::new())).await, Err(Refusal::Closed));
    assert!(engine.stopped().await.is_ok());
}

/// A claim's answer is the envelope as it was at the claim, even when the
/// event is reclaimed and claimed again before the claimer reads it.
#[tokio::test(start_paused = true)]
async fn a_claim_answer_keeps_the_values_it_had_when_it_was_made() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let (slow, mut slow_out) = engine.open("svc").await.unwrap();
    let (client, _) = connect(&engine, "svc", 64).await.unwrap();
    let (id, _) = client.register_claimable(work("work")).await.unwrap();
    slow.call(1, Call::Claim { id, worker: 1, lease_ms: 10 }).await.unwrap();
    // A round trip behind it: the inbox is first in, first out, so the claim is applied.
    client.unsubscribe("barrier".into()).await.unwrap();
    // The slow claimer has not read its answer; the claim expires and is reclaimed and reclaimed.
    tokio::time::advance(Duration::from_millis(50)).await;
    client.register_dispatch(Envelope::dispatch("x", "t", Bytes::new())).await.unwrap();
    let backlog = client.subscribe("work".into(), None).await.unwrap();
    let renewed = backlog.notices[0].id;
    let second = client.claim(renewed, 2, Duration::from_secs(10)).await.unwrap();
    let Some(Outbound::Reply { reply: Reply::Claimed(first), .. }) = slow_out.recv().await else { panic!() };
    assert_eq!(first.id, id);
    assert_eq!(first.claim.unwrap().worker, 1);
    assert_ne!(first.claim.unwrap().token, second.claim.unwrap().token);
}
