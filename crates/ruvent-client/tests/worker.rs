//! The worker framework on the in-process link, against a real engine.
//!
//! Time is virtual (`start_paused`): sleeps and leases advance only when every
//! task is idle, so a test that deadlocks fails at its `within` limit instead of hanging.

use parking_lot::Mutex;
use ruvent::{connect, Bytes, Client, Engine, EngineConfig, Envelope, EventId, Inbox, Incoming, Methods, Refusal, WorkKey};
use ruvent_client::worker::{App, Context, Failure, Service, WorkerError};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};

const LIMIT: Duration = Duration::from_secs(600);

async fn within<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(LIMIT, future).await.expect("the scenario did not finish")
}

async fn engine() -> Engine {
    Engine::volatile(EngineConfig::default()).await
}

async fn open(engine: &Engine) -> (Client, Inbox) {
    connect(engine, "svc", 4096).await.expect("connect")
}

async fn next_delivery(inbox: &mut Inbox) -> Envelope {
    loop {
        match inbox.recv().await.expect("inbox open") {
            Incoming::Delivery(event) => return event,
            Incoming::Closed => panic!("connection closed"),
            _ => {}
        }
    }
}

/// Waits until the event is gone from the queue. Only call once a worker holds
/// the claim, or this would claim it itself.
async fn until_gone(client: &Client, id: EventId) {
    loop {
        match client.claim(id, 0, Duration::from_secs(1)).await {
            Err(Refusal::Missing) => return,
            Err(Refusal::AlreadyClaimed) => tokio::time::sleep(Duration::from_millis(1)).await,
            other => panic!("unexpected {other:?}"),
        }
    }
}

fn gate() -> (watch::Sender<bool>, watch::Receiver<bool>) {
    watch::channel(false)
}

async fn opened(mut gate: watch::Receiver<bool>) {
    let _ = gate.wait_for(|open| *open).await;
}

/// A handled event is removed from the queue, and the dispatch follow-up its
/// handler emitted reaches a subscriber, carrying the handler's payload.
#[tokio::test(start_paused = true)]
async fn a_handled_event_is_completed_and_its_follow_ups_registered() {
    let engine = engine().await;
    let (worker, inbox) = open(&engine).await;
    let running = App::new(worker, inbox)
        .service(Service::new("orders").on("create", |context: Context| async move {
            context.emit_dispatch("orders.done", "created", context.payload().clone());
            context.emit_claimable("billing", "charge", context.payload().clone());
            Ok(())
        }))
        .start()
        .await
        .unwrap();
    let (observer, mut seen) = open(&engine).await;
    observer.subscribe("orders.done".into(), None).await.unwrap();
    let (id, _) = observer.register_claimable(Envelope::claimable("orders", "create", Bytes::from_static(b"42"))).await.unwrap();
    let done = within(next_delivery(&mut seen)).await;
    assert_eq!((done.event_type.as_str(), &done.payload[..]), ("created", &b"42"[..]));
    assert_eq!(observer.claim(id, 0, Duration::from_secs(1)).await, Err(Refusal::Missing));
    let billing = observer.subscribe("billing".into(), None).await.unwrap();
    assert_eq!(billing.notices.len(), 1, "the claimable follow-up is queued");
    running.shutdown().await;
}

/// A failing handler still completes its event; a failure event is sent only
/// by the service that set `failures`, with the event id as correlation.
#[tokio::test(start_paused = true)]
async fn a_failed_handler_completes_the_event_and_reports_only_when_asked() {
    let engine = engine().await;
    let (worker, inbox) = open(&engine).await;
    let (invoked, mut invocations) = mpsc::unbounded_channel();
    let quiet = invoked.clone();
    let running = App::new(worker, inbox)
        .service(Service::new("loud").failures("failed").on("work", move |context: Context| {
            let invoked = invoked.clone();
            async move {
                let _ = invoked.send(context.id());
                Err(context.fail("boom"))
            }
        }))
        .service(Service::new("quiet").on("work", move |context: Context| {
            let invoked = quiet.clone();
            async move {
                let _ = invoked.send(context.id());
                Err(Failure::new("boom"))
            }
        }))
        .start()
        .await
        .unwrap();
    let (observer, mut seen) = open(&engine).await;
    observer.subscribe("failed".into(), None).await.unwrap();

    let (loud, _) = observer.register_claimable(Envelope::claimable("loud", "work", Bytes::new())).await.unwrap();
    let failure = within(next_delivery(&mut seen)).await;
    assert_eq!((failure.event_type.as_str(), &failure.payload[..], failure.correlation), ("work", &b"boom"[..], Some(loud)));
    within(until_gone(&observer, loud)).await;

    let (quiet, _) = observer.register_claimable(Envelope::claimable("quiet", "work", Bytes::new())).await.unwrap();
    within(async { while invocations.recv().await != Some(quiet) {} }).await;
    within(until_gone(&observer, quiet)).await;
    // Anything the quiet failure sent would arrive before this marker.
    let marker = observer.register_dispatch(Envelope::dispatch("failed", "marker", Bytes::new())).await.unwrap();
    assert_eq!(within(next_delivery(&mut seen)).await.id, marker);
    running.shutdown().await;
}

/// A panicking handler is a failure of that one execution: the event is
/// completed with a failure event, and the service keeps handling events.
#[tokio::test(start_paused = true)]
async fn a_panicking_handler_is_a_failure_not_a_dead_worker() {
    let engine = engine().await;
    let (worker, inbox) = open(&engine).await;
    let running = App::new(worker, inbox)
        .service(Service::new("work").failures("failed").permits(1).on("job", |context: Context| async move {
            if context.payload().as_ref() == b"panic" {
                panic!("handler bug");
            }
            context.emit_dispatch("done", "ok", Bytes::new());
            Ok(())
        }))
        .start()
        .await
        .unwrap();
    let (observer, mut seen) = open(&engine).await;
    observer.subscribe("failed".into(), None).await.unwrap();
    observer.subscribe("done".into(), None).await.unwrap();
    observer.register_claimable(Envelope::claimable("work", "job", Bytes::from_static(b"panic"))).await.unwrap();
    let failure = within(next_delivery(&mut seen)).await;
    assert_eq!(&failure.payload[..], b"handler panicked");
    observer.register_claimable(Envelope::claimable("work", "job", Bytes::new())).await.unwrap();
    assert_eq!(within(next_delivery(&mut seen)).await.channel.as_str(), "done");
    running.shutdown().await;
}

/// An event reclaimed more often than `max_retries` is completed without
/// running the handler, with a "retries exceeded" failure event.
#[tokio::test(start_paused = true)]
async fn an_event_past_its_retry_limit_is_completed_without_running_the_handler() {
    let engine = engine().await;
    let (observer, mut seen) = open(&engine).await;
    observer.subscribe("failed".into(), None).await.unwrap();
    let (id, key) = observer.register_claimable(Envelope::claimable("limited", "work", Bytes::new())).await.unwrap();
    observer.claim(id, 0, Duration::from_millis(10)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    // The next registration reclaims the expired claim: new id, retry 1.
    observer.register_claimable(Envelope::claimable("other", "work", Bytes::new())).await.unwrap();
    let backlog = observer.subscribe("limited".into(), None).await.unwrap();
    assert_eq!((backlog.notices[0].key, backlog.notices[0].retry), (Some(key), 1));
    observer.unsubscribe("limited".into()).await.unwrap();

    let runs = Arc::new(Mutex::new(0));
    let counted = runs.clone();
    let (worker, inbox) = open(&engine).await;
    let running = App::new(worker, inbox)
        .service(Service::new("limited").max_retries(0).failures("failed").on("work", move |_context: Context| {
            *counted.lock() += 1;
            async { Ok(()) }
        }))
        .start()
        .await
        .unwrap();
    let failure = within(next_delivery(&mut seen)).await;
    assert_eq!(&failure.payload[..], b"retries exceeded");
    assert_eq!(*runs.lock(), 0);
    assert!(observer.subscribe("limited".into(), None).await.unwrap().notices.is_empty(), "the event is gone");
    running.shutdown().await;
}

/// A handler that runs past its lease is abandoned without a completion; the
/// next registration reclaims the event, and another worker runs it with retry 1.
#[tokio::test(start_paused = true)]
async fn a_handler_that_outlives_its_lease_is_reclaimed_and_run_again_with_retry_plus_one() {
    let engine = engine().await;
    let (started, mut first) = mpsc::unbounded_channel();
    let (worker, inbox) = open(&engine).await;
    let stuck = App::new(worker, inbox)
        .service(Service::new("slow").lease(Duration::from_millis(100)).worker_tag(1).on("job", move |_context: Context| {
            let _ = started.send(());
            std::future::pending::<Result<(), Failure>>()
        }))
        .start()
        .await
        .unwrap();
    let (observer, _seen) = open(&engine).await;
    let (_, key) = observer.register_claimable(Envelope::claimable("slow", "job", Bytes::new())).await.unwrap();
    within(first.recv()).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    stuck.shutdown().await;

    let (ran, mut runs) = mpsc::unbounded_channel();
    let (worker, inbox) = open(&engine).await;
    let healthy = App::new(worker, inbox)
        .service(Service::new("slow").worker_tag(2).on("job", move |context: Context| {
            let ran = ran.clone();
            async move {
                let _ = ran.send((context.key(), context.retry(), context.event().claim.map(|claim| claim.worker)));
                Ok(())
            }
        }))
        .start()
        .await
        .unwrap();
    observer.register_claimable(Envelope::claimable("other", "tick", Bytes::new())).await.unwrap();
    assert_eq!(within(runs.recv()).await, Some((Some(key), 1, Some(2))));
    healthy.shutdown().await;
}

/// With every permit taken, notices are kept; when the permit returns they are
/// run although nothing new is registered.
#[tokio::test(start_paused = true)]
async fn a_worker_with_every_permit_taken_still_runs_work_noticed_meanwhile_once_a_permit_returns() {
    let engine = engine().await;
    let (open_gate, gate_rx) = gate();
    let (started, mut slow_started) = mpsc::unbounded_channel();
    let (ran, mut runs) = mpsc::unbounded_channel();
    let (worker, inbox) = open(&engine).await;
    let running = App::new(worker, inbox)
        .service(
            Service::new("w")
                .permits(1)
                .on("slow", move |_context: Context| {
                    let _ = started.send(());
                    let gate = gate_rx.clone();
                    async move {
                        opened(gate).await;
                        Ok(())
                    }
                })
                .on("fast", move |context: Context| {
                    let _ = ran.send(context.id());
                    async { Ok(()) }
                }),
        )
        .start()
        .await
        .unwrap();
    let (observer, _seen) = open(&engine).await;
    observer.register_claimable(Envelope::claimable("w", "slow", Bytes::new())).await.unwrap();
    within(slow_started.recv()).await;
    let mut expected = BTreeSet::new();
    for _ in 0..3 {
        expected.insert(observer.register_claimable(Envelope::claimable("w", "fast", Bytes::new())).await.unwrap().0);
    }
    open_gate.send(true).unwrap();
    let mut seen = BTreeSet::new();
    while seen.len() < 3 {
        seen.insert(within(runs.recv()).await.unwrap());
    }
    assert_eq!(seen, expected);
    running.shutdown().await;
}

/// Work reclaimed with a key before the last one read, whose notice the local
/// limit dropped, is found by re-reading the backlog from the start, and run.
#[tokio::test(start_paused = true)]
async fn work_reclaimed_ahead_of_the_read_position_and_dropped_by_the_local_limit_is_still_run() {
    let engine = engine().await;
    let (observer, _seen) = open(&engine).await;
    // The earliest registration, claimed by someone else with a short lease.
    let (early, early_key) = observer.register_claimable(Envelope::claimable("w", "fast", Bytes::new())).await.unwrap();
    observer.claim(early, 0, Duration::from_millis(50)).await.unwrap();

    let (open_gate, gate_rx) = gate();
    let (started, mut slow_started) = mpsc::unbounded_channel();
    let (ran, mut runs) = mpsc::unbounded_channel::<(Option<WorkKey>, u16)>();
    let (worker, inbox) = open(&engine).await;
    let running = App::new(worker, inbox)
        .service(
            Service::new("w")
                .permits(1)
                .pending_limit(1)
                .on("slow", move |_context: Context| {
                    let _ = started.send(());
                    let gate = gate_rx.clone();
                    async move {
                        opened(gate).await;
                        Ok(())
                    }
                })
                .on("fast", move |context: Context| {
                    let _ = ran.send((context.key(), context.retry()));
                    async { Ok(()) }
                }),
        )
        .start()
        .await
        .unwrap();
    observer.register_claimable(Envelope::claimable("w", "slow", Bytes::new())).await.unwrap();
    within(slow_started.recv()).await;
    let (_, kept) = observer.register_claimable(Envelope::claimable("w", "fast", Bytes::new())).await.unwrap();
    let (_, dropped) = observer.register_claimable(Envelope::claimable("w", "fast", Bytes::new())).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    // Reclaims the early claim: it returns under a new id with the earliest key.
    let (_, trigger) = observer.register_claimable(Envelope::claimable("w", "fast", Bytes::new())).await.unwrap();
    open_gate.send(true).unwrap();

    let mut seen = BTreeSet::new();
    while seen.len() < 4 {
        let (key, retry) = within(runs.recv()).await.unwrap();
        if key == Some(early_key) {
            assert_eq!(retry, 1);
        }
        seen.insert(key.unwrap());
    }
    assert_eq!(seen, BTreeSet::from([early_key, kept, dropped, trigger]));
    running.shutdown().await;
}

/// An executor full of long jobs takes nothing from the event workers' budget:
/// their events finish with no virtual time passing.
#[tokio::test(start_paused = true)]
async fn a_saturated_executor_does_not_slow_the_event_workers() {
    let engine = engine().await;
    let (done, mut finished) = mpsc::unbounded_channel();
    let (worker, inbox) = open(&engine).await;
    let running = App::new(worker, inbox)
        .service(Service::new("events").permits(4).on("e", move |_context: Context| {
            let _ = done.send(());
            async { Ok(()) }
        }))
        .service(Service::executor("jobs").permits(2).on("j", |_context: Context| std::future::pending::<Result<(), Failure>>()))
        .start()
        .await
        .unwrap();
    let (observer, _seen) = open(&engine).await;
    for _ in 0..10 {
        observer.register_claimable(Envelope::claimable("jobs", "j", Bytes::new())).await.unwrap();
    }
    let began = tokio::time::Instant::now();
    for _ in 0..20 {
        observer.register_claimable(Envelope::claimable("events", "e", Bytes::new())).await.unwrap();
    }
    for _ in 0..20 {
        within(finished.recv()).await.unwrap();
    }
    assert_eq!(began.elapsed(), Duration::ZERO, "event workers waited on the executor");
    drop(running);
}

/// Dispatch events on a listened channel reach the listener in order.
#[tokio::test(start_paused = true)]
async fn a_listener_receives_dispatch_events_in_order() {
    let engine = engine().await;
    let (got, mut received) = mpsc::unbounded_channel();
    let (worker, inbox) = open(&engine).await;
    let running = App::new(worker, inbox)
        .listen("news", move |event: Envelope| {
            let got = got.clone();
            async move {
                let _ = got.send(event.payload);
            }
        })
        .start()
        .await
        .unwrap();
    let (observer, _seen) = open(&engine).await;
    for index in 0..5u8 {
        observer.register_dispatch(Envelope::dispatch("news", "item", vec![index])).await.unwrap();
    }
    for index in 0..5u8 {
        assert_eq!(within(received.recv()).await.unwrap(), Bytes::from(vec![index]));
    }
    running.shutdown().await;
}

/// When the engine stops, the app's `wait` ends with `Disconnected`.
#[tokio::test(start_paused = true)]
async fn the_app_ends_with_an_error_when_the_engine_stops() {
    let engine = engine().await;
    let (worker, inbox) = open(&engine).await;
    let running = App::new(worker, inbox).service(Service::new("w").on("x", |_context: Context| async { Ok(()) })).start().await.unwrap();
    engine.shutdown().await.unwrap();
    assert_eq!(within(running.wait()).await, Err(WorkerError::Disconnected));
}

/// A refused subscription fails `start` instead of running half an app.
#[tokio::test(start_paused = true)]
async fn a_refused_subscription_fails_start() {
    let mut grants = ruvent::Grants::new();
    grants.insert("svc".into(), ruvent::Grant { methods: vec![ruvent::Method::RegisterClaimable], channels: vec![ruvent::Pattern("*".into())], delegate: false });
    let engine = Engine::volatile(EngineConfig { grants: Some(grants), ..EngineConfig::default() }).await;
    let (worker, inbox) = open(&engine).await;
    let started = App::new(worker, inbox).service(Service::new("w").on("x", |_context: Context| async { Ok(()) })).start().await;
    assert_eq!(started.err(), Some(WorkerError::Refused(Refusal::Forbidden)));
}
