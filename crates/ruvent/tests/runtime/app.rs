//! The framework surface: services registered as functions, with the queues,
//! the allow-list and the reaction table derived from the registrations.

use ruvent::{App, Audience, Backoff, Bytes, Context, Envelope, Failure, From, Identity, Proposal, Rejected, Service, WorkerConfig};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn anyone() -> Identity {
    Identity { user: "tester".to_owned() }
}

fn propose(action: &str, key: &str) -> Proposal {
    Proposal { action: action.into(), payload: Bytes::from_static(b"{}"), transaction_key: Some(key.into()), reply_channel: Some("room".into()), ..Proposal::default() }
}

async fn open_turn(cx: Context, command: Arc<Envelope>) -> Result<Bytes, Failure> {
    cx.fact(format!("turn.started:{}", command.transaction_key), "turn.started", "{}", Audience::Client).await?;
    Ok(Bytes::from_static(b"\"opened\""))
}

async fn call_model(cx: Context, command: Arc<Envelope>) -> Result<Bytes, Failure> {
    for token in ["\"a\"", "\"b\"", "\"c\""] {
        cx.progress("model.delta", token);
    }
    // Worker-to-worker: clients never see it, the next reactor does.
    cx.fact(format!("model.replied:{}", command.correlation_id), "model.replied", "{}", Audience::Worker).await?;
    Ok(Bytes::new())
}

async fn decide(cx: Context, command: Arc<Envelope>) -> Result<Bytes, Failure> {
    cx.fact(format!("turn.final:{}", command.correlation_id), "turn.final", "\"answer\"", Audience::Client).await?;
    Ok(Bytes::new())
}

async fn until(room: &mut ruvent::Subscription, action: &str) -> Vec<Arc<Envelope>> {
    let mut seen = Vec::new();
    while !seen.iter().any(|event: &Arc<Envelope>| &*event.action == action) {
        seen.extend(room.next(64).await);
    }
    seen
}

/// The whole turn is three functions that never name each other.
#[tokio::test(start_paused = true)]
async fn a_chain_of_reactions_runs_with_no_handler_naming_the_next() {
    let app = App::volatile()
        .service(Service::new("turn").accept("turn.run", open_turn).on("model.replied", decide))
        .service(Service::new("inference").on("turn.started", call_model).max_in_flight(4))
        .start()
        .await
        .unwrap();

    let mut room = app.engine().subscribe(vec!["room".to_owned()], From::Now);
    app.gateway().submit(&anyone(), propose("turn.run", "turn-1")).await.unwrap();

    let seen: Vec<String> = until(&mut room, "turn.final").await.iter().map(|event| event.action.to_string()).collect();
    let at = |action: &str| seen.iter().position(|seen| seen == action).unwrap_or_else(|| panic!("{action} missing from {seen:?}"));
    assert!(at("turn.run") < at("turn.started") && at("turn.started") < at("model.delta") && at("model.delta") < at("turn.final"), "{seen:?}");
    assert_eq!(seen.iter().filter(|action| *action == "model.delta").count(), 3);
    assert!(!seen.iter().any(|action| action == "model.replied"), "worker-audience facts must not reach a client: {seen:?}");

    // Deltas of one execution arrive in the order they were emitted.
    let deltas: Vec<Bytes> = app.engine().read_stream("channel:room", 0, 100).iter().filter(|event| &*event.action == "model.delta").map(|event| event.payload.clone()).collect();
    assert_eq!(deltas, ["\"a\"", "\"b\"", "\"c\""]);

    assert_eq!(app.gateway().submit(&anyone(), propose("turn.cancel", "x")).await.unwrap_err(), Rejected::NotAdmitted, "an action nobody registered is not accepted");
    app.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_panic_is_retried_and_a_fatal_failure_is_recorded() {
    let tries = Arc::new(AtomicU32::new(0));
    let flaky = {
        let tries = tries.clone();
        move |_cx: Context, _command: Arc<Envelope>| {
            let tries = tries.clone();
            async move {
                if tries.fetch_add(1, Ordering::SeqCst) == 0 {
                    panic!("first attempt dies");
                }
                Ok(Bytes::from_static(b"\"second time\""))
            }
        }
    };
    let refuse = |_cx: Context, _command: Arc<Envelope>| async { Err::<Bytes, _>(Failure::fatal("not possible")) };
    let app = App::volatile().service(Service::new("jobs").accept("flaky", flaky).accept("refuse", refuse)).start().await.unwrap();

    let mut room = app.engine().subscribe(vec!["room".to_owned()], From::Now);
    app.gateway().submit(&anyone(), propose("flaky", "f1")).await.unwrap();
    app.gateway().submit(&anyone(), propose("refuse", "r1")).await.unwrap();

    let mut results = Vec::new();
    while results.len() < 2 {
        for event in room.next(64).await {
            if event.action.ends_with(".result") || event.action.ends_with(".failure") {
                results.push((event.action.to_string(), event.payload.clone()));
            }
        }
    }
    results.sort();
    assert_eq!(results, [("flaky.result".to_owned(), Bytes::from_static(b"\"second time\"")), ("refuse.failure".to_owned(), Bytes::from_static(b"not possible"))]);
    assert_eq!(tries.load(Ordering::SeqCst), 2);
    app.shutdown().await.unwrap();
}

/// A step that overruns its deadline is dropped, not waited for, and runs again.
#[tokio::test(start_paused = true)]
async fn a_step_that_overruns_is_dropped_and_retried() {
    struct Witness(Arc<AtomicU32>);
    impl Drop for Witness {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let (runs, dropped) = (Arc::new(AtomicU32::new(0)), Arc::new(AtomicU32::new(0)));
    let handler = {
        let (runs, dropped) = (runs.clone(), dropped.clone());
        move |_cx: Context, _command: Arc<Envelope>| {
            let (runs, dropped) = (runs.clone(), dropped.clone());
            async move {
                if runs.fetch_add(1, Ordering::SeqCst) == 0 {
                    // Whatever the handler owns goes when the future is dropped.
                    let _owned = Witness(dropped);
                    std::future::pending::<()>().await;
                }
                Ok(Bytes::from_static(b"\"done\""))
            }
        }
    };
    let config = WorkerConfig { timeout: Duration::from_secs(30), backoff: Backoff { first: Duration::from_secs(2), max: Duration::from_secs(60) }, ..WorkerConfig::default() };
    let app = App::volatile().service(Service::new("jobs").accept("slow", handler).config(config)).start().await.unwrap();
    let mut room = app.engine().subscribe(vec!["room".to_owned()], From::Now);
    let started = tokio::time::Instant::now();
    app.gateway().submit(&anyone(), propose("slow", "s1")).await.unwrap();
    until(&mut room, "slow.result").await;

    assert_eq!((runs.load(Ordering::SeqCst), dropped.load(Ordering::SeqCst)), (2, 1));
    assert_eq!(started.elapsed(), Duration::from_secs(32), "the deadline, then the backoff — and no other waiting");
    app.shutdown().await.unwrap();
}

/// Running services cost nothing while there is nothing to do.
#[tokio::test(start_paused = true)]
async fn idle_services_do_not_wake_the_engine() {
    let app = App::volatile()
        .service(Service::new("turn").accept("turn.run", open_turn).on("model.replied", decide))
        .service(Service::new("inference").on("turn.started", call_model))
        .start()
        .await
        .unwrap();
    // Let every lane park, and one turn run to completion.
    let mut room = app.engine().subscribe(vec!["room".to_owned()], From::Now);
    app.gateway().submit(&anyone(), propose("turn.run", "turn-1")).await.unwrap();
    until(&mut room, "turn.final").await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    app.engine().flush().await.unwrap();

    let before = app.engine().stats().wakeups;
    tokio::time::sleep(Duration::from_secs(6 * 3600)).await;
    assert_eq!(app.engine().stats().wakeups, before, "six idle hours must cost no wakeups: no heartbeat, no poll, no sweep");
    app.shutdown().await.unwrap();
}
