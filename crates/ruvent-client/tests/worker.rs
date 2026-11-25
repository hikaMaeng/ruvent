//! The worker (2.4, 2.9): windows over many servers, a killed worker, a
//! restarting server, the "later" ends of a handler, the shutdown procedure.

mod support;

use parking_lot::Mutex;
use ruvent::{Engine, EngineConfig, Envelope, EventId};
use ruvent_client::servers::Servers;
use ruvent_client::worker::{Context, Retry, Service, Worker, WorkerConfig};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use support::*;
use tokio::sync::Notify;

/// What handlers saw: runs by id, the most running at once.
#[derive(Default)]
struct Seen {
    runs: Mutex<HashMap<EventId, u32>>,
    by_server: Mutex<HashMap<String, u64>>,
    running: AtomicUsize,
    most: AtomicUsize,
}

impl Seen {
    fn enter(&self, cx: &Context) {
        *self.runs.lock().entry(cx.id()).or_default() += 1;
        *self.by_server.lock().entry(cx.server().to_owned()).or_default() += 1;
        let now = self.running.fetch_add(1, Ordering::SeqCst) + 1;
        self.most.fetch_max(now, Ordering::SeqCst);
    }

    fn leave(&self) {
        self.running.fetch_sub(1, Ordering::SeqCst);
    }

    fn total(&self) -> usize {
        self.runs.lock().len()
    }

    fn twice(&self) -> usize {
        self.runs.lock().values().filter(|runs| **runs > 1).count()
    }
}

/// B1, 2.4 (M1): 90/10 skew over 2 and 4 servers. Never more than P runs at
/// once (overflow is given back, not run), every event runs exactly once,
/// and while the light servers still have work waiting their window stays
/// at one or more.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn skewed_load_over_two_and_four_servers_never_runs_past_p_and_keeps_every_busy_window_open() {
    for servers in [2usize, 4] {
        let (local, list, engines) = locals(servers, EngineConfig::default()).await;
        let heavy = 450;
        let light = 50 / (servers - 1);
        let mut expected: HashMap<String, u64> = HashMap::new();
        preload(&engines[0], "jobs", "do", heavy).await;
        expected.insert("s0".into(), heavy as u64);
        for (index, engine) in engines.iter().enumerate().skip(1) {
            preload(engine, "jobs", "do", light).await;
            expected.insert(format!("s{index}"), light as u64);
        }
        let seen = Arc::new(Seen::default());
        let handler_seen = seen.clone();
        let slots = 8;
        let worker = Worker::builder(local, &list)
            .service(Service::new("jobs").slots(slots).on("do", move |cx| {
                let seen = handler_seen.clone();
                async move {
                    seen.enter(&cx);
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    seen.leave();
                    Ok(())
                }
            }))
            .start();
        // Past the start (slots placed, the first `Pending`s answered), a
        // server with at least P events left must hold a window.
        let mut checked = 0;
        let mut closed = Vec::new();
        let began = tokio::time::Instant::now();
        while seen.total() < heavy + light * (servers - 1) {
            if began.elapsed() > Duration::from_secs(15) {
                let s0 = stats(&engines[0]).await;
                panic!("stuck at {} runs; windows {:?}; stats {:?}; s0 waiting {} claimed {}", seen.total(), worker.windows(), worker.stats(), s0.waiting, s0.claimed);
            }
            tokio::time::sleep(Duration::from_millis(3)).await;
            if seen.total() < 60 {
                continue;
            }
            let done = seen.by_server.lock().clone();
            for (_, server, seat) in worker.windows() {
                let left = expected[server.as_ref()].saturating_sub(done.get(server.as_ref()).copied().unwrap_or(0));
                if left > slots as u64 * 2 {
                    checked += 1;
                    if seat.window == 0 {
                        closed.push((server.clone(), left));
                    }
                }
            }
            assert!(worker.windows().iter().map(|(_, _, seat)| seat.window).sum::<u32>() <= slots, "the windows add up to at most P");
        }
        within("all runs", eventually("every event ran", || seen.total() == heavy + light * (servers - 1))).await;
        assert!(checked > 0);
        assert!(closed.len() * 20 <= checked, "a busy server sat at window 0 in {} of {checked} looks: {closed:?}", closed.len());
        assert!(seen.most.load(Ordering::SeqCst) <= slots as usize, "never more than P at once");
        assert_eq!(seen.twice(), 0, "nothing ran twice");
        let stats = worker.stats();
        assert!(stats.overflow <= stats.windows, "overflow only where a window moved: {stats:?}");
        worker.shutdown().await;
    }
}

/// 2.4, M1 "P < S": three workers with one slot each over four servers and
/// work arriving now and then: every event is taken, none waits for a
/// lease or a timer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workers_with_fewer_slots_than_servers_reach_every_server() {
    let (local, list, engines) = locals(4, EngineConfig::default()).await;
    let seen = Arc::new(Seen::default());
    let mut workers = Vec::new();
    for _ in 0..3 {
        let handler_seen = seen.clone();
        workers.push(
            Worker::builder(local.clone(), &list)
                .service(Service::new("jobs").slots(1).on("do", move |cx| {
                    let seen = handler_seen.clone();
                    async move {
                        seen.enter(&cx);
                        seen.leave();
                        Ok(())
                    }
                }))
                .start(),
        );
    }
    let mut longest = Duration::ZERO;
    for round in 0..24 {
        let engine = &engines[round % 4];
        let before = seen.total();
        let started = tokio::time::Instant::now();
        preload(engine, "jobs", "do", 1).await;
        eventually("the event is taken", || seen.total() > before).await;
        longest = longest.max(started.elapsed());
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(seen.twice(), 0);
    assert!(longest < Duration::from_secs(2), "an event waited {longest:?}");
    for worker in workers {
        worker.shutdown().await;
    }
}

/// M8-2: a worker killed in the middle of a run (its connection closes): the
/// server pulls the deadline to the reconnect grace and another worker gets
/// the event long before the 30-second lease.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_killed_worker_is_reassigned_within_the_reconnect_grace() {
    let grace = Duration::from_millis(300);
    let engine = Engine::volatile(EngineConfig { reconnect_grace: grace, ..EngineConfig::default() }).await;
    let server = TcpServer::start(&engine).await;
    let list = Servers::new([&server.address]);
    let started = Arc::new(Notify::new());
    let first_started = started.clone();
    let doomed = Worker::builder(tcp(), &list)
        .service(Service::new("jobs").slots(1).on("do", move |_cx| {
            let started = first_started.clone();
            async move {
                started.notify_one();
                std::future::pending::<()>().await;
                Ok(())
            }
        }))
        .start();
    preload(&engine, "jobs", "do", 1).await;
    within("the first run", started.notified()).await;
    let taken = Arc::new(Notify::new());
    let second_taken = taken.clone();
    let survivor = Worker::builder(tcp(), &list)
        .service(Service::new("jobs").slots(1).on("do", move |_cx| {
            let taken = second_taken.clone();
            async move {
                taken.notify_one();
                Ok(())
            }
        }))
        .start();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let killed = tokio::time::Instant::now();
    drop(doomed);
    within("the reassignment", taken.notified()).await;
    let after = killed.elapsed();
    assert!(after < grace + Duration::from_secs(2), "reassigned after {after:?}");
    survivor.shutdown().await;
    server.stop().await;
}

/// M8-6: the server restarts gracefully while a run is in flight. The run
/// goes on, the worker reconnects, extends and completes under the same
/// token: nothing is assigned again and no failure is counted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_graceful_restart_keeps_the_run_claimed_and_counts_nothing() {
    let path = scratch("worker-restart");
    let local = Arc::new(ruvent_client::link::Local::new());
    let engine = durable(&path, EngineConfig::default()).await;
    local.insert("s0", engine.clone());
    let list = Servers::new(["s0"]);
    let seen = Arc::new(Seen::default());
    let gate = Arc::new(Notify::new());
    let (handler_seen, handler_gate) = (seen.clone(), gate.clone());
    let failures = Arc::new(AtomicU64::new(u64::MAX));
    let handler_failures = failures.clone();
    let worker = Worker::builder(local.clone(), &list)
        .service(Service::new("jobs").slots(1).on("do", move |cx| {
            let (seen, gate, failures) = (handler_seen.clone(), handler_gate.clone(), handler_failures.clone());
            async move {
                seen.enter(&cx);
                failures.store(u64::from(cx.failures()), Ordering::SeqCst);
                gate.notified().await;
                seen.leave();
                Ok(())
            }
        }))
        .start();
    preload(&engine, "jobs", "do", 1).await;
    eventually("the run started", || seen.total() == 1).await;
    engine.shutdown().await.unwrap();
    let engine = durable(&path, EngineConfig::default()).await;
    local.insert("s0", engine.clone());
    eventually("the worker noticed the restart", || worker.stats().restarts >= 1).await;
    gate.notify_one();
    eventually_async("the completion landed", || {
        let engine = engine.clone();
        async move {
            let stats = stats(&engine).await;
            stats.waiting == 0 && stats.claimed == 0
        }
    })
    .await;
    let after = stats(&engine).await;
    assert_eq!((after.counters.reclaimed, after.counters.stale), (0, 0), "nothing reclaimed, the token was current");
    assert_eq!(seen.runs.lock().values().copied().collect::<Vec<_>>(), [1], "run once");
    assert_eq!(failures.load(Ordering::SeqCst), 0);
    worker.shutdown().await;
}

/// M8-12, B6, B23: "later, not this event's fault" more often than
/// `isolate_after` isolates nothing; "this event failed" isolates at the
/// n-th failure. The SDK counts neither.
#[tokio::test]
async fn releasing_later_more_often_than_isolate_after_isolates_nothing_but_failures_do() {
    let mut config = EngineConfig::default();
    config.channels.default.isolate_after = Some(3);
    let (local, list, engines) = locals(1, config).await;
    let outage = Arc::new(AtomicU64::new(0));
    let broken = Arc::new(AtomicU64::new(0));
    let (handler_outage, handler_broken) = (outage.clone(), broken.clone());
    let worker = Worker::builder(local, &list)
        .service(
            Service::new("jobs")
                .slots(2)
                .on("flaky", move |_cx| {
                    let outage = handler_outage.clone();
                    async move {
                        if outage.fetch_add(1, Ordering::SeqCst) < 8 {
                            return Err(Retry::later(Duration::ZERO).because("downstream 503"));
                        }
                        Ok(())
                    }
                })
                .on("broken", move |_cx| {
                    let broken = handler_broken.clone();
                    async move {
                        broken.fetch_add(1, Ordering::SeqCst);
                        Err(Retry::failed(Duration::ZERO))
                    }
                }),
        )
        .start();
    preload(&engines[0], "jobs", "flaky", 1).await;
    preload(&engines[0], "jobs", "broken", 1).await;
    eventually_async("both settled", || {
        let engine = engines[0].clone();
        async move {
            let stats = stats(&engine).await;
            stats.counters.isolated == 1 && stats.claimed == 0 && stats.waiting == 1
        }
    })
    .await;
    assert_eq!(outage.load(Ordering::SeqCst), 9, "eight times later, then done: never isolated");
    assert_eq!(broken.load(Ordering::SeqCst), 3, "isolated at the third failure");
    let stats = stats(&engines[0]).await;
    let waiting: Vec<String> = stats.channels.iter().filter(|channel| channel.waiting > 0).map(|channel| channel.channel.as_str().to_owned()).collect();
    assert_eq!(waiting, ["isolated.jobs"]);
    worker.shutdown().await;
}

/// 2.9 "종료": windows to 0, then running handlers get the grace; past it
/// they are cut off and given back as not their fault. A handler that ends
/// within the grace completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_closes_the_windows_and_gives_back_what_outlives_the_grace() {
    let (local, list, engines) = locals(1, EngineConfig::default()).await;
    let started = Arc::new(AtomicUsize::new(0));
    let handler_started = started.clone();
    let worker = Worker::builder(local.clone(), &list)
        .config(WorkerConfig { shutdown_grace: Duration::from_millis(300), ..WorkerConfig::default() })
        .service(
            Service::new("jobs")
                .slots(2)
                .on("stuck", move |_cx| {
                    let started = handler_started.clone();
                    async move {
                        started.fetch_add(1, Ordering::SeqCst);
                        std::future::pending::<()>().await;
                        Ok(())
                    }
                })
                .on("quick", |_cx| async move {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    Ok(())
                }),
        )
        .start();
    preload(&engines[0], "jobs", "stuck", 1).await;
    eventually("the stuck run", || started.load(Ordering::SeqCst) == 1).await;
    preload(&engines[0], "jobs", "quick", 1).await;
    preload(&engines[0], "jobs", "stuck", 2).await;
    eventually_async("both slots busy", || {
        let engine = engines[0].clone();
        async move { stats(&engine).await.claimed == 2 }
    })
    .await;
    let began = tokio::time::Instant::now();
    within("shutdown", worker.shutdown()).await;
    assert!(began.elapsed() >= Duration::from_millis(300), "the grace was given");
    let after = stats(&engines[0]).await;
    assert_eq!((after.waiting, after.claimed), (3, 0), "the quick one completed; the stuck one and the unstarted two wait");
    // Nothing counted against them: a new worker sees failure count 0.
    let failures: Arc<Mutex<Vec<u16>>> = Arc::default();
    let handler_failures = failures.clone();
    let next = Worker::builder(local, &list)
        .service(Service::new("jobs").slots(4).on("stuck", move |cx| {
            let failures = handler_failures.clone();
            async move {
                failures.lock().push(cx.failures());
                Ok(())
            }
        }))
        .start();
    eventually("all three", || failures.lock().len() == 3).await;
    assert_eq!(*failures.lock(), [0, 0, 0]);
    next.shutdown().await;
}

/// 2.2 "실행 중 방출": what a handler emits while running reaches the
/// server before its completion, on the same connection.
#[tokio::test]
async fn a_dispatch_emitted_while_running_comes_before_the_completions_follow_up() {
    let (local, list, engines) = locals(1, EngineConfig::default()).await;
    let (observer, mut inbox) = ruvent::connect(&engines[0], ruvent::Hello::observer("o"), 64).await.unwrap();
    ruvent::Methods::subscribe(&observer, ruvent::Target::Channel("feed".into()), None).await.unwrap();
    let worker = Worker::builder(local, &list)
        .service(Service::new("jobs").on("do", |cx| async move {
            cx.emit_now(Envelope::dispatch("feed", "early", &b"1"[..])).await.unwrap();
            cx.emit_dispatch("feed", "late", &b"2"[..]);
            Ok(())
        }))
        .start();
    preload(&engines[0], "jobs", "do", 1).await;
    let mut types = Vec::new();
    while types.len() < 2 {
        if let Some(ruvent::Incoming::Delivery(event)) = within("delivery", inbox.recv()).await {
            types.push(event.event_type.as_str().to_owned());
        }
    }
    assert_eq!(types, ["early", "late"]);
    worker.shutdown().await;
}
