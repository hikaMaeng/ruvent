//! What leaves relative to the disk, and what a restart finds.

use crate::support::*;
use ruvent::{
    connect, ByeReason, Bytes, Call, Durability, Engine, EngineConfig, Envelope, Hello, Incoming, Methods, Outbound, Pattern, Recovered, Refusal, SegmentLog, SnapshotConfig, StorageConfig, Target,
};
use std::time::Duration;

fn strict() -> EngineConfig {
    EngineConfig { durability: Durability::Strict, ..EngineConfig::default() }
}

/// Under Strict an event is seen by nobody — notice, assignment, backlog,
/// explicit claim — and acknowledged to nobody before its batch is on disk (2.7, M8-3).
#[tokio::test]
async fn under_strict_nothing_uncommitted_is_visible_while_the_disk_is_held() {
    let gate = Gate::new();
    let engine = Engine::start(strict(), gate.clone(), Recovered::default()).await.unwrap();
    let (producer, _) = producer(&engine).await;
    let (observer, mut watched) = observer(&engine).await;
    let (worker, mut assignments) = worker(&engine).await;
    observer.subscribe(Target::Channel("work".into()), None).await.unwrap();
    worker.join("work".into(), Vec::new(), Duration::from_secs(30), false).await.unwrap();
    worker.window("work".into(), 1, 0).await.unwrap();
    let (seq, id) = producer.register(work("work")).await.unwrap();
    let (late, _) = observer_and_backlog(&engine).await;
    assert!(late.is_empty(), "not in a backlog");
    let claimer = tokio::spawn({
        let engine = engine.clone();
        async move {
            let (claimer, _) = crate::support::worker(&engine).await;
            claimer.claim(id, Duration::from_secs(30)).await
        }
    });
    assert!(stays_quiet(&mut watched).await, "no notice");
    assert!(stays_quiet(&mut assignments).await, "no assignment");
    assert!(!claimer.is_finished(), "an explicit claim's answer waits");
    let acknowledged = tokio::spawn({
        let producer = producer.clone();
        async move { producer.committed(seq).await }
    });
    assert!(!acknowledged.is_finished());
    gate.release(16);
    acknowledged.await.unwrap().unwrap();
    assert!(matches!(next_event(&mut watched).await, Some(Incoming::Notice(notice)) if notice.id == id));
    let claimed = claimer.await.unwrap().is_ok();
    let assigned = matches!(tokio::time::timeout(QUIET, assignments.recv()).await, Ok(Some(Incoming::Assign(_))));
    assert_ne!(claimed, assigned, "exactly one of the explicit claim and the window gets it");
}

async fn observer_and_backlog(engine: &Engine) -> (Vec<Envelope>, ruvent::Client) {
    let (late, _) = observer(engine).await;
    (late.subscribe(Target::Channel("work".into()), None).await.unwrap().notices, late)
}

/// Strict: a completion's dispatch follow-up A, then a dispatch B to the same
/// subscriber, arrive as A then B, though only A waited for the disk.
#[tokio::test]
async fn under_strict_one_connection_receives_in_processing_order() {
    let gate = Gate::new();
    let engine = Engine::start(strict(), gate.clone(), Recovered::default()).await.unwrap();
    let (observer, mut watched) = observer(&engine).await;
    observer.subscribe(Target::Channel("feed".into()), None).await.unwrap();
    let (producer, _) = producer(&engine).await;
    let (worker, mut assignments) = worker(&engine).await;
    worker.join("work".into(), Vec::new(), Duration::from_secs(30), false).await.unwrap();
    worker.window("work".into(), 1, 0).await.unwrap();
    producer.register(work("work")).await.unwrap();
    gate.release(1);
    let assignment = next_assignment(&mut assignments).await;
    worker.complete(assignment.event.id, assignment.token, vec![Envelope::dispatch("feed", "A", Bytes::new())]).await.unwrap();
    // A round trip on another connection: the completion is applied before B
    // is. (Not on the observer: its answer would queue behind A.)
    let (barrier, _) = crate::support::observer(&engine).await;
    barrier.stats().await.unwrap();
    producer.register(Envelope::dispatch("feed", "B", Bytes::new())).await.unwrap();
    assert!(stays_quiet(&mut watched).await, "A waits for the disk and B waits behind A");
    gate.release(8);
    let order: Vec<String> = [next_event(&mut watched).await, next_event(&mut watched).await]
        .into_iter()
        .map(|event| match event {
            Some(Incoming::Delivery(event)) => event.event_type.to_string(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(order, ["A", "B"]);
}

/// A failed write stops the engine with the reason; the change it carried is
/// acknowledged to nobody and announced to nobody.
#[tokio::test]
async fn a_failed_write_stops_the_engine_and_releases_nothing_it_carried() {
    let engine = Engine::start(strict(), Failing { good: 1 }, Recovered::default()).await.unwrap();
    let (producer, _) = producer(&engine).await;
    let (observer, mut watched) = observer(&engine).await;
    observer.subscribe(Target::Channel("work".into()), None).await.unwrap();
    let (seq, _) = producer.register(work("work")).await.unwrap();
    assert_eq!(producer.committed(seq).await, Err(Refusal::Closed));
    let stopped = engine.stopped().await.unwrap_err();
    assert!(stopped.to_string().contains("disk on fire"), "{stopped}");
    assert_eq!(next_event(&mut watched).await, Some(Incoming::Closed), "the subscriber sees the connection end, not the notice");
}

/// A connection whose send buffer is full is cut off; the others go on.
#[tokio::test]
async fn a_connection_that_does_not_read_is_cut_off_and_the_others_carry_on() {
    let engine = Engine::volatile(EngineConfig { outbound: 4, ..EngineConfig::default() }).await;
    let (stalled, mut stalled_out) = engine.open(Hello::observer("svc")).await.unwrap();
    stalled.call(Call::Subscribe { req: 1, target: Target::Channel("feed".into()), after: None }).await.unwrap();
    let (producer, _) = producer(&engine).await;
    for _ in 0..10 {
        producer.register(Envelope::dispatch("feed", "t", Bytes::new())).await.unwrap();
    }
    let (seq, _) = producer.register(work("work")).await.unwrap();
    assert!(producer.committed(seq).await.is_ok(), "the engine never waited on the stalled connection");
    let mut received = 0;
    while stalled_out.recv().await.is_some() {
        received += 1;
    }
    assert_eq!(received, 4, "what fit in the buffer (welcome, answer, two deliveries), then the end of the connection");
}

/// What was waiting comes back in order after a restart; what was claimed
/// stays claimed under its token, and the worker's completion is accepted:
/// nothing is assigned again, no failure is counted (2.4, M8-6).
#[tokio::test]
async fn a_restart_keeps_waiting_order_and_claims_and_accepts_the_original_workers_completion() {
    let path = scratch("restart").join("journal");
    let hello = Hello::worker("svc");
    let (ids, assignment) = {
        let (journal, recovered) = SegmentLog::open(&path).unwrap();
        let engine = Engine::start(EngineConfig::default(), journal, recovered).await.unwrap();
        let (producer, _) = producer(&engine).await;
        let mut ids = Vec::new();
        for index in 0..3u8 {
            let (seq, id) = producer.register(Envelope::claimable("work", "do", Bytes::from(vec![index]))).await.unwrap();
            producer.committed(seq).await.unwrap();
            ids.push(id);
        }
        let (worker, mut assignments) = connect(&engine, hello.clone(), 64).await.unwrap();
        worker.join("work".into(), Vec::new(), Duration::from_secs(60), false).await.unwrap();
        worker.window("work".into(), 1, 0).await.unwrap();
        let assignment = next_assignment(&mut assignments).await;
        drop(worker);
        engine.shutdown().await.unwrap();
        (ids, assignment)
    };
    let (journal, recovered) = SegmentLog::open(&path).unwrap();
    let engine = Engine::start(EngineConfig::default(), journal, recovered).await.unwrap();
    let (observer, _) = observer(&engine).await;
    let backlog = observer.subscribe(Target::Channel("work".into()), None).await.unwrap();
    assert_eq!(backlog.notices.iter().map(|notice| notice.id).collect::<Vec<_>>(), ids[1..], "the claimed one is not waiting");
    let (worker, mut assignments) = connect(&engine, hello, 64).await.unwrap();
    worker.join("work".into(), Vec::new(), Duration::from_secs(60), false).await.unwrap();
    worker.extend(assignment.event.id, assignment.token).await.unwrap();
    let done = worker.complete(assignment.event.id, assignment.token, Vec::new()).await.unwrap();
    worker.committed(done).await.unwrap();
    worker.window("work".into(), 1, 0).await.unwrap();
    let next = next_assignment(&mut assignments).await;
    assert_eq!((next.event.id, next.event.failures), (ids[1], 0));
    assert!(observer.stats().await.unwrap().waiting == 1);
    engine.shutdown().await.unwrap();
}

/// A sequence acknowledged under Scheduled survives the process dying right
/// after; the producer resumes from it after the restart (2.3, M8-1).
#[test]
fn an_acknowledged_sequence_survives_a_crash_and_resume_says_so() {
    let path = scratch("crash").join("journal");
    let hello = Hello::producer("svc");
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let (journal, recovered) = SegmentLog::open(&path).unwrap();
        let engine = Engine::start(EngineConfig::default(), journal, recovered).await.unwrap();
        let (producer, _) = connect(&engine, hello.clone(), 64).await.unwrap();
        for _ in 0..3 {
            producer.register_committed(work("work")).await.unwrap();
        }
    });
    // No shutdown: the runtime and every task in it are dropped.
    drop(runtime);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let (journal, recovered) = SegmentLog::open(&path).unwrap();
        let engine = Engine::start(EngineConfig::default(), journal, recovered).await.unwrap();
        let (producer, _) = connect(&engine, hello, 64).await.unwrap();
        assert_eq!(producer.welcome().unwrap().resume, 3, "nothing acknowledged was lost; nothing will be applied twice");
    });
}

/// Checkpoints follow the log's tail; segments wholly before the last one
/// with nothing live in them go; the log restarts from what is left (2.5).
#[tokio::test]
async fn checkpoints_let_empty_segments_go_and_the_log_restarts_from_what_is_left() {
    let path = scratch("segments").join("journal");
    let storage = StorageConfig { segment_bytes: 2048, ..StorageConfig::default() };
    let config = EngineConfig { storage: storage.clone(), ..EngineConfig::default() };
    {
        let (journal, recovered) = SegmentLog::open_with(&path, storage.segment_bytes).unwrap();
        let engine = Engine::start(config.clone(), journal, recovered).await.unwrap();
        let (producer, _) = producer(&engine).await;
        let (worker, mut assignments) = worker(&engine).await;
        worker.join("work".into(), Vec::new(), Duration::from_secs(60), false).await.unwrap();
        worker.window("work".into(), 1, 0).await.unwrap();
        for _ in 0..200 {
            producer.register(work("work")).await.unwrap();
            let assignment = next_assignment(&mut assignments).await;
            let seq = worker.complete(assignment.event.id, assignment.token, Vec::new()).await.unwrap();
            worker.committed(seq).await.unwrap();
        }
        producer.register_committed(work("kept")).await.unwrap();
        let (observer, _) = observer(&engine).await;
        let counters = observer.stats().await.unwrap().counters;
        assert!(counters.checkpoints > 0 && counters.segments_removed > 0, "{counters:?}");
        drop(worker);
        drop(producer);
        engine.shutdown().await.unwrap();
    }
    let segments = std::fs::read_dir(&path).unwrap().filter(|entry| entry.as_ref().unwrap().path().extension().is_some_and(|extension| extension == "seg")).count();
    assert!(segments <= 4, "{segments} segments left of the dozens written");
    let (journal, recovered) = SegmentLog::open_with(&path, storage.segment_bytes).unwrap();
    let engine = Engine::start(config, journal, recovered).await.unwrap();
    let (observer, _) = observer(&engine).await;
    assert_eq!(observer.subscribe(Target::Channel("kept".into()), None).await.unwrap().notices.len(), 1);
    assert!(observer.subscribe(Target::Channel("work".into()), None).await.unwrap().notices.is_empty());
}

/// A normal shutdown writes the stopped ledger's checkpoint last, so the next
/// start reads the checkpoint and no tail; the uptime clock carries on (2.5, B22).
#[tokio::test]
async fn a_shutdown_writes_a_checkpoint_the_next_start_reads_no_tail_and_the_clock_carries_on() {
    let path = scratch("shutdown").join("journal");
    let (before, generation) = {
        let (journal, recovered) = SegmentLog::open(&path).unwrap();
        let engine = Engine::start(EngineConfig::default(), journal, recovered).await.unwrap();
        let (producer, _) = producer(&engine).await;
        producer.register_committed(work("work")).await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        let before = engine.uptime();
        drop(producer);
        engine.shutdown().await.unwrap();
        (before, engine.generation())
    };
    let items = ruvent::journal::read_checkpoint(path.join("checkpoint.ckpt")).unwrap();
    assert!(items.iter().any(|item| matches!(item, ruvent::Replay::Entry(entry) if &*entry.event.channel == "work")));
    let (journal, recovered) = SegmentLog::open(&path).unwrap();
    let mut source = recovered.into_source();
    let mut records = 0;
    while let Some(item) = source.next().unwrap() {
        records += usize::from(matches!(item, ruvent::Replay::Record { .. }));
    }
    assert_eq!(records, 0, "the checkpoint starts where the log ends");
    drop(journal);
    let (journal, recovered) = SegmentLog::open(&path).unwrap();
    let restarted = Engine::start(EngineConfig::default(), journal, recovered).await.unwrap();
    assert!(restarted.uptime() >= before, "the clock starts where it stopped, not at zero");
    assert_eq!(restarted.generation(), generation + 1);
    let (observer, _) = observer(&restarted).await;
    assert_eq!(observer.subscribe(Target::Channel("work".into()), None).await.unwrap().notices.len(), 1);
}

/// Drain: every production connection gets its final `Committed` and then
/// `Bye{draining}`; new ones are sent away; work goes on (2.8, M8-5).
#[tokio::test]
async fn drain_gives_every_producer_a_final_committed_before_bye() {
    let gate = Gate::new();
    let engine = Engine::start(EngineConfig::default(), gate.clone(), Recovered::default()).await.unwrap();
    let (session, mut out) = engine.open(Hello::producer("svc")).await.unwrap();
    assert!(matches!(out.recv().await, Some(Outbound::Welcome(_))));
    for seq in 1..=3 {
        session.call(Call::Register { seq: Some(seq), event: work("work") }).await.unwrap();
    }
    let (operator, _) = observer(&engine).await;
    operator.drain().await.unwrap();
    gate.release(16);
    let mut tail = Vec::new();
    while let Some(message) = out.recv().await {
        tail.push(message);
    }
    assert_eq!(tail.last(), Some(&Outbound::Bye(ByeReason::Draining)));
    assert!(tail.contains(&Outbound::Committed { seq: 3 }), "the final acknowledgement precedes the goodbye: {tail:?}");
    assert_eq!(connect(&engine, Hello::producer("svc"), 8).await.err(), Some(ruvent::ConnectError::Bye(ByeReason::Draining)));
    assert!(operator.stats().await.unwrap().draining);
}

/// The operational snapshot collects the chosen channels in units and writes
/// the last unit on shutdown. It is not replayed.
#[tokio::test]
async fn chosen_channels_are_collected_into_snapshot_files() {
    let directory = scratch("snapshot");
    let snapshot = SnapshotConfig { max_events: 3, max_age: Duration::from_secs(3600), ..SnapshotConfig::new(directory.clone(), vec![Pattern("audit.*".into())]) };
    let engine = Engine::volatile(EngineConfig { snapshot: Some(snapshot), ..EngineConfig::default() }).await;
    let (client, _) = producer(&engine).await;
    for index in 0..7u8 {
        client.register(Envelope::dispatch("audit.login", "login", Bytes::from(vec![index]))).await.unwrap();
        client.register(Envelope::dispatch("noise", "x", Bytes::new())).await.unwrap();
    }
    client.register_committed(work("audit.jobs")).await.unwrap();
    drop(client);
    engine.shutdown().await.unwrap();
    let mut events = Vec::new();
    let mut files: Vec<_> = std::fs::read_dir(&directory).unwrap().map(|entry| entry.unwrap().path()).filter(|path| path.extension().is_some_and(|extension| extension == "rvs")).collect();
    files.sort();
    for file in &files {
        events.extend(ruvent::snapshot::read(file).unwrap());
    }
    assert_eq!(files.len(), 3);
    assert_eq!(events.len(), 8);
    assert!(events.iter().all(|event| event.channel.starts_with("audit.")));
    let stats = engine.snapshot_stats().unwrap();
    assert_eq!(stats.written.load(std::sync::atomic::Ordering::Relaxed), 8);
}
