//! When answers and notices leave relative to the disk, and what a restart finds.

use crate::support::*;
use ruvent::{connect, Bytes, Call, Compaction, Durability, Engine, EngineConfig, Envelope, FileJournal, Incoming, Methods, Outbound, Pattern, Recovered, Refusal, Reply, SnapshotConfig};
use std::time::Duration;

fn strict() -> EngineConfig {
    EngineConfig { durability: Durability::Strict, ..EngineConfig::default() }
}

/// Under Strict a claimable registration's answer and notice wait for the
/// disk; a claim on another connection does not.
#[tokio::test]
async fn under_strict_registration_waits_for_the_disk_and_a_claim_does_not() {
    let gate = Gate::new();
    let engine = Engine::start(strict(), gate.clone(), Recovered::default()).await.unwrap();
    let (producer, _) = connect(&engine, "svc", 64).await.unwrap();
    let (watcher, mut watched) = connect(&engine, "svc", 64).await.unwrap();
    let (claimer, _) = connect(&engine, "svc", 64).await.unwrap();
    watcher.subscribe("work".into(), None).await.unwrap();

    // A first event, made durable.
    let first = tokio::spawn({
        let producer = producer.clone();
        async move { producer.register_claimable(work("work")).await }
    });
    assert!(stays_quiet(&mut watched).await, "no notice before the write");
    assert!(!first.is_finished(), "no answer before the write");
    gate.release(1);
    let (first_id, _) = first.await.unwrap().unwrap();
    assert!(matches!(next_event(&mut watched).await, Some(Incoming::Notice(_))));

    // A second registration is held; meanwhile a claim on another connection is answered.
    let second = tokio::spawn({
        let producer = producer.clone();
        async move { producer.register_claimable(work("work")).await }
    });
    let claimed = tokio::time::timeout(Duration::from_secs(2), claimer.claim(first_id, 1, Duration::from_secs(30))).await;
    assert!(matches!(claimed, Ok(Ok(_))), "a claim never waits for the disk: {claimed:?}");
    gate.release(8);
    second.await.unwrap().unwrap();
}

/// Strict: a completion's dispatch follow-up A, then a direct dispatch B to
/// the same subscriber, arrive as A then B, though only A waited for the disk.
#[tokio::test]
async fn under_strict_one_connection_receives_in_processing_order() {
    let gate = Gate::new();
    let engine = Engine::start(strict(), gate.clone(), Recovered::default()).await.unwrap();
    let (watcher, mut watched) = connect(&engine, "svc", 64).await.unwrap();
    watcher.subscribe("feed".into(), None).await.unwrap();
    // A raw connection: each call is in the engine's inbox when `call` returns,
    // so the completion is processed before the direct dispatch.
    let (worker, mut answers) = engine.open("svc").await.unwrap();
    worker.call(1, Call::RegisterClaimable(work("work"))).await.unwrap();
    gate.release(1);
    let Some(Outbound::Reply { reply: Reply::Registered { id, .. }, .. }) = answers.recv().await else { panic!("not registered") };
    worker.call(2, Call::Claim { id, worker: 1, lease_ms: 30_000 }).await.unwrap();
    let Some(Outbound::Reply { reply: Reply::Claimed(claimed), .. }) = answers.recv().await else { panic!("not claimed") };
    let token = claimed.claim.unwrap().token;
    worker.call(3, Call::Complete { id, token, follow_ups: vec![Envelope::dispatch("feed", "A", Bytes::new())] }).await.unwrap();
    worker.call(4, Call::RegisterDispatch(Envelope::dispatch("feed", "B", Bytes::new()))).await.unwrap();
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
/// announced to nobody and answered to nobody.
#[tokio::test]
async fn a_failed_write_stops_the_engine_and_releases_nothing_it_carried() {
    let engine = Engine::start(strict(), Failing { good: 1 }, Recovered::default()).await.unwrap();
    let (producer, _) = connect(&engine, "svc", 64).await.unwrap();
    let (watcher, mut watched) = connect(&engine, "svc", 64).await.unwrap();
    watcher.subscribe("work".into(), None).await.unwrap();
    assert_eq!(producer.register_claimable(work("work")).await, Err(Refusal::Closed));
    let stopped = engine.stopped().await.unwrap_err();
    assert!(stopped.to_string().contains("disk on fire"), "{stopped}");
    assert_eq!(next_event(&mut watched).await, Some(Incoming::Closed), "the subscriber sees the connection end, not the notice");
}

/// A connection whose send buffer is full is cut off; the others go on.
#[tokio::test]
async fn a_connection_that_does_not_read_is_cut_off_and_the_others_carry_on() {
    let engine = Engine::volatile(EngineConfig { outbound: 4, ..EngineConfig::default() }).await;
    let (stalled, mut stalled_out) = engine.open("svc").await.unwrap();
    stalled.call(1, ruvent::Call::Subscribe { channel: "feed".into(), after: None }).await.unwrap();
    let (producer, _) = connect(&engine, "svc", 64).await.unwrap();
    for _ in 0..10 {
        producer.register_dispatch(Envelope::dispatch("feed", "t", Bytes::new())).await.unwrap();
    }
    let (id, _) = producer.register_claimable(work("work")).await.unwrap();
    assert!(producer.claim(id, 1, Duration::from_secs(1)).await.is_ok(), "the engine never waited on the stalled connection");
    let mut received = 0;
    while stalled_out.recv().await.is_some() {
        received += 1;
    }
    assert_eq!(received, 4, "what fit in the buffer, then the end of the connection");
}

/// What was waiting comes back after a restart, in order; what was claimed
/// comes back waiting, with a new id and one more retry.
#[tokio::test]
async fn a_restart_reloads_the_queue_in_order_and_reissues_claimed_events() {
    let directory = scratch("restart");
    let path = directory.join("events.journal");
    let (ids, claimed_id) = {
        let (journal, recovered) = FileJournal::open(&path).unwrap();
        let engine = Engine::start(EngineConfig::default(), journal, recovered).await.unwrap();
        let (client, _) = connect(&engine, "svc", 64).await.unwrap();
        let mut ids = Vec::new();
        for index in 0..3u8 {
            ids.push(client.register_claimable(Envelope::claimable("work", "do", Bytes::from(vec![index]))).await.unwrap().0);
        }
        client.claim(ids[1], 1, Duration::from_secs(60)).await.unwrap();
        drop(client);
        engine.shutdown().await.unwrap();
        let claimed = ids[1];
        (ids, claimed)
    };
    let (journal, recovered) = FileJournal::open(&path).unwrap();
    let engine = Engine::start(EngineConfig::default(), journal, recovered).await.unwrap();
    let (client, _) = connect(&engine, "svc", 64).await.unwrap();
    let backlog = client.subscribe("work".into(), None).await.unwrap();
    let seen: Vec<(u16, bool)> = backlog.notices.iter().map(|notice| (notice.retry, ids.contains(&notice.id))).collect();
    assert_eq!(seen, [(0, true), (1, false), (0, true)]);
    assert!(!backlog.notices.iter().any(|notice| notice.id == claimed_id));
    engine.shutdown().await.unwrap();
}

/// Under Strict, an answered registration survives the process dying right after.
#[test]
fn under_strict_an_answered_registration_survives_a_crash() {
    let path = scratch("crash").join("events.journal");
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let (journal, recovered) = FileJournal::open(&path).unwrap();
        let engine = Engine::start(strict(), journal, recovered).await.unwrap();
        let (client, _) = connect(&engine, "svc", 64).await.unwrap();
        client.register_claimable(work("work")).await.unwrap();
    });
    // No shutdown: the runtime and every task in it are dropped.
    drop(runtime);
    let (_, recovered) = FileJournal::open(&path).unwrap();
    assert!(recovered.records.iter().any(|record| matches!(record, ruvent::Record::Register(_))));
}

/// Once superseded records outnumber live ones, the journal is rewritten, and
/// the rewritten file restarts to the same queue.
#[tokio::test]
async fn the_journal_is_compacted_and_the_compacted_file_restarts() {
    let path = scratch("compact").join("events.journal");
    let config = EngineConfig { compaction: Compaction { min_records: 20, ratio: 2 }, ..EngineConfig::default() };
    {
        let (journal, recovered) = FileJournal::open(&path).unwrap();
        let engine = Engine::start(config.clone(), journal, recovered).await.unwrap();
        let (client, _) = connect(&engine, "svc", 64).await.unwrap();
        for _ in 0..200 {
            let (id, _) = client.register_claimable(work("work")).await.unwrap();
            let event = client.claim(id, 1, Duration::from_secs(60)).await.unwrap();
            client.complete(id, event.claim.unwrap().token, Vec::new()).await.unwrap();
        }
        client.register_claimable(work("kept")).await.unwrap();
        drop(client);
        engine.shutdown().await.unwrap();
    }
    let (journal, recovered) = FileJournal::open(&path).unwrap();
    assert!(recovered.records.len() < 100, "compacted: {} records left of about 600 written", recovered.records.len());
    let engine = Engine::start(config, journal, recovered).await.unwrap();
    let (client, _) = connect(&engine, "svc", 64).await.unwrap();
    assert_eq!(client.subscribe("kept".into(), None).await.unwrap().notices.len(), 1);
    assert!(client.subscribe("work".into(), None).await.unwrap().notices.is_empty());
}

/// The operational snapshot collects the chosen channels in units and writes
/// the last unit on shutdown. It is not replayed.
#[tokio::test]
async fn chosen_channels_are_collected_into_snapshot_files() {
    let directory = scratch("snapshot");
    let snapshot = SnapshotConfig { max_events: 3, max_age: Duration::from_secs(3600), ..SnapshotConfig::new(directory.clone(), vec![Pattern("audit.*".into())]) };
    let engine = Engine::volatile(EngineConfig { snapshot: Some(snapshot), ..EngineConfig::default() }).await;
    let (client, _) = connect(&engine, "svc", 64).await.unwrap();
    for index in 0..7u8 {
        client.register_dispatch(Envelope::dispatch("audit.login", "login", Bytes::from(vec![index]))).await.unwrap();
        client.register_dispatch(Envelope::dispatch("noise", "x", Bytes::new())).await.unwrap();
    }
    client.register_claimable(work("audit.jobs")).await.unwrap();
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
