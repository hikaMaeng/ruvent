//! Storage under a running engine: segments, checkpoints, the read path,
//! relocation, streamed bodies, crashes (2.5, 2.6, M8-4).

use crate::support::*;
use ruvent::{
    async_trait, connect, Batch, Bytes, Call, CheckpointHead, Checkpoints, Engine, EngineConfig, Envelope, EventId, Hello, Journal, MemoryConfig, Methods, Outbound, Part, Parts, SegmentLog,
    StorageConfig, Written,
};
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn payload(index: u32, len: usize) -> Bytes {
    let unit = format!("{index:08}");
    Bytes::from(unit.repeat(len / unit.len()).into_bytes())
}

/// After a crash, recovery holds metadata and no payload; the payloads come
/// back through the read path, checked, and the next ones of a lane are read
/// ahead (2.5, M3).
#[test]
fn recovery_holds_metadata_only_and_payloads_come_back_through_reads() {
    let path = scratch("cold").join("journal");
    let mut ids = BTreeMap::new();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let (journal, recovered) = SegmentLog::open(&path).unwrap();
        let engine = Engine::start(EngineConfig::default(), journal, recovered).await.unwrap();
        let (producer, _) = producer(&engine).await;
        for index in 0..24u32 {
            let (_, id) = producer.register_committed(Envelope::claimable(ch("work"), 1, payload(index, 128 * 1024))).await.unwrap();
            ids.insert(id, index);
        }
    });
    // No shutdown: the process dies.
    drop(runtime);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let (journal, recovered) = SegmentLog::open(&path).unwrap();
        let engine = Engine::start(EngineConfig::default(), journal, recovered).await.unwrap();
        let used = engine.pressure().memory_used;
        assert!(used < 64 * 1024, "{used} managed bytes after recovering 3 MiB of payloads: metadata only");
        let (worker, mut assignments) = worker(&engine).await;
        let worker_join = worker.join(WORKER, Some(ch("work")), Vec::new(), Duration::from_secs(60), false).await.unwrap();
        worker.window(worker_join, 1, 0).await.unwrap();
        for _ in 0..ids.len() {
            let assignment = next_assignment(&mut assignments).await;
            let index = ids[&assignment.event.id];
            assert_eq!(assignment.event.payload, payload(index, 128 * 1024), "event {index} came back whole");
            let seq = worker.complete(assignment.event.id, WORKER, Vec::new()).await.unwrap();
            worker.committed(seq).await.unwrap();
        }
        let (observer, _) = observer(&engine).await;
        assert!(observer.stats().await.unwrap().counters.read_ahead > 0, "the lane was read ahead");
    });
}

/// A checkpoint writer that, once told to, stalls in its first write and
/// never touches the disk again: a walk that is still running when the
/// process dies.
struct Stalling {
    inner: Box<dyn Checkpoints>,
    stall: Arc<AtomicBool>,
    began: Arc<tokio::sync::Notify>,
    release: Arc<std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>>,
    stalled: bool,
}

impl Checkpoints for Stalling {
    fn begin(&mut self, head: &CheckpointHead) -> io::Result<()> {
        if self.stall.load(Ordering::SeqCst) {
            self.stalled = true;
            self.began.notify_one();
            return Ok(());
        }
        self.inner.begin(head)
    }

    fn write(&mut self, part: &Part) -> io::Result<()> {
        if self.stalled {
            let release = self.release.lock().unwrap().take();
            if let Some(release) = release {
                let _ = release.recv();
            }
            return Err(io::Error::other("the process died"));
        }
        self.inner.write(part)
    }

    fn finish(&mut self) -> io::Result<u64> {
        if self.stalled {
            return Err(io::Error::other("the process died"));
        }
        self.inner.finish()
    }

    fn abort(&mut self) {
        if !self.stalled {
            self.inner.abort();
        }
    }
}

struct StallingLog {
    inner: SegmentLog,
    checkpoints: Option<Stalling>,
}

#[async_trait]
impl Journal for StallingLog {
    async fn append(&mut self, batch: Batch) -> io::Result<Written> {
        self.inner.append(batch).await
    }

    fn parts(&mut self) -> Parts {
        let mut parts = self.inner.parts();
        if let (Some(inner), Some(mut stalling)) = (parts.checkpoints.take(), self.checkpoints.take()) {
            stalling.inner = inner;
            parts.checkpoints = Some(Box::new(stalling));
        }
        parts
    }
}

/// M8-4: segment relocation and removal, reads of cold payloads and the
/// checkpoint walk overlap; then the process dies while a walk is running.
/// Everything committed comes back, with its payload, and nothing completed
/// does (2.5).
#[test]
fn relocation_removal_reads_and_a_walk_overlap_and_a_crash_mid_walk_loses_nothing() {
    let path = scratch("m8-4").join("journal");
    let storage = StorageConfig { segment_bytes: 4096, checkpoint_slice: 1, checkpoint_queue: 1, disk_capacity: Some(1 << 20), read_ahead: 4, ..StorageConfig::default() };
    let memory = MemoryConfig { limit: Some(40 * 1024), ..MemoryConfig::default() };
    let config = EngineConfig { storage: storage.clone(), memory, ..EngineConfig::default() };
    let stall = Arc::new(AtomicBool::new(false));
    let began = Arc::new(tokio::sync::Notify::new());
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let release = Arc::new(std::sync::Mutex::new(Some(release_rx)));
    let mut live: BTreeMap<EventId, u32> = BTreeMap::new();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let (inner, recovered) = SegmentLog::open_with(&path, storage.segment_bytes).unwrap();
        let checkpoints = Stalling { inner: Box::new(NoCheckpoints), stall: stall.clone(), began: began.clone(), release: release.clone(), stalled: false };
        let engine = Engine::start(config.clone(), StallingLog { inner, checkpoints: Some(checkpoints) }, recovered).await.unwrap();
        let (producer, _) = producer(&engine).await;
        let (worker, mut assignments) = worker(&engine).await;
        let worker_join = worker.join(WORKER, Some(ch("work")), Vec::new(), Duration::from_secs(60), false).await.unwrap();
        worker.window(worker_join, 2, 0).await.unwrap();
        let mut index = 0u32;
        for round in 0..400u32 {
            let channel = if round % 10 == 0 { "slow" } else { "work" };
            let (_, id) = producer.register_committed(Envelope::claimable(ch(channel), 1, payload(index, 1024))).await.unwrap();
            live.insert(id, index);
            index += 1;
            if channel == "work" {
                let assignment = next_assignment(&mut assignments).await;
                assert_eq!(assignment.event.payload, payload(live[&assignment.event.id], 1024), "a hot or cold payload, whole");
                let seq = worker.complete(assignment.event.id, WORKER, Vec::new()).await.unwrap();
                worker.committed(seq).await.unwrap();
                live.remove(&assignment.event.id);
            }
        }
        let (observer, _) = observer(&engine).await;
        let counters = observer.stats().await.unwrap().counters;
        assert!(counters.checkpoints > 0, "{counters:?}");
        assert!(counters.segments_removed > 0, "{counters:?}");
        assert!(counters.relocated > 0, "slow events kept sparse segments alive: {counters:?}");
        assert!(counters.released > 0, "memory pressure released cached payloads: {counters:?}");
        // From here a walk never finishes. Grow the tail until one begins,
        // then keep changing the ledger while it runs.
        stall.store(true, Ordering::SeqCst);
        let begun = began.notified();
        tokio::pin!(begun);
        loop {
            let (_, id) = producer.register_committed(Envelope::claimable(ch("work"), 1, payload(index, 1024))).await.unwrap();
            live.insert(id, index);
            index += 1;
            let assignment = next_assignment(&mut assignments).await;
            let seq = worker.complete(assignment.event.id, WORKER, Vec::new()).await.unwrap();
            worker.committed(seq).await.unwrap();
            live.remove(&assignment.event.id);
            if tokio::time::timeout(Duration::from_millis(1), &mut begun).await.is_ok() {
                break;
            }
        }
        drop(worker);
        for _ in 0..20 {
            let (_, id) = producer.register_committed(Envelope::claimable(ch("slow"), 1, payload(index, 1024))).await.unwrap();
            live.insert(id, index);
            index += 1;
        }
    });
    // The process dies with the walk in its first slice.
    runtime.shutdown_background();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let (journal, recovered) = SegmentLog::open_with(&path, storage.segment_bytes).unwrap();
        let engine = Engine::start(config, journal, recovered).await.unwrap();
        let (observer, _) = observer(&engine).await;
        let mut back = BTreeSet::new();
        for channel in ["slow", "work"] {
            let backlog = observer.subscribe(ch(channel), None).await.unwrap();
            assert!(!backlog.more);
            back.extend(backlog.notices.iter().map(|notice| notice.id));
        }
        assert_eq!(back, live.keys().copied().collect::<BTreeSet<_>>(), "everything committed and not completed, and nothing else");
        let (worker, mut assignments) = worker(&engine).await;
        let worker_join = worker.join(WORKER, Some(ch("slow")), Vec::new(), Duration::from_secs(60), false).await.unwrap();
        worker.window(worker_join, 4, 0).await.unwrap();
        for _ in 0..live.len() {
            let assignment = next_assignment(&mut assignments).await;
            assert_eq!(assignment.event.payload, payload(live[&assignment.event.id], 1024), "the payload, wherever relocation left it");
            let seq = worker.complete(assignment.event.id, WORKER, Vec::new()).await.unwrap();
            worker.committed(seq).await.unwrap();
        }
    });
    drop(release_tx);
}

/// A placeholder the stalling writer replaces with the log's own.
struct NoCheckpoints;

impl Checkpoints for NoCheckpoints {
    fn begin(&mut self, _head: &CheckpointHead) -> io::Result<()> {
        Ok(())
    }
    fn write(&mut self, _part: &Part) -> io::Result<()> {
        Ok(())
    }
    fn finish(&mut self) -> io::Result<u64> {
        Ok(0)
    }
    fn abort(&mut self) {}
}

async fn spool(engine: &Engine, body: &'static [u8]) -> ruvent::Spooled {
    let spool = engine.spool().expect("a segment log has a spool");
    tokio::task::spawn_blocking(move || {
        let mut write = spool.receive(body.len() as u32)?;
        write.write(&body[..body.len() / 2])?;
        write.write(&body[body.len() / 2..])?;
        write.finish()
    })
    .await
    .unwrap()
    .unwrap()
}

/// A completion whose follow-ups' payloads the connection owner received to
/// disk: each goes out read from disk, and survives a restart pointing at its
/// body (2.6 item 4). Every claimable event is durable: there are no volatile
/// types (plan 9.15).
#[tokio::test]
async fn streamed_follow_up_bodies_go_out_from_disk_and_survive_a_restart() {
    let path = scratch("streamed").join("journal");
    let labels = ["next", "other"];
    {
        let (journal, recovered) = SegmentLog::open(&path).unwrap();
        let engine = Engine::start(EngineConfig::default(), journal, recovered).await.unwrap();
        let (producer, _) = producer(&engine).await;
        producer.register_committed(work("work")).await.unwrap();
        let (session, mut out) = engine.open(Hello::worker("svc")).await.unwrap();
        assert!(matches!(out.recv().await, Some(Outbound::Welcome(_))));
        session.call(Call::Join { req: 1, worker: WORKER, channel: Some(ch("work")), types: Vec::new(), lease_ms: 30_000, dispatch: false }).await.unwrap();
        session.call(Call::Window { join: 1, count: 1, bytes: 0 }).await.unwrap();
        let assignment = loop {
            if let Some(Outbound::Assign(assignment)) = out.recv().await {
                break assignment;
            }
        };
        let first = spool(&engine, b"a body received to disk").await;
        let second = spool(&engine, b"another body received to disk").await;
        let follow_ups = vec![Envelope::claimable(ch(labels[0]), 1, Bytes::new()), Envelope::claimable(ch(labels[1]), 1, Bytes::new())];
        session.call(Call::Complete { seq: 1, id: assignment.event.id, worker: WORKER, follow_ups, spooled: vec![(0, first), (1, second)] }).await.unwrap();
        loop {
            if let Some(Outbound::Committed { seq: 1 }) = out.recv().await {
                break;
            }
        }
        let (taker, mut taken) = connect(&engine, Hello::worker("svc"), 64).await.unwrap();
        let mut taker_joins = Vec::new();
        for label in labels {
            let taker_join = taker.join(TAKER, Some(ch(label)), Vec::new(), Duration::from_secs(60), false).await.unwrap();
            taker.window(taker_join, 1, 0).await.unwrap();
            taker_joins.push(taker_join);
        }
        let mut bodies = BTreeMap::new();
        let mut held = Vec::new();
        for _ in 0..2 {
            let assignment = next_assignment(&mut taken).await;
            let label = if assignment.event.channel == ch(labels[0]) { labels[0] } else { labels[1] };
            bodies.insert(label, assignment.event.payload.clone());
            held.push(assignment);
        }
        // Given back with the windows shut, so they wait across the restart
        // instead of staying claimed.
        for taker_join in taker_joins {
            taker.window(taker_join, 0, 0).await.unwrap();
        }
        for assignment in held {
            taker.release(assignment.event.id, TAKER, Duration::ZERO, false).await.unwrap();
        }
        assert_eq!(bodies[labels[0]], Bytes::from_static(b"a body received to disk"));
        assert_eq!(bodies[labels[1]], Bytes::from_static(b"another body received to disk"));
        drop((taker, session, producer));
        engine.shutdown().await.unwrap();
    }
    let (journal, recovered) = SegmentLog::open(&path).unwrap();
    let engine = Engine::start(EngineConfig::default(), journal, recovered).await.unwrap();
    let (worker, mut assignments) = worker(&engine).await;
    let worker_join = worker.join(WORKER, None, vec![1], Duration::from_secs(60), false).await.unwrap();
    worker.window(worker_join, 2, 0).await.unwrap();
    let mut bodies = vec![next_assignment(&mut assignments).await.event.payload, next_assignment(&mut assignments).await.event.payload];
    bodies.sort();
    assert_eq!(bodies, [Bytes::from_static(b"a body received to disk"), Bytes::from_static(b"another body received to disk")]);
}

/// `body_to_disk`: a follow-up's body goes to disk once managed memory is at
/// the high watermark (2.6 item 4). No spool, no disk.
#[tokio::test]
async fn body_to_disk_says_yes_at_the_watermark_and_never_without_a_spool() {
    let path = scratch("pressure").join("journal");
    let memory = MemoryConfig { limit: Some(10_000), slot_bytes: 100, ..MemoryConfig::default() };
    let (journal, recovered) = SegmentLog::open(&path).unwrap();
    let engine = Engine::start(EngineConfig { memory: memory.clone(), ..EngineConfig::default() }, journal, recovered).await.unwrap();
    assert!(!engine.body_to_disk(100));
    let (producer, _) = producer(&engine).await;
    // Metadata is never released: 80 events of 100 counted bytes pass the 7,200-byte watermark.
    for _ in 0..80 {
        producer.register_committed(Envelope::claimable(ch("pressure"), 1, Bytes::new())).await.unwrap();
    }
    let pressure = engine.pressure();
    assert!(pressure.memory_used >= pressure.memory_high, "{pressure:?}");
    assert!(engine.body_to_disk(10), "at the high watermark");
    let volatile = Engine::volatile(EngineConfig { memory, ..EngineConfig::default() }).await;
    assert!(!volatile.body_to_disk(u64::MAX / 2) && volatile.spool().is_none());
}
