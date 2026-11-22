//! What reaches the disk, what a restart gets back, what a crash may lose,
//! and the guarantees that keep one journal write from landing on another.

use async_trait::async_trait;
use ruvent::{Begin, Bytes, Command, Compaction, Durability, Engine, EngineConfig, Envelope, Events, FileJournal, From, Journal, Kind, Outcome, Persist, Record, Recovered};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

const NOW: Option<Duration> = Some(Duration::ZERO);

fn scratch(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("ruvent-{}-{name}.journal", std::process::id()));
    for stale in [path.clone(), path.with_extension("journal.lock"), path.with_extension("journal.tmp")] {
        let _ = std::fs::remove_file(stale);
    }
    path
}

fn command(key: &str) -> Command {
    Command::new("job.run", "requests", "{}").transaction_key(key).reply_to("replies")
}

/// Never writes on its own: only an explicit flush reaches the disk.
fn manual() -> EngineConfig {
    EngineConfig { durability: Durability::Scheduled { window: Duration::from_secs(86_400) }, ..EngineConfig::default() }
}

async fn run_one(engine: &Engine, queue: &str) {
    let delivery = engine.dequeue(queue, 1, None, NOW).await.unwrap().remove(0);
    let Begin::Claimed { command, attempt, .. } = engine.begin(queue, delivery.id).await.unwrap() else { panic!("expected a claim") };
    assert!(engine.finish(command, attempt, Outcome::Ok(Bytes::from_static(b"\"ok\"")), queue, delivery.id).await.unwrap());
}

async fn owed(engine: &Engine, queue: &str) -> Vec<String> {
    engine.dequeue(queue, 100, None, NOW).await.unwrap().iter().map(|delivery| delivery.command.transaction_key.to_string()).collect()
}

#[tokio::test]
async fn a_restart_gets_back_the_work_still_owed_and_how_often_it_was_tried() {
    let path = scratch("restart");
    let engine = Engine::open(&path, EngineConfig { max_attempts: 2, ..EngineConfig::default() }).await.unwrap();
    for key in ["t1", "t2", "t3"] {
        engine.submit("q", command(key)).await.unwrap();
    }
    run_one(&engine, "q").await;
    // t2 is being run when the process stops.
    let running = engine.dequeue("q", 1, None, NOW).await.unwrap().remove(0);
    let Begin::Claimed { command: before, attempts, .. } = engine.begin("q", running.id).await.unwrap() else { panic!() };
    assert_eq!((&*before.transaction_key, attempts), ("t2", 1));
    engine.shutdown().await.unwrap();
    assert!(engine.submit("q", command("t9")).await.is_err(), "a stopped engine accepts nothing");

    let engine = Engine::open(&path, EngineConfig { max_attempts: 2, ..EngineConfig::default() }).await.unwrap();
    // t1 finished and is gone. t2 and t3 are owed again, held by nobody.
    let again = engine.dequeue("q", 10, None, NOW).await.unwrap();
    assert_eq!(again.iter().map(|delivery| &*delivery.command.transaction_key).collect::<Vec<_>>(), ["t2", "t3"]);

    // The attempt made before the restart still counts.
    let Begin::Claimed { command: after, attempts, .. } = engine.begin("q", again[0].id).await.unwrap() else { panic!() };
    assert_eq!(attempts, 2);
    // A new incarnation issues positions above everything issued before it.
    assert!(after.position > before.position + (1 << 39), "epoch must lift positions: {} then {}", before.position, after.position);
    // Events are not kept by default: the log starts empty after a restart,
    // and a reader resuming from an old cursor is told it may have missed some.
    assert_eq!(engine.subscribe(vec!["replies".to_owned()], From::Start).poll(10).len(), 0);
    let mut resumed = engine.subscribe(vec!["replies".to_owned()], From::Position(before.position));
    assert!(resumed.lagged());
    assert!(resumed.poll(10).is_empty());
    assert!(!engine.subscribe(vec!["replies".to_owned()], From::Position(after.position)).lagged(), "a cursor from this incarnation has missed nothing");
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn work_that_comes_and_goes_between_two_writes_is_never_written() {
    let path = scratch("net");
    let engine = Engine::open(&path, manual()).await.unwrap();
    engine.flush().await.unwrap();
    assert_eq!(engine.stats().journal_records, 1, "the epoch, written before any operation is served");

    // A thousand commands submitted, run and finished — and one left waiting.
    for index in 0..1000 {
        engine.submit("q", command(&format!("t{index}"))).await.unwrap();
        run_one(&engine, "q").await;
    }
    engine.submit("q", command("waiting")).await.unwrap();
    assert!(engine.stats().journal_lag > 0);
    engine.flush().await.unwrap();
    assert_eq!(engine.stats().journal_records, 2, "one record: the message still owed. The thousand that finished left no trace");
    assert_eq!(engine.stats().journal_lag, 0);

    // When that one finishes, the journal is told it is gone.
    run_one(&engine, "q").await;
    engine.flush().await.unwrap();
    assert_eq!(engine.stats().journal_records, 3);
    engine.shutdown().await.unwrap();

    let engine = Engine::open(&path, manual()).await.unwrap();
    assert!(owed(&engine, "q").await.is_empty());
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_crash_loses_only_what_changed_after_the_last_completed_write() {
    let path = scratch("crash");
    let engine = Engine::open(&path, manual()).await.unwrap();
    engine.submit("q", command("written")).await.unwrap();
    engine.flush().await.unwrap();
    engine.submit("q", command("unwritten")).await.unwrap();
    assert_eq!(engine.stats().journal_lag, 1, "one change is acknowledged but not yet on disk");

    // The file as a crash at this instant would leave it.
    let image = scratch("crash-image");
    std::fs::copy(&path, &image).unwrap();

    let recovered = Engine::open(&image, manual()).await.unwrap();
    assert_eq!(owed(&recovered, "q").await, ["written"]);
    recovered.shutdown().await.unwrap();
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn settled_results_and_events_survive_a_restart_only_where_asked() {
    let path = scratch("policy");
    let config = || EngineConfig { persist: Persist { settled: true, events: Events::Channels(vec!["keep.".to_owned()]), ..Persist::default() }, ..EngineConfig::default() };
    let engine = Engine::open(&path, config()).await.unwrap();
    engine.submit("q", command("t1")).await.unwrap();
    run_one(&engine, "q").await;
    let fact = |channel: &str, kind: Kind| Envelope::draft(kind, "noted", channel, "{}");
    engine.append(vec![fact("keep.a", Kind::Fact), fact("keep.a", Kind::Progress), fact("temp.a", Kind::Fact)]).await.unwrap();
    engine.shutdown().await.unwrap();

    let engine = Engine::open(&path, config()).await.unwrap();
    let kept = engine.subscribe(vec!["keep.a".to_owned(), "temp.a".to_owned()], From::Start).poll(10);
    assert_eq!(kept.iter().map(|event| (&*event.channel, event.kind)).collect::<Vec<_>>(), [("keep.a", Kind::Fact)], "only facts on the named channels; progress never");
    assert!(!engine.subscribe(vec!["keep.a".to_owned()], From::Position(kept[0].position)).lagged(), "a cursor at the last event that survived has missed nothing");

    // The finished execution is remembered: sending it again does not rerun it.
    engine.submit("q", command("t1")).await.unwrap();
    let again = engine.dequeue("q", 1, None, NOW).await.unwrap().remove(0);
    assert!(matches!(engine.begin("q", again.id).await.unwrap(), Begin::Settled));
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_transient_queue_is_never_mentioned_to_the_journal() {
    let path = scratch("transient");
    let config = || EngineConfig { persist: Persist { transient_queues: ["scratch".into()].into(), ..Persist::default() }, ..manual() };
    let engine = Engine::open(&path, config()).await.unwrap();
    engine.submit("scratch", command("a")).await.unwrap();
    let delivery = engine.dequeue("scratch", 1, None, NOW).await.unwrap().remove(0);
    assert!(matches!(engine.begin("scratch", delivery.id).await.unwrap(), Begin::Claimed { .. }));
    engine.flush().await.unwrap();
    assert_eq!(engine.stats().journal_records, 1, "nothing but the epoch: neither the message nor its execution");

    engine.submit("kept", command("b")).await.unwrap();
    engine.shutdown().await.unwrap();

    let engine = Engine::open(&path, config()).await.unwrap();
    assert!(owed(&engine, "scratch").await.is_empty());
    assert_eq!(owed(&engine, "kept").await, ["b"]);
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_torn_tail_is_cut_off_and_everything_before_it_survives() {
    let path = scratch("torn");
    let engine = Engine::open(&path, EngineConfig::default()).await.unwrap();
    engine.submit("q", command("t1")).await.unwrap();
    engine.shutdown().await.unwrap();
    let intact = std::fs::metadata(&path).unwrap().len();

    // Half a frame: a plausible header, then the power goes.
    let mut file = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
    file.write_all(&[200, 0, 0, 0, 1, 2, 3, 4, 1, 9, 9, 9]).unwrap();
    drop(file);

    let engine = Engine::open(&path, EngineConfig::default()).await.unwrap();
    assert_eq!(owed(&engine, "q").await, ["t1"]);
    engine.shutdown().await.unwrap();
    // Cut back to the intact prefix, then extended by exactly this run's epoch record.
    let reopened = std::fs::metadata(&path).unwrap().len();
    assert!(reopened > intact && reopened < intact + 64, "intact {intact}, after reopen {reopened}");
}

#[tokio::test]
async fn a_journal_cannot_be_opened_twice_or_written_out_of_turn() {
    let path = scratch("guard");
    let (mut journal, recovered) = FileJournal::open(&path).unwrap();
    assert_eq!(recovered.through, 0);

    let held = FileJournal::open(&path).err().expect("a second holder must be refused");
    assert_eq!(held.kind(), io::ErrorKind::WouldBlock);

    let record = || vec![Record::Epoch(1)];
    assert!(journal.append(5, 6, &record()).await.is_err(), "a batch that skips ahead must be refused");
    journal.append(0, 1, &record()).await.unwrap();
    assert!(journal.append(0, 1, &record()).await.is_err(), "a batch that would overwrite must be refused");
    journal.append(1, 2, &record()).await.unwrap();
    drop(journal);

    let (_journal, recovered) = FileJournal::open(&path).unwrap();
    assert_eq!((recovered.records.len(), recovered.through), (2, 2), "the refused batches left no trace");
}

/// A slow journal that records how many writes were ever in progress at once.
struct Probe {
    active: Arc<AtomicUsize>,
    most: Arc<AtomicUsize>,
    writes: Arc<AtomicU64>,
    written: Arc<AtomicU64>,
    through: u64,
}

impl Probe {
    async fn write(&mut self, records: u64) {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.most.fetch_max(active, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(3)).await;
        self.writes.fetch_add(1, Ordering::SeqCst);
        self.written.fetch_add(records, Ordering::SeqCst);
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl Journal for Probe {
    async fn append(&mut self, prev: u64, through: u64, records: &[Record]) -> io::Result<()> {
        assert_eq!(prev, self.through, "batches must arrive in turn");
        assert_eq!(through - prev, records.len() as u64);
        self.write(records.len() as u64).await;
        self.through = through;
        Ok(())
    }

    async fn rewrite(&mut self, at: u64, _records: &[Record]) -> io::Result<()> {
        assert_eq!(at, self.through, "a snapshot must be taken at the journal's end");
        self.write(0).await;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn journal_writes_never_overlap_however_eagerly_they_are_wanted() {
    let (active, most, writes, written) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)), Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
    let probe = Probe { active, most: most.clone(), writes: writes.clone(), written: written.clone(), through: 0 };
    // A zero window wants a write the instant anything changes, and the tiny
    // compaction threshold wants snapshots in between. A schedule alone would
    // start hundreds of writes on top of each other.
    let config = EngineConfig { durability: Durability::Scheduled { window: Duration::ZERO }, compaction: Compaction { min_records: 64, ratio: 1 }, ..EngineConfig::default() };
    let engine = Engine::start(config, probe, Recovered::default());

    let mut producers = Vec::new();
    for producer in 0..32 {
        let engine = engine.clone();
        producers.push(tokio::spawn(async move {
            for index in 0..250 {
                engine.submit(format!("q{producer}"), command(&format!("t{producer}-{index}"))).await.unwrap();
                if index % 3 == 0 {
                    let delivery = engine.dequeue(format!("q{producer}"), 1, None, NOW).await.unwrap().remove(0);
                    engine.ack(format!("q{producer}"), delivery.id).await.unwrap();
                }
            }
        }));
    }
    for producer in producers {
        producer.await.unwrap();
    }
    engine.flush().await.unwrap();

    let stats = engine.stats();
    assert_eq!(most.load(Ordering::SeqCst), 1, "more than one journal write was in progress at once");
    assert_eq!(written.load(Ordering::SeqCst), stats.journal_records, "every record issued was written exactly once");
    assert_eq!(writes.load(Ordering::SeqCst), stats.journal_writes + stats.compactions);
    assert!(stats.compactions > 0, "the snapshot path was exercised too");
    assert!(stats.journal_writes < 8000, "a slow journal must make writes larger, not more numerous: {} writes for 8000 submissions", stats.journal_writes);
    assert_eq!(stats.journal_lag, 0);
}

#[tokio::test]
async fn the_journal_folds_itself_when_it_is_mostly_superseded() {
    let path = scratch("compact");
    let config = || EngineConfig { compaction: Compaction { min_records: 100, ratio: 4 }, ..manual() };
    let engine = Engine::open(&path, config()).await.unwrap();
    engine.submit("q", command("stays")).await.unwrap();
    // Churn: each message is written as owed, then written as gone.
    let mut largest = 0;
    for index in 0..300 {
        engine.submit("churn", command(&format!("c{index}"))).await.unwrap();
        engine.flush().await.unwrap();
        let delivery = engine.dequeue("churn", 1, None, NOW).await.unwrap().remove(0);
        engine.ack("churn", delivery.id).await.unwrap();
        engine.flush().await.unwrap();
        largest = largest.max(std::fs::metadata(&path).unwrap().len());
    }
    engine.flush().await.unwrap();
    let stats = engine.stats();
    assert!(stats.compactions >= 1, "600 superseded records over a threshold of 100 must have folded");
    let folded = std::fs::metadata(&path).unwrap().len();
    assert!(folded < largest, "journal peaked at {largest} bytes and is {folded} after folding");
    engine.shutdown().await.unwrap();

    let engine = Engine::open(&path, config()).await.unwrap();
    assert_eq!(owed(&engine, "q").await, ["stays"]);
    assert!(owed(&engine, "churn").await.is_empty());
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn strict_mode_acknowledges_only_what_is_on_disk() {
    let path = scratch("strict");
    let config = EngineConfig { durability: Durability::Strict, ..EngineConfig::default() };
    let engine = Engine::open(&path, config).await.unwrap();
    for index in 0..50 {
        engine.submit("q", command(&format!("t{index}"))).await.unwrap();
        assert_eq!(engine.stats().journal_lag, 0, "acknowledged means written");
    }
    // With nothing left to write, a flush has nothing to wait for.
    tokio::time::timeout(Duration::from_secs(5), engine.flush()).await.expect("flush must not wait for a write that will never come").unwrap();

    // The file as a crash right now would leave it holds all fifty.
    let image = scratch("strict-image");
    std::fs::copy(&path, &image).unwrap();
    let recovered = Engine::open(&image, EngineConfig::default()).await.unwrap();
    assert_eq!(owed(&recovered, "q").await.len(), 50);
    recovered.shutdown().await.unwrap();
    engine.shutdown().await.unwrap();
}
