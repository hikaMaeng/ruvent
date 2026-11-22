//! A real process, killed.
//!
//! The test binary runs itself again as a child with an environment variable
//! naming a scenario. The child opens a journal, does its work, reports on
//! stdout that it is ready, and waits. The parent kills it — `TerminateProcess`
//! on Windows, `SIGKILL` elsewhere: no destructor, no flush, no unlock — and
//! then opens the same journal itself.
//!
//! This is a process crash, not a power cut. What the operating system had
//! accepted from the process is still there; only what the process held in
//! memory is gone. A power cut is not simulated anywhere in this suite.

use ruvent::{App, Begin, Bytes, Command, Context, Durability, Engine, EngineConfig, Envelope, Failure, Identity, Proposal, Service};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command as Process, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

const CHILD: &str = "RUVENT_CRASH_CHILD";
const READY: &str = "ruvent-child-ready";
const NOW: Option<Duration> = Some(Duration::ZERO);

fn scratch(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("ruvent-{}-crash-{name}.journal", std::process::id()));
    for stale in [path.clone(), path.with_extension("journal.lock"), path.with_extension("journal.tmp")] {
        let _ = std::fs::remove_file(stale);
    }
    path
}

/// Runs a scenario in a child process, waits for it to say it is ready, and
/// kills it. Returns what it printed after the ready line's marker.
fn run_and_kill(scenario: &str, path: &Path) -> String {
    let mut child = Process::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash::child", "--nocapture", "--test-threads=1"])
        .env(CHILD, format!("{scenario}|{}", path.display()))
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let report = loop {
        let line = lines.next().expect("the child exited before it was ready").unwrap();
        // libtest has already printed the test's name on this line.
        if let Some(at) = line.find(READY) {
            break line[at + READY.len()..].trim().to_owned();
        }
    };
    child.kill().unwrap();
    child.wait().unwrap();
    report
}

/// The child half. Without the variable it is an ordinary, empty test.
#[test]
fn child() {
    let Ok(spec) = std::env::var(CHILD) else { return };
    let (scenario, path) = spec.split_once('|').unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    runtime.block_on(async move {
        let report = match scenario {
            "strict" => strict_child(path).await,
            "scheduled" => scheduled_child(path).await,
            other => panic!("unknown scenario {other}"),
        };
        let mut stdout = std::io::stdout().lock();
        writeln!(stdout, "{READY} {report}").unwrap();
        stdout.flush().unwrap();
        drop(stdout);
        std::future::pending::<()>().await;
    });
}

const SUBMITTED: usize = 20;
const RUNNING: usize = 8;

/// Strict durability, a service whose handlers never return. Twenty commands
/// are accepted; eight are taken up and are running when the process dies.
async fn strict_child(path: &str) -> String {
    let started = Arc::new((AtomicUsize::new(0), tokio::sync::Notify::new()));
    let hang = {
        let started = started.clone();
        move |_cx: Context, _command: Arc<Envelope>| {
            let started = started.clone();
            async move {
                if started.0.fetch_add(1, Ordering::SeqCst) + 1 == RUNNING {
                    started.1.notify_one();
                }
                std::future::pending::<Result<Bytes, Failure>>().await
            }
        }
    };
    let app = App::open(path).durability(Durability::Strict).service(Service::new("jobs").accept("job.run", hang).max_in_flight(RUNNING)).start().await.unwrap();
    let identity = Identity { user: "child".to_owned() };
    for index in 0..SUBMITTED {
        let proposal = Proposal { action: "job.run".into(), payload: Bytes::from_static(b"{}"), transaction_key: Some(format!("j{index}").into()), ..Proposal::default() };
        app.gateway().submit(&identity, proposal).await.unwrap();
    }
    started.1.notified().await;
    Box::leak(Box::new(app));
    String::new()
}

const FLUSHED: usize = 300;

/// The default, scheduled durability. Three hundred commands, then a flush,
/// then three hundred more that nobody waits to see written.
async fn scheduled_child(path: &str) -> String {
    let engine = Engine::open(path, EngineConfig::default()).await.unwrap();
    for index in 0..2 * FLUSHED {
        engine.submit("q", Command::new("job", "c", "{}").transaction_key(format!("j{index}"))).await.unwrap();
        if index + 1 == FLUSHED {
            engine.flush().await.unwrap();
        }
    }
    let lag = engine.stats().journal_lag;
    Box::leak(Box::new(engine));
    lag.to_string()
}

/// > 하지만 인메모리만으로 운영되면 안된다.
///
/// Strict durability, observed across a killed process: every accepted
/// command is still owed, the eight that were running are owed again with
/// their attempt counted, and the journal's lock died with its holder.
#[tokio::test]
async fn a_killed_process_leaves_every_accepted_command_and_every_attempt() {
    let path = scratch("strict");
    run_and_kill("strict", &path);

    let engine = Engine::open(&path, EngineConfig::default()).await.expect("the lock must not outlive the process that held it");
    let owed = engine.dequeue("jobs", 100, None, NOW).await.unwrap();
    assert_eq!(owed.len(), SUBMITTED, "every accepted command is owed");
    let mut retried = 0;
    for delivery in &owed {
        let Begin::Claimed { attempts, .. } = engine.begin("jobs", delivery.id).await.unwrap() else { panic!("every owed command can be run") };
        assert!(attempts <= 2);
        retried += usize::from(attempts == 2);
    }
    assert_eq!(retried, RUNNING, "the attempts that were running when the process died are counted");
    engine.shutdown().await.unwrap();
}

/// > 단지 이벤트 큐는 입출력이 잦다. 따라서 100% 장애대응을 대비하지 않는다.
///
/// Scheduled durability, observed across a killed process: everything before
/// the last completed flush survives; what came after may or may not; and
/// nothing appears that was never submitted.
#[tokio::test]
async fn a_killed_process_loses_at_most_what_came_after_the_last_write() {
    let path = scratch("scheduled");
    let lag: u64 = run_and_kill("scheduled", &path).parse().unwrap();
    assert!(lag > 0, "the child died with changes not yet written, or this test proves nothing");

    let engine = Engine::open(&path, EngineConfig::default()).await.unwrap();
    let owed: Vec<usize> = engine.dequeue("q", 1_000, None, NOW).await.unwrap().iter().map(|delivery| delivery.command.transaction_key[1..].parse().unwrap()).collect();
    assert!(owed.len() >= FLUSHED, "{} owed after the crash, {FLUSHED} were flushed", owed.len());
    assert_eq!(owed[..FLUSHED], (0..FLUSHED).collect::<Vec<_>>(), "everything flushed survives, in order");
    assert!(owed.iter().all(|index| *index < 2 * FLUSHED), "nothing appears that was never submitted");
    assert!(owed.windows(2).all(|pair| pair[0] < pair[1]), "what survives is a prefix in submission order");
    engine.shutdown().await.unwrap();
}
