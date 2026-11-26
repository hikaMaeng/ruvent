//! What the SDK tests share: engines in this process, the real TCP server,
//! and a relay that can cut connections the way a network does.

#![allow(dead_code)]

use ruvent::{Channel, Engine, EngineConfig, Envelope, EventId, Hello, Methods, SegmentLog, Stats};
use ruvent_client::link::{Local, Tcp};
use ruvent_client::servers::{Server, Servers};
use ruvent_server::{serve, Credentials, ServerOptions};
use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;

pub const TOKEN: &str = "svc-token";

/// A test channel: a UUIDv7-shaped id derived from a label, the same every time.
pub fn ch(label: &str) -> Channel {
    let fnv = |seed: u64| label.bytes().fold(seed, |hash, byte| (hash ^ u64::from(byte)).wrapping_mul(0x100_0000_01b3));
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&fnv(0xcbf2_9ce4_8422_2325).to_le_bytes());
    bytes[8..].copy_from_slice(&fnv(0x9e37_79b9_7f4a_7c15).to_le_bytes());
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Channel(bytes)
}
/// Generous: a broken test fails instead of hanging.
pub const WAIT: Duration = Duration::from_secs(20);

/// Waits until `check` holds, looking again every few milliseconds (a test
/// may poll; the library may not).
pub async fn eventually(what: &str, mut check: impl FnMut() -> bool) {
    let started = tokio::time::Instant::now();
    while !check() {
        assert!(started.elapsed() < WAIT, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// [`eventually`] for a check that must await.
pub async fn eventually_async<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let started = tokio::time::Instant::now();
    while !check().await {
        assert!(started.elapsed() < WAIT, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

pub async fn within<T>(what: &str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(WAIT, future).await.unwrap_or_else(|_| panic!("timed out: {what}"))
}

pub fn scratch(name: &str) -> PathBuf {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let directory = std::env::temp_dir().join(format!("ruvent-client-{name}-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
    let _ = std::fs::remove_dir_all(&directory);
    directory
}

pub async fn durable(path: &PathBuf, config: EngineConfig) -> Engine {
    let (journal, recovered) = SegmentLog::open(path).unwrap();
    Engine::start(config, journal, recovered).await.unwrap()
}

/// Engines in this process under names `s0`, `s1`, …, and the list naming them.
pub async fn locals(count: usize, config: EngineConfig) -> (Arc<Local>, Servers, Vec<Engine>) {
    let local = Arc::new(Local::new());
    let mut engines = Vec::new();
    let mut names = Vec::new();
    for index in 0..count {
        let engine = Engine::volatile(config.clone()).await;
        local.insert(format!("s{index}"), engine.clone());
        engines.push(engine);
        names.push(format!("s{index}"));
    }
    (local, Servers::new(names), engines)
}

pub fn server(name: &str) -> Server {
    Arc::from(name)
}

/// An engine's statistics, asked in-process.
pub async fn stats(engine: &Engine) -> Stats {
    let (client, _inbox) = ruvent::connect(engine, Hello::observer("operator"), 16).await.unwrap();
    client.stats().await.unwrap()
}

/// Registers `count` claimable events on `engine` directly; returns their ids.
pub async fn preload(engine: &Engine, channel: Channel, event_type: u16, count: usize) -> Vec<EventId> {
    let (client, _inbox) = ruvent::connect(engine, Hello::producer("loader"), 16).await.unwrap();
    let mut ids = Vec::new();
    let mut last = 0;
    for index in 0..count {
        let (seq, id) = client.register(Envelope::claimable(channel, event_type, format!("{index}").into_bytes()).with_id(EventId::new_v7())).await.unwrap();
        ids.push(id);
        last = seq;
    }
    client.committed(last).await.unwrap();
    ids
}

/// The real server on a local port.
pub struct TcpServer {
    pub address: String,
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<io::Result<()>>,
}

impl TcpServer {
    pub async fn start(engine: &Engine) -> TcpServer {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let credentials = Arc::new(Credentials::new([("svc".to_owned(), TOKEN.to_owned())]));
        let (stop, stopped) = oneshot::channel::<()>();
        let task = tokio::spawn(serve(listener, engine.clone(), credentials, ServerOptions::default(), async move {
            let _ = stopped.await;
        }));
        TcpServer { address, stop: Some(stop), task }
    }

    pub async fn stop(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let _ = tokio::time::timeout(WAIT, &mut self.task).await;
    }
}

pub fn tcp() -> Arc<Tcp> {
    Arc::new(Tcp::new(TOKEN))
}

/// How the relay cuts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cut {
    /// Both sides close: everyone knows.
    Both,
    /// Only the client's side closes; the server's side stays open and
    /// silent (a NAT or load balancer forgot the flow): the server does not know.
    ClientOnly,
}

/// A TCP relay in front of a server. Connections through it can be cut.
pub struct Relay {
    pub address: String,
    cut: watch::Sender<(u64, Cut)>,
    task: JoinHandle<()>,
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Relay {
    pub async fn start(upstream: &str) -> Relay {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let (cut, cuts) = watch::channel((0u64, Cut::Both));
        let upstream = upstream.to_owned();
        let task = tokio::spawn(async move {
            // Server sides kept open after a client-only cut.
            let parked: Arc<parking_lot::Mutex<Vec<TcpStream>>> = Arc::default();
            loop {
                let Ok((mut client, _)) = listener.accept().await else { return };
                let Ok(mut server) = TcpStream::connect(&upstream).await else { continue };
                let mut cuts = cuts.clone();
                let seen = cuts.borrow_and_update().0;
                let parked = parked.clone();
                tokio::spawn(async move {
                    let how = tokio::select! {
                        _ = tokio::io::copy_bidirectional(&mut client, &mut server) => return,
                        how = async {
                            loop {
                                if cuts.changed().await.is_err() {
                                    std::future::pending::<()>().await;
                                }
                                let (count, how) = *cuts.borrow_and_update();
                                if count != seen {
                                    return how;
                                }
                            }
                        } => how,
                    };
                    drop(client);
                    if how == Cut::ClientOnly {
                        parked.lock().push(server);
                    }
                });
            }
        });
        Relay { address, cut, task }
    }

    /// Cuts every connection open through the relay now.
    pub fn cut(&self, how: Cut) {
        self.cut.send_modify(|(count, kind)| {
            *count += 1;
            *kind = how;
        });
    }
}
