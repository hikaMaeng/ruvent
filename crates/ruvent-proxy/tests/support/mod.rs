//! What the proxy's integration tests share: engines in this process behind
//! the SDK's `Local` connector, the test's own traffic straight to an engine,
//! and a tap on the proxy's upstream connections.

#![allow(dead_code)]

use parking_lot::Mutex;
use ruvent::{async_trait, Bytes, Call, Client, Engine, EngineConfig, Envelope, Hello, Inbox, Incoming, Link, Methods, Outbound, Role, Target};
use ruvent_proxy::{Connector, Local, Servers, Upstream};
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};

/// Generous: a broken test fails instead of hanging.
pub const WAIT: Duration = Duration::from_secs(15);

pub async fn within<T>(what: &str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(WAIT, future).await.unwrap_or_else(|_| panic!("timed out: {what}"))
}

/// Waits until `check` holds, looking again every few milliseconds (a test
/// may poll; the library may not).
pub async fn eventually(what: &str, mut check: impl FnMut() -> bool) {
    let started = tokio::time::Instant::now();
    while !check() {
        assert!(started.elapsed() < WAIT, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Engines in this process under the names `s0`, `s1`, …, and the list naming them.
pub async fn engines(count: usize) -> (Vec<Engine>, Arc<Local>, Servers) {
    let local = Arc::new(Local::new());
    let mut engines = Vec::new();
    let mut names = Vec::new();
    for index in 0..count {
        let engine = Engine::volatile(EngineConfig::default()).await;
        local.insert(format!("s{index}"), engine.clone());
        engines.push(engine);
        names.push(format!("s{index}"));
    }
    (engines, local, Servers::new(names))
}

/// One engine and the upstream that reaches it, under credential `proxy`.
pub async fn one() -> (Engine, Upstream) {
    let (engines, local, servers) = engines(1).await;
    (engines[0].clone(), Upstream::new(local, servers))
}

/// Registers a dispatch event straight on `engine` and waits until it is applied.
pub async fn dispatch(engine: &Engine, channel: &str, payload: impl Into<Bytes>) {
    let (peer, _inbox) = ruvent::connect(engine, Hello::producer("peer"), 64).await.unwrap();
    let (seq, _) = peer.register(Envelope::dispatch(channel, "said", payload.into())).await.unwrap();
    peer.committed(seq).await.unwrap();
}

/// An observation connection straight to `engine`, subscribed to `target`.
pub async fn watch_target(engine: &Engine, target: Target) -> (Client, Inbox) {
    let (watcher, inbox) = ruvent::connect(engine, Hello::observer("watcher"), 256).await.unwrap();
    watcher.subscribe(target, None).await.unwrap();
    (watcher, inbox)
}

/// The next dispatch event an inbox receives.
pub async fn next_delivery(inbox: &mut Inbox) -> Envelope {
    loop {
        match within("a delivery", inbox.recv()).await.expect("the connection closed") {
            Incoming::Delivery(event) => return event,
            _ => continue,
        }
    }
}

/// Records what the proxy's observation connections do, counts the proxy's
/// live upstream connections of every role, and cuts connections the way a
/// dropped socket would.
#[derive(Default)]
pub struct Tap {
    calls: Mutex<Vec<String>>,
    feed: Mutex<Option<mpsc::UnboundedSender<String>>>,
    deliveries: Mutex<HashMap<String, usize>>,
    live: AtomicUsize,
    cuts: Mutex<Option<watch::Sender<u64>>>,
}

impl Tap {
    fn record(&self, call: String) {
        if let Some(feed) = &*self.feed.lock() {
            let _ = feed.send(call.clone());
        }
        self.calls.lock().push(call);
    }

    /// Every later observation call (`subscribe room`, `unsubscribe room.*` for a prefix), in order.
    pub fn follow(&self) -> mpsc::UnboundedReceiver<String> {
        let (feed, calls) = mpsc::unbounded_channel();
        *self.feed.lock() = Some(feed);
        calls
    }

    pub fn calls(&self) -> Vec<String> {
        self.calls.lock().clone()
    }

    /// Dispatch events on `channel` that reached the proxy's observation connections.
    pub fn delivered(&self, channel: &str) -> usize {
        self.deliveries.lock().get(channel).copied().unwrap_or_default()
    }

    /// The proxy's upstream connections open now, every role.
    pub fn live(&self) -> usize {
        self.live.load(Ordering::SeqCst)
    }

    /// Cuts every connection open now.
    pub fn cut(&self) {
        if let Some(cuts) = &*self.cuts.lock() {
            cuts.send_modify(|count| *count += 1);
        }
    }
}

fn shown(target: &Target) -> String {
    match target {
        Target::Channel(name) => name.to_string(),
        Target::Prefix(name) => format!("{name}.*"),
    }
}

struct TapLink {
    inner: Arc<dyn Link>,
    tap: Arc<Tap>,
    observes: bool,
}

impl Drop for TapLink {
    fn drop(&mut self) {
        self.tap.live.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl Link for TapLink {
    async fn send(&self, call: Call) -> Result<(), ruvent::Closed> {
        if self.observes {
            match &call {
                Call::Subscribe { target, .. } => self.tap.record(format!("subscribe {}", shown(target))),
                Call::Unsubscribe { target, .. } => self.tap.record(format!("unsubscribe {}", shown(target))),
                _ => {}
            }
        }
        self.inner.send(call).await
    }
}

/// A connector in front of another, tapped.
pub struct Tapped {
    pub inner: Arc<dyn Connector>,
    pub tap: Arc<Tap>,
}

impl Tapped {
    pub fn new(inner: Arc<dyn Connector>) -> (Arc<Tapped>, Arc<Tap>) {
        let tap = Arc::new(Tap::default());
        *tap.cuts.lock() = Some(watch::channel(0).0);
        (Arc::new(Tapped { inner, tap: tap.clone() }), tap)
    }
}

#[async_trait]
impl Connector for Tapped {
    async fn open(&self, server: &str, hello: Hello) -> io::Result<ruvent_proxy::upstream::Conn> {
        let observes = hello.role == Role::Observe;
        let conn = self.inner.open(server, hello).await?;
        self.tap.live.fetch_add(1, Ordering::SeqCst);
        let link = Arc::new(TapLink { inner: conn.link, tap: self.tap.clone(), observes });
        let mut cuts = self.tap.cuts.lock().as_ref().map(watch::Sender::subscribe).expect("a tap has cuts");
        let (sink, outbound) = mpsc::channel(1024);
        let (tap, mut inner) = (self.tap.clone(), conn.outbound);
        tokio::spawn(async move {
            cuts.borrow_and_update();
            loop {
                let message = tokio::select! {
                    message = inner.recv() => message,
                    _ = cuts.changed() => None,
                };
                let Some(message) = message else { break };
                if let (true, Outbound::Delivery(event)) = (observes, &message) {
                    *tap.deliveries.lock().entry(event.channel.to_string()).or_default() += 1;
                }
                if sink.send(message).await.is_err() {
                    break;
                }
            }
        });
        Ok(ruvent_proxy::upstream::Conn { link, outbound, welcome: conn.welcome, features: conn.features })
    }
}

/// A TCP relay in front of a server. Every connection through it can be cut
/// at once, both sides closing: the proxy sees its upstream connection drop.
pub struct Relay {
    pub address: String,
    cut: watch::Sender<u64>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Relay {
    pub async fn start(upstream: &str) -> Relay {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let (cut, cuts) = watch::channel(0u64);
        let upstream = upstream.to_owned();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut client, _)) = listener.accept().await else { return };
                let Ok(mut server) = tokio::net::TcpStream::connect(&upstream).await else { continue };
                let mut cuts = cuts.clone();
                cuts.borrow_and_update();
                tokio::spawn(async move {
                    tokio::select! {
                        _ = tokio::io::copy_bidirectional(&mut client, &mut server) => {}
                        _ = cuts.changed() => {}
                    }
                });
            }
        });
        Relay { address, cut, task }
    }

    /// Cuts every connection open through the relay now.
    pub fn cut(&self) {
        self.cut.send_modify(|count| *count += 1);
    }
}
