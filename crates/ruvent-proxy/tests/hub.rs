//! The proxy hub against an engine in this process (proposal §6.5, §6.8, P5).

use parking_lot::Mutex;
use ruvent::{async_trait, Backlog, Bytes, ClaimToken, Engine, EngineConfig, Envelope, EventId, Inbox, Incoming, Methods, Name, Refusal, WorkKey};
use ruvent_proxy::config::{AuthMode, StaticToken, WorkerAuth};
use ruvent_proxy::{Connector, Denied, Proxy, ProxyConfig, Publish, Request, User};
use serde_json::Value;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

const WAIT: Duration = Duration::from_secs(5);

/// Records what the proxy's upstream connection does: calls made, deliveries received.
#[derive(Default)]
struct Tap {
    calls: Mutex<Vec<String>>,
    deliveries: Mutex<HashMap<String, usize>>,
    call_feed: Mutex<Option<mpsc::UnboundedSender<String>>>,
    /// Cuts the current upstream connection's inbox, as a dropped socket would.
    kill: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

impl Tap {
    fn record(&self, call: String) {
        if let Some(feed) = &*self.call_feed.lock() {
            let _ = feed.send(call.clone());
        }
        self.calls.lock().push(call);
    }

    fn delivered(&self, channel: &str) -> usize {
        self.deliveries.lock().get(channel).copied().unwrap_or_default()
    }
}

struct Tapped {
    inner: Arc<dyn Methods>,
    tap: Arc<Tap>,
}

#[async_trait]
impl Methods for Tapped {
    async fn register_claimable(&self, event: Envelope) -> Result<(EventId, WorkKey), Refusal> {
        self.inner.register_claimable(event).await
    }
    async fn register_dispatch(&self, event: Envelope) -> Result<EventId, Refusal> {
        self.inner.register_dispatch(event).await
    }
    async fn claim(&self, id: EventId, worker: u32, lease: Duration) -> Result<Envelope, Refusal> {
        self.inner.claim(id, worker, lease).await
    }
    async fn complete(&self, id: EventId, token: ClaimToken, follow_ups: Vec<Envelope>) -> Result<(), Refusal> {
        self.inner.complete(id, token, follow_ups).await
    }
    async fn delete(&self, id: EventId, result: Name, tag: EventId, key: Option<WorkKey>) -> Result<bool, Refusal> {
        self.inner.delete(id, result, tag, key).await
    }
    async fn subscribe(&self, channel: Name, after: Option<WorkKey>) -> Result<Backlog, Refusal> {
        let result = self.inner.subscribe(channel.clone(), after).await;
        self.tap.record(format!("subscribe {channel}"));
        result
    }
    async fn unsubscribe(&self, channel: Name) -> Result<(), Refusal> {
        let result = self.inner.unsubscribe(channel.clone()).await;
        self.tap.record(format!("unsubscribe {channel}"));
        result
    }
}

struct TapConnector {
    engine: Engine,
    tap: Arc<Tap>,
}

#[async_trait]
impl Connector for TapConnector {
    async fn connect(&self) -> io::Result<(Arc<dyn Methods>, Inbox)> {
        let (client, mut upstream) = ruvent::connect(&self.engine, "proxy", 1024).await.map_err(|error| io::Error::new(io::ErrorKind::NotConnected, error))?;
        let (forward, inbox) = mpsc::channel(1024);
        let tap = self.tap.clone();
        let (kill, mut killed) = tokio::sync::oneshot::channel();
        *self.tap.kill.lock() = Some(kill);
        tokio::spawn(async move {
            loop {
                let incoming = tokio::select! {
                    incoming = upstream.recv() => incoming,
                    _ = &mut killed => None,
                };
                let Some(incoming) = incoming else { break };
                if let Incoming::Delivery(event) = &incoming {
                    *tap.deliveries.lock().entry(event.channel.to_string()).or_default() += 1;
                }
                if forward.send(incoming).await.is_err() {
                    break;
                }
            }
        });
        Ok((Arc::new(Tapped { inner: Arc::new(client), tap: self.tap.clone() }), inbox))
    }
}

struct Setup {
    engine: Engine,
    proxy: Proxy,
    tap: Arc<Tap>,
    /// A plain connection to the same engine, for publishing and working.
    peer: ruvent::Client,
    peer_inbox: Inbox,
}

async fn setup(config: ProxyConfig) -> Setup {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let tap = Arc::new(Tap::default());
    let proxy = Proxy::start(config, Arc::new(TapConnector { engine: engine.clone(), tap: tap.clone() }));
    let (peer, peer_inbox) = ruvent::connect(&engine, "peer", 1024).await.unwrap();
    Setup { engine, proxy, tap, peer, peer_inbox }
}

fn open() -> ProxyConfig {
    ProxyConfig::open("127.0.0.1:0", "in-process")
}

async fn next(events: &mut mpsc::Receiver<Arc<str>>) -> Option<Value> {
    tokio::time::timeout(WAIT, events.recv()).await.expect("timed out").map(|text| serde_json::from_str(&text).unwrap())
}

async fn dispatch(peer: &ruvent::Client, channel: &str, n: u64) {
    peer.register_dispatch(Envelope::dispatch(channel, "tick", Bytes::from(n.to_string()))).await.unwrap();
}

async fn user(proxy: &Proxy, name: &str) -> (ruvent_proxy::UserHandle, mpsc::Receiver<Arc<str>>) {
    proxy.attach(Arc::new(User { name: name.into(), scope: vec![ruvent::Pattern("*".into())] })).await.unwrap()
}

/// Publishes a marker event and returns once a user receives it: everything before it has been fanned out.
async fn drain_to_marker(peer: &ruvent::Client, channel: &str, events: &mut mpsc::Receiver<Arc<str>>) -> Vec<u64> {
    dispatch(peer, channel, 999_999).await;
    let mut seen = Vec::new();
    loop {
        let event = next(events).await.expect("stream ended");
        let n = event["payload"].as_u64().unwrap();
        if n == 999_999 {
            return seen;
        }
        seen.push(n);
    }
}

/// Three users on one channel: the event server delivers each event to the proxy once, and each user gets it.
#[tokio::test]
async fn three_users_on_one_channel_cost_one_upstream_delivery() {
    let setup = setup(open()).await;
    setup.proxy.ready().await;
    let mut users = Vec::new();
    for name in ["a", "b", "c"] {
        let (handle, events) = user(&setup.proxy, name).await;
        handle.subscribe("room").await.unwrap();
        users.push((handle, events));
    }
    dispatch(&setup.peer, "room", 1).await;
    for (_, events) in &mut users {
        assert_eq!(next(events).await.unwrap()["payload"], 1);
    }
    assert_eq!(setup.tap.delivered("room"), 1, "one delivery reached the proxy for three users");
}

/// A user joining a shared channel gets only events after its join took effect.
#[tokio::test]
async fn a_late_joiner_gets_only_events_after_it_joined() {
    let setup = setup(open()).await;
    setup.proxy.ready().await;
    let (a, mut a_events) = user(&setup.proxy, "a").await;
    a.subscribe("room").await.unwrap();
    dispatch(&setup.peer, "room", 1).await;
    let (b, mut b_events) = user(&setup.proxy, "b").await;
    b.subscribe("room").await.unwrap();
    dispatch(&setup.peer, "room", 2).await;
    assert_eq!(drain_to_marker(&setup.peer, "room", &mut a_events).await, vec![1, 2]);
    assert_eq!(drain_to_marker(&setup.peer, "room", &mut b_events).await, vec![2], "event 1 was in the proxy before b's join took effect");
    drop((a, b));
}

/// `0 → 1 → 0 → 1`: nothing sent while unsubscribed reaches the second subscription.
#[tokio::test]
async fn resubscribing_never_shows_events_from_the_gap() {
    let setup = setup(open()).await;
    setup.proxy.ready().await;
    let (a, mut events) = user(&setup.proxy, "a").await;
    a.subscribe("room").await.unwrap();
    dispatch(&setup.peer, "room", 1).await;
    assert_eq!(next(&mut events).await.unwrap()["payload"], 1);
    a.unsubscribe("room").await;
    for n in 2..20 {
        dispatch(&setup.peer, "room", n).await;
    }
    a.subscribe("room").await.unwrap();
    dispatch(&setup.peer, "room", 20).await;
    assert_eq!(next(&mut events).await.unwrap()["payload"], 20, "the first event after resubscribing is the one published after it");
}

/// When the last user leaves a channel, the proxy unsubscribes upstream.
#[tokio::test]
async fn the_last_user_leaving_cancels_the_upstream_subscription() {
    let setup = setup(open()).await;
    let (feed, mut calls) = mpsc::unbounded_channel();
    *setup.tap.call_feed.lock() = Some(feed);
    setup.proxy.ready().await;
    let (a, _a_events) = user(&setup.proxy, "a").await;
    let (b, _b_events) = user(&setup.proxy, "b").await;
    a.subscribe("room").await.unwrap();
    b.subscribe("room").await.unwrap();
    a.unsubscribe("room").await;
    drop(b);
    loop {
        let call = tokio::time::timeout(WAIT, calls.recv()).await.expect("no unsubscribe").unwrap();
        if call == "unsubscribe room" {
            break;
        }
        assert_eq!(call, "subscribe room");
    }
    let unsubscribes = setup.tap.calls.lock().iter().filter(|call| *call == "unsubscribe room").count();
    assert_eq!(unsubscribes, 1);
}

/// A user whose buffer fills is disconnected; the others keep receiving.
#[tokio::test]
async fn a_slow_user_is_disconnected_and_nobody_else_is() {
    let mut config = open();
    config.user_buffer = 4;
    let setup = setup(config).await;
    setup.proxy.ready().await;
    let (slow, mut slow_events) = user(&setup.proxy, "slow").await;
    let (fast, mut fast_events) = user(&setup.proxy, "fast").await;
    slow.subscribe("room").await.unwrap();
    fast.subscribe("room").await.unwrap();
    let reader = tokio::spawn(async move {
        let mut got = 0;
        while got < 20 {
            next(&mut fast_events).await.expect("fast user cut off");
            got += 1;
        }
        got
    });
    for n in 0..20 {
        dispatch(&setup.peer, "room", n).await;
    }
    assert_eq!(reader.await.unwrap(), 20);
    let mut kept = 0;
    while next(&mut slow_events).await.is_some() {
        kept += 1;
    }
    assert!(kept <= 4, "the slow user's stream ended after at most its buffer, got {kept}");
    drop((slow, fast));
}

/// A publish outside the user's channels is refused; inside, it is registered.
#[tokio::test]
async fn a_publish_outside_the_users_channels_is_refused() {
    let setup = setup(open()).await;
    setup.proxy.ready().await;
    let user = User { name: "u".into(), scope: vec![ruvent::Pattern("public.*".into())] };
    let refused: Publish = serde_json::from_str(r#"{"kind":"dispatch","channel":"private.x","type":"t","payload":1}"#).unwrap();
    let outcome = setup.proxy.publish(&user, refused).await;
    assert!(!outcome.ok);
    assert_eq!(outcome.error.as_deref(), Some("forbidden channel"));
    let allowed: Publish = serde_json::from_str(r#"{"kind":"dispatch","channel":"public.x","type":"t","payload":1}"#).unwrap();
    assert!(setup.proxy.publish(&user, allowed).await.ok);
}

/// A registration through the proxy carries the end user as `delegate` and the proxy's credential as `principal`.
#[tokio::test]
async fn a_registration_carries_the_end_user_as_delegate() {
    let setup = setup(open()).await;
    setup.proxy.ready().await;
    let user = User { name: "alice".into(), scope: vec![ruvent::Pattern("*".into())] };
    let publish: Publish = serde_json::from_str(r#"{"kind":"claimable","channel":"jobs","type":"run","payload":{"n":1}}"#).unwrap();
    let outcome = setup.proxy.publish(&user, publish).await;
    let id: EventId = outcome.id.unwrap().parse().unwrap();
    let claimed = setup.peer.claim(id, 1, Duration::from_secs(30)).await.unwrap();
    assert_eq!(claimed.delegate.as_deref(), Some("alice"));
    assert_eq!(claimed.principal.as_deref(), Some("proxy"));
    assert_eq!(&claimed.payload[..], br#"{"n":1}"#);
}

/// Static tokens: a listed token is the listed user with the listed channels; anything else is rejected.
#[tokio::test]
async fn static_tokens_let_in_listed_users_only() {
    let mut config = open();
    config.auth.mode = AuthMode::Static;
    config.auth.tokens = vec![StaticToken { token: "t1".into(), user: "bob".into(), channels: vec!["chat.*".into()] }];
    let setup = setup(config).await;
    let bob = setup.proxy.authenticate(Some("t1")).await.unwrap();
    assert_eq!(bob.name, "bob");
    assert!(bob.may_use("chat.x") && !bob.may_use("admin"));
    assert_eq!(setup.proxy.authenticate(Some("nope")).await.unwrap_err(), Denied::Rejected);
    assert_eq!(setup.proxy.authenticate(None).await.unwrap_err(), Denied::Rejected);
}

fn worker_config(timeout_ms: u64) -> ProxyConfig {
    let mut config = open();
    config.auth.mode = AuthMode::Worker;
    config.auth.worker = Some(WorkerAuth { channel: "auth".into(), event_type: "authenticate".into(), reply_channel: "proxy.auth".into(), timeout_ms });
    config
}

/// An authentication worker built on the seven methods: accepts token "good" as user "carol".
async fn run_auth_worker(worker: ruvent::Client, mut inbox: Inbox) {
    worker.subscribe("auth".into(), None).await.unwrap();
    while let Some(incoming) = inbox.recv().await {
        let Incoming::Notice(notice) = incoming else { continue };
        let Ok(event) = worker.claim(notice.id, 7, Duration::from_secs(10)).await else { continue };
        let request: Value = serde_json::from_slice(&event.payload).unwrap();
        let answer = if request["token"] == "good" { r#"{"ok":true,"user":"carol","channels":["carol.*"]}"# } else { r#"{"ok":false}"# };
        let reply = Envelope::dispatch(event.reply.clone().unwrap(), "authenticated", Bytes::from(answer)).with_correlation(event.correlation.unwrap());
        let token = event.claim.unwrap().token;
        worker.complete(event.id, token, vec![reply]).await.unwrap();
    }
}

/// Worker authentication (Q1(a)): the worker's answer decides, matched by correlation.
#[tokio::test]
async fn an_authentication_worker_lets_in_and_turns_away() {
    let setup = setup(worker_config(5_000)).await;
    let (worker, worker_inbox) = ruvent::connect(&setup.engine, "auth-worker", 1024).await.unwrap();
    tokio::spawn(run_auth_worker(worker, worker_inbox));
    setup.proxy.ready().await;
    let carol = setup.proxy.authenticate(Some("good")).await.unwrap();
    assert_eq!(carol.name, "carol");
    assert!(carol.may_use("carol.inbox") && !carol.may_use("other"));
    assert_eq!(setup.proxy.authenticate(Some("bad")).await.unwrap_err(), Denied::Rejected);
}

/// Worker authentication with no worker running: the user is turned away after the timeout.
#[tokio::test]
async fn an_authentication_nobody_answers_times_out() {
    let setup = setup(worker_config(200)).await;
    setup.proxy.ready().await;
    assert_eq!(setup.proxy.authenticate(Some("good")).await.unwrap_err(), Denied::Timeout);
    drop(setup.peer_inbox);
}

/// User messages go through `UserHandle::handle`; results come back in the user's stream with the caller's ref.
#[tokio::test]
async fn requests_are_answered_in_the_users_stream() {
    let setup = setup(open()).await;
    setup.proxy.ready().await;
    let (handle, mut events) = user(&setup.proxy, "u").await;
    let request = Request::parse(r#"{"op":"subscribe","channel":"room","ref":"s1"}"#).unwrap();
    handle.handle(request).await;
    let result = next(&mut events).await.unwrap();
    assert_eq!(result["op"], "result");
    assert_eq!(result["ref"], "s1");
    assert_eq!(result["ok"], true);
    let publish = Request::parse(r#"{"op":"publish","kind":"dispatch","channel":"room","type":"t","payload":{"hi":1},"ref":2}"#).unwrap();
    handle.handle(publish).await;
    let first = next(&mut events).await.unwrap();
    let second = next(&mut events).await.unwrap();
    let (event, result) = if first["op"] == "event" { (first, second) } else { (second, first) };
    assert_eq!(event["payload"]["hi"], 1);
    assert_eq!(result["ref"], 2);
    assert!(result["id"].is_string());
}

/// The upstream connection drops: the proxy reconnects, subscribes its channels again, and users keep receiving.
#[tokio::test]
async fn after_the_upstream_drops_the_proxy_resubscribes() {
    let setup = setup(open()).await;
    let (feed, mut calls) = mpsc::unbounded_channel();
    *setup.tap.call_feed.lock() = Some(feed);
    setup.proxy.ready().await;
    let (handle, mut events) = user(&setup.proxy, "a").await;
    handle.subscribe("room").await.unwrap();
    assert_eq!(tokio::time::timeout(WAIT, calls.recv()).await.unwrap().unwrap(), "subscribe room");
    let kill = setup.tap.kill.lock().take().unwrap();
    let _ = kill.send(());
    assert_eq!(tokio::time::timeout(WAIT, calls.recv()).await.expect("no resubscribe").unwrap(), "subscribe room");
    setup.proxy.ready().await;
    // The resubscribe has been answered, so its marker is already ahead of anything published now.
    dispatch(&setup.peer, "room", 5).await;
    let got = next(&mut events).await.expect("stream ended");
    assert_eq!(got["payload"], 5);
    assert_eq!(got["op"], "event");
    drop(handle);
}
