//! The proxy hub against an engine in this process (proposal §6.5, §6.8, P5).

use parking_lot::Mutex;
use ruvent::{async_trait, Backlog, Bytes, ClaimToken, Engine, EngineConfig, Envelope, EventId, Inbox, Incoming, Methods, Name, Refusal, WorkKey};
use ruvent_proxy::config::{AuthMode, StaticToken, WorkerAuth};
use ruvent_proxy::{Connector, Context, Denied, Hooks, Proxy, ProxyConfig, Publish, Rejection, Request, User};
use serde_json::Value;
use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
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
    /// Told when the proxy lets go of its upstream connection.
    released: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
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

impl Drop for Tapped {
    fn drop(&mut self) {
        if let Some(released) = self.tap.released.lock().take() {
            let _ = released.send(());
        }
    }
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
    setup_with(config, Arc::new(ruvent_proxy::Defaults)).await
}

async fn setup_with(config: ProxyConfig, hooks: Arc<dyn Hooks>) -> Setup {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let tap = Arc::new(Tap::default());
    let proxy = Proxy::start_with(config, Arc::new(TapConnector { engine: engine.clone(), tap: tap.clone() }), hooks);
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
    proxy.attach(Arc::new(User::new(name, ["*"]))).await.unwrap()
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
    assert!(setup.proxy.ready().await);
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
    assert!(setup.proxy.ready().await);
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
    assert!(setup.proxy.ready().await);
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
    assert!(setup.proxy.ready().await);
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
    assert!(setup.proxy.ready().await);
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
    assert!(setup.proxy.ready().await);
    let user = User::new("u", ["public.*"]);
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
    assert!(setup.proxy.ready().await);
    let user = User::new("alice", ["*"]);
    let publish: Publish = serde_json::from_str(r#"{"kind":"claimable","channel":"jobs","type":"run","payload":{"n":1}}"#).unwrap();
    let outcome = setup.proxy.publish(&user, publish).await;
    let id: EventId = outcome.id.unwrap().parse().unwrap();
    let claimed = setup.peer.claim(id, 1, Duration::from_secs(30)).await.unwrap();
    assert_eq!(claimed.delegate.as_deref(), Some("alice"));
    assert_eq!(claimed.principal.as_deref(), Some("proxy"));
    assert_eq!(&claimed.payload[..], br#"{"n":1}"#);
}

/// Static tokens: a listed token is the listed user with the listed channels; another token is rejected, none is missing.
#[tokio::test]
async fn static_tokens_let_in_listed_users_only() {
    let mut config = open();
    config.auth.mode = AuthMode::Static;
    config.auth.tokens = vec![StaticToken { token: "t1".into(), user: "bob".into(), channels: vec!["chat.*".into()] }];
    let setup = setup(config).await;
    let bob = setup.proxy.authenticate_by_config(Some("t1")).await.unwrap();
    assert_eq!(bob.name, "bob");
    assert!(bob.may_use("chat.x") && !bob.may_use("admin"));
    assert_eq!(setup.proxy.authenticate_by_config(Some("nope")).await.unwrap_err(), Denied::Rejected);
    assert_eq!(setup.proxy.authenticate_by_config(None).await.unwrap_err(), Denied::Missing, "no token: a protocol may ask for one");
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
    assert!(setup.proxy.ready().await);
    let carol = setup.proxy.authenticate_by_config(Some("good")).await.unwrap();
    assert_eq!(carol.name, "carol");
    assert!(carol.may_use("carol.inbox") && !carol.may_use("other"));
    assert_eq!(setup.proxy.authenticate_by_config(Some("bad")).await.unwrap_err(), Denied::Rejected);
}

/// Worker authentication with no worker running: the user is turned away after the timeout.
#[tokio::test]
async fn an_authentication_nobody_answers_times_out() {
    let setup = setup(worker_config(200)).await;
    assert!(setup.proxy.ready().await);
    assert_eq!(setup.proxy.authenticate_by_config(Some("good")).await.unwrap_err(), Denied::Timeout);
    drop(setup.peer_inbox);
}

/// User messages go through `UserHandle::handle`; results come back in the user's stream with the caller's ref.
#[tokio::test]
async fn requests_are_answered_in_the_users_stream() {
    let setup = setup(open()).await;
    assert!(setup.proxy.ready().await);
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
    assert!(setup.proxy.ready().await);
    let (handle, mut events) = user(&setup.proxy, "a").await;
    handle.subscribe("room").await.unwrap();
    assert_eq!(tokio::time::timeout(WAIT, calls.recv()).await.unwrap().unwrap(), "subscribe room");
    let kill = setup.tap.kill.lock().take().unwrap();
    let _ = kill.send(());
    assert_eq!(tokio::time::timeout(WAIT, calls.recv()).await.expect("no resubscribe").unwrap(), "subscribe room");
    assert!(setup.proxy.ready().await);
    // The resubscribe has been answered, so its marker is already ahead of anything published now.
    dispatch(&setup.peer, "room", 5).await;
    let got = next(&mut events).await.expect("stream ended");
    assert_eq!(got["payload"], 5);
    assert_eq!(got["op"], "event");
    drop(handle);
}

// ── hooks inside the hub ─────────────────────────────────────────────────

/// A deliver hook that panics for one user; lets everyone else through.
struct PanicsFor(&'static str);

#[ruvent_proxy::async_trait]
impl Hooks for PanicsFor {
    fn deliver(&self, user: &User, _event: &Envelope) -> bool {
        assert!(user.name != self.0, "deliver hook failed for {}", self.0);
        true
    }
}

/// A deliver hook that panics costs that user the event and its stream; the hub and every other user carry on.
#[tokio::test]
async fn a_deliver_hook_that_panics_costs_that_user_its_stream_and_nobody_else() {
    let setup = setup_with(open(), Arc::new(PanicsFor("mallory"))).await;
    assert!(setup.proxy.ready().await);
    let (mallory, mut mallory_events) = user(&setup.proxy, "mallory").await;
    let (alice, mut alice_events) = user(&setup.proxy, "alice").await;
    mallory.subscribe("room").await.unwrap();
    alice.subscribe("room").await.unwrap();
    dispatch(&setup.peer, "room", 1).await;
    assert_eq!(next(&mut alice_events).await.unwrap()["payload"], 1);
    assert!(next(&mut mallory_events).await.is_none(), "the user the hook failed for is disconnected");
    assert!(setup.proxy.is_ready(), "the hub is still running");
    let (carol, mut carol_events) = user(&setup.proxy, "carol").await;
    carol.subscribe("room").await.unwrap();
    dispatch(&setup.peer, "room", 2).await;
    assert_eq!(next(&mut alice_events).await.unwrap()["payload"], 2);
    assert_eq!(next(&mut carol_events).await.unwrap()["payload"], 2, "new users still join and receive");
    drop((mallory, alice, carol));
}

/// A deliver hook that lets nothing through.
struct Silent;

#[ruvent_proxy::async_trait]
impl Hooks for Silent {
    fn deliver(&self, _user: &User, _event: &Envelope) -> bool {
        false
    }
}

/// A user whose stream was dropped is cleaned up at the next delivery even when the hook filters it out,
/// so the last such user still cancels the upstream subscription.
#[tokio::test]
async fn a_user_that_went_away_is_dropped_even_when_the_hook_filters_it_out() {
    let setup = setup_with(open(), Arc::new(Silent)).await;
    let (feed, mut calls) = mpsc::unbounded_channel();
    *setup.tap.call_feed.lock() = Some(feed);
    assert!(setup.proxy.ready().await);
    let (handle, events) = user(&setup.proxy, "gone").await;
    handle.subscribe("room").await.unwrap();
    assert_eq!(tokio::time::timeout(WAIT, calls.recv()).await.unwrap().unwrap(), "subscribe room");
    // The reading side goes away while the handle (and so no Detach) stays.
    drop(events);
    dispatch(&setup.peer, "room", 1).await;
    assert_eq!(tokio::time::timeout(WAIT, calls.recv()).await.expect("no unsubscribe").unwrap(), "unsubscribe room");
    drop(handle);
}

/// Counts publish hook calls and allows everything.
#[derive(Default)]
struct CountsPublishes(AtomicUsize);

#[ruvent_proxy::async_trait]
impl Hooks for CountsPublishes {
    async fn publish(&self, _cx: Context<'_>, _user: &User, _publish: &mut Publish) -> Result<(), Rejection> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// A request's own shape is checked before the publish hook runs: a malformed id never reaches the hook.
#[tokio::test]
async fn a_publish_is_checked_for_shape_before_the_hook_runs() {
    let hooks = Arc::new(CountsPublishes::default());
    let setup = setup_with(open(), hooks.clone()).await;
    assert!(setup.proxy.ready().await);
    let user = User::new("u", ["*"]);
    let malformed: Publish = serde_json::from_str(r#"{"kind":"dispatch","channel":"c","type":"t","payload":1,"id":"nope"}"#).unwrap();
    let outcome = setup.proxy.publish(&user, malformed).await;
    assert_eq!((outcome.ok, outcome.error.as_deref(), outcome.cause), (false, Some("id is not a UUID"), Some(ruvent_proxy::Cause::Invalid)));
    assert_eq!(hooks.0.load(Ordering::SeqCst), 0, "the hook never saw a malformed request");
    let fine: Publish = serde_json::from_str(r#"{"kind":"dispatch","channel":"c","type":"t","payload":1}"#).unwrap();
    assert!(setup.proxy.publish(&user, fine).await.ok);
    assert_eq!(hooks.0.load(Ordering::SeqCst), 1);
}

/// A subscribe hook that writes an audit event through the hook's upstream calls before every subscription.
struct Audits;

#[ruvent_proxy::async_trait]
impl Hooks for Audits {
    async fn subscribe(&self, cx: Context<'_>, user: &User, channel: &str) -> Result<(), Rejection> {
        let upstream = cx.upstream().ok_or_else(|| Rejection::unavailable("upstream unavailable"))?;
        let audit = Envelope::dispatch("audit", "subscribed", Bytes::from(format!("{} {channel}", user.name)));
        upstream.register_dispatch(audit).await.map_err(Rejection::from_refusal)?;
        Ok(())
    }
}

/// A hook calls the event server through the proxy's connection, and the subscription rule still holds:
/// a late joiner gets only events after its own join took effect.
#[tokio::test]
async fn a_hooks_upstream_calls_leave_the_subscription_rule_intact() {
    let mut setup = setup_with(open(), Arc::new(Audits)).await;
    setup.peer.subscribe("audit".into(), None).await.unwrap();
    assert!(setup.proxy.ready().await);
    let (a, mut a_events) = user(&setup.proxy, "a").await;
    a.subscribe("room").await.unwrap();
    dispatch(&setup.peer, "room", 1).await;
    let (b, mut b_events) = user(&setup.proxy, "b").await;
    b.subscribe("room").await.unwrap();
    dispatch(&setup.peer, "room", 2).await;
    assert_eq!(drain_to_marker(&setup.peer, "room", &mut a_events).await, vec![1, 2]);
    assert_eq!(drain_to_marker(&setup.peer, "room", &mut b_events).await, vec![2]);
    let mut audits = Vec::new();
    while audits.len() < 2 {
        if let Incoming::Delivery(event) = tokio::time::timeout(WAIT, setup.peer_inbox.recv()).await.expect("no audit").unwrap() {
            if &*event.channel == "audit" {
                audits.push(String::from_utf8(event.payload.to_vec()).unwrap());
            }
        }
    }
    assert_eq!(audits, vec!["a room", "b room"]);
    drop((a, b));
}

// ── the hub's lifetime ───────────────────────────────────────────────────

/// Stopping the hub ends every user's stream, makes it not ready, refuses new users and lets go of the upstream.
#[tokio::test]
async fn stopping_the_hub_ends_every_stream_and_lets_go_of_the_upstream() {
    let setup = setup(open()).await;
    let (released, upstream_released) = tokio::sync::oneshot::channel();
    *setup.tap.released.lock() = Some(released);
    assert!(setup.proxy.ready().await);
    let (handle, mut events) = user(&setup.proxy, "a").await;
    handle.subscribe("room").await.unwrap();
    setup.proxy.stop().await;
    assert!(next(&mut events).await.is_none(), "the user's stream ended");
    assert!(!setup.proxy.is_ready());
    assert!(!setup.proxy.ready().await, "waiting for readiness returns at once once stopped");
    assert_eq!(setup.proxy.attach(Arc::new(User::new("late", ["*"]))).await.err().as_deref(), Some("proxy stopped"));
    assert_eq!(handle.subscribe("other").await.unwrap_err().message, "proxy stopped");
    tokio::time::timeout(WAIT, upstream_released).await.expect("the upstream connection was kept").unwrap();
}

/// When every handle is gone the hub ends by itself and lets go of the upstream.
#[tokio::test]
async fn dropping_every_handle_stops_the_hub() {
    let setup = setup(open()).await;
    let (released, upstream_released) = tokio::sync::oneshot::channel();
    *setup.tap.released.lock() = Some(released);
    assert!(setup.proxy.ready().await);
    let (handle, _events) = user(&setup.proxy, "a").await;
    handle.subscribe("room").await.unwrap();
    drop(handle);
    drop(setup.proxy);
    tokio::time::timeout(WAIT, upstream_released).await.expect("the hub kept running with no handle").unwrap();
}

/// Connects only once the test opens the gate.
struct Gated {
    engine: Engine,
    gate: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

#[async_trait]
impl Connector for Gated {
    async fn connect(&self) -> io::Result<(Arc<dyn Methods>, Inbox)> {
        let gate = self.gate.lock().take();
        if let Some(gate) = gate {
            let _ = gate.await;
        }
        let (client, inbox) = ruvent::connect(&self.engine, "proxy", 1024).await.map_err(|error| io::Error::new(io::ErrorKind::NotConnected, error))?;
        Ok((Arc::new(client), inbox))
    }
}

/// A user who subscribes before the upstream connection is up is subscribed when it comes up, and receives.
#[tokio::test]
async fn a_subscription_made_before_the_upstream_is_up_takes_effect_when_it_is() {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let (open_gate, gate) = tokio::sync::oneshot::channel();
    let proxy = Proxy::start(open(), Arc::new(Gated { engine: engine.clone(), gate: Mutex::new(Some(gate)) }));
    assert!(!proxy.is_ready());
    let (handle, mut events) = proxy.attach(Arc::new(User::new("early", ["*"]))).await.unwrap();
    let handle = Arc::new(handle);
    let joining = tokio::spawn({
        let handle = handle.clone();
        async move { handle.subscribe("room").await }
    });
    // Let the join reach the hub while it is still disconnected.
    tokio::task::yield_now().await;
    let _ = open_gate.send(());
    tokio::time::timeout(WAIT, joining).await.expect("the early subscription never took effect").unwrap().unwrap();
    let (peer, _inbox) = ruvent::connect(&engine, "peer", 64).await.unwrap();
    dispatch(&peer, "room", 3).await;
    assert_eq!(next(&mut events).await.unwrap()["payload"], 3);
}
