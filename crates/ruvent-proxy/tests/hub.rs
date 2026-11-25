//! The proxy hub against engines in this process, through the client SDK
//! (plan 4장, 2.8, 2.9).

mod support;

use ruvent::{Bytes, Envelope, EventId, Hello, Methods, Target};
use ruvent_client::worker::{Service, Worker};
use ruvent_proxy::config::{AuthMode, StaticToken, WorkerAuth};
use ruvent_proxy::{Context, Denied, Hooks, Outgoing, Proxy, ProxyConfig, Publish, Rejection, Request, RequestError, Upstream, User};
use serde_json::Value;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use support::*;
use tokio::sync::mpsc;

struct Setup {
    engine: ruvent::Engine,
    proxy: Proxy,
    tap: Arc<Tap>,
    upstream: Upstream,
}

async fn setup(config: ProxyConfig) -> Setup {
    setup_with(config, Arc::new(ruvent_proxy::Defaults)).await
}

async fn setup_with(config: ProxyConfig, hooks: Arc<dyn Hooks>) -> Setup {
    let (engine, plain) = one().await;
    let (tapped, tap) = Tapped::new(plain.connector.clone());
    let upstream = Upstream { connector: tapped, ..plain };
    let proxy = Proxy::start_with(config, upstream.clone(), hooks);
    Setup { engine, proxy, tap, upstream }
}

fn open() -> ProxyConfig {
    ProxyConfig::open("127.0.0.1:0", "s0")
}

async fn next(events: &mut mpsc::Receiver<Arc<str>>) -> Option<Value> {
    within("the user's stream", events.recv()).await.map(|text| serde_json::from_str(&text).unwrap())
}

async fn user(proxy: &Proxy, name: &str) -> (ruvent_proxy::UserHandle, mpsc::Receiver<Arc<str>>) {
    proxy.attach(Arc::new(User::new(name, ["*"]))).await.unwrap()
}

async fn tick(engine: &ruvent::Engine, channel: &str, n: u64) {
    dispatch(engine, channel, n.to_string()).await;
}

/// Publishes a marker event and returns once a user receives it: everything before it has been fanned out.
async fn drain_to_marker(engine: &ruvent::Engine, channel: &str, events: &mut mpsc::Receiver<Arc<str>>) -> Vec<u64> {
    tick(engine, channel, 999_999).await;
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

async fn next_call(calls: &mut mpsc::UnboundedReceiver<String>) -> String {
    within("an observation call", calls.recv()).await.unwrap()
}

// ── the subscription rule ────────────────────────────────────────────────

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
    tick(&setup.engine, "room", 1).await;
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
    tick(&setup.engine, "room", 1).await;
    let (b, mut b_events) = user(&setup.proxy, "b").await;
    b.subscribe("room").await.unwrap();
    tick(&setup.engine, "room", 2).await;
    assert_eq!(drain_to_marker(&setup.engine, "room", &mut a_events).await, vec![1, 2]);
    assert_eq!(drain_to_marker(&setup.engine, "room", &mut b_events).await, vec![2], "event 1 was in the proxy before b's join took effect");
    drop((a, b));
}

/// `0 → 1 → 0 → 1`: nothing sent while unsubscribed reaches the second subscription.
#[tokio::test]
async fn resubscribing_never_shows_events_from_the_gap() {
    let setup = setup(open()).await;
    assert!(setup.proxy.ready().await);
    let (a, mut events) = user(&setup.proxy, "a").await;
    a.subscribe("room").await.unwrap();
    tick(&setup.engine, "room", 1).await;
    assert_eq!(next(&mut events).await.unwrap()["payload"], 1);
    a.unsubscribe("room").await;
    for n in 2..20 {
        tick(&setup.engine, "room", n).await;
    }
    a.subscribe("room").await.unwrap();
    tick(&setup.engine, "room", 20).await;
    assert_eq!(next(&mut events).await.unwrap()["payload"], 20, "the first event after resubscribing is the one published after it");
}

/// When the last user leaves a channel, the proxy unsubscribes upstream, once.
#[tokio::test]
async fn the_last_user_leaving_cancels_the_upstream_subscription() {
    let setup = setup(open()).await;
    let mut calls = setup.tap.follow();
    assert!(setup.proxy.ready().await);
    let (a, _a_events) = user(&setup.proxy, "a").await;
    let (b, _b_events) = user(&setup.proxy, "b").await;
    a.subscribe("room").await.unwrap();
    b.subscribe("room").await.unwrap();
    a.unsubscribe("room").await;
    drop(b);
    loop {
        let call = next_call(&mut calls).await;
        if call == "unsubscribe room" {
            break;
        }
        assert_eq!(call, "subscribe room");
    }
    assert_eq!(setup.tap.calls().iter().filter(|call| *call == "unsubscribe room").count(), 1);
}

/// The upstream connection drops: the SDK reconnects, the proxy's subscriptions are said again, and users keep receiving.
#[tokio::test]
async fn after_the_upstream_drops_the_proxy_resubscribes() {
    let setup = setup(open()).await;
    let mut calls = setup.tap.follow();
    assert!(setup.proxy.ready().await);
    let (handle, mut events) = user(&setup.proxy, "a").await;
    handle.subscribe("room").await.unwrap();
    assert_eq!(next_call(&mut calls).await, "subscribe room");
    setup.tap.cut();
    assert_eq!(next_call(&mut calls).await, "subscribe room", "said again on the new connection");
    eventually("the proxy is ready again", || setup.proxy.is_ready()).await;
    // Once the hub has read the new connection's confirmation, what is published reaches the user.
    let mut got = None;
    for n in 0..200u64 {
        tick(&setup.engine, "room", n).await;
        if let Ok(Some(text)) = tokio::time::timeout(std::time::Duration::from_millis(20), events.recv()).await {
            got = Some(serde_json::from_str::<Value>(&text).unwrap());
            break;
        }
    }
    let got = got.expect("events reach the user after the reconnection");
    assert_eq!(got["op"], "event");
    drop(handle);
}

/// A user who subscribes before any server is up is subscribed when one comes up, and receives.
#[tokio::test]
async fn a_subscription_made_before_the_upstream_is_up_takes_effect_when_it_is() {
    let local = Arc::new(ruvent_proxy::Local::new());
    let proxy = Proxy::start(open(), Upstream::new(local.clone(), ruvent_proxy::Servers::new(["s0"])));
    assert!(!proxy.is_ready());
    let (handle, mut events) = proxy.attach(Arc::new(User::new("early", ["*"]))).await.unwrap();
    let handle = Arc::new(handle);
    let joining = tokio::spawn({
        let handle = handle.clone();
        async move { handle.subscribe("room").await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(!joining.is_finished(), "no server confirmed it yet");
    let engine = ruvent::Engine::volatile(ruvent::EngineConfig::default()).await;
    local.insert("s0", engine.clone());
    within("the early subscription", joining).await.unwrap().unwrap();
    tick(&engine, "room", 3).await;
    assert_eq!(next(&mut events).await.unwrap()["payload"], 3);
}

// ── several servers ──────────────────────────────────────────────────────

/// A server added to the list is connected and subscribed for every user already there; its events reach them.
#[tokio::test]
async fn a_server_added_to_the_list_is_subscribed_for_every_user() {
    let (engines, local, servers) = engines(2).await;
    servers.set(["s0"]);
    let proxy = Proxy::start(open(), Upstream::new(local, servers.clone()));
    assert!(proxy.ready().await);
    let (handle, mut events) = user(&proxy, "a").await;
    handle.subscribe("room").await.unwrap();
    servers.set(["s0", "s1"]);
    eventually("both servers are up", || proxy.health().servers.len() == 2 && proxy.is_ready()).await;
    let mut got = None;
    for n in 0..200u64 {
        tick(&engines[1], "room", n).await;
        if let Ok(Some(text)) = tokio::time::timeout(std::time::Duration::from_millis(20), events.recv()).await {
            got = Some(serde_json::from_str::<Value>(&text).unwrap());
            break;
        }
    }
    assert_eq!(got.expect("s1's events reach the user")["op"], "event");
    drop(handle);
}

// ── users ────────────────────────────────────────────────────────────────

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
        tick(&setup.engine, "room", n).await;
    }
    assert_eq!(reader.await.unwrap(), 20);
    let mut kept = 0;
    while next(&mut slow_events).await.is_some() {
        kept += 1;
    }
    assert!(kept <= 4, "the slow user's stream ended after at most its buffer, got {kept}");
    drop((slow, fast));
}

/// A user may take events as the envelopes the event server sent: shared, the payload bytes as they are, never turned into text.
#[tokio::test]
async fn a_raw_user_takes_the_envelopes_as_the_server_sent_them() {
    let setup = setup(open()).await;
    assert!(setup.proxy.ready().await);
    let (raw, mut raw_events) = setup.proxy.attach_raw(Arc::new(User::new("raw", ["*"]))).await.unwrap();
    let (text, mut text_events) = user(&setup.proxy, "text").await;
    raw.subscribe("room").await.unwrap();
    text.subscribe("room").await.unwrap();
    dispatch(&setup.engine, "room", &b"\xff\x00binary"[..]).await;
    match within("the raw event", raw_events.recv()).await.unwrap() {
        Outgoing::Event(event) => assert_eq!((&*event.channel, &event.payload[..]), ("room", &b"\xff\x00binary"[..])),
        other => panic!("expected an event, got {other:?}"),
    }
    assert!(next(&mut text_events).await.unwrap()["payload"].is_string(), "the text user still gets JSON");
    raw.reply(ruvent_proxy::Outcome::ok(None, None)).await;
    assert!(matches!(within("the result", raw_events.recv()).await.unwrap(), Outgoing::Text(_)));
    drop((raw, text));
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

/// A registration through the proxy is committed when answered and carries the end user as `delegate`, the proxy's credential as `principal`.
#[tokio::test]
async fn a_registration_carries_the_end_user_as_delegate() {
    let setup = setup(open()).await;
    assert!(setup.proxy.ready().await);
    let user = User::new("alice", ["*"]);
    let publish: Publish = serde_json::from_str(r#"{"kind":"claimable","channel":"jobs","type":"run","payload":{"n":1}}"#).unwrap();
    let outcome = setup.proxy.publish(&user, publish).await;
    let id: EventId = outcome.id.unwrap().parse().unwrap();
    let (worker, _inbox) = ruvent::connect(&setup.engine, Hello::worker("peer"), 64).await.unwrap();
    let claimed = worker.claim(id, std::time::Duration::from_secs(30)).await.expect("committed before the answer, so claimable now");
    assert_eq!(claimed.event.delegate.as_deref(), Some("alice"));
    assert_eq!(claimed.event.principal.as_deref(), Some("proxy"));
    assert_eq!(&claimed.event.payload[..], br#"{"n":1}"#);
}

/// User messages go through `UserHandle::handle`; results come back in the user's stream with the caller's ref.
#[tokio::test]
async fn requests_are_answered_in_the_users_stream() {
    let setup = setup(open()).await;
    assert!(setup.proxy.ready().await);
    let (handle, mut events) = user(&setup.proxy, "u").await;
    handle.handle(Request::parse(r#"{"op":"subscribe","channel":"room","ref":"s1"}"#).unwrap()).await;
    let result = next(&mut events).await.unwrap();
    assert_eq!((result["op"].clone(), result["ref"].clone(), result["ok"].clone()), (Value::from("result"), Value::from("s1"), Value::from(true)));
    handle.handle(Request::parse(r#"{"op":"publish","kind":"dispatch","channel":"room","type":"t","payload":{"hi":1},"ref":2}"#).unwrap()).await;
    let first = next(&mut events).await.unwrap();
    let second = next(&mut events).await.unwrap();
    let (event, result) = if first["op"] == "event" { (first, second) } else { (second, first) };
    assert_eq!(event["payload"]["hi"], 1);
    assert_eq!(result["ref"], 2);
    assert_eq!(result["id"], event["id"], "the id the proxy gave is the one the event carries");
}

/// A publish's own shape is checked before the hook runs: a malformed id never reaches the hook.
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

// ── authentication ───────────────────────────────────────────────────────

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
    config.auth.worker = Some(WorkerAuth { channel: "auth".into(), event_type: "authenticate".into(), timeout_ms });
    config
}

/// An authentication worker on the SDK: accepts token "good" as user "carol", answering on the request's reply channel.
fn auth_worker(upstream: &Upstream) -> Worker {
    Worker::builder(upstream.connector.clone(), upstream.servers.as_ref())
        .service(Service::new("auth").on("authenticate", |cx| async move {
            let request: Value = serde_json::from_slice(cx.payload()).unwrap();
            let answer = if request["token"] == "good" { r#"{"ok":true,"user":"carol","channels":["carol.*"]}"# } else { r#"{"ok":false}"# };
            cx.reply(Bytes::from_static(answer.as_bytes()));
            Ok(())
        }))
        .start()
}

/// Worker authentication (Q1(a)) is one request–response: the worker's answer decides, matched by correlation.
#[tokio::test]
async fn an_authentication_worker_lets_in_and_turns_away() {
    let setup = setup(worker_config(5_000)).await;
    let worker = auth_worker(&setup.upstream);
    assert!(setup.proxy.ready().await);
    let carol = setup.proxy.authenticate_by_config(Some("good")).await.unwrap();
    assert_eq!(carol.name, "carol");
    assert!(carol.may_use("carol.inbox") && !carol.may_use("other"));
    assert_eq!(setup.proxy.authenticate_by_config(Some("bad")).await.unwrap_err(), Denied::Rejected);
    worker.shutdown().await;
}

/// Worker authentication with no worker running: the user is turned away at the deadline.
#[tokio::test]
async fn an_authentication_nobody_answers_times_out() {
    let setup = setup(worker_config(200)).await;
    assert!(setup.proxy.ready().await);
    assert_eq!(setup.proxy.authenticate_by_config(Some("good")).await.unwrap_err(), Denied::Timeout);
}

// ── request–response ─────────────────────────────────────────────────────

/// `exchange` with no worker ends at its deadline; once the proxy stopped it answers closed at once.
#[tokio::test]
async fn an_exchange_ends_at_its_deadline_and_is_closed_once_the_proxy_stopped() {
    let setup = setup(open()).await;
    assert!(setup.proxy.ready().await);
    let options = ruvent_proxy::RequestOptions { deadline: std::time::Duration::from_millis(150), retries: 0 };
    let ask = || Envelope::claimable("prices", "quote", Bytes::from_static(b"x"));
    assert_eq!(setup.proxy.exchange_with(ask(), options).await.unwrap_err(), RequestError::Timeout);
    setup.proxy.stop().await;
    eventually("the hub let go", || setup.tap.live() == 0).await;
    assert_eq!(setup.proxy.exchange(ask()).await.unwrap_err(), RequestError::Closed);
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
    tick(&setup.engine, "room", 1).await;
    assert_eq!(next(&mut alice_events).await.unwrap()["payload"], 1);
    assert!(next(&mut mallory_events).await.is_none(), "the user the hook failed for is disconnected");
    assert!(setup.proxy.is_ready(), "the hub is still running");
    let (carol, mut carol_events) = user(&setup.proxy, "carol").await;
    carol.subscribe("room").await.unwrap();
    tick(&setup.engine, "room", 2).await;
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
    let mut calls = setup.tap.follow();
    assert!(setup.proxy.ready().await);
    let (handle, events) = user(&setup.proxy, "gone").await;
    handle.subscribe("room").await.unwrap();
    assert_eq!(next_call(&mut calls).await, "subscribe room");
    // The reading side goes away while the handle (and so no Detach) stays.
    drop(events);
    tick(&setup.engine, "room", 1).await;
    assert_eq!(next_call(&mut calls).await, "unsubscribe room");
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

/// A subscribe hook that writes an audit event through the hook's upstream calls before every subscription.
struct Audits;

#[ruvent_proxy::async_trait]
impl Hooks for Audits {
    async fn subscribe(&self, cx: Context<'_>, user: &User, target: &Target) -> Result<(), Rejection> {
        let upstream = cx.upstream().ok_or_else(|| Rejection::unavailable("proxy stopped"))?;
        let audit = Envelope::dispatch("audit", "subscribed", Bytes::from(format!("{} {}", user.name, target.name())));
        upstream.send_committed(audit).await.map_err(Rejection::from_producer)?;
        Ok(())
    }
}

/// A hook calls the event server through the proxy's producer, and the subscription rule still holds:
/// a late joiner gets only events after its own join took effect.
#[tokio::test]
async fn a_hooks_upstream_calls_leave_the_subscription_rule_intact() {
    let setup = setup_with(open(), Arc::new(Audits)).await;
    let (_watcher, mut audits) = watch_target(&setup.engine, Target::Channel("audit".into())).await;
    assert!(setup.proxy.ready().await);
    let (a, mut a_events) = user(&setup.proxy, "a").await;
    a.subscribe("room").await.unwrap();
    tick(&setup.engine, "room", 1).await;
    let (b, mut b_events) = user(&setup.proxy, "b").await;
    b.subscribe("room").await.unwrap();
    tick(&setup.engine, "room", 2).await;
    assert_eq!(drain_to_marker(&setup.engine, "room", &mut a_events).await, vec![1, 2]);
    assert_eq!(drain_to_marker(&setup.engine, "room", &mut b_events).await, vec![2]);
    let mut seen = Vec::new();
    while seen.len() < 2 {
        seen.push(String::from_utf8(next_delivery(&mut audits).await.payload.to_vec()).unwrap());
    }
    assert_eq!(seen, vec!["a room", "b room"]);
    drop((a, b));
}

// ── the hub's lifetime ───────────────────────────────────────────────────

/// Stopping the hub ends every user's stream, makes it not ready, refuses new users and lets go of every upstream connection.
#[tokio::test]
async fn stopping_the_hub_ends_every_stream_and_lets_go_of_the_upstream() {
    let setup = setup(open()).await;
    assert!(setup.proxy.ready().await);
    assert!(setup.tap.live() >= 3, "observation, production and work connections are open");
    let (handle, mut events) = user(&setup.proxy, "a").await;
    handle.subscribe("room").await.unwrap();
    setup.proxy.stop().await;
    assert!(next(&mut events).await.is_none(), "the user's stream ended");
    assert!(!setup.proxy.is_ready());
    assert!(!setup.proxy.ready().await, "waiting for readiness returns at once once stopped");
    assert_eq!(setup.proxy.attach(Arc::new(User::new("late", ["*"]))).await.err().as_deref(), Some("proxy stopped"));
    assert_eq!(handle.subscribe("other").await.unwrap_err().message, "proxy stopped");
    eventually("every upstream connection is let go", || setup.tap.live() == 0).await;
}

/// When every handle is gone the hub ends by itself and lets go of the upstream.
#[tokio::test]
async fn dropping_every_handle_stops_the_hub() {
    let setup = setup(open()).await;
    assert!(setup.proxy.ready().await);
    let (handle, _events) = user(&setup.proxy, "a").await;
    handle.subscribe("room").await.unwrap();
    drop(handle);
    let tap = setup.tap.clone();
    drop(setup);
    eventually("the hub kept running with no handle", || tap.live() == 0).await;
}
