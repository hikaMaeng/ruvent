//! The fan-out against engines in this process, through the client SDK
//! (plan 4장, 2.8).

mod support;

use ruvent::{Channel, Envelope};
use ruvent_proxy::{Fanout, FanoutConfig, Member, SubscribeError, Upstream};
use std::sync::Arc;
use std::time::Duration;
use support::*;
use tokio::sync::mpsc;

struct Setup {
    engine: ruvent::Engine,
    fanout: Fanout,
    tap: Arc<Tap>,
}

async fn setup(config: FanoutConfig) -> Setup {
    let (engine, plain) = one().await;
    let (tapped, tap) = Tapped::new(plain.connector.clone());
    let upstream = Upstream { connector: tapped, ..plain };
    let fanout = Fanout::start(config, upstream);
    Setup { engine, fanout, tap }
}

fn room() -> Channel {
    support::ch("room")
}

async fn next(events: &mut mpsc::Receiver<Arc<Envelope>>) -> Option<u64> {
    within("the member's stream", events.recv()).await.map(|event| String::from_utf8_lossy(&event.payload).parse().unwrap())
}

async fn member(fanout: &Fanout) -> (Member, mpsc::Receiver<Arc<Envelope>>) {
    fanout.attach().await.unwrap()
}

async fn tick(engine: &ruvent::Engine, channel: &str, n: u64) {
    dispatch(engine, channel, n.to_string()).await;
}

/// Publishes a marker event and returns once a member receives it: everything before it has been fanned out.
async fn drain_to_marker(engine: &ruvent::Engine, channel: &str, events: &mut mpsc::Receiver<Arc<Envelope>>) -> Vec<u64> {
    tick(engine, channel, 999_999).await;
    let mut seen = Vec::new();
    loop {
        let n = next(events).await.expect("stream ended");
        if n == 999_999 {
            return seen;
        }
        seen.push(n);
    }
}

async fn next_call(calls: &mut mpsc::UnboundedReceiver<String>) -> String {
    within("an observation call", calls.recv()).await.unwrap()
}

/// Sends on `engine` until `events` receives something: the subscription is in effect on it.
async fn first_after(engine: &ruvent::Engine, events: &mut mpsc::Receiver<Arc<Envelope>>) -> Arc<Envelope> {
    for n in 0..200u64 {
        tick(engine, "room", n).await;
        if let Ok(Some(event)) = tokio::time::timeout(Duration::from_millis(20), events.recv()).await {
            return event;
        }
    }
    panic!("no event reached the member");
}

// ── the subscription rule ────────────────────────────────────────────────

/// Three members on one channel: the event server delivers each event to the fan-out once, and each member gets it.
#[tokio::test]
async fn three_members_on_one_channel_cost_one_upstream_delivery() {
    let setup = setup(FanoutConfig::default()).await;
    assert!(setup.fanout.ready().await);
    let mut members = Vec::new();
    for _ in 0..3 {
        let (handle, events) = member(&setup.fanout).await;
        handle.subscribe(room()).await.unwrap();
        members.push((handle, events));
    }
    tick(&setup.engine, "room", 1).await;
    for (_, events) in &mut members {
        assert_eq!(next(events).await, Some(1));
    }
    assert_eq!(setup.tap.delivered("room"), 1, "one delivery reached the fan-out for three members");
}

/// A member joining a shared channel gets only events after its join took effect.
#[tokio::test]
async fn a_late_joiner_gets_only_events_after_it_joined() {
    let setup = setup(FanoutConfig::default()).await;
    assert!(setup.fanout.ready().await);
    let (a, mut a_events) = member(&setup.fanout).await;
    a.subscribe(room()).await.unwrap();
    tick(&setup.engine, "room", 1).await;
    let (b, mut b_events) = member(&setup.fanout).await;
    b.subscribe(room()).await.unwrap();
    tick(&setup.engine, "room", 2).await;
    assert_eq!(drain_to_marker(&setup.engine, "room", &mut a_events).await, vec![1, 2]);
    assert_eq!(drain_to_marker(&setup.engine, "room", &mut b_events).await, vec![2], "event 1 was in the fan-out before b's join took effect");
    drop((a, b));
}

/// `0 → 1 → 0 → 1`: nothing sent while unsubscribed reaches the second subscription.
#[tokio::test]
async fn resubscribing_never_shows_events_from_the_gap() {
    let setup = setup(FanoutConfig::default()).await;
    assert!(setup.fanout.ready().await);
    let (a, mut events) = member(&setup.fanout).await;
    a.subscribe(room()).await.unwrap();
    tick(&setup.engine, "room", 1).await;
    assert_eq!(next(&mut events).await, Some(1));
    a.unsubscribe(room()).await;
    for n in 2..20 {
        tick(&setup.engine, "room", n).await;
    }
    a.subscribe(room()).await.unwrap();
    tick(&setup.engine, "room", 20).await;
    assert_eq!(next(&mut events).await, Some(20), "the first event after resubscribing is the one published after it");
}

/// When the last member leaves a channel, the fan-out unsubscribes upstream, once.
#[tokio::test]
async fn the_last_member_leaving_cancels_the_upstream_subscription() {
    let setup = setup(FanoutConfig::default()).await;
    let mut calls = setup.tap.follow();
    assert!(setup.fanout.ready().await);
    let (a, _a_events) = member(&setup.fanout).await;
    let (b, _b_events) = member(&setup.fanout).await;
    a.subscribe(room()).await.unwrap();
    b.subscribe(room()).await.unwrap();
    a.unsubscribe(room()).await;
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

/// The upstream connection drops: the SDK reconnects, the fan-out's subscriptions are said again, and members keep receiving.
#[tokio::test]
async fn after_the_upstream_drops_the_fanout_resubscribes() {
    let setup = setup(FanoutConfig::default()).await;
    let mut calls = setup.tap.follow();
    assert!(setup.fanout.ready().await);
    let (handle, mut events) = member(&setup.fanout).await;
    handle.subscribe(room()).await.unwrap();
    assert_eq!(next_call(&mut calls).await, "subscribe room");
    setup.tap.cut();
    assert_eq!(next_call(&mut calls).await, "subscribe room", "said again on the new connection");
    eventually("the fan-out is ready again", || setup.fanout.is_ready()).await;
    assert_eq!(first_after(&setup.engine, &mut events).await.channel, room());
    drop(handle);
}

/// A member who subscribes before any server is up is subscribed when one comes up, and receives.
#[tokio::test]
async fn a_subscription_made_before_the_upstream_is_up_takes_effect_when_it_is() {
    let local = Arc::new(ruvent_proxy::Local::new());
    let fanout = Fanout::start(FanoutConfig::default(), Upstream::new(local.clone(), ruvent_proxy::Servers::new(["s0"])));
    assert!(!fanout.is_ready());
    let (handle, mut events) = fanout.attach().await.unwrap();
    let handle = Arc::new(handle);
    let joining = tokio::spawn({
        let handle = handle.clone();
        async move { handle.subscribe(room()).await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!joining.is_finished(), "no server confirmed it yet");
    let engine = ruvent::Engine::volatile(ruvent::EngineConfig::default()).await;
    local.insert("s0", engine.clone());
    within("the early subscription", joining).await.unwrap().unwrap();
    tick(&engine, "room", 3).await;
    assert_eq!(next(&mut events).await, Some(3));
}

/// A member that leaves before its subscription took effect is told so.
#[tokio::test]
async fn leaving_before_the_subscription_took_effect_answers_left() {
    let local = Arc::new(ruvent_proxy::Local::new());
    let fanout = Fanout::start(FanoutConfig::default(), Upstream::new(local, ruvent_proxy::Servers::new(["s0"])));
    let (handle, _events) = fanout.attach().await.unwrap();
    let handle = Arc::new(handle);
    let joining = tokio::spawn({
        let handle = handle.clone();
        async move { handle.subscribe(room()).await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    handle.unsubscribe(room()).await;
    assert_eq!(within("the join's answer", joining).await.unwrap(), Err(SubscribeError::Left));
}

// ── several servers ──────────────────────────────────────────────────────

/// A server added to the list is connected and subscribed for every member already there; its events reach them.
#[tokio::test]
async fn a_server_added_to_the_list_is_subscribed_for_every_member() {
    let (engines, local, servers) = engines(2).await;
    servers.set(["s0"]);
    let fanout = Fanout::start(FanoutConfig::default(), Upstream::new(local, servers.clone()));
    assert!(fanout.ready().await);
    let (handle, mut events) = member(&fanout).await;
    handle.subscribe(room()).await.unwrap();
    servers.set(["s0", "s1"]);
    eventually("both servers are up", || fanout.health().servers.len() == 2 && fanout.is_ready()).await;
    assert_eq!(first_after(&engines[1], &mut events).await.channel, room());
    drop(handle);
}

// ── members ──────────────────────────────────────────────────────────────

/// A member whose buffer fills is detached; the others keep receiving.
#[tokio::test]
async fn a_slow_member_is_detached_and_nobody_else_is() {
    let setup = setup(FanoutConfig { member_buffer: 4, ..FanoutConfig::default() }).await;
    assert!(setup.fanout.ready().await);
    let (slow, mut slow_events) = member(&setup.fanout).await;
    let (fast, mut fast_events) = member(&setup.fanout).await;
    slow.subscribe(room()).await.unwrap();
    fast.subscribe(room()).await.unwrap();
    let reader = tokio::spawn(async move {
        let mut got = 0;
        while got < 20 {
            next(&mut fast_events).await.expect("fast member cut off");
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
    assert!(kept <= 4, "the slow member's stream ended after at most its buffer, got {kept}");
    drop((slow, fast));
}

/// Members take the envelopes the event server sent: shared, the payload bytes as they are.
#[tokio::test]
async fn members_share_the_envelopes_as_the_server_sent_them() {
    let setup = setup(FanoutConfig::default()).await;
    assert!(setup.fanout.ready().await);
    let (a, mut a_events) = member(&setup.fanout).await;
    let (b, mut b_events) = member(&setup.fanout).await;
    a.subscribe(room()).await.unwrap();
    b.subscribe(room()).await.unwrap();
    dispatch(&setup.engine, "room", &b"\xff\x00binary"[..]).await;
    let (one, two) = (within("a's event", a_events.recv()).await.unwrap(), within("b's event", b_events.recv()).await.unwrap());
    assert_eq!((one.channel, &one.payload[..]), (room(), &b"\xff\x00binary"[..]));
    assert!(Arc::ptr_eq(&one, &two), "one envelope, shared");
    drop((a, b));
}

/// Stopping ends every stream, answers later calls "stopped" and lets go of the upstream.
#[tokio::test]
async fn stopping_the_hub_ends_every_stream_and_lets_go_of_the_upstream() {
    let setup = setup(FanoutConfig::default()).await;
    assert!(setup.fanout.ready().await);
    assert!(setup.tap.live() >= 1, "the observation connection is open");
    let (handle, mut events) = member(&setup.fanout).await;
    handle.subscribe(room()).await.unwrap();
    setup.fanout.stop().await;
    assert!(next(&mut events).await.is_none(), "the member's stream ended");
    assert!(!setup.fanout.is_ready());
    assert!(!setup.fanout.ready().await, "waiting for readiness returns at once once stopped");
    assert_eq!(setup.fanout.attach().await.err(), Some(SubscribeError::Stopped));
    assert_eq!(handle.subscribe(support::ch("other")).await, Err(SubscribeError::Stopped));
    eventually("every upstream connection is let go", || setup.tap.live() == 0).await;
}

/// When every handle is gone the hub ends by itself and lets go of the upstream.
#[tokio::test]
async fn dropping_every_handle_stops_the_hub() {
    let setup = setup(FanoutConfig::default()).await;
    assert!(setup.fanout.ready().await);
    let (handle, _events) = member(&setup.fanout).await;
    handle.subscribe(room()).await.unwrap();
    drop(handle);
    let tap = setup.tap.clone();
    drop(setup);
    eventually("the hub kept running with no handle", || tap.live() == 0).await;
}
