//! The fan-out against real event servers over TCP: no in-process shortcut,
//! the upstream is the SDK's `Tcp` connector to every server named.
//!
//! Each test quotes the requirement it judges: the overhaul plan's proxy
//! chapter (`docs/proposals/library-overhaul.md` 4장).

mod support;

use ruvent::{Channel, Engine, EngineConfig, Envelope, Refusal};
use ruvent_client::link::Connector;
use ruvent_client::producer::{Producer, ProducerConfig};
use ruvent_client::worker::{Service, Worker};
use ruvent_proxy::{Fanout, FanoutConfig, Servers, SubscribeError, Tcp, Upstream};
use ruvent_server::{serve, Credentials, ServerOptions};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use support::{eventually, within};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};

/// A real event server on a TCP port, and its engine for the test's own traffic.
struct EventServer {
    engine: Engine,
    address: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<io::Result<()>>,
}

impl EventServer {
    /// Stops serving: every connection to it is closed.
    async fn stop(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let _ = tokio::time::timeout(support::WAIT, &mut self.task).await;
    }
}

async fn event_server() -> EventServer {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let credentials = Arc::new(Credentials::new([("proxy".to_owned(), "proxy-token".to_owned())]));
    let (stop, stopped) = oneshot::channel::<()>();
    let task = tokio::spawn(serve(listener, engine.clone(), credentials, ServerOptions::default(), async move {
        let _ = stopped.await;
    }));
    EventServer { engine, address, stop: Some(stop), task }
}

/// A fan-out over TCP to `servers`, ready.
async fn fanout(servers: &[&EventServer]) -> Fanout {
    let addresses: Vec<String> = servers.iter().map(|server| server.address.to_string()).collect();
    let fanout = Fanout::start(FanoutConfig::default(), Upstream::new(Arc::new(Tcp::new("proxy-token")), Servers::new(addresses)));
    assert!(within("the fan-out is ready", fanout.ready()).await);
    fanout
}

async fn next(events: &mut mpsc::Receiver<Arc<Envelope>>) -> Arc<Envelope> {
    within("a member's event", events.recv()).await.expect("the member's stream ended")
}

fn text(event: &Arc<Envelope>) -> String {
    String::from_utf8_lossy(&event.payload).into_owned()
}

fn channel(name: &str) -> Channel {
    support::ch(name)
}

/// > 신규 join은 자기 구독 표지를 기다리고, 이미 활성인 같은 사용자의 재호출은 즉시 성공한다
///
/// (Plan 4장, "사용자 구독의 불변식".) Over TCP: a late joiner sees only
/// later events, a second subscribe of an active member succeeds at once,
/// and a member who leaves and comes back sees nothing sent in between
/// (`0 → 1 → 0 → 1`).
#[tokio::test]
async fn the_subscription_rule_holds_through_a_real_event_server() {
    let server = event_server().await;
    let fanout = fanout(&[&server]).await;
    let (early, mut early_events) = fanout.attach().await.unwrap();
    early.subscribe(channel("room")).await.unwrap();
    support::dispatch(&server.engine, "room", "1").await;
    assert_eq!(text(&next(&mut early_events).await), "1");
    let (late, mut late_events) = fanout.attach().await.unwrap();
    late.subscribe(channel("room")).await.unwrap();
    within("an active member's second subscribe", late.subscribe(channel("room"))).await.unwrap();
    support::dispatch(&server.engine, "room", "2").await;
    assert_eq!(text(&next(&mut early_events).await), "2");
    assert_eq!(text(&next(&mut late_events).await), "2", "event 1 was sent before the late join took effect");

    early.unsubscribe(channel("room")).await;
    late.unsubscribe(channel("room")).await;
    support::dispatch(&server.engine, "room", "3").await;
    early.subscribe(channel("room")).await.unwrap();
    support::dispatch(&server.engine, "room", "4").await;
    assert_eq!(text(&next(&mut early_events).await), "4", "nothing from the gap");
}

/// > 채널은 특수한 이름이나 식별자를 갖지 않고 uuid7에 근접한 일관된 형식만 받는다.
///
/// (Plan 9.15.) A channel is a UUIDv7: a subscription to anything else is
/// refused by the event server and the member hears that refusal; a member
/// on one channel hears nothing of another, however alike (no prefixes).
#[tokio::test]
async fn a_channel_that_is_not_a_uuid_v7_is_refused_through_a_real_event_server() {
    let server = event_server().await;
    let fanout = fanout(&[&server]).await;
    let (member, mut events) = fanout.attach().await.unwrap();
    let mut v4 = channel("room").0;
    v4[6] = (v4[6] & 0x0f) | 0x40;
    for bad in [Channel([0; 16]), Channel(v4)] {
        assert_eq!(within("the refusal", member.subscribe(bad)).await, Err(SubscribeError::Refused(Refusal::Malformed)), "{bad}");
    }
    member.subscribe(channel("room")).await.unwrap();
    support::dispatch(&server.engine, "room.1", "0").await;
    support::dispatch(&server.engine, "room", "1").await;
    assert_eq!(text(&next(&mut events).await), "1", "only the channel itself reaches its member");
}

/// > 상향 연결은 서버마다 역할별로 하나다. … 서버 목록 공급자를 받아 모든 서버에 붙도록 넓힌다.
///
/// (Plan 4장.) A fan-out given two event servers connects to both, holds
/// the member's subscription on each, and hands the member what either
/// dispatches. Its health names both.
#[tokio::test]
async fn a_fanout_on_two_servers_hands_members_what_either_dispatches() {
    let (one, two) = (event_server().await, event_server().await);
    let fanout = fanout(&[&one, &two]).await;
    assert_eq!(fanout.health().servers.len(), 2);
    assert!(fanout.health().servers.iter().all(|server| server.observing));
    let (member, mut events) = fanout.attach().await.unwrap();
    member.subscribe(channel("room")).await.unwrap();
    // The join answers once its subscription took effect on a server; the
    // other server's confirmation is already queued ahead of its events.
    support::dispatch(&one.engine, "room", "1").await;
    support::dispatch(&two.engine, "room", "2").await;
    let mut got = vec![text(&next(&mut events).await), text(&next(&mut events).await)];
    got.sort_unstable();
    assert_eq!(got, ["1", "2"], "one event from each server");
}

/// > 조용히 끊긴 상향 연결이 `/health`에 정상으로 남지 않게 한다.
///
/// (Plan 4장.) Of two servers, one goes away: the fan-out turns not ready
/// and its health says which server is down, while the other server keeps
/// serving the members. (A drop without FIN is found by the kernel's
/// keepalive, which the SDK sets on every upstream socket, S13; this test
/// ends the connection outright, so it does not wait out the keepalive.)
#[tokio::test]
async fn an_upstream_that_goes_away_shows_in_health() {
    let (one, two) = (event_server().await, event_server().await);
    let (gone, kept) = (one.address.to_string(), two.address.to_string());
    let fanout = fanout(&[&one, &two]).await;
    let (member, mut events) = fanout.attach().await.unwrap();
    member.subscribe(channel("room")).await.unwrap();
    one.stop().await;
    eventually("the fan-out is not ready", || !fanout.is_ready()).await;
    let health = fanout.health();
    let row = |server: &str| health.servers.iter().find(|row| row.server == server).cloned().unwrap();
    assert!(!row(&gone).observing, "{health:?}");
    assert!(row(&kept).observing, "{health:?}");
    support::dispatch(&two.engine, "room", "7").await;
    assert_eq!(text(&next(&mut events).await), "7", "the server still up keeps serving");
}

/// > 여러클라이언트-프록시-여러채널+같은타입으로 요청이벤트 전송-이벤트서버:타입으로 등록된 워커서버에게 이벤트전달-워커서버:요청타입의 이벤트를 전부 접수하고 채널별로 분리하여 인퍼런스 프로세스 진행-… 해당 채널로 스트리밍조각을 페이로드로 이벤트 전송-이벤트서버는 해당 채널로 들어온 발송이벤트를 프록시에게 전달
///
/// (Plan 9.14, 9.15.) Each request is its own channel, a fresh UUIDv7, and
/// every request has the same type. One worker joins the type, not the
/// channels, and answers each request on that request's channel with plain
/// dispatch — pieces while it runs, the last one with its completion. Two
/// requests in flight at once each get their own pieces, in order, and
/// nothing of the other's.
#[tokio::test]
async fn each_request_is_answered_on_its_own_channel_in_order() {
    let server = event_server().await;
    let fanout = fanout(&[&server]).await;
    let connector: Arc<dyn Connector> = Arc::new(Tcp::new("proxy-token"));
    let list = Servers::new([server.address.to_string()]);
    let worker = Worker::builder(connector.clone(), &list)
        .service(Service::typed().on(15, |cx| async move {
            let answer = cx.channel();
            let name = String::from_utf8(cx.payload().to_vec()).unwrap();
            for n in 0..3 {
                cx.emit_now(Envelope::dispatch(answer, 13, format!("{name}:{n}").into_bytes())).await.unwrap();
            }
            cx.emit_dispatch(answer, 14, Vec::new());
            Ok(())
        }))
        .start();
    let producer = Producer::start(connector, &list, ProducerConfig::default());
    let mut asks = Vec::new();
    for name in ["answer.0b1e", "answer.7c2d"] {
        let (member, events) = fanout.attach().await.unwrap();
        member.subscribe(channel(name)).await.unwrap();
        producer.send(Envelope::claimable(channel(name), 15, name.as_bytes().to_vec())).await.unwrap();
        asks.push((name, member, events));
    }
    for (name, member, mut events) in asks {
        let mut got = Vec::new();
        loop {
            let event = next(&mut events).await;
            assert_eq!(event.channel, channel(name), "only this request's channel reaches its member");
            if event.event_type == 14 {
                break;
            }
            got.push(text(&event));
        }
        assert_eq!(got, [format!("{name}:0"), format!("{name}:1"), format!("{name}:2")], "pieces in order, then the end");
        drop(member); // the program's end condition: the subscription is let go
    }
    worker.shutdown().await;
}
