//! The proxy as its users build and run it, against real event servers over
//! TCP: no in-process shortcut, the upstream is the SDK's `Tcp` connector to
//! every server the configuration names.
//!
//! Each test quotes the requirement it judges: the user's words recorded in
//! `docs/proposals/event-server-redesign.md` §13, or the overhaul plan's
//! proxy chapter (`docs/proposals/library-overhaul.md` 4장).

mod support;

use futures_util::{SinkExt, StreamExt};
use ruvent::{Bytes, Engine, EngineConfig, Envelope, Target};
use ruvent_client::servers::Servers;
use ruvent_client::worker::{Service, Worker};
use ruvent_proxy::{async_trait, Context, Denied, Handshake, Hooks, LaunchError, Part, Proxy, RequestOptions, Server, Tcp, User};
use ruvent_server::{serve, Credentials, ServerOptions};
use serde_json::Value;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use support::Relay;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, Notify};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

const WAIT: Duration = Duration::from_secs(15);

type Socket = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;

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
        let _ = tokio::time::timeout(WAIT, &mut self.task).await;
    }
}

async fn event_server() -> EventServer {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let credentials = Arc::new(Credentials::new([("proxy".to_owned(), "proxy-token".to_owned()), ("worker".to_owned(), "worker-token".to_owned())]));
    let (stop, stopped) = oneshot::channel::<()>();
    let task = tokio::spawn(serve(listener, engine.clone(), credentials, ServerOptions::default(), async move {
        let _ = stopped.await;
    }));
    EventServer { engine, address, stop: Some(stop), task }
}

/// A scratch directory of this test's own, removed when dropped.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Scratch {
        let path = std::env::temp_dir().join(format!("ruvent-proxy-acceptance-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&path).unwrap();
        Scratch(path)
    }

    fn write(&self, file: &str, text: &str) -> PathBuf {
        let path = self.0.join(file);
        std::fs::write(&path, text).unwrap();
        path
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A proxy configuration file: static token `t` for user `u` on `room*`, upstream every server named.
fn configuration(listen: &str, servers: &[&str]) -> String {
    let servers: Vec<String> = servers.iter().map(|server| format!("\"{server}\"")).collect();
    format!(
        r#"{{"listen":"{listen}","upstream":{{"servers":[{}],"token":"proxy-token"}},
            "auth":{{"mode":"static","tokens":[{{"token":"t","user":"u","channels":["room*"]}}]}}}}"#,
        servers.join(",")
    )
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// One HTTP request with `Connection: close`: status and JSON body.
async fn http(address: SocketAddr, method: &str, path: &str, headers: &str, body: &str) -> (u16, Value) {
    let mut stream = TcpStream::connect(address).await.unwrap();
    let request = format!("{method} {path} HTTP/1.1\r\nHost: x\r\n{headers}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    tokio::time::timeout(WAIT, stream.read_to_string(&mut response)).await.expect("no response").unwrap();
    let status = response[9..12].parse().unwrap();
    let body = response.split("\r\n\r\n").nth(1).unwrap_or_default();
    (status, serde_json::from_str(body).unwrap_or(Value::Null))
}

async fn socket(address: SocketAddr, query: &str, cookie: Option<&str>) -> Socket {
    let mut request = format!("ws://{address}/v1/ws{query}").into_client_request().unwrap();
    if let Some(cookie) = cookie {
        request.headers_mut().insert("cookie", cookie.parse().unwrap());
    }
    tokio_tungstenite::connect_async(request).await.unwrap().0
}

async fn read_json(socket: &mut Socket) -> Value {
    loop {
        match tokio::time::timeout(WAIT, socket.next()).await.expect("timed out").expect("closed").unwrap() {
            Message::Text(text) => return serde_json::from_str(text.as_str()).unwrap(),
            _ => continue,
        }
    }
}

async fn send(socket: &mut Socket, text: &str) {
    socket.send(Message::Text(text.into())).await.unwrap();
}

async fn subscribed(socket: &mut Socket, request: &str) {
    send(socket, &format!(r#"{{"op":"subscribe",{request},"ref":"s"}}"#)).await;
    let answer = read_json(socket).await;
    assert_eq!((answer["ref"].clone(), answer["ok"].clone()), (Value::from("s"), Value::from(true)), "{answer}");
}

/// Waits for `/health` to answer 200: the proxy is up and every upstream connected.
async fn healthy(address: SocketAddr, child: &mut Option<&mut Running>) {
    let deadline = Instant::now() + WAIT;
    loop {
        if let Some(running) = child.as_deref_mut() {
            running.exited();
        }
        if let Ok(mut stream) = TcpStream::connect(address).await {
            let _ = stream.write_all(b"GET /health HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").await;
            let mut response = String::new();
            let _ = stream.read_to_string(&mut response).await;
            if response.starts_with("HTTP/1.1 200") {
                return;
            }
        }
        assert!(Instant::now() < deadline, "the proxy at {address} never became healthy");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A program running in its own process, killed when dropped.
struct Running(std::process::Child);

impl Running {
    /// Fails the test, with the program's error output, if it already exited.
    fn exited(&mut self) {
        if let Ok(Some(status)) = self.0.try_wait() {
            let mut errors = String::new();
            if let Some(mut stderr) = self.0.stderr.take() {
                let _ = std::io::Read::read_to_string(&mut stderr, &mut errors);
            }
            panic!("the program exited with {status}: {errors}");
        }
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// `target/<profile>/examples/<name>`, built by `cargo test -p ruvent-proxy` (examples are built with the tests).
fn example(name: &str) -> PathBuf {
    let deps = std::env::current_exe().unwrap().parent().unwrap().to_path_buf();
    let path = deps.parent().unwrap().join("examples").join(format!("{name}{}", std::env::consts::EXE_SUFFIX));
    assert!(path.exists(), "{} is not built; run `cargo test -p ruvent-proxy`, which builds the examples", path.display());
    path
}

fn run(program: &Path, argument: &Path) -> Running {
    Running(Command::new(program).arg(argument).stdout(Stdio::null()).stderr(Stdio::piped()).spawn().unwrap())
}

/// A proxy from `configuration(…)`, bound and serving; its address and hub.
async fn proxy_on(name: &str, servers: &[&str]) -> (SocketAddr, Proxy, Scratch) {
    let scratch = Scratch::new(name);
    let path = scratch.write("proxy.json", &configuration("127.0.0.1:0", servers));
    let bound = Server::new().config_file(&path).bind().await.unwrap();
    let address = bound.local_addr().unwrap();
    let proxy = bound.proxy().clone();
    tokio::spawn(bound.serve());
    healthy(address, &mut None).await;
    (address, proxy, scratch)
}

/// A worker over TCP straight to `servers`, with credential `worker`.
fn worker(servers: &[&str], service: Service) -> Worker {
    Worker::builder(Arc::new(Tcp::new("worker-token")), &Servers::new(servers)).service(service).start()
}

// ── a server from nothing but a configuration ────────────────────────────

/// > 만약 그 라이브러리에 별거 없이 빌드를 하면 뚝딱하고 프록시서버가 실행가능한 서버로 성립되긴해야한다.
///
/// The `plain` example is the three-line `main`. Run as its own process
/// with the configuration file as its only argument, it connects to a real
/// event server over TCP and serves both protocols on its one address.
#[tokio::test]
async fn the_three_line_main_is_a_proxy_server_given_only_a_configuration_file() {
    let server = event_server().await;
    let scratch = Scratch::new("plain");
    let listen: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let path = scratch.write("proxy.json", &configuration(&listen.to_string(), &[&server.address.to_string()]));
    let mut running = run(&example("plain"), &path);
    healthy(listen, &mut Some(&mut running)).await;

    let mut user = socket(listen, "?token=t", None).await;
    assert_eq!(read_json(&mut user).await["ok"], true);
    subscribed(&mut user, r#""channel":"room""#).await;
    let (status, result) = http(listen, "POST", "/v1/events", "Authorization: Bearer t\r\n", r#"{"kind":"dispatch","channel":"room","type":"said","payload":{"n":1}}"#).await;
    assert_eq!((status, result["ok"].clone()), (200, Value::from(true)));
    let event = read_json(&mut user).await;
    assert_eq!((event["payload"]["n"].clone(), event["id"].clone()), (Value::from(1), result["id"].clone()));
    let (status, _) = http(listen, "POST", "/v1/events", "Authorization: Bearer wrong\r\n", "{}").await;
    assert_eq!(status, 401, "the configured authentication decides");
}

/// > 라이브러리가 지원하는 프로토콜을 선택해서 빌드하는 것으로 갈음할 수 있지
///
/// Built with both protocol features, one listener answers for both, and
/// for nothing else.
#[tokio::test]
async fn every_compiled_in_protocol_answers_on_the_one_listen_address() {
    let server = event_server().await;
    let (address, _proxy, _scratch) = proxy_on("routes", &[&server.address.to_string()]).await;
    let mut user = socket(address, "?token=t", None).await;
    assert_eq!(read_json(&mut user).await["ok"], true, "ws: GET /v1/ws");
    assert_eq!(http(address, "GET", "/v1/events", "", "").await.0, 405, "http: /v1/events exists, for POST");
    assert_eq!(http(address, "GET", "/v1/stream?channel=room", "", "").await.0, 401, "http: /v1/stream exists, and authenticates");
    assert_eq!(http(address, "GET", "/nothing", "", "").await.0, 404);
}

// ── hooks ────────────────────────────────────────────────────────────────

/// A domain's sessions; nothing else is written.
struct Sessions;

#[async_trait]
impl Hooks for Sessions {
    async fn authenticate(&self, cx: Context<'_>, handshake: &Handshake) -> Result<User, Denied> {
        match handshake.cookie("session") {
            Some("s-alice") => Ok(User::new("alice", ["room*"])),
            Some(_) => Err(Denied::Rejected),
            None => cx.authenticate_by_config(handshake.token.as_deref()).await,
        }
    }
}

/// > 인증을 비롯한 여러 장치를 붙이고 싶을 때도 스프링 컨트롤러 수준으로 쉽게 훅이 되는 기능만 작성하는 것으로 손쉽게 커스터마이즈된 프록시 서버를 만들 수 있어야한다
///
/// One hook method and `.hooks(…)`: session cookies come in, the
/// configured tokens still work, unknown sessions are turned away, and a
/// registration reaches the real event server carrying the end user.
#[tokio::test]
async fn one_hook_method_customizes_a_proxy_against_a_real_event_server() {
    let server = event_server().await;
    let scratch = Scratch::new("hooks");
    let path = scratch.write("proxy.json", &configuration("127.0.0.1:0", &[&server.address.to_string()]));
    let bound = Server::new().config_file(&path).hooks(Sessions).bind().await.unwrap();
    let address = bound.local_addr().unwrap();
    tokio::spawn(bound.serve());
    healthy(address, &mut None).await;
    let (_watcher, mut seen) = support::watch_target(&server.engine, Target::Channel("room".into())).await;

    let mut alice = socket(address, "", Some("session=s-alice")).await;
    assert_eq!(read_json(&mut alice).await["ok"], true);
    subscribed(&mut alice, r#""channel":"room""#).await;
    let mut service = socket(address, "?token=t", None).await;
    assert_eq!(read_json(&mut service).await["ok"], true, "the configured token still works");
    let mut stranger = socket(address, "", Some("session=forged")).await;
    assert_eq!(read_json(&mut stranger).await["error"], "authentication rejected");

    let (status, _) = http(address, "POST", "/v1/events", "Cookie: session=s-alice\r\n", r#"{"kind":"dispatch","channel":"room","type":"said","payload":"hi"}"#).await;
    assert_eq!(status, 200);
    assert_eq!(read_json(&mut alice).await["payload"], "hi");
    let registered = support::next_delivery(&mut seen).await;
    assert_eq!((registered.delegate.as_deref(), registered.principal.as_deref()), (Some("alice"), Some("proxy")));
}

// ── the subscription rule over a real connection ─────────────────────────

/// > 신규 join은 자기 구독 표지를 기다리고, 이미 활성인 같은 사용자의 재호출은 즉시 성공한다
///
/// (Plan 4장, "사용자 구독의 불변식".) Over TCP: a late joiner sees only
/// later events, and a user who leaves and comes back sees nothing sent in
/// between (`0 → 1 → 0 → 1`).
#[tokio::test]
async fn the_subscription_rule_holds_through_a_real_event_server() {
    let server = event_server().await;
    let (address, _proxy, _scratch) = proxy_on("rule", &[&server.address.to_string()]).await;

    let mut early = socket(address, "?token=t", None).await;
    assert_eq!(read_json(&mut early).await["ok"], true);
    subscribed(&mut early, r#""channel":"room""#).await;
    support::dispatch(&server.engine, "room", "1").await;
    assert_eq!(read_json(&mut early).await["payload"], 1);
    let mut late = socket(address, "?token=t", None).await;
    assert_eq!(read_json(&mut late).await["ok"], true);
    subscribed(&mut late, r#""channel":"room""#).await;
    subscribed(&mut late, r#""channel":"room""#).await;
    support::dispatch(&server.engine, "room", "2").await;
    assert_eq!(read_json(&mut early).await["payload"], 2);
    assert_eq!(read_json(&mut late).await["payload"], 2, "event 1 was sent before the late join took effect");

    send(&mut early, r#"{"op":"unsubscribe","channel":"room","ref":"u"}"#).await;
    assert_eq!(read_json(&mut early).await["ref"], "u");
    send(&mut late, r#"{"op":"unsubscribe","channel":"room","ref":"u"}"#).await;
    assert_eq!(read_json(&mut late).await["ref"], "u");
    support::dispatch(&server.engine, "room", "3").await;
    subscribed(&mut early, r#""channel":"room""#).await;
    support::dispatch(&server.engine, "room", "4").await;
    assert_eq!(read_json(&mut early).await["payload"], 4, "nothing from the gap");
}

/// > 접두사 구독(서버). 서버의 구독 집합이 정확 이름과 접두사를 받는다. … 프록시는 사용자 채널 묶음을 접두사 하나로 구독할 수 있다. 지금 권한 패턴의 `starts_with`와의 차이를 시험한다.
///
/// (Plan 4장.) A user whose scope is the pattern `room*` (plain
/// `starts_with`: it allows `roomy`) subscribes the prefix `room`: it gets
/// every channel below `room` at a `.` boundary, at any depth, in order —
/// not `room` itself and not `roomy`, which the pattern would allow.
#[tokio::test]
async fn a_prefix_subscription_takes_every_channel_below_it_through_a_real_event_server() {
    let server = event_server().await;
    let (address, _proxy, _scratch) = proxy_on("prefix", &[&server.address.to_string()]).await;
    let mut user = socket(address, "?token=t", None).await;
    assert_eq!(read_json(&mut user).await["ok"], true);
    subscribed(&mut user, r#""prefix":"room""#).await;
    for (channel, payload) in [("room", "0"), ("room.1", "1"), ("roomy", "2"), ("room.a.b", "3"), ("roomy.1", "4"), ("room.2", "5")] {
        support::dispatch(&server.engine, channel, payload).await;
    }
    let mut got = Vec::new();
    for _ in 0..3 {
        let event = read_json(&mut user).await;
        got.push((event["channel"].as_str().unwrap().to_owned(), event["payload"].as_u64().unwrap()));
    }
    assert_eq!(got, vec![("room.1".to_owned(), 1), ("room.a.b".to_owned(), 3), ("room.2".to_owned(), 5)]);
    support::dispatch(&server.engine, "room.end", "9").await;
    assert_eq!(read_json(&mut user).await["payload"], 9, "nothing else came in between");
}

// ── several servers ──────────────────────────────────────────────────────

/// > 상향 연결은 서버마다 역할별로 하나다. … 서버 목록 공급자를 받아 모든 서버에 붙도록 넓힌다.
///
/// (Plan 4장.) A proxy configured with two event servers connects to both,
/// holds the user's subscription on each, and hands the user what either
/// dispatches. `/health` names both.
#[tokio::test]
async fn a_proxy_on_two_servers_hands_users_what_either_dispatches() {
    let (one, two) = (event_server().await, event_server().await);
    let (address, proxy, _scratch) = proxy_on("two", &[&one.address.to_string(), &two.address.to_string()]).await;
    assert_eq!(proxy.health().servers.len(), 2);
    assert!(proxy.health().servers.iter().all(|server| server.observing && server.replying));
    let mut user = socket(address, "?token=t", None).await;
    assert_eq!(read_json(&mut user).await["ok"], true);
    subscribed(&mut user, r#""channel":"room""#).await;
    // The join answers once its subscription took effect on a server; the
    // other server's confirmation is already queued behind it.
    support::dispatch(&one.engine, "room", "1").await;
    support::dispatch(&two.engine, "room", "2").await;
    let mut got = vec![read_json(&mut user).await["payload"].as_u64().unwrap(), read_json(&mut user).await["payload"].as_u64().unwrap()];
    got.sort_unstable();
    assert_eq!(got, vec![1, 2], "one event from each server");
}

/// > 조용히 끊긴 상향 연결이 `/health`에 정상으로 남지 않게 한다.
///
/// (Plan 4장.) Of two servers, one goes away: `/health` turns 503 and its
/// body says which server is down, while the other server keeps serving
/// the users. (A drop without FIN is found by the kernel's keepalive, which
/// the SDK sets on every upstream socket, S13; this test ends the
/// connection outright, so it does not wait out the keepalive time.)
#[tokio::test]
async fn an_upstream_that_goes_away_shows_in_health() {
    let (one, two) = (event_server().await, event_server().await);
    let (gone, kept) = (one.address.to_string(), two.address.to_string());
    let (address, proxy, _scratch) = proxy_on("health", &[&gone, &kept]).await;
    let mut user = socket(address, "?token=t", None).await;
    assert_eq!(read_json(&mut user).await["ok"], true);
    subscribed(&mut user, r#""channel":"room""#).await;
    one.stop().await;
    let deadline = Instant::now() + WAIT;
    let (status, body) = loop {
        let (status, body) = http(address, "GET", "/health", "", "").await;
        if status == 503 {
            break (status, body);
        }
        assert!(Instant::now() < deadline, "/health still answers {status} with {body}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!((status, body["ready"].clone()), (503, Value::from(false)));
    let row = |server: &str| body["servers"].as_array().unwrap().iter().find(|row| row["server"] == server).cloned().unwrap();
    assert_eq!(row(&gone)["observing"], false, "{body}");
    assert_eq!((row(&kept)["observing"].clone(), row(&kept)["replying"].clone()), (Value::from(true), Value::from(true)), "{body}");
    assert!(!proxy.is_ready());
    support::dispatch(&two.engine, "room", "7").await;
    assert_eq!(read_json(&mut user).await["payload"], 7, "the server still up keeps serving");
}

// ── request–response ─────────────────────────────────────────────────────

/// > 스트림 조각도 같은 작업 연결로 받는다(2.8). 조각과 종결 결과의 순서가 연결 하나로 지켜진다.
///
/// (Plan 4장.) A worker sends three fragments and then its result; the
/// proxy's `exchange` gets the fragments first, in order, then the result —
/// as a stream and as a whole response.
#[tokio::test]
async fn an_exchange_gets_its_fragments_in_order_then_its_result() {
    let server = event_server().await;
    let at = server.address.to_string();
    let (_address, proxy, _scratch) = proxy_on("fragments", &[&at]).await;
    let worker = worker(
        &[&at],
        Service::new("prices").on("quote", |cx| async move {
            for piece in ["a", "b", "c"] {
                cx.fragment(piece.as_bytes().to_vec()).await.unwrap();
            }
            cx.reply(format!("{}!", String::from_utf8_lossy(cx.payload())).into_bytes());
            Ok(())
        }),
    );
    let ask = |payload: &'static str| Envelope::claimable("prices", "quote", Bytes::from_static(payload.as_bytes()));
    let mut stream = tokio::time::timeout(WAIT, proxy.exchange_stream(ask("x"), RequestOptions::default())).await.unwrap().unwrap();
    let mut parts = Vec::new();
    while let Some(part) = tokio::time::timeout(WAIT, stream.next()).await.expect("the answer").unwrap() {
        parts.push(match part {
            Part::Fragment(fragment) => format!("fragment {}", String::from_utf8_lossy(&fragment.payload)),
            Part::Result(result) => format!("result {}", String::from_utf8_lossy(&result.payload)),
        });
    }
    assert_eq!(parts, ["fragment a", "fragment b", "fragment c", "result x!"]);
    let response = tokio::time::timeout(WAIT, proxy.exchange(ask("y"))).await.unwrap().unwrap();
    let pieces: Vec<&[u8]> = response.fragments.iter().map(|fragment| &fragment.payload[..]).collect();
    assert_eq!((pieces, &response.result.payload[..], response.fragments_lost), (vec![&b"a"[..], b"b", b"c"], &b"y!"[..], false));
    worker.shutdown().await;
}

/// > 대기자는 `correlation`에 귀속된다. 응답, 취소, 전체 기한 중 먼저 온 것으로 끝난다. 상향 연결이 끊겨도 끝나지 않는다. 다시 붙으면 기다리던 결과가 온다.
///
/// (Plan 4장.) The proxy reaches the server through a relay; the worker
/// reaches it directly. While the request runs, every proxy connection is
/// cut. The worker finishes meanwhile; its result waits on the server as a
/// claimable event on the proxy's reply channel, and `exchange` returns it
/// once the proxy has reconnected — run once.
#[tokio::test]
async fn an_exchange_gets_its_result_across_an_upstream_reconnection() {
    let server = event_server().await;
    let at = server.address.to_string();
    let relay = Relay::start(&at).await;
    let (_address, proxy, _scratch) = proxy_on("reconnect", &[&relay.address]).await;
    let runs = Arc::new(AtomicU64::new(0));
    let gate = Arc::new(Notify::new());
    let (handler_runs, handler_gate) = (runs.clone(), gate.clone());
    let worker = worker(
        &[&at],
        Service::new("prices").on("quote", move |cx| {
            let (runs, gate) = (handler_runs.clone(), handler_gate.clone());
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                gate.notified().await;
                cx.reply(&b"kept"[..]);
                Ok(())
            }
        }),
    );
    let waiting = {
        let proxy = proxy.clone();
        tokio::spawn(async move { proxy.exchange(Envelope::claimable("prices", "quote", Bytes::from_static(b"x"))).await })
    };
    support::eventually("the run started", || runs.load(Ordering::SeqCst) == 1).await;
    relay.cut();
    support::eventually("the proxy saw the cut", || !proxy.is_ready()).await;
    gate.notify_one();
    let response = tokio::time::timeout(WAIT, waiting).await.expect("the result after the reconnection").unwrap().unwrap();
    assert_eq!(&response.result.payload[..], b"kept");
    assert_eq!(runs.load(Ordering::SeqCst), 1, "run once");
    support::eventually("every connection is back", || proxy.is_ready()).await;
    worker.shutdown().await;
}

// ── stopping ─────────────────────────────────────────────────────────────

/// > `run()` | 바인드, 허브 시작, 서비스. Ctrl-C에 끝남
///
/// (`crates/ruvent-proxy/README.md`.) `run` is `run_until(Ctrl-C)`. When the
/// signal comes, `run_until` returns, WebSocket users see their socket
/// close, SSE responses end, and the address stops answering.
#[tokio::test]
async fn a_shutdown_signal_stops_the_server_and_every_user() {
    let server = event_server().await;
    let scratch = Scratch::new("stop");
    let listen: SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let path = scratch.write("proxy.json", &configuration(&listen.to_string(), &[&server.address.to_string()]));
    let (signal, shutdown) = oneshot::channel::<()>();
    let serving = tokio::spawn(Server::new().config_file(&path).run_until(async move {
        let _ = shutdown.await;
        Ok(())
    }));
    healthy(listen, &mut None).await;
    let mut user = socket(listen, "?token=t", None).await;
    assert_eq!(read_json(&mut user).await["ok"], true);
    let mut stream = BufReader::new(TcpStream::connect(listen).await.unwrap());
    stream.get_mut().write_all(b"GET /v1/stream?channel=room HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer t\r\nConnection: close\r\n\r\n").await.unwrap();
    let mut line = String::new();
    tokio::time::timeout(WAIT, stream.read_line(&mut line)).await.unwrap().unwrap();
    assert!(line.starts_with("HTTP/1.1 200"), "{line}");

    let _ = signal.send(());
    tokio::time::timeout(WAIT, serving).await.expect("run_until kept running").unwrap().unwrap();
    let closed = loop {
        match tokio::time::timeout(WAIT, user.next()).await.expect("the socket stayed open") {
            None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break true,
            Some(Ok(_)) => continue,
        }
    };
    assert!(closed);
    let mut rest = Vec::new();
    tokio::time::timeout(WAIT, stream.read_to_end(&mut rest)).await.expect("the SSE response stayed open").unwrap();
    assert!(TcpStream::connect(listen).await.is_err(), "the address no longer answers");
}

/// > `LaunchError`의 Debug=Display라 `main`이 `Err`를 돌려주면 읽을 만한 문장이 찍힌다
///
/// (The server's contract in `src/server/mod.rs`.) A shutdown signal that
/// cannot be listened for is an error the program reports, not a silent
/// stop that looks like success.
#[tokio::test]
async fn a_shutdown_signal_that_cannot_be_listened_for_is_an_error() {
    let server = event_server().await;
    let scratch = Scratch::new("signal");
    let path = scratch.write("proxy.json", &configuration("127.0.0.1:0", &[&server.address.to_string()]));
    let result = Server::new().config_file(&path).run_until(async { Err(io::Error::other("no signal handler")) }).await;
    let error = result.expect_err("a failed signal is not success");
    assert!(matches!(error, LaunchError::Signal(_)));
    assert_eq!(format!("{error:?}"), "cannot listen for the shutdown signal: no signal handler");
}
