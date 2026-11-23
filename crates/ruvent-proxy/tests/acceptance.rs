//! The proxy as its users build and run it, against a real event server
//! over TCP: no in-process shortcut, the upstream is [`ruvent_proxy::Remote`].
//!
//! Each test quotes the requirement it judges. The user's words are from
//! the decision recorded in `docs/proposals/event-server-redesign.md` §13.

use futures_util::{SinkExt, StreamExt};
use ruvent::{Bytes, Engine, EngineConfig, Envelope, Incoming, Methods};
use ruvent_proxy::{async_trait, Context, Denied, Handshake, Hooks, LaunchError, Server, User};
use ruvent_server::{serve, Credentials, ServerOptions};
use serde_json::Value;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

const WAIT: Duration = Duration::from_secs(10);

type Socket = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;

/// A real event server on a TCP port, and an in-process connection to its engine for the test's own traffic.
struct EventServer {
    engine: Engine,
    address: SocketAddr,
}

async fn event_server() -> EventServer {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let credentials = Arc::new(Credentials::new([("proxy".to_owned(), "proxy-token".to_owned())]));
    tokio::spawn(serve(listener, engine.clone(), credentials, ServerOptions::default()));
    EventServer { engine, address }
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

/// A proxy configuration file: static token `t` for user `u` on `room*`, upstream the event server.
fn configuration(listen: &str, upstream: SocketAddr) -> String {
    format!(
        r#"{{"listen":"{listen}","upstream":{{"address":"{upstream}","token":"proxy-token"}},
            "auth":{{"mode":"static","tokens":[{{"token":"t","user":"u","channels":["room*"]}}]}}}}"#
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

async fn subscribed(socket: &mut Socket, channel: &str) {
    send(socket, &format!(r#"{{"op":"subscribe","channel":"{channel}","ref":"s"}}"#)).await;
    let answer = read_json(socket).await;
    assert_eq!((answer["ref"].clone(), answer["ok"].clone()), (Value::from("s"), Value::from(true)), "{answer}");
}

/// Waits for `/health` to answer 200: the proxy is up and its upstream connected.
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

/// Registers through the event server's own engine (not through the proxy).
async fn dispatch(engine: &Engine, channel: &str, payload: &'static str) {
    let (peer, _inbox) = ruvent::connect(engine, "peer", 64).await.unwrap();
    peer.register_dispatch(Envelope::dispatch(channel, "said", Bytes::from_static(payload.as_bytes()))).await.unwrap();
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
    let path = scratch.write("proxy.json", &configuration(&listen.to_string(), server.address));
    let mut running = run(&example("plain"), &path);
    healthy(listen, &mut Some(&mut running)).await;

    let mut user = socket(listen, "?token=t", None).await;
    assert_eq!(read_json(&mut user).await["ok"], true);
    subscribed(&mut user, "room").await;
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
    let scratch = Scratch::new("routes");
    let path = scratch.write("proxy.json", &configuration("127.0.0.1:0", server.address));
    let bound = Server::new().config_file(&path).bind().await.unwrap();
    let address = bound.local_addr().unwrap();
    tokio::spawn(bound.serve());
    healthy(address, &mut None).await;
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
    let path = scratch.write("proxy.json", &configuration("127.0.0.1:0", server.address));
    let bound = Server::new().config_file(&path).hooks(Sessions).bind().await.unwrap();
    let address = bound.local_addr().unwrap();
    tokio::spawn(bound.serve());
    healthy(address, &mut None).await;
    let (watcher, mut seen) = ruvent::connect(&server.engine, "watcher", 64).await.unwrap();
    watcher.subscribe("room".into(), None).await.unwrap();

    let mut alice = socket(address, "", Some("session=s-alice")).await;
    assert_eq!(read_json(&mut alice).await["ok"], true);
    subscribed(&mut alice, "room").await;
    let mut service = socket(address, "?token=t", None).await;
    assert_eq!(read_json(&mut service).await["ok"], true, "the configured token still works");
    let mut stranger = socket(address, "", Some("session=forged")).await;
    assert_eq!(read_json(&mut stranger).await["error"], "authentication rejected");

    let (status, _) = http(address, "POST", "/v1/events", "Cookie: session=s-alice\r\n", r#"{"kind":"dispatch","channel":"room","type":"said","payload":"hi"}"#).await;
    assert_eq!(status, 200);
    assert_eq!(read_json(&mut alice).await["payload"], "hi");
    let registered = loop {
        match tokio::time::timeout(WAIT, seen.recv()).await.expect("timed out").expect("closed") {
            Incoming::Delivery(event) => break event,
            _ => continue,
        }
    };
    assert_eq!((registered.delegate.as_deref(), registered.principal.as_deref()), (Some("alice"), Some("proxy")));
}

// ── the subscription rule over a real connection ─────────────────────────

/// > a user never receives an event sent before its subscription took effect — in particular after `0 → 1 → 0 → 1`, nothing of the first subscription leaks into the second.
///
/// (`crates/ruvent-proxy/src/hub/mod.rs`, unchanged by the move to hooks.)
/// Over TCP: a late joiner sees only later events, and a user who leaves and
/// comes back sees nothing sent in between.
#[tokio::test]
async fn the_subscription_rule_holds_through_a_real_event_server() {
    let server = event_server().await;
    let scratch = Scratch::new("rule");
    let path = scratch.write("proxy.json", &configuration("127.0.0.1:0", server.address));
    let bound = Server::new().config_file(&path).bind().await.unwrap();
    let address = bound.local_addr().unwrap();
    tokio::spawn(bound.serve());
    healthy(address, &mut None).await;

    let mut early = socket(address, "?token=t", None).await;
    assert_eq!(read_json(&mut early).await["ok"], true);
    subscribed(&mut early, "room").await;
    dispatch(&server.engine, "room", "1").await;
    assert_eq!(read_json(&mut early).await["payload"], 1);
    let mut late = socket(address, "?token=t", None).await;
    assert_eq!(read_json(&mut late).await["ok"], true);
    subscribed(&mut late, "room").await;
    dispatch(&server.engine, "room", "2").await;
    assert_eq!(read_json(&mut early).await["payload"], 2);
    assert_eq!(read_json(&mut late).await["payload"], 2, "event 1 was sent before the late join took effect");

    send(&mut early, r#"{"op":"unsubscribe","channel":"room","ref":"u"}"#).await;
    assert_eq!(read_json(&mut early).await["ref"], "u");
    send(&mut late, r#"{"op":"unsubscribe","channel":"room","ref":"u"}"#).await;
    assert_eq!(read_json(&mut late).await["ref"], "u");
    dispatch(&server.engine, "room", "3").await;
    subscribed(&mut early, "room").await;
    dispatch(&server.engine, "room", "4").await;
    assert_eq!(read_json(&mut early).await["payload"], 4, "nothing from the gap");
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
    let path = scratch.write("proxy.json", &configuration(&listen.to_string(), server.address));
    let (signal, shutdown) = tokio::sync::oneshot::channel::<()>();
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
    let path = scratch.write("proxy.json", &configuration("127.0.0.1:0", server.address));
    let result = Server::new().config_file(&path).run_until(async { Err(io::Error::other("no signal handler")) }).await;
    let error = result.expect_err("a failed signal is not success");
    assert!(matches!(error, LaunchError::Signal(_)));
    assert_eq!(format!("{error:?}"), "cannot listen for the shutdown signal: no signal handler");
}
