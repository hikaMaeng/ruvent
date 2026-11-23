//! The WebSocket protocol over real TCP, upstream in-process.

use futures_util::{SinkExt, StreamExt};
use ruvent::{Bytes, Engine, EngineConfig, Envelope, Methods};
use ruvent_proxy::config::{AuthMode, StaticToken};
use ruvent_proxy::{InProcess, Proxy, ProxyConfig, Server};
use serde_json::Value;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;

const WAIT: Duration = Duration::from_secs(5);

type Socket = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;

struct Running {
    engine: Engine,
    address: SocketAddr,
    proxy: Proxy,
}

async fn start(config: ProxyConfig) -> Running {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let bound = Server::new().config(config).upstream(InProcess { engine: engine.clone(), credential: "proxy".into() }).bind().await.unwrap();
    assert!(bound.proxy().ready().await);
    let address = bound.local_addr().unwrap();
    let proxy = bound.proxy().clone();
    tokio::spawn(bound.serve());
    Running { engine, address, proxy }
}

/// Static tokens: `t` is user `u` with every channel.
fn static_config() -> ProxyConfig {
    let mut config = ProxyConfig::open("127.0.0.1:0", "in-process");
    config.auth.mode = AuthMode::Static;
    config.auth.tokens = vec![StaticToken { token: "t".into(), user: "u".into(), channels: vec![] }];
    config
}

async fn connect(address: SocketAddr, query: &str) -> Socket {
    tokio_tungstenite::connect_async(format!("ws://{address}/v1/ws{query}")).await.unwrap().0
}

async fn read_json(socket: &mut Socket) -> Value {
    loop {
        let message = tokio::time::timeout(WAIT, socket.next()).await.expect("timed out").expect("closed").unwrap();
        if let Message::Text(text) = message {
            return serde_json::from_str(text.as_str()).unwrap();
        }
    }
}

async fn send(socket: &mut Socket, text: &str) {
    socket.send(Message::Text(text.into())).await.unwrap();
}

/// After a failed result, the socket ends: a close frame or the end of the stream.
async fn closes(socket: &mut Socket) -> bool {
    loop {
        match tokio::time::timeout(WAIT, socket.next()).await.expect("the socket stayed open") {
            None | Some(Err(_)) | Some(Ok(Message::Close(_))) => return true,
            Some(Ok(Message::Text(text))) => panic!("unexpected message {text}"),
            Some(Ok(_)) => continue,
        }
    }
}

async fn get(address: SocketAddr, path: &str) -> String {
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream.write_all(format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    response
}

// ── a working session ────────────────────────────────────────────────────

/// A WebSocket user authenticates by first message, subscribes, receives an event and publishes one.
#[tokio::test]
async fn a_websocket_user_subscribes_receives_and_publishes() {
    let running = start(static_config()).await;
    let mut socket = connect(running.address, "").await;
    send(&mut socket, r#"{"op":"auth","token":"t"}"#).await;
    assert_eq!(read_json(&mut socket).await["ok"], true);
    send(&mut socket, r#"{"op":"subscribe","channel":"room","ref":1}"#).await;
    let subscribed = read_json(&mut socket).await;
    assert_eq!((subscribed["ref"].clone(), subscribed["ok"].clone()), (Value::from(1), Value::from(true)));
    let (peer, _inbox) = ruvent::connect(&running.engine, "peer", 64).await.unwrap();
    peer.register_dispatch(Envelope::dispatch("room", "said", Bytes::from_static(br#"{"text":"hi"}"#))).await.unwrap();
    let event = read_json(&mut socket).await;
    assert_eq!(event["op"], "event");
    assert_eq!(event["payload"]["text"], "hi");
    send(&mut socket, r#"{"op":"publish","kind":"dispatch","channel":"room","type":"said","payload":2,"ref":"p"}"#).await;
    let (first, second) = (read_json(&mut socket).await, read_json(&mut socket).await);
    let result = if first["op"] == "result" { first } else { second };
    assert_eq!(result["ref"], "p");
    assert_eq!(result["ok"], true);
}

/// A message that is not a request is answered as a bad request; the session goes on.
#[tokio::test]
async fn a_malformed_message_is_answered_and_the_session_goes_on() {
    let running = start(static_config()).await;
    let mut socket = connect(running.address, "?token=t").await;
    assert_eq!(read_json(&mut socket).await["ok"], true);
    send(&mut socket, "not json").await;
    let answer = read_json(&mut socket).await;
    assert_eq!(answer["ok"], false);
    assert!(answer["error"].as_str().unwrap().starts_with("bad request: "));
    send(&mut socket, r#"{"op":"auth","token":"t"}"#).await;
    assert_eq!(read_json(&mut socket).await["error"], "already authenticated");
    send(&mut socket, r#"{"op":"subscribe","channel":"room","ref":2}"#).await;
    assert_eq!(read_json(&mut socket).await["ok"], true);
}

// ── tokens ───────────────────────────────────────────────────────────────

/// A wrong token is answered with a failed result and the socket is closed.
#[tokio::test]
async fn a_websocket_with_a_wrong_token_is_turned_away() {
    let running = start(static_config()).await;
    let mut socket = connect(running.address, "?token=wrong").await;
    let answer = read_json(&mut socket).await;
    assert_eq!((answer["ok"].clone(), answer["error"].clone()), (Value::from(false), Value::from("authentication rejected")));
    assert!(closes(&mut socket).await);
}

/// `?token=` with nothing after it is a token, the empty string, and is rejected — not a request to authenticate in band.
#[tokio::test]
async fn an_empty_query_token_is_a_token_and_is_rejected() {
    let running = start(static_config()).await;
    let mut socket = connect(running.address, "?token=").await;
    assert_eq!(read_json(&mut socket).await["error"], "authentication rejected");
    assert!(closes(&mut socket).await);
}

/// A repeated `token` key: the first value is the token.
#[tokio::test]
async fn the_first_of_two_query_tokens_is_the_token() {
    let running = start(static_config()).await;
    let mut socket = connect(running.address, "?token=t&token=wrong").await;
    assert_eq!(read_json(&mut socket).await["ok"], true);
}

/// With no token, a first message that is not `auth` ends the socket with an explanation.
#[tokio::test]
async fn a_first_message_that_is_not_auth_ends_the_socket() {
    let running = start(static_config()).await;
    let mut socket = connect(running.address, "").await;
    send(&mut socket, r#"{"op":"subscribe","channel":"room"}"#).await;
    let answer = read_json(&mut socket).await;
    assert!(answer["error"].as_str().unwrap().starts_with("authenticate first"));
    assert!(closes(&mut socket).await);
}

/// A wrong in-band token is rejected once; the proxy does not ask again.
#[tokio::test]
async fn a_wrong_in_band_token_is_rejected_without_asking_again() {
    let running = start(static_config()).await;
    let mut socket = connect(running.address, "").await;
    send(&mut socket, r#"{"op":"auth","token":"wrong"}"#).await;
    assert_eq!(read_json(&mut socket).await["error"], "authentication rejected");
    assert!(closes(&mut socket).await);
}

/// In `none` mode no token is needed and none is asked for.
#[tokio::test]
async fn mode_none_needs_no_token() {
    let running = start(ProxyConfig::open("127.0.0.1:0", "in-process")).await;
    let mut socket = connect(running.address, "").await;
    assert_eq!(read_json(&mut socket).await["ok"], true);
}

// ── health and stopping ──────────────────────────────────────────────────

/// `/health` answers 200 once the upstream is connected.
#[tokio::test]
async fn health_is_ok_while_the_upstream_is_up() {
    let running = start(ProxyConfig::open("127.0.0.1:0", "in-process")).await;
    let response = get(running.address, "/health").await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
}

/// Once the hub stopped, `/health` answers 503 and connected sockets are closed.
#[tokio::test]
async fn once_the_hub_stopped_health_is_503_and_sockets_close() {
    let running = start(static_config()).await;
    let mut socket = connect(running.address, "?token=t").await;
    assert_eq!(read_json(&mut socket).await["ok"], true);
    running.proxy.stop().await;
    assert!(closes(&mut socket).await);
    let response = get(running.address, "/health").await;
    assert!(response.starts_with("HTTP/1.1 503"), "{response}");
}
