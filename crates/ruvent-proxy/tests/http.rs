//! The HTTP protocol over real TCP, upstream in-process.

mod support;

use ruvent::{Engine, Hello, Methods, Target};
use ruvent_proxy::config::{AuthMode, StaticToken};
use ruvent_proxy::{Proxy, ProxyConfig, Server};
use serde_json::Value;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

const WAIT: Duration = Duration::from_secs(5);
const ID: &str = "0190f5b2-6f1e-7c4a-9d2b-3a4b5c6d7e8f";

/// Static token `t` is user `u` on `room*`.
async fn start() -> (SocketAddr, Proxy) {
    let (address, proxy, _engine) = start_with_engine().await;
    (address, proxy)
}

async fn start_with_engine() -> (SocketAddr, Proxy, Engine) {
    let (engine, upstream) = support::one().await;
    let mut config = ProxyConfig::open("127.0.0.1:0", "s0");
    config.auth.mode = AuthMode::Static;
    config.auth.tokens = vec![StaticToken { token: "t".into(), user: "u".into(), channels: vec!["room*".into()] }];
    let bound = Server::new().config(config).upstream(upstream).bind().await.unwrap();
    assert!(bound.proxy().ready().await);
    let address = bound.local_addr().unwrap();
    let proxy = bound.proxy().clone();
    tokio::spawn(bound.serve());
    (address, proxy, engine)
}

/// One request with `Connection: close`; returns the status and the JSON body.
async fn request(address: SocketAddr, method: &str, path: &str, headers: &str, body: &str) -> (u16, Value) {
    let mut stream = TcpStream::connect(address).await.unwrap();
    let request = format!("{method} {path} HTTP/1.1\r\nHost: x\r\n{headers}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    tokio::time::timeout(WAIT, stream.read_to_string(&mut response)).await.expect("no response").unwrap();
    let status = response[9..12].parse().unwrap();
    let body = response.split("\r\n\r\n").nth(1).unwrap_or_default();
    (status, serde_json::from_str(body).unwrap_or(Value::Null))
}

async fn post(address: SocketAddr, token: &str, body: &str) -> (u16, Value) {
    request(address, "POST", "/v1/events", &format!("Authorization: Bearer {token}\r\n"), body).await
}

/// Opens an SSE stream and reads past its headers; returns the reader and the status.
async fn open_stream(address: SocketAddr, query: &str) -> (BufReader<TcpStream>, u16) {
    let mut stream = BufReader::new(TcpStream::connect(address).await.unwrap());
    stream.get_mut().write_all(format!("GET /v1/stream{query} HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer t\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
    let mut line = String::new();
    tokio::time::timeout(WAIT, stream.read_line(&mut line)).await.unwrap().unwrap();
    let status = line[9..12].parse().unwrap();
    // The headers arrive only after every subscription took effect.
    loop {
        line.clear();
        tokio::time::timeout(WAIT, stream.read_line(&mut line)).await.unwrap().unwrap();
        if line == "\r\n" {
            break;
        }
    }
    (stream, status)
}

async fn next_event(stream: &mut BufReader<TcpStream>) -> Value {
    let mut line = String::new();
    loop {
        line.clear();
        tokio::time::timeout(WAIT, stream.read_line(&mut line)).await.unwrap().unwrap();
        if let Some(data) = line.trim_end().strip_prefix("data: ") {
            return serde_json::from_str(data).unwrap();
        }
    }
}

const ROOM: &str = r#"{"kind":"dispatch","channel":"room","type":"t","payload":1}"#;

// ── publish ──────────────────────────────────────────────────────────────

/// An SSE subscriber receives what a POST publishes on its channel.
#[tokio::test]
async fn a_post_reaches_an_sse_subscriber() {
    let (address, _proxy) = start().await;
    let (mut stream, status) = open_stream(address, "?channel=room").await;
    assert_eq!(status, 200);
    let (status, result) = post(address, "t", r#"{"kind":"dispatch","channel":"room","type":"said","payload":{"n":7}}"#).await;
    assert_eq!((status, result["ok"].clone()), (200, Value::from(true)));
    let event = next_event(&mut stream).await;
    assert_eq!(event["payload"]["n"], 7);
    assert_eq!(event["id"], result["id"]);
}

/// POST answers 401 for a bad token and 403 for a channel outside the user's.
#[tokio::test]
async fn a_post_is_refused_for_a_bad_token_or_a_foreign_channel() {
    let (address, _proxy) = start().await;
    let (status, result) = post(address, "wrong", ROOM).await;
    assert_eq!((status, result["error"].clone()), (401, Value::from("authentication rejected")));
    let (status, result) = post(address, "t", r#"{"kind":"dispatch","channel":"admin","type":"t","payload":1}"#).await;
    assert_eq!((status, result["error"].clone()), (403, Value::from("forbidden channel")));
}

/// No `Authorization` header at all is 401 "authentication required".
#[tokio::test]
async fn a_missing_bearer_token_is_401_authentication_required() {
    let (address, _proxy) = start().await;
    let (status, result) = request(address, "POST", "/v1/events", "", ROOM).await;
    assert_eq!((status, result["error"].clone()), (401, Value::from("authentication required")));
}

/// A body that is not a publish is 400 with the parse error.
#[tokio::test]
async fn a_body_that_is_not_a_publish_is_400() {
    let (address, _proxy) = start().await;
    let (status, result) = post(address, "t", r#"{"kind":"dispatch"}"#).await;
    assert_eq!(status, 400);
    assert!(result["error"].as_str().unwrap().starts_with("bad request: "));
}

/// A malformed id is 400 even when the reply channel is also forbidden: the request's shape is checked first.
#[tokio::test]
async fn a_malformed_id_is_400_even_when_the_reply_channel_is_forbidden() {
    let (address, _proxy) = start().await;
    let (status, result) = post(address, "t", r#"{"kind":"dispatch","channel":"room","type":"t","payload":1,"id":"nope","reply":"admin"}"#).await;
    assert_eq!((status, result["error"].clone()), (400, Value::from("id is not a UUID")));
    let (status, result) = post(address, "t", &format!(r#"{{"kind":"dispatch","channel":"room","type":"t","payload":1,"id":"{ID}","reply":"admin"}}"#)).await;
    assert_eq!((status, result["error"].clone()), (403, Value::from("forbidden reply channel")));
}

/// 2.3: re-posting a claimable event whose id is still waiting, with the same content, is accepted
/// (a resend) and registers nothing twice — no per-registration answer, no 409.
#[tokio::test]
async fn a_repeated_waiting_id_is_accepted_and_registered_once() {
    let (address, _proxy, engine) = start_with_engine().await;
    let body = format!(r#"{{"kind":"claimable","channel":"room.jobs","type":"run","payload":1,"id":"{ID}"}}"#);
    let (status, first) = post(address, "t", &body).await;
    assert_eq!((status, first["id"].clone()), (200, Value::from(ID)));
    let (status, second) = post(address, "t", &body).await;
    assert_eq!((status, second["id"].clone()), (200, Value::from(ID)));
    let (observer, _inbox) = ruvent::connect(&engine, Hello::observer("operator"), 16).await.unwrap();
    let backlog = observer.subscribe(Target::Channel("room.jobs".into()), None).await.unwrap();
    assert_eq!(backlog.notices.len(), 1, "one event waits under that id");
}

// ── subscribe ────────────────────────────────────────────────────────────

/// A stream that names no channel, or only empty ones, is 400.
#[tokio::test]
async fn a_stream_naming_no_channel_is_400() {
    let (address, _proxy) = start().await;
    for query in ["", "?channel=", "?prefix=", "?other=room"] {
        let (status, result) = request(address, "GET", &format!("/v1/stream{query}"), "Authorization: Bearer t\r\n", "").await;
        assert_eq!(status, 400, "{query}");
        assert_eq!(result["error"], "name at least one channel: ?channel=… or ?prefix=…");
    }
}

/// `?prefix=` subscribes to every channel below it; a prefix outside the user's scope is 403, shown as `prefix.*`.
#[tokio::test]
async fn an_sse_stream_takes_a_prefix() {
    let (address, _proxy) = start().await;
    let (mut stream, status) = open_stream(address, "?prefix=room").await;
    assert_eq!(status, 200);
    post(address, "t", r#"{"kind":"dispatch","channel":"room","type":"t","payload":"not below"}"#).await;
    post(address, "t", r#"{"kind":"dispatch","channel":"room.a.b","type":"t","payload":"below"}"#).await;
    assert_eq!(next_event(&mut stream).await["payload"], "below");
    let (status, result) = request(address, "GET", "/v1/stream?prefix=admin", "Authorization: Bearer t\r\n", "").await;
    assert_eq!((status, result["error"].clone()), (403, Value::from("admin.*: forbidden channel")));
}

/// A stream asking for a channel outside the user's scope is 403, naming the channel.
#[tokio::test]
async fn an_sse_subscription_outside_the_scope_is_403() {
    let (address, _proxy) = start().await;
    let (status, result) = request(address, "GET", "/v1/stream?channel=room&channel=admin", "Authorization: Bearer t\r\n", "").await;
    assert_eq!((status, result["error"].clone()), (403, Value::from("admin: forbidden channel")));
}

/// One stream may carry several channels.
#[tokio::test]
async fn one_stream_carries_several_channels() {
    let (address, _proxy) = start().await;
    let (mut stream, _) = open_stream(address, "?channel=room.a&channel=room.b").await;
    post(address, "t", r#"{"kind":"dispatch","channel":"room.a","type":"t","payload":"a"}"#).await;
    post(address, "t", r#"{"kind":"dispatch","channel":"room.b","type":"t","payload":"b"}"#).await;
    let mut got = vec![next_event(&mut stream).await["payload"].as_str().unwrap().to_owned(), next_event(&mut stream).await["payload"].as_str().unwrap().to_owned()];
    got.sort();
    assert_eq!(got, vec!["a", "b"]);
}

/// When the hub stops the SSE response ends and the connection closes.
#[tokio::test]
async fn an_sse_stream_ends_when_the_hub_stops() {
    let (address, proxy) = start().await;
    let (mut stream, _) = open_stream(address, "?channel=room").await;
    proxy.stop().await;
    let mut rest = Vec::new();
    let read = tokio::time::timeout(WAIT, stream.read_to_end(&mut rest)).await.expect("the stream stayed open");
    assert!(read.is_ok());
    assert!(String::from_utf8_lossy(&rest).ends_with("0\r\n\r\n"), "the chunked body was terminated");
}
