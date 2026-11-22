//! The HTTP binary's router over real TCP, upstream in-process.

use ruvent::{Engine, EngineConfig};
use ruvent_proxy::config::{AuthMode, StaticToken};
use ruvent_proxy::{InProcess, Proxy, ProxyConfig};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

async fn start() -> std::net::SocketAddr {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let mut config = ProxyConfig::open("127.0.0.1:0", "in-process");
    config.auth.mode = AuthMode::Static;
    config.auth.tokens = vec![StaticToken { token: "t".into(), user: "u".into(), channels: vec!["room*".into()] }];
    let proxy = Proxy::start(config, Arc::new(InProcess { engine, credential: "proxy".into() }));
    proxy.ready().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(ruvent_proxy_http::serve(listener, proxy));
    address
}

async fn post(address: std::net::SocketAddr, token: &str, body: &str) -> (u16, Value) {
    let mut stream = TcpStream::connect(address).await.unwrap();
    let request =
        format!("POST /v1/events HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    let status = response[9..12].parse().unwrap();
    let body = response.split("\r\n\r\n").nth(1).unwrap_or_default();
    (status, serde_json::from_str(body).unwrap_or(Value::Null))
}

/// An SSE subscriber receives what a POST publishes on its channel.
#[tokio::test]
async fn a_post_reaches_an_sse_subscriber() {
    let address = start().await;
    let mut stream = BufReader::new(TcpStream::connect(address).await.unwrap());
    stream.get_mut().write_all(b"GET /v1/stream?channel=room HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer t\r\n\r\n").await.unwrap();
    let mut line = String::new();
    // The headers arrive only after the subscription took effect.
    loop {
        line.clear();
        tokio::time::timeout(Duration::from_secs(5), stream.read_line(&mut line)).await.unwrap().unwrap();
        if line == "\r\n" {
            break;
        }
    }
    let (status, result) = post(address, "t", r#"{"kind":"dispatch","channel":"room","type":"said","payload":{"n":7}}"#).await;
    assert_eq!((status, result["ok"].clone()), (200, Value::from(true)));
    let event = loop {
        line.clear();
        tokio::time::timeout(Duration::from_secs(5), stream.read_line(&mut line)).await.unwrap().unwrap();
        if let Some(data) = line.trim_end().strip_prefix("data: ") {
            break serde_json::from_str::<Value>(data).unwrap();
        }
    };
    assert_eq!(event["payload"]["n"], 7);
    assert_eq!(event["id"], result["id"]);
}

/// POST answers 401 for a bad token and 403 for a channel outside the user's.
#[tokio::test]
async fn a_post_is_refused_for_a_bad_token_or_a_foreign_channel() {
    let address = start().await;
    let body = r#"{"kind":"dispatch","channel":"room","type":"t","payload":1}"#;
    assert_eq!(post(address, "wrong", body).await.0, 401);
    let foreign = r#"{"kind":"dispatch","channel":"admin","type":"t","payload":1}"#;
    let (status, result) = post(address, "t", foreign).await;
    assert_eq!(status, 403);
    assert_eq!(result["error"], "forbidden channel");
}
