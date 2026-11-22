//! The WebSocket binary's router over real TCP, upstream in-process.

use futures_util::{SinkExt, StreamExt};
use ruvent::{Bytes, Engine, EngineConfig, Envelope, Methods};
use ruvent_proxy::config::{AuthMode, StaticToken};
use ruvent_proxy::{InProcess, Proxy, ProxyConfig};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::tungstenite::Message;

async fn start(config: ProxyConfig) -> (Engine, std::net::SocketAddr) {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let proxy = Proxy::start(config, Arc::new(InProcess { engine: engine.clone(), credential: "proxy".into() }));
    proxy.ready().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(ruvent_proxy_ws::serve(listener, proxy));
    (engine, address)
}

async fn read_json<S>(socket: &mut S) -> Value
where
    S: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    loop {
        let message = tokio::time::timeout(Duration::from_secs(5), socket.next()).await.expect("timed out").expect("closed").unwrap();
        if let Message::Text(text) = message {
            return serde_json::from_str(text.as_str()).unwrap();
        }
    }
}

/// A WebSocket user authenticates by first message, subscribes, receives an event and publishes one.
#[tokio::test]
async fn a_websocket_user_subscribes_receives_and_publishes() {
    let mut config = ProxyConfig::open("127.0.0.1:0", "in-process");
    config.auth.mode = AuthMode::Static;
    config.auth.tokens = vec![StaticToken { token: "t".into(), user: "u".into(), channels: vec![] }];
    let (engine, address) = start(config).await;
    let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}/v1/ws")).await.unwrap();
    socket.send(Message::Text(r#"{"op":"auth","token":"t"}"#.into())).await.unwrap();
    assert_eq!(read_json(&mut socket).await["ok"], true);
    socket.send(Message::Text(r#"{"op":"subscribe","channel":"room","ref":1}"#.into())).await.unwrap();
    let subscribed = read_json(&mut socket).await;
    assert_eq!((subscribed["ref"].clone(), subscribed["ok"].clone()), (Value::from(1), Value::from(true)));
    let (peer, _inbox) = ruvent::connect(&engine, "peer", 64).await.unwrap();
    peer.register_dispatch(Envelope::dispatch("room", "said", Bytes::from_static(br#"{"text":"hi"}"#))).await.unwrap();
    let event = read_json(&mut socket).await;
    assert_eq!(event["op"], "event");
    assert_eq!(event["payload"]["text"], "hi");
    socket.send(Message::Text(r#"{"op":"publish","kind":"dispatch","channel":"room","type":"said","payload":2,"ref":"p"}"#.into())).await.unwrap();
    let (first, second) = (read_json(&mut socket).await, read_json(&mut socket).await);
    let result = if first["op"] == "result" { first } else { second };
    assert_eq!(result["ref"], "p");
    assert_eq!(result["ok"], true);
}

/// A wrong token is answered with a failed result and the socket is closed.
#[tokio::test]
async fn a_websocket_with_a_wrong_token_is_turned_away() {
    let mut config = ProxyConfig::open("127.0.0.1:0", "in-process");
    config.auth.mode = AuthMode::Static;
    let (_engine, address) = start(config).await;
    let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}/v1/ws?token=wrong")).await.unwrap();
    let answer = read_json(&mut socket).await;
    assert_eq!(answer["ok"], false);
    assert_eq!(answer["error"], "authentication rejected");
}

/// `/health` answers 200 once the upstream is connected.
#[tokio::test]
async fn health_is_ok_while_the_upstream_is_up() {
    let (_engine, address) = start(ProxyConfig::open("127.0.0.1:0", "in-process")).await;
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream.write_all(b"GET /health HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
}
