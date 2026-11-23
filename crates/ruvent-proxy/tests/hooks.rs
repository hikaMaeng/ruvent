//! The proxy built the way a user builds it: `Server`, a configuration, and
//! hooks. Real TCP, both protocols on one listener, upstream in-process.

use futures_util::{SinkExt, StreamExt};
use ruvent::{Bytes, Engine, EngineConfig, Envelope, Incoming, Methods};
use ruvent_proxy::{async_trait, Context, Denied, Handshake, Hooks, InProcess, Publish, Rejection, Server, User};
use serde_json::Value;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

const WAIT: Duration = Duration::from_secs(5);

type Socket = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;

async fn serve(server: Server) -> SocketAddr {
    let bound = server.bind().await.expect("binds");
    bound.proxy().ready().await;
    let address = bound.local_addr().unwrap();
    tokio::spawn(bound.serve());
    address
}

fn listen_anywhere() -> ruvent_proxy::ProxyConfig {
    ruvent_proxy::ProxyConfig::open("127.0.0.1:0", "in-process")
}

fn in_process(engine: &Engine) -> InProcess {
    InProcess { engine: engine.clone(), credential: "proxy".into() }
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

/// A raw HTTP/1.1 exchange; returns the status and the JSON body.
async fn http(address: SocketAddr, method: &str, path: &str, header: &str, body: &str) -> (u16, Value) {
    let mut stream = TcpStream::connect(address).await.unwrap();
    let request = format!("{method} {path} HTTP/1.1\r\nHost: x\r\n{header}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    let status = response[9..12].parse().unwrap();
    let body = response.split("\r\n\r\n").nth(1).unwrap_or_default();
    (status, serde_json::from_str(body).unwrap_or(Value::Null))
}

/// A configuration file and no code of one's own: the server runs, and both
/// compiled-in protocols answer on the one `listen` address.
#[tokio::test]
async fn a_configuration_file_and_nothing_else_is_a_running_proxy_for_both_protocols() {
    let directory = std::env::temp_dir().join(format!("ruvent-proxy-hooks-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join("proxy.json");
    let text = r#"{"listen":"127.0.0.1:0","upstream":{"address":"unused","token":"unused"},
        "auth":{"mode":"static","tokens":[{"token":"t","user":"u","channels":["room*"]}]}}"#;
    std::fs::write(&path, text).unwrap();
    let engine = Engine::volatile(EngineConfig::default()).await;
    let address = serve(Server::new().config_file(&path).upstream(in_process(&engine))).await;

    assert_eq!(http(address, "GET", "/health", "X: y", "").await.0, 200);
    let mut ws = socket(address, "?token=t", None).await;
    assert_eq!(read_json(&mut ws).await["ok"], true);
    send(&mut ws, r#"{"op":"subscribe","channel":"room","ref":1}"#).await;
    assert_eq!(read_json(&mut ws).await["ok"], true);
    let (status, result) = http(address, "POST", "/v1/events", "Authorization: Bearer t", r#"{"kind":"dispatch","channel":"room","type":"said","payload":{"n":1}}"#).await;
    assert_eq!((status, result["ok"].clone()), (200, Value::from(true)));
    let event = read_json(&mut ws).await;
    assert_eq!((event["op"].clone(), event["payload"]["n"].clone()), (Value::from("event"), Value::from(1)));
    let _ = std::fs::remove_dir_all(&directory);
}

/// What a domain knows about a user.
struct Tenant(&'static str);

/// Sessions in a cookie, tenants on the user, channels named after the tenant.
struct Domain;

#[async_trait]
impl Hooks for Domain {
    async fn authenticate(&self, _cx: Context<'_>, handshake: &Handshake) -> Result<User, Denied> {
        match handshake.cookie("session") {
            Some("s-alice") => Ok(User::new("alice", ["*"]).with_data(Tenant("acme"))),
            Some("s-bob") => Ok(User::new("bob", ["*"]).with_data(Tenant("acme"))),
            Some(_) => Err(Denied::Rejected),
            None => Err(Denied::Missing),
        }
    }

    async fn subscribe(&self, _cx: Context<'_>, user: &User, channel: &str) -> Result<(), Rejection> {
        let tenant = user.data::<Tenant>().map(|tenant| tenant.0).unwrap_or_default();
        if channel.starts_with(&format!("tenant.{tenant}.")) {
            Ok(())
        } else {
            Err(Rejection::forbidden("other tenant"))
        }
    }

    async fn publish(&self, _cx: Context<'_>, user: &User, publish: &mut Publish) -> Result<(), Rejection> {
        if publish.event_type.starts_with("admin.") {
            return Err(Rejection::forbidden("operators only"));
        }
        let tenant = user.data::<Tenant>().map(|tenant| tenant.0).unwrap_or_default();
        publish.channel = format!("tenant.{tenant}.{}", publish.channel);
        Ok(())
    }

    fn deliver(&self, user: &User, event: &Envelope) -> bool {
        let payload: Value = serde_json::from_slice(&event.payload).unwrap_or(Value::Null);
        payload.get("to").and_then(Value::as_str).is_none_or(|to| to == user.name)
    }
}

async fn domain() -> (Engine, SocketAddr) {
    let engine = Engine::volatile(EngineConfig::default()).await;
    let address = serve(Server::new().config(listen_anywhere()).hooks(Domain).upstream(in_process(&engine))).await;
    (engine, address)
}

/// An authenticate hook reads a cookie and attaches the domain's own value;
/// a subscribe hook decides from that value. Unknown sessions are turned away.
#[tokio::test]
async fn custom_hooks_decide_who_comes_in_and_carry_the_domains_data() {
    let (_engine, address) = domain().await;
    let mut alice = socket(address, "", Some("session=s-alice")).await;
    assert_eq!(read_json(&mut alice).await["ok"], true);
    send(&mut alice, r#"{"op":"subscribe","channel":"tenant.acme.room","ref":1}"#).await;
    assert_eq!(read_json(&mut alice).await["ok"], true);
    send(&mut alice, r#"{"op":"subscribe","channel":"tenant.other.room","ref":2}"#).await;
    let refused = read_json(&mut alice).await;
    assert_eq!((refused["ok"].clone(), refused["error"].clone()), (Value::from(false), Value::from("other tenant")));

    let mut stranger = socket(address, "", Some("session=forged")).await;
    assert_eq!(read_json(&mut stranger).await["error"], "authentication rejected");
}

/// A publish hook refuses with its own message (HTTP 403 from its cause) and
/// rewrites what it lets through: the event lands on the tenant's channel.
#[tokio::test]
async fn a_publish_hook_refuses_with_its_own_cause_and_rewrites_what_it_allows() {
    let (engine, address) = domain().await;
    let (peer, mut inbox) = ruvent::connect(&engine, "peer", 64).await.unwrap();
    peer.subscribe("tenant.acme.room".into(), None).await.unwrap();

    let (status, result) = http(address, "POST", "/v1/events", "Cookie: session=s-bob", r#"{"kind":"dispatch","channel":"room","type":"admin.reset","payload":1}"#).await;
    assert_eq!((status, result["error"].clone()), (403, Value::from("operators only")));
    let (status, result) = http(address, "POST", "/v1/events", "Cookie: session=s-bob", r#"{"kind":"dispatch","channel":"room","type":"said","payload":{"n":2}}"#).await;
    assert_eq!((status, result["ok"].clone()), (200, Value::from(true)));
    let delivered = loop {
        match tokio::time::timeout(WAIT, inbox.recv()).await.expect("timed out").expect("closed") {
            Incoming::Delivery(event) => break event,
            _ => continue,
        }
    };
    assert_eq!(&*delivered.channel, "tenant.acme.room");
    assert_eq!(delivered.delegate.as_deref(), Some("bob"));
}

/// A deliver hook decides per user: two users on one channel, an event addressed to one of them.
#[tokio::test]
async fn a_deliver_hook_picks_which_users_see_an_event() {
    let (engine, address) = domain().await;
    let mut users = Vec::new();
    for session in ["session=s-alice", "session=s-bob"] {
        let mut user = socket(address, "", Some(session)).await;
        assert_eq!(read_json(&mut user).await["ok"], true);
        send(&mut user, r#"{"op":"subscribe","channel":"tenant.acme.room"}"#).await;
        assert_eq!(read_json(&mut user).await["ok"], true);
        users.push(user);
    }
    let (peer, _inbox) = ruvent::connect(&engine, "peer", 64).await.unwrap();
    peer.register_dispatch(Envelope::dispatch("tenant.acme.room", "said", Bytes::from_static(br#"{"to":"alice","n":1}"#))).await.unwrap();
    peer.register_dispatch(Envelope::dispatch("tenant.acme.room", "said", Bytes::from_static(br#"{"n":2}"#))).await.unwrap();
    let (alice, bob) = users.split_at_mut(1);
    assert_eq!(read_json(&mut alice[0]).await["payload"]["n"], 1);
    assert_eq!(read_json(&mut alice[0]).await["payload"]["n"], 2);
    assert_eq!(read_json(&mut bob[0]).await["payload"]["n"], 2, "the event addressed to alice never reached bob");
}
