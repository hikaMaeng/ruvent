//! A transport plugin written the way a third party would write one: against
//! the public `Transport` and `Connection` traits and the shared JSON frames,
//! with no access to anything inside the crate.
//!
//! Its wire is a pair of in-process channels carrying JSON text, so a test
//! can play the client without a socket.

use ruvent::wire::json::{decode_client, encode_server};
use ruvent::{async_trait, CancellationToken, ClientFrame, Connection, Credentials, Gateway, ServerFrame, Transport};
use serde_json::Value;
use std::io;
use std::time::Duration;
use tokio::sync::mpsc;

/// What a client hands the transport to connect.
struct Dial {
    credentials: Credentials,
    inbound: mpsc::Receiver<String>,
    outbound: mpsc::Sender<String>,
}

pub struct Memory {
    dials: mpsc::Receiver<Dial>,
}

/// The client side of the in-memory wire.
#[derive(Clone)]
pub struct Switchboard {
    dials: mpsc::Sender<Dial>,
}

pub fn memory() -> (Memory, Switchboard) {
    let (dials, incoming) = mpsc::channel(16);
    (Memory { dials: incoming }, Switchboard { dials })
}

impl Switchboard {
    pub async fn connect(&self, token: &str) -> Client {
        let (to_server, inbound) = mpsc::channel(64);
        let (outbound, from_server) = mpsc::channel(1024);
        let credentials = Credentials { token: Some(token.to_owned()), peer: None };
        self.dials.send(Dial { credentials, inbound, outbound }).await.expect("the transport is running");
        Client { to_server, from_server }
    }
}

struct Wire {
    inbound: mpsc::Receiver<String>,
    outbound: mpsc::Sender<String>,
}

#[async_trait]
impl Connection for Wire {
    async fn recv(&mut self) -> Option<ClientFrame> {
        // Cancel-safe: a channel receive loses nothing when dropped, and the
        // decode in between does not await.
        loop {
            let text = self.inbound.recv().await?;
            if let Some(frame) = decode_client(&text) {
                return Some(frame);
            }
        }
    }

    async fn send(&mut self, frame: ServerFrame) -> io::Result<()> {
        self.outbound.send(encode_server(&frame)).await.map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))
    }
}

#[async_trait]
impl Transport for Memory {
    async fn run(mut self: Box<Self>, gateway: Gateway, shutdown: CancellationToken) -> io::Result<()> {
        loop {
            let dial = tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                dial = self.dials.recv() => match dial { Some(dial) => dial, None => return Ok(()) },
            };
            let gateway = gateway.clone();
            tokio::spawn(async move {
                // Refused: the wire is simply closed, as a socket would be.
                let Some(identity) = gateway.authenticate(&dial.credentials).await else { return };
                gateway.serve(Wire { inbound: dial.inbound, outbound: dial.outbound }, identity).await;
            });
        }
    }
}

pub struct Client {
    to_server: mpsc::Sender<String>,
    from_server: mpsc::Receiver<String>,
}

impl Client {
    pub async fn send(&self, frame: Value) {
        self.to_server.send(frame.to_string()).await.expect("connection open");
    }

    /// The next frame, or `None` once the server has closed the wire.
    pub async fn next(&mut self) -> Option<Value> {
        let text = tokio::time::timeout(Duration::from_secs(30), self.from_server.recv()).await.expect("no frame within 30 seconds")?;
        Some(serde_json::from_str(&text).expect("the server sends JSON"))
    }

    pub async fn expect(&mut self, kind: &str) -> Value {
        let frame = self.next().await.expect("connection closed");
        assert_eq!(frame["type"], kind, "expected a `{kind}` frame, got {frame}");
        frame
    }

    /// Event frames until one with this action has arrived, inclusive.
    pub async fn events_until(&mut self, action: &str) -> Vec<Value> {
        let mut events = Vec::new();
        loop {
            let frame = self.expect("event").await;
            let done = frame["event"]["action"] == action;
            events.push(frame);
            if done {
                return events;
            }
        }
    }
}
