//! One connection to one server, below the role logic: calls go out as they
//! are (the line numbers them), everything the server sends comes back in
//! order. Two ways to reach a server, behind one [`Connector`]:
//!
//! * [`Tcp`]: a socket and wire v2 ([`crate::remote::open_raw`]);
//! * [`Local`]: an engine in this process ([`ruvent::Engine::open`]), for
//!   tests and embedded use. The same producer, worker, requester and
//!   observer run on either.
//!
//! [`Conn`] normalises the one thing the two paths say differently: an
//! assignment's `until`. Over TCP the server writes the lease left at the
//! moment it starts writing; in-process the engine hands over the absolute
//! uptime. A [`Conn`] always carries the lease left, in ms, when it arrived.

use crate::remote::{self, Options, Refused};
use async_trait::async_trait;
use parking_lot::Mutex;
use ruvent::{Call, Closed, Engine, Hello, Link, Outbound, Session, Welcome};
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use tokio::sync::mpsc;

/// One welcomed connection.
pub struct Conn {
    pub link: Arc<dyn Link>,
    /// Ends after `Bye` or when the connection closes.
    pub outbound: mpsc::Receiver<Outbound>,
    pub welcome: Welcome,
    /// What the server implements.
    pub features: u64,
}

/// Opens connections to servers named by their address in the server list.
#[async_trait]
pub trait Connector: Send + Sync + 'static {
    /// Says `hello` to `server` and waits for `Welcome`. A `Bye` instead is an
    /// error carrying [`Refused`].
    async fn open(&self, server: &str, hello: Hello) -> io::Result<Conn>;
}

/// Servers over TCP, all with one service credential's token.
pub struct Tcp {
    token: String,
    options: Options,
}

impl Tcp {
    pub fn new(token: impl Into<String>) -> Self {
        Tcp { token: token.into(), options: Options::default() }
    }

    pub fn with_options(mut self, options: Options) -> Self {
        self.options = options;
        self
    }
}

#[async_trait]
impl Connector for Tcp {
    async fn open(&self, server: &str, hello: Hello) -> io::Result<Conn> {
        let raw = remote::open_raw(server, &self.token, hello, self.options).await?;
        Ok(Conn { link: raw.link, outbound: raw.outbound, welcome: raw.welcome, features: raw.features })
    }
}

/// Engines in this process by name. A name may be pointed at another engine
/// at any time (a restart): connections to the old one end with it.
///
/// | key | server name |
/// | --- | --- |
/// | scope | this connector |
/// | lifetime | `insert` ~ `remove` or the next `insert` under the name |
/// | bound | names inserted |
/// | cleared by | the caller |
pub struct Local {
    engines: Mutex<HashMap<String, Engine>>,
    /// Capacity of the channel a connection's messages arrive on.
    queue: usize,
}

impl Default for Local {
    fn default() -> Self {
        Local { engines: Mutex::default(), queue: 65_536 }
    }
}

impl Local {
    pub fn new() -> Self {
        Self::default()
    }

    /// Serves `name` from `engine` from now on.
    pub fn insert(&self, name: impl Into<String>, engine: Engine) {
        self.engines.lock().insert(name.into(), engine);
    }

    /// No engine answers `name` any more; new connections are refused.
    pub fn remove(&self, name: &str) -> Option<Engine> {
        self.engines.lock().remove(name)
    }
}

struct LocalLink {
    session: Session,
}

#[async_trait]
impl Link for LocalLink {
    async fn send(&self, call: Call) -> Result<(), Closed> {
        self.session.call(call).await
    }
}

/// The lease left at arrival, from the engine's absolute `until`.
fn arrived(message: Outbound, engine: &Engine) -> Outbound {
    match message {
        Outbound::Assign(mut assignment) => {
            assignment.until = assignment.until.saturating_sub(engine.uptime());
            Outbound::Assign(assignment)
        }
        Outbound::Claimed { req, mut assignment } => {
            assignment.until = assignment.until.saturating_sub(engine.uptime());
            Outbound::Claimed { req, assignment }
        }
        other => other,
    }
}

#[async_trait]
impl Connector for Local {
    async fn open(&self, server: &str, hello: Hello) -> io::Result<Conn> {
        let engine = self.engines.lock().get(server).cloned().ok_or_else(|| io::Error::new(io::ErrorKind::ConnectionRefused, format!("no engine is named {server}")))?;
        let (session, mut messages) = engine.open(hello).await.map_err(|_| io::Error::new(io::ErrorKind::ConnectionRefused, "the engine is closed"))?;
        let welcome = match messages.recv().await {
            Some(Outbound::Welcome(welcome)) => welcome,
            Some(Outbound::Bye(reason)) => return Err(Refused::error(reason)),
            Some(other) => return Err(io::Error::new(io::ErrorKind::InvalidData, format!("expected a welcome, got {other:?}"))),
            None => return Err(io::Error::new(io::ErrorKind::ConnectionReset, "the engine closed the connection")),
        };
        let (sink, outbound) = mpsc::channel(self.queue.max(1));
        tokio::spawn(async move {
            while let Some(message) = messages.recv().await {
                let bye = matches!(message, Outbound::Bye(_));
                if sink.send(arrived(message, &engine)).await.is_err() || bye {
                    break;
                }
            }
        });
        Ok(Conn { link: Arc::new(LocalLink { session }), outbound, welcome, features: ruvent_protocol::features::ALL })
    }
}
