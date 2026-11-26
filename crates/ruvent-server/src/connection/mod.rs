//! One TCP connection to the event server (2.2).
//!
//! ```text
//! socket ─► read task: record head → body (memory, or the spool for a work
//!           connection under pressure) → Call → Session::call
//! engine ─► owner task: Outbound → unsent queue (Committed folded, faults
//!           once, byte bound, stale assignments dropped) → records → vectored write ─► socket
//! ```
//!
//! * `handshake`: the first record, version 1 recognised and refused.
//! * `read`: the read task. A production connection stops reading only
//!   while the engine says so (`Session::reading`, a connection that kept
//!   writing to a paused root); a work connection never stops.
//! * [`write`]: the owner. It holds the connection's unsent messages and is
//!   the only one that encodes and writes.
//! * `close`: every `Bye` is followed by a staged close under one deadline.
//! * [`socket`]: TCP keepalive (S13).
//!
//! The engine never waits on this socket: it hands the owner what it has and
//! moves on. The owner takes everything the engine hands it at once, so the
//! engine's count-bounded buffer never fills; the owner bounds what it holds
//! in bytes instead (S7).

mod close;
mod handshake;
mod read;
pub mod socket;
mod unsent;
mod write;

use ruvent::Engine;
use ruvent_protocol::{features, DIRECT, INLINE, MAX_BODY};
use socket::Keepalive;
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

/// Service credentials by token.
///
/// | key | token |
/// | --- | --- |
/// | scope | one server |
/// | lifetime | the server's run; fixed at start |
/// | bound | the configured credentials |
#[derive(Clone, Debug, Default)]
pub struct Credentials {
    by_token: HashMap<String, String>,
}

impl Credentials {
    /// From `(name, token)` pairs.
    pub fn new(pairs: impl IntoIterator<Item = (String, String)>) -> Self {
        Credentials { by_token: pairs.into_iter().map(|(name, token)| (token, name)).collect() }
    }

    /// The credential name a token stands for.
    pub fn credential(&self, token: &str) -> Option<&str> {
        self.by_token.get(token).map(String::as_str)
    }
}

/// What every connection of one server is held to. Each value is a field
/// with its default (plan chapter 8 is configuration, not constants).
#[derive(Clone, Copy, Debug)]
pub struct ServerOptions {
    /// B13: the largest body one record may carry.
    pub max_body: usize,
    /// S11: a body below this many bytes is copied next to its head when sent.
    pub inline: usize,
    /// S11: a body of at least this many bytes is read straight into its own buffer.
    pub direct: usize,
    /// How long a new connection has to send its `Hello`.
    pub handshake: Duration,
    /// S7: the unsent bytes an observation connection may hold (the dispatch
    /// and notice bytes, for a work connection) before it is sent `Bye{slow}`.
    pub unsent: u64,
    /// S20: how long a staged close waits for the peer to close after `Bye`.
    pub close: Duration,
    /// S13: TCP keepalive on every socket.
    pub keepalive: Keepalive,
    /// Bytes gathered before one blocking write while a follow-up's body is
    /// streamed to disk (2.6).
    pub spool_piece: usize,
    /// Messages encoded into one vectored write, at most.
    pub batch: usize,
    /// The feature bits `Welcome` announces.
    pub features: u64,
}

impl Default for ServerOptions {
    fn default() -> Self {
        ServerOptions {
            max_body: MAX_BODY,
            inline: INLINE,
            direct: DIRECT,
            handshake: Duration::from_secs(10),
            unsent: 256 << 20,
            close: Duration::from_secs(5),
            keepalive: Keepalive::default(),
            spool_piece: 256 << 10,
            batch: 1024,
            features: features::ALL,
        }
    }
}

/// What a connection hears about the server going down, and how it says it
/// has handed its close to the engine.
///
/// | state | key | lifetime | bound | cleared by |
/// | --- | --- | --- | --- | --- |
/// | `stopping` | the server | serve ~ its end | 1 | `serve` (sets it once) |
/// | `session` (a sender clone) | one connection | accepted ~ its engine session closed | open connections | the connection, right after `Session::close` |
#[derive(Clone)]
pub(crate) struct Lifeline {
    pub(crate) stopping: watch::Receiver<bool>,
    pub(crate) session: mpsc::Sender<()>,
}

/// Accepts connections until `shutdown` resolves or the listener fails, then
/// takes the server down in the order of 2.8 (B22) and shuts the engine down:
///
/// 1. stop accepting;
/// 2. drain the engine: every production connection gets its final
///    `Committed`, then `Bye{draining}` and a staged close; new production
///    connections would be sent away;
/// 3. every other connection is sent `Bye{draining}` after what it was
///    already given; assignments it had not started writing go back to
///    waiting, their failure count unchanged;
/// 4. once every connection has handed its close to the engine,
///    `Engine::shutdown`: the stopped ledger's checkpoint. Written
///    assignments stay claimed across the restart (M8-6): a peer slow to
///    close cannot make them run out meanwhile;
/// 5. wait until every staged close finished (each bounded by the close deadline).
///
/// `Err`: the listener failed, or the engine had stopped on its own.
pub async fn serve(listener: TcpListener, engine: Engine, credentials: Arc<Credentials>, options: ServerOptions, shutdown: impl Future<Output = ()>) -> io::Result<()> {
    let (stop, stopping) = watch::channel(false);
    let (held, mut released) = mpsc::channel::<()>(1);
    let lifeline = Lifeline { stopping, session: held };
    let mut connections = JoinSet::new();
    let shutdown = std::pin::pin!(shutdown);
    let mut shutdown = shutdown;
    let outcome = loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    connections.spawn(connection(stream, engine.clone(), credentials.clone(), options, lifeline.clone()));
                }
                // One client's failed connection attempt is not the listener's.
                Err(error) if matches!(error.kind(), io::ErrorKind::ConnectionAborted | io::ErrorKind::ConnectionReset) => {}
                Err(error) => break Err(error),
            },
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
            () = &mut shutdown => break Ok(()),
        }
    };
    drop(listener);
    drop(lifeline);
    let _ = engine.drain().await;
    let _ = stop.send(true);
    // Every connection's sender goes once its session is closed.
    let _ = released.recv().await;
    let stopped = engine.shutdown().await;
    while connections.join_next().await.is_some() {}
    outcome?;
    stopped.map_err(io::Error::other)
}

/// Serves one connection from `Hello` to close.
async fn connection(stream: TcpStream, engine: Engine, credentials: Arc<Credentials>, options: ServerOptions, lifeline: Lifeline) {
    // A socket the kernel cannot watch still serves; it only loses the early
    // notice of a silent disconnection.
    let _ = socket::tune(&stream, &options.keepalive);
    let (read, write) = stream.into_split();
    let mut reader = ruvent_protocol::RecordReader::with_limits(read, options.direct, options.max_body);
    let mut decoder = ruvent_protocol::Decoder::new(ruvent_protocol::Side::Server);
    let encoder = ruvent_protocol::Encoder::new().with_features(options.features).with_inline(options.inline).with_max_body(options.max_body);
    let hello = match handshake::hello(&mut reader, &mut decoder, &credentials, options.handshake).await {
        Ok(hello) => hello,
        Err(refusal) => {
            drop(lifeline);
            return close::refuse(write, reader, encoder, refusal, options.close).await;
        }
    };
    let role = hello.role;
    let Ok((session, outbound)) = engine.open(hello).await else {
        drop(lifeline);
        return close::refuse(write, reader, encoder, handshake::Refusal::Bye(ruvent::ByeReason::Draining), options.close).await;
    };
    let session = Arc::new(session);
    let (closing, closing_rx) = watch::channel(false);
    let (ended, read_end) = tokio::sync::oneshot::channel();
    let reading = read::Reading::new(reader, decoder, session.clone(), engine.clone(), role, closing_rx, options.spool_piece);
    let read_task = tokio::spawn(read::run(reading, ended));
    let owner = write::Owner::new(write, encoder, engine, role, session, closing, read_end, read_task, options, lifeline.session);
    owner.run(outbound, lifeline.stopping).await;
}
