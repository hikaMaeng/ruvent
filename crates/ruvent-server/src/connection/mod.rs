//! One TCP connection to the event server.
//!
//! ```text
//! socket ─► reader: frames → calls → Session::call (waits only while the engine inbox is full)
//! engine ─► send buffer (engine try_sends; full = disconnect) ─► writer: Outbound → frames → socket
//! ```
//!
//! The engine never waits on this socket: it hands the writer's buffer what
//! it has and moves on. When the engine disconnects the session (buffer full,
//! shutdown) the writer ends and the socket is closed, which is the client's
//! signal that it may have missed something.

use ruvent::Engine;
use ruvent_protocol::{read_frame, write_frames, Decoder, Encoder, Message, Side, DICTIONARY, MAX_FRAME, VERSION};
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};

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

#[derive(Clone, Copy, Debug)]
pub struct ServerOptions {
    /// Bound on each direction's name dictionary.
    pub dictionary: usize,
    /// Bound on one frame, length field excluded.
    pub max_frame: usize,
    /// How long a new connection has to send its hello.
    pub handshake: Duration,
}

impl Default for ServerOptions {
    fn default() -> Self {
        ServerOptions { dictionary: DICTIONARY, max_frame: MAX_FRAME, handshake: Duration::from_secs(10) }
    }
}

/// Most messages packed in one pass of a writer.
const PASS: usize = 1024;

/// Accepts connections until the listener fails.
pub async fn serve(listener: TcpListener, engine: Engine, credentials: Arc<Credentials>, options: ServerOptions) -> io::Result<()> {
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(accepted) => accepted,
            // One client's failed connection attempt is not the listener's.
            Err(error) if matches!(error.kind(), io::ErrorKind::ConnectionAborted | io::ErrorKind::ConnectionReset) => continue,
            Err(error) => return Err(error),
        };
        tokio::spawn(connection(stream, engine.clone(), credentials.clone(), options));
    }
}

async fn bye(write: &mut OwnedWriteHalf, encoder: &mut Encoder, reason: &str) {
    if encoder.push(&Message::Bye { reason: reason.to_owned() }).is_ok() {
        let _ = write_frames(write, &encoder.take()).await;
    }
}

/// Serves one connection from hello to close.
pub async fn connection(stream: TcpStream, engine: Engine, credentials: Arc<Credentials>, options: ServerOptions) {
    let _ = stream.set_nodelay(true);
    let (mut read, mut write) = stream.into_split();
    let mut decoder = Decoder::new(Side::Server, options.dictionary, options.max_frame);
    let mut encoder = Encoder::new(options.dictionary, options.max_frame);

    // The hello's deadline: the one place a connection waits on the clock.
    let hello = match tokio::time::timeout(options.handshake, read_frame(&mut read, options.max_frame)).await {
        Ok(Ok(Some(frame))) => decoder.decode(frame).ok(),
        _ => None,
    };
    let (version, token) = match hello.as_deref() {
        Some([Message::Hello { version, token }]) => (*version, token.clone()),
        _ => return bye(&mut write, &mut encoder, "expected a hello").await,
    };
    if version != VERSION {
        return bye(&mut write, &mut encoder, &format!("unsupported protocol version {version}; this server speaks {VERSION}")).await;
    }
    let Some(credential) = credentials.credential(&token).map(str::to_owned) else {
        return bye(&mut write, &mut encoder, "unknown token").await;
    };
    let Ok((session, mut outbound)) = engine.open(&credential).await else {
        return bye(&mut write, &mut encoder, "the server is stopping").await;
    };
    if encoder.push(&Message::Welcome { version: VERSION, credential }).is_err() || write_frames(&mut write, &encoder.take()).await.is_err() {
        return;
    }

    let mut writer = tokio::spawn(async move {
        let mut batch = Vec::with_capacity(PASS);
        loop {
            if outbound.recv_many(&mut batch, PASS).await == 0 {
                // The engine let go of this connection.
                return;
            }
            for message in batch.drain(..) {
                if encoder.push(&Message::Outbound(message)).is_err() {
                    return;
                }
            }
            if write_frames(&mut write, &encoder.take()).await.is_err() {
                return;
            }
        }
    });

    let mut writer_done = false;
    'reading: loop {
        let frame = tokio::select! {
            frame = read_frame(&mut read, decoder.max_frame()) => frame,
            _ = &mut writer => {
                writer_done = true;
                break;
            }
        };
        let Ok(Some(frame)) = frame else { break };
        let Ok(messages) = decoder.decode(frame) else { break };
        for message in messages {
            let Message::Call { req, call } = message else { break 'reading };
            if session.call(req, call).await.is_err() {
                break 'reading;
            }
        }
    }
    drop(session);
    if !writer_done {
        writer.abort();
        let _ = writer.await;
    }
}
