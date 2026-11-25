//! The socket link: one connection to an event server over TCP and wire v2.
//!
//! This is the transport only. [`connect`] says `Hello` with the role the
//! given [`ruvent::Hello`] names, waits for `Welcome` (or the `Bye` that came
//! instead), and returns the same [`ruvent::Client`] an in-process connection
//! returns. [`open_raw`] stops below the client: the SDK's lines
//! (`crate::link`) number their own calls. Reconnection, the unacknowledged
//! buffer, server selection and the worker's window rules sit above this (2.9).
//!
//! Two tasks serve one connection:
//!
//! * the **writer** takes calls in order, encodes as many as are waiting and
//!   writes them with one vectored write (payloads shared, not copied);
//! * the **reader** reads each record head first, its body into its own
//!   buffer, and hands every message to the client's demultiplexer in
//!   arrival order.
//!
//! On `Bye` the client stops sending and closes at once (2.2: the server
//! closes in stages, so the client need not). When either task ends the
//! connection is over: the reader stops the writer first, so no call is sent
//! after the waiting calls were failed, then fails every waiting call with
//! `Closed` and puts `Incoming::Closed` in the inbox. Dropping every clone of
//! the client closes the socket.
//!
//! Every socket gets TCP keepalive ([`Keepalive`], S13).

mod socket;

pub use socket::Keepalive;

use async_trait::async_trait;
use ruvent::{ByeReason, Call, Client, Closed, Hello, Inbox, Link, Outbound, Welcome};
use ruvent_protocol::{features, validate, Body, Decoder, Encoder, Message, ProtocolError, RecordReader, Side, DICTIONARY, DIRECT, INLINE, MAX_BODY, VERSION};
use std::fmt;
use std::io;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

/// How a connection is set up.
#[derive(Clone, Copy, Debug)]
pub struct Options {
    /// Capacity of the inbox of unasked-for traffic.
    pub inbox: usize,
    /// S18: bound on each direction's name dictionary.
    pub dictionary: usize,
    /// B13: the largest body one record may carry.
    pub max_body: usize,
    /// S11: a body below this many bytes is copied next to its head when sent.
    pub inline: usize,
    /// S11: a body of at least this many bytes is read straight into its own buffer.
    pub direct: usize,
    /// Calls queued for the writer before `send` waits.
    pub queue: usize,
    /// Calls encoded into one vectored write, at most.
    pub batch: usize,
    /// The feature bits `Hello` asks for.
    pub features: u64,
    /// S13.
    pub keepalive: Keepalive,
}

impl Default for Options {
    fn default() -> Self {
        Options { inbox: 65_536, dictionary: DICTIONARY, max_body: MAX_BODY, inline: INLINE, direct: DIRECT, queue: 4096, batch: 1024, features: features::ALL, keepalive: Keepalive::default() }
    }
}

/// The server answered `Hello` with `Bye`: the error inside the `io::Error`
/// [`connect`] returns, so a caller can act on the reason (2.9's table).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Refused(pub ByeReason);

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "the server refused the connection: {}", self.0)
    }
}

impl std::error::Error for Refused {}

impl Refused {
    /// The reason, when `error` is a refusal.
    pub fn of(error: &io::Error) -> Option<ByeReason> {
        error.get_ref().and_then(|inner| inner.downcast_ref::<Refused>()).map(|refused| refused.0)
    }
}

impl Refused {
    /// The `io::Error` a refusal is reported as.
    pub fn error(reason: ByeReason) -> io::Error {
        refused(reason)
    }
}

fn refused(reason: ByeReason) -> io::Error {
    let kind = match reason {
        ByeReason::Token | ByeReason::Forbidden => io::ErrorKind::PermissionDenied,
        ByeReason::Version | ByeReason::Protocol => io::ErrorKind::InvalidData,
        ByeReason::Superseded | ByeReason::Slow | ByeReason::Draining => io::ErrorKind::ConnectionRefused,
    };
    io::Error::new(kind, Refused(reason))
}

/// A connection that was welcomed.
pub struct Connected {
    pub client: Client,
    pub inbox: Inbox,
    pub welcome: Welcome,
    /// What the server implements; use nothing it did not announce.
    pub features: u64,
}

/// A welcomed connection below the client API: calls go out through `link`
/// as they are (the caller numbers them), and everything the server sends
/// arrives on `outbound` in order. `outbound` ends after `Bye` or when the
/// socket closes. Dropping every clone of `link` writes what was queued and
/// closes the socket; dropping `outbound` alone stops reading.
pub struct RawConnection {
    pub link: Arc<dyn Link>,
    pub outbound: mpsc::Receiver<Outbound>,
    pub welcome: Welcome,
    pub features: u64,
}

struct Remote {
    calls: mpsc::Sender<Call>,
    max_body: usize,
}

#[async_trait]
impl Link for Remote {
    async fn send(&self, call: Call) -> Result<(), Closed> {
        // Refused here, before a sequence number is spent on it: a call the
        // encoder would refuse must not leave a gap the server closes on.
        let call = Message::Call(call);
        if validate(&call, self.max_body).is_err() {
            return Err(Closed);
        }
        let Message::Call(call) = call else { return Err(Closed) };
        self.calls.send(call).await.map_err(|_| Closed)
    }
}

/// Connects to an event server at `address` with a service credential's
/// `token`, for the role and producer `hello` names (`hello.credential` is
/// not sent: the server resolves the token). Returns once welcomed.
pub async fn connect(address: &str, token: &str, hello: Hello, options: Options) -> io::Result<(Client, Inbox)> {
    let connected = open(address, token, hello, options).await?;
    Ok((connected.client, connected.inbox))
}

/// [`connect`], with the `Welcome` and the server's feature bits.
pub async fn open(address: &str, token: &str, hello: Hello, options: Options) -> io::Result<Connected> {
    let role = hello.role;
    let raw = open_raw(address, token, hello, options).await?;
    let (client, demux, inbox) = ruvent::methods::client(raw.link, role, options.inbox);
    demux.route(Outbound::Welcome(raw.welcome.clone())).await;
    let mut outbound = raw.outbound;
    tokio::spawn(async move {
        while let Some(message) = outbound.recv().await {
            if !demux.route(message).await {
                break;
            }
        }
        // The reader stopped the writer before it let go of `outbound`, so
        // nothing is sent after the waiting calls fail here.
        demux.close().await;
    });
    Ok(Connected { client, inbox, welcome: raw.welcome, features: raw.features })
}

/// Connects like [`open`] and hands over the raw connection: the SDK's
/// lines number their own calls and read the server's messages as they are.
pub async fn open_raw(address: &str, token: &str, hello: Hello, options: Options) -> io::Result<RawConnection> {
    let stream = TcpStream::connect(address).await?;
    socket::tune(&stream, &options.keepalive)?;
    let (read, mut write) = stream.into_split();
    let mut encoder = Encoder::new(options.dictionary).with_inline(options.inline).with_max_body(options.max_body);
    let mut decoder = Decoder::new(Side::Client, options.dictionary);
    let mut reader = RecordReader::with_limits(read, options.direct, options.max_body);

    encoder.push(&Message::Hello { version: VERSION, features: options.features, token: token.to_owned(), hello })?;
    encoder.take().write_to(&mut write).await?;
    let (welcome, features) = match next(&mut reader, &mut decoder).await? {
        Some(Message::Welcome { welcome, features, .. }) => (welcome, features),
        Some(Message::Outbound(Outbound::Bye(reason))) => return Err(refused(reason)),
        Some(other) => return Err(ProtocolError::Handshake(format!("expected a welcome, got {other:?}")).into()),
        None => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "the server closed the connection during the handshake")),
    };

    let (calls, mut queue) = mpsc::channel::<Call>(options.queue.max(1));
    let (sink, outbound) = mpsc::channel::<Outbound>(options.inbox.max(1));

    let batch = options.batch.max(1);
    let mut writer = tokio::spawn(async move {
        let mut pending = Vec::with_capacity(batch);
        loop {
            if queue.recv_many(&mut pending, batch).await == 0 {
                // Every clone of the link is gone: what was queued is written.
                let _ = write.shutdown().await;
                return;
            }
            for call in pending.drain(..) {
                if encoder.push(&Message::Call(call)).is_err() {
                    return;
                }
            }
            if encoder.take().write_to(&mut write).await.is_err() {
                return;
            }
        }
    });

    tokio::spawn(async move {
        let mut writer_done = false;
        let mut detached = false;
        loop {
            let message = tokio::select! {
                message = next(&mut reader, &mut decoder) => message,
                _ = &mut writer => {
                    writer_done = true;
                    break;
                }
            };
            let Ok(Some(Message::Outbound(outbound))) = message else { break };
            let bye = matches!(outbound, Outbound::Bye(_));
            if sink.send(outbound).await.is_err() {
                // Nobody reads any more; the writer ends when the link is dropped.
                detached = true;
                break;
            }
            if bye {
                // 2.2: on `Bye` the client closes at once.
                break;
            }
        }
        // The connection is over: stop the writer before `outbound` ends, so
        // nothing is sent after its reader saw the end.
        if !writer_done && !detached {
            writer.abort();
            let _ = writer.await;
        }
        drop(sink);
    });

    Ok(RawConnection { link: Arc::new(Remote { calls, max_body: options.max_body }), outbound, welcome, features })
}

/// The next whole message. `None`: the server closed at a record boundary.
async fn next(reader: &mut RecordReader<OwnedReadHalf>, decoder: &mut Decoder) -> io::Result<Option<Message>> {
    loop {
        let Some(raw) = reader.head().await? else { return Ok(None) };
        let body = if decoder.admit(&raw)? && raw.body_len > 0 { Body::Bytes(reader.body(raw.body_len).await?) } else { Body::None };
        if let Some(message) = decoder.decode(&raw, body)? {
            return Ok(Some(message));
        }
    }
}
