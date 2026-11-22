//! The socket link: the seven methods over TCP and the binary protocol.
//!
//! [`connect`] returns the same [`ruvent::Client`] an in-process connection
//! returns, so a worker or a proxy cannot tell the two apart. Two tasks serve
//! one connection:
//!
//! * the **writer** takes calls in order, packs as many as are waiting into
//!   frames, writes and flushes;
//! * the **reader** decodes frames and hands every reply, notice and delivery
//!   to the client's demultiplexer, in arrival order.
//!
//! When either ends the connection is over: the reader stops the writer
//! first, so no call can be sent after the waiting calls were failed, then
//! fails every waiting call with `Closed` and puts `Incoming::Closed` in the
//! inbox. Dropping every clone of the client closes the socket.

use async_trait::async_trait;
use ruvent::{Call, Client, Closed, Inbox, Link};
use ruvent_protocol::{read_frame, write_frames, Decoder, Encoder, Message, ProtocolError, Side, DICTIONARY, MAX_FRAME, VERSION};
use std::io;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

/// How a connection is set up.
#[derive(Clone, Copy, Debug)]
pub struct Options {
    /// Capacity of the inbox of notices and deliveries.
    pub inbox: usize,
    /// Bound on each direction's name dictionary.
    pub dictionary: usize,
    /// Bound on one frame, length field excluded.
    pub max_frame: usize,
}

impl Default for Options {
    fn default() -> Self {
        Options { inbox: 65_536, dictionary: DICTIONARY, max_frame: MAX_FRAME }
    }
}

/// Calls queued for the writer.
const QUEUE: usize = 4096;
/// Most calls packed in one pass of the writer.
const PASS: usize = 1024;

struct Remote {
    calls: mpsc::Sender<(u32, Call)>,
}

#[async_trait]
impl Link for Remote {
    async fn send(&self, req: u32, call: Call) -> Result<(), Closed> {
        self.calls.send((req, call)).await.map_err(|_| Closed)
    }
}

fn refused(reason: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, reason.into())
}

/// Connects to an event server at `address` with a service credential's `token`.
pub async fn connect(address: &str, token: &str, options: Options) -> io::Result<(Client, Inbox)> {
    let stream = TcpStream::connect(address).await?;
    stream.set_nodelay(true)?;
    let (mut read, mut write) = stream.into_split();
    let mut encoder = Encoder::new(options.dictionary, options.max_frame);
    let mut decoder = Decoder::new(Side::Client, options.dictionary, options.max_frame);

    encoder.push(&Message::Hello { version: VERSION, token: token.to_owned() })?;
    write_frames(&mut write, &encoder.take()).await?;
    let frame = read_frame(&mut read, options.max_frame).await?.ok_or_else(|| refused("the server closed the connection during the handshake"))?;
    match decoder.decode(frame)?.as_slice() {
        [Message::Welcome { version, .. }] if *version == VERSION => {}
        [Message::Welcome { version, .. }] => return Err(ProtocolError::Version(*version).into()),
        [Message::Bye { reason }] => return Err(refused(reason.clone())),
        other => return Err(ProtocolError::Handshake(format!("expected a welcome, got {other:?}")).into()),
    }

    let (calls, mut queue) = mpsc::channel::<(u32, Call)>(QUEUE);
    let (client, demux, inbox) = ruvent::methods::client(Arc::new(Remote { calls }), options.inbox);

    let mut writer = tokio::spawn(async move {
        let mut batch = Vec::with_capacity(PASS);
        loop {
            if queue.recv_many(&mut batch, PASS).await == 0 {
                // Every clone of the client is gone.
                return;
            }
            for (req, call) in batch.drain(..) {
                if encoder.push(&Message::Call { req, call }).is_err() {
                    return;
                }
            }
            if write_frames(&mut write, &encoder.take()).await.is_err() {
                return;
            }
        }
    });

    tokio::spawn(async move {
        let mut writer_done = false;
        loop {
            let frame = tokio::select! {
                frame = read_frame(&mut read, decoder.max_frame()) => frame,
                _ = &mut writer => {
                    writer_done = true;
                    break;
                }
            };
            let Ok(Some(frame)) = frame else { break };
            let Ok(messages) = decoder.decode(frame) else { break };
            let mut open = true;
            for message in messages {
                open = match message {
                    Message::Outbound(outbound) => demux.route(outbound).await,
                    _ => false,
                };
                if !open {
                    break;
                }
            }
            if !open {
                break;
            }
        }
        // Stop the writer before failing the waiting calls, so nothing is sent after.
        if !writer_done {
            writer.abort();
            let _ = writer.await;
        }
        demux.close().await;
    });

    Ok((client, inbox))
}
