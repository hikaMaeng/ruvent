//! The read task: records → calls.
//!
//! A record's head comes first. Its body is read into its own buffer —
//! except a work connection's follow-up body under memory pressure, which is
//! streamed to disk through the spool (2.6, items 4 and 5). A work connection
//! never stops reading: the completions that free memory arrive on it. A
//! production connection stops only while the engine says so
//! (`Session::reading`): one that kept writing to a paused root past its
//! allowance (S14). Connections that follow `Paused` are never stopped.

use ruvent::{Engine, Role, Session, Spooled};
use ruvent_protocol::records::FOLLOW_UP;
use ruvent_protocol::{Body, Decoder, Message, ProtocolError, RawRecord, RecordReader};
use std::io;
use std::sync::Arc;
use tokio::net::tcp::OwnedReadHalf;
use tokio::sync::{oneshot, watch};

/// Why the read task stopped applying calls.
#[derive(Debug)]
pub(super) enum ReadEnd {
    /// The peer closed its side at a record boundary.
    Eof,
    /// The socket failed, or the stream ended inside a record.
    Broken,
    /// A record was refused: the connection is closed with `Fault` and `Bye{protocol}`.
    Protocol(ProtocolError),
    /// The owner asked it to stop, or the engine is gone. It now reads and
    /// drops until the peer closes.
    Released,
}

pub(super) struct Reading {
    reader: RecordReader<OwnedReadHalf>,
    decoder: Decoder,
    session: Arc<Session>,
    engine: Engine,
    role: Role,
    closing: watch::Receiver<bool>,
    spool_piece: usize,
}

impl Reading {
    pub(super) fn new(reader: RecordReader<OwnedReadHalf>, decoder: Decoder, session: Arc<Session>, engine: Engine, role: Role, closing: watch::Receiver<bool>, spool_piece: usize) -> Self {
        Reading { reader, decoder, session, engine, role, closing, spool_piece: spool_piece.max(1) }
    }

    /// Applies calls until the peer stops, a record is refused, or the owner
    /// says `closing`.
    async fn calls(&mut self) -> ReadEnd {
        let mut closing = self.closing.clone();
        loop {
            let step = tokio::select! {
                biased;
                _ = closing.changed() => return ReadEnd::Released,
                step = self.next() => step,
            };
            if let Err(end) = step {
                return end;
            }
        }
    }

    /// Reads one record and, when it completes a message, applies it.
    async fn next(&mut self) -> Result<(), ReadEnd> {
        let raw = match self.reader.head().await {
            Ok(Some(raw)) => raw,
            Ok(None) => return Err(ReadEnd::Eof),
            Err(error) => return Err(broken(error)),
        };
        let carries = self.decoder.admit(&raw).map_err(ReadEnd::Protocol)?;
        let body = if !carries || raw.body_len == 0 {
            Body::None
        } else if raw.tag == FOLLOW_UP && self.role == Role::Work && self.to_disk(&raw)? {
            Body::Spooled(self.spool(raw.body_len).await.map_err(|_| ReadEnd::Broken)?)
        } else {
            Body::Bytes(self.reader.body(raw.body_len).await.map_err(broken)?)
        };
        match self.decoder.decode(&raw, body).map_err(ReadEnd::Protocol)? {
            Some(Message::Call(call)) => self.session.call(call).await.map_err(|_| ReadEnd::Released),
            Some(_) => Err(ReadEnd::Protocol(ProtocolError::WrongDirection(raw.tag))),
            None => Ok(()),
        }
    }

    /// Whether this follow-up's body goes to disk (2.6): the engine decides
    /// from the memory in use. The head is checked first.
    fn to_disk(&self, raw: &RawRecord) -> Result<bool, ReadEnd> {
        self.decoder.follow_up(raw).map_err(ReadEnd::Protocol)?;
        Ok(self.engine.body_to_disk(u64::from(raw.body_len)))
    }

    /// Streams the next `len` body bytes from the socket into the spool, a
    /// piece at a time, each piece one blocking write.
    async fn spool(&mut self, len: u32) -> io::Result<Spooled> {
        let spool = self.engine.spool().ok_or_else(|| io::Error::other("no spool"))?;
        let mut write = blocking(move || spool.receive(len)).await?;
        let mut left = len as usize;
        while left > 0 {
            let mut pieces = Vec::new();
            let mut gathered = 0;
            while left > 0 && gathered < self.spool_piece {
                let piece = self.reader.chunk(left.min(self.spool_piece - gathered)).await?;
                left -= piece.len();
                gathered += piece.len();
                pieces.push(piece);
            }
            write = blocking(move || {
                for piece in &pieces {
                    write.write(piece)?;
                }
                Ok(write)
            })
            .await?;
        }
        blocking(move || write.finish()).await
    }
}

async fn blocking<T: Send + 'static>(work: impl FnOnce() -> io::Result<T> + Send + 'static) -> io::Result<T> {
    tokio::task::spawn_blocking(work).await.map_err(io::Error::other)?
}

fn broken(error: io::Error) -> ReadEnd {
    match error.get_ref().and_then(|inner| inner.downcast_ref::<ProtocolError>()) {
        Some(protocol) => ReadEnd::Protocol(protocol.clone()),
        None => ReadEnd::Broken,
    }
}

/// The read task: applies calls, lets go of the session and says why it
/// stopped, then — when the connection is being closed — reads and drops
/// until the peer closes (the staged close's last step; the owner's deadline
/// aborts it).
pub(super) async fn run(mut reading: Reading, ended: oneshot::Sender<ReadEnd>) {
    let outcome = reading.calls().await;
    let Reading { mut reader, session, .. } = reading;
    drop(session);
    let discard = matches!(outcome, ReadEnd::Released | ReadEnd::Protocol(_));
    let _ = ended.send(outcome);
    if discard {
        let _ = reader.discard().await;
    }
}
