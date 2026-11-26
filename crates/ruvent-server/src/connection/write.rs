//! The owner: the connection's unsent messages, encoding and writing.
//!
//! The owner takes everything the engine hands the connection at once into
//! its [`Unsent`] queue (one cumulative `Committed`, faults once, the byte
//! bound) and writes from it whenever the socket takes more:
//!
//! * **an assignment is checked when its write starts:** one whose lease
//!   already ran out was reclaimed (or is about to be) and is dropped instead
//!   of written (2.4); an explicit claim's answer becomes `Refused{missing}`.
//!   The remaining lease it carries is measured at that moment;
//! * **one vectored write per batch**, payloads shared, never copied (2.2).
//!
//! When the connection ends the owner tells the engine which assignments it
//! never finished writing, so they go back to waiting with their failure
//! count unchanged (2.4). Every `Bye` — the engine's or its own (`slow`,
//! `protocol`, `draining`) — is followed by a staged close.

use super::close::staged;
use super::read::ReadEnd;
use super::unsent::{assigned_id, Unsent};
use super::ServerOptions;
use ruvent::{ByeReason, Engine, EventId, FaultKind, Outbound, Refusal, Role, Session};
use ruvent_protocol::{Encoder, Message, Parts, ProtocolError};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

/// Bytes encoded into one write, at most (besides one record over it).
const BATCH_BYTES: usize = 4 << 20;

type Written = (OwnedWriteHalf, Parts, io::Result<()>);
type WriteFuture = Pin<Box<dyn Future<Output = Written> + Send>>;

/// What one write in progress carries besides its bytes.
#[derive(Default)]
struct Flight {
    /// Where each assignment's record ends in the batch, and its event id.
    ends: Vec<(u64, EventId)>,
    /// Bytes of the batch counted against the bound.
    weight: u64,
}

/// Why the owner stops serving the connection.
enum Ending {
    /// The engine let go of the connection (its `Bye`, if any, is queued).
    Engine,
    /// The peer went away: no staged close.
    Gone,
    /// The write failed; these assignments never reached the socket whole.
    Broken(Vec<EventId>),
    /// A record was refused; the fault's detail.
    Protocol(&'static str),
    /// Over the byte bound.
    Slow,
    /// The server is going down.
    Stopping,
}

/// One connection's owner.
pub(super) struct Owner {
    write: Option<OwnedWriteHalf>,
    encoder: Encoder,
    engine: Engine,
    role: Role,
    session: Option<Arc<Session>>,
    closing: watch::Sender<bool>,
    read_end: Option<oneshot::Receiver<ReadEnd>>,
    read_task: Option<JoinHandle<()>>,
    options: ServerOptions,
    unsent: Unsent,
    /// Dropped right after the engine was told of the close (`serve` waits
    /// for every one before it shuts the engine down).
    held: Option<mpsc::Sender<()>>,
}

async fn flying(writing: &mut Option<WriteFuture>) -> Written {
    match writing {
        Some(future) => future.await,
        None => std::future::pending().await,
    }
}

async fn read_ended(read_end: &mut Option<oneshot::Receiver<ReadEnd>>) -> ReadEnd {
    match read_end {
        Some(receiver) => receiver.await.unwrap_or(ReadEnd::Broken),
        None => std::future::pending().await,
    }
}

/// The fault detail for a refused record (`Fault` carries a fixed text).
fn detail(error: &ProtocolError) -> &'static str {
    match error {
        ProtocolError::Malformed(_) => "a record's fields are malformed",
        ProtocolError::UnknownTag(_) => "a record tag this version does not know",
        ProtocolError::WrongDirection(_) => "a record that may not come here",
        ProtocolError::Version(_) => "another protocol version",
        ProtocolError::TooLarge(_) => "a body or a count over the bound",
        ProtocolError::UnexpectedBody(_) => "a body on a record that carries none",
        ProtocolError::Utf8 => "text that is not UTF-8",
        ProtocolError::UnknownCode(_) => "a code this version does not know",
        ProtocolError::Handshake(_) => "a record before the handshake",
    }
}

impl Owner {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        write: OwnedWriteHalf,
        encoder: Encoder,
        engine: Engine,
        role: Role,
        session: Arc<Session>,
        closing: watch::Sender<bool>,
        read_end: oneshot::Receiver<ReadEnd>,
        read_task: JoinHandle<()>,
        options: ServerOptions,
        held: mpsc::Sender<()>,
    ) -> Self {
        Owner {
            write: Some(write),
            encoder,
            engine,
            role,
            session: Some(session),
            closing,
            read_end: Some(read_end),
            read_task: Some(read_task),
            options,
            unsent: Unsent::new(role, options.unsent),
            held: Some(held),
        }
    }

    /// Encodes the next batch from the queue and starts writing it.
    fn start(&mut self) -> Option<(WriteFuture, Flight)> {
        let write = self.write.take()?;
        let now = self.engine.uptime();
        self.encoder.set_now(now);
        let mut flight = Flight::default();
        let mut count = 0;
        while count < self.options.batch && self.encoder.len() < BATCH_BYTES {
            let Some((message, weight)) = self.unsent.pop() else { break };
            flight.weight += weight;
            let message = match message {
                // Reclaimed before its write started: cancelled (2.4).
                Outbound::Assign(assignment) if assignment.until <= now => continue,
                Outbound::Claimed { req, assignment } if assignment.until <= now => Outbound::Refused { req, reason: Refusal::Missing },
                other => other,
            };
            let assigned = assigned_id(&message);
            // What the engine hands over always encodes; a message that did
            // not is dropped rather than closing the connection.
            if self.encoder.push(&Message::Outbound(message)).is_err() {
                continue;
            }
            if let Some(id) = assigned {
                flight.ends.push((self.encoder.len() as u64, id));
            }
            count += 1;
        }
        let parts = self.encoder.take();
        if parts.is_empty() {
            self.write = Some(write);
            self.unsent.written(flight.weight);
            return None;
        }
        let future: WriteFuture = Box::pin(async move {
            let mut write = write;
            let mut parts = parts;
            let result = parts.write_to(&mut write).await;
            (write, parts, result)
        });
        Some((future, flight))
    }

    /// A write finished. `Some`: it failed, and these assignments never reached the socket whole.
    fn landed(&mut self, written: Written, flight: Option<Flight>) -> Option<Vec<EventId>> {
        let (write, parts, result) = written;
        self.write = Some(write);
        let flight = flight.unwrap_or_default();
        self.unsent.written(flight.weight);
        result.err().map(|_| flight.ends.iter().filter(|(end, _)| *end > parts.written()).map(|(_, id)| *id).collect())
    }

    /// Serves the connection until it ends, then closes it.
    pub(super) async fn run(mut self, mut outbound: mpsc::Receiver<Outbound>, mut stopping: watch::Receiver<bool>) {
        let mut writing: Option<WriteFuture> = None;
        let mut flight: Option<Flight> = None;
        let mut stopped = *stopping.borrow_and_update();
        let ending = loop {
            // Production connections are taken down by the engine's drain:
            // final `Committed`, then its `Bye{draining}`.
            if stopped && self.role != Role::Produce {
                break Ending::Stopping;
            }
            if writing.is_none() && !self.unsent.is_empty() {
                if let Some((future, meta)) = self.start() {
                    writing = Some(future);
                    flight = Some(meta);
                }
            }
            tokio::select! {
                message = outbound.recv() => match message {
                    Some(message) => if self.unsent.accept(message) { break Ending::Slow },
                    None => break Ending::Engine,
                },
                written = flying(&mut writing) => {
                    writing = None;
                    if let Some(unwritten) = self.landed(written, flight.take()) {
                        break Ending::Broken(unwritten);
                    }
                }
                end = read_ended(&mut self.read_end) => {
                    self.read_end = None;
                    break match end {
                        ReadEnd::Eof | ReadEnd::Broken => Ending::Gone,
                        ReadEnd::Protocol(error) => Ending::Protocol(detail(&error)),
                        ReadEnd::Released => Ending::Engine,
                    };
                }
                changed = stopping.changed(), if !stopped => stopped = changed.is_err() || *stopping.borrow_and_update(),
            }
        };
        self.end(ending, writing, flight, outbound).await;
    }

    /// Hands the engine the connection's close with the assignments it never got.
    async fn close_session(&mut self, unwritten: Vec<EventId>) {
        let Some(session) = self.session.take() else { return };
        match Arc::try_unwrap(session) {
            Ok(session) => session.close(unwritten).await,
            // The read task still holds it (only past the close deadline):
            // dropping it closes the session with every assignment written.
            Err(shared) => drop(shared),
        }
        self.held = None;
    }

    async fn end(mut self, ending: Ending, writing: Option<WriteFuture>, flight: Option<Flight>, outbound: mpsc::Receiver<Outbound>) {
        let deadline = self.options.close;
        match ending {
            Ending::Gone | Ending::Broken(_) => {
                let mut unwritten = match ending {
                    Ending::Broken(ids) => ids,
                    _ => Vec::new(),
                };
                // A write still out fails quickly once the peer is gone.
                if let Some(writing) = writing {
                    if let Some(written) = staged(deadline, writing).await {
                        unwritten.extend(self.landed(written, flight).unwrap_or_default());
                    }
                }
                unwritten.extend(self.unsent.drop_assignments());
                self.close_session(unwritten).await;
            }
            Ending::Engine | Ending::Protocol(_) | Ending::Slow | Ending::Stopping => {
                let unwritten = match ending {
                    Ending::Slow => {
                        let ids = self.unsent.drop_all();
                        self.unsent.push(Outbound::Bye(ByeReason::Slow));
                        ids
                    }
                    Ending::Protocol(detail) => {
                        self.unsent.fault(FaultKind::Malformed, detail);
                        self.unsent.push(Outbound::Bye(ByeReason::Protocol));
                        Vec::new()
                    }
                    Ending::Stopping => {
                        let ids = self.unsent.drop_assignments();
                        self.unsent.push(Outbound::Bye(ByeReason::Draining));
                        ids
                    }
                    _ => Vec::new(),
                };
                // Stop applying what the peer sends; the read task lets go of
                // the session and starts dropping what arrives.
                let _ = self.closing.send(true);
                let mut writing = writing;
                let mut flight = flight;
                staged(deadline, async {
                    if let Some(read_end) = self.read_end.take() {
                        let _ = read_end.await;
                    }
                    self.close_session(unwritten).await;
                    loop {
                        if let Some(future) = writing.take() {
                            if self.landed(future.await, flight.take()).is_some() {
                                return;
                            }
                        }
                        match self.start() {
                            Some((future, meta)) => {
                                writing = Some(future);
                                flight = Some(meta);
                            }
                            None => break,
                        }
                    }
                    if let Some(write) = self.write.as_mut() {
                        if write.shutdown().await.is_err() {
                            return;
                        }
                    }
                    if let Some(task) = self.read_task.as_mut() {
                        let _ = task.await;
                    }
                })
                .await;
            }
        }
        if let Some(task) = self.read_task.take() {
            task.abort();
        }
        // Held until the engine was told about the close, so it did not
        // close the connection on its own first.
        drop(outbound);
        self.close_session(Vec::new()).await;
    }
}
