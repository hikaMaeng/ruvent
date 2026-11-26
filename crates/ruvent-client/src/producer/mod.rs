//! The producer (2.3, 2.9): one production connection per server, a selector
//! that picks the server for each registration, and an unacknowledged buffer
//! per connection.
//!
//! * [`Producer::send`] picks a server, numbers the registration on that
//!   server's line, keeps it in the line's buffer and hands it to the link.
//!   It returns once handed over; [`Producer::send_committed`] also waits for
//!   the cumulative `Committed`. It **waits** — never refuses — while the
//!   chosen line's buffer is at its byte bound (S8), while every candidate
//!   while no server is connected. The server never pauses a producer (plan 9.15).
//! * A registration once numbered goes only to that server again: after a
//!   reconnection everything above `Welcome.resume` is sent again under the
//!   same numbers, or numbered again from `resume + 1` when the server lost
//!   the record ([`crate::buffer`]). Only `Bye{draining}` (after the final
//!   `Committed`) and the server leaving the list move the rest elsewhere.
//! * The producer id is new at every start unless [`ProducerConfig::id`]
//!   keeps one (a durable outbox). `Bye{superseded}`, a reconnection that
//!   finds the id superseded, and a refused token, version or grant end the
//!   producer: every waiting and later send fails with [`ProducerError::Refused`].
//!
//! State (2.10, SDK):
//!
//! | state | key | scope | lifetime | bound | cleared by |
//! | --- | --- | --- | --- | --- | --- |
//! | producer id, instance nonce, next number | (server, production) | process (a kept id: the caller's outbox) | start ~ drop | servers × 1 | SDK |
//! | unacknowledged buffer | (server, seq) | production line | sent ~ `Committed` | [`ProducerConfig::unacked_bytes`] per line (`send` waits) | SDK (acknowledgement) |
//! | server list and lines | server address | process | while the list names it | servers | SDK (list changes) |
//! | selection state | server → connected, unacked bytes, draining | process | the line's | servers | SDK |
//! | reconnection back-off | server | line | disconnection ~ welcome | servers | SDK ([`crate::line`]) |
//! | retry budget | the producer | process | the producer's | one counter | SDK ([`RetryBudget`]) |

pub mod budget;
pub mod selector;

use crate::buffer::{Entry, Unacked};
use crate::line::backoff::Reconnect;
use crate::line::{Event, Line, Spec};
use crate::link::Connector;
use crate::servers::{Server, ServerList};
pub use budget::RetryBudget;
use parking_lot::Mutex;
use ruvent::{ByeReason, Call, Channel, Envelope, EventId, Hello, Link, Outbound, ProducerId, WorkKey};
pub use selector::{Candidate, Choice, LeastUnacked, Selector};
use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

/// Why a send failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProducerError {
    /// A server ended this producer (2.9): `superseded`, `version`, `token`,
    /// `forbidden`. It does not reconnect; nothing more is sent.
    Refused(ByeReason),
    /// [`Producer::retry`] past the retry budget (S9).
    BudgetSpent,
    /// The producer was shut down.
    Closed,
}

impl fmt::Display for ProducerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProducerError::Refused(reason) => write!(f, "a server ended the producer: {reason}"),
            ProducerError::BudgetSpent => f.write_str("the retry budget is spent"),
            ProducerError::Closed => f.write_str("the producer is closed"),
        }
    }
}

impl std::error::Error for ProducerError {}

/// Every value is a field with its default (chapter 8 is configuration).
#[derive(Clone)]
pub struct ProducerConfig {
    /// The credential an in-process connection runs under (over TCP the token decides).
    pub credential: String,
    /// B3: keep this producer id (a durable outbox). `None`: new at every start.
    pub id: Option<ProducerId>,
    /// The instance nonce. `None`: new at every start.
    pub nonce: Option<[u8; 16]>,
    /// B4: ask the server to keep this producer's entry this long after its last connection.
    pub linger: Option<Duration>,
    /// `false`: no `Committed` (a volatile broadcast-only producer); nothing is buffered.
    pub acks: bool,
    /// S8: unacknowledged bytes per line before `send` waits.
    pub unacked_bytes: u64,
    /// S20.
    pub reconnect: Reconnect,
    /// B19.
    pub selector: Arc<dyn Selector>,
    /// S9: retries per ordinary send, the starting balance and the most kept.
    pub retry_ratio: f64,
    pub retry_floor: u64,
    pub retry_ceiling: u64,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        ProducerConfig {
            credential: String::new(),
            id: None,
            nonce: None,
            linger: None,
            acks: true,
            unacked_bytes: 64 << 20,
            reconnect: Reconnect::default(),
            selector: Arc::new(LeastUnacked::default()),
            retry_ratio: 0.1,
            retry_floor: 10,
            retry_ceiling: 1000,
        }
    }
}

/// Where a send went.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sent {
    pub server: Server,
    pub id: EventId,
}

/// The servers a send may go to besides the usual constraints (a delete goes
/// to the one server that holds the event).
pub(crate) type Gate = watch::Receiver<BTreeSet<Server>>;

/// One numbered production call, kept until acknowledged.
#[derive(Clone)]
enum Op {
    Register(Envelope),
    Delete { id: EventId, result: Channel, key: Option<WorkKey> },
}

impl Op {
    fn call(&self, seq: u64) -> Call {
        match self {
            Op::Register(event) => Call::Register { seq: Some(seq), event: event.clone() },
            Op::Delete { id, result, key } => Call::Delete { seq, id: *id, result: *result, key: *key },
        }
    }

    fn channel(&self) -> Option<Channel> {
        match self {
            Op::Register(event) => Some(event.channel),
            Op::Delete { .. } => None,
        }
    }

    /// The type a pause holds: a registration is its own root (2.6).
    fn event_type(&self) -> Option<u16> {
        match self {
            Op::Register(event) => Some(event.event_type),
            Op::Delete { .. } => None,
        }
    }

    fn bytes(&self) -> u64 {
        match self {
            Op::Register(event) => event.payload.len() as u64 + 64,
            Op::Delete { .. } => 64,
        }
    }
}

struct Pending {
    op: Op,
    gate: Option<Gate>,
    /// Told when the operation is committed; dropped when it never will be.
    done: Option<oneshot::Sender<()>>,
}

/// The link of a line, held while a numbered call is handed to it so numbers
/// leave in order (resends after a reconnection included).
#[derive(Default)]
struct Io {
    link: Option<Arc<dyn Link>>,
    epoch: u64,
}

struct LineState {
    _line: Line,
    /// The epoch of the live connection.
    connected: Option<u64>,
    draining: bool,
    buffer: Unacked<Pending>,
    io: Arc<tokio::sync::Mutex<Io>>,
}

#[derive(Default)]
struct State {
    lines: HashMap<Server, LineState>,
    refused: Option<ByeReason>,
    closed: bool,
    restarts: u64,
}

struct Shared {
    state: Mutex<State>,
    /// Bumped at every change a waiting send may care about.
    version: watch::Sender<u64>,
    config: ProducerConfig,
    budget: RetryBudget,
}

impl Shared {
    fn bump(&self) {
        self.version.send_modify(|version| *version = version.wrapping_add(1));
    }
}

/// A producer over every server of a list. Cheap to clone.
#[derive(Clone)]
pub struct Producer {
    shared: Arc<Shared>,
    _task: Arc<Task>,
}

struct Task(JoinHandle<()>);

impl Drop for Task {
    fn drop(&mut self) {
        self.0.abort();
    }
}

enum Pick {
    Send(Server, Arc<tokio::sync::Mutex<Io>>),
    Wait,
}

impl Producer {
    /// Starts connecting to every server of `servers`. Returns at once; sends
    /// wait until a server is connected.
    pub fn start(connector: Arc<dyn Connector>, servers: &dyn ServerList, config: ProducerConfig) -> Producer {
        let budget = RetryBudget::new(config.retry_ratio, config.retry_floor, config.retry_ceiling);
        let shared = Arc::new(Shared { state: Mutex::default(), version: watch::channel(0).0, config, budget });
        let mut hello = Hello::producer(&shared.config.credential);
        if let Some(id) = shared.config.id {
            hello.producer = Some(id);
        }
        if let Some(nonce) = shared.config.nonce {
            hello.nonce = nonce;
        }
        hello.linger_ms = shared.config.linger.map(|linger| u64::try_from(linger.as_millis()).unwrap_or(u64::MAX));
        hello.acks = shared.config.acks;
        let list = servers.watch();
        let task = tokio::spawn(run(shared.clone(), connector, hello, list));
        Producer { shared, _task: Arc::new(Task(task)) }
    }

    /// Registers `event` on a server the selector picks. Returns once it is
    /// numbered and handed over (not yet committed).
    pub async fn send(&self, event: Envelope) -> Result<Sent, ProducerError> {
        self.shared.budget.earn();
        let (sent, _) = self.place(Op::Register(prepare(event)), None, None, false).await?;
        Ok(sent)
    }

    /// [`Producer::send`], then waits for the cumulative `Committed`.
    pub async fn send_committed(&self, event: Envelope) -> Result<Sent, ProducerError> {
        self.shared.budget.earn();
        let (sent, done) = self.place(Op::Register(prepare(event)), None, None, true).await?;
        self.wait(done).await?;
        Ok(sent)
    }

    /// Sends the same work again (2.9): the same logical id, within the
    /// retry budget (S9).
    pub async fn retry(&self, event: Envelope) -> Result<Sent, ProducerError> {
        if !self.shared.budget.spend() {
            return Err(ProducerError::BudgetSpent);
        }
        let (sent, _) = self.place(Op::Register(prepare(event)), None, None, false).await?;
        Ok(sent)
    }

    /// Deletes a waiting event on `server` (an explicit cancel, 7장); the
    /// outcome is a dispatch event of type 0 on `result` whose payload names
    /// the deleted id ([`ruvent::delete_outcome`]).
    pub async fn delete(&self, server: &Server, id: EventId, result: Channel, key: Option<WorkKey>) -> Result<(), ProducerError> {
        let only: BTreeSet<Server> = [server.clone()].into();
        let (_, gate) = watch::channel(only);
        self.place(Op::Delete { id, result, key }, Some(gate), None, false).await.map(|_| ())
    }

    /// Waits until every line's buffer is empty: everything sent so far is committed.
    pub async fn flush(&self) -> Result<(), ProducerError> {
        let mut seen = self.shared.version.subscribe();
        loop {
            seen.borrow_and_update();
            {
                let state = self.shared.state.lock();
                if let Some(reason) = state.refused {
                    return Err(ProducerError::Refused(reason));
                }
                if state.lines.values().all(|line| line.buffer.is_empty()) {
                    return Ok(());
                }
            }
            if seen.changed().await.is_err() {
                return Err(ProducerError::Closed);
            }
        }
    }

    /// Unacknowledged bytes per server now.
    pub fn unacked(&self) -> Vec<(Server, u64)> {
        let state = self.shared.state.lock();
        let mut list: Vec<(Server, u64)> = state.lines.iter().map(|(server, line)| (server.clone(), line.buffer.bytes())).collect();
        list.sort();
        list
    }

    /// Restarts noticed so far: a `Welcome` with another server id or generation.
    pub fn restarts(&self) -> u64 {
        self.shared.state.lock().restarts
    }

    /// Why the producer ended, if a server ended it.
    pub fn refused(&self) -> Option<ByeReason> {
        self.shared.state.lock().refused
    }

    /// The retry budget of [`Producer::retry`].
    pub fn budget(&self) -> &RetryBudget {
        &self.shared.budget
    }

    /// A send from inside a handler: `blocked` is set while it waits — a full
    /// unacknowledged buffer, no server (B23: a run held up by flow control is
    /// not the event's failure).
    pub(crate) async fn send_marked(&self, event: Envelope, blocked: &AtomicBool) -> Result<Sent, ProducerError> {
        self.shared.budget.earn();
        self.place(Op::Register(prepare(event)), None, Some(blocked), false).await.map(|(sent, _)| sent)
    }

    async fn wait(&self, done: Option<oneshot::Receiver<()>>) -> Result<(), ProducerError> {
        let Some(done) = done else { return Ok(()) };
        if done.await.is_ok() {
            return Ok(());
        }
        Err(self.shared.state.lock().refused.map_or(ProducerError::Closed, ProducerError::Refused))
    }

    async fn place(&self, op: Op, gate: Option<Gate>, blocked: Option<&AtomicBool>, committed: bool) -> Result<(Sent, Option<oneshot::Receiver<()>>), ProducerError> {
        let (done, wait) = if committed {
            let (done, wait) = oneshot::channel();
            (Some(done), Some(wait))
        } else {
            (None, None)
        };
        let id = match &op {
            Op::Register(event) => event.id,
            Op::Delete { id, .. } => *id,
        };
        let server = place(&self.shared, Pending { op, gate, done }, blocked).await?;
        Ok((Sent { server, id }, wait))
    }
}

/// A claimable event with a nil id gets a UUIDv7 here, before it is numbered,
/// so a resend carries the same id.
fn prepare(mut event: Envelope) -> Envelope {
    if event.kind == ruvent::Kind::Claimable && event.id.is_nil() {
        event.id = EventId::new_v7();
    }
    event
}

/// Numbers `pending` on a server and hands it over. Waits as the module says.
async fn place(shared: &Arc<Shared>, pending: Pending, blocked: Option<&AtomicBool>) -> Result<Server, ProducerError> {
    let mut seen = shared.version.subscribe();
    let mut gate = pending.gate.clone();
    loop {
        seen.borrow_and_update();
        let pick = pick(shared, &pending, gate.as_mut())?;
        let (server, io) = match pick {
            Pick::Send(server, io) => (server, io),
            Pick::Wait => {
                if let Some(blocked) = blocked {
                    blocked.store(true, Ordering::Relaxed);
                }
                let changed = match gate.as_mut() {
                    Some(gate) => tokio::select! {
                        changed = seen.changed() => changed.is_ok(),
                        changed = gate.changed() => changed.is_ok(),
                    },
                    None => seen.changed().await.is_ok(),
                };
                if let Some(blocked) = blocked {
                    blocked.store(false, Ordering::Relaxed);
                }
                if !changed {
                    return Err(ProducerError::Closed);
                }
                continue;
            }
        };
        let io = io.lock().await;
        let call = {
            let mut state = shared.state.lock();
            if let Some(reason) = state.refused {
                return Err(ProducerError::Refused(reason));
            }
            let limit = shared.config.unacked_bytes;
            let acks = shared.config.acks;
            let Some(line) = state.lines.get_mut(&server) else { continue };
            let open = line.connected == Some(io.epoch) && !line.draining;
            let full = !line.buffer.is_empty() && line.buffer.bytes().saturating_add(pending.op.bytes()) > limit;
            if !open || full {
                continue;
            }
            let op = pending.op.clone();
            let bytes = op.bytes();
            let seq = line.buffer.push(pending, bytes);
            if !acks {
                // Nothing will acknowledge it: keep the number, not the call.
                line.buffer.ack(seq);
            }
            op.call(seq)
        };
        if let Some(link) = io.link.clone() {
            // A failed hand-over leaves it in the buffer: sent again at the next welcome.
            let _ = link.send(call).await;
        }
        return Ok(server);
    }
}

fn pick(shared: &Shared, pending: &Pending, gate: Option<&mut Gate>) -> Result<Pick, ProducerError> {
    let state = shared.state.lock();
    if let Some(reason) = state.refused {
        return Err(ProducerError::Refused(reason));
    }
    if state.closed {
        return Err(ProducerError::Closed);
    }
    let allowed: Option<BTreeSet<Server>> = gate.map(|gate| gate.borrow_and_update().clone());
    let (channel, event_type) = (pending.op.channel(), pending.op.event_type());
    let mut candidates: Vec<(Candidate, Arc<tokio::sync::Mutex<Io>>, bool)> = state
        .lines
        .iter()
        .filter(|(server, line)| line.connected.is_some() && !line.draining && allowed.as_ref().is_none_or(|allowed| allowed.contains(*server)))
        .map(|(server, line)| {
            let full = !line.buffer.is_empty() && line.buffer.bytes().saturating_add(pending.op.bytes()) > shared.config.unacked_bytes;
            (Candidate { server: server.clone(), unacked_bytes: line.buffer.bytes() }, line.io.clone(), full)
        })
        .collect();
    candidates.sort_by(|a, b| a.0.server.cmp(&b.0.server));
    if candidates.is_empty() {
        return Ok(Pick::Wait);
    }
    let listed: Vec<Candidate> = candidates.iter().map(|(candidate, _, _)| candidate.clone()).collect();
    let Some(index) = shared.config.selector.select(&Choice { channel, event_type, candidates: &listed }) else {
        return Ok(Pick::Wait);
    };
    let Some((candidate, io, full)) = candidates.into_iter().nth(index) else {
        return Ok(Pick::Wait);
    };
    if full {
        return Ok(Pick::Wait);
    }
    Ok(Pick::Send(candidate.server, io))
}

/// Follows the server list and the lines' events.
async fn run(shared: Arc<Shared>, connector: Arc<dyn Connector>, hello: Hello, mut list: watch::Receiver<BTreeSet<Server>>) {
    let (events, mut incoming) = mpsc::channel::<Event>(1024);
    let mut listed = true;
    // The list's owner may drop it: the servers named last stay.
    let mut watching = true;
    loop {
        if listed {
            let wanted = list.borrow_and_update().clone();
            let moved = follow_list(&shared, &connector, &hello, &events, &wanted);
            requeue(&shared, moved);
            listed = false;
        }
        tokio::select! {
            changed = list.changed(), if watching => match changed {
                Ok(()) => listed = true,
                Err(_) => watching = false,
            },
            event = incoming.recv() => {
                let Some(event) = event else { return };
                handle(&shared, event).await;
            }
        }
    }
}

/// Opens lines for new servers and stops lines of removed ones. Returns what
/// the removed lines still held.
fn follow_list(shared: &Arc<Shared>, connector: &Arc<dyn Connector>, hello: &Hello, events: &mpsc::Sender<Event>, wanted: &BTreeSet<Server>) -> Vec<Pending> {
    let mut state = shared.state.lock();
    let gone: Vec<Server> = state.lines.keys().filter(|server| !wanted.contains(*server)).cloned().collect();
    let mut moved = Vec::new();
    for server in gone {
        if let Some(mut line) = state.lines.remove(&server) {
            moved.extend(line.buffer.take_all().into_iter().map(|entry| entry.item));
        }
    }
    for server in wanted {
        if state.lines.contains_key(server) {
            continue;
        }
        let spec = Spec { server: server.clone(), hello: hello.clone(), connector: connector.clone(), reconnect: shared.config.reconnect };
        let line = Line::spawn(spec, events.clone());
        state.lines.insert(server.clone(), LineState { _line: line, connected: None, draining: false, buffer: Unacked::new(), io: Arc::default() });
    }
    drop(state);
    shared.bump();
    moved
}

/// Sends what a draining or removed server still held through the selector
/// again, in order (2.9 `draining`: after the final `Committed` the rest is
/// certain not to have been applied there).
fn requeue(shared: &Arc<Shared>, moved: Vec<Pending>) {
    if moved.is_empty() {
        return;
    }
    let shared = shared.clone();
    tokio::spawn(async move {
        for pending in moved {
            if place(&shared, pending, None).await.is_err() {
                return;
            }
        }
    });
}

fn told(entries: Vec<Entry<Pending>>) {
    for entry in entries {
        if let Some(done) = entry.item.done {
            let _ = done.send(());
        }
    }
}

async fn handle(shared: &Arc<Shared>, event: Event) {
    match event {
        Event::Up { server, epoch, link, welcome, restarted, .. } => {
            let Some(io) = shared.state.lock().lines.get(&server).map(|line| line.io.clone()) else { return };
            let mut io = io.lock().await;
            let resend: Vec<Call> = {
                let mut state = shared.state.lock();
                if restarted {
                    state.restarts += 1;
                }
                let Some(line) = state.lines.get_mut(&server) else { return };
                let (dropped, _) = line.buffer.resume(welcome.resume);
                told(dropped);
                line.connected = Some(epoch);
                line.draining = false;
                line.buffer.iter().map(|entry| entry.item.op.call(entry.seq)).collect()
            };
            io.link = Some(link.clone());
            io.epoch = epoch;
            for call in resend {
                if link.send(call).await.is_err() {
                    break;
                }
            }
            drop(io);
            shared.bump();
        }
        Event::Message { server, epoch, message, .. } => {
            let mut state = shared.state.lock();
            let Some(line) = state.lines.get_mut(&server) else { return };
            if line.connected != Some(epoch) {
                return;
            }
            match *message {
                Outbound::Committed { seq } => told(line.buffer.ack(seq)),
                _ => return,
            }
            drop(state);
            shared.bump();
        }
        Event::Down { server, epoch, bye, .. } => {
            let moved = {
                let mut state = shared.state.lock();
                let Some(line) = state.lines.get_mut(&server) else { return };
                if line.connected != Some(epoch) {
                    return;
                }
                line.connected = None;
                if let Ok(mut io) = line.io.try_lock() {
                    io.link = None;
                }
                if bye == Some(ByeReason::Draining) {
                    line.draining = true;
                    line.buffer.take_all().into_iter().map(|entry| entry.item).collect()
                } else {
                    Vec::new()
                }
            };
            shared.bump();
            requeue(shared, moved);
        }
        Event::Fatal { reason, .. } => {
            let mut state = shared.state.lock();
            state.refused = Some(reason);
            for line in state.lines.values_mut() {
                // The receivers of `done` see the end: the producer is refused.
                drop(line.buffer.take_all());
            }
            drop(state);
            shared.bump();
        }
    }
}
