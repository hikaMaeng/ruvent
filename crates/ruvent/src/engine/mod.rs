//! The event server's engine: one task owns the ledger; connections talk to
//! it with [`Call`]s and read [`Outbound`]s.
//!
//! ```text
//! connections ──Op──► inbox ──recv_many──► State (single writer) ──Effects──┐
//!                                                                            ├─► send buffers (try_send; full = disconnect)
//!                                          journal writer ◄── one flight ────┤
//!                                          payload reads ◄── cold sends, reads ahead (pinned segments)
//!                                          checkpoint writer ◄── slices of the walk
//!                                          chores ◄── relocations, segment removal
//!                                          snapshot collector ◄── try_send ──┘
//!                          timing wheel ──(earliest due only)──► State
//! ```
//!
//! The engine never waits on a socket or a disk. It waits on the inbox, the
//! write in flight, the answers of the reads, the checkpoint writer and the
//! chores, and the earliest lease end or visibility time in the ledger's wheel
//! — one one-shot sleep to that time, none when the wheel is empty.
//!
//! A send gated on a commit number is held until that batch is on disk; a
//! send whose payload is only on disk is held until the read path brought it
//! back; anything later for the same connection queues behind either.
//!
//! The clock is the accumulated uptime (ms): it starts where the journal's
//! last batch left it and does not count the time the server was down (0.3).

pub mod config;
pub mod ops;
pub mod state;
mod storage;

use crate::envelope::EventId as Unwritten;
use crate::journal::{Batch, Journal, Record, Recovered, Spool, Written};
use crate::snapshot::{Collector, SnapshotStats};
use bytes::Bytes;
use config::EngineConfig;
use ops::{Call, Hello, Outbound, Pressure, SessionId};
use state::{Effects, IdSource, State, Warm, V7};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fmt;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use storage::{Answer, Chore, Done, Gauge, Reads};
use tokio::sync::{mpsc, oneshot, watch};

/// Why the engine stopped, when it was not asked to.
#[derive(Clone, Debug)]
pub struct EngineError(pub Arc<str>);

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for EngineError {}

/// The engine is gone; nothing was sent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Closed;

impl fmt::Display for Closed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the engine is closed")
    }
}

impl std::error::Error for Closed {}

pub(crate) enum Op {
    Open { session: SessionId, hello: Hello, out: mpsc::Sender<Outbound> },
    Close { session: SessionId, unwritten: Vec<Unwritten> },
    Call { session: SessionId, call: Call },
    Drain,
    Shutdown { done: oneshot::Sender<()> },
}

type Stop = Option<Result<(), EngineError>>;

/// The accumulated uptime: `base` from the journal plus time since start.
#[derive(Clone, Copy)]
struct Clock {
    base: u64,
    epoch: tokio::time::Instant,
}

impl Clock {
    fn now(&self) -> u64 {
        self.base.saturating_add(u64::try_from(self.epoch.elapsed().as_millis()).unwrap_or(u64::MAX))
    }
}

/// A handle to a running engine. Cheap to clone.
#[derive(Clone)]
pub struct Engine {
    ops: mpsc::Sender<Op>,
    next_session: Arc<AtomicU64>,
    stopped: watch::Receiver<Stop>,
    outbound: usize,
    snapshot: Option<Arc<SnapshotStats>>,
    clock: Clock,
    generation: u32,
    server_id: [u8; 16],
    gauge: Arc<Gauge>,
    spool: Option<Spool>,
}

/// One connection to the engine, below the client API: send calls, read
/// [`Outbound`] in order. Dropping it closes the connection with every
/// assignment counted as written.
pub struct Session {
    pub id: SessionId,
    ops: mpsc::Sender<Op>,
    closed: bool,
}

impl Session {
    /// Hands a call to the engine. Waits only while the engine's inbox is full.
    pub async fn call(&self, call: Call) -> Result<(), Closed> {
        self.ops.send(Op::Call { session: self.id, call }).await.map_err(|_| Closed)
    }

    /// Closes the connection. `unwritten` are the event ids of assignments the
    /// connection owner had not finished writing to the socket: they go back
    /// to waiting at once (2.4). Assignments the engine still held for the
    /// connection (waiting for the disk or a payload read) count as unwritten
    /// on their own.
    pub async fn close(mut self, unwritten: Vec<Unwritten>) {
        self.closed = true;
        let _ = self.ops.send(Op::Close { session: self.id, unwritten }).await;
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        if !self.closed {
            // Best effort. If the inbox is full the engine still finds out at
            // the next thing it hands this connection, whose buffer is gone.
            let _ = self.ops.try_send(Op::Close { session: self.id, unwritten: Vec::new() });
        }
    }
}

enum Job {
    Append(Batch, oneshot::Sender<io::Result<Written>>),
}

impl Engine {
    /// Recovers from `recovered`, makes the new generation durable, and starts.
    /// The first answer goes out only after that write.
    pub async fn start(config: EngineConfig, journal: impl Journal, recovered: Recovered) -> io::Result<Engine> {
        Self::start_with(config, journal, recovered, Box::new(V7)).await
    }

    /// An engine with no disk: every channel is volatile and every operation
    /// commits when applied.
    pub async fn volatile(config: EngineConfig) -> Engine {
        match Self::start(config, crate::journal::Volatile, Recovered::default()).await {
            Ok(engine) => engine,
            Err(_) => unreachable!("the volatile journal never fails"),
        }
    }

    /// [`Engine::start`] with a chosen source of fresh ids. For tests that need
    /// to force an id collision; production uses UUIDv7.
    pub async fn start_with(config: EngineConfig, mut journal: impl Journal, recovered: Recovered, ids: Box<dyn IdSource>) -> io::Result<Engine> {
        let journaled = journal.durable();
        let parts = journal.parts();
        // Recovery reads the disk as it goes: off the runtime's threads.
        let state = State::new(&config, ids);
        let (mut state, recovery) = tokio::task::spawn_blocking(move || {
            let mut state = state;
            let recovery = state.recover_from(recovered, journaled);
            (state, recovery)
        })
        .await
        .map_err(io::Error::other)?;
        let (startup, end) = recovery.map_err(|reason| io::Error::new(io::ErrorKind::InvalidData, reason))?;
        let first_commit = end.commit + 1;
        let written = journal.append(Batch { uptime: end.uptime, commit: first_commit, first: end.serial, records: startup }).await?;
        let landed_serial = state.serial();
        state.begin(end.uptime, first_commit, first_commit + 1);
        state.wrote(written, landed_serial);
        state.landed(first_commit);
        let effects = state.take();

        let (jobs, mut job_rx) = mpsc::channel::<Job>(1);
        let writer = tokio::spawn(async move {
            while let Some(job) = job_rx.recv().await {
                match job {
                    Job::Append(batch, done) => {
                        let _ = done.send(journal.append(batch).await);
                    }
                }
            }
        });

        let clock = Clock { base: end.uptime, epoch: tokio::time::Instant::now() };
        let collector = config.snapshot.clone().map(Collector::spawn);
        let snapshot = collector.as_ref().map(|collector| collector.stats.clone());
        let (ops, inbox) = mpsc::channel(config.inbox.max(1));
        let (stop_tx, stopped) = watch::channel(None);
        let generation = state.generation();
        let server_id = state.server_id();
        let gauge = Arc::new(Gauge::default());
        gauge.set(state.pressure());
        let (reads, read_rx) = Reads::new(parts.bodies, config.storage.max_reads);
        let checkpoints = parts.checkpoints.filter(|_| journaled).map(storage::spawn_checkpoints);
        let chores = parts.chores.filter(|_| journaled).map(storage::spawn_chores);
        let spool = parts.spool.filter(|_| journaled);
        let (checkpoint_to, checkpoint_rx, checkpoint_task) = match checkpoints {
            Some((to, rx, task)) => (Some(to), Some(rx), Some(task)),
            None => (None, None, None),
        };
        let (chore_to, chore_rx, chore_task) = match chores {
            Some((to, rx, task)) => (Some(to), Some(rx), Some(task)),
            None => (None, None, None),
        };
        let mut core = Core {
            state,
            inbox,
            conns: HashMap::new(),
            jobs: Some(jobs),
            writer: Some(writer),
            flight: None,
            next_flight: first_commit + 1,
            landed: first_commit,
            landed_serial,
            pending: Vec::new(),
            pending_first: 0,
            held: HashMap::new(),
            gated: BTreeMap::new(),
            journaled,
            config: config.clone(),
            collector,
            clock,
            stop: stop_tx,
            shutdown: Vec::new(),
            reads,
            read_rx,
            checkpoint_to,
            checkpoint_rx,
            checkpoint_task,
            walk: Walk::Idle,
            chore_to,
            chore_rx,
            chore_task,
            chore_busy: false,
            chore_queue: VecDeque::new(),
            deferred: BTreeSet::new(),
            spool: spool.clone(),
            gauge: gauge.clone(),
            failure: None,
        };
        core.route(effects);
        tokio::spawn(core.run());
        Ok(Engine { ops, next_session: Arc::new(AtomicU64::new(1)), stopped, outbound: config.outbound.max(1), snapshot, clock, generation, server_id, gauge, spool })
    }

    /// Opens a connection. Its first message is `Welcome`, or `Bye`.
    pub async fn open(&self, hello: Hello) -> Result<(Session, mpsc::Receiver<Outbound>), Closed> {
        let session = self.next_session.fetch_add(1, Ordering::Relaxed);
        let (out, rx) = mpsc::channel(self.outbound);
        self.ops.send(Op::Open { session, hello, out }).await.map_err(|_| Closed)?;
        Ok((Session { id: session, ops: self.ops.clone(), closed: false }, rx))
    }

    /// Starts draining without an operator connection: what `Call::Drain`
    /// does, asked by the process that owns the engine (a server shutting
    /// down, 2.8). New production connections are sent away; every open one
    /// gets its final `Committed`, then `Bye{draining}`.
    pub async fn drain(&self) -> Result<(), Closed> {
        self.ops.send(Op::Drain).await.map_err(|_| Closed)
    }

    /// Stops accepting calls, writes what is pending and a checkpoint of the
    /// stopped ledger (2.8, B22), and waits until it is on disk.
    pub async fn shutdown(&self) -> Result<(), EngineError> {
        let (done, wait) = oneshot::channel();
        if self.ops.send(Op::Shutdown { done }).await.is_ok() {
            let _ = wait.await;
        }
        self.stopped().await
    }

    /// The operational snapshot's counters, if one is configured.
    pub fn snapshot_stats(&self) -> Option<Arc<SnapshotStats>> {
        self.snapshot.clone()
    }

    /// The accumulated uptime now, in ms: the clock of `Assignment::until`.
    pub fn uptime(&self) -> u64 {
        self.clock.now()
    }

    /// The generation this run started in.
    pub fn generation(&self) -> u32 {
        self.generation
    }

    pub fn server_id(&self) -> [u8; 16] {
        self.server_id
    }

    /// Memory and disk after the last ledger batch (2.6): the high watermark
    /// and limits a connection owner compares against.
    pub fn pressure(&self) -> Pressure {
        self.gauge.get()
    }

    /// Whether a work connection should receive a follow-up's body of `len`
    /// bytes to disk instead of memory (2.6, item 4): once managed memory is
    /// at the high watermark.
    ///
    /// When it says yes, read the body through [`Engine::spool`] and hand the
    /// [`crate::journal::Spooled`] with the completion (`Call::Complete::spooled`).
    /// Always `false` without a spool (no segment log).
    pub fn body_to_disk(&self, len: u64) -> bool {
        if self.spool.is_none() {
            return false;
        }
        let pressure = self.gauge.get();
        pressure.memory_used.saturating_add(len) >= pressure.memory_high
    }

    /// Where streamed follow-up bodies are received, if the journal has one.
    pub fn spool(&self) -> Option<Spool> {
        self.spool.clone()
    }

    /// Resolves when the engine stops: `Ok` after a shutdown, `Err` with the
    /// reason when it stopped on its own (a journal write or read failed).
    pub async fn stopped(&self) -> Result<(), EngineError> {
        let mut stopped = self.stopped.clone();
        let outcome = match stopped.wait_for(Option::is_some).await {
            Ok(state) => state.clone().unwrap_or(Ok(())),
            Err(_) => Err(EngineError("the engine task ended without saying why".into())),
        };
        outcome
    }
}

struct Flight {
    number: u64,
    /// The serial of its first record and how many it carries.
    first: u64,
    records: u64,
    done: oneshot::Receiver<io::Result<Written>>,
}

/// A send held for its connection: until a commit is on disk, until its
/// payload is read, and behind every earlier held send of the connection.
struct Held {
    gate: u64,
    read: Option<u64>,
    /// `None`: close the connection here.
    message: Option<Outbound>,
}

/// Why a payload is being read.
enum Purpose {
    Send(SessionId),
    Warm(Warm),
}

/// The checkpoint walk, as the engine drives it.
enum Walk {
    Idle,
    /// Parts handed over and not yet written; whether `Finish` was sent.
    Running {
        out: usize,
        finished: bool,
    },
    /// The stopped ledger's checkpoint, at shutdown.
    Last,
    /// The last checkpoint is durable.
    Done,
}

enum Wake {
    Landed(Option<io::Result<Written>>),
    Received(usize),
    Due,
    Read(Option<(u64, io::Result<Bytes>)>),
    Checkpoint(Option<Answer>),
    Chore(Option<Done>),
}

/// The outcome of the flight in progress; never resolves when there is none.
async fn wait(flight: &mut Option<Flight>) -> Option<io::Result<Written>> {
    match flight {
        Some(flight) => (&mut flight.done).await.ok(),
        None => std::future::pending().await,
    }
}

/// The next answer of a helper; never resolves when there is no helper.
async fn answer<T>(answers: &mut Option<mpsc::UnboundedReceiver<T>>) -> Option<T> {
    match answers {
        Some(answers) => answers.recv().await,
        None => std::future::pending().await,
    }
}

/// The earliest due time in the ledger; never resolves when there is none.
async fn due(after: Option<u64>) {
    match after {
        Some(millis) => tokio::time::sleep(Duration::from_millis(millis)).await,
        None => std::future::pending().await,
    }
}

/// One open connection's ends.
///
/// | key | session id |
/// | --- | --- |
/// | lifetime | from `Open` to `Close`, or until a send finds the buffer full or gone |
/// | bound | open connections |
struct Conn {
    out: mpsc::Sender<Outbound>,
}

/// The event ids of the assignments in held sends: never written to the socket.
fn unwritten(held: VecDeque<Held>) -> Vec<Unwritten> {
    held.into_iter()
        .filter_map(|held| match held.message {
            Some(Outbound::Assign(assignment)) | Some(Outbound::Claimed { assignment, .. }) => Some(assignment.event.id),
            _ => None,
        })
        .collect()
}

struct Core {
    state: State,
    inbox: mpsc::Receiver<Op>,
    conns: HashMap<SessionId, Conn>,
    /// The writer's inbox, and the writer. Both go before the engine reports
    /// that it stopped, so the journal (and its lock) is released by then.
    jobs: Option<mpsc::Sender<Job>>,
    writer: Option<tokio::task::JoinHandle<()>>,
    flight: Option<Flight>,
    /// The commit number the next flight takes; the batch the ledger fills.
    next_flight: u64,
    /// The last commit number on disk, and the serial after its last record.
    landed: u64,
    landed_serial: u64,
    /// Records not yet handed to the writer. They go in the next flight.
    pending: Vec<Record>,
    pending_first: u64,
    /// Sends held per connection.
    ///
    /// | key | session id |
    /// | --- | --- |
    /// | lifetime | a send is held ~ the last held one is handed over, or the connection closes |
    /// | bound | open connections × their held sends (gated: the flights in progress; read: the reads out) |
    held: HashMap<SessionId, VecDeque<Held>>,
    /// Connections with a send held for a commit, by that commit.
    gated: BTreeMap<u64, Vec<SessionId>>,
    journaled: bool,
    config: EngineConfig,
    collector: Option<Collector>,
    clock: Clock,
    stop: watch::Sender<Stop>,
    shutdown: Vec<oneshot::Sender<()>>,
    reads: Reads<Purpose>,
    read_rx: mpsc::UnboundedReceiver<(u64, io::Result<Bytes>)>,
    checkpoint_to: Option<mpsc::UnboundedSender<storage::Write>>,
    checkpoint_rx: Option<mpsc::UnboundedReceiver<Answer>>,
    checkpoint_task: Option<tokio::task::JoinHandle<()>>,
    walk: Walk,
    chore_to: Option<mpsc::UnboundedSender<Chore>>,
    chore_rx: Option<mpsc::UnboundedReceiver<Done>>,
    chore_task: Option<tokio::task::JoinHandle<()>>,
    chore_busy: bool,
    chore_queue: VecDeque<Chore>,
    /// Removable segments still pinned by a read, or with a streamed body on
    /// its way.
    ///
    /// | key | segment |
    /// | --- | --- |
    /// | lifetime | handed out by the ledger ~ removal sent |
    /// | bound | segments on disk |
    deferred: BTreeSet<u32>,
    spool: Option<Spool>,
    gauge: Arc<Gauge>,
    /// The reason to stop, found while routing.
    failure: Option<EngineError>,
}

impl Core {
    async fn run(mut self) {
        let mut batch = Vec::with_capacity(self.config.batch_max);
        let result = loop {
            if let Some(error) = self.failure.take() {
                break Err(error);
            }
            let accepting = self.shutdown.is_empty();
            if !accepting && self.flight.is_none() && self.pending.is_empty() {
                match self.walk {
                    Walk::Last => {}
                    Walk::Done => break Ok(()),
                    Walk::Idle | Walk::Running { .. } => {
                        if let Some(error) = self.last_checkpoint() {
                            break Err(error);
                        }
                        continue;
                    }
                }
            }
            let after = self.state.next_due().map(|at| at.saturating_sub(self.clock.now()));
            let event = {
                let flight = &mut self.flight;
                let inbox = &mut self.inbox;
                let reads = &mut self.read_rx;
                let checkpoints = &mut self.checkpoint_rx;
                let chores = &mut self.chore_rx;
                tokio::select! {
                    biased;
                    done = wait(flight) => Wake::Landed(done),
                    done = answer(checkpoints) => Wake::Checkpoint(done),
                    done = answer(chores) => Wake::Chore(done),
                    read = reads.recv() => Wake::Read(read),
                    received = inbox.recv_many(&mut batch, self.config.batch_max), if accepting => Wake::Received(received),
                    () = due(after), if accepting => Wake::Due,
                }
            };
            match event {
                Wake::Landed(done) => match (done, self.flight.take()) {
                    (Some(Ok(written)), Some(flight)) => self.landed(flight, written),
                    (Some(Err(error)), _) => break Err(EngineError(format!("journal write failed: {error}").into())),
                    _ => break Err(EngineError("the journal writer is gone".into())),
                },
                Wake::Received(0) => {
                    // Every handle and every connection is gone.
                    let (done, _) = oneshot::channel();
                    self.shutdown.push(done);
                }
                Wake::Received(_) => self.apply(&mut batch),
                Wake::Due => {
                    self.begin();
                    let effects = self.state.take();
                    self.route(effects);
                }
                Wake::Read(Some((id, result))) => self.read(id, result),
                Wake::Read(None) => break Err(EngineError("the read path is gone".into())),
                Wake::Checkpoint(Some(answer)) => self.checkpoint_answer(answer),
                Wake::Checkpoint(None) => break Err(EngineError("the checkpoint writer is gone".into())),
                Wake::Chore(Some(done)) => self.chore_done(done),
                Wake::Chore(None) => break Err(EngineError("the chores are gone".into())),
            }
            if accepting {
                self.tend_checkpoint();
            }
            if let Some(error) = self.launch() {
                break Err(error);
            }
            self.gauge.set(self.state.pressure());
        };
        // Held sends of an unfinished write are dropped with the connections.
        self.conns.clear();
        self.held.clear();
        // The writer ends once its inbox is gone and drops the journal with it.
        // A write still out (only after a fatal error) is not waited for.
        self.jobs = None;
        if let (Some(writer), None) = (self.writer.take(), &self.flight) {
            let _ = writer.await;
        }
        self.checkpoint_to = None;
        self.chore_to = None;
        for task in [self.checkpoint_task.take(), self.chore_task.take()].into_iter().flatten() {
            let _ = task.await;
        }
        // The collector writes its last unit once its sender is gone.
        if let Some(collector) = self.collector.take() {
            drop(collector.sender);
            let _ = collector.task.await;
        }
        for done in self.shutdown.drain(..) {
            let _ = done.send(());
        }
        let _ = self.stop.send(Some(result));
    }

    fn begin(&mut self) {
        self.state.begin(self.clock.now(), self.landed, self.next_flight);
    }

    fn apply(&mut self, batch: &mut Vec<Op>) {
        self.begin();
        for op in batch.drain(..) {
            match op {
                Op::Open { session, hello, out } => {
                    self.conns.insert(session, Conn { out });
                    self.state.open(session, hello);
                }
                Op::Close { session, mut unwritten } => {
                    unwritten.extend(self.forget(session));
                    self.state.close(session, &unwritten);
                }
                Op::Call { session, call } => self.state.call(session, call),
                Op::Drain => self.state.start_drain(),
                Op::Shutdown { done } => self.shutdown.push(done),
            }
        }
        let effects = self.state.take();
        self.route(effects);
    }

    /// Drops a connection's ends and held sends; returns the event ids of the
    /// assignments it never got.
    fn forget(&mut self, session: SessionId) -> Vec<Unwritten> {
        self.conns.remove(&session);
        self.held.remove(&session).map(unwritten).unwrap_or_default()
    }

    /// Hands one message to a connection without waiting. A full or closed
    /// buffer disconnects it: that is the connection's signal that it lost
    /// messages. Returns whether the connection is still there.
    fn hand(&mut self, session: SessionId, message: Option<Outbound>) -> bool {
        let Some(conn) = self.conns.get(&session) else { return true };
        match message {
            Some(message) => {
                if conn.out.try_send(message).is_err() {
                    self.conns.remove(&session);
                    return false;
                }
                true
            }
            None => {
                self.conns.remove(&session);
                true
            }
        }
    }

    /// Hands a connection its held sends that are ready, in order. Returns
    /// whether the connection is still there.
    fn release(&mut self, session: SessionId) -> bool {
        loop {
            let ready = self.held.get(&session).and_then(VecDeque::front).is_some_and(|front| front.gate <= self.landed && front.read.is_none());
            if !ready {
                break;
            }
            let Some(held) = self.held.get_mut(&session).and_then(VecDeque::pop_front) else { break };
            if !self.hand(session, held.message) {
                return false;
            }
        }
        if self.held.get(&session).is_some_and(VecDeque::is_empty) {
            self.held.remove(&session);
        }
        true
    }

    fn route(&mut self, effects: Effects) {
        let mut effects = effects;
        loop {
            let mut dropped = Vec::new();
            for send in effects.sends {
                let gate = send.gate.filter(|gate| *gate > self.landed).unwrap_or(0);
                let read = match send.read {
                    Some(at) => match self.reads.ask(Purpose::Send(send.session), at) {
                        Some(id) => Some(id),
                        None => {
                            self.failure = Some(EngineError("a payload is only on disk and the journal cannot read it back".into()));
                            None
                        }
                    },
                    None => None,
                };
                if gate == 0 && read.is_none() && !self.held.contains_key(&send.session) {
                    if !self.hand(send.session, send.message) {
                        dropped.push(send.session);
                    }
                    continue;
                }
                if gate > 0 {
                    self.gated.entry(gate).or_default().push(send.session);
                }
                self.held.entry(send.session).or_default().push_back(Held { gate, read, message: send.message });
            }
            if !effects.records.is_empty() {
                if self.pending.is_empty() {
                    self.pending_first = effects.first_serial;
                }
                self.pending.extend(effects.records);
            }
            if let Some(collector) = &self.collector {
                for event in effects.observed {
                    if collector.sender.try_send(event).is_err() {
                        collector.stats.dropped.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            for warm in effects.warm {
                if self.reads.ask(Purpose::Warm(warm), warm.at()).is_none() {
                    break;
                }
            }
            if !effects.relocate.is_empty() {
                if self.chore_to.is_some() {
                    self.chore(Chore::Relocate(effects.relocate));
                } else {
                    self.state.relocated(Written::default());
                }
            }
            if !effects.delete.is_empty() {
                self.deferred.extend(effects.delete);
                self.remove_segments();
            }
            if dropped.is_empty() {
                return;
            }
            // A connection that could not take a message is closed; what that
            // changes in the ledger is routed like any other batch.
            self.begin();
            for session in dropped {
                let unwritten = self.forget(session);
                self.state.close(session, &unwritten);
            }
            effects = self.state.take();
        }
    }

    /// Hands removable segments that nothing pins any more to the chores.
    fn remove_segments(&mut self) {
        let spool = self.spool.as_ref();
        let ready: Vec<u32> = self.deferred.iter().copied().filter(|segment| !self.reads.pinned(*segment) && spool.is_none_or(|spool| spool.idle(*segment))).collect();
        if ready.is_empty() || self.chore_to.is_none() {
            return;
        }
        for segment in &ready {
            self.deferred.remove(segment);
        }
        self.chore(Chore::Delete(ready));
    }

    fn chore(&mut self, chore: Chore) {
        self.chore_queue.push_back(chore);
        self.next_chore();
    }

    fn next_chore(&mut self) {
        if self.chore_busy {
            return;
        }
        let Some(chore) = self.chore_queue.pop_front() else { return };
        match &self.chore_to {
            Some(to) if to.send(chore).is_ok() => self.chore_busy = true,
            _ => self.failure = Some(EngineError("the chores are gone".into())),
        }
    }

    fn chore_done(&mut self, done: Done) {
        self.chore_busy = false;
        self.begin();
        match done {
            Done::Relocated(Ok(written)) => self.state.relocated(written),
            Done::Deleted(Ok(written)) => self.state.wrote(written, 0),
            Done::Relocated(Err(error)) | Done::Deleted(Err(error)) => {
                self.failure = Some(EngineError(format!("segment work failed: {error}").into()));
                return;
            }
        }
        let effects = self.state.take();
        self.route(effects);
        self.next_chore();
    }

    /// A payload read answered.
    fn read(&mut self, id: u64, result: io::Result<Bytes>) {
        let Some((purpose, at, unpinned)) = self.reads.answered(id) else { return };
        let payload = match result {
            Ok(payload) => payload,
            Err(error) => {
                self.failure = Some(EngineError(format!("reading a payload from segment {} failed: {error}", at.segment).into()));
                return;
            }
        };
        match purpose {
            Purpose::Send(session) => {
                if let Some(held) = self.held.get_mut(&session).and_then(|queue| queue.iter_mut().find(|held| held.read == Some(id))) {
                    held.read = None;
                    match &mut held.message {
                        Some(Outbound::Assign(assignment)) | Some(Outbound::Claimed { assignment, .. }) => assignment.event.payload = payload,
                        _ => {}
                    }
                }
                if !self.release(session) {
                    self.begin();
                    let unwritten = self.forget(session);
                    self.state.close(session, &unwritten);
                    let effects = self.state.take();
                    self.route(effects);
                }
            }
            Purpose::Warm(warm) => {
                self.begin();
                self.state.warmed(warm, payload);
                let effects = self.state.take();
                self.route(effects);
            }
        }
        if unpinned && self.deferred.contains(&at.segment) {
            self.remove_segments();
        }
    }

    fn launch(&mut self) -> Option<EngineError> {
        if self.flight.is_some() || self.pending.is_empty() {
            return None;
        }
        let records = std::mem::take(&mut self.pending);
        let (done_tx, done) = oneshot::channel();
        let number = self.next_flight;
        self.next_flight += 1;
        let first = self.pending_first;
        let count = records.len() as u64;
        let job = Job::Append(Batch { uptime: self.clock.now(), commit: number, first, records }, done_tx);
        // The writer holds at most the one job in flight, so a refusal means it is gone.
        if self.jobs.as_ref().is_none_or(|jobs| jobs.try_send(job).is_err()) {
            return Some(EngineError("the journal writer is gone".into()));
        }
        self.flight = Some(Flight { number, first, records: count, done });
        None
    }

    fn landed(&mut self, flight: Flight, written: Written) {
        self.landed = flight.number;
        self.landed_serial = flight.first + flight.records;
        let ready: Vec<SessionId> = {
            let later = self.gated.split_off(&(flight.number + 1));
            let due = std::mem::replace(&mut self.gated, later);
            due.into_values().flatten().collect()
        };
        let mut dropped = Vec::new();
        for session in ready {
            if !self.release(session) {
                dropped.push(session);
            }
        }
        self.begin();
        for session in dropped {
            let unwritten = self.forget(session);
            self.state.close(session, &unwritten);
        }
        self.state.wrote(written, self.landed_serial);
        if let Some(spool) = &self.spool {
            let grown = spool.report();
            if !grown.is_empty() {
                self.state.wrote(Written { segments: grown, ..Written::default() }, 0);
            }
        }
        self.state.landed(flight.number);
        let effects = self.state.take();
        self.route(effects);
        if !self.deferred.is_empty() {
            self.remove_segments();
        }
    }

    /// Starts a checkpoint walk when the ledger says the tail is long enough,
    /// and hands over slices while the writer keeps up (S4).
    fn tend_checkpoint(&mut self) {
        if self.checkpoint_to.is_none() {
            return;
        }
        if matches!(self.walk, Walk::Idle | Walk::Done) && self.state.checkpoint_due() {
            let (head, producers) = self.state.begin_checkpoint(self.landed_serial, self.landed);
            self.send_checkpoint(storage::Write::Begin(head));
            self.send_checkpoint(storage::Write::Part(producers));
            self.walk = Walk::Running { out: 1, finished: false };
        }
        let queue = self.config.storage.checkpoint_queue.max(1);
        while let Walk::Running { out, finished: false } = self.walk {
            if out >= queue {
                break;
            }
            match self.state.scan_slice() {
                Some(part) => {
                    self.send_checkpoint(storage::Write::Part(part));
                    self.walk = Walk::Running { out: out + 1, finished: false };
                }
                None => {
                    self.send_checkpoint(storage::Write::Finish);
                    self.walk = Walk::Running { out, finished: true };
                }
            }
        }
    }

    fn send_checkpoint(&mut self, write: storage::Write) {
        if self.checkpoint_to.as_ref().is_none_or(|to| to.send(write).is_err()) {
            self.failure = Some(EngineError("the checkpoint writer is gone".into()));
        }
    }

    fn checkpoint_answer(&mut self, answer: Answer) {
        match answer {
            Answer::Part => {
                if let Walk::Running { out, finished } = self.walk {
                    self.walk = Walk::Running { out: out.saturating_sub(1), finished };
                }
            }
            Answer::Finished(Ok(bytes)) => {
                self.walk = if matches!(self.walk, Walk::Last) { Walk::Done } else { Walk::Idle };
                self.begin();
                self.state.checkpointed(bytes);
                let effects = self.state.take();
                self.route(effects);
            }
            Answer::Finished(Err(error)) | Answer::Failed(error) => {
                self.failure = Some(EngineError(format!("checkpoint write failed: {error}").into()));
            }
        }
    }

    /// The stopped ledger's checkpoint, the last write (2.8, B22). A walk in
    /// progress is dropped: the whole ledger is written now, from where the
    /// log ends, so the next start reads only the checkpoint.
    fn last_checkpoint(&mut self) -> Option<EngineError> {
        if matches!(self.walk, Walk::Running { .. }) {
            self.send_checkpoint(storage::Write::Abort);
            self.state.abort_checkpoint();
        }
        if !self.journaled || self.checkpoint_to.is_none() {
            self.walk = Walk::Done;
            return None;
        }
        let (head, parts) = self.state.full_checkpoint(self.landed_serial, self.landed);
        self.send_checkpoint(storage::Write::Begin(head));
        for part in parts {
            self.send_checkpoint(storage::Write::Part(part));
        }
        self.send_checkpoint(storage::Write::Finish);
        self.walk = Walk::Last;
        self.failure.take()
    }
}
