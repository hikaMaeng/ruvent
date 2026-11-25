//! The event server's engine: one task owns the ledger; connections talk to
//! it with [`Call`]s and read [`Outbound`]s.
//!
//! ```text
//! connections ──Op──► inbox ──recv_many──► State (single writer) ──Effects──┐
//!                                                                            ├─► send buffers (try_send; full = disconnect)
//!                                          journal writer ◄── one flight ────┤
//!                                          snapshot collector ◄── try_send ──┘
//!                          timing wheel ──(earliest due only)──► State
//! ```
//!
//! The engine never waits on a socket. It waits on three things: the inbox,
//! the write in flight, and the earliest lease end or visibility time in the
//! ledger's wheel — one one-shot sleep to that time, none when the wheel is
//! empty. A send gated on a commit number is held until that batch is on
//! disk, and anything later for the same connection queues behind it.
//!
//! The clock is the accumulated uptime (ms): it starts where the journal's
//! last batch left it and does not count the time the server was down (0.3).

pub mod config;
pub mod ops;
pub mod state;

use crate::envelope::ClaimToken;
use crate::journal::{Batch, Journal, Record, Recovered};
use crate::snapshot::{Collector, SnapshotStats};
use config::EngineConfig;
use ops::{Call, Hello, Outbound, SessionId};
use state::{Effects, IdSource, State, V7};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
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
    Open { session: SessionId, hello: Hello, out: mpsc::Sender<Outbound>, reading: watch::Sender<bool> },
    Close { session: SessionId, unwritten: Vec<ClaimToken> },
    Call { session: SessionId, call: Call },
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
}

/// One connection to the engine, below the client API: send calls, read
/// [`Outbound`] in order. Dropping it closes the connection with every
/// assignment counted as written.
pub struct Session {
    pub id: SessionId,
    ops: mpsc::Sender<Op>,
    reading: watch::Receiver<bool>,
    closed: bool,
}

impl Session {
    /// Hands a call to the engine. Waits only while the engine's inbox is full.
    pub async fn call(&self, call: Call) -> Result<(), Closed> {
        self.ops.send(Op::Call { session: self.id, call }).await.map_err(|_| Closed)
    }

    /// Closes the connection. `unwritten` are the tokens of assignments the
    /// connection owner had not finished writing to the socket: they go back
    /// to waiting at once (2.4).
    pub async fn close(mut self, unwritten: Vec<ClaimToken>) {
        self.closed = true;
        let _ = self.ops.send(Op::Close { session: self.id, unwritten }).await;
    }

    /// `false` while the ledger wants this connection's reading stopped: a
    /// production connection that kept writing to a paused root (2.6). The
    /// connection owner watches it.
    pub fn reading(&self) -> watch::Receiver<bool> {
        self.reading.clone()
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
    Append(Batch, oneshot::Sender<io::Result<()>>),
    Rewrite(Batch, oneshot::Sender<io::Result<()>>),
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
        let mut state = State::new(&config, ids);
        let startup = state.recover(recovered.records, recovered.uptime, recovered.serial, journaled).map_err(|reason| io::Error::new(io::ErrorKind::InvalidData, reason))?;
        let file_records = startup.len() as u64;
        let first_commit = recovered.commit + 1;
        journal.append(Batch { uptime: recovered.uptime, commit: first_commit, first: recovered.serial, records: startup }).await?;

        let (jobs, mut job_rx) = mpsc::channel::<Job>(1);
        let writer = tokio::spawn(async move {
            while let Some(job) = job_rx.recv().await {
                match job {
                    Job::Append(batch, done) => {
                        let _ = done.send(journal.append(batch).await);
                    }
                    Job::Rewrite(batch, done) => {
                        let _ = done.send(journal.rewrite(batch).await);
                    }
                }
            }
        });

        let clock = Clock { base: recovered.uptime, epoch: tokio::time::Instant::now() };
        let collector = config.snapshot.clone().map(Collector::spawn);
        let snapshot = collector.as_ref().map(|collector| collector.stats.clone());
        let (ops, inbox) = mpsc::channel(config.inbox.max(1));
        let (stop_tx, stopped) = watch::channel(None);
        let generation = state.generation();
        let server_id = state.server_id();
        let core = Core {
            state,
            inbox,
            conns: HashMap::new(),
            jobs: Some(jobs),
            writer: Some(writer),
            flight: None,
            next_flight: first_commit + 1,
            landed: first_commit,
            pending: Vec::new(),
            pending_first: 0,
            held: VecDeque::new(),
            blocked: HashMap::new(),
            next_group: 0,
            file_records,
            journaled,
            checkpointed: false,
            config: config.clone(),
            collector,
            clock,
            stop: stop_tx,
            shutdown: Vec::new(),
        };
        tokio::spawn(core.run());
        Ok(Engine { ops, next_session: Arc::new(AtomicU64::new(1)), stopped, outbound: config.outbound.max(1), snapshot, clock, generation, server_id })
    }

    /// Opens a connection. Its first message is `Welcome`, or `Bye`.
    pub async fn open(&self, hello: Hello) -> Result<(Session, mpsc::Receiver<Outbound>), Closed> {
        let session = self.next_session.fetch_add(1, Ordering::Relaxed);
        let (out, rx) = mpsc::channel(self.outbound);
        let (reading, watching) = watch::channel(true);
        self.ops.send(Op::Open { session, hello, out, reading }).await.map_err(|_| Closed)?;
        Ok((Session { id: session, ops: self.ops.clone(), reading: watching, closed: false }, rx))
    }

    /// Stops accepting calls, writes what is pending and a final snapshot of
    /// the stopped ledger (2.8, B22), and waits until it is on disk.
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

    /// Resolves when the engine stops: `Ok` after a shutdown, `Err` with the
    /// reason when it stopped on its own (a journal write failed).
    pub async fn stopped(&self) -> Result<(), EngineError> {
        let mut stopped = self.stopped.clone();
        let outcome = match stopped.wait_for(Option::is_some).await {
            Ok(state) => state.clone().unwrap_or(Ok(())),
            Err(_) => Err(EngineError("the engine task ended without saying why".into())),
        };
        outcome
    }
}

/// Sends held until a flight is on disk. `None` closes the connection.
struct Group {
    number: u64,
    /// The flight whose completion releases this group.
    after: u64,
    sends: Vec<(SessionId, Option<Outbound>)>,
}

struct Flight {
    number: u64,
    records: u64,
    rewrite: bool,
    done: oneshot::Receiver<io::Result<()>>,
}

enum Wake {
    Landed(Option<io::Result<()>>),
    Received(usize),
    Due,
}

/// The outcome of the flight in progress; never resolves when there is none.
async fn wait(flight: &mut Option<Flight>) -> Option<io::Result<()>> {
    match flight {
        Some(flight) => (&mut flight.done).await.ok(),
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
    reading: watch::Sender<bool>,
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
    /// The last commit number on disk.
    landed: u64,
    /// Records not yet handed to the writer. They go in the next flight.
    pending: Vec<Record>,
    pending_first: u64,
    held: VecDeque<Group>,
    /// Connections with sends held, and the last group holding one.
    ///
    /// | key | session id |
    /// | --- | --- |
    /// | lifetime | until that group is released |
    /// | bound | open connections |
    blocked: HashMap<SessionId, u64>,
    next_group: u64,
    file_records: u64,
    journaled: bool,
    checkpointed: bool,
    config: EngineConfig,
    collector: Option<Collector>,
    clock: Clock,
    stop: watch::Sender<Stop>,
    shutdown: Vec<oneshot::Sender<()>>,
}

impl Core {
    async fn run(mut self) {
        let mut batch = Vec::with_capacity(self.config.batch_max);
        let result = loop {
            let accepting = self.shutdown.is_empty();
            if !accepting && self.flight.is_none() && self.pending.is_empty() {
                // The stopped ledger's snapshot is the last write (B22).
                if self.journaled && !self.checkpointed {
                    self.checkpointed = true;
                    if let Some(error) = self.launch(true) {
                        break Err(error);
                    }
                    continue;
                }
                break Ok(());
            }
            let after = self.state.next_due().map(|at| at.saturating_sub(self.clock.now()));
            let event = {
                let flight = &mut self.flight;
                let inbox = &mut self.inbox;
                tokio::select! {
                    biased;
                    done = wait(flight) => Wake::Landed(done),
                    received = inbox.recv_many(&mut batch, self.config.batch_max), if accepting => Wake::Received(received),
                    () = due(after), if accepting => Wake::Due,
                }
            };
            match event {
                Wake::Landed(done) => match (done, self.flight.take()) {
                    (Some(Ok(())), Some(flight)) => self.landed(flight),
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
            }
            if let Some(error) = self.launch(false) {
                break Err(error);
            }
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
                Op::Open { session, hello, out, reading } => {
                    self.conns.insert(session, Conn { out, reading });
                    self.state.open(session, hello);
                }
                Op::Close { session, unwritten } => {
                    self.conns.remove(&session);
                    self.state.close(session, &unwritten);
                }
                Op::Call { session, call } => self.state.call(session, call),
                Op::Shutdown { done } => self.shutdown.push(done),
            }
        }
        let effects = self.state.take();
        self.route(effects);
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

    fn route(&mut self, effects: Effects) {
        let mut effects = effects;
        loop {
            let mut dropped = Vec::new();
            let number = self.next_group;
            let mut group = Vec::new();
            let mut after = 0;
            for send in effects.sends {
                let gated = send.gate.is_some_and(|gate| gate > self.landed);
                if gated || self.blocked.contains_key(&send.session) {
                    self.blocked.insert(send.session, number);
                    after = after.max(send.gate.unwrap_or(0));
                    group.push((send.session, send.message));
                } else if !self.hand(send.session, send.message) {
                    dropped.push(send.session);
                }
            }
            for (session, reading) in effects.reading {
                if let Some(conn) = self.conns.get(&session) {
                    let _ = conn.reading.send(reading);
                }
            }
            if !effects.records.is_empty() {
                if self.pending.is_empty() {
                    self.pending_first = effects.first_serial;
                }
                self.pending.extend(effects.records);
            }
            if !group.is_empty() {
                // Released with the flight it waits for, and never before the
                // group ahead of it.
                let after = after.max(self.held.back().map_or(0, |last| last.after));
                self.held.push_back(Group { number, after, sends: group });
                self.next_group += 1;
            }
            if let Some(collector) = &self.collector {
                for event in effects.observed {
                    if collector.sender.try_send(event).is_err() {
                        collector.stats.dropped.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            if dropped.is_empty() {
                return;
            }
            // A connection that could not take a message is closed; what that
            // changes in the ledger is routed like any other batch.
            self.begin();
            for session in dropped {
                self.state.close(session, &[]);
            }
            effects = self.state.take();
        }
    }

    fn launch(&mut self, checkpoint: bool) -> Option<EngineError> {
        if self.flight.is_some() || (self.pending.is_empty() && !checkpoint) {
            return None;
        }
        let records = std::mem::take(&mut self.pending);
        let total = self.file_records + records.len() as u64;
        let live = self.state.live() as u64;
        let compaction = self.config.compaction;
        let (done_tx, done) = oneshot::channel();
        let number = self.next_flight;
        self.next_flight += 1;
        let uptime = self.clock.now();
        let (job, count, rewrite) = if checkpoint || (total > compaction.min_records && total > compaction.ratio.saturating_mul(live.max(1))) {
            // The snapshot reflects every pending change: they were applied before it was taken.
            let snapshot = self.state.snapshot();
            let count = snapshot.len() as u64;
            (Job::Rewrite(Batch { uptime, commit: number, first: self.state.serial(), records: snapshot }, done_tx), count, true)
        } else {
            let count = records.len() as u64;
            (Job::Append(Batch { uptime, commit: number, first: self.pending_first, records }, done_tx), count, false)
        };
        // The writer holds at most the one job in flight, so a refusal means it is gone.
        if self.jobs.as_ref().is_none_or(|jobs| jobs.try_send(job).is_err()) {
            return Some(EngineError("the journal writer is gone".into()));
        }
        self.flight = Some(Flight { number, records: count, rewrite, done });
        None
    }

    fn landed(&mut self, flight: Flight) {
        self.file_records = if flight.rewrite { flight.records } else { self.file_records + flight.records };
        self.landed = flight.number;
        let mut dropped = Vec::new();
        while self.held.front().is_some_and(|group| group.after <= flight.number) {
            let Some(group) = self.held.pop_front() else { break };
            for (session, message) in group.sends {
                if !self.hand(session, message) {
                    dropped.push(session);
                }
            }
            self.blocked.retain(|_, last| *last > group.number);
        }
        self.begin();
        for session in dropped {
            self.state.close(session, &[]);
        }
        self.state.landed(flight.number);
        let effects = self.state.take();
        self.route(effects);
    }
}
