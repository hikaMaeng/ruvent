//! The event server's engine: one task owns all state; connections talk to it
//! with the seven methods.
//!
//! ```text
//! connections ──Op──► inbox ──recv_many──► State (single writer) ──Effects──┐
//!                                                                            ├─► send buffers (try_send; full = disconnect)
//!                                          journal writer ◄── one flight ────┤
//!                                          snapshot collector ◄── try_send ──┘
//! ```
//!
//! The engine never waits on a socket and never waits on a timer. Under
//! `Strict` it holds a batch's gated sends until that batch's records are on
//! disk, and anything later for the same connection queues behind them.

pub mod config;
pub mod ops;
pub mod state;

use crate::journal::{Journal, Record, Recovered};
use crate::names::Name;
use crate::snapshot::{Collector, SnapshotStats};
use config::{Durability, EngineConfig};
use ops::{Call, Outbound, SessionId};
use state::{Effects, IdSource, State, V7};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
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
    Open { session: SessionId, credential: Name, out: mpsc::Sender<Outbound> },
    Close { session: SessionId },
    Call { session: SessionId, req: u32, call: Call },
    Shutdown { done: oneshot::Sender<()> },
}

type Stop = Option<Result<(), EngineError>>;

/// A handle to a running engine. Cheap to clone.
#[derive(Clone)]
pub struct Engine {
    ops: mpsc::Sender<Op>,
    next_session: Arc<AtomicU64>,
    stopped: watch::Receiver<Stop>,
    outbound: usize,
    snapshot: Option<Arc<SnapshotStats>>,
}

/// One connection to the engine, below the method API: send calls, read
/// [`Outbound`] in order. Dropping it closes the connection.
pub struct Session {
    pub id: SessionId,
    ops: mpsc::Sender<Op>,
}

impl Session {
    /// Hands a call to the engine. Waits only while the engine's inbox is full.
    pub async fn call(&self, req: u32, call: Call) -> Result<(), Closed> {
        self.ops.send(Op::Call { session: self.id, req, call }).await.map_err(|_| Closed)
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        // Best effort. If the inbox is full the engine still finds out at the
        // next thing it hands this connection, whose buffer is gone.
        let _ = self.ops.try_send(Op::Close { session: self.id });
    }
}

enum Job {
    Append(Vec<Record>, oneshot::Sender<io::Result<()>>),
    Rewrite(Vec<Record>, oneshot::Sender<io::Result<()>>),
}

impl Engine {
    /// Recovers from `recovered`, makes the new generation durable, and starts.
    /// The first answer goes out only after that write.
    pub async fn start(config: EngineConfig, journal: impl Journal, recovered: Recovered) -> io::Result<Engine> {
        Self::start_with(config, journal, recovered, Box::new(V7)).await
    }

    /// An engine with no disk, for tests and in-memory use.
    pub async fn volatile(config: EngineConfig) -> Engine {
        match Self::start(config, crate::journal::Volatile, Recovered::default()).await {
            Ok(engine) => engine,
            Err(_) => unreachable!("the volatile journal never fails"),
        }
    }

    /// [`Engine::start`] with a chosen source of fresh ids. For tests that need
    /// to force an id collision; production uses UUIDv7.
    pub async fn start_with(config: EngineConfig, mut journal: impl Journal, recovered: Recovered, ids: Box<dyn IdSource>) -> io::Result<Engine> {
        let mut state = State::new(&config, ids);
        let startup = state.recover(recovered.records).map_err(|reason| io::Error::new(io::ErrorKind::InvalidData, reason))?;
        let file_records = startup.len() as u64;
        journal.append(startup).await?;

        let (jobs, mut job_rx) = mpsc::channel::<Job>(1);
        let writer = tokio::spawn(async move {
            while let Some(job) = job_rx.recv().await {
                match job {
                    Job::Append(records, done) => {
                        let _ = done.send(journal.append(records).await);
                    }
                    Job::Rewrite(records, done) => {
                        let _ = done.send(journal.rewrite(records).await);
                    }
                }
            }
        });

        let collector = config.snapshot.clone().map(Collector::spawn);
        let snapshot = collector.as_ref().map(|collector| collector.stats.clone());
        let (ops, inbox) = mpsc::channel(config.inbox.max(1));
        let (stop_tx, stopped) = watch::channel(None);
        let core = Core {
            state,
            inbox,
            outs: HashMap::new(),
            jobs: Some(jobs),
            writer: Some(writer),
            flight: None,
            next_flight: 0,
            pending: Vec::new(),
            held: VecDeque::new(),
            blocked: HashMap::new(),
            next_group: 0,
            file_records,
            config: config.clone(),
            collector,
            epoch: tokio::time::Instant::now(),
            stop: stop_tx,
            shutdown: Vec::new(),
        };
        tokio::spawn(core.run());
        Ok(Engine { ops, next_session: Arc::new(AtomicU64::new(1)), stopped, outbound: config.outbound.max(1), snapshot })
    }

    /// Opens a connection under a service credential.
    pub async fn open(&self, credential: &str) -> Result<(Session, mpsc::Receiver<Outbound>), Closed> {
        let session = self.next_session.fetch_add(1, Ordering::Relaxed);
        let (out, rx) = mpsc::channel(self.outbound);
        self.ops.send(Op::Open { session, credential: Name::new(credential), out }).await.map_err(|_| Closed)?;
        Ok((Session { id: session, ops: self.ops.clone() }, rx))
    }

    /// Stops accepting calls, writes what is pending, and waits until it is on disk.
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

/// Sends held until a flight is on disk.
struct Group {
    number: u64,
    /// The flight whose completion releases this group.
    after: u64,
    sends: Vec<(SessionId, Outbound)>,
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
}

/// The outcome of the flight in progress; never resolves when there is none.
async fn wait(flight: &mut Option<Flight>) -> Option<io::Result<()>> {
    match flight {
        Some(flight) => (&mut flight.done).await.ok(),
        None => std::future::pending().await,
    }
}

struct Core {
    state: State,
    inbox: mpsc::Receiver<Op>,
    /// Send buffers by connection.
    ///
    /// | key | session id |
    /// | --- | --- |
    /// | lifetime | from `Open` to `Close`, or until a send finds the buffer full or gone |
    /// | bound | open connections |
    outs: HashMap<SessionId, mpsc::Sender<Outbound>>,
    /// The writer's inbox, and the writer. Both go before the engine reports
    /// that it stopped, so the journal (and its lock) is released by then.
    jobs: Option<mpsc::Sender<Job>>,
    writer: Option<tokio::task::JoinHandle<()>>,
    flight: Option<Flight>,
    next_flight: u64,
    /// Records not yet handed to the writer. They go in the next flight.
    pending: Vec<Record>,
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
    config: EngineConfig,
    collector: Option<Collector>,
    epoch: tokio::time::Instant,
    stop: watch::Sender<Stop>,
    shutdown: Vec<oneshot::Sender<()>>,
}

impl Core {
    fn now(&self) -> u64 {
        u64::try_from(self.epoch.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    async fn run(mut self) {
        let mut batch = Vec::with_capacity(self.config.batch_max);
        let result = loop {
            let accepting = self.shutdown.is_empty();
            if !accepting && self.flight.is_none() && self.pending.is_empty() {
                break Ok(());
            }
            let event = {
                let flight = &mut self.flight;
                let inbox = &mut self.inbox;
                tokio::select! {
                    biased;
                    done = wait(flight) => Wake::Landed(done),
                    received = inbox.recv_many(&mut batch, self.config.batch_max), if accepting => Wake::Received(received),
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
                Wake::Received(_) => {
                    if let Some(error) = self.apply(&mut batch) {
                        break Err(error);
                    }
                }
            }
            if let Some(error) = self.launch() {
                break Err(error);
            }
        };
        // Held sends of an unfinished write are dropped with the connections.
        self.outs.clear();
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

    fn apply(&mut self, batch: &mut Vec<Op>) -> Option<EngineError> {
        let now = self.now();
        self.state.begin(now);
        for op in batch.drain(..) {
            match op {
                Op::Open { session, credential, out } => {
                    self.state.open(session, &credential);
                    self.outs.insert(session, out);
                }
                Op::Close { session } => self.disconnect(session),
                Op::Call { session, req, call } => self.state.call(session, req, call),
                Op::Shutdown { done } => self.shutdown.push(done),
            }
        }
        let effects = self.state.take();
        if let Some(reason) = effects.fatal {
            return Some(EngineError(reason.into()));
        }
        self.route(effects);
        None
    }

    fn disconnect(&mut self, session: SessionId) {
        self.outs.remove(&session);
        self.state.close(session);
    }

    /// Hands one message to a connection without waiting. A full or closed
    /// buffer disconnects it: that is the connection's signal that it lost events.
    fn hand(&mut self, session: SessionId, message: Outbound) {
        let Some(out) = self.outs.get(&session) else { return };
        if out.try_send(message).is_err() {
            self.disconnect(session);
        }
    }

    fn route(&mut self, effects: Effects) {
        let strict = self.config.durability == Durability::Strict;
        let number = self.next_group;
        let mut group = Vec::new();
        for send in effects.sends {
            let hold = effects.gate_all || (strict && send.gated) || self.blocked.contains_key(&send.session);
            if hold {
                self.blocked.insert(send.session, number);
                group.push((send.session, send.message));
            } else {
                self.hand(send.session, send.message);
            }
        }
        let has_records = !effects.records.is_empty();
        self.pending.extend(effects.records);
        if !group.is_empty() {
            // Released with the flight that will carry this batch's records, or,
            // when the batch wrote nothing, with the group it queues behind.
            let after = if has_records || !self.pending.is_empty() { self.next_flight } else { self.held.back().map_or(self.next_flight, |last| last.after) };
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
    }

    fn launch(&mut self) -> Option<EngineError> {
        if self.flight.is_some() || self.pending.is_empty() {
            return None;
        }
        let records = std::mem::take(&mut self.pending);
        let total = self.file_records + records.len() as u64;
        let live = self.state.live() as u64;
        let compaction = self.config.compaction;
        let (done_tx, done) = oneshot::channel();
        let number = self.next_flight;
        self.next_flight += 1;
        let (job, count, rewrite) = if total > compaction.min_records && total > compaction.ratio.saturating_mul(live.max(1)) {
            // The snapshot reflects every pending change: they were applied before it was taken.
            let snapshot = self.state.snapshot();
            let count = snapshot.len() as u64;
            (Job::Rewrite(snapshot, done_tx), count, true)
        } else {
            let count = records.len() as u64;
            (Job::Append(records, done_tx), count, false)
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
        while self.held.front().is_some_and(|group| group.after <= flight.number) {
            let Some(group) = self.held.pop_front() else { break };
            for (session, message) in group.sends {
                self.hand(session, message);
            }
            self.blocked.retain(|_, last| *last > group.number);
        }
    }
}
