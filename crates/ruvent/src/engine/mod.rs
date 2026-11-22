//! The single-writer engine.
//!
//! One task owns every queue, every execution and the write side of the log.
//! Callers send it operations over a channel; it drains whatever has
//! accumulated and applies the lot in memory.
//!
//! Three properties fall out of that shape rather than being built:
//!
//! * **exclusive delivery** — no two consumers are ever looking at a queue at
//!   the same moment, so there is nothing for a lock to arbitrate;
//! * **a total order** — operations are applied one at a time, so a single
//!   position number is a safe cursor over the whole log;
//! * **one event, one worker** — a message is held by whoever took it until
//!   they finish it or give it back. Work for two workers is two messages.
//!
//! ## Nothing here runs on a clock
//!
//! The engine task sleeps until one of four things happens: an operation
//! arrives, a journal write reports back, a real deadline passes (a delayed
//! retry, a visibility timeout somebody asked for, a waiter's patience), or
//! the journal's coalescing window closes. The last two share one timer, armed
//! only while there is something to wait for. An idle engine is parked by the
//! runtime and woken by the OS; it does not tick.
//!
//! ## Journaling is behind, net, and never overlaps
//!
//! By default nothing waits for the disk ([`Durability::Scheduled`]). When
//! durable state first differs from what the journal says, a window opens;
//! when it closes, the engine writes the *net* difference — not what happened
//! in between. Work that arrived and finished inside the window is never
//! written at all.
//!
//! A write may not start while another is going. That is decided by state,
//! not by the window:
//!
//! * the engine holds at most one `flight` and hands the writer nothing while
//!   it is occupied. Changes keep cancelling and merging in memory meanwhile,
//!   so a slow disk makes the next write *more* net, not concurrent;
//! * each batch names the record number it follows, and the journal refuses
//!   one that does not follow what it last wrote;
//! * compaction goes through the same single flight, so a snapshot can never
//!   replace a file an append is still writing.

mod config;
mod log;
mod queue;
mod state;

use crate::envelope::{Command, Emit, Envelope, Text};
use crate::journal::{FileJournal, Journal, Record, Recovered, Volatile};
use bytes::Bytes;
use state::State;
use std::collections::HashMap;
use std::fmt;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

pub use config::{Compaction, Durability, EngineConfig, Events, Persist, ReactionTable, Retention};
pub use log::{From, Stats, Subscription};
pub use queue::Delivery;

use log::Shared;
use queue::Waiter;

/// The engine has stopped: it was shut down, or its journal failed.
#[derive(Clone, Debug)]
pub struct Closed(pub String);

impl fmt::Display for Closed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "engine closed: {}", self.0)
    }
}

impl std::error::Error for Closed {}

/// What became of a delivered message when work on it was begun.
#[derive(Clone, Debug)]
pub enum Begin {
    /// This caller owns the execution and must run it.
    Claimed { command: Arc<Envelope>, attempt: u64, attempts: u32 },
    /// The same event is already being run; this copy was dropped.
    Duplicate,
    /// It had already finished; the stored result was reported again.
    Settled,
    /// The transaction key was reused with a different action or payload.
    Conflict,
    /// Out of attempts; failed permanently and moved to the dead letters.
    Exhausted,
    /// The message no longer exists.
    Gone,
}

#[derive(Clone, Debug)]
pub enum Outcome {
    Ok(Bytes),
    Failed(Bytes),
}

enum Op {
    Submit { queue: Text, command: Command, reply: oneshot::Sender<u64> },
    Append { drafts: Vec<Envelope>, reply: oneshot::Sender<Vec<Arc<Envelope>>> },
    Dequeue { queue: Text, max: usize, visibility: Option<u64>, wait: Option<u64>, reply: oneshot::Sender<Vec<Delivery>> },
    Begin { queue: Text, id: u64, reply: oneshot::Sender<Begin> },
    Owns { key: Text, attempt: u64, reply: oneshot::Sender<bool> },
    Finish { command: Arc<Envelope>, attempt: u64, outcome: Outcome, queue: Text, id: u64, reply: oneshot::Sender<bool> },
    Release { key: Text, attempt: u64, queue: Text, id: u64, retry: u64, reply: oneshot::Sender<bool> },
    Emit { command: Arc<Envelope>, attempt: u64, emit: Emit, reply: Option<oneshot::Sender<bool>> },
    Ack { queue: Text, id: u64, reply: oneshot::Sender<bool> },
    Nack { queue: Text, id: u64, delay: u64, reply: oneshot::Sender<bool> },
    Flush { reply: oneshot::Sender<()> },
    Compact { reply: oneshot::Sender<io::Result<()>> },
    Shutdown { reply: oneshot::Sender<()> },
    DeadLetters { queue: Text, reply: oneshot::Sender<Vec<Arc<Command>>> },
}

/// Runs once its batch may be acknowledged.
type After = Box<dyn FnOnce() + Send>;

fn answer<T: Send + 'static>(reply: oneshot::Sender<T>, value: T) -> After {
    Box::new(move || {
        let _ = reply.send(value);
    })
}

fn millis(duration: Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128) as u64
}

/// A handle to a running engine. Cheap to clone.
#[derive(Clone)]
pub struct Engine {
    ops: mpsc::Sender<Op>,
    shared: Arc<Shared>,
}

impl Engine {
    /// Opens an engine on a journal file, restoring the work it still owes.
    /// This is the constructor to use in production.
    pub async fn open(path: impl Into<PathBuf>, config: EngineConfig) -> io::Result<Self> {
        let path = path.into();
        let (journal, recovered) = tokio::task::spawn_blocking(move || FileJournal::open(path)).await.map_err(io::Error::other)??;
        Ok(Self::start(config, journal, recovered))
    }

    /// An engine that keeps nothing across a restart. For tests and for
    /// measuring the engine without a disk — not for running a service.
    pub fn volatile(config: EngineConfig) -> Self {
        Self::start(config, Volatile, Recovered::default())
    }

    /// Starts an engine on any journal, from what it recovered.
    pub fn start<J: Journal>(config: EngineConfig, journal: J, recovered: Recovered) -> Self {
        let shared = Arc::new(Shared::new());
        let reactions: HashMap<Text, Vec<Text>> =
            config.reactions.iter().map(|(action, queues)| (Text::from(action.as_str()), queues.iter().map(|queue| Text::from(queue.as_str())).collect())).collect();
        let mut state = State::new(Arc::new(reactions), config.max_attempts, config.retention, config.persist);
        let now = shared.now();
        for record in &recovered.records {
            state.apply(record, now);
        }
        // Events the journal kept are readable from the first moment.
        {
            let mut log = shared.log.write();
            for event in state.staged.drain(..) {
                log.publish(event);
            }
            log.drop_through(state.drop_through);
        }
        let (events, bytes) = state.retained();
        shared.counters.retained_events.store(events, Ordering::Relaxed);
        shared.counters.retained_bytes.store(bytes, Ordering::Relaxed);
        let floor = state.open_epoch();
        shared.log.write().begin_epoch(floor);

        let (ops, inbox) = mpsc::channel(config.inbox.max(1));
        // Capacity one, and the engine only sends when no flight is out: the
        // writer never has a second job waiting behind the one it is doing.
        let (jobs, work) = mpsc::channel(1);
        let writer = tokio::spawn(write(journal, work));
        let (strict, window) = match config.durability {
            Durability::Scheduled { window } => (false, window),
            Durability::Strict => (true, Duration::ZERO),
        };
        let core = Core {
            inbox,
            shared: shared.clone(),
            state,
            jobs,
            writer,
            stopping: None,
            strict,
            window,
            compaction: config.compaction,
            batch_max: config.batch_max.max(1),
            written: recovered.through,
            in_journal: recovered.records.len() as u64,
            flight: None,
            launched: 0,
            landed: 0,
            dirty_since: None,
            wanted: false,
            compact_wanted: false,
            flushes: Vec::new(),
            compactions: Vec::new(),
        };
        tokio::spawn(core.run());
        Self { ops, shared }
    }

    fn closed(&self) -> Closed {
        Closed(self.shared.failure.lock().clone().unwrap_or_else(|| "engine has stopped".to_owned()))
    }

    async fn ask<T>(&self, make: impl FnOnce(oneshot::Sender<T>) -> Op) -> Result<T, Closed> {
        let (reply, answer) = oneshot::channel();
        self.ops.send(make(reply)).await.map_err(|_| self.closed())?;
        answer.await.map_err(|_| self.closed())
    }

    /// Puts a command on a queue.
    pub async fn submit(&self, queue: impl Into<Text>, command: Command) -> Result<u64, Closed> {
        let queue = queue.into();
        self.ask(|reply| Op::Submit { queue, command, reply }).await
    }

    /// Appends events directly — facts recorded by something that is not a
    /// worker execution. Reactions fire exactly as they do for a worker's facts.
    pub async fn append(&self, drafts: Vec<Envelope>) -> Result<Vec<Arc<Envelope>>, Closed> {
        self.ask(|reply| Op::Append { drafts, reply }).await
    }

    /// Takes up to `max` messages.
    ///
    /// `wait` is how long to stay parked on an empty queue: `None` waits until
    /// a message arrives, and costs nothing while it does. `visibility` is a
    /// deadline after which an unfinished message returns by itself; `None`
    /// holds it until it is finished, released or acknowledged, which is right
    /// for a consumer in this process — it cannot vanish without the process.
    pub async fn dequeue(&self, queue: impl Into<Text>, max: usize, visibility: Option<Duration>, wait: Option<Duration>) -> Result<Vec<Delivery>, Closed> {
        let queue = queue.into();
        self.ask(|reply| Op::Dequeue { queue, max, visibility: visibility.map(millis), wait: wait.map(millis), reply }).await
    }

    /// Records the command and takes ownership of its execution, in one step.
    pub async fn begin(&self, queue: impl Into<Text>, id: u64) -> Result<Begin, Closed> {
        let queue = queue.into();
        self.ask(|reply| Op::Begin { queue, id, reply }).await
    }

    /// Whether this attempt is still the one running the execution.
    pub async fn owns(&self, key: impl Into<Text>, attempt: u64) -> Result<bool, Closed> {
        let key = key.into();
        self.ask(|reply| Op::Owns { key, attempt, reply }).await
    }

    /// Completes an execution: its result event and its message's deletion,
    /// as one step. `false` means ownership had been lost and nothing changed.
    pub async fn finish(&self, command: Arc<Envelope>, attempt: u64, outcome: Outcome, queue: impl Into<Text>, id: u64) -> Result<bool, Closed> {
        let queue = queue.into();
        self.ask(|reply| Op::Finish { command, attempt, outcome, queue, id, reply }).await
    }

    /// Gives an execution up so it is run again after `retry`.
    pub async fn release(&self, key: impl Into<Text>, attempt: u64, queue: impl Into<Text>, id: u64, retry: Duration) -> Result<bool, Closed> {
        let (key, queue) = (key.into(), queue.into());
        self.ask(|reply| Op::Release { key, attempt, queue, id, retry: millis(retry), reply }).await
    }

    /// Records a handler emission. `false` means the attempt no longer owns
    /// the execution and nothing was written.
    pub async fn emit(&self, command: Arc<Envelope>, attempt: u64, emit: Emit) -> Result<bool, Closed> {
        self.ask(|reply| Op::Emit { command, attempt, emit, reply: Some(reply) }).await
    }

    /// The same without waiting: never blocks, and drops the emission when the
    /// inbox is full. For streamed progress, where a late delta is worth less
    /// than a stalled handler.
    pub fn emit_nowait(&self, command: Arc<Envelope>, attempt: u64, emit: Emit) -> bool {
        let sent = self.ops.try_send(Op::Emit { command, attempt, emit, reply: None }).is_ok();
        if !sent {
            self.shared.counters.progress_dropped.fetch_add(1, Ordering::Relaxed);
        }
        sent
    }

    /// Deletes a delivered message without an execution — plain queue use.
    /// `false` if it was already gone.
    pub async fn ack(&self, queue: impl Into<Text>, id: u64) -> Result<bool, Closed> {
        let queue = queue.into();
        self.ask(|reply| Op::Ack { queue, id, reply }).await
    }

    /// Returns a delivered message to its queue, at once or after `delay`.
    pub async fn nack(&self, queue: impl Into<Text>, id: u64, delay: Duration) -> Result<bool, Closed> {
        let queue = queue.into();
        self.ask(|reply| Op::Nack { queue, id, delay: millis(delay), reply }).await
    }

    /// Resolves once everything applied before this call is on disk. Call it
    /// wherever one particular change must not be lost.
    pub async fn flush(&self) -> Result<(), Closed> {
        self.ask(|reply| Op::Flush { reply }).await
    }

    /// Folds the journal into a snapshot now. It happens by itself when the
    /// journal is mostly superseded statements; this is for tests and tooling.
    pub async fn compact(&self) -> Result<io::Result<()>, Closed> {
        self.ask(|reply| Op::Compact { reply }).await
    }

    /// Writes everything out, releases the journal and stops the engine.
    /// Every handle reports [`Closed`] afterwards.
    pub async fn shutdown(&self) -> Result<(), Closed> {
        self.ask(|reply| Op::Shutdown { reply }).await
    }

    pub fn is_closed(&self) -> bool {
        self.ops.is_closed()
    }

    /// Messages that ran out of attempts on this queue.
    pub async fn dead_letters(&self, queue: impl Into<Text>) -> Result<Vec<Arc<Command>>, Closed> {
        let queue = queue.into();
        self.ask(|reply| Op::DeadLetters { queue, reply }).await
    }

    /// Opens a read position over a set of channels. Nothing is consumed by
    /// reading, so any number of subscriptions cost each other nothing.
    pub fn subscribe(&self, channels: Vec<String>, from: From) -> Subscription {
        Subscription::open(self.shared.clone(), channels, from)
    }

    /// Reads a stream forward from position `after`, for folding state out of
    /// the log. Unlike a subscription this sees worker-audience events too.
    pub fn read_stream(&self, stream: &str, after: u64, limit: usize) -> Vec<Arc<Envelope>> {
        self.shared.read_stream(stream, after, limit)
    }

    pub fn stats(&self) -> Stats {
        self.shared.stats()
    }
}

enum Job {
    Append { prev: u64, through: u64, records: Vec<Record>, done: oneshot::Sender<io::Result<()>> },
    Rewrite { at: u64, records: Vec<Record>, done: oneshot::Sender<io::Result<()>> },
}

/// The only task that touches the journal. One job at a time, in the order
/// the engine issued them; it stops at the first failure.
async fn write<J: Journal>(mut journal: J, mut work: mpsc::Receiver<Job>) {
    while let Some(job) = work.recv().await {
        let (result, done) = match job {
            Job::Append { prev, through, records, done } => (journal.append(prev, through, &records).await, done),
            Job::Rewrite { at, records, done } => (journal.rewrite(at, &records).await, done),
        };
        let failed = result.is_err();
        let _ = done.send(result);
        if failed {
            return;
        }
    }
}

/// A journal write that has been issued and has not yet reported back.
struct Flight {
    done: oneshot::Receiver<io::Result<()>>,
    /// Records in this write.
    records: u64,
    snapshot: bool,
}

async fn landing(flight: &mut Option<Flight>) -> io::Result<()> {
    match flight {
        Some(flight) => (&mut flight.done).await.unwrap_or_else(|_| Err(io::Error::other("journal writer stopped"))),
        None => std::future::pending().await,
    }
}

struct Core {
    inbox: mpsc::Receiver<Op>,
    shared: Arc<Shared>,
    state: State,
    jobs: mpsc::Sender<Job>,
    writer: tokio::task::JoinHandle<()>,
    stopping: Option<oneshot::Sender<()>>,
    strict: bool,
    window: Duration,
    compaction: Compaction,
    batch_max: usize,
    /// The record number the journal ends at.
    written: u64,
    /// Records the journal file currently holds, superseded ones included.
    in_journal: u64,
    /// The one write that may be in progress. `None` is the only state in
    /// which another may be issued.
    flight: Option<Flight>,
    /// Writes issued and writes completed. A flush waits for a particular one.
    launched: u64,
    landed: u64,
    /// When durable state first came to differ from the journal.
    dirty_since: Option<Instant>,
    /// Someone asked for a write now, window or not.
    wanted: bool,
    compact_wanted: bool,
    flushes: Vec<(u64, oneshot::Sender<()>)>,
    compactions: Vec<oneshot::Sender<io::Result<()>>>,
}

impl Core {
    async fn run(mut self) {
        // The epoch goes to disk before anyone is served.
        if !self.settle().await {
            return;
        }

        // One timer for every deadline there is. It is re-armed only when the
        // earliest deadline changes, and not armed at all when there is none.
        let timer = tokio::time::sleep(Duration::ZERO);
        tokio::pin!(timer);
        let mut armed: Option<Instant> = None;

        let mut ops = Vec::with_capacity(self.batch_max);
        let mut after: Vec<After> = Vec::new();
        loop {
            let queue_deadline = self.state.next_deadline().map(|at| self.shared.instant(at));
            let write_deadline = match (self.flight.is_none() && !self.strict, self.dirty_since) {
                (true, Some(since)) => Some(since + self.window),
                _ => None,
            };
            let next = match (queue_deadline, write_deadline) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
            if next != armed {
                if let Some(at) = next {
                    timer.as_mut().reset(at);
                }
                armed = next;
            }

            tokio::select! {
                received = self.inbox.recv_many(&mut ops, self.batch_max) => {
                    if received == 0 {
                        break;
                    }
                }
                _ = &mut timer, if armed.is_some() => armed = None,
                landed = landing(&mut self.flight), if self.flight.is_some() => {
                    if !self.land(landed) {
                        return;
                    }
                }
            }

            self.shared.counters.wakeups.fetch_add(1, Ordering::Relaxed);
            let now = self.shared.now();
            let mut flushes = Vec::new();
            for op in ops.drain(..) {
                self.handle(op, now, &mut after, &mut flushes);
            }
            self.serve_waiters(now, &mut after);

            if self.dirty_since.is_none() && self.state.is_dirty() {
                self.dirty_since = Some(Instant::now());
            }
            for reply in flushes {
                // Whatever is dirty now goes out with the next write; if
                // nothing is, the write in flight (if any) already covers it.
                let target = if self.state.is_dirty() { self.launched + 1 } else { self.launched };
                self.flushes.push((target, reply));
                self.wanted = true;
            }

            if self.strict && !self.settle().await {
                return;
            }
            self.answer_flushes();
            self.publish();
            for callback in after.drain(..) {
                callback();
            }

            if self.stopping.is_some() {
                break;
            }
            if !self.schedule() {
                return;
            }
        }

        // Shutting down, or every handle is gone. Write what is left, then
        // let the writer go so the journal and its lock are released before
        // anyone is told the engine has stopped.
        self.inbox.close();
        if !self.settle().await {
            return;
        }
        let Core { jobs, writer, stopping, .. } = self;
        drop(jobs);
        let _ = writer.await;
        if let Some(reply) = stopping {
            let _ = reply.send(());
        }
    }

    /// Makes the batch's events readable and drops what retention let go.
    fn publish(&mut self) {
        let counters = &self.shared.counters;
        counters.duplicates.fetch_add(std::mem::take(&mut self.state.duplicates), Ordering::Relaxed);
        counters.enqueued.fetch_add(std::mem::take(&mut self.state.enqueued), Ordering::Relaxed);
        if self.state.staged.is_empty() {
            return;
        }
        counters.events.fetch_add(self.state.staged.len() as u64, Ordering::Relaxed);
        let (events, bytes) = self.state.retained();
        counters.retained_events.store(events, Ordering::Relaxed);
        counters.retained_bytes.store(bytes, Ordering::Relaxed);
        let mut log = self.shared.log.write();
        for event in self.state.staged.drain(..) {
            log.publish(event);
        }
        log.drop_through(self.state.drop_through);
    }

    fn issue(&mut self, job: Job, records: u64, snapshot: bool, done: oneshot::Receiver<io::Result<()>>) -> bool {
        debug_assert!(self.flight.is_none(), "a journal write was issued while another was in flight");
        if self.jobs.try_send(job).is_err() {
            return self.fail("journal writer is gone".to_owned());
        }
        self.flight = Some(Flight { done, records, snapshot });
        self.launched += 1;
        self.dirty_since = None;
        self.wanted = false;
        true
    }

    /// Writes the net change since the last write. Only ever called with no
    /// flight out.
    fn launch(&mut self) -> bool {
        let records = self.state.harvest();
        self.shared.counters.unwritten.store(0, Ordering::Relaxed);
        if records.is_empty() {
            // Everything that changed changed back. Nothing to write, and
            // whoever was waiting for this write has what they wanted.
            self.launched += 1;
            self.landed += 1;
            self.dirty_since = None;
            self.wanted = false;
            self.answer_flushes();
            return true;
        }
        let (prev, count) = (self.written, records.len() as u64);
        self.written += count;
        self.shared.counters.produced.fetch_add(count, Ordering::Relaxed);
        let (done, landed) = oneshot::channel();
        self.issue(Job::Append { prev, through: self.written, records, done }, count, false, landed)
    }

    /// Replaces the journal with a snapshot of durable state as it is now,
    /// which already includes every unwritten change. Only ever called with
    /// no flight out.
    fn compact(&mut self) -> bool {
        let records = self.state.snapshot();
        self.shared.counters.unwritten.store(0, Ordering::Relaxed);
        self.compact_wanted = false;
        let count = records.len() as u64;
        let (done, landed) = oneshot::channel();
        self.issue(Job::Rewrite { at: self.written, records, done }, count, true, landed)
    }

    /// Records the outcome of the flight that just reported back.
    fn land(&mut self, result: io::Result<()>) -> bool {
        let Some(flight) = self.flight.take() else { return true };
        if let Err(error) = result {
            let message = format!("journal write failed: {error}");
            for reply in self.compactions.drain(..) {
                let _ = reply.send(Err(io::Error::other(message.clone())));
            }
            return self.fail(message);
        }
        self.landed += 1;
        let counters = &self.shared.counters;
        if flight.snapshot {
            self.in_journal = flight.records;
            counters.compactions.fetch_add(1, Ordering::Relaxed);
            for reply in self.compactions.drain(..) {
                let _ = reply.send(Ok(()));
            }
        } else {
            self.in_journal += flight.records;
            counters.writes.fetch_add(1, Ordering::Relaxed);
            // Mostly superseded statements by now? Then fold it.
            if self.in_journal > self.compaction.min_records && self.in_journal > self.compaction.ratio.saturating_mul(self.state.live_records()) {
                self.compact_wanted = true;
            }
        }
        self.answer_flushes();
        true
    }

    /// Answers everyone whose write has completed. Called when a write lands
    /// and when a flush is requested — a request made with nothing left to
    /// write must not wait for a write that will never come.
    fn answer_flushes(&mut self) {
        let landed = self.landed;
        let (ready, waiting): (Vec<_>, Vec<_>) = self.flushes.drain(..).partition(|(target, _)| *target <= landed);
        self.flushes = waiting;
        for (_, reply) in ready {
            let _ = reply.send(());
        }
    }

    /// Starts whatever journal work is due — if, and only if, none is in flight.
    fn schedule(&mut self) -> bool {
        self.shared.counters.unwritten.store(self.state.unwritten_changes() as u64, Ordering::Relaxed);
        if self.flight.is_some() {
            return true;
        }
        if self.compact_wanted || !self.compactions.is_empty() {
            return self.compact();
        }
        let due = self.wanted || self.dirty_since.is_some_and(|since| Instant::now() >= since + self.window);
        if due && self.state.is_dirty() {
            return self.launch();
        }
        if !self.state.is_dirty() {
            self.dirty_since = None;
            self.wanted = false;
        }
        true
    }

    /// Writes until the journal agrees with memory.
    async fn settle(&mut self) -> bool {
        loop {
            if self.flight.is_some() {
                let landed = landing(&mut self.flight).await;
                if !self.land(landed) {
                    return false;
                }
            } else if self.state.is_dirty() {
                if !self.launch() {
                    return false;
                }
            } else {
                return true;
            }
        }
    }

    /// Stops for good. Memory has moved past what the journal holds, so
    /// carrying on would build on work a restart would not remember.
    fn fail(&mut self, reason: String) -> bool {
        *self.shared.failure.lock() = Some(reason);
        self.inbox.close();
        false
    }

    fn handle(&mut self, op: Op, now: u64, after: &mut Vec<After>, flushes: &mut Vec<oneshot::Sender<()>>) {
        match op {
            Op::Submit { queue, mut command, reply } => {
                if command.created_at == 0 {
                    command.created_at = now;
                }
                let id = self.state.enqueue(&queue, Arc::new(command));
                after.push(answer(reply, id));
            }
            Op::Append { drafts, reply } => {
                let events = drafts.into_iter().map(|draft| self.state.append(draft, now)).collect();
                after.push(answer(reply, events));
            }
            Op::Dequeue { queue, max, visibility, wait, reply } => {
                self.state.queues.entry(queue).or_default().waiters.push_back(Waiter { max, visibility, deadline: wait.map(|wait| now.saturating_add(wait)), reply });
            }
            Op::Begin { queue, id, reply } => {
                let begin = self.state.begin(&queue, id, now);
                after.push(answer(reply, begin));
            }
            Op::Owns { key, attempt, reply } => {
                let owned = self.state.owns(&key, attempt);
                after.push(answer(reply, owned));
            }
            Op::Finish { command, attempt, outcome, queue, id, reply } => {
                let done = self.state.finish(&command, attempt, outcome, &queue, id, now);
                after.push(answer(reply, done));
            }
            Op::Release { key, attempt, queue, id, retry, reply } => {
                let released = self.state.release(&key, attempt, &queue, id, retry, now);
                after.push(answer(reply, released));
            }
            Op::Emit { command, attempt, emit, reply } => {
                let written = self.state.emit(&command, attempt, emit, now);
                if let Some(reply) = reply {
                    after.push(answer(reply, written));
                }
            }
            Op::Ack { queue, id, reply } => {
                let removed = self.state.ack(&queue, id);
                after.push(answer(reply, removed));
            }
            Op::Nack { queue, id, delay, reply } => {
                let returned = self.state.nack(&queue, id, delay, now);
                after.push(answer(reply, returned));
            }
            Op::Flush { reply } => flushes.push(reply),
            Op::Compact { reply } => self.compactions.push(reply),
            Op::Shutdown { reply } => self.stopping = Some(reply),
            Op::DeadLetters { queue, reply } => {
                let letters = self.state.queues.get(&queue).map(|queue| queue.dead.iter().map(|(_, command)| command.clone()).collect()).unwrap_or_default();
                after.push(answer(reply, letters));
            }
        }
    }

    /// Hands messages to parked consumers and answers the ones whose patience
    /// ran out. This is what an arriving message, a returned message and a
    /// passed deadline all lead to; no consumer ever asks twice.
    fn serve_waiters(&mut self, now: u64, after: &mut Vec<After>) {
        let mut delivered = 0u64;
        for queue in self.state.queues.values_mut() {
            while let Some(waiter) = queue.waiters.front() {
                // A consumer that stopped waiting must not be handed messages
                // nobody will then process.
                if waiter.reply.is_closed() {
                    queue.waiters.pop_front();
                    continue;
                }
                let taken = queue.take(waiter.max, waiter.visibility, now);
                if taken.is_empty() {
                    break;
                }
                delivered += taken.len() as u64;
                let waiter = queue.waiters.pop_front().expect("front was just observed");
                after.push(answer(waiter.reply, taken));
            }
            let expired = |waiter: &Waiter| waiter.deadline.is_some_and(|deadline| deadline <= now);
            if queue.waiters.iter().any(expired) {
                let (gone, waiting): (Vec<_>, Vec<_>) = queue.waiters.drain(..).partition(expired);
                queue.waiters.extend(waiting);
                for waiter in gone {
                    after.push(answer(waiter.reply, Vec::new()));
                }
            }
        }
        if delivered > 0 {
            self.shared.counters.delivered.fetch_add(delivered, Ordering::Relaxed);
        }
    }
}
