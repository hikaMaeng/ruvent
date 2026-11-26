//! The worker (2.4, 2.9): one work connection per server, the services'
//! joins on each, windows over the servers by the one rule of
//! [`windows`], automatic extension, the run limit, and the three ends of a
//! handler.
//!
//! ```text
//! Assign ─► book: run or overflow (give back at once, failed: no)
//!   run ─► handler under the run limit ─► Ok: Complete(follow-ups) | Retry: Release{delay, failed}
//!   meanwhile: Extend at half the lease (one sleep to the earliest due)
//! run end ─► book.ended: Window changes first, then the completion
//! ```
//!
//! * **Restarts and reconnections.** Runs go on. After a reconnection the
//!   line sends again what was not acknowledged (completions kept through the
//!   restart), joins again, extends every run of that server (so the attempt
//!   belongs to the new connection's join, 2.4) and only then says its window.
//!   A restart does not raise any failure count.
//! * **No retry limit here.** The SDK never counts failures and never
//!   completes without running the handler: the server's `isolate_after` is
//!   the one limit (B6).
//! * **Shutdown** ([`Worker::shutdown`]): every window to 0; assignments that
//!   arrive after that are given back (`failed: false`); running handlers get
//!   the shutdown grace; past it they are cut off and given back
//!   (`failed: false`, so a deploy never counts against an event).
//! * Dropping a [`Worker`] without [`Worker::shutdown`] is a crash: every
//!   task stops at once and the connections close.
//!
//! State (2.10, SDK):
//!
//! | state | key | scope | lifetime | bound | cleared by |
//! | --- | --- | --- | --- | --- | --- |
//! | producer id, nonce, next number | (server, work) | process | start ~ drop | servers | SDK |
//! | completion buffer | (server, seq) | work line | handed over ~ `Committed` | S8 (`workline`) | SDK (acknowledgement) |
//! | server list and lines | server address | process | while listed | servers | SDK (list changes) |
//! | reconnection back-off | server | line | disconnection ~ welcome | servers | SDK ([`crate::line`]) |
//! | execution slots | slot | worker service | the service's | P ([`Service::slots`]) | none |
//! | per-server window book | (server, channel) → window, in flight, starving | worker service | `Join` ~ the server leaves | servers × channels | SDK ([`windows::Book`]) |
//! | runs in flight | (server, token) → run, extension due, lease end, run limit | worker | `Assign` ~ completion, release or run limit | P per service | SDK (run end, shutdown) |

mod context;
mod extend;
mod handler;
mod run;
mod service;
mod shutdown;
pub mod windows;

pub use context::{Context, EmitError};
pub use handler::Retry;
pub use service::Service;

use crate::line::backoff::Reconnect;
use crate::line::{Event, Line, Spec};
use crate::link::Connector;
use crate::producer::Producer;
use crate::servers::{Server, ServerList};
use crate::workline::{Completion, WorkLine};
use handler::HandlerFn;
use parking_lot::Mutex;
use ruvent::{Assignment, ByeReason, Call, ClaimToken, Envelope, EventId, Hello, Name, Outbound, Refusal};
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::{AbortHandle, JoinHandle};
use tokio::time::Instant;
use windows::{Arrival, Book, Change, Seat};

/// Every value is a field with its default (chapter 8).
#[derive(Clone, Debug)]
pub struct WorkerConfig {
    /// The credential an in-process connection runs under (over TCP the token decides).
    pub credential: String,
    /// S20.
    pub reconnect: Reconnect,
    /// S8: completion bytes per line before further completions are held.
    pub unacked_bytes: u64,
    /// How long running handlers may finish after a shutdown began.
    pub shutdown_grace: Duration,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        WorkerConfig { credential: String::new(), reconnect: Reconnect::default(), unacked_bytes: 64 << 20, shutdown_grace: Duration::from_secs(30) }
    }
}

/// Counters since start.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkerStats {
    /// Runs started.
    pub runs: u64,
    pub completed: u64,
    /// `Release{failed: false}` and `Release{failed: true}` from handlers and limits.
    pub released_later: u64,
    pub released_failed: u64,
    /// Assignments past the window, given back at once (crossings, 2.4).
    pub overflow: u64,
    /// `Window` frames sent.
    pub windows: u64,
    /// Joins a server refused.
    pub refused: u64,
    /// Restarts noticed (`Welcome` with another server id or generation).
    pub restarts: u64,
}

/// One run's name: the server that assigned it and the attempt.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RunKey {
    pub server: Server,
    pub token: ClaimToken,
}

/// How a run ended.
pub(crate) enum End {
    Complete(Vec<Envelope>),
    Release { delay: Duration, failed: bool },
}

/// What runs tell the worker, in order per run.
pub(crate) enum Message {
    Emit { key: RunKey, event: Envelope, done: oneshot::Sender<Result<(), EmitError>> },
    Done { key: RunKey, end: End },
}

enum Command {
    Shutdown(oneshot::Sender<()>),
}

#[derive(Default)]
struct Counters {
    runs: AtomicU64,
    completed: AtomicU64,
    released_later: AtomicU64,
    released_failed: AtomicU64,
    overflow: AtomicU64,
    windows: AtomicU64,
    refused: AtomicU64,
    restarts: AtomicU64,
}

/// What a handle reads while the worker runs.
struct Shared {
    /// The window books, by service. Changed by the worker task only.
    books: Mutex<Vec<(Name, Book<Server>)>>,
    counters: Counters,
    refused: Mutex<Option<ByeReason>>,
}

/// Starts a worker: see [`Worker::builder`].
pub struct Builder {
    connector: Arc<dyn Connector>,
    servers: watch::Receiver<BTreeSet<Server>>,
    services: Vec<Service>,
    producer: Option<Producer>,
    config: WorkerConfig,
}

impl Builder {
    pub fn service(mut self, service: Service) -> Self {
        self.services.push(service);
        self
    }

    /// The producer [`Context::send`] registers through.
    pub fn producer(mut self, producer: Producer) -> Self {
        self.producer = Some(producer);
        self
    }

    pub fn config(mut self, config: WorkerConfig) -> Self {
        self.config = config;
        self
    }

    /// Starts connecting to every server. Returns at once.
    pub fn start(self) -> Worker {
        let books = self.services.iter().map(|service| (service.channel.clone(), Book::new(service.slots))).collect();
        let shared = Arc::new(Shared { books: Mutex::new(books), counters: Counters::default(), refused: Mutex::default() });
        let (commands, command_rx) = mpsc::channel(4);
        let (messages, message_rx) = mpsc::channel(1024);
        let mut hello = Hello::worker(&self.config.credential);
        hello.acks = true;
        let actor = Actor {
            shared: shared.clone(),
            connector: self.connector,
            hello,
            services: self.services,
            producer: self.producer,
            config: self.config,
            lines: HashMap::new(),
            runs: HashMap::new(),
            messages,
            shutting: None,
            done: Vec::new(),
        };
        let task = tokio::spawn(actor.run(self.servers, command_rx, message_rx));
        Worker { shared, commands, task }
    }
}

/// A running worker. Dropping it without [`Worker::shutdown`] stops it as a crash would.
pub struct Worker {
    shared: Arc<Shared>,
    commands: mpsc::Sender<Command>,
    task: JoinHandle<()>,
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Worker {
    pub fn builder(connector: Arc<dyn Connector>, servers: &dyn ServerList) -> Builder {
        Builder { connector, servers: servers.watch(), services: Vec::new(), producer: None, config: WorkerConfig::default() }
    }

    /// The shutdown procedure (2.9 "종료"). Returns when every run ended or
    /// was given back and the connections are closed.
    pub async fn shutdown(mut self) {
        let (done, wait) = oneshot::channel();
        if self.commands.send(Command::Shutdown(done)).await.is_ok() {
            let _ = wait.await;
        }
        let _ = (&mut self.task).await;
    }

    /// The window book of every service now: (channel, server, seat).
    pub fn windows(&self) -> Vec<(Name, Server, Seat)> {
        let books = self.shared.books.lock();
        books.iter().flat_map(|(channel, book)| book.seats().iter().map(move |(server, seat)| (channel.clone(), server.clone(), *seat))).collect()
    }

    pub fn stats(&self) -> WorkerStats {
        let counters = &self.shared.counters;
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        WorkerStats {
            runs: load(&counters.runs),
            completed: load(&counters.completed),
            released_later: load(&counters.released_later),
            released_failed: load(&counters.released_failed),
            overflow: load(&counters.overflow),
            windows: load(&counters.windows),
            refused: load(&counters.refused),
            restarts: load(&counters.restarts),
        }
    }

    /// Why a server ended the worker's connections for good (`token`, `version`, `superseded`).
    pub fn refused(&self) -> Option<ByeReason> {
        *self.shared.refused.lock()
    }
}

/// One run in flight.
struct Running {
    service: usize,
    id: EventId,
    lease: Duration,
    /// When the next `Extend` is due.
    extend_at: Instant,
    /// When the lease runs out unless extended (the local estimate).
    lease_end: Instant,
    abort: AbortGuard,
}

/// Aborts the run's task when the run is dropped (shutdown past its grace, a crash).
struct AbortGuard(AbortHandle);

impl Drop for AbortGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct Actor {
    shared: Arc<Shared>,
    connector: Arc<dyn Connector>,
    hello: Hello,
    services: Vec<Service>,
    producer: Option<Producer>,
    config: WorkerConfig,
    lines: HashMap<Server, WorkLine>,
    runs: HashMap<RunKey, Running>,
    messages: mpsc::Sender<Message>,
    /// The end of the shutdown grace, once shutting down.
    shutting: Option<Instant>,
    done: Vec<oneshot::Sender<()>>,
}

impl Actor {
    async fn run(mut self, mut list: watch::Receiver<BTreeSet<Server>>, mut commands: mpsc::Receiver<Command>, mut messages: mpsc::Receiver<Message>) {
        let (events, mut incoming) = mpsc::channel::<Event>(1024);
        let mut listed = true;
        // The list's owner may drop it: the servers named last stay.
        let mut watching = true;
        loop {
            if listed && self.shutting.is_none() {
                let wanted = list.borrow_and_update().clone();
                self.follow_list(&wanted, &events).await;
                listed = false;
            }
            if self.shutting.is_some() && self.settled() {
                break;
            }
            let extend_at = self.runs.values().map(|run| run.extend_at).min();
            tokio::select! {
                changed = list.changed(), if watching && self.shutting.is_none() => match changed {
                    Ok(()) => listed = true,
                    Err(_) => watching = false,
                },
                Some(event) = incoming.recv() => self.event(event).await,
                Some(message) = messages.recv() => self.message(message).await,
                command = commands.recv() => match command {
                    Some(Command::Shutdown(done)) => {
                        self.done.push(done);
                        self.begin_shutdown().await;
                    }
                    None => {
                        if self.shutting.is_none() {
                            self.begin_shutdown().await;
                        }
                    }
                },
                _ = extend::until(extend_at) => self.extend_due().await,
                _ = shutdown::grace(self.shutting) => {
                    self.cut_off().await;
                    break;
                }
            }
        }
        for done in self.done.drain(..) {
            let _ = done.send(());
        }
    }

    /// Shutting down and nothing is left: no run, nothing unacknowledged.
    fn settled(&self) -> bool {
        self.runs.is_empty() && self.lines.values().all(|line| line.settled() || line.live.is_none())
    }

    async fn follow_list(&mut self, wanted: &BTreeSet<Server>, events: &mpsc::Sender<Event>) {
        let gone: Vec<Server> = self.lines.keys().filter(|server| !wanted.contains(*server)).cloned().collect();
        for server in gone {
            self.lines.remove(&server);
            for index in 0..self.services.len() {
                let changes = self.shared.books.lock()[index].1.remove(&server);
                self.send_windows(index, changes).await;
            }
        }
        for server in wanted {
            if self.lines.contains_key(server) {
                continue;
            }
            let spec = Spec { server: server.clone(), hello: self.hello.clone(), connector: self.connector.clone(), reconnect: self.config.reconnect };
            self.lines.insert(server.clone(), WorkLine::new(Line::spawn(spec, events.clone()), self.config.unacked_bytes));
            for (_, book) in self.shared.books.lock().iter_mut() {
                book.add(server.clone());
            }
        }
    }

    async fn event(&mut self, event: Event) {
        match event {
            Event::Up { server, epoch, link, welcome, restarted, .. } => {
                if restarted {
                    self.shared.counters.restarts.fetch_add(1, Ordering::Relaxed);
                }
                let services: Vec<(usize, Name, Vec<Name>, Duration)> =
                    self.services.iter().enumerate().map(|(index, service)| (index, service.channel.clone(), service.handlers.keys().cloned().collect(), service.lease)).collect();
                let Some(line) = self.lines.get_mut(&server) else { return };
                line.up(epoch, link, &welcome).await;
                for (index, channel, types, lease) in services {
                    line.join(index, channel, types, lease, false).await;
                }
            }
            Event::Message { server, epoch, message, .. } => {
                if !self.lines.get(&server).is_some_and(|line| line.is(epoch)) {
                    return;
                }
                self.outbound(server, *message).await;
            }
            Event::Down { server, epoch, .. } => {
                let Some(line) = self.lines.get_mut(&server) else { return };
                if !line.down(epoch) {
                    return;
                }
                for index in 0..self.services.len() {
                    let changes = self.shared.books.lock()[index].1.disconnect(&server);
                    self.send_windows(index, changes).await;
                }
            }
            Event::Fatal { reason, .. } => {
                *self.shared.refused.lock() = Some(reason);
            }
        }
    }

    async fn outbound(&mut self, server: Server, message: Outbound) {
        match message {
            Outbound::Joined { req } => {
                let Some(index) = self.lines.get_mut(&server).and_then(|line| line.answered(req)) else { return };
                self.joined(&server, index).await;
            }
            Outbound::Refused { req, reason } => {
                if let Some(line) = self.lines.get_mut(&server) {
                    line.answered(req);
                }
                if reason != Refusal::Closed {
                    self.shared.counters.refused.fetch_add(1, Ordering::Relaxed);
                }
            }
            Outbound::Assign(assignment) => self.assigned(server, assignment).await,
            Outbound::Pending { channel } => {
                let Some(index) = self.service(&channel) else { return };
                let changes = self.shared.books.lock()[index].1.pending(&server);
                self.send_windows(index, changes).await;
            }
            Outbound::Committed { seq } => {
                if let Some(line) = self.lines.get_mut(&server) {
                    line.committed(seq).await;
                }
            }
            _ => {}
        }
    }

    fn service(&self, channel: &Name) -> Option<usize> {
        self.services.iter().position(|service| service.channel == *channel)
    }

    /// The join took effect on a new connection: extend this server's runs of
    /// the service there (the attempts move to this join), then say the window.
    async fn joined(&mut self, server: &Server, index: usize) {
        let now = Instant::now();
        let mut extends = Vec::new();
        for (key, run) in self.runs.iter_mut() {
            if key.server == *server && run.service == index {
                extends.push(Call::Extend { id: run.id, token: key.token });
                run.extend_at = extend::due(now, run.lease, run.lease);
                run.lease_end = now + run.lease;
            }
        }
        if let Some(line) = self.lines.get(server) {
            for call in extends {
                line.tell(call).await;
            }
        }
        let changes = self.shared.books.lock()[index].1.connect(server);
        self.send_windows(index, changes).await;
    }

    async fn assigned(&mut self, server: Server, assignment: Assignment) {
        let Assignment { event, token, lease_ms, until, more } = assignment;
        let id = event.id;
        let index = self.service(&event.channel);
        let arrival = match index {
            Some(index) if self.shutting.is_none() => {
                let (arrival, changes) = self.shared.books.lock()[index].1.assigned(&server, more);
                self.send_windows(index, changes).await;
                arrival
            }
            _ => Arrival::Overflow,
        };
        let handler: Option<HandlerFn> = index.and_then(|index| self.services[index].handlers.get(&event.event_type).cloned());
        let (Arrival::Run, Some(index), Some(handler)) = (arrival, index, handler) else {
            if arrival == Arrival::Run {
                // No handler for the type (the join named only handled types): give it back.
                if let Some(index) = index {
                    self.shared.books.lock()[index].1.ended(&server);
                }
            }
            self.shared.counters.overflow.fetch_add(1, Ordering::Relaxed);
            if let Some(line) = self.lines.get(&server) {
                line.tell(Call::Release { id, token, delay_ms: 0, failed: false }).await;
            }
            return;
        };
        let service = &self.services[index];
        let lease = Duration::from_millis(lease_ms);
        let now = Instant::now();
        let left = Duration::from_millis(until);
        let key = RunKey { server, token };
        let context = Context::new(event, token, key.clone(), self.messages.clone(), self.producer.clone());
        let abort = run::spawn(handler, context, service.run_limit, service.retry_delay, key.clone(), self.messages.clone());
        self.shared.counters.runs.fetch_add(1, Ordering::Relaxed);
        self.runs.insert(key, Running { service: index, id, lease, extend_at: extend::due(now, left, lease), lease_end: now + left, abort: AbortGuard(abort) });
    }

    async fn message(&mut self, message: Message) {
        match message {
            Message::Emit { key, event, done } => {
                let outcome = match self.lines.get(&key.server) {
                    Some(line) if line.live.is_some() => {
                        line.tell(Call::Register { seq: None, event }).await;
                        Ok(())
                    }
                    _ => Err(EmitError::Disconnected),
                };
                let _ = done.send(outcome);
            }
            Message::Done { key, end } => {
                let Some(run) = self.runs.remove(&key) else { return };
                // Windows first: a lowered window reaches the server before the
                // completion frees the slot, so nothing crosses.
                let changes = self.shared.books.lock()[run.service].1.ended(&key.server);
                self.send_windows(run.service, changes).await;
                let Some(line) = self.lines.get_mut(&key.server) else { return };
                match end {
                    End::Complete(follow_ups) => {
                        self.shared.counters.completed.fetch_add(1, Ordering::Relaxed);
                        line.complete(Completion { id: run.id, token: key.token, follow_ups }).await;
                    }
                    End::Release { delay, failed } => {
                        let counter = if failed { &self.shared.counters.released_failed } else { &self.shared.counters.released_later };
                        counter.fetch_add(1, Ordering::Relaxed);
                        let delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX);
                        line.tell(Call::Release { id: run.id, token: key.token, delay_ms, failed }).await;
                    }
                }
            }
        }
    }

    /// Extends every run whose extension is due, on a live connection. A run
    /// whose lease the worker knows ran out (no extension could be sent in
    /// time) had its window closed by the server: the window is said again.
    async fn extend_due(&mut self) {
        let now = Instant::now();
        let mut calls: Vec<(Server, Call)> = Vec::new();
        let mut reopen: Vec<(usize, Server)> = Vec::new();
        for (key, run) in self.runs.iter_mut() {
            if run.extend_at > now {
                continue;
            }
            let live = self.lines.get(&key.server).is_some_and(|line| line.live.is_some());
            if !live {
                // Extended at the reconnection instead; do not wake for it until then.
                run.extend_at = now + run.lease;
                continue;
            }
            if run.lease_end <= now {
                reopen.push((run.service, key.server.clone()));
            }
            calls.push((key.server.clone(), Call::Extend { id: run.id, token: key.token }));
            run.extend_at = extend::due(now, run.lease, run.lease);
            run.lease_end = now + run.lease;
        }
        for (server, call) in calls {
            if let Some(line) = self.lines.get(&server) {
                line.tell(call).await;
            }
        }
        for (index, server) in reopen {
            let window = self.shared.books.lock()[index].1.seat(&server).map(|seat| seat.window);
            if let Some(window) = window {
                self.send_windows(index, vec![(server, window)]).await;
            }
        }
    }

    async fn send_windows(&self, index: usize, changes: Vec<Change<Server>>) {
        let service = &self.services[index];
        for (server, count) in changes {
            let Some(line) = self.lines.get(&server) else { continue };
            if line.live.is_none() {
                continue;
            }
            self.shared.counters.windows.fetch_add(1, Ordering::Relaxed);
            line.tell(Call::Window { channel: service.channel.clone(), count, bytes: service.window_bytes }).await;
        }
    }

    /// 2.9 "종료" 1: every window to 0, then wait for the runs.
    async fn begin_shutdown(&mut self) {
        if self.shutting.is_some() {
            return;
        }
        self.shutting = Some(Instant::now() + self.config.shutdown_grace);
        for index in 0..self.services.len() {
            let changes = self.shared.books.lock()[index].1.close();
            self.send_windows(index, changes).await;
        }
    }

    /// 2.9 "종료" 4: past the grace, cut the handlers off and give their
    /// events back as not their fault.
    async fn cut_off(&mut self) {
        let runs: Vec<(RunKey, Running)> = self.runs.drain().collect();
        for (key, run) in runs {
            drop(run.abort);
            self.shared.counters.released_later.fetch_add(1, Ordering::Relaxed);
            if let Some(line) = self.lines.get(&key.server) {
                line.tell(Call::Release { id: run.id, token: key.token, delay_ms: 0, failed: false }).await;
            }
        }
    }
}
