//! The observer (2.9 "관찰자"): one observation connection per server, the
//! subscriptions said again at every connection, and every dispatch event
//! handed over as the envelope it is (no re-encoding: what a proxy fans out).
//!
//! A connection closed with `Bye{slow}` (or lost) is reconnected; what was
//! dispatched meanwhile is gone (dispatch is one-shot, 0.3) and the stream
//! says so with [`Observed::Gap`].
//!
//! **When a subscription takes effect, per server.** The observer numbers
//! the [`Observer::subscribe`] calls in the order it handles them: the k-th
//! call has ticket k. Every subscribe it sends carries a ticket — the call's
//! own, or, when the subscriptions are said again at a new connection, the
//! number of calls handled so far — and the server's answer comes back in
//! the stream as [`Observed::Subscribed`] with that ticket, behind every
//! delivery the server sent before it. So the k-th call is in effect on a
//! server from its first `Subscribed` for that channel with `ticket ≥ k`:
//! what follows it in the stream was sent after the subscription took
//! effect (a proxy uses this to never hand a user an event sent before its
//! own join).
//!
//! | state | key | scope | lifetime | bound | cleared by |
//! | --- | --- | --- | --- | --- | --- |
//! | subscriptions | channel | observer | `subscribe` ~ `unsubscribe` | channels asked | the caller |
//! | answers waited for | (server, request number) | connection | asked ~ answered or disconnection | calls in progress | SDK |
//! | subscribe tickets | observer | observer | start ~ drop | one counter | never cleared |
//! | server list and lines | server address | process | while listed | servers | SDK |

use crate::line::backoff::Reconnect;
use crate::line::{Event, Line, Spec};
use crate::link::Connector;
use crate::servers::{Server, ServerList};
use parking_lot::Mutex;
use ruvent::{ByeReason, Call, Channel, Envelope, Hello, Link, Outbound, Refusal, Stats};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

/// Every value is a field with its default.
#[derive(Clone, Debug)]
pub struct ObserverConfig {
    pub credential: String,
    pub reconnect: Reconnect,
    /// Capacity of the stream of [`Observed`]. A reader that falls behind
    /// stops the reading of the connections; the server ends a connection
    /// that falls past its bound (S7) with `Bye{slow}`.
    pub queue: usize,
}

impl Default for ObserverConfig {
    fn default() -> Self {
        ObserverConfig { credential: String::new(), reconnect: Reconnect::default(), queue: 65_536 }
    }
}

/// What an observer hands over, per server in arrival order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Observed {
    /// A dispatch event, as sent.
    Delivery { server: Server, event: Envelope },
    /// Claimable work on a subscribed channel: head and work key. With its
    /// isolated bit set, the event was isolated (B6): the work gave up.
    Notice { server: Server, event: Envelope },
    /// Subscribed again on a new connection; `restarted`: the server restarted.
    Up { server: Server, restarted: bool },
    /// `server` confirmed a subscription to `channel` sent with `ticket` (see
    /// the module's "When a subscription takes effect"). Comes before the
    /// subscription's backlog notices and before the call's answer.
    Subscribed { server: Server, channel: Channel, ticket: u64 },
    /// The connection ended; what was dispatched until the next `Up` is lost.
    Gap { server: Server, bye: Option<ByeReason> },
}

enum Ask {
    Subscribe { collect: Option<Arc<Collect>>, ticket: u64 },
    Unsubscribe(Option<Arc<Collect>>),
    Stats(oneshot::Sender<Result<Stats, Refusal>>),
    Drain(oneshot::Sender<Result<(), Refusal>>),
}

/// Answers still awaited, the caller to tell, the first refusal.
type Collecting = (usize, Option<oneshot::Sender<Result<(), Refusal>>>, Option<Refusal>);

/// The answers of one call sent to every connected server.
struct Collect {
    state: Mutex<Collecting>,
}

impl Collect {
    fn new(count: usize, done: oneshot::Sender<Result<(), Refusal>>) -> Arc<Self> {
        let collect = Arc::new(Collect { state: Mutex::new((count, Some(done), None)) });
        if count == 0 {
            collect.finish();
        }
        collect
    }

    fn answer(&self, outcome: Result<(), Refusal>) {
        let mut state = self.state.lock();
        state.0 = state.0.saturating_sub(1);
        if let Err(refusal) = outcome {
            state.2.get_or_insert(refusal);
        }
        let finished = state.0 == 0;
        drop(state);
        if finished {
            self.finish();
        }
    }

    fn finish(&self) {
        let mut state = self.state.lock();
        let outcome = state.2.map_or(Ok(()), Err);
        if let Some(done) = state.1.take() {
            let _ = done.send(outcome);
        }
    }
}

enum Command {
    Subscribe(Channel, oneshot::Sender<Result<(), Refusal>>),
    Unsubscribe(Channel, oneshot::Sender<Result<(), Refusal>>),
    Stats(Server, oneshot::Sender<Result<Stats, Refusal>>),
    Drain(Server, oneshot::Sender<Result<(), Refusal>>),
}

/// An observer. Cheap to clone; stops when every clone is dropped.
#[derive(Clone)]
pub struct Observer {
    commands: mpsc::Sender<Command>,
    _task: Arc<Task>,
}

struct Task(JoinHandle<()>);

impl Drop for Task {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl Observer {
    /// Starts connecting to every server; returns the handle and the stream.
    pub fn start(connector: Arc<dyn Connector>, servers: &dyn ServerList, config: ObserverConfig) -> (Observer, mpsc::Receiver<Observed>) {
        let (out, stream) = mpsc::channel(config.queue.max(1));
        let (commands, command_rx) = mpsc::channel(64);
        let actor = Actor { connector, hello: Hello::observer(&config.credential), config, out, channels: BTreeSet::new(), lines: HashMap::new(), tickets: 0 };
        let task = tokio::spawn(actor.run(servers.watch(), command_rx));
        (Observer { commands, _task: Arc::new(Task(task)) }, stream)
    }

    /// Subscribes on every connected server (and every later connection).
    /// Answers once every connected server answered; the first refusal wins.
    pub async fn subscribe(&self, channel: Channel) -> Result<(), Refusal> {
        self.call(|done| Command::Subscribe(channel, done)).await?
    }

    pub async fn unsubscribe(&self, channel: Channel) -> Result<(), Refusal> {
        self.call(|done| Command::Unsubscribe(channel, done)).await?
    }

    /// One server's statistics (operator grant).
    pub async fn stats(&self, server: &Server) -> Result<Stats, Refusal> {
        self.call(|done| Command::Stats(server.clone(), done)).await?
    }

    /// Starts draining one server (operator grant, 2.8).
    pub async fn drain(&self, server: &Server) -> Result<(), Refusal> {
        self.call(|done| Command::Drain(server.clone(), done)).await?
    }

    async fn call<T>(&self, make: impl FnOnce(oneshot::Sender<T>) -> Command) -> Result<T, Refusal> {
        let (done, wait) = oneshot::channel();
        self.commands.send(make(done)).await.map_err(|_| Refusal::Closed)?;
        wait.await.map_err(|_| Refusal::Closed)
    }
}

struct Live {
    epoch: u64,
    link: Arc<dyn Link>,
    next_req: u32,
    asks: HashMap<u32, Ask>,
}

impl Live {
    async fn ask(&mut self, make: impl FnOnce(u32) -> Call, ask: Ask) {
        let req = self.next_req;
        self.next_req = self.next_req.wrapping_add(1).max(1);
        self.asks.insert(req, ask);
        if self.link.send(make(req)).await.is_err() {
            if let Some(ask) = self.asks.remove(&req) {
                fail(ask);
            }
        }
    }
}

/// An ask whose connection went: subscriptions count as answered (they are
/// said again at the next connection), the rest fail as closed.
fn fail(ask: Ask) {
    match ask {
        Ask::Subscribe { collect: Some(collect), .. } | Ask::Unsubscribe(Some(collect)) => collect.answer(Ok(())),
        Ask::Subscribe { collect: None, .. } | Ask::Unsubscribe(None) => {}
        Ask::Stats(done) => {
            let _ = done.send(Err(Refusal::Closed));
        }
        Ask::Drain(done) => {
            let _ = done.send(Err(Refusal::Closed));
        }
    }
}

struct ObserveLine {
    _line: Line,
    live: Option<Live>,
}

struct Actor {
    connector: Arc<dyn Connector>,
    hello: Hello,
    config: ObserverConfig,
    out: mpsc::Sender<Observed>,
    channels: BTreeSet<Channel>,
    lines: HashMap<Server, ObserveLine>,
    /// `subscribe` calls handled so far: the last ticket given (see the module's
    /// "When a subscription takes effect"). One counter, never reset.
    tickets: u64,
}

impl Actor {
    async fn run(mut self, mut list: watch::Receiver<BTreeSet<Server>>, mut commands: mpsc::Receiver<Command>) {
        let (events, mut incoming) = mpsc::channel::<Event>(1024);
        let mut listed = true;
        // The list's owner may drop it: the servers named last stay.
        let mut watching = true;
        loop {
            if listed {
                let wanted = list.borrow_and_update().clone();
                self.follow_list(&wanted, &events).await;
                listed = false;
            }
            tokio::select! {
                changed = list.changed(), if watching => match changed {
                    Ok(()) => listed = true,
                    Err(_) => watching = false,
                },
                Some(event) = incoming.recv() => {
                    if !self.event(event).await {
                        return;
                    }
                }
                command = commands.recv() => match command {
                    Some(command) => self.command(command).await,
                    None => return,
                },
            }
        }
    }

    async fn follow_list(&mut self, wanted: &BTreeSet<Server>, events: &mpsc::Sender<Event>) {
        let gone: Vec<Server> = self.lines.keys().filter(|server| !wanted.contains(*server)).cloned().collect();
        for server in gone {
            if let Some(line) = self.lines.remove(&server) {
                for (_, ask) in line.live.into_iter().flat_map(|live| live.asks) {
                    fail(ask);
                }
            }
        }
        for server in wanted {
            if self.lines.contains_key(server) {
                continue;
            }
            let spec = Spec { server: server.clone(), hello: self.hello.clone(), connector: self.connector.clone(), reconnect: self.config.reconnect };
            self.lines.insert(server.clone(), ObserveLine { _line: Line::spawn(spec, events.clone()), live: None });
        }
    }

    /// Returns `false` once nobody reads the stream.
    async fn event(&mut self, event: Event) -> bool {
        match event {
            Event::Up { server, epoch, link, restarted, .. } => {
                let channels: Vec<Channel> = self.channels.iter().copied().collect();
                let ticket = self.tickets;
                let Some(line) = self.lines.get_mut(&server) else { return true };
                let mut live = Live { epoch, link, next_req: 1, asks: HashMap::new() };
                for channel in channels {
                    live.ask(|req| Call::Subscribe { req, channel, after: None }, Ask::Subscribe { collect: None, ticket }).await;
                }
                line.live = Some(live);
                self.out.send(Observed::Up { server, restarted }).await.is_ok()
            }
            Event::Message { server, epoch, message, .. } => {
                let Some(line) = self.lines.get_mut(&server) else { return true };
                let Some(live) = line.live.as_mut().filter(|live| live.epoch == epoch) else { return true };
                match *message {
                    Outbound::Delivery(event) => return self.out.send(Observed::Delivery { server, event }).await.is_ok(),
                    Outbound::Notice(event) => return self.out.send(Observed::Notice { server, event }).await.is_ok(),
                    Outbound::Subscribed { req, channel, backlog, .. } => {
                        if let Some(Ask::Subscribe { collect, ticket }) = live.asks.remove(&req) {
                            // The confirmation goes into the stream before the caller is
                            // answered: whoever acts on the answer finds it already queued.
                            if self.out.send(Observed::Subscribed { server: server.clone(), channel, ticket }).await.is_err() {
                                return false;
                            }
                            if let Some(collect) = collect {
                                collect.answer(Ok(()));
                            }
                        }
                        for event in backlog {
                            if self.out.send(Observed::Notice { server: server.clone(), event }).await.is_err() {
                                return false;
                            }
                        }
                    }
                    Outbound::Unsubscribed { req } => {
                        if let Some(Ask::Unsubscribe(Some(collect))) = live.asks.remove(&req) {
                            collect.answer(Ok(()));
                        }
                    }
                    Outbound::StatsReply { req, stats } => {
                        if let Some(Ask::Stats(done)) = live.asks.remove(&req) {
                            let _ = done.send(Ok(*stats));
                        }
                    }
                    Outbound::Draining { req } => {
                        if let Some(Ask::Drain(done)) = live.asks.remove(&req) {
                            let _ = done.send(Ok(()));
                        }
                    }
                    Outbound::Refused { req, reason } => match live.asks.remove(&req) {
                        Some(Ask::Subscribe { collect: Some(collect), .. } | Ask::Unsubscribe(Some(collect))) => collect.answer(Err(reason)),
                        Some(Ask::Stats(done)) => {
                            let _ = done.send(Err(reason));
                        }
                        Some(Ask::Drain(done)) => {
                            let _ = done.send(Err(reason));
                        }
                        _ => {}
                    },
                    _ => {}
                }
                true
            }
            Event::Down { server, epoch, bye, .. } => {
                let Some(line) = self.lines.get_mut(&server) else { return true };
                if line.live.as_ref().is_none_or(|live| live.epoch != epoch) {
                    return true;
                }
                if let Some(live) = line.live.take() {
                    for (_, ask) in live.asks {
                        fail(ask);
                    }
                }
                self.out.send(Observed::Gap { server, bye }).await.is_ok()
            }
            Event::Fatal { server, reason, .. } => self.out.send(Observed::Gap { server, bye: Some(reason) }).await.is_ok(),
        }
    }

    async fn command(&mut self, command: Command) {
        match command {
            Command::Subscribe(channel, done) => {
                self.tickets += 1;
                let ticket = self.tickets;
                self.channels.insert(channel);
                let collect = Collect::new(self.lines.values().filter(|line| line.live.is_some()).count(), done);
                for line in self.lines.values_mut() {
                    if let Some(live) = line.live.as_mut() {
                        live.ask(|req| Call::Subscribe { req, channel, after: None }, Ask::Subscribe { collect: Some(collect.clone()), ticket }).await;
                    }
                }
            }
            Command::Unsubscribe(channel, done) => {
                self.channels.remove(&channel);
                let collect = Collect::new(self.lines.values().filter(|line| line.live.is_some()).count(), done);
                for line in self.lines.values_mut() {
                    if let Some(live) = line.live.as_mut() {
                        live.ask(|req| Call::Unsubscribe { req, channel }, Ask::Unsubscribe(Some(collect.clone()))).await;
                    }
                }
            }
            Command::Stats(server, done) => match self.lines.get_mut(&server).and_then(|line| line.live.as_mut()) {
                Some(live) => live.ask(|req| Call::Stats { req }, Ask::Stats(done)).await,
                None => {
                    let _ = done.send(Err(Refusal::Closed));
                }
            },
            Command::Drain(server, done) => match self.lines.get_mut(&server).and_then(|line| line.live.as_mut()) {
                Some(live) => live.ask(|req| Call::Drain { req }, Ask::Drain(done)).await,
                None => {
                    let _ = done.send(Err(Refusal::Closed));
                }
            },
        }
    }
}
