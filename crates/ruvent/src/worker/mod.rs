//! The worker frame.
//!
//! Everything about *being* a worker lives here — taking messages, taking
//! ownership of executions, bounding how long a step may run, recording the
//! result, deleting the message afterwards. What a task actually does is the
//! one hole, filled by a [`Handler`].
//!
//! A handler is expected to do one bounded thing and stop. Control flow belongs
//! to the reaction table, not to any handler: a long job is a chain of short
//! executions, each owned for only as long as its own step.
//!
//! ## No lease, no heartbeat
//!
//! A lease with a heartbeat exists to tell a dead worker from a slow one when
//! the two are in different processes. Here they are not. The task that runs a
//! handler is supervised by the task that took the message: it sees the
//! handler return, fail, panic or overrun the moment that happens, because it
//! is awaiting it. Nothing has to be renewed and nothing has to expire.
//!
//! What a heartbeat never caught — a handler that is alive and stuck — is
//! caught here by the one thing that does catch it: a deadline on the step.
//! It costs one timer registration per execution and no wakeups unless it
//! actually fires.

mod context;

use crate::engine::{Begin, Delivery, Engine, Outcome};
use crate::envelope::{Envelope, Text};
use async_trait::async_trait;
use bytes::Bytes;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub use context::{Context, Lost};

/// Why a handler did not produce a result.
#[derive(Debug)]
pub enum Failure {
    /// The work cannot succeed. Recorded as a terminal failure; not retried.
    Fatal(String),
    /// The work may succeed later. The execution is given up and run again.
    Retry { after: Duration },
}

impl Failure {
    pub fn fatal(error: impl std::fmt::Display) -> Self {
        Failure::Fatal(error.to_string())
    }

    pub fn retry(after: Duration) -> Self {
        Failure::Retry { after }
    }
}

/// Losing ownership mid-handler is not a result to record: whoever took over
/// will record their own.
impl From<Lost> for Failure {
    fn from(_: Lost) -> Self {
        Failure::Retry { after: Duration::ZERO }
    }
}

#[async_trait]
pub trait Handler: Send + Sync + 'static {
    /// Runs one execution. The returned bytes become the payload of the
    /// `<action>.result` event sent to whoever submitted the command.
    ///
    /// The future is dropped the moment the step overruns its deadline, so
    /// anything it owns — a child process with kill-on-drop, an open request —
    /// goes with it. CPU-bound work belongs in `spawn_blocking`; this runs on
    /// the async pool.
    async fn handle(&self, cx: Context, command: Arc<Envelope>) -> Result<Bytes, Failure>;
}

#[async_trait]
impl<F, Fut> Handler for F
where
    F: Fn(Context, Arc<Envelope>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Bytes, Failure>> + Send + 'static,
{
    async fn handle(&self, cx: Context, command: Arc<Envelope>) -> Result<Bytes, Failure> {
        self(cx, command).await
    }
}

/// How long to wait before running a failed step again.
#[derive(Clone, Copy, Debug)]
pub struct Backoff {
    pub first: Duration,
    pub max: Duration,
}

impl Backoff {
    /// Doubles with each attempt already made, up to `max`.
    pub fn after(&self, attempts: u32) -> Duration {
        self.first.saturating_mul(1u32 << attempts.saturating_sub(1).min(16)).min(self.max)
    }
}

#[derive(Clone, Debug)]
pub struct WorkerConfig {
    /// Executions in flight across every queue this worker watches. Size it
    /// by what downstream can absorb, not by core count: a waiting handler
    /// costs a task, not a thread.
    pub max_in_flight: usize,
    /// Most messages taken per request.
    pub batch: usize,
    /// The longest one execution may run before it is dropped and retried.
    pub timeout: Duration,
    /// Delay before a step that overran or panicked is run again.
    pub backoff: Backoff,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self { max_in_flight: 256, batch: 32, timeout: Duration::from_secs(10 * 60), backoff: Backoff { first: Duration::from_secs(1), max: Duration::from_secs(60) } }
    }
}

/// A worker under construction: which queues it watches and what answers on each.
pub struct Worker {
    engine: Engine,
    config: WorkerConfig,
    routes: Vec<(Text, Arc<dyn Handler>)>,
}

impl Worker {
    pub fn new(engine: Engine, config: WorkerConfig) -> Self {
        Self { engine, config, routes: Vec::new() }
    }

    /// Watches a queue. The queue name is the address: the same action on two
    /// queues can mean two different jobs, and each queue has its own handler.
    pub fn route(mut self, queue: impl Into<Text>, handler: impl Handler) -> Self {
        self.routes.push((queue.into(), Arc::new(handler)));
        self
    }

    pub fn start(self) -> Running {
        let stop = CancellationToken::new();
        let permits = Arc::new(Semaphore::new(self.config.max_in_flight));
        let lanes = self.routes.into_iter().map(|(queue, handler)| tokio::spawn(lane(self.engine.clone(), queue, handler, self.config.clone(), permits.clone(), stop.clone()))).collect();
        Running { stop, lanes, permits, max_in_flight: self.config.max_in_flight }
    }
}

pub struct Running {
    stop: CancellationToken,
    lanes: Vec<JoinHandle<()>>,
    permits: Arc<Semaphore>,
    max_in_flight: usize,
}

impl Running {
    /// Stops taking work and waits for what is in flight to finish.
    pub async fn shutdown(self) {
        self.stop.cancel();
        for lane in self.lanes {
            let _ = lane.await;
        }
        let _ = self.permits.acquire_many(self.max_in_flight as u32).await;
    }
}

/// One reading lane per queue. All lanes draw on one in-flight budget, and a
/// lane never takes a message it has no permit to run.
///
/// An idle lane is parked inside the engine with no deadline: it is woken by
/// a message arriving or by shutdown, and by nothing else.
async fn lane(engine: Engine, queue: Text, handler: Arc<dyn Handler>, config: WorkerConfig, permits: Arc<Semaphore>, stop: CancellationToken) {
    loop {
        let first = tokio::select! {
            _ = stop.cancelled() => return,
            permit = permits.clone().acquire_owned() => match permit { Ok(permit) => permit, Err(_) => return },
        };
        let mut held = vec![first];
        while held.len() < config.batch {
            match permits.clone().try_acquire_owned() {
                Ok(permit) => held.push(permit),
                Err(_) => break,
            }
        }
        let taken = tokio::select! {
            _ = stop.cancelled() => return,
            taken = engine.dequeue(queue.clone(), held.len(), None, None) => taken,
        };
        // The only way this fails is the engine having stopped.
        let Ok(deliveries) = taken else { return };
        for delivery in deliveries {
            let permit = held.pop().expect("the engine returns at most what was asked for");
            tokio::spawn(execute(engine.clone(), queue.clone(), handler.clone(), config.clone(), delivery, permit));
        }
    }
}

async fn execute(engine: Engine, queue: Text, handler: Arc<dyn Handler>, config: WorkerConfig, delivery: Delivery, _permit: OwnedSemaphorePermit) {
    let Ok(Begin::Claimed { command, attempt, attempts }) = engine.begin(queue.clone(), delivery.id).await else {
        // A duplicate, already settled, conflicting, exhausted or gone: the
        // engine already did whatever that called for.
        return;
    };
    let cancel = CancellationToken::new();
    let cx = Context::new(engine.clone(), command.clone(), attempt, cancel.clone());

    // Its own task, so a panic is contained and reported here as a failed step.
    let mut job = tokio::spawn({
        let command = command.clone();
        async move { handler.handle(cx, command).await }
    });
    let finished = tokio::select! {
        finished = tokio::time::timeout(config.timeout, &mut job) => finished.ok(),
        _ = cancel.cancelled() => None,
    };

    let retry = |after: Duration| engine.release(command.transaction_key.clone(), attempt, queue.clone(), delivery.id, after);
    match finished {
        // Overran its deadline, or found it no longer owns the step.
        None => {
            job.abort();
            let _ = retry(config.backoff.after(attempts)).await;
        }
        Some(Err(_panicked)) => {
            let _ = retry(config.backoff.after(attempts)).await;
        }
        Some(Ok(Err(Failure::Retry { after }))) => {
            let _ = retry(after).await;
        }
        Some(Ok(Ok(value))) => {
            let _ = engine.finish(command, attempt, Outcome::Ok(value), queue, delivery.id).await;
        }
        Some(Ok(Err(Failure::Fatal(message)))) => {
            let _ = engine.finish(command, attempt, Outcome::Failed(Bytes::from(message)), queue, delivery.id).await;
        }
    }
}
