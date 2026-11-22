use crate::envelope::Envelope;
use crate::worker::{Context, Failure, Handler, WorkerConfig};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// A named group of handlers with one queue and one concurrency budget.
///
/// Group handlers by how they wait, not by topic. Millisecond bookkeeping,
/// minute-long inference and untrusted commands belong in three services:
/// then a backlog in one cannot occupy the budget of another, and each can be
/// sized by what stands behind it.
pub struct Service {
    pub(crate) name: String,
    handlers: HashMap<String, Arc<dyn Handler>>,
    pub(crate) accepts: Vec<String>,
    pub(crate) reacts: Vec<String>,
    pub(crate) config: WorkerConfig,
    pub(crate) transient: bool,
}

impl Service {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into(), handlers: HashMap::new(), accepts: Vec::new(), reacts: Vec::new(), config: WorkerConfig::default(), transient: false }
    }

    /// Handles a command clients may submit. Registering it is what allows it.
    pub fn accept(mut self, action: impl Into<String>, handler: impl Handler) -> Self {
        let action = action.into();
        self.handlers.insert(action.clone(), Arc::new(handler));
        self.accepts.push(action);
        self
    }

    /// Reacts to a recorded fact. Other services may react to the same fact;
    /// each gets its own copy and none waits for another.
    pub fn on(mut self, action: impl Into<String>, handler: impl Handler) -> Self {
        let action = action.into();
        self.handlers.insert(action.clone(), Arc::new(handler));
        self.reacts.push(action);
        self
    }

    /// Executions in flight at once. Size it by what downstream can absorb.
    pub fn max_in_flight(mut self, max: usize) -> Self {
        self.config.max_in_flight = max.max(1);
        self
    }

    /// The longest one execution may run. When it passes, the handler is
    /// dropped and the step is retried. There is no step without a limit.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.config.timeout = timeout;
        self
    }

    /// Never journals this service's work. A crash drops whatever was queued
    /// for it or running in it. For work a client will simply ask for again
    /// — and worth it when that work is plentiful and short-lived.
    pub fn transient(mut self) -> Self {
        self.transient = true;
        self
    }

    pub fn config(mut self, config: WorkerConfig) -> Self {
        self.config = config;
        self
    }

    pub(crate) fn into_router(self) -> Router {
        Router { name: self.name, handlers: self.handlers }
    }
}

/// Dispatches a service's queue by action.
pub(crate) struct Router {
    name: String,
    handlers: HashMap<String, Arc<dyn Handler>>,
}

#[async_trait]
impl Handler for Router {
    async fn handle(&self, cx: Context, command: Arc<Envelope>) -> Result<Bytes, Failure> {
        match self.handlers.get(&*command.action) {
            Some(handler) => handler.handle(cx, command).await,
            // Pretending to have handled an unknown action is worse than failing loudly.
            None => Err(Failure::Fatal(format!("service `{}` has no handler for `{}`", self.name, command.action))),
        }
    }
}
