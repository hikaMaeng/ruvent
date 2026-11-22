use crate::engine::Engine;
use crate::envelope::{Audience, Emit, Envelope, Kind, Text};
use bytes::Bytes;
use std::fmt;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// This attempt no longer owns its execution.
#[derive(Debug)]
pub struct Lost;

impl fmt::Display for Lost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("this attempt no longer owns its execution")
    }
}

impl std::error::Error for Lost {}

/// What a handler is told about its own execution, and how it speaks.
///
/// Cloning is two reference counts and a token; pass it to whatever needs it.
#[derive(Clone)]
pub struct Context {
    engine: Engine,
    command: Arc<Envelope>,
    attempt: u64,
    cancel: CancellationToken,
}

impl Context {
    pub(crate) fn new(engine: Engine, command: Arc<Envelope>, attempt: u64, cancel: CancellationToken) -> Self {
        Self { engine, command, attempt, cancel }
    }

    /// The engine, for publishing or reading beyond this execution.
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Which attempt at this execution this is, as issued by the engine.
    pub fn attempt(&self) -> u64 {
        self.attempt
    }

    /// Says something mid-execution without waiting.
    ///
    /// Never blocks and never fails the handler. Emissions from one execution
    /// reach the log in the order they were made, because they travel one
    /// channel to one writer — no numbering scheme is needed to restore it.
    /// If the engine's inbox is full the emission is dropped and counted.
    ///
    /// Progress is never written to disk.
    pub fn progress(&self, action: impl Into<Text>, payload: impl Into<Bytes>) {
        let emit = Emit { action: action.into(), payload: payload.into(), kind: Kind::Progress, key: None, audience: Audience::Client };
        self.engine.emit_nowait(self.command.clone(), self.attempt, emit);
    }

    /// Records a fact.
    ///
    /// `key` says what the fact *is* — `"turn.started:<turn>"` — so a re-run
    /// records the same event rather than a second one. Every service that
    /// registered for `action` is issued its own event for it, in the same step.
    pub async fn fact(&self, key: impl Into<Text>, action: impl Into<Text>, payload: impl Into<Bytes>, audience: Audience) -> Result<(), Lost> {
        let emit = Emit { action: action.into(), payload: payload.into(), kind: Kind::Fact, key: Some(key.into()), audience };
        match self.engine.emit(self.command.clone(), self.attempt, emit).await {
            Ok(true) => Ok(()),
            _ => {
                self.cancel.cancel();
                Err(Lost)
            }
        }
    }

    /// Asks, now, whether this attempt still owns the execution.
    ///
    /// Everything recorded through this context is already fenced. Ask this
    /// before an effect that is *not* a record and cannot be taken back:
    /// starting a process, sending a message, spending money.
    pub async fn fence(&self) -> bool {
        let owned = self.engine.owns(self.command.transaction_key.clone(), self.attempt).await.unwrap_or(false);
        if !owned {
            self.cancel.cancel();
        }
        owned
    }

    /// Resolves when ownership is lost. The handler's future is dropped at the
    /// same moment, so this is for work it has handed to something else.
    pub fn cancelled(&self) -> tokio_util::sync::WaitForCancellationFuture<'_> {
        self.cancel.cancelled()
    }

    pub fn cancellation(&self) -> CancellationToken {
        self.cancel.clone()
    }
}
