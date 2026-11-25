//! Handler shapes, the "later" end of a handler, and catching a panic per
//! future instead of per task.

use super::Context;
use std::fmt;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

pub(crate) type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// A handler for claimable events of one type.
pub(crate) type HandlerFn = Arc<dyn Fn(Context) -> BoxFuture<Result<(), Retry>> + Send + Sync>;

pub(crate) fn handler<F, Fut>(function: F) -> HandlerFn
where
    F: Fn(Context) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), Retry>> + Send + 'static,
{
    Arc::new(move |context| Box::pin(function(context)))
}

/// "Do it again later": the two non-complete ends of a handler (2.9).
///
/// | end | sends | failure count |
/// | --- | --- | --- |
/// | `Ok(())` (success, or a permanent failure with failure follow-ups) | `Complete` with the follow-ups | n/a |
/// | [`Retry::later`] (**the default**: not this event's fault, e.g. a downstream outage) | `Release{delay, failed: false}` | unchanged |
/// | [`Retry::failed`] (handling this event failed) | `Release{delay, failed: true}` | rises; the server isolates at `isolate_after` |
///
/// How often and how long to wait is the handler's call; how many failures
/// isolate an event is the server's `isolate_after`. The SDK never counts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Retry {
    /// `None`: the service's default delay.
    pub delay: Option<Duration>,
    pub failed: bool,
    pub reason: String,
}

impl Retry {
    /// Not this event's fault: visible again after `delay`, failure count unchanged.
    pub fn later(delay: Duration) -> Self {
        Retry { delay: Some(delay), failed: false, reason: String::new() }
    }

    /// Handling this event failed: visible again after `delay`, one more failure counted.
    pub fn failed(delay: Duration) -> Self {
        Retry { delay: Some(delay), failed: true, reason: String::new() }
    }

    pub fn because(mut self, reason: impl Into<String>) -> Self {
        self.reason = reason.into();
        self
    }
}

impl fmt::Display for Retry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "retry later (failed: {}): {}", self.failed, self.reason)
    }
}

impl std::error::Error for Retry {}

/// A bare reason is the default end: later, not this event's fault.
impl From<&str> for Retry {
    fn from(reason: &str) -> Self {
        Retry { delay: None, failed: false, reason: reason.to_owned() }
    }
}

impl From<String> for Retry {
    fn from(reason: String) -> Self {
        Retry { delay: None, failed: false, reason }
    }
}

/// Resolves to `Err(())` if polling the inner future panicked.
pub(crate) struct CatchUnwind<T> {
    inner: BoxFuture<T>,
}

impl<T> CatchUnwind<T> {
    pub(crate) fn new(inner: BoxFuture<T>) -> Self {
        CatchUnwind { inner }
    }
}

impl<T> Future for CatchUnwind<T> {
    type Output = Result<T, ()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let inner = &mut self.inner;
        match catch_unwind(AssertUnwindSafe(|| inner.as_mut().poll(cx))) {
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(_) => Poll::Ready(Err(())),
        }
    }
}
