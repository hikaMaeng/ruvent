//! Handler shapes, and catching a panic per future instead of per task.

use super::{Context, Failure};
use ruvent::Envelope;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};

pub(crate) type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// A handler for claimable events of one type.
pub(crate) type HandlerFn = Arc<dyn Fn(Context) -> BoxFuture<Result<(), Failure>> + Send + Sync>;

/// A listener for dispatch events on one channel.
pub(crate) type ListenerFn = Arc<dyn Fn(Envelope) -> BoxFuture<()> + Send + Sync>;

pub(crate) fn handler<F, Fut>(function: F) -> HandlerFn
where
    F: Fn(Context) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), Failure>> + Send + 'static,
{
    Arc::new(move |context| Box::pin(function(context)))
}

pub(crate) fn listener<F, Fut>(function: F) -> ListenerFn
where
    F: Fn(Envelope) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    Arc::new(move |event| Box::pin(function(event)))
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
