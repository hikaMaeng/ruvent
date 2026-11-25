//! One run: the handler under its run limit (B7), and the end it reports.

use super::handler::{CatchUnwind, HandlerFn};
use super::{Context, End, Message, RunKey};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::AbortHandle;

/// Runs `handler` and reports its end on `messages`, behind the run's
/// emissions (one channel, so an emission never overtakes its completion).
pub(crate) fn spawn(handler: HandlerFn, context: Context, limit: Duration, retry_delay: Duration, key: RunKey, messages: mpsc::Sender<Message>) -> AbortHandle {
    tokio::spawn(async move {
        let ran = tokio::time::timeout(limit, CatchUnwind::new(handler(context.clone()))).await;
        let end = match ran {
            Ok(Ok(Ok(()))) => End::Complete(context.take_follow_ups()),
            Ok(Ok(Err(retry))) => End::Release { delay: retry.delay.unwrap_or(retry_delay), failed: retry.failed },
            // A panic is this event's failure.
            Ok(Err(())) => End::Release { delay: retry_delay, failed: true },
            // Past the run limit: this event's failure, unless the run was
            // held up by a paused channel (B23).
            Err(_) => End::Release { delay: retry_delay, failed: !context.blocked() },
        };
        let _ = messages.send(Message::Done { key, end }).await;
    })
    .abort_handle()
}
