//! The automatic extension (2.4, B7): every run's lease is extended at half
//! its length. One one-shot sleep to the earliest extension due, none while
//! no run is due; not a periodic timer.

use std::time::Duration;
use tokio::time::Instant;

/// When the next `Extend` of a run is due: half a lease before its end.
pub(crate) fn due(now: Instant, left: Duration, lease: Duration) -> Instant {
    now + left.saturating_sub(lease / 2)
}

/// Resolves at `at`; never, without one.
pub(crate) async fn until(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep(at.saturating_duration_since(Instant::now())).await,
        None => std::future::pending().await,
    }
}
