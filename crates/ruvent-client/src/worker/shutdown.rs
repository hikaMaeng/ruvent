//! The shutdown grace (2.9 "종료"): how long running handlers may finish
//! after every window went to 0. One one-shot sleep, armed only while
//! shutting down.

use tokio::time::Instant;

/// Resolves at the end of the grace; never, while not shutting down.
pub(crate) async fn grace(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep(at.saturating_duration_since(Instant::now())).await,
        None => std::future::pending().await,
    }
}
