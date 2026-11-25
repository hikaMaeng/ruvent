//! Closing in stages (2.2).
//!
//! Writing `Bye` and closing at once sends a reset when the peer's last
//! bytes are still unread, and a reset can erase the `Bye` and the final
//! `Committed` before the peer reads them (RFC 9112 9.6). So after every
//! `Bye`: write everything, shut the write side, read and drop until the
//! peer closes, then close. A peer that never closes is closed at the
//! deadline (S20) — the only other place a connection waits on the clock.

use super::handshake::Refusal;
use bytes::Bytes;
use ruvent::Outbound;
use ruvent_protocol::io::v1_bye;
use ruvent_protocol::{Encoder, Message, Parts, RecordReader, VERSION};
use std::future::Future;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

/// Runs the steps of a close, giving up at `deadline`. `None`: the deadline passed.
pub(super) async fn staged<F: Future>(deadline: Duration, steps: F) -> Option<F::Output> {
    tokio::time::timeout(deadline, steps).await.ok()
}

/// Answers a connection that did not get past its first record, then closes in stages.
pub(super) async fn refuse(mut write: OwnedWriteHalf, mut reader: RecordReader<OwnedReadHalf>, mut encoder: Encoder, refusal: Refusal, deadline: Duration) {
    let mut parts = match refusal {
        Refusal::Gone => return,
        Refusal::VersionOne => Parts::new(vec![Bytes::from(v1_bye(&format!("unsupported protocol version 1; this server speaks {VERSION}")))]),
        Refusal::Bye(reason) => {
            if encoder.push(&Message::Outbound(Outbound::Bye(reason))).is_err() {
                return;
            }
            encoder.take()
        }
    };
    staged(deadline, async move {
        if parts.write_to(&mut write).await.is_ok() && write.shutdown().await.is_ok() {
            let _ = reader.discard().await;
        }
    })
    .await;
}
