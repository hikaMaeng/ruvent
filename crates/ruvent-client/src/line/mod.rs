//! A line: one server, one role, connected for as long as the server is in
//! the list (2.9 "공통").
//!
//! A line dials, says the same `Hello` every time (the same producer id and
//! instance nonce: a reconnection, never a second instance), and reports
//! what happens to its owner as `Event`s, in order. Between attempts it
//! waits [`Backoff`]. It never sends calls; the owner holds the link it was
//! handed in `Event::Up` and drops it at `Event::Down`.
//!
//! What a `Bye` makes the line do (2.9's table; the owner adds its part):
//!
//! | reason | the line |
//! | --- | --- |
//! | `superseded` | stops; `Event::Fatal`. So does a reconnection whose `Welcome` says "superseded" (2.3) |
//! | `draining` | reconnects with the ordinary back-off (S20): a restarted server is found within the ceiling. The producer keeps the server out of its candidates until it welcomes again |
//! | `slow` | reconnects (what was dispatched meanwhile is lost) |
//! | `version`, `token`, `forbidden` | stops; `Event::Fatal` |
//! | `protocol` | reconnects (the owner's numbering starts over from `Welcome.resume`) |
//!
//! A `Welcome` whose server id or generation differs from the previous one
//! is a restart: `restarted` in `Event::Up` (the generation rises only at a
//! start, 2.1).

pub mod backoff;

use crate::link::Connector;
use crate::remote::Refused;
use crate::servers::Server;
use backoff::{Backoff, Reconnect};
use ruvent::{ByeReason, Hello, Link, Outbound, Welcome};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// What a line tells its owner.
pub(crate) enum Event {
    /// Connected and welcomed. `epoch` counts this line's connections from 1.
    Up { server: Server, epoch: u64, link: Arc<dyn Link>, welcome: Welcome, restarted: bool },
    /// A message of connection `epoch`, in arrival order. `Bye` included.
    Message { server: Server, epoch: u64, message: Box<Outbound> },
    /// Connection `epoch` ended; `bye` is the reason the server gave, if any.
    Down { server: Server, epoch: u64, bye: Option<ByeReason> },
    /// The line stopped for good and will not reconnect.
    Fatal { server: Server, reason: ByeReason },
}

/// Whether a refusal ends the line.
fn fatal(reason: ByeReason) -> bool {
    matches!(reason, ByeReason::Superseded | ByeReason::Version | ByeReason::Token | ByeReason::Forbidden)
}

/// How to open one line.
pub(crate) struct Spec {
    pub server: Server,
    pub hello: Hello,
    pub connector: Arc<dyn Connector>,
    pub reconnect: Reconnect,
}

/// A running line. Dropping it stops the line at once: no further attempts,
/// and the connection's messages are no longer read (the owner drops its link).
pub(crate) struct Line {
    task: JoinHandle<()>,
}

impl Line {
    pub(crate) fn spawn(spec: Spec, events: mpsc::Sender<Event>) -> Line {
        Line { task: tokio::spawn(run(spec, events)) }
    }
}

impl Drop for Line {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn run(spec: Spec, events: mpsc::Sender<Event>) {
    let Spec { server, hello, connector, reconnect } = spec;
    let mut backoff = Backoff::new(reconnect);
    let mut epoch = 0u64;
    let mut identity: Option<([u8; 16], u32)> = None;
    let mut wait = Duration::ZERO;
    loop {
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
        let mut conn = match connector.open(&server, hello.clone()).await {
            Ok(conn) => conn,
            Err(error) => {
                if let Some(reason) = Refused::of(&error).filter(|reason| fatal(*reason)) {
                    let _ = events.send(Event::Fatal { server, reason }).await;
                    return;
                }
                wait = backoff.wait();
                continue;
            }
        };
        if conn.welcome.superseded && epoch > 0 {
            // 2.3: reconnected and found this id in use by another instance.
            drop(conn);
            let _ = events.send(Event::Fatal { server, reason: ByeReason::Superseded }).await;
            return;
        }
        backoff.reset();
        epoch += 1;
        let now = (conn.welcome.server_id, conn.welcome.generation);
        let restarted = identity.is_some_and(|before| before != now);
        identity = Some(now);
        let up = Event::Up { server: server.clone(), epoch, link: conn.link, welcome: conn.welcome, restarted };
        if events.send(up).await.is_err() {
            return;
        }
        let mut bye = None;
        loop {
            let Some(message) = conn.outbound.recv().await else { break };
            if let Outbound::Bye(reason) = message {
                bye = Some(reason);
            }
            if events.send(Event::Message { server: server.clone(), epoch, message: Box::new(message) }).await.is_err() {
                return;
            }
            if bye.is_some() {
                break;
            }
        }
        drop(conn.outbound);
        if events.send(Event::Down { server: server.clone(), epoch, bye }).await.is_err() {
            return;
        }
        if let Some(reason) = bye.filter(|reason| fatal(*reason)) {
            let _ = events.send(Event::Fatal { server, reason }).await;
            return;
        }
        wait = backoff.wait();
    }
}
