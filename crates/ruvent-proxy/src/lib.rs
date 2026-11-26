//! The skeleton of a proxy: the event servers' dispatch fan-out (plan 4장).
//!
//! A proxy connects to every event server of a list, holds one upstream
//! subscription per channel per server however many members it
//! serves, and hands each event to its members. A server's dispatch cost is
//! then proportional to the number of proxies, not of users.
//!
//! The event servers speak one protocol, the ruvent wire over TCP; proxies
//! and worker servers reach them only through it (the client SDK). Everything
//! a proxy shows its own clients is the proxy program's: the protocols it
//! serves (WebSocket, HTTP, WebRTC, …), who a client is and what it may
//! subscribe to, what a client may publish, request deadlines and
//! back-pressure toward a slow client. This crate decides none of it. It
//! gives the part every proxy needs and gets wrong easily: the per-server
//! upstream and the subscription rule.
//!
//! ```no_run
//! use ruvent_proxy::{Channel, Fanout, FanoutConfig, Servers, Tcp, Upstream};
//! use std::sync::Arc;
//!
//! # async fn run() -> Result<(), ruvent_proxy::SubscribeError> {
//! let upstream = Upstream::new(Arc::new(Tcp::new("proxy-token")), Servers::new(["10.0.0.1:7400", "10.0.0.2:7400"]));
//! let fanout = Fanout::start(FanoutConfig::default(), upstream);
//! // One per client connection of the program's own protocol:
//! let (member, mut events) = fanout.attach().await?;
//! let channel = Channel::new(); // the request's own stream, or one the client was told
//! member.subscribe(channel).await?; // after the program's own check
//! while let Some(event) = events.recv().await {
//!     // Write `event` to the client in the program's protocol.
//!     let _ = event;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! | module | what it is |
//! | --- | --- |
//! | [`fanout`] | the upstream subscriptions and the fan-out to members: [`Fanout`], [`Member`] |
//! | [`health`] | each listed server's observation connection, and readiness |
//! | [`upstream`] | where the upstream connections go: connector, server list, credential |
//!
//! A member that was disconnected misses what was sent meanwhile (dispatch
//! is one-shot, 0.3); a member whose stream fills is detached.

pub mod fanout;
pub mod health;
pub mod upstream;

pub use fanout::{Fanout, FanoutConfig, Member, MemberId, SubscribeError};
pub use health::{Health, ServerHealth};
pub use ruvent::{Channel, Envelope};
pub use upstream::{Connector, Local, Server, ServerList, Servers, Tcp, Upstream};
