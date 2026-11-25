//! Build a proxy server for end users.
//!
//! A proxy is the event servers' dispatch fan-out layer (plan 4장): it
//! connects to every event server of a list, holds one upstream
//! subscription per channel (or prefix) per server however many users it
//! serves, and hands each event to its users. A server's dispatch cost is
//! then proportional to the number of proxies, not of users. This crate is
//! the whole proxy: there is no separate binary per protocol. You pick the
//! protocols as features and write a three-line `main`:
//!
//! ```no_run
//! # #[cfg(any(feature = "ws", feature = "http"))]
//! #[tokio::main]
//! async fn main() -> Result<(), ruvent_proxy::LaunchError> {
//!     // `my-proxy proxy.json`: everything it decides comes from the JSON file.
//!     ruvent_proxy::Server::new().run().await
//! }
//! # #[cfg(not(any(feature = "ws", feature = "http")))]
//! # fn main() {}
//! ```
//!
//! Your own rules are hooks: implement only the methods of [`Hooks`] you
//! need, and the rest keep the configured behaviour. A hook trusts nothing
//! the client sent until it has checked it — here a session cookie is
//! looked up in the program's own session table:
//!
//! ```no_run
//! # #[cfg(any(feature = "ws", feature = "http"))]
//! # mod example {
//! use ruvent_proxy::{async_trait, Context, Denied, Handshake, Hooks, User};
//! use std::collections::HashMap;
//!
//! /// session id → user name. A real program asks its session store.
//! struct Sessions(HashMap<String, String>);
//!
//! #[async_trait]
//! impl Hooks for Sessions {
//!     async fn authenticate(&self, _cx: Context<'_>, handshake: &Handshake) -> Result<User, Denied> {
//!         let session = handshake.cookie("session").ok_or(Denied::Missing)?;
//!         let name = self.0.get(session).ok_or(Denied::Rejected)?;
//!         Ok(User::new(name.as_str(), [format!("chat.{name}.*")]))
//!     }
//! }
//!
//! # async fn run(sessions: HashMap<String, String>) -> Result<(), ruvent_proxy::LaunchError> {
//! ruvent_proxy::Server::new().hooks(Sessions(sessions)).run().await
//! # }
//! # }
//! ```
//!
//! | module | what it is |
//! | --- | --- |
//! | [`hooks`] | where a domain plugs in: authenticate, subscribe, publish, deliver |
//! | [`server`] | the builder that turns a configuration and hooks into a running server (feature `ws` or `http`) |
//! | [`protocol`] | the routes of each protocol feature, mountable in your own `axum` app |
//! | [`handshake`] | what a user's connection or request carried: headers, query, peer, token |
//! | [`config`] | the JSON file |
//! | [`auth`] | end users, and the configured modes: `none`, `static` tokens (Q1(c)), an authentication worker (Q1(a)) |
//! | [`hub`] | the upstream subscriptions and the fan-out to users; publishing and request–response |
//! | [`health`] | what `/health` reports: each listed server's connections |
//! | [`message`] | the user-facing JSON, the same for every protocol, and the raw event stream |
//! | [`upstream`] | where the upstream connections go: connector, server list, credential |
//!
//! A user that was disconnected misses what was sent meanwhile (dispatch is
//! one-shot, 0.3); a user whose send buffer fills is disconnected.

pub mod auth;
pub mod config;
pub mod handshake;
pub mod health;
pub mod hooks;
pub mod hub;
pub mod message;
pub mod upstream;

#[cfg(any(feature = "ws", feature = "http"))]
pub mod protocol;
#[cfg(any(feature = "ws", feature = "http"))]
pub mod server;

pub use auth::{Denied, User};
pub use config::ProxyConfig;
pub use handshake::Handshake;
pub use health::{Health, ServerHealth};
pub use hooks::{Context, Defaults, Hooks, UpstreamCalls};
pub use hub::{bad_request, Proxy, UserHandle, UserId};
pub use message::{Cause, Outcome, Outgoing, Publish, PublishKind, Rejection, Request};
#[cfg(any(feature = "ws", feature = "http"))]
pub use protocol::router;
pub use ruvent::{async_trait, Envelope, Target};
pub use ruvent_client::requester::{Part, RequestError, RequestOptions, Response, Stream};
#[cfg(any(feature = "ws", feature = "http"))]
pub use server::{Bound, LaunchError, Server};
pub use upstream::{Connector, Local, ServerList, Servers, Tcp, Upstream};
