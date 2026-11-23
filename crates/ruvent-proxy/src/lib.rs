//! Build a proxy server for end users.
//!
//! A proxy holds one connection to the event server and one upstream
//! subscription per channel, however many users it serves, so the event
//! server's connection count is "proxies + worker servers" (proposal §4.3,
//! §6.5). This crate is the whole proxy: there is no separate binary per
//! protocol. You pick the protocols as features and write a three-line
//! `main`:
//!
//! ```no_run
//! # #[cfg(feature = "ws")]
//! #[tokio::main]
//! async fn main() -> Result<(), ruvent_proxy::LaunchError> {
//!     // `my-proxy proxy.json`: everything it decides comes from the JSON file.
//!     ruvent_proxy::Server::new().run().await
//! }
//! # #[cfg(not(feature = "ws"))]
//! # fn main() {}
//! ```
//!
//! Your own rules are hooks: implement only the methods of [`Hooks`] you
//! need, and the rest keep the configured behaviour.
//!
//! ```no_run
//! # #[cfg(feature = "ws")]
//! # mod example {
//! use ruvent_proxy::{async_trait, Context, Denied, Handshake, Hooks, User};
//!
//! struct Sessions;
//!
//! #[async_trait]
//! impl Hooks for Sessions {
//!     async fn authenticate(&self, _cx: Context<'_>, handshake: &Handshake) -> Result<User, Denied> {
//!         match handshake.cookie("session") {
//!             Some(session) => Ok(User::new(session, ["chat.*"])),
//!             None => Err(Denied::Missing),
//!         }
//!     }
//! }
//!
//! # async fn run() -> Result<(), ruvent_proxy::LaunchError> {
//! ruvent_proxy::Server::new().hooks(Sessions).run().await
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
//! | [`hub`] | the upstream subscriptions and the fan-out to users |
//! | [`message`] | the user-facing JSON, the same for every protocol |
//! | [`upstream`] | where the upstream connection comes from |
//!
//! A user that was disconnected misses what was sent meanwhile (Q8(a)); a
//! user whose send buffer fills is disconnected.

pub mod auth;
pub mod config;
pub mod handshake;
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
pub use hooks::{Context, Defaults, Hooks};
pub use hub::{bad_request, Proxy, UserHandle, UserId};
pub use message::{Cause, Outcome, Publish, PublishKind, Rejection, Request};
#[cfg(any(feature = "ws", feature = "http"))]
pub use protocol::router;
pub use ruvent::{async_trait, Envelope};
#[cfg(any(feature = "ws", feature = "http"))]
pub use server::{Bound, LaunchError, Server};
pub use upstream::{Connector, InProcess, Remote};
