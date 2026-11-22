//! What every protocol proxy shares.
//!
//! A proxy is a finished binary with a JSON configuration and no user code.
//! It holds one connection to the event server and one upstream subscription
//! per channel, however many users it serves, so the event server's
//! connection count is "proxies + worker servers" (proposal §4.3, §6.5).
//!
//! * [`config`] — the JSON file.
//! * [`auth`] — end users: `none`, `static` tokens (Q1(c)), or an
//!   authentication worker answering a claimable event (Q1(a)).
//! * [`hub`] — the upstream subscriptions and the fan-out to users.
//! * [`message`] — the user-facing JSON, the same for every protocol.
//! * [`upstream`] — where the upstream connection comes from.
//!
//! A user that was disconnected misses what was sent meanwhile (Q8(a)); a
//! user whose send buffer fills is disconnected.

pub mod auth;
pub mod config;
pub mod hub;
pub mod message;
pub mod upstream;

pub use auth::{Denied, User};
pub use config::ProxyConfig;
pub use hub::{bad_request, Proxy, UserHandle, UserId};
pub use message::{Outcome, Publish, PublishKind, Request};
pub use upstream::{Connector, InProcess, Remote};
