//! Where a domain plugs into the proxy.
//!
//! Implement only the methods you need; the others keep the configured
//! behaviour. Each default is also a public function ([`rules`]), so an
//! override can run it and add its own rule.
//!
//! | hook | called | default |
//! | --- | --- | --- |
//! | [`Hooks::authenticate`] | once per HTTP request; once per WebSocket, twice when the first answer is [`Denied::Missing`] and the token then comes in band | `auth.mode` of the configuration ([`Context::authenticate_by_config`]) |
//! | [`Hooks::subscribe`] | before a user's subscription is sent upstream; the target is a channel or a prefix | the user's scope covers the target ([`within_scope`]) |
//! | [`Hooks::publish`] | after the request's own shape is checked, before it is registered; may change it | the user's scope on the channel and the reply channel ([`publish_within_scope`]) |
//! | [`Hooks::deliver`] | in the hub, for each event and each user subscribed to its channel | every event goes through |
//!
//! `deliver` runs on the hub's one task, as many times per event as the
//! channel has users. It is synchronous on purpose: it must decide from
//! what it holds — the user and the event — without waiting, or every user
//! of the proxy waits with it. A `deliver` that panics costs that user the
//! event and its connection (a user that misses an event is disconnected);
//! the hub carries on. The other hooks are async and run on the user's own
//! task; awaiting there holds up only that user.
//!
//! | file | role |
//! | --- | --- |
//! | `mod.rs` | the [`Hooks`] trait and [`Defaults`] |
//! | `context.rs` | [`Context`]: what a hook reaches besides its arguments, and [`UpstreamCalls`] |
//! | `rules.rs` | the default rules as functions |

mod context;
pub mod rules;

pub use context::{Context, UpstreamCalls};
pub use rules::{publish_within_scope, within_scope};

use crate::auth::{Denied, User};
use crate::handshake::Handshake;
use crate::message::{Publish, Rejection};
use ruvent::{async_trait, Envelope, Target};

#[async_trait]
pub trait Hooks: Send + Sync + 'static {
    /// Who is this? `Err(Denied::Missing)` lets a protocol that can ask for
    /// credentials in band (WebSocket) ask once and call this again.
    async fn authenticate(&self, cx: Context<'_>, handshake: &Handshake) -> Result<User, Denied> {
        cx.authenticate_by_config(handshake.token.as_deref()).await
    }

    /// May `user` subscribe to `target`: one channel, or every channel below a prefix?
    async fn subscribe(&self, cx: Context<'_>, user: &User, target: &Target) -> Result<(), Rejection> {
        let _ = cx;
        within_scope(user, target)
    }

    /// May `user` register this? The hook may also change it: rename the
    /// channel, set the correlation, replace the payload. What it leaves is
    /// registered as it is, so a hook that rewrites should check the result
    /// (for instance with [`publish_within_scope`]) after rewriting.
    async fn publish(&self, cx: Context<'_>, user: &User, publish: &mut Publish) -> Result<(), Rejection> {
        let _ = cx;
        publish_within_scope(user, publish)
    }

    /// Does `user` see this event? Called for every user whose subscription takes its channel.
    fn deliver(&self, user: &User, event: &Envelope) -> bool {
        let _ = (user, event);
        true
    }
}

/// Every hook as configured.
pub struct Defaults;

impl Hooks for Defaults {}

#[cfg(test)]
mod tests;
