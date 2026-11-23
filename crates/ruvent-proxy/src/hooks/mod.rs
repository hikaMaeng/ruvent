//! Where a domain plugs into the proxy.
//!
//! Implement only the methods you need; the others keep the configured
//! behaviour. Each default is also a public function, so an override can
//! run it first and add its own rule after.
//!
//! | hook | called | default |
//! | --- | --- | --- |
//! | [`Hooks::authenticate`] | once per WebSocket, once per HTTP request | `auth.mode` of the configuration ([`Context::authenticate_by_config`]) |
//! | [`Hooks::subscribe`] | before a user's subscription is sent upstream | the user's channel scope ([`within_scope`]) |
//! | [`Hooks::publish`] | before a user's registration; may change it | the user's scope on the channel and the reply channel ([`publish_within_scope`]) |
//! | [`Hooks::deliver`] | in the hub, for each event and each user subscribed to its channel | every event goes through |
//!
//! `deliver` runs on the hub's one task, as many times per event as the
//! channel has users. It is synchronous on purpose: it must decide from
//! what it holds — the user and the event — without waiting, or every user
//! of the proxy waits with it. The other hooks are async and run on the
//! user's own task; awaiting there holds up only that user.

use crate::auth::{Denied, User};
use crate::config::ProxyConfig;
use crate::handshake::Handshake;
use crate::hub::Proxy;
use crate::message::{Publish, Rejection};
use ruvent::{async_trait, Envelope, Methods};
use std::sync::Arc;

#[async_trait]
pub trait Hooks: Send + Sync + 'static {
    /// Who is this? `Err(Denied::Missing)` lets a protocol that can ask for
    /// credentials in band (WebSocket) ask once and call this again.
    async fn authenticate(&self, cx: Context<'_>, handshake: &Handshake) -> Result<User, Denied> {
        cx.authenticate_by_config(handshake.token.as_deref()).await
    }

    /// May `user` subscribe to `channel`?
    async fn subscribe(&self, cx: Context<'_>, user: &User, channel: &str) -> Result<(), Rejection> {
        let _ = cx;
        within_scope(user, channel)
    }

    /// May `user` register this? The hook may also change it: rename the
    /// channel, set the correlation, replace the payload.
    async fn publish(&self, cx: Context<'_>, user: &User, publish: &mut Publish) -> Result<(), Rejection> {
        let _ = cx;
        publish_within_scope(user, publish)
    }

    /// Does `user` see this event? Called for every user subscribed to its channel.
    fn deliver(&self, user: &User, event: &Envelope) -> bool {
        let _ = (user, event);
        true
    }
}

/// Every hook as configured.
pub struct Defaults;

impl Hooks for Defaults {}

/// What a hook can reach besides its arguments.
#[derive(Clone, Copy)]
pub struct Context<'a> {
    proxy: &'a Proxy,
}

impl<'a> Context<'a> {
    pub(crate) fn new(proxy: &'a Proxy) -> Context<'a> {
        Context { proxy }
    }

    pub fn config(&self) -> &'a ProxyConfig {
        self.proxy.config()
    }

    /// The proxy's connection to the event server, while it is up. Calls on
    /// it run under the proxy's service credential.
    pub fn upstream(&self) -> Option<Arc<dyn Methods>> {
        self.proxy.upstream().ok()
    }

    /// The default authentication: `auth.mode` of the configuration, with this token.
    pub async fn authenticate_by_config(&self, token: Option<&str>) -> Result<User, Denied> {
        self.proxy.authenticate_by_config(token).await
    }
}

/// The default subscribe rule: the channel is within the user's scope.
pub fn within_scope(user: &User, channel: &str) -> Result<(), Rejection> {
    if user.may_use(channel) {
        Ok(())
    } else {
        Err(Rejection::forbidden("forbidden channel"))
    }
}

/// The default publish rule: the channel, and the reply channel if any, are within the user's scope.
pub fn publish_within_scope(user: &User, publish: &Publish) -> Result<(), Rejection> {
    within_scope(user, &publish.channel)?;
    match &publish.reply {
        Some(reply) if !user.may_use(reply) => Err(Rejection::forbidden("forbidden reply channel")),
        _ => Ok(()),
    }
}
