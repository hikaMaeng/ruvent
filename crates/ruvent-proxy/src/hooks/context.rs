//! What a hook can reach besides its arguments.

use crate::auth::{Denied, User};
use crate::config::ProxyConfig;
use crate::hub::Proxy;
use ruvent::{ClaimToken, Envelope, EventId, Methods, Name, Refusal, WorkKey};
use std::sync::Arc;
use std::time::Duration;

/// The proxy's configuration and its upstream connection, as a hook sees them.
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
    pub fn upstream(&self) -> Option<UpstreamCalls> {
        self.proxy.upstream().ok().map(|methods| UpstreamCalls { methods })
    }

    /// The default authentication: `auth.mode` of the configuration, with this token.
    pub async fn authenticate_by_config(&self, token: Option<&str>) -> Result<User, Denied> {
        self.proxy.authenticate_by_config(token).await
    }
}

/// The calls a hook may make on the proxy's upstream connection: register,
/// claim, complete, delete.
///
/// Subscribe and unsubscribe are left out on purpose. The hub decides when
/// a user's subscription takes effect by counting the markers of the
/// subscribes it issued on this connection; a subscribe from anywhere else
/// would be counted as one of them, and an unsubscribe would cancel the
/// subscription every user of that channel relies on. A hook that needs
/// its own subscription opens its own connection.
#[derive(Clone)]
pub struct UpstreamCalls {
    methods: Arc<dyn Methods>,
}

impl UpstreamCalls {
    pub async fn register_claimable(&self, event: Envelope) -> Result<(EventId, WorkKey), Refusal> {
        self.methods.register_claimable(event).await
    }

    pub async fn register_dispatch(&self, event: Envelope) -> Result<EventId, Refusal> {
        self.methods.register_dispatch(event).await
    }

    pub async fn claim(&self, id: EventId, worker: u32, lease: Duration) -> Result<Envelope, Refusal> {
        self.methods.claim(id, worker, lease).await
    }

    pub async fn complete(&self, id: EventId, token: ClaimToken, follow_ups: Vec<Envelope>) -> Result<(), Refusal> {
        self.methods.complete(id, token, follow_ups).await
    }

    pub async fn delete(&self, id: EventId, result: Name, tag: EventId, key: Option<WorkKey>) -> Result<bool, Refusal> {
        self.methods.delete(id, result, tag, key).await
    }
}
