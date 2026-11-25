//! What a hook can reach besides its arguments.

use crate::auth::{Denied, User};
use crate::config::ProxyConfig;
use crate::hub::Proxy;
use ruvent::{Envelope, EventId, Name, WorkKey};
use ruvent_client::producer::{ProducerError, Sent};
use ruvent_client::requester::{RequestError, RequestOptions, Requester, Response, Stream};
use ruvent_client::servers::Server;

/// The proxy's configuration and its upstream connections, as a hook sees them.
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

    /// The proxy's producer and requester, until the proxy stops. Calls on
    /// them run under the proxy's service credential.
    pub fn upstream(&self) -> Option<UpstreamCalls> {
        self.proxy.requester().ok().map(|requester| UpstreamCalls { requester })
    }

    /// The default authentication: `auth.mode` of the configuration, with this token.
    pub async fn authenticate_by_config(&self, token: Option<&str>) -> Result<User, Denied> {
        self.proxy.authenticate_by_config(token).await
    }
}

/// The calls a hook may make on the proxy's upstream connections: send and
/// delete on the production connections (2.3), request–response on the
/// reply channel (2.8).
///
/// Subscribing is left out on purpose. The hub decides when a user's
/// subscription takes effect from the confirmations of the subscribes it
/// issued on its observation connections; a subscribe from anywhere else
/// would be taken for one of them, and an unsubscribe would cancel the
/// subscription every user of that channel relies on. A hook that needs its
/// own subscription opens its own observer.
#[derive(Clone)]
pub struct UpstreamCalls {
    requester: Requester,
}

impl UpstreamCalls {
    /// Registers `event` on a server the selector picks; returns once it is handed over.
    pub async fn send(&self, event: Envelope) -> Result<Sent, ProducerError> {
        self.requester.producer().send(event).await
    }

    /// [`UpstreamCalls::send`], then waits for the server's cumulative `Committed`.
    pub async fn send_committed(&self, event: Envelope) -> Result<Sent, ProducerError> {
        self.requester.producer().send_committed(event).await
    }

    /// Deletes a waiting event on `server`; the outcome is a dispatch event on `result` with correlation `tag`.
    pub async fn delete(&self, server: &Server, id: EventId, result: Name, tag: EventId, key: Option<WorkKey>) -> Result<(), ProducerError> {
        self.requester.producer().delete(server, id, result, tag, key).await
    }

    /// A request and its whole answer ([`crate::Proxy::exchange_with`]).
    pub async fn exchange(&self, event: Envelope, options: RequestOptions) -> Result<Response, RequestError> {
        self.requester.request(event, options).await
    }

    /// A request and its answer as it arrives: fragments, then the result.
    pub async fn exchange_stream(&self, event: Envelope, options: RequestOptions) -> Result<Stream, RequestError> {
        self.requester.stream(event, options).await
    }
}
