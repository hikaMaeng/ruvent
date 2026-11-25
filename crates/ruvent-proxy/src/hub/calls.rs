//! What a proxy sends upstream on the caller's task: registrations through
//! the SDK producer (2.3) and request–response through the SDK requester
//! (2.8). Neither touches the hub task.
//!
//! **Registrations.** A user's publish is numbered on the production
//! connection of a server the selector picks and kept until that server's
//! cumulative `Committed`: [`Proxy::publish`] answers once it is committed.
//! There is no answer per registration: an active id with the same content
//! is dropped by the server without a word (a resend), and configuration
//! faults (a credential that may not write a channel, an id clash) are told
//! once to the proxy's connection, not to the user (2.7). The proxy's own
//! checks — the request's shape, the publish hook — are what turn a user away.
//!
//! **Request–response.** [`Proxy::exchange`] registers a request with
//! `reply` set to the proxy instance's reply channel (`reply.<instance id>`)
//! and a fresh `correlation`, and waits for the result: a claimable event
//! the worker registers on that channel as a follow-up of its completion.
//! Nothing is subscribed or joined per request. The waiter belongs to the
//! correlation and ends with the result, a cancel (dropping the stream) or
//! the deadline — **not** with a lost connection: the result waits on the
//! server and is assigned once the proxy is back. Stream fragments come on
//! the same work connection, ahead of the result. A request past its
//! deadline also leaves the server's queue (`expires`); one already claimed
//! runs to its end and its result waits out the result's own `expires`.

use super::Proxy;
use crate::message::{Outcome, Publish, Rejection};
use crate::User;
use ruvent::{Envelope, EventId};
use ruvent_client::requester::{RequestError, RequestOptions, Response, Stream};
use std::time::Duration;

impl Proxy {
    /// Registers what a user asked for and answers once it is committed.
    ///
    /// Order: the request's own shape ([`Publish::check`], invalid → 400),
    /// then [`crate::Hooks::publish`] (which may change it), then the
    /// registration as the hook left it. An event without an id gets a
    /// UUIDv7 here, so the answer names the id the event carries.
    pub async fn publish(&self, user: &User, mut publish: Publish) -> Outcome {
        let reference = publish.reference.clone();
        match self.try_publish(user, &mut publish).await {
            Ok(id) => Outcome::ok(reference, Some(id.to_string())),
            Err(rejection) => Outcome::rejected(reference, rejection),
        }
    }

    async fn try_publish(&self, user: &User, publish: &mut Publish) -> Result<EventId, Rejection> {
        publish.check()?;
        self.core.hooks.publish(self.context(), user, publish).await?;
        let delegate = self.core.config.delegate.then_some(user.name.as_str());
        let mut event = publish.envelope(delegate)?;
        if event.id.is_nil() {
            event.id = EventId::new_v7();
        }
        let requester = self.requester()?;
        // A send waits while no server is connected (2.9); a user is told now instead.
        if !self.core.health.borrow().any_up() {
            return Err(Rejection::unavailable("upstream unavailable"));
        }
        requester.producer().send_committed(event).await.map(|sent| sent.id).map_err(Rejection::from_producer)
    }

    /// A request and its whole answer, with the configured deadline (`exchange.deadline_ms`).
    pub async fn exchange(&self, event: Envelope) -> Result<Response, RequestError> {
        let options = RequestOptions { deadline: Duration::from_millis(self.core.config.exchange.deadline_ms), retries: 0 };
        self.exchange_with(event, options).await
    }

    /// A request and its whole answer, with these limits.
    pub async fn exchange_with(&self, event: Envelope, options: RequestOptions) -> Result<Response, RequestError> {
        let requester = self.requester().map_err(|_| RequestError::Closed)?;
        requester.request(event, options).await
    }

    /// A request and its answer as it arrives: fragments, then the result.
    pub async fn exchange_stream(&self, event: Envelope, options: RequestOptions) -> Result<Stream, RequestError> {
        let requester = self.requester().map_err(|_| RequestError::Closed)?;
        requester.stream(event, options).await
    }
}
