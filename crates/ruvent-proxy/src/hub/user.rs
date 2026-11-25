//! One attached user: its subscriptions, its requests, its results.

use super::task::Command;
use super::{Proxy, UserId};
use crate::auth::User;
use crate::message::{Outcome, Rejection, Request};
use ruvent::Target;
use std::sync::Arc;
use tokio::sync::oneshot;

/// One attached user. Dropping it detaches the user.
pub struct UserHandle {
    pub id: UserId,
    pub user: Arc<User>,
    proxy: Proxy,
}

impl UserHandle {
    pub(super) fn new(id: UserId, user: Arc<User>, proxy: Proxy) -> UserHandle {
        UserHandle { id, user, proxy }
    }

    /// Subscribes to one channel ([`UserHandle::subscribe_to`]).
    pub async fn subscribe(&self, channel: &str) -> Result<(), Rejection> {
        self.subscribe_to(Target::Channel(channel.into())).await
    }

    /// Subscribes once [`crate::Hooks::subscribe`] allows it, and returns once
    /// the subscription has taken effect on a server. A prefix target takes
    /// every channel below it at a `.` boundary.
    pub async fn subscribe_to(&self, target: Target) -> Result<(), Rejection> {
        self.proxy.core.hooks.subscribe(self.proxy.context(), &self.user, &target).await?;
        let stopped = || Rejection::unavailable("proxy stopped");
        let (reply, done) = oneshot::channel();
        self.proxy.commands.send(Command::Join { user: self.id, target, reply }).await.map_err(|_| stopped())?;
        done.await.map_err(|_| stopped())?
    }

    pub async fn unsubscribe(&self, channel: &str) {
        self.unsubscribe_from(Target::Channel(channel.into())).await;
    }

    pub async fn unsubscribe_from(&self, target: Target) {
        let _ = self.proxy.commands.send(Command::Leave { user: self.id, target }).await;
    }

    /// Puts a result into this user's stream, behind what is already there.
    pub async fn reply(&self, outcome: Outcome) {
        let _ = self.proxy.commands.send(Command::Send { user: self.id, text: outcome.text() }).await;
    }

    /// Handles one user message and puts its result into the stream.
    pub async fn handle(&self, request: Request) {
        let outcome = match request {
            Request::Auth { .. } => Outcome::failed(None, "already authenticated"),
            Request::Publish(publish) => self.proxy.publish(&self.user, publish).await,
            Request::Subscribe { target, reference } => match self.subscribe_to(target).await {
                Ok(()) => Outcome::ok(reference, None),
                Err(rejection) => Outcome::rejected(reference, rejection),
            },
            Request::Unsubscribe { target, reference } => {
                self.unsubscribe_from(target).await;
                Outcome::ok(reference, None)
            }
        };
        self.reply(outcome).await;
    }
}

impl Drop for UserHandle {
    fn drop(&mut self) {
        // If the queue is full the hub still drops this user at the next
        // delivery, which finds the stream closed.
        let _ = self.proxy.commands.try_send(Command::Detach { user: self.id });
    }
}
