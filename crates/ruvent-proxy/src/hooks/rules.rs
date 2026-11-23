//! The default rules, as functions an overriding hook can call.

use crate::auth::User;
use crate::message::{Publish, Rejection};

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
