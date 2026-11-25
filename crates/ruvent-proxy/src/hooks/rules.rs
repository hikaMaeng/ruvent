//! The default rules, as functions an overriding hook can call.

use crate::auth::User;
use crate::message::{Publish, Rejection};
use ruvent::Target;

/// The default subscribe rule: the user's scope covers the target ([`User::covers`]).
pub fn within_scope(user: &User, target: &Target) -> Result<(), Rejection> {
    if user.covers(target) {
        Ok(())
    } else {
        Err(Rejection::forbidden("forbidden channel"))
    }
}

/// The default publish rule: the channel, and the reply channel if any, are within the user's scope.
pub fn publish_within_scope(user: &User, publish: &Publish) -> Result<(), Rejection> {
    if !user.may_use(&publish.channel) {
        return Err(Rejection::forbidden("forbidden channel"));
    }
    match &publish.reply {
        Some(reply) if !user.may_use(reply) => Err(Rejection::forbidden("forbidden reply channel")),
        _ => Ok(()),
    }
}
