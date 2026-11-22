use super::{Authenticator, Credentials, Identity, Policy};
use crate::envelope::Command;
use async_trait::async_trait;

/// Admits every routed command unchanged and lets anyone read any channel.
/// The default, and right for a service with no notion of ownership.
pub struct Open;

impl Policy for Open {}

/// Lets everyone in as `anonymous`. For development and for services that
/// authenticate somewhere else.
pub struct Anonymous;

#[async_trait]
impl Authenticator for Anonymous {
    async fn authenticate(&self, _credentials: &Credentials) -> Option<Identity> {
        Some(Identity { user: "anonymous".to_owned() })
    }
}

type Naming = Box<dyn Fn(&str) -> String + Send + Sync>;

/// Sessions owned by their prefix.
///
/// A session id carries its owner as `<user>-…`, so one minted for another
/// account cannot be replayed here. The reply channel is derived from the
/// session rather than taken from the client, so a client can neither address
/// nor read someone else's.
pub struct OwnedSessions {
    reply_channel_for: Naming,
}

impl OwnedSessions {
    /// `reply_channel_for` derives a session's channel from its id.
    pub fn new(reply_channel_for: impl Fn(&str) -> String + Send + Sync + 'static) -> Self {
        Self { reply_channel_for: Box::new(reply_channel_for) }
    }
}

impl Policy for OwnedSessions {
    fn admit(&self, identity: &Identity, mut command: Command) -> Option<Command> {
        if identity.user.is_empty() {
            return None;
        }
        let session = command.session_id.as_deref().filter(|session| session.starts_with(&format!("{}-", identity.user)))?;
        let reply: crate::envelope::Text = (self.reply_channel_for)(session).into();
        if command.channel.is_empty() {
            command.channel = reply.clone();
        }
        command.reply_channel = Some(reply);
        Some(command)
    }

    fn may_read(&self, identity: &Identity, channel: &str) -> bool {
        // A channel is readable by whoever owns a session that maps onto it.
        // Without the session id the only safe test is that the owner's
        // prefix appears where `reply_channel_for` would have put it.
        let probe = (self.reply_channel_for)(&format!("{}-", identity.user));
        !identity.user.is_empty() && channel.starts_with(&probe)
    }
}
