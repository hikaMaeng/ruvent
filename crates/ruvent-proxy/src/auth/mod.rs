//! End users: who they are, and the authentication modes a configuration can name.
//!
//! The configured modes (`auth.mode`) are what [`crate::Defaults`] does; a
//! [`crate::Hooks::authenticate`] of your own replaces them.
//!
//! | mode | decides | cost per authentication |
//! | --- | --- | --- |
//! | `none` | everyone is `anonymous` with the default channels | none |
//! | `static` | tokens listed in the configuration (Q1(c)) | a lookup |
//! | `worker` | an authentication worker, through a claimable event (Q1(a)) | one event round trip |
//!
//! In `worker` mode the proxy registers a claimable event on `auth.worker.channel`
//! with payload `{"token":…}`, a random correlation and `reply` set to
//! `auth.worker.reply_channel`, which only this proxy subscribes to. The
//! worker completes it with a dispatch follow-up on that reply channel, same
//! correlation, payload `{"ok":true|false,"user":"…","channels":["…"]}`.

use crate::config::{patterns, AuthMode, ProxyConfig};
use ruvent::Pattern;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::fmt;
use std::sync::Arc;

/// An authenticated end user.
///
/// `name` is what registrations carry as `delegate`; `scope` is the channels
/// the default subscribe and publish hooks allow. Anything else a domain
/// knows about the user rides along as [`User::with_data`].
#[derive(Clone)]
pub struct User {
    pub name: String,
    pub scope: Vec<Pattern>,
    data: Option<Arc<dyn Any + Send + Sync>>,
}

impl User {
    /// A user and the channels it may use: exact names, or prefixes ending in `*`.
    pub fn new(name: impl Into<String>, channels: impl IntoIterator<Item = impl Into<String>>) -> User {
        User { name: name.into(), scope: channels.into_iter().map(|channel| Pattern(channel.into())).collect(), data: None }
    }

    /// Attaches the domain's own value: a tenant, roles, a session. Read it back with [`User::data`].
    pub fn with_data<T: Any + Send + Sync>(mut self, data: T) -> User {
        self.data = Some(Arc::new(data));
        self
    }

    /// The value attached by [`User::with_data`], if it has this type.
    pub fn data<T: Any>(&self) -> Option<&T> {
        self.data.as_deref()?.downcast_ref()
    }

    pub fn may_use(&self, channel: &str) -> bool {
        self.scope.iter().any(|pattern| pattern.matches(channel))
    }
}

impl fmt::Debug for User {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("User").field("name", &self.name).field("scope", &self.scope).field("data", &self.data.is_some()).finish()
    }
}

/// The payload of the authentication event.
#[derive(Serialize, Deserialize, Debug)]
pub struct AuthRequest {
    pub token: String,
}

/// The payload of the worker's answer.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AuthAnswer {
    pub ok: bool,
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub channels: Vec<String>,
}

/// Why a user was not let in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Denied {
    /// No credentials were presented. A protocol that can ask for them in
    /// band (WebSocket: a first `{"op":"auth"}` message) asks once and
    /// authenticates again.
    Missing,
    /// The credentials are unknown, or the worker said no.
    Rejected,
    /// The worker did not answer in time.
    Timeout,
    /// The upstream is not connected, or refused the authentication event.
    Unavailable(String),
}

impl fmt::Display for Denied {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Denied::Missing => f.write_str("authentication required"),
            Denied::Rejected => f.write_str("authentication rejected"),
            Denied::Timeout => f.write_str("authentication timed out"),
            Denied::Unavailable(reason) => write!(f, "authentication unavailable: {reason}"),
        }
    }
}

impl std::error::Error for Denied {}

/// Resolves a user in the modes that need no upstream. `None` means "ask the worker".
pub fn local(config: &ProxyConfig, token: Option<&str>) -> Option<Result<User, Denied>> {
    match config.auth.mode {
        AuthMode::None => Some(Ok(User { name: "anonymous".into(), scope: patterns(&config.channels), data: None })),
        AuthMode::Static => Some(match token {
            None => Err(Denied::Missing),
            Some(token) => config
                .auth
                .tokens
                .iter()
                .find(|entry| entry.token == token)
                .map(|entry| User { name: entry.user.clone(), scope: patterns(if entry.channels.is_empty() { &config.channels } else { &entry.channels }), data: None })
                .ok_or(Denied::Rejected),
        }),
        AuthMode::Worker => token.is_none().then_some(Err(Denied::Missing)),
    }
}

/// The user a worker's answer describes.
pub fn from_answer(config: &ProxyConfig, answer: AuthAnswer) -> Result<User, Denied> {
    match (answer.ok, answer.user) {
        (true, Some(name)) => Ok(User { name, scope: patterns(if answer.channels.is_empty() { &config.channels } else { &answer.channels }), data: None }),
        _ => Err(Denied::Rejected),
    }
}
