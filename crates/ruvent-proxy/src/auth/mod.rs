//! End users. Who they are and which channels they may use.
//!
//! Three modes (`auth.mode`):
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

/// An authenticated end user.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct User {
    pub name: String,
    pub scope: Vec<Pattern>,
}

impl User {
    pub fn may_use(&self, channel: &str) -> bool {
        self.scope.iter().any(|pattern| pattern.matches(channel))
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
    /// The token is unknown or the worker said no.
    Rejected,
    /// The worker did not answer in time.
    Timeout,
    /// The upstream is not connected, or refused the authentication event.
    Unavailable(String),
}

impl std::fmt::Display for Denied {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Denied::Rejected => f.write_str("authentication rejected"),
            Denied::Timeout => f.write_str("authentication timed out"),
            Denied::Unavailable(reason) => write!(f, "authentication unavailable: {reason}"),
        }
    }
}

/// Resolves a user in the modes that need no upstream. `None` means "ask the worker".
pub fn local(config: &ProxyConfig, token: Option<&str>) -> Option<Result<User, Denied>> {
    match config.auth.mode {
        AuthMode::None => Some(Ok(User { name: "anonymous".into(), scope: patterns(&config.channels) })),
        AuthMode::Static => Some(
            token
                .and_then(|token| config.auth.tokens.iter().find(|entry| entry.token == token))
                .map(|entry| User { name: entry.user.clone(), scope: patterns(if entry.channels.is_empty() { &config.channels } else { &entry.channels }) })
                .ok_or(Denied::Rejected),
        ),
        AuthMode::Worker => None,
    }
}

/// The user a worker's answer describes.
pub fn from_answer(config: &ProxyConfig, answer: AuthAnswer) -> Result<User, Denied> {
    match (answer.ok, answer.user) {
        (true, Some(name)) => Ok(User { name, scope: patterns(if answer.channels.is_empty() { &config.channels } else { &answer.channels }) }),
        _ => Err(Denied::Rejected),
    }
}
