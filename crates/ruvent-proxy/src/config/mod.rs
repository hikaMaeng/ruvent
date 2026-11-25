//! The proxy's JSON configuration. A proxy built with no hooks decides
//! everything from this file; hooks replace the parts they override.
//!
//! Every value the overhaul plan's chapter 8 gives the proxy is a field here
//! with that value as its default: the reconnect back-off (S20) and the
//! reply-channel lease (S16). TCP keepalive (S13) is the SDK's default on
//! every upstream socket.

use ruvent::Pattern;
use ruvent_client::line::backoff::Reconnect as Backoff;
use serde::Deserialize;
use std::io;
use std::path::Path;
use std::time::Duration;

/// The whole configuration file.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProxyConfig {
    /// Address the proxy listens on, `host:port`.
    pub listen: String,
    pub upstream: Upstream,
    #[serde(default)]
    pub auth: Auth,
    /// Channels a user may use when authentication does not say otherwise.
    /// Exact names, or prefixes ending in `*`.
    #[serde(default = "default_channels")]
    pub channels: Vec<String>,
    /// Messages one user's send buffer holds. A user whose buffer is full is
    /// disconnected: that is the user's signal that it lost events.
    #[serde(default = "default_user_buffer")]
    pub user_buffer: usize,
    /// Whether registrations carry the end user as `delegate`. The upstream
    /// credential then needs the delegate grant.
    #[serde(default = "default_true")]
    pub delegate: bool,
    /// Upstream reconnect back-off (S20), in milliseconds.
    #[serde(default)]
    pub reconnect: Reconnect,
    /// Request–response through the proxy ([`crate::Proxy::exchange`]).
    #[serde(default)]
    pub exchange: Exchange,
}

/// The event servers this proxy talks to: it connects to every one (2.8).
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Upstream {
    /// Every event server, `host:port`. A program that keeps the list
    /// elsewhere (a router) hands the proxy its own [`crate::Upstream`] instead.
    pub servers: Vec<String>,
    /// The proxy's service credential token, the same on every server.
    pub token: String,
}

/// How end users are authenticated.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Auth {
    #[serde(default)]
    pub mode: AuthMode,
    /// For `static`: the accepted tokens.
    #[serde(default)]
    pub tokens: Vec<StaticToken>,
    /// For `worker`: where the authentication event goes.
    #[serde(default)]
    pub worker: Option<WorkerAuth>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthMode {
    /// Everyone is user `anonymous` with the default channels.
    #[default]
    None,
    /// Tokens listed in this file.
    Static,
    /// An authentication worker decides, through a request–response.
    Worker,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StaticToken {
    pub token: String,
    pub user: String,
    /// The user's channels; empty means the default channels.
    #[serde(default)]
    pub channels: Vec<String>,
}

/// The authentication worker. The answer comes back on the proxy's own
/// reply channel (`reply.<instance id>`), so there is no channel to name for it.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerAuth {
    /// Channel of the claimable authentication event.
    pub channel: String,
    /// Its type.
    #[serde(rename = "type", default = "default_auth_type")]
    pub event_type: String,
    /// How long a user waits for the answer: the request's deadline and `expires`.
    #[serde(default = "default_auth_timeout")]
    pub timeout_ms: u64,
}

/// S20: the n-th failed attempt in a row waits a random time up to
/// `min(max_ms, initial_ms × 2^n)`; after a server's `Bye{draining}` the
/// production connection's ceiling is `draining_ms`.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reconnect {
    pub initial_ms: u64,
    pub max_ms: u64,
    #[serde(default = "default_draining")]
    pub draining_ms: u64,
}

impl Default for Reconnect {
    fn default() -> Self {
        let backoff = Backoff::default();
        Reconnect { initial_ms: millis(backoff.initial), max_ms: millis(backoff.cap), draining_ms: millis(backoff.draining) }
    }
}

impl Reconnect {
    /// The SDK's back-off with these values.
    pub fn backoff(&self) -> Backoff {
        Backoff { initial: Duration::from_millis(self.initial_ms), cap: Duration::from_millis(self.max_ms), draining: Duration::from_millis(self.draining_ms) }
    }
}

/// Request–response limits.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Exchange {
    /// The default deadline of [`crate::Proxy::exchange`]: the call's wait and the request's `expires`.
    #[serde(default = "default_deadline")]
    pub deadline_ms: u64,
    /// S16: the lease of a result's assignment on the proxy's reply channel.
    #[serde(default = "default_reply_lease")]
    pub lease_ms: u64,
}

impl Default for Exchange {
    fn default() -> Self {
        Exchange { deadline_ms: default_deadline(), lease_ms: default_reply_lease() }
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn default_channels() -> Vec<String> {
    vec!["*".into()]
}

fn default_user_buffer() -> usize {
    1024
}

fn default_true() -> bool {
    true
}

fn default_auth_type() -> String {
    "authenticate".into()
}

fn default_auth_timeout() -> u64 {
    5_000
}

fn default_draining() -> u64 {
    millis(Backoff::default().draining)
}

fn default_deadline() -> u64 {
    30_000
}

fn default_reply_lease() -> u64 {
    5_000
}

impl ProxyConfig {
    /// Reads and checks a configuration file.
    pub fn load(path: &Path) -> io::Result<ProxyConfig> {
        let text = std::fs::read_to_string(path)?;
        Self::parse(&text)
    }

    /// Reads and checks a configuration text.
    pub fn parse(text: &str) -> io::Result<ProxyConfig> {
        let config: ProxyConfig = serde_json::from_str(text).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        config.check()?;
        Ok(config)
    }

    /// The rules a configuration must meet beyond its JSON shape. The file
    /// path and a configuration built in code go through the same check.
    pub fn check(&self) -> io::Result<()> {
        let invalid = |message: &str| Err(io::Error::new(io::ErrorKind::InvalidData, message.to_owned()));
        if self.upstream.servers.is_empty() {
            return invalid("upstream.servers names no server");
        }
        if self.auth.mode == AuthMode::Worker && self.auth.worker.is_none() {
            return invalid("auth.mode is \"worker\" but auth.worker is missing");
        }
        if self.auth.worker.as_ref().is_some_and(|worker| worker.timeout_ms == 0) {
            return invalid("auth.worker.timeout_ms must be at least 1");
        }
        if self.user_buffer == 0 {
            return invalid("user_buffer must be at least 1");
        }
        if self.reconnect.initial_ms == 0 || self.reconnect.initial_ms > self.reconnect.max_ms || self.reconnect.draining_ms < self.reconnect.max_ms {
            return invalid("reconnect needs 1 <= initial_ms <= max_ms <= draining_ms");
        }
        if self.exchange.deadline_ms == 0 || self.exchange.lease_ms == 0 {
            return invalid("exchange.deadline_ms and exchange.lease_ms must be at least 1");
        }
        Ok(())
    }

    /// A configuration for tests and embedding: no auth, every channel, one server.
    pub fn open(listen: &str, server: &str) -> ProxyConfig {
        ProxyConfig {
            listen: listen.into(),
            upstream: Upstream { servers: vec![server.into()], token: String::new() },
            auth: Auth::default(),
            channels: default_channels(),
            user_buffer: default_user_buffer(),
            delegate: true,
            reconnect: Reconnect::default(),
            exchange: Exchange::default(),
        }
    }
}

/// Turns configured channel strings into patterns.
pub fn patterns(channels: &[String]) -> Vec<Pattern> {
    channels.iter().map(|channel| Pattern(channel.clone())).collect()
}

#[cfg(test)]
mod tests;
