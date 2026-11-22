//! The proxy's JSON configuration. A proxy carries no user code: everything
//! it decides is in this file.

use ruvent::Pattern;
use serde::Deserialize;
use std::io;
use std::path::Path;

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
    /// Upstream reconnect backoff: first wait and ceiling, in milliseconds.
    #[serde(default)]
    pub reconnect: Reconnect,
}

/// The event server this proxy talks to.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Upstream {
    pub address: String,
    /// The proxy's service credential token.
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
    /// For `worker`: where the authentication event goes and where the answer comes back.
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
    /// An authentication worker decides, through a claimable event.
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

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerAuth {
    /// Channel of the claimable authentication event.
    pub channel: String,
    /// Its type.
    #[serde(rename = "type", default = "default_auth_type")]
    pub event_type: String,
    /// The channel only this proxy subscribes to, where answers come back.
    pub reply_channel: String,
    /// How long a user waits for the answer.
    #[serde(default = "default_auth_timeout")]
    pub timeout_ms: u64,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reconnect {
    pub initial_ms: u64,
    pub max_ms: u64,
}

impl Default for Reconnect {
    fn default() -> Self {
        Reconnect { initial_ms: 100, max_ms: 5_000 }
    }
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

impl ProxyConfig {
    /// Reads and checks a configuration file.
    pub fn load(path: &Path) -> io::Result<ProxyConfig> {
        let text = std::fs::read_to_string(path)?;
        Self::parse(&text)
    }

    pub fn parse(text: &str) -> io::Result<ProxyConfig> {
        let config: ProxyConfig = serde_json::from_str(text).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        if config.auth.mode == AuthMode::Worker && config.auth.worker.is_none() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "auth.mode is \"worker\" but auth.worker is missing"));
        }
        Ok(config)
    }

    /// A configuration for tests and embedding: no auth, every channel.
    pub fn open(listen: &str, upstream: &str) -> ProxyConfig {
        ProxyConfig {
            listen: listen.into(),
            upstream: Upstream { address: upstream.into(), token: String::new() },
            auth: Auth::default(),
            channels: default_channels(),
            user_buffer: default_user_buffer(),
            delegate: true,
            reconnect: Reconnect::default(),
        }
    }
}

/// Turns configured channel strings into patterns.
pub fn patterns(channels: &[String]) -> Vec<Pattern> {
    channels.iter().map(|channel| Pattern(channel.clone())).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_example_configuration_parses() {
        let text = include_str!("../../proxy.example.json");
        let config = ProxyConfig::parse(text).unwrap();
        assert_eq!(config.auth.mode, AuthMode::Worker);
        assert!(config.auth.worker.is_some());
    }

    #[test]
    fn worker_mode_without_its_section_is_refused() {
        let text = r#"{"listen":"127.0.0.1:0","upstream":{"address":"a","token":"t"},"auth":{"mode":"worker"}}"#;
        assert!(ProxyConfig::parse(text).is_err());
    }
}
