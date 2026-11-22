//! The server binary's JSON configuration.
//!
//! ```json
//! {
//!   "listen": "127.0.0.1:7400",
//!   "journal": "data/ruvent.journal",
//!   "durability": "strict",
//!   "credentials": [
//!     { "name": "workers", "token": "…", "methods": ["claim", "complete", "subscribe"], "channels": ["orders.*"] },
//!     { "name": "proxy", "token": "…", "channels": ["chat.*"], "delegate": true }
//!   ]
//! }
//! ```
//!
//! Everything but `listen` and `credentials` has a default. `journal` absent
//! means nothing is written to disk. A credential without `methods` may use
//! all seven; without `channels` it may use every channel.

use crate::connection::{Credentials, ServerOptions};
use ruvent::{Compaction, Durability, EngineConfig, Grant, Grants, Method, Pattern, SnapshotConfig};
use serde::Deserialize;
use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Address to listen on, `host:port`.
    pub listen: String,
    /// Journal file. Absent: volatile, nothing survives a restart.
    #[serde(default)]
    pub journal: Option<PathBuf>,
    /// `"scheduled"` (default) or `"strict"`.
    #[serde(default)]
    pub durability: Option<String>,
    pub credentials: Vec<CredentialConfig>,
    #[serde(default)]
    pub batch_max: Option<usize>,
    #[serde(default)]
    pub inbox: Option<usize>,
    /// Each connection's send buffer, in messages. Full means disconnect.
    #[serde(default)]
    pub outbound: Option<usize>,
    #[serde(default)]
    pub backlog_page: Option<usize>,
    #[serde(default)]
    pub compaction: Option<CompactionConfig>,
    #[serde(default)]
    pub snapshot: Option<SnapshotJson>,
    /// Bound on each direction's name dictionary.
    #[serde(default)]
    pub dictionary: Option<usize>,
    /// Bound on one frame, in bytes.
    #[serde(default)]
    pub max_frame: Option<usize>,
    /// How long a new connection has to send its hello, in milliseconds.
    #[serde(default)]
    pub handshake_ms: Option<u64>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialConfig {
    pub name: String,
    pub token: String,
    /// Method names: `register_claimable`, `register_dispatch`, `claim`,
    /// `complete`, `delete`, `subscribe`, `unsubscribe`. Absent: all.
    #[serde(default)]
    pub methods: Option<Vec<String>>,
    /// Channel patterns: an exact name, a prefix ending in `*`, or `*`. Absent: `["*"]`.
    #[serde(default)]
    pub channels: Option<Vec<String>>,
    /// May register on behalf of an end user.
    #[serde(default)]
    pub delegate: bool,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactionConfig {
    pub min_records: u64,
    pub ratio: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotJson {
    pub directory: PathBuf,
    pub channels: Vec<String>,
    #[serde(default)]
    pub types: Vec<String>,
    #[serde(default)]
    pub max_events: Option<usize>,
    #[serde(default)]
    pub max_age_ms: Option<u64>,
    #[serde(default)]
    pub buffer: Option<usize>,
}

fn method(name: &str) -> Result<Method, String> {
    Ok(match name {
        "register_claimable" => Method::RegisterClaimable,
        "register_dispatch" => Method::RegisterDispatch,
        "claim" => Method::Claim,
        "complete" => Method::Complete,
        "delete" => Method::Delete,
        "subscribe" => Method::Subscribe,
        "unsubscribe" => Method::Unsubscribe,
        other => return Err(format!("unknown method {other:?}")),
    })
}

impl Config {
    pub fn from_json(text: &str) -> Result<Config, String> {
        serde_json::from_str(text).map_err(|error| format!("configuration: {error}"))
    }

    fn durability(&self) -> Result<Durability, String> {
        match self.durability.as_deref() {
            None | Some("scheduled") => Ok(Durability::Scheduled),
            Some("strict") => Ok(Durability::Strict),
            Some(other) => Err(format!("durability {other:?}: expected \"scheduled\" or \"strict\"")),
        }
    }

    fn grants(&self) -> Result<Grants, String> {
        if self.credentials.is_empty() {
            return Err("at least one credential is needed, or nobody can connect".into());
        }
        let mut names = HashSet::new();
        let mut tokens = HashSet::new();
        let mut grants = Grants::new();
        for credential in &self.credentials {
            if credential.token.is_empty() {
                return Err(format!("credential {:?} has an empty token", credential.name));
            }
            if !names.insert(credential.name.as_str()) {
                return Err(format!("credential {:?} is listed twice", credential.name));
            }
            if !tokens.insert(credential.token.as_str()) {
                return Err(format!("two credentials share the token of {:?}", credential.name));
            }
            let methods = match &credential.methods {
                Some(methods) => methods.iter().map(|name| method(name)).collect::<Result<Vec<_>, _>>()?,
                None => Method::ALL.to_vec(),
            };
            let channels = credential.channels.clone().unwrap_or_else(|| vec!["*".into()]).into_iter().map(Pattern).collect();
            grants.insert(credential.name.clone(), Grant { methods, channels, delegate: credential.delegate });
        }
        Ok(grants)
    }

    /// The engine configuration this file describes.
    pub fn engine_config(&self) -> Result<EngineConfig, String> {
        let defaults = EngineConfig::default();
        let snapshot = self.snapshot.as_ref().map(|snapshot| {
            let mut config = SnapshotConfig::new(snapshot.directory.clone(), snapshot.channels.iter().cloned().map(Pattern).collect());
            config.types = snapshot.types.clone();
            if let Some(max_events) = snapshot.max_events {
                config.max_events = max_events;
            }
            if let Some(max_age_ms) = snapshot.max_age_ms {
                config.max_age = Duration::from_millis(max_age_ms);
            }
            if let Some(buffer) = snapshot.buffer {
                config.buffer = buffer;
            }
            config
        });
        Ok(EngineConfig {
            batch_max: self.batch_max.unwrap_or(defaults.batch_max),
            inbox: self.inbox.unwrap_or(defaults.inbox),
            outbound: self.outbound.unwrap_or(defaults.outbound),
            backlog_page: self.backlog_page.unwrap_or(defaults.backlog_page),
            durability: self.durability()?,
            compaction: self.compaction.map_or(defaults.compaction, |compaction| Compaction { min_records: compaction.min_records, ratio: compaction.ratio }),
            grants: Some(self.grants()?),
            snapshot,
        })
    }

    pub fn credentials(&self) -> Result<Credentials, String> {
        self.grants()?;
        Ok(Credentials::new(self.credentials.iter().map(|credential| (credential.name.clone(), credential.token.clone()))))
    }

    pub fn options(&self) -> ServerOptions {
        let defaults = ServerOptions::default();
        ServerOptions {
            dictionary: self.dictionary.unwrap_or(defaults.dictionary),
            max_frame: self.max_frame.unwrap_or(defaults.max_frame),
            handshake: self.handshake_ms.map_or(defaults.handshake, Duration::from_millis),
        }
    }
}

#[cfg(test)]
mod tests;
