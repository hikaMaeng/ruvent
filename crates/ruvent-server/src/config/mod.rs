//! The server binary's JSON configuration.
//!
//! ```json
//! {
//!   "listen": "127.0.0.1:7400",
//!   "journal": "data/journal",
//!   "durability": "strict",
//!   "credentials": [
//!     { "name": "workers", "token": "…", "methods": ["claim", "complete", "register_dispatch"], "channels": ["orders.*"] },
//!     { "name": "ops", "token": "…", "methods": ["operate"] }
//!   ],
//!   "memory": { "limit": 8589934592 },
//!   "channels": { "rules": [ { "pattern": "audit.*", "isolate_after": 3 } ] },
//!   "connection": { "keepalive": { "idle_ms": 15000 } }
//! }
//! ```
//!
//! Everything but `listen` and `credentials` has a default, and every
//! default is the engine's or the server's own (plan chapter 8). `journal`
//! absent means nothing is written to disk. A credential without `methods`
//! may use all eight; without `channels` it may use every channel. The
//! sections and their keys are in the crate's README.

use crate::connection::socket::Keepalive;
use crate::connection::{Credentials, ServerOptions};
use ruvent::{ChannelConfig, ChannelRule, ChannelSettings, Durability, EngineConfig, EventId, Grant, Grants, MaxBacklog, MemoryConfig, Method, Pattern, ProducerConfig, SnapshotConfig, StorageConfig};
use serde::Deserialize;
use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Address to listen on, `host:port`.
    pub listen: String,
    /// The segment log's directory. Absent: volatile, nothing survives a restart.
    #[serde(default)]
    pub journal: Option<PathBuf>,
    /// `"scheduled"` (default) or `"strict"`.
    #[serde(default)]
    pub durability: Option<String>,
    /// This server's id in `Welcome`, a UUID. Absent: a fresh one at every start.
    #[serde(default)]
    pub server_id: Option<String>,
    pub credentials: Vec<CredentialConfig>,
    #[serde(default)]
    pub engine: EngineJson,
    #[serde(default)]
    pub producers: ProducersJson,
    #[serde(default)]
    pub memory: MemoryJson,
    #[serde(default)]
    pub storage: StorageJson,
    #[serde(default)]
    pub channels: ChannelsJson,
    #[serde(default)]
    pub connection: ConnectionJson,
    #[serde(default)]
    pub snapshot: Option<SnapshotJson>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialConfig {
    pub name: String,
    pub token: String,
    /// Method names: `register_claimable`, `register_dispatch`, `claim`,
    /// `complete`, `delete`, `subscribe`, `unsubscribe`, `operate`. Absent: all.
    #[serde(default)]
    pub methods: Option<Vec<String>>,
    /// Channel patterns: an exact name, a prefix ending in `*`, or `*`. Absent: `["*"]`.
    #[serde(default)]
    pub channels: Option<Vec<String>>,
    /// May register on behalf of an end user.
    #[serde(default)]
    pub delegate: bool,
}

/// `EngineConfig`'s scalar fields.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineJson {
    pub batch_max: Option<usize>,
    pub inbox: Option<usize>,
    /// Each connection's engine-side buffer, in messages. The connection
    /// owner empties it as it fills and bounds what it holds in bytes (`connection.unsent`).
    pub outbound: Option<usize>,
    pub backlog_page: Option<usize>,
    /// S2.
    pub ack_every: Option<u64>,
    /// S20: a closed connection's written assignments are due this long after the close.
    pub reconnect_grace_ms: Option<u64>,
    /// S10.
    pub max_follow_ups: Option<usize>,
    /// S14.
    pub pause_allowance: Option<u64>,
    /// B24.
    pub stats_top: Option<usize>,
}

/// B4, S15.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProducersJson {
    pub linger_ms: Option<u64>,
    pub linger_max_ms: Option<u64>,
    pub max_entries: Option<usize>,
    pub entry_bytes: Option<u64>,
}

/// S5, S19, S22.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryJson {
    pub limit: Option<u64>,
    pub reserve: Option<f64>,
    pub high_watermark: Option<f64>,
    pub release: Option<f64>,
    pub early_pause: Option<f64>,
    pub net_growth_window: Option<u32>,
    pub slot_bytes: Option<u64>,
    pub depth_target: Option<u64>,
}

/// S3, S4, S6.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageJson {
    pub segment_bytes: Option<u64>,
    pub relocate_at: Option<f64>,
    pub checkpoint_slice: Option<usize>,
    pub checkpoint_tail_multiple: Option<u64>,
    pub checkpoint_tail_cap: Option<u64>,
    pub disk_reserve_segments: Option<u64>,
    pub disk_reserve: Option<f64>,
    pub disk_capacity: Option<u64>,
    pub read_ahead: Option<usize>,
    pub max_reads: Option<usize>,
    pub checkpoint_queue: Option<usize>,
}

/// B6, B9, B17, B26, volatile channels.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelsJson {
    /// The prefix of an isolation channel. Default `isolated.`.
    pub isolation_prefix: Option<String>,
    /// Overrides of the defaults every channel starts from.
    #[serde(default)]
    pub default: ChannelJson,
    /// Rules by pattern; the first match decides. Checked before the built-in
    /// rules (`reply.*` volatile and never isolated, `isolated.*` never isolated).
    #[serde(default)]
    pub rules: Vec<ChannelRuleJson>,
}

/// One channel's settings; an absent key keeps the default.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelJson {
    /// B6: the failure count that isolates. `0`: never.
    pub isolate_after: Option<u16>,
    pub isolated_keep_ms: Option<u64>,
    pub isolate_expired: Option<bool>,
    pub max_backlog: Option<MaxBacklogJson>,
    pub serialize_group: Option<bool>,
    pub volatile: Option<bool>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaxBacklogJson {
    pub count: u64,
    pub bytes: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelRuleJson {
    pub pattern: String,
    #[serde(flatten)]
    pub settings: ChannelJson,
}

/// `ServerOptions`.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionJson {
    /// S18.
    pub dictionary: Option<usize>,
    /// B13.
    pub max_body: Option<usize>,
    /// S11.
    pub inline: Option<usize>,
    /// S11.
    pub direct: Option<usize>,
    pub handshake_ms: Option<u64>,
    /// S7.
    pub unsent: Option<u64>,
    /// S20: the deadline of a staged close.
    pub close_ms: Option<u64>,
    pub spool_piece: Option<usize>,
    pub batch: Option<usize>,
    /// S13.
    #[serde(default)]
    pub keepalive: KeepaliveJson,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeepaliveJson {
    pub idle_ms: Option<u64>,
    pub interval_ms: Option<u64>,
    pub retries: Option<u32>,
    pub user_timeout_ms: Option<u64>,
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
        "operate" => Method::Operate,
        other => return Err(format!("unknown method {other:?}")),
    })
}

fn ms(value: Option<u64>, default: Duration) -> Duration {
    value.map_or(default, Duration::from_millis)
}

fn fraction(name: &str, value: Option<f64>, default: f64) -> Result<f64, String> {
    let value = value.unwrap_or(default);
    if !(0.0..=1.0).contains(&value) {
        return Err(format!("{name} {value}: expected a fraction between 0 and 1"));
    }
    Ok(value)
}

impl ChannelJson {
    fn apply(&self, base: &ChannelSettings) -> ChannelSettings {
        ChannelSettings {
            isolate_after: self.isolate_after.map_or(base.isolate_after, |count| (count > 0).then_some(count)),
            isolated_keep: ms(self.isolated_keep_ms, base.isolated_keep),
            isolate_expired: self.isolate_expired.unwrap_or(base.isolate_expired),
            max_backlog: self.max_backlog.map_or(base.max_backlog, |bound| Some(MaxBacklog { count: bound.count, bytes: bound.bytes })),
            serialize_group: self.serialize_group.unwrap_or(base.serialize_group),
            volatile: self.volatile.unwrap_or(base.volatile),
        }
    }
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

    fn channels(&self) -> ChannelConfig {
        let built_in = ChannelConfig::default();
        let default = self.channels.default.apply(&built_in.default);
        let mut rules: Vec<ChannelRule> = self.channels.rules.iter().map(|rule| ChannelRule { pattern: Pattern(rule.pattern.clone()), settings: rule.settings.apply(&default) }).collect();
        // The built-in rules keep their own meaning on top of the configured default.
        for rule in built_in.rules {
            let settings = ChannelSettings { isolate_after: rule.settings.isolate_after, volatile: rule.settings.volatile || default.volatile, ..default.clone() };
            rules.push(ChannelRule { pattern: rule.pattern, settings });
        }
        ChannelConfig { default, rules, isolation_prefix: self.channels.isolation_prefix.clone().unwrap_or(built_in.isolation_prefix) }
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
        let server_id = match &self.server_id {
            Some(text) => Some(text.parse::<EventId>().map_err(|error| format!("server_id {text:?}: {error}"))?.0),
            None => None,
        };
        let engine = &self.engine;
        let p = &self.producers;
        let producers = ProducerConfig {
            linger: ms(p.linger_ms, defaults.producers.linger),
            linger_max: ms(p.linger_max_ms, defaults.producers.linger_max),
            max_entries: p.max_entries.unwrap_or(defaults.producers.max_entries),
            entry_bytes: p.entry_bytes.unwrap_or(defaults.producers.entry_bytes),
        };
        let m = &self.memory;
        let d = &defaults.memory;
        let memory = MemoryConfig {
            limit: m.limit.or(d.limit),
            reserve: fraction("memory.reserve", m.reserve, d.reserve)?,
            high_watermark: fraction("memory.high_watermark", m.high_watermark, d.high_watermark)?,
            release: fraction("memory.release", m.release, d.release)?,
            early_pause: fraction("memory.early_pause", m.early_pause, d.early_pause)?,
            net_growth_window: m.net_growth_window.unwrap_or(d.net_growth_window),
            slot_bytes: m.slot_bytes.unwrap_or(d.slot_bytes),
            depth_target: m.depth_target.unwrap_or(d.depth_target),
        };
        let s = &self.storage;
        let t = &defaults.storage;
        let storage = StorageConfig {
            segment_bytes: s.segment_bytes.unwrap_or(t.segment_bytes),
            relocate_at: fraction("storage.relocate_at", s.relocate_at, t.relocate_at)?,
            checkpoint_slice: s.checkpoint_slice.unwrap_or(t.checkpoint_slice),
            checkpoint_tail_multiple: s.checkpoint_tail_multiple.unwrap_or(t.checkpoint_tail_multiple),
            checkpoint_tail_cap: s.checkpoint_tail_cap.unwrap_or(t.checkpoint_tail_cap),
            disk_reserve_segments: s.disk_reserve_segments.unwrap_or(t.disk_reserve_segments),
            disk_reserve: fraction("storage.disk_reserve", s.disk_reserve, t.disk_reserve)?,
            disk_capacity: s.disk_capacity.or(t.disk_capacity),
            preallocate: t.preallocate,
            read_ahead: s.read_ahead.unwrap_or(t.read_ahead),
            max_reads: s.max_reads.unwrap_or(t.max_reads),
            checkpoint_queue: s.checkpoint_queue.unwrap_or(t.checkpoint_queue),
        };
        Ok(EngineConfig {
            batch_max: engine.batch_max.unwrap_or(defaults.batch_max),
            inbox: engine.inbox.unwrap_or(defaults.inbox),
            outbound: engine.outbound.unwrap_or(defaults.outbound),
            backlog_page: engine.backlog_page.unwrap_or(defaults.backlog_page),
            durability: self.durability()?,
            grants: Some(self.grants()?),
            snapshot,
            server_id,
            ack_every: engine.ack_every.unwrap_or(defaults.ack_every),
            producers,
            reconnect_grace: ms(engine.reconnect_grace_ms, defaults.reconnect_grace),
            max_follow_ups: engine.max_follow_ups.unwrap_or(defaults.max_follow_ups),
            memory,
            pause_allowance: engine.pause_allowance.unwrap_or(defaults.pause_allowance),
            stats_top: engine.stats_top.unwrap_or(defaults.stats_top),
            channels: self.channels(),
            storage,
        })
    }

    pub fn credentials(&self) -> Result<Credentials, String> {
        self.grants()?;
        Ok(Credentials::new(self.credentials.iter().map(|credential| (credential.name.clone(), credential.token.clone()))))
    }

    pub fn options(&self) -> ServerOptions {
        let defaults = ServerOptions::default();
        let c = &self.connection;
        let k = &c.keepalive;
        ServerOptions {
            dictionary: c.dictionary.unwrap_or(defaults.dictionary),
            max_body: c.max_body.unwrap_or(defaults.max_body),
            inline: c.inline.unwrap_or(defaults.inline),
            direct: c.direct.unwrap_or(defaults.direct),
            handshake: ms(c.handshake_ms, defaults.handshake),
            unsent: c.unsent.unwrap_or(defaults.unsent),
            close: ms(c.close_ms, defaults.close),
            keepalive: Keepalive {
                idle: ms(k.idle_ms, defaults.keepalive.idle),
                interval: ms(k.interval_ms, defaults.keepalive.interval),
                retries: k.retries.unwrap_or(defaults.keepalive.retries),
                user_timeout: ms(k.user_timeout_ms, defaults.keepalive.user_timeout),
            },
            spool_piece: c.spool_piece.unwrap_or(defaults.spool_piece),
            batch: c.batch.unwrap_or(defaults.batch),
            features: defaults.features,
        }
    }
}

#[cfg(test)]
mod tests;
