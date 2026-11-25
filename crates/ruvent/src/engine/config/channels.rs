//! Channel-level settings and the patterns that override them.

use crate::names::Pattern;
use std::time::Duration;

/// B17: a root channel past either bound is paused regardless of the watermark.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MaxBacklog {
    /// Waiting and claimed events held for the root.
    pub count: u64,
    /// Managed bytes held for the root.
    pub bytes: u64,
}

/// What one channel does with its events.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelSettings {
    /// B6: the event moves to `<isolation_prefix><channel>` at this failure
    /// count. `None`: never (reply and isolation channels).
    pub isolate_after: Option<u16>,
    /// B6: how long an isolated event is kept (its `expires`).
    pub isolated_keep: Duration,
    /// B9: an expired event is isolated instead of removed.
    pub isolate_expired: bool,
    /// B17: pause the root channel past these bounds. `None`: no bound.
    pub max_backlog: Option<MaxBacklog>,
    /// B26: assign the events of one `group` one at a time.
    pub serialize_group: bool,
    /// 2.5: the channel's events are never journaled and do not survive a restart.
    pub volatile: bool,
}

impl Default for ChannelSettings {
    fn default() -> Self {
        Self { isolate_after: Some(5), isolated_keep: Duration::from_secs(7 * 24 * 3600), isolate_expired: false, max_backlog: None, serialize_group: false, volatile: false }
    }
}

/// Settings for the channels a pattern matches.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelRule {
    pub pattern: Pattern,
    pub settings: ChannelSettings,
}

/// The defaults and the overriding rules. The first rule whose pattern
/// matches a channel decides all of its settings; no rule means `default`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelConfig {
    pub default: ChannelSettings,
    pub rules: Vec<ChannelRule>,
    /// B6: the prefix an isolated event's channel gets (`isolated.<channel>`).
    pub isolation_prefix: String,
}

impl ChannelConfig {
    pub fn settings(&self, channel: &str) -> &ChannelSettings {
        self.rules.iter().find(|rule| rule.pattern.matches(channel)).map_or(&self.default, |rule| &rule.settings)
    }

    /// The isolation channel of `channel`.
    pub fn isolated(&self, channel: &str) -> String {
        format!("{}{channel}", self.isolation_prefix)
    }
}

impl Default for ChannelConfig {
    /// Reply channels (`reply.*`) are volatile and never isolated (B10, 2.4);
    /// isolation channels are not isolated again.
    fn default() -> Self {
        Self {
            default: ChannelSettings::default(),
            rules: vec![
                ChannelRule { pattern: Pattern("reply.*".into()), settings: ChannelSettings { isolate_after: None, volatile: true, ..ChannelSettings::default() } },
                ChannelRule { pattern: Pattern("isolated.*".into()), settings: ChannelSettings { isolate_after: None, ..ChannelSettings::default() } },
            ],
            isolation_prefix: "isolated.".into(),
        }
    }
}
