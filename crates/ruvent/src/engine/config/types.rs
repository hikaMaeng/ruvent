//! Type-level settings and the type sets that override them (plan 9.15).
//!
//! A channel is an id the server reads nothing from, so what a server does
//! with an event is decided by its type.

use crate::types::TypeSet;
use std::time::Duration;

/// B17: a root type past either bound is paused regardless of the watermark.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MaxBacklog {
    /// Waiting and claimed events held for the root.
    pub count: u64,
    /// Managed bytes held for the root.
    pub bytes: u64,
}

/// What the server does with the events of one type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypeSettings {
    /// B6: the event is isolated at this failure count — it stays on its
    /// channel and waits only for joins that ask for isolated events.
    /// `None`: never.
    pub isolate_after: Option<u16>,
    /// B6: how long an isolated event is kept (its `expires`).
    pub isolated_keep: Duration,
    /// B9: an expired event is isolated instead of removed.
    pub isolate_expired: bool,
    /// B17: pause the root type past these bounds, for the lineages this
    /// type starts. `None`: no bound.
    pub max_backlog: Option<MaxBacklog>,
    /// B26: assign the events of one `group` on a channel one at a time.
    pub serialize_group: bool,
    /// 2.5: the events are never journaled and do not survive a restart.
    pub volatile: bool,
}

impl Default for TypeSettings {
    fn default() -> Self {
        Self { isolate_after: Some(5), isolated_keep: Duration::from_secs(7 * 24 * 3600), isolate_expired: false, max_backlog: None, serialize_group: false, volatile: false }
    }
}

/// Settings for the types of a set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypeRule {
    pub types: TypeSet,
    pub settings: TypeSettings,
}

/// The defaults and the overriding rules. The first rule whose set holds a
/// type decides all of its settings; no rule means `default`.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct TypeConfig {
    pub default: TypeSettings,
    pub rules: Vec<TypeRule>,
}

impl TypeConfig {
    pub fn settings(&self, event_type: u16) -> &TypeSettings {
        self.rules.iter().find(|rule| rule.types.contains(event_type)).map_or(&self.default, |rule| &rule.settings)
    }
}
