//! Root-channel accounting and the pause record (2.6).

use super::lanes::LaneKey;
use crate::engine::ops::SessionId;
use std::collections::{HashMap, HashSet};

/// What a paused root remembers.
#[derive(Debug, Default)]
pub(super) struct Paused {
    /// Its share (bytes) when it was paused.
    pub at_bytes: u64,
    /// The pause-notice record: production connections told `Paused` and what
    /// they sent to the root since.
    ///
    /// | key | production connection |
    /// | --- | --- |
    /// | lifetime | `Paused` sent to `Resumed` or the connection's close |
    /// | bound | the connections that wrote to the root while it was paused |
    /// | cleared by | the ledger, at resume and at the connection's close |
    pub notified: HashMap<SessionId, Notified>,
    /// Lanes where events of this root are held back.
    pub holds: HashSet<LaneKey>,
}

#[derive(Debug, Default)]
pub(super) struct Notified {
    /// Bytes received for the root since the notice.
    pub bytes: u64,
    /// What it may send after the notice before its reading stops (S14).
    pub allowance: u64,
    /// Its reading was stopped.
    pub stopped: bool,
}

/// One root channel's share.
///
/// | key | root channel |
/// | --- | --- |
/// | scope | one engine |
/// | lifetime | while it holds events or is paused |
/// | bound | root channels with events |
/// | cleared by | the ledger, when it holds nothing and is not paused |
#[derive(Debug, Default)]
pub(super) struct Root {
    /// Events held for it, in any state.
    pub events: u64,
    /// Managed bytes held for it: metadata and payloads.
    pub bytes: u64,
    /// Lineages that ended: completions without follow-ups, deletions, expiries.
    pub ended: u64,
    pub paused: Option<Paused>,
}

impl Root {
    pub fn is_empty(&self) -> bool {
        self.events == 0 && self.paused.is_none()
    }

    /// Whether its net-growth lanes must not be assigned from: paused, and
    /// its share is above what it was at the pause.
    pub fn blocks_growth(&self) -> bool {
        self.paused.as_ref().is_some_and(|paused| self.bytes > paused.at_bytes)
    }
}
