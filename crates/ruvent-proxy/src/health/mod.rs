//! What `/health` reports: every listed server and the proxy's two
//! long-lived connections to it (4장: a silently dropped upstream must not
//! stay healthy).
//!
//! * **observing** — the observation connection is up (the SDK's `Up`, until
//!   its `Gap`). It carries every subscription the users hold.
//! * **replying** — the work connection's reply-channel join is confirmed:
//!   request–response (and worker authentication) can get answers there.
//!
//! The proxy is **ready** when the list names at least one server and every
//! listed server is both. A connection the kernel's keepalive (S13) finds
//! dead is closed by the SDK, so a silent drop turns into "not ready"
//! within the keepalive time instead of staying green. Pure: the hub feeds
//! it the three sets whenever one changes.

use ruvent_client::servers::Server;
use serde::Serialize;
use std::collections::BTreeSet;

/// One listed server.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ServerHealth {
    pub server: String,
    pub observing: bool,
    pub replying: bool,
}

/// The proxy's upstream as `/health` shows it. The default is a proxy that
/// is not ready and knows no server (not started yet, or stopped).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Health {
    pub ready: bool,
    pub servers: Vec<ServerHealth>,
}

impl Health {
    /// From the listed servers and those with each connection up.
    pub fn of(listed: &BTreeSet<Server>, observing: &BTreeSet<Server>, replying: &BTreeSet<Server>) -> Health {
        let servers: Vec<ServerHealth> = listed.iter().map(|server| ServerHealth { server: server.to_string(), observing: observing.contains(server), replying: replying.contains(server) }).collect();
        let ready = !servers.is_empty() && servers.iter().all(|server| server.observing && server.replying);
        Health { ready, servers }
    }

    /// Whether any listed server's observation connection is up: a registration has somewhere to go.
    pub fn any_up(&self) -> bool {
        self.servers.iter().any(|server| server.observing)
    }
}

#[cfg(test)]
mod tests;
