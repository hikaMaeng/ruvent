//! The fan-out's upstream: every listed server and whether its observation
//! connection is up (4장: a silently dropped upstream must not stay healthy).
//!
//! **observing** is the SDK observer's `Up`, until its `Gap`. It carries
//! every subscription the members hold. The fan-out is **ready** when the
//! list names at least one server and every listed server is observed. A
//! connection the kernel's keepalive (S13) finds dead is closed by the SDK,
//! so a silent drop turns into "not ready" within the keepalive time instead
//! of staying green. How readiness is shown (an HTTP `/health`, a metric) is
//! the program's. Pure: the hub feeds it the two sets whenever one changes.

use ruvent_client::servers::Server;
use std::collections::BTreeSet;

/// One listed server.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerHealth {
    pub server: String,
    pub observing: bool,
}

/// The fan-out's upstream. The default is a fan-out that is not ready and
/// knows no server (not started yet, or stopped).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Health {
    pub ready: bool,
    pub servers: Vec<ServerHealth>,
}

impl Health {
    /// From the listed servers and those whose observation connection is up.
    pub fn of(listed: &BTreeSet<Server>, observing: &BTreeSet<Server>) -> Health {
        let servers: Vec<ServerHealth> = listed.iter().map(|server| ServerHealth { server: server.to_string(), observing: observing.contains(server) }).collect();
        let ready = !servers.is_empty() && servers.iter().all(|server| server.observing);
        Health { ready, servers }
    }
}

#[cfg(test)]
mod tests;
