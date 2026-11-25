//! Readiness from the listed servers and the observation connection to each.

use super::*;
use std::sync::Arc;

fn set(names: &[&str]) -> BTreeSet<Server> {
    names.iter().map(|name| Arc::from(*name)).collect()
}

/// Ready only when every listed server is observed.
#[test]
fn ready_needs_every_listed_server_observed() {
    let listed = set(&["a", "b"]);
    assert!(Health::of(&listed, &set(&["a", "b"])).ready);
    let shown = Health::of(&listed, &set(&["a"]));
    assert!(!shown.ready, "b's observation is down");
    assert_eq!(shown.servers, vec![ServerHealth { server: "a".into(), observing: true }, ServerHealth { server: "b".into(), observing: false }]);
}

/// No listed server is not ready; a server up but no longer listed does not count.
#[test]
fn an_empty_list_is_not_ready_and_unlisted_servers_do_not_count() {
    assert!(!Health::of(&set(&[]), &set(&["a"])).ready);
    let health = Health::of(&set(&["b"]), &set(&["a"]));
    assert!(!health.ready && !health.servers[0].observing);
    assert_eq!(Health::default(), Health { ready: false, servers: vec![] });
}
