//! Readiness from the listed servers and the two connections to each.

use super::*;
use std::sync::Arc;

fn set(names: &[&str]) -> BTreeSet<Server> {
    names.iter().map(|name| Arc::from(*name)).collect()
}

/// Ready only when every listed server has both connections up.
#[test]
fn ready_needs_both_connections_to_every_listed_server() {
    let listed = set(&["a", "b"]);
    assert!(Health::of(&listed, &set(&["a", "b"]), &set(&["a", "b"])).ready);
    assert!(!Health::of(&listed, &set(&["a"]), &set(&["a", "b"])).ready, "b's observation is down");
    assert!(!Health::of(&listed, &set(&["a", "b"]), &set(&["b"])).ready, "a's reply join is not confirmed");
    let shown = Health::of(&listed, &set(&["a"]), &set(&["a", "b"]));
    assert_eq!(shown.servers, vec![ServerHealth { server: "a".into(), observing: true, replying: true }, ServerHealth { server: "b".into(), observing: false, replying: true }]);
}

/// No listed server is not ready; a server up but no longer listed does not count.
#[test]
fn an_empty_list_is_not_ready_and_unlisted_servers_do_not_count() {
    assert!(!Health::of(&set(&[]), &set(&["a"]), &set(&["a"])).ready);
    let health = Health::of(&set(&["b"]), &set(&["a"]), &set(&["a"]));
    assert!(!health.ready && !health.any_up());
    assert_eq!(Health::default(), Health { ready: false, servers: vec![] });
}
