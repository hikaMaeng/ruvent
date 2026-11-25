//! Users and the configured authentication modes, without the hub.

use super::*;
use crate::config::{StaticToken, WorkerAuth};

fn config(mode: AuthMode) -> ProxyConfig {
    let mut config = ProxyConfig::open("127.0.0.1:0", "x");
    config.auth.mode = mode;
    config.channels = vec!["public.*".into()];
    config
}

fn names(user: &User) -> Vec<&str> {
    user.scope.iter().map(|pattern| pattern.0.as_str()).collect()
}

// ── users ────────────────────────────────────────────────────────────────

/// A user may use exact channel names and prefixes ending in `*`, nothing else.
#[test]
fn a_users_scope_is_exact_names_and_star_prefixes() {
    let user = User::new("u", ["room", "chat.*"]);
    assert!(user.may_use("room") && user.may_use("chat.x") && user.may_use("chat."));
    assert!(!user.may_use("room2") && !user.may_use("chat") && !user.may_use("other"));
    assert!(!User::new("nobody", Vec::<String>::new()).may_use("room"), "an empty scope allows nothing");
}

/// A prefix target is covered only by a star pattern that takes every channel below it; an exact target as `may_use` says.
#[test]
fn a_users_scope_covers_a_prefix_only_through_a_star_pattern() {
    let user = User::new("u", ["room", "chat.*", "ten*"]);
    let prefix = |name: &str| Target::Prefix(name.into());
    assert!(user.covers(&Target::Channel("room".into())) && !user.covers(&Target::Channel("rooms".into())));
    assert!(user.covers(&prefix("chat")), "chat.* takes every chat.<x>");
    assert!(user.covers(&prefix("chat.eu")) && user.covers(&prefix("tenant")) && user.covers(&prefix("ten")));
    assert!(!user.covers(&prefix("room")), "an exact name covers no prefix");
    assert!(!user.covers(&prefix("te")), "te.x is not under ten*");
    assert!(User::new("all", ["*"]).covers(&prefix("anything")));
}

/// A domain value rides on the user and comes back only as its own type.
#[test]
fn domain_data_comes_back_only_as_its_type() {
    struct Tenant(&'static str);
    let user = User::new("u", ["*"]).with_data(Tenant("acme"));
    assert_eq!(user.data::<Tenant>().map(|tenant| tenant.0), Some("acme"));
    assert!(user.data::<String>().is_none());
    assert!(User::new("u", ["*"]).data::<Tenant>().is_none());
    assert_eq!(user.clone().data::<Tenant>().map(|tenant| tenant.0), Some("acme"), "a clone shares the value");
}

/// Debug output names the user and its scope but never prints the domain value.
#[test]
fn debug_output_hides_the_domain_value() {
    let shown = format!("{:?}", User::new("alice", ["a"]).with_data("secret-session"));
    assert!(shown.contains("alice") && shown.contains("data: true"));
    assert!(!shown.contains("secret-session"));
}

/// Each denial reads as a sentence a user can be shown.
#[test]
fn denials_read_as_sentences() {
    assert_eq!(Denied::Missing.to_string(), "authentication required");
    assert_eq!(Denied::Rejected.to_string(), "authentication rejected");
    assert_eq!(Denied::Timeout.to_string(), "authentication timed out");
    assert_eq!(Denied::Unavailable("down".into()).to_string(), "authentication unavailable: down");
}

// ── configured modes ─────────────────────────────────────────────────────

/// `none`: everyone is `anonymous` with the default channels, token or not.
#[test]
fn mode_none_lets_everyone_in_as_anonymous() {
    let config = config(AuthMode::None);
    for token in [None, Some("anything")] {
        let user = local(&config, token).unwrap().unwrap();
        assert_eq!((user.name.as_str(), names(&user)), ("anonymous", vec!["public.*"]));
    }
}

/// `static`: a listed token is its user with its channels, or the default channels when it lists none.
#[test]
fn mode_static_maps_listed_tokens_to_users() {
    let mut config = config(AuthMode::Static);
    config.auth.tokens = vec![StaticToken { token: "t1".into(), user: "bob".into(), channels: vec!["chat.*".into()] }, StaticToken { token: "t2".into(), user: "eve".into(), channels: vec![] }];
    let bob = local(&config, Some("t1")).unwrap().unwrap();
    assert_eq!((bob.name.as_str(), names(&bob)), ("bob", vec!["chat.*"]));
    let eve = local(&config, Some("t2")).unwrap().unwrap();
    assert_eq!(names(&eve), vec!["public.*"]);
}

/// `static`: no token is missing (a protocol may ask), an unlisted or empty token is rejected.
#[test]
fn mode_static_tells_missing_from_rejected() {
    let config = config(AuthMode::Static);
    assert_eq!(local(&config, None).unwrap().unwrap_err(), Denied::Missing);
    assert_eq!(local(&config, Some("nope")).unwrap().unwrap_err(), Denied::Rejected);
    assert_eq!(local(&config, Some("")).unwrap().unwrap_err(), Denied::Rejected);
}

/// `worker`: no token is missing locally; a token is left to the worker.
#[test]
fn mode_worker_leaves_tokens_to_the_worker() {
    let mut config = config(AuthMode::Worker);
    config.auth.worker = Some(WorkerAuth { channel: "auth".into(), event_type: "authenticate".into(), timeout_ms: 10 });
    assert_eq!(local(&config, None).unwrap().unwrap_err(), Denied::Missing);
    assert!(local(&config, Some("t")).is_none());
}

/// A worker's yes with a user lets it in; channels default when it names none; anything else is rejected.
#[test]
fn a_workers_answer_decides() {
    let config = config(AuthMode::Worker);
    let carol = from_answer(&config, AuthAnswer { ok: true, user: Some("carol".into()), channels: vec!["carol.*".into()] }).unwrap();
    assert_eq!((carol.name.as_str(), names(&carol)), ("carol", vec!["carol.*"]));
    let plain = from_answer(&config, AuthAnswer { ok: true, user: Some("dan".into()), channels: vec![] }).unwrap();
    assert_eq!(names(&plain), vec!["public.*"]);
    assert_eq!(from_answer(&config, AuthAnswer { ok: true, user: None, channels: vec![] }).unwrap_err(), Denied::Rejected, "yes without a user is no");
    assert_eq!(from_answer(&config, AuthAnswer { ok: false, user: Some("x".into()), channels: vec![] }).unwrap_err(), Denied::Rejected);
}

/// The worker's answer is read leniently: missing fields default.
#[test]
fn a_workers_answer_reads_with_defaults() {
    let answer: AuthAnswer = serde_json::from_str(r#"{"ok":false}"#).unwrap();
    assert_eq!(answer, AuthAnswer { ok: false, user: None, channels: vec![] });
}
