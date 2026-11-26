use super::*;

#[test]
fn the_example_configuration_parses_into_grants_and_every_section() {
    let text = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/server.example.json")).unwrap();
    let config = Config::from_json(&text).unwrap();
    let engine = config.engine_config().unwrap();
    assert_eq!(engine.durability, Durability::Strict);
    let grants = engine.grants.as_ref().unwrap();
    assert!(grants["workers"].allows(Method::Claim));
    assert!(!grants["workers"].allows(Method::Delete));
    assert!(grants["proxy"].allows(Method::Delete));
    assert!(grants["ops"].allows(Method::Operate) && !grants["ops"].allows(Method::Claim));
    assert_eq!(config.credentials().unwrap().credential("proxy-secret-change-me"), Some("proxy"));
    assert_eq!(engine.memory.limit, Some(8 << 30));
    assert_eq!(engine.storage.disk_capacity, Some(100 << 30));
    assert!(engine.snapshot.as_ref().unwrap().wants(11) && !engine.snapshot.as_ref().unwrap().wants(150));
    let options = config.options();
    assert_eq!((options.unsent, options.close, options.keepalive.retries), (256 << 20, Duration::from_secs(5), 3));
}

/// Every value of chapter 8 has its default when the file says nothing.
#[test]
fn a_minimal_configuration_takes_every_default() {
    let config = Config::from_json(r#"{"listen":"127.0.0.1:0","credentials":[{"name":"a","token":"t"}]}"#).unwrap();
    let engine = config.engine_config().unwrap();
    let defaults = EngineConfig::default();
    assert_eq!((engine.ack_every, engine.max_follow_ups, engine.stats_top), (defaults.ack_every, defaults.max_follow_ups, defaults.stats_top));
    assert_eq!(engine.memory, defaults.memory);
    assert_eq!(engine.storage, defaults.storage);
    assert_eq!(engine.producers, defaults.producers);
    let options = config.options();
    let server = ServerOptions::default();
    assert_eq!((options.unsent, options.close, options.keepalive, options.inline, options.direct), (server.unsent, server.close, server.keepalive, server.inline, server.direct));
}

#[test]
fn bad_configurations_are_refused_with_a_reason() {
    let refuse = |text: &str| Config::from_json(text).and_then(|config| config.engine_config()).unwrap_err();
    assert!(refuse(r#"{"listen":"x","credentials":[]}"#).contains("at least one credential"));
    assert!(refuse(r#"{"listen":"x","credentials":[{"name":"a","token":"t","methods":["fly"]}]}"#).contains("unknown method"));
    assert!(refuse(r#"{"listen":"x","credentials":[{"name":"a","token":"t"},{"name":"b","token":"t"}]}"#).contains("share the token"));
    assert!(refuse(r#"{"listen":"x","durability":"eventually","credentials":[{"name":"a","token":"t"}]}"#).contains("durability"));
    assert!(refuse(r#"{"listen":"x","port":1,"credentials":[{"name":"a","token":"t"}]}"#).contains("unknown field"));
    assert!(refuse(r#"{"listen":"x","memory":{"reserve":2},"credentials":[{"name":"a","token":"t"}]}"#).contains("fraction"));
    assert!(refuse(r#"{"listen":"x","server_id":"nope","credentials":[{"name":"a","token":"t"}]}"#).contains("server_id"));
    assert!(refuse(r#"{"listen":"x","compaction":{},"credentials":[{"name":"a","token":"t"}]}"#).contains("unknown field"));
    // Channel patterns, type scopes, delegation and type rules are gone (plan 9.15).
    assert!(refuse(r#"{"listen":"x","credentials":[{"name":"a","token":"t","channels":["a.*"]}]}"#).contains("unknown field"));
    assert!(refuse(r#"{"listen":"x","credentials":[{"name":"a","token":"t","types":[1]}]}"#).contains("unknown field"));
    assert!(refuse(r#"{"listen":"x","credentials":[{"name":"a","token":"t","delegate":true}]}"#).contains("unknown field"));
    assert!(refuse(r#"{"listen":"x","types":{},"credentials":[{"name":"a","token":"t"}]}"#).contains("unknown field"));
    // Pausing producers and the name dictionary are gone too.
    assert!(refuse(r#"{"listen":"x","engine":{"pause_allowance":1},"credentials":[{"name":"a","token":"t"}]}"#).contains("unknown field"));
    assert!(refuse(r#"{"listen":"x","memory":{"early_pause":0.1},"credentials":[{"name":"a","token":"t"}]}"#).contains("unknown field"));
    assert!(refuse(r#"{"listen":"x","connection":{"dictionary":4096},"credentials":[{"name":"a","token":"t"}]}"#).contains("unknown field"));
}
