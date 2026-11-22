use super::*;

#[test]
fn the_example_configuration_parses_into_grants() {
    let text = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/server.example.json")).unwrap();
    let config = Config::from_json(&text).unwrap();
    let engine = config.engine_config().unwrap();
    assert_eq!(engine.durability, Durability::Strict);
    let grants = engine.grants.unwrap();
    assert!(grants["workers"].allows(Method::Claim, "orders.create"));
    assert!(!grants["workers"].allows(Method::Delete, "orders.create"));
    assert!(grants["proxy"].delegate);
    assert_eq!(config.credentials().unwrap().credential("proxy-secret-change-me"), Some("proxy"));
}

#[test]
fn bad_configurations_are_refused_with_a_reason() {
    let refuse = |text: &str| Config::from_json(text).and_then(|config| config.engine_config()).unwrap_err();
    assert!(refuse(r#"{"listen":"x","credentials":[]}"#).contains("at least one credential"));
    assert!(refuse(r#"{"listen":"x","credentials":[{"name":"a","token":"t","methods":["fly"]}]}"#).contains("unknown method"));
    assert!(refuse(r#"{"listen":"x","credentials":[{"name":"a","token":"t"},{"name":"b","token":"t"}]}"#).contains("share the token"));
    assert!(refuse(r#"{"listen":"x","durability":"eventually","credentials":[{"name":"a","token":"t"}]}"#).contains("durability"));
    assert!(refuse(r#"{"listen":"x","port":1,"credentials":[{"name":"a","token":"t"}]}"#).contains("unknown field"));
}
