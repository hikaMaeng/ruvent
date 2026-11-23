//! The JSON configuration: shape, defaults and the rules `check` adds.

use super::*;

const MINIMAL: &str = r#"{"listen":"127.0.0.1:0","upstream":{"address":"a","token":"t"}}"#;

fn with(extra: &str) -> String {
    format!(r#"{{"listen":"127.0.0.1:0","upstream":{{"address":"a","token":"t"}},{extra}}}"#)
}

fn refused(text: &str, needle: &str) {
    let error = ProxyConfig::parse(text).expect_err(text);
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains(needle), "`{error}` does not mention `{needle}`");
}

/// The example file in the repository is a valid configuration.
#[test]
fn the_example_configuration_parses() {
    let config = ProxyConfig::parse(include_str!("../../proxy.example.json")).unwrap();
    assert_eq!(config.auth.mode, AuthMode::Worker);
    assert!(config.auth.worker.is_some());
}

/// Only `listen` and `upstream` are required; everything else has a documented default.
#[test]
fn a_minimal_configuration_takes_the_defaults() {
    let config = ProxyConfig::parse(MINIMAL).unwrap();
    assert_eq!(config.auth.mode, AuthMode::None);
    assert_eq!(config.channels, vec!["*"]);
    assert_eq!((config.user_buffer, config.delegate), (1024, true));
    assert_eq!((config.reconnect.initial_ms, config.reconnect.max_ms), (100, 5_000));
    let worker = ProxyConfig::parse(&with(r#""auth":{"mode":"worker","worker":{"channel":"c","reply_channel":"r"}}"#)).unwrap().auth.worker.unwrap();
    assert_eq!((worker.event_type.as_str(), worker.timeout_ms), ("authenticate", 5_000));
}

/// A misspelt key is an error, at the top level and inside sections.
#[test]
fn unknown_keys_are_refused() {
    refused(&with(r#""chanels":["x"]"#), "unknown field");
    refused(&with(r#""auth":{"mod":"none"}"#), "unknown field");
    refused(r#"{"listen":"x"}"#, "upstream");
}

/// `worker` mode without its section is refused.
#[test]
fn worker_mode_without_its_section_is_refused() {
    refused(&with(r#""auth":{"mode":"worker"}"#), "auth.worker is missing");
}

/// Values that would make the proxy useless are refused rather than adjusted.
#[test]
fn values_that_cannot_work_are_refused() {
    refused(&with(r#""user_buffer":0"#), "user_buffer");
    refused(&with(r#""reconnect":{"initial_ms":0,"max_ms":10}"#), "reconnect");
    refused(&with(r#""reconnect":{"initial_ms":20,"max_ms":10}"#), "reconnect");
    refused(&with(r#""auth":{"mode":"worker","worker":{"channel":"c","reply_channel":"r","timeout_ms":0}}"#), "timeout_ms");
}

/// A configuration built in code goes through the same check as a file.
#[test]
fn a_configuration_built_in_code_is_checked_the_same_way() {
    let mut config = ProxyConfig::open("127.0.0.1:0", "x");
    assert!(config.check().is_ok());
    config.user_buffer = 0;
    assert!(config.check().is_err());
}

/// A file that cannot be read is an error naming the reason, not a panic.
#[test]
fn a_missing_file_is_an_error() {
    let error = ProxyConfig::load(Path::new("no/such/proxy.json")).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
}
