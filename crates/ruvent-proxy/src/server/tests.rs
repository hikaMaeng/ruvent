//! The server builder's decisions that need no network: where the
//! configuration comes from, and how a failure reads.

use super::*;
use std::ffi::OsString;
use std::path::Path;

fn arguments(list: &[&str]) -> Vec<OsString> {
    list.iter().map(OsString::from).collect()
}

/// The configuration is the program's first argument; later arguments are ignored.
#[test]
fn the_first_argument_names_the_configuration() {
    assert_eq!(config_path(arguments(&["my-proxy", "proxy.json", "extra"])).ok(), Some(PathBuf::from("proxy.json")));
}

/// A program run with no argument is told how to run it.
#[test]
fn no_argument_is_a_usage_error() {
    assert!(matches!(config_path(arguments(&["my-proxy"])), Err(LaunchError::Usage)));
    assert!(matches!(config_path(Vec::new()), Err(LaunchError::Usage)));
    assert_eq!(LaunchError::Usage.to_string(), "usage: <program> <config.json>");
}

/// A configuration given in code is checked like a file and refused with the rule it breaks.
#[test]
fn a_configuration_given_in_code_is_checked() {
    let mut config = ProxyConfig::open("127.0.0.1:0", "x");
    config.user_buffer = 0;
    let error = Source::Given(Box::new(config)).resolve(Vec::new()).expect_err("refused");
    assert!(matches!(error, LaunchError::Invalid(_)));
    assert_eq!(error.to_string(), "invalid configuration: user_buffer must be at least 1");
}

/// A configuration file that is missing or invalid is refused naming the file.
#[test]
fn a_bad_configuration_file_is_refused_naming_it() {
    let error = Source::Arguments.resolve(arguments(&["p", "no/such/proxy.json"])).expect_err("refused");
    assert!(matches!(&error, LaunchError::Config { path, .. } if path == Path::new("no/such/proxy.json")));
    assert!(error.to_string().starts_with("no/such/proxy.json: "), "{error}");
}

/// What `main` prints for an error is the sentence, not a debug dump.
#[test]
fn a_launch_error_prints_as_its_sentence() {
    let error = LaunchError::Listen { address: "1.2.3.4:5".into(), error: io::Error::new(io::ErrorKind::AddrInUse, "in use") };
    assert_eq!(format!("{error:?}"), "cannot listen on 1.2.3.4:5: in use");
    assert_eq!(LaunchError::Signal(io::Error::other("no handler")).to_string(), "cannot listen for the shutdown signal: no handler");
    assert_eq!(LaunchError::Serve(io::Error::other("gone")).to_string(), "proxy stopped: gone");
}

/// A server that cannot bind its address says which address.
#[tokio::test]
async fn an_address_that_cannot_be_bound_is_named() {
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = taken.local_addr().unwrap().to_string();
    let error = Server::new().config(ProxyConfig::open(&address, "x")).bind().await.err().expect("refused");
    assert!(matches!(&error, LaunchError::Listen { address: named, .. } if *named == address));
}
