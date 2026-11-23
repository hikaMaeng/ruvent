//! Reading a handshake: query values, bearer tokens, cookies, headers.

use super::*;

fn with_header(name: &'static str, value: &str) -> Handshake {
    let mut handshake = Handshake::new("http");
    handshake.headers.append(name, value.parse().unwrap());
    handshake
}

// ── query ────────────────────────────────────────────────────────────────

/// Every value of a key, in order, decoded; other keys and keys that only share a prefix are skipped.
#[test]
fn channel_values_are_collected_in_order_and_decoded() {
    assert_eq!(query_values("channel=a&x=1&channel=b%2Ec&channel=d+e&channels=z", "channel"), vec!["a", "b.c", "d e"]);
}

/// Keys are decoded like values, so an encoded key is the same key.
#[test]
fn encoded_keys_are_the_same_key() {
    assert_eq!(query_values("%74oken=t1&to%6Ben=t2", "token"), vec!["t1", "t2"]);
}

/// Empty values are kept: what an empty value means is the protocol's decision.
#[test]
fn empty_values_are_kept_for_the_protocol_to_judge() {
    assert_eq!(query_values("channel=&channel=a&channel", "channel"), vec!["", "a", ""]);
    let mut handshake = Handshake::new("ws");
    handshake.query = Some("token=".into());
    assert_eq!(handshake.query_value("token").as_deref(), Some(""));
}

/// A repeated key: `query_value` is the first occurrence.
#[test]
fn the_first_of_a_repeated_key_wins() {
    let mut handshake = Handshake::new("ws");
    handshake.query = Some("token=first&token=second".into());
    assert_eq!(handshake.query_value("token").as_deref(), Some("first"));
    handshake.query = None;
    assert_eq!(handshake.query_value("token"), None);
}

/// Broken percent escapes are kept as text instead of failing.
#[test]
fn broken_escapes_are_kept_as_text() {
    assert_eq!(query_values("v=50%&w=%zz&x=%e2%82%ac", "v"), vec!["50%"]);
    assert_eq!(query_values("v=50%&w=%zz&x=%e2%82%ac", "w"), vec!["%zz"]);
    assert_eq!(query_values("v=50%&w=%zz&x=%e2%82%ac", "x"), vec!["€"]);
}

// ── bearer ───────────────────────────────────────────────────────────────

/// `Authorization: Bearer <token>`, the scheme in any case, surrounding spaces trimmed.
#[test]
fn a_bearer_token_is_read_whatever_the_schemes_case() {
    assert_eq!(with_header("authorization", "Bearer abc").bearer(), Some("abc"));
    assert_eq!(with_header("authorization", "bearer abc").bearer(), Some("abc"));
    assert_eq!(with_header("authorization", "BEARER   abc ").bearer(), Some("abc"));
}

/// Another scheme, a scheme with no token, or no header at all is no bearer token.
#[test]
fn other_schemes_and_empty_tokens_are_no_bearer_token() {
    assert_eq!(with_header("authorization", "Basic dTpw").bearer(), None);
    assert_eq!(with_header("authorization", "Bearer").bearer(), None);
    assert_eq!(with_header("authorization", "Bearer    ").bearer(), None);
    assert_eq!(Handshake::new("http").bearer(), None);
}

// ── cookies ──────────────────────────────────────────────────────────────

/// Cookies are found across several `Cookie` headers; the first pair of a name wins.
#[test]
fn cookies_are_found_across_headers_and_the_first_wins() {
    let mut handshake = with_header("cookie", "a=1; session=s9");
    handshake.headers.append("cookie", "b=2; session=later".parse().unwrap());
    assert_eq!((handshake.cookie("session"), handshake.cookie("b"), handshake.cookie("nope")), (Some("s9"), Some("2"), None));
}

/// A name must match exactly: no prefix match, no case folding; pairs without `=` are skipped.
#[test]
fn a_cookie_name_matches_exactly() {
    let handshake = with_header("cookie", "flag; sessionid=x; Session=y; session = z");
    assert_eq!(handshake.cookie("session"), Some("z"));
    assert_eq!(handshake.cookie("flag"), None);
    assert_eq!(with_header("cookie", "k=a=b").cookie("k"), Some("a=b"), "a value may contain =");
}

// ── headers ──────────────────────────────────────────────────────────────

/// A header that is not visible ASCII reads as absent instead of failing.
#[test]
fn a_header_that_is_not_text_reads_as_absent() {
    let mut handshake = Handshake::new("http");
    handshake.headers.insert("x-raw", http::HeaderValue::from_bytes(b"\xfe\xff").unwrap());
    assert_eq!(handshake.header("x-raw"), None);
    assert_eq!(with_header("x-name", "v").header("X-Name"), Some("v"), "header names are case-insensitive");
}
