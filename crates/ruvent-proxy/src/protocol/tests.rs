//! What the protocols share: status codes and the handshake built from a request.

use super::*;
use axum::extract::ConnectInfo;
use axum::http::Request;

/// Each rejection cause has its own status; no cause is 200.
#[test]
fn every_cause_has_its_status() {
    assert_eq!(status(None), StatusCode::OK);
    assert_eq!(status(Some(Cause::Invalid)), StatusCode::BAD_REQUEST);
    assert_eq!(status(Some(Cause::Forbidden)), StatusCode::FORBIDDEN);
    assert_eq!(status(Some(Cause::Conflict)), StatusCode::CONFLICT);
    assert_eq!(status(Some(Cause::Unavailable)), StatusCode::SERVICE_UNAVAILABLE);
}

/// Missing and rejected credentials are 401; a worker that is slow or unreachable is 503.
#[test]
fn every_denial_has_its_status() {
    assert_eq!(denied_status(&Denied::Missing), StatusCode::UNAUTHORIZED);
    assert_eq!(denied_status(&Denied::Rejected), StatusCode::UNAUTHORIZED);
    assert_eq!(denied_status(&Denied::Timeout), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(denied_status(&Denied::Unavailable("x".into())), StatusCode::SERVICE_UNAVAILABLE);
}

/// The handshake carries the request's headers, raw query and peer, and no token until a protocol sets one.
#[test]
fn a_handshake_is_built_from_the_request() {
    let peer: SocketAddr = "10.0.0.7:5000".parse().unwrap();
    let mut request = Request::get("/v1/ws?token=a%20b&x=1").header("cookie", "session=s").body(()).unwrap();
    request.extensions_mut().insert(ConnectInfo(peer));
    let (parts, ()) = request.into_parts();
    let handshake = handshake("ws", &parts);
    assert_eq!(handshake.protocol, "ws");
    assert_eq!(handshake.token, None);
    assert_eq!(handshake.query.as_deref(), Some("token=a%20b&x=1"));
    assert_eq!(handshake.query_value("token").as_deref(), Some("a b"));
    assert_eq!(handshake.cookie("session"), Some("s"));
    assert_eq!(handshake.peer, Some(peer));
}

/// Without connection info (a router mounted elsewhere) the peer is unknown, not invented.
#[test]
fn without_connection_info_the_peer_is_unknown() {
    let (parts, ()) = Request::get("/v1/stream").body(()).unwrap().into_parts();
    let handshake = handshake("http", &parts);
    assert_eq!((handshake.peer, handshake.query), (None, None));
}
