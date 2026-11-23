//! The routes of each protocol feature, merged into one `axum` router.
//!
//! | feature | routes |
//! | --- | --- |
//! | always | `GET /health`: 200 while the upstream connection is up, 503 otherwise |
//! | `ws` | `GET /v1/ws` ([`ws`]) |
//! | `http` | `POST /v1/events`, `GET /v1/stream` ([`http`]) |
//!
//! [`crate::Server`] serves this router. To add routes of your own, or to
//! put the proxy inside an existing `axum` app, take [`router`] and merge it.

#[cfg(feature = "http")]
pub mod http;
#[cfg(feature = "ws")]
pub mod ws;

use crate::auth::Denied;
use crate::handshake::Handshake;
use crate::hub::Proxy;
use crate::message::Cause;
use axum::extract::{ConnectInfo, State};
use axum::http::request::Parts;
use axum::http::StatusCode;
use axum::routing::get;
use axum::Router;
use std::net::SocketAddr;

/// Every route of the protocols this crate was built with.
pub fn router(proxy: Proxy) -> Router {
    let routes = Router::new().route("/health", get(health));
    #[cfg(feature = "ws")]
    let routes = routes.merge(ws::routes());
    #[cfg(feature = "http")]
    let routes = routes.merge(http::routes());
    routes.with_state(proxy)
}

async fn health(State(proxy): State<Proxy>) -> StatusCode {
    if proxy.is_ready() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// The handshake of a request; `token` is left to the protocol.
fn handshake(protocol: &'static str, parts: &Parts) -> Handshake {
    Handshake { protocol, token: None, headers: parts.headers.clone(), query: parts.uri.query().map(str::to_owned), peer: parts.extensions.get::<ConnectInfo<SocketAddr>>().map(|info| info.0) }
}

/// The HTTP status of a rejection's cause.
#[cfg_attr(not(feature = "http"), allow(dead_code))]
fn status(cause: Option<Cause>) -> StatusCode {
    match cause {
        None => StatusCode::OK,
        Some(Cause::Invalid) => StatusCode::BAD_REQUEST,
        Some(Cause::Forbidden) => StatusCode::FORBIDDEN,
        Some(Cause::Conflict) => StatusCode::CONFLICT,
        Some(Cause::Unavailable) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

/// The HTTP status of a failed authentication.
#[cfg_attr(not(feature = "http"), allow(dead_code))]
fn denied_status(denied: &Denied) -> StatusCode {
    match denied {
        Denied::Missing | Denied::Rejected => StatusCode::UNAUTHORIZED,
        Denied::Timeout | Denied::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

#[cfg(test)]
mod tests;
