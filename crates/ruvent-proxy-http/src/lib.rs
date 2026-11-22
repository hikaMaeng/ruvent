//! The HTTP proxy: register over `POST`, subscribe over server-sent events.
//!
//! * `POST /v1/events` — body is a publish (the user JSON of
//!   [`ruvent_proxy::message`] without `op`), `Authorization: Bearer <token>`.
//!   Answers the result JSON: 200 when registered, 403 for a channel outside
//!   the user's, 409 for a duplicate id, 400 otherwise, 401 when
//!   authentication fails.
//! * `GET /v1/stream?channel=a&channel=b` — subscribes and streams events as
//!   SSE `data:` lines, one event JSON each. The stream starts only after
//!   every subscription took effect; it ends when the user's buffer fills.
//! * `GET /health` — 200 while the upstream connection is up, 503 otherwise.
//!
//! Every request authenticates on its own; in `worker` mode that is one event
//! round trip per request.

use axum::body::Bytes;
use axum::extract::{RawQuery, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::stream;
use ruvent_proxy::{Denied, Outcome, Proxy, Publish};
use std::convert::Infallible;
use std::io;
use tokio::net::TcpListener;

/// The proxy's routes.
pub fn router(proxy: Proxy) -> Router {
    Router::new().route("/v1/events", post(publish)).route("/v1/stream", get(subscribe)).route("/health", get(health)).with_state(proxy)
}

/// Serves until the listener fails.
pub async fn serve(listener: TcpListener, proxy: Proxy) -> io::Result<()> {
    axum::serve(listener, router(proxy)).await
}

async fn health(State(proxy): State<Proxy>) -> StatusCode {
    if proxy.is_ready() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers.get(header::AUTHORIZATION)?.to_str().ok()?.strip_prefix("Bearer ")
}

fn denied(denied: Denied) -> Response {
    let status = match denied {
        Denied::Rejected => StatusCode::UNAUTHORIZED,
        Denied::Timeout | Denied::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
    };
    (status, Json(Outcome::failed(None, denied.to_string()))).into_response()
}

fn status_of(outcome: &Outcome) -> StatusCode {
    match outcome.error.as_deref() {
        None => StatusCode::OK,
        Some(error) if error.starts_with("forbidden") => StatusCode::FORBIDDEN,
        Some("duplicate") => StatusCode::CONFLICT,
        Some("upstream unavailable") | Some("closed") => StatusCode::SERVICE_UNAVAILABLE,
        Some(_) => StatusCode::BAD_REQUEST,
    }
}

async fn publish(State(proxy): State<Proxy>, headers: HeaderMap, body: Bytes) -> Response {
    let user = match proxy.authenticate(bearer(&headers)).await {
        Ok(user) => user,
        Err(error) => return denied(error),
    };
    let publish: Publish = match serde_json::from_slice(&body) {
        Ok(publish) => publish,
        Err(error) => return (StatusCode::BAD_REQUEST, Json(ruvent_proxy::bad_request(error))).into_response(),
    };
    let outcome = proxy.publish(&user, publish).await;
    (status_of(&outcome), Json(outcome)).into_response()
}

/// Decodes `%XX` and `+` in one query value.
fn unescape(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut at = 0;
    while at < bytes.len() {
        match bytes[at] {
            b'+' => out.push(b' '),
            b'%' if at + 2 < bytes.len() => match u8::from_str_radix(std::str::from_utf8(&bytes[at + 1..at + 3]).unwrap_or("zz"), 16) {
                Ok(byte) => {
                    out.push(byte);
                    at += 2;
                }
                Err(_) => out.push(b'%'),
            },
            other => out.push(other),
        }
        at += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Every `channel=` value of a query string, in order.
pub fn channels(query: &str) -> Vec<String> {
    query.split('&').filter_map(|pair| pair.strip_prefix("channel=")).map(unescape).filter(|channel| !channel.is_empty()).collect()
}

async fn subscribe(State(proxy): State<Proxy>, headers: HeaderMap, RawQuery(query): RawQuery) -> Response {
    let user = match proxy.authenticate(bearer(&headers)).await {
        Ok(user) => user,
        Err(error) => return denied(error),
    };
    let wanted = channels(query.as_deref().unwrap_or_default());
    if wanted.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(Outcome::failed(None, "name at least one channel: ?channel=…"))).into_response();
    }
    let (handle, events) = match proxy.attach(user).await {
        Ok(attached) => attached,
        Err(error) => return (StatusCode::SERVICE_UNAVAILABLE, Json(Outcome::failed(None, error))).into_response(),
    };
    for channel in &wanted {
        if let Err(error) = handle.subscribe(channel).await {
            let outcome = Outcome::failed(None, format!("{channel}: {error}"));
            return (status_of(&outcome), Json(outcome)).into_response();
        }
    }
    // The handle lives in the stream: when the client goes away the stream is
    // dropped and the user detached.
    let events = stream::unfold((events, handle), |(mut events, handle)| async move {
        let text = events.recv().await?;
        Some((Ok::<_, Infallible>(Event::default().data(text.as_ref())), (events, handle)))
    });
    Sse::new(events).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_values_are_collected_and_unescaped() {
        assert_eq!(channels("channel=a&x=1&channel=b%2Ec&channel=d+e&channel="), vec!["a", "b.c", "d e"]);
        assert_eq!(unescape("50%"), "50%");
        assert_eq!(unescape("%zz"), "%zz");
    }
}
