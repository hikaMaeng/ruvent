//! HTTP: register over `POST`, subscribe over server-sent events.
//!
//! * `POST /v1/events` — body is a publish (the user JSON of
//!   [`crate::message`] without `op`); the handshake's token is
//!   `Authorization: Bearer <token>`. Answers the result JSON: 200 when
//!   registered, 401 when authentication fails, and for a rejection the
//!   status of its [`crate::Cause`]: 400 invalid, 403 forbidden, 409 conflict,
//!   503 unavailable.
//! * `GET /v1/stream?channel=a&channel=b&prefix=c` — subscribes and streams
//!   events as SSE `data:` lines, one event JSON each. `prefix=c` takes every
//!   channel below `c` at a `.` boundary. The stream starts only after every
//!   subscription took effect; it ends when the user's buffer fills.
//!
//! `POST /v1/events` answers once the registration is committed on the
//! server the proxy's producer picked (2.3). There is no per-registration
//! answer from the event server: re-posting an id that is still waiting, with
//! the same content, is accepted and registers nothing twice.
//!
//! Every request authenticates on its own; with the `worker` mode that is
//! one event round trip per request. A missing or empty bearer token is
//! [`Denied::Missing`] and answers 401 like a rejected one.
//!
//! A request's own shape is checked before the publish hook: a malformed
//! `id` answers 400 even when the hook would also forbid the request.

use super::{denied_status, status};
use crate::auth::Denied;
use crate::handshake::query_values;
use crate::hub::Proxy;
use crate::message::{Outcome, Publish};
use axum::body::Bytes;
use axum::extract::State;
use axum::http::request::Parts;
use axum::http::StatusCode;
use axum::response::sse::{Event, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::stream;
use ruvent::{Name, Target};
use std::convert::Infallible;
use std::sync::Arc;

pub(super) fn routes() -> Router<Proxy> {
    Router::new().route("/v1/events", post(publish)).route("/v1/stream", get(subscribe))
}

fn denied(denied: Denied) -> Response {
    (denied_status(&denied), Json(Outcome::failed(None, denied.to_string()))).into_response()
}

fn answer(outcome: Outcome) -> Response {
    (status(outcome.cause), Json(outcome)).into_response()
}

async fn authenticate(proxy: &Proxy, parts: &Parts) -> Result<Arc<crate::User>, Denied> {
    let mut handshake = super::handshake("http", parts);
    handshake.token = handshake.bearer().map(str::to_owned);
    proxy.authenticate(&handshake).await
}

async fn publish(State(proxy): State<Proxy>, parts: Parts, body: Bytes) -> Response {
    let user = match authenticate(&proxy, &parts).await {
        Ok(user) => user,
        Err(error) => return denied(error),
    };
    let publish: Publish = match serde_json::from_slice(&body) {
        Ok(publish) => publish,
        Err(error) => return answer(crate::hub::bad_request(error)),
    };
    answer(proxy.publish(&user, publish).await)
}

async fn subscribe(State(proxy): State<Proxy>, parts: Parts) -> Response {
    let user = match authenticate(&proxy, &parts).await {
        Ok(user) => user,
        Err(error) => return denied(error),
    };
    // An empty `channel=` or `prefix=` names nothing and is skipped.
    let query = parts.uri.query().unwrap_or_default();
    let named = |key: &str| query_values(query, key).into_iter().filter(|name| !name.is_empty());
    let channels = named("channel").map(|channel| Target::Channel(Name::new(&channel)));
    let wanted: Vec<Target> = channels.chain(named("prefix").map(|prefix| Target::Prefix(Name::new(&prefix)))).collect();
    if wanted.is_empty() {
        return answer(Outcome::failed(None, "name at least one channel: ?channel=… or ?prefix=…"));
    }
    let (handle, events) = match proxy.attach(user).await {
        Ok(attached) => attached,
        Err(error) => return (StatusCode::SERVICE_UNAVAILABLE, Json(Outcome::failed(None, error))).into_response(),
    };
    for target in wanted {
        let shown = match &target {
            Target::Channel(name) => name.to_string(),
            Target::Prefix(name) => format!("{name}.*"),
        };
        if let Err(mut rejection) = handle.subscribe_to(target).await {
            rejection.message = format!("{shown}: {}", rejection.message);
            return answer(Outcome::rejected(None, rejection));
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
