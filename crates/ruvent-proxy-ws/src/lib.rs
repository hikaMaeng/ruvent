//! The WebSocket proxy: one WebSocket per user, the user JSON of
//! [`ruvent_proxy::message`] in text frames.
//!
//! * `GET /v1/ws?token=…` upgrades. Without `token`, and unless `auth.mode`
//!   is `none`, the first message must be `{"op":"auth","token":"…"}`.
//! * `GET /health` answers 200 while the upstream connection is up, 503 otherwise.
//!
//! After authentication the proxy sends `{"op":"result","ok":true}`. A user
//! whose send buffer fills is disconnected.

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use ruvent_proxy::config::AuthMode;
use ruvent_proxy::{bad_request, Outcome, Proxy, Request};
use serde::Deserialize;
use std::io;
use tokio::net::TcpListener;

#[derive(Deserialize)]
struct Login {
    token: Option<String>,
}

/// The proxy's routes.
pub fn router(proxy: Proxy) -> Router {
    Router::new().route("/v1/ws", get(upgrade)).route("/health", get(health)).with_state(proxy)
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

async fn upgrade(State(proxy): State<Proxy>, Query(login): Query<Login>, ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(move |socket| session(proxy, login.token, socket)).into_response()
}

fn text(outcome: &Outcome) -> Message {
    Message::Text(outcome.text().as_ref().into())
}

async fn session(proxy: Proxy, token: Option<String>, socket: WebSocket) {
    let (mut sink, mut stream) = socket.split();
    let token = match token {
        Some(token) => Some(token),
        None if proxy.config().auth.mode == AuthMode::None => None,
        None => loop {
            match stream.next().await {
                Some(Ok(Message::Text(first))) => match Request::parse(first.as_str()) {
                    Ok(Request::Auth { token }) => break Some(token),
                    _ => {
                        let _ = sink.send(text(&Outcome::failed(None, "authenticate first: {\"op\":\"auth\",\"token\":…}"))).await;
                        return;
                    }
                },
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                _ => return,
            }
        },
    };
    let user = match proxy.authenticate(token.as_deref()).await {
        Ok(user) => user,
        Err(denied) => {
            let _ = sink.send(text(&Outcome::failed(None, denied.to_string()))).await;
            let _ = sink.close().await;
            return;
        }
    };
    let Ok((handle, mut events)) = proxy.attach(user).await else { return };
    handle.reply(Outcome::ok(None, None)).await;
    let mut writer = tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            if sink.send(Message::Text(event.as_ref().into())).await.is_err() {
                return;
            }
        }
        // The stream ended: the proxy disconnected this user.
        let _ = sink.close().await;
    });
    loop {
        tokio::select! {
            message = stream.next() => match message {
                Some(Ok(Message::Text(body))) => match Request::parse(body.as_str()) {
                    Ok(request) => handle.handle(request).await,
                    Err(error) => handle.reply(bad_request(error)).await,
                },
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                Some(Ok(_)) => {}
            },
            _ = &mut writer => break,
        }
    }
    drop(handle);
    writer.abort();
}
