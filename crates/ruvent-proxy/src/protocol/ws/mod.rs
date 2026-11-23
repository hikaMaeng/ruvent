//! WebSocket: one socket per user, the user JSON of [`crate::message`] in text frames.
//!
//! * `GET /v1/ws` upgrades. The handshake's token is `?token=`. When
//!   authentication answers [`Denied::Missing`] and there was no token, the
//!   first message must be `{"op":"auth","token":"…"}`, and authentication
//!   runs again with it.
//!
//! After authentication the proxy sends `{"op":"result","ok":true}`. A user
//! whose send buffer fills is disconnected: the closed socket is the signal
//! that it lost events.

use crate::auth::Denied;
use crate::hub::{bad_request, Proxy};
use crate::message::{Outcome, Request};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::request::Parts;
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};

pub(super) fn routes() -> Router<Proxy> {
    Router::new().route("/v1/ws", get(upgrade))
}

async fn upgrade(State(proxy): State<Proxy>, parts: Parts, ws: WebSocketUpgrade) -> Response {
    let mut handshake = super::handshake("ws", &parts);
    handshake.token = handshake.query_value("token");
    ws.on_upgrade(move |socket| session(proxy, handshake, socket))
}

fn text(outcome: &Outcome) -> Message {
    Message::Text(outcome.text().as_ref().into())
}

/// Reads the token of a first `{"op":"auth"}` message; anything else ends the session.
async fn in_band_token(sink: &mut SplitSink<WebSocket, Message>, stream: &mut SplitStream<WebSocket>) -> Option<String> {
    loop {
        match stream.next().await {
            Some(Ok(Message::Text(first))) => {
                if let Ok(Request::Auth { token }) = Request::parse(first.as_str()) {
                    return Some(token);
                }
                let _ = sink.send(text(&Outcome::failed(None, "authenticate first: {\"op\":\"auth\",\"token\":…}"))).await;
                return None;
            }
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
            _ => return None,
        }
    }
}

async fn session(proxy: Proxy, mut handshake: crate::Handshake, socket: WebSocket) {
    let (mut sink, mut stream) = socket.split();
    let user = loop {
        match proxy.authenticate(&handshake).await {
            Ok(user) => break user,
            Err(Denied::Missing) if handshake.token.is_none() => match in_band_token(&mut sink, &mut stream).await {
                Some(token) => handshake.token = Some(token),
                None => return,
            },
            Err(denied) => {
                let _ = sink.send(text(&Outcome::failed(None, denied.to_string()))).await;
                let _ = sink.close().await;
                return;
            }
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
