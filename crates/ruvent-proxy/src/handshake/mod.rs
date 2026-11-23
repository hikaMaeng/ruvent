//! What a user's connection or request carried, as [`crate::Hooks::authenticate`] sees it.
//!
//! The protocols fill it the same way: headers, the raw query, the peer
//! address when the server knows it, and `token` where the protocol has a
//! place for one (WebSocket: `?token=` or a first `{"op":"auth"}` message;
//! HTTP: `Authorization: Bearer`).
//!
//! Reading is all this module does; what an empty or repeated value means
//! is the protocol's decision. The query is read as
//! `application/x-www-form-urlencoded`: keys and values are both decoded,
//! `+` is a space, and every occurrence of a key is kept in order.
//!
//! `peer` is the TCP peer. Behind a reverse proxy it is that proxy, not the
//! user; headers such as `X-Forwarded-For` are whatever the client sent
//! unless something you run in front rewrites them.

use http::HeaderMap;
use std::net::SocketAddr;

#[derive(Clone, Debug)]
pub struct Handshake {
    /// `"ws"` or `"http"`.
    pub protocol: &'static str,
    /// The token the protocol found, if any.
    pub token: Option<String>,
    pub headers: HeaderMap,
    /// The raw query string, without `?`.
    pub query: Option<String>,
    /// The TCP peer, when the server was started with connection info (the [`crate::Server`] always is).
    pub peer: Option<SocketAddr>,
}

impl Handshake {
    /// An empty handshake, for tests and for embedding.
    pub fn new(protocol: &'static str) -> Handshake {
        Handshake { protocol, token: None, headers: HeaderMap::new(), query: None, peer: None }
    }

    /// A header as text; `None` when absent or not visible ASCII. The first one when repeated.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name)?.to_str().ok()
    }

    /// The token of `Authorization: Bearer <token>`. The scheme is matched
    /// without regard to case; an empty token is no token.
    pub fn bearer(&self) -> Option<&str> {
        let (scheme, token) = self.header("authorization")?.trim().split_once(' ')?;
        let token = token.trim();
        (scheme.eq_ignore_ascii_case("bearer") && !token.is_empty()).then_some(token)
    }

    /// One cookie of the `Cookie` headers: the first pair with this exact name.
    pub fn cookie(&self, name: &str) -> Option<&str> {
        self.headers.get_all("cookie").iter().filter_map(|value| value.to_str().ok()).flat_map(|value| value.split(';')).find_map(|pair| {
            let (key, value) = pair.trim().split_once('=')?;
            (key.trim() == name).then_some(value.trim())
        })
    }

    /// The first value of a query parameter, decoded. An empty value is `Some("")`.
    pub fn query_value(&self, name: &str) -> Option<String> {
        query_values(self.query.as_deref()?, name).into_iter().next()
    }
}

/// Every value of `name` in a query string, in order, decoded; empty values included.
pub fn query_values(query: &str, name: &str) -> Vec<String> {
    form_urlencoded::parse(query.as_bytes()).filter(|(key, _)| key == name).map(|(_, value)| value.into_owned()).collect()
}

#[cfg(test)]
mod tests;
