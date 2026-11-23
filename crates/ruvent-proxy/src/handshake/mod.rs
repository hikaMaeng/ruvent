//! What a user's connection or request carried, as [`crate::Hooks::authenticate`] sees it.
//!
//! The protocols fill it the same way: headers, the raw query, the peer
//! address when the server knows it, and `token` where the protocol has a
//! place for one (WebSocket: `?token=` or a first `{"op":"auth"}` message;
//! HTTP: `Authorization: Bearer`).

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
    /// The peer, when the server was started with connection info (the [`crate::Server`] always is).
    pub peer: Option<SocketAddr>,
}

impl Handshake {
    /// An empty handshake, for tests and for embedding.
    pub fn new(protocol: &'static str) -> Handshake {
        Handshake { protocol, token: None, headers: HeaderMap::new(), query: None, peer: None }
    }

    /// A header as text; `None` when absent or not visible ASCII.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name)?.to_str().ok()
    }

    /// The token of `Authorization: Bearer <token>`.
    pub fn bearer(&self) -> Option<&str> {
        self.header("authorization")?.strip_prefix("Bearer ")
    }

    /// One cookie of the `Cookie` headers.
    pub fn cookie(&self, name: &str) -> Option<&str> {
        self.headers.get_all("cookie").iter().filter_map(|value| value.to_str().ok()).flat_map(|value| value.split(';')).find_map(|pair| {
            let (key, value) = pair.trim().split_once('=')?;
            (key == name).then_some(value)
        })
    }

    /// The first value of a query parameter, decoded.
    pub fn query_value(&self, name: &str) -> Option<String> {
        query_values(self.query.as_deref()?, name).into_iter().next()
    }
}

/// Every value of `name` in a query string, in order, decoded. Empty values are left out.
pub fn query_values(query: &str, name: &str) -> Vec<String> {
    query.split('&').filter_map(|pair| pair.strip_prefix(name)?.strip_prefix('=')).map(unescape).filter(|value| !value.is_empty()).collect()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_values_are_collected_and_unescaped() {
        assert_eq!(query_values("channel=a&x=1&channel=b%2Ec&channel=d+e&channel=&channels=z", "channel"), vec!["a", "b.c", "d e"]);
        assert_eq!(unescape("50%"), "50%");
        assert_eq!(unescape("%zz"), "%zz");
    }

    #[test]
    fn headers_cookies_and_bearer_tokens_are_read() {
        let mut handshake = Handshake::new("http");
        handshake.headers.insert("authorization", "Bearer abc".parse().unwrap());
        handshake.headers.append("cookie", "a=1; session=s9".parse().unwrap());
        handshake.headers.append("cookie", "b=2".parse().unwrap());
        handshake.query = Some("token=t%201&x=2".into());
        assert_eq!(handshake.bearer(), Some("abc"));
        assert_eq!((handshake.cookie("session"), handshake.cookie("b"), handshake.cookie("nope")), (Some("s9"), Some("2"), None));
        assert_eq!(handshake.query_value("token").as_deref(), Some("t 1"));
    }
}
