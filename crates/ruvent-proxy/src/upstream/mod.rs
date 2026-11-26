//! Where the proxy's upstream connections come from (4장, 2.8).
//!
//! The fan-out connects to **every** server of a server list, with one
//! observation connection on each (one subscription per channel or prefix
//! per server, however many members want it). It is the client SDK's
//! observer: reconnection with back-off, TCP keepalive on every socket
//! (S13), the `Bye` table. A program that also publishes gives its own SDK
//! producer the same connector and list.
//!
//! [`Upstream`] names the three things that decide it: how a connection is
//! opened ([`Connector`]: [`Tcp`] to real servers, [`Local`] to engines in
//! this process), which servers ([`ServerList`]: [`Servers`] is a list set in
//! code; a router keeps its own), and the credential an in-process
//! connection runs under (over TCP the token decides).

pub use ruvent_client::link::{Conn, Connector, Local, Tcp};
pub use ruvent_client::servers::{Server, ServerList, Servers};

use std::sync::Arc;

/// Where the proxy connects.
#[derive(Clone)]
pub struct Upstream {
    pub connector: Arc<dyn Connector>,
    pub servers: Arc<dyn ServerList>,
    /// The credential an in-process connection runs under. Over TCP the
    /// server maps the connector's token to a credential and this is unused.
    pub credential: String,
}

impl Upstream {
    /// `servers`, reached through `connector`, under credential `proxy`.
    pub fn new(connector: Arc<dyn Connector>, servers: impl ServerList) -> Upstream {
        Upstream { connector, servers: Arc::new(servers), credential: "proxy".into() }
    }

    /// The same, under another in-process credential.
    pub fn credential(mut self, credential: impl Into<String>) -> Upstream {
        self.credential = credential.into();
        self
    }
}
