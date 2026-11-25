//! The server list (2.8): the SDK reads the current list and is told when it
//! changes, then connects to new servers and lets go of removed ones. How the
//! list is kept — fixed configuration, a router — is the caller's (out of
//! scope); [`Servers`] is the fixed (and settable) one.

use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::watch;

/// A server's address as the list names it. The connector resolves it.
pub type Server = Arc<str>;

/// The current list and its changes.
pub trait ServerList: Send + Sync + 'static {
    /// The list now, and every later version.
    fn watch(&self) -> watch::Receiver<BTreeSet<Server>>;
}

/// A list set in code: fixed at construction, replaceable with [`Servers::set`].
///
/// | key | server address |
/// | --- | --- |
/// | scope | this list and every SDK handle watching it |
/// | lifetime | construction ~ drop |
/// | bound | the servers set |
/// | cleared by | [`Servers::set`] |
#[derive(Clone)]
pub struct Servers {
    list: Arc<watch::Sender<BTreeSet<Server>>>,
}

impl Servers {
    pub fn new<I, S>(servers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let (list, _) = watch::channel(Self::collect(servers));
        Servers { list: Arc::new(list) }
    }

    /// Replaces the list. Handles connect to the new servers and leave the
    /// ones no longer named.
    pub fn set<I, S>(&self, servers: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.list.send_replace(Self::collect(servers));
    }

    pub fn current(&self) -> BTreeSet<Server> {
        self.list.borrow().clone()
    }

    fn collect<I, S>(servers: I) -> BTreeSet<Server>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        servers.into_iter().map(|server| Arc::from(server.as_ref())).collect()
    }
}

impl ServerList for Servers {
    fn watch(&self) -> watch::Receiver<BTreeSet<Server>> {
        self.list.subscribe()
    }
}
