//! Where the proxy's one upstream connection comes from. The proxy only sees
//! [`ruvent::Methods`] and an [`ruvent::Inbox`], so the same hub runs against
//! a remote event server or an engine in the same process.

use ruvent::{async_trait, Engine, Inbox, Methods};
use std::io;
use std::sync::Arc;

/// Opens the upstream connection; called again after it drops.
#[async_trait]
pub trait Connector: Send + Sync + 'static {
    async fn connect(&self) -> io::Result<(Arc<dyn Methods>, Inbox)>;
}

/// Messages the upstream inbox holds before the server's send buffer for
/// this proxy starts filling.
pub const INBOX: usize = 65_536;

/// An engine in this process, under a credential.
pub struct InProcess {
    pub engine: Engine,
    pub credential: String,
}

#[async_trait]
impl Connector for InProcess {
    async fn connect(&self) -> io::Result<(Arc<dyn Methods>, Inbox)> {
        let (client, inbox) = ruvent::connect(&self.engine, &self.credential, INBOX).await.map_err(|error| io::Error::new(io::ErrorKind::NotConnected, error))?;
        Ok((Arc::new(client), inbox))
    }
}

/// An event server over the binary protocol.
pub struct Remote {
    pub address: String,
    pub token: String,
}

#[async_trait]
impl Connector for Remote {
    async fn connect(&self) -> io::Result<(Arc<dyn Methods>, Inbox)> {
        let (client, inbox) = ruvent_client::remote::connect(&self.address, &self.token, ruvent_client::remote::Options::default()).await?;
        Ok((Arc::new(client), inbox))
    }
}
