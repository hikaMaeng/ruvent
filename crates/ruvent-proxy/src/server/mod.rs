//! From a configuration and hooks to a running proxy server.
//!
//! ```text
//! Server::new()            configuration: argv[1] unless .config / .config_file
//!   .hooks(H)              optional; every hook not written keeps the configured behaviour
//!   .upstream(C)           optional; default is the event server named in the configuration
//!   .run()                 bind `listen`, start the hub, serve every compiled-in protocol, stop on Ctrl-C
//! ```
//!
//! [`Server::bind`] stops before serving and hands back the listener and the
//! hub, for tests and for programs that serve it their own way.

use crate::config::ProxyConfig;
use crate::hooks::{Defaults, Hooks};
use crate::hub::Proxy;
use crate::protocol::router;
use crate::upstream::{Connector, Remote};
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;

/// Builds a proxy server.
pub struct Server {
    config: Source,
    hooks: Arc<dyn Hooks>,
    upstream: Option<Arc<dyn Connector>>,
}

enum Source {
    Arguments,
    File(PathBuf),
    Given(Box<ProxyConfig>),
}

impl Default for Server {
    fn default() -> Self {
        Server::new()
    }
}

impl Server {
    /// A server configured by the file named as the program's first argument.
    pub fn new() -> Server {
        Server { config: Source::Arguments, hooks: Arc::new(Defaults), upstream: None }
    }

    /// Takes the configuration from this file instead.
    pub fn config_file(mut self, path: impl Into<PathBuf>) -> Server {
        self.config = Source::File(path.into());
        self
    }

    /// Takes this configuration instead.
    pub fn config(mut self, config: ProxyConfig) -> Server {
        self.config = Source::Given(Box::new(config));
        self
    }

    /// The domain's hooks.
    pub fn hooks(mut self, hooks: impl Hooks) -> Server {
        self.hooks = Arc::new(hooks);
        self
    }

    /// Where the upstream connection comes from, instead of `upstream` in the configuration.
    pub fn upstream(mut self, connector: impl Connector) -> Server {
        self.upstream = Some(Arc::new(connector));
        self
    }

    /// Reads the configuration, binds `listen` and starts the hub; serves nothing yet.
    pub async fn bind(self) -> Result<Bound, LaunchError> {
        let config = match self.config {
            Source::Given(config) => *config,
            Source::File(path) => ProxyConfig::load(&path).map_err(|error| LaunchError::Config { path, error })?,
            Source::Arguments => {
                let path = std::env::args_os().nth(1).map(PathBuf::from).ok_or(LaunchError::Usage)?;
                ProxyConfig::load(&path).map_err(|error| LaunchError::Config { path, error })?
            }
        };
        let listener = TcpListener::bind(&config.listen).await.map_err(|error| LaunchError::Listen { address: config.listen.clone(), error })?;
        let upstream = self.upstream.unwrap_or_else(|| Arc::new(Remote { address: config.upstream.address.clone(), token: config.upstream.token.clone() }));
        Ok(Bound { proxy: Proxy::start_with(config, upstream, self.hooks), listener })
    }

    /// Binds and serves until the listener fails or the process gets Ctrl-C.
    pub async fn run(self) -> Result<(), LaunchError> {
        let bound = self.bind().await?;
        tokio::select! {
            served = bound.serve() => served,
            _ = tokio::signal::ctrl_c() => Ok(()),
        }
    }
}

/// A bound listener and a started hub.
pub struct Bound {
    proxy: Proxy,
    listener: TcpListener,
}

impl Bound {
    pub fn proxy(&self) -> &Proxy {
        &self.proxy
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Serves every compiled-in protocol until the listener fails.
    pub async fn serve(self) -> Result<(), LaunchError> {
        axum::serve(self.listener, router(self.proxy).into_make_service_with_connect_info::<SocketAddr>()).await.map_err(LaunchError::Serve)
    }
}

/// Why a proxy server did not start or stopped. Shown as it is when `main` returns it.
pub enum LaunchError {
    /// No configuration was given and the program has no first argument.
    Usage,
    Config {
        path: PathBuf,
        error: io::Error,
    },
    Listen {
        address: String,
        error: io::Error,
    },
    Serve(io::Error),
}

impl fmt::Display for LaunchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LaunchError::Usage => f.write_str("usage: <program> <config.json>"),
            LaunchError::Config { path, error } => write!(f, "{}: {error}", path.display()),
            LaunchError::Listen { address, error } => write!(f, "cannot listen on {address}: {error}"),
            LaunchError::Serve(error) => write!(f, "proxy stopped: {error}"),
        }
    }
}

/// The same text as [`fmt::Display`]: a `main` returning `Err` prints this.
impl fmt::Debug for LaunchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl std::error::Error for LaunchError {}
