//! From a configuration and hooks to a running proxy server.
//!
//! ```text
//! Server::new()            configuration: argv[1] unless .config / .config_file
//!   .hooks(H)              optional; every hook not written keeps the configured behaviour
//!   .upstream(U)           optional; default is every server the configuration names, over TCP
//!   .run()                 bind `listen`, start the hub, serve every compiled-in protocol, stop on Ctrl-C
//!   .run_until(signal)     the same, stopping when `signal` resolves
//! ```
//!
//! [`Server::bind`] stops before serving and hands back the listener and the
//! hub, for tests and for programs that serve it their own way.
//!
//! Stopping stops the hub too: every user's stream ends, WebSocket users see
//! their socket close, and every upstream connection is dropped.

use crate::config::ProxyConfig;
use crate::hooks::{Defaults, Hooks};
use crate::hub::Proxy;
use crate::protocol::router;
use crate::upstream::Upstream;
use std::ffi::OsString;
use std::fmt;
use std::future::{Future, IntoFuture};
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;

/// Builds a proxy server.
pub struct Server {
    config: Source,
    hooks: Arc<dyn Hooks>,
    upstream: Option<Upstream>,
}

enum Source {
    Arguments,
    File(PathBuf),
    Given(Box<ProxyConfig>),
}

impl Source {
    /// The configuration, read and checked. `arguments` are the program's, name first.
    fn resolve(self, arguments: impl IntoIterator<Item = OsString>) -> Result<ProxyConfig, LaunchError> {
        let path = match self {
            Source::Given(config) => return config.check().map(|()| *config).map_err(LaunchError::Invalid),
            Source::File(path) => path,
            Source::Arguments => config_path(arguments)?,
        };
        ProxyConfig::load(&path).map_err(|error| LaunchError::Config { path, error })
    }
}

/// The configuration file a program names as its first argument.
fn config_path(arguments: impl IntoIterator<Item = OsString>) -> Result<PathBuf, LaunchError> {
    arguments.into_iter().nth(1).map(PathBuf::from).ok_or(LaunchError::Usage)
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

    /// Takes this configuration instead. It is checked as a file would be.
    pub fn config(mut self, config: ProxyConfig) -> Server {
        self.config = Source::Given(Box::new(config));
        self
    }

    /// The domain's hooks.
    pub fn hooks(mut self, hooks: impl Hooks) -> Server {
        self.hooks = Arc::new(hooks);
        self
    }

    /// Where the upstream connections go, instead of `upstream` in the
    /// configuration: another connector (engines in this process), or a
    /// server list kept elsewhere (a router).
    pub fn upstream(mut self, upstream: Upstream) -> Server {
        self.upstream = Some(upstream);
        self
    }

    /// Reads and checks the configuration, binds `listen` and starts the hub; serves nothing yet.
    pub async fn bind(self) -> Result<Bound, LaunchError> {
        let config = self.config.resolve(std::env::args_os())?;
        let listener = TcpListener::bind(&config.listen).await.map_err(|error| LaunchError::Listen { address: config.listen.clone(), error })?;
        let upstream = self.upstream.unwrap_or_else(|| Upstream::from_config(&config));
        Ok(Bound { proxy: Proxy::start_with(config, upstream, self.hooks), listener })
    }

    /// Binds and serves until the listener fails or the process gets Ctrl-C.
    pub async fn run(self) -> Result<(), LaunchError> {
        self.run_until(tokio::signal::ctrl_c()).await
    }

    /// Binds and serves until the listener fails or `shutdown` resolves.
    /// `shutdown` failing (Ctrl-C could not be listened for) is an error too.
    pub async fn run_until(self, shutdown: impl Future<Output = io::Result<()>>) -> Result<(), LaunchError> {
        self.bind().await?.serve_until(shutdown).await
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
        self.serve_until(std::future::pending()).await
    }

    /// Serves until the listener fails or `shutdown` resolves, then stops the hub.
    pub async fn serve_until(self, shutdown: impl Future<Output = io::Result<()>>) -> Result<(), LaunchError> {
        let proxy = self.proxy.clone();
        let service = router(self.proxy).into_make_service_with_connect_info::<SocketAddr>();
        let result = tokio::select! {
            served = axum::serve(self.listener, service).into_future() => served.map_err(LaunchError::Serve),
            signal = shutdown => signal.map_err(LaunchError::Signal),
        };
        proxy.stop().await;
        result
    }
}

/// Why a proxy server did not start or stopped. Shown as it is when `main` returns it.
pub enum LaunchError {
    /// No configuration was given and the program has no first argument.
    Usage,
    /// The configuration file could not be read, or is not a valid configuration.
    Config {
        path: PathBuf,
        error: io::Error,
    },
    /// A configuration given in code breaks a rule of [`ProxyConfig::check`].
    Invalid(io::Error),
    Listen {
        address: String,
        error: io::Error,
    },
    Serve(io::Error),
    /// The shutdown signal could not be listened for.
    Signal(io::Error),
}

impl fmt::Display for LaunchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LaunchError::Usage => f.write_str("usage: <program> <config.json>"),
            LaunchError::Config { path, error } => write!(f, "{}: {error}", path.display()),
            LaunchError::Invalid(error) => write!(f, "invalid configuration: {error}"),
            LaunchError::Listen { address, error } => write!(f, "cannot listen on {address}: {error}"),
            LaunchError::Serve(error) => write!(f, "proxy stopped: {error}"),
            LaunchError::Signal(error) => write!(f, "cannot listen for the shutdown signal: {error}"),
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

#[cfg(test)]
mod tests;
