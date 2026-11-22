//! The event server: the binary protocol in front of one engine.
//!
//! The server does not tell workers, proxies and clients apart. Every
//! connection authenticates with a service credential's token in its first
//! frame, and from then on calls the seven methods under that credential.
//! What a credential may do is the engine's grant table.
//!
//! * [`config`] — the JSON configuration the binary reads.
//! * [`connection`] — one TCP connection: handshake, reader, writer.
//! * [`run`] — engine, journal and listener together, until shutdown or failure.

pub mod config;
pub mod connection;

pub use config::Config;
pub use connection::{serve, Credentials, ServerOptions};

use ruvent::{Engine, FileJournal};
use std::future::Future;
use std::sync::Arc;
use tokio::net::TcpListener;

/// Starts the engine (recovering the journal if one is configured), listens,
/// and runs until `shutdown` resolves or the engine stops on its own.
///
/// Returns an error when the engine stopped because of a failure (a journal
/// write), so the binary can exit with a failure code.
pub async fn run(config: &Config, shutdown: impl Future<Output = ()>) -> Result<(), String> {
    let engine_config = config.engine_config()?;
    let credentials = Arc::new(config.credentials()?);
    let engine = match &config.journal {
        Some(path) => {
            if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
                std::fs::create_dir_all(parent).map_err(|error| format!("{}: {error}", parent.display()))?;
            }
            let (journal, recovered) = FileJournal::open(path).map_err(|error| format!("journal {}: {error}", path.display()))?;
            Engine::start(engine_config, journal, recovered).await.map_err(|error| format!("engine start: {error}"))?
        }
        None => Engine::volatile(engine_config).await,
    };
    let listener = TcpListener::bind(&config.listen).await.map_err(|error| format!("listen {}: {error}", config.listen))?;
    tokio::select! {
        served = serve(listener, engine.clone(), credentials, config.options()) => {
            let stopped = engine.shutdown().await;
            served.map_err(|error| format!("listener failed: {error}"))?;
            stopped.map_err(|error| format!("engine stopped: {error}"))
        }
        stopped = engine.stopped() => stopped.map_err(|error| format!("engine stopped: {error}")),
        () = shutdown => engine.shutdown().await.map_err(|error| format!("engine stopped: {error}")),
    }
}
