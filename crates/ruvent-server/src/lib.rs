//! The event server: the binary protocol (wire v2) in front of one engine.
//!
//! Every connection authenticates with a service credential's token in its
//! `Hello`, says its role (production, work, observation, 2.2), and from then
//! on sends that role's calls under that credential. What a credential may do
//! is the engine's grant table.
//!
//! * [`config`] — the JSON configuration the binary reads.
//! * [`connection`] — one TCP connection: handshake, read task, owner, staged close.
//! * [`run`] — engine, segment log and listener together, until shutdown or failure.

pub mod config;
pub mod connection;

pub use config::Config;
pub use connection::socket::Keepalive;
pub use connection::{serve, Credentials, ServerOptions};

use ruvent::{Engine, SegmentLog};
use std::future::Future;
use std::sync::Arc;
use tokio::net::TcpListener;

/// Starts the engine (recovering the segment log if one is configured),
/// listens, and runs until `shutdown` resolves or the engine stops on its own.
///
/// A shutdown takes the server down in the order of 2.8 ([`serve`]), which
/// ends with the engine's shutdown and the stopped ledger's checkpoint.
/// Returns an error when the engine stopped because of a failure (a journal
/// write), so the binary can exit with a failure code.
pub async fn run(config: &Config, shutdown: impl Future<Output = ()>) -> Result<(), String> {
    let engine_config = config.engine_config()?;
    let credentials = Arc::new(config.credentials()?);
    let engine = match &config.journal {
        Some(path) => {
            let (journal, recovered) = SegmentLog::open_with(path, engine_config.storage.segment_bytes).map_err(|error| format!("journal {}: {error}", path.display()))?;
            Engine::start(engine_config, journal, recovered).await.map_err(|error| format!("engine start: {error}"))?
        }
        None => Engine::volatile(engine_config).await,
    };
    let listener = TcpListener::bind(&config.listen).await.map_err(|error| format!("listen {}: {error}", config.listen))?;
    let served = serve(listener, engine.clone(), credentials, config.options(), shutdown);
    let mut served = std::pin::pin!(served);
    tokio::select! {
        result = &mut served => result.map_err(|error| format!("server stopped: {error}")),
        stopped = engine.stopped() => match stopped {
            Err(error) => Err(format!("engine stopped: {error}")),
            // The shutdown `serve` itself asked for: let it finish its closes.
            Ok(()) => served.await.map_err(|error| format!("server stopped: {error}")),
        },
    }
}
