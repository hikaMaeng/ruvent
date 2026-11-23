//! A proxy server with nothing added: every decision comes from the JSON file.
//!
//! ```text
//! cargo run -p ruvent-proxy --example plain -- crates/ruvent-proxy/proxy.example.json
//! ```
//!
//! This is the whole program. A crate of your own that depends on
//! `ruvent-proxy` (features `ws`, `http`, or both) and has this `main` is a
//! proxy server.

#[cfg(any(feature = "ws", feature = "http"))]
#[tokio::main]
async fn main() -> Result<(), ruvent_proxy::LaunchError> {
    ruvent_proxy::Server::new().run().await
}

/// Built with no protocol there is no server to run.
#[cfg(not(any(feature = "ws", feature = "http")))]
fn main() {}
