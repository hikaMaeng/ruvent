//! A proxy server with nothing added: every decision comes from the JSON file.
//!
//! ```text
//! cargo run -p ruvent-proxy --example plain -- crates/ruvent-proxy/proxy.example.json
//! ```
//!
//! This is the whole program. A crate of your own that depends on
//! `ruvent-proxy` (features `ws`, `http`, or both) and has this `main` is a
//! proxy server.

#[tokio::main]
async fn main() -> Result<(), ruvent_proxy::LaunchError> {
    ruvent_proxy::Server::new().run().await
}
