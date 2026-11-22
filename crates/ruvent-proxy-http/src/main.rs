//! `ruvent-proxy-http <config.json>`
#![allow(clippy::print_stderr)]

use ruvent_proxy::{Proxy, ProxyConfig, Remote};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

#[tokio::main]
async fn main() -> ExitCode {
    let Some(path) = std::env::args().nth(1).map(PathBuf::from) else {
        eprintln!("usage: ruvent-proxy-http <config.json>");
        return ExitCode::from(2);
    };
    let config = match ProxyConfig::load(&path) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{}: {error}", path.display());
            return ExitCode::from(2);
        }
    };
    let listener = match tokio::net::TcpListener::bind(&config.listen).await {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!("cannot listen on {}: {error}", config.listen);
            return ExitCode::FAILURE;
        }
    };
    let upstream = Arc::new(Remote { address: config.upstream.address.clone(), token: config.upstream.token.clone() });
    let proxy = Proxy::start(config, upstream);
    match ruvent_proxy_http::serve(listener, proxy).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("proxy stopped: {error}");
            ExitCode::FAILURE
        }
    }
}
