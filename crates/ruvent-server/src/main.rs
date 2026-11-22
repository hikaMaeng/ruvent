//! `ruvent-server <config.json>` — the event server.
//!
//! Exits with status 0 after Ctrl-C and a clean shutdown, and with a failure
//! status when the configuration is wrong or the engine stopped on its own
//! (a journal write failed).

// A command-line program reports its failure on the terminal.
#![allow(clippy::print_stderr)]

use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    let Some(path) = std::env::args().nth(1) else {
        eprintln!("usage: ruvent-server <config.json>");
        return ExitCode::from(2);
    };
    let config = match std::fs::read_to_string(&path).map_err(|error| format!("{path}: {error}")).and_then(|text| ruvent_server::Config::from_json(&text)) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("ruvent-server: {error}");
            return ExitCode::from(2);
        }
    };
    let shutdown = async {
        // Without a signal handler the server runs until the engine stops.
        if tokio::signal::ctrl_c().await.is_err() {
            std::future::pending::<()>().await;
        }
    };
    match ruvent_server::run(&config, shutdown).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("ruvent-server: {error}");
            ExitCode::FAILURE
        }
    }
}
