//! `cargo xtask verify` — every gate this repository has, in order, stopping
//! at the first that fails. A change is done when this passes.
//!
//! ```text
//! cargo xtask verify            the gates
//! cargo xtask verify --deep     the gates, then the property tests at 100,000 cases each
//! cargo xtask mutants           mutation testing of the core (needs cargo-mutants)
//! ```

// A command-line tool reports on the terminal and exits with a status.
#![allow(clippy::print_stdout, clippy::print_stderr, clippy::exit)]

use std::process::{Command, ExitCode};
use std::time::Instant;

struct Gate {
    name: &'static str,
    args: &'static [&'static str],
    env: &'static [(&'static str, &'static str)],
}

const fn gate(name: &'static str, args: &'static [&'static str]) -> Gate {
    Gate { name, args, env: &[] }
}

const GATES: &[Gate] = &[
    gate("format", &["fmt", "--all", "--", "--check"]),
    gate("lint", &["clippy", "--workspace", "--all-targets", "--", "-D", "warnings"]),
    Gate { name: "api docs", args: &["doc", "--workspace", "--no-deps"], env: &[("RUSTDOCFLAGS", "-D warnings")] },
    gate("static", &["test", "-p", "ruvent", "--test", "static"]),
    gate("state machine", &["test", "-p", "ruvent", "--lib"]),
    gate("runtime", &["test", "-p", "ruvent", "--test", "runtime"]),
    gate("acceptance", &["test", "-p", "ruvent", "--test", "acceptance"]),
    gate("protocol", &["test", "-p", "ruvent-protocol"]),
    gate("server", &["test", "-p", "ruvent-server"]),
    gate("client and workers", &["test", "-p", "ruvent-client"]),
    gate("proxies", &["test", "-p", "ruvent-proxy", "-p", "ruvent-proxy-ws", "-p", "ruvent-proxy-http"]),
];

const DEEP: &[Gate] = &[Gate { name: "properties at 100,000 cases", args: &["test", "--release", "-p", "ruvent", "-p", "ruvent-protocol", "--lib"], env: &[("PROPTEST_CASES", "100000")] }];

/// The files whose logic the mutation run judges the tests against.
const MUTANTS: &[Gate] = &[Gate {
    name: "mutants",
    args: &[
        "mutants",
        "--package",
        "ruvent",
        "--file",
        "crates/ruvent/src/engine/state/mod.rs",
        "--file",
        "crates/ruvent/src/engine/mod.rs",
        "--file",
        "crates/ruvent/src/envelope/codec.rs",
        "--file",
        "crates/ruvent/src/journal/codec.rs",
        "--timeout",
        "300",
        "--jobs",
        "8",
        "--",
        "--lib",
        "--test",
        "runtime",
        "--test",
        "acceptance",
    ],
    env: &[("PROPTEST_CASES", "256")],
}];

fn run(gates: &[Gate]) -> bool {
    for gate in gates {
        let started = Instant::now();
        println!("── {} : cargo {}", gate.name, gate.args.join(" "));
        let status = Command::new(env!("CARGO")).args(gate.args).envs(gate.env.iter().copied()).status();
        let seconds = started.elapsed().as_secs_f32();
        match status {
            Ok(status) if status.success() => println!("gate name={} status=ok seconds={seconds:.1}", gate.name),
            Ok(status) => {
                println!("gate name={} status=failed code={:?} seconds={seconds:.1}", gate.name, status.code());
                return false;
            }
            Err(error) => {
                println!("gate name={} status=failed error={error}", gate.name);
                return false;
            }
        }
    }
    true
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let passed = match args.iter().map(String::as_str).collect::<Vec<_>>().as_slice() {
        ["verify"] => run(GATES),
        ["verify", "--deep"] => run(GATES) && run(DEEP),
        ["mutants"] => run(MUTANTS),
        _ => {
            eprintln!("usage: cargo xtask verify [--deep] | cargo xtask mutants");
            return ExitCode::from(2);
        }
    };
    println!("verify status={}", if passed { "ok" } else { "failed" });
    if passed {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
