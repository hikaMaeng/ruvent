//! Structural rules, checked against the source text.

use crate::source::{crate_root, library, Source};
use std::collections::{BTreeMap, BTreeSet};

/// Which modules each module may name. A dependency not listed here is a
/// decision, and making it means editing this table.
///
/// ```text
/// app ──► worker ──► engine ──► journal ──► envelope
///   └───► gateway ───┘   wire ──► gateway
/// ```
const LAYERS: &[(&str, &[&str])] = &[
    ("envelope", &[]),
    ("journal", &["envelope"]),
    ("engine", &["envelope", "journal"]),
    ("worker", &["engine", "envelope"]),
    ("gateway", &["engine", "envelope"]),
    ("wire", &["engine", "envelope", "gateway"]),
    ("app", &["engine", "envelope", "gateway", "worker"]),
];

fn crate_paths(code: &str) -> BTreeSet<String> {
    code.match_indices("crate::").map(|(at, _)| code[at + 7..].chars().take_while(|ch| ch.is_alphanumeric() || *ch == '_').collect()).collect()
}

#[test]
fn modules_depend_only_downward() {
    let layers: BTreeMap<&str, &[&str]> = LAYERS.iter().copied().collect();
    let mut violations = Vec::new();
    for source in library().iter().filter(|source| source.path != "lib.rs") {
        let module = source.module();
        let allowed = layers.get(module).unwrap_or_else(|| panic!("module `{module}` is not in the layer table; decide where it sits"));
        for used in crate_paths(&source.code()) {
            if used != module && !allowed.contains(&used.as_str()) {
                violations.push(format!("{} names crate::{used}", source.path));
            }
        }
        // Reaching two levels up is a way around this table.
        if source.code().contains("super::super") {
            violations.push(format!("{} uses super::super", source.path));
        }
    }
    assert!(violations.is_empty(), "layering violations:\n{}", violations.join("\n"));
}

#[test]
fn every_module_is_in_the_layer_table_and_no_layer_is_stale() {
    let present: BTreeSet<String> = library().iter().filter(|source| source.path != "lib.rs").map(|source| source.module().to_owned()).collect();
    let listed: BTreeSet<String> = LAYERS.iter().map(|(module, _)| (*module).to_owned()).collect();
    assert_eq!(present, listed);
}

/// Every place the library waits on the clock. Nothing else may.
///
/// | where | what it is |
/// | --- | --- |
/// | `engine/mod.rs` `time::sleep(` | the one pinned timer, re-armed only when the earliest real deadline changes |
/// | `worker/mod.rs` `time::timeout(` | the deadline on one execution step |
/// | `gateway/mod.rs` `time::timeout(` | how long a connectionless read waits for events |
const TIMERS: &[(&str, &str, usize)] = &[("engine/mod.rs", "time::sleep(", 1), ("worker/mod.rs", "time::timeout(", 1), ("gateway/mod.rs", "time::timeout(", 1)];

/// Ways of finding out by asking again. None may appear in the library.
const POLLING: &[&str] = &["interval(", "interval_at(", "thread::sleep", "yield_now", "spin_loop", "try_recv(", "sleep_until(", "timeout_at(", "MissedTickBehavior"];

#[test]
fn nothing_polls_and_every_timer_is_declared() {
    let mut violations = Vec::new();
    let mut found: BTreeMap<(String, &str), usize> = BTreeMap::new();
    for source in library() {
        for needle in POLLING {
            if source.count(needle) > 0 {
                violations.push(format!("{} uses `{needle}`", source.path));
            }
        }
        for needle in ["time::sleep(", "time::timeout("] {
            let count = source.count(needle);
            if count > 0 {
                found.insert((source.path.clone(), needle), count);
            }
        }
        // A bare `sleep(` or `timeout(` imported by name would slip past the
        // qualified search above.
        for import in ["time::{", "time::sleep;", "time::timeout;"] {
            if source.code().contains(&format!("use tokio::{import}")) {
                violations.push(format!("{} imports from tokio::time by name; write `tokio::time::…` at the call so it is counted", source.path));
            }
        }
    }
    let declared: BTreeMap<(String, &str), usize> = TIMERS.iter().map(|(path, needle, count)| (((*path).to_owned(), *needle), *count)).collect();
    if found != declared {
        violations.push(format!("timers in the source {found:?} are not the declared set {declared:?}"));
    }
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

fn only<'a>(sources: &'a [Source], needle: &str) -> Vec<(&'a str, usize)> {
    sources.iter().map(|source| (source.path.as_str(), source.count(needle))).filter(|(_, count)| *count > 0).collect()
}

/// "A write may not start while another is going" is a property of where the
/// journal is touched from. There is one writer task, one place a write is
/// issued, and one field that says a write is out.
#[test]
fn the_journal_is_written_from_one_place() {
    let sources = library();
    assert_eq!(only(&sources, "journal.append("), [("engine/mod.rs", 1)], "exactly one call site appends to the journal");
    assert_eq!(only(&sources, "journal.rewrite("), [("engine/mod.rs", 1)], "exactly one call site rewrites the journal");
    assert_eq!(only(&sources, "self.flight = Some("), [("engine/mod.rs", 1)], "a flight is taken out in exactly one place");
    assert_eq!(only(&sources, "self.issue("), [("engine/mod.rs", 2)], "only `launch` and `compact` issue journal work");
    assert_eq!(only(&sources, "jobs.try_send(").len(), 1, "journal jobs are handed over without waiting, in one place");
    // The channel to the writer holds one job: there is never a second one
    // queued behind the write in progress.
    assert_eq!(only(&sources, "mpsc::channel(1)"), [("engine/mod.rs", 1)]);
}

#[test]
fn the_engine_state_has_one_owner() {
    let sources = library();
    // The state machine is built once and moved into the engine task.
    assert_eq!(only(&sources, "State::new("), [("engine/mod.rs", 1)]);
    assert_eq!(only(&sources, "tokio::spawn(core.run())"), [("engine/mod.rs", 1)]);
    // The readable log is written from the engine task and from recovery only.
    assert_eq!(only(&sources, ".log.write()"), [("engine/log.rs", 1), ("engine/mod.rs", 3)], "write locks on the readable log: subscription open, recovery (2), publish");
}

#[test]
fn the_library_has_no_unsafe_and_writes_nothing_to_the_terminal() {
    let mut violations = Vec::new();
    for source in library() {
        for needle in ["unsafe ", "unsafe{", "println!", "eprintln!", "print!(", "dbg!(", "todo!(", "unimplemented!("] {
            if source.count(needle) > 0 {
                violations.push(format!("{} contains `{needle}`", source.path));
            }
        }
    }
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

/// The engine never looks inside a payload. JSON exists only at the wire, and
/// in two helpers on the envelope that a handler may call.
#[test]
fn only_the_wire_knows_json() {
    let users: Vec<String> = library().iter().filter(|source| source.count("serde_json") > 0).map(|source| source.path.clone()).collect();
    assert_eq!(users, ["envelope/mod.rs", "wire/json.rs"]);
    let envelope = library().into_iter().find(|source| source.path == "envelope/mod.rs").unwrap();
    assert_eq!(envelope.count("#[cfg(feature = \"json\")]"), 2, "both JSON helpers on the envelope are behind the feature");
}

#[test]
fn the_wall_clock_is_read_once() {
    assert_eq!(only(&library(), "SystemTime::now"), [("engine/log.rs", 1)], "every timestamp derives from one reading plus the runtime clock");
}

fn section<'a>(manifest: &'a str, header: &str) -> Vec<&'a str> {
    manifest.lines().skip_while(|line| line.trim() != header).skip(1).take_while(|line| !line.starts_with('[')).filter(|line| !line.trim().is_empty() && !line.trim_start().starts_with('#')).collect()
}

fn names(lines: &[&str]) -> Vec<String> {
    lines.iter().map(|line| line.split(['=', '.', ' ']).next().unwrap().to_owned()).collect()
}

/// A dependency is a decision. Adding one means editing this list.
#[test]
fn runtime_dependencies_are_the_declared_set() {
    let manifest = std::fs::read_to_string(crate_root().join("Cargo.toml")).unwrap();
    assert_eq!(names(&section(&manifest, "[dependencies]")), ["async-trait", "bincode", "bytes", "crc32fast", "parking_lot", "serde", "serde_json", "tokio", "tokio-util", "uuid", "xxhash-rust"]);
    let json: Vec<&str> = section(&manifest, "[dependencies]").into_iter().filter(|line| line.starts_with("serde_json")).collect();
    assert!(json[0].contains("optional = true"), "serde_json stays optional: {json:?}");
    assert_eq!(section(&manifest, "[lints]"), ["workspace = true"], "the crate takes the workspace's lint decisions");
}

/// Source files are UTF-8 with LF line ends and no control characters. A
/// stray NUL or CR in a string literal compiles and changes what the program
/// does; an editor will not show it.
#[test]
fn source_files_are_plain_utf8_text() {
    let root = crate::source::crate_root();
    let mut violations = Vec::new();
    let mut stack = vec![root.join("src"), root.join("tests"), root.join("examples")];
    while let Some(directory) = stack.pop() {
        for entry in std::fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                let bytes = std::fs::read(&path).unwrap();
                if std::str::from_utf8(&bytes).is_err() {
                    violations.push(format!("{} is not UTF-8", path.display()));
                }
                if let Some(at) = bytes.iter().position(|byte| (*byte < 0x20 && *byte != b'\n' && *byte != b'\t') || *byte == 0x7f) {
                    violations.push(format!("{} has control byte {:#04x} at offset {at}", path.display(), bytes[at]));
                }
            }
        }
    }
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}
