//! Structural rules, checked against the source text.

use crate::source::{crate_root, library, strip_comments, workspace_root, Source};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

/// Which modules of the core each module may name. A dependency not listed
/// here is a decision, and making it means editing this table.
///
/// ```text
/// methods ──► engine ──► journal ──► envelope ──► names
///                  └───► snapshot ──┘
/// ```
const LAYERS: &[(&str, &[&str])] = &[
    ("names", &[]),
    ("envelope", &["names"]),
    ("journal", &["envelope", "names"]),
    ("snapshot", &["envelope", "names"]),
    ("engine", &["envelope", "journal", "names", "snapshot"]),
    ("methods", &["engine", "envelope", "names"]),
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

/// Library source of every crate in the workspace: `crates/*/src/**`, without
/// binaries' `main.rs` and without in-tree tests. Paths are `crate/path`.
fn workspace_library() -> Vec<Source> {
    let mut found = Vec::new();
    let crates = workspace_root().join("crates");
    let mut members: Vec<_> = std::fs::read_dir(&crates).unwrap().map(|entry| entry.unwrap().path()).filter(|path| path.join("Cargo.toml").exists()).collect();
    members.sort();
    for member in members {
        let name = member.file_name().unwrap().to_string_lossy().into_owned();
        if name == "xtask" {
            continue;
        }
        let mut stack = vec![member.join("src")];
        while let Some(directory) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&directory) else { continue };
            for entry in entries {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                let relative = path.strip_prefix(member.join("src")).unwrap().to_string_lossy().replace('\\', "/");
                let is_test = relative.ends_with("tests.rs") || relative.contains("/tests/");
                if path.extension().is_some_and(|extension| extension == "rs") && relative != "main.rs" && !is_test {
                    let text = std::fs::read_to_string(&path).unwrap().replace("\r\n", "\n");
                    found.push(Source { path: format!("{name}/{relative}"), text });
                }
            }
        }
    }
    found
}

/// Every place a library waits on the clock. Nothing else may.
///
/// | where | what it is |
/// | --- | --- |
/// | `ruvent/snapshot/mod.rs` `time::timeout(` | a snapshot unit's age limit |
/// | `ruvent-client/worker/lane.rs` `time::timeout(` | a handler's lease: past it the run is abandoned |
/// | `ruvent-server/connection/mod.rs` `time::timeout(` | how long a new connection has to say hello |
/// | `ruvent-proxy/hub/mod.rs` `time::timeout(` | how long an authentication worker has to answer |
/// | `ruvent-proxy/hub/mod.rs` `time::sleep(` | back-off before reconnecting to the upstream server |
const TIMERS: &[(&str, &str, usize)] = &[
    ("ruvent/snapshot/mod.rs", "time::timeout(", 1),
    ("ruvent-client/worker/lane.rs", "time::timeout(", 1),
    ("ruvent-server/connection/mod.rs", "time::timeout(", 1),
    ("ruvent-proxy/hub/mod.rs", "time::timeout(", 1),
    ("ruvent-proxy/hub/mod.rs", "time::sleep(", 1),
];

/// Ways of finding out by asking again. None may appear in a library.
const POLLING: &[&str] = &["interval(", "interval_at(", "thread::sleep", "yield_now", "spin_loop", "try_recv(", "sleep_until(", "timeout_at(", "MissedTickBehavior"];

#[test]
fn nothing_polls_and_every_timer_is_declared() {
    let mut violations = Vec::new();
    let mut found: BTreeMap<(String, &str), usize> = BTreeMap::new();
    for source in workspace_library() {
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

/// One writer task owns the journal; the engine hands it one job at a time.
#[test]
fn the_journal_is_written_from_one_place() {
    let sources = library();
    assert_eq!(only(&sources, "journal.append("), [("engine/mod.rs", 2)], "the startup write, and the writer task");
    assert_eq!(only(&sources, "journal.rewrite("), [("engine/mod.rs", 1)]);
    assert_eq!(only(&sources, "self.flight = Some("), [("engine/mod.rs", 1)], "a flight is taken out in exactly one place");
    assert_eq!(only(&sources, "jobs.try_send("), [("engine/mod.rs", 1)]);
    assert_eq!(only(&sources, "mpsc::channel::<Job>(1)"), [("engine/mod.rs", 1)], "the writer never has a second job queued behind the one in flight");
}

#[test]
fn the_engine_state_has_one_owner() {
    let sources = library();
    assert_eq!(only(&sources, "State::new("), [("engine/mod.rs", 1)]);
    assert_eq!(only(&sources, "tokio::spawn(core.run())"), [("engine/mod.rs", 1)]);
    // No lock guards shared state in the core: the engine task owns it.
    assert_eq!(only(&sources, "Mutex<"), [("methods/mod.rs", 1)], "only the client's table of calls waiting for an answer is shared");
}

#[test]
fn no_library_has_unsafe_or_writes_to_the_terminal() {
    let mut violations = Vec::new();
    for source in workspace_library() {
        for needle in ["unsafe ", "unsafe{", "println!", "eprintln!", "print!(", "dbg!(", "todo!(", "unimplemented!("] {
            if source.count(needle) > 0 {
                violations.push(format!("{} contains `{needle}`", source.path));
            }
        }
    }
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

/// The server never looks inside a payload: no JSON in the core.
#[test]
fn the_core_knows_no_json() {
    let users: Vec<String> = library().iter().filter(|source| source.count("serde") > 0).map(|source| source.path.clone()).collect();
    assert!(users.is_empty(), "{users:?}");
}

#[test]
fn the_wall_clock_is_read_once_in_the_core() {
    assert_eq!(only(&library(), "SystemTime::now"), [("snapshot/mod.rs", 1)], "it names snapshot files; deadlines use the monotonic clock");
}

/// The server keeps no policy: none of the old engine's policy words appear in the core.
#[test]
fn the_server_keeps_no_event_policy() {
    let mut violations = Vec::new();
    for source in library() {
        for word in ["max_attempts", "dead_letter", "Dead", "ReactionTable", "transaction_key", "Retention", "audience"] {
            if source.count(word) > 0 {
                violations.push(format!("{} contains `{word}`", source.path));
            }
        }
    }
    assert!(violations.is_empty(), "{}", violations.join("\n"));
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
    assert_eq!(names(&section(&manifest, "[dependencies]")), ["async-trait", "bytes", "crc32fast", "parking_lot", "tokio", "uuid"]);
    for member in std::fs::read_dir(workspace_root().join("crates")).unwrap() {
        let manifest = std::fs::read_to_string(member.unwrap().path().join("Cargo.toml")).unwrap();
        assert_eq!(section(&manifest, "[lints]"), ["workspace = true"], "every crate takes the workspace's lint decisions");
    }
}

/// Source files are UTF-8 with LF line ends and no control characters.
#[test]
fn source_files_are_plain_utf8_text() {
    let mut violations = Vec::new();
    let mut stack = vec![workspace_root().join("crates")];
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

/// Every crate has a README, and the root README links to it.
#[test]
fn every_crate_is_documented_and_linked() {
    let root = workspace_root();
    let index = std::fs::read_to_string(root.join("README.md")).unwrap();
    let mut missing = Vec::new();
    for member in std::fs::read_dir(root.join("crates")).unwrap() {
        let path = member.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        if name == "xtask" || !path.join("Cargo.toml").exists() {
            continue;
        }
        if !Path::new(&path.join("README.md")).exists() {
            missing.push(format!("{name}: no README.md"));
        }
        if !index.contains(&format!("crates/{name}/README.md")) {
            missing.push(format!("{name}: not linked from README.md"));
        }
    }
    assert!(missing.is_empty(), "{}", missing.join("\n"));
}

#[test]
fn the_comment_stripper_is_used_on_code() {
    assert_eq!(strip_comments("a // b\n"), "a \n");
}
