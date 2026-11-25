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

/// Inside the two modules that grew parts: which of its own submodules each
/// part may name (6장, "새 모듈의 자리").
///
/// ```text
/// journal (the log):  codec ◄── bodies, checkpoint, spool ◄── recover ◄── segment
/// engine:             config, ops ◄── state (the ledger: accounting in state/pause, state/disk, state/roots; the wheel in state/wheel)
///                                ◄── storage (the checkpoint writer, the chores, reads, the gauge)
/// ```
///
/// `segment` and `checkpoint` name each other: the container hands out its
/// checkpoint writer, which renames into the container's directory.
const SUBLAYERS: &[(&str, Parts)] = &[
    (
        "journal",
        &[
            ("codec", &[]),
            ("bodies", &["codec", "segment"]),
            ("checkpoint", &["codec", "segment"]),
            ("spool", &["codec", "segment"]),
            ("recover", &["checkpoint", "codec", "segment"]),
            ("segment", &["bodies", "checkpoint", "codec", "recover", "spool"]),
        ],
    ),
    ("engine", &[("config", &[]), ("ops", &[]), ("state", &["config", "ops"]), ("storage", &["ops"])]),
];

/// One module's parts: each part and the parts it may name.
type Parts = &'static [(&'static str, &'static [&'static str])];

/// The module path of a source file: `journal/segment/mod.rs` is
/// `journal::segment`, `engine/state/disk.rs` is `engine::state::disk`.
fn module_path(path: &str) -> Vec<String> {
    let stem = path.trim_end_matches(".rs");
    let stem = stem.strip_suffix("/mod").unwrap_or(stem);
    stem.split('/').map(str::to_owned).collect()
}

/// The first segment of every path in a `use` tree or path that starts right
/// after `at` in `code`: `a::x`, or each item of `{a::x, b, c::{..}}`.
fn heads(code: &str, at: usize) -> Vec<String> {
    let rest = &code[at..];
    if let Some(inner) = rest.strip_prefix('{') {
        let mut depth = 0usize;
        let mut items = Vec::new();
        let mut current = String::new();
        for ch in inner.chars() {
            match ch {
                '{' => {
                    depth += 1;
                    current.push(ch);
                }
                '}' if depth == 0 => break,
                '}' => {
                    depth -= 1;
                    current.push(ch);
                }
                ',' if depth == 0 => items.push(std::mem::take(&mut current)),
                _ => current.push(ch),
            }
        }
        items.push(current);
        items.iter().map(|item| item.trim().chars().take_while(|ch| ch.is_alphanumeric() || *ch == '_').collect()).filter(|head: &String| !head.is_empty()).collect()
    } else {
        vec![rest.chars().take_while(|ch| ch.is_alphanumeric() || *ch == '_').collect()]
    }
}

#[test]
fn the_parts_of_the_log_and_the_engine_name_only_the_parts_they_may() {
    let mut violations = Vec::new();
    for source in library() {
        let path = module_path(&source.path);
        let Some((top, table)) = SUBLAYERS.iter().find(|(top, _)| path.first().map(String::as_str) == Some(*top)) else { continue };
        if path.len() < 2 {
            continue;
        }
        let own = path[1].as_str();
        let allowed = table.iter().find(|(part, _)| *part == own).map(|(_, allowed)| *allowed).unwrap_or_else(|| panic!("`{top}::{own}` is not in the sublayer table; decide where it sits"));
        let parts: BTreeSet<&str> = table.iter().map(|(part, _)| *part).collect();
        let code = source.code();
        let mut named = BTreeSet::new();
        // `super::x` from this file means a sibling at the second level only
        // when this file is the part's root (`<top>/<part>.rs` or `/mod.rs`).
        if path.len() == 2 {
            for (at, _) in code.match_indices("super::") {
                named.extend(heads(&code, at + 7));
            }
        }
        let prefix = format!("crate::{top}::");
        for (at, _) in code.match_indices(&prefix) {
            named.extend(heads(&code, at + prefix.len()));
        }
        for name in named {
            if parts.contains(name.as_str()) && name != own && !allowed.contains(&name.as_str()) {
                violations.push(format!("{} names {top}::{name}", source.path));
            }
        }
    }
    assert!(violations.is_empty(), "sublayer violations:\n{}", violations.join("\n"));
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
/// | `ruvent/engine/mod.rs` `time::sleep(` | the timing wheel: one one-shot sleep to the earliest lease end or visibility time, none when the wheel is empty (2.4) |
/// | `ruvent-client/line/mod.rs` `time::sleep(` | reconnection back-off: one wait between two attempts of a line, exponential with full jitter (S20) |
/// | `ruvent-client/requester/mod.rs` `time::timeout(` | the call deadline of a request: past it the request is sent again (same id) or fails (2.8, 2.9) |
/// | `ruvent-client/worker/extend.rs` `time::sleep(` | the automatic extension: one one-shot sleep to the earliest `Extend` due (half a lease), none while no run is due (2.4, B7) |
/// | `ruvent-client/worker/shutdown.rs` `time::sleep(` | the shutdown grace: armed only while shutting down (2.9 "종료") |
/// | `ruvent-client/worker/run.rs` `time::timeout(` | a handler's run limit: past it the run is cut off and released (B7; the lease is no longer the run limit) |
/// | `ruvent-server/connection/handshake.rs` `time::timeout(` | how long a new connection has to say hello |
/// | `ruvent-server/connection/close.rs` `time::timeout(` | the deadline of a staged close after `Bye` (2.2, S20) |
/// | `ruvent-proxy/hub/mod.rs` `time::timeout(` | how long an authentication worker has to answer |
/// | `ruvent-proxy/hub/task.rs` `time::sleep(` | back-off before reconnecting to the upstream server |
const TIMERS: &[(&str, &str, usize)] = &[
    ("ruvent/snapshot/mod.rs", "time::timeout(", 1),
    ("ruvent/engine/mod.rs", "time::sleep(", 1),
    ("ruvent-client/line/mod.rs", "time::sleep(", 1),
    ("ruvent-client/requester/mod.rs", "time::timeout(", 1),
    ("ruvent-client/worker/extend.rs", "time::sleep(", 1),
    ("ruvent-client/worker/shutdown.rs", "time::sleep(", 1),
    ("ruvent-client/worker/run.rs", "time::timeout(", 1),
    ("ruvent-server/connection/handshake.rs", "time::timeout(", 1),
    ("ruvent-server/connection/close.rs", "time::timeout(", 1),
    ("ruvent-proxy/hub/mod.rs", "time::timeout(", 1),
    ("ruvent-proxy/hub/task.rs", "time::sleep(", 1),
];

/// Ways of finding out by asking again. None may appear in a library, except
/// where [`NOT_POLLING`] declares it.
const POLLING: &[&str] = &["interval(", "interval_at(", "thread::sleep", "yield_now", "spin_loop", "try_recv(", "sleep_until(", "timeout_at(", "MissedTickBehavior"];

/// Places a polling word appears without polling, by (file, word, count).
///
/// | where | what it is |
/// | --- | --- |
/// | `ruvent-server/connection/socket.rs` `interval(` | `socket2`'s `with_interval(`: the kernel's keepalive probe interval (2.2, S13), not an application timer |
/// | `ruvent-client/remote/socket.rs` `interval(` | the same on the client's sockets |
const NOT_POLLING: &[(&str, &str, usize)] = &[("ruvent-server/connection/socket.rs", "interval(", 1), ("ruvent-client/remote/socket.rs", "interval(", 1)];

#[test]
fn nothing_polls_and_every_timer_is_declared() {
    let mut violations = Vec::new();
    let mut found: BTreeMap<(String, &str), usize> = BTreeMap::new();
    for source in workspace_library() {
        for needle in POLLING {
            let count = source.count(needle);
            let allowed = NOT_POLLING.iter().find(|(path, word, _)| *path == source.path && word == needle).map_or(0, |(_, _, count)| *count);
            if count != allowed {
                violations.push(format!("{} uses `{needle}` {count} times; declared {allowed}", source.path));
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
    let sources = workspace_library();
    for (path, word, _) in NOT_POLLING {
        if !sources.iter().any(|source| source.path == *path) {
            violations.push(format!("{path} is declared to use `{word}` but is gone; update the table"));
        }
    }
    let declared: BTreeMap<(String, &str), usize> = TIMERS.iter().map(|(path, needle, count)| (((*path).to_owned(), *needle), *count)).collect();
    if found != declared {
        violations.push(format!("timers in the source {found:?} are not the declared set {declared:?}"));
    }
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

/// The proxy's roles stay apart, so each can change and be tested alone:
///
/// * the subscription book (`hub/channels`) is pure state: no task, no queue,
///   no clock, nothing awaited;
/// * a hook's upstream handle cannot subscribe or unsubscribe on the hub's
///   connection, whose markers decide when a user's subscription takes effect;
/// * the deliver hook is called in one place, behind a panic guard, so a
///   failing hook cannot end the hub task.
#[test]
fn the_proxy_keeps_its_roles_apart() {
    let sources = workspace_library();
    let file = |path: &str| sources.iter().find(|source| source.path == path).unwrap_or_else(|| panic!("{path} is gone; update this rule with the move")).code();
    let book = file("ruvent-proxy/hub/channels/mod.rs");
    for needle in ["tokio", "mpsc", "oneshot", "Instant", "time::", "async ", ".await", "spawn"] {
        assert!(!book.contains(needle), "the subscription book uses `{needle}`; it must stay pure state");
    }
    let context = file("ruvent-proxy/hooks/context.rs");
    for needle in ["fn subscribe", "fn unsubscribe", "pub methods", "-> Option<Arc<dyn Methods>>"] {
        assert!(!context.contains(needle), "the hooks' upstream handle exposes `{needle}`");
    }
    let calls: Vec<(&str, usize)> =
        sources.iter().filter(|source| source.path.starts_with("ruvent-proxy/")).map(|source| (source.path.as_str(), source.count(".deliver("))).filter(|(_, count)| *count > 0).collect();
    assert_eq!(calls, [("ruvent-proxy/hub/task.rs", 1)], "the deliver hook is called from one place");
    assert!(file("ruvent-proxy/hub/task.rs").contains("catch_unwind(AssertUnwindSafe(|| hooks.deliver("), "the deliver hook runs behind a panic guard");
}

fn only<'a>(sources: &'a [Source], needle: &str) -> Vec<(&'a str, usize)> {
    sources.iter().map(|source| (source.path.as_str(), source.count(needle))).filter(|(_, count)| *count > 0).collect()
}

/// One writer task owns the log; the engine hands it one job at a time. The
/// checkpoint writer and the chores each run in one place of their own (6장,
/// "저널 작성 지점").
#[test]
fn the_journal_is_written_from_one_place() {
    let sources = library();
    assert_eq!(only(&sources, "journal.append("), [("engine/mod.rs", 2)], "the startup write, and the writer task");
    assert_eq!(only(&sources, "self.flight = Some("), [("engine/mod.rs", 1)], "a flight is taken out in exactly one place");
    assert_eq!(only(&sources, "jobs.try_send("), [("engine/mod.rs", 1)]);
    assert_eq!(only(&sources, "mpsc::channel::<Job>(1)"), [("engine/mod.rs", 1)], "the writer never has a second job queued behind the one in flight");
    assert_eq!(only(&sources, "encode_frame("), [("journal/codec.rs", 1), ("journal/segment/mod.rs", 2)], "log frames are encoded by the log writer alone (once more after a roll)");
    for call in ["checkpoints.begin(", "checkpoints.write(", "checkpoints.finish("] {
        assert_eq!(only(&sources, call), [("engine/storage/mod.rs", 1)], "the checkpoint writer runs in one place: {call}");
    }
    for call in ["chores.relocate(", "chores.delete("] {
        assert_eq!(only(&sources, call), [("engine/storage/mod.rs", 1)], "the chores run in one place: {call}");
    }
}

#[test]
fn the_engine_state_has_one_owner() {
    let sources = library();
    assert_eq!(only(&sources, "State::new("), [("engine/mod.rs", 1)]);
    assert_eq!(only(&sources, "tokio::spawn(core.run())"), [("engine/mod.rs", 1)]);
    // No lock guards shared state in the core: the engine task owns it.
    assert_eq!(
        only(&sources, "Mutex<"),
        [("journal/spool/mod.rs", 1), ("methods/mod.rs", 2)],
        "only the spool's reservations, shared by the connection owners; the client's table of calls waiting for an answer, and its sequence counter held while a numbered call is handed over"
    );
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

/// The server keeps no policy: none of the old engine's policy words appear in
/// the core. One threshold is the exception (2.4, B6): `isolate_after`, the
/// failure count at which an event moves to its isolation channel. The
/// producer table's keep time is `linger` (not a retention), and the wheel's
/// types avoid the word `Dead`.
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
