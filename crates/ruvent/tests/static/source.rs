//! Reading the crate's own source tree.

use std::path::{Path, PathBuf};

pub struct Source {
    /// Path under `src/`, with forward slashes: `engine/mod.rs`.
    pub path: String,
    pub text: String,
}

impl Source {
    /// The top-level module this file belongs to: `engine`, or `lib` for `lib.rs`.
    pub fn module(&self) -> &str {
        self.path.split('/').next().map(|first| first.trim_end_matches(".rs")).unwrap_or("lib")
    }

    /// The text with comments removed, so a rule about code is not tripped by
    /// prose that mentions the thing it forbids.
    pub fn code(&self) -> String {
        strip_comments(&self.text)
    }

    pub fn count(&self, needle: &str) -> usize {
        self.code().matches(needle).count()
    }
}

pub fn crate_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

pub fn workspace_root() -> PathBuf {
    crate_root().parent().and_then(Path::parent).expect("the crate lives in <workspace>/crates/ruvent").to_path_buf()
}

fn walk(directory: &Path, found: &mut Vec<PathBuf>) {
    let mut entries: Vec<_> = std::fs::read_dir(directory).unwrap_or_else(|error| panic!("{}: {error}", directory.display())).map(|entry| entry.unwrap().path()).collect();
    entries.sort();
    for entry in entries {
        if entry.is_dir() {
            walk(&entry, found);
        } else if entry.extension().is_some_and(|extension| extension == "rs") {
            found.push(entry);
        }
    }
}

fn load(base: &Path) -> Vec<Source> {
    let mut files = Vec::new();
    walk(base, &mut files);
    files
        .into_iter()
        .map(|file| {
            let path = file.strip_prefix(base).unwrap().to_string_lossy().replace('\\', "/");
            let text = std::fs::read_to_string(&file).unwrap().replace("\r\n", "\n");
            Source { path, text }
        })
        .collect()
}

fn is_test_file(path: &str) -> bool {
    path.ends_with("/tests.rs") || path.contains("/tests/")
}

/// Every file of the library proper: `src/**`, without in-tree unit tests.
pub fn library() -> Vec<Source> {
    load(&crate_root().join("src")).into_iter().filter(|source| !is_test_file(&source.path)).collect()
}

/// Every file that holds tests: `tests/**` and the unit tests under `src/**`.
pub fn tests() -> Vec<Source> {
    let mut found = load(&crate_root().join("tests"));
    found.extend(load(&crate_root().join("src")).into_iter().filter(|source| is_test_file(&source.path)));
    found
}

/// Removes `//` comments and `/* */` blocks. String literals are respected, so
/// a `//` inside one (a URL) survives.
pub fn strip_comments(text: &str) -> String {
    let chars: Vec<char> = text.chars().collect();
    let at = |index: usize| chars.get(index).copied();
    let mut out = String::with_capacity(text.len());
    let mut index = 0;
    let mut in_string = false;
    while let Some(ch) = at(index) {
        if in_string {
            out.push(ch);
            if ch == '\\' {
                out.extend(at(index + 1));
                index += 1;
            } else if ch == '"' {
                in_string = false;
            }
            index += 1;
        } else if ch == '\'' && at(index + 2) == Some('\'') {
            // A character literal, which may be a quote.
            out.extend([ch, chars[index + 1], '\'']);
            index += 3;
        } else if ch == '"' {
            in_string = true;
            out.push(ch);
            index += 1;
        } else if ch == '/' && at(index + 1) == Some('/') {
            while at(index).is_some_and(|ch| ch != '\n') {
                index += 1;
            }
        } else if ch == '/' && at(index + 1) == Some('*') {
            index += 2;
            while at(index).is_some() && !(at(index) == Some('*') && at(index + 1) == Some('/')) {
                index += 1;
            }
            index += 2;
        } else {
            out.push(ch);
            index += 1;
        }
    }
    out
}

#[test]
fn the_comment_stripper_keeps_code_and_strings() {
    let stripped = strip_comments("let a = \"http://x\"; // gone\n/* gone\n too */ let b = 1; /// doc\n");
    assert_eq!(stripped, "let a = \"http://x\"; \n let b = 1; \n");
}
