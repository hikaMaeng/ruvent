//! The documents and the tests agree.
//!
//! `docs/testing.md` is the map from requirement to test. It is only worth
//! anything while it is complete and current, so both directions are checked:
//! every test is on the map, and everything on the map is a test.

use crate::source::{tests, workspace_root, Source};
use std::collections::BTreeSet;

/// Names of every test function in the suite, as `name`.
fn test_names(sources: &[Source]) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for source in sources {
        let code = source.code();
        let mut pending = false;
        for line in code.lines().map(str::trim) {
            if line.starts_with("#[test]") || line.starts_with("#[tokio::test") {
                pending = true;
            } else if pending && (line.starts_with("fn ") || line.starts_with("async fn ")) {
                let name = line.trim_start_matches("async ").trim_start_matches("fn ").split(['(', '<']).next().unwrap().to_owned();
                names.insert(name);
                pending = false;
            }
        }
    }
    names
}

fn testing_md() -> String {
    std::fs::read_to_string(workspace_root().join("docs/testing.md")).expect("docs/testing.md").replace("\r\n", "\n")
}

/// Backticked snake_case words in table rows.
fn referenced(markdown: &str) -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    for line in markdown.lines().filter(|line| line.starts_with('|')) {
        for (index, piece) in line.split('`').enumerate() {
            if index % 2 == 1 && !piece.is_empty() && piece.chars().all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_') {
                found.insert(piece.to_owned());
            }
        }
    }
    found
}

#[test]
fn every_test_is_on_the_map_and_the_map_names_only_tests() {
    let names = test_names(&tests());
    assert!(names.len() > 50, "the test-name scan found only {} tests; it is broken", names.len());
    let map = referenced(&testing_md());
    let missing: Vec<&String> = names.difference(&map).collect();
    // A long snake_case name in a table row is a test reference; one that
    // names no test is left over from a test that was renamed or removed.
    let stale: Vec<&String> = map.iter().filter(|word| word.matches('_').count() >= 4 && !names.contains(*word)).collect();
    assert!(missing.is_empty() && stale.is_empty(), "docs/testing.md is out of date.\nnot on the map: {missing:#?}\non the map but not a test: {stale:#?}");
}

/// An acceptance test is judged against a requirement, so it must quote one.
#[test]
fn every_acceptance_test_quotes_its_requirement() {
    let mut unquoted = Vec::new();
    for source in tests().iter().filter(|source| source.path.starts_with("acceptance/") && !source.path.ends_with("main.rs") && !source.path.ends_with("memory.rs")) {
        let lines: Vec<&str> = source.text.lines().collect();
        for (at, line) in lines.iter().enumerate() {
            let trimmed = line.trim();
            if !(trimmed.starts_with("async fn ") || trimmed.starts_with("fn ")) || at == 0 {
                continue;
            }
            let attribute = lines[..at].iter().rev().take_while(|line| line.trim().starts_with("#[")).count();
            let is_test = lines[at - attribute..at].iter().any(|line| line.contains("#[test]") || line.contains("#[tokio::test"));
            if !is_test || trimmed.contains("fn child(") {
                continue;
            }
            let docs: Vec<&str> = lines[..at - attribute].iter().rev().take_while(|line| line.trim().starts_with("///")).copied().collect();
            if !docs.iter().any(|line| line.trim().starts_with("/// >")) {
                unquoted.push(format!("{}: {trimmed}", source.path));
            }
        }
    }
    assert!(unquoted.is_empty(), "acceptance tests without a quoted requirement:\n{}", unquoted.join("\n"));
}

/// Every relative link in the documents points at a file that exists.
#[test]
fn document_links_resolve() {
    let root = workspace_root();
    let mut documents = vec![root.join("README.md")];
    documents.extend(std::fs::read_dir(root.join("docs")).unwrap().map(|entry| entry.unwrap().path()).filter(|path| path.extension().is_some_and(|extension| extension == "md")));
    let mut broken = Vec::new();
    for document in documents {
        let text = std::fs::read_to_string(&document).unwrap();
        for (at, _) in text.match_indices("](") {
            let target: String = text[at + 2..].chars().take_while(|ch| *ch != ')').collect();
            if target.starts_with("http") || target.starts_with('#') || target.starts_with("mailto:") {
                continue;
            }
            let path = target.split('#').next().unwrap();
            let path = path.rsplit_once(':').filter(|(_, line)| line.chars().all(|ch| ch.is_ascii_digit())).map_or(path, |(file, _)| file);
            if !document.parent().unwrap().join(path).exists() {
                broken.push(format!("{}: {target}", document.strip_prefix(&root).unwrap().display()));
            }
        }
    }
    assert!(broken.is_empty(), "broken links:\n{}", broken.join("\n"));
}

/// The documents are UTF-8 text with no control characters.
#[test]
fn documents_are_plain_utf8_text() {
    let root = workspace_root();
    let mut documents = vec![root.join("README.md")];
    documents.extend(std::fs::read_dir(root.join("docs")).unwrap().map(|entry| entry.unwrap().path()).filter(|path| path.extension().is_some_and(|extension| extension == "md")));
    for document in documents {
        let bytes = std::fs::read(&document).unwrap();
        assert!(std::str::from_utf8(&bytes).is_ok(), "{} is not UTF-8", document.display());
        let at = bytes.iter().position(|byte| (*byte < 0x20 && *byte != b'\n' && *byte != b'\t') || *byte == 0x7f);
        assert!(at.is_none(), "{} has a control byte at offset {at:?}", document.display());
    }
}
