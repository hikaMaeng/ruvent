//! The journal file as bytes: what it refuses to touch, what survives damage,
//! and a fixed copy of the format so a change to it cannot go unnoticed.

use ruvent::{Audience, Bytes, Command, Envelope, EventId, FileJournal, Journal, Kind, Record};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn scratch(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("ruvent-{}-format-{name}.journal", std::process::id()));
    for stale in [path.clone(), path.with_extension("journal.lock"), path.with_extension("journal.tmp")] {
        let _ = std::fs::remove_file(stale);
    }
    path
}

/// A file that is not a journal is refused, and left exactly as it was.
/// Opening the wrong path must never cost someone their data.
#[test]
fn a_file_that_is_not_a_journal_is_refused_and_left_alone() {
    let path = scratch("foreign");
    let contents = b"id,name\n1,precious\n2,data\n".repeat(20);
    std::fs::write(&path, &contents).unwrap();

    let refused = FileJournal::open(&path).err().expect("a foreign file must be refused");
    assert_eq!(refused.kind(), io::ErrorKind::InvalidData);
    assert_eq!(std::fs::read(&path).unwrap(), contents, "the file must be untouched");

    // Shorter than the header, and not the start of one: still foreign.
    std::fs::write(&path, b"hello").unwrap();
    assert_eq!(FileJournal::open(&path).err().expect("a short foreign file must be refused").kind(), io::ErrorKind::InvalidData);
    assert_eq!(std::fs::read(&path).unwrap(), b"hello");
}

/// A crash while the journal was being created can leave part of the header.
/// That is an empty journal, not a foreign file.
#[test]
fn a_torn_header_is_an_empty_journal() {
    let path = scratch("torn-header");
    let (journal, _) = FileJournal::open(&path).unwrap();
    drop(journal);
    let header = std::fs::read(&path).unwrap();
    assert!(!header.is_empty(), "a new journal starts with its header");

    for cut in 0..header.len() {
        std::fs::write(&path, &header[..cut]).unwrap();
        let (journal, recovered) = FileJournal::open(&path).unwrap_or_else(|error| panic!("cut at {cut}: {error}"));
        assert_eq!((recovered.records.len(), recovered.through), (0, 0));
        drop(journal);
        assert_eq!(std::fs::read(&path).unwrap(), header, "cut at {cut}: the header is restored");
    }
}

fn command(key: &str) -> Arc<Command> {
    let mut command = Command::new("job.run", "requests", Bytes::from(format!("{{\"key\":\"{key}\"}}"))).transaction_key(key).reply_to("replies").session("s-1");
    command.event_id = EventId::derive(&[b"fixture", key.as_bytes()]);
    command.created_at = 1_700_000_000_000;
    command.principal = Some("alice".into());
    Arc::new(command)
}

fn event() -> Arc<Envelope> {
    let mut event = Envelope::from_command(&command("e"));
    event.kind = Kind::Fact;
    event.position = (3 << 40) + 7;
    event.causation = Some(EventId::derive(&[b"cause"]));
    event.audience = Audience::Worker;
    Arc::new(event)
}

/// Every kind of record, with every optional field both set and empty.
fn batches() -> Vec<Vec<Record>> {
    vec![
        vec![Record::Epoch(3), Record::Put { queue: "q".into(), id: 1, command: command("a") }, Record::Put { queue: "q".into(), id: 2, command: command("b") }],
        vec![
            Record::Del { queue: "q".into(), id: 1 },
            Record::Exec { key: "a".into(), request: command("a").event_id, fingerprint: u128::MAX - 7, attempt: 9, attempts: 2, done: None },
            Record::Exec { key: "b".into(), request: command("b").event_id, fingerprint: 42, attempt: 10, attempts: 1, done: Some((false, Bytes::from_static(b"no"), 1_700_000_000_500)) },
        ],
        vec![Record::Dead { queue: "q".into(), id: 2, command: command("b") }, Record::Forget { key: "a".into() }, Record::Event(event())],
    ]
}

async fn write_fixture(path: &Path) {
    let (mut journal, _) = FileJournal::open(path).unwrap();
    let mut through = 0;
    for batch in batches() {
        let next = through + batch.len() as u64;
        journal.append(through, next, &batch).await.unwrap();
        through = next;
    }
}

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/runtime/fixtures/journal-v1.bin")
}

/// The format is fixed by a committed file. Writing the same records today
/// must produce the same bytes, and the committed file must still read back
/// as those records. A change that fails this is a format change: it needs a
/// new version number and a decision about old journals, not a re-bless.
///
/// `RUVENT_BLESS=1` rewrites the fixture.
#[tokio::test]
async fn the_file_format_is_fixed() {
    let path = scratch("fixture");
    write_fixture(&path).await;
    let written = std::fs::read(&path).unwrap();
    if std::env::var_os("RUVENT_BLESS").is_some() {
        std::fs::create_dir_all(fixture().parent().unwrap()).unwrap();
        std::fs::write(fixture(), &written).unwrap();
    }
    let committed = std::fs::read(fixture()).expect("fixture missing; run once with RUVENT_BLESS=1");
    assert!(written == committed, "the bytes written for the fixture records changed ({} bytes, committed {})", written.len(), committed.len());

    let copy = scratch("fixture-copy");
    std::fs::copy(fixture(), &copy).unwrap();
    let (_journal, recovered) = FileJournal::open(&copy).unwrap();
    let expected: Vec<Record> = batches().into_iter().flatten().collect();
    assert_eq!(format!("{:?}", recovered.records), format!("{expected:?}"));
    assert_eq!(recovered.through, expected.len() as u64);
}

/// Damage anywhere — a cut at any byte, any single bit flipped — yields a
/// prefix of the batches that were written: never a reordering, never a
/// record that was not written, never a panic.
#[tokio::test]
async fn damage_anywhere_yields_a_prefix_of_what_was_written() {
    let path = scratch("damage");
    write_fixture(&path).await;
    let intact = std::fs::read(&path).unwrap();
    let batches = batches();
    let prefixes: Vec<String> = (0..=batches.len()).map(|count| format!("{:?}", batches[..count].iter().flatten().collect::<Vec<_>>())).collect();
    let check = |damaged: &[u8], what: &str| {
        std::fs::write(&path, damaged).unwrap();
        match FileJournal::open(&path) {
            Ok((journal, recovered)) => {
                let got = format!("{:?}", recovered.records.iter().collect::<Vec<_>>());
                assert!(prefixes.contains(&got), "{what}: recovered records are not a prefix of what was written");
                drop(journal);
            }
            // Only damage to the header itself may be called foreign.
            Err(error) => assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{what}: {error}"),
        }
    };
    for cut in 0..=intact.len() {
        check(&intact[..cut], &format!("cut at {cut}"));
    }
    for byte in 0..intact.len() {
        for bit in [0, 3, 7] {
            let mut damaged = intact.clone();
            damaged[byte] ^= 1 << bit;
            check(&damaged, &format!("bit {bit} of byte {byte} flipped"));
        }
    }
}

/// A frame built to pass its CRC while claiming a body shorter than the
/// frame header is refused like any damage — cut off, never read past.
#[test]
fn a_crafted_frame_too_short_for_its_header_is_cut_off_not_read() {
    let path = scratch("crafted");
    let (journal, _) = FileJournal::open(&path).unwrap();
    drop(journal);
    let header = std::fs::read(&path).unwrap();
    let body = [1u8, 0, 0, 0, 0];
    let mut crafted = header.clone();
    crafted.extend_from_slice(&(body.len() as u32).to_le_bytes());
    crafted.extend_from_slice(&crc32fast::hash(&body).to_le_bytes());
    crafted.extend_from_slice(&body);
    std::fs::write(&path, &crafted).unwrap();

    let (journal, recovered) = FileJournal::open(&path).expect("refused as damage, not as a panic");
    drop(journal);
    assert_eq!((recovered.records.len(), recovered.through), (0, 0));
    assert_eq!(std::fs::read(&path).unwrap(), header, "cut back to the header");
}

/// What is written after a torn tail was found survives the next restart:
/// the tail is cut off on open, so new frames follow the last intact one
/// instead of landing behind garbage that would hide them.
#[tokio::test]
async fn writes_after_a_torn_tail_survive_the_next_restart() {
    let path = scratch("after-torn");
    write_fixture(&path).await;
    let mut file = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
    std::io::Write::write_all(&mut file, &[200, 0, 0, 0, 1, 2, 3, 4, 1, 9, 9]).unwrap();
    drop(file);

    let (mut journal, recovered) = FileJournal::open(&path).unwrap();
    let through = recovered.through;
    journal.append(through, through + 1, &[Record::Epoch(9)]).await.unwrap();
    drop(journal);

    let (_journal, recovered) = FileJournal::open(&path).unwrap();
    assert_eq!(recovered.through, through + 1, "the batch written after the torn tail was lost");
    assert!(matches!(recovered.records.last(), Some(Record::Epoch(9))));
}
