//! The journal file, on a real disk.

use super::*;
use crate::envelope::{ClaimToken, Envelope, EventId};
use bytes::Bytes;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

fn scratch() -> PathBuf {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let directory = std::env::temp_dir().join(format!("ruvent-journal-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).unwrap();
    directory.join("events.journal")
}

fn key(arrival: u64) -> WorkKey {
    WorkKey { generation: 1, arrival }
}

fn registration(arrival: u64, channel: &str) -> Record {
    let mut event = Envelope::claimable(channel, "do", Bytes::from(vec![arrival as u8; 3])).with_id(EventId::new_v7()).with_reply("replies");
    event.key = Some(key(arrival));
    event.principal = Some("svc".into());
    Record::Register { event, root: Name::new(channel) }
}

fn batch(first: u64, records: Vec<Record>) -> Batch {
    Batch { uptime: 1000 + first, commit: first + 1, first, records }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
}

#[test]
fn every_record_kind_survives_a_reopen() {
    let path = scratch();
    let mut timed = Envelope::claimable("orders", "do", Bytes::from_static(b"t")).with_id(EventId::new_v7()).with_group([5; 16]);
    timed.key = Some(key(9));
    timed.not_before = Some(70_000);
    timed.expires = Some(90_000);
    timed.failures = 3;
    let written = vec![
        Record::Generation(1),
        Record::ProducerDef { number: 0, id: [9; 16] },
        registration(0, "orders"),
        Record::Claim { key: key(0), token: ClaimToken { generation: 1, seq: u64::MAX }, lease_ms: 30_000, credential: "svc".into() },
        Record::Reclaim { key: key(0), failures: 4 },
        Record::Release { key: key(0), failures: 5, visible_at: Some(123_456) },
        Record::Release { key: key(0), failures: 5, visible_at: None },
        Record::Isolate { key: key(0), channel: "isolated.orders".into(), failures: 5, expires: Some(604_800_000) },
        Record::Relocate { key: key(0), at: Location { segment: 3, offset: 1 << 40, len: 16 } },
        Record::Body { payload: Bytes::from_static(b"streamed") },
        Record::Register { event: timed, root: "upstream".into() },
        registration(1, "orders"),
        Record::Remove(key(1)),
        Record::Producer { number: 0, seq: 77 },
    ];
    runtime().block_on(async {
        let (mut journal, recovered) = FileJournal::open(&path).unwrap();
        assert!(recovered.records.is_empty());
        journal.append(batch(0, written[..4].to_vec())).await.unwrap();
        journal.append(batch(4, written[4..].to_vec())).await.unwrap();
    });
    let (_, recovered) = FileJournal::open(&path).unwrap();
    assert_eq!(recovered.records, written);
    assert_eq!((recovered.uptime, recovered.commit, recovered.serial), (1004, 5, written.len() as u64), "the clock, the commit number and the serial carry on");
}

/// A `Claim` whose `Remove` is in the same frame is not written (2.5).
#[test]
fn a_claim_removed_in_the_same_batch_is_not_written() {
    let path = scratch();
    let claim = |arrival| Record::Claim { key: key(arrival), token: ClaimToken { generation: 1, seq: arrival }, lease_ms: 30_000, credential: "svc".into() };
    runtime().block_on(async {
        let (mut journal, _) = FileJournal::open(&path).unwrap();
        journal.append(batch(0, vec![registration(0, "work"), claim(0), Record::Remove(key(0))])).await.unwrap();
        journal.append(batch(3, vec![registration(1, "work"), claim(1)])).await.unwrap();
    });
    let (_, recovered) = FileJournal::open(&path).unwrap();
    let claims = recovered.records.iter().filter(|record| matches!(record, Record::Claim { .. })).count();
    assert_eq!(claims, 1, "only the claim that outlived its batch");
    assert_eq!(recovered.records.len(), 4);
}

#[test]
fn a_second_engine_cannot_open_a_journal_in_use() {
    let path = scratch();
    let (_held, _) = FileJournal::open(&path).unwrap();
    assert_eq!(FileJournal::open(&path).err().map(|error| error.kind()), Some(std::io::ErrorKind::WouldBlock));
}

/// Format 2 and format 1 files are refused, not migrated (B12).
#[test]
fn a_format_2_or_older_journal_is_refused_and_left_byte_for_byte() {
    let path = scratch();
    for version in [1u8, 2] {
        let mut old = b"ruvent\0".to_vec();
        old.push(version);
        old.extend_from_slice(&[7; 64]);
        std::fs::write(&path, &old).unwrap();
        let error = FileJournal::open(&path).err().expect("refused");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(std::fs::read(&path).unwrap(), old);
    }
    // Not a journal at all: the same.
    std::fs::write(&path, b"hello").unwrap();
    assert!(FileJournal::open(&path).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), b"hello");
}

#[test]
fn a_torn_last_frame_is_cut_and_everything_before_it_recovered() {
    let path = scratch();
    runtime().block_on(async {
        let (mut journal, _) = FileJournal::open(&path).unwrap();
        journal.append(batch(0, vec![Record::Generation(1), registration(0, "a")])).await.unwrap();
        journal.append(batch(2, vec![registration(1, "b")])).await.unwrap();
    });
    let whole = std::fs::read(&path).unwrap();
    std::fs::write(&path, &whole[..whole.len() - 5]).unwrap();
    let (journal, recovered) = FileJournal::open(&path).unwrap();
    drop(journal);
    assert_eq!(recovered.records.len(), 2);
    assert_eq!(recovered.serial, 2);
    let cut = std::fs::read(&path).unwrap();
    assert!(cut.len() < whole.len() - 5, "the torn tail is removed from the file");
    // And the next append continues the chain from the clean end.
    runtime().block_on(async {
        let (mut journal, _) = FileJournal::open(&path).unwrap();
        journal.append(batch(2, vec![Record::Remove(key(0))])).await.unwrap();
    });
    assert_eq!(FileJournal::open(&path).unwrap().1.records.len(), 3);
}

/// A batch whose first serial is not where the last one ended was written out
/// of turn; replay stops before it.
#[test]
fn a_batch_that_breaks_the_serial_chain_ends_replay() {
    let path = scratch();
    runtime().block_on(async {
        let (mut journal, _) = FileJournal::open(&path).unwrap();
        journal.append(batch(0, vec![Record::Generation(1)])).await.unwrap();
        journal.append(batch(5, vec![registration(0, "a")])).await.unwrap();
    });
    assert_eq!(FileJournal::open(&path).unwrap().1.records, vec![Record::Generation(1)]);
}

/// A completion and its follow-ups are one frame: cut anywhere, recovery
/// gives the parent and none of the follow-ups, or no parent and all of them.
#[test]
fn a_completion_frame_cut_at_any_byte_recovers_all_or_nothing() {
    let path = scratch();
    let completion = vec![Record::Remove(key(0)), registration(1, "next"), registration(2, "next"), registration(3, "other"), Record::Producer { number: 0, seq: 2 }];
    runtime().block_on(async {
        let (mut journal, _) = FileJournal::open(&path).unwrap();
        journal.append(batch(0, vec![Record::Generation(1), registration(0, "work")])).await.unwrap();
    });
    let before = std::fs::read(&path).unwrap().len();
    runtime().block_on(async {
        let (mut journal, _) = FileJournal::open(&path).unwrap();
        journal.append(batch(2, completion.clone())).await.unwrap();
    });
    let whole = std::fs::read(&path).unwrap();
    for cut in before..=whole.len() {
        std::fs::write(&path, &whole[..cut]).unwrap();
        let (journal, recovered) = FileJournal::open(&path).unwrap();
        drop(journal);
        let tail = &recovered.records[2..];
        if cut == whole.len() {
            assert_eq!(tail, completion.as_slice());
        } else {
            assert!(tail.is_empty(), "cut at {cut}: {} records of the completion survived", tail.len());
        }
    }
}

#[test]
fn a_rewritten_journal_decodes_on_its_own_and_accepts_appends() {
    let path = scratch();
    runtime().block_on(async {
        let (mut journal, _) = FileJournal::open(&path).unwrap();
        journal.append(batch(0, (0..50).map(|arrival| registration(arrival, &format!("c{arrival}"))).collect())).await.unwrap();
        journal.rewrite(Batch { uptime: 5000, commit: 2, first: 50, records: vec![Record::Generation(2), registration(7, "c7")] }).await.unwrap();
        journal.append(Batch { uptime: 6000, commit: 3, first: 50, records: vec![registration(8, "c7"), registration(9, "fresh")] }).await.unwrap();
    });
    let (_, recovered) = FileJournal::open(&path).unwrap();
    assert_eq!(recovered.records.len(), 4);
    assert_eq!((recovered.uptime, recovered.serial), (6000, 52));
    let channels: Vec<String> = recovered
        .records
        .iter()
        .filter_map(|record| match record {
            Record::Register { event, .. } => Some(event.channel.to_string()),
            _ => None,
        })
        .collect();
    assert_eq!(channels, ["c7", "c7", "fresh"], "names after a rewrite are the new file's own");
}
