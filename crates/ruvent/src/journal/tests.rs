//! The journal file, on a real disk.

use super::*;
use crate::envelope::Envelope;
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

fn key(arrival: u32) -> WorkKey {
    WorkKey { generation: 1, arrival }
}

fn registration(arrival: u32, channel: &str) -> Record {
    let mut event = Envelope::claimable(channel, "do", Bytes::from(vec![arrival as u8; 3])).with_id(EventId::new_v7()).with_reply("replies");
    event.key = Some(key(arrival));
    event.principal = Some("svc".into());
    Record::Register(event)
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
}

#[test]
fn every_record_kind_survives_a_reopen() {
    let path = scratch();
    let written = vec![
        Record::Generation(1),
        registration(0, "orders"),
        Record::Claim(key(0)),
        Record::Reclaim { key: key(0), id: EventId::new_v7(), retry: 4 },
        registration(1, "orders"),
        Record::Remove(key(1)),
    ];
    runtime().block_on(async {
        let (mut journal, recovered) = FileJournal::open(&path).unwrap();
        assert!(recovered.records.is_empty());
        journal.append(written[..3].to_vec()).await.unwrap();
        journal.append(written[3..].to_vec()).await.unwrap();
    });
    let (_, recovered) = FileJournal::open(&path).unwrap();
    assert_eq!(recovered.records, written);
}

#[test]
fn a_second_engine_cannot_open_a_journal_in_use() {
    let path = scratch();
    let (_held, _) = FileJournal::open(&path).unwrap();
    assert_eq!(FileJournal::open(&path).err().map(|error| error.kind()), Some(std::io::ErrorKind::WouldBlock));
}

#[test]
fn a_version_1_journal_is_refused_and_left_byte_for_byte() {
    let path = scratch();
    let mut old = b"ruvent\0\x01".to_vec();
    old.extend_from_slice(&[7; 64]);
    std::fs::write(&path, &old).unwrap();
    let error = FileJournal::open(&path).err().expect("refused");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert_eq!(std::fs::read(&path).unwrap(), old);
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
        journal.append(vec![Record::Generation(1), registration(0, "a")]).await.unwrap();
        journal.append(vec![registration(1, "b")]).await.unwrap();
    });
    let whole = std::fs::read(&path).unwrap();
    std::fs::write(&path, &whole[..whole.len() - 5]).unwrap();
    let (journal, recovered) = FileJournal::open(&path).unwrap();
    drop(journal);
    assert_eq!(recovered.records.len(), 2);
    let cut = std::fs::read(&path).unwrap();
    assert!(cut.len() < whole.len() - 5, "the torn tail is removed from the file");
    // And the next append continues the chain from the clean end.
    runtime().block_on(async {
        let (mut journal, _) = FileJournal::open(&path).unwrap();
        journal.append(vec![Record::Remove(key(0))]).await.unwrap();
    });
    assert_eq!(FileJournal::open(&path).unwrap().1.records.len(), 3);
}

/// A completion and its follow-ups are one frame: cut anywhere, recovery
/// gives the parent and none of the follow-ups, or no parent and all of them.
#[test]
fn a_completion_frame_cut_at_any_byte_recovers_all_or_nothing() {
    let path = scratch();
    let completion = vec![Record::Remove(key(0)), registration(1, "next"), registration(2, "next"), registration(3, "other")];
    runtime().block_on(async {
        let (mut journal, _) = FileJournal::open(&path).unwrap();
        journal.append(vec![Record::Generation(1), registration(0, "work")]).await.unwrap();
    });
    let before = std::fs::read(&path).unwrap().len();
    runtime().block_on(async {
        let (mut journal, _) = FileJournal::open(&path).unwrap();
        journal.append(completion.clone()).await.unwrap();
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
        journal.append((0..50).map(|arrival| registration(arrival, &format!("c{arrival}"))).collect()).await.unwrap();
        journal.rewrite(vec![Record::Generation(2), registration(7, "c7")]).await.unwrap();
        journal.append(vec![registration(8, "c7"), registration(9, "fresh")]).await.unwrap();
    });
    let (_, recovered) = FileJournal::open(&path).unwrap();
    assert_eq!(recovered.records.len(), 4);
    let channels: Vec<String> = recovered
        .records
        .iter()
        .filter_map(|record| match record {
            Record::Register(event) => Some(event.channel.to_string()),
            _ => None,
        })
        .collect();
    assert_eq!(channels, ["c7", "c7", "fresh"], "names after a rewrite are the new file's own");
}
