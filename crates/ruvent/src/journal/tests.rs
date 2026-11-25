//! The segment container, the checkpoint file, the chores and the spool, on
//! a real disk.

use super::*;
use crate::envelope::{ClaimToken, Envelope, EventId};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

fn scratch() -> PathBuf {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let directory = std::env::temp_dir().join(format!("ruvent-journal-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).unwrap();
    directory.join("log")
}

fn key(arrival: u64) -> WorkKey {
    WorkKey { generation: 1, arrival }
}

fn registration(arrival: u64, channel: &str) -> Record {
    let mut event = Envelope::claimable(channel, "do", Bytes::from(vec![arrival as u8; 3 + arrival as usize])).with_id(EventId::new_v7()).with_reply("replies");
    event.key = Some(key(arrival));
    event.principal = Some("svc".into());
    Record::Register { event, root: Name::new(channel), at: None }
}

fn batch(first: u64, records: Vec<Record>) -> Batch {
    Batch { uptime: 1000 + first, commit: first + 1, first, records }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
}

fn append(path: &std::path::Path, segment_bytes: u64, batches: Vec<Batch>) -> Vec<Written> {
    runtime().block_on(async {
        let (mut log, _) = SegmentLog::open_with(path, segment_bytes).unwrap();
        let mut written = Vec::new();
        for batch in batches {
            written.push(log.append(batch).await.unwrap());
        }
        written
    })
}

/// Everything a source gives, and where it says the log ended.
fn drain(recovered: Recovered) -> io::Result<(Vec<Replay>, End)> {
    let mut source = recovered.into_source();
    let mut items = Vec::new();
    while let Some(item) = source.next()? {
        items.push(item);
    }
    Ok((items, source.end()))
}

fn records(items: &[Replay]) -> Vec<(u64, Record)> {
    items.iter().filter_map(|item| if let Replay::Record { serial, record } = item { Some((*serial, record.clone())) } else { None }).collect()
}

/// What a reopened segment gives back for a written record: payloads stay
/// on disk, a registration comes with its location.
fn as_recovered(record: &Record, at: Option<Location>) -> Record {
    match record {
        Record::Register { event, root, at: None } => Record::Register { event: Envelope { payload: Bytes::new(), ..event.clone() }, root: root.clone(), at },
        other => other.clone(),
    }
}

#[test]
fn every_record_kind_survives_a_reopen_and_payloads_stay_on_disk() {
    let path = scratch();
    let mut timed = Envelope::claimable("orders", "do", Bytes::from_static(b"t")).with_id(EventId::new_v7()).with_group([5; 16]);
    timed.key = Some(key(9));
    timed.not_before = Some(70_000);
    timed.expires = Some(90_000);
    timed.failures = 3;
    let streamed = Location { segment: 40, offset: 28, len: 5, crc: 77 };
    let written = vec![
        Record::Generation(1),
        Record::ProducerDef { number: 0, id: [9; 16] },
        registration(0, "orders"),
        Record::Claim { key: key(0), token: ClaimToken { generation: 1, seq: u64::MAX }, lease_ms: 30_000, credential: "svc".into() },
        Record::Reclaim { key: key(0), failures: 4 },
        Record::Release { key: key(0), failures: 5, visible_at: Some(123_456) },
        Record::Release { key: key(0), failures: 5, visible_at: None },
        Record::Isolate { key: key(0), channel: "isolated.orders".into(), failures: 5, expires: Some(604_800_000) },
        Record::Relocate { key: key(0), at: Location { segment: 3, offset: 1 << 40, len: 16, crc: 9 } },
        Record::Body { payload: Bytes::from_static(b"inline, pointed at by nothing") },
        Record::Register { event: Envelope { payload: Bytes::new(), ..timed }, root: "upstream".into(), at: Some(streamed) },
        registration(1, "orders"),
        Record::Remove(key(1)),
        Record::Producer { number: 0, seq: 77 },
    ];
    let landed = append(&path, DEFAULT_SEGMENT_BYTES, vec![batch(0, written[..4].to_vec()), batch(4, written[4..].to_vec())]);
    let located: HashMap<u64, Location> = landed.iter().flat_map(|written| written.located.iter().copied()).collect();
    assert_eq!(located.len(), 2, "the two inline payloads");
    let (_log, recovered) = SegmentLog::open(&path).unwrap();
    let (items, end) = drain(recovered).unwrap();
    let expected: Vec<(u64, Record)> = written
        .iter()
        .enumerate()
        .filter(|(_, record)| !matches!(record, Record::Body { .. }))
        .map(|(serial, record)| (serial as u64, as_recovered(record, located.get(&(serial as u64)).copied())))
        .collect();
    assert_eq!(records(&items), expected);
    assert_eq!((end.uptime, end.commit, end.serial), (1004, 5, written.len() as u64), "the clock, the commit number and the serial carry on");
    let bodies = SegmentBodies::new(path.clone());
    for (serial, at) in &located {
        let Record::Register { event, .. } = &written[*serial as usize] else { panic!() };
        assert_eq!(bodies.read(at).unwrap(), event.payload, "the payload reads back from its location");
    }
    let wrong = Location { crc: located[&2].crc ^ 1, ..located[&2] };
    assert_eq!(bodies.read(&wrong).unwrap_err().kind(), io::ErrorKind::InvalidData, "a read is checked against the CRC");
}

/// A `Claim` whose `Remove` is in the same frame is not written; its serial
/// is kept, so replay can still split on serials (2.5).
#[test]
fn a_claim_removed_in_the_same_frame_is_not_written_and_keeps_its_serial() {
    let path = scratch();
    let claim = |arrival| Record::Claim { key: key(arrival), token: ClaimToken { generation: 1, seq: arrival }, lease_ms: 30_000, credential: "svc".into() };
    append(&path, DEFAULT_SEGMENT_BYTES, vec![batch(0, vec![registration(0, "work"), claim(0), Record::Remove(key(0)), registration(1, "work")]), batch(4, vec![claim(1)])]);
    let (items, _) = drain(SegmentLog::open(&path).unwrap().1).unwrap();
    let serials: Vec<u64> = records(&items).iter().map(|(serial, _)| *serial).collect();
    assert_eq!(serials, [0, 2, 3, 4], "serial 1 was the claim");
}

#[test]
fn a_second_engine_cannot_open_a_log_in_use() {
    let path = scratch();
    let (_held, _) = SegmentLog::open(&path).unwrap();
    assert_eq!(SegmentLog::open(&path).err().map(|error| error.kind()), Some(io::ErrorKind::WouldBlock));
}

/// A single-file journal (format 2 or older, or format 3's single file) and a
/// segment of another format are refused and left as they were (B12).
#[test]
fn a_single_file_or_older_journal_is_refused_and_left_byte_for_byte() {
    let path = scratch();
    for version in [1u8, 2, 3] {
        let mut old = b"ruvent\0".to_vec();
        old.push(version);
        old.extend_from_slice(&[7; 64]);
        std::fs::write(&path, &old).unwrap();
        assert_eq!(SegmentLog::open(&path).err().map(|error| error.kind()), Some(io::ErrorKind::InvalidData));
        assert_eq!(std::fs::read(&path).unwrap(), old);
    }
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir_all(&path).unwrap();
    let mut foreign = b"rvseg\0\x02".to_vec();
    foreign.extend_from_slice(&[1; 40]);
    let segment = path.join("0000000001.seg");
    std::fs::write(&segment, &foreign).unwrap();
    assert_eq!(SegmentLog::open(&path).err().map(|error| error.kind()), Some(io::ErrorKind::InvalidData));
    assert_eq!(std::fs::read(&segment).unwrap(), foreign);
}

#[test]
fn a_torn_last_frame_is_cut_and_everything_before_it_recovered() {
    let path = scratch();
    append(&path, DEFAULT_SEGMENT_BYTES, vec![batch(0, vec![Record::Generation(1), registration(0, "a")]), batch(2, vec![registration(1, "b")])]);
    let segment = path.join("0000000001.seg");
    let whole = std::fs::read(&segment).unwrap();
    std::fs::write(&segment, &whole[..whole.len() - 5]).unwrap();
    let (log, recovered) = SegmentLog::open(&path).unwrap();
    let (items, end) = drain(recovered).unwrap();
    drop(log);
    assert_eq!(records(&items).len(), 2);
    assert_eq!(end.serial, 2);
    assert!(std::fs::read(&segment).unwrap().len() < whole.len() - 5, "the torn tail is removed from the file");
    // The next append continues the chain from the clean end, in a new segment.
    append(&path, DEFAULT_SEGMENT_BYTES, vec![batch(2, vec![Record::Remove(key(0))])]);
    assert_eq!(records(&drain(SegmentLog::open(&path).unwrap().1).unwrap().0).len(), 3);
}

/// Damage with good frames after it is not a torn write: the open stops when
/// it is in the last segment, and recovery stops when it is in an earlier one.
/// Nothing after it is dropped (2.5).
#[test]
fn damage_in_the_middle_of_the_log_stops_recovery_instead_of_cutting_it() {
    let path = scratch();
    append(&path, DEFAULT_SEGMENT_BYTES, (0..3).map(|at| batch(at, vec![registration(at, "a")])).collect());
    let segment = path.join("0000000001.seg");
    let mut bytes = std::fs::read(&segment).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0xff;
    std::fs::write(&segment, &bytes).unwrap();
    assert_eq!(SegmentLog::open(&path).err().map(|error| error.kind()), Some(io::ErrorKind::InvalidData), "the last segment is read at open");
    assert_eq!(std::fs::read(&segment).unwrap(), bytes, "and left as it was");

    let path = scratch();
    append(&path, 64, (0..4).map(|at| batch(at, vec![registration(at, "a")])).collect());
    let first = path.join("0000000001.seg");
    let mut bytes = std::fs::read(&first).unwrap();
    let last = bytes.len() - 2;
    bytes[last] ^= 0xff;
    std::fs::write(&first, &bytes).unwrap();
    let (_log, recovered) = SegmentLog::open(&path).unwrap();
    assert_eq!(drain(recovered).err().map(|error| error.kind()), Some(io::ErrorKind::InvalidData), "an earlier segment is read by recovery");
}

/// A completion and its follow-ups are one frame: cut anywhere, recovery
/// gives the parent and none of the follow-ups, or no parent and all of them.
#[test]
fn a_completion_frame_cut_at_any_byte_recovers_all_or_nothing() {
    let path = scratch();
    let completion = vec![Record::Remove(key(0)), registration(1, "next"), registration(2, "next"), registration(3, "other"), Record::Producer { number: 0, seq: 2 }];
    append(&path, DEFAULT_SEGMENT_BYTES, vec![batch(0, vec![Record::Generation(1), registration(0, "work")]), batch(2, completion.clone())]);
    let segment = path.join("0000000001.seg");
    let whole = std::fs::read(&segment).unwrap();
    let before = {
        let path = scratch();
        append(&path, DEFAULT_SEGMENT_BYTES, vec![batch(0, vec![Record::Generation(1), registration(0, "work")])]);
        std::fs::read(path.join("0000000001.seg")).unwrap().len()
    };
    for cut in before..=whole.len() {
        std::fs::write(&segment, &whole[..cut]).unwrap();
        let (log, recovered) = SegmentLog::open(&path).unwrap();
        let (items, _) = drain(recovered).unwrap();
        drop(log);
        let tail = records(&items).len() - 2;
        assert!(tail == 0 || (cut == whole.len() && tail == completion.len()), "cut at {cut}: {tail} records of the completion survived");
    }
}

/// Segments roll at their size; each defines the names it uses, so its
/// dictionary is its own (2.5, 2.10).
#[test]
fn segments_roll_at_their_size_and_each_defines_its_own_names() {
    let path = scratch();
    let written = append(&path, 200, (0..12).map(|at| batch(at, vec![registration(at, "orders")])).collect());
    let created: Vec<u32> = written.iter().flat_map(|written| written.segments.iter().map(|segment| segment.number)).collect::<std::collections::BTreeSet<_>>().into_iter().collect();
    assert!(created.len() >= 3, "{created:?}");
    for number in &created {
        let bytes = std::fs::read(path.join(format!("{number:010}.seg"))).unwrap();
        assert!(bytes.windows(6).any(|window| window == b"orders"), "segment {number} defines the channel it uses");
    }
    let (items, end) = drain(SegmentLog::open_with(&path, 200).unwrap().1).unwrap();
    assert_eq!((records(&items).len(), end.serial), (12, 12));
}

fn entry(arrival: u64, at: Option<Location>, serial: u64) -> Entry {
    let Record::Register { event, root, .. } = registration(arrival, "work") else { unreachable!() };
    Entry { event: Envelope { payload: Bytes::new(), ..event }, root, at, serial, standing: Standing::Claimed { token: ClaimToken { generation: 1, seq: 4 }, lease_ms: 9, credential: "svc".into() } }
}

/// Recovery reads the checkpoint, then the tail from the checkpoint's start:
/// records before it are not given (2.5).
#[test]
fn recovery_reads_the_checkpoint_and_the_tail_from_its_start() {
    let path = scratch();
    append(&path, 120, (0..6).map(|at| batch(at, vec![registration(at, "work")])).collect());
    let head = CheckpointHead { start: 4, uptime: 1003, generation: 1, commit: 4 };
    let entries = vec![entry(0, Some(Location { segment: 1, offset: 30, len: 3, crc: 1 }), 0), entry(3, None, 3)];
    let producers = vec![ProducerEntry { number: 0, id: [3; 16], seq: 12 }];
    let mut checkpoint = CheckpointFile::new(path.clone());
    checkpoint.begin(&head).unwrap();
    checkpoint.write(&Part::Producers(producers.clone())).unwrap();
    checkpoint.write(&Part::Entries(entries.clone())).unwrap();
    let bytes = checkpoint.finish().unwrap();
    assert_eq!(bytes, std::fs::metadata(path.join("checkpoint.ckpt")).unwrap().len());
    append(&path, 120, vec![batch(6, vec![Record::Remove(key(3))])]);
    let (items, end) = drain(SegmentLog::open_with(&path, 120).unwrap().1).unwrap();
    assert_eq!(items[0], Replay::Head(head));
    assert_eq!(items[1], Replay::Producer(producers[0]));
    assert_eq!(items[2..4], [Replay::Entry(entries[0].clone()), Replay::Entry(entries[1].clone())]);
    assert_eq!(records(&items).iter().map(|(serial, _)| *serial).collect::<Vec<_>>(), [4, 5, 6]);
    assert_eq!(end.serial, 7);
}

/// A checkpoint that was being written when the process died is not the one
/// recovery reads: the previous one is, and the unfinished file goes.
#[test]
fn an_unfinished_checkpoint_is_ignored_and_the_previous_one_read() {
    let path = scratch();
    append(&path, DEFAULT_SEGMENT_BYTES, (0..3).map(|at| batch(at, vec![registration(at, "work")])).collect());
    let mut checkpoint = CheckpointFile::new(path.clone());
    let previous = CheckpointHead { start: 1, uptime: 1001, generation: 1, commit: 2 };
    checkpoint.begin(&previous).unwrap();
    checkpoint.finish().unwrap();
    checkpoint.begin(&CheckpointHead { start: 3, ..previous }).unwrap();
    checkpoint.write(&Part::Entries(vec![entry(1, None, 1)])).unwrap();
    // The process dies here: no finish, no abort.
    drop(checkpoint);
    assert!(path.join("checkpoint.tmp").exists());
    let (_log, recovered) = SegmentLog::open(&path).unwrap();
    assert!(!path.join("checkpoint.tmp").exists());
    let (items, _) = drain(recovered).unwrap();
    assert_eq!(items[0], Replay::Head(previous));
    assert_eq!(records(&items).len(), 2, "the tail from serial 1");
}

#[test]
fn a_damaged_checkpoint_stops_recovery() {
    let path = scratch();
    append(&path, DEFAULT_SEGMENT_BYTES, vec![batch(0, vec![registration(0, "work")])]);
    let mut checkpoint = CheckpointFile::new(path.clone());
    checkpoint.begin(&CheckpointHead::default()).unwrap();
    checkpoint.write(&Part::Entries(vec![entry(0, None, 0)])).unwrap();
    checkpoint.finish().unwrap();
    let file = path.join("checkpoint.ckpt");
    let mut bytes = std::fs::read(&file).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0xff;
    std::fs::write(&file, &bytes).unwrap();
    let (_log, recovered) = SegmentLog::open(&path).unwrap();
    assert_eq!(drain(recovered).err().map(|error| error.kind()), Some(io::ErrorKind::InvalidData));
}

/// Relocation copies payloads to the long-lived lane, durably and checked;
/// removal deletes whole segments (2.5).
#[test]
fn relocation_copies_payloads_to_the_long_lane_and_removal_deletes_whole_segments() {
    let path = scratch();
    let written = append(&path, 100, vec![batch(0, vec![registration(0, "work"), registration(1, "work")]), batch(2, vec![registration(2, "work")])]);
    let old: Vec<Location> = written[0].located.iter().map(|(_, at)| *at).collect();
    let (mut log, _) = SegmentLog::open_with(&path, 100).unwrap();
    let parts = log.parts();
    let (mut chores, bodies) = (parts.chores.unwrap(), parts.bodies.unwrap());
    let moved = chores.relocate(&old).unwrap();
    assert_eq!(moved.located.len(), 2);
    for (position, new) in &moved.located {
        let from = old[*position as usize];
        assert_ne!(new.segment, from.segment);
        assert_eq!(bodies.read(new).unwrap(), bodies.read(&from).unwrap());
    }
    assert!(moved.segments.iter().any(|segment| segment.lane == Lane::Long));
    let removed = chores.delete(&[old[0].segment]).unwrap();
    assert!(removed.segments.iter().all(|segment| segment.removed));
    assert!(bodies.read(&old[0]).is_err(), "the segment is gone");
    assert!(bodies.read(&moved.located[0].1).is_ok(), "the copy stays");
}

/// A streamed body reads back from its location; its segment is idle only
/// once it takes no more bodies and every body is handed over or dropped (2.6).
#[test]
fn a_streamed_body_reads_back_and_its_segment_waits_for_it() {
    let path = scratch();
    let (mut log, _) = SegmentLog::open_with(&path, 64).unwrap();
    let parts = log.parts();
    let (spool, bodies) = (parts.spool.unwrap(), parts.bodies.unwrap());
    let mut write = spool.receive(10).unwrap();
    write.write(b"hello").unwrap();
    write.write(b"world").unwrap();
    assert_eq!(write.write(b"!").unwrap_err().kind(), io::ErrorKind::InvalidInput, "no more than its length");
    let first = write.finish().unwrap();
    assert_eq!(bodies.read(&first.at).unwrap(), Bytes::from_static(b"helloworld"));
    assert!(!spool.idle(first.segment()), "still the current body segment");
    let second = spool.receive(60).unwrap();
    assert_ne!(second.len(), 0);
    assert!(!spool.idle(first.segment()), "rolled, but a body in it is not handed over");
    let segment = first.segment();
    drop(first);
    assert!(spool.idle(segment), "nothing on its way");
    let unfinished = spool.receive(60).unwrap();
    drop(unfinished);
    drop(second);
}
