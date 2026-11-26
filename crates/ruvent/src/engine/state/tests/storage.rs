//! The ledger's side of storage: the fuzzy checkpoint walk, segment removal,
//! the body cache and the read path, streamed bodies, the disk watermark and
//! relocation (2.5, 2.6).

use super::*;
use crate::engine::config::StorageConfig;
use crate::journal::{End, Journal as _, Lane, Location, Part, Recovered, Replay, SegmentLog, SegmentState, Spooled, Written};

/// Per event: key, channel, failures, attempt, delayed until, expiry, isolated.
type View = Vec<(WorkKey, Channel, u16, Option<ClaimToken>, Option<u64>, Option<u64>, bool)>;

fn view(state: &State) -> View {
    let mut slots: Vec<_> = state
        .slots
        .iter()
        .map(|(_, slot)| {
            let delayed = match slot.place {
                Place::Delayed { until } => Some(until),
                _ => None,
            };
            (slot.fixed.key, slot.channel, slot.failures, slot.attempt().map(|attempt| attempt.token), delayed, slot.expires, slot.isolated)
        })
        .collect();
    slots.sort();
    slots
}

fn parts_into(items: &mut Vec<Replay>, part: Part) {
    match part {
        Part::Producers(producers) => items.extend(producers.into_iter().map(Replay::Producer)),
        Part::Entries(entries) => items.extend(entries.into_iter().map(Replay::Entry)),
    }
}

fn tail(h: &H, start: u64) -> Vec<Replay> {
    h.log.iter().filter(|(serial, _)| *serial >= start).map(|(serial, record)| Replay::Record { serial: *serial, record: record.clone() }).collect()
}

fn replayed(config: &EngineConfig, items: Vec<Replay>, h: &H) -> State {
    let mut state = State::new(config, Box::new(V7));
    state.recover_from(Recovered::from_replay(items, End { uptime: h.now, serial: h.s.serial(), commit: 0 }), true).unwrap();
    state
}

fn with_payload(len: usize) -> Envelope {
    Envelope::claimable(ch("work"), 1, Bytes::from(vec![7u8; len]))
}

/// Changes made while the walk runs — to slots it has seen and to slots it
/// has not — all come back: replay applies a record only if it is newer than
/// the entry (2.5, B25).
#[test]
fn a_fuzzy_checkpoint_misses_no_change_made_during_its_walk() {
    let config = EngineConfig { storage: StorageConfig { checkpoint_slice: 2, ..StorageConfig::default() }, ..config() };
    let mut h = H::with(config.clone(), true).cast();
    let joined = h.join(W1, "work", 3);
    assert!(assigned(&joined, W1).is_empty());
    let mut assignments = Vec::new();
    for byte in 1..=8 {
        assignments.extend(assigned(&h.produce(P, work("work").with_id(id(byte))), W1));
    }
    assert_eq!(assignments.len(), 3, "ids 1 to 3 claimed");
    let start = h.s.serial();
    let (head, producers) = h.s.begin_checkpoint(start, h.durable);
    let mut items = vec![Replay::Head(head)];
    parts_into(&mut items, producers);
    let first = h.s.scan_slice().expect("a slice");
    parts_into(&mut items, first);
    // Seen by the walk already (ids 1 and 2): completed with a follow-up, released.
    h.complete(W1, &assignments[0], vec![work("next").with_id(id(20))]);
    h.call(W1, Call::Release { id: id(2), token: assignments[1].token, delay_ms: 5_000, failed: true });
    // Not seen yet (ids 7 and 8): claimed explicitly, deleted.
    h.call(W2, Call::Claim { req: 1, id: id(7), lease_ms: 30_000 });
    let seq = h.next_seq(P);
    h.call(P, Call::Delete { seq, id: id(8), result: ch("out"), key: None });
    let mut more = 30;
    while let Some(part) = h.s.scan_slice() {
        parts_into(&mut items, part);
        // New registrations land in freed slots behind the walk and ahead of it.
        h.produce(P, work("work").with_id(id(more)));
        more += 1;
    }
    items.extend(tail(&h, start));
    let after = replayed(&config, items, &h);
    assert_eq!(view(&after), view(&h.s));
}

/// An entry whose registration was not on disk when the walk began has no
/// location; the registration in the tail supplies it.
#[test]
fn a_tail_registration_gives_a_checkpoint_entry_its_payload_location() {
    let mut h = H::durable(false).cast();
    h.produce(P, with_payload(64).with_id(id(1)));
    let start = h.log.iter().find(|(_, record)| matches!(record, Record::Register { .. })).map(|(serial, _)| *serial).unwrap();
    let (head, parts) = h.s.full_checkpoint(start, h.durable);
    h.land_in(1, 4096);
    let at = h.slot(id(1)).and_then(|slot| slot.at).expect("located when it landed");
    let mut items = vec![Replay::Head(head)];
    for part in parts {
        parts_into(&mut items, part);
    }
    assert!(items.iter().any(|item| matches!(item, Replay::Entry(entry) if entry.at.is_none())));
    // As a segment gives it back: the payload skipped, its location set.
    items.extend(tail(&h, start).into_iter().map(|item| match item {
        Replay::Record { serial, record: Record::Register { event, root, .. } } => {
            Replay::Record { serial, record: Record::Register { event: Envelope { payload: Bytes::new(), ..event }, root, at: Some(at) } }
        }
        other => other,
    }));
    let after = replayed(&config(), items, &h);
    let slot = after.slots.iter().map(|(_, slot)| slot).next().unwrap();
    assert_eq!((slot.at, slot.cold()), (Some(at), true), "located, and only on disk until read");
}

/// A segment goes once nothing in it is live — counted when the removal is
/// on disk, not before — and the last checkpoint starts after it (2.5).
#[test]
fn a_segment_goes_once_its_removals_land_and_the_checkpoint_starts_after_it() {
    let mut h = H::durable(false).cast();
    h.join(W1, "work", 10);
    let mut assignments = Vec::new();
    for byte in 1..=3 {
        assignments.extend(assigned(&h.produce(P, work("work").with_id(id(byte))), W1));
    }
    assert!(h.land_in(1, 4096).delete.is_empty());
    h.produce(P, work("work").with_id(id(4)));
    assert!(h.land_in(2, 512).delete.is_empty(), "segment 1 still holds three payloads");
    let start = h.s.serial();
    h.s.full_checkpoint(start, h.durable);
    assert!(h.step(|state| state.checkpointed(100)).delete.is_empty(), "segment 1 still holds three payloads");
    let mut removed = Vec::new();
    for assignment in &assignments {
        removed.extend(h.complete(W1, assignment, Vec::new()).delete);
    }
    assert!(removed.is_empty(), "the removals are not on disk yet");
    assert_eq!(h.land_in(2, 600).delete, [1]);
    assert!(h.land_in(2, 700).delete.is_empty(), "handed out once");
}

/// At the high watermark cached durable payloads are released, newest first,
/// without a write; a released payload goes out through a read, and the next
/// ones of its lane are read ahead (2.5, 2.6 item 1).
#[test]
fn cached_payloads_are_released_at_the_watermark_and_go_out_through_a_read() {
    let memory = MemoryConfig { limit: Some(10_000), slot_bytes: 100, early_pause: 1.0, ..MemoryConfig::default() };
    let config = EngineConfig { memory, producers: crate::engine::config::ProducerConfig { entry_bytes: 0, ..Default::default() }, ..config() };
    let mut h = H::with(config, true).cast();
    for byte in 1..=6 {
        h.produce(P, with_payload(1000).with_id(id(byte)));
    }
    h.land_in(1, 8192);
    assert_eq!(h.s.counters.released, 0, "6,600 bytes is under 80% of a 9,000-byte budget");
    h.produce(P, with_payload(1000).with_id(id(7)));
    assert_eq!(h.s.counters.released, 2, "down to 7/8 of the watermark: ids 6 and 5");
    assert!(h.slot(id(6)).unwrap().cold() && h.slot(id(5)).unwrap().cold() && !h.slot(id(4)).unwrap().cold());
    assert!(h.s.paused.is_empty(), "a durable payload is not its root's share");
    h.call(W1, Call::Join { req: 1, channel: Some(ch("work")), types: Vec::new(), isolated: false, lease_ms: 30_000, dispatch: false });
    let joined = h.call(W1, Call::Window { join: 1, count: 5, bytes: 0 });
    let reads: Vec<Location> = joined.sends.iter().filter_map(|send| send.read).collect();
    assert_eq!(reads, [h.slot(id(5)).unwrap().at.unwrap()], "only the cold one is read");
    let warm = joined.warm.first().copied().expect("the next of the lane is read ahead");
    assert_eq!(Some(warm.at()), h.slot(id(6)).unwrap().at);
    h.step(|state| state.warmed(warm, Bytes::from(vec![7u8; 1000])));
    assert!(!h.slot(id(6)).unwrap().cold(), "read ahead into the cache");
    let first = assigned(&joined, W1).remove(0);
    let next = h.complete(W1, &first, Vec::new());
    assert!(next.sends.iter().all(|send| send.read.is_none()) && assigned(&next, W1)[0].event.id == id(6));
}

fn spooled(dir: &std::path::Path, body: &[u8]) -> (SegmentLog, Spooled) {
    let (mut log, _) = SegmentLog::open(dir).unwrap();
    let spool = log.parts().spool.unwrap();
    let mut write = spool.receive(body.len() as u32).unwrap();
    write.write(body).unwrap();
    (log, write.finish().unwrap())
}

/// A follow-up whose payload was received to disk is registered pointing at
/// it; a volatile one stays volatile — journaled nowhere, gone at a restart —
/// and its bytes are counted (2.6 items 4 and 5, B15).
#[test]
fn streamed_follow_ups_point_at_their_body_and_a_volatile_one_is_journaled_nowhere() {
    let dir = std::env::temp_dir().join(format!("ruvent-state-spool-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let (_log, durable_body) = spooled(&dir.join("a"), b"durable body");
    let (_other, volatile_body) = spooled(&dir.join("b"), b"volatile");
    let mut h = H::durable(false).cast();
    h.join(W1, "work", 1);
    let parent = assigned(&h.produce(P, work("work").with_id(id(1))), W1).remove(0);
    let follow_ups = vec![Envelope::claimable(ch("next"), 1, Bytes::new()).with_id(id(2)), Envelope::claimable(ch("scratch.r"), VOLATILE, Bytes::new()).with_id(id(3))];
    let seq = h.next_seq(W1);
    let effects = h.call(W1, Call::Complete { seq, id: parent.event.id, token: parent.token, follow_ups, spooled: vec![(0, durable_body.clone()), (1, volatile_body.clone())] });
    let registered: Vec<(Channel, Option<Location>)> =
        effects.records.iter().filter_map(|record| if let Record::Register { event, at, .. } = record { Some((event.channel, *at)) } else { None }).collect();
    assert_eq!(registered, [(ch("next"), Some(durable_body.at))], "the volatile follow-up writes nothing");
    let (durable, volatile) = (h.slot(id(2)).unwrap(), h.slot(id(3)).unwrap());
    assert!(durable.cold() && volatile.cold() && durable.len() == 12 && volatile.len() == 8);
    assert_eq!(h.s.counters.spilled_bytes, 8);
    let after = {
        let mut s = State::new(&config(), Box::new(V7));
        s.recover(h.records.clone(), h.now, 0, true).unwrap();
        s
    };
    let back: Vec<EventId> = after.slots.iter().map(|(_, slot)| slot.fixed.id).collect();
    assert_eq!(back, [id(2)], "the durable follow-up comes back pointing at its body; the volatile one is gone");
    // A body that names no empty claimable follow-up is refused whole.
    let mut h = H::durable(false).cast();
    h.join(W1, "work", 1);
    let parent = assigned(&h.produce(P, work("work").with_id(id(1))), W1).remove(0);
    let seq = h.next_seq(W1);
    let refused = h.call(W1, Call::Complete { seq, id: parent.event.id, token: parent.token, follow_ups: vec![work("next")], spooled: vec![(0, durable_body)] });
    assert_eq!(faults(&refused, W1), [FaultKind::Malformed]);
    assert!(h.slot(id(1)).is_some());
    let _ = std::fs::remove_dir_all(&dir);
}

/// At the disk's high watermark the root with the biggest disk share pauses
/// (2.6, S6): budget = capacity − max(4 segments, 10%).
#[test]
fn the_disk_watermark_pauses_the_root_with_the_biggest_disk_share() {
    let storage = StorageConfig { disk_capacity: Some(100_000), segment_bytes: 1000, ..StorageConfig::default() };
    let memory = MemoryConfig { early_pause: 1.0, ..config().memory };
    let mut h = H::with(EngineConfig { storage, memory, ..config() }, true).cast();
    for _ in 0..3 {
        h.produce(P, Envelope::claimable(ch("a"), 1, Bytes::from(vec![1u8; 20_000])));
    }
    h.produce(P, Envelope::claimable(ch("b"), 2, Bytes::from(vec![1u8; 1_000])));
    assert_eq!(h.s.disk_budget(), Some(90_000));
    assert!(h.s.paused.is_empty());
    h.land_in(1, 75_000);
    assert!(h.s.paused.contains(&1) && !h.s.paused.contains(&2), "75,000 bytes is past 80% of 90,000");
    assert!(h.s.roots[&1].paused.as_ref().is_some_and(|paused| paused.by_disk));
    assert_eq!(h.s.pressure().disk_used, 75_000);
}

/// Dead bytes that a few live payloads keep on disk past the budget's share
/// (S3) start a relocation of the sparsest segment; each payload still there
/// gets a `Relocate`, and its old segment lets go once that is on disk (2.5).
#[test]
fn pinned_dead_bytes_relocate_the_sparsest_segment_and_free_it_once_on_disk() {
    let storage = StorageConfig { disk_capacity: Some(100_000), segment_bytes: 1000, ..StorageConfig::default() };
    let mut h = H::with(EngineConfig { storage, ..config() }, true).cast();
    h.produce(P, work("slow").with_id(id(1)));
    assert!(h.land_in(1, 20_000).relocate.is_empty(), "segment 1 is still being written");
    h.produce(P, work("work").with_id(id(2)));
    let old = h.slot(id(1)).unwrap().at.unwrap();
    let chosen = h.land_in(2, 500);
    assert_eq!(chosen.relocate, [old], "19,999 dead bytes kept by one payload, past 10% of 90,000");
    let new = Location { segment: 3, offset: 28, len: old.len, crc: old.crc };
    let moved = h.step(|state| state.relocated(Written { located: vec![(0, new)], segments: vec![SegmentState { number: 3, lane: Lane::Long, bytes: 40, first: 0, removed: false }], log_bytes: 0 }));
    assert!(matches!(moved.records.as_slice(), [Record::Relocate { at, .. }] if *at == new));
    assert_eq!(h.slot(id(1)).unwrap().at, Some(new));
    assert_eq!(h.s.book.segments[&1].live, 1, "not before the relocation is on disk");
    h.land_in(2, 600);
    assert_eq!(h.s.book.segments[&1].live, 0);
    assert_eq!(h.s.book.segments[&3].live, 1);
}

/// A checkpoint starts when the tail passes max(one segment, last × 8), capped
/// at max(cap, last) (S4).
#[test]
fn a_checkpoint_starts_when_the_tail_passes_one_segment_or_eight_times_the_last() {
    let storage = StorageConfig { segment_bytes: 1000, ..StorageConfig::default() };
    let mut h = H::with(EngineConfig { storage, ..config() }, true).cast();
    for _ in 0..9 {
        h.land_in(1, 0);
    }
    assert!(!h.s.checkpoint_due(), "900 bytes of tail");
    h.land_in(1, 0);
    assert!(h.s.checkpoint_due(), "a segment's worth");
    let start = h.s.serial();
    h.s.full_checkpoint(start, h.durable);
    assert!(!h.s.checkpoint_due(), "one at a time");
    h.step(|state| state.checkpointed(200));
    for _ in 0..15 {
        h.land_in(1, 0);
    }
    assert!(!h.s.checkpoint_due(), "1,500 bytes: under 8 × 200");
    h.land_in(1, 0);
    assert!(h.s.checkpoint_due());
}
