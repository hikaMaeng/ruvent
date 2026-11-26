//! Rebuilding the ledger from the journal (2.4, 2.5).
//!
//! Items arrive one at a time from a [`Source`]: the checkpoint's head,
//! producers and entries, then the tail's records. Nothing is collected
//! first, and payloads stay on disk — a slot comes back with its location and
//! an empty cache — so recovery holds metadata only (2.5, M3).
//!
//! A tail record is applied only if its serial is past what the checkpoint
//! entry of its event says; a record for a key the checkpoint does not have
//! is a registration of the tail or about an event that left before the walk
//! reached it (ARIES' fuzzy checkpoint).

use super::producers::Producer;
use super::slots::{Attempt, Fixed, Index, Place, Slot};
use super::wheel::Due;
use super::State;
use crate::envelope::{Envelope, ProducerId, WorkKey};
use crate::journal::{End, Entry, Location, Record, Recovered, Replay, Standing};
use std::collections::HashMap;

/// What replay keeps while it runs.
///
/// | key | work key → slot; producer number → id |
/// | --- | --- |
/// | lifetime | one recovery |
/// | bound | the live events and producers being rebuilt |
/// | cleared by | dropped when recovery ends |
#[derive(Default)]
struct Replayer {
    keys: HashMap<WorkKey, Index>,
    numbers: HashMap<u32, ProducerId>,
    start: u64,
    generation: u32,
}

impl State {
    /// Rebuilds the ledger from journal records alone (no checkpoint) at
    /// uptime `uptime`; `serial` is the serial the next record takes.
    pub fn recover(&mut self, records: Vec<Record>, uptime: u64, serial: u64, journaled: bool) -> Result<Vec<Record>, String> {
        let end = End { uptime, serial: serial.max(records.len() as u64), commit: 0 };
        self.recover_from(Recovered::from_records(records, end), journaled).map(|(startup, _)| startup)
    }

    /// Rebuilds the ledger from what a journal held and starts a new
    /// generation.
    ///
    /// The clock carries on from where the log ended. A claimed event stays
    /// claimed (2.4): its token, lease length and worker come back, the
    /// lease is re-armed at its full length from now, and its failure count
    /// does not change — a restart is not a closed connection. `not_before`
    /// is absolute and keeps its remaining time.
    ///
    /// Returns the records that must be durable before the first answer (the
    /// new generation) and where the log ended.
    pub fn recover_from(&mut self, recovered: Recovered, journaled: bool) -> Result<(Vec<Record>, End), String> {
        self.journaled = journaled;
        let mut source = recovered.into_source();
        let mut replayer = Replayer::default();
        while let Some(item) = source.next().map_err(|error| error.to_string())? {
            self.replay(&mut replayer, item);
        }
        let end = source.end();
        let startup = self.settle_recovery(replayer, end)?;
        Ok((startup, end))
    }

    fn producer_number(&mut self, replayer: &mut Replayer, number: u32, id: ProducerId) {
        replayer.numbers.insert(number, id);
        let linger = super::millis(self.config.producers.linger);
        let entry = self.producers.entries.entry(id).or_insert_with(|| Producer::new([0; 16], linger));
        entry.number = Some(number);
        self.producers.next_number = self.producers.next_number.max(number + 1);
    }

    /// A slot from a registration or a checkpoint entry.
    fn rebuild(&mut self, event: Envelope, at: Option<Location>, serial: u64, place: Place) -> Option<Index> {
        let key = event.key?;
        let fixed = Fixed { id: event.id, key, event_type: event.event_type, not_before: event.not_before, durable: self.journaled, registered_at: 0, visible_from: 0, digest: None };
        let slot = Slot { fixed, channel: event.channel, failures: event.failures, place, commit: 0, serial, payload: event.payload, at };
        Some(self.slots.insert(slot))
    }

    fn replay(&mut self, replayer: &mut Replayer, item: Replay) {
        match item {
            Replay::Head(head) => {
                replayer.start = head.start;
                // Segments wholly before it may go once nothing in them is live.
                self.book.checkpoint_start = Some(head.start);
                replayer.generation = replayer.generation.max(head.generation);
            }
            Replay::Producer(producer) => {
                self.producer_number(replayer, producer.number, producer.id);
                if let Some(entry) = self.producers.entries.get_mut(&producer.id) {
                    entry.applied = entry.applied.max(producer.seq);
                    entry.acked = entry.applied;
                }
            }
            Replay::ProducerName { number, id } => self.producer_number(replayer, number, id),
            Replay::Entry(Entry { event, at, serial, standing }) => {
                let place = match standing {
                    Standing::Waiting => Place::Waiting,
                    Standing::Delayed { until } => Place::Delayed { until },
                    Standing::Claimed { token, lease_ms, worker } => Place::Claimed(Attempt { token, lease_ms, until: 0, worker, join: None }),
                };
                let key = event.key;
                if let (Some(key), Some(index)) = (key, self.rebuild(event, at, serial, place)) {
                    replayer.keys.insert(key, index);
                }
            }
            Replay::Record { serial, record } if serial >= replayer.start => self.replay_record(replayer, serial, record),
            Replay::Record { .. } => {}
        }
    }

    fn replay_record(&mut self, replayer: &mut Replayer, serial: u64, record: Record) {
        // The slot this record is about, if the record is newer than what it holds.
        let newer = |state: &State, key: &WorkKey| replayer.keys.get(key).copied().filter(|index| state.slots.get(*index).is_some_and(|slot| slot.serial < serial));
        match record {
            Record::Register { event, at } => {
                let Some(key) = event.key else { return };
                if let Some(index) = replayer.keys.get(&key).copied() {
                    // The checkpoint has it: only the payload's place can be news.
                    if let Some(slot) = self.slots.get_mut(index) {
                        if slot.at.is_none() {
                            slot.at = at;
                        }
                        if slot.payload.is_empty() && !event.payload.is_empty() {
                            slot.payload = event.payload;
                        }
                    }
                    return;
                }
                let place = match event.not_before {
                    Some(until) => Place::Delayed { until },
                    None => Place::Waiting,
                };
                if let Some(index) = self.rebuild(event, at, serial, place) {
                    replayer.keys.insert(key, index);
                }
            }
            Record::Body { .. } => {}
            Record::Claim { key, token, lease_ms, worker } => {
                if let Some(slot) = newer(self, &key).and_then(|index| self.slots.get_mut(index)) {
                    slot.place = Place::Claimed(Attempt { token, lease_ms, until: 0, worker, join: None });
                    slot.serial = serial;
                }
            }
            Record::Remove(key) => {
                if let Some(index) = newer(self, &key) {
                    replayer.keys.remove(&key);
                    self.slots.remove(index);
                }
            }
            Record::Reclaim { key, failures } => {
                if let Some(slot) = newer(self, &key).and_then(|index| self.slots.get_mut(index)) {
                    slot.failures = failures;
                    slot.place = Place::Waiting;
                    slot.serial = serial;
                }
            }
            Record::Release { key, failures, visible_at } => {
                if let Some(slot) = newer(self, &key).and_then(|index| self.slots.get_mut(index)) {
                    slot.failures = failures;
                    slot.place = match visible_at {
                        Some(until) => Place::Delayed { until },
                        None => Place::Waiting,
                    };
                    slot.serial = serial;
                }
            }
            Record::Relocate { key, at } => {
                if let Some(slot) = newer(self, &key).and_then(|index| self.slots.get_mut(index)) {
                    slot.at = Some(at);
                    slot.serial = serial;
                }
            }
            Record::ProducerDef { number, id } => self.producer_number(replayer, number, id),
            Record::Producer { number, seq } => {
                if let Some(entry) = replayer.numbers.get(&number).and_then(|id| self.producers.entries.get_mut(id)) {
                    entry.applied = entry.applied.max(seq);
                    entry.acked = entry.applied;
                }
            }
            Record::Generation(generation) => replayer.generation = replayer.generation.max(generation),
        }
    }

    /// Everything rebuilt: lanes, the wheel, accounting, the live counts of
    /// segments; then the new generation.
    fn settle_recovery(&mut self, replayer: Replayer, end: End) -> Result<Vec<Record>, String> {
        let uptime = end.uptime;
        self.now = uptime;
        self.serial = end.serial;
        self.generation = replayer.generation.checked_add(1).ok_or("the generation counter is exhausted; the server cannot start")?;
        self.arrival = 0;
        self.claim_seq = 0;

        let indices: Vec<Index> = replayer.keys.into_values().collect();
        for index in indices {
            let generation = self.slots.generation(index);
            let Some(slot) = self.slots.get_mut(index) else { continue };
            slot.fixed.registered_at = uptime;
            if matches!(slot.place, Place::Delayed { until } if until <= uptime) {
                slot.place = Place::Waiting;
            }
            let (lane, key, at) = (slot.lane(), slot.fixed.key, slot.at);
            let due = match &mut slot.place {
                Place::Claimed(attempt) => {
                    attempt.until = uptime.saturating_add(attempt.lease_ms);
                    Some((attempt.until, Due::Lease(attempt.token)))
                }
                Place::Delayed { until } => Some((*until, Due::Visible)),
                Place::Waiting => None,
            };
            match due {
                Some((at, due)) => self.wheel.wake_at(at, index, generation, due),
                None => {
                    self.lane_mut(&lane).queue.insert(key, index);
                }
            }
            if let Some(at) = at {
                self.book.live_add(&at);
            }
            if let Some(slot) = self.slots.get(index) {
                let usage = self.usage(slot);
                self.account_add(usage);
            }
        }
        let ids: Vec<ProducerId> = self.producers.entries.keys().copied().collect();
        for id in ids {
            if let Some(entry) = self.producers.entries.get_mut(&id) {
                entry.closed_at = Some(uptime);
                self.producers.idle.insert((uptime, id));
            }
        }
        let startup = vec![Record::Generation(self.generation)];
        self.serial += startup.len() as u64;
        Ok(startup)
    }

    /// The serial the next record takes.
    pub fn serial(&self) -> u64 {
        self.serial
    }

    pub fn now(&self) -> u64 {
        self.now
    }
}
