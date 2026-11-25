//! Rebuilding the ledger from the journal, and the ledger as journal records.

use super::producers::Producer;
use super::slots::{Attempt, Fixed, Index, Place, Slot};
use super::wheel::Due;
use super::State;
use crate::envelope::{ProducerId, WorkKey};
use crate::journal::Record;
use std::collections::HashMap;

impl State {
    /// Rebuilds the ledger from journal records and starts a new generation.
    ///
    /// The clock carries on from `uptime`. A claimed event stays claimed
    /// (2.4): its token, lease length and credential come back, the lease is
    /// re-armed at its full length from now, and its failure count does not
    /// change — a restart is not a closed connection. `not_before` and
    /// `expires` are absolute and keep their remaining time.
    ///
    /// Returns the records that must be durable before the first answer: the
    /// new generation.
    pub fn recover(&mut self, records: Vec<Record>, uptime: u64, serial: u64, journaled: bool) -> Result<Vec<Record>, &'static str> {
        self.now = uptime;
        self.serial = serial;
        self.journaled = journaled;
        let mut keys: HashMap<WorkKey, Index> = HashMap::new();
        let mut numbers: HashMap<u32, ProducerId> = HashMap::new();
        let mut last_generation = 0u32;
        let linger = super::millis(self.config.producers.linger);
        for record in records {
            match record {
                Record::Register { event, root } => {
                    let key = event.key.ok_or("a journal registration without a work key")?;
                    if let Some(old) = keys.remove(&key) {
                        self.slots.remove(old);
                    }
                    let place = match event.not_before {
                        Some(until) if until > uptime => Place::Delayed { until },
                        _ => Place::Waiting,
                    };
                    let fixed = Fixed {
                        id: event.id,
                        key,
                        event_type: self.names.adopt(&event.event_type),
                        root: self.names.adopt(&root),
                        reply: event.reply.map(|name| self.names.adopt(&name)),
                        correlation: event.correlation,
                        principal: event.principal.map(|name| self.names.adopt(&name)),
                        delegate: event.delegate.map(|name| self.names.adopt(&name)),
                        group: event.group,
                        not_before: event.not_before,
                        durable: journaled,
                        registered_at: uptime,
                        visible_from: 0,
                        digest: None,
                    };
                    let channel = self.names.adopt(&event.channel);
                    let slot = Slot { fixed, channel, failures: event.failures, expires: event.expires, place, commit: 0, serial: 0, payload: event.payload, at: None };
                    keys.insert(key, self.slots.insert(slot));
                }
                // A body nothing points at is skipped (2.5).
                Record::Body { .. } => {}
                Record::Claim { key, token, lease_ms, credential } => {
                    let credential = self.names.adopt(&credential);
                    if let Some(slot) = keys.get(&key).and_then(|index| self.slots.get_mut(*index)) {
                        slot.place = Place::Claimed(Attempt { token, lease_ms, until: 0, credential, join: None });
                    }
                }
                Record::Remove(key) => {
                    if let Some(index) = keys.remove(&key) {
                        self.slots.remove(index);
                    }
                }
                Record::Reclaim { key, failures } => {
                    if let Some(slot) = keys.get(&key).and_then(|index| self.slots.get_mut(*index)) {
                        slot.failures = failures;
                        slot.place = Place::Waiting;
                    }
                }
                Record::Release { key, failures, visible_at } => {
                    if let Some(slot) = keys.get(&key).and_then(|index| self.slots.get_mut(*index)) {
                        slot.failures = failures;
                        slot.place = match visible_at {
                            Some(until) if until > uptime => Place::Delayed { until },
                            _ => Place::Waiting,
                        };
                    }
                }
                Record::Isolate { key, channel, failures, expires } => {
                    let channel = self.names.adopt(&channel);
                    if let Some(slot) = keys.get(&key).and_then(|index| self.slots.get_mut(*index)) {
                        slot.channel = channel;
                        slot.failures = failures;
                        slot.expires = expires;
                        slot.place = Place::Waiting;
                    }
                }
                Record::Relocate { key, at } => {
                    if let Some(slot) = keys.get(&key).and_then(|index| self.slots.get_mut(*index)) {
                        slot.at = Some(at);
                    }
                }
                Record::ProducerDef { number, id } => {
                    numbers.insert(number, id);
                    let entry = self.producers.entries.entry(id).or_insert_with(|| Producer::new([0; 16], linger));
                    entry.number = Some(number);
                    self.producers.next_number = self.producers.next_number.max(number + 1);
                }
                Record::Producer { number, seq } => {
                    if let Some(entry) = numbers.get(&number).and_then(|id| self.producers.entries.get_mut(id)) {
                        entry.applied = seq;
                        entry.acked = seq;
                    }
                }
                Record::Generation(generation) => last_generation = last_generation.max(generation),
            }
        }
        self.generation = last_generation.checked_add(1).ok_or("the generation counter is exhausted; the server cannot start")?;
        self.arrival = 0;
        self.claim_seq = 0;

        let indices: Vec<Index> = keys.into_values().collect();
        for index in indices {
            let generation = self.slots.generation(index);
            let Some(slot) = self.slots.get_mut(index) else { continue };
            let (root, len, lane, key, expires) = (slot.fixed.root.clone(), slot.len(), slot.lane(), slot.fixed.key, slot.expires);
            let due = match &mut slot.place {
                Place::Claimed(attempt) => {
                    attempt.until = uptime.saturating_add(attempt.lease_ms);
                    Some((attempt.until, Due::Lease(attempt.token)))
                }
                Place::Delayed { until } => Some((*until, Due::Visible)),
                Place::Waiting | Place::Held => None,
            };
            match due {
                Some((at, due)) => self.wheel.wake_at(at, index, generation, due),
                None => {
                    self.lane_mut(&lane).queue.insert(key, index);
                }
            }
            if let Some(at) = expires {
                self.wheel.expire_at(at, index, generation);
            }
            let cost = self.cost(len);
            self.account_add(&root, cost);
        }
        // History before the restart is unknown: no recovered root counts as
        // one that never drained.
        for root in self.roots.values_mut() {
            root.ended = root.ended.max(1);
        }
        self.grown.clear();
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

    /// The current state as journal records: what a rewrite writes. Producer
    /// entries whose linger ran out are dropped first.
    pub fn snapshot(&mut self) -> Vec<Record> {
        self.producers.expire(self.now);
        let mut records = vec![Record::Generation(self.generation)];
        let mut producers: Vec<(u32, ProducerId, u64)> = self.producers.entries.iter().filter_map(|(id, entry)| entry.number.map(|number| (number, *id, entry.applied))).collect();
        producers.sort();
        for (number, id, seq) in producers {
            records.push(Record::ProducerDef { number, id });
            records.push(Record::Producer { number, seq });
        }
        let mut slots: Vec<&Slot> = self.slots.iter().map(|(_, slot)| slot).filter(|slot| slot.fixed.durable).collect();
        slots.sort_by_key(|slot| slot.fixed.key);
        for slot in slots {
            records.push(Record::Register { event: slot.envelope(), root: slot.fixed.root.clone() });
            match &slot.place {
                Place::Claimed(attempt) => records.push(Record::Claim { key: slot.fixed.key, token: attempt.token, lease_ms: attempt.lease_ms, credential: attempt.credential.clone() }),
                Place::Delayed { until } => records.push(Record::Release { key: slot.fixed.key, failures: slot.failures, visible_at: Some(*until) }),
                Place::Waiting | Place::Held => {}
            }
        }
        records
    }

    /// The serial the next record takes.
    pub fn serial(&self) -> u64 {
        self.serial
    }

    pub fn now(&self) -> u64 {
        self.now
    }
}
