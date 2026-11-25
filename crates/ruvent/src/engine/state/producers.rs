//! The producer sequence table and cumulative commit acknowledgement (2.3).

use crate::engine::ops::SessionId;
use crate::envelope::ProducerId;
use std::collections::{BTreeSet, HashMap, VecDeque};

/// One producer: a retransmission buffer's name.
#[derive(Debug)]
pub(super) struct Producer {
    /// Its number in the journal, given at its first durable operation.
    pub number: Option<u32>,
    /// The last applied sequence number.
    pub applied: u64,
    /// The last sequence number told in `Committed`.
    pub acked: u64,
    /// Unacknowledged durable work: (commit number of the batch, first
    /// sequence number in it that needs that batch), oldest first.
    pub waits: VecDeque<(u64, u64)>,
    pub session: Option<SessionId>,
    pub nonce: [u8; 16],
    /// When its last connection closed (uptime), while it has none.
    pub closed_at: Option<u64>,
    pub linger_ms: u64,
    /// It applied a durable operation in this batch: its sequence goes in the batch.
    pub durable_now: bool,
    /// Its connection asked for no `Committed`.
    pub quiet: bool,
}

impl Producer {
    pub fn new(nonce: [u8; 16], linger_ms: u64) -> Self {
        Producer { number: None, applied: 0, acked: 0, waits: VecDeque::new(), session: None, nonce, closed_at: None, linger_ms, durable_now: false, quiet: false }
    }

    /// The highest sequence number whose effects are all committed.
    pub fn confirmable(&self) -> u64 {
        self.waits.front().map_or(self.applied, |(_, first)| first - 1)
    }

    /// Notes that `seq` needs the batch with commit number `commit`.
    pub fn wait_for(&mut self, commit: u64, seq: u64) {
        if self.waits.back().is_none_or(|(last, _)| *last != commit) {
            self.waits.push_back((commit, seq));
        }
        self.durable_now = true;
    }

    /// The batch `commit` is on disk.
    pub fn landed(&mut self, commit: u64) {
        while self.waits.front().is_some_and(|(waiting, _)| *waiting <= commit) {
            self.waits.pop_front();
        }
    }

    /// The commit number everything applied so far waits for, if anything.
    pub fn last_wait(&self) -> Option<u64> {
        self.waits.back().map(|(commit, _)| *commit)
    }

    /// The `Committed` to send now, if the rule says so (2.3): when nothing
    /// waits for the disk any more, or once `every` operations are
    /// confirmable while later ones still wait. Marks it told.
    pub fn ack(&mut self, every: u64) -> Option<u64> {
        let confirmable = self.confirmable();
        if confirmable <= self.acked || (!self.waits.is_empty() && confirmable - self.acked < every.max(1)) {
            return None;
        }
        self.acked = confirmable;
        Some(confirmable)
    }
}

/// The table.
///
/// | key | producer id |
/// | --- | --- |
/// | scope | one engine, persisted in the journal (`ProducerDef`, `Producer`) and in snapshots |
/// | lifetime | first connection to last connection's close + linger (B4) |
/// | bound | `max_entries` (S15) × `entry_bytes`; at the bound the oldest entry without a connection goes |
/// | cleared by | the ledger: lingered-out entries at a snapshot, the oldest idle entry at the bound |
///
/// `idle` orders entries without a connection by close time, for eviction.
/// `acks` is the queue of (commit number, producer) whose waits a landing
/// batch settles (the "unacknowledged commit" row of 2.10): bounded by the
/// connected producers × batches in flight.
#[derive(Default)]
pub(super) struct Producers {
    pub entries: HashMap<ProducerId, Producer>,
    pub idle: BTreeSet<(u64, ProducerId)>,
    pub acks: VecDeque<(u64, ProducerId)>,
    /// Touched in this batch.
    pub touched: Vec<ProducerId>,
    pub next_number: u32,
}

impl Producers {
    pub fn touch(&mut self, id: ProducerId) {
        if self.touched.last() != Some(&id) {
            self.touched.push(id);
        }
    }

    /// Notes a durable operation and queues its producer for the landing.
    pub fn wait_for(&mut self, id: ProducerId, commit: u64, seq: u64) {
        if let Some(producer) = self.entries.get_mut(&id) {
            producer.wait_for(commit, seq);
            if self.acks.back() != Some(&(commit, id)) {
                self.acks.push_back((commit, id));
            }
        }
    }

    /// The producer's connection closed at `now`.
    pub fn detach(&mut self, id: &ProducerId, session: SessionId, now: u64) {
        if let Some(producer) = self.entries.get_mut(id) {
            if producer.session == Some(session) {
                producer.session = None;
                producer.closed_at = Some(now);
                self.idle.insert((now, *id));
            }
        }
    }

    /// The producer's connection opened.
    pub fn attach(&mut self, id: &ProducerId, session: SessionId) {
        if let Some(producer) = self.entries.get_mut(id) {
            if let Some(closed) = producer.closed_at.take() {
                self.idle.remove(&(closed, *id));
            }
            producer.session = Some(session);
        }
    }

    /// Makes room for one more entry by evicting the oldest idle one.
    pub fn make_room(&mut self, max: usize) {
        while self.entries.len() >= max.max(1) {
            let Some(oldest) = self.idle.pop_first() else { break };
            self.entries.remove(&oldest.1);
        }
    }

    /// Drops the idle entries whose linger ran out by `now`.
    pub fn expire(&mut self, now: u64) {
        let gone: Vec<(u64, ProducerId)> = self.idle.iter().filter(|(closed, id)| self.entries.get(id).is_none_or(|producer| closed.saturating_add(producer.linger_ms) <= now)).copied().collect();
        for entry in gone {
            self.idle.remove(&entry);
            self.entries.remove(&entry.1);
        }
    }
}
