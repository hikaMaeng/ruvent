//! The event slots: a dense arena, an id index and slot generations.

use crate::envelope::{Channel, Digest, Envelope, EventId, Kind, WorkKey, WorkerId};
use crate::journal::Location;
use bytes::Bytes;
use std::collections::HashMap;

/// A slot's position in the arena. Reused after the event leaves; the slot
/// generation tells an old reference from a new one.
pub(super) type Index = u32;

/// A join: (work connection, the `req` of its `Join`).
pub(super) type JoinId = (crate::engine::ops::SessionId, u32);

/// The claim of a claimed event: who holds it and until when. (The event
/// and the worker name it; there is no other token.)
#[derive(Clone, Debug)]
pub(super) struct Attempt {
    pub lease_ms: u64,
    /// Absolute uptime at which the lease runs out.
    pub until: u64,
    /// The uuid of the worker that claimed it, as its claim request said.
    /// A completion, an extension or a failure report must name it.
    pub worker: WorkerId,
    /// The join it counts against. `None` after its connection closed or a
    /// restart, until an `Extend` moves it to a join.
    pub join: Option<JoinId>,
}

/// Where an event is.
#[derive(Clone, Debug)]
pub(super) enum Place {
    /// In its lane, `(channel, type)`, at its work key.
    Waiting,
    /// In the wheel until `until` (absolute uptime).
    Delayed {
        until: u64,
    },
    Claimed(Attempt),
}

/// What is fixed when the event is registered: everything a `Register`
/// record restores, and nothing that changes afterwards (2.1, the immutable
/// part a cold-queue pager would leave on disk).
#[derive(Clone, Debug)]
pub(super) struct Fixed {
    pub id: EventId,
    pub key: WorkKey,
    pub event_type: u16,
    /// The visibility time it was registered with (absolute), for the envelope.
    pub not_before: Option<u64>,
    /// Journaled: on a server with a journal.
    pub durable: bool,
    /// Uptime when registered, for "oldest waiting" ages.
    pub registered_at: u64,
    /// The commit number of the batch that carries its registration; `0`
    /// when that is on disk already or nothing is written. Strict visibility
    /// compares this (2.7).
    pub visible_from: u64,
    /// The payload digest, for producer-chosen ids. Server-assigned ids are
    /// never offered twice, so theirs is computed only if ever needed.
    pub digest: Option<Digest>,
}

/// One held event: the fixed part and the part that changes.
#[derive(Clone, Debug)]
pub(super) struct Slot {
    pub fixed: Fixed,
    pub channel: Channel,
    pub failures: u16,
    pub place: Place,
    /// The commit number and record serial of its last change: what the fuzzy
    /// checkpoint splits replay on (2.5). `0` before anything is written.
    pub commit: u64,
    pub serial: u64,
    /// The payload in memory: the body cache (2.5). Empty while the payload
    /// is only on disk — released under memory pressure, recovered, or
    /// streamed (then [`Slot::cold`]).
    pub payload: Bytes,
    /// Where the payload is on disk: set when its registration lands, when it
    /// was streamed, and by relocation. A durable payload's only copy on disk.
    pub at: Option<Location>,
}

impl Slot {
    pub fn digest(&self) -> Digest {
        self.fixed.digest.or_else(|| self.at.map(|at| at.digest())).unwrap_or_else(|| Digest::of(&self.payload))
    }

    /// The payload is on disk and not in memory: it is read before it goes out.
    pub fn cold(&self) -> bool {
        self.at.is_some_and(|at| self.payload.len() as u64 != u64::from(at.len))
    }

    /// The event as it goes out: head, key, failures, optional fields, payload.
    pub fn envelope(&self) -> Envelope {
        let fixed = &self.fixed;
        Envelope {
            id: fixed.id,
            channel: self.channel,
            event_type: fixed.event_type,
            kind: Kind::Claimable,
            failures: self.failures,
            key: Some(fixed.key),
            not_before: fixed.not_before,
            worker: self.attempt().map(|attempt| attempt.worker),
            payload: self.payload.clone(),
        }
    }

    pub fn lane(&self) -> (Channel, u16) {
        (self.channel, self.fixed.event_type)
    }

    /// The payload's length, wherever it is.
    pub fn len(&self) -> u64 {
        self.at.map_or(self.payload.len() as u64, |at| u64::from(at.len))
    }

    pub fn attempt(&self) -> Option<&Attempt> {
        match &self.place {
            Place::Claimed(attempt) => Some(attempt),
            _ => None,
        }
    }
}

struct Entry {
    generation: u32,
    slot: Option<Slot>,
}

/// The ledger's events.
///
/// | key | slot index; `ids`: logical id → slot index |
/// | --- | --- |
/// | scope | one engine |
/// | lifetime | registration to completion or deletion |
/// | bound | the events registered and not completed; metadata stays, payloads page out (2.5) |
/// | cleared by | the ledger, in `State::remove_event` |
#[derive(Default)]
pub(super) struct Slots {
    entries: Vec<Entry>,
    free: Vec<Index>,
    ids: HashMap<EventId, Index>,
}

impl Slots {
    pub fn insert(&mut self, slot: Slot) -> Index {
        let id = slot.fixed.id;
        let index = match self.free.pop() {
            Some(index) => {
                self.entries[index as usize].slot = Some(slot);
                index
            }
            None => {
                self.entries.push(Entry { generation: 0, slot: Some(slot) });
                (self.entries.len() - 1) as Index
            }
        };
        self.ids.insert(id, index);
        index
    }

    pub fn remove(&mut self, index: Index) -> Option<Slot> {
        let entry = self.entries.get_mut(index as usize)?;
        let slot = entry.slot.take()?;
        entry.generation = entry.generation.wrapping_add(1);
        if self.ids.get(&slot.fixed.id) == Some(&index) {
            self.ids.remove(&slot.fixed.id);
        }
        self.free.push(index);
        Some(slot)
    }

    pub fn get(&self, index: Index) -> Option<&Slot> {
        self.entries.get(index as usize)?.slot.as_ref()
    }

    pub fn get_mut(&mut self, index: Index) -> Option<&mut Slot> {
        self.entries.get_mut(index as usize)?.slot.as_mut()
    }

    /// The slot at `index` if it is still the one `generation` named.
    pub fn current(&self, index: Index, generation: u32) -> Option<&Slot> {
        let entry = self.entries.get(index as usize)?;
        if entry.generation == generation {
            entry.slot.as_ref()
        } else {
            None
        }
    }

    pub fn current_mut(&mut self, index: Index, generation: u32) -> Option<&mut Slot> {
        let entry = self.entries.get_mut(index as usize)?;
        if entry.generation == generation {
            entry.slot.as_mut()
        } else {
            None
        }
    }

    pub fn generation(&self, index: Index) -> u32 {
        self.entries.get(index as usize).map_or(0, |entry| entry.generation)
    }

    pub fn by_id(&self, id: &EventId) -> Option<Index> {
        self.ids.get(id).copied()
    }

    pub fn contains_id(&self, id: &EventId) -> bool {
        self.ids.contains_key(id)
    }

    pub fn len(&self) -> usize {
        self.ids.len()
    }

    /// One past the highest index ever used: what a scan of the arena walks.
    pub fn extent(&self) -> usize {
        self.entries.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (Index, &Slot)> {
        self.entries.iter().enumerate().filter_map(|(index, entry)| entry.slot.as_ref().map(|slot| (index as Index, slot)))
    }
}
