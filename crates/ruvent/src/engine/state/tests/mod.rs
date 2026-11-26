//! The ledger's rules, decided against the state machine alone: no task, no
//! socket, no clock. A harness feeds it the time and the commit numbers.

mod consume;
mod invariants;
mod observe;
mod produce;
mod recover;
mod resources;
mod storage;
mod timing;

use super::slots::{Place, Slot};
use super::*;
use crate::engine::config::{Durability, Grant, MemoryConfig, TypeRule, TypeSettings};
use crate::engine::ops::{Assignment, ByeReason, FaultKind, Hello, Outbound, Refusal, Role};
use crate::types::TypeSet;
use bytes::Bytes;
use std::time::Duration;

pub const P: SessionId = 1;
pub const W1: SessionId = 2;
pub const W2: SessionId = 3;
pub const O: SessionId = 4;

/// A ledger with a clock and commit numbers under the test's control.
pub struct H {
    pub s: State,
    pub now: u64,
    pub durable: u64,
    pub carrying: u64,
    /// Every record written so far, in order.
    pub records: Vec<Record>,
    /// The same records with their serials.
    pub log: Vec<(u64, Record)>,
    /// How many of `log` a simulated segment writer has landed (`land_in`).
    pub written: usize,
    /// The first serial of each simulated segment.
    pub firsts: HashMap<u32, u64>,
    /// The next sequence number per numbered connection.
    seqs: HashMap<SessionId, u64>,
    /// The `req` of each (connection, channel label) join made through [`H::join`].
    joined: HashMap<(SessionId, String), u32>,
}

/// The tests' volatile type (a type rule, see [`config`]).
pub const VOLATILE: u16 = 900;

/// A test channel: a UUIDv7-shaped id derived from a label, the same every time.
pub fn ch(label: &str) -> Channel {
    let fnv = |seed: u64| label.bytes().fold(seed, |hash, byte| (hash ^ u64::from(byte)).wrapping_mul(0x100_0000_01b3));
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&fnv(0xcbf2_9ce4_8422_2325).to_le_bytes());
    bytes[8..].copy_from_slice(&fnv(0x9e37_79b9_7f4a_7c15).to_le_bytes());
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Channel(bytes)
}

/// A type rule over `types` with `settings`.
pub fn rule(types: impl IntoIterator<Item = u16>, settings: TypeSettings) -> TypeRule {
    TypeRule { types: TypeSet::of(types), settings }
}

/// The tests' engine: a memory limit far from every test's size, so no pause
/// happens unless the test asks for one, and [`VOLATILE`] a volatile type (no
/// default rule makes any type volatile).
pub fn config() -> EngineConfig {
    let mut config = EngineConfig { memory: MemoryConfig { limit: Some(1 << 40), ..MemoryConfig::default() }, ..EngineConfig::default() };
    config.types.rules.insert(0, rule([VOLATILE], TypeSettings { volatile: true, ..TypeSettings::default() }));
    config
}

pub fn strict() -> EngineConfig {
    EngineConfig { durability: Durability::Strict, ..config() }
}

pub fn hello(role: Role, byte: u8) -> Hello {
    Hello { role, credential: "svc".into(), producer: (role != Role::Observe).then_some([byte; 16]), nonce: [byte; 16], linger_ms: None, acks: true, types: Vec::new() }
}

pub fn id(byte: u8) -> EventId {
    let mut bytes = [byte; 16];
    bytes[6] = 0x70 | (byte & 0x0f);
    bytes[8] = 0x80 | (byte & 0x3f);
    EventId(bytes)
}

/// A claimable event of type 1 on the channel labelled `channel`.
pub fn work(channel: &str) -> Envelope {
    Envelope::claimable(ch(channel), 1, Bytes::from_static(b"p"))
}

/// A claimable event of the volatile type on the channel labelled `channel`.
pub fn volatile(channel: &str) -> Envelope {
    Envelope::claimable(ch(channel), VOLATILE, Bytes::from_static(b"p"))
}

/// What `session` was sent, in order (closes left out).
pub fn sent(effects: &Effects, session: SessionId) -> Vec<Outbound> {
    effects.sends.iter().filter(|send| send.session == session).filter_map(|send| send.message.clone()).collect()
}

pub fn assigned(effects: &Effects, session: SessionId) -> Vec<Assignment> {
    sent(effects, session)
        .into_iter()
        .filter_map(|message| match message {
            Outbound::Assign(assignment) => Some(assignment),
            _ => None,
        })
        .collect()
}

pub fn committed(effects: &Effects, session: SessionId) -> Vec<u64> {
    sent(effects, session)
        .into_iter()
        .filter_map(|message| match message {
            Outbound::Committed { seq } => Some(seq),
            _ => None,
        })
        .collect()
}

pub fn faults(effects: &Effects, session: SessionId) -> Vec<FaultKind> {
    sent(effects, session)
        .into_iter()
        .filter_map(|message| match message {
            Outbound::Fault { kind, .. } => Some(kind),
            _ => None,
        })
        .collect()
}

pub fn closed(effects: &Effects, session: SessionId) -> bool {
    effects.sends.iter().any(|send| send.session == session && send.message.is_none())
}

impl H {
    pub fn with(config: EngineConfig, journaled: bool) -> H {
        let mut s = State::new(&config, Box::new(V7));
        let records = s.recover(Vec::new(), 0, 0, journaled).unwrap();
        let log = records.iter().cloned().map(|record| (0, record)).collect();
        H { s, now: 0, durable: 1, carrying: 2, records, log, written: 0, firsts: HashMap::new(), seqs: HashMap::new(), joined: HashMap::new() }
    }

    /// No journal: every channel volatile, every operation committed when applied.
    pub fn volatile() -> H {
        Self::with(config(), false)
    }

    /// A journal; Scheduled or Strict.
    pub fn durable(strict: bool) -> H {
        Self::with(if strict { self::strict() } else { config() }, true)
    }

    /// The usual cast: a producer, two workers, an observer.
    pub fn cast(mut self) -> H {
        self.open(P, hello(Role::Produce, 1));
        self.open(W1, hello(Role::Work, 2));
        self.open(W2, hello(Role::Work, 3));
        self.open(O, hello(Role::Observe, 4));
        self
    }

    pub fn step(&mut self, apply: impl FnOnce(&mut State)) -> Effects {
        self.s.begin(self.now, self.durable, self.carrying);
        apply(&mut self.s);
        let effects = self.s.take();
        self.keep(&effects);
        effects
    }

    fn keep(&mut self, effects: &Effects) {
        self.records.extend(effects.records.iter().cloned());
        self.log.extend(effects.records.iter().cloned().enumerate().map(|(at, record)| (effects.first_serial + at as u64, record)));
    }

    /// Everything recorded since the last call lands in log segment `segment`
    /// as a segment log would write it: payloads at made-up offsets, the
    /// segment reported. The batch being filled is on disk afterwards.
    pub fn land_in(&mut self, segment: u32, bytes: u64) -> Effects {
        let fresh = &self.log[self.written..];
        let located: Vec<(u64, crate::journal::Location)> = fresh
            .iter()
            .filter_map(|(serial, record)| match record {
                Record::Register { event, at: None, .. } => {
                    Some((*serial, crate::journal::Location { segment, offset: 1000 + serial * 100, len: event.payload.len() as u32, crc: crc32fast::hash(&event.payload) }))
                }
                _ => None,
            })
            .collect();
        let through = self.log.last().map_or(0, |(serial, _)| serial + 1);
        let first = fresh.first().map_or(through, |(serial, _)| *serial);
        let first = *self.firsts.entry(segment).or_insert(first);
        self.written = self.log.len();
        let state = crate::journal::SegmentState { number: segment, lane: crate::journal::Lane::Log, bytes, first, removed: false };
        let written = crate::journal::Written { located, segments: vec![state], log_bytes: 100 };
        let landing = self.carrying;
        self.carrying += 1;
        self.durable = landing;
        self.s.begin(self.now, self.durable, self.carrying);
        self.s.wrote(written, through);
        self.s.landed(landing);
        let effects = self.s.take();
        self.keep(&effects);
        effects
    }

    /// The batch being filled is now on disk.
    pub fn land(&mut self) -> Effects {
        let landing = self.carrying;
        self.carrying += 1;
        self.durable = landing;
        self.s.begin(self.now, self.durable, self.carrying);
        self.s.landed(landing);
        let effects = self.s.take();
        self.keep(&effects);
        effects
    }

    /// The engine hands the batch being filled to the writer; later records go
    /// in the next one. Returns the handed batch's commit number.
    pub fn launch(&mut self) -> u64 {
        self.carrying += 1;
        self.carrying - 1
    }

    /// The batches up to `commit` are on disk; the one being filled is not.
    pub fn land_through(&mut self, commit: u64) -> Effects {
        self.durable = commit;
        self.s.begin(self.now, self.durable, self.carrying);
        self.s.landed(commit);
        let effects = self.s.take();
        self.keep(&effects);
        effects
    }

    pub fn advance(&mut self, millis: u64) -> Effects {
        self.now += millis;
        self.step(|_| {})
    }

    pub fn open(&mut self, session: SessionId, hello: Hello) -> Effects {
        self.step(|state| state.open(session, hello))
    }

    pub fn close(&mut self, session: SessionId, unwritten: &[ClaimToken]) -> Effects {
        let unwritten = unwritten.to_vec();
        self.step(move |state| state.close(session, &unwritten))
    }

    pub fn call(&mut self, session: SessionId, call: Call) -> Effects {
        self.step(|state| state.call(session, call))
    }

    pub fn next_seq(&mut self, session: SessionId) -> u64 {
        let next = self.seqs.entry(session).or_insert(1);
        let seq = *next;
        *next += 1;
        seq
    }

    /// `Register` from the producer `session`, under its next sequence number.
    pub fn produce(&mut self, session: SessionId, event: Envelope) -> Effects {
        let seq = self.next_seq(session);
        self.call(session, Call::Register { seq: Some(seq), event })
    }

    /// Registers a claimable event with a fresh id and returns the id.
    pub fn register(&mut self, channel: &str) -> EventId {
        let id = EventId::new_v7();
        self.produce(P, work(channel).with_id(id));
        id
    }

    /// Joins the channel labelled `channel` for every type (a fresh `req`
    /// per connection, from 100) and opens a window of `count`.
    pub fn join(&mut self, session: SessionId, channel: &str, count: u32) -> Effects {
        let req = 100 + self.joined.keys().filter(|(joiner, _)| *joiner == session).count() as u32;
        let req = *self.joined.entry((session, channel.to_string())).or_insert(req);
        let mut effects = self.call(session, Call::Join { req, channel: Some(ch(channel)), types: Vec::new(), isolated: false, lease_ms: 30_000, dispatch: false });
        let more = self.call(session, Call::Window { join: req, count, bytes: 0 });
        effects.sends.extend(more.sends);
        effects
    }

    /// The join id [`H::join`] gave `session` on the channel labelled `channel`.
    pub fn join_id(&self, session: SessionId, channel: &str) -> JoinId {
        (session, self.joined[&(session, channel.to_string())])
    }

    pub fn complete(&mut self, session: SessionId, assignment: &Assignment, follow_ups: Vec<Envelope>) -> Effects {
        let seq = self.next_seq(session);
        self.call(session, Call::Complete { seq, id: assignment.event.id, token: assignment.token, follow_ups, spooled: Vec::new() })
    }

    /// The slot holding `id`.
    pub fn slot(&self, id: EventId) -> Option<&Slot> {
        self.s.slots.by_id(&id).and_then(|index| self.s.slots.get(index))
    }

    pub fn waiting(&self, channel: &str) -> usize {
        self.s.lanes.iter().filter(|(key, _)| key.0 == ch(channel)).map(|(_, lane)| lane.queue.len()).sum()
    }
}
