//! The ledger: the engine's state and the rules the server keeps — nothing else.
//!
//! The rules: producer sequences and cumulative commit acknowledgement,
//! windows and round-robin assignment, leases, delays and expiries,
//! isolation at a failure threshold, Strict visibility, root-type pause.
//! How often and how long to retry is the workers' business (0.3).
//!
//! This is a plain state machine: calls go in, [`Effects`] come out. It owns
//! no task, no socket and no clock; the engine feeds it the time, the commit
//! number of the batch it is filling (`carrying`) and the last one on disk
//! (`durable`).
//!
//! ## State (2.10, the ledger's rows)
//!
//! | state | key | scope | lifetime | bound | cleared by |
//! | --- | --- | --- | --- | --- | --- |
//! | ledger metadata | slot (`Slots`, id → slot) | server | registration ~ completion, deletion, expiry | managed memory (root pause) | ledger |
//! | waiting order | (channel, type, isolated) → work key (`Lane`) | server | while waiting | as the metadata | ledger |
//! | root accounting | root type → events, bytes, ended lineages, share at pause (`Root`) | server | while it holds events or is paused | root types with events (≤ 65,536) | ledger |
//! | drained roots | root (`drained`) | server | emptied after draining ~ refilled | `pause::DRAINED` | ledger (cleared whole at the bound) |
//! | pause-notice record | (production connection, root) → bytes since (`Paused::notified`) | server | `Paused` ~ `Resumed`, close | connections writing to paused roots | ledger |
//! | net-growth counters | (channel, type, isolated) → recent parents and follow-ups (`Growth`) | server | while the lane has members or events | lanes × four counters | ledger |
//! | hold lanes | (root type, lane) → work key (`holds`) | server | while the root is paused | the root's waiting events | ledger (released first when the share comes down) |
//! | groups being assigned | (channel, group) → attempts (`groups`) | server | assignment ~ completion, release, reclaim | the channel's attempts | ledger; only for `serialize_group` types |
//! | uptime | server | server, in every journal batch | from the first start | 8 bytes | ledger |
//! | producer table | producer id (`Producers`) | server, journaled | first connection ~ last close + linger | `max_entries` × entry bytes | ledger |
//! | unacknowledged commits | producer (`Producers::acks`, `Producer::waits`) | ledger | apply ~ `Committed` | connected producers | ledger (landing batch and batch end) |
//! | joins and windows | (work connection, `Join` req) → selection, window, held, signal bit (`Join`) | server | `Join` ~ close | the window | ledger |
//! | timing wheel | (time, slot, generation) (`Wheel`) | server | assignment, delay, expiry ~ due | claimed + delayed + expiring | ledger |
//! | subscriptions | channel → connections (`Subscribers`) | server | subscribe ~ unsubscribe, close | subscriptions | ledger |
//! | isolated events | in place: `Slot::isolated`, their own lane (channel, type, isolated) | server | isolation ~ completion, deletion, keep time | counted to their root | isolated joins, expiry |
//! | drain mark | server | server | `Drain` ~ stop | 1 | operator |
//! | body cache | slot (`Slot::payload`; release order in `cache`) | server | registration or read-ahead ~ released at the watermark, or the event leaves | managed memory: released past the high watermark | ledger (`release_cache`) |
//! | unlocated registrations | serial → slot (`unlocated`) | ledger | durable registration ~ its write lands | registrations in flight | ledger (`wrote`) |
//! | reads ahead | slot (`warming`) | ledger | asked ~ answered | `read_ahead` × cold heads out | ledger (`warmed`) |
//! | segment book, frees, relocation | segment (`disk::Book`) | log directory | reported ~ removed | segments on disk | ledger, on the container's reports |
//! | checkpoint walk | slot cursor (`checkpoint::Scan`) | one checkpoint | begin ~ durable or aborted | one | ledger |
//!
//! Fault dedup per (connection, kind) lives in `Session::faults`: the
//! connection owner may dedup again; the ledger does it so a misconfigured
//! producer cannot fill the send buffer with faults.

mod checkpoint;
mod consume;
mod disk;
mod lanes;
mod lifecycle;
mod observe;
mod pause;
mod produce;
mod producers;
mod recover;
mod roots;
mod slots;
mod wheel;

pub use disk::Warm;

use super::config::Grant;
use super::config::{EngineConfig, Grants, Method};
use super::ops::{ByeReason, Call, Counters, FaultKind, Hello, Outbound, Refusal, Role, SessionId, Welcome};
use crate::envelope::{Channel, ClaimToken, Envelope, EventId, GroupId, ProducerId, WorkKey};
use crate::journal::{Location, Record};
use crate::names::{Name, Names};
use lanes::{Join, Lane, LaneKey};
use producers::Producers;
use roots::Root;
use slots::{Index, JoinId, Slots};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use wheel::Wheel;

/// Where fresh ids come from. UUIDv7 unless a test supplies its own.
pub trait IdSource: Send + 'static {
    fn next(&mut self) -> EventId;
}

/// UUIDv7.
pub struct V7;

impl IdSource for V7 {
    fn next(&mut self) -> EventId {
        EventId::new_v7()
    }
}

/// Something to hand a connection.
#[derive(Debug)]
pub struct Outgoing {
    pub session: SessionId,
    /// `None`: close the connection after everything before it.
    pub message: Option<Outbound>,
    /// Released only once the batch with this commit number is on disk.
    pub gate: Option<u64>,
    /// The event's payload is only on disk here: read it into the message's
    /// envelope before it goes out (2.5, the read path).
    pub read: Option<Location>,
}

/// What the ledger produced since the last [`State::take`].
#[derive(Default, Debug)]
pub struct Effects {
    pub records: Vec<Record>,
    /// The serial of `records[0]`.
    pub first_serial: u64,
    pub sends: Vec<Outgoing>,
    /// Copies for the operational snapshot.
    pub observed: Vec<Envelope>,
    /// Production connections whose reading must stop (`false`) or resume (`true`), 2.6.
    pub reading: Vec<(SessionId, bool)>,
    /// Payloads to read ahead into the cache; answer each with [`State::warmed`].
    pub warm: Vec<Warm>,
    /// Payloads to copy to the long-lived lane; answer with [`State::relocated`].
    pub relocate: Vec<Location>,
    /// Segments that may be removed once no read pins them.
    pub delete: Vec<u32>,
}

/// One open connection.
///
/// | key | session id |
/// | --- | --- |
/// | lifetime | `open` to `close` |
/// | bound | open connections |
/// | cleared by | `State::close` |
struct Session {
    role: Role,
    credential: Name,
    producer: Option<ProducerId>,
    acks: bool,
    /// Its subscriptions.
    channels: HashSet<Channel>,
    /// Its credential's grant, for what it may read. `None`: no grants are configured.
    grant: Option<Grant>,
    /// Its joins, by the `req` of their `Join`.
    joins: Vec<u32>,
    /// Fault kinds already told on this connection (2.7).
    faults: u8,
}

/// Who receives what on a channel.
///
/// | key | channel (subscriptions), channel (dispatch joins) |
/// | --- | --- |
/// | lifetime | subscribe or join ~ unsubscribe or close |
/// | bound | subscriptions + dispatch joins |
/// | cleared by | `State::unsubscribe_from`, `State::close` |
#[derive(Default)]
struct Subscribers {
    exact: HashMap<Channel, Vec<SessionId>>,
    dispatch: HashMap<Channel, Vec<SessionId>>,
}

fn remove_from<K: std::hash::Hash + Eq, V: PartialEq>(map: &mut HashMap<K, Vec<V>>, name: &K, item: V) {
    if let Some(list) = map.get_mut(name) {
        list.retain(|held| *held != item);
        if list.is_empty() {
            map.remove(name);
        }
    }
}

impl Subscribers {
    fn add(&mut self, channel: Channel, session: SessionId) {
        let list = self.exact.entry(channel).or_default();
        if !list.contains(&session) {
            list.push(session);
        }
    }

    fn remove(&mut self, channel: &Channel, session: SessionId) {
        remove_from(&mut self.exact, channel, session);
    }

    /// Subscribers of `channel`; with `dispatch`, also the joins that take
    /// the channel's dispatch events.
    fn of(&self, channel: &Channel, dispatch: bool) -> Vec<SessionId> {
        let mut found: Vec<SessionId> = self.exact.get(channel).cloned().unwrap_or_default();
        if dispatch {
            if let Some(list) = self.dispatch.get(channel) {
                found.extend(list);
            }
        }
        if found.len() > 1 {
            found.sort_unstable();
            found.dedup();
        }
        found
    }
}

pub struct State {
    config: EngineConfig,
    names: Names,
    server_id: [u8; 16],
    generation: u32,
    arrival: u64,
    claim_seq: u64,
    /// Accumulated uptime (ms) of the batch being applied.
    now: u64,
    /// The last commit number on disk.
    durable: u64,
    /// The commit number of the batch this batch's records go in.
    carrying: u64,
    /// The serial the next record takes.
    serial: u64,
    journaled: bool,
    strict: bool,
    memory_limit: u64,
    slots: Slots,
    lanes: HashMap<LaneKey, Lane>,
    /// Hold lanes: events of a paused root skipped in net-growth lanes.
    holds: HashMap<(u16, LaneKey), BTreeMap<WorkKey, Index>>,
    wheel: Wheel,
    /// Groups with an attempt out, for `serialize_group` types.
    groups: HashMap<(Channel, GroupId), u32>,
    producers: Producers,
    /// By root type: the type of the event that started the lineage.
    roots: HashMap<u16, Root>,
    /// Roots that are paused, for the per-batch resume check.
    paused: BTreeSet<u16>,
    /// Roots that grew in this batch, for the early-pause and max-backlog checks.
    grown: Vec<u16>,
    /// Roots that emptied after a lineage of theirs ended: refilled, they do
    /// not count as never drained. Bound `pause::DRAINED`; cleared whole past it.
    drained: HashSet<u16>,
    /// Bytes counted for every slot.
    slot_bytes: u64,
    over_limit: bool,
    sessions: HashMap<SessionId, Session>,
    joins: HashMap<JoinId, Join>,
    /// Joins with a channel, by channel, in join order.
    channel_joins: HashMap<Channel, Vec<JoinId>>,
    /// Joins without a channel, by each of their types, in join order.
    ///
    /// | key | type | scope | one engine | lifetime | while such a join selects it |
    /// | bound | joins × their types | cleared by | `State::part` and a replacing `Join` |
    type_joins: HashMap<u16, Vec<JoinId>>,
    /// The lanes there are, by type: where a join without a channel looks.
    ///
    /// | key | type | scope | one engine | lifetime | while a lane of that type exists |
    /// | bound | the lanes | cleared by | `State::tidy` |
    typed_lanes: HashMap<u16, HashSet<LaneKey>>,
    next_order: u64,
    subscribers: Subscribers,
    /// Lanes that got events still waiting for the disk (Strict): offered when it lands.
    awaiting: VecDeque<(u64, LaneKey)>,
    /// Work for the end of the current step.
    to_fill: Vec<JoinId>,
    to_offer: Vec<LaneKey>,
    draining: bool,
    counters: Counters,
    id_source: Box<dyn IdSource>,
    effects: Effects,
    book: disk::Book,
    unlocated: VecDeque<(u64, Index, u32)>,
    cache: VecDeque<(Index, u32)>,
    warming: HashSet<Index>,
    scan: Option<checkpoint::Scan>,
}

impl State {
    pub fn new(config: &EngineConfig, id_source: Box<dyn IdSource>) -> Self {
        State {
            config: config.clone(),
            names: Names::default(),
            server_id: config.server_id.unwrap_or_else(|| EventId::new_v7().0),
            generation: 0,
            arrival: 0,
            claim_seq: 0,
            now: 0,
            durable: 0,
            carrying: 1,
            serial: 0,
            journaled: true,
            strict: config.durability == super::config::Durability::Strict,
            memory_limit: config.memory.resolved_limit(),
            slots: Slots::default(),
            lanes: HashMap::new(),
            holds: HashMap::new(),
            wheel: Wheel::default(),
            groups: HashMap::new(),
            producers: Producers::default(),
            roots: HashMap::new(),
            paused: BTreeSet::new(),
            grown: Vec::new(),
            drained: HashSet::new(),
            slot_bytes: 0,
            over_limit: false,
            sessions: HashMap::new(),
            joins: HashMap::new(),
            channel_joins: HashMap::new(),
            type_joins: HashMap::new(),
            typed_lanes: HashMap::new(),
            next_order: 0,
            subscribers: Subscribers::default(),
            awaiting: VecDeque::new(),
            to_fill: Vec::new(),
            to_offer: Vec::new(),
            draining: false,
            counters: Counters::default(),
            id_source,
            effects: Effects::default(),
            book: disk::Book::default(),
            unlocated: VecDeque::new(),
            cache: VecDeque::new(),
            warming: HashSet::new(),
            scan: None,
        }
    }

    pub fn generation(&self) -> u32 {
        self.generation
    }

    pub fn server_id(&self) -> [u8; 16] {
        self.server_id
    }

    /// Claimable events held, in any state.
    pub fn live(&self) -> usize {
        self.slots.len()
    }

    pub fn draining(&self) -> bool {
        self.draining
    }

    /// The earliest uptime at which a lease ends or a delay is over.
    pub fn next_due(&self) -> Option<u64> {
        self.wheel.next()
    }

    /// Starts a batch at uptime `now`. `durable` is the last commit number on
    /// disk; `carrying` the one this batch's records will be written under.
    pub fn begin(&mut self, now: u64, durable: u64, carrying: u64) {
        self.now = self.now.max(now);
        self.durable = durable;
        self.carrying = carrying;
        if self.effects.records.is_empty() {
            self.effects.first_serial = self.serial;
        }
    }

    /// Ends a batch: what is due, expired, the producers' sequences and
    /// acknowledgements, the memory check; then hands over what it produced.
    pub fn take(&mut self) -> Effects {
        self.process_due();
        self.purge_expired();
        self.settle();
        self.flush_producers();
        self.check_memory();
        self.settle();
        self.tend_disk();
        let effects = std::mem::take(&mut self.effects);
        self.effects.first_serial = self.serial;
        effects
    }

    /// The batch with commit number `commit` is on disk.
    pub fn landed(&mut self, commit: u64) {
        self.durable = self.durable.max(commit);
        self.apply_frees(commit);
        while self.producers.acks.front().is_some_and(|(waiting, _)| *waiting <= commit) {
            let Some((_, id)) = self.producers.acks.pop_front() else { break };
            if let Some(producer) = self.producers.entries.get_mut(&id) {
                producer.landed(commit);
                let session = producer.session;
                let quiet = producer.quiet;
                if let Some(seq) = producer.ack(self.config.ack_every) {
                    if let (Some(session), false) = (session, quiet) {
                        self.send(session, Outbound::Committed { seq }, None);
                    }
                }
            }
        }
        while self.awaiting.front().is_some_and(|(waiting, _)| *waiting <= commit) {
            let Some((_, lane)) = self.awaiting.pop_front() else { break };
            self.to_offer.push(lane);
        }
        self.settle();
    }

    fn record(&mut self, record: Record) -> u64 {
        let serial = self.serial;
        self.serial += 1;
        self.effects.records.push(record);
        serial
    }

    /// Records a change of the slot at `index` and stamps it with the commit
    /// number and serial (2.1: the last change's).
    fn record_for(&mut self, index: Index, record: Record) {
        let serial = self.record(record);
        let carrying = self.carrying;
        if let Some(slot) = self.slots.get_mut(index) {
            slot.commit = carrying;
            slot.serial = serial;
        }
    }

    fn send(&mut self, session: SessionId, message: Outbound, gate: Option<u64>) {
        self.effects.sends.push(Outgoing { session, message: Some(message), gate, read: None });
    }

    /// A message carrying the event at `index`: its payload is read first if
    /// it is cold.
    fn send_event(&mut self, session: SessionId, message: Outbound, gate: Option<u64>, index: Index) {
        let read = self.slots.get(index).filter(|slot| slot.cold()).and_then(|slot| slot.at);
        self.effects.sends.push(Outgoing { session, message: Some(message), gate, read });
    }

    /// Tells the connection why, closes it after everything sent before, and
    /// forgets it now: nothing it sent later is applied.
    fn close_session(&mut self, session: SessionId, bye: Option<ByeReason>) {
        if let Some(reason) = bye {
            self.send(session, Outbound::Bye(reason), None);
        }
        self.effects.sends.push(Outgoing { session, message: None, gate: None, read: None });
        self.close(session, &[]);
    }

    /// A fault, told once per (connection, kind) and counted always.
    fn fault(&mut self, session: SessionId, kind: FaultKind, detail: &'static str) {
        self.counters.faults += 1;
        if let Some(open) = self.sessions.get_mut(&session) {
            if open.faults & kind.bit() == 0 {
                open.faults |= kind.bit();
                self.send(session, Outbound::Fault { kind, detail }, None);
            }
        }
    }

    fn allowed(&self, credential: &Name, method: Method, event_type: u16) -> bool {
        allowed(&self.config.grants, credential, method, event_type)
    }

    /// Whether the credential holds `method` at all, whatever the type: for
    /// calls whose event is not there to take a type from.
    fn has_method(&self, credential: &Name, method: Method) -> bool {
        match &self.config.grants {
            None => true,
            Some(grants) => grants.get(credential.as_str()).is_some_and(|grant| grant.methods.contains(&method)),
        }
    }

    /// The connections an event of `event_type` on `channel` goes to: the
    /// channel's subscribers (and with `dispatch`, its dispatch joins) whose
    /// credential reads the type.
    fn readers(&self, channel: &Channel, dispatch: bool, event_type: u16) -> Vec<SessionId> {
        let mut found = self.subscribers.of(channel, dispatch);
        found.retain(|session| self.sessions.get(session).is_some_and(|open| open.grant.as_ref().is_none_or(|grant| grant.reads(event_type))));
        found
    }

    fn may_delegate(&self, credential: &Name) -> bool {
        match &self.config.grants {
            None => true,
            Some(grants) => grants.get(credential.as_str()).is_some_and(|grant| grant.delegate),
        }
    }

    fn fresh_id(&mut self) -> EventId {
        loop {
            let id = self.id_source.next();
            if !id.is_nil() && !self.slots.contains_id(&id) {
                return id;
            }
        }
    }

    fn next_key(&mut self) -> WorkKey {
        let key = WorkKey { generation: self.generation, arrival: self.arrival };
        self.arrival += 1;
        key
    }

    fn next_token(&mut self) -> ClaimToken {
        let token = ClaimToken { generation: self.generation, seq: self.claim_seq };
        self.claim_seq += 1;
        token
    }

    /// The commit number a send about this slot must wait for under Strict.
    fn gate_for(&self, index: Index) -> Option<u64> {
        let slot = self.slots.get(index)?;
        (self.strict && slot.fixed.durable && slot.fixed.visible_from > self.durable).then_some(slot.fixed.visible_from)
    }

    /// Opens a connection. Its first message is `Welcome`, or `Bye`.
    pub fn open(&mut self, session: SessionId, hello: Hello) {
        let credential = self.names.adopt(&hello.credential);
        let grant =
            self.config.grants.as_ref().map(|grants| grants.get(credential.as_str()).cloned().unwrap_or_else(|| Grant { methods: Vec::new(), types: crate::types::TypeSet::none(), delegate: false }));
        self.sessions.insert(session, Session { role: hello.role, credential: credential.clone(), producer: None, acks: hello.acks, channels: HashSet::new(), grant, joins: Vec::new(), faults: 0 });
        let mut resume = 0;
        let mut superseded = false;
        let mut gate = None;
        if hello.role != Role::Observe {
            let Some(id) = hello.producer else {
                return self.close_session(session, Some(ByeReason::Protocol));
            };
            if hello.role == Role::Produce && self.draining {
                return self.close_session(session, Some(ByeReason::Draining));
            }
            let writes = |method| hello.types.iter().all(|kind| self.allowed(&credential, method, *kind));
            if hello.role == Role::Produce && !(writes(Method::RegisterClaimable) || writes(Method::RegisterDispatch)) {
                return self.close_session(session, Some(ByeReason::Forbidden));
            }
            let linger = hello.linger_ms.unwrap_or(millis(self.config.producers.linger)).min(millis(self.config.producers.linger_max));
            if !self.producers.entries.contains_key(&id) {
                self.producers.make_room(self.config.producers.max_entries);
                self.producers.entries.insert(id, producers::Producer::new(hello.nonce, linger));
            }
            let previous = self.producers.entries.get(&id).and_then(|producer| producer.session.map(|old| (old, producer.nonce)));
            if let Some((old, nonce)) = previous {
                // The same process reconnecting replaces its old connection
                // quietly; another instance under the same id pushes it out.
                if nonce == hello.nonce {
                    self.close_session(old, None);
                } else {
                    superseded = true;
                    self.close_session(old, Some(ByeReason::Superseded));
                }
            }
            self.producers.attach(&id, session);
            if let Some(producer) = self.producers.entries.get_mut(&id) {
                producer.nonce = hello.nonce;
                producer.linger_ms = producer.linger_ms.max(linger);
                producer.quiet = !hello.acks;
                resume = producer.applied;
                gate = producer.last_wait();
            }
            if let Some(open) = self.sessions.get_mut(&session) {
                open.producer = Some(id);
            }
        }
        let welcome = Welcome { server_id: self.server_id, generation: self.generation, credential, resume, superseded };
        self.send(session, Outbound::Welcome(welcome), gate);
    }

    /// A connection closed. `unwritten` are the tokens of assignments its
    /// owner had not finished writing: they go back to waiting at once, with
    /// their failure count unchanged. Fully written ones are due within the
    /// reconnect grace (2.4).
    pub fn close(&mut self, session: SessionId, unwritten: &[ClaimToken]) {
        let Some(gone) = self.sessions.remove(&session) else { return };
        for channel in &gone.channels {
            self.subscribers.remove(channel, session);
        }
        for join in gone.joins {
            self.part(session, join, unwritten);
        }
        if let Some(id) = gone.producer {
            self.producers.detach(&id, session, self.now);
        }
        for root in &self.paused {
            if let Some(paused) = self.roots.get_mut(root).and_then(|root| root.paused.as_mut()) {
                paused.notified.remove(&session);
            }
        }
        self.settle();
    }

    /// Applies one call from `session`. A call from a connection that is no
    /// longer open has nobody to answer and is dropped.
    pub fn call(&mut self, session: SessionId, call: Call) {
        let Some((role, credential, producer)) = self.sessions.get(&session).map(|open| (open.role, open.credential.clone(), open.producer)) else { return };
        let fits = matches!(
            (role, &call),
            (Role::Produce, Call::Register { seq: Some(_), .. } | Call::Delete { .. })
                | (Role::Work, Call::Register { seq: None, .. } | Call::Join { .. } | Call::Window { .. } | Call::Complete { .. } | Call::Extend { .. } | Call::Release { .. } | Call::Claim { .. })
                | (Role::Observe, Call::Subscribe { .. } | Call::Unsubscribe { .. } | Call::Stats { .. } | Call::Drain { .. })
        );
        if !fits {
            self.fault(session, FaultKind::Malformed, "this call is not one this connection's role sends");
            if let Some(req) = answered(&call) {
                self.send(session, Outbound::Refused { req, reason: Refusal::Malformed }, None);
            }
            return;
        }
        // Sequenced calls: drop retransmissions, close on a gap.
        let sequenced = match &call {
            Call::Register { seq: Some(seq), .. } | Call::Delete { seq, .. } | Call::Complete { seq, .. } => Some(*seq),
            _ => None,
        };
        let mut numbered = None;
        if let (Some(seq), Some(id)) = (sequenced, producer) {
            let applied = self.producers.entries.get(&id).map_or(0, |producer| producer.applied);
            if seq <= applied {
                self.counters.retransmits += 1;
                return;
            }
            if seq > applied + 1 {
                self.fault(session, FaultKind::SequenceGap, "sequence number skipped ahead");
                return self.close_session(session, Some(ByeReason::Protocol));
            }
            if let Some(entry) = self.producers.entries.get_mut(&id) {
                entry.applied = seq;
            }
            self.producers.touch(id);
            numbered = Some((id, seq));
        }
        let before = self.effects.records.len();
        match call {
            Call::Register { event, .. } => self.register(session, role, &credential, event),
            Call::Delete { id, result, key, .. } => self.delete(session, &credential, id, result, key),
            Call::Join { req, channel, types, isolated, lease_ms, dispatch } => self.join(session, &credential, req, channel, types, isolated, lease_ms, dispatch),
            Call::Window { join, count, bytes } => self.window(session, join, count, bytes),
            Call::Complete { id, token, follow_ups, spooled, .. } => self.complete(session, &credential, id, token, follow_ups, spooled),
            Call::Extend { id, token } => self.extend(session, &credential, id, token),
            Call::Release { id, token, delay_ms, failed } => self.give_back(session, &credential, id, token, delay_ms, failed),
            Call::Claim { req, id, lease_ms } => self.claim(session, &credential, req, id, lease_ms),
            Call::Subscribe { req, channel, after } => self.subscribe(session, &credential, req, channel, after),
            Call::Unsubscribe { req, channel } => {
                if let Some(open) = self.sessions.get_mut(&session) {
                    open.channels.remove(&channel);
                }
                self.subscribers.remove(&channel, session);
                self.send(session, Outbound::Unsubscribed { req }, None);
            }
            Call::Stats { req } => self.stats(session, &credential, req),
            Call::Drain { req } => self.drain(session, &credential, req),
        }
        if let Some((id, seq)) = numbered {
            if self.effects.records.len() > before {
                let carrying = self.carrying;
                self.producers.wait_for(id, carrying, seq);
            }
        }
        self.settle();
    }

    /// Puts the producers' sequences in this batch and tells them `Committed`
    /// where the rule says so (2.3).
    fn flush_producers(&mut self) {
        let touched = std::mem::take(&mut self.producers.touched);
        for id in touched {
            let Some(producer) = self.producers.entries.get_mut(&id) else { continue };
            if std::mem::take(&mut producer.durable_now) && self.journaled {
                let seq = producer.applied;
                let (number, define) = match producer.number {
                    Some(number) => (number, false),
                    None => {
                        let number = self.producers.next_number;
                        producer.number = Some(number);
                        (number, true)
                    }
                };
                if define {
                    self.producers.next_number += 1;
                    self.record(Record::ProducerDef { number, id });
                }
                self.record(Record::Producer { number, seq });
            }
            self.ack_producer(&id);
        }
    }

    fn ack_producer(&mut self, id: &ProducerId) {
        let every = self.config.ack_every;
        let Some(producer) = self.producers.entries.get_mut(id) else { return };
        let session = producer.session;
        let quiet = producer.quiet;
        if let Some(seq) = producer.ack(every) {
            if let (Some(session), false) = (session, quiet) {
                self.send(session, Outbound::Committed { seq }, None);
            }
        }
    }

    /// Runs the work a step left: fills joins that got room, then offers the
    /// lanes that got events.
    fn settle(&mut self) {
        loop {
            if let Some(join) = self.to_fill.pop() {
                self.fill(&join);
                continue;
            }
            if self.to_offer.is_empty() {
                break;
            }
            let mut lanes = std::mem::take(&mut self.to_offer);
            lanes.sort();
            lanes.dedup();
            for lane in lanes {
                self.offer(&lane);
            }
        }
    }
}

fn millis(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn allowed(grants: &Option<Grants>, credential: &Name, method: Method, event_type: u16) -> bool {
    match grants {
        None => true,
        Some(grants) => grants.get(credential.as_str()).is_some_and(|grant| grant.allows(method, event_type)),
    }
}

/// The request number of a call that is answered.
fn answered(call: &Call) -> Option<u32> {
    match call {
        Call::Join { req, .. } | Call::Claim { req, .. } | Call::Subscribe { req, .. } | Call::Unsubscribe { req, .. } | Call::Stats { req } | Call::Drain { req } => Some(*req),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
