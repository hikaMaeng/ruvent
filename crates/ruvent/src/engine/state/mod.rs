//! The ledger: the engine's state and the rules the server keeps — nothing else.
//!
//! The rules: producer sequences and cumulative commit acknowledgement,
//! windows and round-robin assignment, leases, delays and expiries,
//! isolation at a failure threshold, Strict visibility, root-channel pause.
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
//! | waiting order | (channel, type) → work key (`Lane`) | server | while waiting | as the metadata | ledger |
//! | root accounting | root → events, bytes, ended lineages, share at pause (`Root`) | server | while it holds events or is paused | roots with events | ledger |
//! | drained roots | root (`drained`) | server | emptied after draining ~ refilled | `pause::DRAINED` | ledger (cleared whole at the bound) |
//! | pause-notice record | (production connection, root) → bytes since (`Paused::notified`) | server | `Paused` ~ `Resumed`, close | connections writing to paused roots | ledger |
//! | net-growth counters | (channel, type) → recent parents and follow-ups (`Growth`) | server | while the lane has members or events | lanes × four counters | ledger |
//! | hold lanes | (root, channel, type) → work key (`holds`) | server | while the root is paused | the root's waiting events | ledger (released first when the share comes down) |
//! | groups being assigned | (channel, group) → attempts (`groups`) | server | assignment ~ completion, release, reclaim | the channel's attempts | ledger; only on `serialize_group` channels |
//! | uptime | server | server, in every journal batch | from the first start | 8 bytes | ledger |
//! | producer table | producer id (`Producers`) | server, journaled | first connection ~ last close + linger | `max_entries` × entry bytes | ledger |
//! | unacknowledged commits | producer (`Producers::acks`, `Producer::waits`) | ledger | apply ~ `Committed` | connected producers | ledger (landing batch and batch end) |
//! | joins and windows | (work connection, channel) → window, held, signal bit (`Join`) | server | `Join` ~ close | the window | ledger |
//! | timing wheel | (time, slot, generation) (`Wheel`) | server | assignment, delay, expiry ~ due | claimed + delayed + expiring | ledger |
//! | subscriptions | channel or prefix → connections (`Subscribers`) | server | subscribe ~ unsubscribe, close | subscriptions | ledger |
//! | isolated events | logical id, on `isolated.<channel>` | server | isolation ~ completion, deletion, keep time | counted to the original root | workers, expiry |
//! | reply-channel results | logical id, on `reply.<instance>` | server | follow-up ~ completion or expiry | counted to the request's root | requesters, expiry |
//! | drain mark | server | server | `Drain` ~ stop | 1 | operator |
//!
//! Fault dedup per (connection, kind) lives in `Session::faults`: the
//! connection owner may dedup again; the ledger does it so a misconfigured
//! producer cannot fill the send buffer with faults.

mod consume;
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

use super::config::{EngineConfig, Grants, Method};
use super::ops::{ByeReason, Call, Counters, FaultKind, Hello, Outbound, Refusal, Role, SessionId, Target, Welcome};
use crate::envelope::{ClaimToken, Envelope, EventId, GroupId, ProducerId, WorkKey};
use crate::journal::Record;
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
    targets: HashSet<Target>,
    joins: Vec<Name>,
    /// Fault kinds already told on this connection (2.7).
    faults: u8,
}

/// Who receives what on a channel.
///
/// | key | channel (exact), prefix, channel (dispatch joins) |
/// | --- | --- |
/// | lifetime | subscribe or join ~ unsubscribe or close |
/// | bound | subscriptions + dispatch joins |
/// | cleared by | `State::unsubscribe_from`, `State::close` |
#[derive(Default)]
struct Subscribers {
    exact: HashMap<Name, Vec<SessionId>>,
    prefix: HashMap<Name, Vec<SessionId>>,
    dispatch: HashMap<Name, Vec<SessionId>>,
}

fn remove_from(map: &mut HashMap<Name, Vec<SessionId>>, name: &Name, session: SessionId) {
    if let Some(list) = map.get_mut(name) {
        list.retain(|held| *held != session);
        if list.is_empty() {
            map.remove(name);
        }
    }
}

impl Subscribers {
    fn add(&mut self, target: &Target, session: SessionId) {
        let (map, name) = match target {
            Target::Channel(name) => (&mut self.exact, name),
            Target::Prefix(name) => (&mut self.prefix, name),
        };
        let list = map.entry(name.clone()).or_default();
        if !list.contains(&session) {
            list.push(session);
        }
    }

    fn remove(&mut self, target: &Target, session: SessionId) {
        match target {
            Target::Channel(name) => remove_from(&mut self.exact, name, session),
            Target::Prefix(name) => remove_from(&mut self.prefix, name, session),
        }
    }

    /// Subscribers of `channel`: exact, and every prefix at a `.` boundary.
    /// With `dispatch`, also the joins that take the channel's dispatch events.
    fn of(&self, channel: &str, dispatch: bool) -> Vec<SessionId> {
        let mut found: Vec<SessionId> = self.exact.get(channel).cloned().unwrap_or_default();
        if !self.prefix.is_empty() {
            for (at, _) in channel.match_indices('.') {
                if let Some(list) = self.prefix.get(&channel[..at]) {
                    found.extend(list);
                }
            }
        }
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
    holds: HashMap<(Name, LaneKey), BTreeMap<WorkKey, Index>>,
    wheel: Wheel,
    /// Groups with an attempt out, on `serialize_group` channels.
    groups: HashMap<(Name, GroupId), u32>,
    producers: Producers,
    roots: HashMap<Name, Root>,
    /// Roots that are paused, for the per-batch resume check.
    paused: BTreeSet<Name>,
    /// Roots that grew in this batch, for the early-pause and max-backlog checks.
    grown: Vec<Name>,
    /// Roots that emptied after a lineage of theirs ended: refilled, they do
    /// not count as never drained. Bound `pause::DRAINED`; cleared whole past it.
    drained: HashSet<Name>,
    /// Bytes counted for every slot.
    slot_bytes: u64,
    over_limit: bool,
    sessions: HashMap<SessionId, Session>,
    joins: HashMap<JoinId, Join>,
    /// Joins by channel, in join order.
    channel_joins: HashMap<Name, Vec<JoinId>>,
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
            next_order: 0,
            subscribers: Subscribers::default(),
            awaiting: VecDeque::new(),
            to_fill: Vec::new(),
            to_offer: Vec::new(),
            draining: false,
            counters: Counters::default(),
            id_source,
            effects: Effects::default(),
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
        let effects = std::mem::take(&mut self.effects);
        self.effects.first_serial = self.serial;
        effects
    }

    /// The batch with commit number `commit` is on disk.
    pub fn landed(&mut self, commit: u64) {
        self.durable = self.durable.max(commit);
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
        self.effects.sends.push(Outgoing { session, message: Some(message), gate });
    }

    /// Tells the connection why, closes it after everything sent before, and
    /// forgets it now: nothing it sent later is applied.
    fn close_session(&mut self, session: SessionId, bye: Option<ByeReason>) {
        if let Some(reason) = bye {
            self.send(session, Outbound::Bye(reason), None);
        }
        self.effects.sends.push(Outgoing { session, message: None, gate: None });
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

    fn allowed(&self, credential: &Name, method: Method, channel: &str) -> bool {
        allowed(&self.config.grants, credential, method, channel)
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
        self.sessions.insert(session, Session { role: hello.role, credential: credential.clone(), producer: None, acks: hello.acks, targets: HashSet::new(), joins: Vec::new(), faults: 0 });
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
            let writes = |method| hello.channels.iter().all(|channel| self.allowed(&credential, method, channel));
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
        for target in &gone.targets {
            self.subscribers.remove(target, session);
        }
        for channel in gone.joins {
            self.part(session, channel, unwritten);
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
            Call::Delete { id, result, tag, key, .. } => self.delete(session, &credential, id, &result, tag, key),
            Call::Join { req, channel, types, lease_ms, dispatch } => self.join(session, &credential, req, &channel, types, lease_ms, dispatch),
            Call::Window { channel, count, bytes } => self.window(session, &channel, count, bytes),
            Call::Complete { id, token, follow_ups, .. } => self.complete(session, &credential, id, token, follow_ups),
            Call::Extend { id, token } => self.extend(session, &credential, id, token),
            Call::Release { id, token, delay_ms, failed } => self.give_back(session, &credential, id, token, delay_ms, failed),
            Call::Claim { req, id, lease_ms } => self.claim(session, &credential, req, id, lease_ms),
            Call::Subscribe { req, target, after } => self.subscribe(session, &credential, req, target, after),
            Call::Unsubscribe { req, target } => {
                let target = self.adopt_target(target);
                if let Some(open) = self.sessions.get_mut(&session) {
                    open.targets.remove(&target);
                }
                self.subscribers.remove(&target, session);
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

    fn adopt_target(&mut self, target: Target) -> Target {
        match target {
            Target::Channel(name) => Target::Channel(self.names.adopt(&name)),
            Target::Prefix(name) => Target::Prefix(self.names.adopt(&name)),
        }
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

fn allowed(grants: &Option<Grants>, credential: &Name, method: Method, channel: &str) -> bool {
    match grants {
        None => true,
        Some(grants) => grants.get(credential.as_str()).is_some_and(|grant| grant.allows(method, channel)),
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
