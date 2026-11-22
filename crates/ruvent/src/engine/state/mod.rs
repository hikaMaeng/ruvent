//! The engine's state and the rules the server keeps — nothing else.
//!
//! The rules: refuse an id that is in the claimable queue, compare the claim
//! token, reclaim after a deadline, send dispatch events, journal. Retry
//! limits, failure handling and follow-ups are the workers' business.
//!
//! This is a plain state machine: calls go in, [`Effects`] come out. It owns
//! no task, no socket and no clock; the engine feeds it the time.

use super::config::{EngineConfig, Grants, Method, SnapshotConfig};
use super::ops::{Call, DeleteFailure, Outbound, Refusal, Reply, SessionId, DELETED, DELETE_FAILED};
use crate::envelope::{Claim, ClaimToken, Envelope, EventId, Kind, WorkKey};
use crate::journal::Record;
use crate::names::{Name, Names};
use bytes::{BufMut, Bytes, BytesMut};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet};

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
    pub message: Outbound,
    /// Under `Strict`, released only after this batch's records are on disk.
    pub gated: bool,
}

/// What one batch of calls produced.
#[derive(Default, Debug)]
pub struct Effects {
    pub records: Vec<Record>,
    pub sends: Vec<Outgoing>,
    /// Copies for the operational snapshot.
    pub observed: Vec<Envelope>,
    /// The generation advanced in this batch: nothing of it may leave before
    /// the new generation is on disk, under any durability.
    pub gate_all: bool,
    /// The generation counter is exhausted; the engine must stop.
    pub fatal: Option<&'static str>,
}

struct Session {
    credential: Name,
    channels: HashSet<Name>,
    attribution: Option<u32>,
}

/// One claim attribution: the credential and connection a claim was made under.
///
/// | key | attribution number (the claim block's `claimer`) |
/// | --- | --- |
/// | scope | one engine run |
/// | lifetime | while its connection is open or an active claim names it |
/// | bound | open connections + active claims |
/// | cleared by | `release` (last claim gone, connection closed) and `close` |
struct Attribution {
    credential: Name,
    session: Option<SessionId>,
    claims: u32,
}

pub struct State {
    names: Names,
    generation: u32,
    arrival: u32,
    claim_seq: u32,
    next_attribution: u32,
    /// Every claimable event, waiting or claimed, in work-key (FIFO) order.
    queue: BTreeMap<WorkKey, Envelope>,
    /// id → registration, for every event in `queue`.
    ids: HashMap<EventId, WorkKey>,
    /// Waiting (unclaimed) registrations per channel, for subscription backlogs.
    waiting: HashMap<Name, BTreeSet<WorkKey>>,
    /// Claims by deadline. Entries of claims that ended are skipped when popped.
    deadlines: BinaryHeap<Reverse<(u64, WorkKey, ClaimToken)>>,
    subscribers: HashMap<Name, Vec<SessionId>>,
    sessions: HashMap<SessionId, Session>,
    attributions: HashMap<u32, Attribution>,
    id_source: Box<dyn IdSource>,
    grants: Option<Grants>,
    snapshot: Option<SnapshotConfig>,
    backlog_page: usize,
    now: u64,
    swept: bool,
    effects: Effects,
}

fn delete_payload(id: EventId, failure: Option<DeleteFailure>) -> Bytes {
    let mut payload = BytesMut::with_capacity(17);
    payload.put_slice(&id.0);
    if let Some(failure) = failure {
        payload.put_u8(failure as u8);
    }
    payload.freeze()
}

impl State {
    pub fn new(config: &EngineConfig, id_source: Box<dyn IdSource>) -> Self {
        State {
            names: Names::default(),
            generation: 0,
            arrival: 0,
            claim_seq: 0,
            next_attribution: 0,
            queue: BTreeMap::new(),
            ids: HashMap::new(),
            waiting: HashMap::new(),
            deadlines: BinaryHeap::new(),
            subscribers: HashMap::new(),
            sessions: HashMap::new(),
            attributions: HashMap::new(),
            id_source,
            grants: config.grants.clone(),
            snapshot: config.snapshot.clone(),
            backlog_page: config.backlog_page.max(1),
            now: 0,
            swept: false,
            effects: Effects::default(),
        }
    }

    /// Rebuilds the queue from journal records and starts a new generation.
    ///
    /// Returns the records that must be durable before the first answer: the
    /// new generation, and a reclaim (new id, retry + 1) for every event that
    /// was claimed when the server stopped.
    pub fn recover(&mut self, records: Vec<Record>) -> Result<Vec<Record>, &'static str> {
        let mut last_generation = 0u32;
        let mut claimed = BTreeSet::new();
        for record in records {
            match record {
                Record::Register(event) => {
                    let event = self.adopt(event);
                    let key = event.key.ok_or("a journal registration without a work key")?;
                    if let Some(old) = self.queue.insert(key, event.clone()) {
                        self.ids.remove(&old.id);
                    }
                    self.ids.insert(event.id, key);
                    claimed.remove(&key);
                }
                Record::Claim(key) => {
                    if self.queue.contains_key(&key) {
                        claimed.insert(key);
                    }
                }
                Record::Reclaim { key, id, retry } => {
                    if let Some(event) = self.queue.get_mut(&key) {
                        self.ids.remove(&event.id);
                        event.id = id;
                        event.retry = retry;
                        self.ids.insert(id, key);
                        claimed.remove(&key);
                    }
                }
                Record::Remove(key) => {
                    if let Some(event) = self.queue.remove(&key) {
                        self.ids.remove(&event.id);
                    }
                    claimed.remove(&key);
                }
                Record::Generation(generation) => last_generation = last_generation.max(generation),
            }
        }
        self.generation = last_generation.checked_add(1).ok_or("the generation counter is exhausted; the server cannot start")?;
        self.arrival = 0;
        self.claim_seq = 0;
        let mut startup = vec![Record::Generation(self.generation)];
        for key in claimed {
            let id = self.fresh_id();
            let event = self.queue.get_mut(&key).ok_or("lost a recovered registration")?;
            self.ids.remove(&event.id);
            event.id = id;
            event.retry = event.retry.saturating_add(1);
            self.ids.insert(id, key);
            startup.push(Record::Reclaim { key, id, retry: event.retry });
        }
        for (key, event) in &self.queue {
            self.waiting.entry(event.channel.clone()).or_default().insert(*key);
        }
        Ok(startup)
    }

    /// Replaces every name of a decoded envelope with the dictionary's handle.
    fn adopt(&mut self, mut event: Envelope) -> Envelope {
        event.channel = self.names.adopt(&event.channel);
        event.event_type = self.names.adopt(&event.event_type);
        for name in [&mut event.reply, &mut event.principal, &mut event.delegate].into_iter().flatten() {
            *name = self.names.adopt(name);
        }
        event
    }

    pub fn generation(&self) -> u32 {
        self.generation
    }

    /// Claimable events held, waiting or claimed.
    pub fn live(&self) -> usize {
        self.queue.len()
    }

    /// The current state as journal records: what a rewrite writes.
    pub fn snapshot(&self) -> Vec<Record> {
        let mut records = Vec::with_capacity(self.queue.len() + 1);
        records.push(Record::Generation(self.generation));
        let mut claimed = Vec::new();
        for (key, event) in &self.queue {
            records.push(Record::Register(Envelope { claim: None, ..event.clone() }));
            if event.claim.is_some() {
                claimed.push(Record::Claim(*key));
            }
        }
        records.extend(claimed);
        records
    }

    /// Starts a batch at `now` (milliseconds on the server's monotonic clock).
    pub fn begin(&mut self, now: u64) {
        self.now = now;
        self.swept = false;
    }

    /// Ends a batch and hands over what it produced.
    pub fn take(&mut self) -> Effects {
        std::mem::take(&mut self.effects)
    }

    pub fn open(&mut self, session: SessionId, credential: &Name) {
        let credential = self.names.adopt(credential);
        self.sessions.insert(session, Session { credential, channels: HashSet::new(), attribution: None });
    }

    pub fn close(&mut self, session: SessionId) {
        let Some(gone) = self.sessions.remove(&session) else { return };
        for channel in gone.channels {
            self.unsubscribe_from(session, &channel);
        }
        if let Some(number) = gone.attribution {
            if let Some(attribution) = self.attributions.get_mut(&number) {
                attribution.session = None;
                if attribution.claims == 0 {
                    self.attributions.remove(&number);
                }
            }
        }
    }

    fn unsubscribe_from(&mut self, session: SessionId, channel: &Name) {
        if let Some(subscribers) = self.subscribers.get_mut(channel) {
            subscribers.retain(|subscriber| *subscriber != session);
            if subscribers.is_empty() {
                self.subscribers.remove(channel);
            }
        }
    }

    /// Takes a registration off its channel's waiting set, and the set off the
    /// map once it is empty, so one-off channels leave nothing behind.
    fn unwait(&mut self, channel: &Name, key: WorkKey) {
        if let Some(waiting) = self.waiting.get_mut(channel) {
            waiting.remove(&key);
            if waiting.is_empty() {
                self.waiting.remove(channel);
            }
        }
    }

    fn send(&mut self, session: SessionId, message: Outbound, gated: bool) {
        self.effects.sends.push(Outgoing { session, message, gated });
    }

    fn reply(&mut self, session: SessionId, req: u32, reply: Reply, gated: bool) {
        self.send(session, Outbound::Reply { req, reply }, gated);
    }

    fn allowed(&self, credential: &Name, method: Method, channel: &str) -> bool {
        match &self.grants {
            None => true,
            Some(grants) => grants.get(credential.as_str()).is_some_and(|grant| grant.allows(method, channel)),
        }
    }

    fn may_delegate(&self, credential: &Name) -> bool {
        match &self.grants {
            None => true,
            Some(grants) => grants.get(credential.as_str()).is_some_and(|grant| grant.delegate),
        }
    }

    /// Applies one call from `session`. A call from a connection that is no
    /// longer open has nobody to answer and is dropped.
    pub fn call(&mut self, session: SessionId, req: u32, call: Call) {
        let Some(credential) = self.sessions.get(&session).map(|session| session.credential.clone()) else { return };
        let (reply, gated) = match call {
            Call::RegisterClaimable(event) => self.register(&credential, event, Kind::Claimable),
            Call::RegisterDispatch(event) => self.register(&credential, event, Kind::Dispatch),
            Call::Claim { id, worker, lease_ms } => (self.claim(session, &credential, id, worker, lease_ms), false),
            Call::Complete { id, token, follow_ups } => self.complete(&credential, id, token, follow_ups),
            Call::Delete { id, result, tag, key } => self.delete(&credential, id, &result, tag, key),
            Call::Subscribe { channel, after } => (self.subscribe(session, &credential, &channel, after), false),
            Call::Unsubscribe { channel } => {
                let channel = self.names.adopt(&channel);
                if let Some(open) = self.sessions.get_mut(&session) {
                    open.channels.remove(&channel);
                }
                self.unsubscribe_from(session, &channel);
                (Reply::Unsubscribed, false)
            }
        };
        self.reply(session, req, reply, gated);
    }

    /// Checks a registration request (method 1 or 2, or a follow-up) and
    /// resolves its names. Does not assign an id or change the queue.
    fn admit(&mut self, credential: &Name, event: Envelope) -> Result<Envelope, Refusal> {
        if event.claim.is_some() || event.retry != 0 || event.key.is_some() || event.principal.is_some() || event.channel.is_empty() {
            return Err(Refusal::Malformed);
        }
        if !event.id.is_nil() {
            if !event.id.is_uuid() {
                return Err(Refusal::InvalidId);
            }
            if self.ids.contains_key(&event.id) {
                return Err(Refusal::Duplicate);
            }
        }
        let method = match event.kind {
            Kind::Claimable => Method::RegisterClaimable,
            Kind::Dispatch => Method::RegisterDispatch,
        };
        if !self.allowed(credential, method, &event.channel) || (event.delegate.is_some() && !self.may_delegate(credential)) {
            return Err(Refusal::Forbidden);
        }
        let mut event = self.adopt(event);
        event.principal = Some(credential.clone());
        Ok(event)
    }

    fn fresh_id(&mut self) -> EventId {
        loop {
            let id = self.id_source.next();
            if !id.is_nil() && !self.ids.contains_key(&id) {
                return id;
            }
        }
    }

    /// Moves to the next generation, because a sequence ran out.
    fn advance_generation(&mut self) {
        match self.generation.checked_add(1) {
            Some(next) => self.generation = next,
            None => self.effects.fatal = Some("the generation counter is exhausted"),
        }
        self.arrival = 0;
        self.claim_seq = 0;
        self.effects.records.push(Record::Generation(self.generation));
        self.effects.gate_all = true;
    }

    fn next_key(&mut self) -> WorkKey {
        if self.arrival == u32::MAX {
            self.advance_generation();
        }
        let key = WorkKey { generation: self.generation, arrival: self.arrival };
        self.arrival += 1;
        key
    }

    fn next_token(&mut self) -> ClaimToken {
        if self.claim_seq == u32::MAX {
            self.advance_generation();
        }
        let token = ClaimToken { generation: self.generation, seq: self.claim_seq };
        self.claim_seq += 1;
        token
    }

    fn observe(&mut self, event: &Envelope) {
        if self.snapshot.as_ref().is_some_and(|snapshot| snapshot.wants(&event.channel, &event.event_type)) {
            self.effects.observed.push(event.clone());
        }
    }

    fn subscribers_of(&self, channel: &Name) -> Vec<SessionId> {
        self.subscribers.get(channel).cloned().unwrap_or_default()
    }

    fn announce(&mut self, event: &Envelope, gated: bool) {
        let notice = event.notice();
        for session in self.subscribers_of(&event.channel) {
            self.send(session, Outbound::Notice(notice.clone()), gated);
        }
    }

    /// Queues an admitted claimable event (its id already assigned).
    fn enqueue(&mut self, mut event: Envelope, gated: bool) -> WorkKey {
        let key = self.next_key();
        event.key = Some(key);
        self.ids.insert(event.id, key);
        self.waiting.entry(event.channel.clone()).or_default().insert(key);
        self.effects.records.push(Record::Register(event.clone()));
        self.announce(&event, gated);
        self.observe(&event);
        self.queue.insert(key, event);
        key
    }

    /// Hands an admitted dispatch event (its id already assigned) to the
    /// channel's subscribers as of now, and forgets it.
    fn deliver(&mut self, event: Envelope, gated: bool) {
        for session in self.subscribers_of(&event.channel) {
            self.send(session, Outbound::Delivery(event.clone()), gated);
        }
        self.observe(&event);
    }

    /// Methods 1 and 2.
    fn register(&mut self, credential: &Name, event: Envelope, kind: Kind) -> (Reply, bool) {
        self.sweep();
        if event.kind != kind {
            return (Reply::Refused(Refusal::Malformed), false);
        }
        let mut event = match self.admit(credential, event) {
            Ok(event) => event,
            Err(refusal) => return (Reply::Refused(refusal), false),
        };
        if event.id.is_nil() {
            event.id = self.fresh_id();
        }
        let id = event.id;
        match kind {
            Kind::Claimable => {
                let key = self.enqueue(event, true);
                (Reply::Registered { id, key }, true)
            }
            Kind::Dispatch => {
                self.deliver(event, false);
                (Reply::Accepted { id }, false)
            }
        }
    }

    /// Reclaims every claim whose deadline has passed. Runs once per batch,
    /// right before the batch's first external registration.
    fn sweep(&mut self) {
        if self.swept {
            return;
        }
        self.swept = true;
        while let Some(Reverse((deadline, key, token))) = self.deadlines.peek().copied() {
            if deadline > self.now {
                break;
            }
            self.deadlines.pop();
            let current = self.queue.get(&key).and_then(|event| event.claim).map(|claim| claim.token);
            if current == Some(token) {
                self.reclaim(key);
            }
        }
    }

    fn reclaim(&mut self, key: WorkKey) {
        let id = self.fresh_id();
        let Some(event) = self.queue.get_mut(&key) else { return };
        let claim = event.claim.take();
        self.ids.remove(&event.id);
        event.id = id;
        event.retry = event.retry.saturating_add(1);
        let retry = event.retry;
        let channel = event.channel.clone();
        let notice = event.notice();
        self.ids.insert(id, key);
        self.waiting.entry(channel).or_default().insert(key);
        if let Some(claim) = claim {
            self.release(claim.claimer);
        }
        self.effects.records.push(Record::Reclaim { key, id, retry });
        self.announce(&notice, false);
    }

    /// The attribution number for claims made by `session`.
    fn attribution(&mut self, session: SessionId, credential: &Name) -> u32 {
        if let Some(number) = self.sessions.get(&session).and_then(|open| open.attribution) {
            return number;
        }
        loop {
            if self.next_attribution == u32::MAX {
                self.next_attribution = 0;
                self.advance_generation();
            }
            let number = self.next_attribution;
            self.next_attribution += 1;
            if let std::collections::hash_map::Entry::Vacant(free) = self.attributions.entry(number) {
                free.insert(Attribution { credential: credential.clone(), session: Some(session), claims: 0 });
                if let Some(open) = self.sessions.get_mut(&session) {
                    open.attribution = Some(number);
                }
                return number;
            }
        }
    }

    fn release(&mut self, number: u32) {
        if let Some(attribution) = self.attributions.get_mut(&number) {
            attribution.claims = attribution.claims.saturating_sub(1);
            if attribution.claims == 0 && attribution.session.is_none() {
                self.attributions.remove(&number);
            }
        }
    }

    /// Method 3.
    fn claim(&mut self, session: SessionId, credential: &Name, id: EventId, worker: u32, lease_ms: u64) -> Reply {
        let Some(key) = self.ids.get(&id).copied() else { return Reply::Refused(Refusal::Missing) };
        let Some(event) = self.queue.get(&key) else { return Reply::Refused(Refusal::Missing) };
        if event.claim.is_some() {
            return Reply::Refused(Refusal::AlreadyClaimed);
        }
        if !self.allowed(credential, Method::Claim, &event.channel.clone()) {
            return Reply::Refused(Refusal::Forbidden);
        }
        let token = self.next_token();
        let claimer = self.attribution(session, credential);
        if let Some(attribution) = self.attributions.get_mut(&claimer) {
            attribution.claims += 1;
        }
        let deadline = self.now.saturating_add(lease_ms);
        let Some(event) = self.queue.get_mut(&key) else { return Reply::Refused(Refusal::Missing) };
        event.claim = Some(Claim { token, claimer, worker, deadline });
        let answer = event.clone();
        self.unwait(&answer.channel, key);
        self.deadlines.push(Reverse((deadline, key, token)));
        self.effects.records.push(Record::Claim(key));
        Reply::Claimed(answer)
    }

    /// Method 4. Everything is checked before anything changes: either the
    /// event goes and every follow-up is registered, or nothing happens.
    fn complete(&mut self, credential: &Name, id: EventId, token: ClaimToken, follow_ups: Vec<Envelope>) -> (Reply, bool) {
        let refused = |refusal| (Reply::Refused(refusal), false);
        let Some(key) = self.ids.get(&id).copied() else { return refused(Refusal::Missing) };
        let Some(event) = self.queue.get(&key) else { return refused(Refusal::Missing) };
        let Some(claim) = event.claim else { return refused(Refusal::NotClaimed) };
        if claim.token != token {
            return refused(Refusal::StaleToken);
        }
        if self.attributions.get(&claim.claimer).map(|attribution| &attribution.credential) != Some(credential) {
            return refused(Refusal::WrongCredential);
        }
        if !self.allowed(credential, Method::Complete, &event.channel.clone()) {
            return refused(Refusal::Forbidden);
        }
        let mut admitted = Vec::with_capacity(follow_ups.len());
        let mut chosen = HashSet::new();
        for follow_up in follow_ups {
            match self.admit(credential, follow_up) {
                Ok(follow_up) => {
                    if !follow_up.id.is_nil() && !chosen.insert(follow_up.id) {
                        return refused(Refusal::Duplicate);
                    }
                    admitted.push(follow_up);
                }
                Err(refusal) => return refused(refusal),
            }
        }

        let Some(parent) = self.queue.remove(&key) else { return refused(Refusal::Missing) };
        self.ids.remove(&parent.id);
        self.release(claim.claimer);
        self.effects.records.push(Record::Remove(key));
        for mut follow_up in admitted {
            if follow_up.id.is_nil() {
                follow_up.id = loop {
                    let candidate = self.fresh_id();
                    if !chosen.contains(&candidate) {
                        break candidate;
                    }
                };
            }
            match follow_up.kind {
                Kind::Claimable => {
                    self.enqueue(follow_up, true);
                }
                Kind::Dispatch => self.deliver(follow_up, true),
            }
        }
        (Reply::Completed, true)
    }

    /// Method 5.
    fn delete(&mut self, credential: &Name, id: EventId, result: &Name, tag: EventId, key: Option<WorkKey>) -> (Reply, bool) {
        let result = self.names.adopt(result);
        let found = self.ids.get(&id).copied();
        let channel = found.and_then(|found| self.queue.get(&found)).map(|event| event.channel.clone());
        if !self.allowed(credential, Method::Delete, &result) || channel.as_ref().is_some_and(|channel| !self.allowed(credential, Method::Delete, channel)) {
            return (Reply::Refused(Refusal::Forbidden), false);
        }
        let failure = match found {
            None => Some(DeleteFailure::Missing),
            Some(found) if key.is_some_and(|key| key != found) => Some(DeleteFailure::OtherRegistration),
            Some(found) if self.queue.get(&found).is_some_and(|event| event.claim.is_some()) => Some(DeleteFailure::Claimed),
            Some(_) => None,
        };
        if let (None, Some(found)) = (failure, found) {
            if let Some(event) = self.queue.remove(&found) {
                self.ids.remove(&event.id);
                self.unwait(&event.channel, found);
            }
            self.effects.records.push(Record::Remove(found));
        }
        let removed = failure.is_none();
        let event_type = self.names.intern(if removed { DELETED } else { DELETE_FAILED });
        let notice = Envelope { id: self.fresh_id(), principal: Some(credential.clone()), correlation: Some(tag), ..Envelope::dispatch(result, event_type, delete_payload(id, failure)) };
        self.deliver(notice, removed);
        (Reply::Deleted { removed }, removed)
    }

    /// Method 6.
    fn subscribe(&mut self, session: SessionId, credential: &Name, channel: &Name, after: Option<WorkKey>) -> Reply {
        if !self.allowed(credential, Method::Subscribe, channel) {
            return Reply::Refused(Refusal::Forbidden);
        }
        let channel = self.names.adopt(channel);
        if let Some(open) = self.sessions.get_mut(&session) {
            if open.channels.insert(channel.clone()) {
                self.subscribers.entry(channel.clone()).or_default().push(session);
            }
        }
        let mut backlog = Vec::new();
        let mut more = false;
        if let Some(waiting) = self.waiting.get(&channel) {
            let start = match after {
                Some(after) => std::ops::Bound::Excluded(after),
                None => std::ops::Bound::Unbounded,
            };
            for key in waiting.range((start, std::ops::Bound::Unbounded)) {
                if backlog.len() == self.backlog_page {
                    more = true;
                    break;
                }
                if let Some(event) = self.queue.get(key) {
                    backlog.push(event.notice());
                }
            }
        }
        Reply::Subscribed { channel, backlog, more }
    }
}

#[cfg(test)]
mod tests;
