//! The production path: admission, registration, deletion, dispatch (2.3).

use super::slots::{Fixed, Index, Place, Slot};
use super::wheel::Due;
use super::State;
use crate::engine::config::Method;
use crate::engine::ops::{DeleteFailure, FaultKind, Outbound, Role, SessionId, DELETED, DELETE_FAILED};
use crate::envelope::{Digest, Envelope, EventId, Kind, WorkKey};
use crate::journal::Record;
use crate::names::Name;
use bytes::{BufMut, Bytes, BytesMut};

fn delete_payload(id: EventId, failure: Option<DeleteFailure>) -> Bytes {
    let mut payload = BytesMut::with_capacity(17);
    payload.put_slice(&id.0);
    if let Some(failure) = failure {
        payload.put_u8(failure as u8);
    }
    payload.freeze()
}

impl State {
    /// Checks a registration request or a follow-up and resolves its names.
    /// Faults (once per kind) and returns `None` when it may not be applied.
    pub(super) fn admit(&mut self, session: SessionId, credential: &Name, event: Envelope) -> Option<Envelope> {
        if event.failures != 0 || event.key.is_some() || event.principal.is_some() || event.channel.is_empty() {
            self.fault(session, FaultKind::Malformed, "a request may not carry failures, a work key or a principal");
            return None;
        }
        if event.kind == Kind::Dispatch && (event.not_before.is_some() || event.expires.is_some() || event.group.is_some()) {
            self.fault(session, FaultKind::Malformed, "timing and group are for claimable events");
            return None;
        }
        if !event.id.is_nil() && !event.id.is_uuid() {
            self.fault(session, FaultKind::InvalidId, "the id is neither nil nor a UUID");
            return None;
        }
        let method = match event.kind {
            Kind::Claimable => Method::RegisterClaimable,
            Kind::Dispatch => Method::RegisterDispatch,
        };
        if !self.allowed(credential, method, &event.channel) || (event.delegate.is_some() && !self.may_delegate(credential)) {
            self.fault(session, FaultKind::Forbidden, "the credential may not register this here");
            return None;
        }
        let mut event = event;
        event.channel = self.names.adopt(&event.channel);
        event.event_type = self.names.adopt(&event.event_type);
        for name in [&mut event.reply, &mut event.delegate].into_iter().flatten() {
            *name = self.names.adopt(name);
        }
        event.principal = Some(credential.clone());
        Some(event)
    }

    /// `Register` from a production connection, or an emit from a work connection.
    pub(super) fn register(&mut self, session: SessionId, role: Role, credential: &Name, event: Envelope) {
        if role == Role::Work && event.kind == Kind::Claimable {
            return self.fault(session, FaultKind::Malformed, "a work connection registers dispatch events only");
        }
        let Some(event) = self.admit(session, credential, event) else { return };
        match event.kind {
            Kind::Dispatch => self.deliver(event, None),
            Kind::Claimable => {
                if !event.id.is_nil() {
                    if let Some(index) = self.slots.by_id(&event.id) {
                        return self.same_id(session, index, &event, FaultKind::IdConflict);
                    }
                }
                let root = event.channel.clone();
                self.pause_notice(session, &root, event.payload.len() as u64);
                self.enqueue(event, root);
            }
        }
    }

    /// An active event already has this id: the same content is a duplicate
    /// and is dropped silently; other content is a conflict (2.3).
    fn same_id(&mut self, session: SessionId, index: Index, event: &Envelope, conflict: FaultKind) {
        let same = self.slots.get(index).is_some_and(|slot| slot.digest() == Digest::of(&event.payload));
        if same {
            self.counters.duplicates += 1;
        } else {
            self.fault(session, conflict, "an active event has this id with other content");
        }
    }

    /// Queues an admitted claimable event (its id may still be nil) under `root`.
    pub(super) fn enqueue(&mut self, mut event: Envelope, root: Name) -> Index {
        let explicit = !event.id.is_nil();
        if !explicit {
            event.id = self.fresh_id();
        }
        let durable = self.journaled && !self.config.channels.settings(&event.channel).volatile;
        let key = self.next_key();
        let not_before = event.not_before.map(|delay| self.now.saturating_add(delay));
        let expires = event.expires.map(|life| self.now.saturating_add(life));
        let fixed = Fixed {
            id: event.id,
            key,
            event_type: event.event_type,
            root: root.clone(),
            reply: event.reply,
            correlation: event.correlation,
            principal: event.principal,
            delegate: event.delegate,
            group: event.group,
            not_before,
            durable,
            registered_at: self.now,
            visible_from: if durable { self.carrying } else { 0 },
            digest: explicit.then(|| Digest::of(&event.payload)),
        };
        let slot = Slot { fixed, channel: event.channel, failures: 0, expires, place: Place::Waiting, commit: 0, serial: 0, payload: event.payload, at: None };
        let cost = self.cost(slot.len());
        let index = self.slots.insert(slot);
        self.account_add(&root, cost);
        let generation = self.slots.generation(index);
        let envelope = self.slots.get(index).map(|slot| slot.envelope());
        if let Some(envelope) = envelope {
            if durable {
                self.record_for(index, Record::Register { event: envelope.clone(), root });
            }
            self.observe(&envelope);
        }
        if let Some(at) = expires {
            self.wheel.expire_at(at, index, generation);
        }
        match not_before {
            Some(until) if until > self.now => {
                if let Some(slot) = self.slots.get_mut(index) {
                    slot.place = Place::Delayed { until };
                }
                self.wheel.wake_at(until, index, generation, Due::Visible);
            }
            _ => self.wait(index),
        }
        index
    }

    /// Hands a dispatch event to the channel's subscribers as of now, and forgets it.
    pub(super) fn deliver(&mut self, event: Envelope, gate: Option<u64>) {
        for session in self.subscribers.of(&event.channel, true) {
            self.send(session, Outbound::Delivery(event.clone()), gate);
        }
        self.observe(&event);
    }

    pub(super) fn observe(&mut self, event: &Envelope) {
        if self.config.snapshot.as_ref().is_some_and(|snapshot| snapshot.wants(&event.channel, &event.event_type)) {
            self.effects.observed.push(event.clone());
        }
    }

    /// `Delete`: a waiting (or delayed, or held) event goes; the outcome is a
    /// dispatch event on `result` either way.
    pub(super) fn delete(&mut self, session: SessionId, credential: &Name, id: EventId, result: &Name, tag: EventId, key: Option<WorkKey>) {
        let result = self.names.adopt(result);
        let found = self.slots.by_id(&id);
        let channel = found.and_then(|index| self.slots.get(index)).map(|slot| slot.channel.clone());
        if !self.allowed(credential, Method::Delete, &result) || channel.as_ref().is_some_and(|channel| !self.allowed(credential, Method::Delete, channel)) {
            return self.fault(session, FaultKind::Forbidden, "the credential may not delete here");
        }
        let failure = match found.and_then(|index| self.slots.get(index).map(|slot| (index, slot))) {
            None => Some(DeleteFailure::Missing),
            Some((_, slot)) if key.is_some_and(|key| key != slot.fixed.key) => Some(DeleteFailure::OtherRegistration),
            Some((_, slot)) if slot.attempt().is_some() => Some(DeleteFailure::Claimed),
            Some(_) => None,
        };
        let mut gate = None;
        if let (None, Some(index)) = (failure, found) {
            if let Some(slot) = self.remove_event(index, true) {
                if slot.fixed.durable {
                    self.record(Record::Remove(slot.fixed.key));
                    gate = self.strict.then_some(self.carrying);
                }
            }
        }
        let event_type = self.names.intern(if failure.is_none() { DELETED } else { DELETE_FAILED });
        let notice = Envelope { id: self.fresh_id(), principal: Some(credential.clone()), correlation: Some(tag), ..Envelope::dispatch(result, event_type, delete_payload(id, failure)) };
        self.deliver(notice, gate);
    }
}
