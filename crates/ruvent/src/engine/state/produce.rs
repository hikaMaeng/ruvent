//! The production path: admission, registration, deletion, dispatch (2.3).

use super::slots::{Fixed, Index, Place, Slot};
use super::wheel::Due;
use super::State;
use crate::engine::config::Method;
use crate::engine::ops::{DeleteFailure, FaultKind, Outbound, Role, SessionId};
use crate::envelope::{Channel, Digest, Envelope, EventId, Kind, WorkKey, SERVER_TYPE};
use crate::journal::{Location, Record};
use crate::names::Name;
use bytes::{BufMut, Bytes, BytesMut};

fn delete_payload(id: EventId, failure: Option<DeleteFailure>) -> Bytes {
    let mut payload = BytesMut::with_capacity(17);
    payload.put_slice(&id.0);
    payload.put_u8(failure.map_or(0, |failure| failure as u8));
    payload.freeze()
}

impl State {
    /// Checks a registration request or a follow-up. Faults (once per kind)
    /// and returns `None` when it may not be applied.
    pub(super) fn admit(&mut self, session: SessionId, credential: &Name, event: Envelope) -> Option<Envelope> {
        if event.failures != 0 || event.key.is_some() || event.claim.is_some() {
            self.fault(session, FaultKind::Malformed, "a request may not carry failures, a work key or a claim mark");
            return None;
        }
        if !event.channel.is_valid() {
            self.fault(session, FaultKind::Malformed, "the channel is not a UUIDv7");
            return None;
        }
        if event.event_type == SERVER_TYPE {
            self.fault(session, FaultKind::Malformed, "type 0 is the server's own");
            return None;
        }
        if event.kind == Kind::Dispatch && event.not_before.is_some() {
            self.fault(session, FaultKind::Malformed, "not_before is for claimable events");
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
        if !self.allowed(credential, method) {
            self.fault(session, FaultKind::Forbidden, "the credential may not register this kind");
            return None;
        }
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
                self.enqueue(event, None);
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

    /// Queues an admitted claimable event (its id may still be nil) under
    /// `root`. `streamed`: its payload was received to disk there (2.6) and
    /// `event.payload` is empty.
    pub(super) fn enqueue(&mut self, mut event: Envelope, streamed: Option<Location>) -> Index {
        let explicit = !event.id.is_nil();
        if !explicit {
            event.id = self.fresh_id();
        }
        let durable = self.journaled;
        let digest = match streamed {
            Some(at) => at.digest(),
            None => Digest::of(&event.payload),
        };
        let key = self.next_key();
        let not_before = event.not_before.map(|delay| self.now.saturating_add(delay));
        let fixed = Fixed {
            id: event.id,
            key,
            event_type: event.event_type,
            not_before,
            durable,
            registered_at: self.now,
            visible_from: if durable { self.carrying } else { 0 },
            digest: explicit.then_some(digest),
        };
        let slot = Slot { fixed, channel: event.channel, failures: 0, place: Place::Waiting, commit: 0, serial: 0, payload: event.payload, at: streamed };
        let usage = self.usage(&slot);
        let index = self.slots.insert(slot);
        self.account_add(usage);
        let generation = self.slots.generation(index);
        if let Some(at) = streamed {
            // A streamed payload is live in its body segment from now on.
            self.book.live_add(&at);
        }
        let envelope = self.slots.get(index).map(|slot| slot.envelope());
        if let Some(envelope) = envelope {
            if durable {
                let serial = self.serial;
                self.record_for(index, Record::Register { event: envelope.clone(), at: streamed });
                if streamed.is_none() {
                    self.unlocated.push_back((serial, index, generation));
                }
            }
            self.observe(&envelope);
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

    /// Hands a dispatch event to the channel's subscribers and dispatch joins
    /// as of now, and forgets it.
    pub(super) fn deliver(&mut self, event: Envelope, gate: Option<u64>) {
        for session in self.subscribers.of(&event.channel, true) {
            self.send(session, Outbound::Delivery(event.clone()), gate);
        }
        self.observe(&event);
    }

    pub(super) fn observe(&mut self, event: &Envelope) {
        if self.config.snapshot.as_ref().is_some_and(|snapshot| snapshot.wants(event.event_type)) {
            self.effects.observed.push(event.clone());
        }
    }

    /// `Delete`: a waiting (or delayed) event goes; the outcome is a dispatch
    /// event of type 0 on `result` either way.
    pub(super) fn delete(&mut self, session: SessionId, credential: &Name, id: EventId, result: Channel, key: Option<WorkKey>) {
        if !result.is_valid() {
            return self.fault(session, FaultKind::Malformed, "the result channel is not a UUIDv7");
        }
        if !self.allowed(credential, Method::Delete) {
            return self.fault(session, FaultKind::Forbidden, "the credential may not delete");
        }
        let found = self.slots.by_id(&id);
        let failure = match found.and_then(|index| self.slots.get(index).map(|slot| (index, slot))) {
            None => Some(DeleteFailure::Missing),
            Some((_, slot)) if key.is_some_and(|key| key != slot.fixed.key) => Some(DeleteFailure::OtherRegistration),
            Some((_, slot)) if slot.attempt().is_some() => Some(DeleteFailure::Claimed),
            Some(_) => None,
        };
        let mut gate = None;
        if let (None, Some(index)) = (failure, found) {
            if let Some(slot) = self.remove_event(index) {
                if slot.fixed.durable {
                    self.record(Record::Remove(slot.fixed.key));
                    gate = self.strict.then_some(self.carrying);
                }
            }
        }
        let notice = Envelope { id: self.fresh_id(), ..Envelope::dispatch(result, SERVER_TYPE, delete_payload(id, failure)) };
        self.deliver(notice, gate);
    }
}
