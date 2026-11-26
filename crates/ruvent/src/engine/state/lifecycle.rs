//! An event's way out and back: completion, release, lease expiry, and the
//! moves between lane and wheel (2.4). The server counts failures and never
//! acts on the count: what to do after so many is the worker's (requirement 6).

use super::slots::{Attempt, Index, Place, Slot};
use super::wheel::Due;
use super::State;
use crate::engine::config::Method;
use crate::engine::ops::{FaultKind, Outbound, SessionId};
use crate::envelope::{Digest, Envelope, EventId, Kind, WorkerId};
use crate::journal::{Location, Record, Spooled};
use crate::names::Name;
use std::collections::HashMap;

impl State {
    /// `Complete`: everything is checked before anything changes. Either the
    /// event goes and every follow-up is registered in the same step, or
    /// nothing happens. `spooled` gives the follow-ups whose payload the
    /// connection received to disk (2.6), by position.
    pub(super) fn complete(&mut self, session: SessionId, credential: &Name, id: EventId, worker: WorkerId, follow_ups: Vec<Envelope>, spooled: Vec<(u32, Spooled)>) {
        let Some(index) = self.attempt_for(session, id, worker) else { return };
        if !self.allowed(credential, Method::Complete) {
            return self.fault(session, FaultKind::Forbidden, "the credential may not complete");
        }
        if follow_ups.len() > self.config.max_follow_ups {
            return self.fault(session, FaultKind::Malformed, "more follow-ups than one completion may register");
        }
        let mut streamed: HashMap<usize, Location> = HashMap::with_capacity(spooled.len());
        for (position, body) in &spooled {
            let position = *position as usize;
            if position >= follow_ups.len() || follow_ups[position].kind != Kind::Claimable || !follow_ups[position].payload.is_empty() || streamed.insert(position, body.at).is_some() {
                return self.fault(session, FaultKind::Malformed, "a streamed body names no claimable follow-up without a payload");
            }
        }
        let mut admitted = Vec::with_capacity(follow_ups.len());
        for (position, follow_up) in follow_ups.into_iter().enumerate() {
            let Some(follow_up) = self.admit(session, credential, follow_up) else { return };
            admitted.push((follow_up, streamed.get(&position).copied()));
        }
        // A follow-up whose id is active, or repeats a sibling's, is dropped
        // when the content is the same and refuses the completion when not (2.3).
        let mut chosen: HashMap<EventId, Digest> = HashMap::new();
        let mut kept = Vec::with_capacity(admitted.len());
        for (follow_up, body) in admitted {
            if follow_up.kind == Kind::Claimable && !follow_up.id.is_nil() {
                let digest = body.map_or_else(|| Digest::of(&follow_up.payload), |at| at.digest());
                // The parent leaves in this step, so its own id is free for a follow-up.
                if let Some(active) = self.slots.by_id(&follow_up.id).filter(|active| *active != index) {
                    if self.slots.get(active).is_some_and(|slot| slot.digest() == digest) {
                        continue;
                    }
                    return self.fault(session, FaultKind::FollowUpConflict, "a follow-up's id is active with other content");
                }
                match chosen.insert(follow_up.id, digest) {
                    Some(earlier) if earlier == digest => continue,
                    Some(_) => return self.fault(session, FaultKind::FollowUpConflict, "two follow-ups share an id with different content"),
                    None => {}
                }
            }
            kept.push((follow_up, body));
        }

        let before = self.effects.records.len();
        let Some(parent) = self.remove_event(index) else { return };
        if parent.fixed.durable {
            self.record(Record::Remove(parent.fixed.key));
        }
        let mut dispatches = Vec::new();
        for (follow_up, body) in kept {
            match follow_up.kind {
                Kind::Claimable => {
                    self.enqueue(follow_up, body);
                }
                Kind::Dispatch => dispatches.push(follow_up),
            }
        }
        // Dispatch follow-ups are seen only once the completion is (Strict).
        let gate = (self.strict && self.effects.records.len() > before).then_some(self.carrying);
        for event in dispatches {
            self.deliver(event, gate);
        }
    }

    /// `Release`: the worker uuid leaves the event, which waits again (after
    /// `delay_ms`); with `failed`, a failure report, its failure count rises
    /// first. Another worker may then claim it.
    pub(super) fn give_back(&mut self, session: SessionId, credential: &Name, id: EventId, worker: WorkerId, delay_ms: u64, failed: bool) {
        let Some(index) = self.attempt_for(session, id, worker) else { return };
        if !self.allowed(credential, Method::Complete) {
            return self.fault(session, FaultKind::Forbidden, "the credential may not release");
        }
        if self.end_attempt(index).is_none() {
            return;
        }
        let Some(slot) = self.slots.get_mut(index) else { return };
        if failed {
            slot.failures = slot.failures.saturating_add(1);
        }
        let (key, failures, durable) = (slot.fixed.key, slot.failures, slot.fixed.durable);
        let visible_at = (delay_ms > 0).then(|| self.now.saturating_add(delay_ms));
        if let Some(until) = visible_at {
            slot.place = Place::Delayed { until };
            let generation = self.slots.generation(index);
            self.wheel.wake_at(until, index, generation, Due::Visible);
        }
        if durable {
            self.record_for(index, Record::Release { key, failures, visible_at });
        }
        if visible_at.is_none() {
            self.wait(index);
        }
    }

    /// A closed connection's assignment that was never fully written goes back
    /// at once, its failure count unchanged (2.4).
    pub(super) fn release_unwritten(&mut self, index: Index) {
        if self.slots.get(index).and_then(|slot| slot.attempt()).is_none() {
            return;
        }
        if self.end_attempt(index).is_none() {
            return;
        }
        let Some(slot) = self.slots.get(index) else { return };
        let (key, failures, durable) = (slot.fixed.key, slot.failures, slot.fixed.durable);
        if durable {
            self.record_for(index, Record::Release { key, failures, visible_at: None });
        }
        self.wait(index);
    }

    /// The lease ran out: the event waits again in its place, one failure
    /// more, and the join's window closes (2.4).
    fn reclaim(&mut self, index: Index) {
        let Some(attempt) = self.end_attempt(index) else { return };
        if let Some(join) = attempt.join.as_ref().and_then(|join| self.joins.get_mut(join)) {
            join.count = 0;
        }
        self.counters.reclaimed += 1;
        let Some(slot) = self.slots.get_mut(index) else { return };
        slot.failures = slot.failures.saturating_add(1);
        let (key, failures, durable) = (slot.fixed.key, slot.failures, slot.fixed.durable);
        if durable {
            self.record_for(index, Record::Reclaim { key, failures });
        }
        self.wait(index);
    }

    /// Ends the attempt at `index`: the join gets its room back. The slot is left with no place; the caller gives it one.
    fn end_attempt(&mut self, index: Index) -> Option<Attempt> {
        let attempt = self.slots.get(index)?.attempt()?.clone();
        self.unclaim(index, &attempt);
        if let Some(slot) = self.slots.get_mut(index) {
            slot.place = Place::Waiting;
        }
        Some(attempt)
    }

    /// Takes an attempt off its join.
    fn unclaim(&mut self, index: Index, attempt: &Attempt) {
        let Some(len) = self.slots.get(index).map(Slot::len) else { return };
        if let Some(join_id) = &attempt.join {
            if let Some(join) = self.joins.get_mut(join_id) {
                join.claimed = join.claimed.saturating_sub(1);
                join.claimed_bytes = join.claimed_bytes.saturating_sub(len);
                join.attempts.remove(&index);
                self.to_fill.push(*join_id);
            }
        }
    }

    /// Takes an event out of whatever holds it — lane, wheel, attempt —
    /// leaving it in the arena with no place.
    fn take_out(&mut self, index: Index) {
        let Some(slot) = self.slots.get(index) else { return };
        let (place, lane, key) = (slot.place.clone(), slot.lane(), slot.fixed.key);
        match place {
            Place::Waiting => {
                if let Some(found) = self.lanes.get_mut(&lane) {
                    found.queue.remove(&key);
                }
                self.tidy(&lane);
            }
            Place::Delayed { .. } => {}
            Place::Claimed(attempt) => self.unclaim(index, &attempt),
        }
    }

    /// Takes an event out of the ledger wherever it is. Records nothing.
    pub(super) fn remove_event(&mut self, index: Index) -> Option<Slot> {
        self.take_out(index);
        let slot = self.slots.remove(index)?;
        let usage = self.usage(&slot);
        self.account_remove(usage);
        self.free_payload(slot.at, slot.fixed.durable);
        Some(slot)
    }

    /// Puts the event in its lane: waiting, announced, offered.
    pub(super) fn wait(&mut self, index: Index) {
        let Some(slot) = self.slots.get(index) else { return };
        let (lane, key, channel) = (slot.lane(), slot.fixed.key, slot.channel);
        let notice = slot.envelope().notice();
        if let Some(slot) = self.slots.get_mut(index) {
            slot.place = Place::Waiting;
        }
        self.lane_mut(&lane).queue.insert(key, index);
        let gate = self.gate_for(index);
        for session in self.subscribers.of(&channel, false) {
            self.send(session, Outbound::Notice(notice.clone()), gate);
        }
        match gate {
            Some(commit) => {
                if self.awaiting.back() != Some(&(commit, lane)) {
                    self.awaiting.push_back((commit, lane));
                }
            }
            None => self.to_offer.push(lane),
        }
    }

    /// Leases that ran out and delays that are over.
    pub(super) fn process_due(&mut self) {
        while let Some(entry) = self.wheel.pop_due(self.now) {
            let Some(slot) = self.slots.current(entry.index, entry.generation) else { continue };
            let now = self.now;
            let reclaim = match (entry.due, &slot.place) {
                (Due::Lease, Place::Claimed(attempt)) if attempt.until <= now => Some(true),
                (Due::Visible, Place::Delayed { until }) if *until <= now => Some(false),
                _ => None,
            };
            match reclaim {
                Some(true) => self.reclaim(entry.index),
                Some(false) => self.wait(entry.index),
                None => {}
            }
        }
    }
}
