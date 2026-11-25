//! An event's way out and back: completion, release, lease expiry,
//! isolation, expiry, and the moves between lane, hold lane and wheel (2.4).

use super::lanes::LaneKey;
use super::slots::{Attempt, Index, Place, Slot};
use super::wheel::Due;
use super::State;
use crate::engine::config::Method;
use crate::engine::ops::{FaultKind, Outbound, SessionId};
use crate::envelope::{ClaimToken, Digest, Envelope, EventId, Kind, WorkKey};
use crate::journal::Record;
use crate::names::Name;
use std::collections::HashMap;

impl State {
    /// `Complete`: everything is checked before anything changes. Either the
    /// event goes and every follow-up is registered in the same step, or
    /// nothing happens.
    pub(super) fn complete(&mut self, session: SessionId, credential: &Name, id: EventId, token: ClaimToken, follow_ups: Vec<Envelope>) {
        let Some(index) = self.attempt_for(session, credential, id, token) else { return };
        let Some(channel) = self.slots.get(index).map(|slot| slot.channel.clone()) else { return };
        if !self.allowed(credential, Method::Complete, &channel) {
            return self.fault(session, FaultKind::Forbidden, "the credential may not complete here");
        }
        if follow_ups.len() > self.config.max_follow_ups {
            return self.fault(session, FaultKind::Malformed, "more follow-ups than one completion may register");
        }
        let mut admitted = Vec::with_capacity(follow_ups.len());
        for follow_up in follow_ups {
            let Some(follow_up) = self.admit(session, credential, follow_up) else { return };
            admitted.push(follow_up);
        }
        // A follow-up whose id is active, or repeats a sibling's, is dropped
        // when the content is the same and refuses the completion when not (2.3).
        let mut chosen: HashMap<EventId, Digest> = HashMap::new();
        let mut kept = Vec::with_capacity(admitted.len());
        for follow_up in admitted {
            if follow_up.kind == Kind::Claimable && !follow_up.id.is_nil() {
                let digest = Digest::of(&follow_up.payload);
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
            kept.push(follow_up);
        }

        let before = self.effects.records.len();
        let Some(parent) = self.remove_event(index, kept.is_empty()) else { return };
        if parent.fixed.durable {
            self.record(Record::Remove(parent.fixed.key));
        }
        let children = kept.len() as u64;
        let child_bytes: u64 = kept.iter().map(|event| event.payload.len() as u64).sum();
        let window = self.config.memory.net_growth_window;
        if let Some(lane) = self.lanes.get_mut(&parent.lane()) {
            lane.growth.record(parent.len(), children, child_bytes, window);
        }
        let mut dispatches = Vec::new();
        for follow_up in kept {
            match follow_up.kind {
                Kind::Claimable => {
                    self.enqueue(follow_up, parent.fixed.root.clone());
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

    /// `Release`: the worker gives the event back, visible again after
    /// `delay_ms`. Only `failed` counts against the event (2.4).
    pub(super) fn give_back(&mut self, session: SessionId, credential: &Name, id: EventId, token: ClaimToken, delay_ms: u64, failed: bool) {
        let Some(index) = self.attempt_for(session, credential, id, token) else { return };
        if self.end_attempt(index).is_none() {
            return;
        }
        let Some(slot) = self.slots.get_mut(index) else { return };
        if failed {
            slot.failures = slot.failures.saturating_add(1);
        }
        if failed && self.should_isolate(index) {
            return self.isolate(index);
        }
        let Some(slot) = self.slots.get_mut(index) else { return };
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
    pub(super) fn release_unwritten(&mut self, index: Index, token: ClaimToken) {
        if self.slots.get(index).and_then(|slot| slot.attempt()).map(|attempt| attempt.token) != Some(token) {
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
        if self.should_isolate(index) {
            return self.isolate(index);
        }
        let Some(slot) = self.slots.get(index) else { return };
        let (key, failures, durable) = (slot.fixed.key, slot.failures, slot.fixed.durable);
        if durable {
            self.record_for(index, Record::Reclaim { key, failures });
        }
        self.wait(index);
    }

    /// Ends the attempt at `index`: the join gets its room back, the group is
    /// free. The slot is left with no place; the caller gives it one.
    fn end_attempt(&mut self, index: Index) -> Option<Attempt> {
        let attempt = self.slots.get(index)?.attempt()?.clone();
        self.unclaim(index, &attempt);
        if let Some(slot) = self.slots.get_mut(index) {
            slot.place = Place::Waiting;
        }
        Some(attempt)
    }

    /// Takes an attempt off its join and its group.
    fn unclaim(&mut self, index: Index, attempt: &Attempt) {
        let Some(slot) = self.slots.get(index) else { return };
        let (len, group, channel, lane) = (slot.len(), slot.fixed.group, slot.channel.clone(), slot.lane());
        if let Some(join_id) = &attempt.join {
            if let Some(join) = self.joins.get_mut(join_id) {
                join.claimed = join.claimed.saturating_sub(1);
                join.claimed_bytes = join.claimed_bytes.saturating_sub(len);
                join.attempts.remove(&index);
                self.to_fill.push(join_id.clone());
            }
        }
        if let Some(group) = group {
            if self.config.channels.settings(&channel).serialize_group {
                let key = (channel, group);
                if let Some(count) = self.groups.get_mut(&key) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        self.groups.remove(&key);
                    }
                }
                self.to_offer.push(lane);
            }
        }
    }

    fn should_isolate(&self, index: Index) -> bool {
        self.slots.get(index).is_some_and(|slot| self.config.channels.settings(&slot.channel).isolate_after.is_some_and(|limit| slot.failures >= limit))
    }

    /// Moves the event to `isolated.<channel>` in one step, with its failure
    /// count, root and durability, and the channel's keep time as its expiry (B6).
    fn isolate(&mut self, index: Index) {
        let Some(slot) = self.slots.get(index) else { return };
        let channel = self.config.channels.isolated(&slot.channel);
        let keep = super::millis(self.config.channels.settings(&slot.channel).isolated_keep);
        let channel = self.names.intern(&channel);
        let expires = Some(self.now.saturating_add(keep));
        let generation = self.slots.generation(index);
        let Some(slot) = self.slots.get_mut(index) else { return };
        slot.channel = channel.clone();
        slot.expires = expires;
        let (key, failures, durable) = (slot.fixed.key, slot.failures, slot.fixed.durable);
        if let Some(at) = expires {
            self.wheel.expire_at(at, index, generation);
        }
        if durable {
            self.record_for(index, Record::Isolate { key, channel, failures, expires });
        }
        self.counters.isolated += 1;
        self.wait(index);
    }

    /// An expired event is removed (or isolated, if its channel says so) and
    /// counted; nobody is told (B9).
    pub(super) fn expire(&mut self, index: Index) {
        let Some(slot) = self.slots.get(index) else { return };
        let settings = self.config.channels.settings(&slot.channel);
        if settings.isolate_expired && !slot.channel.starts_with(self.config.channels.isolation_prefix.as_str()) {
            self.take_out(index);
            return self.isolate(index);
        }
        let Some(slot) = self.remove_event(index, true) else { return };
        self.counters.expired += 1;
        if slot.fixed.durable {
            self.record(Record::Remove(slot.fixed.key));
        }
    }

    /// Takes an event out of whatever holds it — lane, hold lane, wheel,
    /// attempt — leaving it in the arena with no place.
    fn take_out(&mut self, index: Index) {
        let Some(slot) = self.slots.get(index) else { return };
        let (place, lane, key, root) = (slot.place.clone(), slot.lane(), slot.fixed.key, slot.fixed.root.clone());
        match place {
            Place::Waiting => {
                if let Some(found) = self.lanes.get_mut(&lane) {
                    found.queue.remove(&key);
                }
                self.tidy(&lane);
            }
            Place::Held => self.unhold(&root, &lane, key),
            Place::Delayed { .. } => {}
            Place::Claimed(attempt) => self.unclaim(index, &attempt),
        }
    }

    /// Takes an event out of the ledger wherever it is. Records nothing.
    /// `ended`: its lineage ends here (a completion without follow-ups, a
    /// deletion, an expiry), counted before the root's share goes down.
    pub(super) fn remove_event(&mut self, index: Index, ended: bool) -> Option<Slot> {
        self.take_out(index);
        let slot = self.slots.remove(index)?;
        if ended {
            if let Some(root) = self.roots.get_mut(&slot.fixed.root) {
                root.ended += 1;
            }
        }
        let cost = self.cost(slot.len());
        self.account_remove(&slot.fixed.root, cost);
        Some(slot)
    }

    /// Puts the event in its lane: waiting, announced, offered. An event that
    /// expired meanwhile is expired instead.
    pub(super) fn wait(&mut self, index: Index) {
        let Some(slot) = self.slots.get(index) else { return };
        if slot.expires.is_some_and(|at| at <= self.now) {
            return self.expire(index);
        }
        let (lane, key, channel) = (slot.lane(), slot.fixed.key, slot.channel.clone());
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
                if self.awaiting.back() != Some(&(commit, lane.clone())) {
                    self.awaiting.push_back((commit, lane));
                }
            }
            None => self.to_offer.push(lane),
        }
    }

    /// Moves a waiting event to its root's hold lane.
    pub(super) fn hold(&mut self, index: Index) {
        let Some(slot) = self.slots.get(index) else { return };
        let (lane, key, root) = (slot.lane(), slot.fixed.key, slot.fixed.root.clone());
        if let Some(found) = self.lanes.get_mut(&lane) {
            found.queue.remove(&key);
        }
        if let Some(slot) = self.slots.get_mut(index) {
            slot.place = Place::Held;
        }
        self.holds.entry((root.clone(), lane.clone())).or_default().insert(key, index);
        if let Some(paused) = self.roots.get_mut(&root).and_then(|root| root.paused.as_mut()) {
            paused.holds.insert(lane);
        }
    }

    pub(super) fn unhold(&mut self, root: &Name, lane: &LaneKey, key: WorkKey) {
        let hold = (root.clone(), lane.clone());
        if let Some(held) = self.holds.get_mut(&hold) {
            held.remove(&key);
            if held.is_empty() {
                self.holds.remove(&hold);
            }
        }
    }

    /// Puts a root's held events back in their lanes, ahead of what came
    /// after them (they keep their work keys).
    pub(super) fn release_holds(&mut self, root: &Name) {
        let lanes: Vec<LaneKey> = self.roots.get_mut(root).and_then(|root| root.paused.as_mut()).map(|paused| paused.holds.drain().collect()).unwrap_or_default();
        for lane in lanes {
            let Some(held) = self.holds.remove(&(root.clone(), lane.clone())) else { continue };
            for (key, index) in held {
                if let Some(slot) = self.slots.get_mut(index) {
                    slot.place = Place::Waiting;
                }
                self.lane_mut(&lane).queue.insert(key, index);
            }
            self.to_offer.push(lane);
        }
    }

    /// Leases that ran out and delays that are over.
    pub(super) fn process_due(&mut self) {
        while let Some(entry) = self.wheel.pop_due(self.now) {
            let Some(slot) = self.slots.current(entry.index, entry.generation) else { continue };
            let now = self.now;
            let reclaim = match (entry.due, &slot.place) {
                (Due::Lease(token), Place::Claimed(attempt)) if attempt.token == token && attempt.until <= now => Some(true),
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

    /// Expiries collected from the quiet side of the wheel: no timer wakes for
    /// them, the batch that runs anyway does (2.4).
    pub(super) fn purge_expired(&mut self) {
        while let Some((at, index, generation)) = self.wheel.pop_expired(self.now) {
            let Some(slot) = self.slots.current(index, generation) else { continue };
            // A claimed event is checked when it comes back.
            if slot.expires == Some(at) && slot.attempt().is_none() {
                self.expire(index);
            }
        }
    }
}
