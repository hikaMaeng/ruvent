//! The consumption path: joins, windows, round-robin assignment, explicit
//! claims and extensions (2.4).

use super::lanes::{Join, Lane, LaneKey};
use super::slots::{Attempt, Index, JoinId, Place};
use super::wheel::Due;
use super::State;
use crate::engine::config::Method;
use crate::engine::ops::{Assignment, FaultKind, Outbound, Refusal, SessionId};
use crate::envelope::{Channel, EventId, WorkKey, WorkerId};
use crate::journal::Record;
use crate::names::Name;
use std::collections::HashSet;

impl State {
    /// The lane, created with its members if it is new.
    pub(super) fn lane_mut(&mut self, key: &LaneKey) -> &mut Lane {
        if !self.lanes.contains_key(key) {
            let mut members: Vec<(u64, JoinId)> = Vec::new();
            for join_id in self.channel_joins.get(&key.0).into_iter().flatten() {
                if let Some(join) = self.joins.get_mut(join_id) {
                    if join.selects(key) {
                        join.lanes.push(*key);
                        members.push((join.order, *join_id));
                    }
                }
            }
            members.sort();
            self.lanes.insert(*key, Lane { members: members.into_iter().map(|(_, join)| join).collect(), ..Lane::default() });
            self.typed_lanes.entry(key.1).or_default().insert(*key);
        }
        self.lanes.entry(*key).or_default()
    }

    /// Drops a lane that has neither events nor members.
    pub(super) fn tidy(&mut self, key: &LaneKey) {
        if self.lanes.get(key).is_some_and(Lane::is_idle) {
            self.lanes.remove(key);
            if let Some(lanes) = self.typed_lanes.get_mut(&key.1) {
                lanes.remove(key);
                if lanes.is_empty() {
                    self.typed_lanes.remove(&key.1);
                }
            }
        }
    }

    /// The joins that may take from the lane, in join order: its channel
    /// joins, then the joins without a channel that select it.
    pub(super) fn candidates(&self, key: &LaneKey) -> Vec<JoinId> {
        let mut found: Vec<JoinId> = self.lanes.get(key).map(|lane| lane.members.clone()).unwrap_or_default();
        for id in self.type_joins.get(&key.1).into_iter().flatten() {
            if self.joins.get(id).is_some_and(|join| join.selects(key)) {
                found.push(*id);
            }
        }
        found
    }

    /// The lanes the join may take from (see [`State::candidates`]).
    fn lanes_of(&self, id: &JoinId) -> Vec<LaneKey> {
        let Some(join) = self.joins.get(id) else { return Vec::new() };
        if join.channel.is_some() {
            return join.lanes.clone();
        }
        let mut found = Vec::new();
        for kind in &join.types {
            found.extend(self.typed_lanes.get(kind).into_iter().flatten());
        }
        found
    }

    /// The connection's join of `worker` that selects the lane, if any (at most one, `Overlap`).
    fn join_for(&self, session: SessionId, lane: &LaneKey, worker: &WorkerId) -> Option<JoinId> {
        let open = self.sessions.get(&session)?;
        open.joins.iter().map(|req| (session, *req)).find(|id| self.joins.get(id).is_some_and(|join| join.worker == *worker && join.selects(lane)))
    }

    /// `Join`: needs the claim grant (2.7) and a worker uuid (never nil). The
    /// channel, if any, must be a UUIDv7.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn join(&mut self, session: SessionId, credential: &Name, req: u32, worker: WorkerId, channel: Option<Channel>, types: Vec<u16>, lease_ms: u64, dispatch: bool) {
        let refuse = |state: &mut State, reason| state.send(session, Outbound::Refused { req, reason }, None);
        if worker == [0; 16] || (channel.is_none() && (types.is_empty() || dispatch)) || channel.is_some_and(|channel| !channel.is_valid()) {
            return refuse(self, Refusal::Malformed);
        }
        let mut types = types;
        types.sort_unstable();
        types.dedup();
        if !self.allowed(credential, Method::Claim) {
            return refuse(self, Refusal::Forbidden);
        }
        let mine: Vec<JoinId> = self.sessions.get(&session).map(|open| open.joins.iter().map(|join| (session, *join)).collect()).unwrap_or_default();
        let same = mine.iter().find(|id| self.joins.get(*id).is_some_and(|join| join.is(&channel, &types))).copied();
        if mine.iter().any(|id| Some(id) != same.as_ref() && self.joins.get(id).is_some_and(|join| join.overlaps(&channel, &types))) {
            return refuse(self, Refusal::Overlap);
        }
        let id: JoinId = (session, req);
        // A name names one join: another selection may not take a name in use,
        // or the old join would stay in the lanes and the type lists unnamed.
        if mine.contains(&id) && same != Some(id) {
            return refuse(self, Refusal::Malformed);
        }
        // A join that repeats a selection replaces its terms and name, keeping what it holds.
        let held = same.and_then(|old| self.unlink(&old).map(|join| (old, join))).map(|(old, join)| {
            for index in &join.attempts {
                if let Some(attempt) = self.slots.get_mut(*index).and_then(|slot| match &mut slot.place {
                    Place::Claimed(attempt) => Some(attempt),
                    _ => None,
                }) {
                    if attempt.join.as_ref() == Some(&old) {
                        attempt.join = Some(id);
                    }
                }
            }
            (join.count, join.bytes, join.claimed, join.claimed_bytes, join.attempts, join.order)
        });
        let (count, bytes, claimed, claimed_bytes, attempts, order) = held.unwrap_or_else(|| {
            let order = self.next_order;
            self.next_order += 1;
            (0, 0, 0, 0, HashSet::new(), order)
        });
        let mut join = Join { channel, types, worker, lease_ms, count, bytes, claimed, claimed_bytes, signalled: false, order, dispatch, lanes: Vec::new(), attempts };
        if let Some(channel) = &channel {
            let covered: Vec<LaneKey> = self.lanes.keys().filter(|key| join.selects(key)).cloned().collect();
            for key in &covered {
                if let Some(lane) = self.lanes.get_mut(key) {
                    let at = lane.members.iter().position(|member| self.joins.get(member).is_some_and(|other| other.order > order)).unwrap_or(lane.members.len());
                    lane.members.insert(at, id);
                }
            }
            join.lanes = covered;
            insert_ordered(self.channel_joins.entry(*channel).or_default(), &self.joins, id, order);
            if dispatch {
                self.subscribers.dispatch.entry(*channel).or_default().push(session);
            }
        } else {
            for kind in join.types.clone() {
                insert_ordered(self.type_joins.entry(kind).or_default(), &self.joins, id, order);
            }
        }
        self.joins.insert(id, join);
        if let Some(open) = self.sessions.get_mut(&session) {
            open.joins.push(req);
        }
        self.send(session, Outbound::Joined { req }, None);
        self.to_fill.push(id);
    }

    /// `Window`: the absolute number of assignments and bytes this join may hold (2.4).
    pub(super) fn window(&mut self, session: SessionId, join: u32, count: u32, bytes: u64) {
        let id: JoinId = (session, join);
        let Some(join) = self.joins.get_mut(&id) else {
            return self.fault(session, FaultKind::Malformed, "a window for a join this connection does not have");
        };
        join.count = count;
        join.bytes = bytes;
        join.signalled = false;
        // A window said while full (0 included) and work it could take
        // already waits is told `Pending` now, not only when more work
        // arrives: a worker that joins a server with a backlog hears it
        // (2.4, at most once per `Window`).
        if !join.has_room() {
            self.signal_waiting(&id);
        }
        self.to_fill.push(id);
    }

    /// Leaves a join because its connection closed.
    pub(super) fn part(&mut self, session: SessionId, join: u32, unwritten: &[EventId]) {
        let id: JoinId = (session, join);
        let Some(join) = self.unlink(&id) else { return };
        let grace = super::millis(self.config.reconnect_grace);
        for index in join.attempts {
            let generation = self.slots.generation(index);
            let now = self.now;
            let Some(slot) = self.slots.get_mut(index) else { continue };
            let id = slot.fixed.id;
            let Place::Claimed(attempt) = &mut slot.place else { continue };
            attempt.join = None;
            if unwritten.contains(&id) {
                // Never fully written: the worker cannot have started it.
                self.release_unwritten(index);
            } else {
                attempt.until = attempt.until.min(now.saturating_add(grace));
                let until = attempt.until;
                self.wheel.wake_at(until, index, generation, Due::Lease);
            }
        }
    }

    /// Gives a join that has room the earliest eligible head of its lanes,
    /// until it is full or nothing fits (2.4). A join without a channel looks
    /// at every lane of its types: the cost is one head per such lane, and a
    /// lane only it takes exists only while it has waiting events.
    pub(super) fn fill(&mut self, id: &JoinId) {
        loop {
            let Some(join) = self.joins.get(id) else { return };
            if !join.has_room() {
                return;
            }
            let lanes = self.lanes_of(id);
            let mut best: Option<(WorkKey, Index, LaneKey)> = None;
            for lane in lanes {
                if let Some((key, index, len)) = self.head(&lane) {
                    let fits = self.joins.get(id).is_some_and(|join| join.fits(len));
                    if fits && best.as_ref().is_none_or(|(best, _, _)| key < *best) {
                        best = Some((key, index, lane));
                    }
                }
            }
            let Some((key, index, lane)) = best else { return };
            self.assign(id, &lane, key, index);
        }
    }

    /// Assigns the lane's eligible heads to members with room, round robin in
    /// join order. Work left over with every member full raises `Pending`.
    pub(super) fn offer(&mut self, lane: &LaneKey) {
        loop {
            let Some((key, index, len)) = self.head(lane) else { return };
            let Some(member) = self.pick(lane, len) else {
                return self.signal_pending(lane);
            };
            self.assign(&member, lane, key, index);
        }
    }

    /// The next member with room for `len` bytes, starting at the cursor.
    fn pick(&mut self, lane: &LaneKey, len: u64) -> Option<JoinId> {
        let found = self.lanes.get(lane)?;
        let cursor = found.cursor;
        let candidates = self.candidates(lane);
        let count = candidates.len();
        for step in 0..count {
            let at = (cursor + step) % count;
            let member = &candidates[at];
            if self.joins.get(member).is_some_and(|join| join.fits(len)) {
                let member = *member;
                if let Some(found) = self.lanes.get_mut(lane) {
                    found.cursor = (at + 1) % count;
                }
                return Some(member);
            }
        }
        None
    }

    /// `Pending` to one join whose lanes have an eligible head.
    fn signal_waiting(&mut self, id: &JoinId) {
        let Some(join) = self.joins.get(id) else { return };
        if join.signalled {
            return;
        }
        for lane in self.lanes_of(id) {
            if self.head(&lane).is_some() {
                if let Some(join) = self.joins.get_mut(id) {
                    join.signalled = true;
                }
                self.send(id.0, Outbound::Pending { join: id.1 }, None);
                return;
            }
        }
    }

    fn signal_pending(&mut self, lane: &LaneKey) {
        for member in self.candidates(lane) {
            if let Some(join) = self.joins.get_mut(&member) {
                if !join.signalled {
                    join.signalled = true;
                    self.send(member.0, Outbound::Pending { join: member.1 }, None);
                }
            }
        }
    }

    /// The first event of the lane that may be assigned now: its head, or,
    /// under Strict, nothing while the head's registration is not on disk —
    /// the rest of the lane is newer (2.7).
    pub(super) fn head(&self, key: &LaneKey) -> Option<(WorkKey, Index, u64)> {
        let (&work_key, &index) = self.lanes.get(key)?.queue.iter().next()?;
        let slot = self.slots.get(index)?;
        if self.strict && slot.fixed.durable && slot.fixed.visible_from > self.durable {
            return None;
        }
        Some((work_key, index, slot.len()))
    }

    /// Hands the event to the join: one step for assignment, claim and delivery.
    fn assign(&mut self, id: &JoinId, lane: &LaneKey, key: WorkKey, index: Index) {
        if let Some(found) = self.lanes.get_mut(lane) {
            found.queue.remove(&key);
        }
        // A lane only joins without a channel take goes once it is empty.
        self.tidy(lane);
        let Some((lease_ms, worker)) = self.joins.get(id).map(|join| (join.lease_ms, join.worker)) else { return };
        let until = self.now.saturating_add(lease_ms);
        let attempt = Attempt { lease_ms, until, worker, join: Some(*id) };
        let Some(len) = self.start(index, attempt) else { return };
        let more = {
            let Some(join) = self.joins.get_mut(id) else { return };
            join.claimed += 1;
            join.claimed_bytes += len;
            join.attempts.insert(index);
            let full = !join.has_room();
            let lanes = if full { self.lanes_of(id) } else { Vec::new() };
            let more = full && lanes.iter().any(|lane| self.lanes.get(lane).is_some_and(|lane| !lane.queue.is_empty()));
            if more {
                if let Some(join) = self.joins.get_mut(id) {
                    join.signalled = true;
                }
            }
            more
        };
        if let Some((event, cold)) = self.slots.get(index).map(|slot| (slot.envelope(), slot.cold())) {
            self.send_event(id.0, Outbound::Assign(Assignment { event, lease_ms, until, more }), None, index);
            if cold {
                self.read_ahead(lane);
            }
        }
    }

    /// Marks the event claimed by `attempt`: its worker uuid, the lease on the
    /// wheel, `Claim` journaled. Returns the payload length.
    fn start(&mut self, index: Index, attempt: Attempt) -> Option<u64> {
        let generation = self.slots.generation(index);
        let slot = self.slots.get_mut(index)?;
        let (until, lease_ms, worker) = (attempt.until, attempt.lease_ms, attempt.worker);
        slot.place = Place::Claimed(attempt);
        let (len, durable, key) = (slot.len(), slot.fixed.durable, slot.fixed.key);
        self.wheel.wake_at(until, index, generation, Due::Lease);
        if durable {
            self.record_for(index, Record::Claim { key, lease_ms, worker });
        }
        Some(len)
    }

    /// Explicit `Claim{id}`: the event with this id, whichever lane it waits
    /// in, if it is visible. Under Strict the answer waits for its
    /// registration to be on disk (2.7).
    pub(super) fn claim(&mut self, session: SessionId, credential: &Name, req: u32, id: EventId, worker: WorkerId, lease_ms: u64) {
        let refuse = |state: &mut State, reason| state.send(session, Outbound::Refused { req, reason }, None);
        if worker == [0; 16] {
            return refuse(self, Refusal::Malformed);
        }
        let Some(index) = self.slots.by_id(&id) else { return refuse(self, Refusal::Missing) };
        let Some(slot) = self.slots.get(index) else { return refuse(self, Refusal::Missing) };
        if !self.allowed(credential, Method::Claim) {
            return refuse(self, Refusal::Forbidden);
        }
        match slot.place {
            Place::Claimed(_) => return refuse(self, Refusal::AlreadyClaimed),
            Place::Delayed { .. } => return refuse(self, Refusal::Missing),
            Place::Waiting => {}
        }
        let (lane, key) = (slot.lane(), slot.fixed.key);
        if let Some(found) = self.lanes.get_mut(&lane) {
            found.queue.remove(&key);
        }
        let join = self.join_for(session, &lane, &worker);
        let until = self.now.saturating_add(lease_ms);
        let Some(len) = self.start(index, Attempt { lease_ms, until, worker, join }) else { return };
        if let Some(join) = join.and_then(|join| self.joins.get_mut(&join)) {
            join.claimed += 1;
            join.claimed_bytes += len;
            join.attempts.insert(index);
        }
        self.tidy(&lane);
        let gate = self.gate_for(index);
        if let Some(event) = self.slots.get(index).map(|slot| slot.envelope()) {
            self.send_event(session, Outbound::Claimed { req, assignment: Assignment { event, lease_ms, until, more: false } }, gate, index);
        }
    }

    /// The claimed event `id` if `worker` holds it: both must match. A report
    /// that matches nothing — the claim ran out and went elsewhere, or the
    /// event is gone — is late or stale: counted, not told (2.7). A nil
    /// worker is a fault: every claim names one.
    pub(super) fn attempt_for(&mut self, session: SessionId, id: EventId, worker: WorkerId) -> Option<Index> {
        if worker == [0; 16] {
            self.fault(session, FaultKind::Malformed, "a report on a claim must name the worker uuid");
            return None;
        }
        let current = self.slots.by_id(&id).and_then(|index| self.slots.get(index).and_then(|slot| slot.attempt()).map(|attempt| (index, attempt.worker)));
        match current {
            Some((index, holder)) if holder == worker => Some(index),
            _ => {
                self.counters.stale += 1;
                None
            }
        }
    }

    /// `Extend`: re-arms the lease at its length from now, and moves the
    /// attempt to this connection's join that selects its lane, if it has one (2.4).
    /// Never journaled: a restart re-arms every lease anyway.
    pub(super) fn extend(&mut self, session: SessionId, id: EventId, worker: WorkerId) {
        let Some(index) = self.attempt_for(session, id, worker) else { return };
        let generation = self.slots.generation(index);
        let now = self.now;
        let Some(slot) = self.slots.get_mut(index) else { return };
        let len = slot.len();
        let lane = slot.lane();
        let Some(target) = self.join_for(session, &lane, &worker) else {
            let Some(Place::Claimed(attempt)) = self.slots.get_mut(index).map(|slot| &mut slot.place) else { return };
            attempt.until = now.saturating_add(attempt.lease_ms);
            let until = attempt.until;
            self.wheel.wake_at(until, index, generation, Due::Lease);
            return;
        };
        let Some(slot) = self.slots.get_mut(index) else { return };
        let Place::Claimed(attempt) = &mut slot.place else { return };
        attempt.until = now.saturating_add(attempt.lease_ms);
        let until = attempt.until;
        let moved = if attempt.join.as_ref() != Some(&target) && self.joins.contains_key(&target) { Some(attempt.join.replace(target)) } else { None };
        self.wheel.wake_at(until, index, generation, Due::Lease);
        if let Some(previous) = moved {
            if let Some(previous) = previous.and_then(|previous| self.joins.get_mut(&previous).map(|join| (previous, join))) {
                let (previous, join) = previous;
                join.claimed = join.claimed.saturating_sub(1);
                join.claimed_bytes = join.claimed_bytes.saturating_sub(len);
                join.attempts.remove(&index);
                self.to_fill.push(previous);
            }
            if let Some(join) = self.joins.get_mut(&target) {
                join.claimed += 1;
                join.claimed_bytes += len;
                join.attempts.insert(index);
            }
        }
    }

    /// Takes a join out of every index (lanes, channel or type lists, the
    /// dispatch subscribers, its connection's list) and returns it.
    fn unlink(&mut self, id: &JoinId) -> Option<Join> {
        let join = self.joins.remove(id)?;
        for lane in &join.lanes {
            if let Some(found) = self.lanes.get_mut(lane) {
                found.drop_member(id);
            }
            self.tidy(lane);
        }
        match &join.channel {
            Some(channel) => {
                super::remove_from(&mut self.channel_joins, channel, *id);
                if join.dispatch {
                    super::remove_from(&mut self.subscribers.dispatch, channel, id.0);
                }
            }
            None => {
                for kind in &join.types {
                    super::remove_from(&mut self.type_joins, kind, *id);
                }
            }
        }
        if let Some(open) = self.sessions.get_mut(&id.0) {
            open.joins.retain(|req| *req != id.1);
        }
        Some(join)
    }
}

/// Puts `id` into a join list at its join order.
fn insert_ordered(list: &mut Vec<JoinId>, joins: &std::collections::HashMap<JoinId, Join>, id: JoinId, order: u64) {
    let at = list.iter().position(|other| joins.get(other).is_some_and(|other| other.order > order)).unwrap_or(list.len());
    list.insert(at, id);
}
