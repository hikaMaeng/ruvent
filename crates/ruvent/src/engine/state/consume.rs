//! The consumption path: joins, windows, round-robin assignment, explicit
//! claims and extensions (2.4).

use super::lanes::{Join, Lane, LaneKey};
use super::slots::{Attempt, Index, JoinId, Place};
use super::wheel::Due;
use super::State;
use crate::engine::config::Method;
use crate::engine::ops::{Assignment, FaultKind, Outbound, Refusal, SessionId};
use crate::envelope::{ClaimToken, EventId, WorkKey};
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
                    if join.covers(&key.1) {
                        join.lanes.push(key.clone());
                        members.push((join.order, join_id.clone()));
                    }
                }
            }
            members.sort();
            let serialize = self.config.channels.settings(&key.0).serialize_group;
            self.lanes.insert(key.clone(), Lane { members: members.into_iter().map(|(_, join)| join).collect(), serialize, ..Lane::default() });
        }
        self.lanes.entry(key.clone()).or_default()
    }

    /// Drops a lane that has neither events nor members.
    pub(super) fn tidy(&mut self, key: &LaneKey) {
        if self.lanes.get(key).is_some_and(Lane::is_idle) {
            self.lanes.remove(key);
        }
    }

    /// `Join`: checks the claim grant here, once (2.7).
    #[allow(clippy::too_many_arguments)]
    pub(super) fn join(&mut self, session: SessionId, credential: &Name, req: u32, channel: &Name, types: Vec<Name>, lease_ms: u64, dispatch: bool) {
        if !self.allowed(credential, Method::Claim, channel) {
            return self.send(session, Outbound::Refused { req, reason: crate::engine::ops::Refusal::Forbidden }, None);
        }
        let channel = self.names.adopt(channel);
        let types: Vec<Name> = types.iter().map(|name| self.names.adopt(name)).collect();
        let id: JoinId = (session, channel.clone());
        // A second join on the same channel replaces the first's terms, keeping what it holds.
        let held = self.joins.remove(&id).map(|old| {
            for lane in &old.lanes {
                if let Some(lane) = self.lanes.get_mut(lane) {
                    lane.drop_member(&id);
                }
            }
            if old.dispatch {
                super::remove_from(&mut self.subscribers.dispatch, &channel, session);
            }
            (old.count, old.bytes, old.claimed, old.claimed_bytes, old.attempts, old.order)
        });
        let (count, bytes, claimed, claimed_bytes, attempts, order) = held.unwrap_or_else(|| {
            let order = self.next_order;
            self.next_order += 1;
            (0, 0, 0, 0, HashSet::new(), order)
        });
        let mut join = Join { types, lease_ms, count, bytes, claimed, claimed_bytes, signalled: false, order, dispatch, lanes: Vec::new(), attempts };
        let covered: Vec<LaneKey> = self.lanes.keys().filter(|key| key.0 == channel && join.covers(&key.1)).cloned().collect();
        for key in &covered {
            if let Some(lane) = self.lanes.get_mut(key) {
                let at = lane.members.iter().position(|member| self.joins.get(member).is_some_and(|other| other.order > order)).unwrap_or(lane.members.len());
                lane.members.insert(at, id.clone());
            }
        }
        join.lanes = covered;
        self.joins.insert(id.clone(), join);
        let list = self.channel_joins.entry(channel.clone()).or_default();
        if !list.contains(&id) {
            list.push(id.clone());
        }
        if dispatch {
            self.subscribers.dispatch.entry(channel.clone()).or_default().push(session);
        }
        if let Some(open) = self.sessions.get_mut(&session) {
            if !open.joins.contains(&channel) {
                open.joins.push(channel);
            }
        }
        self.send(session, Outbound::Joined { req }, None);
        self.to_fill.push(id);
    }

    /// `Window`: the absolute number of assignments and bytes this join may hold (2.4).
    pub(super) fn window(&mut self, session: SessionId, channel: &Name, count: u32, bytes: u64) {
        let id: JoinId = (session, channel.clone());
        let Some(join) = self.joins.get_mut(&id) else {
            return self.fault(session, FaultKind::Malformed, "a window for a channel this connection has not joined");
        };
        join.count = count;
        join.bytes = bytes;
        join.signalled = false;
        self.to_fill.push(id);
    }

    /// Leaves a join because its connection closed.
    pub(super) fn part(&mut self, session: SessionId, channel: Name, unwritten: &[ClaimToken]) {
        let id: JoinId = (session, channel.clone());
        let Some(join) = self.joins.remove(&id) else { return };
        for lane in &join.lanes {
            if let Some(found) = self.lanes.get_mut(lane) {
                found.drop_member(&id);
            }
            self.tidy(lane);
        }
        if let Some(list) = self.channel_joins.get_mut(&channel) {
            list.retain(|member| *member != id);
            if list.is_empty() {
                self.channel_joins.remove(&channel);
            }
        }
        if join.dispatch {
            super::remove_from(&mut self.subscribers.dispatch, &channel, session);
        }
        let grace = super::millis(self.config.reconnect_grace);
        for index in join.attempts {
            let generation = self.slots.generation(index);
            let now = self.now;
            let Some(slot) = self.slots.get_mut(index) else { continue };
            let Place::Claimed(attempt) = &mut slot.place else { continue };
            attempt.join = None;
            if unwritten.contains(&attempt.token) {
                // Never fully written: the worker cannot have started it.
                let token = attempt.token;
                self.release_unwritten(index, token);
            } else {
                attempt.until = attempt.until.min(now.saturating_add(grace));
                let (until, token) = (attempt.until, attempt.token);
                self.wheel.wake_at(until, index, generation, Due::Lease(token));
            }
        }
    }

    /// Gives a join that has room the earliest eligible head of its lanes,
    /// until it is full or nothing fits (2.4).
    pub(super) fn fill(&mut self, id: &JoinId) {
        loop {
            let Some(join) = self.joins.get(id) else { return };
            if !join.has_room() {
                return;
            }
            let lanes = join.lanes.clone();
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
        let count = found.members.len();
        for step in 0..count {
            let at = (found.cursor + step) % count;
            let member = &found.members[at];
            if self.joins.get(member).is_some_and(|join| join.fits(len)) {
                let member = member.clone();
                if let Some(found) = self.lanes.get_mut(lane) {
                    found.cursor = (at + 1) % count;
                }
                return Some(member);
            }
        }
        None
    }

    fn signal_pending(&mut self, lane: &LaneKey) {
        let Some(found) = self.lanes.get(lane) else { return };
        let members = found.members.clone();
        for member in members {
            if let Some(join) = self.joins.get_mut(&member) {
                if !join.signalled {
                    join.signalled = true;
                    let channel = member.1.clone();
                    self.send(member.0, Outbound::Pending { channel }, None);
                }
            }
        }
    }

    /// The first event of the lane that may be assigned now. On the way it
    /// removes what expired and moves what a paused root may not grow by to
    /// the root's hold lane. Under Strict it stops at the first event whose
    /// registration is not on disk: the rest of the lane is newer (2.7).
    pub(super) fn head(&mut self, key: &LaneKey) -> Option<(WorkKey, Index, u64)> {
        let lane = self.lanes.get(key)?;
        let net = lane.growth.net();
        if net && self.over_limit {
            return None;
        }
        let mut expired = Vec::new();
        let mut held = Vec::new();
        let mut found = None;
        for (&work_key, &index) in &lane.queue {
            let Some(slot) = self.slots.get(index) else { continue };
            if self.strict && slot.fixed.durable && slot.fixed.visible_from > self.durable {
                break;
            }
            if slot.expires.is_some_and(|at| at <= self.now) {
                expired.push(index);
                continue;
            }
            if lane.serialize && slot.fixed.group.is_some_and(|group| self.groups.contains_key(&(slot.channel.clone(), group))) {
                continue;
            }
            if net && self.roots.get(&slot.fixed.root).is_some_and(|root| root.blocks_growth()) {
                held.push(index);
                continue;
            }
            found = Some((work_key, index, slot.len()));
            break;
        }
        for index in held {
            self.hold(index);
        }
        for index in expired {
            self.expire(index);
        }
        found
    }

    /// Hands the event to the join: one step for assignment, claim and delivery.
    fn assign(&mut self, id: &JoinId, lane: &LaneKey, key: WorkKey, index: Index) {
        if let Some(found) = self.lanes.get_mut(lane) {
            found.queue.remove(&key);
        }
        let Some((lease_ms, credential)) = self.joins.get(id).map(|join| join.lease_ms).zip(self.sessions.get(&id.0).map(|open| open.credential.clone())) else { return };
        let token = self.next_token();
        let until = self.now.saturating_add(lease_ms);
        let attempt = Attempt { token, lease_ms, until, credential, join: Some(id.clone()) };
        let Some(len) = self.start(index, attempt) else { return };
        let more = {
            let Some(join) = self.joins.get_mut(id) else { return };
            join.claimed += 1;
            join.claimed_bytes += len;
            join.attempts.insert(index);
            let full = !join.has_room();
            let lanes = join.lanes.clone();
            let more = full && lanes.iter().any(|lane| self.lanes.get(lane).is_some_and(|lane| !lane.queue.is_empty()));
            if more {
                if let Some(join) = self.joins.get_mut(id) {
                    join.signalled = true;
                }
            }
            more
        };
        if let Some((event, cold)) = self.slots.get(index).map(|slot| (slot.envelope(), slot.cold())) {
            self.send_event(id.0, Outbound::Assign(Assignment { event, token, lease_ms, until, more }), None, index);
            if cold {
                self.read_ahead(lane);
            }
        }
    }

    /// Marks the event claimed by `attempt`: lease on the wheel, group taken,
    /// `Claim` journaled. Returns the payload length.
    fn start(&mut self, index: Index, attempt: Attempt) -> Option<u64> {
        let generation = self.slots.generation(index);
        let slot = self.slots.get_mut(index)?;
        let (token, until, lease_ms, credential) = (attempt.token, attempt.until, attempt.lease_ms, attempt.credential.clone());
        slot.place = Place::Claimed(attempt);
        let (len, durable, key, group, channel) = (slot.len(), slot.fixed.durable, slot.fixed.key, slot.fixed.group, slot.channel.clone());
        if let Some(group) = group {
            if self.config.channels.settings(&channel).serialize_group {
                *self.groups.entry((channel, group)).or_default() += 1;
            }
        }
        self.wheel.wake_at(until, index, generation, Due::Lease(token));
        if durable {
            self.record_for(index, Record::Claim { key, token, lease_ms, credential });
        }
        Some(len)
    }

    /// Explicit `Claim{id}`: the event with this id, whichever lane it waits
    /// in, if it is visible. Under Strict the answer waits for its
    /// registration to be on disk (2.7).
    pub(super) fn claim(&mut self, session: SessionId, credential: &Name, req: u32, id: EventId, lease_ms: u64) {
        let refuse = |state: &mut State, reason| state.send(session, Outbound::Refused { req, reason }, None);
        let Some(index) = self.slots.by_id(&id) else { return refuse(self, Refusal::Missing) };
        let Some(slot) = self.slots.get(index) else { return refuse(self, Refusal::Missing) };
        if !self.allowed(credential, Method::Claim, &slot.channel) {
            return refuse(self, Refusal::Forbidden);
        }
        match slot.place {
            Place::Claimed(_) => return refuse(self, Refusal::AlreadyClaimed),
            Place::Delayed { .. } => return refuse(self, Refusal::Missing),
            Place::Waiting | Place::Held => {}
        }
        if slot.expires.is_some_and(|at| at <= self.now) {
            self.expire(index);
            return refuse(self, Refusal::Missing);
        }
        let (lane, key, root, channel) = (slot.lane(), slot.fixed.key, slot.fixed.root.clone(), slot.channel.clone());
        if matches!(slot.place, Place::Held) {
            self.unhold(&root, &lane, key);
        } else if let Some(found) = self.lanes.get_mut(&lane) {
            found.queue.remove(&key);
        }
        let join_id: JoinId = (session, channel);
        let join = self.joins.contains_key(&join_id).then_some(join_id.clone());
        let token = self.next_token();
        let until = self.now.saturating_add(lease_ms);
        let Some(len) = self.start(index, Attempt { token, lease_ms, until, credential: credential.clone(), join: join.clone() }) else { return };
        if let Some(join) = join.and_then(|join| self.joins.get_mut(&join)) {
            join.claimed += 1;
            join.claimed_bytes += len;
            join.attempts.insert(index);
        }
        self.tidy(&lane);
        let gate = self.gate_for(index);
        if let Some(event) = self.slots.get(index).map(|slot| slot.envelope()) {
            self.send_event(session, Outbound::Claimed { req, assignment: Assignment { event, token, lease_ms, until, more: false } }, gate, index);
        }
    }

    /// The attempt at `index` if it is `token` and was made under `credential`.
    /// A wrong token is a late or stale report: counted, not told (2.7).
    pub(super) fn attempt_for(&mut self, session: SessionId, credential: &Name, id: EventId, token: ClaimToken) -> Option<Index> {
        let current = self.slots.by_id(&id).and_then(|index| self.slots.get(index).and_then(|slot| slot.attempt()).map(|attempt| (index, attempt.token, attempt.credential.clone())));
        match current {
            Some((index, held, owner)) if held == token => {
                if owner != *credential {
                    self.fault(session, FaultKind::Forbidden, "the attempt was made under another credential");
                    return None;
                }
                Some(index)
            }
            _ => {
                self.counters.stale += 1;
                None
            }
        }
    }

    /// `Extend`: re-arms the lease at its length from now, and moves the
    /// attempt to this connection's join on the channel, if it has one (2.4).
    /// Never journaled: a restart re-arms every lease anyway.
    pub(super) fn extend(&mut self, session: SessionId, credential: &Name, id: EventId, token: ClaimToken) {
        let Some(index) = self.attempt_for(session, credential, id, token) else { return };
        let generation = self.slots.generation(index);
        let now = self.now;
        let Some(slot) = self.slots.get_mut(index) else { return };
        let len = slot.len();
        let channel = slot.channel.clone();
        let Place::Claimed(attempt) = &mut slot.place else { return };
        attempt.until = now.saturating_add(attempt.lease_ms);
        let until = attempt.until;
        let target: JoinId = (session, channel);
        let moved = if attempt.join.as_ref() != Some(&target) && self.joins.contains_key(&target) { Some(attempt.join.replace(target.clone())) } else { None };
        self.wheel.wake_at(until, index, generation, Due::Lease(token));
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
}
