//! The observation path: subscriptions and backlogs, statistics, drain (2.8).

use super::lanes::LaneKey;
use super::slots::Index;
use super::State;
use crate::engine::config::Method;
use crate::engine::ops::{ByeReason, IdleLane, Outbound, Refusal, Role, SessionId, Stats, TypeStat};
use crate::envelope::{Channel, Envelope, WorkKey};
use crate::names::Name;
use std::collections::HashMap;

impl State {
    /// `Subscribe`: from now on, the channel's notices and dispatch events;
    /// and one page of what waits there, in work-key order, after `after`.
    pub(super) fn subscribe(&mut self, session: SessionId, credential: &Name, req: u32, channel: Channel, after: Option<WorkKey>) {
        if !channel.is_valid() {
            return self.send(session, Outbound::Refused { req, reason: Refusal::Malformed }, None);
        }
        if !self.allowed(credential, Method::Subscribe) {
            return self.send(session, Outbound::Refused { req, reason: Refusal::Forbidden }, None);
        }
        if let Some(open) = self.sessions.get_mut(&session) {
            if open.channels.insert(channel) {
                self.subscribers.add(channel, session);
            }
        }
        let (backlog, more) = self.backlog(&channel, after);
        self.send(session, Outbound::Subscribed { req, channel, backlog, more }, None);
    }

    /// Waiting events of the channel after `after`, one page. Under Strict,
    /// nothing whose registration is not on disk (2.7).
    fn backlog(&self, channel: &Channel, after: Option<WorkKey>) -> (Vec<Envelope>, bool) {
        let page = self.config.backlog_page.max(1);
        let start = match after {
            Some(after) => std::ops::Bound::Excluded(after),
            None => std::ops::Bound::Unbounded,
        };
        let mut found: Vec<(WorkKey, Index)> = Vec::new();
        for queue in self.lanes.iter().filter(|(key, _)| key.0 == *channel).map(|(_, lane)| &lane.queue) {
            let visible = queue.range((start, std::ops::Bound::Unbounded)).take(page + 1).take_while(|(_, index)| self.visible(**index));
            found.extend(visible.map(|(key, index)| (*key, *index)));
        }
        found.sort_unstable();
        let more = found.len() > page;
        found.truncate(page);
        (found.into_iter().filter_map(|(_, index)| self.slots.get(index).map(|slot| slot.envelope().notice())).collect(), more)
    }

    /// Whether other connections may see the event now (Strict, 2.7).
    fn visible(&self, index: Index) -> bool {
        self.slots.get(index).is_some_and(|slot| !(self.strict && slot.fixed.durable && slot.fixed.visible_from > self.durable))
    }

    /// Whether the credential holds the operator grant.
    fn operator(&self, credential: &Name) -> bool {
        self.allowed(credential, Method::Operate)
    }

    /// `Stats`: values copied when asked (2.8, B24).
    pub(super) fn stats(&mut self, session: SessionId, credential: &Name, req: u32) {
        if !self.operator(credential) {
            return self.send(session, Outbound::Refused { req, reason: Refusal::Forbidden }, None);
        }
        let stats = self.snapshot_stats();
        self.send(session, Outbound::StatsReply { req, stats: Box::new(stats) }, None);
    }

    pub(super) fn snapshot_stats(&self) -> Stats {
        let top = self.config.stats_top;
        let age = |index: &Index| self.slots.get(*index).map_or(0, |slot| self.now.saturating_sub(slot.fixed.registered_at));
        let mut types: HashMap<u16, (u64, u64)> = HashMap::new();
        let mut idle_lanes = Vec::new();
        let mut add = |key: &LaneKey, waiting: u64, oldest: u64| {
            let entry = types.entry(key.1).or_default();
            entry.0 += waiting;
            entry.1 = entry.1.max(oldest);
        };
        for (key, lane) in &self.lanes {
            let oldest = lane.queue.values().next().map_or(0, age);
            add(key, lane.queue.len() as u64, oldest);
            if !lane.queue.is_empty() && self.candidates(key).is_empty() {
                idle_lanes.push(IdleLane { channel: key.0, event_type: key.1, waiting: lane.queue.len() as u64 });
            }
        }
        let mut stats = Stats {
            memory_used: self.memory_used(),
            memory_budget: self.memory_budget(),
            memory_limit: self.memory_limit,
            depth_target: self.config.memory.depth_target,
            disk_used: self.disk_used(),
            disk_budget: self.disk_budget().unwrap_or(0),
            disk_limit: self.config.storage.disk_capacity.unwrap_or(0),
            segments: self.book.segments.values().filter(|segment| segment.known).count() as u64,
            draining: self.draining,
            counters: self.counters.clone(),
            ..Stats::default()
        };
        stats.depth_capacity = stats.memory_budget / self.config.memory.slot_bytes.max(1);
        for (_, slot) in self.slots.iter() {
            match slot.place {
                super::slots::Place::Waiting => {
                    stats.waiting += 1;
                    stats.oldest_ms = stats.oldest_ms.max(self.now.saturating_sub(slot.fixed.registered_at));
                }
                super::slots::Place::Delayed { .. } => stats.delayed += 1,
                super::slots::Place::Claimed(_) => stats.claimed += 1,
            }
        }
        let mut types: Vec<TypeStat> = types.into_iter().map(|(event_type, (waiting, oldest_ms))| TypeStat { event_type, waiting, oldest_ms }).collect();
        types.sort_by(|a, b| b.waiting.cmp(&a.waiting).then_with(|| a.event_type.cmp(&b.event_type)));
        types.truncate(top);
        idle_lanes.sort_by(|a, b| b.waiting.cmp(&a.waiting).then_with(|| (a.channel, a.event_type).cmp(&(b.channel, b.event_type))));
        idle_lanes.truncate(top);
        stats.types = types;
        stats.idle_lanes = idle_lanes;
        stats
    }

    /// Memory and disk now, for the connection owners (2.6).
    pub fn pressure(&self) -> crate::engine::ops::Pressure {
        crate::engine::ops::Pressure {
            memory_used: self.memory_used(),
            memory_budget: self.memory_budget(),
            memory_limit: self.memory_limit,
            memory_high: self.high_watermark(),
            disk_used: self.disk_used(),
            disk_budget: self.disk_budget(),
            disk_limit: self.config.storage.disk_capacity,
        }
    }

    /// `Drain`: new production connections are sent away; every open one gets
    /// its final `Committed` once what it sent is on disk, then `Bye{draining}`
    /// (2.8, B22). Work and observation connections carry on.
    pub(super) fn drain(&mut self, session: SessionId, credential: &Name, req: u32) {
        if !self.operator(credential) {
            return self.send(session, Outbound::Refused { req, reason: Refusal::Forbidden }, None);
        }
        self.send(session, Outbound::Draining { req }, None);
        self.start_drain();
    }

    /// Draining asked for by the process that owns the engine (a server
    /// shutting down without a `Drain`, 2.8 step 5 "drain 없이 종료 신호를
    /// 받아도 2번과 5번은 한다"): the same as `Drain`, with nobody to answer.
    pub fn start_drain(&mut self) {
        self.draining = true;
        let producing: Vec<(SessionId, Option<crate::envelope::ProducerId>, bool)> =
            self.sessions.iter().filter(|(_, open)| open.role == Role::Produce).map(|(id, open)| (*id, open.producer, open.acks)).collect();
        for (target, producer, acks) in producing {
            if let Some(entry) = producer.and_then(|id| self.producers.entries.get_mut(&id)) {
                let gate = entry.last_wait();
                let seq = entry.applied;
                entry.acked = seq;
                if acks {
                    self.send(target, Outbound::Committed { seq }, gate);
                }
            }
            self.close_session(target, Some(ByeReason::Draining));
        }
    }
}
