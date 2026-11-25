//! The observation path: subscriptions and backlogs, statistics, drain (2.8).

use super::lanes::LaneKey;
use super::slots::Index;
use super::State;
use crate::engine::config::Method;
use crate::engine::ops::{ByeReason, ChannelStat, IdleLane, Outbound, Refusal, Role, RootStat, SessionId, Stats, Target};
use crate::envelope::{Envelope, WorkKey};
use crate::names::Name;
use std::collections::HashMap;

impl State {
    /// `Subscribe`: from now on, the target's notices and dispatch events;
    /// and one page of what waits there, in work-key order, after `after`.
    pub(super) fn subscribe(&mut self, session: SessionId, credential: &Name, req: u32, target: Target, after: Option<WorkKey>) {
        let checked = match &target {
            Target::Channel(name) => name.to_string(),
            Target::Prefix(name) => format!("{name}."),
        };
        if !self.allowed(credential, Method::Subscribe, &checked) {
            return self.send(session, Outbound::Refused { req, reason: Refusal::Forbidden }, None);
        }
        let target = self.adopt_target(target);
        if let Some(open) = self.sessions.get_mut(&session) {
            if open.targets.insert(target.clone()) {
                self.subscribers.add(&target, session);
            }
        }
        let (backlog, more) = self.backlog(&target, after);
        self.send(session, Outbound::Subscribed { req, target, backlog, more }, None);
    }

    /// Waiting events (in a lane or held) of the target's channels after
    /// `after`, one page. Under Strict, nothing whose registration is not on
    /// disk (2.7).
    fn backlog(&self, target: &Target, after: Option<WorkKey>) -> (Vec<Envelope>, bool) {
        let page = self.config.backlog_page.max(1);
        let start = match after {
            Some(after) => std::ops::Bound::Excluded(after),
            None => std::ops::Bound::Unbounded,
        };
        let mut found: Vec<(WorkKey, Index)> = Vec::new();
        let lanes = self.lanes.iter().filter(|(key, _)| target.matches(&key.0)).map(|(_, lane)| &lane.queue);
        let holds = self.holds.iter().filter(|((_, key), _)| target.matches(&key.0)).map(|(_, held)| held);
        for queue in lanes.chain(holds) {
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

    /// Whether the credential holds the operator grant. Channels do not matter.
    fn operator(&self, credential: &Name) -> bool {
        match &self.config.grants {
            None => true,
            Some(grants) => grants.get(credential.as_str()).is_some_and(|grant| grant.methods.contains(&Method::Operate)),
        }
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
        let mut channels: HashMap<Name, (u64, u64)> = HashMap::new();
        let mut idle_lanes = Vec::new();
        let mut add = |key: &LaneKey, waiting: u64, oldest: u64| {
            let entry = channels.entry(key.0.clone()).or_default();
            entry.0 += waiting;
            entry.1 = entry.1.max(oldest);
        };
        for (key, lane) in &self.lanes {
            let oldest = lane.queue.values().next().map_or(0, age);
            add(key, lane.queue.len() as u64, oldest);
            if lane.members.is_empty() && !lane.queue.is_empty() {
                idle_lanes.push(IdleLane { channel: key.0.clone(), event_type: key.1.clone(), waiting: lane.queue.len() as u64 });
            }
        }
        for ((_, key), held) in &self.holds {
            add(key, held.len() as u64, held.values().next().map_or(0, age));
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
                super::slots::Place::Waiting | super::slots::Place::Held => {
                    stats.waiting += 1;
                    stats.oldest_ms = stats.oldest_ms.max(self.now.saturating_sub(slot.fixed.registered_at));
                }
                super::slots::Place::Delayed { .. } => stats.delayed += 1,
                super::slots::Place::Claimed(_) => stats.claimed += 1,
            }
        }
        let mut channels: Vec<ChannelStat> = channels.into_iter().map(|(channel, (waiting, oldest_ms))| ChannelStat { channel, waiting, oldest_ms }).collect();
        channels.sort_by(|a, b| b.waiting.cmp(&a.waiting).then_with(|| a.channel.cmp(&b.channel)));
        channels.truncate(top);
        idle_lanes.sort_by(|a, b| b.waiting.cmp(&a.waiting).then_with(|| (&a.channel, &a.event_type).cmp(&(&b.channel, &b.event_type))));
        idle_lanes.truncate(top);
        let mut roots: Vec<RootStat> =
            self.roots.iter().map(|(root, entry)| RootStat { root: root.clone(), events: entry.events, bytes: entry.bytes, disk: entry.disk, paused: entry.paused.is_some() }).collect();
        roots.sort_by(|a, b| b.bytes.cmp(&a.bytes).then_with(|| a.root.cmp(&b.root)));
        roots.truncate(top);
        stats.channels = channels;
        stats.idle_lanes = idle_lanes;
        stats.roots = roots;
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
        self.draining = true;
        self.send(session, Outbound::Draining { req }, None);
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
