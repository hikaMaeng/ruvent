//! A connection's unsent messages, as plain state: no socket, no task.
//!
//! * **One cumulative `Committed`.** A newer one replaces the one still
//!   queued, so a production connection that does not read holds one
//!   acknowledgement, not one per batch (2.2, 2.3).
//! * **Faults once** per (connection, kind) (2.7).
//! * **A byte bound** (S7): what an observation connection holds unsent, and
//!   a work connection's dispatch and notice bytes (its assignments are bound
//!   by its windows). Past it the connection is slow (2.6). A production
//!   connection holds a constant: one `Committed`, pause notices, faults.

use ruvent::envelope::codec::{self, Layout};
use ruvent::{EventId, FaultKind, Outbound, Role};
use std::collections::VecDeque;

/// Bytes counted for a message that carries no event on an observation connection.
const SMALL: u64 = 16;

/// The unsent queue.
///
/// | key | the connection's send order (`pushed`, `popped`) |
/// | --- | --- |
/// | scope | one connection |
/// | lifetime | handed over by the engine ~ written, dropped at close, or the connection's end |
/// | bound | observation: `bound` bytes; work: its windows, plus `bound` for dispatch and notices; production: a constant |
/// | cleared by | the connection's owner |
pub(super) struct Unsent {
    role: Role,
    bound: u64,
    queue: VecDeque<Outbound>,
    pushed: u64,
    popped: u64,
    /// The send number of the queued `Committed`, while there is one.
    commit_at: Option<u64>,
    /// Bytes queued or taken and not yet written, counted against the bound.
    counted: u64,
    /// Fault kinds already told.
    told: u8,
}

/// The event id of an assignment message.
pub(super) fn assigned_id(message: &Outbound) -> Option<EventId> {
    match message {
        Outbound::Assign(assignment) | Outbound::Claimed { assignment, .. } => Some(assignment.event.id),
        _ => None,
    }
}

impl Unsent {
    pub(super) fn new(role: Role, bound: u64) -> Self {
        Unsent { role, bound, queue: VecDeque::new(), pushed: 0, popped: 0, commit_at: None, counted: 0, told: 0 }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.queue.len()
    }

    /// Bytes counted against the bound now.
    #[cfg(test)]
    pub(super) fn counted(&self) -> u64 {
        self.counted
    }

    /// What `message` counts against the bound.
    pub(super) fn weight(&self, message: &Outbound) -> u64 {
        let events = match message {
            Outbound::Delivery(event) => codec::size(event, Layout::Delivery) as u64,
            Outbound::Notice(event) => codec::size(event, Layout::Notice) as u64,
            Outbound::Subscribed { backlog, .. } => backlog.iter().map(|event| codec::size(event, Layout::Notice) as u64).sum::<u64>() + SMALL,
            _ => 0,
        };
        match self.role {
            Role::Produce => 0,
            Role::Work => events,
            Role::Observe => events.max(SMALL),
        }
    }

    /// Queues a message the owner decided on (its own `Bye`), with no checks.
    pub(super) fn push(&mut self, message: Outbound) {
        self.counted += self.weight(&message);
        self.queue.push_back(message);
        self.pushed += 1;
    }

    /// Takes the next message to write. It stays counted until [`Unsent::written`].
    pub(super) fn pop(&mut self) -> Option<(Outbound, u64)> {
        let message = self.queue.pop_front()?;
        if self.commit_at == Some(self.popped) {
            self.commit_at = None;
        }
        self.popped += 1;
        let weight = self.weight(&message);
        Some((message, weight))
    }

    /// Messages taken by [`Unsent::pop`] weighing `weight` are written (or dropped).
    pub(super) fn written(&mut self, weight: u64) {
        self.counted = self.counted.saturating_sub(weight);
    }

    /// Takes a message from the engine. `true`: the connection is past its bound.
    pub(super) fn accept(&mut self, message: Outbound) -> bool {
        match message {
            Outbound::Committed { seq } => {
                if let Some(at) = self.commit_at {
                    if let Some(Outbound::Committed { seq: queued }) = self.queue.get_mut((at - self.popped) as usize) {
                        *queued = (*queued).max(seq);
                        return false;
                    }
                }
                self.commit_at = Some(self.pushed);
                self.push(Outbound::Committed { seq });
            }
            Outbound::Fault { kind, detail } => self.fault(kind, detail),
            other => self.push(other),
        }
        self.counted > self.bound
    }

    /// Queues a fault unless one of its kind was told already.
    pub(super) fn fault(&mut self, kind: FaultKind, detail: &'static str) {
        if self.told & kind.bit() == 0 {
            self.told |= kind.bit();
            self.push(Outbound::Fault { kind, detail });
        }
    }

    /// Drops every queued assignment and returns their event ids: they were
    /// never written, so they go back to waiting.
    pub(super) fn drop_assignments(&mut self) -> Vec<EventId> {
        let mut ids = Vec::new();
        let mut kept = VecDeque::with_capacity(self.queue.len());
        while let Some((message, weight)) = self.pop() {
            self.written(weight);
            match assigned_id(&message) {
                Some(id) => ids.push(id),
                None => kept.push_back(message),
            }
        }
        for message in kept {
            self.push(message);
        }
        ids
    }

    /// Drops everything queued (a slow connection's dispatches are one-shot,
    /// 0.3) and returns the event ids of the assignments among them.
    pub(super) fn drop_all(&mut self) -> Vec<EventId> {
        let ids = self.drop_assignments();
        while let Some((_, weight)) = self.pop() {
            self.written(weight);
        }
        ids
    }
}

#[cfg(test)]
mod tests;
