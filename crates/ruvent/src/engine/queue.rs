//! One work queue.
//!
//! This is the whole of the "lock management" a message queue needs, and it
//! needs no locks. A message is handed out by flipping `in_flight`; it comes
//! back when whoever holds it says so, or — for a consumer that asked for a
//! visibility deadline — when that deadline passes. Only the engine task ever
//! touches this struct, so two consumers cannot be given the same message:
//! not because a row lock arbitrates between them, but because there is no
//! moment at which two of them are looking.
//!
//! Nothing here polls. A consumer that finds the queue empty is parked inside
//! it and is answered by the event that makes a message available. A timer
//! exists only for a real deadline: a delayed retry, a visibility timeout
//! somebody asked for, or a waiter that set a limit on its patience.

use crate::envelope::Command;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::oneshot;

/// A message handed to a consumer. It stays on the queue, in flight, until it
/// is finished, released or acknowledged.
#[derive(Clone, Debug)]
pub struct Delivery {
    pub id: u64,
    pub command: Arc<Command>,
    /// How many times this message has been handed out since the engine started.
    pub read_count: u32,
}

pub(crate) struct Message {
    pub command: Arc<Command>,
    /// When an in-flight message returns by itself. Zero: it does not.
    deadline: u64,
    in_flight: bool,
    read_count: u32,
    /// Whether the journal currently says this message exists.
    pub on_disk: bool,
}

/// A consumer parked on an empty queue.
pub(crate) struct Waiter {
    pub max: usize,
    pub visibility: Option<u64>,
    pub deadline: Option<u64>,
    pub reply: oneshot::Sender<Vec<Delivery>>,
}

#[derive(Default)]
pub(crate) struct Queue {
    pub messages: HashMap<u64, Message>,
    ready: VecDeque<u64>,
    /// `(deadline, id)`. Entries are never removed early: one whose deadline
    /// no longer matches its message is stale and is skipped when it surfaces.
    timers: BinaryHeap<Reverse<(u64, u64)>>,
    pub waiters: VecDeque<Waiter>,
    pub dead: VecDeque<(u64, Arc<Command>)>,
}

impl Queue {
    pub fn put(&mut self, id: u64, command: Arc<Command>, on_disk: bool) {
        self.messages.insert(id, Message { command, deadline: 0, in_flight: false, read_count: 0, on_disk });
        self.ready.push_back(id);
    }

    pub fn remove(&mut self, id: u64) -> Option<Message> {
        self.messages.remove(&id)
    }

    /// Adds to the dead letters, keeping at most `keep` of them.
    pub fn bury(&mut self, id: u64, command: Arc<Command>, keep: usize) {
        self.dead.push_back((id, command));
        while self.dead.len() > keep {
            self.dead.pop_front();
        }
    }

    /// Returns every message whose deadline has passed to the ready line.
    fn promote(&mut self, now: u64) {
        while let Some(&Reverse((at, id))) = self.timers.peek() {
            if at > now {
                break;
            }
            self.timers.pop();
            if let Some(message) = self.messages.get_mut(&id) {
                if message.in_flight && message.deadline == at {
                    message.in_flight = false;
                    message.deadline = 0;
                    self.ready.push_back(id);
                }
            }
        }
    }

    /// Hands out up to `max` messages. With a `visibility` each returns by
    /// itself after that long; without one it is held until released.
    pub fn take(&mut self, max: usize, visibility: Option<u64>, now: u64) -> Vec<Delivery> {
        self.promote(now);
        let mut taken = Vec::new();
        while taken.len() < max {
            let Some(id) = self.ready.pop_front() else { break };
            // Deleted while it waited in line.
            let Some(message) = self.messages.get_mut(&id) else { continue };
            if message.in_flight {
                continue;
            }
            message.in_flight = true;
            message.read_count += 1;
            message.deadline = visibility.map_or(0, |visibility| now + visibility.max(1));
            if message.deadline != 0 {
                self.timers.push(Reverse((message.deadline, id)));
            }
            taken.push(Delivery { id, command: message.command.clone(), read_count: message.read_count });
        }
        taken
    }

    /// Gives an in-flight message back: at once, or after `delay` ms.
    pub fn give_back(&mut self, id: u64, delay: u64, now: u64) -> bool {
        let Some(message) = self.messages.get_mut(&id) else { return false };
        if !message.in_flight {
            return false;
        }
        if delay == 0 {
            message.in_flight = false;
            message.deadline = 0;
            self.ready.push_back(id);
        } else {
            message.deadline = now + delay;
            self.timers.push(Reverse((message.deadline, id)));
        }
        true
    }

    /// The earliest moment something here needs the engine's attention. Only
    /// a queue somebody is waiting on has one: with no waiter, an expired
    /// deadline is noticed by the next consumer to arrive.
    pub fn next_deadline(&self) -> Option<u64> {
        if self.waiters.is_empty() {
            return None;
        }
        let timer = self.timers.peek().map(|&Reverse((at, _))| at);
        let waiter = self.waiters.iter().filter_map(|waiter| waiter.deadline).min();
        match (timer, waiter) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    /// Everything still owed, in arrival order, marked as written. Used to
    /// build a snapshot.
    pub fn snapshot(&mut self) -> Vec<(u64, Arc<Command>)> {
        let mut pending: Vec<_> = self
            .messages
            .iter_mut()
            .map(|(id, message)| {
                message.on_disk = true;
                (*id, message.command.clone())
            })
            .collect();
        pending.sort_by_key(|(id, _)| *id);
        pending
    }
}
