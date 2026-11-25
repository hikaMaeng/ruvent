//! What the engine runs beside the ledger for storage (2.5): the checkpoint
//! writer and the chores on their own blocking threads, payload reads on
//! blocking calls, and the gauge the connection owners read.
//!
//! None of it is waited on by the ledger. Each piece answers through a
//! channel the engine task selects on, like the journal writer's flight.

use super::ops::Pressure;
use crate::journal::{Bodies, CheckpointHead, Checkpoints, Chores, Location, Part, Written};
use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

/// To the checkpoint writer.
pub(super) enum Write {
    Begin(CheckpointHead),
    Part(Part),
    Finish,
    Abort,
}

/// From the checkpoint writer.
pub(super) enum Answer {
    /// One part is on its way to disk: the walk may hand over the next.
    Part,
    Finished(io::Result<u64>),
    Failed(io::Error),
}

/// Runs a checkpoint writer on a thread of its own until its inbox closes.
pub(super) fn spawn_checkpoints(mut checkpoints: Box<dyn Checkpoints>) -> (mpsc::UnboundedSender<Write>, mpsc::UnboundedReceiver<Answer>, tokio::task::JoinHandle<()>) {
    let (to, mut inbox) = mpsc::unbounded_channel::<Write>();
    let (answer, answers) = mpsc::unbounded_channel();
    let task = tokio::task::spawn_blocking(move || {
        while let Some(message) = inbox.blocking_recv() {
            let reply = match message {
                Write::Begin(head) => checkpoints.begin(&head).err().map(Answer::Failed),
                Write::Part(part) => Some(match checkpoints.write(&part) {
                    Ok(()) => Answer::Part,
                    Err(error) => Answer::Failed(error),
                }),
                Write::Finish => Some(Answer::Finished(checkpoints.finish())),
                Write::Abort => {
                    checkpoints.abort();
                    None
                }
            };
            if let Some(reply) = reply {
                if answer.send(reply).is_err() {
                    break;
                }
            }
        }
        checkpoints.abort();
    });
    (to, answers, task)
}

/// To the chores.
pub(super) enum Chore {
    Relocate(Vec<Location>),
    Delete(Vec<u32>),
}

/// From the chores.
pub(super) enum Done {
    Relocated(io::Result<Written>),
    Deleted(io::Result<Written>),
}

/// Runs the chores on a thread of its own until its inbox closes.
pub(super) fn spawn_chores(mut chores: Box<dyn Chores>) -> (mpsc::UnboundedSender<Chore>, mpsc::UnboundedReceiver<Done>, tokio::task::JoinHandle<()>) {
    let (to, mut inbox) = mpsc::unbounded_channel::<Chore>();
    let (answer, answers) = mpsc::unbounded_channel();
    let task = tokio::task::spawn_blocking(move || {
        while let Some(chore) = inbox.blocking_recv() {
            let done = match chore {
                Chore::Relocate(from) => Done::Relocated(chores.relocate(&from)),
                Chore::Delete(segments) => Done::Deleted(chores.delete(&segments)),
            };
            if answer.send(done).is_err() {
                break;
            }
        }
    });
    (to, answers, task)
}

/// Payload reads in progress and waiting, and the segments they pin.
///
/// | state | key | lifetime | bound | cleared by |
/// | --- | --- | --- | --- | --- |
/// | reads out | read id | asked ~ answered | `max_reads` | the engine, on the answer |
/// | reads waiting | read id | asked ~ started | sends and reads ahead holding a cold payload | the engine, as reads finish |
/// | pins | segment → reads | read asked ~ answered (2.10, "세그먼트 pin") | reads out and waiting | the engine |
pub(super) struct Reads<P> {
    bodies: Option<Arc<dyn Bodies>>,
    max: usize,
    next: u64,
    running: usize,
    waiting: VecDeque<(u64, Location)>,
    purposes: HashMap<u64, (P, Location)>,
    pins: HashMap<u32, u32>,
    answer: mpsc::UnboundedSender<(u64, io::Result<Bytes>)>,
}

impl<P> Reads<P> {
    pub fn new(bodies: Option<Arc<dyn Bodies>>, max: usize) -> (Self, mpsc::UnboundedReceiver<(u64, io::Result<Bytes>)>) {
        let (answer, answers) = mpsc::unbounded_channel();
        (Reads { bodies, max: max.max(1), next: 0, running: 0, waiting: VecDeque::new(), purposes: HashMap::new(), pins: HashMap::new(), answer }, answers)
    }

    /// Asks for the payload at `at`. `None`: this journal cannot read payloads.
    pub fn ask(&mut self, purpose: P, at: Location) -> Option<u64> {
        self.bodies.as_ref()?;
        let id = self.next;
        self.next += 1;
        *self.pins.entry(at.segment).or_default() += 1;
        self.purposes.insert(id, (purpose, at));
        self.waiting.push_back((id, at));
        self.start();
        Some(id)
    }

    fn start(&mut self) {
        let Some(bodies) = &self.bodies else { return };
        while self.running < self.max {
            let Some((id, at)) = self.waiting.pop_front() else { return };
            self.running += 1;
            let bodies = bodies.clone();
            let answer = self.answer.clone();
            tokio::task::spawn_blocking(move || {
                let _ = answer.send((id, bodies.read(&at)));
            });
        }
    }

    /// A read answered: its purpose, and whether its segment is unpinned now.
    pub fn answered(&mut self, id: u64) -> Option<(P, Location, bool)> {
        self.running = self.running.saturating_sub(1);
        self.start();
        let (purpose, at) = self.purposes.remove(&id)?;
        let unpinned = match self.pins.get_mut(&at.segment) {
            Some(count) if *count > 1 => {
                *count -= 1;
                false
            }
            _ => {
                self.pins.remove(&at.segment);
                true
            }
        };
        Some((purpose, at, unpinned))
    }

    pub fn pinned(&self, segment: u32) -> bool {
        self.pins.contains_key(&segment)
    }
}

/// Memory and disk after the last ledger batch, for the connection owners.
/// `u64::MAX` stands for "none" in the optional fields.
#[derive(Default)]
pub(super) struct Gauge {
    memory_used: AtomicU64,
    memory_budget: AtomicU64,
    memory_limit: AtomicU64,
    memory_high: AtomicU64,
    disk_used: AtomicU64,
    disk_budget: AtomicU64,
    disk_limit: AtomicU64,
}

impl Gauge {
    pub fn set(&self, pressure: Pressure) {
        let store = |cell: &AtomicU64, value: u64| cell.store(value, Ordering::Relaxed);
        store(&self.memory_used, pressure.memory_used);
        store(&self.memory_budget, pressure.memory_budget);
        store(&self.memory_limit, pressure.memory_limit);
        store(&self.memory_high, pressure.memory_high);
        store(&self.disk_used, pressure.disk_used);
        store(&self.disk_budget, pressure.disk_budget.unwrap_or(u64::MAX));
        store(&self.disk_limit, pressure.disk_limit.unwrap_or(u64::MAX));
    }

    pub fn get(&self) -> Pressure {
        let load = |cell: &AtomicU64| cell.load(Ordering::Relaxed);
        let optional = |value: u64| (value != u64::MAX).then_some(value);
        Pressure {
            memory_used: load(&self.memory_used),
            memory_budget: load(&self.memory_budget),
            memory_limit: load(&self.memory_limit),
            memory_high: load(&self.memory_high),
            disk_used: load(&self.disk_used),
            disk_budget: optional(load(&self.disk_budget)),
            disk_limit: optional(load(&self.disk_limit)),
        }
    }
}
