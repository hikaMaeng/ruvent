//! The engine's private state machine.
//!
//! Decisions mutate memory directly and mark what they touched. Nothing is
//! logged as it happens. When a journal write is due, [`State::harvest`]
//! compares what was touched against what the journal last said and emits
//! only the net difference; [`State::snapshot`] emits everything, superseding
//! all of it. [`State::apply`] is the inverse of both and is used on restart.
//!
//! Everything here that can grow is bounded at the moment it grows: the event
//! log by count, bytes and age as events are appended, settled executions by
//! window and count as executions settle. Nothing is swept on a timer.

use super::config::{Persist, Retention};
use super::queue::Queue;
use super::{Begin, Outcome};
use crate::envelope::{Audience, Command, Emit, Envelope, EventId, Kind, Text};
use crate::journal::Record;
use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use xxhash_rust::xxh3::Xxh3;

/// Positions are `epoch << 40 | counter`.
///
/// A crash can lose events that were already published. If positions resumed
/// from a remembered counter, the new incarnation would reissue positions
/// clients have already seen — for different events — and a resuming client
/// would skip them. Starting every incarnation in a higher range makes every
/// new event sort after anything any client was ever shown.
const EPOCH_SHIFT: u32 = 40;

/// Fixed part of an event's memory cost, on top of its payload.
const EVENT_OVERHEAD: usize = 256;

struct Execution {
    request: EventId,
    fingerprint: u128,
    attempt: u64,
    attempts: u32,
    /// The message being run right now, if anyone is running it. Volatile:
    /// empty after a restart, so every unfinished execution can be taken up
    /// again.
    held: Option<(Text, u64)>,
    done: Option<(bool, Bytes, u64)>,
    /// Its message is on a queue that is configured not to be journaled.
    transient: bool,
    /// Changed since the journal was last told about it.
    dirty: bool,
    /// Whether the journal currently says this execution exists.
    on_disk: bool,
}

pub(crate) struct State {
    // ---- the event log's bookkeeping (the readable copy lives in `LogIndex`)
    ids: HashMap<EventId, Arc<Envelope>>,
    order: VecDeque<Arc<Envelope>>,
    bytes: usize,
    next_position: u64,
    /// Events of the batch in progress, to be published.
    pub staged: Vec<Arc<Envelope>>,
    /// Highest position retention has dropped; the readable log follows.
    pub drop_through: u64,

    // ---- work
    pub queues: HashMap<Text, Queue>,
    executions: HashMap<Text, Execution>,
    /// Settled executions in the order they settled, for expiry.
    settled: VecDeque<(u64, Text)>,
    next_message: u64,
    next_attempt: u64,

    // ---- what the journal has not been told yet
    epoch: u64,
    epoch_written: bool,
    fresh: Vec<(Text, u64)>,
    gone: Vec<(Text, u64)>,
    buried: Vec<(Text, u64, Arc<Command>)>,
    touched: Vec<Text>,
    forgotten: Vec<Text>,
    unwritten: Vec<Arc<Envelope>>,

    // ---- configuration
    reactions: Arc<HashMap<Text, Vec<Text>>>,
    max_attempts: u32,
    retention: Retention,
    persist: Persist,

    pub duplicates: u64,
    pub enqueued: u64,
}

/// A fast content fingerprint. It tells "the same event sent again" from
/// "the same key reused for something else"; it is not a security boundary.
fn fingerprint(action: &str, payload: &[u8]) -> u128 {
    let mut hasher = Xxh3::new();
    hasher.update(&(action.len() as u32).to_le_bytes());
    hasher.update(action.as_bytes());
    hasher.update(payload);
    hasher.digest128()
}

/// Where a command's result goes: its reply channel, or its own channel.
fn reply_channel(command: &Envelope) -> &str {
    command.reply_channel.as_deref().unwrap_or(&command.channel)
}

/// The one result of one command. Its id is derived, so reporting the same
/// outcome twice stores one event.
fn result_event(command: &Envelope, ok: bool, payload: Bytes, now: u64) -> Envelope {
    let id = EventId::derive(&[b"result", command.transaction_key.as_bytes(), command.stream.as_bytes(), reply_channel(command).as_bytes()]);
    let (kind, suffix) = if ok { (Kind::Result, "result") } else { (Kind::Failure, "failure") };
    Envelope::follow(command, id, kind, format!("{}.{suffix}", command.action).into(), payload, now)
}

fn cost(event: &Envelope) -> usize {
    EVENT_OVERHEAD + event.payload.len()
}

impl State {
    pub fn new(reactions: Arc<HashMap<Text, Vec<Text>>>, max_attempts: u32, retention: Retention, persist: Persist) -> Self {
        Self {
            ids: HashMap::new(),
            order: VecDeque::new(),
            bytes: 0,
            next_position: 1,
            staged: Vec::new(),
            drop_through: 0,
            queues: HashMap::new(),
            executions: HashMap::new(),
            settled: VecDeque::new(),
            next_message: 1,
            next_attempt: 1,
            epoch: 0,
            epoch_written: false,
            fresh: Vec::new(),
            gone: Vec::new(),
            buried: Vec::new(),
            touched: Vec::new(),
            forgotten: Vec::new(),
            unwritten: Vec::new(),
            reactions,
            max_attempts,
            retention,
            persist,
            duplicates: 0,
            enqueued: 0,
        }
    }

    // ================================================================ restart

    /// Rebuilds state from one journal record. Later records supersede earlier ones.
    pub fn apply(&mut self, record: &Record, now: u64) {
        match record {
            Record::Epoch(epoch) => self.epoch = self.epoch.max(*epoch),
            Record::Put { queue, id, command } => {
                self.queues.entry(queue.clone()).or_default().put(*id, command.clone(), true);
                self.next_message = self.next_message.max(id + 1);
            }
            Record::Del { queue, id } => {
                if let Some(queue) = self.queues.get_mut(queue) {
                    queue.remove(*id);
                }
            }
            Record::Dead { queue, id, command } => {
                let keep = self.retention.dead_letters;
                let queue = self.queues.entry(queue.clone()).or_default();
                queue.remove(*id);
                queue.bury(*id, command.clone(), keep);
                self.next_message = self.next_message.max(id + 1);
            }
            Record::Exec { key, request, fingerprint, attempt, attempts, done } => {
                if let Some((_, _, at)) = done {
                    self.settled.push_back((*at, key.clone()));
                }
                self.next_attempt = self.next_attempt.max(attempt + 1);
                self.executions.insert(
                    key.clone(),
                    Execution { request: *request, fingerprint: *fingerprint, attempt: *attempt, attempts: *attempts, held: None, done: done.clone(), transient: false, dirty: false, on_disk: true },
                );
            }
            Record::Forget { key } => {
                self.executions.remove(key);
            }
            Record::Event(event) => {
                // Retention dropped this event from memory, it was appended
                // again under the same id, and the journal holds both. The
                // later statement wins: the log never holds one event twice.
                if let Some(earlier) = self.ids.get(&event.event_id).map(|earlier| earlier.position) {
                    if let Some(at) = self.order.iter().position(|kept| kept.position == earlier) {
                        let superseded = self.order.remove(at).expect("index was just found");
                        self.bytes -= cost(&superseded);
                    }
                    self.staged.retain(|staged| staged.position != earlier);
                }
                self.remember(event.clone(), now, false);
            }
        }
    }

    /// Starts a new incarnation after replay. Returns where its positions begin.
    pub fn open_epoch(&mut self) -> u64 {
        self.epoch += 1;
        self.epoch_written = false;
        self.next_position = (self.epoch << EPOCH_SHIFT) + 1;
        self.epoch << EPOCH_SHIFT
    }

    // ================================================================ journal

    pub fn is_dirty(&self) -> bool {
        self.unwritten_changes() > 0
    }

    /// How many changes the journal has not been told about. An upper bound on
    /// what a crash right now would lose; changes that cancel out are counted
    /// until the next harvest notices they did.
    pub fn unwritten_changes(&self) -> usize {
        usize::from(!self.epoch_written) + self.fresh.len() + self.gone.len() + self.buried.len() + self.touched.len() + self.forgotten.len() + self.unwritten.len()
    }

    /// Whether the journal should hold this execution.
    fn wanted(persist: &Persist, execution: &Execution) -> bool {
        !execution.transient && (execution.done.is_none() || persist.settled)
    }

    fn exec_record(key: &Text, execution: &Execution) -> Record {
        Record::Exec { key: key.clone(), request: execution.request, fingerprint: execution.fingerprint, attempt: execution.attempt, attempts: execution.attempts, done: execution.done.clone() }
    }

    /// The net difference between durable state now and what the journal was
    /// last told. A message that was put and removed since the last harvest
    /// appears in neither list; an execution that was claimed and settled
    /// appears at most once, in its final state, or not at all.
    pub fn harvest(&mut self) -> Vec<Record> {
        let mut records = Vec::new();
        if !self.epoch_written {
            self.epoch_written = true;
            records.push(Record::Epoch(self.epoch));
        }
        for (name, id) in std::mem::take(&mut self.fresh) {
            if let Some(message) = self.queues.get_mut(&name).and_then(|queue| queue.messages.get_mut(&id)) {
                if !message.on_disk {
                    message.on_disk = true;
                    records.push(Record::Put { queue: name, id, command: message.command.clone() });
                }
            }
        }
        for (queue, id) in std::mem::take(&mut self.gone) {
            records.push(Record::Del { queue, id });
        }
        for (queue, id, command) in std::mem::take(&mut self.buried) {
            records.push(Record::Dead { queue, id, command });
        }
        // Forgetting comes first. A key forgotten when its dedup window ran
        // out may have been started again since, and that new execution is
        // what the journal must end up saying.
        for key in std::mem::take(&mut self.forgotten) {
            records.push(Record::Forget { key });
        }
        for key in std::mem::take(&mut self.touched) {
            let Some(execution) = self.executions.get_mut(&key) else { continue };
            // A key can be listed twice: its execution expired while listed,
            // and a new one under the same key was listed again. `dirty`
            // belongs to the execution, so the second listing finds it clean
            // and states nothing twice.
            if !execution.dirty {
                continue;
            }
            let wanted = Self::wanted(&self.persist, execution);
            if wanted {
                records.push(Self::exec_record(&key, execution));
            } else if execution.on_disk {
                records.push(Record::Forget { key: key.clone() });
            }
            execution.dirty = false;
            execution.on_disk = wanted;
        }
        let dropped = self.drop_through;
        records.extend(std::mem::take(&mut self.unwritten).into_iter().filter(|event| event.position > dropped).map(Record::Event));
        records
    }

    /// Every statement needed to rebuild durable state, with nothing
    /// superseded. Afterwards the journal and memory agree.
    pub fn snapshot(&mut self) -> Vec<Record> {
        let mut records = vec![Record::Epoch(self.epoch)];
        self.epoch_written = true;
        for (name, queue) in &mut self.queues {
            if self.persist.transient_queues.contains(name) {
                continue;
            }
            records.extend(queue.snapshot().into_iter().map(|(id, command)| Record::Put { queue: name.clone(), id, command }));
            records.extend(queue.dead.iter().map(|(id, command)| Record::Dead { queue: name.clone(), id: *id, command: command.clone() }));
        }
        for (key, execution) in &mut self.executions {
            execution.dirty = false;
            execution.on_disk = Self::wanted(&self.persist, execution);
            if execution.on_disk {
                records.push(Self::exec_record(key, execution));
            }
        }
        records.extend(self.order.iter().filter(|event| self.persist.events.wants(event)).cloned().map(Record::Event));
        self.fresh.clear();
        self.gone.clear();
        self.buried.clear();
        self.touched.clear();
        self.forgotten.clear();
        self.unwritten.clear();
        records
    }

    /// How many records a snapshot would hold right now: exactly what
    /// [`State::snapshot`] would write, and nothing it would not. Settled
    /// executions and events the policy does not keep live only in memory;
    /// counting them would make a journal full of superseded statements look
    /// healthy and stop it from ever being folded.
    ///
    /// A full scan. The engine asks only now and then; see `Core::land`.
    pub fn live_records(&self) -> u64 {
        let messages: usize = self.queues.iter().filter(|(name, _)| !self.persist.transient_queues.contains(*name)).map(|(_, queue)| queue.messages.len() + queue.dead.len()).sum();
        let executions = self.executions.values().filter(|execution| Self::wanted(&self.persist, execution)).count();
        let events = self.order.iter().filter(|event| self.persist.events.wants(event)).count();
        (1 + messages + executions + events) as u64
    }

    pub fn retained(&self) -> (u64, u64) {
        (self.order.len() as u64, self.bytes as u64)
    }

    // ================================================================= events

    /// Takes an event into the log's bookkeeping and applies retention.
    fn remember(&mut self, event: Arc<Envelope>, now: u64, new: bool) {
        self.next_position = self.next_position.max(event.position + 1);
        self.bytes += cost(&event);
        self.ids.insert(event.event_id, event.clone());
        self.order.push_back(event.clone());
        if new && self.persist.events.wants(&event) {
            self.unwritten.push(event.clone());
        }
        self.staged.push(event);

        // Bounded here, as it grows — never by a sweep.
        let horizon = now.saturating_sub(self.retention.max_age.as_millis() as u64);
        while let Some(oldest) = self.order.front() {
            if self.order.len() <= self.retention.max_events && self.bytes <= self.retention.max_bytes && oldest.created_at >= horizon {
                break;
            }
            let oldest = self.order.pop_front().expect("front was just observed");
            self.bytes -= cost(&oldest);
            self.drop_through = oldest.position;
            if self.ids.get(&oldest.event_id).is_some_and(|known| known.position == oldest.position) {
                self.ids.remove(&oldest.event_id);
            }
        }
    }

    /// Appends an event, or returns the stored one if this id is already in
    /// the log. A new fact is also put on every queue the reaction table names
    /// for it — in the same step, so the two cannot disagree.
    pub fn append(&mut self, mut event: Envelope, now: u64) -> Arc<Envelope> {
        if let Some(existing) = self.ids.get(&event.event_id) {
            self.duplicates += 1;
            return existing.clone();
        }
        event.position = self.next_position;
        if event.created_at == 0 {
            event.created_at = now;
        }
        let event = Arc::new(event);
        self.remember(event.clone(), now, true);

        // A command row records that work was handed out. Reacting to it would
        // answer the engine's own dispatch, for ever.
        if event.kind != Kind::Command {
            let reactions = self.reactions.clone();
            if let Some(queues) = reactions.get(&event.action) {
                for queue in queues {
                    // Unique per (reactor, fact), identical on a repeat: a
                    // second reactor gets its own copy, a redelivery joins.
                    let identity = EventId::reaction(queue, event.event_id);
                    let reaction = Command {
                        event_id: identity,
                        created_at: now,
                        action: event.action.clone(),
                        transaction_key: identity.to_string().into(),
                        channel: event.channel.clone(),
                        audience: Audience::Worker,
                        reply_channel: event.reply_channel.clone(),
                        session_id: event.session_id.clone(),
                        run_id: event.run_id.clone(),
                        turn_id: event.turn_id.clone(),
                        correlation_id: Some(event.correlation_id.clone()),
                        principal: event.principal.clone(),
                        payload: event.payload.clone(),
                    };
                    self.enqueue(queue, Arc::new(reaction));
                }
            }
        }
        event
    }

    // ================================================================== queue

    pub fn enqueue(&mut self, queue: &Text, command: Arc<Command>) -> u64 {
        let id = self.next_message;
        self.next_message += 1;
        self.enqueued += 1;
        self.queues.entry(queue.clone()).or_default().put(id, command, false);
        // Work on a transient queue is never mentioned to the journal.
        if !self.persist.transient_queues.contains(queue) {
            self.fresh.push((queue.clone(), id));
        }
        id
    }

    fn remove(&mut self, queue: &Text, id: u64) -> bool {
        let Some(message) = self.queues.get_mut(queue).and_then(|queue| queue.remove(id)) else { return false };
        // Only a message the journal knows about needs un-saying.
        if message.on_disk {
            self.gone.push((queue.clone(), id));
        }
        true
    }

    fn bury(&mut self, queue: &Text, id: u64) {
        let keep = self.retention.dead_letters;
        let Some(owner) = self.queues.get_mut(queue) else { return };
        let Some(message) = owner.remove(id) else { return };
        owner.bury(id, message.command.clone(), keep);
        if !self.persist.transient_queues.contains(queue) {
            self.buried.push((queue.clone(), id, message.command));
        }
    }

    /// Deletes a message outright. For queue use without executions.
    pub fn ack(&mut self, queue: &Text, id: u64) -> bool {
        self.remove(queue, id)
    }

    /// Returns an in-flight message to its queue without an execution.
    pub fn nack(&mut self, queue: &str, id: u64, delay: u64, now: u64) -> bool {
        self.queues.get_mut(queue).is_some_and(|queue| queue.give_back(id, delay, now))
    }

    pub fn next_deadline(&self) -> Option<u64> {
        self.queues.values().filter_map(Queue::next_deadline).min()
    }

    // ============================================================= executions

    fn touch(&mut self, key: &Text) {
        if let Some(execution) = self.executions.get_mut(key) {
            if !execution.dirty && (!execution.transient || execution.on_disk) {
                execution.dirty = true;
                self.touched.push(key.clone());
            }
        }
    }

    /// Ends an execution and records its one result, addressed to whoever
    /// sent the command.
    fn settle(&mut self, command: &Envelope, ok: bool, payload: Bytes, now: u64) {
        let key = &command.transaction_key;
        let Some(execution) = self.executions.get_mut(key) else { return };
        execution.done = Some((ok, payload.clone(), now));
        execution.held = None;
        self.touch(key);
        self.append(result_event(command, ok, payload, now), now);

        // Remembered for the deduplication window, then dropped — here, as
        // the next execution settles, not by a sweep.
        self.settled.push_back((now, command.transaction_key.clone()));
        let horizon = now.saturating_sub(self.retention.settled.as_millis() as u64);
        while let Some((at, _)) = self.settled.front() {
            if *at >= horizon && self.settled.len() <= self.retention.max_settled {
                break;
            }
            let (at, key) = self.settled.pop_front().expect("front was just observed");
            let expired = self.executions.get(&key).is_some_and(|execution| execution.done.as_ref().is_some_and(|done| done.2 == at));
            if expired && self.executions.remove(&key).is_some_and(|execution| execution.on_disk) {
                self.forgotten.push(key);
            }
        }
    }

    /// Starts work on a delivered message: records the command, then decides
    /// who owns the execution. Everything that does not end in `Claimed` is
    /// finished here, inside the engine — the caller has nothing left to do.
    pub fn begin(&mut self, queue: &Text, id: u64, now: u64) -> Begin {
        let Some(message) = self.queues.get(queue).and_then(|queue| queue.messages.get(&id)) else { return Begin::Gone };
        let queued = message.command.clone();
        let command = self.append(Envelope::from_command(&queued), now);
        let key = queued.transaction_key.clone();
        let print = fingerprint(&queued.action, &queued.payload);

        enum Seen {
            New,
            Conflict,
            Done(bool, Bytes),
            Idle(u32),
            Held(Option<(Text, u64)>),
        }
        let seen = match self.executions.get(&key) {
            None => Seen::New,
            Some(execution) if execution.fingerprint != print => Seen::Conflict,
            Some(execution) => match &execution.done {
                Some((ok, payload, _)) => Seen::Done(*ok, payload.clone()),
                None if execution.held.is_some() => Seen::Held(execution.held.clone()),
                None => Seen::Idle(execution.attempts),
            },
        };

        let attempts = match seen {
            Seen::New => 1,
            Seen::Idle(attempts) if attempts < self.max_attempts => attempts + 1,
            Seen::Idle(_) => {
                // Out of attempts. The failure is recorded and the message
                // leaves the live queue.
                self.settle(&command, false, Bytes::from_static(b"processing_permanent_failure"), now);
                self.bury(queue, id);
                return Begin::Exhausted;
            }
            Seen::Conflict => {
                let conflict = EventId::derive(&[b"conflict", command.event_id.as_bytes()]);
                let event = Envelope::follow(&command, conflict, Kind::Failure, format!("{}.conflict", command.action).into(), Bytes::from_static(b"idempotency_conflict"), now);
                self.append(event, now);
                self.remove(queue, id);
                return Begin::Conflict;
            }
            Seen::Done(ok, payload) => {
                self.append(result_event(&command, ok, payload, now), now);
                self.remove(queue, id);
                return Begin::Settled;
            }
            Seen::Held(holder) => {
                // One event, one worker. A second submission of a running
                // transaction is the same event sent again, so it is dropped:
                // it does not attach itself to the execution.
                //
                // Only the message being run is kept — it is what a later
                // attempt would run, and it can come back here when a consumer
                // asked for a visibility deadline. Any other message is
                // dropped, even one carrying the very same event: nobody
                // would ever finish or return it.
                if holder.as_ref() != Some(&(queue.clone(), id)) {
                    self.remove(queue, id);
                }
                return Begin::Duplicate;
            }
        };

        let attempt = self.next_attempt;
        self.next_attempt += 1;
        let transient = self.persist.transient_queues.contains(queue);
        let execution = self.executions.entry(key.clone()).or_insert_with(|| Execution {
            request: queued.event_id,
            fingerprint: print,
            attempt,
            attempts,
            held: Some((queue.clone(), id)),
            done: None,
            transient,
            dirty: false,
            on_disk: false,
        });
        execution.attempt = attempt;
        execution.attempts = attempts;
        execution.held = Some((queue.clone(), id));
        self.touch(&key);
        Begin::Claimed { command, attempt, attempts }
    }

    fn running(execution: &Execution, attempt: u64) -> bool {
        execution.done.is_none() && execution.held.is_some() && execution.attempt == attempt
    }

    fn owned(&mut self, key: &str, attempt: u64) -> Option<&mut Execution> {
        self.executions.get_mut(key).filter(|execution| Self::running(execution, attempt))
    }

    /// Whether this attempt is still the one running the execution.
    pub fn owns(&self, key: &str, attempt: u64) -> bool {
        self.executions.get(key).is_some_and(|execution| Self::running(execution, attempt))
    }

    /// Gives the execution up so another attempt may run it after `retry` ms.
    pub fn release(&mut self, key: &str, attempt: u64, queue: &Text, id: u64, retry: u64, now: u64) -> bool {
        let Some(execution) = self.owned(key, attempt) else { return false };
        execution.held = None;
        if let Some(queue) = self.queues.get_mut(queue) {
            queue.give_back(id, retry, now);
        }
        true
    }

    /// Completes the execution, records its result and deletes the message
    /// — one step, so "write, then delete" cannot be interrupted.
    pub fn finish(&mut self, command: &Envelope, attempt: u64, outcome: Outcome, queue: &Text, id: u64, now: u64) -> bool {
        if self.owned(&command.transaction_key, attempt).is_none() {
            return false;
        }
        let (ok, payload) = match outcome {
            Outcome::Ok(payload) => (true, payload),
            Outcome::Failed(payload) => (false, payload),
        };
        self.settle(command, ok, payload, now);
        self.remove(queue, id);
        true
    }

    /// Records what a handler said — if, and only if, its attempt still owns
    /// the execution. This is the fence: a superseded attempt cannot write.
    pub fn emit(&mut self, command: &Envelope, attempt: u64, emit: Emit, now: u64) -> bool {
        if self.owned(&command.transaction_key, attempt).is_none() {
            return false;
        }
        let id = match &emit.key {
            Some(key) => EventId::derive(&[b"fact", command.stream.as_bytes(), key.as_bytes()]),
            // Nothing to derive an identity from, and none is needed: a burst
            // of deltas is fenced by ownership, not deduplicated. Its place
            // in the log is unique for ever and costs no hashing.
            None => EventId::positional(self.next_position),
        };
        let mut event = Envelope::follow(command, id, emit.kind, emit.action, emit.payload, now);
        event.audience = emit.audience;
        self.append(event, now);
        true
    }
}

#[cfg(test)]
mod tests;
