//! The readable side of the log.
//!
//! Events arrive here in position order, under one write lock per batch. A
//! reader therefore never sees position `p` without everything before it —
//! which is what lets a subscription's whole cursor be a single number.
//!
//! Delivery is a read, not a hand-off. A subscription holds a position and
//! pulls forward from it; the log itself is the buffer. So there is no
//! per-connection mailbox to overflow, no replay-to-live handover to get
//! wrong, and a slow reader delays nobody but itself.
//!
//! The log is bounded. The engine decides what has aged out as it appends and
//! tells this side how far to drop; nothing sweeps on a timer.

use crate::envelope::{Audience, Envelope, Text};
use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use tokio::sync::Notify;
use tokio::time::Instant;

#[derive(Default)]
struct ChannelLog {
    events: VecDeque<Arc<Envelope>>,
    wakers: Vec<Weak<Notify>>,
}

#[derive(Default)]
pub(crate) struct LogIndex {
    /// Every retained event, oldest first. Retention removes from the front.
    order: VecDeque<Arc<Envelope>>,
    streams: HashMap<Text, VecDeque<Arc<Envelope>>>,
    channels: HashMap<Text, ChannelLog>,
    head: u64,
    /// Highest position retention has dropped.
    dropped_through: u64,
    /// Where this incarnation's positions begin, and the last position the
    /// journal gave back from before it.
    epoch_floor: u64,
    recovered_head: u64,
}

impl LogIndex {
    pub fn publish(&mut self, event: Arc<Envelope>) {
        self.head = event.position;
        self.order.push_back(event.clone());
        self.streams.entry(event.stream.clone()).or_default().push_back(event.clone());
        // Worker-to-worker traffic is never indexed for clients at all.
        if event.audience == Audience::Client {
            let channel = self.channels.entry(event.channel.clone()).or_default();
            channel.events.push_back(event);
            channel.wakers.retain(|waker| match waker.upgrade() {
                // `notify_one` stores a permit when nobody is waiting, so a
                // wakeup between a reader's read and its wait is not lost.
                Some(notify) => {
                    notify.notify_one();
                    true
                }
                None => false,
            });
        }
    }

    /// Marks the start of a new incarnation, after recovery.
    pub fn begin_epoch(&mut self, floor: u64) {
        self.epoch_floor = floor;
        self.recovered_head = self.head;
    }

    /// Whether a reader resuming from `position` may have missed events that
    /// no longer exist: retention dropped them, or they were published by an
    /// earlier incarnation and did not survive the restart.
    fn missed(&self, position: u64) -> bool {
        position != 0 && (position < self.dropped_through || (position < self.epoch_floor && position > self.recovered_head))
    }

    /// Drops every event at or before `through`. Each index is in position
    /// order, so the event leaving the front of the log is at the front of
    /// its stream and of its channel too.
    pub fn drop_through(&mut self, through: u64) {
        while self.order.front().is_some_and(|event| event.position <= through) {
            let event = self.order.pop_front().expect("front was just observed");
            self.dropped_through = event.position;
            if let Some(stream) = self.streams.get_mut(&event.stream) {
                stream.pop_front();
                if stream.is_empty() {
                    self.streams.remove(&*event.stream);
                }
            }
            if event.audience == Audience::Client {
                if let Some(channel) = self.channels.get_mut(&event.channel) {
                    channel.events.pop_front();
                    if channel.events.is_empty() {
                        channel.wakers.retain(|waker| waker.strong_count() > 0);
                        if channel.wakers.is_empty() {
                            self.channels.remove(&*event.channel);
                        }
                    }
                }
            }
        }
    }
}

#[derive(Default)]
pub(crate) struct Counters {
    pub events: AtomicU64,
    pub duplicates: AtomicU64,
    pub enqueued: AtomicU64,
    pub delivered: AtomicU64,
    pub writes: AtomicU64,
    pub compactions: AtomicU64,
    pub produced: AtomicU64,
    pub unwritten: AtomicU64,
    pub retained_events: AtomicU64,
    pub retained_bytes: AtomicU64,
    pub progress_dropped: AtomicU64,
    pub wakeups: AtomicU64,
}

/// A point-in-time copy of the engine's counters.
#[derive(Clone, Copy, Debug, Default)]
pub struct Stats {
    /// Events appended to the log since this engine started.
    pub events: u64,
    /// Appends that named an event id already in the log and stored nothing.
    pub duplicates: u64,
    /// Messages put on a queue, including the ones a reaction put there.
    pub enqueued: u64,
    /// Messages handed to a consumer. Redeliveries count again.
    pub delivered: u64,
    /// Journal records written since this engine started. Not the same as
    /// events: by default no event is written, and only the net change in
    /// owed work is.
    pub journal_records: u64,
    /// Journal append writes completed.
    pub journal_writes: u64,
    /// Times the journal was rewritten as a snapshot.
    pub compactions: u64,
    /// Changes the journal has not been told about, as of the engine's last
    /// batch — an upper bound on what a crash right now would lose. Changes
    /// that cancel each other out are counted until the next write notices.
    pub journal_lag: u64,
    /// Events currently held in memory for readers.
    pub retained_events: u64,
    /// Approximate bytes those events occupy.
    pub retained_bytes: u64,
    /// Progress emissions dropped because the engine's inbox was full.
    pub progress_dropped: u64,
    /// Times the engine task has woken. It wakes for an operation, a journal
    /// write reporting back, or a real deadline — so on an idle engine this
    /// number does not move.
    pub wakeups: u64,
}

pub(crate) struct Shared {
    pub log: RwLock<LogIndex>,
    pub counters: Counters,
    pub failure: parking_lot::Mutex<Option<String>>,
    wall: u64,
    epoch: Instant,
}

impl Shared {
    pub fn new() -> Self {
        let wall = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |since| since.as_millis() as u64);
        Self { log: RwLock::default(), counters: Counters::default(), failure: parking_lot::Mutex::new(None), wall, epoch: Instant::now() }
    }

    /// Milliseconds since the Unix epoch, advanced by the runtime's clock so a
    /// paused test clock moves deadlines with it.
    pub fn now(&self) -> u64 {
        self.wall + self.epoch.elapsed().as_millis() as u64
    }

    /// The runtime instant a millisecond timestamp from [`Shared::now`] names.
    pub fn instant(&self, at: u64) -> Instant {
        self.epoch + std::time::Duration::from_millis(at.saturating_sub(self.wall))
    }

    pub fn stats(&self) -> Stats {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        let c = &self.counters;
        Stats {
            events: load(&c.events),
            duplicates: load(&c.duplicates),
            enqueued: load(&c.enqueued),
            delivered: load(&c.delivered),
            journal_records: load(&c.produced),
            journal_writes: load(&c.writes),
            compactions: load(&c.compactions),
            journal_lag: load(&c.unwritten),
            retained_events: load(&c.retained_events),
            retained_bytes: load(&c.retained_bytes),
            progress_dropped: load(&c.progress_dropped),
            wakeups: load(&c.wakeups),
        }
    }

    pub fn read_stream(&self, stream: &str, after: u64, limit: usize) -> Vec<Arc<Envelope>> {
        let log = self.log.read();
        let Some(events) = log.streams.get(stream) else { return Vec::new() };
        let start = events.partition_point(|event| event.position <= after);
        events.iter().skip(start).take(limit).cloned().collect()
    }
}

/// Where a subscription starts reading.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum From {
    /// The oldest retained event.
    Start,
    /// Only what is appended from now on.
    Now,
    /// Everything after this position — a cursor handed back by a client.
    Position(u64),
}

/// A read position over a set of channels.
pub struct Subscription {
    shared: Arc<Shared>,
    notify: Arc<Notify>,
    channels: Vec<String>,
    cursor: u64,
    lagged: bool,
}

impl Subscription {
    pub(crate) fn open(shared: Arc<Shared>, mut channels: Vec<String>, from: From) -> Self {
        channels.sort();
        channels.dedup();
        let notify = Arc::new(Notify::new());
        let (cursor, lagged) = {
            let mut log = shared.log.write();
            for name in &channels {
                let channel = log.channels.entry(Text::from(name.as_str())).or_default();
                channel.wakers.retain(|waker| waker.strong_count() > 0);
                channel.wakers.push(Arc::downgrade(&notify));
            }
            match from {
                From::Start => (0, false),
                From::Now => (log.head, false),
                From::Position(position) => (position, log.missed(position)),
            }
        };
        Self { shared, notify, channels, cursor, lagged }
    }

    /// The position of the last event handed out. Give it back as
    /// [`From::Position`] to resume exactly here.
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// True if events after this subscription's cursor may have been dropped
    /// before it read them. The reader should rebuild from its own source of
    /// truth rather than trust the stream to be complete.
    pub fn lagged(&self) -> bool {
        self.lagged
    }

    /// Whatever is available right now, up to `max`, in commit order across
    /// all subscribed channels.
    pub fn poll(&mut self, max: usize) -> Vec<Arc<Envelope>> {
        let mut found = Vec::new();
        {
            let log = self.shared.log.read();
            if log.missed(self.cursor) {
                self.lagged = true;
            }
            for name in &self.channels {
                let Some(channel) = log.channels.get(name.as_str()) else { continue };
                let start = channel.events.partition_point(|event| event.position <= self.cursor);
                found.extend(channel.events.iter().skip(start).take(max).cloned());
            }
        }
        // Each channel offered its first `max`, so the first `max` overall are
        // all present: merging and truncating loses nothing before the cut.
        if self.channels.len() > 1 {
            found.sort_by_key(|event| event.position);
            found.truncate(max);
        }
        if let Some(last) = found.last() {
            self.cursor = last.position;
        }
        found
    }

    /// Waits until at least one event is available. Cancel-safe: dropping the
    /// future consumes nothing. Woken by the append itself; never polls.
    pub async fn next(&mut self, max: usize) -> Vec<Arc<Envelope>> {
        loop {
            let found = self.poll(max);
            if !found.is_empty() {
                return found;
            }
            self.notify.notified().await;
        }
    }
}
