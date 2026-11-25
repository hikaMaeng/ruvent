//! The unacknowledged buffer of one numbered connection (2.3, 2.9).
//!
//! Every numbered call — a registration or deletion on a production line, a
//! completion on a work line — is kept here under its sequence number until
//! the cumulative `Committed` reaches it. After a reconnection `Welcome.resume`
//! says what the server applied: what is at or below it is dropped, the rest
//! is sent again under the same numbers. When the server lost this producer's
//! record (`resume` below the first kept number − 1: a restart without a
//! journal, an entry evicted at S15) the rest is numbered again from
//! `resume + 1`, so no gap reaches the server.
//!
//! | key | sequence number |
//! | --- | --- |
//! | scope | one line (server, role) |
//! | lifetime | sent ~ `Committed` reached it |
//! | bound | bytes, S8 (the owner waits while [`Unacked::bytes`] is at its limit) |
//! | cleared by | [`Unacked::ack`], [`Unacked::resume`], [`Unacked::take_all`] |

use std::collections::VecDeque;

/// One numbered call kept for retransmission.
#[derive(Debug)]
pub struct Entry<T> {
    pub seq: u64,
    /// What it counts against the byte bound.
    pub bytes: u64,
    pub item: T,
}

/// What a `Welcome.resume` did to the buffer.
#[derive(Debug, PartialEq, Eq)]
pub struct Resumed {
    /// Entries the server had applied: dropped, now committed.
    pub dropped: usize,
    /// The server had lost this producer's record: the rest was numbered again.
    pub renumbered: bool,
}

#[derive(Debug)]
pub struct Unacked<T> {
    entries: VecDeque<Entry<T>>,
    next: u64,
    bytes: u64,
}

impl<T> Default for Unacked<T> {
    fn default() -> Self {
        Unacked { entries: VecDeque::new(), next: 1, bytes: 0 }
    }
}

impl<T> Unacked<T> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Keeps `item` under the next number and returns it.
    pub fn push(&mut self, item: T, bytes: u64) -> u64 {
        let seq = self.next;
        self.next += 1;
        self.bytes = self.bytes.saturating_add(bytes);
        self.entries.push_back(Entry { seq, bytes, item });
        seq
    }

    /// The cumulative `Committed{seq}`: drops and returns everything at or below it.
    pub fn ack(&mut self, seq: u64) -> Vec<Entry<T>> {
        let mut done = Vec::new();
        while self.entries.front().is_some_and(|entry| entry.seq <= seq) {
            if let Some(entry) = self.entries.pop_front() {
                self.bytes = self.bytes.saturating_sub(entry.bytes);
                done.push(entry);
            }
        }
        done
    }

    /// `Welcome.resume`. Returns the dropped entries and whether the rest was
    /// numbered again; afterwards [`Unacked::iter`] is what to send again, in order.
    pub fn resume(&mut self, resume: u64) -> (Vec<Entry<T>>, Resumed) {
        let dropped = self.ack(resume);
        let renumbered = self.entries.front().is_some_and(|entry| entry.seq != resume + 1);
        if renumbered {
            for (offset, entry) in self.entries.iter_mut().enumerate() {
                entry.seq = resume + 1 + offset as u64;
            }
        }
        self.next = resume + 1 + self.entries.len() as u64;
        let resumed = Resumed { dropped: dropped.len(), renumbered };
        (dropped, resumed)
    }

    /// Everything kept, in number order.
    pub fn iter(&self) -> impl Iterator<Item = &Entry<T>> {
        self.entries.iter()
    }

    /// Empties the buffer (the entries go to another server, 2.9 `draining`).
    pub fn take_all(&mut self) -> Vec<Entry<T>> {
        self.bytes = 0;
        self.entries.drain(..).collect()
    }

    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The number the next push gets.
    pub fn next(&self) -> u64 {
        self.next
    }
}

#[cfg(test)]
mod tests;
