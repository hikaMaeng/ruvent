//! Durability, as a plugin.
//!
//! ## What is written, and why only that
//!
//! A journal exists so that a restart can carry on. So it holds what a
//! restart needs and nothing else:
//!
//! | kept | until | because |
//! | --- | --- | --- |
//! | queued work | it is finished | it is the one thing still owed |
//! | execution claims and attempt counts | the execution settles | a message that kills the process must still run out of attempts |
//! | settled results | only if asked, for the deduplication window | a resubmission across a restart then does not run the work again |
//! | events | only on the channels the policy names | the machine does not need them: the work a fact causes is already queued |
//!
//! Progress events — streamed deltas — are never written. They are the bulk of
//! the volume, they are worth the least, and a re-run would say something
//! different anyway.
//!
//! Visibility deadlines and "who is running this right now" are not written
//! either. After a restart every unfinished message is simply owed again.
//!
//! ## Net change, not history
//!
//! The engine does not log each operation. When a write is due it works out
//! how durable state now differs from what the journal last said, and writes
//! only that difference. Work that arrived and finished between two writes
//! costs no disk at all, and a backlog that did not change is not rewritten.
//! When superseded statements come to outnumber live ones, the journal is
//! folded into a snapshot.
//!
//! ## Writes cannot land out of turn
//!
//! Records are numbered. A batch says which record number it follows (`prev`)
//! and which it ends at (`through`), and a journal must refuse a batch that
//! does not follow what it last wrote. That refusal — not a timer, and not
//! the caller's good behaviour — is what guarantees one write can never land
//! on top of, or ahead of, another.

mod file;

use crate::envelope::{Command, Envelope, EventId, Text};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::io;
use std::sync::Arc;

pub use file::FileJournal;

/// One statement about durable state.
///
/// Records are declarations, not a history: "this message exists", "this
/// execution is in this state". Replaying them in order and letting later
/// ones overwrite earlier ones rebuilds the state, and a snapshot is simply
/// the same statements with nothing superseded.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Record {
    /// A new incarnation of the engine. Made durable before anything else.
    Epoch(u64),
    /// This message is owed.
    Put { queue: Text, id: u64, command: Arc<Command> },
    /// This message is no longer owed.
    Del { queue: Text, id: u64 },
    /// This message ran out of attempts.
    Dead { queue: Text, id: u64, command: Arc<Command> },
    /// This execution exists, in this state. Replaces whatever was known of it.
    Exec { key: Text, request: EventId, fingerprint: u128, attempt: u64, attempts: u32, done: Option<(bool, Bytes, u64)> },
    /// This execution is no longer remembered.
    Forget { key: Text },
    /// An event the persistence policy chose to keep.
    Event(Arc<Envelope>),
}

/// What a journal held when it was opened.
#[derive(Default)]
pub struct Recovered {
    pub records: Vec<Record>,
    /// The record number the journal ends at. The next batch must follow it.
    pub through: u64,
}

#[async_trait]
pub trait Journal: Send + 'static {
    /// Writes records `prev+1 ..= through` and makes them durable.
    ///
    /// Must fail without writing if `prev` is not where this journal ends.
    /// Any error is terminal: the engine stops rather than retry, because a
    /// failed write leaves the medium in a state no retry can reason about.
    async fn append(&mut self, prev: u64, through: u64, records: &[Record]) -> io::Result<()>;

    /// Replaces the whole journal with `records`, a snapshot of the state as
    /// of record number `at`. Atomic: afterwards the journal is either the
    /// old one or the new one, and it ends at `at`.
    async fn rewrite(&mut self, at: u64, records: &[Record]) -> io::Result<()>;
}

/// Keeps nothing. For tests and for measuring the engine without a disk.
pub struct Volatile;

#[async_trait]
impl Journal for Volatile {
    async fn append(&mut self, _prev: u64, _through: u64, _records: &[Record]) -> io::Result<()> {
        Ok(())
    }

    async fn rewrite(&mut self, _at: u64, _records: &[Record]) -> io::Result<()> {
        Ok(())
    }
}
