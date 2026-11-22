//! Durability. Not a method: the engine writes it, nobody calls it.
//!
//! The journal exists so that a restart can carry on: it holds the claimable
//! events still waiting or claimed, the generation, and the names they use.
//! Dispatch events are never written; they are gone the moment they are sent.
//!
//! ## Net change, keyed by work key
//!
//! | record | means |
//! | --- | --- |
//! | `Register` | this registration exists, with this envelope (never the claim block) |
//! | `Claim` | this registration was claimed (only its work key) |
//! | `Remove` | this registration is gone: completed or deleted |
//! | `Reclaim` | this registration missed its deadline: new id, this retry count, waiting again |
//! | `Generation` | the server now runs in this generation |
//!
//! Records name registrations by work key, which is unique per registration,
//! so replay never has to look anything up by id. All the net change one
//! operation causes — a completion's removal and all of its follow-ups — is
//! written in one batch frame, and replay stops at the last complete frame,
//! so a torn write leaves either all of an operation or none of it.

mod codec;
mod file;

use crate::envelope::{Envelope, EventId, WorkKey};
use async_trait::async_trait;
use std::io;

pub use file::FileJournal;

/// One statement about durable state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Record {
    Register(Envelope),
    Claim(WorkKey),
    Remove(WorkKey),
    Reclaim { key: WorkKey, id: EventId, retry: u16 },
    Generation(u32),
}

/// What a journal held when it was opened.
#[derive(Default, Debug)]
pub struct Recovered {
    pub records: Vec<Record>,
}

/// Where the engine's records go. One task owns it; writes never overlap.
#[async_trait]
pub trait Journal: Send + 'static {
    /// Writes `records` as one frame and makes them durable. Any error is
    /// terminal: the engine stops rather than retry, because after a failed
    /// write the medium is in a state no retry can reason about.
    async fn append(&mut self, records: Vec<Record>) -> io::Result<()>;

    /// Replaces the whole journal with `records`, a snapshot of the live state.
    /// Atomic: afterwards the journal is either the old one or the new one.
    async fn rewrite(&mut self, records: Vec<Record>) -> io::Result<()>;
}

/// Keeps nothing. For tests and for measuring the engine without a disk.
pub struct Volatile;

#[async_trait]
impl Journal for Volatile {
    async fn append(&mut self, _records: Vec<Record>) -> io::Result<()> {
        Ok(())
    }

    async fn rewrite(&mut self, _records: Vec<Record>) -> io::Result<()> {
        Ok(())
    }
}

pub use codec::{decode_frames, encode_frame, FrameNames, MAGIC};

#[cfg(test)]
mod tests;
