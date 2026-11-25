//! Streamed bodies: a work connection's follow-up body received straight to
//! disk (2.6, items 4 and 5).
//!
//! Under memory pressure a work connection does not stop reading: the server
//! reads a follow-up's body from the socket into the body lane through a
//! [`SpoolWrite`], and hands the engine the resulting [`Spooled`] with the
//! completion. The ledger's `Register` then points at it (`RegisterAt`), or —
//! for a volatile follow-up past the reserve (B15) — the event stays volatile
//! with its body on disk, journaled nowhere and dropped at a restart.
//!
//! Many connection owners spool at once, each into its own reserved range of
//! the current body segment through its own file handle, so no write waits on
//! another. A body that never reaches the ledger (the connection died, the
//! completion was refused) is dead bytes; its segment goes once nothing in it
//! is live and nothing is still on its way.

use super::codec::{body_frame_header, HEADER, SEGMENT_HEADER};
use super::segment::{create_segment, segment_path};
use super::{Lane, Location, SegmentState};
use crc32fast::Hasher;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

/// The spool's reservations.
///
/// | state | key | lifetime | bound | cleared by |
/// | --- | --- | --- | --- | --- |
/// | current segment and its size | the spool | until it rolls | one | `Spool::receive` |
/// | bodies on their way | body segment → count | `receive` ~ the `Spooled` is adopted or dropped | bodies being received or handed over | `Hold::drop` |
/// | sizes not yet reported | body segment | grown ~ `Spool::report` | body segments grown since | `Spool::report` |
#[derive(Default)]
struct Reservations {
    current: Option<(u32, u64)>,
    outstanding: HashMap<u32, u32>,
    grown: HashMap<u32, u64>,
}

struct Shared {
    dir: PathBuf,
    numbers: Arc<AtomicU32>,
    segment_bytes: u64,
    reservations: parking_lot::Mutex<Reservations>,
}

/// Receives streamed bodies into the body lane of one log directory. Cheap to
/// clone; every connection owner may hold one.
#[derive(Clone)]
pub struct Spool {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for Spool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Spool")
    }
}

/// A body on its way: keeps its segment from being removed until the ledger
/// has it or it is dropped.
struct Hold {
    shared: Arc<Shared>,
    segment: u32,
}

impl Drop for Hold {
    fn drop(&mut self) {
        let mut reservations = self.shared.reservations.lock();
        if let Some(count) = reservations.outstanding.get_mut(&self.segment) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                reservations.outstanding.remove(&self.segment);
            }
        }
    }
}

impl Spool {
    pub(super) fn new(dir: PathBuf, numbers: Arc<AtomicU32>, segment_bytes: u64) -> Self {
        Spool { shared: Arc::new(Shared { dir, numbers, segment_bytes, reservations: parking_lot::Mutex::new(Reservations::default()) }) }
    }

    /// Reserves room for a body of `len` bytes and opens it for writing.
    /// Blocking: call it where blocking is allowed.
    pub fn receive(&self, len: u32) -> io::Result<SpoolWrite> {
        let shared = &self.shared;
        let frame = HEADER + u64::from(len);
        let (segment, at) = {
            let mut reservations = shared.reservations.lock();
            let roll = reservations.current.is_none_or(|(_, bytes)| bytes > SEGMENT_HEADER && bytes + frame > shared.segment_bytes);
            if roll {
                let number = shared.numbers.fetch_add(1, Ordering::Relaxed);
                create_segment(&shared.dir, Lane::Body, number, 0)?;
                reservations.current = Some((number, SEGMENT_HEADER));
            }
            let Some((segment, bytes)) = reservations.current.as_mut() else { return Err(io::Error::other("no body segment")) };
            let (segment, at) = (*segment, *bytes);
            *bytes += frame;
            let size = *bytes;
            reservations.grown.insert(segment, size);
            *reservations.outstanding.entry(segment).or_default() += 1;
            (segment, at)
        };
        let hold = Hold { shared: shared.clone(), segment };
        let mut file = OpenOptions::new().write(true).open(segment_path(&shared.dir, segment))?;
        file.seek(SeekFrom::Start(at + HEADER))?;
        Ok(SpoolWrite { file, at, len, written: 0, crc: Hasher::new(), hold })
    }

    /// Whether body segment `number` takes no more bodies and none is still on
    /// its way to the ledger: then it may be removed once nothing in it is live.
    pub fn idle(&self, number: u32) -> bool {
        let reservations = self.shared.reservations.lock();
        reservations.current.is_none_or(|(current, _)| current != number) && !reservations.outstanding.contains_key(&number)
    }

    /// Body segments that grew since the last call, for the disk accounting.
    pub fn report(&self) -> Vec<SegmentState> {
        let mut reservations = self.shared.reservations.lock();
        reservations.grown.drain().map(|(number, bytes)| SegmentState { number, lane: Lane::Body, bytes, first: 0, removed: false }).collect()
    }
}

/// One body being received: write it in pieces, then finish.
pub struct SpoolWrite {
    file: File,
    at: u64,
    len: u32,
    written: u64,
    crc: Hasher,
    hold: Hold,
}

impl SpoolWrite {
    /// The body's length, as reserved.
    pub fn len(&self) -> u32 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Bytes still to write.
    pub fn remaining(&self) -> u64 {
        u64::from(self.len) - self.written
    }

    /// Appends the next piece. Blocking.
    pub fn write(&mut self, piece: &[u8]) -> io::Result<()> {
        if piece.len() as u64 > self.remaining() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "more bytes than the body's length"));
        }
        self.file.write_all(piece)?;
        self.crc.update(piece);
        self.written += piece.len() as u64;
        Ok(())
    }

    /// Writes the frame header and makes the body durable. Blocking.
    pub fn finish(mut self) -> io::Result<Spooled> {
        if self.remaining() != 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "the body is not complete"));
        }
        let crc = self.crc.clone().finalize();
        self.file.seek(SeekFrom::Start(self.at))?;
        self.file.write_all(&body_frame_header(self.len, crc))?;
        self.file.sync_data()?;
        let at = Location { segment: self.hold.segment, offset: self.at + HEADER, len: self.len, crc };
        Ok(Spooled { at, hold: Arc::new(self.hold) })
    }
}

/// A body on disk, not yet adopted by the ledger. Hand it to the engine with
/// the completion whose follow-up it is (`Call::Complete::spooled`).
#[derive(Clone)]
pub struct Spooled {
    pub at: Location,
    hold: Arc<Hold>,
}

impl Spooled {
    /// The body segment's number, for diagnostics.
    pub fn segment(&self) -> u32 {
        self.hold.segment
    }
}

impl PartialEq for Spooled {
    fn eq(&self, other: &Self) -> bool {
        self.at == other.at
    }
}

impl Eq for Spooled {}

impl std::fmt::Debug for Spooled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Spooled").field("at", &self.at).finish()
    }
}
