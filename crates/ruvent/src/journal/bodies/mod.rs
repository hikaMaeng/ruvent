//! Reading a payload back from its segment (2.2, 2.5: the read path).
//!
//! The engine never reads a segment itself: a payload that is not in memory
//! is read here, on a blocking thread, and handed back to the engine's inbox.
//! A read opens the segment for its own use, so a segment being read is never
//! held open by anything else; the engine pins it against removal while the
//! read is out.

use super::codec::invalid;
use super::segment::segment_path;
use super::{Bodies, Location};
use bytes::Bytes;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::PathBuf;

/// Reads payloads from the segments of one log directory.
#[derive(Clone, Debug)]
pub struct SegmentBodies {
    dir: PathBuf,
}

impl SegmentBodies {
    pub fn new(dir: PathBuf) -> Self {
        SegmentBodies { dir }
    }
}

impl Bodies for SegmentBodies {
    fn read(&self, at: &Location) -> io::Result<Bytes> {
        let mut file = File::open(segment_path(&self.dir, at.segment))?;
        file.seek(SeekFrom::Start(at.offset))?;
        let mut payload = vec![0; at.len as usize];
        file.read_exact(&mut payload)?;
        if crc32fast::hash(&payload) != at.crc {
            return Err(invalid(format!("segment {} byte {}: a payload of {} bytes fails its CRC", at.segment, at.offset, at.len)));
        }
        Ok(Bytes::from(payload))
    }
}
