//! Journal file format, version 2.
//!
//! ```text
//! "ruvent\0" 0x02                                                        file header, once
//! [len u32][crc32 u32]  [kind u8][prev u64][through u64][count u32][record]…
//!                       '------------------ covered by crc ------------------'
//! record = [tag u8] then
//!   1 Register   envelope body, journal layout
//!   2 Claim      work key 8
//!   3 Remove     work key 8
//!   4 Reclaim    work key 8, id 16, retry u16
//!   5 Generation u32
//!   6 Name       number u32, length u16, UTF-8
//! ```
//!
//! Every file defines its own names before their first use, so one file
//! decodes on its own. A frame is the unit of atomicity: replay keeps whole
//! frames only.

use super::Record;
use crate::envelope::codec::{self as body, Layout, Malformed, Reader};
use crate::envelope::{EventId, WorkKey};
use crate::names::Name;
use bytes::{BufMut, Bytes};
use std::collections::HashMap;
use std::io;

/// The first bytes of every journal: a name and a format version.
pub const MAGIC: &[u8; 8] = b"ruvent\0\x02";
/// `[len u32][crc32 u32]` in front of every frame.
pub const HEADER: usize = 8;
/// `[kind u8][prev u64][through u64][count u32]` at the start of every frame body.
pub const PREFIX: usize = 21;
/// A frame appended after the last.
pub const APPEND: u8 = 1;
/// A frame of a snapshot written by a rewrite.
pub const SNAPSHOT: u8 = 0;

const REGISTER: u8 = 1;
const CLAIM: u8 = 2;
const REMOVE: u8 = 3;
const RECLAIM: u8 = 4;
const GENERATION: u8 = 5;
const NAME: u8 = 6;

/// The name numbers one file has defined.
///
/// | key | name text |
/// | --- | --- |
/// | scope | one journal file |
/// | lifetime | until the file is replaced by a rewrite |
/// | bound | the distinct names written to this file since its last rewrite |
/// | cleared by | `FileJournal::rewrite`, which starts a new file |
#[derive(Default)]
pub struct FrameNames {
    numbers: HashMap<Name, u32>,
}

impl FrameNames {
    /// The dictionary a reopened file continues with.
    pub fn from_defined(defined: &HashMap<u32, Name>) -> Self {
        FrameNames { numbers: defined.iter().map(|(number, name)| (name.clone(), *number)).collect() }
    }

    fn number(&mut self, name: &Name, out: &mut Vec<u8>, count: &mut u32) -> u32 {
        if let Some(number) = self.numbers.get(name) {
            return *number;
        }
        let number = self.numbers.len() as u32;
        self.numbers.insert(name.clone(), number);
        let text = name.as_bytes();
        let length = text.len().min(u16::MAX as usize);
        out.put_u8(NAME);
        out.put_u32_le(number);
        out.put_u16_le(length as u16);
        out.put_slice(&text[..length]);
        *count += 1;
        number
    }
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn put_key(out: &mut Vec<u8>, key: WorkKey) {
    out.put_u32_le(key.generation);
    out.put_u32_le(key.arrival);
}

/// One frame holding `records`, chained after record number `prev`.
/// Returns the bytes and the record number the frame ends at.
pub fn encode_frame(kind: u8, prev: u64, records: &[Record], names: &mut FrameNames) -> io::Result<(Vec<u8>, u64)> {
    let mut records_out = Vec::with_capacity(records.len() * 48);
    let mut count = 0u32;
    for record in records {
        match record {
            Record::Register(event) => {
                // Names first, so the definitions precede the record that uses them.
                let numbers: Vec<u32> = event.names(Layout::Journal).map(|name| names.number(name, &mut records_out, &mut count)).collect();
                let mut lookup = numbers.into_iter();
                records_out.put_u8(REGISTER);
                body::encode(event, Layout::Journal, &mut |_| lookup.next().unwrap_or_default(), &mut records_out).map_err(|error| invalid(error.to_string()))?;
            }
            Record::Claim(key) => {
                records_out.put_u8(CLAIM);
                put_key(&mut records_out, *key);
            }
            Record::Remove(key) => {
                records_out.put_u8(REMOVE);
                put_key(&mut records_out, *key);
            }
            Record::Reclaim { key, id, retry } => {
                records_out.put_u8(RECLAIM);
                put_key(&mut records_out, *key);
                records_out.put_slice(&id.0);
                records_out.put_u16_le(*retry);
            }
            Record::Generation(generation) => {
                records_out.put_u8(GENERATION);
                records_out.put_u32_le(*generation);
            }
        }
        count += 1;
    }
    let through = prev + u64::from(count);
    let length = u32::try_from(PREFIX + records_out.len()).map_err(|_| invalid("journal frame exceeds 4 GiB"))?;
    let mut frame = Vec::with_capacity(HEADER + PREFIX + records_out.len());
    frame.put_u32_le(length);
    frame.put_u32_le(0);
    frame.put_u8(kind);
    frame.put_u64_le(prev);
    frame.put_u64_le(through);
    frame.put_u32_le(count);
    frame.extend_from_slice(&records_out);
    let crc = crc32fast::hash(&frame[HEADER..]);
    frame[4..8].copy_from_slice(&crc.to_le_bytes());
    Ok((frame, through))
}

/// What reading a file's frames found.
#[derive(Default)]
pub struct Frames {
    pub records: Vec<Record>,
    /// Bytes up to the end of the last whole frame, header included.
    pub good: usize,
    /// The record number the last whole frame ends at.
    pub through: u64,
    /// Every name the file defined, by number.
    pub names: HashMap<u32, Name>,
}

/// Decodes one frame body. Names it defines go to `defined`, so a frame that
/// fails leaves the file's dictionary as it was.
fn decode_records(body: &Bytes, known: &HashMap<u32, Name>, defined: &mut HashMap<u32, Name>, records: &mut Vec<Record>) -> Result<(), Malformed> {
    let mut reader = Reader::new(body);
    reader.take(PREFIX)?;
    let count = u32::from_le_bytes(body[PREFIX - 4..PREFIX].try_into().unwrap_or_default());
    for _ in 0..count {
        match reader.u8()? {
            REGISTER => {
                let event = body::decode(&mut reader, Layout::Journal, &|number| defined.get(&number).or_else(|| known.get(&number)).cloned())?;
                records.push(Record::Register(event));
            }
            CLAIM => records.push(Record::Claim(WorkKey { generation: reader.u32()?, arrival: reader.u32()? })),
            REMOVE => records.push(Record::Remove(WorkKey { generation: reader.u32()?, arrival: reader.u32()? })),
            RECLAIM => {
                let key = WorkKey { generation: reader.u32()?, arrival: reader.u32()? };
                let id: EventId = reader.id()?;
                let retry = reader.u16()?;
                records.push(Record::Reclaim { key, id, retry });
            }
            GENERATION => records.push(Record::Generation(reader.u32()?)),
            NAME => {
                let number = reader.u32()?;
                let length = reader.u16()? as usize;
                let text = std::str::from_utf8(reader.take(length)?).map_err(|_| Malformed::Forbidden("name not UTF-8"))?;
                defined.insert(number, Name::new(text));
            }
            _ => return Err(Malformed::Forbidden("unknown record tag")),
        }
    }
    reader.finish()
}

/// Reads whole frames from `bytes` (header included) and stops at the first
/// frame that is torn, fails its CRC, breaks the chain or does not decode.
pub fn decode_frames(bytes: &Bytes) -> io::Result<Frames> {
    if !bytes.starts_with(MAGIC) {
        return Err(invalid("not a ruvent journal of format version 2; it was left untouched"));
    }
    let mut frames = Frames { good: MAGIC.len(), ..Frames::default() };
    let mut at = MAGIC.len();
    while bytes.len() - at >= HEADER {
        let length = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap_or_default()) as usize;
        let crc = u32::from_le_bytes(bytes[at + 4..at + 8].try_into().unwrap_or_default());
        let Some(end) = (at + HEADER).checked_add(length).filter(|end| *end <= bytes.len()) else { break };
        let body = bytes.slice(at + HEADER..end);
        if length < PREFIX || crc32fast::hash(&body) != crc {
            break;
        }
        let kind = body[0];
        let prev = u64::from_le_bytes(body[1..9].try_into().unwrap_or_default());
        let through = u64::from_le_bytes(body[9..17].try_into().unwrap_or_default());
        // A frame that does not follow its predecessor was written out of turn.
        if kind == APPEND && prev != frames.through {
            break;
        }
        let mut defined = HashMap::new();
        let mut records = Vec::new();
        if decode_records(&body, &frames.names, &mut defined, &mut records).is_err() {
            break;
        }
        frames.names.extend(defined);
        frames.records.extend(records);
        frames.through = through;
        at = end;
        frames.good = at;
    }
    Ok(frames)
}
