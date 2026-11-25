//! Journal file format, version 3: the record set in the single-file container.
//!
//! ```text
//! "ruvent\0" 0x03                                                       file header, once
//! [len u32][crc32 u32]  [kind u8][uptime u64][commit v][first v][next v][count v][record]…
//!                       '-------------------- covered by crc --------------------'
//! v = unsigned LEB128 varint; key = generation v, arrival v
//! record = [tag u8] then
//!    1 Register     root name v, envelope body (journal layout)
//!    2 Body         length v, bytes
//!    3 Claim        key, token generation v, token seq v, lease ms v, credential name v
//!    4 Remove       key
//!    5 Reclaim      key, failures v
//!    6 Release      key, failures v, visible u8 (0 none, 1 then uptime v)
//!    7 Isolate      key, channel name v, failures v, expires u8 (0 none, 1 then uptime v)
//!    8 Relocate     key, segment v, offset v, length v
//!    9 ProducerDef  number v, producer id 16
//!   10 Producer     number v, seq v
//!   11 Generation   v
//!   12 Name         number v, length v, UTF-8
//! ```
//!
//! `kind` is 1 for an appended batch and 0 for a frame of a snapshot. An
//! appended batch's records take serials `first..next`; a snapshot's take
//! none (`first == next`). Every file defines its own names before their
//! first use, so one file decodes on its own. A frame is the unit of
//! atomicity: replay keeps whole frames only.
//!
//! A `Claim` whose `Remove` is in the same frame is not written: if the frame
//! is lost both are, so no guarantee depends on it (2.5, "작게 쓴다").

use super::{Batch, Location, Record};
use crate::envelope::codec::{self as body, put_varint, Layout, Malformed, Reader};
use crate::envelope::{ClaimToken, WorkKey};
use crate::names::Name;
use bytes::{BufMut, Bytes};
use std::collections::HashMap;
use std::io;

/// The first bytes of every journal: a name and a format version.
pub const MAGIC: &[u8; 8] = b"ruvent\0\x03";
/// `[len u32][crc32 u32]` in front of every frame.
pub const HEADER: usize = 8;
/// A frame appended after the last.
pub const APPEND: u8 = 1;
/// A frame of a snapshot written by a rewrite.
pub const SNAPSHOT: u8 = 0;

const REGISTER: u8 = 1;
const BODY: u8 = 2;
const CLAIM: u8 = 3;
const REMOVE: u8 = 4;
const RECLAIM: u8 = 5;
const RELEASE: u8 = 6;
const ISOLATE: u8 = 7;
const RELOCATE: u8 = 8;
const PRODUCER_DEF: u8 = 9;
const PRODUCER: u8 = 10;
const GENERATION: u8 = 11;
const NAME: u8 = 12;

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

    fn number(&mut self, name: &Name, out: &mut Vec<u8>, count: &mut u64) -> u32 {
        if let Some(number) = self.numbers.get(name) {
            return *number;
        }
        let number = self.numbers.len() as u32;
        self.numbers.insert(name.clone(), number);
        let text = name.as_bytes();
        out.put_u8(NAME);
        put_varint(out, u64::from(number));
        put_varint(out, text.len() as u64);
        out.put_slice(text);
        *count += 1;
        number
    }
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn put_key(out: &mut Vec<u8>, key: WorkKey) {
    put_varint(out, u64::from(key.generation));
    put_varint(out, key.arrival);
}

fn put_optional(out: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            out.put_u8(1);
            put_varint(out, value);
        }
        None => out.put_u8(0),
    }
}

/// One frame holding `batch`. A `SNAPSHOT` frame's records take no serials.
pub fn encode_frame(kind: u8, batch: &Batch, names: &mut FrameNames) -> io::Result<Vec<u8>> {
    let removed: HashMap<WorkKey, usize> = batch.records.iter().enumerate().filter_map(|(at, record)| if let Record::Remove(key) = record { Some((*key, at)) } else { None }).collect();
    let mut out = Vec::with_capacity(batch.records.len() * 32);
    let mut count = 0u64;
    for (at, record) in batch.records.iter().enumerate() {
        match record {
            Record::Register { event, root } => {
                // Names first, so the definitions precede the record that uses them.
                let root = names.number(root, &mut out, &mut count);
                let numbers: Vec<u32> = event.names(Layout::Journal).map(|name| names.number(name, &mut out, &mut count)).collect();
                let mut lookup = numbers.into_iter();
                out.put_u8(REGISTER);
                put_varint(&mut out, u64::from(root));
                body::encode(event, Layout::Journal, &mut |_| lookup.next().unwrap_or_default(), &mut out).map_err(|error| invalid(error.to_string()))?;
            }
            Record::Body { payload } => {
                out.put_u8(BODY);
                put_varint(&mut out, payload.len() as u64);
                out.put_slice(payload);
            }
            Record::Claim { key, token, lease_ms, credential } => {
                if removed.get(key).is_some_and(|removal| *removal > at) {
                    continue;
                }
                let credential = names.number(credential, &mut out, &mut count);
                out.put_u8(CLAIM);
                put_key(&mut out, *key);
                put_varint(&mut out, u64::from(token.generation));
                put_varint(&mut out, token.seq);
                put_varint(&mut out, *lease_ms);
                put_varint(&mut out, u64::from(credential));
            }
            Record::Remove(key) => {
                out.put_u8(REMOVE);
                put_key(&mut out, *key);
            }
            Record::Reclaim { key, failures } => {
                out.put_u8(RECLAIM);
                put_key(&mut out, *key);
                put_varint(&mut out, u64::from(*failures));
            }
            Record::Release { key, failures, visible_at } => {
                out.put_u8(RELEASE);
                put_key(&mut out, *key);
                put_varint(&mut out, u64::from(*failures));
                put_optional(&mut out, *visible_at);
            }
            Record::Isolate { key, channel, failures, expires } => {
                let channel = names.number(channel, &mut out, &mut count);
                out.put_u8(ISOLATE);
                put_key(&mut out, *key);
                put_varint(&mut out, u64::from(channel));
                put_varint(&mut out, u64::from(*failures));
                put_optional(&mut out, *expires);
            }
            Record::Relocate { key, at } => {
                out.put_u8(RELOCATE);
                put_key(&mut out, *key);
                put_varint(&mut out, u64::from(at.segment));
                put_varint(&mut out, at.offset);
                put_varint(&mut out, u64::from(at.len));
            }
            Record::ProducerDef { number, id } => {
                out.put_u8(PRODUCER_DEF);
                put_varint(&mut out, u64::from(*number));
                out.put_slice(id);
            }
            Record::Producer { number, seq } => {
                out.put_u8(PRODUCER);
                put_varint(&mut out, u64::from(*number));
                put_varint(&mut out, *seq);
            }
            Record::Generation(generation) => {
                out.put_u8(GENERATION);
                put_varint(&mut out, u64::from(*generation));
            }
        }
        count += 1;
    }
    let next = if kind == SNAPSHOT { batch.first } else { batch.first + batch.records.len() as u64 };
    let mut prefix = Vec::with_capacity(48);
    prefix.put_u8(kind);
    prefix.put_u64_le(batch.uptime);
    put_varint(&mut prefix, batch.commit);
    put_varint(&mut prefix, batch.first);
    put_varint(&mut prefix, next);
    put_varint(&mut prefix, count);
    let length = u32::try_from(prefix.len() + out.len()).map_err(|_| invalid("journal frame exceeds 4 GiB"))?;
    let mut frame = Vec::with_capacity(HEADER + prefix.len() + out.len());
    frame.put_u32_le(length);
    frame.put_u32_le(0);
    frame.extend_from_slice(&prefix);
    frame.extend_from_slice(&out);
    let crc = crc32fast::hash(&frame[HEADER..]);
    frame[4..8].copy_from_slice(&crc.to_le_bytes());
    Ok(frame)
}

/// What reading a file's frames found.
#[derive(Default)]
pub struct Frames {
    pub records: Vec<Record>,
    /// Bytes up to the end of the last whole frame, header included.
    pub good: usize,
    /// The serial the next record takes.
    pub next: u64,
    /// The uptime and commit number of the last whole frame.
    pub uptime: u64,
    pub commit: u64,
    /// Every name the file defined, by number.
    pub names: HashMap<u32, Name>,
    /// Whether any frame was read.
    pub any: bool,
}

struct Prefix {
    kind: u8,
    uptime: u64,
    commit: u64,
    first: u64,
    next: u64,
    count: u64,
}

fn key(reader: &mut Reader<'_>) -> Result<WorkKey, Malformed> {
    Ok(WorkKey { generation: reader.varint32()?, arrival: reader.varint()? })
}

fn optional(reader: &mut Reader<'_>) -> Result<Option<u64>, Malformed> {
    match reader.u8()? {
        0 => Ok(None),
        1 => Ok(Some(reader.varint()?)),
        _ => Err(Malformed::Forbidden("optional marker")),
    }
}

fn failures(reader: &mut Reader<'_>) -> Result<u16, Malformed> {
    u16::try_from(reader.varint()?).map_err(|_| Malformed::Overlong)
}

/// Decodes one frame body. Names it defines go to `defined`, so a frame that
/// fails leaves the file's dictionary as it was.
fn decode_records(body: &Bytes, known: &HashMap<u32, Name>, defined: &mut HashMap<u32, Name>, records: &mut Vec<Record>) -> Result<Prefix, Malformed> {
    let mut reader = Reader::new(body);
    let prefix = Prefix { kind: reader.u8()?, uptime: reader.u64()?, commit: reader.varint()?, first: reader.varint()?, next: reader.varint()?, count: reader.varint()? };
    let name = |number: u32, defined: &HashMap<u32, Name>| defined.get(&number).or_else(|| known.get(&number)).cloned().ok_or(Malformed::UnknownName(number));
    for _ in 0..prefix.count {
        match reader.u8()? {
            REGISTER => {
                let root = name(reader.varint32()?, defined)?;
                let event = body::decode(&mut reader, Layout::Journal, &|number| defined.get(&number).or_else(|| known.get(&number)).cloned())?;
                records.push(Record::Register { event, root });
            }
            BODY => {
                let length = usize::try_from(reader.varint()?).map_err(|_| Malformed::Overlong)?;
                records.push(Record::Body { payload: reader.take_shared(length)? });
            }
            CLAIM => {
                let key = key(&mut reader)?;
                let token = ClaimToken { generation: reader.varint32()?, seq: reader.varint()? };
                let lease_ms = reader.varint()?;
                let credential = name(reader.varint32()?, defined)?;
                records.push(Record::Claim { key, token, lease_ms, credential });
            }
            REMOVE => records.push(Record::Remove(key(&mut reader)?)),
            RECLAIM => records.push(Record::Reclaim { key: key(&mut reader)?, failures: failures(&mut reader)? }),
            RELEASE => records.push(Record::Release { key: key(&mut reader)?, failures: failures(&mut reader)?, visible_at: optional(&mut reader)? }),
            ISOLATE => {
                let key = key(&mut reader)?;
                let channel = name(reader.varint32()?, defined)?;
                records.push(Record::Isolate { key, channel, failures: failures(&mut reader)?, expires: optional(&mut reader)? });
            }
            RELOCATE => {
                let key = key(&mut reader)?;
                let at = Location { segment: reader.varint32()?, offset: reader.varint()?, len: reader.varint32()? };
                records.push(Record::Relocate { key, at });
            }
            PRODUCER_DEF => records.push(Record::ProducerDef { number: reader.varint32()?, id: reader.bytes16()? }),
            PRODUCER => records.push(Record::Producer { number: reader.varint32()?, seq: reader.varint()? }),
            GENERATION => records.push(Record::Generation(reader.varint32()?)),
            NAME => {
                let number = reader.varint32()?;
                let length = usize::try_from(reader.varint()?).map_err(|_| Malformed::Overlong)?;
                let text = std::str::from_utf8(reader.take(length)?).map_err(|_| Malformed::Forbidden("name not UTF-8"))?;
                defined.insert(number, Name::new(text));
            }
            _ => return Err(Malformed::Forbidden("unknown record tag")),
        }
    }
    reader.finish()?;
    Ok(prefix)
}

/// Reads whole frames from `bytes` (header included) and stops at the first
/// frame that is torn, fails its CRC, breaks the chain or does not decode.
pub fn decode_frames(bytes: &Bytes) -> io::Result<Frames> {
    if !bytes.starts_with(MAGIC) {
        return Err(invalid("not a ruvent journal of format version 3 (format 2 and older are refused, not migrated); it was left untouched"));
    }
    let mut frames = Frames { good: MAGIC.len(), ..Frames::default() };
    let mut at = MAGIC.len();
    while bytes.len() - at >= HEADER {
        let length = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap_or_default()) as usize;
        let crc = u32::from_le_bytes(bytes[at + 4..at + 8].try_into().unwrap_or_default());
        let Some(end) = (at + HEADER).checked_add(length).filter(|end| *end <= bytes.len()) else { break };
        let body = bytes.slice(at + HEADER..end);
        if crc32fast::hash(&body) != crc {
            break;
        }
        let mut defined = HashMap::new();
        let mut records = Vec::new();
        let Ok(prefix) = decode_records(&body, &frames.names, &mut defined, &mut records) else { break };
        // A batch that does not follow its predecessor was written out of turn.
        if prefix.kind == APPEND && frames.any && prefix.first != frames.next {
            break;
        }
        if prefix.kind != APPEND && prefix.kind != SNAPSHOT {
            break;
        }
        frames.names.extend(defined);
        frames.records.extend(records);
        frames.next = prefix.next;
        frames.uptime = prefix.uptime;
        frames.commit = prefix.commit;
        frames.any = true;
        at = end;
        frames.good = at;
    }
    Ok(frames)
}
