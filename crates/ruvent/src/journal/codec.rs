//! Journal format 5 in a segment: the frames of the log lane, and the pieces
//! the checkpoint file shares with them.
//!
//! ```text
//! segment header (20 bytes): "rvseg\0" 0x05 lane u8 · number u32 · first serial u64
//! log frame:  [len u32][crc32 u32] [kind u8][uptime u64][commit v][first v][next v][count v][item]…
//!                                   '------------------- covered by crc --------------------'
//! body frame (long and body lanes): [len u32][crc32 u32][payload]   (crc of the payload alone)
//! v = unsigned LEB128 varint; key = generation v, arrival v
//! head = length v, then the envelope in the journal layout without its trailing payload length
//! location = segment v, offset v, length v, crc u32
//! item = [tag u8] then
//!    1 Register      root type v, head, body length v, body
//!    2 Body          length v, body
//!    3 Claim         key, token generation v, token seq v, lease ms v, credential name v
//!    4 Remove        key
//!    5 Reclaim       key, failures v
//!    6 Release       key, failures v, visible u8 (0 none, 1 then uptime v)
//!    7 Isolate       key, failures v, expires u8 (0 none, 1 then uptime v)
//!    8 Relocate      key, location
//!    9 ProducerDef   number v, producer id 16
//!   10 Producer      number v, seq v
//!   11 Generation    v
//!   12 Name          number v, length v, UTF-8          (dictionary; no serial)
//!   13 RegisterAt    root type v, head, location        (the payload was streamed earlier)
//!   14 Gap           a serial whose record was not written
//!   15 ProducerName  number v, producer id 16           (dictionary; no serial)
//! ```
//!
//! Every record takes the next serial, from `first` up to `next`; dictionary
//! items take none. A `Claim` whose `Remove` is in the same frame is not
//! written — if the frame is lost both are — and a `Gap` keeps its serial
//! (2.5, "작게 쓴다").
//!
//! Every segment defines its names and producer numbers before their first
//! use in it, so one segment decodes on its own and its dictionary is bounded
//! by what it holds (2.10, "세그먼트 사전").
//!
//! A payload's CRC is computed once: it is the location's CRC, and it is
//! combined into the frame's CRC, so the frame check costs no second pass.

use super::{Batch, Lane, Location, Record, Replay};
use crate::envelope::codec::{self as body, put_varint, Layout, Malformed, Reader};
use crate::envelope::{ClaimToken, Envelope, ProducerId, WorkKey};
use crate::names::Name;
use bytes::{BufMut, Bytes};
use crc32fast::Hasher;
use std::collections::{HashMap, HashSet};
use std::io::{self, Read};

/// `[len u32][crc32 u32]` in front of every frame.
pub const HEADER: u64 = 8;
/// A frame appended to the log lane.
pub const APPEND: u8 = 1;
/// `"rvseg\0"`, the format, then the lane.
pub const SEGMENT_MAGIC: &[u8; 7] = b"rvseg\0\x05";
/// Magic + lane + number + first serial.
pub const SEGMENT_HEADER: u64 = 20;

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
const REGISTER_AT: u8 = 13;
const GAP: u8 = 14;
const PRODUCER_NAME: u8 = 15;

pub fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

pub fn lane_byte(lane: Lane) -> u8 {
    match lane {
        Lane::Log => 1,
        Lane::Long => 2,
        Lane::Body => 3,
    }
}

pub fn lane_of(byte: u8) -> Option<Lane> {
    match byte {
        1 => Some(Lane::Log),
        2 => Some(Lane::Long),
        3 => Some(Lane::Body),
        _ => None,
    }
}

/// A segment's header.
pub fn segment_header(lane: Lane, number: u32, first: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(SEGMENT_HEADER as usize);
    out.extend_from_slice(SEGMENT_MAGIC);
    out.push(lane_byte(lane));
    out.put_u32_le(number);
    out.put_u64_le(first);
    out
}

/// The names and producer numbers one segment has defined.
///
/// | key | name text; producer number |
/// | --- | --- |
/// | scope | one segment |
/// | lifetime | the segment's; a new segment starts empty |
/// | bound | the distinct names and producers written to that segment |
/// | cleared by | the log writer, when it rolls to a new segment |
#[derive(Default)]
pub struct Dictionary {
    names: HashMap<Name, u32>,
    producers: HashSet<u32>,
}

impl Dictionary {
    pub fn producers(&self) -> &HashSet<u32> {
        &self.producers
    }
}

/// Bytes being built and their CRC, with payloads hashed once (see the module doc).
struct Out {
    bytes: Vec<u8>,
    crc: Hasher,
    hashed: usize,
    count: u64,
}

impl Out {
    fn body(&mut self, payload: &[u8]) -> u32 {
        self.crc.update(&self.bytes[self.hashed..]);
        let mut own = Hasher::new();
        own.update(payload);
        let crc = own.clone().finalize();
        self.crc.combine(&own);
        self.bytes.extend_from_slice(payload);
        self.hashed = self.bytes.len();
        crc
    }

    fn name(&mut self, dictionary: &mut Dictionary, name: &Name) -> u32 {
        if let Some(number) = dictionary.names.get(name) {
            return *number;
        }
        let number = dictionary.names.len() as u32;
        dictionary.names.insert(name.clone(), number);
        let text = name.as_bytes();
        self.bytes.put_u8(NAME);
        put_varint(&mut self.bytes, u64::from(number));
        put_varint(&mut self.bytes, text.len() as u64);
        self.bytes.put_slice(text);
        self.count += 1;
        number
    }

    fn finish(mut self) -> (Vec<u8>, Hasher) {
        self.crc.update(&self.bytes[self.hashed..]);
        (self.bytes, self.crc)
    }
}

pub fn put_key(out: &mut Vec<u8>, key: WorkKey) {
    put_varint(out, u64::from(key.generation));
    put_varint(out, key.arrival);
}

pub fn put_optional(out: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            out.put_u8(1);
            put_varint(out, value);
        }
        None => out.put_u8(0),
    }
}

pub fn put_location(out: &mut Vec<u8>, at: &Location) {
    put_varint(out, u64::from(at.segment));
    put_varint(out, at.offset);
    put_varint(out, u64::from(at.len));
    out.put_u32_le(at.crc);
}

/// The envelope's head in the journal layout, without the payload: its
/// trailing payload length (always zero here) is left out.
pub fn put_head(event: &Envelope, number: &mut dyn FnMut(&Name) -> u32, out: &mut Vec<u8>) -> io::Result<()> {
    let bare = Envelope { payload: Bytes::new(), ..event.clone() };
    let mut head = Vec::with_capacity(64);
    body::encode(&bare, Layout::Journal, number, &mut head).map_err(|error| invalid(error.to_string()))?;
    head.truncate(head.len().saturating_sub(4));
    put_varint(out, head.len() as u64);
    out.extend_from_slice(&head);
    Ok(())
}

/// Reads a head written by [`put_head`].
pub fn head_from(bytes: &[u8], name: &dyn Fn(u32) -> Option<Name>) -> Result<Envelope, Malformed> {
    let mut whole = Vec::with_capacity(bytes.len() + 4);
    whole.extend_from_slice(bytes);
    whole.extend_from_slice(&[0; 4]);
    let whole = Bytes::from(whole);
    let mut reader = Reader::new(&whole);
    let event = body::decode(&mut reader, Layout::Journal, name)?;
    reader.finish()?;
    Ok(event)
}

/// One encoded log frame.
pub struct Encoded {
    pub frame: Vec<u8>,
    /// Payload locations by the serial of their `Register`.
    pub located: Vec<(u64, Location)>,
}

/// Encodes `batch` as one log frame that will start at byte `base` of segment
/// `segment`. `known` maps producer numbers to ids, so a segment can define a
/// number the ledger defined in an earlier segment.
pub fn encode_frame(batch: &Batch, dictionary: &mut Dictionary, known: &mut HashMap<u32, ProducerId>, segment: u32, base: u64) -> io::Result<Encoded> {
    let removed: HashMap<WorkKey, usize> = batch.records.iter().enumerate().filter_map(|(at, record)| if let Record::Remove(key) = record { Some((*key, at)) } else { None }).collect();
    let payloads: usize = batch.records.iter().map(|record| if let Record::Register { event, .. } = record { event.payload.len() } else { 0 }).sum();
    let mut out = Out { bytes: Vec::with_capacity(batch.records.len() * 48 + payloads), crc: Hasher::new(), hashed: 0, count: 0 };
    let mut relative = Vec::new();
    for (at, record) in batch.records.iter().enumerate() {
        let serial = batch.first + at as u64;
        match record {
            Record::Register { event, root, at: place } => {
                // Names first, so the definitions precede the record that uses them.
                let numbers: Vec<u32> = event.names(Layout::Journal).map(|name| out.name(dictionary, name)).collect();
                let mut lookup = numbers.into_iter();
                out.bytes.put_u8(if place.is_some() { REGISTER_AT } else { REGISTER });
                put_varint(&mut out.bytes, u64::from(*root));
                put_head(event, &mut |_| lookup.next().unwrap_or_default(), &mut out.bytes)?;
                match place {
                    Some(place) => put_location(&mut out.bytes, place),
                    None => {
                        let len = u32::try_from(event.payload.len()).map_err(|_| invalid("payload over 4 GiB"))?;
                        put_varint(&mut out.bytes, u64::from(len));
                        let offset = out.bytes.len() as u64;
                        let crc = out.body(&event.payload);
                        relative.push((serial, offset, len, crc));
                    }
                }
            }
            Record::Body { payload } => {
                out.bytes.put_u8(BODY);
                put_varint(&mut out.bytes, payload.len() as u64);
                out.body(payload);
            }
            Record::Claim { key, token, lease_ms, credential } => {
                if removed.get(key).is_some_and(|removal| *removal > at) {
                    out.bytes.put_u8(GAP);
                } else {
                    let credential = out.name(dictionary, credential);
                    out.bytes.put_u8(CLAIM);
                    put_key(&mut out.bytes, *key);
                    put_varint(&mut out.bytes, u64::from(token.generation));
                    put_varint(&mut out.bytes, token.seq);
                    put_varint(&mut out.bytes, *lease_ms);
                    put_varint(&mut out.bytes, u64::from(credential));
                }
            }
            Record::Remove(key) => {
                out.bytes.put_u8(REMOVE);
                put_key(&mut out.bytes, *key);
            }
            Record::Reclaim { key, failures } => {
                out.bytes.put_u8(RECLAIM);
                put_key(&mut out.bytes, *key);
                put_varint(&mut out.bytes, u64::from(*failures));
            }
            Record::Release { key, failures, visible_at } => {
                out.bytes.put_u8(RELEASE);
                put_key(&mut out.bytes, *key);
                put_varint(&mut out.bytes, u64::from(*failures));
                put_optional(&mut out.bytes, *visible_at);
            }
            Record::Isolate { key, failures, expires } => {
                out.bytes.put_u8(ISOLATE);
                put_key(&mut out.bytes, *key);
                put_varint(&mut out.bytes, u64::from(*failures));
                put_optional(&mut out.bytes, *expires);
            }
            Record::Relocate { key, at } => {
                out.bytes.put_u8(RELOCATE);
                put_key(&mut out.bytes, *key);
                put_location(&mut out.bytes, at);
            }
            Record::ProducerDef { number, id } => {
                known.insert(*number, *id);
                dictionary.producers.insert(*number);
                out.bytes.put_u8(PRODUCER_DEF);
                put_varint(&mut out.bytes, u64::from(*number));
                out.bytes.put_slice(id);
            }
            Record::Producer { number, seq } => {
                if !dictionary.producers.contains(number) {
                    if let Some(id) = known.get(number) {
                        out.bytes.put_u8(PRODUCER_NAME);
                        put_varint(&mut out.bytes, u64::from(*number));
                        out.bytes.put_slice(id);
                        out.count += 1;
                        dictionary.producers.insert(*number);
                    }
                }
                out.bytes.put_u8(PRODUCER);
                put_varint(&mut out.bytes, u64::from(*number));
                put_varint(&mut out.bytes, *seq);
            }
            Record::Generation(generation) => {
                out.bytes.put_u8(GENERATION);
                put_varint(&mut out.bytes, u64::from(*generation));
            }
        }
        out.count += 1;
    }
    let count = out.count;
    let (records, records_crc) = out.finish();
    let next = batch.first + batch.records.len() as u64;
    let mut prefix = Vec::with_capacity(48);
    prefix.put_u8(APPEND);
    prefix.put_u64_le(batch.uptime);
    put_varint(&mut prefix, batch.commit);
    put_varint(&mut prefix, batch.first);
    put_varint(&mut prefix, next);
    put_varint(&mut prefix, count);
    let length = u32::try_from(prefix.len() + records.len()).map_err(|_| invalid("journal frame exceeds 4 GiB"))?;
    let mut crc = Hasher::new();
    crc.update(&prefix);
    crc.combine(&records_crc);
    let mut frame = Vec::with_capacity(HEADER as usize + prefix.len() + records.len());
    frame.put_u32_le(length);
    frame.put_u32_le(crc.finalize());
    frame.extend_from_slice(&prefix);
    frame.extend_from_slice(&records);
    let start = base + HEADER + prefix.len() as u64;
    let located = relative.into_iter().map(|(serial, offset, len, crc)| (serial, Location { segment, offset: start + offset, len, crc })).collect();
    Ok(Encoded { frame, located })
}

/// A body frame of the long or body lane: `[len][crc][payload]`.
pub fn body_frame_header(len: u32, crc: u32) -> [u8; 8] {
    let mut header = [0; 8];
    header[..4].copy_from_slice(&len.to_le_bytes());
    header[4..].copy_from_slice(&crc.to_le_bytes());
    header
}

/// Why reading a frame stopped short.
#[derive(Debug)]
pub enum Cut {
    /// The file ended inside the frame.
    Eof,
    /// The frame's bytes do not decode, or an item runs past the frame's length.
    Malformed(&'static str),
    Io(io::Error),
}

impl From<Malformed> for Cut {
    fn from(error: Malformed) -> Self {
        Cut::Malformed(match error {
            Malformed::Short => "item cut short",
            Malformed::Reserved => "reserved bit",
            Malformed::Forbidden(what) => what,
            Malformed::UnknownName(_) => "unknown name number",
            Malformed::Missing(what) => what,
            Malformed::Trailing => "trailing bytes",
            Malformed::Overlong => "overlong number",
        })
    }
}

/// Reads frames from a file: a position, the CRC of the frame being read, and
/// how many of its bytes are left. Payloads are hashed and skipped, never kept.
pub struct Stream<R> {
    inner: R,
    pub pos: u64,
    crc: Hasher,
    left: u64,
    scratch: Vec<u8>,
}

impl<R: Read> Stream<R> {
    pub fn new(inner: R, pos: u64) -> Self {
        Stream { inner, pos, crc: Hasher::new(), left: 0, scratch: Vec::new() }
    }

    fn raw(&mut self, buffer: &mut [u8]) -> Result<(), Cut> {
        match self.inner.read_exact(buffer) {
            Ok(()) => {
                self.pos += buffer.len() as u64;
                Ok(())
            }
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Err(Cut::Eof),
            Err(error) => Err(Cut::Io(error)),
        }
    }

    /// Starts a frame of `len` bytes: they are hashed as they are read.
    pub fn enter(&mut self, len: u64) {
        self.crc = Hasher::new();
        self.left = len;
    }

    pub fn left(&self) -> u64 {
        self.left
    }

    /// The CRC of what was read of the frame.
    pub fn crc(&self) -> u32 {
        self.crc.clone().finalize()
    }

    pub fn take(&mut self, count: usize) -> Result<Vec<u8>, Cut> {
        if count as u64 > self.left {
            return Err(Cut::Malformed("item runs past the frame"));
        }
        let mut buffer = vec![0; count];
        self.raw(&mut buffer)?;
        self.crc.update(&buffer);
        self.left -= count as u64;
        Ok(buffer)
    }

    pub fn u8(&mut self) -> Result<u8, Cut> {
        Ok(self.take(1)?[0])
    }

    pub fn u32(&mut self) -> Result<u32, Cut> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap_or_default()))
    }

    pub fn u64(&mut self) -> Result<u64, Cut> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap_or_default()))
    }

    pub fn varint(&mut self) -> Result<u64, Cut> {
        let mut value = 0u64;
        for index in 0..10 {
            let byte = self.u8()?;
            let part = u64::from(byte & 0x7f);
            if index == 9 && part > 1 {
                return Err(Cut::Malformed("overlong number"));
            }
            value |= part << (7 * index);
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(Cut::Malformed("overlong number"))
    }

    pub fn varint32(&mut self) -> Result<u32, Cut> {
        u32::try_from(self.varint()?).map_err(|_| Cut::Malformed("overlong number"))
    }

    pub fn id16(&mut self) -> Result<[u8; 16], Cut> {
        Ok(self.take(16)?.try_into().unwrap_or_default())
    }

    /// Hashes and skips `len` payload bytes; returns the payload's own CRC.
    pub fn skip_body(&mut self, len: u64) -> Result<u32, Cut> {
        if len > self.left {
            return Err(Cut::Malformed("payload runs past the frame"));
        }
        let mut own = Hasher::new();
        let mut rest = len;
        self.scratch.resize(64 * 1024, 0);
        while rest > 0 {
            let step = rest.min(self.scratch.len() as u64) as usize;
            let mut chunk = std::mem::take(&mut self.scratch);
            let read = self.raw(&mut chunk[..step]);
            if read.is_ok() {
                own.update(&chunk[..step]);
            }
            self.scratch = chunk;
            read?;
            rest -= step as u64;
        }
        self.left -= len;
        let crc = own.clone().finalize();
        self.crc.combine(&own);
        Ok(crc)
    }

    /// Skips what is left of the frame, hashing it.
    pub fn drain(&mut self) -> Result<(), Cut> {
        let left = self.left;
        self.skip_body(left).map(|_| ())
    }

    /// Reads the 8-byte frame header. `Ok(None)`: the file ends exactly here.
    pub fn header(&mut self) -> Result<Option<(u32, u32)>, Cut> {
        let mut header = [0u8; 8];
        let mut got = 0;
        while got < header.len() {
            match self.inner.read(&mut header[got..]) {
                Ok(0) => break,
                Ok(read) => got += read,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(Cut::Io(error)),
            }
        }
        self.pos += got as u64;
        match got {
            0 => Ok(None),
            8 => Ok(Some((u32::from_le_bytes(header[..4].try_into().unwrap_or_default()), u32::from_le_bytes(header[4..].try_into().unwrap_or_default())))),
            _ => Err(Cut::Eof),
        }
    }

    pub fn name(&mut self, names: &HashMap<u32, Name>) -> Result<Name, Cut> {
        let number = self.varint32()?;
        names.get(&number).cloned().ok_or(Cut::Malformed("unknown name number"))
    }

    pub fn key(&mut self) -> Result<WorkKey, Cut> {
        Ok(WorkKey { generation: self.varint32()?, arrival: self.varint()? })
    }

    pub fn optional(&mut self) -> Result<Option<u64>, Cut> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.varint()?)),
            _ => Err(Cut::Malformed("optional marker")),
        }
    }

    /// A type number, written as a varint.
    pub fn type16(&mut self) -> Result<u16, Cut> {
        u16::try_from(self.varint()?).map_err(|_| Cut::Malformed("type over 65535"))
    }

    pub fn failures(&mut self) -> Result<u16, Cut> {
        u16::try_from(self.varint()?).map_err(|_| Cut::Malformed("overlong number"))
    }

    pub fn location(&mut self) -> Result<Location, Cut> {
        Ok(Location { segment: self.varint32()?, offset: self.varint()?, len: self.varint32()?, crc: self.u32()? })
    }

    /// A head written by [`put_head`], names resolved from `names`.
    pub fn head(&mut self, names: &HashMap<u32, Name>) -> Result<Envelope, Cut> {
        let len = usize::try_from(self.varint()?).map_err(|_| Cut::Malformed("overlong number"))?;
        let bytes = self.take(len)?;
        Ok(head_from(&bytes, &|number| names.get(&number).cloned())?)
    }

    /// A dictionary name definition, after its tag.
    pub fn define(&mut self, names: &mut HashMap<u32, Name>) -> Result<(), Cut> {
        let number = self.varint32()?;
        let len = usize::try_from(self.varint()?).map_err(|_| Cut::Malformed("overlong number"))?;
        let text = self.take(len)?;
        let text = std::str::from_utf8(&text).map_err(|_| Cut::Malformed("name not UTF-8"))?;
        names.insert(number, Name::new(text));
        Ok(())
    }
}

/// A log frame's prefix.
#[derive(Clone, Copy, Debug, Default)]
pub struct Prefix {
    pub uptime: u64,
    pub commit: u64,
    pub first: u64,
    pub next: u64,
}

/// What reading one log frame found.
pub enum Frame {
    Whole {
        prefix: Prefix,
        items: Vec<Replay>,
    },
    /// The segment ends cleanly here.
    End,
    /// The frame starting at `at` is incomplete: a write that did not finish.
    Torn {
        at: u64,
    },
    /// The frame starting at `at` is damaged with bytes after it.
    Damaged {
        at: u64,
        why: String,
    },
}

/// The segment-local dictionaries a frame reader keeps.
#[derive(Default)]
pub struct Defined {
    pub names: HashMap<u32, Name>,
}

/// Reads the next log frame of segment `segment`, whose file is `file_len`
/// bytes long. Payloads are skipped; `Register` items get their location.
pub fn read_frame<R: Read>(stream: &mut Stream<R>, file_len: u64, segment: u32, defined: &mut Defined) -> Result<Frame, io::Error> {
    let at = stream.pos;
    let (len, crc) = match stream.header() {
        Ok(Some(header)) => header,
        Ok(None) => return Ok(Frame::End),
        Err(Cut::Io(error)) => return Err(error),
        Err(_) => return Ok(Frame::Torn { at }),
    };
    let end = at + HEADER + u64::from(len);
    // A zero length is what a zero-filled tail reads as; no frame is empty.
    if len == 0 || end > file_len {
        return Ok(Frame::Torn { at });
    }
    stream.enter(u64::from(len));
    let decoded = decode_items(stream, segment, defined);
    let decoded = match decoded {
        Err(Cut::Io(error)) => return Err(error),
        Err(Cut::Eof) => return Ok(Frame::Torn { at }),
        Ok(decoded) if stream.left() == 0 => Ok(decoded),
        Ok(_) => Err("bytes left over in a frame"),
        Err(Cut::Malformed(why)) => Err(why),
    };
    if decoded.is_err() {
        match stream.drain() {
            Ok(()) => {}
            Err(Cut::Io(error)) => return Err(error),
            Err(_) => return Ok(Frame::Torn { at }),
        }
    }
    let whole = stream.crc() == crc;
    match (decoded, whole) {
        (Ok((prefix, items)), true) => Ok(Frame::Whole { prefix, items }),
        (Err(why), true) => Ok(Frame::Damaged { at, why: format!("a frame with a good CRC does not decode: {why}") }),
        (_, false) if end == file_len => Ok(Frame::Torn { at }),
        (_, false) => Ok(Frame::Damaged { at, why: "CRC mismatch with frames after it".into() }),
    }
}

fn decode_items<R: Read>(stream: &mut Stream<R>, segment: u32, defined: &mut Defined) -> Result<(Prefix, Vec<Replay>), Cut> {
    let kind = stream.u8()?;
    if kind != APPEND {
        return Err(Cut::Malformed("unknown frame kind"));
    }
    let prefix = Prefix { uptime: stream.u64()?, commit: stream.varint()?, first: stream.varint()?, next: stream.varint()? };
    let count = stream.varint()?;
    let mut items = Vec::new();
    let mut serial = prefix.first;
    for _ in 0..count {
        let tag = stream.u8()?;
        let record = match tag {
            NAME => {
                stream.define(&mut defined.names)?;
                continue;
            }
            PRODUCER_NAME => {
                items.push(Replay::ProducerName { number: stream.varint32()?, id: stream.id16()? });
                continue;
            }
            GAP => None,
            REGISTER => {
                let root = stream.type16()?;
                let event = stream.head(&defined.names)?;
                let len = stream.varint32()?;
                let offset = stream.pos;
                let crc = stream.skip_body(u64::from(len))?;
                Some(Record::Register { event, root, at: Some(Location { segment, offset, len, crc }) })
            }
            REGISTER_AT => {
                let root = stream.type16()?;
                let event = stream.head(&defined.names)?;
                Some(Record::Register { event, root, at: Some(stream.location()?) })
            }
            BODY => {
                let len = stream.varint()?;
                stream.skip_body(len)?;
                None
            }
            CLAIM => {
                let key = stream.key()?;
                let token = ClaimToken { generation: stream.varint32()?, seq: stream.varint()? };
                let lease_ms = stream.varint()?;
                let credential = stream.name(&defined.names)?;
                Some(Record::Claim { key, token, lease_ms, credential })
            }
            REMOVE => Some(Record::Remove(stream.key()?)),
            RECLAIM => Some(Record::Reclaim { key: stream.key()?, failures: stream.failures()? }),
            RELEASE => Some(Record::Release { key: stream.key()?, failures: stream.failures()?, visible_at: stream.optional()? }),
            ISOLATE => Some(Record::Isolate { key: stream.key()?, failures: stream.failures()?, expires: stream.optional()? }),
            RELOCATE => Some(Record::Relocate { key: stream.key()?, at: stream.location()? }),
            PRODUCER_DEF => Some(Record::ProducerDef { number: stream.varint32()?, id: stream.id16()? }),
            PRODUCER => Some(Record::Producer { number: stream.varint32()?, seq: stream.varint()? }),
            GENERATION => Some(Record::Generation(stream.varint32()?)),
            _ => return Err(Cut::Malformed("unknown record tag")),
        };
        if let Some(record) = record {
            items.push(Replay::Record { serial, record });
        }
        serial += 1;
    }
    if serial != prefix.next {
        return Err(Cut::Malformed("the frame's serials do not add up"));
    }
    Ok((prefix, items))
}

/// Reads a segment's header from its first bytes.
pub fn parse_segment_header(bytes: &[u8]) -> Option<(Lane, u32, u64)> {
    if bytes.len() < SEGMENT_HEADER as usize || !bytes.starts_with(SEGMENT_MAGIC) {
        return None;
    }
    let lane = lane_of(bytes[7])?;
    let number = u32::from_le_bytes(bytes[8..12].try_into().ok()?);
    let first = u64::from_le_bytes(bytes[12..20].try_into().ok()?);
    Some((lane, number, first))
}
