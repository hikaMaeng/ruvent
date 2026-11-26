//! The envelope's byte layout. One body codec, used by the wire and the journal.
//!
//! Fixed-width numbers are little-endian; `id` and `group` are 16
//! octets. The work key's arrival and the two times are unsigned LEB128
//! varints ([`put_varint`]). The channel is 16 octets. Names (the credential
//! and the end user) travel as numbers from a dictionary the surrounding
//! stream defines first; this module only asks for the number of a name and
//! the name of a number. A decoder knows every length from the
//! [`Layout`] and the `flags` word alone.
//!
//! ## `flags` (u16)
//!
//! | bit | meaning |
//! | --- | --- |
//! | 0 | kind: set means dispatch |
//! | 1 | reserved (was "claimed" in format 2) |
//! | 2 | reserved (was `reply` in format 3) |
//! | 3 | reserved (was `correlation` in format 3) |
//! | 4 | `principal` present |
//! | 5 | `delegate` present |
//! | 6 | `not_before` present |
//! | 7 | `expires` present |
//! | 8 | `group` present |
//! | 9 | isolated (claimable; set by the server) |
//! | 10–15 | reserved, always zero |

use super::{Channel, Envelope, EventId, Kind, WorkKey};
use crate::names::Name;
use bytes::{BufMut, Bytes};
use std::fmt;

pub const KIND: u16 = 1 << 0;
pub const PRINCIPAL: u16 = 1 << 4;
pub const DELEGATE: u16 = 1 << 5;
pub const NOT_BEFORE: u16 = 1 << 6;
pub const EXPIRES: u16 = 1 << 7;
pub const GROUP: u16 = 1 << 8;
pub const ISOLATED: u16 = 1 << 9;
const OPTIONAL: u16 = PRINCIPAL | DELEGATE | NOT_BEFORE | EXPIRES | GROUP;
/// Bits no layout may set.
pub const RESERVED: u16 = !(KIND | OPTIONAL | ISOLATED);
/// Fields that only mean something on a claimable event.
const CLAIMABLE_ONLY: u16 = NOT_BEFORE | EXPIRES | GROUP | ISOLATED;

/// id 16 + channel 16 + type 2 + flags 2 + failures 2. (Format 4 had the
/// channel and the type as 4-byte name numbers.)
pub const HEAD: usize = 38;

/// Which fields a record of each kind carries.
///
/// | layout | carries | refused |
/// | --- | --- | --- |
/// | `Request` | head, optional, payload | failures ≠ 0, principal, isolated |
/// | `Notice` | head, work key | any optional field |
/// | `Assigned` | head, work key, optional, payload | dispatch |
/// | `Delivery` | head, optional, payload | failures ≠ 0, claimable |
/// | `Journal` | head, work key, optional, payload | dispatch |
///
/// Every layout refuses the reserved bits, and `not_before`, `expires`,
/// `group` and isolated on a dispatch event.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Layout {
    /// Registration requests and the follow-ups of a completion.
    Request,
    /// "There is claimable work": a notice, or one entry of a subscription's backlog.
    Notice,
    /// The event of an assignment or of an explicit claim's answer.
    Assigned,
    /// A dispatch event handed to a subscriber.
    Delivery,
    /// The body of a journal registration.
    Journal,
}

impl Layout {
    pub fn has_key(self) -> bool {
        matches!(self, Layout::Notice | Layout::Assigned | Layout::Journal)
    }

    pub fn has_optional(self) -> bool {
        !matches!(self, Layout::Notice)
    }

    pub fn has_payload(self) -> bool {
        !matches!(self, Layout::Notice)
    }

    fn has_failures(self) -> bool {
        !matches!(self, Layout::Request | Layout::Delivery)
    }

    /// The flag bits this layout writes; the rest are cleared on encode.
    fn written(self) -> u16 {
        match self {
            Layout::Notice => KIND | ISOLATED,
            _ => KIND | OPTIONAL | ISOLATED,
        }
    }

    fn check(self, flags: u16, failures: u16) -> Result<(), Malformed> {
        if flags & RESERVED != 0 {
            return Err(Malformed::Reserved);
        }
        let dispatch = flags & KIND != 0;
        if dispatch && flags & CLAIMABLE_ONLY != 0 {
            return Err(Malformed::Forbidden("timing or group on a dispatch event"));
        }
        if !self.has_failures() && failures != 0 {
            return Err(Malformed::Forbidden("failures"));
        }
        match self {
            Layout::Request if flags & PRINCIPAL != 0 => Err(Malformed::Forbidden("principal")),
            Layout::Request if flags & ISOLATED != 0 => Err(Malformed::Forbidden("isolated")),
            Layout::Notice if flags & OPTIONAL != 0 => Err(Malformed::Forbidden("optional field")),
            Layout::Delivery if !dispatch => Err(Malformed::Forbidden("claimable")),
            Layout::Assigned | Layout::Journal if dispatch => Err(Malformed::Forbidden("dispatch")),
            _ => Ok(()),
        }
    }
}

/// Why bytes were refused.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Malformed {
    /// Fewer bytes than the layout needs.
    Short,
    Reserved,
    /// A field this layout does not allow.
    Forbidden(&'static str),
    /// A name number the stream never defined.
    UnknownName(u32),
    /// The envelope lacks a block the layout needs (encoding only).
    Missing(&'static str),
    /// Bytes left over after the last record.
    Trailing,
    /// A varint longer than ten bytes or past `u64`.
    Overlong,
}

impl fmt::Display for Malformed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Malformed::Short => f.write_str("record cut short"),
            Malformed::Reserved => f.write_str("reserved flag bit set"),
            Malformed::Forbidden(what) => write!(f, "{what} not allowed in this record"),
            Malformed::UnknownName(number) => write!(f, "name {number} was never defined"),
            Malformed::Missing(what) => write!(f, "{what} missing"),
            Malformed::Trailing => f.write_str("bytes left over"),
            Malformed::Overlong => f.write_str("varint too long"),
        }
    }
}

impl std::error::Error for Malformed {}

/// Appends `value` as an unsigned LEB128 varint.
pub fn put_varint(out: &mut impl BufMut, mut value: u64) {
    while value >= 0x80 {
        out.put_u8((value as u8) | 0x80);
        value >>= 7;
    }
    out.put_u8(value as u8);
}

/// The encoded size of `value` as a varint.
pub fn varint_len(value: u64) -> usize {
    let bits = 64 - value.leading_zeros() as usize;
    bits.div_ceil(7).max(1)
}

/// The encoded size of `envelope` in `layout`, not counting name definitions.
pub fn size(envelope: &Envelope, layout: Layout) -> usize {
    let flags = envelope.flags() & layout.written();
    let mut size = HEAD;
    if layout.has_key() {
        size += 4 + varint_len(envelope.key.map_or(0, |key| key.arrival));
    }
    for (bit, width) in [(PRINCIPAL, 4), (DELEGATE, 4), (GROUP, 16)] {
        if flags & bit != 0 {
            size += width;
        }
    }
    if flags & NOT_BEFORE != 0 {
        size += varint_len(envelope.not_before.unwrap_or_default());
    }
    if flags & EXPIRES != 0 {
        size += varint_len(envelope.expires.unwrap_or_default());
    }
    if layout.has_payload() {
        size += 4 + envelope.payload.len();
    }
    size
}

/// Appends `envelope` in `layout`. `number` gives the dictionary number of a
/// name; the caller has defined every name of [`Envelope::names`] beforehand.
pub fn encode(envelope: &Envelope, layout: Layout, number: &mut dyn FnMut(&Name) -> u32, out: &mut impl BufMut) -> Result<(), Malformed> {
    encode_parts(envelope, layout, number, out, true)
}

/// [`encode`] without the payload block: what a record that carries the
/// payload apart writes as its head (wire v2: the payload is the record's
/// body, so it is never copied into the head). Same checks as [`encode`].
pub fn encode_head(envelope: &Envelope, layout: Layout, number: &mut dyn FnMut(&Name) -> u32, out: &mut impl BufMut) -> Result<(), Malformed> {
    encode_parts(envelope, layout, number, out, false)
}

/// The encoded size of [`encode_head`]'s output.
pub fn head_size(envelope: &Envelope, layout: Layout) -> usize {
    let whole = size(envelope, layout);
    if layout.has_payload() {
        whole - 4 - envelope.payload.len()
    } else {
        whole
    }
}

/// Whether [`encode`] (and [`encode_head`]) would accept `envelope` in
/// `layout`, without writing anything. An encoder that writes name
/// definitions before the record checks first, so a refused record leaves
/// nothing half-written behind.
pub fn validate(envelope: &Envelope, layout: Layout) -> Result<(), Malformed> {
    let flags = envelope.flags() & layout.written();
    let failures = if layout.has_failures() { envelope.failures } else { 0 };
    layout.check(flags, failures)?;
    if layout.has_payload() && envelope.payload.len() > u32::MAX as usize {
        return Err(Malformed::Forbidden("payload over 4 GiB"));
    }
    if layout.has_key() && envelope.key.is_none() {
        return Err(Malformed::Missing("work key"));
    }
    Ok(())
}

fn encode_parts(envelope: &Envelope, layout: Layout, number: &mut dyn FnMut(&Name) -> u32, out: &mut impl BufMut, payload: bool) -> Result<(), Malformed> {
    let flags = envelope.flags() & layout.written();
    let failures = if layout.has_failures() { envelope.failures } else { 0 };
    layout.check(flags, failures)?;
    if layout.has_payload() && envelope.payload.len() > u32::MAX as usize {
        return Err(Malformed::Forbidden("payload over 4 GiB"));
    }
    out.put_slice(&envelope.id.0);
    out.put_slice(&envelope.channel.0);
    out.put_u16_le(envelope.event_type);
    out.put_u16_le(flags);
    out.put_u16_le(failures);
    if layout.has_key() {
        let key = envelope.key.ok_or(Malformed::Missing("work key"))?;
        out.put_u32_le(key.generation);
        put_varint(out, key.arrival);
    }
    if let (true, Some(principal)) = (flags & PRINCIPAL != 0, &envelope.principal) {
        out.put_u32_le(number(principal));
    }
    if let (true, Some(delegate)) = (flags & DELEGATE != 0, &envelope.delegate) {
        out.put_u32_le(number(delegate));
    }
    if flags & NOT_BEFORE != 0 {
        put_varint(out, envelope.not_before.unwrap_or_default());
    }
    if flags & EXPIRES != 0 {
        put_varint(out, envelope.expires.unwrap_or_default());
    }
    if flags & GROUP != 0 {
        out.put_slice(&envelope.group.unwrap_or_default());
    }
    if payload && layout.has_payload() {
        out.put_u32_le(envelope.payload.len() as u32);
        out.put_slice(&envelope.payload);
    }
    Ok(())
}

/// A read position over a shared buffer. Payloads are slices of it, not copies.
pub struct Reader<'a> {
    source: &'a Bytes,
    pub at: usize,
}

impl<'a> Reader<'a> {
    pub fn new(source: &'a Bytes) -> Self {
        Reader { source, at: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.source.len() - self.at
    }

    pub fn take(&mut self, count: usize) -> Result<&'a [u8], Malformed> {
        let end = self.at.checked_add(count).ok_or(Malformed::Short)?;
        let slice = self.source.get(self.at..end).ok_or(Malformed::Short)?;
        self.at = end;
        Ok(slice)
    }

    /// `count` bytes as a slice of the shared buffer.
    pub fn take_shared(&mut self, count: usize) -> Result<Bytes, Malformed> {
        let start = self.at;
        self.take(count)?;
        Ok(self.source.slice(start..self.at))
    }

    pub fn u8(&mut self) -> Result<u8, Malformed> {
        Ok(self.take(1)?[0])
    }

    pub fn u16(&mut self) -> Result<u16, Malformed> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap_or_default()))
    }

    pub fn u32(&mut self) -> Result<u32, Malformed> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap_or_default()))
    }

    pub fn u64(&mut self) -> Result<u64, Malformed> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap_or_default()))
    }

    /// An unsigned LEB128 varint.
    pub fn varint(&mut self) -> Result<u64, Malformed> {
        let mut value = 0u64;
        for index in 0..10 {
            let byte = self.u8()?;
            let part = u64::from(byte & 0x7f);
            if index == 9 && part > 1 {
                return Err(Malformed::Overlong);
            }
            value |= part << (7 * index);
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(Malformed::Overlong)
    }

    /// A varint that must fit in `u32`.
    pub fn varint32(&mut self) -> Result<u32, Malformed> {
        u32::try_from(self.varint()?).map_err(|_| Malformed::Overlong)
    }

    pub fn id(&mut self) -> Result<EventId, Malformed> {
        Ok(EventId(self.bytes16()?))
    }

    pub fn bytes16(&mut self) -> Result<[u8; 16], Malformed> {
        Ok(self.take(16)?.try_into().unwrap_or_default())
    }

    pub fn finish(&self) -> Result<(), Malformed> {
        if self.remaining() == 0 {
            Ok(())
        } else {
            Err(Malformed::Trailing)
        }
    }
}

/// Reads one envelope in `layout`. `name` resolves a dictionary number.
pub fn decode(reader: &mut Reader<'_>, layout: Layout, name: &dyn Fn(u32) -> Option<Name>) -> Result<Envelope, Malformed> {
    decode_parts(reader, layout, name, true)
}

/// Reads what [`encode_head`] wrote. The payload is empty; the caller puts
/// the record's body there.
pub fn decode_head(reader: &mut Reader<'_>, layout: Layout, name: &dyn Fn(u32) -> Option<Name>) -> Result<Envelope, Malformed> {
    decode_parts(reader, layout, name, false)
}

fn decode_parts(reader: &mut Reader<'_>, layout: Layout, name: &dyn Fn(u32) -> Option<Name>, with_payload: bool) -> Result<Envelope, Malformed> {
    let lookup = |number: u32| name(number).ok_or(Malformed::UnknownName(number));
    let id = reader.id()?;
    let channel = Channel(reader.bytes16()?);
    let event_type = reader.u16()?;
    let flags = reader.u16()?;
    let failures = reader.u16()?;
    layout.check(flags, failures)?;
    let key = if layout.has_key() { Some(WorkKey { generation: reader.u32()?, arrival: reader.varint()? }) } else { None };
    let principal = if flags & PRINCIPAL != 0 { Some(lookup(reader.u32()?)?) } else { None };
    let delegate = if flags & DELEGATE != 0 { Some(lookup(reader.u32()?)?) } else { None };
    let not_before = if flags & NOT_BEFORE != 0 { Some(reader.varint()?) } else { None };
    let expires = if flags & EXPIRES != 0 { Some(reader.varint()?) } else { None };
    let group = if flags & GROUP != 0 { Some(reader.bytes16()?) } else { None };
    let payload = if with_payload && layout.has_payload() {
        let length = reader.u32()? as usize;
        reader.take_shared(length)?
    } else {
        Bytes::new()
    };
    let kind = if flags & KIND != 0 { Kind::Dispatch } else { Kind::Claimable };
    let isolated = flags & ISOLATED != 0;
    Ok(Envelope { id, channel, event_type, kind, failures, key, isolated, principal, delegate, not_before, expires, group, payload })
}
