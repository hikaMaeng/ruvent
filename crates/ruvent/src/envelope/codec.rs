//! The envelope's byte layout. One body codec, used by the wire and the journal.
//!
//! Numbers are little-endian; `id` and `correlation` are 16 octets in RFC 9562
//! order. Names travel as numbers from a dictionary the surrounding stream
//! defines first; this module only asks for the number of a name and the name
//! of a number. A decoder knows every length from the [`Layout`] and the
//! `flags` byte alone.

use super::{Claim, ClaimToken, Envelope, EventId, Kind, WorkKey};
use crate::names::Name;
use bytes::{BufMut, Bytes};
use std::fmt;

/// Bit 0: the kind. Set means dispatch.
pub const KIND: u8 = 1 << 0;
/// Bit 1: claimed.
pub const CLAIMED: u8 = 1 << 1;
/// Bits 2 to 5: optional fields present.
pub const REPLY: u8 = 1 << 2;
pub const CORRELATION: u8 = 1 << 3;
pub const PRINCIPAL: u8 = 1 << 4;
pub const DELEGATE: u8 = 1 << 5;
/// Bits 6 and 7: reserved, always zero.
pub const RESERVED: u8 = 0b1100_0000;
const OPTIONAL: u8 = REPLY | CORRELATION | PRINCIPAL | DELEGATE;

/// id 16 + channel 4 + type 4 + flags 1 + retry 2.
pub const HEAD: usize = 27;
/// generation 4 + arrival 4.
pub const KEY: usize = 8;
/// token 8 + claimer 4 + worker 4 + deadline 8.
pub const CLAIM: usize = 24;

/// Which fields a record of each kind carries.
///
/// | layout | carries | refused |
/// | --- | --- | --- |
/// | `Request` | head, optional, payload | claimed, retry ≠ 0, principal |
/// | `Notice` | head, work key | claimed, any optional field |
/// | `Claimed` | head, work key, claim block, optional, payload | not claimed, dispatch |
/// | `Delivery` | head, optional, payload | claimed, retry ≠ 0, claimable |
/// | `Journal` | head, work key, optional, payload | claimed, dispatch |
///
/// Every layout refuses dispatch together with claimed, and the reserved bits.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Layout {
    /// Registration requests (methods 1 and 2) and the follow-ups of a completion.
    Request,
    /// "There is claimable work": a notice, or one entry of a subscription's backlog.
    Notice,
    /// The answer to a successful claim.
    Claimed,
    /// A dispatch event handed to a subscriber.
    Delivery,
    /// The body of a journal registration.
    Journal,
}

impl Layout {
    pub fn has_key(self) -> bool {
        matches!(self, Layout::Notice | Layout::Claimed | Layout::Journal)
    }

    pub fn has_optional(self) -> bool {
        !matches!(self, Layout::Notice)
    }

    pub fn has_payload(self) -> bool {
        !matches!(self, Layout::Notice)
    }

    /// The flag bits this layout writes; the rest are cleared on encode.
    fn written(self) -> u8 {
        match self {
            Layout::Notice => KIND,
            Layout::Claimed => KIND | CLAIMED | OPTIONAL,
            Layout::Request | Layout::Delivery | Layout::Journal => KIND | OPTIONAL,
        }
    }

    fn check(self, flags: u8, retry: u16) -> Result<(), Malformed> {
        if flags & RESERVED != 0 {
            return Err(Malformed::Reserved);
        }
        if flags & KIND != 0 && flags & CLAIMED != 0 {
            return Err(Malformed::ClaimedDispatch);
        }
        let dispatch = flags & KIND != 0;
        let claimed = flags & CLAIMED != 0;
        match self {
            Layout::Request => {
                if claimed {
                    return Err(Malformed::Forbidden("claimed"));
                }
                if retry != 0 {
                    return Err(Malformed::Forbidden("retry"));
                }
                if flags & PRINCIPAL != 0 {
                    return Err(Malformed::Forbidden("principal"));
                }
            }
            Layout::Notice => {
                if claimed {
                    return Err(Malformed::Forbidden("claimed"));
                }
                if flags & OPTIONAL != 0 {
                    return Err(Malformed::Forbidden("optional field"));
                }
            }
            Layout::Claimed => {
                if !claimed {
                    return Err(Malformed::Forbidden("unclaimed"));
                }
            }
            Layout::Delivery => {
                if claimed {
                    return Err(Malformed::Forbidden("claimed"));
                }
                if retry != 0 {
                    return Err(Malformed::Forbidden("retry"));
                }
                if !dispatch {
                    return Err(Malformed::Forbidden("claimable"));
                }
            }
            Layout::Journal => {
                if claimed {
                    return Err(Malformed::Forbidden("claimed"));
                }
                if dispatch {
                    return Err(Malformed::Forbidden("dispatch"));
                }
            }
        }
        Ok(())
    }
}

/// Why bytes were refused.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Malformed {
    /// Fewer bytes than the layout needs.
    Short,
    Reserved,
    ClaimedDispatch,
    /// A field this layout does not allow.
    Forbidden(&'static str),
    /// A name number the stream never defined.
    UnknownName(u32),
    /// The envelope lacks a block the layout needs (encoding only).
    Missing(&'static str),
    /// Bytes left over after the last record.
    Trailing,
}

impl fmt::Display for Malformed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Malformed::Short => f.write_str("record cut short"),
            Malformed::Reserved => f.write_str("reserved flag bit set"),
            Malformed::ClaimedDispatch => f.write_str("dispatch event marked claimed"),
            Malformed::Forbidden(what) => write!(f, "{what} not allowed in this record"),
            Malformed::UnknownName(number) => write!(f, "name {number} was never defined"),
            Malformed::Missing(what) => write!(f, "{what} missing"),
            Malformed::Trailing => f.write_str("bytes left over"),
        }
    }
}

impl std::error::Error for Malformed {}

/// The encoded size of `envelope` in `layout`, not counting name definitions.
pub fn size(envelope: &Envelope, layout: Layout) -> usize {
    let flags = envelope.flags() & layout.written();
    let mut size = HEAD;
    if layout.has_key() {
        size += KEY;
    }
    if layout == Layout::Claimed && flags & CLAIMED != 0 {
        size += CLAIM;
    }
    if flags & REPLY != 0 {
        size += 4;
    }
    if flags & CORRELATION != 0 {
        size += 16;
    }
    if flags & PRINCIPAL != 0 {
        size += 4;
    }
    if flags & DELEGATE != 0 {
        size += 4;
    }
    if layout.has_payload() {
        size += 4 + envelope.payload.len();
    }
    size
}

/// Appends `envelope` in `layout`. `number` gives the dictionary number of a
/// name; the caller has defined every name of [`Envelope::names`] beforehand.
pub fn encode(envelope: &Envelope, layout: Layout, number: &mut dyn FnMut(&Name) -> u32, out: &mut impl BufMut) -> Result<(), Malformed> {
    let flags = envelope.flags() & layout.written();
    let retry = if matches!(layout, Layout::Request | Layout::Delivery) { 0 } else { envelope.retry };
    layout.check(flags, retry)?;
    if layout.has_payload() && envelope.payload.len() > u32::MAX as usize {
        return Err(Malformed::Forbidden("payload over 4 GiB"));
    }
    out.put_slice(&envelope.id.0);
    out.put_u32_le(number(&envelope.channel));
    out.put_u32_le(number(&envelope.event_type));
    out.put_u8(flags);
    out.put_u16_le(retry);
    if layout.has_key() {
        let key = envelope.key.ok_or(Malformed::Missing("work key"))?;
        out.put_u32_le(key.generation);
        out.put_u32_le(key.arrival);
    }
    if layout == Layout::Claimed {
        let claim = envelope.claim.ok_or(Malformed::Missing("claim block"))?;
        out.put_u32_le(claim.token.generation);
        out.put_u32_le(claim.token.seq);
        out.put_u32_le(claim.claimer);
        out.put_u32_le(claim.worker);
        out.put_u64_le(claim.deadline);
    }
    if flags & REPLY != 0 {
        out.put_u32_le(number(envelope.reply.as_ref().unwrap_or(&envelope.channel)));
    }
    if flags & CORRELATION != 0 {
        out.put_slice(&envelope.correlation.unwrap_or_default().0);
    }
    if flags & PRINCIPAL != 0 {
        out.put_u32_le(number(envelope.principal.as_ref().unwrap_or(&envelope.channel)));
    }
    if flags & DELEGATE != 0 {
        out.put_u32_le(number(envelope.delegate.as_ref().unwrap_or(&envelope.channel)));
    }
    if layout.has_payload() {
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

    pub fn id(&mut self) -> Result<EventId, Malformed> {
        Ok(EventId(self.take(16)?.try_into().unwrap_or_default()))
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
    let lookup = |number: u32| name(number).ok_or(Malformed::UnknownName(number));
    let id = reader.id()?;
    let channel = lookup(reader.u32()?)?;
    let event_type = lookup(reader.u32()?)?;
    let flags = reader.u8()?;
    let retry = reader.u16()?;
    layout.check(flags, retry)?;
    let key = if layout.has_key() { Some(WorkKey { generation: reader.u32()?, arrival: reader.u32()? }) } else { None };
    let claim = if layout == Layout::Claimed {
        Some(Claim { token: ClaimToken { generation: reader.u32()?, seq: reader.u32()? }, claimer: reader.u32()?, worker: reader.u32()?, deadline: reader.u64()? })
    } else {
        None
    };
    let reply = if flags & REPLY != 0 { Some(lookup(reader.u32()?)?) } else { None };
    let correlation = if flags & CORRELATION != 0 { Some(reader.id()?) } else { None };
    let principal = if flags & PRINCIPAL != 0 { Some(lookup(reader.u32()?)?) } else { None };
    let delegate = if flags & DELEGATE != 0 { Some(lookup(reader.u32()?)?) } else { None };
    let payload = if layout.has_payload() {
        let length = reader.u32()? as usize;
        reader.take_shared(length)?
    } else {
        Bytes::new()
    };
    let kind = if flags & KIND != 0 { Kind::Dispatch } else { Kind::Claimable };
    Ok(Envelope { id, channel, event_type, kind, retry, key, claim, reply, correlation, principal, delegate, payload })
}
