//! The field encodings inside a record's head, and the record header.

use crate::error::ProtocolError;
use crate::io::HEADER;
use bytes::{BufMut, BytesMut};
use ruvent::envelope::codec::{self, Malformed, Reader};
use ruvent::{Channel, ClaimToken, WorkKey};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

/// Starts a record: the tag and a header whose lengths [`close`] fills in.
/// Returns where the record starts.
pub(crate) fn open(out: &mut BytesMut, tag: u8) -> usize {
    let at = out.len();
    out.put_u8(tag);
    out.put_u16_le(0);
    out.put_u32_le(0);
    at
}

/// Ends a record's head: writes its length and the body length that follows.
/// The encoder has checked beforehand that the head fits in `u16`.
pub(crate) fn close(out: &mut BytesMut, at: usize, body_len: u32) {
    let head = out.len() - at - HEADER;
    let head = u16::try_from(head).unwrap_or(u16::MAX);
    out[at + 1..at + 3].copy_from_slice(&head.to_le_bytes());
    out[at + 3..at + 7].copy_from_slice(&body_len.to_le_bytes());
}

pub(crate) fn put_varint(out: &mut impl BufMut, value: u64) {
    codec::put_varint(out, value);
}

pub(crate) fn put_bool(out: &mut impl BufMut, value: bool) {
    out.put_u8(u8::from(value));
}

/// Text: `[varint length][UTF-8]`. The encoder bounds text lengths first.
pub(crate) fn put_text(out: &mut impl BufMut, text: &str) {
    put_varint(out, text.len() as u64);
    out.put_slice(text.as_bytes());
}

pub(crate) fn put_token(out: &mut impl BufMut, token: ClaimToken) {
    put_varint(out, u64::from(token.generation));
    put_varint(out, token.seq);
}

pub(crate) fn put_key(out: &mut impl BufMut, key: Option<WorkKey>) {
    match key {
        Some(key) => {
            out.put_u8(1);
            put_varint(out, u64::from(key.generation));
            put_varint(out, key.arrival);
        }
        None => out.put_u8(0),
    }
}

/// A channel: its 16 octets. The server checks that it is a UUIDv7.
pub(crate) fn put_channel(out: &mut impl BufMut, channel: &Channel) {
    out.put_slice(&channel.0);
}

pub(crate) fn text(reader: &mut Reader<'_>) -> Result<String, ProtocolError> {
    let length = usize::try_from(reader.varint()?).map_err(|_| Malformed::Overlong)?;
    std::str::from_utf8(reader.take(length)?).map(str::to_owned).map_err(|_| ProtocolError::Utf8)
}

pub(crate) fn boolean(reader: &mut Reader<'_>) -> Result<bool, ProtocolError> {
    match reader.u8()? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(ProtocolError::Malformed(Malformed::Forbidden("boolean byte"))),
    }
}

pub(crate) fn varint32(reader: &mut Reader<'_>) -> Result<u32, ProtocolError> {
    Ok(reader.varint32()?)
}

pub(crate) fn token(reader: &mut Reader<'_>) -> Result<ClaimToken, ProtocolError> {
    Ok(ClaimToken { generation: reader.varint32()?, seq: reader.varint()? })
}

pub(crate) fn key(reader: &mut Reader<'_>) -> Result<Option<WorkKey>, ProtocolError> {
    match reader.u8()? {
        0 => Ok(None),
        1 => Ok(Some(WorkKey { generation: reader.varint32()?, arrival: reader.varint()? })),
        _ => Err(ProtocolError::Malformed(Malformed::Forbidden("key presence byte"))),
    }
}

pub(crate) fn channel(reader: &mut Reader<'_>) -> Result<Channel, ProtocolError> {
    Ok(Channel(reader.bytes16()?))
}

/// The longest fault detail sent; longer ones are cut at a character boundary.
pub(crate) const DETAIL: usize = 255;

pub(crate) fn cut(text: &str, max: usize) -> &str {
    if text.len() <= max {
        return text;
    }
    let mut end = max;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// Distinct fault details kept as `&'static str` on the receiving side.
const INTERNED: usize = 256;

/// `Outbound::Fault` holds a `&'static str`: the server's details are
/// constants, and a client keeps each distinct one it was told.
///
/// | key | the detail text |
/// | --- | --- |
/// | scope | the process |
/// | lifetime | the process |
/// | bound | [`INTERNED`] entries (at most [`DETAIL`] bytes each); past it every new detail reads as a fixed text |
/// | cleared by | nobody: the bound is the cleanup |
pub(crate) fn intern(text: String) -> &'static str {
    static TABLE: OnceLock<Mutex<HashMap<String, &'static str>>> = OnceLock::new();
    let table = TABLE.get_or_init(Mutex::default);
    let Ok(mut table) = table.lock() else { return "(fault detail unavailable)" };
    if let Some(known) = table.get(&text) {
        return known;
    }
    if table.len() >= INTERNED {
        return "(fault detail not kept: too many distinct details)";
    }
    let kept: &'static str = Box::leak(text.clone().into_boxed_str());
    table.insert(text, kept);
    kept
}
