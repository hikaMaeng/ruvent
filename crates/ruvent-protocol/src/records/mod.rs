//! Records: what travels inside a frame.
//!
//! | tag | record | direction | fields after the tag |
//! | --- | --- | --- | --- |
//! | `0x01` | Hello | client → server | version u16, token text |
//! | `0x02` | Welcome | server → client | version u16, credential text |
//! | `0x03` | Bye | server → client | reason text |
//! | `0x10` | name definition | both | number u32, text |
//! | `0x11` | dictionary reset | both | — |
//! | `0x20` | 1 register claimable | client → server | req u32, body (request layout) |
//! | `0x21` | 2 register dispatch | client → server | req u32, body (request layout) |
//! | `0x22` | 3 claim | client → server | req u32, id 16, worker u32, lease ms u64 |
//! | `0x23` | 4 complete | client → server | req u32, id 16, token 8, count u32, bodies (request layout) |
//! | `0x24` | 5 delete | client → server | req u32, id 16, result name u32, tag 16, has key u8, [key 8] |
//! | `0x25` | 6 subscribe | client → server | req u32, channel name u32, has key u8, [key 8] |
//! | `0x26` | 7 unsubscribe | client → server | req u32, channel name u32 |
//! | `0x30` | reply: registered | server → client | req u32, id 16, key 8 |
//! | `0x31` | reply: accepted | server → client | req u32, id 16 |
//! | `0x32` | reply: claimed | server → client | req u32, body (claimed layout) |
//! | `0x33` | reply: completed | server → client | req u32 |
//! | `0x34` | reply: deleted | server → client | req u32, removed u8 |
//! | `0x35` | reply: subscribed | server → client | req u32, channel name u32, more u8, count u32, bodies (notice layout) |
//! | `0x36` | reply: unsubscribed | server → client | req u32 |
//! | `0x37` | reply: refused | server → client | req u32, refusal code u8 |
//! | `0x40` | notice | server → client | body (notice layout) |
//! | `0x41` | delivery | server → client | body (delivery layout) |
//!
//! Text is `[length u16][UTF-8]`. Numbers are little-endian; ids are 16 octets
//! in RFC 9562 order. A name number refers to the sender's dictionary as of
//! that point in the stream.

use crate::dictionary::{Incoming, Outgoing};
use crate::error::ProtocolError;
use bytes::{BufMut, Bytes};
use ruvent::envelope::codec::{self, Layout, Reader};
use ruvent::{Call, ClaimToken, Envelope, Name, Outbound, Refusal, Reply, WorkKey};

pub const HELLO: u8 = 0x01;
pub const WELCOME: u8 = 0x02;
pub const BYE: u8 = 0x03;
pub const NAME: u8 = 0x10;
pub const RESET: u8 = 0x11;
pub const REGISTER_CLAIMABLE: u8 = 0x20;
pub const REGISTER_DISPATCH: u8 = 0x21;
pub const CLAIM: u8 = 0x22;
pub const COMPLETE: u8 = 0x23;
pub const DELETE: u8 = 0x24;
pub const SUBSCRIBE: u8 = 0x25;
pub const UNSUBSCRIBE: u8 = 0x26;
pub const REGISTERED: u8 = 0x30;
pub const ACCEPTED: u8 = 0x31;
pub const CLAIMED: u8 = 0x32;
pub const COMPLETED: u8 = 0x33;
pub const DELETED: u8 = 0x34;
pub const SUBSCRIBED: u8 = 0x35;
pub const UNSUBSCRIBED: u8 = 0x36;
pub const REFUSED: u8 = 0x37;
pub const NOTICE: u8 = 0x40;
pub const DELIVERY: u8 = 0x41;

/// Which end is reading: decides which records are allowed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    /// The server reads what clients send.
    Server,
    /// A client reads what the server sends.
    Client,
}

/// One protocol message. Name definitions and resets are not messages: the
/// encoder writes them and the decoder consumes them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    Hello { version: u16, token: String },
    Welcome { version: u16, credential: String },
    Bye { reason: String },
    Call { req: u32, call: Call },
    Outbound(Outbound),
}

/// Records being gathered into one frame.
#[derive(Default)]
pub struct Records {
    pub buffer: Vec<u8>,
    pub count: u32,
}

pub(crate) fn put_text(out: &mut Vec<u8>, text: &str) {
    let bytes = text.as_bytes();
    let length = bytes.len().min(u16::MAX as usize);
    out.put_u16_le(length as u16);
    out.put_slice(&bytes[..length]);
}

fn put_key(out: &mut Vec<u8>, key: Option<WorkKey>) {
    match key {
        Some(key) => {
            out.put_u8(1);
            out.put_u32_le(key.generation);
            out.put_u32_le(key.arrival);
        }
        None => out.put_u8(0),
    }
}

/// Every name `message` encodes, in no particular order.
fn names_of(message: &Message) -> Vec<&Name> {
    let mut names = Vec::new();
    match message {
        Message::Call { call, .. } => match call {
            Call::RegisterClaimable(event) | Call::RegisterDispatch(event) => names.extend(event.names(Layout::Request)),
            Call::Complete { follow_ups, .. } => {
                for event in follow_ups {
                    names.extend(event.names(Layout::Request));
                }
            }
            Call::Delete { result, .. } => names.push(result),
            Call::Subscribe { channel, .. } | Call::Unsubscribe { channel } => names.push(channel),
            Call::Claim { .. } => {}
        },
        Message::Outbound(outbound) => match outbound {
            Outbound::Notice(event) => names.extend(event.names(Layout::Notice)),
            Outbound::Delivery(event) => names.extend(event.names(Layout::Delivery)),
            Outbound::Reply { reply, .. } => match reply {
                Reply::Claimed(event) => names.extend(event.names(Layout::Claimed)),
                Reply::Subscribed { channel, backlog, .. } => {
                    names.push(channel);
                    for event in backlog {
                        names.extend(event.names(Layout::Notice));
                    }
                }
                _ => {}
            },
        },
        Message::Hello { .. } | Message::Welcome { .. } | Message::Bye { .. } => {}
    }
    names
}

fn body(out: &mut Vec<u8>, event: &Envelope, layout: Layout, dictionary: &Outgoing) -> Result<(), ProtocolError> {
    codec::encode(event, layout, &mut |name| dictionary.number(name), out)?;
    Ok(())
}

/// Writes `message`, its name definitions first, into `records`.
fn encode_message(message: &Message, dictionary: &mut Outgoing, records: &mut Records) -> Result<(), ProtocolError> {
    dictionary.define(names_of(message), records)?;
    let out = &mut records.buffer;
    match message {
        Message::Hello { version, token } => {
            out.put_u8(HELLO);
            out.put_u16_le(*version);
            put_text(out, token);
        }
        Message::Welcome { version, credential } => {
            out.put_u8(WELCOME);
            out.put_u16_le(*version);
            put_text(out, credential);
        }
        Message::Bye { reason } => {
            out.put_u8(BYE);
            put_text(out, reason);
        }
        Message::Call { req, call } => match call {
            Call::RegisterClaimable(event) | Call::RegisterDispatch(event) => {
                out.put_u8(if matches!(call, Call::RegisterClaimable(_)) { REGISTER_CLAIMABLE } else { REGISTER_DISPATCH });
                out.put_u32_le(*req);
                body(out, event, Layout::Request, dictionary)?;
            }
            Call::Claim { id, worker, lease_ms } => {
                out.put_u8(CLAIM);
                out.put_u32_le(*req);
                out.put_slice(&id.0);
                out.put_u32_le(*worker);
                out.put_u64_le(*lease_ms);
            }
            Call::Complete { id, token, follow_ups } => {
                out.put_u8(COMPLETE);
                out.put_u32_le(*req);
                out.put_slice(&id.0);
                out.put_u32_le(token.generation);
                out.put_u32_le(token.seq);
                out.put_u32_le(u32::try_from(follow_ups.len()).map_err(|_| ProtocolError::Malformed(codec::Malformed::Forbidden("too many follow-ups")))?);
                for event in follow_ups {
                    body(out, event, Layout::Request, dictionary)?;
                }
            }
            Call::Delete { id, result, tag, key } => {
                out.put_u8(DELETE);
                out.put_u32_le(*req);
                out.put_slice(&id.0);
                out.put_u32_le(dictionary.number(result));
                out.put_slice(&tag.0);
                put_key(out, *key);
            }
            Call::Subscribe { channel, after } => {
                out.put_u8(SUBSCRIBE);
                out.put_u32_le(*req);
                out.put_u32_le(dictionary.number(channel));
                put_key(out, *after);
            }
            Call::Unsubscribe { channel } => {
                out.put_u8(UNSUBSCRIBE);
                out.put_u32_le(*req);
                out.put_u32_le(dictionary.number(channel));
            }
        },
        Message::Outbound(Outbound::Notice(event)) => {
            out.put_u8(NOTICE);
            body(out, event, Layout::Notice, dictionary)?;
        }
        Message::Outbound(Outbound::Delivery(event)) => {
            out.put_u8(DELIVERY);
            body(out, event, Layout::Delivery, dictionary)?;
        }
        Message::Outbound(Outbound::Reply { req, reply }) => {
            let tag = match reply {
                Reply::Registered { .. } => REGISTERED,
                Reply::Accepted { .. } => ACCEPTED,
                Reply::Claimed(_) => CLAIMED,
                Reply::Completed => COMPLETED,
                Reply::Deleted { .. } => DELETED,
                Reply::Subscribed { .. } => SUBSCRIBED,
                Reply::Unsubscribed => UNSUBSCRIBED,
                Reply::Refused(_) => REFUSED,
            };
            out.put_u8(tag);
            out.put_u32_le(*req);
            match reply {
                Reply::Registered { id, key } => {
                    out.put_slice(&id.0);
                    out.put_u32_le(key.generation);
                    out.put_u32_le(key.arrival);
                }
                Reply::Accepted { id } => out.put_slice(&id.0),
                Reply::Claimed(event) => body(out, event, Layout::Claimed, dictionary)?,
                Reply::Deleted { removed } => out.put_u8(u8::from(*removed)),
                Reply::Subscribed { channel, backlog, more } => {
                    out.put_u32_le(dictionary.number(channel));
                    out.put_u8(u8::from(*more));
                    out.put_u32_le(u32::try_from(backlog.len()).map_err(|_| ProtocolError::Malformed(codec::Malformed::Forbidden("backlog too long")))?);
                    for event in backlog {
                        body(out, event, Layout::Notice, dictionary)?;
                    }
                }
                Reply::Refused(refusal) => out.put_u8(refusal.code()),
                Reply::Completed | Reply::Unsubscribed => {}
            }
        }
    }
    records.count += 1;
    Ok(())
}

/// Length field, count field.
const FRAME_HEADER: usize = 8;

/// Turns messages into frames for one direction of one connection.
pub struct Encoder {
    dictionary: Outgoing,
    max_frame: usize,
    current: Records,
    frames: Vec<Bytes>,
}

impl Encoder {
    pub fn new(dictionary: usize, max_frame: usize) -> Self {
        Encoder { dictionary: Outgoing::new(dictionary), max_frame: max_frame.max(64), current: Records::default(), frames: Vec::new() }
    }

    /// Adds a message. Starts a new frame when it would not fit in the current
    /// one; a message that does not fit in an empty frame is refused.
    pub fn push(&mut self, message: &Message) -> Result<(), ProtocolError> {
        let mut scratch = Records::default();
        encode_message(message, &mut self.dictionary, &mut scratch)?;
        // `max_frame` bounds what follows the length field: the count and the records.
        if 4 + scratch.buffer.len() > self.max_frame {
            return Err(ProtocolError::FrameTooLarge(4 + scratch.buffer.len()));
        }
        if 4 + self.current.buffer.len() + scratch.buffer.len() > self.max_frame {
            self.close_frame();
        }
        self.current.buffer.extend_from_slice(&scratch.buffer);
        self.current.count += scratch.count;
        Ok(())
    }

    fn close_frame(&mut self) {
        if self.current.count == 0 {
            return;
        }
        let records = std::mem::take(&mut self.current);
        let mut frame = Vec::with_capacity(FRAME_HEADER + records.buffer.len());
        frame.put_u32_le((4 + records.buffer.len()) as u32);
        frame.put_u32_le(records.count);
        frame.extend_from_slice(&records.buffer);
        self.frames.push(Bytes::from(frame));
    }

    /// Every frame gathered so far, length field included, ready to write.
    pub fn take(&mut self) -> Vec<Bytes> {
        self.close_frame();
        std::mem::take(&mut self.frames)
    }
}

fn text(reader: &mut Reader<'_>) -> Result<String, ProtocolError> {
    let length = reader.u16()? as usize;
    std::str::from_utf8(reader.take(length)?).map(str::to_owned).map_err(|_| ProtocolError::Utf8)
}

fn key(reader: &mut Reader<'_>) -> Result<Option<WorkKey>, ProtocolError> {
    match reader.u8()? {
        0 => Ok(None),
        1 => Ok(Some(WorkKey { generation: reader.u32()?, arrival: reader.u32()? })),
        _ => Err(ProtocolError::Malformed(codec::Malformed::Forbidden("key presence byte"))),
    }
}

fn flag(reader: &mut Reader<'_>) -> Result<bool, ProtocolError> {
    match reader.u8()? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(ProtocolError::Malformed(codec::Malformed::Forbidden("boolean byte"))),
    }
}

/// Turns frames back into messages for one direction of one connection.
pub struct Decoder {
    side: Side,
    dictionary: Incoming,
    max_frame: usize,
}

impl Decoder {
    pub fn new(side: Side, dictionary: usize, max_frame: usize) -> Self {
        Decoder { side, dictionary: Incoming::new(dictionary), max_frame: max_frame.max(64) }
    }

    pub fn max_frame(&self) -> usize {
        self.max_frame
    }

    fn allowed(&self, tag: u8) -> bool {
        match self.side {
            Side::Server => matches!(tag, HELLO | NAME | RESET | REGISTER_CLAIMABLE..=UNSUBSCRIBE),
            Side::Client => matches!(tag, WELCOME | BYE | NAME | RESET | REGISTERED..=REFUSED | NOTICE | DELIVERY),
        }
    }

    /// Decodes one frame: the bytes after its length field. Any error refuses
    /// the whole frame; the connection must then be closed, because the
    /// dictionary may already hold part of it.
    pub fn decode(&mut self, frame: Bytes) -> Result<Vec<Message>, ProtocolError> {
        if frame.len() > self.max_frame {
            return Err(ProtocolError::FrameTooLarge(frame.len()));
        }
        let mut reader = Reader::new(&frame);
        let count = reader.u32()?;
        let mut messages = Vec::new();
        for _ in 0..count {
            let tag = reader.u8()?;
            if !(HELLO..=DELIVERY).contains(&tag) || !self.known(tag) {
                return Err(ProtocolError::UnknownTag(tag));
            }
            if !self.allowed(tag) {
                return Err(ProtocolError::WrongDirection(tag));
            }
            if let Some(message) = self.record(tag, &mut reader)? {
                messages.push(message);
            }
        }
        reader.finish()?;
        Ok(messages)
    }

    fn known(&self, tag: u8) -> bool {
        matches!(tag, HELLO | WELCOME | BYE | NAME | RESET | REGISTER_CLAIMABLE..=UNSUBSCRIBE | REGISTERED..=REFUSED | NOTICE | DELIVERY)
    }

    fn name(&self, number: u32) -> Result<Name, ProtocolError> {
        self.dictionary.get(number).ok_or(ProtocolError::Malformed(codec::Malformed::UnknownName(number)))
    }

    fn body(&self, reader: &mut Reader<'_>, layout: Layout) -> Result<Envelope, ProtocolError> {
        let dictionary = &self.dictionary;
        Ok(codec::decode(reader, layout, &|number| dictionary.get(number))?)
    }

    fn record(&mut self, tag: u8, reader: &mut Reader<'_>) -> Result<Option<Message>, ProtocolError> {
        let message = match tag {
            HELLO => Message::Hello { version: reader.u16()?, token: text(reader)? },
            WELCOME => Message::Welcome { version: reader.u16()?, credential: text(reader)? },
            BYE => Message::Bye { reason: text(reader)? },
            NAME => {
                let number = reader.u32()?;
                let name = Name::from(text(reader)?);
                self.dictionary.define(number, name)?;
                return Ok(None);
            }
            RESET => {
                self.dictionary.reset();
                return Ok(None);
            }
            REGISTER_CLAIMABLE..=UNSUBSCRIBE => {
                let req = reader.u32()?;
                let call = match tag {
                    REGISTER_CLAIMABLE => Call::RegisterClaimable(self.body(reader, Layout::Request)?),
                    REGISTER_DISPATCH => Call::RegisterDispatch(self.body(reader, Layout::Request)?),
                    CLAIM => Call::Claim { id: reader.id()?, worker: reader.u32()?, lease_ms: reader.u64()? },
                    COMPLETE => {
                        let id = reader.id()?;
                        let token = ClaimToken { generation: reader.u32()?, seq: reader.u32()? };
                        let count = reader.u32()?;
                        let mut follow_ups = Vec::new();
                        for _ in 0..count {
                            follow_ups.push(self.body(reader, Layout::Request)?);
                        }
                        Call::Complete { id, token, follow_ups }
                    }
                    DELETE => {
                        let id = reader.id()?;
                        let result = self.name(reader.u32()?)?;
                        let tag = reader.id()?;
                        Call::Delete { id, result, tag, key: key(reader)? }
                    }
                    SUBSCRIBE => {
                        let channel = self.name(reader.u32()?)?;
                        Call::Subscribe { channel, after: key(reader)? }
                    }
                    _ => Call::Unsubscribe { channel: self.name(reader.u32()?)? },
                };
                Message::Call { req, call }
            }
            REGISTERED..=REFUSED => {
                let req = reader.u32()?;
                let reply = match tag {
                    REGISTERED => Reply::Registered { id: reader.id()?, key: WorkKey { generation: reader.u32()?, arrival: reader.u32()? } },
                    ACCEPTED => Reply::Accepted { id: reader.id()? },
                    CLAIMED => Reply::Claimed(self.body(reader, Layout::Claimed)?),
                    COMPLETED => Reply::Completed,
                    DELETED => Reply::Deleted { removed: flag(reader)? },
                    SUBSCRIBED => {
                        let channel = self.name(reader.u32()?)?;
                        let more = flag(reader)?;
                        let count = reader.u32()?;
                        let mut backlog = Vec::new();
                        for _ in 0..count {
                            backlog.push(self.body(reader, Layout::Notice)?);
                        }
                        Reply::Subscribed { channel, backlog, more }
                    }
                    UNSUBSCRIBED => Reply::Unsubscribed,
                    _ => {
                        let code = reader.u8()?;
                        Reply::Refused(Refusal::from_code(code).ok_or(ProtocolError::UnknownRefusal(code))?)
                    }
                };
                Message::Outbound(Outbound::Reply { req, reply })
            }
            NOTICE => Message::Outbound(Outbound::Notice(self.body(reader, Layout::Notice)?)),
            DELIVERY => Message::Outbound(Outbound::Delivery(self.body(reader, Layout::Delivery)?)),
            other => return Err(ProtocolError::UnknownTag(other)),
        };
        Ok(Some(message))
    }
}
