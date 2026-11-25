//! Records → messages, for one direction of one connection.

use super::fields::{boolean, intern, key, name, target, text, token, varint32};
use super::*;
use crate::dictionary::Incoming;
use crate::error::ProtocolError;
use crate::io::RawRecord;
use crate::VERSION;
use bytes::Bytes;
use ruvent::envelope::codec::{self, Layout, Malformed, Reader};
use ruvent::{Assignment, ByeReason, ClaimToken, Envelope, EventId, FaultKind, Refusal, Role, Target};

/// Most follow-ups one `Complete`, or notices one `Subscribed`, may announce.
/// The engine bounds both far lower (S10, `backlog_page`); this only keeps a
/// count from a broken peer from meaning anything.
const GATHERED: u64 = 1 << 16;
/// Most channels a `Hello` may declare.
const DECLARED: u64 = 1 << 12;

/// A message whose records are still arriving.
enum Gather {
    Complete { seq: u64, id: EventId, token: ClaimToken, left: u64, follow_ups: Vec<Envelope>, spooled: Vec<(u32, Spooled)> },
    Subscribed { req: u32, target: Target, more: bool, left: u64, backlog: Vec<Envelope> },
}

/// Turns records back into messages.
///
/// Reading one record is three steps, so the body can go where it belongs
/// before it is read: [`Decoder::admit`] the head (may this record come now,
/// does it carry a body), read the body, then [`Decoder::decode`]. Any error
/// refuses the record and the connection must be closed, because the
/// dictionary may already be out of step.
///
/// The gathering state:
///
/// | key | the message being gathered |
/// | --- | --- |
/// | scope | one direction of one connection |
/// | lifetime | from `Complete`/`Subscribed` to its last `FollowUp`/`Notice` |
/// | bound | one message; its records are counted by its head |
/// | cleared by | the last record of the message |
pub struct Decoder {
    side: Side,
    dictionary: Incoming,
    started: bool,
    gather: Option<Gather>,
}

fn known(tag: u8) -> bool {
    to_server(tag) || to_client(tag)
}

fn role(code: u8) -> Result<Role, ProtocolError> {
    match code {
        1 => Ok(Role::Produce),
        2 => Ok(Role::Work),
        3 => Ok(Role::Observe),
        other => Err(ProtocolError::UnknownCode(other)),
    }
}

fn req(reader: &mut Reader<'_>) -> Result<u32, ProtocolError> {
    varint32(reader)
}

fn count(reader: &mut Reader<'_>, bound: u64) -> Result<u64, ProtocolError> {
    let count = reader.varint()?;
    if count > bound {
        return Err(ProtocolError::TooLarge(usize::try_from(count).unwrap_or(usize::MAX)));
    }
    Ok(count)
}

impl Decoder {
    pub fn new(side: Side, dictionary: usize) -> Self {
        Decoder { side, dictionary: Incoming::new(dictionary), started: false, gather: None }
    }

    /// Whether a message is half gathered: a record boundary that is not a
    /// message boundary.
    pub fn gathering(&self) -> bool {
        self.gather.is_some()
    }

    /// Checks a record's head before its body is read: whether it may come
    /// now, and whether it carries a body. Returns whether it does.
    pub fn admit(&self, record: &RawRecord) -> Result<bool, ProtocolError> {
        let tag = record.tag;
        if !known(tag) {
            return Err(ProtocolError::UnknownTag(tag));
        }
        let allowed = match self.side {
            Side::Server => to_server(tag),
            Side::Client => to_client(tag),
        };
        if !allowed {
            return Err(ProtocolError::WrongDirection(tag));
        }
        if !self.started {
            let first = match self.side {
                Side::Server => tag == HELLO,
                Side::Client => matches!(tag, WELCOME | BYE),
            };
            if !first {
                return Err(ProtocolError::Handshake(format!("record {tag:#04x} before the handshake")));
            }
        } else if matches!(tag, HELLO | WELCOME) {
            return Err(ProtocolError::WrongDirection(tag));
        }
        let fits = match &self.gather {
            Some(Gather::Complete { .. }) => matches!(tag, FOLLOW_UP | NAME_DEF | RESET),
            Some(Gather::Subscribed { .. }) => matches!(tag, NOTICE | NAME_DEF | RESET),
            None => tag != FOLLOW_UP,
        };
        if !fits {
            return Err(ProtocolError::WrongDirection(tag));
        }
        let body = has_body(tag);
        if !body && record.body_len != 0 {
            return Err(ProtocolError::UnexpectedBody(tag));
        }
        Ok(body)
    }

    /// The event head of a `FollowUp` (payload empty), read without
    /// consuming the record: where its body should go depends on its channel.
    pub fn follow_up(&self, record: &RawRecord) -> Result<Envelope, ProtocolError> {
        if record.tag != FOLLOW_UP {
            return Err(ProtocolError::WrongDirection(record.tag));
        }
        let mut reader = Reader::new(&record.head);
        self.event(&mut reader, Layout::Request)
    }

    /// Decodes one admitted record whose body is `body`. `None`: a dictionary
    /// record, or a record of a message still being gathered.
    pub fn decode(&mut self, record: &RawRecord, body: Body) -> Result<Option<Message>, ProtocolError> {
        self.admit(record)?;
        let mut reader = Reader::new(&record.head);
        let message = self.record(record, &mut reader, body)?;
        reader.finish()?;
        Ok(message)
    }

    fn event(&self, reader: &mut Reader<'_>, layout: Layout) -> Result<Envelope, ProtocolError> {
        let dictionary = &self.dictionary;
        Ok(codec::decode_head(reader, layout, &|number| dictionary.get(number))?)
    }

    /// An event head with the record's body as its payload.
    fn carried(&self, reader: &mut Reader<'_>, layout: Layout, record: &RawRecord, body: Body) -> Result<Envelope, ProtocolError> {
        let mut event = self.event(reader, layout)?;
        event.payload = bytes(record, body)?;
        Ok(event)
    }

    fn assignment(&self, reader: &mut Reader<'_>, record: &RawRecord, body: Body) -> Result<Assignment, ProtocolError> {
        let token = token(reader)?;
        let lease_ms = reader.varint()?;
        let remaining = reader.varint()?;
        let more = boolean(reader)?;
        let event = self.carried(reader, Layout::Assigned, record, body)?;
        Ok(Assignment { event, token, lease_ms, until: remaining, more })
    }

    fn record(&mut self, record: &RawRecord, reader: &mut Reader<'_>, body: Body) -> Result<Option<Message>, ProtocolError> {
        let tag = record.tag;
        let message = match tag {
            NAME_DEF => {
                let number = varint32(reader)?;
                let name = Name::from(text(reader)?);
                self.dictionary.define(number, name)?;
                return Ok(None);
            }
            RESET => {
                self.dictionary.reset();
                return Ok(None);
            }
            HELLO => {
                let version = varint32(reader)?;
                if version != VERSION {
                    return Err(ProtocolError::Version(version));
                }
                let features = reader.varint()?;
                let role = role(reader.u8()?)?;
                let token = text(reader)?;
                let presence = reader.u8()?;
                if presence & !0b111 != 0 {
                    return Err(ProtocolError::Malformed(Malformed::Reserved));
                }
                let producer = if presence & 1 != 0 { Some(reader.bytes16()?) } else { None };
                let nonce = reader.bytes16()?;
                let linger_ms = if presence & 2 != 0 { Some(reader.varint()?) } else { None };
                let acks = presence & 4 == 0;
                let mut channels = Vec::new();
                for _ in 0..count(reader, DECLARED)? {
                    channels.push(Name::from(text(reader)?));
                }
                self.started = true;
                Message::Hello { version, features, token, hello: Hello { role, credential: Name::new(""), producer, nonce, linger_ms, acks, channels } }
            }
            WELCOME => {
                let version = varint32(reader)?;
                if version != VERSION {
                    return Err(ProtocolError::Version(version));
                }
                let features = reader.varint()?;
                let server_id = reader.bytes16()?;
                let generation = varint32(reader)?;
                let credential = Name::from(text(reader)?);
                let resume = reader.varint()?;
                let superseded = boolean(reader)?;
                self.started = true;
                Message::Welcome { version, features, welcome: Welcome { server_id, generation, credential, resume, superseded } }
            }
            BYE => {
                let code = reader.u8()?;
                self.started = true;
                Message::Outbound(Outbound::Bye(ByeReason::from_code(code).ok_or(ProtocolError::UnknownCode(code))?))
            }
            FAULT => {
                let code = reader.u8()?;
                let kind = FaultKind::from_code(code).ok_or(ProtocolError::UnknownCode(code))?;
                Message::Outbound(Outbound::Fault { kind, detail: intern(text(reader)?) })
            }
            REGISTER => {
                let seq = reader.varint()?;
                let event = self.carried(reader, Layout::Request, record, body)?;
                Message::Call(Call::Register { seq: (seq != 0).then_some(seq), event })
            }
            DELETE => {
                let seq = reader.varint()?;
                let id = reader.id()?;
                let result = name(reader, &self.dictionary)?;
                let tag = reader.id()?;
                Message::Call(Call::Delete { seq, id, result, tag, key: key(reader)? })
            }
            JOIN => {
                let req = req(reader)?;
                let channel = name(reader, &self.dictionary)?;
                let mut types = Vec::new();
                for _ in 0..count(reader, GATHERED)? {
                    types.push(name(reader, &self.dictionary)?);
                }
                let lease_ms = reader.varint()?;
                Message::Call(Call::Join { req, channel, types, lease_ms, dispatch: boolean(reader)? })
            }
            WINDOW => {
                let channel = name(reader, &self.dictionary)?;
                Message::Call(Call::Window { channel, count: varint32(reader)?, bytes: reader.varint()? })
            }
            COMPLETE => {
                let seq = reader.varint()?;
                let id = reader.id()?;
                let token = token(reader)?;
                let left = count(reader, GATHERED)?;
                if left > 0 {
                    self.gather = Some(Gather::Complete { seq, id, token, left, follow_ups: Vec::new(), spooled: Vec::new() });
                    return Ok(None);
                }
                Message::Call(Call::Complete { seq, id, token, follow_ups: Vec::new(), spooled: Vec::new() })
            }
            FOLLOW_UP => {
                let event = self.event(reader, Layout::Request)?;
                let Some(Gather::Complete { follow_ups, spooled, left, .. }) = &mut self.gather else { return Err(ProtocolError::WrongDirection(tag)) };
                let position = follow_ups.len() as u32;
                let mut event = event;
                match body {
                    Body::Spooled(body) => {
                        if u64::from(body.at.len) != u64::from(record.body_len) {
                            return Err(ProtocolError::Malformed(Malformed::Short));
                        }
                        spooled.push((position, body));
                    }
                    other => event.payload = bytes(record, other)?,
                }
                follow_ups.push(event);
                *left -= 1;
                if *left > 0 {
                    return Ok(None);
                }
                match self.gather.take() {
                    Some(Gather::Complete { seq, id, token, follow_ups, spooled, .. }) => Message::Call(Call::Complete { seq, id, token, follow_ups, spooled }),
                    _ => return Err(ProtocolError::WrongDirection(tag)),
                }
            }
            EXTEND => {
                let id = reader.id()?;
                Message::Call(Call::Extend { id, token: token(reader)? })
            }
            RELEASE => {
                let id = reader.id()?;
                let token = token(reader)?;
                let delay_ms = reader.varint()?;
                Message::Call(Call::Release { id, token, delay_ms, failed: boolean(reader)? })
            }
            CLAIM => {
                let req = req(reader)?;
                let id = reader.id()?;
                Message::Call(Call::Claim { req, id, lease_ms: reader.varint()? })
            }
            SUBSCRIBE => {
                let req = req(reader)?;
                let target = target(reader, &self.dictionary)?;
                Message::Call(Call::Subscribe { req, target, after: key(reader)? })
            }
            UNSUBSCRIBE => {
                let req = req(reader)?;
                Message::Call(Call::Unsubscribe { req, target: target(reader, &self.dictionary)? })
            }
            STATS => Message::Call(Call::Stats { req: req(reader)? }),
            DRAIN => Message::Call(Call::Drain { req: req(reader)? }),
            COMMITTED => Message::Outbound(Outbound::Committed { seq: reader.varint()? }),
            PAUSED => Message::Outbound(Outbound::Paused { channel: name(reader, &self.dictionary)? }),
            RESUMED => Message::Outbound(Outbound::Resumed { channel: name(reader, &self.dictionary)? }),
            JOINED => Message::Outbound(Outbound::Joined { req: req(reader)? }),
            REFUSED => {
                let req = req(reader)?;
                let code = reader.u8()?;
                Message::Outbound(Outbound::Refused { req, reason: Refusal::from_code(code).ok_or(ProtocolError::UnknownCode(code))? })
            }
            PENDING => Message::Outbound(Outbound::Pending { channel: name(reader, &self.dictionary)? }),
            ASSIGN => Message::Outbound(Outbound::Assign(self.assignment(reader, record, body)?)),
            CLAIMED => {
                let req = req(reader)?;
                Message::Outbound(Outbound::Claimed { req, assignment: self.assignment(reader, record, body)? })
            }
            SUBSCRIBED => {
                let req = req(reader)?;
                let target = target(reader, &self.dictionary)?;
                let more = boolean(reader)?;
                let left = count(reader, GATHERED)?;
                if left > 0 {
                    self.gather = Some(Gather::Subscribed { req, target, more, left, backlog: Vec::new() });
                    return Ok(None);
                }
                Message::Outbound(Outbound::Subscribed { req, target, backlog: Vec::new(), more })
            }
            UNSUBSCRIBED => Message::Outbound(Outbound::Unsubscribed { req: req(reader)? }),
            NOTICE => {
                let event = self.event(reader, Layout::Notice)?;
                match &mut self.gather {
                    Some(Gather::Subscribed { backlog, left, .. }) => {
                        backlog.push(event);
                        *left -= 1;
                        if *left > 0 {
                            return Ok(None);
                        }
                        match self.gather.take() {
                            Some(Gather::Subscribed { req, target, more, backlog, .. }) => Message::Outbound(Outbound::Subscribed { req, target, backlog, more }),
                            _ => return Err(ProtocolError::WrongDirection(tag)),
                        }
                    }
                    _ => Message::Outbound(Outbound::Notice(event)),
                }
            }
            DELIVERY => Message::Outbound(Outbound::Delivery(self.carried(reader, Layout::Delivery, record, body)?)),
            STATS_REPLY => {
                let req = req(reader)?;
                Message::Outbound(Outbound::StatsReply { req, stats: Box::new(stats::decode(reader, &self.dictionary)?) })
            }
            DRAINING => Message::Outbound(Outbound::Draining { req: req(reader)? }),
            other => return Err(ProtocolError::UnknownTag(other)),
        };
        Ok(Some(message))
    }
}

/// A record's body as bytes: it must be exactly as long as the head said.
fn bytes(record: &RawRecord, body: Body) -> Result<Bytes, ProtocolError> {
    let bytes = match body {
        Body::None => Bytes::new(),
        Body::Bytes(bytes) => bytes,
        Body::Spooled(_) => return Err(ProtocolError::Malformed(Malformed::Forbidden("a spooled body on a record that takes it in memory"))),
    };
    if bytes.len() as u64 != u64::from(record.body_len) {
        return Err(ProtocolError::Malformed(Malformed::Short));
    }
    Ok(bytes)
}
