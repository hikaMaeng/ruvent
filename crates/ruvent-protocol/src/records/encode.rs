//! Messages → records, for one direction of one connection.

use super::fields::{close, cut, open, put_bool, put_channel, put_key, put_text, put_token, put_varint, DETAIL};
use super::*;
use crate::dictionary::Outgoing;
use crate::error::ProtocolError;
use crate::io::Parts;
use crate::{features, DICTIONARY, INLINE, MAX_BODY, VERSION};
use bytes::{BufMut, Bytes, BytesMut};
use ruvent::envelope::codec::{self, Layout};
use ruvent::{Assignment, Envelope, Role};

/// The longest text one head carries (a name, a token, a credential).
const TEXT: usize = 32 << 10;
/// The longest head a record may have.
const HEAD: usize = u16::MAX as usize;

pub(crate) fn role_code(role: Role) -> u8 {
    match role {
        Role::Produce => 1,
        Role::Work => 2,
        Role::Observe => 3,
    }
}

/// Turns messages into records.
///
/// Heads (and bodies below the inline bound, S11) gather in one buffer; a
/// larger body is its own entry, the shared payload itself, so a payload
/// going to many connections is never copied per connection. [`Encoder::take`]
/// hands the gathered entries to one vectored write.
///
/// A message is checked whole before anything is written, so a refused
/// message leaves neither a record nor a name definition behind.
pub struct Encoder {
    dictionary: Outgoing,
    features: u64,
    inline: usize,
    max_body: usize,
    /// The sender's uptime, for an assignment's remaining lease.
    now: u64,
    buf: BytesMut,
    parts: Vec<Bytes>,
    /// Bytes in `parts`.
    parted: usize,
}

impl Default for Encoder {
    fn default() -> Self {
        Encoder::new(DICTIONARY)
    }
}

impl Encoder {
    pub fn new(dictionary: usize) -> Self {
        Encoder { dictionary: Outgoing::new(dictionary), features: features::ALL, inline: INLINE, max_body: MAX_BODY, now: 0, buf: BytesMut::with_capacity(4096), parts: Vec::new(), parted: 0 }
    }

    /// The feature bits written into `Welcome` (from `Outbound::Welcome`).
    pub fn with_features(mut self, features: u64) -> Self {
        self.features = features;
        self
    }

    /// Bodies below this many bytes are copied into the head buffer (S11).
    pub fn with_inline(mut self, inline: usize) -> Self {
        self.inline = inline;
        self
    }

    /// The largest body a record may carry (B13).
    pub fn with_max_body(mut self, max_body: usize) -> Self {
        self.max_body = max_body;
        self
    }

    /// The sender's uptime now: an assignment's remaining lease is its
    /// `until` minus this. Left at 0, `until` is written as it is.
    pub fn set_now(&mut self, now: u64) {
        self.now = now;
    }

    /// Bytes gathered and not yet taken.
    pub fn len(&self) -> usize {
        self.buf.len() + self.parted
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Everything gathered so far, ready for one vectored write.
    pub fn take(&mut self) -> Parts {
        if !self.buf.is_empty() {
            self.parts.push(self.buf.split().freeze());
        }
        self.parted = 0;
        Parts::new(std::mem::take(&mut self.parts))
    }

    /// Whether `payload` is one of the gathered entries itself (not a copy).
    #[cfg(test)]
    pub(crate) fn shares(&self, payload: &Bytes) -> bool {
        self.parts.iter().any(|part| part.as_ptr() == payload.as_ptr() && part.len() == payload.len())
    }

    /// Adds a message: its name definitions, then its records.
    pub fn push(&mut self, message: &Message) -> Result<(), ProtocolError> {
        self.check(message)?;
        self.dictionary.define(names_of(message), &mut self.buf)?;
        self.write(message);
        Ok(())
    }

    /// Everything that could refuse `message`, before a byte is written.
    fn check(&self, message: &Message) -> Result<(), ProtocolError> {
        validate(message, self.max_body)
    }

    /// A body after its record's head: copied below the inline bound, its own
    /// entry otherwise.
    fn body(&mut self, payload: &Bytes) {
        if payload.is_empty() {
            return;
        }
        if payload.len() < self.inline {
            self.buf.put_slice(payload);
            return;
        }
        if !self.buf.is_empty() {
            self.parted += self.buf.len();
            self.parts.push(self.buf.split().freeze());
        }
        self.parted += payload.len();
        self.parts.push(payload.clone());
    }

    /// A record that carries an event: `prefix` writes the fields before the event's head.
    fn event(&mut self, tag: u8, prefix: impl FnOnce(&mut BytesMut, &Outgoing), event: &Envelope, layout: Layout) {
        let Encoder { dictionary, buf, .. } = &mut *self;
        let at = open(buf, tag);
        prefix(buf, dictionary);
        let _ = codec::encode_head(event, layout, &mut |name| dictionary.number(name), buf);
        let body = if layout.has_payload() { event.payload.len() as u32 } else { 0 };
        close(buf, at, body);
        if layout.has_payload() {
            self.body(&event.payload);
        }
    }

    /// A record without a body.
    fn plain(&mut self, tag: u8, fields: impl FnOnce(&mut BytesMut, &Outgoing)) {
        let Encoder { dictionary, buf, .. } = &mut *self;
        let at = open(buf, tag);
        fields(buf, dictionary);
        close(buf, at, 0);
    }

    fn assignment(&mut self, tag: u8, req: Option<u32>, assignment: &Assignment) {
        let remaining = assignment.until.saturating_sub(self.now);
        let prefix = |out: &mut BytesMut, _: &Outgoing| {
            if let Some(req) = req {
                put_varint(out, u64::from(req));
            }
            put_token(out, assignment.token);
            put_varint(out, assignment.lease_ms);
            put_varint(out, remaining);
            put_bool(out, assignment.more);
        };
        self.event(tag, prefix, &assignment.event, Layout::Assigned);
    }

    fn write(&mut self, message: &Message) {
        match message {
            Message::Hello { version, features, token, hello } => self.plain(HELLO, |out, _| {
                put_varint(out, u64::from(*version));
                put_varint(out, *features);
                out.put_u8(role_code(hello.role));
                put_text(out, token);
                let presence = u8::from(hello.producer.is_some()) | (u8::from(hello.linger_ms.is_some()) << 1) | (u8::from(!hello.acks) << 2);
                out.put_u8(presence);
                if let Some(producer) = &hello.producer {
                    out.put_slice(producer);
                }
                out.put_slice(&hello.nonce);
                if let Some(linger) = hello.linger_ms {
                    put_varint(out, linger);
                }
                put_varint(out, hello.types.len() as u64);
                for kind in &hello.types {
                    out.put_u16_le(*kind);
                }
            }),
            Message::Welcome { version, features, welcome } => self.welcome(*version, *features, welcome),
            Message::Outbound(Outbound::Welcome(welcome)) => self.welcome(VERSION, self.features, welcome),
            Message::Call(call) => self.call(call),
            Message::Outbound(outbound) => self.outbound(outbound),
        }
    }

    fn welcome(&mut self, version: u32, features: u64, welcome: &Welcome) {
        self.plain(WELCOME, |out, _| {
            put_varint(out, u64::from(version));
            put_varint(out, features);
            out.put_slice(&welcome.server_id);
            put_varint(out, u64::from(welcome.generation));
            put_text(out, welcome.credential.as_str());
            put_varint(out, welcome.resume);
            put_bool(out, welcome.superseded);
        });
    }

    fn call(&mut self, call: &Call) {
        match call {
            Call::Register { seq, event } => self.event(REGISTER, |out, _| put_varint(out, seq.unwrap_or(0)), event, Layout::Request),
            Call::Delete { seq, id, result, key } => self.plain(DELETE, |out, _| {
                put_varint(out, *seq);
                out.put_slice(&id.0);
                put_channel(out, result);
                put_key(out, *key);
            }),
            Call::Join { req, channel, types, isolated, lease_ms, dispatch } => self.plain(JOIN, |out, _| {
                put_varint(out, u64::from(*req));
                put_bool(out, channel.is_some());
                if let Some(channel) = channel {
                    put_channel(out, channel);
                }
                put_varint(out, types.len() as u64);
                for kind in types {
                    out.put_u16_le(*kind);
                }
                put_bool(out, *isolated);
                put_varint(out, *lease_ms);
                put_bool(out, *dispatch);
            }),
            Call::Window { join, count, bytes } => self.plain(WINDOW, |out, _| {
                put_varint(out, u64::from(*join));
                put_varint(out, u64::from(*count));
                put_varint(out, *bytes);
            }),
            Call::Complete { seq, id, token, follow_ups, .. } => {
                self.plain(COMPLETE, |out, _| {
                    put_varint(out, *seq);
                    out.put_slice(&id.0);
                    put_token(out, *token);
                    put_varint(out, follow_ups.len() as u64);
                });
                for event in follow_ups {
                    self.event(FOLLOW_UP, |_, _| {}, event, Layout::Request);
                }
            }
            Call::Extend { id, token } => self.plain(EXTEND, |out, _| {
                out.put_slice(&id.0);
                put_token(out, *token);
            }),
            Call::Release { id, token, delay_ms, failed } => self.plain(RELEASE, |out, _| {
                out.put_slice(&id.0);
                put_token(out, *token);
                put_varint(out, *delay_ms);
                put_bool(out, *failed);
            }),
            Call::Claim { req, id, lease_ms } => self.plain(CLAIM, |out, _| {
                put_varint(out, u64::from(*req));
                out.put_slice(&id.0);
                put_varint(out, *lease_ms);
            }),
            Call::Subscribe { req, channel, after } => self.plain(SUBSCRIBE, |out, _| {
                put_varint(out, u64::from(*req));
                put_channel(out, channel);
                put_key(out, *after);
            }),
            Call::Unsubscribe { req, channel } => self.plain(UNSUBSCRIBE, |out, _| {
                put_varint(out, u64::from(*req));
                put_channel(out, channel);
            }),
            Call::Stats { req } => self.plain(STATS, |out, _| put_varint(out, u64::from(*req))),
            Call::Drain { req } => self.plain(DRAIN, |out, _| put_varint(out, u64::from(*req))),
        }
    }

    fn outbound(&mut self, outbound: &Outbound) {
        match outbound {
            Outbound::Welcome(welcome) => self.welcome(VERSION, self.features, welcome),
            Outbound::Bye(reason) => self.plain(BYE, |out, _| out.put_u8(reason.code())),
            Outbound::Fault { kind, detail } => self.plain(FAULT, |out, _| {
                out.put_u8(kind.code());
                put_text(out, cut(detail, DETAIL));
            }),
            Outbound::Committed { seq } => self.plain(COMMITTED, |out, _| put_varint(out, *seq)),
            Outbound::Paused { event_type } => self.plain(PAUSED, |out, _| out.put_u16_le(*event_type)),
            Outbound::Resumed { event_type } => self.plain(RESUMED, |out, _| out.put_u16_le(*event_type)),
            Outbound::Joined { req } => self.plain(JOINED, |out, _| put_varint(out, u64::from(*req))),
            Outbound::Refused { req, reason } => self.plain(REFUSED, |out, _| {
                put_varint(out, u64::from(*req));
                out.put_u8(reason.code());
            }),
            Outbound::Pending { join } => self.plain(PENDING, |out, _| put_varint(out, u64::from(*join))),
            Outbound::Assign(assignment) => self.assignment(ASSIGN, None, assignment),
            Outbound::Claimed { req, assignment } => self.assignment(CLAIMED, Some(*req), assignment),
            Outbound::Subscribed { req, channel, backlog, more } => {
                self.plain(SUBSCRIBED, |out, _| {
                    put_varint(out, u64::from(*req));
                    put_channel(out, channel);
                    put_bool(out, *more);
                    put_varint(out, backlog.len() as u64);
                });
                for event in backlog {
                    self.event(NOTICE, |_, _| {}, event, Layout::Notice);
                }
            }
            Outbound::Unsubscribed { req } => self.plain(UNSUBSCRIBED, |out, _| put_varint(out, u64::from(*req))),
            Outbound::Notice(event) => self.event(NOTICE, |_, _| {}, event, Layout::Notice),
            Outbound::Delivery(event) => self.event(DELIVERY, |_, _| {}, event, Layout::Delivery),
            Outbound::StatsReply { req, stats } => self.plain(STATS_REPLY, |out, _| {
                put_varint(out, u64::from(*req));
                stats::encode(stats, out);
            }),
            Outbound::Draining { req } => self.plain(DRAINING, |out, _| put_varint(out, u64::from(*req))),
        }
    }
}

fn check_event(event: &Envelope, layout: Layout, max_body: usize) -> Result<(), ProtocolError> {
    codec::validate(event, layout)?;
    if layout.has_payload() && event.payload.len() > max_body {
        return Err(ProtocolError::TooLarge(event.payload.len()));
    }
    Ok(())
}

/// Whether an [`Encoder`] with this body bound would take `message`: the
/// envelope rules of each record's layout, the body bound, text and count
/// bounds. A sender that numbers calls checks before it takes a number, so a
/// refused call never leaves a gap in the sequence.
pub fn validate(message: &Message, max_body: usize) -> Result<(), ProtocolError> {
    for name in names_of(message) {
        if name.as_str().len() > TEXT {
            return Err(ProtocolError::TooLarge(name.as_str().len()));
        }
    }
    match message {
        Message::Hello { token, hello, .. } => {
            let head = 64 + token.len() + hello.types.len() * 2;
            if token.len() > TEXT || head > HEAD {
                return Err(ProtocolError::TooLarge(head));
            }
        }
        Message::Welcome { welcome, .. } | Message::Outbound(Outbound::Welcome(welcome)) => {
            if welcome.credential.as_str().len() > TEXT {
                return Err(ProtocolError::TooLarge(welcome.credential.as_str().len()));
            }
        }
        Message::Call(Call::Register { event, .. }) => check_event(event, Layout::Request, max_body)?,
        Message::Call(Call::Complete { follow_ups, spooled, .. }) => {
            if !spooled.is_empty() {
                return Err(ProtocolError::Malformed(codec::Malformed::Forbidden("a spooled body travels only inside the server")));
            }
            for event in follow_ups {
                check_event(event, Layout::Request, max_body)?;
            }
        }
        Message::Call(Call::Join { types, .. }) if types.len() > TEXT / 2 => return Err(ProtocolError::TooLarge(types.len())),
        Message::Outbound(Outbound::Assign(assignment) | Outbound::Claimed { assignment, .. }) => check_event(&assignment.event, Layout::Assigned, max_body)?,
        Message::Outbound(Outbound::Subscribed { backlog, .. }) => {
            for event in backlog {
                check_event(event, Layout::Notice, max_body)?;
            }
        }
        Message::Outbound(Outbound::Notice(event)) => check_event(event, Layout::Notice, max_body)?,
        Message::Outbound(Outbound::Delivery(event)) => check_event(event, Layout::Delivery, max_body)?,
        _ => {}
    }
    Ok(())
}
