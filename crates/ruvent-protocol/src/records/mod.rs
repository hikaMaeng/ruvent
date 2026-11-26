//! Records: the messages of 2.11 and their tags. The tag table is in the
//! crate's documentation.
//!
//! * [`Encoder`] turns messages into records for one direction of one
//!   connection: names defined first, heads gathered into one buffer, bodies
//!   kept as the shared payloads they are.
//! * [`Decoder`] turns records back into messages, gathering a completion's
//!   `FollowUp`s and a subscription answer's `Notice`s.

mod decode;
mod encode;
pub(crate) mod fields;
mod stats;

pub use decode::Decoder;
pub use encode::{validate, Encoder};

use bytes::Bytes;
use ruvent::envelope::codec::Layout;
use ruvent::{Call, Hello, Name, Outbound, Spooled, Welcome};

pub const HELLO: u8 = 0x01;
pub const WELCOME: u8 = 0x02;
pub const BYE: u8 = 0x03;
pub const FAULT: u8 = 0x04;
pub const NAME_DEF: u8 = 0x08;
pub const RESET: u8 = 0x09;
pub const REGISTER: u8 = 0x10;
pub const DELETE: u8 = 0x11;
pub const JOIN: u8 = 0x12;
pub const WINDOW: u8 = 0x13;
pub const COMPLETE: u8 = 0x14;
pub const FOLLOW_UP: u8 = 0x15;
pub const EXTEND: u8 = 0x16;
pub const RELEASE: u8 = 0x17;
pub const CLAIM: u8 = 0x18;
pub const SUBSCRIBE: u8 = 0x19;
pub const UNSUBSCRIBE: u8 = 0x1a;
pub const STATS: u8 = 0x1b;
pub const DRAIN: u8 = 0x1c;
pub const COMMITTED: u8 = 0x20;
pub const PAUSED: u8 = 0x21;
pub const RESUMED: u8 = 0x22;
pub const JOINED: u8 = 0x23;
pub const REFUSED: u8 = 0x24;
pub const PENDING: u8 = 0x25;
pub const ASSIGN: u8 = 0x26;
pub const CLAIMED: u8 = 0x27;
pub const SUBSCRIBED: u8 = 0x28;
pub const UNSUBSCRIBED: u8 = 0x29;
pub const NOTICE: u8 = 0x2a;
pub const DELIVERY: u8 = 0x2b;
pub const STATS_REPLY: u8 = 0x2c;
pub const DRAINING: u8 = 0x2d;

/// Which end is reading: decides which records are allowed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    /// The server reads what clients send.
    Server,
    /// A client reads what the server sends.
    Client,
}

/// Records a client sends.
pub(crate) fn to_server(tag: u8) -> bool {
    matches!(tag, HELLO | NAME_DEF | RESET | REGISTER..=DRAIN)
}

/// Records the server sends.
pub(crate) fn to_client(tag: u8) -> bool {
    matches!(tag, WELCOME | BYE | FAULT | NAME_DEF | RESET | COMMITTED..=DRAINING)
}

/// Records that carry a body.
pub(crate) fn has_body(tag: u8) -> bool {
    matches!(tag, REGISTER | FOLLOW_UP | ASSIGN | CLAIMED | DELIVERY)
}

/// One protocol message. `NameDef` and `DictReset` are not messages: the
/// encoder writes them and the decoder consumes them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    /// `hello.credential` is not sent (the server resolves `token`); a
    /// decoded hello has an empty credential.
    Hello {
        version: u32,
        features: u64,
        token: String,
        hello: Hello,
    },
    /// A decoded `Welcome` is always this. An encoder also takes
    /// `Outbound::Welcome` and writes its own version and features.
    Welcome {
        version: u32,
        features: u64,
        welcome: Welcome,
    },
    Call(Call),
    Outbound(Outbound),
}

/// What a record's body became on the receiving side.
pub enum Body {
    /// The record carries no body, or it was empty.
    None,
    Bytes(Bytes),
    /// A follow-up's body the server received straight to disk (2.6).
    Spooled(Spooled),
}

/// Every name `message` encodes as a dictionary number, in no particular order.
pub(crate) fn names_of(message: &Message) -> Vec<&Name> {
    let mut names = Vec::new();
    match message {
        Message::Hello { .. } | Message::Welcome { .. } => {}
        Message::Call(call) => match call {
            Call::Register { event, .. } => names.extend(event.names(Layout::Request)),
            Call::Complete { follow_ups, .. } => {
                for event in follow_ups {
                    names.extend(event.names(Layout::Request));
                }
            }
            Call::Delete { .. }
            | Call::Join { .. }
            | Call::Window { .. }
            | Call::Extend { .. }
            | Call::Release { .. }
            | Call::Claim { .. }
            | Call::Subscribe { .. }
            | Call::Unsubscribe { .. }
            | Call::Stats { .. }
            | Call::Drain { .. } => {}
        },
        Message::Outbound(outbound) => match outbound {
            Outbound::Assign(assignment) | Outbound::Claimed { assignment, .. } => names.extend(assignment.event.names(Layout::Assigned)),
            Outbound::Subscribed { backlog, .. } => {
                for event in backlog {
                    names.extend(event.names(Layout::Notice));
                }
            }
            Outbound::Notice(event) => names.extend(event.names(Layout::Notice)),
            Outbound::Delivery(event) => names.extend(event.names(Layout::Delivery)),
            Outbound::Welcome(_)
            | Outbound::StatsReply { .. }
            | Outbound::Paused { .. }
            | Outbound::Resumed { .. }
            | Outbound::Bye(_)
            | Outbound::Fault { .. }
            | Outbound::Committed { .. }
            | Outbound::Joined { .. }
            | Outbound::Pending { .. }
            | Outbound::Refused { .. }
            | Outbound::Unsubscribed { .. }
            | Outbound::Draining { .. } => {}
        },
    }
    names
}
