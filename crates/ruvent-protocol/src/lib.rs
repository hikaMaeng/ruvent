//! The event server's binary wire protocol, version 3 (plan 2.2, 2.11).
//!
//! Version 3 differs from 2 in two places only: the envelope has no
//! `correlation` (flag bit 3 is reserved and refused) and `Delete` carries no
//! tag (its outcome names the deleted id in its payload). A version-2 peer is
//! refused at `Hello`, not translated.
//!
//! ## Records
//!
//! There is no outer frame. A connection is a stream of records:
//!
//! ```text
//! record = [tag u8][head_len u16][body_len u32][head …][body …]      (little-endian)
//! ```
//!
//! The head is the record's fields. The body is an event's payload, carried
//! apart so a receiver reads the head first, learns the body's length, and
//! reads the body straight into that event's own buffer ([`io::RecordReader`]);
//! a sender writes a shared payload with a vectored write instead of copying
//! it ([`records::Encoder`]). A record carries at most one body: a completion
//! with follow-ups is `Complete` followed by that many `FollowUp` records, and
//! a subscription answer is `Subscribed` followed by that many `Notice`s. The
//! decoder gathers them and hands over one message when the last one arrived.
//!
//! Inside a head: numbers (sequence numbers, keys, lengths, counts, times)
//! are unsigned LEB128 varints; ids, nonces and groups are 16
//! octets; booleans are one byte, 0 or 1; text is `[varint length][UTF-8]`;
//! a name (channel, type, credential) is a varint number from the sender's
//! dictionary ([`dictionary`]). An event's head is `ruvent::envelope::codec`'s
//! layout without the payload block ([`ruvent::envelope::codec::encode_head`]).
//!
//! ## Tags
//!
//! | tag | record | direction | head | body |
//! | --- | --- | --- | --- | --- |
//! | `0x01` | `Hello` | → server | version, features, role u8, token, presence u8, [producer 16], nonce 16, [linger ms], channel count, channels (text) | — |
//! | `0x02` | `Welcome` | ← server | version, features, server id 16, generation, credential (text), resume, superseded u8 | — |
//! | `0x03` | `Bye` | ← server | reason code u8 | — |
//! | `0x04` | `Fault` | ← server | kind code u8, detail (text, ≤ 255 bytes) | — |
//! | `0x08` | `NameDef` | both | number, name (text) | — |
//! | `0x09` | `DictReset` | both | — | — |
//! | `0x10` | `Register` | → server | seq (0: none, a work connection's dispatch), event head (request layout) | payload |
//! | `0x11` | `Delete` | → server | seq, id 16, result name, key? | — |
//! | `0x12` | `Join` | → server | req, channel, type count, types, lease ms, dispatch u8 | — |
//! | `0x13` | `Window` | → server | channel, count, bytes | — |
//! | `0x14` | `Complete` | → server | seq, id 16, token, follow-up count | — |
//! | `0x15` | `FollowUp` | → server | event head (request layout) | payload |
//! | `0x16` | `Extend` | → server | id 16, token | — |
//! | `0x17` | `Release` | → server | id 16, token, delay ms, failed u8 | — |
//! | `0x18` | `Claim` | → server | req, id 16, lease ms | — |
//! | `0x19` | `Subscribe` | → server | req, target, key? | — |
//! | `0x1a` | `Unsubscribe` | → server | req, target | — |
//! | `0x1b` | `Stats` | → server | req | — |
//! | `0x1c` | `Drain` | → server | req | — |
//! | `0x20` | `Committed` | ← server | seq | — |
//! | `0x21` | `Paused` | ← server | channel | — |
//! | `0x22` | `Resumed` | ← server | channel | — |
//! | `0x23` | `Joined` | ← server | req | — |
//! | `0x24` | `Refused` | ← server | req, refusal code u8 | — |
//! | `0x25` | `Pending` | ← server | channel | — |
//! | `0x26` | `Assign` | ← server | assignment | payload |
//! | `0x27` | `Claimed` | ← server | req, assignment | payload |
//! | `0x28` | `Subscribed` | ← server | req, target, more u8, notice count | — |
//! | `0x29` | `Unsubscribed` | ← server | req | — |
//! | `0x2a` | `Notice` | ← server | event head (notice layout) | — |
//! | `0x2b` | `Delivery` | ← server | event head (delivery layout) | payload |
//! | `0x2c` | `StatsReply` | ← server | req, totals, counters, top channels, idle lanes, top roots | — |
//! | `0x2d` | `Draining` | ← server | req | — |
//!
//! * token = generation, seq. key? = presence u8, then generation, arrival.
//!   target = kind u8 (1 channel, 2 prefix), name.
//! * assignment = token, lease ms, remaining lease ms, more u8, event head
//!   (assigned layout: work key and failure count included). The server
//!   writes the lease left at the moment it starts writing the record; a
//!   decoded `ruvent::Assignment` carries it in `until` (the client has no
//!   server clock), so `until` on the client side is "ms left when sent".
//! * Hello presence bits: 1 producer id present, 2 linger present, 4 no `Committed` (acks off).
//! * The reason, fault and refusal codes are `ruvent::ByeReason`,
//!   `ruvent::FaultKind` and `ruvent::Refusal` (`code()`/`from_code()`).
//! * Records without a body must say `body_len` 0. A body is at most the
//!   agreed bound (B13, 16 MiB by default).
//! * The first record of a connection is `Hello`; nothing may come before it
//!   (its channels are text, not dictionary names).
//!
//! The pre-dictionary `Reply` record of the plan's transition stage (3a) does
//! not exist: the overhaul is implemented at once, so no per-call reply is
//! ever carried.
//!
//! ## Feature bits ([`features`])
//!
//! | bit | feature |
//! | --- | --- |
//! | `1 << 0` | producer sequences and cumulative `Committed` (2.3) |
//! | `1 << 1` | `Paused`/`Resumed` (2.6) |
//! | `1 << 2` | windows: `Join`, `Window`, `Assign`, `Pending`, `Extend`, `Release` (2.4) |
//! | `1 << 3` | `not_before` and `expires` (2.4) |
//! | `1 << 4` | `Stats` and `Drain` (2.8) |
//! | `1 << 5` | prefix subscriptions (4장) |
//! | `1 << 6` | `group` (B26) |
//!
//! The server announces what it implements in `Welcome`; a client uses
//! nothing the server did not announce.
//!
//! ## Version 1
//!
//! Version 1 is refused, not translated (B12). A version-1 client's first
//! bytes are a frame length, never the `Hello` tag, and [`io::sniff_v1`]
//! recognises its hello so the server can answer it in its own format.
//!
//! ## Errors
//!
//! Anything this version does not know — an unknown tag, a record from the
//! wrong direction, a forbidden envelope combination, an undefined name, a
//! head with bytes left over, a body on a record that has none — refuses the
//! record; the connection is then closed, because the dictionary may already
//! be out of step.

pub mod dictionary;
pub mod error;
pub mod io;
pub mod records;

pub use dictionary::{Incoming, Outgoing};
pub use error::ProtocolError;
pub use io::{sniff_v1, Parts, RawRecord, RecordReader, HEADER};
pub use records::{validate, Body, Decoder, Encoder, Message, Side};

/// The only protocol version this build speaks.
pub const VERSION: u32 = 3;
/// Default bound on each direction's name dictionary (S18).
pub const DICTIONARY: usize = 4096;
/// Default bound on one record's body (B13).
pub const MAX_BODY: usize = 16 << 20;
/// Default: a body below this many bytes is copied into the head's buffer
/// when sent, instead of being its own vector entry (S11).
pub const INLINE: usize = 256;
/// Default: a body of at least this many bytes is read straight from the
/// socket into its own buffer; smaller ones are copied out of the read
/// buffer (S11).
pub const DIRECT: usize = 4096;

/// The feature bits of `Hello` and `Welcome`.
pub mod features {
    pub const SEQUENCE: u64 = 1 << 0;
    pub const PAUSE: u64 = 1 << 1;
    pub const WINDOW: u64 = 1 << 2;
    pub const TIMING: u64 = 1 << 3;
    pub const OPERATE: u64 = 1 << 4;
    pub const PREFIX: u64 = 1 << 5;
    pub const GROUP: u64 = 1 << 6;
    /// Everything this build implements.
    pub const ALL: u64 = SEQUENCE | PAUSE | WINDOW | TIMING | OPERATE | PREFIX | GROUP;
}

#[cfg(test)]
mod tests;
