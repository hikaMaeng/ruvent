//! The event server's binary wire protocol.
//!
//! One body codec (`ruvent::envelope::codec`), one outer framing:
//!
//! ```text
//! frame  = [length u32][count u32][record]…      length counts the bytes after itself
//! record = [tag u8] then the record's fields     (tag table in `records`)
//! ```
//!
//! A connection opens with `Hello { version, token }` from the client,
//! answered by `Welcome { version, credential }` or `Bye { reason }`. After
//! that the client sends method calls and the server sends replies, notices
//! and deliveries, all in order on the one connection.
//!
//! Names (channels, types, credentials) travel as numbers. Each direction of
//! each connection keeps its own dictionary: the sender defines a name in the
//! stream before its first use and, when the dictionary reaches its bound,
//! writes a reset and starts over. Because the sender encodes in send order,
//! an old number can never be read as a new name. The receiver enforces the
//! same bound.
//!
//! Anything this version does not know — an unknown version, an unknown or
//! wrong-direction tag, a forbidden envelope combination, an undefined name,
//! bytes left over — refuses the whole frame; the connection is then closed.

pub mod dictionary;
pub mod error;
pub mod io;
pub mod records;

pub use dictionary::{Incoming, Outgoing};
pub use error::ProtocolError;
pub use io::{read_frame, write_frames};
pub use records::{Decoder, Encoder, Message, Side};

/// The only protocol version this build speaks.
pub const VERSION: u16 = 1;
/// Default bound on each direction's name dictionary.
pub const DICTIONARY: usize = 4096;
/// Default bound on one frame, length field excluded.
pub const MAX_FRAME: usize = 16 << 20;

#[cfg(test)]
mod tests;
