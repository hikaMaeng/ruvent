//! Why a record was refused.

use ruvent::envelope::codec::Malformed;
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProtocolError {
    /// The event head or a field was malformed (see the codec).
    Malformed(Malformed),
    /// A record tag this version does not know.
    UnknownTag(u8),
    /// A known record that may not travel in this direction, or not here
    /// (a `FollowUp` outside a completion, a `Hello` after the first record).
    WrongDirection(u8),
    /// The peer speaks another protocol version.
    Version(u32),
    /// A body longer than the agreed bound, or a head longer than `u16`.
    TooLarge(usize),
    /// A body on a record that carries none.
    UnexpectedBody(u8),
    /// A text field was not UTF-8.
    Utf8,
    /// A code byte this version does not know.
    UnknownCode(u8),
    /// The handshake did not go as the protocol says.
    Handshake(String),
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolError::Malformed(error) => write!(f, "malformed record: {error}"),
            ProtocolError::UnknownTag(tag) => write!(f, "unknown record tag {tag:#04x}"),
            ProtocolError::WrongDirection(tag) => write!(f, "record tag {tag:#04x} may not travel here"),
            ProtocolError::Version(version) => write!(f, "unsupported protocol version {version}"),
            ProtocolError::TooLarge(length) => write!(f, "{length} bytes exceed the bound"),
            ProtocolError::UnexpectedBody(tag) => write!(f, "record tag {tag:#04x} carries no body"),
            ProtocolError::Utf8 => f.write_str("text field is not UTF-8"),
            ProtocolError::UnknownCode(code) => write!(f, "unknown code {code}"),
            ProtocolError::Handshake(reason) => write!(f, "handshake: {reason}"),
        }
    }
}

impl std::error::Error for ProtocolError {}

impl From<Malformed> for ProtocolError {
    fn from(error: Malformed) -> Self {
        ProtocolError::Malformed(error)
    }
}

impl From<ProtocolError> for std::io::Error {
    fn from(error: ProtocolError) -> Self {
        std::io::Error::new(std::io::ErrorKind::InvalidData, error)
    }
}
