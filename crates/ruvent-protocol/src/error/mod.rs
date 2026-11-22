//! Why a frame was refused.

use ruvent::envelope::codec::Malformed;
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProtocolError {
    /// The envelope body or a field was malformed (see the codec).
    Malformed(Malformed),
    /// A record tag this version does not know.
    UnknownTag(u8),
    /// A known record that may not travel in this direction.
    WrongDirection(u8),
    /// The peer speaks another protocol version.
    Version(u16),
    /// A frame longer than the agreed bound.
    FrameTooLarge(usize),
    /// A dictionary rule was broken: over its bound, or a number redefined without a reset.
    Dictionary(&'static str),
    /// A text field was not UTF-8.
    Utf8,
    /// A refusal code this version does not know.
    UnknownRefusal(u8),
    /// The handshake did not go as the protocol says.
    Handshake(String),
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolError::Malformed(error) => write!(f, "malformed record: {error}"),
            ProtocolError::UnknownTag(tag) => write!(f, "unknown record tag {tag:#04x}"),
            ProtocolError::WrongDirection(tag) => write!(f, "record tag {tag:#04x} may not travel in this direction"),
            ProtocolError::Version(version) => write!(f, "unsupported protocol version {version}"),
            ProtocolError::FrameTooLarge(length) => write!(f, "frame of {length} bytes exceeds the bound"),
            ProtocolError::Dictionary(rule) => write!(f, "name dictionary: {rule}"),
            ProtocolError::Utf8 => f.write_str("text field is not UTF-8"),
            ProtocolError::UnknownRefusal(code) => write!(f, "unknown refusal code {code}"),
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
