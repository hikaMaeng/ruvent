//! The small enumerations a record carries as a code byte.
//!
//! Codes start at 1; 0 is never a valid code, so a zeroed byte is caught.
//!
//! | code | [`ByeReason`] | [`FaultKind`] | [`Refusal`] |
//! | --- | --- | --- | --- |
//! | 1 | `Version` | `Forbidden` | `Forbidden` |
//! | 2 | `Token` | `Malformed` | `Missing` |
//! | 3 | `Forbidden` | `InvalidId` | `AlreadyClaimed` |
//! | 4 | `Superseded` | `IdConflict` | `Malformed` |
//! | 5 | `Slow` | `FollowUpConflict` | `Closed` |
//! | 6 | `Draining` | `SequenceGap` | |
//! | 7 | `Protocol` | | |

use std::fmt;

macro_rules! coded {
    ($name:ident { $($variant:ident),* $(,)? }) => {
        impl $name {
            pub const ALL: &'static [$name] = &[$($name::$variant),*];

            pub fn code(self) -> u8 {
                $name::ALL.iter().position(|item| *item == self).unwrap_or_default() as u8 + 1
            }

            pub fn from_code(code: u8) -> Option<$name> {
                $name::ALL.get(usize::from(code).checked_sub(1)?).copied()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Debug::fmt(self, f)
            }
        }
    };
}

/// Why the server ends a connection (2.9 says what the SDK does for each).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ByeReason {
    /// The wire version is not one this server speaks.
    Version,
    /// The token resolves to no credential.
    Token,
    /// The credential may not write to a channel declared in `Hello`.
    Forbidden,
    /// Another instance opened a connection under the same producer id (2.3).
    Superseded,
    /// The connection did not read what it was sent (2.6).
    Slow,
    /// The server is draining (2.8).
    Draining,
    /// A sequence gap or a record the role may not send.
    Protocol,
}

coded!(ByeReason { Version, Token, Forbidden, Superseded, Slow, Draining, Protocol });

/// A configuration, format or conflict error, told once per (connection, kind) (2.7).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FaultKind {
    /// The credential may not do this on this channel.
    Forbidden,
    /// A field the request may not carry, the wrong kind or role, too many follow-ups.
    Malformed,
    /// The id is neither nil nor a UUID.
    InvalidId,
    /// An active event has this id with other content.
    IdConflict,
    /// A completion's follow-up conflicts with an active event or a sibling.
    FollowUpConflict,
    /// A sequence number skipped ahead.
    SequenceGap,
}

coded!(FaultKind { Forbidden, Malformed, InvalidId, IdConflict, FollowUpConflict, SequenceGap });

impl FaultKind {
    /// The bit of this kind in a connection's "already told" set.
    pub fn bit(self) -> u8 {
        1 << (self.code() - 1)
    }
}

/// Why a call that is answered (`Join`, explicit `Claim`, `Subscribe`,
/// `Stats`, `Drain`) had no effect.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Refusal {
    /// The credential may not do this on this channel.
    Forbidden,
    /// No event with this id is waiting and visible.
    Missing,
    /// The event is claimed.
    AlreadyClaimed,
    /// The call does not fit the connection's role or carries what it may not.
    Malformed,
    /// The connection is closed; nothing was sent or nothing came back. Client side only.
    Closed,
    /// A `Join` selects lanes another join of this connection selects.
    Overlap,
}

coded!(Refusal { Forbidden, Missing, AlreadyClaimed, Malformed, Closed, Overlap });

impl std::error::Error for Refusal {}
