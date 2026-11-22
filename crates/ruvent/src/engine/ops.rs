//! The seven methods as values: what a connection asks and what it is told.
//! The protocol encodes exactly these; the in-process client sends them as they are.

use crate::envelope::{ClaimToken, Envelope, EventId, WorkKey};
use crate::names::Name;
use std::fmt;

/// One engine-wide connection number. Never reused by one engine.
pub type SessionId = u64;

/// A method call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Call {
    /// 1. Register a claimable event.
    RegisterClaimable(Envelope),
    /// 2. Register a dispatch event.
    RegisterDispatch(Envelope),
    /// 3. Mark a claim on the event that has this id now. `lease_ms` is a duration.
    Claim { id: EventId, worker: u32, lease_ms: u64 },
    /// 4. Report completion of the claim `token` on `id`, registering `follow_ups` in the same step.
    Complete { id: EventId, token: ClaimToken, follow_ups: Vec<Envelope> },
    /// 5. Delete a waiting event; the outcome is announced on `result` with correlation `tag`.
    Delete { id: EventId, result: Name, tag: EventId, key: Option<WorkKey> },
    /// 6. Subscribe to a channel, with the waiting backlog after `after`.
    Subscribe { channel: Name, after: Option<WorkKey> },
    /// 7. Unsubscribe.
    Unsubscribe { channel: Name },
}

/// What a call was answered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reply {
    Registered {
        id: EventId,
        key: WorkKey,
    },
    Accepted {
        id: EventId,
    },
    /// The whole envelope as it was when the claim was made: work key and claim block included.
    Claimed(Envelope),
    Completed,
    /// Whether the event was removed. The result event says the same on the result channel.
    Deleted {
        removed: bool,
    },
    /// Notices for waiting events in work-key order; `more` means call again after the last key.
    Subscribed {
        channel: Name,
        backlog: Vec<Envelope>,
        more: bool,
    },
    Unsubscribed,
    Refused(Refusal),
}

/// Why a call had no effect. "This request had no effect", and nothing more.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Refusal {
    /// An event with this id is in the claimable queue, or two follow-ups share an id.
    Duplicate,
    /// The id is neither nil nor a UUID.
    InvalidId,
    /// The connection's credential may not do this on this channel.
    Forbidden,
    /// The request carried a field it may not (claim block, retry, work key, principal) or the wrong kind.
    Malformed,
    /// No event with this id is in the claimable queue.
    Missing,
    /// The event is already claimed.
    AlreadyClaimed,
    /// The event is not claimed.
    NotClaimed,
    /// The claim token is not the current claim's.
    StaleToken,
    /// The reporting connection's credential is not the claimer's.
    WrongCredential,
    /// The connection is closed; nothing was sent or nothing came back.
    Closed,
}

impl Refusal {
    pub const ALL: [Refusal; 10] = [
        Refusal::Duplicate,
        Refusal::InvalidId,
        Refusal::Forbidden,
        Refusal::Malformed,
        Refusal::Missing,
        Refusal::AlreadyClaimed,
        Refusal::NotClaimed,
        Refusal::StaleToken,
        Refusal::WrongCredential,
        Refusal::Closed,
    ];

    pub fn code(self) -> u8 {
        Refusal::ALL.iter().position(|refusal| *refusal == self).unwrap_or_default() as u8 + 1
    }

    pub fn from_code(code: u8) -> Option<Refusal> {
        Refusal::ALL.get(usize::from(code).checked_sub(1)?).copied()
    }
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl std::error::Error for Refusal {}

/// What the engine hands a connection, in order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outbound {
    Reply {
        req: u32,
        reply: Reply,
    },
    /// Claimable work: head and work key.
    Notice(Envelope),
    /// A dispatch event.
    Delivery(Envelope),
}

/// The type of the dispatch event announcing a successful deletion.
pub const DELETED: &str = "deleted";
/// The type of the dispatch event announcing a failed deletion.
pub const DELETE_FAILED: &str = "delete-failed";

/// Why a deletion failed, the last payload byte of a [`DELETE_FAILED`] event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum DeleteFailure {
    /// No event with that id is waiting (never registered, completed, sent, or reclaimed under a new id).
    Missing = 1,
    /// It is claimed.
    Claimed = 2,
    /// The id is there but belongs to another registration than the work key named.
    OtherRegistration = 3,
}
