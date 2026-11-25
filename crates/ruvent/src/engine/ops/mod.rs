//! What a connection asks and what it is told, as values.
//!
//! These map one to one onto the wire v2 records of the plan's 2.11, so the
//! protocol encodes exactly these and the in-process client sends them as
//! they are. Records that belong to the connection alone — `NameDef`,
//! `DictReset`, the version and feature bits — are not here.
//!
//! | 2.11 record | value |
//! | --- | --- |
//! | `Hello` | [`Hello`] (given to `Engine::open`) |
//! | `Welcome` | [`Outbound::Welcome`] |
//! | `Bye` | [`Outbound::Bye`] with [`ByeReason`] |
//! | `Fault` | [`Outbound::Fault`] with [`FaultKind`] |
//! | `Register` | [`Call::Register`] |
//! | `Delete` | [`Call::Delete`] |
//! | `Committed` | [`Outbound::Committed`] |
//! | `Paused`, `Resumed` | [`Outbound::Paused`], [`Outbound::Resumed`] |
//! | `Join` | [`Call::Join`] |
//! | `Joined`, `Refused` | [`Outbound::Joined`], [`Outbound::Refused`] with [`Refusal`] |
//! | `Window` | [`Call::Window`] |
//! | `Pending` | [`Outbound::Pending`] |
//! | `Assign` | [`Outbound::Assign`] with [`Assignment`] |
//! | `Complete` + `FollowUp`… | [`Call::Complete`] (the follow-ups gathered) |
//! | `Extend` | [`Call::Extend`] |
//! | `Release` | [`Call::Release`] |
//! | `Claim` | [`Call::Claim`] |
//! | `Claimed` | [`Outbound::Claimed`] |
//! | `Subscribe`, `Unsubscribe` | [`Call::Subscribe`], [`Call::Unsubscribe`] with [`Target`] |
//! | `Subscribed` (+ its `Notice`s) | [`Outbound::Subscribed`] |
//! | `Unsubscribed` | [`Outbound::Unsubscribed`] |
//! | `Notice` | [`Outbound::Notice`] |
//! | `Delivery` | [`Outbound::Delivery`] |
//! | `Stats` | [`Call::Stats`] |
//! | `StatsReply` | [`Outbound::StatsReply`] with [`Stats`] |
//! | `Drain` | [`Call::Drain`] |
//! | `Draining` | [`Outbound::Draining`] |

mod codes;
mod stats;

use crate::envelope::{ClaimToken, Envelope, EventId, ProducerId, WorkKey};
use crate::names::Name;

pub use codes::{ByeReason, FaultKind, Refusal};
pub use stats::{ChannelStat, Counters, IdleLane, RootStat, Stats};

/// One engine-wide connection number. Never reused by one engine.
pub type SessionId = u64;

/// What a connection is for, fixed when it opens (2.2). Each role has its own
/// flow control, so an application with several roles opens several connections.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Role {
    /// Registrations and deletions, numbered by a producer sequence.
    Produce,
    /// Joins, windows, completions (numbered), extensions, releases, explicit
    /// claims, and dispatch events emitted while running.
    Work,
    /// Subscriptions, statistics and operator commands.
    Observe,
}

/// The `Hello` of a connection, after the connection owner has checked the
/// version and resolved the token to a service credential.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hello {
    pub role: Role,
    /// The service credential the token resolved to.
    pub credential: Name,
    /// The retransmission buffer this connection numbers for. Required for
    /// `Produce` and `Work`; ignored for `Observe`.
    pub producer: Option<ProducerId>,
    /// New at every process start (2.3): the same producer id with the same
    /// nonce is a reconnection, with another nonce a shared id.
    pub nonce: [u8; 16],
    /// How long the server keeps this producer's entry after its last
    /// connection closes. `None`: the server's default (B4).
    pub linger_ms: Option<u64>,
    /// `false`: no `Committed` on this connection (a volatile broadcast-only producer).
    pub acks: bool,
    /// The channels this producer will write to, checked before `Welcome` (2.7).
    pub channels: Vec<Name>,
}

impl Hello {
    fn fresh(role: Role, credential: &str, producer: bool) -> Self {
        Hello { role, credential: Name::new(credential), producer: producer.then(|| EventId::new_v7().0), nonce: EventId::new_v7().0, linger_ms: None, acks: true, channels: Vec::new() }
    }

    /// A production connection with a fresh producer id and nonce.
    pub fn producer(credential: &str) -> Self {
        Self::fresh(Role::Produce, credential, true)
    }

    /// A work connection with a fresh producer id (for its completions) and nonce.
    pub fn worker(credential: &str) -> Self {
        Self::fresh(Role::Work, credential, true)
    }

    /// An observation connection.
    pub fn observer(credential: &str) -> Self {
        Self::fresh(Role::Observe, credential, false)
    }
}

/// The answer to a `Hello` that was accepted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Welcome {
    pub server_id: [u8; 16],
    /// Rises at every server start and at no other time (2.1).
    pub generation: u32,
    /// The credential the connection runs under.
    pub credential: Name,
    /// The producer's last applied sequence number, sent only once everything
    /// up to it is committed (2.3). `0` for a new producer and for `Observe`.
    pub resume: u64,
    /// This producer id was in use by another instance, which was sent
    /// `Bye{Superseded}` (2.3).
    pub superseded: bool,
}

/// What a subscription names: one channel, or every channel under a prefix
/// at a `.` boundary (`a.b` matches `a.b.c` and `a.b.c.d`, not `a.bc` and not `a.b` itself).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Target {
    Channel(Name),
    Prefix(Name),
}

impl Target {
    pub fn matches(&self, channel: &str) -> bool {
        match self {
            Target::Channel(name) => name.as_str() == channel,
            Target::Prefix(prefix) => channel.len() > prefix.len() && channel.starts_with(prefix.as_str()) && channel.as_bytes()[prefix.len()] == b'.',
        }
    }

    pub fn name(&self) -> &Name {
        match self {
            Target::Channel(name) | Target::Prefix(name) => name,
        }
    }
}

/// One event handed to a worker: an assignment from its window, or the answer
/// to an explicit claim.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Assignment {
    /// Head, work key, failure count, optional fields and payload.
    pub event: Envelope,
    /// The attempt's name and fencing token.
    pub token: ClaimToken,
    /// The lease length the attempt runs under.
    pub lease_ms: u64,
    /// Absolute uptime at which the lease runs out. The connection owner puts
    /// "remaining lease" = `until` − uptime at the moment it starts writing,
    /// and drops an assignment whose `until` has passed before it starts.
    pub until: u64,
    /// This assignment filled the window and more work is waiting (2.4).
    pub more: bool,
}

/// A call from a connection. Which role may send which is in each variant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Call {
    /// Production: `seq` is required and the event may be either kind. Work:
    /// `seq` is `None` and only dispatch events may be emitted (2.2).
    Register {
        seq: Option<u64>,
        event: Envelope,
    },
    /// Production. The outcome is a dispatch event on `result` with correlation `tag`.
    Delete {
        seq: u64,
        id: EventId,
        result: Name,
        tag: EventId,
        key: Option<WorkKey>,
    },
    /// Work. `types` empty means every type on the channel. With `dispatch`,
    /// the channel's dispatch events come to this connection too (2.8).
    Join {
        req: u32,
        channel: Name,
        types: Vec<Name>,
        lease_ms: u64,
        dispatch: bool,
    },
    /// Work. How many assignments, and how many payload bytes (`0`: no byte
    /// bound), this join may hold at once. Absolute (2.4).
    Window {
        channel: Name,
        count: u32,
        bytes: u64,
    },
    /// Work. `seq` numbers it under the connection's producer id.
    Complete {
        seq: u64,
        id: EventId,
        token: ClaimToken,
        follow_ups: Vec<Envelope>,
    },
    /// Work. Re-arms the lease and moves the attempt to this connection's join.
    Extend {
        id: EventId,
        token: ClaimToken,
    },
    /// Work. Gives the event back; visible again after `delay_ms`; `failed`
    /// counts a failure against it (2.4).
    Release {
        id: EventId,
        token: ClaimToken,
        delay_ms: u64,
        failed: bool,
    },
    /// Work. Claims the event with this id now, whichever lane it waits in.
    Claim {
        req: u32,
        id: EventId,
        lease_ms: u64,
    },
    /// Observation. With the waiting backlog after `after`.
    Subscribe {
        req: u32,
        target: Target,
        after: Option<WorkKey>,
    },
    Unsubscribe {
        req: u32,
        target: Target,
    },
    /// Observation, operator grant.
    Stats {
        req: u32,
    },
    /// Observation, operator grant: start draining (2.8).
    Drain {
        req: u32,
    },
}

/// What the engine hands a connection, in order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outbound {
    /// The first message of an accepted connection.
    Welcome(Welcome),
    /// The last message; the connection is closed after it.
    Bye(ByeReason),
    /// The first configuration, format or conflict error of this kind on this connection (2.7).
    Fault {
        kind: FaultKind,
        detail: &'static str,
    },
    /// The producer's operations up to `seq` are committed (2.3).
    Committed {
        seq: u64,
    },
    /// The root channel is paused: hold registrations to it (2.6).
    Paused {
        channel: Name,
    },
    Resumed {
        channel: Name,
    },
    Joined {
        req: u32,
    },
    /// A `Join` or an explicit `Claim` had no effect.
    Refused {
        req: u32,
        reason: Refusal,
    },
    /// A full window would receive work that waits (2.4).
    Pending {
        channel: Name,
    },
    Assign(Assignment),
    Claimed {
        req: u32,
        assignment: Assignment,
    },
    /// Notices for waiting events in work-key order; `more`: subscribe again after the last key.
    Subscribed {
        req: u32,
        target: Target,
        backlog: Vec<Envelope>,
        more: bool,
    },
    Unsubscribed {
        req: u32,
    },
    /// Claimable work: head and work key.
    Notice(Envelope),
    /// A dispatch event.
    Delivery(Envelope),
    StatsReply {
        req: u32,
        stats: Box<Stats>,
    },
    Draining {
        req: u32,
    },
}

/// The type of the dispatch event announcing a successful deletion.
pub const DELETED: &str = "deleted";
/// The type of the dispatch event announcing a failed deletion.
pub const DELETE_FAILED: &str = "delete-failed";

/// Why a deletion failed, the last payload byte of a [`DELETE_FAILED`] event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum DeleteFailure {
    /// No event with that id is held (never registered, completed, sent or expired).
    Missing = 1,
    /// It is claimed.
    Claimed = 2,
    /// The id is there but belongs to another registration than the work key named.
    OtherRegistration = 3,
}
