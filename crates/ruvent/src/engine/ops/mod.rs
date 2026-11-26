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
//! | `Subscribe`, `Unsubscribe` | [`Call::Subscribe`], [`Call::Unsubscribe`] |
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

use crate::envelope::{Channel, Envelope, EventId, ProducerId, WorkKey, WorkerId};
use crate::journal::Spooled;
use crate::names::Name;

pub use codes::{ByeReason, FaultKind, Refusal};
pub use stats::{Counters, IdleLane, Pressure, Stats, TypeStat};

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
    /// `false`: no `Committed` on this connection (a broadcast-only producer).
    pub acks: bool,
}

impl Hello {
    fn fresh(role: Role, credential: &str, producer: bool) -> Self {
        Hello { role, credential: Name::new(credential), producer: producer.then(|| EventId::new_v7().0), nonce: EventId::new_v7().0, linger_ms: None, acks: true }
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

/// One event handed to a worker: an assignment from its window, or the answer
/// to an explicit claim.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Assignment {
    /// Head, work key, failure count, `not_before`, the worker uuid it is
    /// claimed under, and payload.
    pub event: Envelope,
    /// The lease length the claim runs under.
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
    /// Production. The outcome is a dispatch event of type 0 on `result`
    /// whose payload names the deleted id ([`delete_outcome`]).
    Delete {
        seq: u64,
        id: EventId,
        result: Channel,
        key: Option<WorkKey>,
    },
    /// Work. What this connection takes: the claimable events of `channel`
    /// (`None`: of every channel) whose type is in `types` (empty: every
    /// type). At least one of the two is given. The join is named by this
    /// `req` in `Window` and `Pending`; a name another join of the connection
    /// holds is refused (`Malformed`) unless this join repeats that one's
    /// selection. A join that repeats one of the connection's selections
    /// changes its terms and takes this name; one that overlaps another is
    /// refused (`Overlap`), so every lane has at most one join per
    /// connection. With `dispatch` (a channel join only), the channel's
    /// dispatch events come to this connection too (2.8). `worker` is the
    /// uuid every assignment of this join is claimed under: required, never
    /// nil (a claim without a worker uuid is refused, `Malformed`).
    Join {
        req: u32,
        worker: WorkerId,
        channel: Option<Channel>,
        types: Vec<u16>,
        lease_ms: u64,
        dispatch: bool,
    },
    /// Work. How many assignments, and how many payload bytes (`0`: no byte
    /// bound), the join named `join` may hold at once. Absolute (2.4).
    Window {
        join: u32,
        count: u32,
        bytes: u64,
    },
    /// Work. `seq` numbers it under the connection's producer id.
    /// `spooled`: follow-ups (by position) whose payload the connection owner
    /// received to disk instead of memory (2.6); their envelope's payload is
    /// empty. See `Engine::body_to_disk`.
    /// The event and the worker that claimed it must both match its claim.
    Complete {
        seq: u64,
        id: EventId,
        worker: WorkerId,
        follow_ups: Vec<Envelope>,
        spooled: Vec<(u32, Spooled)>,
    },
    /// Work. Re-arms the lease and moves the claim to this connection's
    /// join. The event and its worker must match.
    Extend {
        id: EventId,
        worker: WorkerId,
    },
    /// Work. Gives the event back: the worker uuid leaves the event, which is
    /// visible again after `delay_ms`, and with `failed` its failure count
    /// rises — a failure report. The event and its worker must match (2.4).
    Release {
        id: EventId,
        worker: WorkerId,
        delay_ms: u64,
        failed: bool,
    },
    /// Work. Claims the event with this id now, under `worker` (required,
    /// never nil), whichever lane it waits in.
    Claim {
        req: u32,
        id: EventId,
        worker: WorkerId,
        lease_ms: u64,
    },
    /// Observation. From now on, the channel's notices and dispatch events;
    /// with the waiting backlog after `after`.
    Subscribe {
        req: u32,
        channel: Channel,
        after: Option<WorkKey>,
    },
    Unsubscribe {
        req: u32,
        channel: Channel,
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
    Joined {
        req: u32,
    },
    /// A `Join` or an explicit `Claim` had no effect.
    Refused {
        req: u32,
        reason: Refusal,
    },
    /// The full window of the join named `join` would receive work that waits (2.4).
    Pending {
        join: u32,
    },
    Assign(Assignment),
    Claimed {
        req: u32,
        assignment: Assignment,
    },
    /// Notices for waiting events in work-key order; `more`: subscribe again after the last key.
    Subscribed {
        req: u32,
        channel: Channel,
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

/// A delete outcome, read from its dispatch event's payload (its type is 0;
/// the server routes it by the `result` channel alone): the id the `Delete`
/// named (16 bytes), then one byte, `0` for deleted and otherwise the
/// [`DeleteFailure`]. `None` for a payload of another shape.
pub fn delete_outcome(payload: &[u8]) -> Option<(EventId, Result<(), DeleteFailure>)> {
    let (id, rest) = payload.split_first_chunk::<16>()?;
    let outcome = match rest {
        [0] => Ok(()),
        [1] => Err(DeleteFailure::Missing),
        [2] => Err(DeleteFailure::Claimed),
        [3] => Err(DeleteFailure::OtherRegistration),
        _ => return None,
    };
    Some((EventId(*id), outcome))
}

/// Why a deletion failed, the last payload byte of a delete outcome ([`delete_outcome`]).
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
