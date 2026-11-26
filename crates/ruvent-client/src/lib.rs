//! The client SDK of the event server (plan 2.9).
//!
//! The server moves its load to the clients: this crate keeps the rules a
//! client must follow so applications do not have to.
//!
//! * [`servers`] — the server list a handle follows (2.8).
//! * [`link`] — how one connection is opened: TCP ([`link::Tcp`]) or an
//!   engine in this process ([`link::Local`]).
//! * [`mod@line`] — one server, one role: reconnection with back-off, the `Bye`
//!   table, restart detection.
//! * [`buffer`] — the unacknowledged buffer of a numbered connection.
//! * [`producer`] — registrations: selection, acknowledgements, pauses.
//! * [`worker`] — windows over many servers, leases, the three ends of a handler.
//! * [`observer`] — subscriptions and dispatch events.
//! * [`remote`] — the socket transport (wire v3).
//!
//! There is no request–response helper. A request and its answer are a
//! project's convention: the request's payload names the channel its answer
//! goes to (one per request), the asker subscribes that channel with an
//! [`observer`], and the worker sends there with [`worker::Context::emit_now`]
//! (while running) or [`worker::Context::emit_dispatch`] (with the completion).

//!
//! ## State (plan 2.10, SDK)
//!
//! Every row is declared in full where the state lives.
//!
//! | state | key | lives in |
//! | --- | --- | --- |
//! | producer id, instance nonce, next number | (server, role) | [`producer`], [`worker`] (each line's `Hello` and [`buffer::Unacked`]) |
//! | unacknowledged buffer | (server, seq) | [`buffer`] |
//! | server list and connections | server address | [`servers`], each handle's lines |
//! | selection state | server → connected, unacked bytes, draining | [`producer`] |
//! | pause marks | (server, channel) | [`producer`] |
//! | reconnection back-off | server | [`line::backoff::Backoff`] |
//! | execution slots | slot | [`worker::windows`] |
//! | per-server window book | (server, channel) → window, in flight, starving | [`worker::windows::Book`] |
//! | runs in flight | token → run, extension due, run limit | [`worker`] |
//! | retry budget | producer | [`producer::RetryBudget`] |

pub mod buffer;
pub mod line;
pub mod link;
pub mod observer;
pub mod producer;
pub mod remote;
pub mod servers;
pub mod worker;
mod workline;
