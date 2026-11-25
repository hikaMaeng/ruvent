//! Connecting to the event server.
//!
//! * [`remote`] — the socket link: wire v2 over TCP, the same
//!   [`ruvent::Client`] as in-process.
//!
//! The worker framework (`src/worker/`) is being moved to the new contract
//! (plan 2.9: windows, leases, releases) and is not compiled until then.

pub mod remote;
