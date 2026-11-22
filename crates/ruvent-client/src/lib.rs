//! Connecting to the event server, and running workers on its methods.
//!
//! * [`remote`] — the socket link: the same [`ruvent::Methods`] as in-process.
//! * [`worker`] — the worker framework: services, permits, executors, retry
//!   policy. It sees only [`ruvent::Methods`], so the same services run
//!   in-process or against a remote server.

pub mod remote;
pub mod worker;
