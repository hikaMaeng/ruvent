//! Things the type system must refuse, each next to a twin that compiles.
//!
//! A `compile_fail` doctest passes for *any* error — a typo would do. So every
//! case is paired with a twin that is the same code without the forbidden
//! line, and that twin must compile. Together they show the refusal comes
//! from the rule, not from a mistake in the test.
//!
//! Compiled only by `cargo test --doc`.
//!
//! # A handler cannot forge its attempt
//!
//! Fencing rests on the attempt number the engine issued. A context with any
//! other number cannot be built.
//!
//! ```compile_fail
//! let engine = ruvent::Engine::volatile(Default::default());
//! let command = std::sync::Arc::new(ruvent::Envelope::draft(ruvent::Kind::Command, "a", "c", "{}"));
//! let _forged = ruvent::Context::new(engine, command, 7, ruvent::CancellationToken::new());
//! ```
//!
//! ```
//! # async fn twin() {
//! let engine = ruvent::Engine::volatile(Default::default());
//! let command = std::sync::Arc::new(ruvent::Envelope::draft(ruvent::Kind::Command, "a", "c", "{}"));
//! let _ = (engine, command, ruvent::CancellationToken::new());
//! # }
//! ```
//!
//! # Positional identities are the engine's
//!
//! ```compile_fail
//! let _ = ruvent::EventId::positional(1);
//! ```
//!
//! ```
//! let _ = ruvent::EventId::derive(&[b"x"]);
//! ```
//!
//! # The state machine is not reachable
//!
//! Everything that changes engine state goes through the engine's one task.
//!
//! ```compile_fail
//! use ruvent::engine::state::State;
//! ```
//!
//! ```
//! use ruvent::engine::Stats;
//! ```
//!
//! # A handler cannot hold what is not `Send`
//!
//! Handlers run on a multi-threaded pool. State that is not thread-safe
//! cannot be captured.
//!
//! ```compile_fail
//! let shared = std::rc::Rc::new(());
//! let handler = move |_cx: ruvent::Context, _command: std::sync::Arc<ruvent::Envelope>| {
//!     let shared = shared.clone();
//!     async move { let _ = &shared; Ok::<_, ruvent::Failure>(ruvent::Bytes::new()) }
//! };
//! let _ = ruvent::Service::new("s").accept("a", handler);
//! ```
//!
//! ```
//! let shared = std::sync::Arc::new(());
//! let handler = move |_cx: ruvent::Context, _command: std::sync::Arc<ruvent::Envelope>| {
//!     let shared = shared.clone();
//!     async move { let _ = &shared; Ok::<_, ruvent::Failure>(ruvent::Bytes::new()) }
//! };
//! let _ = ruvent::Service::new("s").accept("a", handler);
//! ```
//!
//! # The principal is the gateway's to set
//!
//! A client proposal has no field for who sent it.
//!
//! ```compile_fail
//! let _ = ruvent::Proposal { principal: Some("root".into()), ..Default::default() };
//! ```
//!
//! ```
//! let _ = ruvent::Proposal { session_id: Some("s".into()), ..Default::default() };
//! ```
