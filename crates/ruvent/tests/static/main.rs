//! Static level: what can be decided from the source text alone, without
//! running the engine.
//!
//! These exist because a rule nobody checks is a comment. Each test here is
//! one structural decision of the library, written so that breaking it fails
//! a build instead of waiting for someone to notice.

mod architecture;
mod docs;
mod source;
