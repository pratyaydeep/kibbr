//! What a log line is before anything stores it: its keys flattened, each value typed by
//! the ladder, and the fingerprint its template is found by - with the errors a line is
//! refused with and the few things every crate above prints and draws with.
//!
//! The bottom of the workspace: this crate names no other kibbr crate.

pub mod error;
pub mod flatten;
pub mod fmt;
pub mod infer;
pub mod key;
pub mod lines;
pub mod rng;
pub mod types;
