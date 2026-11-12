// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! kibbr, a database for structured JSON logs: each line's shape stored once in a trie,
//! its values as typed columns, a fraction of the raw size and still queryable.
//!
//! This crate is the `kibbr` binary, and the library is how the binary is built: nothing
//! here is an API. Use the binary and its HTTP API, which are versioned in the
//! [changelog](https://github.com/pratyaydeep/kibbr/blob/main/CHANGELOG.md). The code is
//! toured in [ARCHITECTURE.md](https://github.com/pratyaydeep/kibbr/blob/main/ARCHITECTURE.md):
//! the engine (`engine`), the HTTP server over it (`server`), and the command line over
//! both (`cli`), each naming only the ones under it.
//!
//! Licensed AGPL-3.0-only; contributions under MIT.

mod cli;
mod engine;
mod server;

#[cfg(feature = "bench")]
mod bench;

// The doors, each for one caller and none of them a promise: the two binaries, the fuzz
// targets under fuzz/, and the tests under tests/.

#[doc(hidden)]
pub use cli::run;

#[cfg(feature = "bench")]
#[doc(hidden)]
pub use bench::main as bench_main;

#[cfg(any(test, feature = "fuzzing"))]
#[doc(hidden)]
pub mod fuzzing;

/// What the tests under tests/ reach past the binary for.
#[doc(hidden)]
pub mod testing {
    /// A data file read the way the engine reads it, to hold a compacted one to it.
    pub use crate::engine::inspect::{open_sealed, FormatError, Reader};

    /// The settings a server is run on, as the command line builds them from its flags.
    pub mod config {
        pub use crate::cli::config::{settings, File, Log, ServeFlags};
    }

    /// The server, run on a thread of the test's own process.
    pub mod server {
        pub use crate::server::{run, Report, Result};
    }
}
