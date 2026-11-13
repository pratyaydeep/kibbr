// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! What a server can fail with: `Error`, which stops one from starting or from finishing
//! cleanly. What stops the database under it is the engine's, carried as it is.

use thiserror::Error;

pub use crate::engine::{chain, IngestError};

/// What stops a server from starting or from finishing cleanly, and, last, the engine's
/// errors a request is refused with.
#[derive(Debug, Error)]
pub enum Error {
    /// Almost always "address already in use"; names the address, because the remedy is
    /// `--port`.
    #[error("cannot listen on {addr}: {source}")]
    Bind {
        addr: String,
        #[source]
        source: std::io::Error,
    },

    #[error("starting the async runtime: {0}")]
    Runtime(#[source] std::io::Error),

    #[error("serving: {0}")]
    Serve(#[source] std::io::Error),

    /// Registering the SIGTERM handler failed, checked at startup: a server that cannot hear
    /// SIGTERM is killed by the SIGKILL that follows it.
    #[error("installing the {signal} handler: {source}")]
    Signal {
        signal: &'static str,
        #[source]
        source: std::io::Error,
    },

    /// What the database could not open or close with, or refused a TrieQL string with:
    /// transparent, so it prints as the engine's own sentence and no wrapper above it.
    #[error(transparent)]
    Database(#[from] crate::engine::Error),
}

/// What this crate's functions return when they fail.
pub type Result<T, E = Error> = std::result::Result<T, E>;
