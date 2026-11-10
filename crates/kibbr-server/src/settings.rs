// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! What a server runs on: `Settings`, every value in it plain data with a default.
//!
//! Nothing here reads a flag, an environment variable or a file - where a value came from
//! is the binary's business, and a test's is a struct literal.

use std::time::Duration;

pub use crate::engine::query::limits::Limits;
// Where rows are kept is the database's to say, and is declared there; a server is
// started with it all the same.
pub use crate::engine::database::{
    Schema, Storage, DEFAULT_COMPACT_INTERVAL_MS, DEFAULT_FLUSH_INTERVAL_MS,
    DEFAULT_ROW_GROUP_TIMEOUT_MS,
};

use crate::DEFAULT_PORT;

/// The largest `/ingest` body accepted off the wire, 10 MiB: above a shipper's routine
/// batch, and bounded because the body is buffered whole.
pub const DEFAULT_MAX_BODY_BYTES: usize = 10 * 1024 * 1024;

/// The largest an `/ingest` body may become once decompressed, capped at
/// `lines::MAX_LINE_BYTES` so `LineTooLong` stays unreachable over HTTP.
pub const DEFAULT_MAX_DECOMPRESSED_BYTES: usize = crate::engine::MAX_LINE_BYTES;

/// How many `/ingest` bodies may be in the parse-and-walk at once: one per core, as the
/// work is CPU-bound, and 1 where the core count is unknowable.
pub fn default_ingest_concurrency() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
}

/// How long an `/ingest` body is kept waiting, in milliseconds, before it is shed: short,
/// so a burst queues and an overload is refused well before the shipper's own timeout.
pub const DEFAULT_INGEST_WAIT_MS: usize = 100;

/// How long after SIGTERM or SIGINT the requests in flight are given to finish: 25
/// seconds, inside the thirty Kubernetes allows before its SIGKILL.
pub const DEFAULT_SHUTDOWN_DEADLINE: Duration = Duration::from_secs(25);

/// How long a request may take from its first line to its answer: 30 seconds, except an
/// `/ingest` body read whole, whose `504` would make a shipper resend lines the log took.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Where a server listens when nothing says otherwise: loopback, since every interface
/// would publish an unauthenticated write endpoint.
pub const DEFAULT_HOST: &str = "127.0.0.1";

/// A duration as `parse_duration` reads one back: whole seconds as `25s`, and anything
/// finer in milliseconds.
pub fn duration_words(d: Duration) -> String {
    match d.as_millis() {
        ms if ms % 1000 == 0 => format!("{}s", ms / 1000),
        ms => format!("{ms}ms"),
    }
}

/// What a *request* may not exceed; server-only, since a file has no wire and no
/// second caller competing for the machine.
#[derive(Debug, Clone, Copy)]
pub struct ApiLimits {
    /// The largest `/ingest` body accepted, measured on the wire before any decompression.
    pub max_body_bytes: usize,
    /// The largest that body may become once decompressed.
    pub max_decompressed_bytes: usize,
    /// How many `/ingest` bodies may be in the parse-and-walk at once; never 0, which
    /// `load` refuses rather than start a process that 503s every request.
    pub ingest_concurrency: usize,
    /// How long an `/ingest` body may be kept waiting before it is shed, zero waiting for
    /// nothing (`DEFAULT_INGEST_WAIT_MS`).
    pub ingest_wait: Duration,
    /// How long after the signal the requests in flight are given before they are cut off,
    /// `None` waiting for every one (`DEFAULT_SHUTDOWN_DEADLINE`).
    pub shutdown_deadline: Option<Duration>,
    /// How long a request may take from its first line to its answer, `None` being as long
    /// as it takes (`DEFAULT_REQUEST_TIMEOUT`).
    pub request_timeout: Option<Duration>,
}

impl Default for ApiLimits {
    fn default() -> Self {
        Self {
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            max_decompressed_bytes: DEFAULT_MAX_DECOMPRESSED_BYTES,
            ingest_concurrency: default_ingest_concurrency(),
            ingest_wait: Duration::from_millis(DEFAULT_INGEST_WAIT_MS as u64),
            shutdown_deadline: Some(DEFAULT_SHUTDOWN_DEADLINE),
            request_timeout: Some(DEFAULT_REQUEST_TIMEOUT),
        }
    }
}

/// How a server says it has started.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Banner {
    /// A paragraph for a person at a terminal, sent as one event under `BANNER`.
    #[default]
    Paragraph,
    /// One event with the address and settings as fields, for whatever reads an object a
    /// line; storage in memory is a warning of its own.
    Event,
}

/// The target of the event that is the banner's paragraph, which a subscriber writes
/// bare whatever its level, so no level can silence it.
pub const BANNER: &str = "crate::server::banner";

/// Everything a server runs on; the default touches nothing: loopback, storage in
/// memory, the default limits.
#[derive(Debug, Clone)]
pub struct Settings {
    /// Where it listens: loopback unless told otherwise, see `DEFAULT_HOST`.
    pub host: String,
    pub port: u16,
    /// What a query may not exceed.
    pub limits: Limits,
    /// What a request may not exceed.
    pub api: ApiLimits,
    /// Where rows go, and how the files and the log are written.
    pub storage: Storage,
    /// Which key names may carry a template's time.
    pub schema: Schema,
    /// How the start is said.
    pub banner: Banner,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            host: DEFAULT_HOST.to_string(),
            port: DEFAULT_PORT,
            limits: Limits::default(),
            api: ApiLimits::default(),
            storage: Storage::default(),
            schema: Schema::default(),
            banner: Banner::default(),
        }
    }
}
