// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The HTTP server of [kibbr](https://github.com/pratyaydeep/kibbr), a database for
//! structured JSON logs, over one `Database`: it never reaches past the engine's public API.
//! Internal to kibbr; no API stability promised.
//!
//! The server, as a library: `run` is given a `Settings` and a token to stop it by, and
//! serves the native HTTP surface until the token is cancelled or a signal arrives.
//!
//! Nothing here reads a flag or writes to stderr, and the flush clock lives here, not in
//! the store: deciding when to write is a fact about the process lifecycle.

use crate::engine;

use std::borrow::Cow;
use std::future::IntoFuture;
use std::io::{Cursor, Read};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use axum::body::{Body, Bytes, HttpBody};
use axum::extract::rejection::BytesRejection;
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use flate2::read::MultiGzDecoder;
use http_body::{Frame, SizeHint};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::{Notify, Semaphore};
use tokio_util::sync::CancellationToken;

use std::path::Path;

use crate::server::error::IngestError;
use engine::display::{commas, rfc3339_at};
use engine::query::answer;
use engine::{compaction, load, CompactRefused, Database, Fan, Stop, StoreError};

/// The default port this server listens on.
pub const DEFAULT_PORT: u16 = 7700;

/// How long, once the shutdown deadline has cut the requests in flight off, their
/// connections are given to take the 503 each was answered with.
const CUT_OFF_GRACE: Duration = Duration::from_secs(1);

/// How many `/ingest` bodies may be between the door and their answer at once, each a
/// blocking thread: unbounded, a stalled log took the whole pool and the server went quiet.
const INGEST_BODIES: usize = 256;

/// What the blocking pool holds beyond the bodies at the door: for queries and the flush
/// tick, which must not queue behind bodies a stalled log cannot answer.
const OTHER_BLOCKING_THREADS: usize = 256;

/// The bodies that may be between the door and their answer on this server: always more
/// places than permits, so that running out of places means they are waiting on the log.
fn ingest_bodies(api: &ApiLimits) -> usize {
    INGEST_BODIES.max(api.ingest_concurrency.saturating_mul(2))
}

/// The `Retry-After` on a shed body, in seconds: not an estimate of when the server
/// will be free, but a floor under the retry.
const RETRY_AFTER_SECONDS: u32 = 1;

/// The web console, compiled in: the binary is the deployment unit, and the page cannot
/// drift from the endpoints it calls. Built from `web/` (docs/viewer.md).
const VIEWER: &str = include_str!("server/viewer.html");

/// Where the source of this build is: AGPL §13 has a server offer it to whoever uses it
/// over a network, so `/health` and the console's Status page both carry it. A modified
/// build that is served to others names its own.
pub const SOURCE: &str = "https://github.com/pratyaydeep/kibbr";
/// This build's version, beside `SOURCE` in both places.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

// ── State ──────────────────────────────────────────────────────────────────────────

/// Everything the handlers share, none of it behind a lock of the server's, so an ingest
/// and a query proceed together and a body is not atomic to a query beside it.
pub struct Server {
    /// The database: the trie, the stores, the log and the compactions it runs.
    db: Arc<Database>,
    /// Wakes the flush before its next tick: rung by an ingest body that left the buffers
    /// past their memory limit.
    flush_now: Notify,
    /// Rung after every flush: a body waiting at the door for the buffers to come back
    /// under twice their limit looks again.
    flushed: Notify,
    /// Cancelled when shutdown begins: whoever is waiting at the door is refused at once.
    stopping: CancellationToken,
    /// Cancelled when the shutdown deadline passes with requests still in flight: each
    /// is answered 503 where it stands and let go (`track`).
    cut_off: CancellationToken,
    started: Instant,
    started_at: DateTime<Utc>,
    lines_read: AtomicU64,
    lines_ingested: AtomicU64,
    lines_rejected: AtomicU64,
    queries: AtomicU64,
    /// Queries whose scan was told to stop and did: the request timed out, its client
    /// left, or a shutdown cut it off. None of them is in `queries`.
    queries_stopped: AtomicU64,
    /// Requests answered `408` or `504` because `api.request_timeout` passed first.
    timed_out: AtomicU64,
    /// Requests currently in a handler. Read at shutdown, to say what is being waited on.
    in_flight: AtomicU64,
    /// What the door has made wait, and the bodies shed, each under its cause; a body shed
    /// at the door is in no other counter, since `lines_read` never saw it.
    door: Door,
    /// The caps, held on the server and not negotiated per request: a client that could
    /// override one could sidestep the protection it is there for.
    limits: Limits,
    api: ApiLimits,
    /// How many bodies may be in the parse-and-walk at once: the backpressure for the part
    /// that is CPU. Given back once the body is in the log's queue, not kept across the flush.
    ingest_permits: Arc<Semaphore>,
    /// How many bodies may be between the door and their answer: `ingest_bodies`. Taken
    /// at the door with the permit above, and kept until the body's task has returned.
    ingest_bodies: Arc<Semaphore>,
    /// The bulk loads this server has been asked to run: at most one at a time, on a
    /// thread of its own, never on the runtime or the blocking pool the door's bodies use.
    loads: std::sync::Mutex<Loads>,
    /// What `/load` and `/load/<job>` are asked with, as `LOCK` holds it. `None` without a
    /// data directory, where there is nothing to load into.
    load_token: Option<String>,
}

/// The door's own counts, each an atomic, so `/stats` adds them up with no lock.
#[derive(Default)]
struct Door {
    /// Bodies waiting at the door now, their bodies unread.
    waiting: AtomicU64,
    /// Bodies that have had to wait at the door, whatever came of the wait; with no wait
    /// to give, one that found something missing is counted here all the same.
    waits: AtomicU64,
    /// How long those waited, all together, in microseconds.
    waited_micros: AtomicU64,
    shed: Shed,
}

/// Bodies answered `503` and told to come back, by what there was none of: which of
/// them is rising says what to do about it.
#[derive(Default)]
struct Shed {
    /// No permit for the parse came free inside the wait.
    permit: AtomicU64,
    /// As many bodies as may be were between the door and their answer.
    place: AtomicU64,
    /// As many bodies as may be were already waiting at the door.
    line: AtomicU64,
    /// The buffers were past twice their limit.
    buffers: AtomicU64,
    /// The write-ahead log had no room: for a body already parsed, or at the door while
    /// the bodies ahead of it waited for that room.
    log: AtomicU64,
    /// The server was stopping.
    stopping: AtomicU64,
}

impl Shed {
    /// Every body shed, whatever for: `/stats`'s `ingest.requests_shed`.
    fn total(&self) -> u64 {
        self.each().iter().map(|(_, count)| count).sum()
    }

    /// Each cause with its count, in the order `/stats` lists them.
    fn each(&self) -> [(&'static str, u64); 6] {
        [
            ("permit", &self.permit),
            ("place", &self.place),
            ("line", &self.line),
            ("buffers", &self.buffers),
            ("log", &self.log),
            ("stopping", &self.stopping),
        ]
        .map(|(cause, count)| (cause, count.load(Ordering::Relaxed)))
    }
}

/// The header a `/load` or `/compact` request carries its token in.
const LOAD_TOKEN_HEADER: &str = "x-kibbr-load-token";

/// Whether a request shows the server's load token. First, before the body is read or a
/// path is looked at, so a caller without it learns nothing - not even which paths exist.
fn load_refused(server: &Server, headers: &HeaderMap) -> Option<Response> {
    token_refused(
        server,
        headers,
        "a load is taken from `kibbr load` on this machine, which shows the token this \
         server wrote to its data directory's LOCK",
    )
}

/// As `load_refused`, for `/compact`: a day rewritten on every core it is given is
/// taken only from an account that can read the directory.
fn compact_refused(server: &Server, headers: &HeaderMap) -> Option<Response> {
    token_refused(
        server,
        headers,
        "a compaction is taken from `kibbr compact` on this machine, which shows the token \
         this server wrote to its data directory's LOCK",
    )
}

fn token_refused(server: &Server, headers: &HeaderMap, why: &str) -> Option<Response> {
    let shown = headers
        .get(LOAD_TOKEN_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let ours = server.load_token.as_deref()?;
    // Compared whole, not up to the first difference, so the time an answer takes says
    // nothing about how much of a guess was right.
    let same = shown.len() == ours.len()
        && shown
            .bytes()
            .zip(ours.bytes())
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0;
    (!same).then(|| fail(StatusCode::FORBIDDEN, why))
}

/// Whether a request meant another data directory than the one this server serves, as
/// `LOCK` can briefly name the last holder's address. `done` is what did not happen.
fn wrong_directory(server: &Server, asked: Option<&Path>, done: &str) -> Option<Response> {
    let (asked, ours) = (asked?, server.db.storage().base_path.as_ref()?);
    let ours = std::fs::canonicalize(ours).unwrap_or_else(|_| ours.clone());
    (asked != ours).then(|| {
        fail(
            StatusCode::CONFLICT,
            &format!(
                "this server serves {}, not {}; nothing was {done}",
                ours.display(),
                asked.display()
            ),
        )
    })
}

/// Load jobs, by number, and the one running.
#[derive(Default)]
struct Loads {
    next: u64,
    jobs: std::collections::BTreeMap<u64, Arc<LoadJob>>,
    running: Option<std::thread::JoinHandle<()>>,
}

/// The largest `POST /load` body: about 200,000 input paths.
const MAX_LOAD_REQUEST_BYTES: usize = 64 * 1024 * 1024;

/// Finished jobs kept for `GET /load/<job>`. A client polls until its job is done, so only
/// the recent ones are ever asked about.
const LOAD_JOBS_KEPT: usize = 64;

struct LoadJob {
    control: load::Control,
    state: std::sync::Mutex<LoadState>,
}

enum LoadState {
    Running,
    Done(Box<load::Report>),
    Failed(String),
}

impl Server {
    /// The server's own state around a database already open.
    fn with(
        db: Database,
        now: DateTime<Utc>,
        limits: Limits,
        api: ApiLimits,
        load_token: Option<String>,
    ) -> Self {
        Self {
            db: Arc::new(db),
            flush_now: Notify::new(),
            flushed: Notify::new(),
            stopping: CancellationToken::new(),
            cut_off: CancellationToken::new(),
            started: Instant::now(),
            started_at: now,
            lines_read: AtomicU64::new(0),
            lines_ingested: AtomicU64::new(0),
            lines_rejected: AtomicU64::new(0),
            queries: AtomicU64::new(0),
            queries_stopped: AtomicU64::new(0),
            timed_out: AtomicU64::new(0),
            in_flight: AtomicU64::new(0),
            door: Door::default(),
            limits,
            api,
            ingest_permits: Arc::new(Semaphore::new(api.ingest_concurrency)),
            ingest_bodies: Arc::new(Semaphore::new(ingest_bodies(&api))),
            loads: std::sync::Mutex::new(Loads::default()),
            load_token,
        }
    }

    /// Build the state, restoring what is on disk, with no lock taken: as a start does,
    /// for a test.
    #[cfg(test)]
    fn new(
        now: DateTime<Utc>,
        limits: Limits,
        api: ApiLimits,
        storage: Storage,
        schema: Schema,
        load_token: Option<String>,
    ) -> Result<Self, Error> {
        let db = engine::database::Claim::unlocked(engine::Options { storage, schema }).open()?;
        Ok(Self::with(db, now, limits, api, load_token))
    }
}

/// Increments the in-flight count for as long as it is alive; a `Drop` guard, because
/// a client that hangs up leaves the handler future dropped, not polled to completion.
struct InFlight(Arc<Server>);

impl InFlight {
    fn enter(server: &Arc<Server>) -> Self {
        server.in_flight.fetch_add(1, Ordering::Relaxed);
        Self(Arc::clone(server))
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

mod body;
mod compactor;
mod error;
mod handlers;
mod routes;
mod runtime;
pub mod settings;
#[cfg(test)]
#[path = "server_tests.rs"]
mod tests;
#[cfg(test)]
use engine::database::MAX_REPORTED_ERRORS;

pub use error::{Error, Result};
pub use runtime::{run, Report};
pub use settings::{ApiLimits, Banner, Limits, Settings, BANNER};
#[cfg(test)]
use settings::{Schema, Storage};

use body::*;
use compactor::*;
use handlers::*;
use routes::*;
use runtime::*;
