//! `triedb serve` - the native HTTP surface, on :7700.
//!
//! One `Trie` and one `StoreManager`, held for the life of the process, wrapped in the
//! five endpoints below. Nothing here knows how a line becomes a row: `/query` builds the
//! same `Query` the CLI builds and calls the same `engine::execute`, and `/ingest` goes
//! through `db::ingest_line`, which is what `db::load` calls.
//!
//! **What survives, and what does not.** With a data directory, rows go to day partitions
//! on a timer and the trie is snapshotted on a graceful shutdown, so `SIGTERM` and restart
//! give the same answers. There is no write-ahead log, so a `kill -9` loses whatever had
//! not reached a flush - up to one interval. With `--storage memory` nothing is written at
//! all. The banner says which of the two this process is, `/stats` repeats it as a field,
//! and the shutdown log names every step including the ones still skipped.
//!
//! **The flush clock lives here and not in the store.** A `LeafStore` has a buffer and a
//! synchronous `flush()` and no timer, so it can be driven from a test with no runtime;
//! the interval that calls it watches the same cancellation token as the listener, because
//! deciding *when* to write is a fact about the process lifecycle.
//!
//! A future `Ingestor` would sit between the API and the trie. There is no such type here:
//! `ingest` is a plain function with one implementation, and a trait with one caller is a
//! guess at what the second one will need. The WAL and the detector cache are the arrows
//! that will make it a type, and neither exists yet.

use std::borrow::Cow;
use std::io::{Cursor, Read};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::rejection::BytesRejection;
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use flate2::read::MultiGzDecoder;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::{Notify, Semaphore};
use tokio_util::sync::CancellationToken;

use std::path::Path;

use crate::catalog::Catalog;
use crate::config::{ApiLimits, Limits, Schema, Storage};
use crate::db;
use crate::engine;
use crate::error::{self, CliError, DetectorError, DetectorErrorKind, ServeError};
use crate::fmt::{commas, rfc3339_at};
use crate::load;
use crate::query;
use crate::store::{FlushKind, StoreError, StoreManager};
use crate::trie::Trie;

/// The default port this server listens on.
pub const DEFAULT_PORT: u16 = 7700;

/// How long an `/ingest` body waits for a permit before it is shed.
///
/// What separates a burst from an overload. Two shippers arriving together on a
/// four-permit server should queue for the millisecond it takes; twenty arriving together
/// every second is not a burst, and waiting will not fix it.
///
/// A constant, not a flag: what an operator tunes is the permit count, which is the
/// statement about the machine. A longer wait only moves the refusal closer to the
/// shipper's own timeout, which is the outcome the shed exists to avoid.
const INGEST_QUEUE_WAIT: Duration = Duration::from_millis(100);

/// The `Retry-After` on a shed body, in seconds.
///
/// Not an estimate of when the server will be free - nothing here knows that - but a floor
/// under the retry, so a shipper that would come straight back waits out the request
/// already running.
const RETRY_AFTER_SECONDS: u32 = 1;

/// The most rejections one `/ingest` response will describe.
///
/// The body is bounded and the response was not, which is the wrong way round: a 1.2 MB
/// body of nothing but bad lines produced a 15 MB response, one `{"line":N,"msg":"..."}`
/// per line, and the smallest malformed line is a handful of bytes. The server built all
/// of that in memory to describe input it had already refused.
///
/// A thousand is a sample, not a transcript. An operator reading this is identifying a
/// pattern - one shipper truncating, one field arriving as `NaN` - and the thousandth
/// example says nothing the tenth did not. `errors_omitted` carries the rest as a count,
/// so the total is never wrong, only the enumeration is short.
const MAX_REPORTED_ERRORS: usize = 1000;

/// The log viewer, compiled in.
///
/// `include_str!` and not a file read at startup: the binary is the deployment unit, and a
/// server that answers `/` only when it happens to be started from the right directory is
/// a server that works on the developer's machine. It also makes the page unable to drift
/// from the endpoints it calls - they are in this file.
const VIEWER: &str = include_str!("viewer.html");

// ── State ──────────────────────────────────────────────────────────────────────────

/// Everything the handlers share.
///
/// **Nothing here is behind a lock of the server's.** `Trie` synchronises itself per node,
/// so an ingest and a walk proceed together; `StoreManager` locks each leaf on its own, and
/// a flush holds no lock an append or a query waits on while it writes. There used to be
/// one `RwLock` over the whole registry, taken for writing by every ingest body and by the
/// flush tick across every fsync, and for reading by every query for the whole of its run:
/// with queries running, ingest fell from 44,800 lines a second to 1,645, and a count by a
/// promoted value went from 33ms to 1.1s.
///
/// **What that gave up: a body is no longer atomic to a query running beside it.** Each
/// row is visible from the moment its leaf takes it, so a query that runs while a body is
/// applied can count part of it. A client that ingests and then queries still sees all of
/// what it sent - the response goes out after the last line is applied - and no query ever
/// sees a row twice or loses one it could see before.
pub struct Server {
    trie: Trie,
    stores: StoreManager,
    /// Wakes the flush before its next tick: an ingest body that left the buffers past
    /// their memory limit rings it, so the limit is enforced within a body and not within
    /// half a second of bodies.
    flush_now: Notify,
    started: Instant,
    started_at: DateTime<Utc>,
    lines_read: AtomicU64,
    lines_ingested: AtomicU64,
    lines_rejected: AtomicU64,
    queries: AtomicU64,
    /// Requests currently in a handler. Read at shutdown, to say what is being waited on.
    in_flight: AtomicU64,
    /// Bodies refused for want of a permit, or while the buffers were past twice their
    /// limit. In no other counter - `lines_read` never saw them - and the only number that
    /// says the shed is firing rather than merely present.
    requests_shed: AtomicU64,
    /// The caps, held on the server and not negotiated per request: all clients get the
    /// same limits, because a client that could override one could sidestep the
    /// protection it is there for.
    limits: Limits,
    api: ApiLimits,
    storage: Storage,
    /// How many bodies may be in the parse-and-walk at once.
    ///
    /// The whole of the backpressure answer. Without it every body takes a blocking-pool
    /// slot, the pool queues 512 deep, and a shipper waiting on a request that will not
    /// finish in time gives up and opens another - which is how a slow server becomes an
    /// unreachable one. No permit within `INGEST_QUEUE_WAIT` is a 503, which Vector's
    /// adaptive concurrency reads as "send fewer" rather than "the server is gone".
    ingest_permits: Arc<Semaphore>,
    /// Whether the last flush tick could write. Shared with the registry and read without
    /// its lock, because `/health` must not queue behind a flush and `/ingest` must not
    /// buffer a body it is about to refuse.
    write_health: Arc<crate::store::WriteHealth>,
    /// The bulk loads this server has been asked to run: at most one at a time, on a
    /// thread of its own with its own pool, never on the runtime or the blocking pool the
    /// live door's bodies use.
    loads: std::sync::Mutex<Loads>,
    /// What `/load` and `/load/<job>` are asked with, as `LOCK` holds it. `None` without a
    /// data directory, where there is nothing to load into.
    load_token: Option<String>,
}

/// The header a `/load` request carries its token in.
const LOAD_TOKEN_HEADER: &str = "x-triedb-load-token";

/// Whether a request shows the server's load token. First, before the body is read or a
/// path is looked at, so a caller without it learns nothing - not even which paths exist.
fn load_refused(server: &Server, headers: &HeaderMap) -> Option<Response> {
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
    (!same).then(|| {
        fail(
            StatusCode::FORBIDDEN,
            "a load is taken from `triedb load` on this machine, which shows the token this \
             server wrote to its data directory's LOCK",
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
    /// Build the state, restoring what is on disk (`open_durable`).
    fn new(
        now: DateTime<Utc>,
        limits: Limits,
        api: ApiLimits,
        storage: Storage,
        schema: Schema,
        load_token: Option<String>,
    ) -> Result<Self, ServeError> {
        let (trie, stores) = match storage.base_path {
            Some(_) => open_durable(&storage, &schema)?,
            None => (
                Trie::new().with_time_columns(schema.time_columns.clone()),
                StoreManager::memory(),
            ),
        };

        let write_health = stores.health();
        Ok(Self {
            trie,
            stores,
            flush_now: Notify::new(),
            started: Instant::now(),
            started_at: now,
            lines_read: AtomicU64::new(0),
            lines_ingested: AtomicU64::new(0),
            lines_rejected: AtomicU64::new(0),
            queries: AtomicU64::new(0),
            in_flight: AtomicU64::new(0),
            requests_shed: AtomicU64::new(0),
            limits,
            api,
            storage,
            ingest_permits: Arc::new(Semaphore::new(api.ingest_concurrency)),
            write_health,
            loads: std::sync::Mutex::new(Loads::default()),
            load_token,
        })
    }
}

/// The trie and the stores of a data directory, restored: what a server serves, and what a
/// load running alone commits into - the same steps, so a directory a load opened is left
/// exactly as a server's start would have left it.
///
/// The trie comes back first because it is what says which leaves exist; every one of them
/// is then opened, so a template that existed before the restart is a template that exists
/// after it even when its directory holds no rows. A leaf opened and empty is the "known
/// template, nothing in it" answer, which is a different thing from "no such template" and
/// has to stay different across a restart.
///
/// The caller holds the directory's `LOCK`: recovery writes.
pub(crate) fn open_durable(
    storage: &Storage,
    schema: &Schema,
) -> Result<(Trie, StoreManager), ServeError> {
    let Some(base) = storage.base_path.clone() else {
        return Err(ServeError::Storage {
            detail: "no data directory".to_string(),
        });
    };
    let data_dir = storage.data_dir();
    let trie = open_catalog(&base, storage.trie_file.as_deref(), &data_dir, schema)?;

    let stores = StoreManager::disk(base, storage.disk_options()).map_err(storage_fatal)?;

    // Applied whatever the snapshot said, including when it parsed cleanly:
    // the hazard is a trie that does not know about stored rows, and a stale
    // or hand-edited snapshot that parses is as unaware as one that did not.
    // See `Trie::reserve_template_ids_through` for what reissuing an id costs.
    let claimed = stores.claimed_template_ids();
    if let Some(highest) = claimed.last() {
        trie.reserve_template_ids_through(*highest);
    }
    let known: std::collections::BTreeSet<u64> = trie
        .all_leaves()
        .iter()
        .filter_map(|leaf| {
            leaf.leaf_meta
                .read()
                .ok()
                .and_then(|slot| slot.as_ref().map(|meta| meta.template_id))
        })
        .collect();
    let orphans = claimed.iter().filter(|id| !known.contains(id)).count();
    if orphans > 0 {
        log(&format!(
            "WARN storage - {} template id(s) with rows on disk that no template \
             refers to; those rows are not queryable, and new templates take \
             fresh ids rather than reusing them",
            commas(orphans as u64)
        ));
    }
    for leaf in trie.all_leaves() {
        let placed = leaf.leaf_meta.read().ok().and_then(|slot| {
            slot.as_ref()
                .map(|meta| (meta.template_id, meta.time_source.column().to_string()))
        });
        if let Some((id, time_column)) = placed {
            if let Err(e) = stores.open_existing(id, &time_column) {
                log(&format!("WARN leaf {id} - {}", error::chain(&e)));
            }
        }
    }
    Ok((trie, stores))
}

/// A store that will not open, as the error startup exits on. An earlier layout or another
/// format version is not a disk that failed; it is a disk holding something this build
/// must not read, and it is named.
fn storage_fatal(e: StoreError) -> ServeError {
    match e {
        StoreError::LegacyLayout { path } => ServeError::LegacyLayout { path },
        StoreError::Format(crate::store::v3::FormatError::Version { path, found }) => {
            ServeError::SegmentVersion { path, found }
        }
        e => ServeError::Storage {
            detail: error::chain(&e),
        },
    }
}

/// The trie, from `catalog.log` - and the catalog attached to it, so every template from
/// here on is durable before it exists.
///
/// The catalog's order. Read it from the start; a torn tail is truncated with a WARN
/// naming the offset and an unknown kind is skipped with one. **No catalog but a
/// `trie.json`** is the one-time migration: the snapshot is read once, a catalog is
/// written from it and flushed, and the trie is then built from that catalog and not the
/// snapshot - so the first start after the upgrade exercises the same path every later
/// one does. Once a catalog exists `trie.json` is not opened at all.
///
/// A catalog that cannot be *read* (permissions, a directory in its place) is fatal, and
/// deliberately so: unlike a torn record there is nothing to truncate to, and starting
/// empty beside a file that holds every template would make every leaf an orphan.
fn open_catalog(
    base: &Path,
    trie_file: Option<&Path>,
    data_dir: &str,
    schema: &Schema,
) -> Result<Trie, ServeError> {
    let path = crate::catalog::path_in(base);
    let existed = path.exists();
    let mut warn = |message: String| log(&format!("WARN catalog.log - {message}"));
    fn storage_error(e: &(dyn std::error::Error + 'static)) -> ServeError {
        ServeError::Storage {
            detail: error::chain(e),
        }
    }

    let (mut catalog, mut records) =
        Catalog::open(&path, &mut warn).map_err(|e| storage_error(&e))?;

    if !existed {
        match trie_file {
            Some(snapshot) if snapshot.exists() => {
                let (from_snapshot, failure) = crate::triefile::load_reporting(snapshot, data_dir);
                match failure {
                    Some(e) => {
                        log(&format!("WARN trie.json - {}", error::chain(&e)));
                        log("WARN trie.json - starting with an empty trie; templates \
                             are relearned from the next lines ingested");
                    }
                    None => {
                        let migrated = from_snapshot.catalog_records();
                        catalog.append(&migrated).map_err(|e| storage_error(&e))?;
                        log(&format!(
                            "catalog.log - none found; wrote {} from {} - {} template(s), \
                             {} record(s), {} byte(s)",
                            path.display(),
                            snapshot.display(),
                            commas(from_snapshot.template_count()),
                            commas(migrated.len() as u64),
                            commas(catalog.len()),
                        ));
                        // Carry on from the catalog: reopen and read back what was just
                        // written, so the trie this process serves is the one the next
                        // process will rebuild.
                        drop(catalog);
                        let reopened =
                            Catalog::open(&path, &mut warn).map_err(|e| storage_error(&e))?;
                        catalog = reopened.0;
                        records = reopened.1;
                    }
                }
            }
            _ => log(&format!(
                "catalog.log - {} not found, starting empty",
                path.display()
            )),
        }
    }

    let (trie, backfill) =
        Trie::from_catalog_with(data_dir, &records, schema.time_columns.clone(), &mut warn)
            .map_err(|e| storage_error(&e))?;
    // A template from before the time source existed has its source derived from the
    // column types the catalog holds; written now, once, so the choice is durable and
    // the next start makes the same one whatever `time_columns` says by then.
    if !backfill.is_empty() {
        catalog.append(&backfill).map_err(|e| storage_error(&e))?;
        log(&format!(
            "catalog.log - wrote a time_source record for {} template(s) that had none",
            commas(backfill.len() as u64)
        ));
    }
    if existed {
        log(&format!(
            "catalog.log - {} restored {} template(s) from {} record(s)",
            path.display(),
            commas(trie.template_count()),
            commas(records.len() as u64)
        ));
    } else if !records.is_empty() {
        log(&format!(
            "catalog.log - restored {} template(s) from the catalog just written",
            commas(trie.template_count())
        ));
    }
    trie.attach_catalog(catalog);
    Ok(trie)
}

/// Increments the in-flight count for as long as it is alive.
///
/// A `Drop` guard rather than a decrement after `next.run(...)`: a client that hangs up
/// mid-request leaves the handler future to be dropped, not polled to completion, and a
/// counter that only decrements on the happy path drifts upward until shutdown claims to
/// be waiting on requests that ended minutes ago.
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

// ── The ingest path ────────────────────────────────────────────────────────────────

/// Why a body could not be turned into NDJSON, before any line of it was read.
///
/// Distinct from `DetectorError`, which is one bad line among good ones that still land.
/// These three are the whole body. Answering a corrupt gzip with 500 per-line rejections
/// would describe the symptom - unreadable bytes - rather than the fault, which is that
/// nothing in the body was ever going to parse.
enum BodyError {
    /// A `Content-Encoding` this server cannot decode.
    Unsupported(String),
    /// `gzip`, but not gzip: wrong magic, a truncated stream, a failed CRC.
    Corrupt(String),
    /// Valid gzip that inflates past the cap. Carries the compressed size because the
    /// *ratio* identifies a bomb: 10 MB from 1 MB is a log batch, 10 GB from 1 MB is not.
    TooLarge { cap: usize, compressed: usize },
}

impl BodyError {
    /// The status, and the reason it is that status and not another.
    fn status(&self) -> StatusCode {
        match self {
            // 415 not 400: the bytes may be well formed. What is unacceptable is the
            // encoding they are in, which is what 415 names.
            Self::Unsupported(_) => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            // Said gzip, sent something else. Re-sending the same bytes fails the same
            // way, so a 4xx - which tells a shipper not to retry it forever.
            Self::Corrupt(_) => StatusCode::BAD_REQUEST,
            // What the wire cap answers with, because it is the same statement: too big.
            // That it was small until inflated belongs in the message, not the code.
            Self::TooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
        }
    }

    fn message(&self) -> String {
        match self {
            Self::Unsupported(encoding) => format!(
                "unsupported Content-Encoding `{encoding}`; this endpoint accepts gzip or \
                 an unencoded body"
            ),
            Self::Corrupt(why) => {
                format!("Content-Encoding is gzip but the body could not be decoded: {why}")
            }
            Self::TooLarge { cap, compressed } => format!(
                "gzip body of {compressed} bytes expands past the {cap} byte decompressed \
                 limit; it was refused while inflating, not after"
            ),
        }
    }
}

/// The body as NDJSON, decompressing first when the client said it was gzipped.
///
/// **Borrowed when there is nothing to do.** An unencoded body is the common case, and
/// copying 10 MiB to hand it to the same reader buys nothing.
///
/// **The cap is enforced while inflating, not after.** `Read::take` is load bearing:
/// without it the check that a body was too big happens after the allocation that killed
/// the process. With it the reader stops at `cap + 1` bytes however the body was built -
/// the extra byte being what separates "filled the cap" from "would have gone further".
///
/// `MultiGzDecoder`, not `GzDecoder`: concatenated members are a valid stream and a
/// shipper that batches by appending produces them. `GzDecoder` stops silently after the
/// first, dropping the rest of a batch and reporting success.
fn decode<'a>(
    body: &'a [u8],
    encoding: Option<&str>,
    cap: usize,
) -> Result<Cow<'a, [u8]>, BodyError> {
    let encoding = encoding.unwrap_or("").trim();
    if encoding.is_empty() || encoding.eq_ignore_ascii_case("identity") {
        return Ok(Cow::Borrowed(body));
    }
    if !encoding.eq_ignore_ascii_case("gzip") && !encoding.eq_ignore_ascii_case("x-gzip") {
        return Err(BodyError::Unsupported(encoding.to_string()));
    }

    let mut out = Vec::new();
    MultiGzDecoder::new(Cursor::new(body))
        .take(cap as u64 + 1)
        .read_to_end(&mut out)
        // Every decoder failure arrives here as an `io::Error` - and arrives, rather than
        // unwinding. A body is attacker-supplied input on a path a shipper hits thousands
        // of times an hour, and a panic in it takes the server, not the body.
        .map_err(|e| BodyError::Corrupt(e.to_string()))?;

    if out.len() > cap {
        return Err(BodyError::TooLarge {
            cap,
            compressed: body.len(),
        });
    }
    Ok(Cow::Owned(out))
}

/// One malformed line: `{"line":400,"msg":"..."}`.
///
/// The line number and the message are two fields, which is why `DetectorError` keeps
/// them as two things - see its own note.
struct LineError {
    line: usize,
    msg: String,
    /// Refused by the store, not by the parser: what makes the body a `503`.
    not_stored: bool,
}

/// What one `/ingest` body did.
struct IngestReport {
    ingested: u64,
    errors: Vec<LineError>,
    /// Rejections past `MAX_REPORTED_ERRORS`, counted but not described.
    omitted: u64,
    /// Lines the store refused, listed in `errors` up to its cap like any other rejection.
    /// Any at all makes the answer a `503`: see `partly_stored`.
    not_stored: u64,
    /// Why the first of them was refused, for the `503`'s own `error`: past the cap, or
    /// behind a thousand malformed lines, `errors` may not hold a single one.
    store_error: Option<String>,
}

impl IngestReport {
    fn to_json(&self) -> Value {
        let errors: Vec<Value> = self
            .errors
            .iter()
            .map(|e| json!({ "line": e.line, "msg": e.msg }))
            .collect();
        let mut report = json!({
            // "every line went in", not "the request was handled". A client that checks
            // one field should be told that 3 of its 500 lines are missing; the count and
            // the array are there for the client that wants to know which.
            "ok": self.errors.is_empty(),
            "ingested": self.ingested,
            "errors": errors,
        });

        // Present only when something was actually dropped, so a clean body is still
        // exactly `{"ok":true,"ingested":N,"errors":[]}` and no existing client sees a
        // field it has never seen. One that is almost always zero teaches clients to
        // ignore it, which is the opposite of what it is for.
        if self.omitted > 0 {
            report["errors_omitted"] = json!(self.omitted);
        }
        report
    }
}

/// The two ways a body fails whole: the client's fault and the server's, which is the
/// difference between a 4xx and a 5xx.
enum IngestFailure {
    /// The body could not be decoded: wrong encoding, corrupt gzip, or a bomb.
    Body(BodyError),
    /// A lock poisoned mid-body. Fails every line after it too, so the body is abandoned
    /// rather than answered with 500 copies of one message. A catalog append that fails
    /// is not this: it costs its line, reported in `errors`, and the body goes on.
    Fatal(CliError),
}

/// An NDJSON body into the database, one line at a time.
///
/// A plain function, not an `Ingestor` - see the module header.
///
/// **Per-line error isolation.** A bad line is recorded against its number and
/// the loop continues, so line 400 of 500 costs line 400 and nothing else. This is not a
/// second copy of that logic: `db::read_lines` is the same loop the CLI reads files with,
/// and the only thing this caller changes is where a rejection goes - to a `Vec` that
/// becomes the response body, instead of to a stderr an HTTP client cannot read.
///
/// The `Err` return is not a bad line. It is the trie's lock poisoned, which fails every
/// line after it too, so the body is abandoned rather than answered with 500 copies of
/// one message.
fn ingest(server: &Server, body: &[u8], now: DateTime<Utc>) -> Result<IngestReport, CliError> {
    // ── Half one: split and parse, holding nothing ─────────────────────────────────
    //
    // The lock used to be taken here, before the body had been split into lines, so every
    // body's parse - which is most of the cost of a line - ran one at a time and ingest
    // measured flat at 30,000 lines a second from one sender to eight.
    let mut parse_errors = Vec::new();
    let (parsed, stats) = db::parse_and_prepare(Cursor::new(body), &mut |e: DetectorError| {
        // Past the cap the rejection is counted and dropped. `stats.rejected` is
        // still the true total, so nothing is lost but the text.
        if parse_errors.len() < MAX_REPORTED_ERRORS {
            parse_errors.push(LineError {
                line: e.line,
                msg: e.kind.to_string(),
                not_stored: false,
            });
        }
    })?;

    // ── Half two: apply them, in order ─────────────────────────────────────────────
    //
    // No lock over the stores: each append takes its own leaf's, for as long as it takes
    // to push one row. Every line is applied before the response is sent, so a client
    // that ingests 500 lines and then queries sees all 500.
    let mut apply_errors = Vec::new();
    let mut ingested = 0u64;
    let mut rejected = stats.rejected;
    let mut not_stored = 0u64;
    let mut store_error = None;
    {
        let stores = &server.stores;
        for (line, parsed, prepared) in parsed {
            match db::ingest_prepared(&server.trie, Some(stores), &parsed, prepared, now) {
                Ok(()) => ingested += 1,
                // The line parsed and still did not land. Counted and reported exactly as
                // a line that did not parse: to the client both are "this one is not in
                // the database", and the message says which.
                Err(db::SinkError::Line(kind)) => {
                    rejected += 1;
                    let refused = matches!(kind, DetectorErrorKind::NotStored { .. });
                    if let DetectorErrorKind::NotStored { detail } = &kind {
                        not_stored += 1;
                        store_error.get_or_insert_with(|| detail.clone());
                    }
                    if apply_errors.len() < MAX_REPORTED_ERRORS {
                        apply_errors.push(LineError {
                            line,
                            msg: kind.to_string(),
                            not_stored: refused,
                        });
                    }
                }
                // The trie's lock poisoned, which fails every line after it too. The body
                // is abandoned rather than answered with 500 copies of one message.
                Err(db::SinkError::Fatal(e)) => return Err(e),
            }
        }
    }

    // **The report is by line number, and it was already.** One pass numbered its
    // rejections in the order it met them, which was line order; two passes meet them in
    // two runs, so the merge has to put them back. Capping each half at
    // `MAX_REPORTED_ERRORS` before the merge changes nothing: an error inside the merged
    // first thousand has fewer than a thousand errors before it overall, so it has fewer
    // than a thousand before it in its own half.
    let mut errors = parse_errors;
    errors.extend(apply_errors);
    errors.sort_by_key(|e| e.line);
    errors.truncate(MAX_REPORTED_ERRORS);

    server.lines_read.fetch_add(stats.read, Ordering::Relaxed);
    server.lines_ingested.fetch_add(ingested, Ordering::Relaxed);
    server.lines_rejected.fetch_add(rejected, Ordering::Relaxed);
    // Past the memory limit the flush runs now, not at its next tick: a burst that filled
    // the buffers in one body should not get half a second more to fill them further.
    if server.stores.over_buffer_limit() {
        server.flush_now.notify_one();
    }

    Ok(IngestReport {
        ingested,
        omitted: rejected - errors.len() as u64,
        errors,
        not_stored,
        store_error,
    })
}

// ── Handlers ───────────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct QueryRequest {
    q: String,
}

/// `{"ok":true,"status":"ok"}`, or `degraded` while the store cannot write.
///
/// Still takes no lock and reads no data - a health check that blocks behind a long ingest
/// reports the wrong thing - which is why the verdict lives on an `Arc<WriteHealth>`
/// beside the registry rather than inside it.
///
/// **`ok` stays `true` when degraded, and the status says otherwise.** This is the
/// liveness check an orchestrator kills the process on, and a process that cannot write
/// is not a process worth killing: restarting it does not empty the disk, and the rows
/// already on it are still queryable. `degraded` is for the dashboard and the human;
/// `/ingest` is where the refusal belongs, because that is the caller who would otherwise
/// be told a lie.
async fn health(State(server): State<Arc<Server>>) -> Json<Value> {
    if server.write_health.is_degraded() {
        return Json(json!({
            "ok": true,
            "status": "degraded",
            "detail": server
                .write_health
                .last_error()
                .unwrap_or_else(|| "the last flush could not write".to_string()),
        }));
    }
    Json(json!({ "ok": true, "status": "ok" }))
}

/// The refusal a shipper gets while the store cannot write.
///
/// 503 and a `Retry-After`, the same shape as the shed at `busy`, and for a related
/// reason: this server cannot take the batch now and the caller should keep it and come
/// back. The difference is in the body, which names the write error - an operator reading
/// a shipper's log needs to see `No space left on device` there and not "try later".
///
/// **Why a refusal at all.** With no write-ahead log a `200` means *buffered*, and a
/// buffer that cannot drain is not durability. Measured on a full disk: every batch got a
/// `200`, the shutdown log said "90,000 rows written", and after a restart `COUNT LOG {}`
/// was 0. A `503` is the only answer that keeps the line at the shipper, where it can
/// still be sent somewhere else. When the WAL lands this moves to the WAL: a store append
/// that fails after a durable WAL write is logged and answered `Ok`, because the data is
/// safe by then and a 500 would make a shipper resend a line that is already stored.
fn cannot_write(detail: &str) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::RETRY_AFTER, RETRY_AFTER_SECONDS.to_string())],
        Json(json!({
            "ok": false,
            "error": format!(
                "the store cannot write, so a 200 would be a lie - there is no \
                 write-ahead log and a buffered line is not a durable one: {detail}"
            ),
            "retry_after": RETRY_AFTER_SECONDS,
        })),
    )
        .into_response()
}

/// A body the store could not take all of: `503`, a `Retry-After`, and the ordinary
/// report, so the lines that did land and the ones that did not are both named.
///
/// **A 503 and not a 200**, though most of the body may be in. With no write-ahead log a
/// `200` over a line the store refused leaves the shipper believing it was delivered,
/// and nothing after that can bring it back. A `503` keeps the whole batch at the shipper,
/// and a retry that resends the lines that landed duplicates them - a cost that is visible
/// and bounded, where the lost line was neither. Bounded because the store stays degraded
/// until the leaf opens (`StoreManager::retry_unopened`), so every retry before then is
/// refused at the door and stores nothing: the lines this body stored are stored twice at
/// most. `errors` says which lines to resend for a client that reads it.
///
/// The message claims no more than the body holds. `errors` is capped, so past the cap -
/// or behind a thousand malformed lines - it may list few of the refused lines or none,
/// and the reason is carried here as well.
fn partly_stored(report: &IngestReport) -> Response {
    let listed = report.errors.iter().filter(|e| e.not_stored).count() as u64;
    let where_listed = if listed == report.not_stored {
        "each is in `errors` by line number".to_string()
    } else {
        format!(
            "{listed} of them are in `errors` by line number, and `errors_omitted` counts \
             the rest with the other rejections past the report's cap"
        )
    };
    let mut body = report.to_json();
    body["error"] = json!(format!(
        "the store could not take {} line(s) of this body ({}); {where_listed}. A 200 would \
         be a lie: there is no write-ahead log, and a line the store refused is nowhere",
        report.not_stored,
        report
            .store_error
            .as_deref()
            .unwrap_or("no reason recorded"),
    ));
    body["retry_after"] = json!(RETRY_AFTER_SECONDS);
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::RETRY_AFTER, RETRY_AFTER_SECONDS.to_string())],
        Json(body),
    )
        .into_response()
}

/// NDJSON in, `{"ok":true,"ingested":N,"errors":[]}` out. Optionally gzipped.
///
/// **Single tenant, deliberately.** No stream or tenant identifier, in the path or in a
/// header, and every line lands in the one trie this process holds. Recorded here because
/// the omission gets expensive with time: once a fleet is configured against this URL,
/// *adding* an identifier breaks every one of their configs.
///
/// It is also not a header away. The trie path is built from the line's own keys
/// (`ParsedLog::walk_tokens`) and a header is not part of the line, so separating tenants
/// means either a forced-first `__stream` token in the fingerprint - changing every
/// `template_id` - or one `Trie` and `Stores` per tenant, with a tenant selector on
/// `/query`, `/templates`, `/stats` and the viewer. Both are large changes, and a header
/// accepted but ignored would be worse than neither: it reads as isolation that is not
/// there. Revisit with the deployment work.
///
/// `body` is a `Result`, not a `Bytes`, for the reason `post_query` decodes its own body:
/// axum's rejection is plain text, and a client that parses one error format should not
/// need a second.
async fn post_ingest(
    State(server): State<Arc<Server>>,
    uri: axum::http::Uri,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    // `?received_at=<RFC 3339>`: the arrival time the ingest call is given, for a
    // replay or a differential that needs two servers to stamp one body identically.
    // Absent, it is now. Malformed, the body is refused rather than stamped with a guess.
    let now = match received_at_of(uri.query()) {
        Ok(Some(at)) => at,
        Ok(None) => Utc::now(),
        Err(text) => {
            return fail(
                StatusCode::BAD_REQUEST,
                &format!("received_at=`{text}` is not an RFC 3339 instant"),
            )
        }
    };
    let body = match body {
        Ok(body) => body,
        // Two unrelated things behind one rejection, and only one of them is the client's
        // to fix. Over the cap is the answer a shipper meets most often, and axum's
        // wording for it ("length limit exceeded") names neither the limit nor the size to
        // split to. A read that died halfway keeps axum's text: it has no number to
        // report, and "you sent too much" would send a shipper off to fix a batch that was
        // never too big.
        Err(rejection) if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE => {
            return fail(
                StatusCode::PAYLOAD_TOO_LARGE,
                &format!(
                    "body is larger than the {} byte limit, measured before any \
                     decompression; send a smaller batch",
                    server.api.max_body_bytes
                ),
            )
        }
        Err(rejection) => return fail(rejection.status(), &rejection.body_text()),
    };
    // Non-UTF-8 header bytes are not a header this server understands. `None` reaches
    // `decode` as "no encoding", so the body is rejected line by line rather than the
    // request being refused on a guess about what the sender meant.
    let encoding = headers
        .get(header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);

    let held = Arc::clone(&server);
    // The parse and the trie walk are CPU work, and each append after them takes a `std`
    // lock, so none of it belongs on a runtime thread that also answers `/health`.
    // Inflating joins them rather than running above: a gzip bomb is seconds of CPU, and
    // spending it on a runtime thread would stall every other endpoint while the server
    // was being attacked.
    let done = tokio::task::spawn_blocking(move || {
        let decoded = decode(&body, encoding.as_deref(), held.api.max_decompressed_bytes)
            .map_err(IngestFailure::Body)?;
        ingest(&held, &decoded, now).map_err(IngestFailure::Fatal)
    })
    .await;

    match done {
        // The server could not store some of it, which is not the client's fault and not
        // a thing a 200 can say.
        Ok(Ok(report)) if report.not_stored > 0 => partly_stored(&report),
        // 200 even with errors in it. The body was accepted and part of it was stored;
        // a 4xx would tell a shipper to retry the whole batch and duplicate the 497
        // lines that landed, and there is nothing the client could have sent differently
        // for the other 3 beyond not sending them.
        Ok(Ok(report)) => (StatusCode::OK, Json(report.to_json())).into_response(),
        Ok(Err(IngestFailure::Body(e))) => fail(e.status(), &e.message()),
        Ok(Err(IngestFailure::Fatal(e))) => {
            fail(StatusCode::INTERNAL_SERVER_ERROR, &error::chain(&e))
        }
        Err(join) => fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("ingest task failed: {join}"),
        ),
    }
}

/// The `received_at` query parameter, or `None` when there is not one. `Err` carries the
/// text that would not parse.
fn received_at_of(query: Option<&str>) -> Result<Option<DateTime<Utc>>, String> {
    let Some(query) = query else {
        return Ok(None);
    };
    for pair in query.split('&') {
        let Some(("received_at", value)) = pair.split_once('=') else {
            continue;
        };
        let text = percent_decode(value);
        return DateTime::parse_from_rfc3339(&text)
            .map(|at| Some(at.with_timezone(&Utc)))
            .map_err(|_| text);
    }
    Ok(None)
}

/// `%XX` escapes and `+` resolved, which is all an RFC 3339 instant in a query string
/// can carry. Anything else is left as it was.
fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
                match u8::from_str_radix(hex, 16) {
                    Ok(b) => {
                        out.push(b);
                        i += 3;
                    }
                    Err(_) => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Admission control for `/ingest`: a 503 while the store cannot write, then a permit or
/// a 503 that says when to come back.
///
/// **A layer, not a check inside the handler.** A layer wraps the handler, so it runs
/// before the extractors - before a byte of the body is pulled off the socket. Shedding in
/// the handler would buffer `max_body_bytes` first and *then* refuse, spending the memory
/// the shed exists to protect.
///
/// 503 and not 429: 429 claims a per-client quota and there is none here. This server is
/// saturated, which is what 503 says and what `Retry-After` dates. Either makes Vector back
/// off; only one is true.
///
/// What this must never do is take the body and go quiet. A shipper with no answer keeps
/// the batch, times out, and opens another connection - so a merely slow server gathers
/// connections until it is unreachable. The immediate refusal is the kinder answer
/// precisely because a client knows what to do with it.
async fn admit(State(server): State<Arc<Server>>, request: Request, next: Next) -> Response {
    // **First, and in the layer rather than in the handler**, for the reason the shed is
    // here: a layer runs before the extractors, so a body this server cannot store is
    // refused before ten megabytes of it are pulled off the socket. In the handler the
    // `Bytes` extractor has already buffered it.
    //
    // Before the permit too. A server that cannot write has no use for the slot, and a
    // shipper told "at capacity" would read that as "send fewer" when the answer is "send
    // none until the disk is back".
    if server.write_health.is_degraded() {
        let detail = server
            .write_health
            .last_error()
            .unwrap_or_else(|| "the last flush could not write".to_string());
        return cannot_write(&detail);
    }
    // Past twice the buffer limit the flush is falling behind what arrives, even closing
    // the largest buffers early, and a body taken now is memory the process may not have.
    // Refused like a shed - a shipper told to come back in a second keeps its batch - and
    // counted as one.
    if server.stores.buffers_full() {
        server.requests_shed.fetch_add(1, Ordering::Relaxed);
        return buffers_full(server.stores.buffer_bytes());
    }

    let permits = Arc::clone(&server.ingest_permits);
    // `acquire_owned`, so the permit outlives this scope and covers the blocking task,
    // which is the part that is actually scarce.
    let waited = tokio::time::timeout(INGEST_QUEUE_WAIT, permits.acquire_owned()).await;
    let _permit = match waited {
        Ok(Ok(permit)) => permit,
        // The semaphore is never closed, so this is the timeout and nothing else.
        _ => {
            server.requests_shed.fetch_add(1, Ordering::Relaxed);
            return busy(server.api.ingest_concurrency);
        }
    };
    next.run(request).await
}

/// The refusal while the buffers are past twice their limit: the shed's shape, with what
/// is held against what may be.
fn buffers_full((held, limit): (u64, u64)) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::RETRY_AFTER, RETRY_AFTER_SECONDS.to_string())],
        Json(json!({
            "ok": false,
            "error": format!(
                "the buffers hold {} bytes of memory, past twice the {} byte limit: the \
                 flush is behind what is arriving; retry in {RETRY_AFTER_SECONDS}s",
                commas(held),
                commas(limit)
            ),
        })),
    )
        .into_response()
}

/// The shed response: 503, a `Retry-After`, and the same error shape as everything else.
fn busy(permits: usize) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::RETRY_AFTER, RETRY_AFTER_SECONDS.to_string())],
        Json(json!({
            "ok": false,
            "error": format!(
                "ingest is at capacity ({permits} concurrent bodies); retry in \
                 {RETRY_AFTER_SECONDS}s"
            ),
        })),
    )
        .into_response()
}

/// `{"q":"<trieql>"}` in, `query::result_json` out - the same JSON `--format json` prints.
///
/// The body is read as `Bytes` and decoded here rather than through `Json<QueryRequest>`,
/// so that a malformed request body is answered with the same `{"ok":false,"error":...}`
/// shape as a malformed query. axum's own rejection is plain text, and a client that
/// parses one error format should not need a second.
async fn post_query(State(server): State<Arc<Server>>, body: Bytes) -> Response {
    let request: QueryRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(e) => {
            return fail(
                StatusCode::BAD_REQUEST,
                &format!("expected {{\"q\": \"…\"}}: {e}"),
            )
        }
    };

    // Parsed before the blocking task, so a bad query costs no thread: it is a string
    // and a grammar, and the answer is a 400 either way.
    let query = match query::from_trieql(&request.q, Utc::now(), server.limits) {
        Ok(query) => query,
        Err(e) => return fail(StatusCode::BAD_REQUEST, &error::chain(&e)),
    };

    let held = Arc::clone(&server);
    let done = tokio::task::spawn_blocking(move || {
        let result = engine::execute(&held.trie, &held.stores, &query)?;
        held.queries.fetch_add(1, Ordering::Relaxed);
        Ok::<Value, engine::ExecError>(query::result_json(&query, &result))
    })
    .await;

    match done {
        Ok(Ok(body)) => (StatusCode::OK, Json(body)).into_response(),
        // A bucket count over the cap is the client's to fix - a narrower range or a wider
        // bucket - so it is a 400 beside the parse errors above, not a 500. A storage
        // failure is the server's and stays one.
        Ok(Err(e @ engine::ExecError::Buckets(_))) => {
            fail(StatusCode::BAD_REQUEST, &error::chain(&e))
        }
        Ok(Err(e)) => fail(StatusCode::INTERNAL_SERVER_ERROR, &error::chain(&e)),
        Err(join) => fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("query task failed: {join}"),
        ),
    }
}

/// `POST /load`: what `triedb load` sends when this server holds the data directory.
#[derive(Debug, Deserialize)]
struct LoadRequest {
    /// Files, already expanded from any directory by the client, which is where the command
    /// line was: each with the file name the load id hashes. The load itself puts them in
    /// load order and keeps each file once, whoever sent them.
    inputs: Vec<LoadInput>,
    #[serde(default)]
    replace_day: Option<String>,
    #[serde(default)]
    max_mb_per_sec: Option<f64>,
    #[serde(default)]
    threads: Option<usize>,
    #[serde(default)]
    received_at: Option<String>,
    #[serde(default)]
    proceed: bool,
    #[serde(default)]
    max_arrival_share: Option<f64>,
    /// The data directory the client meant, canonical. `LOCK` can still name the last
    /// holder's address in the moment before a new holder announces its own, and a load
    /// sent there would be committed into whatever that address now serves.
    #[serde(default)]
    data_dir: Option<std::path::PathBuf>,
}

#[derive(Debug, Deserialize)]
struct LoadInput {
    path: std::path::PathBuf,
    name: String,
}

/// Parse and check a load request into the loader's options, or say what is wrong with it.
fn load_options(request: LoadRequest) -> Result<load::Options, String> {
    if request.inputs.is_empty() {
        return Err("inputs is empty: nothing to load".to_string());
    }
    let mut inputs = Vec::with_capacity(request.inputs.len());
    for input in request.inputs {
        // Absolute, because it is this process's working directory that resolves it and
        // not the client's.
        if !input.path.is_absolute() {
            return Err(format!("{}: not an absolute path", input.path.display()));
        }
        match std::fs::metadata(&input.path) {
            Ok(meta) if meta.is_file() => {}
            Ok(_) => return Err(format!("{}: not a file", input.path.display())),
            Err(e) => return Err(format!("{}: {e}", input.path.display())),
        }
        inputs.push(load::Input {
            path: input.path,
            name: input.name,
        });
    }
    let replace_day = match request.replace_day.as_deref() {
        None => None,
        Some(text) => Some(
            crate::store::disk::day_of_name(text)
                .ok_or_else(|| format!("replace_day `{text}` is not a YYYY-MM-DD day"))?,
        ),
    };
    let received_at = match request.received_at.as_deref() {
        None => Utc::now(),
        Some(text) => DateTime::parse_from_rfc3339(text)
            .map_err(|e| format!("received_at `{text}` is not RFC 3339: {e}"))?
            .with_timezone(&Utc),
    };
    let share = request
        .max_arrival_share
        .unwrap_or(load::DEFAULT_MAX_ARRIVAL_SHARE);
    if !(share.is_finite() && share >= 0.0) {
        return Err(format!("max_arrival_share {share} is not a percentage"));
    }
    Ok(load::Options {
        inputs,
        replace_day,
        max_bytes_per_sec: request.max_mb_per_sec.map(|mb| mb * 1_000_000.0),
        threads: request.threads.unwrap_or_else(load::server_threads).max(1),
        received_at,
        proceed: request.proceed,
        max_arrival_share: share,
    })
}

/// Start a load job and answer at once with its number; `GET /load/<job>` says how it went.
///
/// **Not held open for the load.** A request that lasted the whole load would hold up a
/// graceful shutdown for as long - axum waits for every connection - and a client that
/// gave up would not stop it. The job runs on a thread of its own, the client polls, and
/// a shutdown stops the job before its commit and waits for it.
async fn post_load(State(server): State<Arc<Server>>, headers: HeaderMap, body: Bytes) -> Response {
    if server.stores.disk_store().is_none() {
        return fail(
            StatusCode::SERVICE_UNAVAILABLE,
            &load::LoadError::NoDisk.to_string(),
        );
    }
    if let Some(refused) = load_refused(&server, &headers) {
        return refused;
    }
    let request: LoadRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => return fail(StatusCode::BAD_REQUEST, &format!("load request: {e}")),
    };
    if let (Some(asked), Some(ours)) = (&request.data_dir, &server.storage.base_path) {
        let ours = std::fs::canonicalize(ours).unwrap_or_else(|_| ours.clone());
        if *asked != ours {
            return fail(
                StatusCode::CONFLICT,
                &format!(
                    "this server serves {}, not {}; nothing was loaded",
                    ours.display(),
                    asked.display()
                ),
            );
        }
    }
    let options = match load_options(request) {
        Ok(o) => o,
        Err(message) => return fail(StatusCode::BAD_REQUEST, &message),
    };
    let mut loads = server.loads.lock().unwrap_or_else(|e| e.into_inner());
    if let Some((id, _)) = loads.jobs.iter().find(|(_, job)| {
        matches!(
            *job.state.lock().unwrap_or_else(|e| e.into_inner()),
            LoadState::Running
        )
    }) {
        return fail(
            StatusCode::CONFLICT,
            &format!("load job {id} is still running; one load runs at a time"),
        );
    }
    // Reap the last one's thread, which has finished: its state says so.
    if let Some(done) = loads.running.take() {
        let _ = done.join();
    }
    loads.next += 1;
    let id = loads.next;
    let job = Arc::new(LoadJob {
        control: load::Control::default(),
        state: std::sync::Mutex::new(LoadState::Running),
    });
    loads.jobs.insert(id, Arc::clone(&job));
    while loads.jobs.len() > LOAD_JOBS_KEPT {
        let oldest = *loads.jobs.keys().next().expect("more than kept");
        loads.jobs.remove(&oldest);
    }
    let held = Arc::clone(&server);
    let spawned = std::thread::Builder::new()
        .name(format!("load-{id}"))
        .spawn(move || {
            log(&format!(
                "load job {id} - {} input(s){}",
                options.inputs.len(),
                options
                    .replace_day
                    .map(|d| format!(", replacing {}", crate::store::disk::day_name(d)))
                    .unwrap_or_default()
            ));
            // A panic is a failed job and not a job running for ever: left `running`, it
            // would refuse every later load with a 409 and keep its client polling. Its
            // staging stays - a panic past the commit record leaves the only copy there -
            // and the next start finishes or removes it.
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                load::run(
                    &held.trie,
                    &held.stores,
                    &options,
                    &job.control,
                    &mut |_| {},
                )
            }));
            let state = match result {
                Ok(Ok(report)) => {
                    log(&format!("load job {id} - {}", report.summary()));
                    LoadState::Done(Box::new(report))
                }
                Ok(Err(e)) => {
                    log(&format!("load job {id} - failed: {}", error::chain(&e)));
                    LoadState::Failed(error::chain(&e))
                }
                Err(_) => {
                    let why = "the load failed on a bug in this build (a panic); anything it \
                               staged is finished or removed by the next start";
                    log(&format!("load job {id} - {why}"));
                    LoadState::Failed(why.to_string())
                }
            };
            *job.state.lock().unwrap_or_else(|e| e.into_inner()) = state;
        });
    match spawned {
        Ok(handle) => {
            loads.running = Some(handle);
            (StatusCode::ACCEPTED, Json(json!({"ok": true, "job": id}))).into_response()
        }
        Err(e) => {
            loads.jobs.remove(&id);
            fail(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("starting the load job: {e}"),
            )
        }
    }
}

/// `GET /load/<job>`: running, done with its report, or failed with why.
async fn get_load(
    State(server): State<Arc<Server>>,
    headers: HeaderMap,
    axum::extract::Path(id): axum::extract::Path<u64>,
) -> Response {
    if let Some(refused) = load_refused(&server, &headers) {
        return refused;
    }
    let job = {
        let loads = server.loads.lock().unwrap_or_else(|e| e.into_inner());
        loads.jobs.get(&id).map(Arc::clone)
    };
    let Some(job) = job else {
        return fail(StatusCode::NOT_FOUND, &format!("no load job {id}"));
    };
    let state = job.state.lock().unwrap_or_else(|e| e.into_inner());
    let body = match &*state {
        LoadState::Running => json!({
            "ok": true, "job": id, "state": "running",
            "lines": job.control.lines.load(Ordering::Relaxed),
        }),
        LoadState::Done(report) => {
            json!({"ok": true, "job": id, "state": "done", "report": report})
        }
        LoadState::Failed(error) => {
            json!({"ok": true, "job": id, "state": "failed", "error": error})
        }
    };
    Json(body).into_response()
}

/// Stop a running load before it commits, and wait for its thread: shutdown's first step
/// after the last request. A load already inside its commit finishes it - that is a few
/// renames - and one before it removes its staging directory.
fn stop_loads(server: &Server) {
    let (running, jobs): (Option<std::thread::JoinHandle<()>>, Vec<Arc<LoadJob>>) = {
        let mut loads = server.loads.lock().unwrap_or_else(|e| e.into_inner());
        (loads.running.take(), loads.jobs.values().cloned().collect())
    };
    let Some(running) = running else {
        return;
    };
    for job in &jobs {
        job.control.cancel.store(true, Ordering::Relaxed);
    }
    if !running.is_finished() {
        log("waiting for the running load job to stop");
    }
    let _ = running.join();
}

/// The log viewer. One HTML file, inline CSS and JS, no other request needed.
///
/// `no-store`, because the page is rebuilt with the binary. A cached viewer against a
/// newer server is a debugging tool lying about what the database answered, which costs
/// more than the few kilobytes it saves.
async fn get_viewer() -> Response {
    (
        [(axum::http::header::CACHE_CONTROL, "no-store")],
        Html(VIEWER),
    )
        .into_response()
}

/// Every template: its path, its columns, its row count, and when it was first and last
/// seen.
///
/// The path is the template's identity and the columns alone cannot reconstruct it -
/// `LeafMeta` does not record which names were promoted into the fingerprint, so
/// `event=login_failed, ts, user` and a leaf holding the same three names arranged
/// differently would print identically. `Trie::leaf_paths` carries the tokens for that
/// reason, as `PlannedLeaf::path` does in the engine.
async fn get_templates(State(server): State<Arc<Server>>) -> Response {
    let stores = &server.stores;
    let mut templates = Vec::new();

    for (path, leaf) in server.trie.leaf_paths() {
        // A leaf whose meta will not read is a poisoned lock, and there is nothing to
        // report about it that is not a guess. Skipped rather than faked - `count` below
        // is the length of what was actually readable.
        let Ok(slot) = leaf.leaf_meta.read() else {
            continue;
        };
        let Some(meta) = slot.as_ref() else {
            continue;
        };

        let columns: Vec<Value> = meta
            .columns
            .iter()
            .map(|col| {
                json!({
                    "name": col.name,
                    // `null`, not "String": a column that has only ever held nulls has no
                    // type yet, and that is a different fact from having widened to text.
                    "type": col.col_type.map(|t| t.as_str()),
                    "widened_at": col.widened_at.map(rfc3339_at),
                })
            })
            .collect();

        templates.push(json!({
            "template_id": meta.template_id,
            "path": path,
            "columns": columns,
            // A template with no store is one that exists and holds no rows. Zero, not
            // absent - the two empty answers stay apart here as they do in the engine.
            "rows": stores.lookup(meta.template_id).map_or(0, |s| s.len()),
            "first_seen": rfc3339_at(meta.first_seen),
            "last_seen": rfc3339_at(meta.last_seen),
            "storage_path": meta.storage_path,
            // `column:<name>` or `arrival`, as the catalog spells it.
            "time_source": meta.time_source.as_record(),
        }));
    }

    Json(json!({ "count": templates.len(), "templates": templates })).into_response()
}

/// Throughput, template count, storage sizes.
///
/// `storage` says which of the two modes this process is in and what that costs. The
/// banner is seen once by whoever started it; this is the field a dashboard can read.
///
/// `buffered_rows` and `write_failures` are the two numbers that are only visible here.
/// The first is exactly what a `kill -9` would cost while there is no write-ahead log. The
/// second exists because a disk that has gone away is otherwise invisible: `/ingest` keeps
/// answering 200 - the line *was* parsed, and the trie *did* take it - while nothing
/// reaches a file. A silent write failure behind a successful response is the worst
/// outcome available here, so it gets a counter.
async fn get_stats(State(server): State<Arc<Server>>) -> Response {
    let stores = &server.stores;
    let (open_segments, max_open_segments) = stores.open_files();
    let (buffered_bytes, max_buffer_bytes) = stores.buffer_bytes();
    let uptime = server.started.elapsed().as_secs_f64();
    let ingested = server.lines_ingested.load(Ordering::Relaxed);
    let queries = server.queries.load(Ordering::Relaxed);

    Json(json!({
        "uptime_seconds": uptime,
        "started_at": rfc3339_at(server.started_at),
        "storage": {
            "kind": stores.kind(),
            "durable": server.storage.is_durable(),
            "note": if server.storage.is_durable() {
                "one shared data file per UTC day, named in manifest.log; a row group closes at 8,192 rows, 8MB or the row group timeout, at most max_open_segments day files are open for writing at once and a row for a day with none open past that goes into today's, and there is no write-ahead log yet - so an unclean exit loses up to one row group"
            } else {
                "in-memory only - every row is lost when this process exits"
            },
            "base_path": stores.base_path().map(|p| p.display().to_string()),
            "trie_file": server
                .storage
                .trie_file
                .as_ref()
                .map(|p| p.display().to_string()),
            // The file templates are rebuilt from. `trie_file` is the readable snapshot
            // nothing loads while this exists.
            "catalog": server.trie.catalog_path().map(|p| p.display().to_string()),
            "flush_interval_ms": server.storage.flush_interval.as_millis() as u64,
            // How long a row group may stay open, which is also what an unclean exit
            // costs until the write-ahead log lands.
            "row_group_timeout_ms": server.storage.row_group_timeout.as_millis() as u64,
            "stores_open": stores.open(),
            // Day files held open for writing, and the cap they are held under. Past it the
            // least recently written one is sealed.
            "open_segments": open_segments,
            "max_open_segments": max_open_segments,
            "segment_max_bytes": server.storage.segment_max_bytes,
            "rows": stores.rows(),
            "buffered_rows": stores.buffered(),
            // Memory the buffered rows hold - estimated, hash tables and allocations
            // included - against the limit past which the largest are closed early; and
            // how many have been, which is nonzero only when the buffers could not keep up.
            "buffered_bytes": buffered_bytes,
            "max_buffer_bytes": max_buffer_bytes,
            "forced_closes": stores.forced_closes(),
            "write_failures": stores.write_failures(),
            "last_write_error": stores.last_write_error(),
            // True while the last flush tick failed, which is also while /ingest answers
            // 503 and /health says degraded. The first tick that writes everything
            // clears it.
            "degraded": stores.is_degraded(),
            // On disk this is the segments' bytes as of the last row group closed, plus what the
            // buffer would add. In memory it is payload bytes - key text, string values,
            // and a scalar's fixed width - and the resident size is larger, because
            // allocator slack and the hash tables are not in it. Estimated either way.
            "approx_bytes": stores.approx_bytes(),
        },
        "trie": {
            "templates": server.trie.template_count(),
            "nodes": server.trie.node_count(),
            "tokens": server.trie.token_count(),
        },
        "ingest": {
            "lines_read": server.lines_read.load(Ordering::Relaxed),
            "lines_ingested": ingested,
            "lines_rejected": server.lines_rejected.load(Ordering::Relaxed),
            // Averaged over uptime, not sampled over a window: an idle server reports a
            // falling rate rather than a stale spike, which is the honest reading of a
            // counter divided by a clock.
            "lines_per_second": rate(ingested, uptime),
            // Nonzero means the server was telling shippers to slow down, which is the
            // design working. Only visible here: a shed body was never read, so it is in
            // none of the line counters above.
            "requests_shed": server.requests_shed.load(Ordering::Relaxed),
            "ingest_concurrency": server.api.ingest_concurrency,
        },
        "queries": {
            "served": queries,
            "per_second": rate(queries, uptime),
        },
    }))
    .into_response()
}

/// Zero for the first instant of uptime, rather than an infinity that serialises to
/// `null` and reads as "unknown".
fn rate(n: u64, seconds: f64) -> f64 {
    if seconds <= 0.0 {
        0.0
    } else {
        n as f64 / seconds
    }
}

/// Every failure this surface reports, in one shape: `{"ok":false,"error":"..."}`.
fn fail(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({ "ok": false, "error": message }))).into_response()
}

async fn track(State(server): State<Arc<Server>>, request: Request, next: Next) -> Response {
    let _guard = InFlight::enter(&server);
    next.run(request).await
}

fn router(server: Arc<Server>) -> Router {
    Router::new()
        .route("/", get(get_viewer))
        .route("/health", get(health))
        .route("/stats", get(get_stats))
        .route("/templates", get(get_templates))
        .route("/query", post(post_query))
        // A load names its files, not their lines: a list of absolute paths, which for a
        // directory of months of hourly files passes axum's 2MB default.
        .route(
            "/load",
            post(post_load).layer(DefaultBodyLimit::max(MAX_LOAD_REQUEST_BYTES)),
        )
        .route("/load/{job}", get(get_load))
        // Order matters. `.layer()` wraps what is already there, so the *last* added is
        // outermost: `admit` runs first, and a shed body never reaches the extractor that
        // would have buffered it.
        .route(
            "/ingest",
            post(post_ingest)
                .layer(DefaultBodyLimit::max(server.api.max_body_bytes))
                .layer(axum::middleware::from_fn_with_state(
                    Arc::clone(&server),
                    admit,
                )),
        )
        .layer(axum::middleware::from_fn_with_state(
            Arc::clone(&server),
            track,
        ))
        .with_state(server)
}

// ── Lifecycle ──────────────────────────────────────────────────────────────────────

/// Build the runtime and run the server until a signal cancels it.
///
/// The runtime is built here rather than by putting `#[tokio::main]` on `main`, because
/// every other subcommand is synchronous and none of them should pay for a thread pool
/// to parse a file.
pub fn run(
    host: &str,
    port: u16,
    limits: Limits,
    api: ApiLimits,
    storage: Storage,
    schema: Schema,
) -> Result<(), CliError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(ServeError::Runtime)?;
    runtime.block_on(serve(host, port, limits, api, storage, schema))?;
    Ok(())
}

async fn serve(
    host: &str,
    port: u16,
    limits: Limits,
    api: ApiLimits,
    storage: Storage,
    schema: Schema,
) -> Result<(), ServeError> {
    // Read-only, so it runs before the lock and the port: a data directory holding a
    // segment from another format version is not served at all, and nothing is listening
    // when the process exits over it. Everything that writes - catalog recovery, segment
    // recovery - waits for the directory's LOCK below.
    if let Some(base) = storage.base_path.as_deref() {
        crate::store::disk::refuse_other_versions(base).map_err(storage_fatal)?;
    }
    // Taken before anything is written and before the port: two servers on one directory
    // - on two ports, or both on `--port 0` - would each append to the catalog and the
    // manifest over the other's records. Held until this function returns.
    let mut lock = match storage.base_path.as_deref() {
        Some(base) => {
            let mut lock = crate::lock::DataLock::acquire(base)?;
            let _ = lock.announce(&crate::lock::Holder::server(None, None));
            Some(lock)
        }
        None => None,
    };

    let addr = format!("{host}:{port}");
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(|source| ServeError::Bind {
            addr: addr.clone(),
            source,
        })?;
    let bound: SocketAddr = listener
        .local_addr()
        .map_err(|source| ServeError::Bind { addr, source })?;

    // Written to `LOCK` beside the address: a `/load` shows it or is refused. See lock.rs.
    let load_token = match &lock {
        Some(_) => Some(
            crate::store::v3::file::new_file_id()
                .map(|id| crate::store::manifest::hex(&id))
                .map_err(|e| ServeError::Storage {
                    detail: format!("drawing the load token: {e}"),
                })?,
        ),
        None => None,
    };
    let server = Arc::new(Server::new(
        Utc::now(),
        limits,
        api,
        storage,
        schema,
        load_token.clone(),
    )?);
    // Where `triedb load` finds this server - written once it can answer. An address of
    // all interfaces is reached on loopback from the same machine, which is where the
    // directory is.
    if let Some(lock) = lock.as_mut() {
        let reach = if bound.ip().is_unspecified() {
            SocketAddr::new(std::net::Ipv4Addr::LOCALHOST.into(), bound.port())
        } else {
            bound
        };
        let holder = crate::lock::Holder::server(Some(reach.to_string()), load_token);
        if let Err(e) = lock.announce(&holder) {
            log(&format!(
                "WARN {e}; `triedb load` will not find this server"
            ));
        }
    }
    let token = CancellationToken::new();
    // Installed before the banner: a server that cannot hear SIGTERM will be killed by
    // the SIGKILL that follows it, and a deploy is a bad place to discover that.
    let signals = signals(token.clone())?;

    banner(bound, &server);
    tokio::spawn(signals);

    // The clock the stores deliberately do not own. Held rather than detached: a tick
    // still running after the final flush would write behind the `trie.json` snapshot, and
    // the two would then disagree about what exists.
    let flusher = tokio::spawn(flush_loop(Arc::clone(&server), token.clone()));

    let shutdown = {
        let token = token.clone();
        let server = Arc::clone(&server);
        async move {
            token.cancelled().await;
            // Shutdown, step one. Everything after this is the wind-down; the log names each
            // step so an operator can see which ones this build actually has.
            log(&format!(
                "stop accepting - {} request(s) in flight",
                server.in_flight.load(Ordering::Relaxed)
            ));
        }
    };

    axum::serve(listener, router(Arc::clone(&server)))
        .with_graceful_shutdown(shutdown)
        .await
        .map_err(ServeError::Serve)?;

    // `axum::serve` returns once every connection it accepted has finished, so reaching
    // this line *is* "in-flight requests complete first".
    //
    // Loads stop next: one past its commit finishes it, one before it removes its staging,
    // so the final flush and the trie snapshot below see the directory as it will stay - a
    // load committing beside them would publish rows the snapshot does not describe.
    stop_loads(&server);

    // The flush task is joined before the wind-down and not after: a tick arriving between
    // the final flush and the trie snapshot would write rows the snapshot does not
    // describe, and a tick beside the final flush would only queue behind it.
    let _ = flusher.await;
    let result = shutdown_sequence(&server);
    drop(lock);
    result
}

/// Ask every store whether a row group is due, every interval, until the token is
/// cancelled.
///
/// **The tick no longer writes on its own account.** A row group closes at 8,192 rows,
/// 8MB or the row group timeout, and the tick is what notices - so most ticks on a quiet
/// server do nothing, and on a busy one the first tick after the 8,192nd row writes every
/// row group then due and flushes each file it wrote to once.
///
/// It is also woken early, by an ingest body that left the buffers past their memory limit.
///
/// Two details that are not decoration:
///
/// - **`spawn_blocking`.** The flush is file I/O. On a runtime worker that blocks the
///   whole executor, including `/health`, which is the endpoint an orchestrator reads to
///   decide whether to kill the process.
/// - **`MissedTickBehavior::Delay`.** The default fires catch-up ticks back to back after
///   a slow flush, which turns one stall into a burst of them at exactly the moment the
///   disk is already behind.
async fn flush_loop(server: Arc<Server>, token: CancellationToken) {
    if !server.storage.is_durable() {
        return;
    }
    let mut tick = tokio::time::interval(server.storage.flush_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The first tick completes immediately; consumed here so the loop's first real wait is
    // a whole interval rather than none.
    tick.tick().await;
    // Whether the last flush was closing buffers early for the memory limit: said once when
    // it starts and once when it stops, not on every tick in between.
    let mut forcing = false;
    loop {
        tokio::select! {
            () = token.cancelled() => return,
            _ = tick.tick() => {}
            () = server.flush_now.notified() => {}
        }
        let held = Arc::clone(&server);
        let flushed = tokio::task::spawn_blocking(move || held.stores.flush_all(FlushKind::Tick));
        match flushed.await {
            Ok(summary) => {
                if summary.failed > 0 {
                    log(&format!(
                        "WARN flush - {} leaf/leaves could not be written; their rows stay \
                         buffered and the next tick retries",
                        summary.failed
                    ));
                }
                if (summary.forced > 0) != forcing {
                    forcing = summary.forced > 0;
                    let (_, limit) = server.stores.buffer_bytes();
                    log(&if forcing {
                        format!(
                            "WARN buffers - past the {} byte limit; closing the largest early \
                             ({} this flush), which makes small row groups",
                            commas(limit),
                            summary.forced
                        )
                    } else {
                        format!(
                            "buffers - back under the {} byte limit; {} closed early in all",
                            commas(limit),
                            commas(server.stores.forced_closes())
                        )
                    });
                }
            }
            Err(join) => log(&format!("WARN flush task failed: {join}")),
        }
    }
}

/// SIGTERM and SIGINT, both cancelling the one token that every long-running part of the
/// server is watching.
///
/// Returns the future rather than spawning it, so a handler that could not be installed
/// is an error at startup instead of a task that quietly never fires.
fn signals(token: CancellationToken) -> Result<impl std::future::Future<Output = ()>, ServeError> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).map_err(|source| ServeError::Signal {
            signal: "SIGTERM",
            source,
        })?;
        let mut int = signal(SignalKind::interrupt()).map_err(|source| ServeError::Signal {
            signal: "SIGINT",
            source,
        })?;
        Ok(async move {
            let name = tokio::select! {
                _ = term.recv() => "SIGTERM",
                _ = int.recv() => "SIGINT",
            };
            log(&format!("{name} received - shutting down"));
            token.cancel();
        })
    }
    #[cfg(not(unix))]
    {
        Ok(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                log("interrupt received - shutting down");
            }
            token.cancel();
        })
    }
}

/// The fixed shutdown order, with the steps this build does not have named as skipped.
///
/// ```text
/// stop accepting → final flush → WAL checkpoint → trie.json → close stores → close WAL
/// ```
///
/// Printing the skipped steps rather than omitting them is the point: an operator watching
/// a clean shutdown scroll past should see exactly which durability they have and which
/// they do not. Two of the six are real; the WAL's two are not yet.
fn shutdown_sequence(server: &Server) -> Result<(), ServeError> {
    log("in-flight requests complete");

    // What the final flush could not write, if anything. Carried to the end rather than
    // returned here: the trie snapshot and the close-stores line are still worth doing
    // and still worth printing, and an operator reading a failed shutdown wants the whole
    // sequence and not the first half of it.
    let mut lost: Option<(usize, u64)> = None;

    if server.storage.is_durable() {
        let summary = server.stores.flush_all(FlushKind::Final);
        // Rows still in a buffer once the final flush has done all it can. **This is the
        // honest count of what was lost**: with no write-ahead log a buffered row has never
        // been anywhere but memory, and this process is about to end.
        if summary.failed > 0 {
            lost = Some((summary.failed, server.stores.buffered()));
        }
        // Bounded by the open days and not by the templates: one flush and one seal per day
        // file, however many templates' row groups went into it. Sealing a file per
        // template per day took 349 seconds for 200,000 rows, where Kubernetes sends
        // SIGKILL after 30.
        log(&format!(
            "final flush - {} row(s) into {} row group(s) in {} file(s), {} sealed, {} \
             byte(s), {:.2}s{}",
            commas(summary.rows),
            commas(summary.row_groups as u64),
            summary.syncs,
            summary.sealed,
            commas(summary.bytes),
            summary.took.as_secs_f64(),
            // Nought rows against a failed leaf is a real outcome and not a rounding: a
            // leaf whose row groups all landed and whose `_leaf.json` would not write has
            // failed without losing anything. Said in as many words, because "0 row(s)
            // were NOT written" beside "FAILED" reads as a bug in the counter.
            match lost {
                None => String::new(),
                Some((leaves, 0)) => format!(
                    ", {leaves} leaf/leaves FAILED - every row was written, so nothing is \
                     lost, but something the shutdown had to do did not happen"
                ),
                Some((leaves, rows)) => format!(
                    ", {leaves} leaf/leaves FAILED: {} row(s) were NOT written and are lost",
                    commas(rows)
                ),
            },
        ));
    } else {
        log("final flush - skipped, storage is memory (--data-dir puts the stores on disk)");
    }

    log("WAL checkpoint - skipped, there is no WAL yet");

    match server.storage.trie_file.as_deref() {
        Some(path) => match crate::triefile::save(&server.trie, path) {
            Ok(bytes) => log(&format!(
                "trie.json - {} {} byte(s), {} template(s)",
                path.display(),
                commas(bytes),
                commas(server.trie.template_count())
            )),
            // Not a reason to fail the exit. The rows are already written, and the next
            // start relearns the templates from the lines it is sent - which is the same
            // recovery an unparsable snapshot gets, for the same reason.
            Err(e) => log(&format!("WARN trie.json - {}", error::chain(&e))),
        },
        None => log("trie.json - skipped, storage is memory"),
    }

    let stores = &server.stores;
    // Rows still buffered are rows that never reached a file, so they do not belong in a
    // count of what was written. The line used to be `stores.rows()` whole, which after a
    // failed final flush claimed 90,000 written where 85,000 were - the shutdown log
    // contradicting the line above it.
    let held = stores.rows();
    let unwritten = stores.buffered();
    let dropped = stores.rows_dropped();
    let kept = if server.storage.is_durable() {
        "written"
    } else {
        "discarded, they were only in memory"
    };
    log(&format!(
        "close stores - {} store(s), {} row(s) {kept}{}",
        commas(stores.open() as u64),
        commas(held.saturating_sub(unwritten)),
        match (server.storage.is_durable(), unwritten) {
            (true, 0) => String::new(),
            (true, n) => format!(", {} row(s) still buffered and lost", commas(n)),
            (false, _) => String::new(),
        },
    ));

    log("close WAL - skipped, there is no WAL yet");

    // **The exit code is the last honest thing a process says.** It used to be 0 whatever
    // happened, including when leaves failed to flush and their rows went with the
    // process: 20,000 lines accepted and 10,625 back after a restart, exit 0. An
    // orchestrator reading that code has no other way to know.
    //
    // Rows the store refused while running count too, though each was refused to its
    // sender: the process did not store what it was sent, and a clean exit would say it
    // had. When both happened the final flush is the error, and the line names both.
    let refused = match dropped {
        0 => String::new(),
        n => format!(
            "{} row(s) the store refused while running were never stored",
            commas(n)
        ),
    };
    match lost {
        None if dropped == 0 => {
            log("exit 0");
            Ok(())
        }
        None => {
            log(&format!("exit 1 - {refused}"));
            Err(ServeError::RowsDropped { rows: dropped })
        }
        Some((leaves, rows)) => {
            log(&format!(
                "exit 1 - the final flush failed for {leaves} leaf/leaves, costing {} row(s){}",
                commas(rows),
                if refused.is_empty() {
                    String::new()
                } else {
                    format!("; and {refused}")
                }
            ));
            Err(ServeError::RowsLost { leaves, rows })
        }
    }
}

/// The startup banner.
///
/// stderr, and not stdout, so that `triedb serve >/dev/null` still prints the paragraph
/// about losing data. A warning a redirect can silence is a warning that will be silenced
/// by the first person who pipes the process into a log file, and this is something to
/// say rather than hide.
fn banner(addr: SocketAddr, server: &Server) {
    let (limits, api) = (server.limits, server.api);
    let lines = [
        String::new(),
        "triedb serve".to_string(),
        String::new(),
        format!("  listening   http://{addr}"),
        String::new(),
        // Two paragraphs, one per storage mode, and the memory one is unchanged from when
        // it was the only one. A warning that softens once a feature lands beside it is a
        // warning nobody reads the second time.
        match (&server.storage.base_path, &server.storage.trie_file) {
            (Some(base), trie_file) => format!(
                "  storage     {}\n\
                 \x20             trie      {}\n\
                 \x20             segments  a row is queryable the moment it is ingested; it\n\
                 \x20                       reaches disk when its row group closes - 8,192\n\
                 \x20                       rows, 8MB, or {}ms, whichever comes first. The\n\
                 \x20                       thresholds are checked every {}ms. At most {} day\n\
                 \x20                       files are open for writing at once; past that a\n\
                 \x20                       row for a day with none open goes into today's,\n\
                 \x20                       as a row group of its own day. Keep it under\n\
                 \x20                       `ulimit -n`.\n\
                 \x20                       SIGTERM closes and seals everything before exiting.\n\
                 \x20             catalog   {}/catalog.log - a template is written here, and\n\
                 \x20                       fsynced, before the line that made it counts as\n\
                 \x20                       ingested; trie.json is a readable snapshot nothing\n\
                 \x20                       loads while the catalog exists.\n\
                 \x20             NO WRITE-AHEAD LOG YET. A clean exit loses nothing. A\n\
                 \x20             kill -9 or a power cut loses up to one row group of rows -\n\
                 \x20             at most {}ms of them. Templates survive either.",
                base.display(),
                trie_file
                    .as_ref()
                    .map_or_else(|| "none".to_string(), |p| p.display().to_string()),
                server.storage.row_group_timeout.as_millis(),
                server.storage.flush_interval.as_millis(),
                commas(server.storage.max_open_segments as u64),
                base.display(),
                server.storage.row_group_timeout.as_millis(),
            ),
            (None, _) => {
                "  STORAGE IS IN MEMORY. Everything ingested is lost when this process exits.\n\
                 \x20 Nothing is written to disk: there is no write-ahead log and no snapshot of\n\
                 \x20 the trie. A restart starts an empty database. Do not point anything at this\n\
                 \x20 that you would be unhappy to lose."
                    .to_string()
            }
        },
        String::new(),
        "  endpoints   GET  /            the log viewer, in a browser".to_string(),
        "              POST /ingest      NDJSON body, one log line per line; gzip ok".to_string(),
        "              POST /query       {\"q\": \"<trieql>\"}".to_string(),
        "              POST /load        a bulk load job: what `triedb load` sends".to_string(),
        "              GET  /templates   every template, its columns and row count".to_string(),
        "              GET  /stats       throughput, template count, storage size".to_string(),
        "              GET  /health      {\"ok\":true}".to_string(),
        String::new(),
        match limits.max_rows {
            0 => "  max rows    none - a MATCH with no LIMIT materialises every matching row"
                .to_string(),
            n => format!(
                "  max rows    {n} - a MATCH with no LIMIT is answered a page at a time,\n\
                 \x20             truncated with a next_cursor to continue from"
            ),
        },
        match (limits.max_buckets, limits.max_groups) {
            (0, 0) => "  max buckets none - a GROUP BY time(...) emits whatever the range holds"
                .to_string(),
            (buckets, groups) => format!(
                "  max buckets {buckets} buckets, {groups} groups - a wider GROUP BY time(...)\n\
                 \x20             is refused rather than drawn with its tail missing"
            ),
        },
        format!(
            "  ingest      {} byte body, {} byte decompressed, {} at once\n\
             \x20             a body that waits longer than {}ms for a slot is answered\n\
             \x20             503 with a Retry-After rather than left to time out",
            commas(api.max_body_bytes as u64),
            commas(api.max_decompressed_bytes as u64),
            api.ingest_concurrency,
            INGEST_QUEUE_WAIT.as_millis(),
        ),
        // On every start: the cost of learning it later is a fleet of shipper configs that
        // all have to change at once. `post_ingest` has the long form.
        "  tenancy     single tenant - /ingest takes no stream or tenant identifier,\n\
         \x20             and adding one later changes every shipper config in the\n\
         \x20             fleet. Deliberate; revisit with the deployment work."
            .to_string(),
        String::new(),
        "  shutdown    SIGINT (Ctrl-C) or SIGTERM. In-flight requests finish first.".to_string(),
        String::new(),
    ];
    eprintln!("{}", lines.join("\n"));
}

/// One lifecycle line. stderr for the reason the banner is there - these are logs, and
/// this surface's data goes out over HTTP.
fn log(message: &str) {
    eprintln!("triedb: {message}");
}
