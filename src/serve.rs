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
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
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
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use std::path::Path;

use crate::catalog::Catalog;
use crate::config::{ApiLimits, Limits, Storage};
use crate::db;
use crate::engine;
use crate::error::{self, CliError, DetectorError, ServeError};
use crate::fmt::{commas, rfc3339_at};
use crate::query;
use crate::store::{FlushKind, StoreManager};
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
/// `Trie` is not behind a lock: it synchronises itself per node, so an ingest and a walk
/// can proceed together. The store registry is, because `append` and `flush` need `&mut`
/// and `engine::execute` needs `&`.
///
/// The write lock is taken **once per body**, not once per line. That makes a body atomic
/// to anything reading rows - a client that ingests 500 lines and then queries cannot
/// observe 200 of them - and it costs a long body blocking readers for its duration.
/// Worth it while a body is bounded by `MAX_INGEST_BYTES` and the alternative is a
/// half-applied batch, which is the harder thing for an operator to reason about.
pub struct Server {
    trie: Trie,
    stores: RwLock<StoreManager>,
    started: Instant,
    started_at: DateTime<Utc>,
    lines_read: AtomicU64,
    lines_ingested: AtomicU64,
    lines_rejected: AtomicU64,
    queries: AtomicU64,
    /// Requests currently in a handler. Read at shutdown, to say what is being waited on.
    in_flight: AtomicU64,
    /// Bodies refused for want of a permit. In no other counter - `lines_read` never saw
    /// them - and the only number that says the shed is firing rather than merely present.
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
}

impl Server {
    /// Build the state, restoring what is on disk.
    ///
    /// The trie comes back first because it is what says which leaves exist; every one of
    /// them is then opened, so a template that existed before the restart is a template
    /// that exists after it even when its directory holds no rows. A leaf opened and empty
    /// is the "known template, nothing in it" answer, which is a different thing from "no
    /// such template" and has to stay different across a restart.
    fn new(
        now: DateTime<Utc>,
        limits: Limits,
        api: ApiLimits,
        storage: Storage,
    ) -> Result<Self, ServeError> {
        let (trie, stores) = match storage.base_path.clone() {
            Some(base) => {
                let data_dir = storage.data_dir();
                let trie = open_catalog(&base, storage.trie_file.as_deref(), &data_dir)?;

                // Applied whatever the snapshot said, including when it parsed cleanly:
                // the hazard is a trie that does not know about a directory, and a stale
                // or hand-edited snapshot that parses is as unaware as one that did not.
                // See `Trie::reserve_template_ids_through` for what reissuing an id costs.
                let claimed = crate::store::disk::claimed_template_ids(&base);
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
                        "WARN storage - {} leaf director(ies) on disk that no template \
                         refers to; those rows are not queryable, and new templates take \
                         fresh ids rather than reusing them",
                        commas(orphans as u64)
                    ));
                }
                let mut stores =
                    StoreManager::disk(base, storage.row_group_limits()).map_err(|e| match e {
                        // The old layout is not a disk that failed; it is a disk holding
                        // something this build must not read. Named and fatal.
                        crate::store::StoreError::LegacyLayout { path } => {
                            ServeError::LegacyLayout { path }
                        }
                        e => ServeError::Storage {
                            detail: error::chain(&e),
                        },
                    })?;
                for leaf in trie.all_leaves() {
                    let id = leaf
                        .leaf_meta
                        .read()
                        .ok()
                        .and_then(|slot| slot.as_ref().map(|meta| meta.template_id));
                    if let Some(id) = id {
                        if let Err(e) = stores.open_existing(id) {
                            // One unreadable leaf directory is one template answering
                            // empty, not a server that will not start.
                            log(&format!("WARN leaf {id} - {}", error::chain(&e)));
                        }
                    }
                }
                (trie, stores)
            }
            None => (Trie::new(), StoreManager::memory()),
        };

        Ok(Self {
            trie,
            stores: RwLock::new(stores),
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
        })
    }

    /// Take the stores lock, recovering from a poisoning rather than propagating it.
    ///
    /// A poisoned lock means some handler panicked while holding it. `Stores::append` is
    /// a map insert and a counter bump, so there is no torn state to inherit, and
    /// refusing every later request would turn one bad line into a dead server. This is
    /// `PoisonError::into_inner`, not `.unwrap()`: nothing here can end the
    /// process.
    /// **The invariant this lock carries, and it is not the obvious one.**
    ///
    /// A query reads a leaf's segments *and* its unwritten buffer - a row group closes on
    /// size now, so a quiet leaf's rows would otherwise be invisible for up to the row
    /// group timeout. Double counting is impossible by construction rather than by
    /// deduplicating: **a row leaves the buffer only after the row group holding it is
    /// fsynced, and that swap happens under this same lock.** Write the rows, release the
    /// lock, then drain, and a row is briefly in both - a count occasionally too high,
    /// which nothing reports and nobody reproduces.
    fn read_stores(&self) -> RwLockReadGuard<'_, StoreManager> {
        self.stores.read().unwrap_or_else(|e| e.into_inner())
    }

    /// The other side of the invariant at `read_stores`: the flush that moves rows out of
    /// a buffer and into a row group holds this for the whole of both steps.
    fn write_stores(&self) -> RwLockWriteGuard<'_, StoreManager> {
        self.stores.write().unwrap_or_else(|e| e.into_inner())
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
fn open_catalog(base: &Path, trie_file: Option<&Path>, data_dir: &str) -> Result<Trie, ServeError> {
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

    let trie = Trie::from_catalog(data_dir, &records, &mut warn).map_err(|e| storage_error(&e))?;
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
}

/// What one `/ingest` body did.
struct IngestReport {
    ingested: u64,
    errors: Vec<LineError>,
    /// Rejections past `MAX_REPORTED_ERRORS`, counted but not described.
    omitted: u64,
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
    let mut errors = Vec::new();
    let mut stores = server.write_stores();

    let stats = db::read_lines(
        Cursor::new(body),
        &mut |parsed| db::ingest_line(&server.trie, Some(&mut stores), &parsed, now),
        &mut |e: DetectorError| {
            // Past the cap the rejection is counted and dropped. `stats.rejected` is
            // still the true total, so nothing is lost but the text.
            if errors.len() < MAX_REPORTED_ERRORS {
                errors.push(LineError {
                    line: e.line,
                    msg: e.kind.to_string(),
                });
            }
        },
    )?;
    drop(stores);

    server.lines_read.fetch_add(stats.read, Ordering::Relaxed);
    server
        .lines_ingested
        .fetch_add(stats.ingested, Ordering::Relaxed);
    server
        .lines_rejected
        .fetch_add(stats.rejected, Ordering::Relaxed);

    Ok(IngestReport {
        ingested: stats.ingested,
        omitted: stats.rejected - errors.len() as u64,
        errors,
    })
}

// ── Handlers ───────────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct QueryRequest {
    q: String,
}

/// `{"ok":true}`. K8s liveness and Vector's healthcheck, so it takes no lock and reads no
/// data - a health check that blocks behind a long ingest reports the wrong thing.
async fn health() -> Json<Value> {
    Json(json!({ "ok": true }))
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
/// `/query`, `/templates`, `/stats` and the viewer. Both are M9-sized, and a header
/// accepted but ignored would be worse than neither: it reads as isolation that is not
/// there. Revisit at M9, with the deployment work.
///
/// `body` is a `Result`, not a `Bytes`, for the reason `post_query` decodes its own body:
/// axum's rejection is plain text, and a client that parses one error format should not
/// need a second.
async fn post_ingest(
    State(server): State<Arc<Server>>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Response {
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
    // The parse and the trie walk are CPU work holding a `std` lock, so they do not belong
    // on a runtime thread that also answers `/health`. Inflating joins them rather than
    // running above: a gzip bomb is seconds of CPU, and spending it on a runtime thread
    // would stall every other endpoint while the server was being attacked.
    let done = tokio::task::spawn_blocking(move || {
        let decoded = decode(&body, encoding.as_deref(), held.api.max_decompressed_bytes)
            .map_err(IngestFailure::Body)?;
        ingest(&held, &decoded, Utc::now()).map_err(IngestFailure::Fatal)
    })
    .await;

    match done {
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

/// Admission control for `/ingest`: a permit, or a 503 that says when to come back.
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
        let stores = held.read_stores();
        let result = engine::execute(&held.trie, &stores, &query)?;
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
    let stores = server.read_stores();
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
    let stores = server.read_stores();
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
                "day partitions of segments on disk; a row group closes at 8,192 rows, 8MB or the row group timeout, and there is no write-ahead log yet - so an unclean exit loses up to one row group"
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
            "rows": stores.rows(),
            "buffered_rows": stores.buffered(),
            "write_failures": stores.write_failures(),
            "last_write_error": stores.last_write_error(),
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
) -> Result<(), CliError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(ServeError::Runtime)?;
    runtime.block_on(serve(host, port, limits, api, storage))?;
    Ok(())
}

async fn serve(
    host: &str,
    port: u16,
    limits: Limits,
    api: ApiLimits,
    storage: Storage,
) -> Result<(), ServeError> {
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

    let server = Arc::new(Server::new(Utc::now(), limits, api, storage)?);
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
            // step so an operator can see which ones this milestone actually has.
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
    // The flush task is joined before the wind-down and not after: the final flush and the
    // trie snapshot both take the same lock, and a tick arriving between them would write
    // rows the snapshot does not describe.
    let _ = flusher.await;
    shutdown_sequence(&server);
    Ok(())
}

/// Ask every store whether a row group is due, every interval, until the token is
/// cancelled.
///
/// **The tick no longer writes on its own account.** A row group closes at 8,192 rows,
/// 8MB or the row group timeout, and the tick is what notices - so most ticks on a quiet
/// server do nothing, and on a busy one the first tick after the 8,192nd row writes them
/// all in a single fsync.
///
/// Two details that are not decoration:
///
/// - **`spawn_blocking`.** The guard is a `std::sync::RwLock` and the work under it is
///   file I/O. On a runtime worker that blocks the whole executor, including `/health`,
///   which is the endpoint an orchestrator reads to decide whether to kill the process.
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
    loop {
        tokio::select! {
            () = token.cancelled() => return,
            _ = tick.tick() => {}
        }
        let held = Arc::clone(&server);
        let flushed =
            tokio::task::spawn_blocking(move || held.write_stores().flush_all(FlushKind::Tick));
        match flushed.await {
            Ok(summary) if summary.failed > 0 => log(&format!(
                "WARN flush - {} leaf/leaves could not be written; their rows stay buffered \
                 and the next tick retries",
                summary.failed
            )),
            Ok(_) => {}
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

/// The fixed shutdown order, with the steps this milestone does not have named as skipped.
///
/// ```text
/// stop accepting → final flush → WAL checkpoint → trie.json → close stores → close WAL
/// ```
///
/// Printing the skipped steps rather than omitting them is the point: an operator watching
/// a clean shutdown scroll past should see exactly which durability they have and which
/// they do not. Two of the six are real; the WAL's two are not yet.
fn shutdown_sequence(server: &Server) {
    log("in-flight requests complete");

    if server.storage.is_durable() {
        let summary = server.write_stores().flush_all(FlushKind::Final);
        log(&format!(
            "final flush - {} row(s) into {} partition(s), {} byte(s){}",
            commas(summary.rows),
            summary.partitions,
            commas(summary.bytes),
            match summary.failed {
                0 => String::new(),
                n => format!(", {n} leaf/leaves FAILED and their rows are lost"),
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

    let stores = server.read_stores();
    let kept = if server.storage.is_durable() {
        "written"
    } else {
        "discarded, they were only in memory"
    };
    log(&format!(
        "close stores - {} store(s), {} row(s) {kept}",
        commas(stores.open() as u64),
        commas(stores.rows()),
    ));
    drop(stores);

    log("close WAL - skipped, there is no WAL yet");
    log("exit 0");
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
                 \x20                       thresholds are checked every {}ms. SIGTERM closes\n\
                 \x20                       and seals everything before exiting.\n\
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
         \x20             fleet. Deliberate; revisit at M9."
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
