//! `triedb serve` - the native HTTP surface, on :7700.
//!
//! One `Trie` and one set of `Stores`, held for the life of the process, wrapped in the
//! five endpoints below. The engine underneath is M2's, unchanged: `/query` builds
//! the same `Query` the CLI builds and calls the same `engine::execute`, and `/ingest`
//! goes through `db::ingest_line`, which is also what `db::load` calls. Nothing in this
//! module knows how a line becomes a row.
//!
//! **Storage is memory and only memory.** There is no WAL (M6) and nothing writes the
//! trie down (M5), so every byte ingested here dies with the process. The banner says so
//! in as many words, `/stats` repeats it as a field, and the shutdown log names each
//! persistence step it is skipping. It is the single most important thing about this
//! milestone and it is not something to find out afterwards.
//!
//! A future `Ingestor` would sit between the API and the trie. There is no such type here:
//! `ingest` is a plain function with one implementation, and a trait with one caller is a
//! guess at what the second one will need. The WAL, the detector cache and the
//! flush timer are the arrows that will make it a type, and none of them exists yet.

use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Instant;

use axum::body::Bytes;
use axum::extract::rejection::BytesRejection;
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use crate::db;
use crate::engine;
use crate::error::{self, CliError, DetectorError, ServeError};
use crate::fmt::{commas, rfc3339_at};
use crate::query;
use crate::store::Stores;
use crate::trie::Trie;

/// The default port this server listens on.
pub const DEFAULT_PORT: u16 = 7700;

/// The largest body `/ingest` will read, ~64 MiB.
///
/// axum defaults to 2 MiB, which is a sensible default for a form and the wrong one for a
/// log shipper: Vector's default batch is larger than that, and a 413 on a routine batch
/// would look like TrieDB rejecting valid logs. Not unlimited, because the body is
/// buffered whole and this process has nowhere to spill it.
const MAX_INGEST_BYTES: usize = 64 * 1024 * 1024;

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

// ── State ──────────────────────────────────────────────────────────────────────────

/// Everything the handlers share.
///
/// `Trie` is not behind a lock: it synchronises itself per node, so an ingest and a walk
/// can proceed together. `Stores` is, because `append` needs `&mut` and `engine::execute`
/// needs `&`.
///
/// The write lock is taken **once per body**, not once per line. That makes a body atomic
/// to anything reading rows - a client that ingests 500 lines and then queries cannot
/// observe 200 of them - and it costs a long body blocking readers for its duration.
/// Worth it while a body is bounded by `MAX_INGEST_BYTES` and the alternative is a
/// half-applied batch, which is the harder thing for an operator to reason about.
pub struct Server {
    trie: Trie,
    stores: RwLock<Stores>,
    started: Instant,
    started_at: DateTime<Utc>,
    lines_read: AtomicU64,
    lines_ingested: AtomicU64,
    lines_rejected: AtomicU64,
    queries: AtomicU64,
    /// Requests currently in a handler. Read at shutdown, to say what is being waited on.
    in_flight: AtomicU64,
}

impl Server {
    fn new(now: DateTime<Utc>) -> Self {
        Self {
            trie: Trie::new(),
            stores: RwLock::new(Stores::new()),
            started: Instant::now(),
            started_at: now,
            lines_read: AtomicU64::new(0),
            lines_ingested: AtomicU64::new(0),
            lines_rejected: AtomicU64::new(0),
            queries: AtomicU64::new(0),
            in_flight: AtomicU64::new(0),
        }
    }

    /// Take the stores lock, recovering from a poisoning rather than propagating it.
    ///
    /// A poisoned lock means some handler panicked while holding it. `Stores::append` is
    /// a map insert and a counter bump, so there is no torn state to inherit, and
    /// refusing every later request would turn one bad line into a dead server. This is
    /// `PoisonError::into_inner`, not `.unwrap()`: nothing here can end the
    /// process.
    fn read_stores(&self) -> RwLockReadGuard<'_, Stores> {
        self.stores.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write_stores(&self) -> RwLockWriteGuard<'_, Stores> {
        self.stores.write().unwrap_or_else(|e| e.into_inner())
    }
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

/// `body` is a `Result`, not a `Bytes`, for the reason `post_query` decodes its own body:
/// axum's own rejection is plain text, and a client that parses one error format should
/// not need a second. A batch over `MAX_INGEST_BYTES` is the one response on this surface
/// that used to arrive as a bare sentence, which is also the response a log shipper is
/// most likely to meet in production.
async fn post_ingest(
    State(server): State<Arc<Server>>,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let body = match body {
        Ok(body) => body,
        // `status` rather than a hardcoded 413: the rejection also covers a body that
        // failed mid-read, and telling a shipper it sent too much when the connection
        // dropped would send it away to split a batch that was never too big.
        Err(rejection) => return fail(rejection.status(), &rejection.body_text()),
    };
    let held = Arc::clone(&server);
    // The parse and the trie walk are CPU work holding a `std` lock, so they do not
    // belong on a runtime thread that also has to answer `/health`.
    let done = tokio::task::spawn_blocking(move || ingest(&held, &body, Utc::now())).await;
    match done {
        // 200 even with errors in it. The body was accepted and part of it was stored;
        // a 4xx would tell a shipper to retry the whole batch and duplicate the 497
        // lines that landed, and there is nothing the client could have sent differently
        // for the other 3 beyond not sending them.
        Ok(Ok(report)) => (StatusCode::OK, Json(report.to_json())).into_response(),
        Ok(Err(e)) => fail(StatusCode::INTERNAL_SERVER_ERROR, &error::chain(&e)),
        Err(join) => fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("ingest task failed: {join}"),
        ),
    }
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
    let query = match query::from_trieql(&request.q, Utc::now()) {
        Ok(query) => query,
        Err(e) => return fail(StatusCode::BAD_REQUEST, &error::chain(&e)),
    };

    let held = Arc::clone(&server);
    let done = tokio::task::spawn_blocking(move || {
        let stores = held.read_stores();
        let result = engine::execute(&held.trie, &stores, &query)?;
        held.queries.fetch_add(1, Ordering::Relaxed);
        Ok::<Value, crate::store::StoreError>(query::result_json(&result))
    })
    .await;

    match done {
        Ok(Ok(body)) => (StatusCode::OK, Json(body)).into_response(),
        Ok(Err(e)) => fail(StatusCode::INTERNAL_SERVER_ERROR, &error::chain(&e)),
        Err(join) => fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("query task failed: {join}"),
        ),
    }
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
/// `storage.durable` is `false` and `storage.note` says what that costs. The banner is
/// seen once by whoever started the process; this is the field a dashboard can read, and
/// M5 is where it starts being able to say something else.
async fn get_stats(State(server): State<Arc<Server>>) -> Response {
    let stores = server.read_stores();
    let uptime = server.started.elapsed().as_secs_f64();
    let ingested = server.lines_ingested.load(Ordering::Relaxed);
    let queries = server.queries.load(Ordering::Relaxed);

    Json(json!({
        "uptime_seconds": uptime,
        "started_at": rfc3339_at(server.started_at),
        "storage": {
            "kind": "memory",
            "durable": false,
            "note": "in-memory only - every row is lost when this process exits",
            "stores_open": stores.open(),
            "rows": stores.rows(),
            // Payload bytes: key text, string values, and a scalar's fixed width. The
            // resident size is larger - allocator slack and the hash tables are not in
            // it - so the name says estimated and the number is a floor.
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
        .route("/health", get(health))
        .route("/stats", get(get_stats))
        .route("/templates", get(get_templates))
        .route("/query", post(post_query))
        .route(
            "/ingest",
            post(post_ingest).layer(DefaultBodyLimit::max(MAX_INGEST_BYTES)),
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
pub fn run(host: &str, port: u16) -> Result<(), CliError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(ServeError::Runtime)?;
    runtime.block_on(serve(host, port))?;
    Ok(())
}

async fn serve(host: &str, port: u16) -> Result<(), ServeError> {
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

    let server = Arc::new(Server::new(Utc::now()));
    let token = CancellationToken::new();
    // Installed before the banner: a server that cannot hear SIGTERM will be killed by
    // the SIGKILL that follows it, and a deploy is a bad place to discover that.
    let signals = signals(token.clone())?;

    banner(bound);
    tokio::spawn(signals);

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
    shutdown_sequence(&server);
    Ok(())
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
/// Printing the skipped steps rather than omitting them is the point. Four of the six are
/// the difference between this milestone and a database that survives being restarted,
/// and an operator watching a clean shutdown scroll past should be able to see exactly
/// which durability they do not have yet.
fn shutdown_sequence(server: &Server) {
    log("in-flight requests complete");
    log("final flush - skipped, storage is memory (M5 puts the stores on disk)");
    log("WAL checkpoint - skipped, there is no WAL yet (M6)");
    log("trie.json - skipped, nothing persists the trie yet (M5)");

    let stores = server.read_stores();
    log(&format!(
        "close stores - {} store(s), {} row(s) discarded, they were only in memory",
        commas(stores.open() as u64),
        commas(stores.rows()),
    ));
    drop(stores);

    log("close WAL - skipped, there is no WAL yet (M6)");
    log("exit 0");
}

/// The startup banner.
///
/// stderr, and not stdout, so that `triedb serve >/dev/null` still prints the paragraph
/// about losing data. A warning a redirect can silence is a warning that will be silenced
/// by the first person who pipes the process into a log file, and this is something to
/// say rather than hide.
fn banner(addr: SocketAddr) {
    let lines = [
        String::new(),
        "triedb serve".to_string(),
        String::new(),
        format!("  listening   http://{addr}"),
        String::new(),
        "  STORAGE IS IN MEMORY. Everything ingested is lost when this process exits.".to_string(),
        "  Nothing is written to disk: there is no write-ahead log and no snapshot of".to_string(),
        "  the trie. A restart starts an empty database. Do not point anything at this".to_string(),
        "  that you would be unhappy to lose.".to_string(),
        String::new(),
        "  endpoints   POST /ingest      NDJSON body, one log line per line".to_string(),
        "              POST /query       {\"q\": \"<trieql>\"}".to_string(),
        "              GET  /templates   every template, its columns and row count".to_string(),
        "              GET  /stats       throughput, template count, storage size".to_string(),
        "              GET  /health      {\"ok\":true}".to_string(),
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
