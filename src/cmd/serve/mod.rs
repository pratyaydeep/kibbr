//! `kibbr serve` - the native HTTP surface, on :7700.
//!
//! One `Trie` and one `StoreManager`, held for the life of the process, wrapped in the
//! five endpoints below. Nothing here knows how a line becomes a row: `/query` builds the
//! same `Query` the CLI builds and calls the same `engine::execute`, and `/ingest` parses
//! and routes a line by the functions `db::load` does (`db::parse_and_prepare`,
//! `db::place_in`) - with the write-ahead log between the routing and the store.
//!
//! **What survives, and what does not.** With a data directory, a line is in the
//! write-ahead log, flushed, before `/ingest` acknowledges it; its row reaches a day's file
//! when its row group closes, and until then a restart puts it back from the log, once -
//! after `SIGTERM` and after `kill -9` alike. With `--storage memory` nothing is written at
//! all. The banner says which of the two this process is, `/stats` repeats it as a field,
//! and the shutdown log names every step including the ones a memory server skips.
//!
//! **The flush clock lives here and not in the store.** A `LeafStore` has a buffer and a
//! synchronous `flush()` and no timer, so it can be driven from a test with no runtime;
//! the interval that calls it watches the same cancellation token as the listener, because
//! deciding *when* to write is a fact about the process lifecycle.
//!
//! A future `Ingestor` would sit between the API and the trie. There is no such type here:
//! `ingest` is a plain function with one implementation, and a trait with one caller is a
//! guess at what the second one will need. The write-ahead log is beside it (`wal`); the
//! detector cache is the arrow that would make it a type, and does not exist yet.

use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap};
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

use crate::cmd::compact;
use crate::config::{ApiLimits, Limits, Schema, Storage};
use crate::error::{self, CliError, DetectorError, DetectorErrorKind, IngestError, ServeError};
use crate::cmd::load;
use crate::cmd::query;
use kibbr_core::fmt::{commas, rfc3339_at};
use kibbr_engine::engine;
use kibbr_storage::catalog::{Catalog, CatalogError};
use kibbr_storage::db;
use kibbr_storage::store::disk::CompactOptions;
use kibbr_storage::store::fan::Fan;
use kibbr_storage::store::{FlushKind, Stop, StoreError, StoreManager};
use kibbr_storage::trie::{Trie, TrieError};
use kibbr_storage::wal::{self, Wal};

/// The default port this server listens on.
pub const DEFAULT_PORT: u16 = 7700;

/// How long, once the shutdown deadline has cut the requests in flight off, their
/// connections are given to take the 503 each was answered with.
const CUT_OFF_GRACE: Duration = Duration::from_secs(1);

/// How many `/ingest` bodies may be between the door and their answer at once: being
/// parsed, waiting for room in the write-ahead log, or waiting on its flush.
///
/// Each is a thread of the blocking pool, parked while it waits, and the wait is as long
/// as the log's disk makes it. The permits for the parse do not bound it - they are kept
/// while a body waits for room and go back once it is in the log's queue, before its
/// flush, so that one flush can carry more bodies than there are cores - and with
/// nothing bounding it a log volume that stalled took every thread of the pool: queries
/// and the flush tick queued behind bodies that could not be answered, and the server
/// went quiet, which is the one thing the door must never do. Past this many the door
/// sheds, before it reads the body.
const INGEST_BODIES: usize = 256;

/// What the blocking pool holds beyond the bodies at the door: for queries and the flush
/// tick, which must not queue behind bodies a stalled log cannot answer.
const OTHER_BLOCKING_THREADS: usize = 256;

/// The bodies that may be between the door and their answer on this server:
/// `INGEST_BODIES`, or twice the permits for the parse where an operator has asked for
/// more of those than half of it. Always more places than permits, so that running out of
/// places means what the refusal says - the bodies inside are waiting on the log, not
/// being parsed.
fn ingest_bodies(api: &ApiLimits) -> usize {
    INGEST_BODIES.max(api.ingest_concurrency.saturating_mul(2))
}

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
/// **Nothing here is behind a lock of the server's.** `Trie` synchronises itself: each node
/// has its own locks, and the one lock for the whole trie is its key index's, which a plan
/// reads while it chooses its templates and only a new template or a changed column writes,
/// for one `put` - so an ingest and a query proceed together; `StoreManager` locks each leaf
/// on its own, and a flush holds no lock an append or a query waits on while it writes.
/// There used to be one `RwLock` over the whole registry, taken for writing by every ingest
/// body and by the flush tick across every fsync, and for reading by every query for the
/// whole of its run: with queries running, ingest fell from 44,800 lines a second to 1,645,
/// and a count by a promoted value went from 33ms to 1.1s.
///
/// **What that gave up: a body is no longer atomic to a query running beside it.** Each
/// row is visible from the moment its leaf takes it, so a query that runs while a body is
/// applied can count part of it. A client that ingests and then queries still sees all of
/// what it sent - the response goes out after the last line is applied - and no query ever
/// sees a row twice or loses one it could see before.
pub struct Server {
    trie: Trie,
    /// Shared with the write-ahead log's applier, which is the one thread that appends a
    /// row behind a log; everything else here reads it, flushes it, or loads into it.
    stores: Arc<StoreManager>,
    /// The write-ahead log. `None` with `--storage memory`: nothing is durable there, and
    /// a line is applied by the body that carried it.
    wal: Option<Wal>,
    /// Rows the store would not take from the log, and what that holds back.
    held: Arc<Held>,
    /// What the start read back from the log: `/stats`'s `storage.wal.replay`.
    replayed: Replayed,
    /// Whether the last move of the checkpoint failed: said once when it starts.
    checkpoint_failing: AtomicBool,
    /// Wakes the flush before its next tick: an ingest body that left the buffers past
    /// their memory limit rings it, so the limit is enforced within a body and not within
    /// half a second of bodies.
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
    /// What the door has made wait, and the bodies shed, each under its cause: refused
    /// for want of a permit or a place, because the buffers stayed past twice their
    /// limit, because the write-ahead log had no room, with the line at the door full,
    /// or while the server was stopping. Their sum is the only number that says the shed
    /// is firing rather than merely present. A body shed at the door is in no other
    /// counter - `lines_read` never saw it; one the log sheds was read and routed first,
    /// and its lines are in `lines_read` and `lines_rejected` too.
    door: Door,
    /// The caps, held on the server and not negotiated per request: all clients get the
    /// same limits, because a client that could override one could sidestep the
    /// protection it is there for.
    limits: Limits,
    api: ApiLimits,
    storage: Storage,
    /// How many bodies may be in the parse-and-walk at once.
    ///
    /// The backpressure answer for the part that is CPU. Without it every body takes a
    /// blocking-pool slot, the pool queues 512 deep, and a shipper waiting on a request
    /// that will not finish in time gives up and opens another - which is how a slow
    /// server becomes an unreachable one. No permit within `api.ingest_wait` is a 503,
    /// which Vector's adaptive concurrency reads as "send fewer" rather than "the server
    /// is gone".
    ///
    /// **Given back once the body is in the write-ahead log's queue, and not kept across
    /// the flush**, so one flush carries as many bodies as have queued and not as many as
    /// there are cores. A body waiting on the log still holds a blocking-pool thread;
    /// what bounds those is `ingest_bodies`. Kept while a body waits for room in a log
    /// that is full: see `ingest`.
    ingest_permits: Arc<Semaphore>,
    /// How many bodies may be between the door and their answer: `ingest_bodies`. Taken
    /// at the door with the permit above, and kept until the body's task has returned.
    ingest_bodies: Arc<Semaphore>,
    /// Whether the last flush tick could write. Shared with the registry and read without
    /// its lock, because `/health` must not queue behind a flush.
    write_health: Arc<kibbr_storage::store::WriteHealth>,
    /// The bulk loads this server has been asked to run: at most one at a time, on a
    /// thread of its own with its own pool, never on the runtime or the blocking pool the
    /// live door's bodies use.
    loads: std::sync::Mutex<Loads>,
    /// What `/load` and `/load/<job>` are asked with, as `LOCK` holds it. `None` without a
    /// data directory, where there is nothing to load into.
    load_token: Option<String>,
    /// The compactions this server has been asked to run or found due: at most one at a
    /// time, on a thread of its own as a load is, and in a slot apart from the load's - a
    /// load and a compaction run side by side. Their counters are `/stats`'s
    /// `storage.compaction`, read under this lock and no other.
    compactions: std::sync::Mutex<compact::Jobs>,
    /// Set once, when shutdown begins, and the one switch every compaction here is run
    /// with: whichever is running then stops before its record and removes what it wrote.
    compact_cancel: Arc<AtomicBool>,
    /// The disk store's scan threads, where it has any: for the door to say a body is
    /// in its task, which keeps a query from sharing its scan out meanwhile, and for
    /// `/stats` to say how many runs of row groups were shared out.
    fan: Option<Arc<Fan>>,
}

/// The door's own counts: what it has made wait and what it has turned away.
///
/// Every one is an atomic, so `/stats` adds them up with no lock and a body pays one
/// addition for the count it lands in.
#[derive(Default)]
struct Door {
    /// Bodies waiting at the door now, their bodies unread.
    waiting: AtomicU64,
    /// Bodies that have had to wait at the door, whatever came of the wait. With no
    /// wait to give, a body that found something missing is counted here all the same,
    /// answered at once and with no time against it.
    waits: AtomicU64,
    /// How long those waited, all together, in microseconds.
    waited_micros: AtomicU64,
    shed: Shed,
}

/// Bodies answered `503` and told to come back, by what there was none of. A sum of these
/// says a server is shedding; which of them is rising says what to do about it - more
/// permits, a faster disk under the log, a faster one under the store.
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

/// As `load_refused`, for `/compact`. A compaction reads no file its caller names, but it
/// is a day rewritten on every core it is given: taken, as a load is, only from an account
/// that can read the directory it rewrites.
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

/// Whether a request meant another data directory than the one this server serves.
/// `LOCK` can still name the last holder's address in the moment before a new holder
/// announces its own, and a job sent there would be run on whatever that address now
/// serves. `done` is what did not happen: "loaded", "compacted".
fn wrong_directory(server: &Server, asked: Option<&Path>, done: &str) -> Option<Response> {
    let (asked, ours) = (asked?, server.storage.base_path.as_ref()?);
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
    /// Build the state, restoring what is on disk (`open_durable`).
    fn new(
        now: DateTime<Utc>,
        limits: Limits,
        api: ApiLimits,
        storage: Storage,
        schema: Schema,
        load_token: Option<String>,
    ) -> Result<Self, ServeError> {
        let (trie, stores, wal, held, replayed) = match storage.base_path.as_deref() {
            Some(base) => {
                let restored = restore(&storage, &schema, true)?;
                let stores = Arc::new(restored.stores);
                let held = Arc::new(restored.held);
                // A directory with no log yet gets one, and a new stream with it.
                let wal = match restored.wal {
                    Some(wal) => wal,
                    None => Wal::create(base)?,
                };
                if let Some(disk) = stores.disk_store() {
                    disk.attach_wal(wal.stream(), wal.checkpoint());
                }
                // The one applier: every row the log has made durable, in the log's order.
                let apply: wal::Apply = {
                    let (stores, held) = (Arc::clone(&stores), Arc::clone(&held));
                    Box::new(move |rows| {
                        // Held across the batch: a replace-a-day waits for it, and
                        // retires its day with no row applied meanwhile.
                        let disk = stores.disk_store();
                        let _applying = disk.map(|disk| disk.applying());
                        // What a replace-a-day retires through: every position handed
                        // over, stored or refused - a refused row arrived before the
                        // replace too, and the replay that would store it must not put
                        // it into the day the replace made.
                        if let (Some(disk), Some((last, _))) = (disk, rows.last()) {
                            disk.note_applied(*last);
                        }
                        for (position, placed) in rows {
                            let template_id = placed.template_id;
                            // A panic in the store is a row it did not take, and no more
                            // than that: this thread is the one every body waits on.
                            let applied =
                                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    held.apply(&stores, position, placed)
                                }));
                            if applied.is_err() {
                                held.defer(template_id, position, "the store panicked");
                            }
                        }
                    })
                };
                wal.start(storage.wal_options(), apply)?;
                (restored.trie, stores, Some(wal), held, restored.replayed)
            }
            None => (
                Trie::new().with_time_columns(schema.time_columns.clone()),
                Arc::new(StoreManager::memory()),
                None,
                Arc::new(Held::new()),
                Replayed::default(),
            ),
        };

        let write_health = stores.health();
        let fan = stores.fan();
        Ok(Self {
            trie,
            stores,
            wal,
            held,
            replayed,
            checkpoint_failing: AtomicBool::new(false),
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
            storage,
            ingest_permits: Arc::new(Semaphore::new(api.ingest_concurrency)),
            ingest_bodies: Arc::new(Semaphore::new(ingest_bodies(&api))),
            write_health,
            loads: std::sync::Mutex::new(Loads::default()),
            load_token,
            compactions: std::sync::Mutex::new(compact::Jobs::default()),
            compact_cancel: Arc::new(AtomicBool::new(false)),
            fan,
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
/// **The write-ahead log is the server's to put back, and a command running alone leaves
/// it be.** A server's start stores every line the log holds that no row group does
/// (`replay`). A load or a compaction needs none of those rows: it reads where the log
/// ends - what a replace-a-day says it retired through - and which days it still holds
/// lines of, applies nothing, starts no writer and moves no checkpoint. The log's lines
/// are as the next server finds them; all it may remove is a segment a stop cut short as
/// it was made, which holds none.
///
/// The caller holds the directory's `LOCK`: recovery writes.
pub(crate) fn open_durable(
    storage: &Storage,
    schema: &Schema,
) -> Result<(Trie, StoreManager), ServeError> {
    let restored = restore(storage, schema, false)?;
    Ok((restored.trie, restored.stores))
}

/// A data directory, restored: see `open_durable`.
struct Restored {
    trie: Trie,
    stores: StoreManager,
    /// The log the directory holds, read back and not yet started; `None` if it has none.
    wal: Option<Wal>,
    held: Held,
    replayed: Replayed,
}

/// `apply`: whether the lines the log holds are stored - a server's start - or only
/// counted.
fn restore(storage: &Storage, schema: &Schema, apply: bool) -> Result<Restored, ServeError> {
    let Some(base) = storage.base_path.clone() else {
        return Err(ServeError::Storage {
            detail: "no data directory".to_string(),
        });
    };
    let data_dir = storage.data_dir();
    let trie = open_catalog(&base, storage.trie_file.as_deref(), &data_dir, schema)?;

    let stores = StoreManager::disk(base.clone(), storage.disk_options()).map_err(storage_fatal)?;

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
        log_warn(&format!(
            "storage - {} template id(s) with rows on disk that no template \
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
                log_warn(&format!("leaf {id} - {}", error::chain(&e)));
            }
        }
    }

    // Last, once every template has its leaf and every row group is in one: the log is
    // held against them.
    let held = Held::new();
    let wal = Wal::open(&base, &mut |m| log_warn(&format!("wal - {m}")))?;
    let replayed = match &wal {
        Some(wal) => replay(&trie, &stores, wal, &held, apply)?,
        None => Replayed::default(),
    };
    Ok(Restored {
        trie,
        stores,
        wal,
        held,
        replayed,
    })
}

/// What a start read back from the write-ahead log.
#[derive(Debug, Default, Clone, Copy)]
struct Replayed {
    /// Lines past the checkpoint: the ones looked at.
    records: u64,
    /// Put back in memory: no row group held them.
    applied: u64,
    /// Skipped: a durable row group's range covers their position.
    covered: u64,
    /// Left in the log: the store would not take them, or nothing says what they are.
    held: u64,
    /// Skipped: lines of a day a load has since replaced.
    replaced: u64,
    /// Lines at or behind the checkpoint, not looked at.
    behind: u64,
    /// Segments that ended in a record a stop cut short.
    torn: usize,
    corrupt: u64,
    seconds: f64,
}

/// Put back every line the write-ahead log holds that no row group does, and no other.
///
/// **A line is skipped only if a durable row group's range covers its position** - a row
/// group of the line's template and its UTC day, written from this log (`wal_ranges`).
/// Not by the checkpoint: that says where every line before it is, and says nothing of
/// the row groups that reached the disk since it last moved, which are most of them. A
/// replay that went by the checkpoint alone would store each of those rows again, and a
/// test that only asked whether anything acknowledged was lost would pass it.
///
/// **Routed by the record, not decided again.** The record names the template the line
/// went to, whose catalog record was durable before the log's, and beside it the time its
/// row was filed under. So the day a position is held against is the day the row *was*
/// stored on, whatever would parse the line today; a line a row group holds is skipped
/// without being parsed at all; and only a line that has to be put back is read again -
/// by the same parse, with the arrival time the record carries where the line has none
/// of its own.
///
/// **A line that would not go back where it was is not put anywhere.** If the row read
/// back now has another time than the record's - the epoch window was changed, or the
/// build's reading of a value was - filing it would put it on a day its position was
/// never held against. It is held instead, with its template, and said: start once under
/// the settings it was logged with.
///
/// A line that cannot be put back is never dropped: it stays in the log (`Held`), and the
/// checkpoint stays behind it. That is a line whose template the catalog does not know,
/// that no longer parses, that reads to another time, or whose day has a row group that
/// will not read - none of which a kill does - and a row the store refuses.
///
/// Rows put back are buffered as any others are. Past the memory limit the buffers are
/// flushed as the tick would flush them, so a long log does not have to fit in memory.
///
/// With `apply` false - a command running alone - nothing is put back: see
/// `open_durable`.
fn replay(
    trie: &Trie,
    stores: &StoreManager,
    wal: &Wal,
    held: &Held,
    apply: bool,
) -> Result<Replayed, ServeError> {
    let began = Instant::now();
    let Some(disk) = stores.disk_store() else {
        return Ok(Replayed::default());
    };
    let stream = wal.stream();
    disk.attach_wal(stream, wal.checkpoint());
    let mut warn = |m: String| log_warn(&format!("wal - {m}"));
    if !apply {
        // Read for where the log ends, for the template ids it names - none of which
        // this command may hand to a template of its own making - and for the day each
        // line is of: a day the log still holds lines for is not behind its checkpoint,
        // and the days this command finds due do not include it.
        let replaced = disk.replaced_days(stream);
        let mut highest_template = 0u64;
        let scanned = wal.replay(&mut warn, &mut |record| {
            highest_template = highest_template.max(record.template_id);
            let day = kibbr_storage::store::v3::day_of(record.time);
            if replaced
                .get(&day)
                .is_none_or(|through| record.seq > *through)
            {
                disk.note_position(day, record.seq);
            }
            Ok(())
        })?;
        trie.reserve_template_ids_through(highest_template);
        disk.note_applied(scanned.last_seq);
        if scanned.records > 0 {
            log(&format!(
                "wal - {} holds {} line(s) past its checkpoint; they are the server's to \
                 put back at its next start, and are left as they are",
                wal.dir().display(),
                commas(scanned.records),
            ));
        }
        return Ok(Replayed {
            records: scanned.records,
            behind: scanned.behind,
            torn: scanned.torn,
            corrupt: scanned.corrupt,
            ..Replayed::default()
        });
    }
    let time_columns: HashMap<u64, String> = trie
        .all_leaves()
        .iter()
        .filter_map(|leaf| {
            leaf.leaf_meta.read().ok().and_then(|slot| {
                slot.as_ref()
                    .map(|meta| (meta.template_id, meta.time_source.column().to_string()))
            })
        })
        .collect();

    let replaced = disk.replaced_days(stream);
    let mut out = Replayed::default();
    let mut ranges: HashMap<(u64, i64), Vec<(u64, u64)>> = HashMap::new();
    // The first of each kind of line that could not be put back, to say once.
    let mut unknown: Option<(u64, u64)> = None;
    let mut unreadable: Option<(u64, String)> = None;
    let mut moved: Option<(u64, i64, i64)> = None;
    let mut damaged: Option<(u64, String)> = None;
    let mut highest_template = 0u64;
    // When the buffers are next flushed for memory, and whether a flush has failed: one
    // that cannot write is not tried again for every line after it.
    let mut flush_at = 0u64;
    let mut flush_failed = false;
    let scanned = wal.replay(&mut warn, &mut |record| {
        highest_template = highest_template.max(record.template_id);
        let day = kibbr_storage::store::v3::day_of(record.time);
        // A day a load replaced: the lines the log held of it then were in the files the
        // replace retired, and no row group covers them any more.
        if replaced
            .get(&day)
            .is_some_and(|through| record.seq <= *through)
        {
            out.replaced += 1;
            return Ok(());
        }
        disk.note_position(day, record.seq);
        let covering = match ranges.entry((record.template_id, day)) {
            std::collections::hash_map::Entry::Occupied(slot) => slot.into_mut(),
            std::collections::hash_map::Entry::Vacant(slot) => {
                match disk.wal_ranges(record.template_id, day, stream) {
                    Ok(found) => slot.insert(found),
                    // A row group whose header will not read: whether it holds this line
                    // cannot be known, so the line is neither stored nor let go. It and
                    // its template's later lines stay in the log, and the start goes on -
                    // the damaged file is a query's to report.
                    Err(e) => {
                        damaged.get_or_insert_with(|| (record.seq, e.to_string()));
                        held.set_aside(record.template_id, record.seq);
                        out.held += 1;
                        // Remembered as nothing covered: the headers are not read again
                        // for every later line of the day, and those lines - their
                        // template set aside - are held as this one is.
                        slot.insert(Vec::new());
                        return Ok(());
                    }
                }
            }
        };
        // The ranges are in order and apart: the one that could hold the position is
        // the last that begins at or before it.
        let after = covering.partition_point(|(first, _)| *first <= record.seq);
        if after > 0 && covering[after - 1].1 >= record.seq {
            out.covered += 1;
            return Ok(());
        }

        // To be put back: only now is the line read.
        let Some(time_column) = time_columns.get(&record.template_id) else {
            unknown.get_or_insert((record.seq, record.template_id));
            held.hold(record.seq);
            out.held += 1;
            return Ok(());
        };
        let row = match db::row_from_log(record.line, time_column, record.received_at) {
            Ok(row) => row,
            Err(e) => {
                unreadable.get_or_insert_with(|| (record.seq, e.kind.to_string()));
                held.set_aside(record.template_id, record.seq);
                out.held += 1;
                return Ok(());
            }
        };
        let time = kibbr_storage::store::effective_ts(&row, time_column).unwrap_or(0);
        if time != record.time {
            moved.get_or_insert((record.seq, record.time, time));
            held.set_aside(record.template_id, record.seq);
            out.held += 1;
            return Ok(());
        }
        let placed = db::Placed {
            template_id: record.template_id,
            time_column: time_column.clone(),
            row,
        };
        if held.apply(stores, record.seq, placed) {
            out.applied += 1;
        } else {
            out.held += 1;
        }
        if !flush_failed && stores.over_buffer_limit() && stores.buffer_bytes().0 >= flush_at {
            flush_failed = stores.flush_all(FlushKind::Tick).failed > 0;
            let (left, limit) = stores.buffer_bytes();
            flush_at = left + limit / 8;
        }
        Ok(())
    })?;
    out.records = scanned.records;
    out.behind = scanned.behind;
    out.torn = scanned.torn;
    out.corrupt = scanned.corrupt;
    out.seconds = began.elapsed().as_secs_f64();

    // No id a record names is handed out again: a template that took one would be given
    // that record's line by a later start.
    trie.reserve_template_ids_through(highest_template);
    disk.note_applied(scanned.last_seq);
    // The rows put back have waited already: the first tick closes them, and the
    // checkpoint moves past them, where they would otherwise wait their wait again.
    disk.age_buffers();

    let stays = "it and any like it stay in the log, and the checkpoint behind them";
    if let Some((seq, template_id)) = unknown {
        log_warn(&format!(
            "wal - position {seq} names template {template_id}, which catalog.log does \
             not define; {stays}"
        ));
    }
    if let Some((seq, why)) = unreadable {
        log_warn(&format!(
            "wal - the line at position {seq} no longer parses ({why}); {stays}"
        ));
    }
    if let Some((seq, was, now)) = moved {
        log_warn(&format!(
            "wal - the line at position {seq} was stored under {} and now reads as {}: \
             the epoch window or the build differs from the one that logged it. {stays} - \
             start once under the settings it was logged with",
            rfc3339_nanos(was),
            rfc3339_nanos(now),
        ));
    }
    if let Some((seq, why)) = damaged {
        log_warn(&format!(
            "wal - whether a row group holds the line at position {seq} cannot be \
             read ({why}); {stays}"
        ));
    }
    if out.records > 0 || out.torn > 0 || out.corrupt > 0 {
        log(&format!(
            "wal - {} read back {} line(s) past checkpoint {}: {} put back in memory, {} \
             already in row groups{}{}, {:.2}s",
            wal.dir().display(),
            commas(out.records),
            commas(wal.checkpoint()),
            commas(out.applied),
            commas(out.covered),
            match out.replaced {
                0 => String::new(),
                n => format!(", {} of a day since replaced", commas(n)),
            },
            match out.held {
                0 => String::new(),
                n => format!(", {} left in the log", commas(n)),
            },
            out.seconds,
        ));
    }
    Ok(out)
}

/// An instant as replay's warnings print it.
fn rfc3339_nanos(ns: i64) -> String {
    kibbr_core::fmt::rfc3339(ns)
}

/// Rows the store would not take from the write-ahead log, and what that holds back.
///
/// **A store write that fails after the log has the line is not an error.** The line is
/// durable and was, or is about to be, acknowledged: answering its sender with a failure
/// would be a lie, and a shipper would send again a line that is already safe. So the
/// failure is logged here and goes no further - `apply` returns no `Result`, and the
/// type `/ingest` fails with cannot hold a store's error (`error::IngestError`).
///
/// **What it costs is time, not the row.** The checkpoint never passes the lowest position
/// held here, so the line's segment is not deleted and the next start's replay stores it.
///
/// **And the template takes nothing more from this process.** A row group's range says
/// every line of its template and day inside it is durable; a later row group of the
/// template, cut around a row that was skipped, would say that of the skipped line, and
/// the replay that should store it would step over it. Every later row of the template
/// waits in the log with the first.
struct Held {
    /// The lowest position held; `u64::MAX` while there is none.
    from: AtomicU64,
    rows: AtomicU64,
    templates: std::sync::Mutex<BTreeSet<u64>>,
}

impl Held {
    fn new() -> Self {
        Self {
            from: AtomicU64::new(u64::MAX),
            rows: AtomicU64::new(0),
            templates: std::sync::Mutex::new(BTreeSet::new()),
        }
    }

    /// Hand one row to the store at its position in the log; 0 is a row with no log
    /// behind it. Whether the store took it.
    fn apply(&self, stores: &StoreManager, position: u64, placed: db::Placed) -> bool {
        let template_id = placed.template_id;
        let deferred = self
            .templates
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(&template_id);
        if deferred {
            self.hold(position);
            return false;
        }
        match stores.append_at(template_id, &placed.time_column, placed.row, position) {
            Ok(()) => true,
            // `to_string`: a `StoreError` prints its cause itself.
            Err(e) => {
                self.defer(template_id, position, &e.to_string());
                false
            }
        }
    }

    /// The row at `position` could not be stored, and no later row of its template will
    /// be by this process: said once for the template, and held.
    fn defer(&self, template_id: u64, position: u64, why: &str) {
        let first = self
            .templates
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(template_id);
        if first {
            log_error(&format!(
                "store - {why}; the line was acknowledged and is {}",
                if position > 0 {
                    format!(
                        "in the write-ahead log at position {position}: its row, and every \
                         later row of template {template_id}, is stored by the next start's \
                         replay"
                    )
                } else {
                    "nowhere: storage is memory".to_string()
                }
            ));
        }
        self.hold(position);
    }

    /// As `defer`, for a line replay could not put back: its template waits with it, for
    /// the reason a refused row's does, and replay says why once for all of them.
    fn set_aside(&self, template_id: u64, position: u64) {
        self.templates
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(template_id);
        self.hold(position);
    }

    /// Keep the checkpoint behind `position`; 0 is a row with no log behind it, which
    /// nothing can keep.
    fn hold(&self, position: u64) {
        self.rows.fetch_add(1, Ordering::Relaxed);
        if position > 0 {
            self.from.fetch_min(position, Ordering::Relaxed);
        }
    }

    fn from(&self) -> Option<u64> {
        match self.from.load(Ordering::Relaxed) {
            u64::MAX => None,
            position => Some(position),
        }
    }

    fn rows(&self) -> u64 {
        self.rows.load(Ordering::Relaxed)
    }
}

/// A store that will not open, as the error startup exits on. An earlier layout or another
/// format version is not a disk that failed; it is a disk holding something this build
/// must not read, and it is named.
fn storage_fatal(e: StoreError) -> ServeError {
    match e {
        StoreError::LegacyLayout { path } => ServeError::LegacyLayout { path },
        StoreError::Format(kibbr_storage::store::v3::FormatError::Version { path, found }) => {
            ServeError::SegmentVersion { path, found }
        }
        StoreError::Format(kibbr_storage::store::v3::FormatError::PreRename { path }) => {
            ServeError::PreRename { path }
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
    let path = kibbr_storage::catalog::path_in(base);
    let existed = path.exists();
    let mut warn = |message: String| log_warn(&format!("catalog.log - {message}"));
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
                let (from_snapshot, failure) =
                    kibbr_storage::triefile::load_reporting(snapshot, data_dir);
                match failure {
                    Some(e) => {
                        log_warn(&format!("trie.json - {}", error::chain(&e)));
                        log_warn(
                            "trie.json - starting with an empty trie; templates \
                             are relearned from the next lines ingested",
                        );
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

mod compaction;
mod handlers;
mod ingest;
mod lifecycle;
mod routes;
#[cfg(test)]
mod tests;

pub use lifecycle::run;

use compaction::*;
use handlers::*;
use ingest::*;
use lifecycle::*;
use routes::*;
