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
use std::io::{Cursor, Read, Write};
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

use crate::compact;
use crate::config::{ApiLimits, Limits, Schema, Storage};
use crate::error::{self, CliError, DetectorError, DetectorErrorKind, IngestError, ServeError};
use crate::load;
use crate::query;
use kibbr_core::fmt::{commas, rfc3339_at};
use kibbr_engine::engine;
use kibbr_storage::catalog::{Catalog, CatalogError};
use kibbr_storage::db;
use kibbr_storage::store::disk::CompactOptions;
use kibbr_storage::store::fan::Fan;
use kibbr_storage::store::{FlushKind, StoreError, StoreManager};
use kibbr_storage::trie::{Trie, TrieError};
use kibbr_storage::wal::{self, Wal};

/// The default port this server listens on.
pub const DEFAULT_PORT: u16 = 7700;

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
    started: Instant,
    started_at: DateTime<Utc>,
    lines_read: AtomicU64,
    lines_ingested: AtomicU64,
    lines_rejected: AtomicU64,
    queries: AtomicU64,
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
            started: Instant::now(),
            started_at: now,
            lines_read: AtomicU64::new(0),
            lines_ingested: AtomicU64::new(0),
            lines_rejected: AtomicU64::new(0),
            queries: AtomicU64::new(0),
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

    // Last, once every template has its leaf and every row group is in one: the log is
    // held against them.
    let held = Held::new();
    let wal = Wal::open(&base, &mut |m| log(&format!("WARN wal - {m}")))?;
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
    let mut warn = |m: String| log(&format!("WARN wal - {m}"));
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
        log(&format!(
            "WARN wal - position {seq} names template {template_id}, which catalog.log does \
             not define; {stays}"
        ));
    }
    if let Some((seq, why)) = unreadable {
        log(&format!(
            "WARN wal - the line at position {seq} no longer parses ({why}); {stays}"
        ));
    }
    if let Some((seq, was, now)) = moved {
        log(&format!(
            "WARN wal - the line at position {seq} was stored under {} and now reads as {}: \
             the epoch window or the build differs from the one that logged it. {stays} - \
             start once under the settings it was logged with",
            rfc3339_nanos(was),
            rfc3339_nanos(now),
        ));
    }
    if let Some((seq, why)) = damaged {
        log(&format!(
            "WARN wal - whether a row group holds the line at position {seq} cannot be \
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
            log(&format!(
                "ERROR store - {why}; the line was acknowledged and is {}",
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
                let (from_snapshot, failure) =
                    kibbr_storage::triefile::load_reporting(snapshot, data_dir);
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
}

/// What one `/ingest` body did.
struct IngestReport {
    ingested: u64,
    errors: Vec<LineError>,
    /// Rejections past `MAX_REPORTED_ERRORS`, counted but not described.
    omitted: u64,
    /// Of the lines rejected - described or not - the ones that parsed and were refused
    /// because the catalog could not write the record they needed. Counted where the
    /// append fails, by the error's type, and not read back out of a message. Not in the
    /// JSON: it decides the status (`nothing_taken`).
    unwritten: u64,
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

    /// No line of the body was taken, and every line was refused for a write this server
    /// could not make. Then the answer is not a `200`: the sender got nothing wrong, has
    /// nothing to drop, and should send the body again (`nothing_taken`).
    ///
    /// False for a body with no refusal at all - an empty one, or blank lines - and for
    /// one where any refusal is the line's own: that line is refused the same way every
    /// time it is sent. `omitted` is in the sum because the rule is about every line, and
    /// `errors` stops at `MAX_REPORTED_ERRORS`.
    fn nothing_taken(&self) -> bool {
        self.ingested == 0
            && self.unwritten > 0
            && self.unwritten == self.errors.len() as u64 + self.omitted
    }
}

/// The two ways a body fails whole: the client's fault and the server's, which is the
/// difference between a 4xx and a 5xx.
enum IngestFailure {
    /// The body could not be decoded: wrong encoding, corrupt gzip, or a bomb.
    Body(BodyError),
    /// A lock poisoned mid-body. Fails every line after it too, so the body is abandoned
    /// rather than answered with 500 copies of one message. A catalog append that fails
    /// is not this: it costs its line, reported in `errors`, and the body goes on - and a
    /// body that lost every line that way is refused from its report, with the errors
    /// still in it, and not from here (`IngestReport::nothing_taken`). Nor is a store
    /// that cannot take a row, which costs nothing and is not an error at all.
    Fatal(IngestError),
}

/// An NDJSON body into the database: parsed, routed, logged, applied.
///
/// A plain function, not an `Ingestor` - see the module header.
///
/// **The order is the durability.** Every line is parsed with nothing held; each is then
/// routed to its template, which makes the template - or the widening the line causes -
/// durable in the catalog first; then the body's lines go to the write-ahead log, whose
/// record names that template; and this returns once the log has flushed them **and** the
/// log's one applier has handed their rows to the store. The answer is sent after that:
/// the flush is what makes a `200` true, and the wait for the applier is what lets a
/// client that ingests and then queries see all of what it sent.
///
/// **Per-line error isolation.** A bad line is recorded against its number and
/// the loop continues, so line 400 of 500 costs line 400 and nothing else. This is not a
/// second copy of that logic: `db::read_lines` is the same loop the CLI reads files with,
/// and the only thing this caller changes is where a rejection goes - to a `Vec` that
/// becomes the response body, instead of to a stderr an HTTP client cannot read.
///
/// **A body the log cannot take is refused, and so is one the catalog could write
/// nothing for.** Not a row the store cannot take: by then the line is durable, its
/// sender is told `Ok`, and the failure is the applier's to log (`Held`). The error this
/// returns cannot carry a store's - see `IngestError` - and is the log's, or the trie's
/// lock poisoned, which fails every line after it too, so the body is abandoned rather
/// than answered with 500 copies of one message. The catalog's refusal is not an error of
/// this function: each line it cost is in the report, the report counts them, and the
/// handler answers `503` when they are all there is (`IngestReport::nothing_taken`).
fn ingest(
    server: &Server,
    body: &[u8],
    now: DateTime<Utc>,
    permit: Permit,
) -> Result<IngestReport, IngestError> {
    // `place` is this body's place among those between the door and their answer, kept
    // until this returns, and what says whether its sender is still there; `permit` is
    // for the parse and the routing.
    let Permit {
        parse: permit,
        body: place,
    } = permit;
    // A body no write of the log would hold is refused before it is parsed: it would
    // be refused after, and the parse of that many lines is the cost.
    if let Some(wal::Refused::TooLarge { bytes, limit }) =
        server.wal.as_ref().and_then(|_| wal::too_large(body))
    {
        return Err(IngestError::TooLarge { bytes, limit });
    }

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
            });
        }
    })
    // Reading a body already in memory does not fail. What can is a body with no newline
    // in more than a line may hold, which the default body cap rules out and a larger
    // `--max-body-bytes` does not: a message, and a 500.
    .map_err(|e| IngestError::Body(error::chain(&e)))?;

    // ── Half two: route each line to its template, durably ─────────────────────────
    //
    // Before the log, because the log's record carries the template's id: the catalog
    // record that defines the id is flushed by the time `place_in` returns.
    let mut apply_errors = Vec::new();
    let mut rejected = stats.rejected;
    let mut unwritten = 0u64;
    let mut lines: Vec<wal::Line> = Vec::with_capacity(parsed.len());
    for (line, parsed, prepared) in parsed {
        let kind = match db::place_in(&server.trie, &parsed, prepared, now) {
            Ok(Some(placed)) => {
                lines.push(wal::Line {
                    raw: parsed.raw_line,
                    // The time the row is filed under, as the store reads it: what the
                    // log writes down beside the line, for replay to hold it to.
                    time: kibbr_storage::store::effective_ts(&placed.row, &placed.time_column)
                        .unwrap_or(0),
                    placed,
                });
                continue;
            }
            // The template's own lock is poisoned: nothing can say which template the
            // line is, so there is no id to log it under. It used to be counted as
            // ingested and stored nowhere.
            Ok(None) => DetectorErrorKind::NotDurable {
                detail: "the template's lock is poisoned".to_string(),
            },
            // The line parsed and its template could not be made durable. Counted and
            // reported exactly as a line that did not parse: to the client both are
            // "this one is not in the database", and the message says which.
            Err(e @ TrieError::Catalog(_)) => {
                // And counted apart, as a write that failed: a body of nothing but these
                // is sent again, and a record that would not encode is no reason to -
                // it would not encode the next time either.
                if !matches!(e, TrieError::Catalog(CatalogError::Serialize { .. })) {
                    unwritten += 1;
                }
                DetectorErrorKind::NotDurable {
                    detail: error::chain(&e),
                }
            }
            // The trie's lock poisoned, which fails every line after it too.
            Err(e) => return Err(e.into()),
        };
        rejected += 1;
        if apply_errors.len() < MAX_REPORTED_ERRORS {
            apply_errors.push(LineError {
                line,
                msg: kind.to_string(),
            });
        }
    }

    // ── Half three: the log, and then the rows ─────────────────────────────────────
    //
    // With a log: the body's lines durable in one flush, then applied by the log's one
    // applier in the log's order, and back here once both are done. With none - storage
    // is memory - applied here, as it always was: nothing is durable, and the banner
    // says so.
    //
    // **All of the body, or none of it.** A log that cannot take it has none of its
    // lines, so the refusal is of the body: the sender keeps it, and sends it again
    // without storing a line twice.
    let ingested = lines.len() as u64;
    match &server.wal {
        Some(wal) => {
            // A body the log refuses was read and routed, and none of it was taken: its
            // lines are counted as read and as rejected, whichever refusal it was.
            let refused = |refused: wal::Refused| {
                server.lines_read.fetch_add(stats.read, Ordering::Relaxed);
                server
                    .lines_rejected
                    .fetch_add(rejected + ingested, Ordering::Relaxed);
                match refused {
                    wal::Refused::Cannot(why) => IngestError::Log(why),
                    wal::Refused::Busy => IngestError::Busy,
                    wal::Refused::Left => IngestError::Left,
                    wal::Refused::TooLarge { bytes, limit } => {
                        IngestError::TooLarge { bytes, limit }
                    }
                }
            };
            // A log with no room for the body makes it wait for some, for as long as a
            // body may be kept waiting, where it used to be shed at once - after it had
            // been read, inflated, parsed and routed, all of which its sender's retry
            // paid for again a second later.
            let wait = wal::Wait {
                at_most: server.api.ingest_wait,
                left: &place.left,
            };
            let ticket = wal
                .enqueue(lines, db::arrival_nanos(now), wait)
                .map_err(&refused)?;
            // The permit is for the parse and the routing, which are what a core is
            // short of. Held across the flush as well, a flush could never carry more
            // bodies than there are cores - and it is there to carry whatever has queued.
            // What bounds the bodies waiting from here is `ingest_bodies`, at the door.
            //
            // **Kept until the body is in the log's queue, and so across a wait for
            // room.** A body waiting for room is a parsed body held in memory that the
            // log's bound does not count; with its permit kept there are no more of
            // those than there are permits, which is the memory the parse already had.
            // And it is what carries the pressure back: once every permit is held by a
            // body the log has no room for, the door lets nobody in, and every later
            // sender waits there with its body unread.
            drop(permit);
            ticket.wait().map_err(&refused)?;
        }
        None => {
            for line in lines {
                server.held.apply(&server.stores, 0, line.placed);
            }
            drop(permit);
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
        unwritten,
    })
}

// ── Handlers ───────────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct QueryRequest {
    q: String,
}

/// `{"ok":true,"status":"ok"}`, or `degraded` while something here cannot write: the
/// write-ahead log, the store, or rows the store refused (`Server::degraded`).
///
/// Waits on no flush and reads no data - a health check that blocks behind a long ingest
/// reports the wrong thing - which is why each verdict is a flag beside what it is about
/// and not a question put to it.
///
/// **`ok` stays `true` when degraded, and the status says otherwise.** This is the
/// liveness check an orchestrator kills the process on, and a process that cannot write
/// is not a process worth killing: restarting it does not empty the disk, and the rows
/// already on it are still queryable. `degraded` is for the dashboard and the human;
/// `/ingest` is where the refusal belongs, because that is the caller who would otherwise
/// be told a lie.
async fn health(State(server): State<Arc<Server>>) -> Json<Value> {
    match server.degraded() {
        Some(detail) => Json(json!({ "ok": true, "status": "degraded", "detail": detail })),
        None => Json(json!({ "ok": true, "status": "ok" })),
    }
}

impl Server {
    /// What is wrong with this server's writing, if anything is: the log first, because
    /// while it cannot write `/ingest` refuses every body.
    ///
    /// A store that cannot write is degraded and refuses nothing: its rows are in the log
    /// and are written when it can, or put back by the next start. A catalog that cannot
    /// write is not reported here: it costs the lines that needed a new record, and a body
    /// of nothing else is refused in its own answer, which names the write (`nothing_taken`).
    fn degraded(&self) -> Option<String> {
        if let Some(why) = self.wal.as_ref().and_then(Wal::refusal) {
            return Some(format!(
                "the write-ahead log cannot write, and /ingest answers 503 until it can: {why}"
            ));
        }
        if self.write_health.is_degraded() {
            return Some(
                self.write_health
                    .last_error()
                    .unwrap_or_else(|| "the last flush could not write".to_string()),
            );
        }
        match self.held.rows() {
            0 => None,
            rows => Some(format!(
                "{} line(s) are in the write-ahead log and not in the store - the store \
                 refused their rows, or the start could not read them back. The log keeps \
                 them, and the next start tries again",
                commas(rows)
            )),
        }
    }
}

/// The refusal a shipper gets while the write-ahead log cannot write.
///
/// 503 and a `Retry-After`, the same shape as the shed at `busy`, and for a related
/// reason: this server cannot take the batch now and the caller should keep it and come
/// back. The difference is in the body, which names the write error - an operator reading
/// a shipper's log needs to see `No space left on device` there and not "try later".
///
/// **This is the refusal that is about durability, and it is the log's.** A `200` means
/// the line is in the log and flushed, so a log that cannot write has no `200` to give; a
/// `503` keeps the line at the shipper, where it can still be sent again. It used to be
/// the store's: with no log a `200` meant *buffered*, and on a full disk every batch got
/// one, the shutdown log said "90,000 rows written", and after a restart `COUNT LOG {}`
/// was 0. A store that cannot write is no longer a reason to refuse anything - the line is
/// safe in the log, and a `503` would make a shipper resend a line that is already stored.
///
/// One other refusal is about durability, and it is the catalog's: a body that never
/// reached the log, because no line of it could be given the record it needed before it
/// counts. That one is answered apart (`nothing_taken`), with its errors by line.
fn cannot_log(detail: &str) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::RETRY_AFTER, RETRY_AFTER_SECONDS.to_string())],
        Json(json!({
            "ok": false,
            "error": format!(
                "the write-ahead log cannot write, so nothing of this body was taken - a \
                 200 means the line is durable. Keep the body and send it again: {detail}"
            ),
            "retry_after": RETRY_AFTER_SECONDS,
        })),
    )
        .into_response()
}

/// The answer to a body of which nothing was taken because nothing could be written for
/// it (`IngestReport::nothing_taken`): 503 and a `Retry-After`, as the log's refusal is,
/// around the report a `200` would have carried - `ingested: 0` and each line's error
/// against its number, which is where the write that failed and its path are named.
///
/// A `200` here told a shipper that goes by the status to drop a body of well-formed
/// lines this server had none of. `error` is for the client that reads that field of
/// every refusal and no further, and repeats the first line's reason.
///
/// The body was read to its end, so the connection is left as it is.
fn nothing_taken(report: &IngestReport) -> Response {
    let first = match report.errors.first() {
        Some(error) => format!("line {}: {}", error.line, error.msg),
        None => "no line's error was kept".to_string(),
    };
    let mut body = report.to_json();
    body["error"] = json!(format!(
        "the catalog could not write the record each line of this body needed, so nothing \
         of it was taken. Keep the body and send it again: {first}"
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
    axum::Extension(permit): axum::Extension<Permit>,
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
    // What says the sender has gone, to a body that is waiting for room in the log: set
    // if this future is dropped before the task below has returned, which is what a
    // connection that closes does to it.
    let place = Arc::clone(&permit.body);
    let mut leaving = Leaving {
        server: &server,
        place: &place,
        armed: true,
    };
    // The parse and the trie walk are CPU work, and the wait for the log's flush after
    // them blocks, so none of it belongs on a runtime thread that also answers `/health`.
    // Inflating joins them rather than running above: a gzip bomb is seconds of CPU, and
    // spending it on a runtime thread would stall every other endpoint while the server
    // was being attacked.
    let done = tokio::task::spawn_blocking(move || {
        // Said from before the body is inflated until its answer is ready: while any
        // body is in its task a query chooses its rows on its own thread, so the scan
        // threads take no core from the parse and the walk - and none while the body
        // waits on the log either, which is most of a small body's time. So one sender
        // of small bodies back to back is enough to keep every scan on one thread.
        let _body = held.fan.as_deref().map(Fan::body);
        let decoded = decode(&body, encoding.as_deref(), held.api.max_decompressed_bytes)
            .map_err(IngestFailure::Body)?;
        ingest(&held, &decoded, now, permit).map_err(IngestFailure::Fatal)
    })
    .await;
    // The task has returned, and its answer is this future's to give: nobody has left.
    leaving.armed = false;

    match done {
        // The one report that is not a 200: no line was taken, and every one was refused
        // for a write the catalog could not make. Nothing landed that a retry could
        // duplicate and nothing in the body is the sender's to fix, so it is told what
        // the log's refusal tells it - keep the body, come back.
        Ok(Ok(report)) if report.nothing_taken() => nothing_taken(&report),
        // 200 even with errors in it. The body was accepted and part of it was stored;
        // a 4xx would tell a shipper to retry the whole batch and duplicate the 497
        // lines that landed, and there is nothing the client could have sent differently
        // for the other 3 beyond not sending them.
        Ok(Ok(report)) => (StatusCode::OK, Json(report.to_json())).into_response(),
        Ok(Err(IngestFailure::Body(e))) => fail(e.status(), &e.message()),
        // The log could not take the body, which is not the client's fault and not a
        // thing a 200 can say: the door's own refusal, for a body that was already past it.
        Ok(Err(IngestFailure::Fatal(IngestError::Log(why)))) => cannot_log(&why),
        // And a shed: the log is behind what arrives, and the body waits at its sender.
        Ok(Err(IngestFailure::Fatal(IngestError::Busy))) => {
            server.door.shed.log.fetch_add(1, Ordering::Relaxed);
            log_busy()
        }
        // The body was let go because its sender had gone, so nobody reads this: the
        // future that would have sent it is the one that was dropped. Not counted as a
        // shed, which is a sender told to come back.
        Ok(Err(IngestFailure::Fatal(IngestError::Left))) => log_busy(),
        // The sender's to fix, as a body past the wire cap is: no retry of it will fit.
        Ok(Err(IngestFailure::Fatal(e @ IngestError::TooLarge { .. }))) => {
            fail(StatusCode::PAYLOAD_TOO_LARGE, &e.to_string())
        }
        Ok(Err(IngestFailure::Fatal(e))) => {
            fail(StatusCode::INTERNAL_SERVER_ERROR, &error::chain(&e))
        }
        Err(join) => fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("ingest task failed: {join}"),
        ),
    }
}

/// Tells the log when a handler's future is dropped before its task has returned: the
/// body's sender hung up.
///
/// **Only a body waiting for room in the log gives anything up for it.** One that is
/// being parsed, or is already in the log's queue, goes on and is stored whole, as it
/// always has: the log looks for room before it looks at this flag, and never looks at
/// it for a body it has taken. But a body the log has no room for holds a permit for the
/// parse while it waits, and one whose sender has gone would hold it for the whole wait
/// with nobody to answer at the end of it - and its sender, having given up, has very
/// likely sent the body again.
///
/// **The flag first, with no lock, and then the wake-up.** A body about to wait reads
/// the flag under the log's queue lock and keeps that lock until it is parked; the
/// wake-up takes the same lock after the flag is set. So the body sees the flag, or is
/// parked when the wake-up comes: it is never left to wait out its time.
///
/// What this costs a request whose sender stays is two references and a flag on the
/// handler's stack. The clone of the server and the task are made only in `drop`, and
/// only when a sender has in fact left.
struct Leaving<'a> {
    server: &'a Arc<Server>,
    place: &'a Arc<Place>,
    /// Cleared once the task has returned: there is then nothing to tell.
    armed: bool,
}

impl Drop for Leaving<'_> {
    fn drop(&mut self) {
        if !self.armed || self.server.wal.is_none() {
            return;
        }
        self.place.left.store(true, Ordering::Release);
        let server = Arc::clone(self.server);
        let wake = move || {
            if let Some(wal) = &server.wal {
                wal.wake_room();
            }
        };
        // The wake-up takes the queue's lock, which the log's writer can keep for as
        // long as moving a large body's lines takes: not a runtime worker's to wait on.
        // With no runtime left to hand it to, this is not a worker either.
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => drop(runtime.spawn_blocking(wake)),
            Err(_) => wake(),
        }
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

/// Admission control for `/ingest`: a 503 while the write-ahead log cannot write, then
/// both permits, at once when they are free and after a bounded wait when they are not,
/// or a 503 that says when to come back.
///
/// **A layer, not a check inside the handler.** A layer wraps the handler, so it runs
/// before the extractors - before a byte of the body is pulled off the socket. Shedding in
/// the handler would buffer `max_body_bytes` first and *then* refuse, spending the memory
/// the shed exists to protect. And waiting here is waiting with the body unread: it stays
/// in the kernel's buffers and at its sender, whose write blocks when those are full,
/// which is the one place a body can wait at no cost to this process.
///
/// 503 and not 429: 429 claims a per-client quota and there is none here. This server is
/// saturated, which is what 503 says and what `Retry-After` dates. Either makes Vector back
/// off; only one is true.
///
/// What this must never do is take the body and go quiet. A shipper with no answer keeps
/// the batch, times out, and opens another connection - so a merely slow server gathers
/// connections until it is unreachable. Every wait here ends by `api.ingest_wait`, and a
/// refusal is the kinder answer precisely because a client knows what to do with it.
///
/// **A refusal from here tells the client that the connection closes.** The body is still
/// unread when it is answered, so the connection cannot carry another request and the
/// server closes it. `say_close`, the outermost layer, puts `Connection: close` on the
/// answer for that reason; without it a shipper that keeps its connection sends its next
/// batch into a closed socket. The refusals do not set the header themselves: the handler
/// returns two of them, `cannot_log` and `log_busy`, after the body was read, where the
/// connection stays open.
async fn admit(State(server): State<Arc<Server>>, mut request: Request, next: Next) -> Response {
    // **First, and in the layer rather than in the handler**, for the reason the shed is
    // here: a layer runs before the extractors, so a body this server cannot make durable
    // is refused before ten megabytes of it are pulled off the socket. In the handler the
    // `Bytes` extractor has already buffered it.
    //
    // Before the permit too. A server that cannot log has no use for the slot, and a
    // shipper told "at capacity" would read that as "send fewer" when the answer is "send
    // none until the disk is back". The flush tick asks the log to try again
    // (`Wal::probe`), so the door opens within a tick of the disk coming back.
    //
    // **The log, and not the store.** A store that cannot write refuses nothing here.
    if let Some(detail) = server.wal.as_ref().and_then(Wal::refusal) {
        return cannot_log(&detail);
    }
    // Nearly every body finds both permits free, and is let in without a clock read or a
    // timer made for it. Only one that does not goes on to where the waits are.
    let permit = match server.admit_now() {
        Some(permit) => permit,
        None => match server.admit_waiting().await {
            Ok(permit) => permit,
            Err(refusal) => return *refusal,
        },
    };
    // **Handed to the handler, which hands them to its blocking task.** Held only here
    // they went back the moment a client hung up - this future is dropped then - while
    // the task they were meant to cover ran on: a sender that timed out and tried again
    // took the freed permit and queued a second copy of its body beside the first.
    request.extensions_mut().insert(permit);
    next.run(request).await
}

impl Server {
    /// Both permits, when nothing has to be waited for: the buffers under twice their
    /// limit, a permit for the parse free, and a place.
    ///
    /// Taken without waiting, and that does not jump the line at the door: a permit that
    /// is given back goes to whoever has waited longest for it, and only one that nobody
    /// is waiting for is there for this to take.
    fn admit_now(&self) -> Option<Permit> {
        if self.stores.buffers_full() {
            return None;
        }
        let parse = Arc::clone(&self.ingest_permits)
            .try_acquire_owned()
            .ok()?;
        // A permit got without a place goes straight back, as this returns.
        let place = Arc::clone(&self.ingest_bodies)
            .try_acquire_owned()
            .ok()?;
        Some(Permit::of(parse, place))
    }

    /// The most bodies that may wait at the door at once, and `None` for no bound.
    ///
    /// **None at the default wait, as there never was one.** Nobody is in the line for
    /// longer than a hundred milliseconds then, so it bounds itself, and a burst queues
    /// for the moment it takes with no count held against it. A wait an operator has
    /// raised is another matter: each waiter is a socket, with up to a receive buffer of
    /// unread body behind it, kept for as long as the wait - and that needs a number. It
    /// is the places, so there are never more bodies outside the door than may be inside.
    fn line_max(&self) -> Option<usize> {
        let default = Duration::from_millis(crate::config::DEFAULT_INGEST_WAIT_MS as u64);
        (self.api.ingest_wait > default).then(|| ingest_bodies(&self.api))
    }

    /// Both permits, with every wait in it, all before one deadline: for the buffers to
    /// come back under twice their limit, for a permit for the parse, for a place. `Err`
    /// is the refusal, already counted under its cause.
    ///
    /// **The body is unread throughout**, and nothing is held while it waits for the
    /// buffers or for a permit but its turn. Waiting for a place it holds its permit for
    /// the parse, which goes back when it is refused or dropped: a sender that goes away
    /// costs nobody a permit.
    ///
    /// **A wait that has run out sheds nobody by itself.** What the body was waiting for
    /// is looked at once more, after the wait: a flush empties the buffers before it
    /// says so, and a permit can come back in the same instant. The body is shed only
    /// for what is still not there, and told what that is. With no wait to give
    /// (`--ingest-wait-ms 0`) that look is the first one, and nothing is waited for.
    ///
    /// **Every wait watches for the stop first.** Once shutdown has begun a waiter is
    /// refused at once, and one that finds its permit and the stop ready together is
    /// refused too - let in, it would be read and parsed only to be shed by a log that
    /// has been told to hurry. So the stop is asked before anything else, every time.
    ///
    /// No lock is held across any wait here, and every wait is on a thing of the
    /// runtime's own - a semaphore, a `Notify`, a timer, a token - so a waiter costs no
    /// thread. Dropped - the connection went away - it leaves the line as it was.
    async fn admit_waiting(&self) -> Result<Permit, Box<Response>> {
        if let Some(max) = self.line_max() {
            if self.door.waiting.load(Ordering::Relaxed) >= max as u64 {
                self.door.shed.line.fetch_add(1, Ordering::Relaxed);
                return Err(Box::new(line_full(max)));
            }
        }
        let waiting = Waiting::begin(&self.door);
        let until = waiting.since + self.api.ingest_wait;
        // Whether the wait has run out: from the start, with no wait to give. Nothing
        // is asked of a timer then. One set for a moment that is already here still
        // takes a turn of the runtime's clock to say so, which is a wait of a
        // millisecond for a body that was to have none.
        let mut spent = self.api.ingest_wait.is_zero();
        let turned_away = || {
            self.door.shed.stopping.fetch_add(1, Ordering::Relaxed);
            Box::new(stopping())
        };
        // The flush is rung once for this body. A flush that starts after the ring sees
        // every row that filled the buffers before it; one that leaves them full has done
        // what a flush can, and the tick will try again.
        let mut rung = false;
        let permit = loop {
            // The buffers. Past twice their limit the flush is falling behind what
            // arrives, even closing the largest buffers early, and a body taken now is
            // memory the process may not have. A flush is on its way: the body waits for
            // it, where it used to be shed with that flush a few milliseconds from done.
            // If it is shed in the end, that is a shed and not a verdict on durability:
            // the log could take the line, and the row it becomes is memory this process
            // has run out of.
            loop {
                // Listening before the look, so a flush that ends between the look and
                // the wait is not missed.
                let flushed = self.flushed.notified();
                tokio::pin!(flushed);
                flushed.as_mut().enable();
                if !self.stores.buffers_full() {
                    break;
                }
                if let Some(detail) = self.wal.as_ref().and_then(Wal::refusal) {
                    return Err(Box::new(cannot_log(&detail)));
                }
                // A store that cannot write: nothing leaves the buffers, so a wait would
                // only be the same refusal later. At once, with the store's error in it.
                if self.write_health.is_degraded() {
                    self.door.shed.buffers.fetch_add(1, Ordering::Relaxed);
                    return Err(Box::new(buffers_full(
                        self.stores.buffer_bytes(),
                        self.write_health.last_error(),
                    )));
                }
                if !rung {
                    self.flush_now.notify_one();
                    rung = true;
                }
                // Out of time, and the looks above were made after it ran out: the
                // buffers are full now, the log can write and so can the store. Shed,
                // with what the buffers hold at this moment.
                if spent {
                    if self.stopping.is_cancelled() {
                        return Err(turned_away());
                    }
                    self.door.shed.buffers.fetch_add(1, Ordering::Relaxed);
                    return Err(Box::new(buffers_full(self.stores.buffer_bytes(), None)));
                }
                tokio::select! {
                    biased;
                    () = self.stopping.cancelled() => return Err(turned_away()),
                    () = &mut flushed => {}
                    // Not shed from here. A flush brings the buffers down before it
                    // rings `flushed`, so a wait can run out beside buffers that
                    // already have room; and a log or a store that failed while this
                    // body waited is the reason it is given, here as everywhere. So
                    // round once more, for the same looks a body makes on arriving.
                    () = tokio::time::sleep_until(until) => spent = true,
                }
            }
            // The permit for the parse, and then the place: in that order, so that
            // running out of places means what its refusal says - the bodies inside are
            // not being parsed, there is a permit free for that, they are waiting on the
            // log. One deadline for both, so the refusal is no later for there being two.
            let parse = tokio::select! {
                biased;
                () = self.stopping.cancelled() => return Err(turned_away()),
                got = permit_by(&self.ingest_permits, until, spent) => got,
            };
            // `None` is the deadline: the semaphores are never closed.
            let Some(parse) = parse else {
                return Err(Box::new(self.out_of_time(Wanted::Permit)));
            };
            let place = tokio::select! {
                biased;
                () = self.stopping.cancelled() => return Err(turned_away()),
                got = permit_by(&self.ingest_bodies, until, spent) => got,
            };
            let Some(place) = place else {
                return Err(Box::new(self.out_of_time(Wanted::Place)));
            };
            // The buffers again, because a permit may have been waited for, and for as
            // long as the whole wait: every body that passed the first look could
            // otherwise be let in onto buffers that filled meanwhile. Full, the permits
            // go back - a body waiting for a flush holds nothing - and it goes round.
            if !self.stores.buffers_full() {
                break Permit::of(parse, place);
            }
        };
        // The log again, because something was waited for.
        if let Some(detail) = self.wal.as_ref().and_then(Wal::refusal) {
            return Err(Box::new(cannot_log(&detail)));
        }
        Ok(permit)
    }

    /// The refusal of a body whose wait at the door ran out with no permit, or with no
    /// place, counted under what there was none of.
    ///
    /// **A log that has failed meanwhile is the reason, whatever was waited for.** And
    /// while bodies inside are waiting for room in the log, that is the reason too: they
    /// hold the permits, the log's disk or the store is what is behind, and a shipper
    /// told "at capacity" would be told to send fewer when more permits would not help.
    fn out_of_time(&self, wanted: Wanted) -> Response {
        if let Some(detail) = self.wal.as_ref().and_then(Wal::refusal) {
            return cannot_log(&detail);
        }
        let shed = &self.door.shed;
        if self.wal.as_ref().is_some_and(|wal| wal.waiting_for_room() > 0) {
            shed.log.fetch_add(1, Ordering::Relaxed);
            return log_busy();
        }
        match wanted {
            Wanted::Permit => {
                shed.permit.fetch_add(1, Ordering::Relaxed);
                busy(self.api.ingest_concurrency)
            }
            Wanted::Place => {
                shed.place.fetch_add(1, Ordering::Relaxed);
                log_busy()
            }
        }
    }
}

/// One permit of `from` for a body at the door, waited for until `until` and no longer.
/// `None` is the wait run out with no permit.
///
/// With the wait already `spent` - run out while the body waited for the buffers, or
/// none to give - nothing is waited for, and a permit is taken only if one is free at
/// this moment. Asked for and not waited for, rather than raced against a timer that
/// has already rung: the one that is free is then taken on every run, and the body is
/// shed only when there is none.
async fn permit_by(
    from: &Arc<Semaphore>,
    until: tokio::time::Instant,
    spent: bool,
) -> Option<tokio::sync::OwnedSemaphorePermit> {
    if spent {
        return Arc::clone(from).try_acquire_owned().ok();
    }
    tokio::select! {
        biased;
        got = Arc::clone(from).acquire_owned() => got.ok(),
        () = tokio::time::sleep_until(until) => None,
    }
}

/// What a body at the door was waiting for when its wait ran out.
enum Wanted {
    /// A permit for the parse.
    Permit,
    /// A place among the bodies between the door and their answer.
    Place,
}

/// One body waiting at the door, counted for as long as this lives: however the wait
/// ends - let in, refused, or dropped with its connection - the count goes back and the
/// time it took is added up.
struct Waiting<'a> {
    door: &'a Door,
    since: tokio::time::Instant,
}

impl<'a> Waiting<'a> {
    fn begin(door: &'a Door) -> Self {
        door.waits.fetch_add(1, Ordering::Relaxed);
        door.waiting.fetch_add(1, Ordering::Relaxed);
        Self {
            door,
            since: tokio::time::Instant::now(),
        }
    }
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.door.waiting.fetch_sub(1, Ordering::Relaxed);
        let waited = self.since.elapsed().as_micros() as u64;
        self.door.waited_micros.fetch_add(waited, Ordering::Relaxed);
    }
}

/// Shutdown has begun: nobody is kept waiting from here on. Every body at the door is
/// refused at once, and the log is told that no body waits for room - so a body already
/// in the log is finished, one still being parsed is taken if the log has room when it
/// gets there and shed (`log_busy`, counted under `shed.log`) if it has none, and a stop
/// never takes as long as the longest wait.
///
/// The log's part takes its queue's lock, which is no runtime worker's to take: it is
/// handed to the blocking pool.
fn begin_stop(server: &Arc<Server>) {
    server.stopping.cancel();
    // A compaction hears of it now, while the requests in flight finish, and is waited
    // for after the final flush: see `join_compactions`.
    cancel_compactions(server);
    if server.wal.is_none() {
        return;
    }
    let held = Arc::clone(server);
    let hurry = move || {
        if let Some(wal) = &held.wal {
            wal.hurry();
        }
    };
    match tokio::runtime::Handle::try_current() {
        Ok(runtime) => drop(runtime.spawn_blocking(hurry)),
        Err(_) => hurry(),
    }
}

/// One `/ingest` body's permits, as the door hands them to the handler: see `admit`.
/// `parse` goes back once the body is parsed, routed and in the log's queue; `body` when
/// its task returns.
#[derive(Clone)]
struct Permit {
    parse: Arc<tokio::sync::OwnedSemaphorePermit>,
    body: Arc<Place>,
}

impl Permit {
    fn of(
        parse: tokio::sync::OwnedSemaphorePermit,
        place: tokio::sync::OwnedSemaphorePermit,
    ) -> Self {
        Self {
            parse: Arc::new(parse),
            body: Arc::new(Place {
                _permit: place,
                left: AtomicBool::new(false),
            }),
        }
    }
}

/// A body's place among those between the door and their answer, and whether anybody is
/// still waiting for that answer.
struct Place {
    _permit: tokio::sync::OwnedSemaphorePermit,
    /// The body's sender hung up before its answer was ready. What a body waiting for
    /// room in the log looks at, to give the wait up.
    left: AtomicBool,
}

/// The refusal of a body that arrives while the server is stopping and would have had to
/// wait: the shed's shape. The body is unread, and whatever takes this server's place
/// can have it.
fn stopping() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::RETRY_AFTER, RETRY_AFTER_SECONDS.to_string())],
        Json(json!({
            "ok": false,
            "error": format!(
                "the server is stopping, and nothing of this body was taken; keep it and \
                 send it again in {RETRY_AFTER_SECONDS}s"
            ),
        })),
    )
        .into_response()
}

/// The refusal of a body that found as many waiting at the door as may: the shed's shape.
fn line_full(max: usize) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::RETRY_AFTER, RETRY_AFTER_SECONDS.to_string())],
        Json(json!({
            "ok": false,
            "error": format!(
                "{max} bodies are already waiting for ingest to have room for them, which \
                 is as many as may; retry in {RETRY_AFTER_SECONDS}s"
            ),
        })),
    )
        .into_response()
}

/// The refusal while the buffers are past twice their limit: the shed's shape, with what
/// is held against what may be.
///
/// `failing` is the store's last write error while its flushes are failing: then the
/// buffers are full because nothing leaves them, not because too much arrives, and the
/// refusal says so - the remedy is the disk, not the shippers. The rows already
/// acknowledged are in the write-ahead log either way.
fn buffers_full((held, limit): (u64, u64), failing: Option<String>) -> Response {
    let why = match failing {
        Some(error) => format!(
            "the store cannot write ({error}), so nothing leaves them - every line already \
             acknowledged is in the write-ahead log"
        ),
        None => "the flush is behind what is arriving".to_string(),
    };
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::RETRY_AFTER, RETRY_AFTER_SECONDS.to_string())],
        Json(json!({
            "ok": false,
            "error": format!(
                "the buffers hold {} bytes of memory, past twice the {} byte limit: {why}; \
                 retry in {RETRY_AFTER_SECONDS}s",
                commas(held),
                commas(limit)
            ),
        })),
    )
        .into_response()
}

/// The shed of a body the write-ahead log had no room for - as many bodies between the
/// door and their answer as may, or as many bytes in the log and not yet in the store:
/// the log's disk or the store is behind what arrives (`/stats`
/// `storage.wal.queue_bytes` against `unapplied_bytes` says which). Nothing of the body
/// is in the log.
fn log_busy() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::RETRY_AFTER, RETRY_AFTER_SECONDS.to_string())],
        Json(json!({
            "ok": false,
            "error": format!(
                "the write-ahead log has more waiting to be written or stored than it may \
                 hold: its disk or the store is behind what is arriving; retry in \
                 {RETRY_AFTER_SECONDS}s"
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

/// `POST /load`: what `kibbr load` sends when this server holds the data directory.
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
    /// The loader's own bound on the rows it holds, not the live door's: see
    /// `load::DEFAULT_LOAD_BUFFER_BYTES`.
    #[serde(default)]
    load_buffer_bytes: Option<usize>,
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
            kibbr_storage::store::disk::day_of_name(text)
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
    let load_buffer_bytes = request
        .load_buffer_bytes
        .unwrap_or(load::DEFAULT_LOAD_BUFFER_BYTES);
    if load_buffer_bytes == 0 {
        return Err("load_buffer_bytes 0: a load has to hold at least one row".to_string());
    }
    Ok(load::Options {
        inputs,
        replace_day,
        max_bytes_per_sec: request.max_mb_per_sec.map(|mb| mb * 1_000_000.0),
        threads: request.threads.unwrap_or_else(load::server_threads).max(1),
        load_buffer_bytes,
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
    if let Some(refused) = wrong_directory(&server, request.data_dir.as_deref(), "loaded") {
        return refused;
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
                    .map(|d| format!(", replacing {}", kibbr_storage::store::disk::day_name(d)))
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

/// Whether a load job is running: the scheduler leaves compaction until it is not.
fn load_running(server: &Server) -> bool {
    let loads = server.loads.lock().unwrap_or_else(|e| e.into_inner());
    loads.jobs.values().any(|job| {
        matches!(
            *job.state.lock().unwrap_or_else(|e| e.into_inner()),
            LoadState::Running
        )
    })
}

// ── Compaction ─────────────────────────────────────────────────────────────────────

/// A compaction as `kibbr compact` sends it.
#[derive(Debug, Deserialize)]
struct CompactRequest {
    /// The UTC day to compact now, YYYY-MM-DD. Absent or null, every day that is due.
    #[serde(default)]
    day: Option<String>,
    #[serde(default)]
    threads: Option<usize>,
    /// The data directory the client meant, canonical: see `wrong_directory`.
    #[serde(default)]
    data_dir: Option<std::path::PathBuf>,
}

/// Start a compaction job and answer at once with its number; `GET /compact/<job>` says how
/// it went. Not held open for the compaction, for the reasons `post_load` is not.
///
/// **One at a time, and apart from the load slot.** A second compaction is a 409. A load
/// beside one is not: the two meet only under the store's own lock, and a load that
/// replaces the day being compacted wins it - the compaction finds its inputs retired and
/// removes what it rebuilt.
async fn post_compact(
    State(server): State<Arc<Server>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if server.stores.disk_store().is_none() {
        return fail(
            StatusCode::SERVICE_UNAVAILABLE,
            &compact::CompactError::NoDisk.to_string(),
        );
    }
    if let Some(refused) = compact_refused(&server, &headers) {
        return refused;
    }
    let request: CompactRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => return fail(StatusCode::BAD_REQUEST, &format!("compact request: {e}")),
    };
    if let Some(refused) = wrong_directory(&server, request.data_dir.as_deref(), "compacted") {
        return refused;
    }
    let day = match request.day.as_deref() {
        None => None,
        Some(text) => match kibbr_storage::store::disk::day_of_name(text) {
            Some(day) => Some(day),
            None => {
                return fail(
                    StatusCode::BAD_REQUEST,
                    &format!("day `{text}` is not a YYYY-MM-DD day"),
                )
            }
        },
    };
    let threads = request.threads.unwrap_or_else(load::server_threads).max(1);

    let mut jobs = server.compactions.lock().unwrap_or_else(|e| e.into_inner());
    if jobs.is_stopping() {
        return fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "this server is shutting down; nothing was compacted",
        );
    }
    let Some(id) = jobs.start() else {
        return fail(
            StatusCode::CONFLICT,
            &match jobs.running() {
                Some(id) => {
                    format!("compaction job {id} is still running; one compaction runs at a time")
                }
                None => "a compaction is still running; one runs at a time".to_string(),
            },
        );
    };
    // Reap the last one's thread, which has finished: no job was running, and a job's
    // state is the last thing its thread touches here.
    if let Some(done) = jobs.asked.take() {
        let _ = done.join();
    }
    let held = Arc::clone(&server);
    let spawned = std::thread::Builder::new()
        .name(format!("compact-{id}"))
        .spawn(move || compaction(&held, day, threads, Some(id)));
    match spawned {
        Ok(handle) => {
            jobs.asked = Some(handle);
            (StatusCode::ACCEPTED, Json(json!({"ok": true, "job": id}))).into_response()
        }
        Err(e) => {
            jobs.forget(id);
            fail(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("starting the compaction job: {e}"),
            )
        }
    }
}

/// `GET /compact/<job>`: running, done with its report, or failed with why - and with the
/// days it had finished before it failed.
async fn get_compact(
    State(server): State<Arc<Server>>,
    headers: HeaderMap,
    axum::extract::Path(id): axum::extract::Path<u64>,
) -> Response {
    if let Some(refused) = compact_refused(&server, &headers) {
        return refused;
    }
    let state = server
        .compactions
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .state(id);
    let body = match state {
        None => return fail(StatusCode::NOT_FOUND, &format!("no compaction job {id}")),
        Some(compact::State::Running) => json!({"ok": true, "job": id, "state": "running"}),
        Some(compact::State::Done(report)) => {
            json!({"ok": true, "job": id, "state": "done", "report": report})
        }
        Some(compact::State::Failed(error, report)) => {
            json!({"ok": true, "job": id, "state": "failed", "error": error, "report": report})
        }
    };
    Json(body).into_response()
}

/// A compaction thread, from its first look to its job's last word: which days, each of
/// them in turn, and how the job ended.
///
/// `job` is the number a request was answered with. The scheduler's thread comes with
/// none, and **takes one only once it has a day to try**: a look that finds nothing due,
/// or only days still waiting out a failure, is not a job - it would fill the kept jobs
/// with empty ones and refuse a request that arrived while it looked.
///
/// **The due days are listed here, on this thread.** The listing takes the flush's lock,
/// which a flush holds across its writes; taken on the runtime it would park a worker
/// behind the disk.
///
/// **A failure here is not a write failure.** It is logged, kept for `/stats` and
/// remembered by the scheduler; the store's health, which `/ingest` and `/health` answer
/// from, is the flush's alone and is not touched.
fn compaction(server: &Server, day: Option<i64>, threads: usize, job: Option<u64>) {
    let jobs = || server.compactions.lock().unwrap_or_else(|e| e.into_inner());
    let every = server.storage.compact_interval;
    let scheduled = job.is_none();
    // Held from the moment there is a job, so that nothing which ends this thread - the
    // panic caught below, or one that is not - leaves the job `running`.
    let ending = |id| compact::Ending::of(&server.compactions, id);
    let asked = job.map(ending);
    let Some(store) = server.stores.disk_store() else {
        if let Some(asked) = asked {
            let why = compact::CompactError::NoDisk.to_string();
            let state = compact::State::Failed(why, compact::Report::default());
            asked.finish(state, false);
        }
        return;
    };
    let mut days = compact::plan(store, day);
    let (id, ending) = match job.zip(asked) {
        Some(asked) => asked,
        None => {
            // A day that failed and has nothing left to compact - a load replaced it - is
            // never tried again, so its failure is dropped here or never. Listed before
            // the jobs' lock is taken: the two locks are never held together.
            let compactable = store.compactable_days();
            let mut jobs = jobs();
            jobs.forget_except(&compactable);
            days.retain(|planned| jobs.wait(*planned, every).is_none());
            if days.is_empty() {
                return;
            }
            // None while a request's job runs, or once shutdown began: the next look.
            let Some(id) = jobs.start() else {
                return;
            };
            (id, ending(id))
        }
    };
    log(&format!(
        "compaction job {id} - {}",
        match day {
            Some(day) => kibbr_storage::store::disk::day_name(day),
            None => format!("{} day(s) due", days.len()),
        }
    ));
    let opts = CompactOptions {
        threads,
        cancel: Arc::clone(&server.compact_cancel),
    };
    let mut noted = 0;
    // A panic is a failed job and not a job running for ever, as for a load. Whatever it
    // had written is unnamed, and the next start removes it.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        compact::run(store, &days, &opts, &mut |planned, result| {
            noted += 1;
            let wait = {
                let mut jobs = jobs();
                jobs.note(planned, result);
                jobs.wait(planned, every).filter(|_| scheduled)
            };
            log(&match result {
                Ok(done) => format!("compaction job {id} - {}", compact::line(done)),
                Err(e @ (StoreError::Cancelled { .. } | StoreError::InputsChanged { .. })) => {
                    format!("compaction job {id} - {}", error::chain(e))
                }
                Err(e) => format!(
                    "WARN compaction job {id} - {}{}",
                    error::chain(e),
                    wait.map_or_else(String::new, |left| format!(
                        "; the day is left for {}s, unless its files change first",
                        left.as_secs().max(1)
                    ))
                ),
            });
        })
    }));
    let state = match result {
        Ok(ran) => match ran.failed.first() {
            None => compact::State::Done(ran.report),
            Some(first) => {
                let mut why = error::chain(first);
                if ran.failed.len() > 1 {
                    why.push_str(&format!(
                        "; and {} more day(s) failed, each in this server's log",
                        ran.failed.len() - 1
                    ));
                }
                compact::State::Failed(why, ran.report)
            }
        },
        Err(_) => {
            let why = "the compaction failed on a bug in this build (a panic); a file it wrote \
                       and had not named is removed by the next start";
            log(&format!("WARN compaction job {id} - {why}"));
            // The day it was on, remembered as failed: a look every interval would
            // otherwise walk into the same panic every interval.
            if let Some(planned) = days.get(noted) {
                let failed = Err(StoreError::CompactFailed {
                    day: kibbr_storage::store::disk::day_name(planned.0),
                    source: Box::new(StoreError::Io {
                        template_id: 0,
                        source: std::io::Error::other(why),
                    }),
                });
                jobs().note(*planned, &failed);
            }
            compact::State::Failed(why.to_string(), compact::Report::default())
        }
    };
    ending.finish(state, !days.is_empty());
}

/// Look for days that are due, every interval, until the token is cancelled.
///
/// **This task keeps the clock and nothing else.** It watches the token, asks whether a
/// load or a compaction is running, and starts a thread; the thread lists the due days and
/// compacts them (`compaction`). Nothing here takes the store's flush lock, so a slow
/// flush cannot park a runtime worker behind it.
///
/// Not part of `flush_loop`: a compaction awaited there would hold up every tick and the
/// memory relief with it.
///
/// A look is skipped while a load runs - a load that replaces a day would make a rebuild
/// of it wasted work, and the two would share the cores - and while the last flush tick
/// could not write, since a rebuild needs the day's size again on a disk that is already
/// refusing.
async fn compact_loop(server: Arc<Server>, token: CancellationToken) {
    let every = server.storage.compact_interval;
    // Zero is "never look", and is returned on before it reaches an interval, which
    // panics on a period of zero.
    if !server.storage.is_durable() || every.is_zero() {
        return;
    }
    let mut tick = tokio::time::interval(every);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The first tick completes immediately, and is consumed: nothing is compacted the
    // moment a server starts, so one that a compaction brought down comes back up and
    // serves for an interval before it tries again.
    tick.tick().await;
    loop {
        tokio::select! {
            () = token.cancelled() => return,
            _ = tick.tick() => {}
        }
        look_for_due_days(&server);
    }
}

/// Whether this look is left to the next tick: a load is running, or the last flush tick
/// could not write. Why each is in `compact_loop`.
fn look_waits(server: &Server) -> bool {
    server.stores.is_degraded() || load_running(server)
}

/// One look: start the scheduler's thread, unless the look waits (`look_waits`), a
/// compaction is running, shutdown has begun, or the last look's thread is still at it.
fn look_for_due_days(server: &Arc<Server>) {
    if look_waits(server) {
        return;
    }
    let mut jobs = server.compactions.lock().unwrap_or_else(|e| e.into_inner());
    if jobs.is_stopping() || jobs.running().is_some() {
        return;
    }
    // Joined only once it has finished: a thread still looking may be waiting for the
    // lock held here.
    match jobs.scheduled.take() {
        Some(last) if !last.is_finished() => {
            jobs.scheduled = Some(last);
            return;
        }
        Some(last) => {
            let _ = last.join();
        }
        None => {}
    }
    let held = Arc::clone(server);
    let spawned = std::thread::Builder::new()
        .name("compact-due".to_string())
        .spawn(move || compaction(&held, None, load::server_threads(), None));
    match spawned {
        Ok(handle) => jobs.scheduled = Some(handle),
        Err(e) => log(&format!(
            "WARN compaction - starting the look for due days: {e}"
        )),
    }
}

/// Shutdown has begun: tell the running compaction to stop, and start no other.
///
/// Called the moment the signal arrives, while the last requests are still draining,
/// rather than when the compaction is waited for: the earlier it hears, the less there is
/// to wait for. The flag is raised under the jobs' lock, which both ways of starting a job
/// take their number under, so none starts after it.
fn cancel_compactions(server: &Server) {
    let mut jobs = server.compactions.lock().unwrap_or_else(|e| e.into_inner());
    jobs.stop();
    server.compact_cancel.store(true, Ordering::Relaxed);
}

/// Wait for the compaction threads: after the final flush, before the lock is let go.
///
/// **After the final flush and not before it.** A process told to stop may be killed
/// before it has finished, and a kill before the flush costs the next start a replay of
/// every row still buffered: waiting here first would put a compaction's last file between
/// a SIGTERM and the write that saves it. After it, a kill during this wait costs
/// nothing: a rebuilt file the manifest does not name is removed by the next start. Waiting
/// later is safe because a compaction adds no row and no template: it and the flush meet
/// only under the store's lock, one after the other.
///
/// **Before the lock is released**, because a thread still writing the directory after it
/// would be writing beside whichever process took the lock next.
fn join_compactions(server: &Server) {
    cancel_compactions(server);
    let threads = {
        let mut jobs = server.compactions.lock().unwrap_or_else(|e| e.into_inner());
        [jobs.asked.take(), jobs.scheduled.take()]
    };
    for thread in threads.into_iter().flatten() {
        if !thread.is_finished() {
            log("waiting for the running compaction to stop");
        }
        let _ = thread.join();
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
/// reason, as `Template::path` does for the engine's plan.
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
/// The first is what the next start would put back from the write-ahead log - and, with
/// storage in memory, what an exit costs. The second exists because a disk that has gone
/// away under `data/` is otherwise invisible: `/ingest` keeps answering 200, rightly - the
/// line is durable in the log - while no row reaches a file. `wal` is the log itself:
/// where it has got to, where the store has, how far behind them the checkpoint is, and
/// what this start had to read back.
async fn get_stats(State(server): State<Arc<Server>>) -> Response {
    let stores = &server.stores;
    let (open_segments, max_open_segments) = stores.open_files();
    let (buffered_bytes, max_buffer_bytes) = stores.buffer_bytes();
    let wal = server.wal.as_ref().map(Wal::stats);
    let uptime = server.started.elapsed().as_secs_f64();
    let ingested = server.lines_ingested.load(Ordering::Relaxed);
    let queries = server.queries.load(Ordering::Relaxed);
    let shed: serde_json::Map<String, Value> = server
        .door
        .shed
        .each()
        .into_iter()
        .map(|(cause, count)| (cause.to_string(), json!(count)))
        .collect();

    Json(json!({
        "uptime_seconds": uptime,
        "started_at": rfc3339_at(server.started_at),
        "storage": {
            "kind": stores.kind(),
            "durable": server.storage.is_durable(),
            "note": if server.storage.is_durable() {
"a line is written to the write-ahead log under wal/ and flushed before /ingest acknowledges it, so an unclean exit loses no acknowledged line: the next start puts back every line no row group holds, once; one shared data file per UTC day, named in manifest.log; a row group closes at 8,192 rows, 8MB or the row group timeout, at most max_open_segments day files are open for writing at once and a row for a day with none open past that seals the least recently written and opens its own day's, and a day is rebuilt into full row groups in finished files the day after its last live row, once all of it is behind the log's checkpoint"
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
            // How long a row group may stay open: what a quiet template's rows wait in
            // the log and in memory before they reach a file.
            "row_group_timeout_ms": server.storage.row_group_timeout.as_millis() as u64,
            // The write-ahead log; null when storage is memory. `last_seq` is the last
            // position made durable, `applied` the last handed to the store, and every
            // position up to `checkpoint` is in a row group - the segments wholly behind
            // it are gone. `healthy` false is /ingest answering 503. `replay` is what
            // this start read back: `applied` put back in memory, `covered` skipped
            // because a row group already held them, `held` left in the log.
            "wal": wal.as_ref().map(|wal| wal_stats(&server, wal)),
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
            // How often a day that has ended is looked for; 0 is never. And what the
            // looks and `kibbr compact` have done: `runs` counts the jobs that tried a
            // day, `days_compacted` the days rebuilt - one found with nothing to compact
            // is not one - `last` is the last of those, and `last_error` the latest
            // failure among the days still failing. From the jobs' own lock: this does
            // not wait on a flush.
            "compact_interval_ms": server.storage.compact_interval.as_millis() as u64,
            "compaction": server
                .compactions
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .stats(),
            "forced_closes": stores.forced_closes(),
            "write_failures": stores.write_failures(),
            "last_write_error": stores.last_write_error(),
            // True while the last flush tick failed, which is also while /health says
            // degraded. The first tick that writes everything clears it. /ingest goes on
            // answering 200 meanwhile: its lines are in the log.
            "degraded": stores.is_degraded(),
            // On disk this is the segments' bytes as of the last row group closed, plus what the
            // buffer would add. In memory it is payload bytes - key text, string values,
            // and a scalar's fixed width - and the resident size is larger, because
            // allocator slack and the hash tables are not in it. Estimated either way.
            // The log's segments count toward it: they are bytes this database holds.
            "approx_bytes": stores.approx_bytes() + wal.as_ref().map_or(0, |wal| wal.bytes),
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
            // design working. A body shed at the door was never read, so it is in none
            // of the line counters above; one the log shed was, and is in two of them.
            "requests_shed": server.door.shed.total(),
            // How long a body may be kept waiting before it is shed.
            "ingest_wait_ms": server.api.ingest_wait.as_millis() as u64,
            "ingest_concurrency": server.api.ingest_concurrency,
            "ingest_bodies": ingest_bodies(&server.api),
            // Permits for the parse in use, and places between the door and an answer in
            // use: the second less the first is the bodies waiting on the log.
            "parsing": server
                .api
                .ingest_concurrency
                .saturating_sub(server.ingest_permits.available_permits()),
            "bodies_inside": ingest_bodies(&server.api)
                .saturating_sub(server.ingest_bodies.available_permits()),
            // Bodies waiting at the door now, unread; how many ever have; and how long
            // they waited, all together.
            // `line_max` is the most that may wait there at once, and null while the
            // line has no bound: at the default wait, where it never had one.
            "door": {
                "waiting": server.door.waiting.load(Ordering::Relaxed),
                "waits": server.door.waits.load(Ordering::Relaxed),
                "waited_ms": server.door.waited_micros.load(Ordering::Relaxed) / 1000,
                "line_max": server.line_max(),
            },
            // `requests_shed`, by what there was none of.
            "shed": shed,
        },
        "queries": {
            "served": queries,
            "per_second": rate(queries, uptime),
            // Runs of a day's row groups whose rows were chosen on more threads than the
            // query's own, over the life of the process. Zero on a server with one scan
            // thread, and it stays where it is while bodies are being ingested: a query
            // asked then is answered on its own thread.
            "scan_fan_outs": server.fan.as_ref().map_or(0, |fan| fan.fanned()),
            // What this server resolved: the threads one query may choose a day's rows
            // on, its own among them - 1 where there is nobody to share with (the memory
            // store, or `--scan-threads` 0 or 1) - and the rows under which a run of row
            // groups is never shared out. Without them a count of 0 above reads the same
            // for one thread as for four with nothing worth sharing.
            "scan_threads": scan_threads(&server),
            "scan_fan_rows": server.storage.scan_fan_rows,
        },
    }))
    .into_response()
}

/// `/stats`'s `storage.wal`: where the log has got to, and what is waiting on it.
///
/// Two objects joined, because one `json!` holds only so many keys before the compiler
/// stops expanding it.
fn wal_stats(server: &Server, wal: &wal::Stats) -> Value {
    let mut stats = json!({
        "dir": server.wal.as_ref().map(|w| w.dir().display().to_string()),
        "stream": format!("{:016x}", wal.stream),
        "segments": wal.segments,
        "first_segment": wal.first_segment,
        "bytes": wal.bytes,
        "last_seq": wal.last_seq,
        "applied": wal.applied,
        "checkpoint": wal.checkpoint,
        "flushes": wal.flushes,
        "healthy": wal.healthy,
        "last_error": wal.last_error,
        "segment_bytes": server.storage.wal_segment_bytes,
        "batch_size": server.storage.wal_batch_lines,
        "held_rows": server.held.rows(),
        "replay": {
            "records": server.replayed.records,
            "applied": server.replayed.applied,
            "covered": server.replayed.covered,
            "held": server.replayed.held,
            "replaced": server.replayed.replaced,
            "behind_checkpoint": server.replayed.behind,
            "torn": server.replayed.torn,
            "corrupt": server.replayed.corrupt,
            "seconds": server.replayed.seconds,
        },
    });
    let queue = json!({
        // The most bytes of lines that may be in the log and not yet in the store;
        // those waiting for its writer now; those it has written that the store does
        // not have yet; and the most the two have come to together. The two are each
        // exact and are not read together, so their sum can miss the one body the
        // writer is taking at that instant, and never counts one twice.
        "queue_max_bytes": server.storage.wal_queue_bytes,
        "queue_bytes": wal.queue_bytes,
        "unapplied_bytes": wal.unapplied_bytes,
        "queue_peak_bytes": wal.queue_peak_bytes,
        // Bodies parsed and waiting for room in the log now, and how many ever have;
        // those whose wait ran out, which were shed; and those let go because their
        // sender hung up while they waited.
        "queue_waiting": wal.queue_waiting,
        "queue_waits": wal.queue_waits,
        "queue_sheds": wal.queue_sheds,
        "queue_left": wal.queue_left,
    });
    if let (Value::Object(stats), Value::Object(queue)) = (&mut stats, queue) {
        stats.extend(queue);
    }
    stats
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

/// `Connection: close` on every answer after which this server closes the connection
/// (RFC 9112 section 9.6).
///
/// An answer written while the request's body is still unread leaves half a request on
/// the connection, and it cannot carry another: the server closes it once the answer is
/// out. A client that is not told sends its next request into the closed socket and gets
/// no answer at all. With the log failing that was every other batch of a shipper that
/// keeps its connection, where each should have been a 503 it knows what to do with.
///
/// **One layer, and not a header on each refusal.** What closes the connection is the body
/// left unread, not which answer was built. `cannot_log` and `log_busy` come from the door
/// before the body is read, and from the handler after it - where the connection stays
/// open and must not be told to close. So the body is wrapped in `Watched`, and the answer
/// is marked when the body it was given was not read to its end. That covers the answers
/// this file does not build as well: axum's 413 from a body limit, its 404 and 405, and
/// a `GET` that was sent a body.
///
/// The header is not only a report. With it in the answer the connection is closed after
/// the answer whatever becomes of the rest of the body, so the two cannot disagree. A
/// refused body small enough to have arrived whole with its head used to leave its
/// connection open, by the luck of one read; it is closed now, and said to be.
///
/// Nothing is read here to keep the connection: the refusal comes before the body is read,
/// and stays that way.
async fn say_close(request: Request, next: Next) -> Response {
    // A request with no body has nothing left unread, whatever answers it.
    if request.body().is_end_stream() {
        return next.run(request).await;
    }
    let version = request.version();
    let ended = Arc::new(AtomicBool::new(false));
    let request = request.map(|inner| {
        Body::new(Watched {
            inner,
            ended: Arc::clone(&ended),
        })
    });
    let mut response = next.run(request).await;
    if !ended.load(Ordering::Relaxed) {
        // `insert`, so an answer that came with a `Connection` header leaves with this one
        // and not with two.
        response
            .headers_mut()
            .insert(header::CONNECTION, HeaderValue::from_static("close"));
        // In the client's own version. An answer of HTTP/1.1 to a client of HTTP/1.0 that
        // asked to keep its connection is given `keep-alive` on its way out, over the
        // header above, and the connection is closed all the same; one of HTTP/1.0 keeps
        // the header it came with.
        *response.version_mut() = version;
    }
    response
}

/// A request body that records whether it was read to its end: see `say_close`.
struct Watched {
    inner: Body,
    ended: Arc<AtomicBool>,
}

impl HttpBody for Watched {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, axum::Error>>> {
        let polled = Pin::new(&mut self.inner).poll_frame(cx);
        // The end is `None`, or a frame with nothing after it. A body with a
        // `Content-Length` ends on its last data frame, and a reader that stops there has
        // read all of it: the body limit does, when the frame that crosses the limit is
        // also the last.
        let ended = match &polled {
            Poll::Ready(None) => true,
            Poll::Ready(Some(Ok(_))) => self.inner.is_end_stream(),
            _ => false,
        };
        if ended {
            self.ended.store(true, Ordering::Relaxed);
        }
        polled
    }

    // Both as the body underneath has them, so the body limit and the `Bytes` extractor
    // see what they saw before this was put around it.
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
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
        .route("/compact", post(post_compact))
        .route("/compact/{job}", get(get_compact))
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
        // Last, so outermost of all: outside `admit` and outside every body limit. It has
        // to see the body before anything can refuse it or stop reading it, to know that
        // the answer went out with the body unread - and then the answer tells the client
        // that the connection closes.
        .layer(axum::middleware::from_fn(say_close))
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
    // The scan threads' last owner. Declared before the runtime, so let go after it: the
    // pool's own `Drop` then runs here, on the main thread, when no runtime thread exists
    // and no `async fn` is on the stack (see `store::fan`).
    let scan: OnceLock<Arc<Fan>> = OnceLock::new();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .max_blocking_threads(ingest_bodies(&api) + OTHER_BLOCKING_THREADS)
        .build()
        .map_err(ServeError::Runtime)?;
    runtime.block_on(serve(host, port, limits, api, storage, schema, &scan))?;
    Ok(())
}

async fn serve(
    host: &str,
    port: u16,
    limits: Limits,
    api: ApiLimits,
    storage: Storage,
    schema: Schema,
    scan: &OnceLock<Arc<Fan>>,
) -> Result<(), ServeError> {
    // Read-only, so it runs before the lock and the port: a data directory holding a
    // segment from another format version is not served at all, and nothing is listening
    // when the process exits over it. Everything that writes - catalog recovery, segment
    // recovery - waits for the directory's LOCK below.
    if let Some(base) = storage.base_path.as_deref() {
        kibbr_storage::store::disk::refuse_other_versions(base).map_err(storage_fatal)?;
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
            kibbr_storage::store::v3::file::new_file_id()
                .map(|id| kibbr_storage::store::manifest::hex(&id))
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
    // Handed to `run` before anything below can fail or answer a query, so that on every
    // way out of here the scan threads are let go by `run`, after the runtime.
    if let Some(fan) = &server.fan {
        let _ = scan.set(Arc::clone(fan));
    }
    // Where `kibbr load` finds this server - written once it can answer. An address of
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
                "WARN {e}; `kibbr load` and `kibbr compact` will not find this server"
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
    // And the clock compaction is looked for by. Held for the same reason: a look after
    // the wind-down would start a compaction nothing waits for.
    let compactor = tokio::spawn(compact_loop(Arc::clone(&server), token.clone()));

    let shutdown = {
        let token = token.clone();
        let server = Arc::clone(&server);
        async move {
            token.cancelled().await;
            // Whoever is waiting - at the door, or for room in the log - is refused now,
            // and a compaction is told to stop; the requests below then finish or, with
            // no room in the log, are shed.
            begin_stop(&server);
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
    // this line *is* "in-flight requests complete first" - but for a body whose client
    // hung up: its connection is finished and its blocking task is not. Every body's
    // permit taken back is every such task returned - its lines in the log and applied,
    // or refused - so the final flush below writes all there will be, and nothing of
    // `/ingest` runs on beside the shutdown or after the directory's lock is let go.
    let _ingest_done = server
        .ingest_bodies
        .acquire_many(ingest_bodies(&server.api) as u32)
        .await;
    //
    // Loads stop next: one past its commit finishes it, one before it removes its staging,
    // so the final flush and the trie snapshot below see the directory as it will stay - a
    // load committing beside them would publish rows the snapshot does not describe.
    stop_loads(&server);

    // The flush task is joined before the wind-down and not after: a tick arriving between
    // the final flush and the trie snapshot would write rows the snapshot does not
    // describe, and a tick beside the final flush would only queue behind it.
    let _ = flusher.await;
    // No look starts a compaction from here on; the one running, if any, has been told to
    // stop and is waited for inside the sequence, behind the final flush.
    let _ = compactor.await;
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
        let flushed = tokio::task::spawn_blocking(move || {
            let summary = held.stores.flush_all(FlushKind::Tick);
            // Behind the flush, never ahead of it: the checkpoint moves past a row only
            // once the row group holding it has been flushed and published.
            move_checkpoint(&held);
            summary
        });
        let flushed = flushed.await;
        // Said to whoever is waiting at the door for the buffers, whatever the flush
        // did: each looks again, and one that finds them still full goes on waiting.
        server.flushed.notify_waiters();
        match flushed {
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

/// After a flush: ask a log that cannot write to try again, and move its checkpoint up
/// behind the oldest row that is still only in memory.
///
/// **The checkpoint moves while the server runs**, here, every tick. Left to anything
/// rarer - a shutdown, a compaction - the log would grow for as long as the process lived.
/// What bounds it is the oldest row no row group holds yet: a buffered one, or one the
/// store refused (`Held`), which waits for the next start.
fn move_checkpoint(server: &Server) -> Option<wal::Advanced> {
    let wal = server.wal.as_ref()?;
    wal.probe();
    let (moved, problem) =
        wal.advance(
            || match (server.stores.oldest_position(), server.held.from()) {
                (Some(buffered), Some(held)) => Some(buffered.min(held)),
                (buffered, held) => buffered.or(held),
            },
        );
    // Told whatever else happened: the checkpoint is where it is whether or not its file
    // could be written, and the scheduler's compaction waits on it.
    if let (Some(moved), Some(disk)) = (moved, server.stores.disk_store()) {
        disk.note_checkpoint(moved.checkpoint);
    }
    match problem {
        None => server.checkpoint_failing.store(false, Ordering::Relaxed),
        // Said once, not every tick: nothing is lost by it - a start reads a little more
        // of the log, or finds a segment this could not delete and deletes it.
        Some(e) => {
            if !server.checkpoint_failing.swap(true, Ordering::Relaxed) {
                log(&format!(
                    "WARN wal - the checkpoint moved and could not be kept: {e}"
                ));
            }
        }
    }
    moved
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

/// The fixed shutdown order, with the steps a memory server does not have named as
/// skipped.
///
/// ```text
/// stop accepting → final flush → WAL checkpoint → trie.json → close stores → close WAL
/// ```
///
/// Printing the skipped steps rather than omitting them is the point: an operator watching
/// a clean shutdown scroll past should see exactly which durability they have and which
/// they do not.
///
/// **No row depends on this running to the end.** Every line a body was told `200` for is
/// in the write-ahead log, so a final flush that fails - or a `SIGKILL` that lands halfway
/// through - costs the next start a replay and nothing else. The flush is still done: a
/// clean stop then leaves every row in a sealed file and nothing to read back.
fn shutdown_sequence(server: &Server) -> Result<(), ServeError> {
    log("in-flight requests complete");

    if server.storage.is_durable() {
        let summary = server.stores.flush_all(FlushKind::Final);
        // Rows still in a buffer once the final flush has done all it can. Not lost: they
        // are in the log, and the next start puts them back.
        let unwritten = match summary.failed {
            0 => None,
            leaves => Some((leaves, server.stores.buffered())),
        };
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
            match unwritten {
                None => String::new(),
                Some((leaves, rows)) => format!(
                    ", {leaves} leaf/leaves FAILED: {} row(s) were not written to a row \
                     group. They are in the write-ahead log, and the next start puts them back",
                    commas(rows)
                ),
            },
        ));
    } else {
        log("final flush - skipped, storage is memory (--data-dir puts the stores on disk)");
    }

    // Not one of the six steps: it writes nothing a restart needs. Here because a
    // compaction's own flush belongs behind the one above.
    join_compactions(server);

    // Behind the final flush, so that a clean stop leaves the checkpoint at the end of the
    // log and the next start with nothing to read back.
    match &server.wal {
        Some(wal) => {
            let moved = move_checkpoint(server);
            let stats = wal.stats();
            log(&format!(
                "WAL checkpoint - position {} of {}{}; {} segment(s), {} byte(s)",
                commas(stats.checkpoint),
                commas(stats.last_seq),
                match moved {
                    Some(moved) if moved.deleted > 0 =>
                        format!(", {} segment(s) behind it deleted", moved.deleted),
                    _ => String::new(),
                },
                stats.segments,
                commas(stats.bytes),
            ));
        }
        None => log("WAL checkpoint - skipped, storage is memory"),
    }

    match server.storage.trie_file.as_deref() {
        Some(path) => match kibbr_storage::triefile::save(&server.trie, path) {
            Ok(bytes) => log(&format!(
                "trie.json - {} {} byte(s), {} template(s)",
                path.display(),
                commas(bytes),
                commas(server.trie.template_count())
            )),
            // Not a reason to fail the exit. Nothing loads the snapshot while the catalog
            // exists: it is there for a human to read.
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
    let kept = if server.storage.is_durable() {
        "written"
    } else {
        "discarded, they were only in memory"
    };
    log(&format!(
        "close stores - {} store(s), {} row(s) {kept}{}{}",
        commas(stores.open() as u64),
        commas(held.saturating_sub(unwritten)),
        match (server.storage.is_durable(), unwritten) {
            (true, 0) => String::new(),
            (true, n) => format!(
                ", {} row(s) still buffered: in the write-ahead log, put back at the next start",
                commas(n)
            ),
            (false, _) => String::new(),
        },
        match server.held.rows() {
            0 => String::new(),
            n => format!(
                ", {} row(s) the store refused: in the write-ahead log, stored at the next start",
                commas(n)
            ),
        },
    ));

    match &server.wal {
        Some(wal) => {
            wal.close();
            log("close WAL - closed");
        }
        None => log("close WAL - skipped, storage is memory"),
    }

    // **Always 0 once it gets here.** The exit code is the last thing a process says, and
    // it used to be 1 for a final flush that could not write every row - rightly, while a
    // buffered row was only ever in memory: 20,000 lines accepted, 10,625 back after a
    // restart. Those rows are in the log now. A stop that leaves every acknowledged line
    // durable has lost nothing, and an orchestrator told otherwise would be told a lie.
    log("exit 0");
    Ok(())
}

/// The threads one query of this server may choose a day's rows on, its own among them:
/// what was resolved from the flag, the variable and the cores, and 1 where there is
/// nobody to share with - the memory store, or a disk store told 0 or 1.
fn scan_threads(server: &Server) -> usize {
    server.fan.as_ref().map_or(1, |_| server.storage.scan_threads)
}

/// The banner's line about the scan threads. The default depends on the machine, and
/// nothing else at startup says what it came to.
fn scan_line(server: &Server) -> String {
    match scan_threads(server) {
        1 => "  scan        1 thread to a query - no scan is shared out".to_string(),
        threads => format!(
            "  scan        {threads} threads to a query, its own among them; a run of a\n\
             \x20             day's row groups under {} rows is never shared out, nor\n\
             \x20             any while an /ingest body is between the door and its answer",
            commas(server.storage.scan_fan_rows),
        ),
    }
}

/// The banner's lines about `/ingest`: the limits on a body, how long one may be kept
/// waiting and for what, and the bound on the line at the door when it has one.
fn ingest_line(server: &Server) -> String {
    let api = server.api;
    let wait = api.ingest_wait.as_millis();
    let waits = match &server.wal {
        Some(_) => format!(
            "a body waits up to {wait}ms for a slot, for a flush of full buffers or\n\
             \x20             for room in the log, and is then answered 503 with a Retry-After"
        ),
        None => format!(
            "a body waits up to {wait}ms for a slot, and is then answered 503 with\n\
             \x20             a Retry-After rather than left to time out"
        ),
    };
    let line = match server.line_max() {
        Some(max) => format!(
            "\n\x20             at most {max} wait at the door at once; one more is answered 503"
        ),
        None => String::new(),
    };
    format!(
        "  ingest      {} byte body, {} byte decompressed, {} at once\n\
         \x20             {waits}{line}",
        commas(api.max_body_bytes as u64),
        commas(api.max_decompressed_bytes as u64),
        api.ingest_concurrency,
    )
}

/// The startup banner.
///
/// stderr, and not stdout, so that `kibbr serve >/dev/null` still prints the paragraph
/// about losing data. A warning a redirect can silence is a warning that will be silenced
/// by the first person who pipes the process into a log file, and this is something to
/// say rather than hide.
fn banner(addr: SocketAddr, server: &Server) {
    let limits = server.limits;
    let lines = [
        String::new(),
        "kibbr serve".to_string(),
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
                 \x20                       reaches a file when its row group closes - 8,192\n\
                 \x20                       rows, 8MB, or {}ms, whichever comes first. The\n\
                 \x20                       thresholds are checked every {}ms. At most {} day\n\
                 \x20                       files are open for writing at once; past that a\n\
                 \x20                       row for a day with none open seals the least\n\
                 \x20                       recently written and opens its own day's. Keep\n\
                 \x20                       it under `ulimit -n`.\n\
                 \x20                       SIGTERM closes and seals everything before exiting.\n\
                 \x20             compact   a day's small row groups are rebuilt into full ones,\n\
                 \x20                       in finished files, the day after its last live\n\
                 \x20                       row. {}\n\
                 \x20             catalog   {}/catalog.log - a template is written here, and\n\
                 \x20                       fsynced, before the line that made it counts as\n\
                 \x20                       ingested; trie.json is a readable snapshot nothing\n\
                 \x20                       loads while the catalog exists.\n\
                 \x20             wal       {}/wal - a line is flushed here before /ingest\n\
                 \x20                       acknowledges it. A kill -9 or a power cut loses no\n\
                 \x20                       acknowledged line: the next start puts each back, once.\n\
                 \x20                       At most {} bytes of lines are in the log and\n\
                 \x20                       not yet in the store; a body past that waits for room.",
                base.display(),
                trie_file
                    .as_ref()
                    .map_or_else(|| "none".to_string(), |p| p.display().to_string()),
                server.storage.row_group_timeout.as_millis(),
                server.storage.flush_interval.as_millis(),
                commas(server.storage.max_open_segments as u64),
                match server.storage.compact_interval.as_millis() {
                    0 => "Not looked for (--compact-interval-ms 0):\n\
                          \x20                       `kibbr compact` does it when asked."
                        .to_string(),
                    ms => format!(
                        "Looked for every {ms}ms; `kibbr compact`\n\
                         \x20                       does it when asked."
                    ),
                },
                base.display(),
                base.display(),
                commas(server.storage.wal_queue_bytes as u64),
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
        "              POST /load        a bulk load job: what `kibbr load` sends".to_string(),
        "              POST /compact     a compaction job: what `kibbr compact` sends".to_string(),
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
        ingest_line(server),
        scan_line(server),
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
///
/// **A line that cannot be written is dropped, as `error::report` drops one.** `eprintln!`
/// panics when stderr is gone - the log collector at the other end of the pipe went
/// away - and this is called from the flush task and from the threads a load and a
/// compaction run on: the panic would end the work for want of somewhere to say it was
/// being done.
fn log(message: &str) {
    let _ = writeln!(std::io::stderr().lock(), "kibbr: {message}");
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A server over a store in memory: the job registries and the shutdown steps are the
    /// same, and nothing is opened.
    fn server() -> Arc<Server> {
        let server = Server::new(
            Utc::now(),
            Limits::default(),
            ApiLimits::default(),
            Storage::default(),
            Schema::default(),
            None,
        );
        Arc::new(server.expect("a server in memory"))
    }

    /// The wait is what keeps a compaction's last writes inside the directory's lock: a
    /// shutdown that dropped the thread's handle would let the lock go with the thread
    /// still writing. A thread that was told to stop is usually gone by the time it is
    /// waited for, so only one that takes its time shows the wait is there.
    #[test]
    fn shutdown_waits_for_a_compaction_thread_still_running() {
        for by_request in [true, false] {
            let server = server();
            let ended = Arc::new(AtomicBool::new(false));
            let thread = {
                let ended = Arc::clone(&ended);
                std::thread::spawn(move || {
                    std::thread::sleep(Duration::from_millis(200));
                    ended.store(true, Ordering::SeqCst);
                })
            };
            {
                let mut jobs = server.compactions.lock().expect("the lock");
                match by_request {
                    true => jobs.asked = Some(thread),
                    false => jobs.scheduled = Some(thread),
                }
            }

            join_compactions(&server);
            assert!(
                ended.load(Ordering::SeqCst),
                "shutdown went on past a compaction thread still running (asked: {by_request})"
            );
            assert!(server.compact_cancel.load(Ordering::Relaxed));
            let mut jobs = server.compactions.lock().expect("the lock");
            assert!(jobs.is_stopping() && jobs.start().is_none());
            assert!(jobs.asked.is_none() && jobs.scheduled.is_none());
        }
    }

    /// The scheduler's look is left to the next tick while a load job runs - no thread is
    /// started for it - and taken once the load has ended, done or failed.
    #[test]
    fn a_look_for_due_days_waits_while_a_load_job_runs() {
        let server = server();
        let job = Arc::new(LoadJob {
            control: load::Control::default(),
            state: std::sync::Mutex::new(LoadState::Running),
        });
        server
            .loads
            .lock()
            .expect("the lock")
            .jobs
            .insert(1, Arc::clone(&job));
        let looked = || {
            server
                .compactions
                .lock()
                .expect("the lock")
                .scheduled
                .take()
        };

        assert!(look_waits(&server));
        look_for_due_days(&server);
        assert!(looked().is_none(), "a look was taken beside a running load");

        *job.state.lock().expect("the lock") = LoadState::Failed("why".to_string());
        assert!(!look_waits(&server));
        look_for_due_days(&server);
        let thread = looked().expect("no look was taken once the load had ended");
        thread.join().expect("the look's thread");
    }

    /// And while the last flush tick could not write: the data directory is made
    /// read-only under a row, so the tick that would write it fails, and writable again
    /// for the tick that clears it.
    #[cfg(unix)]
    #[test]
    fn a_look_for_due_days_waits_while_the_last_flush_could_not_write() {
        use std::os::unix::fs::PermissionsExt;

        let dir = std::env::temp_dir().join(format!("kibbr-serve-look-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let storage = Storage {
            base_path: Some(dir.clone()),
            trie_file: Some(dir.join("trie.json")),
            row_group_timeout: Duration::ZERO,
            ..Storage::default()
        };
        std::fs::create_dir_all(&dir).expect("mkdir");
        let server = Server::new(
            Utc::now(),
            Limits::default(),
            ApiLimits::default(),
            storage,
            Schema::default(),
            None,
        );
        let server = Arc::new(server.expect("a server on disk"));
        let data = dir.join(kibbr_storage::store::disk::DATA_DIR);
        let mode = |mode| std::fs::set_permissions(&data, std::fs::Permissions::from_mode(mode));
        mode(0o555).expect("chmod -w");
        if std::fs::create_dir(data.join("probe")).is_ok() {
            // Root, or a filesystem that ignores the mode: nothing here can fail a flush.
            let _ = std::fs::remove_dir_all(&dir);
            return;
        }
        let looked = || {
            server
                .compactions
                .lock()
                .expect("the lock")
                .scheduled
                .take()
        };

        let line = &b"{\"ts\":\"2024-01-15T00:00:00Z\",\"level\":\"info\"}\n"[..];
        let now = Utc::now();
        db::read_lines(
            line,
            &mut |_, parsed| db::ingest_line(&server.trie, Some(&server.stores), &parsed, now),
            &mut |_| {},
        )
        .expect("the line is read");
        let flushed = server.stores.flush_all(FlushKind::Tick);
        let waited = look_waits(&server);
        look_for_due_days(&server);
        let taken = looked();
        let _ = mode(0o755);
        assert_eq!(
            flushed.failed, 1,
            "the flush wrote to a read-only directory"
        );
        assert!(waited, "a store that could not write did not hold the look");
        assert!(
            taken.is_none(),
            "a look was taken on a disk that is refusing"
        );

        let again = server.stores.flush_all(FlushKind::Tick);
        assert_eq!(again.failed, 0, "the flush still fails on a writable disk");
        assert!(!look_waits(&server));
        look_for_due_days(&server);
        let thread = looked().expect("no look was taken once a flush had written");
        thread.join().expect("the look's thread");
        drop(server);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A store whose leaves will not open while `shut` is set: what the disk store never
    /// does today, and what the log's applier has to survive if a store ever does.
    struct Refusing(Arc<AtomicBool>);

    impl kibbr_storage::store::Store for Refusing {
        fn open(
            &self,
            template_id: u64,
            time_column: &str,
        ) -> Result<Arc<dyn kibbr_storage::store::LeafStore>, StoreError> {
            if self.0.load(Ordering::Relaxed) {
                return Err(StoreError::Io {
                    template_id,
                    source: std::io::Error::other("the leaf will not open"),
                });
            }
            kibbr_storage::store::MemoryStore::new().open(template_id, time_column)
        }

        fn kind(&self) -> &'static str {
            "refusing"
        }

        fn base_path(&self) -> Option<&Path> {
            None
        }
    }

    /// A row the store refuses after the log has its line is not an error and not a
    /// loss: it is held - the checkpoint stays behind it - and so is every later row of
    /// its template, though the store would take them by then. A row group cut around
    /// the one that was skipped would say its line was stored, and the replay that
    /// should store it would step over it. Other templates go on.
    #[test]
    fn a_row_the_store_refuses_is_held_and_its_template_waits_with_it() {
        let shut = Arc::new(AtomicBool::new(true));
        let stores = StoreManager::new(Box::new(Refusing(Arc::clone(&shut))));
        let held = Held::new();
        let placed = |template_id| db::Placed {
            template_id,
            time_column: "ts".to_string(),
            row: kibbr_storage::store::Row::new(),
        };
        assert_eq!(held.from(), None);

        assert!(!held.apply(&stores, 40, placed(7)));
        assert_eq!((held.from(), held.rows()), (Some(40), 1));

        shut.store(false, Ordering::Relaxed);
        assert!(
            !held.apply(&stores, 41, placed(7)),
            "the template was let back in"
        );
        assert_eq!(stores.rows(), 0);
        assert!(held.apply(&stores, 42, placed(8)));
        assert_eq!(stores.rows(), 1);
        assert_eq!((held.from(), held.rows()), (Some(40), 2));
    }

    // ── A body of which nothing was taken ─────────────────────────────────────────────
    //
    // These call the handler itself, with the door's permits made by hand, so the status,
    // the headers and the body are the ones a client gets; nothing here needs a disk that
    // is full.

    /// What `/ingest` answers `body`: status, headers, and the JSON.
    fn answer(server: &Arc<Server>, body: &[u8]) -> (StatusCode, HeaderMap, Value) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a runtime");
        runtime.block_on(async {
            let permit = server.admit_now().expect("both permits");
            let response = post_ingest(
                State(Arc::clone(server)),
                axum::Extension(permit),
                axum::http::Uri::from_static("/ingest"),
                HeaderMap::new(),
                Ok(Bytes::copy_from_slice(body)),
            )
            .await;
            let (parts, body) = response.into_parts();
            let bytes = axum::body::to_bytes(body, usize::MAX)
                .await
                .expect("the response body");
            let report = serde_json::from_slice(&bytes).expect("the answer is JSON");
            (parts.status, parts.headers, report)
        })
    }

    /// A server in memory whose catalog refuses every append: what a full disk under
    /// `catalog.log` looks like to a line that needs a new template. `known` is taken
    /// first, while there is no catalog to ask, so its template exists.
    fn server_whose_catalog_cannot_write(tag: &str, known: &[u8]) -> Arc<Server> {
        let server = server();
        if !known.is_empty() {
            let (status, _, report) = answer(&server, known);
            assert_eq!(status, StatusCode::OK, "{report}");
        }
        let dir = std::env::temp_dir().join(format!(
            "kibbr-serve-catalog-{tag}-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("mkdir");
        server
            .trie
            .attach_catalog(Catalog::read_only(&dir.join("dead.log")).expect("read-only"));
        // The catalog keeps its handle, and nothing reads the file by name again.
        let _ = std::fs::remove_dir_all(&dir);
        server
    }

    /// No line taken, and every refusal the catalog's write: a 503, a `Retry-After`, and
    /// the errors still by line. It was a 200.
    #[test]
    fn a_body_the_catalog_could_write_none_of_is_a_503_with_its_errors_by_line() {
        let server = server_whose_catalog_cannot_write("none", b"");
        let body = b"{\"ts\":1705314600,\"a\":1}\n{\"ts\":1705314601,\"a\":2}\n\
                     {\"ts\":1705314602,\"b\":3}\n";
        let (status, headers, report) = answer(&server, body);

        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "three well-formed lines, none taken: {report}"
        );
        assert_eq!(
            headers.get(header::RETRY_AFTER).and_then(|v| v.to_str().ok()),
            Some(RETRY_AFTER_SECONDS.to_string().as_str()),
            "{headers:?}"
        );
        // The handler has the whole body, and does not itself ask for the connection to be
        // closed: that is `say_close`'s to say, and only of a body left unread.
        assert!(headers.get(header::CONNECTION).is_none(), "{headers:?}");
        assert_eq!(report["ok"], false);
        assert_eq!(report["ingested"], 0);
        assert_eq!(report["retry_after"], RETRY_AFTER_SECONDS, "{report}");
        let errors = report["errors"].as_array().expect("errors");
        assert_eq!(errors.len(), 3, "{report}");
        for (at, error) in errors.iter().enumerate() {
            assert_eq!(error["line"], at + 1, "{report}");
            let msg = error["msg"].as_str().unwrap_or_default();
            assert!(msg.starts_with("not ingested: catalog: "), "{msg}");
            assert!(msg.contains("dead.log"), "{msg}");
        }
        // The shape every other 503 here has, for a client that reads `error` and no
        // further: what to do with the body, and the first line's own reason.
        let said = report["error"].as_str().unwrap_or_default();
        assert!(said.contains("send it again"), "{report}");
        assert!(
            said.ends_with(errors[0]["msg"].as_str().unwrap_or("no message")),
            "{report}"
        );
        assert_eq!(server.trie.template_count(), 0, "a refused template exists");
    }

    /// Any line taken, or any line the sender got wrong, and the answer is the one it
    /// was: a 200 that lists what did not land. A 503 for the first would have the
    /// taken line sent and stored again; a 503 for the second would have the malformed
    /// line sent again for ever.
    #[test]
    fn a_body_with_a_line_taken_or_a_malformed_line_keeps_its_200() {
        let known = b"{\"ts\":1705314600,\"known\":1}\n";

        // A line of a template that exists is taken; the line beside it is not.
        let server = server_whose_catalog_cannot_write("taken", known);
        let (status, headers, report) = answer(
            &server,
            b"{\"ts\":1705314601,\"known\":2}\n{\"ts\":1705314602,\"new\":3}\n",
        );
        assert_eq!(status, StatusCode::OK, "{report}");
        assert!(headers.get(header::RETRY_AFTER).is_none(), "{headers:?}");
        assert_eq!((&report["ok"], &report["ingested"]), (&json!(false), &json!(1)));
        assert_eq!(report["errors"][0]["line"], 2);
        assert!(report.get("error").is_none() && report.get("retry_after").is_none());

        // Nothing taken, and one of the refusals is the line's own fault.
        let server = server_whose_catalog_cannot_write("mixed", b"");
        let (status, headers, report) =
            answer(&server, b"{\"ts\":1705314601,\"new\":2}\nnot json\n");
        assert_eq!(status, StatusCode::OK, "{report}");
        assert!(headers.get(header::RETRY_AFTER).is_none(), "{headers:?}");
        assert_eq!((&report["ok"], &report["ingested"]), (&json!(false), &json!(0)));
        assert_eq!(report["errors"].as_array().map(Vec::len), Some(2));
        assert!(report.get("error").is_none() && report.get("retry_after").is_none());

        // Nothing taken and nothing written for: every line is malformed.
        let (status, _, report) = answer(&server, b"not json\n[1]\n");
        assert_eq!(status, StatusCode::OK, "{report}");
        assert_eq!((&report["ok"], &report["ingested"]), (&json!(false), &json!(0)));

        // And nothing sent: no line, or blank ones. Nothing was refused.
        for empty in [&b""[..], &b"\n\n  \n"[..]] {
            let (status, _, report) = answer(&server, empty);
            assert_eq!(status, StatusCode::OK, "{report}");
            assert_eq!(report, json!({"ok": true, "ingested": 0, "errors": []}));
        }
    }

    /// The rule counts every refusal and not the thousand the answer describes: a
    /// malformed line past the cap still makes the body a mix, and a body of catalog
    /// refusals past the cap is still all of them.
    #[test]
    fn the_rule_counts_refusals_past_the_ones_the_answer_describes() {
        let server = server_whose_catalog_cannot_write("cap", b"");
        let mut body = Vec::new();
        for n in 0..=MAX_REPORTED_ERRORS {
            body.extend_from_slice(format!("{{\"ts\":1705314600,\"new\":{n}}}\n").as_bytes());
        }
        let (status, headers, report) = answer(&server, &body);
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(headers.get(header::RETRY_AFTER).is_some());
        assert_eq!(report["ingested"], 0);
        assert_eq!(report["errors"].as_array().map(Vec::len), Some(MAX_REPORTED_ERRORS));
        assert_eq!(report["errors_omitted"], 1);

        body.extend_from_slice(b"not json\n");
        let (status, _, report) = answer(&server, &body);
        assert_eq!(status, StatusCode::OK, "a malformed line past the cap was not counted");
        assert_eq!(report["errors_omitted"], 2);
    }

    /// **A scan is shared between threads from a thread that may block, and never from a
    /// runtime worker.** Sharing a run of row groups out blocks the thread that asked
    /// until the run is chosen; on a worker that is every request in flight, stalled.
    ///
    /// A server on disk whose queries may use four threads, with one template's day in
    /// two row groups, on a runtime of one worker. `post_query` is run as a task on that
    /// worker, as a request is: the one run its count shares out is shared out from
    /// another thread - the `spawn_blocking` thread the handler moves the query to.
    ///
    /// **And the control**, so that this is known to be able to fail: the same query
    /// answered by a task that calls the engine itself. The run is then shared out from
    /// the worker. A worker and a blocking thread cannot be told apart from inside, which
    /// is why this is a test and not an assertion in the scan.
    ///
    /// The scan threads are kept by a local declared before the runtime, as `run` keeps
    /// them, so that they are let go after it.
    #[test]
    fn a_scan_is_shared_out_from_a_blocking_thread_and_never_from_a_worker() {
        let dir = std::env::temp_dir().join(format!("kibbr-serve-fan-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("mkdir");
        let storage = Storage {
            base_path: Some(dir.clone()),
            trie_file: Some(dir.join("trie.json")),
            row_group_timeout: Duration::ZERO,
            scan_threads: 4,
            scan_fan_rows: 0,
            ..Storage::default()
        };
        let scan: OnceLock<Arc<Fan>> = OnceLock::new();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("a runtime");
        let server = Server::new(
            Utc::now(),
            Limits::default(),
            ApiLimits::default(),
            storage,
            Schema::default(),
            None,
        );
        let server = Arc::new(server.expect("a server on disk"));
        let fan = Arc::clone(server.fan.as_ref().expect("the server has scan threads"));
        let _ = scan.set(Arc::clone(&fan));

        // Two row groups in one day: each tick closes what is buffered.
        let now = Utc::now();
        for half in 0..2 {
            let lines: String = (0..50)
                .map(|n| {
                    format!(
                        "{{\"ts\":\"2024-01-15T00:0{half}:{n:02}Z\",\"level\":\"info\",\"n\":{n}}}\n"
                    )
                })
                .collect();
            db::read_lines(
                lines.as_bytes(),
                &mut |_, parsed| db::ingest_line(&server.trie, Some(&server.stores), &parsed, now),
                &mut |_| {},
            )
            .expect("the lines are read");
            let flushed = server.stores.flush_all(FlushKind::Tick);
            assert_eq!((flushed.failed, flushed.row_groups), (0, 1));
        }

        let text = r#"COUNT LOG {"level":"info"}"#;
        let me = std::thread::current().id();
        let worker = runtime
            .block_on(async { tokio::spawn(async { std::thread::current().id() }).await })
            .expect("the task ran");
        assert_ne!(worker, me);

        // As a request: the handler, on the worker.
        let asked = Bytes::from(json!({ "q": text }).to_string());
        let answer: Value = runtime.block_on(async {
            let response = tokio::spawn(post_query(State(Arc::clone(&server)), asked))
                .await
                .expect("the handler ran");
            assert_eq!(response.status(), StatusCode::OK);
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("the response body");
            serde_json::from_slice(&bytes).expect("the answer is JSON")
        });
        assert_eq!(answer["count"], 100, "{answer}");
        assert_eq!(answer["meta"]["row_groups_read"], 2, "{answer}");
        let entered = fan.entered();
        assert_eq!(entered.len(), 1, "{entered:?}");
        assert!(
            entered[0] != worker && entered[0] != me,
            "the scan was shared out from a runtime thread"
        );

        // The control: the engine called from a task, on the worker.
        let query = query::from_trieql(text, Utc::now(), server.limits).expect("a query");
        let held = Arc::clone(&server);
        let count = runtime
            .block_on(async {
                tokio::spawn(async move {
                    engine::execute(&held.trie, &held.stores, &query).map(|result| result.count)
                })
                .await
            })
            .expect("the task ran")
            .expect("the query answers");
        assert_eq!(count, 100);
        assert_eq!(fan.entered(), [entered[0], worker]);
        assert_eq!(fan.fanned(), 2);

        drop(server);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// **A server says what its queries may use**, in `/stats` and in its banner: the
    /// threads one query may choose a day's rows on, and the rows under which a run of
    /// row groups is never shared out. The default is half the cores, so it is not the
    /// same on two machines, and `scan_fan_outs` at 0 reads the same for a server with
    /// one thread as for one with four and nothing worth sharing out.
    ///
    /// The threads said are the ones a scan can use, which is not always what was
    /// resolved: a server in memory has no scan to share whatever its flags came to, and
    /// a disk store told none or one has the query's own thread and no other.
    #[test]
    fn a_server_says_the_scan_threads_it_resolved_in_stats_and_in_its_banner() {
        let dir = std::env::temp_dir().join(format!("kibbr-serve-scan-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let on_disk = |name: &str, scan_threads: usize, scan_fan_rows: u64| {
            let base = dir.join(name);
            std::fs::create_dir_all(&base).expect("mkdir");
            Storage {
                trie_file: Some(base.join("trie.json")),
                base_path: Some(base),
                scan_threads,
                scan_fan_rows,
                ..Storage::default()
            }
        };
        // In memory, with the four threads `with_scan` resolves on a machine of eight
        // cores still in its settings.
        let in_memory = Storage {
            scan_threads: 4,
            ..Storage::default()
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime");
        // Each server's settings, and the threads and the rows it must then say.
        let cases = [
            (in_memory, 1, kibbr_storage::store::DEFAULT_FAN_ROWS),
            (on_disk("none", 0, 7), 1, 7),
            (on_disk("one", 1, 7), 1, 7),
            (
                on_disk("two", 2, kibbr_storage::store::DEFAULT_FAN_ROWS),
                2,
                32_768,
            ),
            (on_disk("four", 4, 0), 4, 0),
        ];
        for (storage, threads, fan_rows) in cases {
            let server = Server::new(
                Utc::now(),
                Limits::default(),
                ApiLimits::default(),
                storage,
                Schema::default(),
                None,
            );
            let server = Arc::new(server.expect("a server"));
            assert_eq!(server.fan.is_some(), threads > 1);
            let stats: Value = runtime.block_on(async {
                let response = get_stats(State(Arc::clone(&server))).await;
                assert_eq!(response.status(), StatusCode::OK);
                let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                    .await
                    .expect("the response body");
                serde_json::from_slice(&bytes).expect("/stats is JSON")
            });
            let queries = &stats["queries"];
            assert_eq!(queries["scan_threads"], threads, "{queries}");
            assert_eq!(queries["scan_fan_rows"], fan_rows, "{queries}");
            assert_eq!(queries["scan_fan_outs"], 0, "{queries}");

            let line = scan_line(&server);
            if threads == 1 {
                assert_eq!(
                    line,
                    "  scan        1 thread to a query - no scan is shared out"
                );
                continue;
            }
            let first = format!("  scan        {threads} threads to a query, its own among them;");
            assert!(line.starts_with(&first), "{line}");
            let rows = format!("under {} rows is never shared out", commas(fan_rows));
            assert!(line.contains(&rows), "{line}");
            // And when else none is: for as long as a body is at the door, not only
            // while it is parsed.
            assert!(
                line.ends_with("any while an /ingest body is between the door and its answer"),
                "{line}"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── Backpressure ──────────────────────────────────────────────────────────────────

    /// A data directory of one test's own, removed when the test lets it go.
    struct Scratch(std::path::PathBuf);

    impl Scratch {
        fn new(tag: &str) -> Self {
            let dir = std::env::temp_dir()
                .join(format!("kibbr-serve-{tag}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).expect("mkdir");
            Self(dir)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// A server on `scratch`, with these limits on a request and these settings of the
    /// store over the defaults.
    fn server_on(scratch: &Scratch, api: ApiLimits, storage: Storage) -> Arc<Server> {
        let storage = Storage {
            base_path: Some(scratch.0.clone()),
            trie_file: Some(scratch.0.join("trie.json")),
            ..storage
        };
        let server = Server::new(
            Utc::now(),
            Limits::default(),
            api,
            storage,
            Schema::default(),
            None,
        );
        Arc::new(server.expect("a server on disk"))
    }

    /// `/stats`, as a client reads it.
    fn stats_of(server: &Arc<Server>) -> Value {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a runtime");
        runtime.block_on(async {
            let response = get_stats(State(Arc::clone(server))).await;
            assert_eq!(response.status(), StatusCode::OK);
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("the response body");
            serde_json::from_slice(&bytes).expect("/stats is JSON")
        })
    }

    /// **`/stats` says what is queued, what waited and what was shed, and under which
    /// cause.** `requests_shed` is what it always was - every body told to come back -
    /// and is the sum of the causes beside it, so a reader of the one number loses
    /// nothing and a reader of the six can tell a slow disk from too few permits.
    #[test]
    fn stats_say_what_is_queued_and_count_every_shed_under_its_cause() {
        let scratch = Scratch::new("stats-shed");
        let server = server_on(&scratch, ApiLimits::default(), Storage::default());
        let stats = stats_of(&server);
        let ingest = &stats["ingest"];
        assert_eq!(ingest["requests_shed"], 0, "{ingest}");
        assert_eq!(ingest["ingest_wait_ms"], 100, "{ingest}");
        assert_eq!(ingest["parsing"], 0, "{ingest}");
        assert_eq!(ingest["bodies_inside"], 0, "{ingest}");
        assert_eq!(
            ingest["door"],
            json!({"waiting": 0, "waits": 0, "waited_ms": 0, "line_max": null}),
            "{ingest}"
        );
        assert_eq!(
            ingest["shed"],
            json!({"permit": 0, "place": 0, "line": 0, "buffers": 0, "log": 0, "stopping": 0}),
            "{ingest}"
        );
        let wal = &stats["storage"]["wal"];
        assert_eq!(wal["queue_max_bytes"], 256 * 1024 * 1024, "{wal}");
        assert_eq!(wal["queue_bytes"], 0, "{wal}");
        assert_eq!(wal["unapplied_bytes"], 0, "{wal}");
        assert_eq!(wal["queue_peak_bytes"], 0, "{wal}");
        assert_eq!(wal["queue_sheds"], 0, "{wal}");
        assert_eq!(wal["queue_waiting"], 0, "{wal}");
        assert_eq!(wal["queue_waits"], 0, "{wal}");
        assert_eq!(wal["queue_left"], 0, "{wal}");

        // A permit and a place in use are said, each under its own name.
        let parse = Arc::clone(&server.ingest_permits)
            .try_acquire_owned()
            .expect("a parse permit");
        let places = Arc::clone(&server.ingest_bodies)
            .try_acquire_many_owned(3)
            .expect("three places");
        let ingest = stats_of(&server)["ingest"].clone();
        assert_eq!((&ingest["parsing"], &ingest["bodies_inside"]), (&json!(1), &json!(3)));
        drop((parse, places));

        // Each cause counted apart, and the total their sum.
        let shed = &server.door.shed;
        let causes = [
            (&shed.permit, "permit", 1u64),
            (&shed.place, "place", 20),
            (&shed.line, "line", 300),
            (&shed.buffers, "buffers", 4_000),
            (&shed.log, "log", 50_000),
            (&shed.stopping, "stopping", 600_000),
        ];
        for (count, _, by) in &causes {
            count.fetch_add(*by, Ordering::Relaxed);
        }
        let ingest = stats_of(&server)["ingest"].clone();
        for (_, cause, by) in &causes {
            assert_eq!(ingest["shed"][cause], *by, "{ingest}");
        }
        assert_eq!(ingest["requests_shed"], 654_321, "{ingest}");
        assert_eq!(ingest["shed"].as_object().map(|causes| causes.len()), Some(6));

        // A server in memory has a door and no log.
        let in_memory = stats_of(&self::server());
        assert_eq!(in_memory["storage"]["wal"], Value::Null);
        assert_eq!(in_memory["ingest"]["requests_shed"], 0);
        assert_eq!(in_memory["ingest"]["shed"]["log"], 0);
    }

    // ── The door and the log, with a slow hop held ────────────────────────────────────
    //
    // A server on a data directory of the test's own, its router on a port of its own
    // inside a runtime the test owns, and plain sockets for clients. The slow hop is one
    // the test holds: the store's applier, kept still as a replace-a-day keeps it, or
    // the flush, which no task here runs. Nothing sleeps to let a thing happen - a state
    // is waited for, read from the gauges, and the wait's limit fails the test.

    /// How long a body that a test expects to be let go may wait, and how long the test
    /// gives it. Far apart, so that being woken is the only way out in time: a waiter
    /// also looks again when its own wait runs out.
    const LONG_WAIT: Duration = Duration::from_secs(60);
    const WOKEN_WITHIN: Duration = Duration::from_secs(10);

    /// The lines in each body these tests send.
    const PER_BODY: usize = 20;

    /// Body `nth`: lines of one template and one day, each as long as every other, so
    /// that every body counts the same against the log's bound.
    fn lines(nth: usize) -> Vec<u8> {
        lines_at(1_705_314_600, nth)
    }

    /// The same, of the day `ts` is in.
    fn lines_at(ts: u64, nth: usize) -> Vec<u8> {
        (0..PER_BODY)
            .map(|n| format!("{{\"ts\":{ts},\"n\":{}}}\n", 1_000_000 + nth * PER_BODY + n))
            .collect::<String>()
            .into_bytes()
    }

    /// What one such body counts for against the log's bound.
    fn one_body() -> usize {
        wal::counted(&lines(0))
    }

    /// Wait for a state, looking every few milliseconds. The limit fails the test: it is
    /// far past what any machine needs, and nothing passes by reaching it.
    fn until(what: &str, mut reached: impl FnMut() -> bool) {
        let began = Instant::now();
        while !reached() {
            assert!(
                began.elapsed() < Duration::from_secs(30),
                "timed out waiting for {what}"
            );
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    /// A server answering on a port of its own.
    struct Listening {
        server: Arc<Server>,
        addr: SocketAddr,
        runtime: Option<tokio::runtime::Runtime>,
        _scratch: Scratch,
    }

    impl Listening {
        /// `permits` bodies parsed at once, `wait` at the door and for room in the log,
        /// and `storage` over the defaults. No flush task and no signal: a test flushes
        /// by hand and stops by `begin_stop`.
        fn start(tag: &str, permits: usize, wait: Duration, storage: Storage) -> Self {
            let scratch = Scratch::new(tag);
            let api = ApiLimits {
                ingest_concurrency: permits,
                ingest_wait: wait,
                ..ApiLimits::default()
            };
            let server = server_on(&scratch, api, storage);
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("a runtime");
            let app = router(Arc::clone(&server));
            let addr = runtime.block_on(async {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .expect("bind");
                let addr = listener.local_addr().expect("the bound address");
                tokio::spawn(async move {
                    let _ = axum::serve(listener, app).await;
                });
                addr
            });
            Self {
                server,
                addr,
                runtime: Some(runtime),
                _scratch: scratch,
            }
        }

        fn wal(&self) -> wal::Stats {
            self.server.wal.as_ref().expect("a log").stats()
        }

        /// Permits for the parse in use.
        fn parsing(&self) -> usize {
            self.server.api.ingest_concurrency - self.server.ingest_permits.available_permits()
        }

        /// Places between the door and an answer in use.
        fn inside(&self) -> usize {
            ingest_bodies(&self.server.api) - self.server.ingest_bodies.available_permits()
        }

        /// Bodies waiting at the door now.
        fn at_the_door(&self) -> u64 {
            self.server.door.waiting.load(Ordering::Relaxed)
        }

        fn shed(&self) -> u64 {
            self.server.door.shed.total()
        }

        /// `COUNT LOG {}`: every row a query can see.
        fn count(&self) -> u64 {
            let server = &self.server;
            let query = query::from_trieql("COUNT LOG {}", Utc::now(), server.limits)
                .expect("a query");
            let result = engine::execute(&server.trie, &server.stores, &query);
            result.expect("the query answers").count
        }

        /// Hold the log's applier still, as a replace-a-day does: bodies are written to
        /// the log and none is stored - or answered - until the guard is let go. Taken
        /// after the server and so let go before it, whatever way the test ends.
        fn still(&self) -> std::sync::RwLockWriteGuard<'_, ()> {
            let disk = self.server.stores.disk_store().expect("a disk store");
            disk.still()
        }

        /// A body sent on a connection and a thread of its own: its answer when joined.
        fn post(&self, body: Vec<u8>) -> std::thread::JoinHandle<Answer> {
            let addr = self.addr;
            std::thread::spawn(move || Client::connect(addr).post(&body))
        }
    }

    impl Drop for Listening {
        fn drop(&mut self) {
            // Bounded: a test that failed with a body parked must fail, not hang here.
            if let Some(runtime) = self.runtime.take() {
                runtime.shutdown_timeout(Duration::from_secs(5));
            }
        }
    }

    /// One answer: its status, its headers in lower case, and its JSON.
    #[derive(Debug)]
    struct Answer {
        status: u16,
        headers: String,
        body: Value,
    }

    impl Answer {
        /// Whether the answer says the connection closes after it.
        fn closes(&self) -> bool {
            self.headers
                .lines()
                .filter_map(|line| line.strip_prefix("connection:"))
                .any(|value| value.split(',').any(|option| option.trim() == "close"))
        }

        fn retry_after(&self) -> bool {
            self.headers.lines().any(|line| line.starts_with("retry-after:"))
        }

        fn error(&self) -> &str {
            self.body["error"].as_str().unwrap_or_default()
        }
    }

    /// What `log_busy` says, and what `busy` says: how a test tells the two refusals.
    const SAYS_LOG_BUSY: &str = "the write-ahead log has more waiting";
    const SAYS_BUSY: &str = "ingest is at capacity";

    /// One connection, kept between requests.
    struct Client(std::net::TcpStream);

    impl Client {
        fn connect(addr: SocketAddr) -> Self {
            let socket = std::net::TcpStream::connect(addr).expect("connect");
            // Past any wait a test stages: a server that never answers fails the test.
            socket
                .set_read_timeout(Some(Duration::from_secs(120)))
                .expect("a read timeout");
            Self(socket)
        }

        /// A request's head, saying a body of `length` bytes follows.
        fn head(&mut self, method: &str, path: &str, length: usize) {
            let head = format!(
                "{method} {path} HTTP/1.1\r\nHost: kibbr\r\nContent-Length: {length}\r\n\r\n"
            );
            self.0.write_all(head.as_bytes()).expect("the head");
        }

        /// Bytes of a body. A body the door refuses is never read, so its write may
        /// break; the answer is what a test reads.
        fn write(&mut self, bytes: &[u8]) {
            let _ = self.0.write_all(bytes);
            let _ = self.0.flush();
        }

        /// The next answer on the connection, read by its `Content-Length`.
        ///
        /// In as few reads as the answer comes in, and not a byte at a time: the door
        /// answers with the body unread and then closes, and what is left of an answer
        /// in a socket that has been reset is not always there for a second read.
        fn answer(&mut self) -> Answer {
            let mut raw = Vec::new();
            let mut chunk = [0u8; 4096];
            let head = loop {
                if let Some(at) = raw.windows(4).position(|four| four == b"\r\n\r\n") {
                    break at + 4;
                }
                let read = self.0.read(&mut chunk).expect("the answer's head");
                assert!(read > 0, "the connection ended before an answer's head did");
                raw.extend_from_slice(&chunk[..read]);
            };
            let headers = String::from_utf8_lossy(&raw[..head]).to_lowercase();
            let length: usize = headers
                .lines()
                .find_map(|line| line.strip_prefix("content-length:"))
                .and_then(|value| value.trim().parse().ok())
                .unwrap_or_else(|| panic!("no Content-Length in the answer:\n{headers}"));
            while raw.len() < head + length {
                let read = self.0.read(&mut chunk).expect("the answer's body");
                assert!(read > 0, "the connection ended before an answer's body did");
                raw.extend_from_slice(&chunk[..read]);
            }
            // One request at a time on a connection, so nothing follows the answer.
            assert_eq!(raw.len(), head + length, "bytes after the answer");
            let body = raw[head..].to_vec();
            let status = headers
                .split_whitespace()
                .nth(1)
                .and_then(|code| code.parse().ok())
                .unwrap_or_else(|| panic!("no status in the answer:\n{headers}"));
            let body = serde_json::from_slice(&body).expect("the answer is JSON");
            Answer {
                status,
                headers,
                body,
            }
        }

        fn post(&mut self, body: &[u8]) -> Answer {
            self.head("POST", "/ingest", body.len());
            self.write(body);
            self.answer()
        }

        fn get(&mut self, path: &str) -> Answer {
            self.head("GET", path, 0);
            self.answer()
        }
    }

    /// **A wait for room that runs out is a `503` on a connection that is kept.** The
    /// body was read and parsed before the log was found full, so nothing is left unread
    /// and the connection carries the next request; the answer says when to come back.
    ///
    /// One request at a time, so that nothing here is a race: two bodies are parked in
    /// the log, which is then exactly full, both permits are back, and then one body is
    /// sent alone. The fifty milliseconds are how long the test takes - the applier is
    /// held throughout, so no speed turns the `503` into a `200`.
    #[test]
    fn a_wait_for_room_that_runs_out_is_a_503_on_a_kept_connection() {
        let bound = 2 * one_body();
        let storage = Storage {
            wal_queue_bytes: bound,
            ..Storage::default()
        };
        let at = Listening::start("room-503", 2, Duration::from_millis(50), storage);
        let still = at.still();
        let parked = [at.post(lines(0)), at.post(lines(1))];
        // Both written, and neither stored: nothing is waiting for the log's disk here,
        // and the bound is full all the same.
        until("two bodies to fill the log", || {
            let wal = at.wal();
            (wal.queue_bytes, wal.unapplied_bytes) == (0, bound) && at.parsing() == 0
        });

        let mut client = Client::connect(at.addr);
        let answer = client.post(&lines(2));
        assert_eq!(answer.status, 503, "{answer:?}");
        assert!(answer.retry_after(), "{answer:?}");
        assert!(answer.error().contains(SAYS_LOG_BUSY), "{answer:?}");
        // So the refusal may not put it on the log's disk alone: what is behind here is
        // the store, and the answer names both places a line can be waiting.
        assert!(
            answer.error().contains("to be written or stored")
                && answer.error().contains("its disk or the store is behind"),
            "{answer:?}"
        );
        let wal = at.wal();
        assert_eq!((wal.queue_bytes, wal.unapplied_bytes), (0, bound));
        assert!(!answer.closes(), "the body was read: {answer:?}");
        let health = client.get("/health");
        assert_eq!(health.status, 200, "{health:?}");

        let shed = &at.server.door.shed;
        assert_eq!(shed.log.load(Ordering::Relaxed), 1);
        assert_eq!(at.shed(), 1);
        let wal = at.wal();
        assert_eq!((wal.queue_sheds, wal.queue_waits), (1, 1));
        // Read and refused: in the two counters a body the log sheds has always been in.
        let read = at.server.lines_read.load(Ordering::Relaxed);
        let rejected = at.server.lines_rejected.load(Ordering::Relaxed);
        assert_eq!((read, rejected), (PER_BODY as u64, PER_BODY as u64));

        drop(still);
        for body in parked {
            let answer = body.join().expect("a sender");
            assert_eq!(answer.status, 200, "{answer:?}");
        }
        assert_eq!(at.count(), 2 * PER_BODY as u64);
        assert_eq!(at.wal().queue_peak_bytes, bound);
    }

    /// **A slow applier makes senders wait, and none is shed.** Two permits, a log that
    /// holds two bodies, and eight senders of three bodies each, with the applier held
    /// still. Two bodies go into the log; the next two are parsed and wait for room,
    /// holding both permits; the other four wait at the door with their bodies unread.
    /// That is the one state the eight can settle in, and nothing in it has been refused
    /// or acknowledged. When the applier is let go every body lands.
    #[test]
    fn senders_wait_for_a_slow_applier_and_none_is_shed() {
        const SENDERS: usize = 8;
        const BODIES: usize = 3;
        let bound = 2 * one_body();
        let storage = Storage {
            wal_queue_bytes: bound,
            ..Storage::default()
        };
        let at = Listening::start("door-wait", 2, LONG_WAIT, storage);
        let still = at.still();
        let senders: Vec<_> = (0..SENDERS)
            .map(|sender| {
                let addr = at.addr;
                std::thread::spawn(move || {
                    let mut client = Client::connect(addr);
                    (0..BODIES)
                        .map(|nth| client.post(&lines(sender * BODIES + nth)).status)
                        .collect::<Vec<u16>>()
                })
            })
            .collect();

        until("two in the log, two waiting for room, four at the door", || {
            let wal = at.wal();
            wal.queue_waiting == 2
                && at.at_the_door() == 4
                && wal.last_seq == 2 * PER_BODY as u64
        });
        let wal = at.wal();
        assert_eq!(at.shed(), 0);
        assert_eq!(at.server.lines_ingested.load(Ordering::Relaxed), 0);
        assert_eq!(wal.applied, 0);
        assert!(wal.queue_bytes + wal.unapplied_bytes <= bound, "{wal:?}");
        assert_eq!((at.parsing(), at.inside()), (2, 4));

        drop(still);
        for sender in senders {
            assert_eq!(sender.join().expect("a sender"), [200; BODIES]);
        }
        assert_eq!(at.shed(), 0);
        assert_eq!(at.count(), (SENDERS * BODIES * PER_BODY) as u64);
        let wal = at.wal();
        assert!(wal.queue_peak_bytes <= bound, "{wal:?}");
        assert_eq!((wal.queue_sheds, wal.queue_left), (0, 0));
        assert_eq!((at.parsing(), at.inside(), at.at_the_door()), (0, 0, 0));
    }

    /// **Every shed answer is one of two shapes, whichever way the timing fell.** The
    /// staging above with a wait of fifty milliseconds and one body a sender: two bodies
    /// get into the log, and the other six cannot while the applier is held. Which of
    /// the six were parsed first, and under which cause each was counted, is the
    /// scheduler's to decide and is not asserted. What is: each is a `503` that says when
    /// to come back, and is either the log's refusal on a connection that is kept - its
    /// body was read - or a refusal from the door on a connection that is closed; and
    /// the count of sheds is the count of `503`s, and the sum of its causes.
    #[test]
    fn every_shed_answer_is_one_of_two_shapes() {
        const SENDERS: usize = 8;
        let bound = 2 * one_body();
        let storage = Storage {
            wal_queue_bytes: bound,
            ..Storage::default()
        };
        let at = Listening::start("door-shapes", 2, Duration::from_millis(50), storage);
        let still = at.still();
        let (said, answers) = std::sync::mpsc::channel::<Answer>();
        for sender in 0..SENDERS {
            let (addr, said) = (at.addr, said.clone());
            std::thread::spawn(move || {
                let _ = said.send(Client::connect(addr).post(&lines(sender)));
            });
        }
        drop(said);

        until("six of the eight to be shed", || at.shed() == 6);
        for _ in 0..6 {
            let answer = answers
                .recv_timeout(Duration::from_secs(30))
                .expect("a shed body was not answered");
            assert_eq!(answer.status, 503, "{answer:?}");
            assert!(answer.retry_after(), "{answer:?}");
            if answer.closes() {
                let said = answer.error();
                assert!(
                    said.contains(SAYS_BUSY) || said.contains(SAYS_LOG_BUSY),
                    "{answer:?}"
                );
            } else {
                assert!(answer.error().contains(SAYS_LOG_BUSY), "{answer:?}");
            }
        }
        assert_eq!(at.shed(), 6);
        let causes: u64 = at.server.door.shed.each().iter().map(|(_, n)| n).sum();
        assert_eq!(causes, 6);
        assert_eq!(at.server.lines_ingested.load(Ordering::Relaxed), 0);

        drop(still);
        for _ in 0..2 {
            let answer = answers
                .recv_timeout(Duration::from_secs(30))
                .expect("a body in the log was not answered");
            assert_eq!(answer.status, 200, "{answer:?}");
        }
        assert_eq!(at.count(), 2 * PER_BODY as u64);
    }

    /// A log that holds one body, with one written and held at the applier, and one
    /// permit, which a second body holds while it waits for room: what the door looks
    /// like to whoever comes third. The two senders, in that order.
    fn one_in_the_log_one_waiting_for_room(
        at: &Listening,
    ) -> [std::thread::JoinHandle<Answer>; 2] {
        let first = at.post(lines(0));
        until("the first body to be in the log", || {
            at.wal().unapplied_bytes == one_body() && at.parsing() == 0
        });
        let second = at.post(lines(1));
        until("the second body to wait for room", || {
            at.wal().queue_waiting == 1
        });
        [first, second]
    }

    /// A server with one permit whose log holds one body.
    fn one_at_a_time(tag: &str, wait: Duration) -> Listening {
        let storage = Storage {
            wal_queue_bytes: one_body(),
            ..Storage::default()
        };
        Listening::start(tag, 1, wait, storage)
    }

    /// **A sender that hangs up at the door holds nothing.** It has no permit and no
    /// place while it waits, so its going costs nobody anything. Its turn comes when the
    /// permit does: reading its body then fails, and the permit goes straight back.
    #[test]
    fn a_sender_that_hangs_up_at_the_door_holds_nothing() {
        let at = one_at_a_time("door-left", LONG_WAIT);
        let still = at.still();
        let inside = one_in_the_log_one_waiting_for_room(&at);
        {
            let body = lines(2);
            let mut third = Client::connect(at.addr);
            third.head("POST", "/ingest", body.len());
            third.write(&body[..body.len() / 2]);
            until("the third body to wait at the door", || at.at_the_door() == 1);
        }

        drop(still);
        for body in inside {
            let answer = body.join().expect("a sender");
            assert_eq!(answer.status, 200, "{answer:?}");
        }
        until("the door to be empty", || {
            at.parsing() == 0
                && at.inside() == 0
                && at.at_the_door() == 0
                && at.server.in_flight.load(Ordering::Relaxed) == 0
        });
        assert_eq!(at.count(), 2 * PER_BODY as u64);
        assert_eq!(at.wal().last_seq, 2 * PER_BODY as u64);
        assert_eq!(at.shed(), 0);
    }

    /// **A stop refuses whoever is waiting and finishes whoever is inside.** One body in
    /// the log, one waiting for room, one at the door, each with a minute to wait. The
    /// stop answers the last two at once, while the applier is still held: the one that
    /// was parsed with the log's refusal on its kept connection, the one at the door
    /// with the door's, unread. The body in the log is finished and answered `200`. A
    /// body that is neither - still being parsed when the stop is said - has the test
    /// after this one.
    ///
    /// The third body is where the order of the door's waits shows. The second body's
    /// permit reaches it after the stop was said; a door that looked at its permit
    /// before the stop would let it in to be read, parsed and shed by the log.
    #[test]
    fn shutdown_refuses_whoever_is_waiting_and_finishes_whoever_is_inside() {
        let at = one_at_a_time("door-stop", LONG_WAIT);
        let still = at.still();
        let [first, second] = one_in_the_log_one_waiting_for_room(&at);
        let third = at.post(lines(2));
        until("the third body to wait at the door", || at.at_the_door() == 1);

        let began = Instant::now();
        let runtime = at.runtime.as_ref().expect("the runtime");
        runtime.block_on(async { begin_stop(&at.server) });
        let second = second.join().expect("the second sender");
        let third = third.join().expect("the third sender");
        assert!(
            began.elapsed() < WOKEN_WITHIN,
            "a stop waited out a body that was waiting"
        );
        assert_eq!(second.status, 503, "{second:?}");
        assert!(second.error().contains(SAYS_LOG_BUSY), "{second:?}");
        assert!(second.retry_after() && !second.closes(), "{second:?}");
        assert_eq!(third.status, 503, "{third:?}");
        assert!(third.error().contains("the server is stopping"), "{third:?}");
        assert!(third.retry_after() && third.closes(), "{third:?}");
        let shed = &at.server.door.shed;
        assert_eq!(shed.log.load(Ordering::Relaxed), 1);
        assert_eq!(shed.stopping.load(Ordering::Relaxed), 1);
        assert_eq!(at.shed(), 2);
        assert!(!first.is_finished(), "the body in the log was not waited for");

        drop(still);
        let first = first.join().expect("the first sender");
        assert_eq!(first.status, 200, "{first:?}");
        // What the stop waits on before its final flush: every place back.
        let places = ingest_bodies(&at.server.api) as u32;
        let all = runtime.block_on(async {
            tokio::time::timeout(WOKEN_WITHIN, at.server.ingest_bodies.acquire_many(places)).await
        });
        assert!(matches!(all, Ok(Ok(_))), "a place was never given back");
        assert_eq!(at.count(), PER_BODY as u64);
    }

    /// **A body still being parsed when a stop begins is taken if the log has room for
    /// it, and shed at once if it has none.** It is neither waiting nor in the log. A
    /// stop finishes what the log has taken and keeps nobody waiting, so what this body
    /// is told depends on the room it finds when its parse ends. With none it is the
    /// log's shed, on its kept connection and counted under `log` - not the door's
    /// "stopping", which is for a body that was never read.
    ///
    /// The second body holds the one permit, half sent, when the stop is said, and the
    /// rest of it is sent after. Whether the log has been told to hurry by the time the
    /// body gets there, or is told while it waits, the answer is the same and comes at
    /// once. The third body arrives whole after the stop and after the log has emptied:
    /// it has nothing to wait for, and is taken.
    #[test]
    fn a_body_being_parsed_at_a_stop_is_shed_if_the_log_has_no_room_for_it() {
        let at = one_at_a_time("stop-parsing", LONG_WAIT);
        let still = at.still();
        let first = at.post(lines(0));
        until("the first body to be in the log", || {
            at.wal().unapplied_bytes == one_body() && at.parsing() == 0
        });
        let body = lines(1);
        let mut second = Client::connect(at.addr);
        second.head("POST", "/ingest", body.len());
        second.write(&body[..body.len() / 2]);
        until("the second body to hold the permit", || at.parsing() == 1);

        let runtime = at.runtime.as_ref().expect("the runtime");
        runtime.block_on(async { begin_stop(&at.server) });
        let began = Instant::now();
        second.write(&body[body.len() / 2..]);
        let answer = second.answer();
        assert!(
            began.elapsed() < WOKEN_WITHIN,
            "a stop let a body wait for room"
        );
        assert_eq!(answer.status, 503, "{answer:?}");
        assert!(answer.error().contains(SAYS_LOG_BUSY), "{answer:?}");
        assert!(answer.retry_after() && !answer.closes(), "{answer:?}");
        let shed = &at.server.door.shed;
        assert_eq!(shed.log.load(Ordering::Relaxed), 1);
        assert_eq!(shed.stopping.load(Ordering::Relaxed), 0);
        assert_eq!(at.shed(), 1);
        assert_eq!(at.wal().queue_sheds, 1);
        assert!(
            !first.is_finished(),
            "the body in the log was not waited for"
        );

        drop(still);
        let first = first.join().expect("the first sender");
        assert_eq!(first.status, 200, "{first:?}");
        let third = at.post(lines(2)).join().expect("the third sender");
        assert_eq!(third.status, 200, "{third:?}");
        assert_eq!(at.shed(), 1);
        assert_eq!(at.count(), 2 * PER_BODY as u64);
        let rejected = at.server.lines_rejected.load(Ordering::Relaxed);
        assert_eq!(rejected, PER_BODY as u64);
    }

    /// **A log that fails answers every waiter with its reason.** One body in the log,
    /// two parsed and waiting for room, one at the door; the log's directory then takes
    /// no new file, and the applier is let go. The body in the log is `200`. Each of the
    /// other three is a `503` that names the write error, inside the time a waiter that
    /// was never woken would not make.
    ///
    /// Nothing is asserted of the fourth body's connection. It is closed if the door
    /// refused it, and kept if the door let it through in the moment between the log
    /// being handed a body and that body's flush failing.
    #[test]
    #[cfg(unix)]
    fn a_failed_log_answers_every_waiter_with_its_reason() {
        use std::os::unix::fs::PermissionsExt;

        let storage = Storage {
            wal_queue_bytes: one_body(),
            // A segment a frame long: the flush after the first needs a new file.
            wal_segment_bytes: 1,
            ..Storage::default()
        };
        let at = Listening::start("door-failed", 2, LONG_WAIT, storage);
        let still = at.still();
        let first = at.post(lines(0));
        until("the first body to be in the log", || {
            at.wal().unapplied_bytes == one_body() && at.parsing() == 0
        });
        let mut waiting = vec![at.post(lines(1)), at.post(lines(2))];
        until("two bodies to wait for room", || at.wal().queue_waiting == 2);
        waiting.push(at.post(lines(3)));
        until("the fourth body to wait at the door", || at.at_the_door() == 1);

        let dir = at.server.wal.as_ref().expect("a log").dir().to_path_buf();
        let mode = |mode| std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(mode));
        mode(0o555).expect("chmod -w");
        let began = Instant::now();
        drop(still);
        let first = first.join().expect("the first sender");
        let refused: Vec<Answer> = waiting
            .into_iter()
            .map(|body| body.join().expect("a waiting sender"))
            .collect();
        let took = began.elapsed();
        let degraded = at.server.degraded();
        mode(0o755).expect("chmod +w");

        assert_eq!(first.status, 200, "{first:?}");
        let why = at.wal().last_error.expect("the log says why it cannot write");
        for answer in &refused {
            assert_eq!(answer.status, 503, "{answer:?}");
            assert!(answer.retry_after(), "{answer:?}");
            assert!(answer.error().contains(&why), "{answer:?}\n{why}");
        }
        assert!(
            took < WOKEN_WITHIN,
            "a waiter was not told that the log had failed"
        );
        assert!(degraded.is_some_and(|said| said.contains(&why)));
        assert_eq!(at.count(), PER_BODY as u64);

        // And the door opens again at the first write that succeeds.
        until("the log to write again", || {
            move_checkpoint(&at.server);
            at.server.wal.as_ref().is_some_and(Wal::is_healthy)
        });
        let answer = at.post(lines(4)).join().expect("a sender");
        assert_eq!(answer.status, 200, "{answer:?}");
        assert_eq!(at.count(), 2 * PER_BODY as u64);
    }

    /// A server whose buffers hold next to nothing, so that one body leaves them past
    /// twice their limit, and which no task flushes: the test does.
    fn small_buffers(tag: &str, permits: usize) -> Listening {
        let storage = Storage {
            max_buffer_bytes: 1024,
            ..Storage::default()
        };
        Listening::start(tag, permits, LONG_WAIT, storage)
    }

    /// Flush as the tick does, and say so to whoever is waiting at the door for it.
    fn tick(at: &Listening) -> kibbr_storage::store::FlushSummary {
        let summary = at.server.stores.flush_all(FlushKind::Tick);
        at.server.flushed.notify_waiters();
        summary
    }

    /// **A body waits at the door while the buffers are past twice their limit, and is
    /// let in by the flush that brings them back.** It was shed at once, though the
    /// flush that would have let it in was a few milliseconds from done.
    ///
    /// **And is not made to wait on a store that cannot write.** Nothing leaves the
    /// buffers then, so the wait could only end in the same refusal, later: it is given
    /// at once, with the store's error in it.
    #[test]
    #[cfg(unix)]
    fn a_body_waits_at_the_door_while_the_buffers_are_past_twice_their_limit() {
        use std::os::unix::fs::PermissionsExt;

        let at = small_buffers("door-buffers", 2);
        let first = at.post(lines(0)).join().expect("a sender");
        assert_eq!(first.status, 200, "{first:?}");
        assert!(at.server.stores.buffers_full());

        let second = at.post(lines(1));
        until("the second body to wait at the door", || at.at_the_door() == 1);
        assert_eq!(at.parsing(), 0, "a body waiting for the flush holds a permit");
        let began = Instant::now();
        assert_eq!(tick(&at).failed, 0);
        assert!(!at.server.stores.buffers_full());
        let second = second.join().expect("the second sender");
        assert!(
            began.elapsed() < WOKEN_WITHIN,
            "the body at the door was not let in by the flush"
        );
        assert_eq!(second.status, 200, "{second:?}");
        let door = &at.server.door;
        assert_eq!(door.shed.buffers.load(Ordering::Relaxed), 0);
        assert_eq!(door.waits.load(Ordering::Relaxed), 1);

        // The store stops writing: a day with no file yet, and a directory that will
        // not take one.
        assert_eq!(tick(&at).failed, 0);
        let data = at.server.storage.base_path.as_ref().expect("on disk");
        let data = data.join(kibbr_storage::store::disk::DATA_DIR);
        let mode = |mode| std::fs::set_permissions(&data, std::fs::Permissions::from_mode(mode));
        mode(0o555).expect("chmod -w");
        let third = at.post(lines_at(1_705_401_000, 2)).join().expect("a sender");
        let failed = tick(&at).failed;
        let full = at.server.stores.buffers_full() && at.server.write_health.is_degraded();
        let began = Instant::now();
        let fourth = at.post(lines(3)).join().expect("a sender");
        let took = began.elapsed();
        mode(0o755).expect("chmod +w");

        assert_eq!(third.status, 200, "{third:?}");
        assert!(failed > 0 && full, "the store wrote to a read-only directory");
        assert_eq!(fourth.status, 503, "{fourth:?}");
        assert!(fourth.retry_after() && fourth.closes(), "{fourth:?}");
        assert!(fourth.error().contains("the store cannot write"), "{fourth:?}");
        assert!(took < WOKEN_WITHIN, "a refusal that could not change was waited for");
        assert_eq!(door.shed.buffers.load(Ordering::Relaxed), 1);
        assert_eq!(at.shed(), 1);
    }

    /// **A body given its permit onto full buffers goes back to wait.** The door looks at
    /// the buffers before it waits for a permit, and a permit can be a long wait: without
    /// a second look, every body that passed the first could be let in onto buffers that
    /// filled meanwhile.
    ///
    /// One permit. The first body is in the log with its rows not yet stored, so the
    /// buffers are empty; the second holds the permit, half sent; the third waits at the
    /// door for that permit. Then the first body's rows are stored - the buffers are
    /// full - and the second finishes. The permit passes to the third with no moment at
    /// which it is free, so the state waited for here - no permit in use, one body at
    /// the door - is one only a door that gave the permit back can reach.
    #[test]
    fn a_body_given_its_permit_onto_full_buffers_goes_back_to_wait() {
        let at = small_buffers("door-second-look", 1);
        let still = at.still();
        let first = at.post(lines(0));
        until("the first body to be in the log", || {
            at.wal().unapplied_bytes == one_body() && at.parsing() == 0
        });
        let body = lines(1);
        let mut second = Client::connect(at.addr);
        second.head("POST", "/ingest", body.len());
        second.write(&body[..body.len() / 2]);
        until("the second body to hold the permit", || at.parsing() == 1);
        let third = at.post(lines(2));
        until("the third body to wait at the door", || at.at_the_door() == 1);
        assert!(!at.server.stores.buffers_full());

        drop(still);
        let first = first.join().expect("the first sender");
        assert_eq!(first.status, 200, "{first:?}");
        assert!(at.server.stores.buffers_full());
        second.write(&body[body.len() / 2..]);
        let second = second.answer();
        assert_eq!(second.status, 200, "{second:?}");
        until("the third body to give its permit back", || {
            at.parsing() == 0 && at.at_the_door() == 1
        });
        assert!(!third.is_finished());

        let began = Instant::now();
        assert_eq!(tick(&at).failed, 0);
        let third = third.join().expect("the third sender");
        assert!(began.elapsed() < WOKEN_WITHIN);
        assert_eq!(third.status, 200, "{third:?}");
        assert_eq!(at.shed(), 0);
        assert_eq!(at.count(), 3 * PER_BODY as u64);
    }

    /// **The line at the door has no bound at the default wait, and one above it.** At
    /// a hundred milliseconds nobody is in the line for longer than that, and a burst
    /// queues for the moment it takes with no count held against it, as it always has.
    /// A wait an operator has raised keeps each waiter's socket for as long, and that
    /// needs a number: the places.
    #[test]
    fn the_line_has_no_bound_at_the_default_wait() {
        let with_wait = |ms: u64| {
            let api = ApiLimits {
                ingest_wait: Duration::from_millis(ms),
                ..ApiLimits::default()
            };
            let server = Server::new(
                Utc::now(),
                Limits::default(),
                api,
                Storage::default(),
                Schema::default(),
                None,
            );
            Arc::new(server.expect("a server in memory"))
        };
        let places = ingest_bodies(&ApiLimits::default());
        let default = crate::config::DEFAULT_INGEST_WAIT_MS as u64;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime");

        for ms in [0, 1, default] {
            let server = with_wait(ms);
            assert_eq!(server.line_max(), None, "{ms}ms");
            assert_eq!(stats_of(&server)["ingest"]["door"]["line_max"], Value::Null);
            // As many at the door as there are places, and one more is let wait: here
            // it has nothing to wait for, and is let in.
            server.door.waiting.store(places as u64, Ordering::Relaxed);
            let admitted = runtime.block_on(server.admit_waiting());
            assert!(admitted.is_ok(), "{ms}ms");
            assert_eq!(server.door.shed.total(), 0, "{ms}ms");
        }
        for ms in [default + 1, 30_000] {
            let server = with_wait(ms);
            assert_eq!(server.line_max(), Some(places), "{ms}ms");
            assert_eq!(stats_of(&server)["ingest"]["door"]["line_max"], places);
            // One short of the bound waits; at the bound the next is shed at once.
            server.door.waiting.store(places as u64 - 1, Ordering::Relaxed);
            assert!(runtime.block_on(server.admit_waiting()).is_ok(), "{ms}ms");
            server.door.waiting.store(places as u64, Ordering::Relaxed);
            let refused = runtime.block_on(server.admit_waiting());
            let refused = refused.err().expect("the line was full");
            assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert!(refused.headers().get(header::RETRY_AFTER).is_some());
            assert_eq!(server.door.shed.line.load(Ordering::Relaxed), 1, "{ms}ms");
            assert_eq!(server.door.shed.total(), 1, "{ms}ms");
        }
    }

    /// **The flush task says so when it has flushed, and that is what lets in a body
    /// waiting at the door.** The buffers are full, a body is waiting at the door, and
    /// only then is the task started - on a tick an hour long, so the flush it runs is
    /// the one it was rung for. Without its word to the door the body would wait out its
    /// minute beside buffers that had long been emptied.
    #[test]
    fn the_flush_task_lets_in_a_body_waiting_at_the_door() {
        let storage = Storage {
            max_buffer_bytes: 1024,
            flush_interval: Duration::from_secs(3600),
            ..Storage::default()
        };
        let at = Listening::start("door-flush-task", 2, LONG_WAIT, storage);
        let first = at.post(lines(0)).join().expect("a sender");
        assert_eq!(first.status, 200, "{first:?}");
        assert!(at.server.stores.buffers_full());
        let second = at.post(lines(1));
        until("the second body to wait at the door", || at.at_the_door() == 1);

        let runtime = at.runtime.as_ref().expect("the runtime");
        let token = CancellationToken::new();
        let began = Instant::now();
        let flusher = runtime.spawn(flush_loop(Arc::clone(&at.server), token.clone()));
        let second = second.join().expect("the second sender");
        assert!(
            began.elapsed() < WOKEN_WITHIN,
            "the body at the door was not told that the buffers had been flushed"
        );
        assert_eq!(second.status, 200, "{second:?}");
        assert_eq!(at.shed(), 0);
        assert_eq!(at.count(), 2 * PER_BODY as u64);
        token.cancel();
        runtime.block_on(flusher).expect("the flush task");
    }

    // ── The door's own decision, a look at a time ─────────────────────────────────────
    //
    // The tests below ask the door directly and turn it by hand: a body at the door is
    // a future, looked at only when the test looks at it. What happens between its first
    // look and its last is then in the test's order on every run, whatever the wait is
    // and however long the test takes over it - nothing is raced against the deadline.

    /// A runtime for a body at the door that the test turns by hand.
    fn by_hand() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime")
    }

    /// One look by a body at the door, and no more: what it had come to by the end of
    /// it. Nothing wakes it between two of these.
    fn look<F: std::future::Future>(
        runtime: &tokio::runtime::Runtime,
        door: &mut Pin<Box<F>>,
    ) -> Poll<F::Output> {
        runtime.block_on(std::future::poll_fn(|cx| {
            Poll::Ready(door.as_mut().poll(cx))
        }))
    }

    /// What a refusal at the door says: its status, whether it carries a `Retry-After`,
    /// and its error.
    fn refusal(
        runtime: &tokio::runtime::Runtime,
        refused: Result<Permit, Box<Response>>,
    ) -> (StatusCode, bool, String) {
        let Err(response) = refused else {
            panic!("the body was let in");
        };
        let status = response.status();
        let retry = response.headers().get(header::RETRY_AFTER).is_some();
        let bytes = runtime
            .block_on(axum::body::to_bytes(response.into_body(), usize::MAX))
            .expect("the refusal's body");
        let body: Value = serde_json::from_slice(&bytes).expect("a refusal is JSON");
        let error = body["error"].as_str().unwrap_or_default().to_string();
        (status, retry, error)
    }

    /// A server whose buffers one body fills past twice their limit, which no task
    /// flushes, and whose door keeps a body for `wait`. The one body is in it.
    fn full_buffers(tag: &str, wait: Duration) -> Listening {
        let storage = Storage {
            max_buffer_bytes: 1024,
            ..Storage::default()
        };
        let at = Listening::start(tag, 2, wait, storage);
        let first = at.post(lines(0)).join().expect("a sender");
        assert_eq!(first.status, 200, "{first:?}");
        assert!(at.server.stores.buffers_full());
        at
    }

    /// **A wait for the buffers that runs out sheds nobody by itself: the buffers are
    /// looked at once more.** A flush empties the buffers and only then says so to the
    /// door, so a body's wait can run out between the two - beside buffers that have
    /// room. It was shed there, and told that the buffers held nothing and were past
    /// their limit.
    ///
    /// The flush here is the test's and says nothing to the door, so the only thing that
    /// brings the body back is its own deadline, and what it finds then is what the
    /// test put there: full buffers, and it is shed with what they hold; emptied ones,
    /// and it is let in. The twenty milliseconds are how long each half takes.
    #[test]
    fn a_wait_for_the_buffers_that_runs_out_looks_at_them_once_more() {
        let at = full_buffers("door-last-look", Duration::from_millis(20));
        let runtime = by_hand();
        let door = &at.server.door;

        // Still full when the wait runs out: a shed, and the bytes in it are the ones
        // that are held.
        let (held, limit) = at.server.stores.buffer_bytes();
        assert!(held > 2 * limit, "{held} of {limit}");
        let mut waiting = Box::pin(at.server.admit_waiting());
        assert!(look(&runtime, &mut waiting).is_pending());
        assert_eq!(at.at_the_door(), 1);
        let (status, retry, error) = refusal(&runtime, runtime.block_on(&mut waiting));
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(retry, "{error}");
        let says = format!(
            "the buffers hold {} bytes of memory, past twice the {} byte limit: the flush \
             is behind what is arriving",
            commas(held),
            commas(limit)
        );
        assert!(error.starts_with(&says), "{error}");
        assert_eq!(door.shed.buffers.load(Ordering::Relaxed), 1);
        assert_eq!(at.shed(), 1);

        // Emptied while the body waits, with no word to the door: let in when its wait
        // runs out, and not shed for buffers that have room.
        let mut waiting = Box::pin(at.server.admit_waiting());
        assert!(look(&runtime, &mut waiting).is_pending());
        let flushed = at.server.stores.flush_all(FlushKind::Tick);
        assert_eq!(flushed.failed, 0);
        assert!(!at.server.stores.buffers_full());
        let admitted = runtime.block_on(&mut waiting);
        assert!(admitted.is_ok(), "shed beside buffers that had room");
        assert_eq!(at.shed(), 1, "the one shed is the first half's");
        assert_eq!(door.waits.load(Ordering::Relaxed), 2);
        assert_eq!(at.at_the_door(), 0);
        assert_eq!(
            (at.parsing(), at.inside()),
            (1, 1),
            "the permit it was given"
        );
    }

    /// **A store that stops writing while a body waits for the buffers is the reason
    /// the body is given.** The wait ran out into "the flush is behind what is
    /// arriving" whatever had happened during it; a store that cannot write is another
    /// thing to tell a shipper, and the door tells it to a body that arrives then.
    ///
    /// The body is parked on healthy, full buffers; then a flush fails, with no word to
    /// the door; then the body's wait runs out.
    #[test]
    #[cfg(unix)]
    fn a_store_that_fails_during_a_wait_for_the_buffers_is_the_reason_given() {
        use std::os::unix::fs::PermissionsExt;

        let at = full_buffers("door-last-look-store", Duration::from_millis(20));
        let runtime = by_hand();
        // The first body's day gets its file, and the buffers are empty again. Then a
        // day with no file yet, and a directory that will not take one.
        assert_eq!(at.server.stores.flush_all(FlushKind::Tick).failed, 0);
        let data = at.server.storage.base_path.as_ref().expect("on disk");
        let data = data.join(kibbr_storage::store::disk::DATA_DIR);
        let mode = |mode| std::fs::set_permissions(&data, std::fs::Permissions::from_mode(mode));
        mode(0o555).expect("chmod -w");
        let second = at
            .post(lines_at(1_705_401_000, 1))
            .join()
            .expect("a sender");
        let full = at.server.stores.buffers_full();

        let mut waiting = Box::pin(at.server.admit_waiting());
        let parked = look(&runtime, &mut waiting).is_pending();
        let failed = at.server.stores.flush_all(FlushKind::Tick).failed;
        let still_full = at.server.stores.buffers_full() && at.server.write_health.is_degraded();
        let answered = runtime.block_on(&mut waiting);
        mode(0o755).expect("chmod +w");

        assert_eq!(second.status, 200, "{second:?}");
        assert!(full, "one body did not fill the buffers");
        assert!(
            parked,
            "a body was not kept waiting for a flush that could come"
        );
        assert!(
            failed > 0 && still_full,
            "the store wrote to a read-only directory"
        );
        let (status, retry, error) = refusal(&runtime, answered);
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(retry, "{error}");
        assert!(error.contains("the store cannot write"), "{error}");
        assert!(!error.contains("the flush is behind"), "{error}");
        assert_eq!(at.server.door.shed.buffers.load(Ordering::Relaxed), 1);
        assert_eq!(at.shed(), 1);
    }

    /// **With no wait to give, a body at the door waits for nothing.** It is let in or
    /// shed at its first look, whichever of the three it would have waited for is
    /// missing - not parked on a timer for a moment that has already come, which is a
    /// millisecond's wait for a body that was to have none.
    ///
    /// Each answer here is the one look's: a body still pending after it fails the test.
    #[test]
    fn with_no_wait_a_body_at_the_door_is_answered_at_its_first_look() {
        let at = full_buffers("door-no-wait", Duration::ZERO);
        let runtime = by_hand();
        let server = &at.server;
        let shed = &server.door.shed;
        let first_look = |what: &str| {
            let mut asking = Box::pin(server.admit_waiting());
            match look(&runtime, &mut asking) {
                Poll::Ready(answer) => answer,
                Poll::Pending => panic!("{what}: a body with no wait to give was kept waiting"),
            }
        };

        // The buffers past twice their limit.
        let (status, retry, error) = refusal(&runtime, first_look("full buffers"));
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(retry && error.starts_with("the buffers hold"), "{error}");
        assert!(!error.contains("hold 0 bytes"), "{error}");
        assert_eq!(shed.buffers.load(Ordering::Relaxed), 1);
        assert_eq!(server.stores.flush_all(FlushKind::Tick).failed, 0);
        assert!(!server.stores.buffers_full());

        // No permit for the parse.
        let permits = Arc::clone(&server.ingest_permits)
            .try_acquire_many_owned(server.api.ingest_concurrency as u32)
            .expect("every permit");
        let (status, retry, error) = refusal(&runtime, first_look("no permit"));
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(retry && error.contains(SAYS_BUSY), "{error}");
        assert_eq!(shed.permit.load(Ordering::Relaxed), 1);
        drop(permits);

        // No place, and the permit that was taken on the way to it goes back.
        let places = Arc::clone(&server.ingest_bodies)
            .try_acquire_many_owned(ingest_bodies(&server.api) as u32)
            .expect("every place");
        let (status, retry, error) = refusal(&runtime, first_look("no place"));
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(retry && error.contains(SAYS_LOG_BUSY), "{error}");
        assert_eq!(shed.place.load(Ordering::Relaxed), 1);
        assert_eq!(at.parsing(), 0);
        drop(places);

        // And with nothing missing it is let in, at that same first look.
        let admitted = first_look("nothing missing");
        assert!(admitted.is_ok());
        assert_eq!((at.parsing(), at.inside()), (1, 1));
        drop(admitted);
        assert_eq!((at.parsing(), at.inside()), (0, 0));
        assert_eq!(at.shed(), 3);
        assert_eq!(at.at_the_door(), 0);
    }

    /// **A sender that hangs up while its body waits for room frees its slot.** The body
    /// is parsed and holds the one permit, and the log has no room for it; its sender
    /// then closes the connection. The body gives up its wait - it does not sit on the
    /// permit for the minute it was allowed - nothing of it is in the log, and the next
    /// body takes the permit.
    ///
    /// A body that is being parsed, or is already in the log, is another matter and is
    /// stored whole: this is only the body that has nowhere to go yet.
    #[test]
    fn a_sender_that_hangs_up_while_waiting_for_room_frees_its_slot() {
        let at = one_at_a_time("room-hangup", LONG_WAIT);
        let still = at.still();
        let first = at.post(lines(0));
        until("the first body to be in the log", || {
            at.wal().unapplied_bytes == one_body() && at.parsing() == 0
        });
        let began = {
            // Written whole, and the connection closed with no answer read.
            let body = lines(1);
            let mut second = Client::connect(at.addr);
            second.head("POST", "/ingest", body.len());
            second.write(&body);
            until("the second body to wait for room", || {
                at.wal().queue_waiting == 1
            });
            Instant::now()
        };
        until("the second body to give up its wait", || {
            at.wal().queue_left == 1 && at.parsing() == 0
        });
        assert!(
            began.elapsed() < WOKEN_WITHIN,
            "a body whose sender had hung up went on holding its permit"
        );
        assert_eq!(at.shed(), 0, "a sender that left is not a shed");

        let third = at.post(lines(2));
        until("the third body to take the permit", || {
            at.wal().queue_waiting == 1
        });
        drop(still);
        for body in [first, third] {
            let answer = body.join().expect("a sender");
            assert_eq!(answer.status, 200, "{answer:?}");
        }
        until("every place to come back", || at.inside() == 0);
        // The first and the third, and no line of the second: not stored, not logged,
        // and not counted as taken.
        assert_eq!(at.count(), 2 * PER_BODY as u64);
        assert_eq!(at.wal().last_seq, 2 * PER_BODY as u64);
        let server = &at.server;
        assert_eq!(
            server.lines_ingested.load(Ordering::Relaxed),
            2 * PER_BODY as u64
        );
        assert_eq!(
            server.lines_rejected.load(Ordering::Relaxed),
            PER_BODY as u64
        );
    }
}
