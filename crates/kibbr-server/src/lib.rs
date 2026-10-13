//! The server, as a library: `run` is given a `Settings` and a token to stop it by, and
//! serves the native HTTP surface until the token is cancelled or a signal arrives.
//!
//! Nothing here reads a flag or writes to stderr, and the flush clock lives here, not in
//! the store: deciding when to write is a fact about the process lifecycle.

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

use crate::error::{DetectorError, DetectorErrorKind, IngestError};
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

/// The most rejections one `/ingest` response will describe: a sample, since the body is
/// bounded and the response was not; `errors_omitted` counts the rest.
const MAX_REPORTED_ERRORS: usize = 1000;

/// The log viewer, compiled in: the binary is the deployment unit, and the page cannot
/// drift from the endpoints it calls.
const VIEWER: &str = include_str!("viewer.html");

/// Where the source of this build is: AGPL §13 has a server offer it to whoever uses it
/// over a network, so `/health` and the viewer's footer both carry it. A modified build
/// that is served to others names its own.
pub const SOURCE: &str = "https://github.com/pratyaydeep/kibbr";
/// This build's version, beside `SOURCE` in both places.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

// ── State ──────────────────────────────────────────────────────────────────────────

/// Everything the handlers share, none of it behind a lock of the server's, so an ingest
/// and a query proceed together and a body is not atomic to a query beside it.
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
    storage: Storage,
    /// How many bodies may be in the parse-and-walk at once: the backpressure for the part
    /// that is CPU. Given back once the body is in the log's queue, not kept across the flush.
    ingest_permits: Arc<Semaphore>,
    /// How many bodies may be between the door and their answer: `ingest_bodies`. Taken
    /// at the door with the permit above, and kept until the body's task has returned.
    ingest_bodies: Arc<Semaphore>,
    /// Whether the last flush tick could write. Shared with the registry and read without
    /// its lock, because `/health` must not queue behind a flush.
    write_health: Arc<kibbr_storage::store::WriteHealth>,
    /// The bulk loads this server has been asked to run: at most one at a time, on a
    /// thread of its own, never on the runtime or the blocking pool the door's bodies use.
    loads: std::sync::Mutex<Loads>,
    /// What `/load` and `/load/<job>` are asked with, as `LOCK` holds it. `None` without a
    /// data directory, where there is nothing to load into.
    load_token: Option<String>,
    /// The compactions this server has been asked to run or found due: at most one at a
    /// time, in a slot apart from the load's, so the two run side by side.
    compactions: std::sync::Mutex<compact::Jobs>,
    /// Set once, when shutdown begins, and the one switch every compaction here is run
    /// with: whichever is running then stops before its record and removes what it wrote.
    compact_cancel: Arc<AtomicBool>,
    /// The disk store's scan threads, where it has any: for the door to say a body is in
    /// its task, which keeps a query from sharing its scan out meanwhile.
    fan: Option<Arc<Fan>>,
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
    ) -> Result<Self, Error> {
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
                        // Every position handed over, stored or refused: a refused row arrived
                        // before the replace too, and replay must not put it into the new day.
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

/// The trie and the stores of a data directory, restored as a server's start restores
/// them but with the log left be; the caller holds the directory's `LOCK`.
pub fn open_durable(
    storage: &Storage,
    schema: &Schema,
) -> Result<(Trie, StoreManager), Error> {
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
fn restore(storage: &Storage, schema: &Schema, apply: bool) -> Result<Restored, Error> {
    let Some(base) = storage.base_path.clone() else {
        return Err(Error::Storage {
            detail: "no data directory".to_string(),
        });
    };
    let data_dir = storage.data_dir();
    let trie = open_catalog(&base, storage.trie_file.as_deref(), &data_dir, schema)?;

    let stores = StoreManager::disk(base.clone(), storage.disk_options()).map_err(storage_fatal)?;

    // Applied whatever the snapshot said: a stale snapshot that parses is as unaware of
    // stored rows as one that did not (`Trie::reserve_template_ids_through`).
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

/// Put back every line the log holds that no row group's range covers - not by the
/// checkpoint - routed by the record; one that cannot go back stays in the log.
fn replay(
    trie: &Trie,
    stores: &StoreManager,
    wal: &Wal,
    held: &Held,
    apply: bool,
) -> Result<Replayed, Error> {
    let began = Instant::now();
    let Some(disk) = stores.disk_store() else {
        return Ok(Replayed::default());
    };
    let stream = wal.stream();
    disk.attach_wal(stream, wal.checkpoint());
    let mut warn = |m: String| log_warn(&format!("wal - {m}"));
    if !apply {
        // Read for where the log ends, for the template ids it names, and for the day each
        // line is of: a day the log still holds lines for is not behind its checkpoint.
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
                    // cannot be known, so it and its template's later lines stay in the log.
                    Err(e) => {
                        damaged.get_or_insert_with(|| (record.seq, e.to_string()));
                        held.set_aside(record.template_id, record.seq);
                        out.held += 1;
                        // Remembered as nothing covered, so the headers are not read again
                        // for every later line of the day.
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

/// Rows the store would not take from the log: not an error, since the line is durable
///; the checkpoint stays behind it, and its template takes nothing more here.
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

/// A store that will not open, as the error startup exits on: an earlier layout or
/// another format version is named, not reported as a disk that failed.
fn storage_fatal(e: StoreError) -> Error {
    match e {
        StoreError::LegacyLayout { path } => Error::LegacyLayout { path },
        StoreError::Format(kibbr_storage::store::v3::FormatError::Version { path, found }) => {
            Error::SegmentVersion { path, found }
        }
        StoreError::Format(kibbr_storage::store::v3::FormatError::PreRename { path }) => {
            Error::PreRename { path }
        }
        e => Error::Storage {
            detail: error::chain(&e),
        },
    }
}

/// The trie, from `catalog.log`, with the catalog attached so every later template is
/// durable before it exists; a `trie.json` alone is migrated once.
fn open_catalog(
    base: &Path,
    trie_file: Option<&Path>,
    data_dir: &str,
    schema: &Schema,
) -> Result<Trie, Error> {
    let path = kibbr_storage::catalog::path_in(base);
    let existed = path.exists();
    let mut warn = |message: String| log_warn(&format!("catalog.log - {message}"));
    fn storage_error(e: &(dyn std::error::Error + 'static)) -> Error {
        Error::Storage {
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
                        // Reopen and read back what was just written, so the trie this process
                        // serves is the one the next process will rebuild.
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
    // A template from before the time source existed has it derived from its column
    // types; written now, once, so the next start makes the same choice.
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

pub mod compact;
mod compaction;
mod error;
mod handlers;
mod ingest;
mod lifecycle;
pub mod load;
pub mod query;
mod routes;
pub mod settings;
#[cfg(test)]
mod tests;

pub use error::{Error, Result};
pub use lifecycle::{run, Report};
pub use settings::{ApiLimits, Banner, Limits, Schema, Settings, Storage, BANNER};

use compaction::*;
use handlers::*;
use ingest::*;
use lifecycle::*;
use routes::*;
