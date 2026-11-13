// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The whole database behind one handle: `Database`, the three ways it opens, and the
//! `Options` it is opened with.
//!
//! No HTTP, no flags and no async: the server and the command line each hold one
//! `Database`, and what they do to the data goes through it. Back-pressure comes out of
//! it as values; whoever holds the handle decides what a wait or a refusal looks like.

pub mod compaction;
pub(crate) mod lifecycle;

use std::io::Cursor;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::engine::error::DetectorError;
use crate::engine::model::types::ColumnType;
use crate::engine::query::{self, ExecError, Plan, Query, QueryResult};
use crate::engine::error::{self, DetectorErrorKind, Error, IngestError};
use lifecycle::{log, log_warn, Held, Replayed};
use crate::engine::load;
use crate::engine::templates::catalog::CatalogError;
use crate::engine::ingest;
use crate::engine::lock::{DataLock, Holder};
use crate::engine::store::disk::{self, CompactOptions};
use crate::engine::store::fan::{self, Fan};
use crate::engine::stop::Stop;
use crate::engine::store::{FlushSummary, StoreError, StoreManager, WriteHealth};
use crate::engine::templates::trie::{Trie, TrieError};
use crate::engine::wal::{self, Wal};

pub use crate::engine::templates::trie::Schema;

/// The defaults of `Storage`'s fields that the stores and the log decide.
pub use crate::engine::store::disk::DEFAULT_SEGMENT_MAX_BYTES;
pub use crate::engine::store::{
    DEFAULT_FAN_ROWS, DEFAULT_MAX_BUFFER_BYTES, DEFAULT_MAX_OPEN_SEGMENTS,
};
pub use crate::engine::wal::{
    DEFAULT_BATCH_LINES as DEFAULT_WAL_BATCH_LINES, DEFAULT_QUEUE_BYTES as DEFAULT_WAL_QUEUE_BYTES,
    DEFAULT_SEGMENT_BYTES as DEFAULT_WAL_SEGMENT_BYTES,
};

// ── Options ────────────────────────────────────────────────────────────────────────

/// How often the thresholds that close a row group are checked, which bounds how late a
/// fired threshold is noticed and is not a durability interval.
pub const DEFAULT_FLUSH_INTERVAL_MS: usize = 500;

/// How long a row group may stay open with rows in it: five minutes, which costs replay
/// time and no rows, and cannot go below the flush interval whose tick notices it.
pub const DEFAULT_ROW_GROUP_TIMEOUT_MS: usize = 300_000;

/// How often a server looks for a day that has ended and compacts it; `0` switches the
/// look off, and `kibbr compact` still compacts a day when asked.
pub const DEFAULT_COMPACT_INTERVAL_MS: usize = 60_000;

/// Where rows and the trie snapshot live, and how often the buffer reaches them.
#[derive(Debug, Clone)]
pub struct Storage {
    /// `None` = memory, and everything ingested is lost at exit.
    pub base_path: Option<PathBuf>,
    /// `None` exactly when `base_path` is.
    pub trie_file: Option<PathBuf>,
    pub flush_interval: Duration,
    /// What a row group closes on, when no size threshold got there first.
    pub row_group_timeout: Duration,
    /// The cap on data files open for writing, a setting because it follows the process's
    /// descriptor limit (`store::DEFAULT_MAX_OPEN_SEGMENTS`).
    pub max_open_segments: usize,
    /// A day's data file is sealed at this size and the next number opened.
    pub segment_max_bytes: u64,
    /// Memory every buffer together may hold before the largest are closed early.
    pub max_buffer_bytes: u64,
    /// How often a server looks for a day that is due for compaction. Zero is never.
    pub compact_interval: Duration,
    /// A segment of the write-ahead log rolls to the next number at this size.
    pub wal_segment_bytes: u64,
    /// Where one flush of the write-ahead log stops taking more bodies: once it holds
    /// this many lines.
    pub wal_batch_lines: usize,
    /// The most bytes of lines that may be in the write-ahead log and not yet in the
    /// store: past it a body waits for room, and is then shed.
    pub wal_queue_bytes: usize,
    /// Threads one query may choose a day's rows on, its own among them; only a server asks
    /// for more than its own (`with_scan`).
    pub scan_threads: usize,
    /// A run of row groups holding fewer rows than this is never shared between those
    /// threads: see `store::DEFAULT_FAN_ROWS`.
    pub scan_fan_rows: u64,
}

impl Default for Storage {
    fn default() -> Self {
        Self {
            base_path: None,
            trie_file: None,
            flush_interval: Duration::from_millis(DEFAULT_FLUSH_INTERVAL_MS as u64),
            row_group_timeout: Duration::from_millis(DEFAULT_ROW_GROUP_TIMEOUT_MS as u64),
            max_open_segments: crate::engine::store::DEFAULT_MAX_OPEN_SEGMENTS,
            segment_max_bytes: crate::engine::store::disk::DEFAULT_SEGMENT_MAX_BYTES,
            max_buffer_bytes: crate::engine::store::DEFAULT_MAX_BUFFER_BYTES,
            compact_interval: Duration::from_millis(DEFAULT_COMPACT_INTERVAL_MS as u64),
            wal_segment_bytes: crate::engine::wal::DEFAULT_SEGMENT_BYTES,
            wal_batch_lines: crate::engine::wal::DEFAULT_BATCH_LINES,
            wal_queue_bytes: crate::engine::wal::DEFAULT_QUEUE_BYTES,
            scan_threads: 0,
            scan_fan_rows: crate::engine::store::DEFAULT_FAN_ROWS,
        }
    }
}

impl Storage {
    /// What the write-ahead log is written with.
    pub fn wal_options(&self) -> crate::engine::wal::Options {
        crate::engine::wal::Options {
            segment_bytes: self.wal_segment_bytes,
            batch_lines: self.wal_batch_lines,
            queue_bytes: self.wal_queue_bytes,
        }
    }

    pub fn is_durable(&self) -> bool {
        self.base_path.is_some()
    }

    /// The row group thresholds these settings imply; only the timeout is worth a knob.
    pub fn row_group_limits(&self) -> crate::engine::store::RowGroupLimits {
        crate::engine::store::RowGroupLimits {
            age: self.row_group_timeout,
            ..crate::engine::store::RowGroupLimits::default()
        }
    }

    /// What the disk store is held to.
    pub fn disk_options(&self) -> crate::engine::store::DiskOptions {
        crate::engine::store::DiskOptions {
            row_groups: self.row_group_limits(),
            max_open_files: self.max_open_segments,
            segment_max_bytes: self.segment_max_bytes,
            max_buffer_bytes: self.max_buffer_bytes,
            scan_threads: self.scan_threads,
            fan_rows: self.scan_fan_rows,
        }
    }

    /// What a freshly-stamped `LeafMeta` records as its storage path: documentation
    /// rather than authority, see `trie::DEFAULT_DATA_DIR`.
    pub fn data_dir(&self) -> String {
        self.base_path.as_ref().map_or_else(
            || crate::engine::templates::trie::DEFAULT_DATA_DIR.to_string(),
            |path| path.display().to_string(),
        )
    }
}

/// What a database is opened with: where and how its rows are kept, and which key names
/// may carry a template's time. No directory in `storage` is a database in memory.
#[derive(Debug, Clone, Default)]
pub struct Options {
    pub storage: Storage,
    pub schema: Schema,
}

// ── The handle ─────────────────────────────────────────────────────────────────────

/// The most rejections one ingested body will describe: a sample, since the body is
/// bounded and the report was not; `omitted` counts the rest.
pub const MAX_REPORTED_ERRORS: usize = 1000;

/// One database: the trie, the stores, the write-ahead log and the directory's lock,
/// none of it behind a lock of the handle's, so an ingest and a query proceed together
/// and a body is not atomic to a query beside it.
pub struct Database {
    pub(crate) trie: Trie,
    /// Shared with the write-ahead log's applier, which is the one thread that appends a
    /// row behind a log; everything else here reads it, flushes it, or loads into it.
    pub(crate) stores: Arc<StoreManager>,
    /// The write-ahead log. `None` with `--storage memory`: nothing is durable there, and
    /// a line is applied by the body that carried it. `None` offline too, where the log is
    /// read and left as it is.
    pub(crate) wal: Option<Wal>,
    /// Rows the store would not take from the log, and what that holds back.
    pub(crate) held: Arc<Held>,
    /// What the start read back from the log: `/stats`'s `storage.wal.replay`.
    pub(crate) replayed: Replayed,
    /// Whether the last move of the checkpoint failed: said once when it starts.
    pub(crate) checkpoint_failing: AtomicBool,
    pub(crate) storage: Storage,
    /// Whether the last flush tick could write. Shared with the registry and read without
    /// its lock, because `/health` must not queue behind a flush.
    write_health: Arc<WriteHealth>,
    /// The compactions this database has been asked to run or found due: at most one at a
    /// time, in a slot apart from a load's, so the two run side by side.
    compactions: Mutex<compaction::Jobs>,
    /// Set once, when shutdown begins, and the one switch every compaction here is run
    /// with: whichever is running then stops before its record and removes what it wrote.
    compact_cancel: Arc<AtomicBool>,
    /// The disk store's scan threads, where it has any: for the door to say a body is in
    /// its task, which keeps a query from sharing its scan out meanwhile.
    fan: Option<Arc<Fan>>,
    /// The directory's `LOCK`, held from the open until `unlock`; `None` in memory.
    lock: Mutex<Option<DataLock>>,
}

/// A data directory claimed to be served, and not yet opened: one holding another format
/// version is refused, and its `LOCK` taken, before anything in it is written. A server
/// binds its port between the claim and the open.
pub struct Claim {
    options: Options,
    lock: Option<DataLock>,
}

impl Claim {
    /// A claim with no lock taken, for a test that opens a directory as a start does.
    #[cfg(test)]
    pub fn unlocked(options: Options) -> Self {
        Self {
            options,
            lock: None,
        }
    }

    /// Whether a directory was claimed: `false` for a database in memory.
    pub fn is_durable(&self) -> bool {
        self.options.storage.is_durable()
    }

    /// Restore the trie and the stores, replay the log and apply it, and start the log: a
    /// server's start. In memory, an empty database. The lock goes to the database only
    /// once it is open: on a failure the claim still holds it, so whoever holds the claim
    /// lets go of what it took after the claim (a port) before the lock.
    pub fn open(&mut self) -> Result<Database, Error> {
        let db = Database::serving(self.options.clone(), None)?;
        *db.lock.lock().unwrap_or_else(|e| e.into_inner()) = self.lock.take();
        Ok(db)
    }
}

/// Who opens a data directory offline, as its `LOCK` names them to another process that
/// looks: a load, a compaction, or a measurement that says nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Opener {
    Load,
    Compact,
    Quiet,
}

impl Database {
    /// Refuse a directory that holds another format version and take its `LOCK`, before
    /// anything is written; nothing is read back yet.
    pub fn claim(options: Options) -> Result<Claim, Error> {
        // Read-only, so it runs before the lock and the port: a data directory holding a
        // segment from another format version is not served at all.
        if let Some(base) = options.storage.base_path.as_deref() {
            disk::refuse_other_versions(base).map_err(lifecycle::storage_fatal)?;
        }
        // Taken before anything is written and before the port: two servers on one directory
        // would each append to the catalog and the manifest over the other's records.
        let lock = match options.storage.base_path.as_deref() {
            Some(base) => {
                let mut lock = DataLock::acquire(base)?;
                let _ = lock.announce(&Holder::server(None, None));
                Some(lock)
            }
            None => None,
        };
        Ok(Claim { options, lock })
    }

    /// A database with no directory and no log: a line is applied by the call that
    /// carried it, and everything is gone when the handle is.
    pub fn in_memory(options: Options) -> Self {
        let mut options = options;
        options.storage.base_path = None;
        options.storage.trie_file = None;
        Self::assemble(
            Trie::new().with_time_columns(options.schema.time_columns.clone()),
            Arc::new(StoreManager::memory()),
            None,
            Arc::new(Held::new()),
            Replayed::default(),
            options.storage,
            None,
        )
    }

    /// Open a data directory with its log left as it is: what `kibbr load`, `kibbr compact`
    /// and `kibbr bench-storage` open when no server holds it. The lock is taken here, and
    /// another holder is the `Error::Lock` that names it.
    pub fn open_offline(options: Options, opener: Opener) -> Result<Self, Error> {
        let Some(base) = options.storage.base_path.clone() else {
            return Err(Error::Storage {
                detail: "no data directory".to_string(),
            });
        };
        Self::open_offline_locking(&base, options, opener)
    }

    /// `open_offline`, with the lock taken through `lock_dir`: the same directory, by the
    /// path a caller built rather than the canonical one, which a refusal then names.
    pub fn open_offline_locking(
        lock_dir: &std::path::Path,
        options: Options,
        opener: Opener,
    ) -> Result<Self, Error> {
        let Some(base) = options.storage.base_path.clone() else {
            return Err(Error::Storage {
                detail: "no data directory".to_string(),
            });
        };
        let mut lock = DataLock::acquire(lock_dir)?;
        let holder = match opener {
            Opener::Load => Some(Holder::load()),
            Opener::Compact => Some(Holder::compact()),
            Opener::Quiet => None,
        };
        if let Some(holder) = holder {
            let _ = lock.announce(&holder);
        }
        disk::refuse_other_versions(&base).map_err(Error::Store)?;
        let restored = lifecycle::restore(&options.storage, &options.schema, false)?;
        Ok(Self::assemble(
            restored.trie,
            Arc::new(restored.stores),
            None,
            Arc::new(restored.held),
            restored.replayed,
            options.storage,
            Some(lock),
        ))
    }

    /// Build the state, restoring what is on disk (`open_durable`).
    fn serving(options: Options, lock: Option<DataLock>) -> Result<Self, Error> {
        let Options { storage, schema } = options;
        let (trie, stores, wal, held, replayed) = match storage.base_path.as_deref() {
            Some(base) => {
                let restored = lifecycle::restore(&storage, &schema, true)?;
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
        Ok(Self::assemble(
            trie, stores, wal, held, replayed, storage, lock,
        ))
    }

    fn assemble(
        trie: Trie,
        stores: Arc<StoreManager>,
        wal: Option<Wal>,
        held: Arc<Held>,
        replayed: Replayed,
        storage: Storage,
        lock: Option<DataLock>,
    ) -> Self {
        let write_health = stores.health();
        let fan = stores.fan();
        Self {
            trie,
            stores,
            wal,
            held,
            replayed,
            checkpoint_failing: AtomicBool::new(false),
            storage,
            write_health,
            compactions: Mutex::new(compaction::Jobs::default()),
            compact_cancel: Arc::new(AtomicBool::new(false)),
            fan,
            lock: Mutex::new(lock),
        }
    }

    /// Say who holds the directory, in its `LOCK`: a server's address once it can answer,
    /// and the token a `/load` shows. Nothing in memory, which has no lock.
    pub fn announce(
        &self,
        address: Option<String>,
        token: Option<String>,
    ) -> Result<(), crate::engine::lock::LockError> {
        let mut lock = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        match lock.as_mut() {
            Some(lock) => lock.announce(&Holder::server(address, token)),
            None => Ok(()),
        }
    }

    /// A token nobody can guess, for whoever holds the directory to have a load show: see
    /// `lock.rs`.
    pub fn new_token() -> Result<String, Error> {
        crate::engine::format::file::new_file_id()
            .map(|id| crate::engine::store::manifest::hex(&id))
            .map_err(|e| Error::Storage {
                detail: format!("drawing the load token: {e}"),
            })
    }

    /// The settings the database was opened with.
    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    /// Whether what is ingested outlives the process.
    pub fn is_durable(&self) -> bool {
        self.storage.is_durable()
    }

    /// Whether there is a write-ahead log: a served data directory has one.
    pub fn has_log(&self) -> bool {
        self.wal.is_some()
    }

    /// Whether rows are kept in files, which a load and a compaction need.
    pub fn has_disk_store(&self) -> bool {
        self.stores.disk_store().is_some()
    }

    // ── Close ──

    /// The fixed close order: final flush, compactions joined, the checkpoint, `trie.json`,
    /// the stores, the log; each step said, and a memory database's as skipped.
    pub fn close(&self) -> Result<(), Error> {
        lifecycle::close(self)
    }

    /// Let the directory's `LOCK` go, once nothing more is written: the next process may
    /// take it.
    pub fn unlock(&self) {
        drop(self.lock.lock().unwrap_or_else(|e| e.into_inner()).take());
    }

    // ── The live door ──

    /// An NDJSON body into the database, in the order that is the durability: parsed, routed
    /// to a template made durable, logged, and answered once the rows are applied.
    ///
    /// `wait` is how long a body may wait for room in the log and `left` says its sender has
    /// gone; `queued` is called once the body is in the log's queue, or applied where there
    /// is no log - what was held for the parse goes back then, not after the flush.
    pub fn ingest(
        &self,
        body: &[u8],
        now: DateTime<Utc>,
        wait: Duration,
        left: &AtomicBool,
        queued: impl FnOnce(),
    ) -> Result<Ingested, Refused> {
        // A body no write of the log would hold is refused before it is parsed: it would
        // be refused after, and the parse of that many lines is the cost.
        if let Some(wal::Refused::TooLarge { bytes, limit }) =
            self.wal.as_ref().and_then(|_| wal::too_large(body))
        {
            return Err(Refused::uncounted(IngestError::TooLarge { bytes, limit }));
        }

        // ── Half one: split and parse, holding nothing ─────────────────────────────────
        let mut parse_errors = Vec::new();
        let (parsed, stats) = ingest::parse_and_prepare(Cursor::new(body), &mut |e: DetectorError| {
            // Past the cap the rejection is counted and dropped. `stats.rejected` is
            // still the true total, so nothing is lost but the text.
            if parse_errors.len() < MAX_REPORTED_ERRORS {
                parse_errors.push(LineError {
                    line: e.line,
                    msg: e.kind.to_string(),
                });
            }
        })
        // Reading a body in memory fails only on a line longer than a line may hold, which a
        // larger `--max-body-bytes` allows: a message, and a 500.
        .map_err(|e| Refused::uncounted(IngestError::Body(error::chain(&e))))?;

        // ── Half two: route each line to its template, durably ─────────────────────────
        //
        // Before the log, because the log's record carries the template's id: the catalog
        // record that defines the id is flushed by the time `place_in` returns.
        let mut apply_errors = Vec::new();
        let mut rejected = stats.rejected;
        let mut unwritten = 0u64;
        let mut lines: Vec<wal::Line> = Vec::with_capacity(parsed.len());
        for (line, parsed, prepared) in parsed {
            let kind = match ingest::place_in(&self.trie, &parsed, prepared, now) {
                Ok(Some(placed)) => {
                    lines.push(wal::Line {
                        raw: parsed.raw_line,
                        // The time the row is filed under, as the store reads it: what the
                        // log writes down beside the line, for replay to hold it to.
                        time: crate::engine::store::effective_ts(&placed.row, &placed.time_column)
                            .unwrap_or(0),
                        placed,
                    });
                    continue;
                }
                // The template's own lock is poisoned: nothing can say which template the
                // line is, so there is no id to log it under.
                Ok(None) => DetectorErrorKind::NotDurable {
                    detail: "the template's lock is poisoned".to_string(),
                },
                // The line parsed and its template could not be made durable: reported as a
                // line that did not parse, and the message says which.
                Err(e @ TrieError::Catalog(_)) => {
                    // Counted apart, as a write that failed and is worth sending again; a record
                    // that would not encode would not encode the next time either.
                    if !matches!(e, TrieError::Catalog(CatalogError::Serialize { .. })) {
                        unwritten += 1;
                    }
                    DetectorErrorKind::NotDurable {
                        detail: error::chain(&e),
                    }
                }
                // The trie's lock poisoned, which fails every line after it too.
                Err(e) => return Err(Refused::uncounted(e.into())),
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
        // With a log, the lines are durable in one flush and applied in the log's order; with
        // none, applied here. All of the body or none of it, so a refused sender can resend.
        let ingested = lines.len() as u64;
        match &self.wal {
            Some(wal) => {
                // A body the log refuses was read and routed, and none of it was taken: its
                // lines are counted as read and as rejected, whichever refusal it was.
                let refused = |refused: wal::Refused| Refused {
                    read: stats.read,
                    rejected: rejected + ingested,
                    error: match refused {
                        wal::Refused::Cannot(why) => IngestError::Log(why),
                        wal::Refused::Busy => IngestError::Busy,
                        wal::Refused::Left => IngestError::Left,
                        wal::Refused::TooLarge { bytes, limit } => {
                            IngestError::TooLarge { bytes, limit }
                        }
                    },
                };
                // A log with no room makes the body wait, for as long as a body may be kept
                // waiting: shed at once, its sender's retry pays for the parse again.
                let wait = wal::Wait {
                    at_most: wait,
                    left,
                };
                let ticket = wal
                    .enqueue(lines, ingest::arrival_nanos(now), wait)
                    .map_err(&refused)?;
                // The permit is for the parse and the routing, so it is not held across the flush;
                // kept across a wait for room, it bounds the parsed bodies held in memory.
                queued();
                ticket.wait().map_err(&refused)?;
            }
            None => {
                for line in lines {
                    self.held.apply(&self.stores, 0, line.placed);
                }
                queued();
            }
        }

        // Two passes meet their rejections in two runs, so the merge puts them back in line
        // order; capping each half at `MAX_REPORTED_ERRORS` first loses nothing.
        let mut errors = parse_errors;
        errors.extend(apply_errors);
        errors.sort_by_key(|e| e.line);
        errors.truncate(MAX_REPORTED_ERRORS);

        Ok(Ingested {
            read: stats.read,
            ingested,
            rejected,
            omitted: rejected - errors.len() as u64,
            errors,
            unwritten,
            // Past the memory limit the flush runs now, not at its next tick: a burst that
            // filled the buffers in one body should not get half a second more to fill them.
            over_buffer_limit: self.stores.over_buffer_limit(),
        })
    }

    /// Why the write-ahead log cannot write, while it cannot: every body is refused then.
    pub fn log_refusal(&self) -> Option<String> {
        self.wal.as_ref().and_then(Wal::refusal)
    }

    /// Whether the buffers are past twice their limit: the flush is falling behind, and a
    /// body taken now is memory the process may not have.
    pub fn buffers_full(&self) -> bool {
        self.stores.buffers_full()
    }

    /// What the buffers hold, and the limit past which the largest are closed early.
    pub fn buffer_bytes(&self) -> (u64, u64) {
        self.stores.buffer_bytes()
    }

    /// Buffers closed early for the memory limit, since the start.
    pub fn forced_closes(&self) -> u64 {
        self.stores.forced_closes()
    }

    /// Whether the last flush tick could not write: nothing then leaves the buffers.
    pub fn store_failing(&self) -> bool {
        self.write_health.is_degraded()
    }

    /// The error the last failed write ended with.
    pub fn last_write_error(&self) -> Option<String> {
        self.write_health.last_error()
    }

    /// How many bodies are waiting for room in the log.
    pub fn waiting_for_room(&self) -> usize {
        self.wal.as_ref().map_or(0, Wal::waiting_for_room)
    }

    /// Have every body waiting for room in the log look again: one whose sender has gone
    /// gives its wait up. Takes the queue's lock, which the log's writer can keep a while.
    pub fn wake_room(&self) {
        if let Some(wal) = &self.wal {
            wal.wake_room();
        }
    }

    /// Stopping: no body waits for room in the log from here on.
    pub fn hurry(&self) {
        if let Some(wal) = &self.wal {
            wal.hurry();
        }
    }

    /// Held for as long as an ingested body is in its task: a query meanwhile scans on its
    /// own thread, so the scan takes no core from the parse. `None` with no scan threads.
    pub fn ingest_body(&self) -> Option<fan::Body<'_>> {
        self.fan.as_deref().map(Fan::body)
    }

    /// The scan threads, for whoever must let them go last: see `store::fan`.
    pub fn fan(&self) -> Option<Arc<Fan>> {
        self.fan.clone()
    }

    /// The threads one query may choose a day's rows on, its own among them; 1 where there
    /// is nobody to share with - the memory store, or a disk store told 0 or 1.
    pub fn scan_threads(&self) -> usize {
        self.fan.as_ref().map_or(1, |_| self.storage.scan_threads)
    }

    // ── Queries ──

    /// The templates a query must read, without reading a row.
    pub fn plan(&self, query: &Query) -> Plan {
        query::plan(&self.trie, query)
    }

    /// A query over the stores.
    pub fn execute(&self, query: &Query) -> Result<QueryResult, ExecError> {
        query::execute(&self.trie, &self.stores, query)
    }

    /// The answer as the one JSON every surface gives (`answer::result_json`): the scan and
    /// then the answer put into JSON both ask `stop` as they go.
    pub fn answer_until(&self, query: &Query, stop: &Stop) -> Result<serde_json::Value, ExecError> {
        query::execute_until(&self.trie, &self.stores, query, stop).and_then(|result| {
            crate::engine::query::answer::result_json_until(query, &result, stop)
                .ok_or(ExecError::Store(StoreError::Stopped))
        })
    }

    // ── Loads ──

    /// A bulk load of closed days: staged, and committed in one manifest record.
    /// `on_error` is told of each line that does not parse.
    pub fn load(
        &self,
        options: &load::Options,
        control: &load::Control,
        on_error: &mut dyn FnMut(&DetectorError),
    ) -> Result<load::Report, load::LoadError> {
        load::run(&self.trie, &self.stores, options, control, on_error)
    }

    /// Where a load stages its files, which the next start deletes: `None` in memory.
    pub fn staging_dir(&self) -> Option<PathBuf> {
        self.storage
            .base_path
            .as_ref()
            .map(|base| base.join(disk::STAGING_DIR))
    }

    // ── A database read from a file ──

    /// Every line of `input` into this database, each arriving at `now` and with no log
    /// behind it, a line that does not parse reported and skipped: what `kibbr query FILE`
    /// answers from. With `rows` false only the templates are learned, as `--explain` needs.
    pub fn ingest_file<R: std::io::BufRead>(
        &self,
        input: R,
        now: DateTime<Utc>,
        rows: bool,
    ) -> Result<ingest::LoadStats, ingest::ReadError> {
        ingest::read_lines(
            input,
            &mut |_, parsed| ingest::ingest_line(&self.trie, rows.then_some(&*self.stores), &parsed, now),
            &mut ingest::report_rejected,
        )
    }

    /// One parsed line's template, and no row: a trie to plan on, for `kibbr-bench`'s plan
    /// step.
    #[cfg(feature = "bench")]
    pub fn learn(
        &self,
        parsed: &crate::engine::parse::flatten::ParsedLog,
        now: DateTime<Utc>,
    ) -> Result<(), TrieError> {
        self.trie.ingest(parsed, now).map(drop)
    }

    // ── The periodic work ──

    /// One flush tick: every row group that is due is closed and written, and the
    /// checkpoint moved up behind it.
    pub fn flush_due(&self) -> FlushSummary {
        let summary = self.stores.flush_all(crate::engine::store::FlushKind::Tick);
        // Behind the flush, never ahead of it: the checkpoint moves past a row only
        // once the row group holding it has been flushed and published.
        lifecycle::move_checkpoint(self);
        summary
    }

    /// After a flush: ask a log that cannot write to try again, and move its checkpoint up
    /// behind the oldest row still only in memory.
    #[cfg(test)]
    pub fn checkpoint(&self) -> Option<wal::Advanced> {
        lifecycle::move_checkpoint(self)
    }

    // ── What the read endpoints report ──

    /// What is wrong with this database's writing, if anything is: the log first, because
    /// while it cannot write `/ingest` refuses every body.
    pub fn degraded(&self) -> Option<String> {
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
                crate::engine::display::commas(rows)
            )),
        }
    }

    /// Every template with its path, which is its identity: the columns alone do not say
    /// which names were promoted into the fingerprint.
    pub fn templates(&self) -> Vec<Template> {
        let stores = &self.stores;
        let mut templates = Vec::new();

        for (path, leaf) in self.trie.leaf_paths() {
            // A leaf whose meta will not read is a poisoned lock: skipped rather than faked, and
            // `count` below is the length of what was readable.
            let Ok(slot) = leaf.leaf_meta.read() else {
                continue;
            };
            let Some(meta) = slot.as_ref() else {
                continue;
            };

            templates.push(Template {
                template_id: meta.template_id,
                path,
                columns: meta
                    .columns
                    .iter()
                    .map(|col| TemplateColumn {
                        name: col.name.clone(),
                        col_type: col.col_type,
                        widened_at: col.widened_at,
                    })
                    .collect(),
                // A template with no store is one that exists and holds no rows. Zero, not
                // absent - the two empty answers stay apart here as they do in the engine.
                rows: stores.lookup(meta.template_id).map_or(0, |s| s.len()),
                first_seen: meta.first_seen,
                last_seen: meta.last_seen,
                storage_path: meta.storage_path.clone(),
                // `column:<name>` or `arrival`, as the catalog spells it.
                time_source: meta.time_source.as_record(),
            });
        }
        templates
    }

    /// The storage half of `/stats`, read as it stands; whoever holds the handle adds its
    /// own counters.
    pub fn stats(&self) -> Stats {
        let stores = &self.stores;
        let (open_segments, max_open_segments) = stores.open_files();
        let (buffered_bytes, max_buffer_bytes) = stores.buffer_bytes();
        let wal = self.wal.as_ref().map(Wal::stats);
        Stats {
            kind: stores.kind(),
            base_path: stores.base_path().map(|p| p.to_path_buf()),
            catalog: self.trie.catalog_path(),
            wal_dir: self.wal.as_ref().map(|w| w.dir().to_path_buf()),
            held_rows: self.held.rows(),
            replayed: self.replayed,
            stores_open: stores.open(),
            open_segments,
            max_open_segments,
            rows: stores.rows(),
            buffered_rows: stores.buffered(),
            buffered_bytes,
            max_buffer_bytes,
            compaction: self
                .compactions
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .stats(),
            forced_closes: stores.forced_closes(),
            write_failures: stores.write_failures(),
            last_write_error: stores.last_write_error(),
            degraded: stores.is_degraded(),
            approx_bytes: stores.approx_bytes() + wal.as_ref().map_or(0, |wal| wal.bytes),
            wal,
            templates: self.trie.template_count(),
            nodes: self.trie.node_count(),
            tokens: self.trie.token_count(),
            scan_fan_outs: self.fan.as_ref().map_or(0, |fan| fan.fanned()),
        }
    }

    // ── Compactions ──

    /// Start a compaction job of `day`, or of every day that is due, on a thread of its
    /// own, and say its number; one runs at a time.
    pub fn compact(
        self: &Arc<Self>,
        day: Option<i64>,
        threads: usize,
    ) -> Result<u64, CompactRefused> {
        let mut jobs = self.compactions.lock().unwrap_or_else(|e| e.into_inner());
        if jobs.is_stopping() {
            return Err(CompactRefused::Stopping);
        }
        let Some(id) = jobs.start() else {
            return Err(CompactRefused::Running(jobs.running()));
        };
        // Reap the last one's thread, which has finished: no job was running, and a job's
        // state is the last thing its thread touches here.
        if let Some(done) = jobs.asked.take() {
            let _ = done.join();
        }
        let held = Arc::clone(self);
        let spawned = std::thread::Builder::new()
            .name(format!("compact-{id}"))
            .spawn(move || held.compaction(day, threads, Some(id)));
        match spawned {
            Ok(handle) => {
                jobs.asked = Some(handle);
                Ok(id)
            }
            Err(e) => {
                jobs.forget(id);
                Err(CompactRefused::Spawn(e))
            }
        }
    }

    /// Compact `day`, or every day that is due, here and now on `threads`, telling `each`
    /// how every day went: what `kibbr compact` runs alone. The days it tried, and how the
    /// run ended; `None` for a database with no files.
    pub fn compact_now(
        &self,
        day: Option<i64>,
        threads: usize,
        each: &mut dyn FnMut(compaction::Planned, &Result<disk::Compacted, StoreError>),
    ) -> Option<(Vec<compaction::Planned>, compaction::Ran)> {
        let store = self.stores.disk_store()?;
        let opts = CompactOptions {
            threads,
            cancel: Arc::default(),
        };
        let days = compaction::plan(store, day);
        let ran = compaction::run(store, &days, &opts, each);
        Some((days, ran))
    }

    /// How compaction job `id` stands, while it is one of the recent ones.
    pub fn compaction_state(&self, id: u64) -> Option<compaction::State> {
        self.compactions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .state(id)
    }

    /// One look for due days: start the scheduler's thread, unless the store cannot write,
    /// a compaction is running, shutdown has begun, or the last look's thread is still at it.
    /// Whoever calls it leaves it while a load runs, which would waste the rebuild.
    pub fn compact_due(self: &Arc<Self>) {
        if self.stores.is_degraded() {
            return;
        }
        let mut jobs = self.compactions.lock().unwrap_or_else(|e| e.into_inner());
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
        let held = Arc::clone(self);
        let spawned = std::thread::Builder::new()
            .name("compact-due".to_string())
            .spawn(move || held.compaction(None, load::server_threads(), None));
        match spawned {
            Ok(handle) => jobs.scheduled = Some(handle),
            Err(e) => log_warn(&format!("compaction - starting the look for due days: {e}")),
        }
    }

    /// Shutdown has begun: tell the running compaction to stop and start no other, at the
    /// signal rather than at the join, so there is less to wait for.
    pub fn stop_compactions(&self) {
        let mut jobs = self.compactions.lock().unwrap_or_else(|e| e.into_inner());
        jobs.stop();
        self.compact_cancel.store(true, Ordering::Relaxed);
    }

    /// Wait for the compaction threads: after the final flush, so a kill during the wait
    /// costs nothing, and before the lock is released.
    pub(crate) fn join_compactions(&self) {
        self.stop_compactions();
        let threads = {
            let mut jobs = self.compactions.lock().unwrap_or_else(|e| e.into_inner());
            [jobs.asked.take(), jobs.scheduled.take()]
        };
        for thread in threads.into_iter().flatten() {
            if !thread.is_finished() {
                log("waiting for the running compaction to stop");
            }
            let _ = thread.join();
        }
    }

    /// A compaction thread. It lists the due days itself, since the listing takes the flush's
    /// lock, and the scheduler's takes a job number only once it has a day to try.
    fn compaction(&self, day: Option<i64>, threads: usize, job: Option<u64>) {
        let jobs = || self.compactions.lock().unwrap_or_else(|e| e.into_inner());
        let every = self.storage.compact_interval;
        let scheduled = job.is_none();
        // Held from the moment there is a job, so that nothing which ends this thread - the
        // panic caught below, or one that is not - leaves the job `running`.
        let ending = |id| compaction::Ending::of(&self.compactions, id);
        let asked = job.map(ending);
        let Some(store) = self.stores.disk_store() else {
            if let Some(asked) = asked {
                let why = compaction::NO_DISK.to_string();
                let state = compaction::State::Failed(why, compaction::Report::default());
                asked.finish(state, false);
            }
            return;
        };
        let mut days = compaction::plan(store, day);
        let (id, ending) = match job.zip(asked) {
            Some(asked) => asked,
            None => {
                // A failed day with nothing left to compact is never tried again, so its failure is
                // dropped here. Listed before the jobs' lock is taken: the two are never held together.
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
                Some(day) => crate::engine::store::disk::day_name(day),
                None => format!("{} day(s) due", days.len()),
            }
        ));
        let opts = CompactOptions {
            threads,
            cancel: Arc::clone(&self.compact_cancel),
        };
        let mut noted = 0;
        // A panic is a failed job and not a job running for ever, as for a load. Whatever it
        // had written is unnamed, and the next start removes it.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            compaction::run(store, &days, &opts, &mut |planned, result| {
                noted += 1;
                let wait = {
                    let mut jobs = jobs();
                    jobs.note(planned, result);
                    jobs.wait(planned, every).filter(|_| scheduled)
                };
                match result {
                    Ok(done) => log(&format!("compaction job {id} - {}", compaction::line(done))),
                    Err(e @ (StoreError::Cancelled { .. } | StoreError::InputsChanged { .. })) => {
                        log(&format!("compaction job {id} - {}", error::chain(e)))
                    }
                    Err(e) => log_warn(&format!(
                        "compaction job {id} - {}{}",
                        error::chain(e),
                        wait.map_or_else(String::new, |left| format!(
                            "; the day is left for {}s, unless its files change first",
                            left.as_secs().max(1)
                        ))
                    )),
                }
            })
        }));
        let state = match result {
            Ok(ran) => match ran.failed.first() {
                None => compaction::State::Done(ran.report),
                Some(first) => {
                    let mut why = error::chain(first);
                    if ran.failed.len() > 1 {
                        why.push_str(&format!(
                            "; and {} more day(s) failed, each in this server's log",
                            ran.failed.len() - 1
                        ));
                    }
                    compaction::State::Failed(why, ran.report)
                }
            },
            Err(_) => {
                let why = "the compaction failed on a bug in this build (a panic); a file it wrote \
                           and had not named is removed by the next start";
                log_warn(&format!("compaction job {id} - {why}"));
                // The day it was on, remembered as failed: a look every interval would
                // otherwise walk into the same panic every interval.
                if let Some(planned) = days.get(noted) {
                    let failed = Err(StoreError::CompactFailed {
                        day: crate::engine::store::disk::day_name(planned.0),
                        source: Box::new(StoreError::Io {
                            template_id: 0,
                            source: std::io::Error::other(why),
                        }),
                    });
                    jobs().note(*planned, &failed);
                }
                compaction::State::Failed(why.to_string(), compaction::Report::default())
            }
        };
        ending.finish(state, !days.is_empty());
    }
}

/// The parts a diagnostic command of the command line reads past the handle: see
/// `inspect`.
impl Database {
    pub(crate) fn parts(&self) -> (&Trie, &StoreManager) {
        (&self.trie, &self.stores)
    }
}

/// What reaches into a database for a test of a layer above (the server's): its parts, and the
/// compaction jobs as they stand. Not for anything but a test.
#[cfg(test)]
impl Database {
    pub fn trie(&self) -> &Trie {
        &self.trie
    }

    pub fn stores(&self) -> &Arc<StoreManager> {
        &self.stores
    }

    pub fn wal(&self) -> Option<&Wal> {
        self.wal.as_ref()
    }

    pub fn compaction_jobs(&self) -> &Mutex<compaction::Jobs> {
        &self.compactions
    }
}

// ── What the door hands back ───────────────────────────────────────────────────────

/// One malformed line: `{"line":400,"msg":"..."}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineError {
    pub line: usize,
    pub msg: String,
}

/// What one ingested body did.
#[derive(Debug)]
pub struct Ingested {
    /// The body's lines, good and bad.
    pub read: u64,
    pub ingested: u64,
    pub rejected: u64,
    /// In line order, at most `MAX_REPORTED_ERRORS`.
    pub errors: Vec<LineError>,
    /// Rejections past `MAX_REPORTED_ERRORS`, counted but not described.
    pub omitted: u64,
    /// Of the lines rejected, those that parsed and were refused because the catalog could
    /// not write their record.
    pub unwritten: u64,
    /// The buffers are past their memory limit: the flush should run now, not at its tick.
    pub over_buffer_limit: bool,
}

/// A body the database did not take, and the lines it read and refused on the way: a
/// body the log refused was read and routed, and counts as read and rejected.
#[derive(Debug)]
pub struct Refused {
    pub error: IngestError,
    pub read: u64,
    pub rejected: u64,
}

impl Refused {
    /// Refused before any line of it was counted.
    fn uncounted(error: IngestError) -> Self {
        Self {
            error,
            read: 0,
            rejected: 0,
        }
    }
}

/// Why a compaction job did not start.
#[derive(Debug)]
pub enum CompactRefused {
    /// Shutdown has begun.
    Stopping,
    /// One is running already: its number, where there is one.
    Running(Option<u64>),
    /// Its thread could not be started.
    Spawn(std::io::Error),
}

/// One template as `/templates` lists it.
#[derive(Debug, Clone)]
pub struct Template {
    pub template_id: u64,
    pub path: Vec<String>,
    pub columns: Vec<TemplateColumn>,
    pub rows: u64,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub storage_path: String,
    pub time_source: String,
}

/// One column of a template.
#[derive(Debug, Clone)]
pub struct TemplateColumn {
    pub name: String,
    /// `None`: the column has only ever held nulls, which is not `String`.
    pub col_type: Option<ColumnType>,
    pub widened_at: Option<DateTime<Utc>>,
}

/// The storage half of `/stats`, as one read of the database.
#[derive(Debug, Clone)]
pub struct Stats {
    pub kind: &'static str,
    pub base_path: Option<PathBuf>,
    /// The file templates are rebuilt from.
    pub catalog: Option<PathBuf>,
    /// The write-ahead log, and where it is; `None` without one.
    pub wal: Option<wal::Stats>,
    pub wal_dir: Option<PathBuf>,
    pub held_rows: u64,
    pub replayed: Replayed,
    pub stores_open: usize,
    pub open_segments: usize,
    pub max_open_segments: usize,
    pub rows: u64,
    pub buffered_rows: u64,
    pub buffered_bytes: u64,
    pub max_buffer_bytes: u64,
    /// The compaction jobs, as `/stats` prints them.
    pub compaction: serde_json::Value,
    pub forced_closes: u64,
    pub write_failures: u64,
    pub last_write_error: Option<String>,
    pub degraded: bool,
    /// The segments' bytes plus what the buffers would add, and the log's segments.
    pub approx_bytes: u64,
    pub templates: u64,
    pub nodes: u64,
    pub tokens: u64,
    /// Runs of a day's row groups chosen on more threads than the query's own.
    pub scan_fan_outs: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// A database in memory: the job registries and the close steps are the same, and
    /// nothing is opened.
    fn database() -> Database {
        Database::in_memory(Options::default())
    }

    /// The wait keeps a compaction's last writes inside the directory's lock. A thread told
    /// to stop is usually gone already, so only one that takes its time shows the wait.
    #[test]
    fn shutdown_waits_for_a_compaction_thread_still_running() {
        for by_request in [true, false] {
            let db = database();
            let ended = Arc::new(AtomicBool::new(false));
            let thread = {
                let ended = Arc::clone(&ended);
                std::thread::spawn(move || {
                    std::thread::sleep(Duration::from_millis(200));
                    ended.store(true, Ordering::SeqCst);
                })
            };
            {
                let mut jobs = db.compactions.lock().expect("the lock");
                match by_request {
                    true => jobs.asked = Some(thread),
                    false => jobs.scheduled = Some(thread),
                }
            }

            db.join_compactions();
            assert!(
                ended.load(Ordering::SeqCst),
                "shutdown went on past a compaction thread still running (asked: {by_request})"
            );
            assert!(db.compact_cancel.load(Ordering::Relaxed));
            let mut jobs = db.compactions.lock().expect("the lock");
            assert!(jobs.is_stopping() && jobs.start().is_none());
            assert!(jobs.asked.is_none() && jobs.scheduled.is_none());
        }
    }
}
