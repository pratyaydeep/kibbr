// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The database's life: a data directory opened - restored, and its log replayed - its
//! checkpoint moved behind the flush, and the order it is closed in.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use crate::fmt::commas;
use crate::database::{Database, Schema, Storage};
use crate::error::{self, Error};
use crate::templates::catalog::Catalog;
use crate::ingest;
use crate::rows::{FlushKind, StoreError, StoreManager};
use crate::templates::trie::Trie;
use crate::wal::{self, Wal};

/// The trie and the stores of a data directory, restored as a server's start restores
/// them but with the log left be; the caller holds the directory's `LOCK`.
#[cfg(test)]
pub(crate) fn open_durable(
    storage: &Storage,
    schema: &Schema,
) -> Result<(Trie, StoreManager), Error> {
    let restored = restore(storage, schema, false)?;
    Ok((restored.trie, restored.stores))
}

/// A data directory, restored: see `open_durable`.
pub(crate) struct Restored {
    pub(crate) trie: Trie,
    pub(crate) stores: StoreManager,
    /// The log the directory holds, read back and not yet started; `None` if it has none.
    pub(crate) wal: Option<Wal>,
    pub(crate) held: Held,
    pub(crate) replayed: Replayed,
}

/// `apply`: whether the lines the log holds are stored - a server's start - or only
/// counted.
pub(crate) fn restore(storage: &Storage, schema: &Schema, apply: bool) -> Result<Restored, Error> {
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
pub struct Replayed {
    /// Lines past the checkpoint: the ones looked at.
    pub records: u64,
    /// Put back in memory: no row group held them.
    pub applied: u64,
    /// Skipped: a durable row group's range covers their position.
    pub covered: u64,
    /// Left in the log: the store would not take them, or nothing says what they are.
    pub held: u64,
    /// Skipped: lines of a day a load has since replaced.
    pub replaced: u64,
    /// Lines at or behind the checkpoint, not looked at.
    pub behind: u64,
    /// Segments that ended in a record a stop cut short.
    pub torn: usize,
    pub corrupt: u64,
    pub seconds: f64,
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
            let day = crate::format::day_of(record.time);
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
        let day = crate::format::day_of(record.time);
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
        let row =
            match ingest::row_from_log(record.line, time_column, record.received_at, record.time) {
                Ok(row) => row,
                Err(e) => {
                    unreadable.get_or_insert_with(|| (record.seq, e.kind.to_string()));
                    held.set_aside(record.template_id, record.seq);
                    out.held += 1;
                    return Ok(());
                }
            };
        let time = crate::rows::effective_ts(&row, time_column).unwrap_or(0);
        if time != record.time {
            moved.get_or_insert((record.seq, record.time, time));
            held.set_aside(record.template_id, record.seq);
            out.held += 1;
            return Ok(());
        }
        let placed = ingest::Placed {
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
    crate::fmt::rfc3339(ns)
}

/// Rows the store would not take from the log: not an error, since the line is durable
///; the checkpoint stays behind it, and its template takes nothing more here.
pub(crate) struct Held {
    /// The lowest position held; `u64::MAX` while there is none.
    from: AtomicU64,
    rows: AtomicU64,
    templates: std::sync::Mutex<BTreeSet<u64>>,
}

impl Held {
    pub(crate) fn new() -> Self {
        Self {
            from: AtomicU64::new(u64::MAX),
            rows: AtomicU64::new(0),
            templates: std::sync::Mutex::new(BTreeSet::new()),
        }
    }

    /// Hand one row to the store at its position in the log; 0 is a row with no log
    /// behind it. Whether the store took it.
    pub(crate) fn apply(&self, stores: &StoreManager, position: u64, placed: ingest::Placed) -> bool {
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
    pub(crate) fn defer(&self, template_id: u64, position: u64, why: &str) {
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
    pub(crate) fn set_aside(&self, template_id: u64, position: u64) {
        self.templates
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(template_id);
        self.hold(position);
    }

    /// Keep the checkpoint behind `position`; 0 is a row with no log behind it, which
    /// nothing can keep.
    pub(crate) fn hold(&self, position: u64) {
        self.rows.fetch_add(1, Ordering::Relaxed);
        if position > 0 {
            self.from.fetch_min(position, Ordering::Relaxed);
        }
    }

    pub(crate) fn from(&self) -> Option<u64> {
        match self.from.load(Ordering::Relaxed) {
            u64::MAX => None,
            position => Some(position),
        }
    }

    pub(crate) fn rows(&self) -> u64 {
        self.rows.load(Ordering::Relaxed)
    }
}

/// A store that will not open, as the error startup exits on: an earlier layout or
/// another format version is named, not reported as a disk that failed.
pub(crate) fn storage_fatal(e: StoreError) -> Error {
    match e {
        StoreError::LegacyLayout { path } => Error::LegacyLayout { path },
        StoreError::Format(crate::format::FormatError::Version { path, found }) => {
            Error::SegmentVersion { path, found }
        }
        StoreError::Format(crate::format::FormatError::PreRename { path }) => {
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
    let path = crate::templates::catalog::path_in(base);
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
                    crate::templates::snapshot::load_reporting(snapshot, data_dir);
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

/// After a flush: ask a log that cannot write to try again, and move its checkpoint up
/// behind the oldest row still only in memory, every tick, or the log would grow.
pub(crate) fn move_checkpoint(db: &Database) -> Option<wal::Advanced> {
    let wal = db.wal.as_ref()?;
    wal.probe();
    let (moved, problem) =
        wal.advance(
            || match (db.stores.oldest_position(), db.held.from()) {
                (Some(buffered), Some(held)) => Some(buffered.min(held)),
                (buffered, held) => buffered.or(held),
            },
        );
    // Told whatever else happened: the checkpoint is where it is whether or not its file
    // could be written, and the scheduler's compaction waits on it.
    if let (Some(moved), Some(disk)) = (moved, db.stores.disk_store()) {
        disk.note_checkpoint(moved.checkpoint);
    }
    match problem {
        None => db.checkpoint_failing.store(false, Ordering::Relaxed),
        // Said once, not every tick: nothing is lost by it - a start reads a little more
        // of the log, or finds a segment this could not delete and deletes it.
        Some(e) => {
            if !db.checkpoint_failing.swap(true, Ordering::Relaxed) {
                log_warn(&format!(
                    "wal - the checkpoint moved and could not be kept: {e}"
                ));
            }
        }
    }
    moved
}

/// The fixed shutdown order, the steps a memory server lacks printed as skipped
/// so an operator sees which durability they have. No row depends on it finishing.
pub(crate) fn close(db: &Database) -> Result<(), Error> {
    if db.storage.is_durable() {
        let summary = db.stores.flush_all(FlushKind::Final);
        // Rows still in a buffer once the final flush has done all it can. Not lost: they
        // are in the log, and the next start puts them back.
        let unwritten = match summary.failed {
            0 => None,
            leaves => Some((leaves, db.stores.buffered())),
        };
        // Bounded by the open days and not by the templates: one flush and one seal per day
        // file, however many templates' row groups went into it.
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
    db.join_compactions();

    // Behind the final flush, so that a clean stop leaves the checkpoint at the end of the
    // log and the next start with nothing to read back.
    match &db.wal {
        Some(wal) => {
            let moved = move_checkpoint(db);
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

    match db.storage.trie_file.as_deref() {
        Some(path) => match crate::templates::snapshot::save(&db.trie, path) {
            Ok(bytes) => log(&format!(
                "trie.json - {} {} byte(s), {} template(s)",
                path.display(),
                commas(bytes),
                commas(db.trie.template_count())
            )),
            // Not a reason to fail the exit. Nothing loads the snapshot while the catalog
            // exists: it is there for a human to read.
            Err(e) => log_warn(&format!("trie.json - {}", error::chain(&e))),
        },
        None => log("trie.json - skipped, storage is memory"),
    }

    let stores = &db.stores;
    // Rows still buffered never reached a file, so they do not belong in a count of what
    // was written.
    let held = stores.rows();
    let unwritten = stores.buffered();
    let kept = if db.storage.is_durable() {
        "written"
    } else {
        "discarded, they were only in memory"
    };
    log(&format!(
        "close stores - {} store(s), {} row(s) {kept}{}{}",
        commas(stores.open() as u64),
        commas(held.saturating_sub(unwritten)),
        match (db.storage.is_durable(), unwritten) {
            (true, 0) => String::new(),
            (true, n) => format!(
                ", {} row(s) still buffered: in the write-ahead log, put back at the next start",
                commas(n)
            ),
            (false, _) => String::new(),
        },
        match db.held.rows() {
            0 => String::new(),
            n => format!(
                ", {} row(s) the store refused: in the write-ahead log, stored at the next start",
                commas(n)
            ),
        },
    ));

    match &db.wal {
        Some(wal) => {
            wal.close();
            log("close WAL - closed");
        }
        None => log("close WAL - skipped, storage is memory"),
    }

    Ok(())
}

/// One lifecycle line, an `info` event and never `eprintln!`, which panics when stderr
/// is gone and would end the flush, load or compaction that called it.
pub(crate) fn log(message: &str) {
    tracing::info!("{message}");
}

/// A line an operator should read: something was skipped, delayed or left as it was.
pub(crate) fn log_warn(message: &str) {
    tracing::warn!("{message}");
}

/// A line that says the server cannot do what it is for.
pub(crate) fn log_error(message: &str) {
    tracing::error!("{message}");
}

#[cfg(test)]
mod tests;
