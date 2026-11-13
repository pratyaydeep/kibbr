// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! Compaction: a day the live door has finished with, rebuilt into full row groups in
//! finished files and swapped in with one `replace` record.
//!
//! Three steps - `begin`, `rebuild`, `commit` - and the flush's lock is held for the first
//! and the last only; a stop at any moment leaves every row once.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, MutexGuard, OnceLock};
use std::time::Instant;

use serde::{Deserialize, Serialize};

use super::{
    day_name, day_of_name, file_name, index_rows, spliced, warn, DataFile, DiskStore,
    EncodedRowGroup, Files, Stored, DATA_DIR, STAGING_DIR,
};
use crate::engine::store::manifest::{self, FileRecord, Record};
use crate::engine::format::build::{self, schema_of, Level, RowGroup};
use crate::engine::format::file::{Index, Reader, Writer};
use crate::engine::format::{day_of, FormatError, Source};
use crate::engine::store::{
    effective_ts, lock, value_bytes, FlushKind, Row, RowGroupLimits, StoreError, RECEIVED_AT_COLUMN,
};
use crate::engine::model::types::ColValue;

/// Input files kept open for reading at once: a descriptor per input would take a day of
/// many files past the limit the live door's files and the sockets share.
const MAX_READERS: usize = 64;

/// How a caller runs a compaction.
pub struct CompactOptions {
    /// Threads row groups are encoded on, held to `1..=cores`.
    pub threads: usize,
    /// Set to stop it, looked at between the row groups it reads and before every file it
    /// seals; a cancelled compaction removes what it wrote and changes nothing.
    pub cancel: Arc<AtomicBool>,
}

/// What compacting one day did; a day with nothing to compact is `compacted: false`, every
/// count zero.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Compacted {
    pub day: String,
    pub compacted: bool,
    pub rows: u64,
    pub files_in: usize,
    pub files_out: usize,
    /// Row groups in the files read, and in the files written.
    pub row_groups_in: u64,
    pub row_groups_out: u64,
    /// File sizes on disk.
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub seconds: f64,
    /// By path under `data/`.
    pub added: Vec<String>,
    pub removed: Vec<String>,
}

/// A day that is due, and its inputs hashed to one number, so a caller that remembers a
/// failed day can tell whether what failed is still there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Due {
    /// Days since the epoch.
    pub day: i64,
    pub inputs: u64,
}

/// One file a compaction reads and retires.
struct Input {
    rel: String,
    path: PathBuf,
    /// As the manifest spells it.
    file_id: String,
    bytes: u64,
    rows: u64,
}

/// One template's row groups in the inputs, in storage order.
struct Template {
    /// The leaf's time column, which a rebuilt row group's time range is read from; `None`
    /// for a template no leaf is open for, whose row groups are copied as they are.
    time_column: Option<String>,
    stored: Vec<Stored>,
}

/// A compaction between `begin` and `commit`. It holds the inputs' row groups and so their
/// files: a replace-a-day meanwhile cannot delete a file this still reads.
struct Job {
    day: i64,
    began: Instant,
    /// In path order.
    inputs: Vec<Input>,
    /// The inputs as a rebuilt file's footer names them.
    parents: Vec<(String, [u8; 16])>,
    /// Each input a row group names, once.
    files: Vec<Arc<DataFile>>,
    templates: BTreeMap<u64, Template>,
    row_groups: u64,
    /// The first file number reserved for the outputs, and how many.
    first: u32,
    reserved: u32,
}

/// A rebuilt file: sealed, flushed, and not yet named.
struct Written {
    rel: String,
    path: PathBuf,
    index: Index,
    bytes: u64,
}

/// What a rebuild wrote, in path order. Empty when the inputs held no row group.
#[derive(Default)]
struct Outputs {
    files: Vec<Written>,
}

impl DiskStore {
    /// Compact `day`, as days since the epoch, whatever its age, with one manifest record;
    /// `Cancelled` and `InputsChanged` are not failures.
    pub fn compact_day(&self, day: i64, opts: &CompactOptions) -> Result<Compacted, StoreError> {
        let Some(job) = self.begin(day)? else {
            return Ok(Compacted {
                day: day_name(day),
                ..Compacted::default()
            });
        };
        match self.rebuild(&job, opts) {
            Ok(outputs) => self.commit(job, outputs),
            Err(e) => {
                self.abandon(job);
                Err(e)
            }
        }
    }

    /// The days due at `today`, as days since the epoch: before `today`, no live row group
    /// written since, and all of it behind the write-ahead log's checkpoint.
    pub fn due_days(&self, today: i64) -> Vec<Due> {
        let files = lock(&self.files);
        if !files.uncertain.is_empty() || !files.unmoved.is_empty() {
            return Vec::new();
        }
        let days: BTreeSet<i64> = files
            .live
            .keys()
            .filter_map(|rel| day_of_name(rel.split_once('/')?.0))
            .collect();
        let ahead = self.ahead_of_checkpoint(&files);
        days.into_iter()
            .filter(|day| {
                *day < today
                    && !files.mixed.contains_key(day)
                    && files.last_live.get(day).is_none_or(|last| *last < today)
                    && stuck(&files, *day).is_none()
                    && !ahead.contains(day)
            })
            .filter_map(|day| {
                let run = trailing(&files.live, day);
                (!run.is_empty()).then(|| Due {
                    day,
                    inputs: fingerprint(&run),
                })
            })
            .collect()
    }

    /// The days the log has delivered rows of that are not all in a row group and behind its
    /// checkpoint. What the scheduler waits for; a day asked for by name is not held to it.
    fn ahead_of_checkpoint(&self, files: &Files) -> BTreeSet<i64> {
        if self.stream.load(Ordering::Relaxed) == 0 {
            return BTreeSet::new();
        }
        let checkpoint = self.wal_checkpoint.load(Ordering::Relaxed);
        let mut ahead: BTreeSet<i64> = files
            .wal_through
            .iter()
            .filter(|(_, through)| **through > checkpoint)
            .map(|(day, _)| *day)
            .collect();
        let leaves = self.leaves.read().unwrap_or_else(|e| e.into_inner());
        for leaf in leaves.values() {
            ahead.extend(leaf.days_in_memory());
        }
        ahead
    }

    /// The days that hold a file compaction would take, due or not: what a failure is
    /// remembered against, where `due_days` is what is tried.
    pub fn compactable_days(&self) -> BTreeSet<i64> {
        let files = lock(&self.files);
        let days: BTreeSet<i64> = files
            .live
            .keys()
            .filter_map(|rel| day_of_name(rel.split_once('/')?.0))
            .collect();
        days.into_iter()
            .filter(|day| !trailing(&files.live, *day).is_empty())
            .collect()
    }

    /// The first step, in one hold of the flush's lock; `None` when the day has nothing
    /// to compact. The row groups come from the leaves: what queries were answered from.
    fn begin(&self, day: i64) -> Result<Option<Job>, StoreError> {
        let began = Instant::now();
        let mut files = lock(&self.files);
        if let Some(busy) = files.compacting {
            return Err(StoreError::CompactBusy {
                day: day_name(day),
                busy: day_name(busy),
            });
        }
        if let Some(pending) = files.unmoved.iter().next() {
            return Err(refused(
                day,
                format!("{STAGING_DIR}/{pending:016x}"),
                "a committed load's files are still here, so this process's view of data/ is \
                 short of them; restart to settle the load, then compact",
            ));
        }
        if let Some(path) = files.uncertain.first() {
            return Err(refused(
                day,
                path.display().to_string(),
                "an earlier compaction wrote it, and the record naming it may be on disk; \
                 restart to settle that, then compact",
            ));
        }
        if let Some(rel) = files.mixed.get(&day) {
            return Err(refused(
                day,
                rel.clone(),
                "it holds row groups of more than one day - written before every row went \
                 into its own day's file - so the day's files are not the day's rows; load \
                 the day again with --replace-day",
            ));
        }
        // Before the flush as well as after: a refusal then has written nothing at all.
        if let Some(rel) = stuck(&files, day) {
            return Err(unsealed(day, rel));
        }
        let flushed = self.flush_locked(&mut files, FlushKind::Final, Some(day));
        let writing = flushed.failed.into_iter().map(|(_, e)| e).next();
        let sealing = flushed.seal_failed.into_iter().next();
        // A file left unsealed is the seal's doing when a seal failed, so that is the
        // cause named beside it; otherwise the first row group that could not be written.
        let left = stuck(&files, day);
        let failure = if left.is_some() {
            sealing.or(writing)
        } else {
            writing.or(sealing)
        };
        // Not `failed`: that says the day's files are as they were, and this flush may
        // have written row groups, named a file, or left one unsealed for the next start.
        if let Some(e) = failure {
            return Err(match left {
                Some(rel) => refused(
                    day,
                    rel,
                    &format!(
                        "the flush before the compaction failed ({e}) and left it unsealed; \
                         the next start recovers and seals it - restart, then compact"
                    ),
                ),
                None => StoreError::CompactFlushFailed {
                    day: day_name(day),
                    source: Box::new(e),
                },
            });
        }
        // Nothing of the day is open now, so a file still unsealed is one a seal left.
        if let Some(rel) = stuck(&files, day) {
            return Err(unsealed(day, rel));
        }

        let mut inputs = Vec::new();
        let mut parents = Vec::new();
        for record in trailing(&files.live, day) {
            let path = files.data.join(&record.path);
            let bytes = fs::metadata(&path)
                .map_err(|source| {
                    failed(
                        day,
                        StoreError::from(FormatError::Io {
                            path: path.clone(),
                            source,
                        }),
                    )
                })?
                .len();
            let Some(id) = manifest::file_id_of(&record.file_id) else {
                return Err(refused(
                    day,
                    record.path.clone(),
                    "manifest.log's file id for it is not 32 hex digits",
                ));
            };
            parents.push((record.path.clone(), id));
            inputs.push(Input {
                rel: record.path.clone(),
                path,
                file_id: record.file_id.clone(),
                bytes,
                rows: record.rows.unwrap_or(0),
            });
        }
        if inputs.is_empty() {
            return Ok(None);
        }

        let bytes_in: u64 = inputs.iter().map(|input| input.bytes).sum();
        let needed = bytes_in
            .saturating_add(self.segment_max_bytes)
            .saturating_add(self.max_buffer_bytes);
        if let Some(free) = free_bytes(&files.data) {
            if free < needed {
                return Err(StoreError::NoSpace {
                    day: day_name(day),
                    path: files.data.clone(),
                    free,
                    needed,
                });
            }
        }

        // The inputs' row groups, as the leaves hold them now: with the flush's lock held
        // nothing is mid-publish, so each is in a leaf's list or waiting in `stored`.
        let mut templates: BTreeMap<u64, Template> = BTreeMap::new();
        let mut found = vec![0u64; inputs.len()];
        let mut held: Vec<Arc<DataFile>> = Vec::new();
        let mut row_groups = 0u64;
        {
            let at: HashMap<&Path, usize> = inputs
                .iter()
                .enumerate()
                .map(|(at, input)| (input.path.as_path(), at))
                .collect();
            let of_inputs = |list: &[Stored]| -> Vec<Stored> {
                list.iter()
                    .filter(|s| at.contains_key(s.file.path.as_path()))
                    .cloned()
                    .collect()
            };
            {
                let leaves = self.leaves.read().unwrap_or_else(|e| e.into_inner());
                for leaf in leaves.values() {
                    let stored = match leaf.state().days.get(&day) {
                        Some(part) => of_inputs(&part.stored),
                        None => continue,
                    };
                    if !stored.is_empty() {
                        templates.insert(
                            leaf.template_id,
                            Template {
                                time_column: Some(leaf.time_column.clone()),
                                stored,
                            },
                        );
                    }
                }
                // Still under the leaves' lock, which a leaf opens under: a template is in
                // one place or the other, never between.
                for (id, list) in lock(&self.stored).iter() {
                    let stored = of_inputs(list);
                    if !stored.is_empty() {
                        templates.insert(
                            *id,
                            Template {
                                time_column: None,
                                stored,
                            },
                        );
                    }
                }
            }
            let mut seen: BTreeSet<&Path> = BTreeSet::new();
            for s in templates.values().flat_map(|t| &t.stored) {
                if let Some(input) = at.get(s.file.path.as_path()) {
                    found[*input] += u64::from(s.entry.row_count);
                }
                row_groups += 1;
                if seen.insert(s.file.path.as_path()) {
                    held.push(Arc::clone(&s.file));
                }
            }
        }
        for (input, found) in inputs.iter_mut().zip(found) {
            let named = files.live.get(&input.rel).and_then(|record| record.rows);
            if named != Some(found) {
                return Err(refused(
                    day,
                    input.rel.clone(),
                    &format!(
                        "manifest.log gives it {} row(s) and this process holds row groups \
                         of {found} for it, so rebuilding the day from them would not keep \
                         every row",
                        named.map_or_else(|| "an unknown number of".to_string(), |n| n.to_string()),
                    ),
                ));
            }
            input.rows = found;
        }

        // One file more than the inputs' bytes fill, since a file is sealed only once it is past
        // the cap, and never more files than row groups. A number not used stays a gap.
        let wanted = bytes_in
            .div_ceil(self.segment_max_bytes.max(1))
            .saturating_add(1)
            .min(row_groups);
        let reserved = u32::try_from(wanted).unwrap_or(u32::MAX);
        let first = files.next.get(&day).copied().unwrap_or(1);
        if reserved > 0 {
            files.next.insert(day, first.saturating_add(reserved));
        }
        files.compacting = Some(day);
        Ok(Some(Job {
            day,
            began,
            inputs,
            parents,
            files: held,
            templates,
            row_groups,
            first,
            reserved,
        }))
    }

    /// The second step, holding no lock of the store's. Whatever fails, nothing is published
    /// and the caller abandons the job.
    fn rebuild(&self, job: &Job, opts: &CompactOptions) -> Result<Outputs, StoreError> {
        // A panic here would otherwise leave the day marked as being compacted for the
        // life of the process, and its half-written files with it.
        catch_unwind(AssertUnwindSafe(|| self.rebuild_all(job, opts)))
            .unwrap_or_else(|_| {
                Err(StoreError::Io {
                    template_id: 0,
                    source: std::io::Error::other("the rebuild panicked"),
                })
            })
            .map_err(|e| failed(job.day, e))
    }

    fn rebuild_all(&self, job: &Job, opts: &CompactOptions) -> Result<Outputs, StoreError> {
        let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
        let mut out = Sink {
            job,
            dir: self.base.join(DATA_DIR).join(day_name(job.day)),
            max_bytes: self.segment_max_bytes,
            cancel: &opts.cancel,
            threads: opts.threads.clamp(1, cores),
            pending: Vec::new(),
            writer: None,
            done: Vec::new(),
        };
        let mut readers = Readers::default();
        for (id, template) in &job.templates {
            // A replace-a-day took the day: what is being rebuilt is no longer its rows, and there
            // is nothing to gain by reading on.
            if let Some(file) = job.files.iter().find(|f| f.is_retired()) {
                return Err(StoreError::InputsChanged {
                    day: day_name(job.day),
                    path: file.path.display().to_string(),
                });
            }
            match &template.time_column {
                Some(column) => merge(
                    job,
                    *id,
                    column,
                    &template.stored,
                    &self.limits,
                    &mut readers,
                    &mut out,
                )?,
                None => {
                    out.drain()?;
                    for s in &template.stored {
                        out.cancelled()?;
                        let schema_id = out.schema_id(&s.schema)?;
                        let copied =
                            readers
                                .get(&s.file.path)?
                                .copy(&s.entry, s.schema.len(), schema_id)?;
                        out.put(*id, &copied)?;
                    }
                }
            }
        }
        let files = out.finish()?;

        // Counted from the sealed files' own indexes, against the row groups the job set
        // out with: a template that came out a row short or a row over is not published.
        let mut written: BTreeMap<u64, u64> = BTreeMap::new();
        for e in files.iter().flat_map(|f| &f.index.entries) {
            *written.entry(e.template_id).or_default() += u64::from(e.row_count);
        }
        for (id, template) in &job.templates {
            let read: u64 = template
                .stored
                .iter()
                .map(|s| u64::from(s.entry.row_count))
                .sum();
            let wrote = written.remove(id).unwrap_or(0);
            if read != wrote {
                return Err(StoreError::Io {
                    template_id: *id,
                    source: std::io::Error::other(format!(
                        "the day's files hold {read} row(s) of it and the rebuilt ones {wrote}"
                    )),
                });
            }
        }
        if let Some((id, rows)) = written.into_iter().next() {
            return Err(StoreError::Io {
                template_id: id,
                source: std::io::Error::other(format!(
                    "the rebuilt files hold {rows} row(s) of it and the day's files none"
                )),
            });
        }
        Ok(Outputs { files })
    }

    /// The third step. Record and publish share one hold of the lock; the inputs are
    /// unlinked after it is released, so a day of files does not hold every tick behind it.
    fn commit(&self, job: Job, outputs: Outputs) -> Result<Compacted, StoreError> {
        let mut files = Unlatch(lock(&self.files));
        match self.commit_locked(&mut files.0, &job, &outputs) {
            Ok(()) => {}
            Err(e @ StoreError::CompactUncertain { .. }) => {
                files
                    .0
                    .uncertain
                    .extend(outputs.files.iter().map(|f| f.path.clone()));
                files.0.compacting = None;
                return Err(e);
            }
            Err(e) => {
                drop(files);
                self.abandon(job);
                return Err(e);
            }
        }
        files.0.compacting = None;
        drop(files);

        let report = Compacted {
            day: day_name(job.day),
            compacted: true,
            rows: job.inputs.iter().map(|input| input.rows).sum(),
            files_in: job.inputs.len(),
            files_out: outputs.files.len(),
            row_groups_in: job.row_groups,
            row_groups_out: outputs
                .files
                .iter()
                .map(|f| f.index.entries.len() as u64)
                .sum(),
            bytes_in: job.inputs.iter().map(|input| input.bytes).sum(),
            bytes_out: outputs.files.iter().map(|f| f.bytes).sum(),
            seconds: job.began.elapsed().as_secs_f64(),
            added: outputs.files.iter().map(|f| f.rel.clone()).collect(),
            removed: job.inputs.iter().map(|input| input.rel.clone()).collect(),
        };
        let named: BTreeSet<&Path> = job.files.iter().map(|f| f.path.as_path()).collect();
        for file in &job.files {
            file.retire();
        }
        // A file no row group named - one that held no rows - has no reader to wait for.
        for input in &job.inputs {
            if !named.contains(input.path.as_path()) {
                let _ = fs::remove_file(&input.path);
            }
        }
        Ok(report)
    }

    fn commit_locked(
        &self,
        files: &mut Files,
        job: &Job,
        outputs: &Outputs,
    ) -> Result<(), StoreError> {
        // Exactly the files that were read, and only if each is still what it was: a
        // replace-a-day since has retired them, and the day is the load's.
        for input in &job.inputs {
            let same = files.live.get(&input.rel).is_some_and(|record| {
                record.file_id == input.file_id && record.sealed && !record.finished
            });
            if !same {
                return Err(StoreError::InputsChanged {
                    day: day_name(job.day),
                    path: input.rel.clone(),
                });
            }
        }
        match files.record(vec![replace_record(job, outputs)]) {
            Ok(()) => {}
            // Refused before a byte was written: nothing is committed.
            Err(e @ manifest::ManifestError::Serialize { .. })
            | Err(
                e @ manifest::ManifestError::Log(crate::engine::format::frame::LogError::Torn {
                    ..
                }),
            ) => return Err(failed(job.day, e.into())),
            Err(source) => {
                // A write or flush failed partway. Unless the log's cut of the record is itself
                // flushed, the record may be on disk whole, and then it is the commit.
                if !files.manifest.is_torn() && files.manifest.sync().is_ok() {
                    return Err(failed(job.day, source.into()));
                }
                return Err(StoreError::CompactUncertain {
                    day: day_name(job.day),
                    source,
                });
            }
        }

        // Committed. One step across every template, as a load's publish is: see
        // `publish_load`.
        let gone: BTreeSet<PathBuf> = job.inputs.iter().map(|input| input.path.clone()).collect();
        let mut new: BTreeMap<u64, Vec<Stored>> = BTreeMap::new();
        for f in &outputs.files {
            index_rows(&mut new, Arc::new(DataFile::new(f.path.clone())), &f.index);
        }
        let _publishing = self.publish.write().unwrap_or_else(|e| e.into_inner());
        let leaves = self.leaves.write().unwrap_or_else(|e| e.into_inner());
        self.generation.fetch_add(1, Ordering::Relaxed);
        for leaf in leaves.values() {
            // A leaf opened since the job began has taken its template's row groups out
            // of `stored`, the inputs' among them: the outputs go where those are.
            let rows = new.remove(&leaf.template_id).unwrap_or_default();
            leaf.replace(job.day, &gone, rows);
        }
        let mut stored = lock(&self.stored);
        for (id, list) in stored.iter_mut() {
            let rows = new.remove(id).unwrap_or_default();
            if !rows.is_empty() || list.iter().any(|s| gone.contains(&s.file.path)) {
                *list = spliced(list, &gone, rows);
            }
        }
        for (id, rows) in new {
            stored.entry(id).or_default().extend(rows);
        }
        Ok(())
    }

    /// Give up a job that will not commit: delete what it wrote under its reserved names, and
    /// hand the numbers back only if every name is gone - a file left would block the day's next.
    fn abandon(&self, job: Job) {
        let dir = self.base.join(DATA_DIR).join(day_name(job.day));
        let end = job.first.saturating_add(job.reserved);
        // By name, not from a list: whatever stopped the rebuild, these are its files.
        let mut left = Vec::new();
        for number in job.first..end {
            let path = dir.join(file_name(number));
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => left.push((path, e)),
            }
        }
        {
            let mut files = lock(&self.files);
            if left.is_empty() && job.reserved > 0 && files.next.get(&job.day) == Some(&end) {
                files.next.insert(job.day, job.first);
            }
            files.compacting = None;
        }
        // Said with the latch off and the lock released: nothing waits on a log line.
        for (path, e) in left {
            warn(format!(
                "{}: written by a compaction that was given up, and could not be deleted \
                 ({e}); no manifest record names it, its number is left unused, and the next \
                 start deletes it",
                path.display()
            ));
        }
    }
}

/// The flush's lock as a commit holds it: let go by a panic, it takes the day's latch off.
/// A guard, not `catch_unwind`: a panic here may fall either side of the record.
struct Unlatch<'a>(MutexGuard<'a, Files>);

impl Drop for Unlatch<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.compacting = None;
        }
    }
}

/// The one record a compaction commits with: the outputs in, the inputs out.
fn replace_record(job: &Job, outputs: &Outputs) -> Record {
    Record::Replace {
        add: outputs
            .files
            .iter()
            .map(|f| FileRecord::sealed(f.rel.clone(), &f.index))
            .collect(),
        remove: job.inputs.iter().map(|input| input.rel.clone()).collect(),
        source: Some(manifest::COMPACT_SOURCE.to_string()),
        // The rebuilt row groups carry their inputs' ranges: nothing for a replay to
        // be told.
        wal: None,
    }
}

fn refused(day: i64, path: String, why: &str) -> StoreError {
    StoreError::CompactRefused {
        day: day_name(day),
        path,
        why: why.to_string(),
    }
}

fn unsealed(day: i64, rel: String) -> StoreError {
    refused(
        day,
        rel,
        "a seal that failed left it unsealed, with files of the day either side of it; the \
         next start recovers and seals it - restart, then compact",
    )
}

/// An error from inside a compaction, with the day and what became of it - unless it says
/// so already.
fn failed(day: i64, e: StoreError) -> StoreError {
    match e {
        StoreError::Cancelled { .. }
        | StoreError::InputsChanged { .. }
        | StoreError::CompactFailed { .. } => e,
        other => StoreError::CompactFailed {
            day: day_name(day),
            source: Box::new(other),
        },
    }
}

/// The files compaction takes from `day`: its live files after the last finished one, in
/// path order, the unsealed file being written included until `begin` seals it.
fn trailing(live: &BTreeMap<String, FileRecord>, day: i64) -> Vec<&FileRecord> {
    let prefix = format!("{}/", day_name(day));
    let mut run = Vec::new();
    for (rel, record) in live.range(prefix.clone()..) {
        if !rel.starts_with(&prefix) {
            break;
        }
        if record.finished {
            run.clear();
        } else {
            run.push(record);
        }
    }
    run
}

/// A live file of `day` that is unsealed and is not the one open for writing: left by a
/// seal that failed, and sealed only by the next start's recovery.
fn stuck(files: &Files, day: i64) -> Option<String> {
    let prefix = format!("{}/", day_name(day));
    let open = files.open.get(&day).map(|open| open.rel.as_str());
    files
        .live
        .range(prefix.clone()..)
        .take_while(|(rel, _)| rel.starts_with(&prefix))
        .find(|(rel, record)| !record.sealed && Some(rel.as_str()) != open)
        .map(|(rel, _)| rel.clone())
}

/// A set of files as one number: FNV-1a over each path and file id. Its own few lines
/// rather than std's hasher, whose algorithm is not promised from one release to the next.
fn fingerprint(files: &[&FileRecord]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    let mut eat = |bytes: &[u8]| {
        for b in bytes.iter().chain(&[0xff]) {
            hash = (hash ^ u64::from(*b)).wrapping_mul(0x0000_0100_0000_01b3);
        }
    };
    for f in files {
        eat(f.path.as_bytes());
        eat(f.file_id.as_bytes());
    }
    hash
}

/// Bytes an unprivileged writer can still take from the volume holding `dir`, or `None`
/// where that cannot be asked - and then nothing is refused on it.
#[cfg(unix)]
// The fields are `u32` on some platforms and `u64` on others; the cast is to the wider.
#[allow(clippy::unnecessary_cast)]
fn free_bytes(dir: &Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(dir.as_os_str().as_bytes()).ok()?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `path` is NUL-terminated and outlives the call, and `stat` is room for one
    // `statvfs`, read only when the call returns 0 and has filled it.
    let stat = unsafe {
        if libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) != 0 {
            return None;
        }
        stat.assume_init()
    };
    Some((stat.f_bavail as u64).saturating_mul(stat.f_frsize as u64))
}

#[cfg(not(unix))]
fn free_bytes(_dir: &Path) -> Option<u64> {
    None
}

// ── Reading the inputs ─────────────────────────────────────────────────────────────

/// The input files open for reading, least recently read first: at most `MAX_READERS`.
#[derive(Default)]
struct Readers {
    open: Vec<(PathBuf, Reader)>,
}

impl Readers {
    fn get(&mut self, path: &Path) -> Result<&mut Reader, FormatError> {
        let at = match self.open.iter().position(|(p, _)| p == path) {
            Some(at) => at,
            None => {
                if self.open.len() >= MAX_READERS {
                    self.open.remove(0);
                }
                self.open.push((path.to_path_buf(), Reader::open(path)?));
                self.open.len() - 1
            }
        };
        self.open[at..].rotate_left(1);
        let last = self.open.len() - 1;
        Ok(&mut self.open[last].1)
    }
}

/// Rows of one template on their way to being one row group.
struct Batch {
    template_id: u64,
    rows: Vec<Row>,
    times: Vec<i64>,
    /// Payload, as the live door counts it against the same threshold.
    bytes: usize,
    /// Where the rows came from: the smallest range that holds every row group merged in.
    source: Source,
}

impl Batch {
    fn is_full(&self, limits: &RowGroupLimits) -> bool {
        self.rows.len() >= limits.rows || self.bytes >= limits.bytes
    }
}

/// What row groups must share to be merged: the kind of source, and the stream.
fn stream_of(source: Source) -> (u8, u64) {
    let (kind, stream, _, _) = source.bytes();
    (kind, stream)
}

/// The smallest range of one stream that holds both.
fn widened(a: Source, b: Source) -> Source {
    match (a, b) {
        (
            Source::Wal {
                stream,
                first,
                last,
            },
            Source::Wal {
                first: f, last: l, ..
            },
        ) => Source::Wal {
            stream,
            first: first.min(f),
            last: last.max(l),
        },
        (
            Source::Load {
                stream,
                first,
                last,
            },
            Source::Load {
                first: f, last: l, ..
            },
        ) => Source::Load {
            stream,
            first: first.min(f),
            last: last.max(l),
        },
        _ => a,
    }
}

/// One template's row groups, decoded in storage order and cut into batches as a load cuts
/// them; never sorted, and only row groups of one stream are merged.
fn merge(
    job: &Job,
    template_id: u64,
    time_column: &str,
    stored: &[Stored],
    limits: &RowGroupLimits,
    readers: &mut Readers,
    out: &mut Sink<'_>,
) -> Result<(), StoreError> {
    let mut batch: Option<Batch> = None;
    for s in stored {
        out.cancelled()?;
        let reader = readers.get(&s.file.path)?;
        let header = reader.header(&s.entry, s.schema.len())?;
        let every: Vec<usize> = (0..s.schema.len()).collect();
        let (columns, _) = reader.chunks(&s.entry, &header, &every)?;
        let mut columns: Vec<_> = columns.into_iter().map(Vec::into_iter).collect();
        if batch
            .as_ref()
            .is_some_and(|b| stream_of(b.source) != stream_of(header.source))
        {
            if let Some(b) = batch.take() {
                out.close(b)?;
            }
        }
        for _ in 0..header.row_count {
            let mut row = Row::with_capacity(s.schema.len());
            let mut bytes = 0;
            for (name, cells) in s.schema.iter().zip(columns.iter_mut()) {
                let Some((value, written)) = cells.next() else {
                    return Err(FormatError::Corrupt {
                        path: s.file.path.clone(),
                        offset: s.entry.offset,
                        what: format!("column `{name}` is short of the row group's rows"),
                    }
                    .into());
                };
                // A row that carried its own time reads a null arrival time in a row group it
                // shared with one that fell back. Left out, it is the row the live door buffered.
                if name == RECEIVED_AT_COLUMN && matches!(value, ColValue::Null) {
                    continue;
                }
                bytes += name.len() + value_bytes(&value);
                row.insert_written(name.clone(), value, written);
            }
            let time = effective_ts(&row, time_column).unwrap_or(0);
            if day_of(time) != job.day {
                return Err(StoreError::Io {
                    template_id,
                    source: std::io::Error::other(format!(
                        "{} holds a row of {}, whose time is read differently now than \
                         when it was written",
                        s.file.path.display(),
                        day_name(day_of(time))
                    )),
                });
            }
            let b = batch.get_or_insert_with(|| Batch {
                template_id,
                rows: Vec::new(),
                times: Vec::new(),
                bytes: 0,
                source: header.source,
            });
            b.source = widened(b.source, header.source);
            b.rows.push(row);
            b.times.push(time);
            b.bytes += bytes;
            if b.is_full(limits) {
                if let Some(b) = batch.take() {
                    out.close(b)?;
                }
            }
        }
    }
    match batch {
        Some(b) => out.close(b),
        None => Ok(()),
    }
}

/// Every batch's row group, encoded on up to `threads` threads, in batch order. An encoder
/// that panics fails its batch, and with it the compaction, rather than the process.
fn encode_all(batches: &[Batch], threads: usize) -> Vec<EncodedRowGroup> {
    let encode = |batch: &Batch| {
        let refs: Vec<&Row> = batch.rows.iter().collect();
        let encoded = catch_unwind(AssertUnwindSafe(|| {
            let columns = schema_of(refs.iter().copied());
            let encoded = build::encode(
                batch.template_id,
                &columns,
                &refs,
                &batch.times,
                batch.source,
                Level::Finished,
            )?;
            Ok((columns, encoded))
        }));
        encoded.unwrap_or_else(|_| {
            Err(StoreError::Io {
                template_id: batch.template_id,
                source: std::io::Error::other("the row group encoder panicked"),
            })
        })
    };
    let threads = threads.min(batches.len());
    if threads <= 1 {
        return batches.iter().map(encode).collect();
    }
    let next = AtomicUsize::new(0);
    let done: Vec<OnceLock<EncodedRowGroup>> = batches.iter().map(|_| OnceLock::new()).collect();
    std::thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| loop {
                let at = next.fetch_add(1, Ordering::Relaxed);
                let Some(batch) = batches.get(at) else {
                    break;
                };
                let _ = done[at].set(encode(batch));
            });
        }
    });
    done.into_iter()
        .zip(batches)
        .map(|(slot, batch)| {
            slot.into_inner().unwrap_or_else(|| {
                Err(StoreError::Io {
                    template_id: batch.template_id,
                    source: std::io::Error::other("a row group was never encoded"),
                })
            })
        })
        .collect()
}

// ── Writing the outputs ────────────────────────────────────────────────────────────

/// Where a rebuild's row groups go: the reserved files, one at a time, in number order.
/// A file is created with its first row group, so inputs holding none make no output.
struct Sink<'a> {
    job: &'a Job,
    dir: PathBuf,
    max_bytes: u64,
    cancel: &'a AtomicBool,
    threads: usize,
    /// Closed and not yet encoded: at most two a thread, which bounds what a rebuild holds
    /// in memory to that many row groups of rows.
    pending: Vec<Batch>,
    writer: Option<(String, Writer)>,
    done: Vec<Written>,
}

impl Sink<'_> {
    fn cancelled(&self) -> Result<(), StoreError> {
        if self.cancel.load(Ordering::Relaxed) {
            return Err(StoreError::Cancelled {
                day: day_name(self.job.day),
            });
        }
        Ok(())
    }

    fn close(&mut self, batch: Batch) -> Result<(), StoreError> {
        self.pending.push(batch);
        if self.pending.len() >= self.threads * 2 {
            self.drain()?;
        }
        Ok(())
    }

    /// Encode every batch that is waiting and append them, in order.
    fn drain(&mut self) -> Result<(), StoreError> {
        let batches = std::mem::take(&mut self.pending);
        for (batch, encoded) in batches.iter().zip(encode_all(&batches, self.threads)) {
            let (columns, encoded) = encoded?;
            let schema_id = self.schema_id(&columns)?;
            self.put(batch.template_id, &encoded.frame(schema_id))?;
        }
        Ok(())
    }

    /// The file being written, created under the next reserved number if there is none:
    /// flagged finished, and naming every input as a file it replaces.
    fn writer(&mut self) -> Result<&mut Writer, StoreError> {
        let open = match self.writer.take() {
            Some(open) => open,
            None => {
                let name = file_name(self.job.first.saturating_add(self.done.len() as u32));
                let mut writer = Writer::create(&self.dir.join(&name))?;
                writer.finish();
                writer.replaces(self.job.parents.clone());
                (format!("{}/{name}", day_name(self.job.day)), writer)
            }
        };
        Ok(&mut self.writer.insert(open).1)
    }

    fn schema_id(&mut self, columns: &[String]) -> Result<u32, StoreError> {
        Ok(self.writer()?.schema_id(columns)?)
    }

    /// Append a row group, and seal the file if that took it past the cap - unless it is the
    /// last one reserved, which takes whatever is left.
    fn put(&mut self, template_id: u64, rg: &RowGroup) -> Result<(), StoreError> {
        let max_bytes = self.max_bytes;
        let writer = self.writer()?;
        writer.append(template_id, rg)?;
        let full = writer.len() >= max_bytes;
        if full && (self.done.len() as u32).saturating_add(1) < self.job.reserved {
            self.seal()?;
        }
        Ok(())
    }

    /// Seal and flush the file being written. Cancel is looked at first: a seal flushes
    /// the whole file, and a shutdown should not wait on that for a file it will delete.
    fn seal(&mut self) -> Result<(), StoreError> {
        self.cancelled()?;
        let Some((rel, writer)) = self.writer.take() else {
            return Ok(());
        };
        let path = writer.path().to_path_buf();
        let index = writer.seal()?;
        let bytes = fs::metadata(&path)
            .map_err(|source| FormatError::Io {
                path: path.clone(),
                source,
            })?
            .len();
        self.done.push(Written {
            rel,
            path,
            index,
            bytes,
        });
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<Written>, StoreError> {
        self.drain()?;
        self.seal()?;
        Ok(self.done)
    }
}

#[cfg(test)]
#[path = "rebuild_tests.rs"]
mod tests;
