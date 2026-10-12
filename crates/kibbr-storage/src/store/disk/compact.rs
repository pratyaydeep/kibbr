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
use crate::store::manifest::{self, FileRecord, Record};
use crate::store::v3::build::{self, schema_of, Level, RowGroup};
use crate::store::v3::file::{Index, Reader, Writer};
use crate::store::v3::{day_of, FormatError, Source};
use crate::store::{
    effective_ts, lock, value_bytes, FlushKind, Row, RowGroupLimits, StoreError, RECEIVED_AT_COLUMN,
};
use kibbr_core::types::ColValue;

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
            | Err(e @ manifest::ManifestError::Log(crate::frame::LogError::Torn { .. })) => {
                return Err(failed(job.day, e.into()))
            }
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
mod tests {
    use super::super::tests::{
        eager, ns, row, snapshot_ns, staged, store, store_with, Scratch, T0,
    };
    use super::super::{data_files, today, LoadCommit, StagedFile};
    use super::*;
    use crate::store::manifest::Manifest;
    use crate::store::v3::file;
    use crate::store::{DiskOptions, LeafStore, Rows, ScanOptions, Store};
    use std::time::{Duration, UNIX_EPOCH};

    fn options() -> CompactOptions {
        CompactOptions {
            threads: 2,
            cancel: Arc::new(AtomicBool::new(false)),
        }
    }

    /// A row group closes on every flush, a full one is sixteen rows, and a file is sealed
    /// at `cap` bytes: a few flushes make many small row groups in several files.
    fn small(cap: u64) -> DiskOptions {
        DiskOptions {
            row_groups: RowGroupLimits {
                rows: 16,
                age: Duration::ZERO,
                ..RowGroupLimits::default()
            },
            segment_max_bytes: cap,
            ..eager()
        }
    }

    /// `rounds` flushes of `each` rows to every leaf, numbered from `from`: one small row
    /// group a leaf a flush.
    fn fill(s: &DiskStore, leaves: &[&Arc<dyn LeafStore>], rounds: i64, each: i64, from: i64) {
        for round in 0..rounds {
            for leaf in leaves {
                for i in 0..each {
                    let n = from + round * each + i;
                    leaf.append(row(T0 + n, n));
                }
            }
            let done = s.flush(FlushKind::Tick);
            assert!(done.failed.is_empty(), "{:?}", done.failed);
            assert!(done.seal_failed.is_empty(), "{:?}", done.seal_failed);
        }
    }

    /// Every row of a leaf beside its position, in the order a visit hands them over.
    fn visited(leaf: &dyn LeafStore) -> Vec<(Row, u64)> {
        let mut out = Vec::new();
        leaf.visit(
            &ScanOptions::default(),
            &[],
            &mut Rows(|r: &Row, seq: u64| out.push((r.clone(), seq))),
        )
        .expect("scan");
        out
    }

    fn records(scratch: &Scratch) -> Vec<Record> {
        Manifest::open(&manifest::path_in(&scratch.0), &mut |_| {})
            .expect("manifest")
            .1
    }

    fn live(scratch: &Scratch) -> BTreeMap<String, FileRecord> {
        manifest::live(&records(scratch))
    }

    fn rel(day: i64, number: u32) -> String {
        format!("{}/{}", day_name(day), file_name(number))
    }

    fn paths(scratch: &Scratch, rels: &[String]) -> BTreeSet<PathBuf> {
        let data = scratch.0.join(DATA_DIR);
        rels.iter().map(|rel| data.join(rel)).collect()
    }

    /// The row counts of each template's row groups across `rels`, in storage order.
    fn row_groups(scratch: &Scratch, rels: &[String]) -> BTreeMap<u64, Vec<u32>> {
        let mut out: BTreeMap<u64, Vec<u32>> = BTreeMap::new();
        for path in paths(scratch, rels) {
            let index = file::open_sealed(&path).expect("readable").expect("sealed");
            for e in &index.entries {
                out.entry(e.template_id).or_default().push(e.row_count);
            }
        }
        out
    }

    /// Many small row groups over several files become a few full ones in finished files, with
    /// one record, and nothing a query reads moves - before a restart and after.
    #[test]
    fn a_compacted_day_reads_back_row_for_row_from_fewer_full_row_groups() {
        let scratch = Scratch::new("compact-whole");
        let day = day_of(T0);
        let (before, added) = {
            let s = store_with(&scratch, small(700));
            let a = s.open(7, "ts").expect("leaf");
            let b = s.open(8, "ts").expect("leaf");
            fill(&s, &[&a, &b], 12, 5, 0);
            assert!(s.flush(FlushKind::Final).seal_failed.is_empty());
            let before = (visited(a.as_ref()), visited(b.as_ref()));
            let recorded = records(&scratch).len();
            let inputs = live(&scratch);
            assert!(inputs.len() >= 3, "{} file(s): lower the cap", inputs.len());
            assert!(inputs.values().all(|f| f.sealed && !f.finished));
            let bytes_in: u64 = data_files(&scratch.0)
                .iter()
                .map(|p| fs::metadata(p).expect("stat").len())
                .sum();

            let done = s.compact_day(day, &options()).expect("compact");
            assert!(done.compacted);
            assert_eq!(
                (done.rows, done.files_in, done.row_groups_in, done.bytes_in),
                (120, inputs.len(), 24, bytes_in)
            );
            assert_eq!(done.removed, inputs.keys().cloned().collect::<Vec<_>>());
            assert_eq!((visited(a.as_ref()), visited(b.as_ref())), before);

            // One record, and it is the whole of the change.
            let all = records(&scratch);
            assert_eq!(all.len(), recorded + 1, "{:?}", &all[recorded..]);
            match &all[recorded] {
                Record::Replace {
                    add,
                    remove,
                    source,
                    wal: None,
                } => {
                    assert_eq!(remove, &done.removed);
                    assert_eq!(
                        add.iter().map(|f| f.path.clone()).collect::<Vec<_>>(),
                        done.added
                    );
                    assert!(add.iter().all(|f| f.sealed && f.finished), "{add:?}");
                    assert_eq!(add.iter().filter_map(|f| f.rows).sum::<u64>(), 120);
                    assert_eq!(source.as_deref(), Some("compact"));
                }
                other => panic!("the record is {other:?}"),
            }
            let now = live(&scratch);
            assert_eq!(now.keys().cloned().collect::<Vec<_>>(), done.added);
            assert!(now.values().all(|f| f.sealed && f.finished), "{now:?}");

            // Each output names every input, by path and id; every one but the last
            // reached the cap; and the inputs are gone, nothing reading them.
            let parents: Vec<(String, [u8; 16])> = inputs
                .iter()
                .map(|(rel, f)| (rel.clone(), manifest::file_id_of(&f.file_id).expect("id")))
                .collect();
            let written: Vec<PathBuf> = paths(&scratch, &done.added).into_iter().collect();
            assert!(written.len() >= 2, "one output: lower the cap");
            assert_eq!(done.files_out, written.len());
            let mut bytes_out = 0;
            for (at, path) in written.iter().enumerate() {
                let index = file::open_sealed(path).expect("readable").expect("sealed");
                assert!(index.finished, "{} is not finished", path.display());
                assert_eq!(index.replaced, parents);
                let len = fs::metadata(path).expect("stat").len();
                assert!(at + 1 == written.len() || len >= 700, "{len} bytes");
                bytes_out += len;
            }
            assert_eq!(done.bytes_out, bytes_out);
            assert_eq!(
                data_files(&scratch.0),
                written.iter().cloned().collect::<BTreeSet<_>>()
            );

            // Twelve row groups of five a template are now three of sixteen and the rest.
            let groups = row_groups(&scratch, &done.added);
            assert_eq!(groups[&7], vec![16, 16, 16, 12]);
            assert_eq!(groups[&8], vec![16, 16, 16, 12]);
            assert_eq!(done.row_groups_out, 8);
            (before, done.added)
        };

        let s = store_with(&scratch, small(700));
        let a = s.open(7, "ts").expect("leaf");
        let b = s.open(8, "ts").expect("leaf");
        assert_eq!((visited(a.as_ref()), visited(b.as_ref())), before);
        assert_eq!(data_files(&scratch.0), paths(&scratch, &added));
        // And a second compaction finds nothing to do, and writes nothing.
        let len = lock(&s.files).manifest.len();
        let again = s.compact_day(day, &options()).expect("compact");
        assert_eq!(
            again,
            Compacted {
                day: day_name(day),
                ..Compacted::default()
            }
        );
        assert_eq!(lock(&s.files).manifest.len(), len);
        assert_eq!((visited(a.as_ref()), visited(b.as_ref())), before);
    }

    /// Every row of a leaf as a query shows it - each key's JSON under the name it is
    /// shown by - beside its position.
    fn shown(leaf: &dyn LeafStore) -> Vec<(BTreeMap<String, serde_json::Value>, u64)> {
        visited(leaf)
            .into_iter()
            .map(|(row, seq)| {
                let keys = crate::store::present(&row);
                let json = keys
                    .into_iter()
                    .filter_map(|(name, key)| Some((name, row.json(key)?)))
                    .collect();
                (json, seq)
            })
            .collect()
    }

    /// Rows that carry their own time and rows that fell back to their arrival time share a
    /// row group: merged, every row is shown as it was, where it was.
    #[test]
    fn rows_with_and_without_an_arrival_time_are_shown_as_they_were() {
        let scratch = Scratch::new("compact-arrival");
        let day = day_of(T0);
        let before = {
            let s = store_with(&scratch, small(900));
            let leaves: Vec<Arc<dyn LeafStore>> =
                (7..10).map(|id| s.open(id, "ts").expect("leaf")).collect();
            let mut state = 0x9e37_79b9u32;
            let mut draw = |below: u32| {
                state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                (state >> 16) % below
            };
            for n in 0..400i64 {
                // A handful of distinct times, so most rows are tied with others.
                let at = T0 + i64::from(draw(6));
                let mut r = row(at, n);
                if draw(3) == 0 {
                    r.insert("ts".into(), ColValue::String("N/A".into()));
                    r.insert(RECEIVED_AT_COLUMN.into(), ColValue::Datetime(at));
                }
                leaves[draw(3) as usize].append(r);
                if draw(9) == 0 {
                    let done = s.flush(FlushKind::Tick);
                    assert!(done.failed.is_empty(), "{:?}", done.failed);
                }
            }
            s.flush(FlushKind::Final);
            let before: Vec<_> = leaves.iter().map(|leaf| shown(leaf.as_ref())).collect();
            assert_eq!(before.iter().map(Vec::len).sum::<usize>(), 400);
            let done = s.compact_day(day, &options()).expect("compact");
            assert!(done.files_in >= 3 && done.files_out >= 2, "{done:?}");
            assert!(done.row_groups_out < done.row_groups_in, "{done:?}");
            assert_eq!(done.rows, 400);
            for (leaf, was) in leaves.iter().zip(&before) {
                assert_eq!(&shown(leaf.as_ref()), was);
            }
            before
        };
        let s = store_with(&scratch, small(900));
        for (id, was) in (7..10).zip(&before) {
            assert_eq!(&shown(s.open(id, "ts").expect("leaf").as_ref()), was);
        }
    }

    /// Whether each of a template's row groups across `rels`, in storage order, has the
    /// arrival-time column in its schema.
    fn arrival_columns(scratch: &Scratch, rels: &[String], id: u64) -> Vec<(u32, bool)> {
        let mut out = Vec::new();
        for path in paths(scratch, rels) {
            let index = file::open_sealed(&path).expect("readable").expect("sealed");
            for e in index.entries.iter().filter(|e| e.template_id == id) {
                let schema = &index.schemas[e.schema_id as usize];
                out.push((e.row_count, schema.iter().any(|c| c == RECEIVED_AT_COLUMN)));
            }
        }
        out
    }

    /// Rebuilt into a row group of rows that all carry their own time, the arrival-time nulls
    /// are not columns: the schema is the one the live door would have written.
    #[test]
    fn a_row_group_rebuilt_from_rows_that_all_carry_their_own_time_has_no_arrival_column() {
        let scratch = Scratch::new("compact-own-time");
        let day = day_of(T0);
        let s = store_with(&scratch, small(1 << 20));
        let leaf = s.open(7, "ts").expect("leaf");
        // Eight rows with their own time; then eight more and eight without in one row
        // group. Sixteen to a rebuilt row group: the first is the sixteen with their own.
        fill(&s, &[&leaf], 1, 8, 0);
        for n in 8..24 {
            let mut r = row(T0 + n, n);
            if n >= 16 {
                r.insert("ts".into(), ColValue::String("N/A".into()));
                r.insert(RECEIVED_AT_COLUMN.into(), ColValue::Datetime(T0 + n));
            }
            leaf.append(r);
        }
        assert!(s.flush(FlushKind::Final).failed.is_empty());
        let before = shown(leaf.as_ref());
        let inputs: Vec<String> = live(&scratch).keys().cloned().collect();
        assert_eq!(
            arrival_columns(&scratch, &inputs, 7),
            vec![(8, false), (16, true)],
            "no row group holds rows of both kinds"
        );

        let done = s.compact_day(day, &options()).expect("compact");
        assert_eq!(
            arrival_columns(&scratch, &done.added, 7),
            vec![(16, false), (8, true)]
        );
        assert_eq!(shown(leaf.as_ref()), before);
    }

    /// A query that froze the day before the compaction published reads the old files to
    /// the end: they stay on disk while it holds them, and go when it lets go.
    #[test]
    fn a_snapshot_taken_before_a_compaction_reads_the_old_files_until_it_is_dropped() {
        let scratch = Scratch::new("compact-snapshot");
        let s = store_with(&scratch, small(700));
        let leaf = s.open(7, "ts").expect("leaf");
        fill(&s, &[&leaf], 6, 5, 0);
        s.flush(FlushKind::Final);
        let old = data_files(&scratch.0);
        let opts = ScanOptions::default();
        let mut held = s.snapshot(vec![(7, Arc::clone(&leaf))], &opts);
        let generation = s.generation();

        let done = s.compact_day(day_of(T0), &options()).expect("compact");
        assert!(done.compacted);
        assert_eq!(s.generation(), generation + 1, "a publish is not counted");
        assert_eq!(ns(leaf.as_ref()), (0..30).collect::<Vec<_>>());
        assert!(old.iter().all(|p| p.exists()), "deleted under a snapshot");
        assert!(paths(&scratch, &done.added).iter().all(|p| p.exists()));
        assert_eq!(
            snapshot_ns(held.as_mut(), 7),
            (0..30).collect::<Vec<_>>(),
            "the snapshot lost the day it was taken with"
        );
        drop(held);
        assert!(
            old.iter().all(|p| !p.exists()),
            "a retired file outlived its last snapshot"
        );
        assert_eq!(data_files(&scratch.0), paths(&scratch, &done.added));
    }

    /// A row for the day that arrives while it is being rebuilt opens a file numbered after
    /// the outputs: every position is what it was, in memory and after a restart.
    #[test]
    fn a_file_the_live_door_opens_during_a_compaction_moves_no_position() {
        let scratch = Scratch::new("compact-late");
        let day = day_of(T0);
        let (before, late) = {
            let s = store_with(&scratch, small(700));
            let leaf = s.open(7, "ts").expect("leaf");
            fill(&s, &[&leaf], 6, 5, 0);
            let job = s.begin(day).expect("begin").expect("a job");
            fill(&s, &[&leaf], 1, 5, 30);
            let before = visited(leaf.as_ref());
            assert_eq!(before.len(), 35);
            let outputs = s.rebuild(&job, &options()).expect("rebuild");
            let done = s.commit(job, outputs).expect("commit");
            assert_eq!(done.rows, 30);
            assert_eq!(visited(leaf.as_ref()), before, "the publish moved a row");
            let unfinished: Vec<String> = lock(&s.files)
                .live
                .values()
                .filter(|f| !f.finished)
                .map(|f| f.path.clone())
                .collect();
            assert_eq!(unfinished.len(), 1, "{unfinished:?}");
            assert!(
                done.added.iter().all(|rel| *rel < unfinished[0]),
                "{:?} do not all sort before {}",
                done.added,
                unfinished[0]
            );
            (before, unfinished[0].clone())
            // No final flush: the late file is left as a crash leaves it.
        };

        let s = store_with(&scratch, small(700));
        let leaf = s.open(7, "ts").expect("leaf");
        assert_eq!(visited(leaf.as_ref()), before, "a restart moved a row");
        assert!(!live(&scratch)[&late].finished);
        let again = s.compact_day(day, &options()).expect("compact");
        assert_eq!(again.removed, vec![late]);
        assert_eq!(again.rows, 5);
        assert_eq!(visited(leaf.as_ref()), before);
    }

    /// A load into a day the live door had written leaves a finished file between live
    /// ones. Only the files after it are taken: the one before it would move behind it.
    #[test]
    fn only_the_files_after_the_days_last_finished_file_are_compacted() {
        let scratch = Scratch::new("compact-trailing");
        let day = day_of(T0);
        let before = {
            let s = store(&scratch);
            let leaf = s.open(7, "ts").expect("leaf");
            for n in 0..3 {
                leaf.append(row(T0 + n, n));
            }
            s.flush(FlushKind::Final);
            let dir = s.stage(9).expect("stage");
            s.commit_load(LoadCommit {
                load_id: 9,
                files: vec![staged(&dir, "a.col", 7, 10..13)],
                replace_day: None,
            })
            .expect("commit");
            for n in 20..23 {
                leaf.append(row(T0 + n, n));
            }
            s.flush(FlushKind::Tick);
            let before = visited(leaf.as_ref());
            let kept: Vec<FileRecord> = [1, 2]
                .iter()
                .map(|n| lock(&s.files).live[&rel(day, *n)].clone())
                .collect();
            assert!(!kept[0].finished && kept[1].finished);
            assert_eq!(s.compactable_days(), BTreeSet::from([day]));

            let done = s.compact_day(day, &options()).expect("compact");
            assert_eq!(done.removed, vec![rel(day, 3)]);
            assert_eq!(done.added, vec![rel(day, 4)]);
            assert_eq!((done.rows, done.files_in), (3, 1));
            assert_eq!(visited(leaf.as_ref()), before);
            let now = live(&scratch);
            assert_eq!(
                now.keys().cloned().collect::<Vec<_>>(),
                vec![rel(day, 1), rel(day, 2), rel(day, 4)]
            );
            assert_eq!(
                vec![now[&rel(day, 1)].clone(), now[&rel(day, 2)].clone()],
                kept,
                "a file before the inputs was touched"
            );
            // The first file is unfinished still, and nothing takes it: the day is done.
            assert!(!s.compact_day(day, &options()).expect("compact").compacted);
            assert!(s.due_days(today() + 1).is_empty());
            assert!(s.compactable_days().is_empty());
            before
        };
        let s = store(&scratch);
        assert_eq!(visited(s.open(7, "ts").expect("leaf").as_ref()), before);
    }

    /// What a refusal must leave as it was: the manifest, the live set, the numbers, and
    /// no day marked as being compacted.
    fn state(s: &DiskStore) -> (u64, BTreeMap<String, FileRecord>, BTreeMap<i64, u32>, bool) {
        let files = lock(&s.files);
        (
            files.manifest.len(),
            files.live.clone(),
            files.next.clone(),
            files.compacting.is_some(),
        )
    }

    /// A file a failed seal left - named, unsealed, no longer open - sits between files
    /// of the day. The day is refused, naming it, and nothing at all is written.
    #[test]
    fn a_day_holding_an_unsealed_file_that_is_not_open_is_refused_and_is_not_due() {
        let scratch = Scratch::new("compact-stuck");
        let day = day_of(T0);
        let s = store(&scratch);
        let leaf = s.open(7, "ts").expect("leaf");
        for n in 0..3 {
            leaf.append(row(T0 + n, n));
        }
        s.flush(FlushKind::Tick);
        assert_eq!(s.due_days(today() + 1).len(), 1, "the open file is not due");
        let left = lock(&s.files).open.remove(&day).expect("an open file").rel;
        for n in 3..6 {
            leaf.append(row(T0 + n, n));
        }
        s.flush(FlushKind::Tick);
        let before = visited(leaf.as_ref());
        let was = state(&s);

        match s.compact_day(day, &options()) {
            Err(StoreError::CompactRefused { path, .. }) => assert_eq!(path, left),
            other => panic!("compacted around an unsealed file: {other:?}"),
        }
        assert_eq!(state(&s), was, "a refusal wrote something");
        assert_eq!(visited(leaf.as_ref()), before);
        assert_eq!(data_files(&scratch.0).len(), 2);
        assert!(s.due_days(today() + 1).is_empty(), "a refused day is due");
        // Not due, and still a day with files to compact: what a failure is kept against.
        assert_eq!(s.compactable_days(), BTreeSet::from([day]));
        // The start that recovers the file makes the day whole again.
        drop(leaf);
        drop(s);
        let s = store(&scratch);
        let leaf = s.open(7, "ts").expect("leaf");
        assert_eq!(visited(leaf.as_ref()), before);
        assert!(s.compact_day(day, &options()).expect("compact").compacted);
        assert_eq!(visited(leaf.as_ref()), before);
    }

    /// A mixed day, and any day while a committed load is still in staging, is refused before
    /// the flush - and is not due, so the scheduler does not walk into the refusal.
    #[test]
    fn a_day_with_a_mixed_file_or_an_unsettled_load_is_refused_and_is_not_due() {
        let scratch = Scratch::new("compact-not-taken");
        let day = day_of(T0);
        let s = store(&scratch);
        let leaf = s.open(7, "ts").expect("leaf");
        fill(&s, &[&leaf], 2, 5, 0);
        let open = lock(&s.files).open[&day].rel.clone();
        let was = state(&s);
        assert_eq!(s.due_days(today() + 1).len(), 1);

        lock(&s.files).mixed.insert(day, open.clone());
        match s.compact_day(day, &options()) {
            Err(StoreError::CompactRefused { path, .. }) => assert_eq!(path, open),
            other => panic!("compacted a day sharing a file with another: {other:?}"),
        }
        assert!(s.due_days(today() + 1).is_empty(), "a mixed day is due");
        assert_eq!(state(&s), was, "a refusal wrote something");
        lock(&s.files).mixed.clear();

        lock(&s.files).unmoved.insert(1);
        match s.compact_day(day, &options()) {
            Err(StoreError::CompactRefused { path, .. }) => {
                assert_eq!(path, format!("{STAGING_DIR}/{:016x}", 1))
            }
            other => panic!("compacted beside a load still in staging: {other:?}"),
        }
        assert!(
            s.due_days(today() + 1).is_empty(),
            "a day is due beside a load still in staging"
        );
        assert_eq!(state(&s), was, "a refusal wrote something");
        // Not sealed by either refusal: the day's file is still the one being written.
        assert_eq!(lock(&s.files).open[&day].rel, open);
        lock(&s.files).unmoved.clear();

        assert_eq!(s.due_days(today() + 1).len(), 1);
        let done = s.compact_day(day, &options()).expect("compact");
        assert_eq!((done.compacted, done.rows), (true, 10));
    }

    /// A file whose row groups in this process are not the rows the manifest gives it is
    /// refused by name: rebuilt from what is held, the day would come out a row short or over.
    #[test]
    fn a_file_whose_row_groups_do_not_add_up_to_its_manifest_rows_is_refused() {
        let scratch = Scratch::new("compact-rows");
        let day = day_of(T0);
        let s = store_with(&scratch, small(700));
        let leaf = s.open(7, "ts").expect("leaf");
        fill(&s, &[&leaf], 6, 5, 0);
        s.flush(FlushKind::Final);
        let before = visited(leaf.as_ref());
        let old = data_files(&scratch.0);
        assert!(old.len() >= 2, "{} file(s): lower the cap", old.len());
        let short = rel(day, 2);
        let rows = lock(&s.files).live[&short].rows.expect("a sealed file");
        let set = |rows: u64| {
            if let Some(record) = lock(&s.files).live.get_mut(&short) {
                record.rows = Some(rows);
            }
        };
        set(rows + 1);
        let was = state(&s);

        match s.compact_day(day, &options()) {
            Err(StoreError::CompactRefused { path, .. }) => assert_eq!(path, short),
            other => panic!("rebuilt a file a row short: {other:?}"),
        }
        assert_eq!(state(&s), was, "a refusal wrote something");
        assert_eq!(data_files(&scratch.0), old);
        assert_eq!(visited(leaf.as_ref()), before);

        set(rows);
        let done = s.compact_day(day, &options()).expect("compact");
        assert_eq!((done.compacted, done.rows), (true, 30));
        assert_eq!(visited(leaf.as_ref()), before);
    }

    /// The flush a compaction begins with can fail, and has changed the day by then: neither
    /// a failed write nor a failed seal is reported as leaving the day's files as they were.
    #[test]
    fn a_compaction_whose_own_flush_fails_says_so_and_one_whose_seal_fails_names_the_file() {
        let day = day_of(T0);
        let dead = |scratch: &Scratch| {
            Manifest::read_only(&scratch.0.join("dead.log")).expect("read-only")
        };

        // The seal: the day's file is open, and the manifest takes no record of its seal.
        let scratch = Scratch::new("compact-begin-seal");
        let s = store(&scratch);
        let leaf = s.open(7, "ts").expect("leaf");
        fill(&s, &[&leaf], 2, 5, 0);
        let open = lock(&s.files).open[&day].rel.clone();
        lock(&s.files).manifest = dead(&scratch);
        let said = match s.compact_day(day, &options()) {
            Err(e @ StoreError::CompactRefused { .. }) => e.to_string(),
            other => panic!("a seal that failed returned {other:?}"),
        };
        assert!(said.contains(&open), "{said}");
        assert!(said.contains("dead.log"), "the cause is not in: {said}");
        assert!(!said.contains("as they were"), "{said}");
        assert!(lock(&s.files).compacting.is_none());
        match s.compact_day(day, &options()) {
            Err(StoreError::CompactRefused { path, .. }) => assert_eq!(path, open),
            other => panic!("compacted around an unsealed file: {other:?}"),
        }
        assert!(s.due_days(today() + 1).is_empty(), "a refused day is due");
        assert_eq!(ns(leaf.as_ref()), (0..10).collect::<Vec<_>>());

        // The write: the day has rows in memory and no file, and none can be named.
        let scratch = Scratch::new("compact-begin-flush");
        let s = store(&scratch);
        let leaf = s.open(7, "ts").expect("leaf");
        for n in 0..5 {
            leaf.append(row(T0 + n, n));
        }
        lock(&s.files).manifest = dead(&scratch);
        let said = match s.compact_day(day, &options()) {
            Err(e @ StoreError::CompactFlushFailed { .. }) => e.to_string(),
            other => panic!("a flush that failed returned {other:?}"),
        };
        assert!(said.contains("dead.log"), "the cause is not in: {said}");
        assert!(!said.contains("as they were"), "{said}");
        assert!(lock(&s.files).compacting.is_none());
        assert_eq!(leaf.buffered(), 5, "the rows are not back in their buffer");
        assert_eq!(ns(leaf.as_ref()), (0..5).collect::<Vec<_>>());
        assert!(data_files(&scratch.0).is_empty());
    }

    /// Row groups written from the log keep, rebuilt, the smallest range holding every position
    /// they held: a replay goes on skipping the lines a compacted day holds.
    #[test]
    fn a_compacted_day_still_covers_the_log_positions_its_row_groups_held() {
        let scratch = Scratch::new("compact-wal");
        let day = day_of(T0);
        let ranges = |s: &DiskStore, of: i64| s.wal_ranges(7, of, 0xabc).expect("ranges");
        {
            let s = store(&scratch);
            s.attach_wal(0xabc, 0);
            let leaf = s.open(7, "ts").expect("leaf");
            for group in [vec![2u64, 3, 4], vec![10, 11], vec![20]] {
                for pos in group {
                    leaf.append_at(row(T0 + pos as i64, pos as i64), pos);
                }
                // The next day's rows of the same template, between them in the log.
                leaf.append_at(row(T0 + crate::store::v3::DAY_NS, 0), 5);
                s.flush(FlushKind::Tick);
            }
            assert_eq!(ranges(&s, day), [(2, 4), (10, 11), (20, 20)]);

            let done = s.compact_day(day, &options()).expect("compact");
            assert!(done.compacted, "{done:?}");
            assert_eq!((done.row_groups_in, done.row_groups_out), (3, 1));
            assert_eq!(ranges(&s, day), [(2, 20)]);
            // The other day's row groups are their own, and were not touched.
            assert_eq!(ranges(&s, day + 1), [(5, 5)]);
            assert_eq!(ns(leaf.as_ref()), vec![0, 0, 0, 2, 3, 4, 10, 11, 20]);
        }
        let s = store(&scratch);
        s.open(7, "ts").expect("leaf");
        assert_eq!(ranges(&s, day), [(2, 20)]);
    }

    /// Under a write-ahead log the scheduler leaves a day until all of it is behind the log's
    /// checkpoint; asked for by name the day is compacted regardless.
    #[test]
    fn a_day_is_not_due_until_it_is_behind_the_logs_checkpoint() {
        let scratch = Scratch::new("compact-checkpoint");
        let day = day_of(T0);
        let s = store(&scratch);
        s.attach_wal(0xabc, 0);
        let leaf = s.open(7, "ts").expect("leaf");
        leaf.append_at(row(T0, 0), 5);
        s.flush(FlushKind::Tick);
        let now = lock(&s.files).last_live[&day];
        assert!(
            s.due_days(now + 1).is_empty(),
            "due ahead of the checkpoint"
        );
        s.note_checkpoint(4);
        assert!(
            s.due_days(now + 1).is_empty(),
            "due ahead of the checkpoint"
        );
        s.note_checkpoint(5);
        assert_eq!(s.due_days(now + 1).len(), 1);

        leaf.append_at(row(T0 + 1, 1), 6);
        assert!(
            s.due_days(now + 1).is_empty(),
            "due with a row of the day still in memory"
        );
        let done = s.compact_day(day, &options()).expect("compact");
        assert_eq!((done.compacted, done.rows), (true, 2));
    }

    /// Due the day after the day's last live row group, never the day itself - and a start
    /// reads the last write off the files, the newest of them.
    #[test]
    fn a_day_is_due_the_day_after_its_last_live_row() {
        let scratch = Scratch::new("compact-due");
        let day = day_of(T0);
        let days = |s: &DiskStore, today: i64| -> Vec<i64> {
            s.due_days(today).iter().map(|due| due.day).collect()
        };
        // The day a flush wrote on, as the store took it down - not read off the clock
        // here, a moment before or after: across UTC midnight the two are a day apart.
        let wrote = |s: &DiskStore| lock(&s.files).last_live[&day];
        {
            let s = store(&scratch);
            let leaf = s.open(7, "ts").expect("leaf");
            leaf.append(row(T0, 0));
            assert!(
                days(&s, today() + 1).is_empty(),
                "a day with no file is due"
            );
            s.flush(FlushKind::Tick);
            let now = wrote(&s);
            assert!(days(&s, now).is_empty(), "due on the day of its last row");
            let due = s.due_days(now + 1);
            assert_eq!(days(&s, now + 1), vec![day]);

            // What is due is named by its files: another file is another number.
            s.flush(FlushKind::Final);
            leaf.append(row(T0 + 1, 1));
            s.flush(FlushKind::Tick);
            let now = wrote(&s);
            let more = s.due_days(now + 1);
            assert_eq!(more, s.due_days(now + 9));
            assert_ne!(more[0].inputs, due[0].inputs);

            s.compact_day(day, &options()).expect("compact");
            assert!(days(&s, now + 1).is_empty(), "a finished day is due");
            // A late row makes it due again, the day after that row.
            leaf.append(row(T0 + 2, 2));
            s.flush(FlushKind::Final);
            let now = wrote(&s);
            assert!(days(&s, now).is_empty());
            assert_eq!(days(&s, now + 1), vec![day]);
            // And a later one still: a second unfinished file for a start to read.
            leaf.append(row(T0 + 3, 3));
            s.flush(FlushKind::Final);
        }
        let dated = |path: &Path, written: i64| {
            let at = UNIX_EPOCH + Duration::from_secs(written as u64 * 86_400 + 3_600);
            let file = fs::File::options().write(true).open(path).expect("open");
            file.set_modified(at).expect("set the time");
        };
        // Written three days after the day itself; and written the day before it, which
        // leaves only the rule that a day is never due while it is today.
        for (written, not_yet, due) in [(day + 3, day + 3, day + 4), (day - 1, day, day + 1)] {
            for path in data_files(&scratch.0) {
                dated(&path, written);
            }
            let s = store(&scratch);
            assert!(days(&s, not_yet).is_empty(), "written {written}: due early");
            assert_eq!(days(&s, due), vec![day], "written {written}");
        }
        // Two unfinished files written two days apart: the day's last row is in the newer,
        // whichever of the two sorts first.
        let unfinished: Vec<PathBuf> = live(&scratch)
            .values()
            .filter(|f| !f.finished)
            .map(|f| scratch.0.join(DATA_DIR).join(&f.path))
            .collect();
        assert_eq!(unfinished.len(), 2, "{unfinished:?}");
        for (first, second) in [(day + 1, day + 3), (day + 3, day + 1)] {
            dated(&unfinished[0], first);
            dated(&unfinished[1], second);
            let s = store(&scratch);
            assert!(
                days(&s, day + 3).is_empty(),
                "written {first} and {second}: due by the older file"
            );
            assert_eq!(days(&s, day + 4), vec![day], "written {first} and {second}");
        }
    }

    /// A template no leaf is open for has no time column anyone here knows, so its row groups
    /// go across byte for byte, and read back as they were when its leaf opens.
    #[test]
    fn a_template_with_no_leaf_is_copied_as_it_is_and_reads_back_once_its_leaf_opens() {
        let scratch = Scratch::new("compact-no-leaf");
        let day = day_of(T0);
        let (known, orphan) = {
            let s = store_with(&scratch, small(700));
            let a = s.open(7, "ts").expect("leaf");
            let b = s.open(8, "ts").expect("leaf");
            fill(&s, &[&a, &b], 6, 5, 0);
            s.flush(FlushKind::Final);
            (visited(a.as_ref()), visited(b.as_ref()))
        };
        {
            let s = store_with(&scratch, small(700));
            let a = s.open(7, "ts").expect("leaf");
            let done = s.compact_day(day, &options()).expect("compact");
            assert!(done.compacted);
            assert_eq!(
                (done.rows, done.row_groups_in, done.row_groups_out),
                (60, 12, 8)
            );
            let groups = row_groups(&scratch, &done.added);
            assert_eq!(groups[&7], vec![16, 14], "the template with a leaf");
            assert_eq!(groups[&8], vec![5; 6], "the template with none was rebuilt");
            assert_eq!(visited(a.as_ref()), known);
            let b = s.open(8, "ts").expect("leaf");
            assert_eq!(visited(b.as_ref()), orphan);
            // Read from the rebuilt files, and not from inputs a list still held.
            assert_eq!(
                data_files(&scratch.0),
                paths(&scratch, &done.added),
                "an input outlived the compaction"
            );
        }
        let s = store_with(&scratch, small(700));
        assert_eq!(visited(s.open(8, "ts").expect("leaf").as_ref()), orphan);
        assert_eq!(visited(s.open(7, "ts").expect("leaf").as_ref()), known);
    }

    /// A template whose leaf opens between the start and the commit was copied as one with
    /// none; the commit swaps its row groups in the leaf, which reads every row once.
    #[test]
    fn a_leaf_that_opens_during_a_compaction_takes_the_rebuilt_row_groups() {
        let scratch = Scratch::new("compact-leaf-mid");
        let day = day_of(T0);
        let (known, orphan) = {
            let s = store_with(&scratch, small(700));
            let a = s.open(7, "ts").expect("leaf");
            let b = s.open(8, "ts").expect("leaf");
            fill(&s, &[&a, &b], 6, 5, 0);
            s.flush(FlushKind::Final);
            (visited(a.as_ref()), visited(b.as_ref()))
        };
        {
            let s = store_with(&scratch, small(700));
            let a = s.open(7, "ts").expect("leaf");
            let job = s.begin(day).expect("begin").expect("a job");
            let b = s.open(8, "ts").expect("leaf");
            assert_eq!(visited(b.as_ref()), orphan, "before the commit");
            let outputs = s.rebuild(&job, &options()).expect("rebuild");
            let done = s.commit(job, outputs).expect("commit");
            assert_eq!(visited(a.as_ref()), known);
            assert_eq!(visited(b.as_ref()), orphan, "after the commit");
            assert_eq!(
                data_files(&scratch.0),
                paths(&scratch, &done.added),
                "an input outlived the compaction"
            );
        }
        let s = store_with(&scratch, small(700));
        assert_eq!(visited(s.open(8, "ts").expect("leaf").as_ref()), orphan);
        assert_eq!(visited(s.open(7, "ts").expect("leaf").as_ref()), known);
    }

    /// A replace-a-day that commits while the day is being rebuilt wins: the commit finds its
    /// inputs retired and removes what it wrote. A rebuild after the replace stops at once.
    #[test]
    fn a_day_replaced_during_its_compaction_is_the_loads_and_the_outputs_are_removed() {
        let scratch = Scratch::new("compact-replaced");
        let day = day_of(T0);
        let s = store_with(&scratch, small(700));
        let leaf = s.open(7, "ts").expect("leaf");
        fill(&s, &[&leaf], 6, 5, 0);
        s.flush(FlushKind::Final);
        let old = data_files(&scratch.0);

        let job = s.begin(day).expect("begin").expect("a job");
        let outputs = s.rebuild(&job, &options()).expect("rebuild");
        let written: Vec<PathBuf> = outputs.files.iter().map(|f| f.path.clone()).collect();
        assert!(!written.is_empty() && written.iter().all(|p| p.exists()));
        let dir = s.stage(9).expect("stage");
        let replaced = s
            .commit_load(LoadCommit {
                load_id: 9,
                files: vec![staged(&dir, "a.col", 7, 100..105)],
                replace_day: Some(day),
            })
            .expect("replace");
        assert!(old.iter().all(|p| p.exists()), "deleted under the job");
        match s.commit(job, outputs) {
            Err(StoreError::InputsChanged { .. }) => {}
            other => panic!("committed over a replaced day: {other:?}"),
        }
        assert!(written.iter().all(|p| !p.exists()), "an output was kept");
        assert!(old.iter().all(|p| !p.exists()), "a retired input was kept");
        assert_eq!(ns(leaf.as_ref()), (100..105).collect::<Vec<_>>());
        assert_eq!(data_files(&scratch.0), paths(&scratch, &replaced.added));
        assert_eq!(
            live(&scratch).keys().cloned().collect::<Vec<_>>(),
            replaced.added
        );
        assert!(!s.compact_day(day, &options()).expect("compact").compacted);

        // Begun before a replace and rebuilt after it.
        fill(&s, &[&leaf], 2, 5, 200);
        let job = s.begin(day).expect("begin").expect("a job");
        let dir = s.stage(10).expect("stage");
        s.commit_load(LoadCommit {
            load_id: 10,
            files: vec![staged(&dir, "a.col", 7, 300..302)],
            replace_day: Some(day),
        })
        .expect("replace");
        match s.rebuild(&job, &options()) {
            Err(StoreError::InputsChanged { .. }) => s.abandon(job),
            Ok(_) => panic!("rebuilt a day that was replaced"),
            Err(other) => panic!("{other}"),
        }
        assert_eq!(ns(leaf.as_ref()), vec![300, 301]);
        assert_eq!(data_files(&scratch.0).len(), 1);
    }

    /// A stop before the record leaves the outputs unnamed and a stop after it the inputs; the
    /// next start deletes whichever, and every row is there once, where it was.
    #[test]
    fn a_stop_before_the_record_keeps_the_old_files_and_a_stop_after_it_the_new() {
        for recorded in [false, true] {
            let scratch = Scratch::new(if recorded {
                "compact-stop-after"
            } else {
                "compact-stop-before"
            });
            let day = day_of(T0);
            let (before, old, new) = {
                let s = store_with(&scratch, small(700));
                let a = s.open(7, "ts").expect("leaf");
                let b = s.open(8, "ts").expect("leaf");
                fill(&s, &[&a, &b], 6, 5, 0);
                s.flush(FlushKind::Final);
                let before = (visited(a.as_ref()), visited(b.as_ref()));
                let old = data_files(&scratch.0);
                let job = s.begin(day).expect("begin").expect("a job");
                let outputs = s.rebuild(&job, &options()).expect("rebuild");
                let new: BTreeSet<PathBuf> = outputs.files.iter().map(|f| f.path.clone()).collect();
                assert!(!new.is_empty() && new.iter().all(|p| p.exists()));
                if recorded {
                    lock(&s.files)
                        .record(vec![replace_record(&job, &outputs)])
                        .expect("record");
                }
                // The process stops here: nothing published, nothing unlinked.
                assert!(old.iter().all(|p| p.exists()));
                (before, old, new)
            };
            let s = store_with(&scratch, small(700));
            let a = s.open(7, "ts").expect("leaf");
            let b = s.open(8, "ts").expect("leaf");
            assert_eq!(
                (visited(a.as_ref()), visited(b.as_ref())),
                before,
                "recorded: {recorded}"
            );
            assert_eq!(
                data_files(&scratch.0),
                if recorded { new } else { old },
                "recorded: {recorded}"
            );
            let done = s.compact_day(day, &options()).expect("compact");
            assert_eq!(done.compacted, !recorded, "recorded: {recorded}");
            assert_eq!((visited(a.as_ref()), visited(b.as_ref())), before);
        }
    }

    /// Inputs with no row group in them - a file named and sealed before a row reached it
    /// - make no output, and are still retired: one record with nothing to add.
    #[test]
    fn inputs_that_hold_no_row_group_are_removed_with_one_record_and_no_output() {
        let scratch = Scratch::new("compact-empty");
        let day = day_of(T0);
        {
            let s = store(&scratch);
            let empty = {
                let mut files = lock(&s.files);
                let rel = files.open_in(day).expect("open").rel.clone();
                let (sealed, failed) = files.seal_days(vec![day]);
                assert_eq!((sealed, failed.len()), (1, 0));
                rel
            };
            let recorded = records(&scratch).len();
            let done = s.compact_day(day, &options()).expect("compact");
            assert_eq!(
                done,
                Compacted {
                    day: day_name(day),
                    compacted: true,
                    files_in: 1,
                    bytes_in: done.bytes_in,
                    seconds: done.seconds,
                    removed: vec![empty.clone()],
                    ..Compacted::default()
                }
            );
            let all = records(&scratch);
            assert_eq!(all.len(), recorded + 1);
            match &all[recorded] {
                Record::Replace { add, remove, .. } => {
                    assert!(add.is_empty(), "{add:?}");
                    assert_eq!(remove, &vec![empty]);
                }
                other => panic!("the record is {other:?}"),
            }
            assert!(data_files(&scratch.0).is_empty(), "the empty file was kept");
            assert!(!s.compact_day(day, &options()).expect("compact").compacted);
        }
        let s = store(&scratch);
        assert!(lock(&s.files).live.is_empty());
    }

    /// A cancelled compaction, and one given up after it wrote its files, leave the day as
    /// it was - and hand their numbers back, so the next file takes the first of them.
    #[test]
    fn a_cancelled_compaction_removes_its_files_and_hands_its_numbers_back() {
        let scratch = Scratch::new("compact-cancel");
        let day = day_of(T0);
        let s = store_with(&scratch, small(700));
        let leaf = s.open(7, "ts").expect("leaf");
        fill(&s, &[&leaf], 6, 5, 0);
        s.flush(FlushKind::Final);
        let before = visited(leaf.as_ref());
        let old = data_files(&scratch.0);
        let next = lock(&s.files).next[&day];
        let len = lock(&s.files).manifest.len();

        let cancelled = CompactOptions {
            threads: 2,
            cancel: Arc::new(AtomicBool::new(true)),
        };
        match s.compact_day(day, &cancelled) {
            Err(StoreError::Cancelled { .. }) => {}
            other => panic!("a cancelled compaction returned {other:?}"),
        }
        assert_eq!(data_files(&scratch.0), old);
        assert_eq!(lock(&s.files).next[&day], next, "the numbers were kept");

        let job = s.begin(day).expect("begin").expect("a job");
        assert!(lock(&s.files).next[&day] > next, "nothing was reserved");
        let outputs = s.rebuild(&job, &options()).expect("rebuild");
        let written: Vec<PathBuf> = outputs.files.iter().map(|f| f.path.clone()).collect();
        assert_eq!(
            written[0],
            paths(&scratch, &[rel(day, next)])
                .into_iter()
                .next()
                .expect("one")
        );
        s.abandon(job);
        assert_eq!(data_files(&scratch.0), old, "an output outlived its job");
        assert_eq!(lock(&s.files).next[&day], next, "the numbers were kept");
        assert_eq!(lock(&s.files).manifest.len(), len);
        assert_eq!(visited(leaf.as_ref()), before);

        // The live door's next file for the day takes the number the first output had.
        fill(&s, &[&leaf], 1, 5, 30);
        assert!(lock(&s.files).live.contains_key(&rel(day, next)));
        let done = s.compact_day(day, &options()).expect("compact");
        assert_eq!((done.compacted, done.rows), (true, 35));
    }

    /// An output that cannot be deleted still holds its name, so the numbers are not handed
    /// back: the live door's next file for the day would be created over it, and fail.
    #[test]
    #[cfg(unix)]
    fn an_abandon_that_cannot_delete_an_output_keeps_its_numbers_and_the_live_door_writes_on() {
        use std::os::unix::fs::PermissionsExt;
        let scratch = Scratch::new("compact-undeleted");
        let day = day_of(T0);
        let (before, added) = {
            let s = store_with(&scratch, small(700));
            let leaf = s.open(7, "ts").expect("leaf");
            fill(&s, &[&leaf], 6, 5, 0);
            s.flush(FlushKind::Final);
            let job = s.begin(day).expect("begin").expect("a job");
            let end = lock(&s.files).next[&day];
            let outputs = s.rebuild(&job, &options()).expect("rebuild");
            let written: Vec<PathBuf> = outputs.files.iter().map(|f| f.path.clone()).collect();
            assert!(!written.is_empty());

            let dir = scratch.0.join(DATA_DIR).join(day_name(day));
            fs::set_permissions(&dir, fs::Permissions::from_mode(0o555)).expect("chmod");
            s.abandon(job);
            fs::set_permissions(&dir, fs::Permissions::from_mode(0o755)).expect("chmod");
            if written.iter().all(|p| !p.exists()) {
                // Root, or a filesystem that ignores the mode: nothing here was left.
                return;
            }
            assert!(written.iter().all(|p| p.exists()));
            assert_eq!(
                lock(&s.files).next[&day],
                end,
                "a number a file still holds was handed back"
            );
            assert!(lock(&s.files).compacting.is_none());

            fill(&s, &[&leaf], 1, 5, 30);
            assert!(lock(&s.files).live.contains_key(&rel(day, end)));
            let before = visited(leaf.as_ref());
            assert_eq!(before.len(), 35);
            let done = s.compact_day(day, &options()).expect("compact");
            assert_eq!((done.compacted, done.rows), (true, 35));
            assert_eq!(visited(leaf.as_ref()), before);
            assert!(written.iter().all(|p| p.exists()), "deleted by a later job");
            (before, done.added)
        };
        let s = store_with(&scratch, small(700));
        assert_eq!(data_files(&scratch.0), paths(&scratch, &added));
        assert_eq!(visited(s.open(7, "ts").expect("leaf").as_ref()), before);
    }

    /// A commit that panics takes the day's latch off as it unwinds, whichever side of the
    /// record it fell: the next compaction is not refused as busy for the life of the process.
    #[test]
    fn a_commit_that_panics_leaves_no_day_marked_as_being_compacted() {
        let scratch = Scratch::new("compact-panic");
        let day = day_of(T0);
        let (before, new) = {
            let s = store(&scratch);
            let leaf = s.open(7, "ts").expect("leaf");
            fill(&s, &[&leaf], 3, 5, 0);
            s.flush(FlushKind::Final);
            let before = visited(leaf.as_ref());
            let old = data_files(&scratch.0);
            let job = s.begin(day).expect("begin").expect("a job");
            let mut outputs = s.rebuild(&job, &options()).expect("rebuild");
            let new: BTreeSet<PathBuf> = outputs.files.iter().map(|f| f.path.clone()).collect();
            // A schema the file has not got: the publish indexes its row groups by it.
            outputs.files[0].index.entries[0].schema_id = u32::MAX;
            let panicked = catch_unwind(AssertUnwindSafe(|| s.commit(job, outputs)));
            assert!(panicked.is_err(), "the commit returned");
            assert_eq!(lock(&s.files).compacting, None, "the day is still marked");
            match s.compact_day(day + 1, &options()) {
                Ok(done) => assert!(!done.compacted),
                Err(e) => panic!("a later compaction was refused: {e}"),
            }
            assert!(
                old.iter().chain(&new).all(|p| p.exists()),
                "a set was removed"
            );
            assert_eq!(visited(leaf.as_ref()), before);
            (before, new)
        };
        let s = store(&scratch);
        assert_eq!(data_files(&scratch.0), new);
        assert_eq!(visited(s.open(7, "ts").expect("leaf").as_ref()), before);
    }

    /// A start that refuses a rebuilt file does not delete the unnamed inputs: they are the
    /// only whole copy of the day left.
    #[test]
    fn a_start_refused_for_a_damaged_compacted_file_leaves_the_unnamed_files_alone() {
        let scratch = Scratch::new("compact-damaged");
        let day = day_of(T0);
        let (before, inputs, outputs) = {
            let s = store_with(&scratch, small(700));
            let leaf = s.open(7, "ts").expect("leaf");
            fill(&s, &[&leaf], 6, 5, 0);
            s.flush(FlushKind::Final);
            let before = visited(leaf.as_ref());
            let inputs: Vec<(PathBuf, Vec<u8>)> = data_files(&scratch.0)
                .into_iter()
                .map(|path| {
                    let bytes = fs::read(&path).expect("read");
                    (path, bytes)
                })
                .collect();
            let done = s.compact_day(day, &options()).expect("compact");
            (before, inputs, paths(&scratch, &done.added))
        };
        assert!(inputs.iter().all(|(path, _)| !path.exists()));
        // As a stop just after the record leaves the day: the inputs still on disk.
        for (path, bytes) in &inputs {
            fs::write(path, bytes).expect("write");
        }
        let damaged = outputs.iter().next().expect("an output").clone();
        let whole = fs::read(&damaged).expect("read");
        fs::write(&damaged, &whole[..whole.len() - 1]).expect("cut");

        match DiskStore::open(scratch.0.clone(), small(700)) {
            Err(StoreError::Format(FormatError::Corrupt { path, .. })) => {
                assert_eq!(path, damaged)
            }
            Err(other) => panic!("refused for another reason: {other}"),
            Ok(_) => panic!("opened a day with a damaged file"),
        }
        for (path, bytes) in &inputs {
            assert_eq!(
                fs::read(path).ok().as_ref(),
                Some(bytes),
                "{}: deleted by a start that was refused",
                path.display()
            );
        }

        fs::write(&damaged, &whole).expect("restore");
        let s = store_with(&scratch, small(700));
        assert_eq!(data_files(&scratch.0), outputs);
        assert_eq!(visited(s.open(7, "ts").expect("leaf").as_ref()), before);
    }

    /// A manifest naming both a compacted file and a file it was rebuilt from holds the
    /// parent's rows twice. No compaction leaves one, and a start refuses it by both names.
    #[test]
    fn a_start_refuses_a_compacted_file_named_beside_a_parent_it_replaced() {
        let scratch = Scratch::new("compact-parent");
        let day = day_of(T0);
        let (child, parents) = {
            let s = store(&scratch);
            let leaf = s.open(7, "ts").expect("leaf");
            fill(&s, &[&leaf], 2, 5, 0);
            s.flush(FlushKind::Final);
            let parents = data_files(&scratch.0);
            let job = s.begin(day).expect("begin").expect("a job");
            let outputs = s.rebuild(&job, &options()).expect("rebuild");
            // The outputs added, and the inputs never removed.
            lock(&s.files)
                .record(vec![Record::Add {
                    files: outputs
                        .files
                        .iter()
                        .map(|f| FileRecord::sealed(f.rel.clone(), &f.index))
                        .collect(),
                    source: None,
                }])
                .expect("record");
            (outputs.files[0].path.clone(), parents)
        };
        match DiskStore::open(scratch.0.clone(), eager()) {
            Err(StoreError::ReplacedLive { child: c, parent }) => {
                assert_eq!(c, child);
                assert!(parents.contains(&parent), "{}", parent.display());
            }
            Err(other) => panic!("refused for another reason: {other}"),
            Ok(_) => panic!("opened a day stored twice"),
        }
        assert!(child.exists() && parents.iter().all(|p| p.exists()));
    }

    /// One day at a time: a second compaction is refused while the first is between its
    /// start and its commit, and not after.
    #[test]
    fn a_second_compaction_is_refused_while_one_is_between_begin_and_commit() {
        let scratch = Scratch::new("compact-busy");
        let day = day_of(T0);
        let s = store(&scratch);
        let leaf = s.open(7, "ts").expect("leaf");
        fill(&s, &[&leaf], 3, 5, 0);
        let job = s.begin(day).expect("begin").expect("a job");
        for other in [day, day + 1] {
            match s.compact_day(other, &options()) {
                Err(StoreError::CompactBusy { busy, .. }) => assert_eq!(busy, day_name(day)),
                got => panic!("a second compaction returned {got:?}"),
            }
        }
        let outputs = s.rebuild(&job, &options()).expect("rebuild");
        assert!(s.commit(job, outputs).expect("commit").compacted);
        assert!(!s.compact_day(day, &options()).expect("compact").compacted);
        assert_eq!(ns(leaf.as_ref()), (0..15).collect::<Vec<_>>());
    }

    /// A record that failed and could not be cut back may be on disk whole: both sets of files
    /// stay, and nothing is compacted until that is settled - here by a start.
    #[test]
    fn a_record_that_may_be_on_disk_keeps_both_sets_and_stops_compaction() {
        let scratch = Scratch::new("compact-uncertain");
        let day = day_of(T0);
        let (before, old) = {
            let s = store_with(&scratch, small(700));
            let leaf = s.open(7, "ts").expect("leaf");
            fill(&s, &[&leaf], 6, 5, 0);
            s.flush(FlushKind::Final);
            let before = visited(leaf.as_ref());
            let old = data_files(&scratch.0);
            let job = s.begin(day).expect("begin").expect("a job");
            let outputs = s.rebuild(&job, &options()).expect("rebuild");
            let new: Vec<PathBuf> = outputs.files.iter().map(|f| f.path.clone()).collect();
            lock(&s.files).manifest =
                Manifest::read_only(&scratch.0.join("dead.log")).expect("read-only");
            match s.commit(job, outputs) {
                Err(StoreError::CompactUncertain { .. }) => {}
                other => panic!("an uncertain commit returned {other:?}"),
            }
            assert!(
                old.iter().chain(&new).all(|p| p.exists()),
                "a set was removed"
            );
            assert_eq!(visited(leaf.as_ref()), before);
            match s.compact_day(day, &options()) {
                Err(StoreError::CompactRefused { path, .. }) => {
                    assert_eq!(PathBuf::from(path), new[0])
                }
                other => panic!("compacted beside an unsettled commit: {other:?}"),
            }
            assert!(s.due_days(today() + 1).is_empty());
            (before, old)
        };
        let s = store_with(&scratch, small(700));
        assert_eq!(visited(s.open(7, "ts").expect("leaf").as_ref()), before);
        assert_eq!(data_files(&scratch.0), old);
    }

    /// The next record the manifest takes is written where the uncertain one would have been:
    /// from then on that one is not on disk, its files go, and the day can be compacted again.
    #[test]
    fn the_next_record_settles_an_uncertain_commit_and_removes_its_files() {
        let scratch = Scratch::new("compact-settled");
        let day = day_of(T0);
        let s = store(&scratch);
        let leaf = s.open(7, "ts").expect("leaf");
        fill(&s, &[&leaf], 2, 5, 0);
        s.flush(FlushKind::Final);
        let job = s.begin(day).expect("begin").expect("a job");
        let outputs = s.rebuild(&job, &options()).expect("rebuild");
        let new: Vec<PathBuf> = outputs.files.iter().map(|f| f.path.clone()).collect();
        // As `commit` leaves a record it cannot vouch for: the files kept, the day free.
        {
            let mut files = lock(&s.files);
            files.uncertain.extend(new.iter().cloned());
            files.compacting = None;
        }
        drop(job);
        match s.compact_day(day, &options()) {
            Err(StoreError::CompactRefused { .. }) => {}
            other => panic!("compacted beside an unsettled commit: {other:?}"),
        }
        assert!(new.iter().all(|p| p.exists()));

        // The live door names a file for the day: a record, and a durable one.
        fill(&s, &[&leaf], 1, 5, 10);
        assert!(
            new.iter().all(|p| !p.exists()),
            "an unnamed output was kept"
        );
        let done = s.compact_day(day, &options()).expect("compact");
        assert_eq!((done.compacted, done.rows), (true, 15));
        assert!(paths(&scratch, &done.added)
            .iter()
            .all(|p| !new.contains(p)));
        assert_eq!(ns(leaf.as_ref()), (0..15).collect::<Vec<_>>());
    }

    /// A volume without room for the day a second time is refused before a file is made
    /// or a number reserved.
    #[test]
    #[cfg(unix)]
    fn a_day_the_volume_has_no_room_to_rebuild_is_refused_and_nothing_is_reserved() {
        let scratch = Scratch::new("compact-space");
        let day = day_of(T0);
        let s = store_with(
            &scratch,
            DiskOptions {
                max_buffer_bytes: u64::MAX / 2,
                ..eager()
            },
        );
        let leaf = s.open(7, "ts").expect("leaf");
        fill(&s, &[&leaf], 2, 5, 0);
        s.flush(FlushKind::Final);
        let old = data_files(&scratch.0);
        let next = lock(&s.files).next[&day];
        for _ in 0..2 {
            match s.compact_day(day, &options()) {
                Err(StoreError::NoSpace { free, needed, .. }) => assert!(free < needed),
                other => panic!("compacted with no room: {other:?}"),
            }
        }
        assert_eq!(data_files(&scratch.0), old);
        assert_eq!(lock(&s.files).next[&day], next);
    }

    /// An unfinished file for template `id` holding one row group for each of `groups` -
    /// a stream, and the rows numbered `ns` of it.
    fn streamed(dir: &Path, id: u64, groups: &[(u64, std::ops::Range<i64>)]) -> StagedFile {
        let path = dir.join("streams.col");
        let mut writer = Writer::create(&path).expect("create");
        for (stream, ns) in groups {
            let rows: Vec<Row> = ns.clone().map(|n| row(T0 + n, n)).collect();
            let refs: Vec<&Row> = rows.iter().collect();
            let times: Vec<i64> = ns.clone().map(|n| T0 + n).collect();
            let columns = schema_of(refs.iter().copied());
            let source = Source::Load {
                stream: *stream,
                first: ns.start as u64 + 1,
                last: ns.end as u64,
            };
            let encoded =
                build::encode(id, &columns, &refs, &times, source, Level::Live).expect("encode");
            let schema_id = writer.schema_id(&columns).expect("schema");
            writer
                .append(id, &encoded.frame(schema_id))
                .expect("append");
        }
        StagedFile {
            day: day_of(T0),
            path,
            index: writer.seal().expect("seal"),
        }
    }

    /// Row groups are merged within one stream and never across two, and a merged row
    /// group's range is the smallest that holds the ranges of those it was made from.
    #[test]
    fn row_groups_of_one_stream_merge_and_their_range_is_the_smallest_that_holds_them() {
        let scratch = Scratch::new("compact-streams");
        let s = store(&scratch);
        let leaf = s.open(7, "ts").expect("leaf");
        let dir = s.stage(9).expect("stage");
        s.commit_load(LoadCommit {
            load_id: 9,
            files: vec![streamed(&dir, 7, &[(9, 0..3), (9, 3..6), (10, 6..8)])],
            replace_day: None,
        })
        .expect("commit");
        let before = visited(leaf.as_ref());
        let done = s.compact_day(day_of(T0), &options()).expect("compact");
        assert_eq!((done.row_groups_in, done.row_groups_out), (3, 2));
        assert_eq!(visited(leaf.as_ref()), before);

        let path = paths(&scratch, &done.added)
            .into_iter()
            .next()
            .expect("one");
        let index = file::open_sealed(&path).expect("readable").expect("sealed");
        let mut reader = Reader::open(&path).expect("open");
        let sources: Vec<Source> = index
            .entries
            .iter()
            .map(|e| {
                let columns = index.schemas[e.schema_id as usize].len();
                reader.header(e, columns).expect("header").source
            })
            .collect();
        assert_eq!(
            sources,
            vec![
                Source::Load {
                    stream: 9,
                    first: 1,
                    last: 6
                },
                Source::Load {
                    stream: 10,
                    first: 7,
                    last: 8
                },
            ]
        );
    }
}
