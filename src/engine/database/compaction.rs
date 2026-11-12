// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! What a server keeps about the compactions it runs, and the run itself.
//!
//! The rebuild is the store's (`store::disk::rebuild`); here is what is around it: which
//! days a run takes, what it reports, and the jobs, counters and failures a server keeps.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;
use std::sync::Mutex;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::engine::display::commas;
use crate::engine::store::disk::{self, CompactOptions, Compacted, DiskStore, Due};
use crate::engine::store::StoreError;

// ── What a run did ─────────────────────────────────────────────────────────────────

/// A run's days, as `--json` prints them and `GET /compact/<job>` returns them: one entry
/// for each day it finished with, compacted or found with nothing to compact.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Report {
    /// False when a day of the run failed; `days` then holds the ones that did not.
    pub ok: bool,
    #[serde(default)]
    pub days: Vec<Compacted>,
}

/// One day, as a line for a terminal.
pub fn line(day: &Compacted) -> String {
    if !day.compacted {
        return format!("{}: nothing to compact", day.day);
    }
    format!(
        "compacted {}: {} rows, {} row groups in {} file(s) ({} bytes) -> {} row groups in {} \
         file(s) ({} bytes), in {:.1}s",
        day.day,
        commas(day.rows),
        commas(day.row_groups_in),
        commas(day.files_in as u64),
        commas(day.bytes_in),
        commas(day.row_groups_out),
        commas(day.files_out as u64),
        commas(day.bytes_out),
        day.seconds
    )
}

/// What a compaction asked of a store that keeps its rows in memory is refused with: by a
/// server, and by the command running alone.
pub const NO_DISK: &str = "this store keeps its rows in memory; a compaction rewrites files, \
                           and needs a server started with a data directory";

// ── A run ──────────────────────────────────────────────────────────────────────────

/// A day a run will try, and - for one taken because it was due - its inputs as the store
/// hashed them, which is what a failure is remembered against.
pub type Planned = (i64, Option<u64>);

/// The days a run takes: the one asked for, or every day that is due, oldest first; the
/// listing holds the flush's lock, so this is never called from the runtime.
pub fn plan(store: &DiskStore, day: Option<i64>) -> Vec<Planned> {
    match day {
        Some(day) => vec![(day, None)],
        None => store
            .due_days(disk::today())
            .into_iter()
            .map(|Due { day, inputs }| (day, Some(inputs)))
            .collect(),
    }
}

/// What a run finished with: the days it is done with, and why the others failed, in the
/// order they were tried.
pub struct Ran {
    pub report: Report,
    pub failed: Vec<StoreError>,
}

/// Compact each planned day in turn, telling `each` how every one went; a failed day does
/// not stop the run, a cancel does, and a day a load replaced has nothing to compact.
pub fn run(
    store: &DiskStore,
    days: &[Planned],
    opts: &CompactOptions,
    each: &mut dyn FnMut(Planned, &Result<Compacted, StoreError>),
) -> Ran {
    let mut report = Report::default();
    let mut failed = Vec::new();
    for planned in days {
        let result = if opts.cancel.load(Ordering::Relaxed) {
            Err(StoreError::Cancelled {
                day: disk::day_name(planned.0),
            })
        } else {
            store.compact_day(planned.0, opts)
        };
        each(*planned, &result);
        match result {
            Ok(done) => report.days.push(done),
            Err(StoreError::InputsChanged { .. }) => report.days.push(Compacted {
                day: disk::day_name(planned.0),
                ..Compacted::default()
            }),
            Err(e @ StoreError::Cancelled { .. }) => {
                failed.push(e);
                break;
            }
            Err(e) => failed.push(e),
        }
    }
    report.ok = failed.is_empty();
    Ran { report, failed }
}

// ── Inside a server ────────────────────────────────────────────────────────────────

/// Finished jobs kept for `GET /compact/<job>`. A client polls until its job is done, so
/// only the recent ones are ever asked about.
const JOBS_KEPT: usize = 64;

/// The longest the scheduler leaves a failing day alone.
const LONGEST_WAIT: Duration = Duration::from_secs(24 * 60 * 60);

/// How a compaction job stands.
#[derive(Debug, Clone, PartialEq)]
pub enum State {
    Running,
    Done(Report),
    /// Why, and the days it had finished before it failed.
    Failed(String, Report),
}

/// A day that failed, as the scheduler remembers it.
struct Failure {
    /// The inputs it failed on. `None` for a day compacted on demand, which is never the
    /// same as a hash: the scheduler's first look at the day tries it.
    inputs: Option<u64>,
    /// In a row, on these inputs.
    times: u32,
    at: Instant,
    error: String,
}

/// The compaction jobs a server has run, the one running, and what `/stats` says of them,
/// behind one mutex never held across a compaction or with the store's flush lock.
#[derive(Default)]
pub struct Jobs {
    next: u64,
    jobs: BTreeMap<u64, State>,
    /// The threads of the last requested job and of the scheduler's last look: two, because
    /// the scheduler's exists before it knows there is a job to run.
    pub asked: Option<JoinHandle<()>>,
    pub scheduled: Option<JoinHandle<()>>,
    /// Set when shutdown begins: no job starts after it.
    stopping: bool,
    failed: BTreeMap<i64, Failure>,
    /// Jobs that tried at least one day.
    runs: u64,
    /// Days rebuilt. A day found with nothing to compact is not one.
    days_compacted: u64,
    last: Option<Compacted>,
}

impl Jobs {
    /// The job that is running, if one is.
    pub fn running(&self) -> Option<u64> {
        self.jobs
            .iter()
            .find(|(_, state)| **state == State::Running)
            .map(|(id, _)| *id)
    }

    pub fn is_stopping(&self) -> bool {
        self.stopping
    }

    /// Shutdown has begun: refuse every job from here on.
    pub fn stop(&mut self) {
        self.stopping = true;
    }

    /// A new running job's number, or `None` while one is running or once shutdown began,
    /// checked and taken in the one hold so two callers cannot both be given a job.
    pub fn start(&mut self) -> Option<u64> {
        if self.stopping || self.running().is_some() {
            return None;
        }
        self.next += 1;
        self.jobs.insert(self.next, State::Running);
        while self.jobs.len() > JOBS_KEPT {
            let Some(oldest) = self.jobs.keys().next().copied() else {
                break;
            };
            self.jobs.remove(&oldest);
        }
        Some(self.next)
    }

    /// A job whose thread could not be started: it never ran.
    pub fn forget(&mut self, id: u64) {
        self.jobs.remove(&id);
    }

    pub fn state(&self, id: u64) -> Option<State> {
        self.jobs.get(&id).cloned()
    }

    /// How a job ended; `tried` is whether it got as far as trying a day.
    pub fn finish(&mut self, id: u64, state: State, tried: bool) {
        if tried {
            self.runs += 1;
        }
        if let Some(slot) = self.jobs.get_mut(&id) {
            *slot = state;
        }
    }

    /// How one day went: the counters, and the memory of a failure, which a cancel and a
    /// replaced day are not.
    pub fn note(&mut self, planned: Planned, result: &Result<Compacted, StoreError>) {
        let (day, inputs) = planned;
        match result {
            Ok(done) => {
                self.failed.remove(&day);
                if done.compacted {
                    self.days_compacted += 1;
                    self.last = Some(done.clone());
                }
            }
            Err(StoreError::Cancelled { .. }) => {}
            Err(StoreError::InputsChanged { .. }) => {
                self.failed.remove(&day);
            }
            Err(e) => {
                let before = self
                    .failed
                    .get(&day)
                    .filter(|f| f.inputs == inputs)
                    .map_or(0, |f| f.times);
                self.failed.insert(
                    day,
                    Failure {
                        inputs,
                        times: before.saturating_add(1),
                        at: Instant::now(),
                        error: crate::engine::error::chain(e),
                    },
                );
            }
        }
    }

    /// Forget the failures of every day not in `keep`, the days compaction could still take:
    /// a day a load replaced is never tried again, and `last_error` would name it for ever.
    pub fn forget_except(&mut self, keep: &BTreeSet<i64>) {
        self.failed.retain(|day, _| keep.contains(day));
    }

    /// How much longer the scheduler leaves a due day alone: `None` for a day that has not
    /// failed, whose inputs have changed since it did, or whose wait is over.
    pub fn wait(&self, planned: Planned, interval: Duration) -> Option<Duration> {
        let (day, inputs) = planned;
        let failure = self.failed.get(&day).filter(|f| f.inputs == inputs)?;
        let left = backoff(interval, failure.times).checked_sub(failure.at.elapsed())?;
        (!left.is_zero()).then_some(left)
    }

    /// `/stats`'s `storage.compaction`; `last_error` is the latest among the days still
    /// failing, and does not clear because another day went well.
    pub fn stats(&self) -> serde_json::Value {
        let last_error = self
            .failed
            .values()
            .max_by_key(|f| f.at)
            .map(|f| f.error.as_str());
        serde_json::json!({
            "running": self.running().is_some(),
            "runs": self.runs,
            "days_compacted": self.days_compacted,
            "last": self.last,
            "last_error": last_error,
        })
    }
}

/// What a job is said to have failed on when its thread ended before it said anything.
const ENDED_UNSAID: &str = "the compaction thread ended without finishing; a file it wrote \
                            and had not named is removed by the next start";

/// A running job's last word, said whatever ends its thread: dropped unsaid, the job is
/// failed, since one left `running` refuses every later request with a 409.
pub struct Ending<'a> {
    jobs: &'a Mutex<Jobs>,
    id: u64,
    said: bool,
}

impl<'a> Ending<'a> {
    pub fn of(jobs: &'a Mutex<Jobs>, id: u64) -> Self {
        Self {
            jobs,
            id,
            said: false,
        }
    }

    /// How the job ended: `Jobs::finish`, and nothing left for the drop to do.
    pub fn finish(mut self, state: State, tried: bool) {
        let mut jobs = self.jobs.lock().unwrap_or_else(|e| e.into_inner());
        jobs.finish(self.id, state, tried);
        self.said = true;
    }
}

impl Drop for Ending<'_> {
    fn drop(&mut self) {
        if self.said {
            return;
        }
        let mut jobs = self.jobs.lock().unwrap_or_else(|e| e.into_inner());
        if jobs.state(self.id) == Some(State::Running) {
            let state = State::Failed(ENDED_UNSAID.to_string(), Report::default());
            jobs.finish(self.id, state, false);
        }
    }
}

/// How long the scheduler leaves a day alone after it has failed `times` in a row: the
/// interval doubled for each, and never more than a day.
fn backoff(interval: Duration, times: u32) -> Duration {
    let doubled = 1u32.checked_shl(times).unwrap_or(u32::MAX);
    interval.saturating_mul(doubled).min(LONGEST_WAIT)
}

#[cfg(test)]
mod tests;
