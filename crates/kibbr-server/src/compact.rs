//! What a server keeps about the compactions it runs, and the run itself.
//!
//! The rebuild is the store's (`store::disk::compact`): one day's small row groups read
//! back, re-cut and swapped in with one manifest record. What is here is what is around
//! it wherever it runs: which days a run takes and what it reports - and, inside a server,
//! the job a client polls, the counters `/stats` shows and what the scheduler remembers of
//! a day that failed. `kibbr compact`, the command, is the binary's: it runs the same
//! `plan` and `run` alone, or asks the server that holds the directory.
//!
//! **A day, or every day that is due.** A run asked for a day compacts that day now,
//! whatever its age. Without one it takes the days the store says are due - each the day
//! after its last live row - and none being due is a run that did what it was asked.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;
use std::sync::Mutex;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use kibbr_core::fmt::commas;
use kibbr_storage::store::disk::{self, CompactOptions, Compacted, DiskStore, Due};
use kibbr_storage::store::StoreError;

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

/// The days a run takes: the one asked for, or every day that is due today, oldest first.
/// The due listing is one short hold of the flush's lock, so this is called from the thread
/// that will compact and never from the runtime.
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

/// Compact each planned day in turn, telling `each` how every one went.
///
/// **A failed day does not stop the run**; the days after it are still tried. A cancel
/// does, between days as well as inside one - the store looks at the flag only once it is
/// rebuilding, and a run of many small days would otherwise start every one of them.
///
/// **A day a load replaced meanwhile is a day with nothing to compact**, not a failure: its
/// files are the load's, finished already, and the rebuilt ones were removed.
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

/// The compaction jobs a server has run, the one running, and what `/stats` says of them.
///
/// **One mutex, held for moments.** Everything here is behind the server's one lock on
/// this struct, taken to start a job, to note how a day went and to answer `/stats` - never
/// across a compaction, and never together with the store's flush lock, so `/stats` cannot
/// queue behind a flush.
///
/// **A failed day is remembered**, so that a day that cannot be compacted - a corrupt row
/// group, a full disk - is not read and re-encoded up to its fault on every look. The
/// scheduler leaves it for the interval doubled per failure, a day at most, and tries at
/// once when its inputs are no longer the ones that failed.
#[derive(Default)]
pub struct Jobs {
    next: u64,
    jobs: BTreeMap<u64, State>,
    /// The thread of the last job a request started, and of the scheduler's last look.
    /// Two, because the scheduler's thread exists before it knows there is a job to run.
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

    /// A new running job's number, or `None` while one is running or once shutdown began.
    /// Checked and taken in the one hold, so two callers cannot both be given a job.
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

    /// How a job ended. `tried` is whether it got as far as trying a day.
    pub fn finish(&mut self, id: u64, state: State, tried: bool) {
        if tried {
            self.runs += 1;
        }
        if let Some(slot) = self.jobs.get_mut(&id) {
            *slot = state;
        }
    }

    /// How one day went: the counters, and the memory of a failure.
    ///
    /// A cancel is the server stopping and a replaced day is a load's doing; neither says
    /// anything about whether the day can be compacted, so neither is remembered.
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
                        error: kibbr_core::error::chain(e),
                    },
                );
            }
        }
    }

    /// Forget the failures of every day not in `keep`: the days that still hold a file
    /// compaction would take.
    ///
    /// A failure is otherwise dropped only by a later try of its day, and a day that
    /// stopped having anything to compact some other way - a load replaced it, which is
    /// what several refusals say to do - is never tried again: `last_error` would name it
    /// until the server stopped. Not the due days: a day that is not due can still be
    /// failing - one holding a file only a restart seals, or any day while a commit is
    /// unsettled - and has to keep its error and its wait.
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

    /// `/stats`'s `storage.compaction`.
    ///
    /// `last_error` is the latest failure among the days still failing: it clears when
    /// that day is next compacted, or found with nothing to compact - by a try of it, or
    /// by the scheduler's look (`forget_except`) - and not because another day went well
    /// after it.
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

/// A running job's last word, said whatever ends its thread.
///
/// **A job left `running` is never looked at again**: every later request is a 409 naming
/// it, the scheduler stands down on every look, and the `kibbr compact` that asked polls
/// for as long as the server lives. So the thread holds one of these from the moment it
/// has a job, and if it is dropped unsaid - a panic outside the one the thread catches -
/// the job is failed.
///
/// **`finish` is the only lock a thread that ends well takes at its end.** A request that
/// starts the next job joins this thread while it holds the jobs' lock, which it may do
/// the moment this job stops being `running`; a drop that locked again after `finish`
/// would wait on that request for ever, and the request on it.
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
mod tests {
    use super::*;

    fn done(day: &str, compacted: bool) -> Compacted {
        Compacted {
            day: day.to_string(),
            compacted,
            rows: 7,
            ..Compacted::default()
        }
    }

    fn refused(day: &str) -> StoreError {
        StoreError::CompactRefused {
            day: day.to_string(),
            path: format!("{day}/seg_0_000002.col"),
            why: "it was left unsealed".to_string(),
        }
    }

    /// The scripts match this line with a regular expression, so its shape is the surface.
    #[test]
    fn a_compacted_day_is_one_line_with_every_count_grouped_by_thousands() {
        let day = Compacted {
            day: "2024-01-15".into(),
            compacted: true,
            rows: 200_000,
            files_in: 7,
            files_out: 1,
            row_groups_in: 2_240,
            row_groups_out: 219,
            bytes_in: 8_817_294,
            bytes_out: 7_178_772,
            seconds: 3.12,
            added: vec!["2024-01-15/seg_0_000008.col".into()],
            removed: Vec::new(),
        };
        assert_eq!(
            line(&day),
            "compacted 2024-01-15: 200,000 rows, 2,240 row groups in 7 file(s) (8,817,294 \
             bytes) -> 219 row groups in 1 file(s) (7,178,772 bytes), in 3.1s"
        );
        assert_eq!(
            line(&done("2024-01-15", false)),
            "2024-01-15: nothing to compact"
        );
    }

    #[test]
    fn a_report_is_ok_then_days_and_a_day_with_nothing_to_do_is_all_zeros() {
        let report = Report {
            ok: true,
            days: vec![Compacted {
                day: "2024-01-15".into(),
                ..Compacted::default()
            }],
        };
        assert_eq!(
            serde_json::to_string(&report).expect("serialize"),
            r#"{"ok":true,"days":[{"day":"2024-01-15","compacted":false,"rows":0,"files_in":0,"files_out":0,"row_groups_in":0,"row_groups_out":0,"bytes_in":0,"bytes_out":0,"seconds":0.0,"added":[],"removed":[]}]}"#
        );
        assert_eq!(
            serde_json::to_string(&Report {
                ok: true,
                days: Vec::new(),
            })
            .expect("serialize"),
            r#"{"ok":true,"days":[]}"#
        );
    }

    #[test]
    fn a_second_job_is_refused_while_one_runs_and_after_shutdown_began() {
        let mut jobs = Jobs::default();
        let first = jobs.start().expect("the first job");
        assert_eq!(jobs.running(), Some(first));
        assert_eq!(jobs.start(), None, "a second job started beside the first");

        jobs.finish(
            first,
            State::Done(Report {
                ok: true,
                days: Vec::new(),
            }),
            false,
        );
        assert_eq!(jobs.running(), None);
        let second = jobs.start().expect("a job after the first finished");
        assert!(second > first);
        jobs.finish(second, State::Failed("why".into(), Report::default()), true);

        jobs.stop();
        assert_eq!(jobs.start(), None, "a job started after shutdown began");
        assert_eq!(
            jobs.stats()["runs"],
            1,
            "only the job that tried a day is a run"
        );
    }

    #[test]
    fn only_the_last_jobs_are_kept_and_a_running_one_is_among_them() {
        let mut jobs = Jobs::default();
        for _ in 0..JOBS_KEPT + 5 {
            let id = jobs.start().expect("a job");
            jobs.finish(id, State::Done(Report::default()), false);
        }
        let running = jobs.start().expect("a job");
        assert_eq!(jobs.jobs.len(), JOBS_KEPT);
        assert_eq!(jobs.state(1), None, "the oldest job is still kept");
        assert_eq!(jobs.state(running), Some(State::Running));
    }

    #[test]
    fn a_day_with_nothing_to_compact_is_not_counted_and_is_not_the_last() {
        let mut jobs = Jobs::default();
        jobs.note((19_737, Some(1)), &Ok(done("2024-01-15", false)));
        let stats = jobs.stats();
        assert_eq!(stats["days_compacted"], 0);
        assert_eq!(stats["last"], serde_json::Value::Null);
        assert_eq!(stats["last_error"], serde_json::Value::Null);

        jobs.note((19_737, Some(1)), &Ok(done("2024-01-15", true)));
        jobs.note((19_738, Some(2)), &Ok(done("2024-01-16", false)));
        let stats = jobs.stats();
        assert_eq!(stats["days_compacted"], 1);
        assert_eq!(stats["last"]["day"], "2024-01-15");
        assert_eq!(stats["last"]["rows"], 7);
        assert_eq!(stats["running"], false);
    }

    #[test]
    fn a_failed_day_waits_the_interval_doubled_per_failure_until_its_inputs_change() {
        let minute = Duration::from_secs(60);
        assert_eq!(backoff(minute, 1), Duration::from_secs(120));
        assert_eq!(backoff(minute, 3), Duration::from_secs(480));
        assert_eq!(backoff(minute, 30), LONGEST_WAIT);
        assert_eq!(backoff(minute, 400), LONGEST_WAIT);

        let mut jobs = Jobs::default();
        let day = (19_737, Some(11));
        assert_eq!(
            jobs.wait(day, minute),
            None,
            "a day that has not failed waits"
        );
        jobs.note(day, &Err(refused("2024-01-15")));
        let first = jobs
            .wait(day, minute)
            .expect("a failed day is tried at once");
        assert!(first > Duration::from_secs(110) && first <= Duration::from_secs(120));
        jobs.note(day, &Err(refused("2024-01-15")));
        let second = jobs
            .wait(day, minute)
            .expect("a day failed twice is tried at once");
        assert!(second > Duration::from_secs(230), "{second:?}");

        // Other inputs are another question: a file was added, or a restart sealed one.
        assert_eq!(jobs.wait((19_737, Some(12)), minute), None);
        jobs.note((19_737, Some(12)), &Err(refused("2024-01-15")));
        let again = jobs.wait((19_737, Some(12)), minute).expect("it waits");
        assert!(
            again <= Duration::from_secs(120),
            "the count did not start again"
        );
        assert!(jobs.stats()["last_error"]
            .as_str()
            .is_some_and(|e| e.contains("left unsealed")));
    }

    #[test]
    fn a_cancel_and_a_replaced_day_are_not_failures_and_a_success_clears_one() {
        let mut jobs = Jobs::default();
        let day = (19_737, Some(11));
        jobs.note(
            day,
            &Err(StoreError::Cancelled {
                day: "2024-01-15".into(),
            }),
        );
        assert_eq!(jobs.wait(day, Duration::from_secs(60)), None);
        assert_eq!(jobs.stats()["last_error"], serde_json::Value::Null);

        jobs.note(day, &Err(refused("2024-01-15")));
        assert!(jobs.stats()["last_error"].is_string());
        jobs.note(
            day,
            &Err(StoreError::InputsChanged {
                day: "2024-01-15".into(),
                path: "2024-01-15/seg_0_000001.col".into(),
            }),
        );
        assert_eq!(jobs.stats()["last_error"], serde_json::Value::Null);

        jobs.note(day, &Err(refused("2024-01-15")));
        jobs.note(day, &Ok(done("2024-01-15", true)));
        assert_eq!(jobs.wait(day, Duration::from_secs(60)), None);
        assert_eq!(jobs.stats()["last_error"], serde_json::Value::Null);
    }

    /// A day that failed and then stopped having anything to compact - a load replaced
    /// it - is never tried again, so nothing but this would drop its error.
    #[test]
    fn a_failed_day_with_nothing_left_to_compact_is_forgotten() {
        let mut jobs = Jobs::default();
        let (day, other) = ((19_737, Some(11)), (19_738, Some(12)));
        jobs.note(day, &Err(refused("2024-01-15")));
        jobs.note(other, &Err(refused("2024-01-16")));

        jobs.forget_except(&BTreeSet::from([day.0, other.0]));
        assert!(
            jobs.wait(day, Duration::from_secs(60)).is_some(),
            "a day that can still be compacted lost its wait"
        );
        assert!(jobs.stats()["last_error"]
            .as_str()
            .is_some_and(|e| e.contains("2024-01-16")));

        jobs.forget_except(&BTreeSet::from([day.0]));
        assert!(
            jobs.stats()["last_error"]
                .as_str()
                .is_some_and(|e| e.contains("2024-01-15")),
            "the error is not the one day still failing: {}",
            jobs.stats()
        );
        assert!(jobs.wait(day, Duration::from_secs(60)).is_some());
        assert_eq!(jobs.wait(other, Duration::from_secs(60)), None);

        jobs.forget_except(&BTreeSet::new());
        assert_eq!(jobs.stats()["last_error"], serde_json::Value::Null);
        assert_eq!(jobs.wait(day, Duration::from_secs(60)), None);
    }

    /// A panic the thread does not catch - its own log line, with stderr gone - must not
    /// leave the job `running`: the next request would be a 409 for as long as the server
    /// lived, and the client that asked would poll as long.
    #[test]
    fn a_job_whose_thread_unwinds_is_failed_and_not_running() {
        let jobs = Mutex::new(Jobs::default());
        let start = || jobs.lock().expect("the lock").start().expect("a job");
        let state = |id| jobs.lock().expect("the lock").state(id);

        let first = start();
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ending = Ending::of(&jobs, first);
            panic!("the thread died outside the panic it catches");
        }));
        assert!(unwound.is_err());
        match state(first) {
            Some(State::Failed(why, report)) => {
                assert!(why.contains("ended without finishing"), "{why}");
                assert_eq!(report, Report::default());
            }
            other => panic!("a job whose thread unwound is {other:?}"),
        }
        assert_eq!(jobs.lock().expect("the lock").running(), None);
        assert_eq!(jobs.lock().expect("the lock").stats()["runs"], 0);

        // A job that said how it ended keeps what it said, and the next one starts.
        let second = start();
        let report = Report {
            ok: true,
            days: vec![done("2024-01-15", true)],
        };
        Ending::of(&jobs, second).finish(State::Done(report.clone()), true);
        assert_eq!(state(second), Some(State::Done(report)));
        assert_eq!(jobs.lock().expect("the lock").stats()["runs"], 1);
        assert!(start() > second);
    }
}
