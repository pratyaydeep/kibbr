// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

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

/// A panic the thread does not catch must not leave the job `running`: the next request
/// would be a 409 for as long as the server lived.
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
