// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

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
