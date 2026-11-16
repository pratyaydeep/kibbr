// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The threads a disk scan chooses a day's rows on beside the thread that asked.
//!
//! A scan reads and hands its rows over on its own thread; only choosing and decoding is
//! shared out. `Turn::each` blocks its caller, so it runs on a thread that may block.
//! The only file that names rayon: a pool of its own, never the global one or `par_iter`.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use super::lock;

/// The scan threads of one disk store, and what says whether a scan may use them now.
pub struct Fan {
    /// The pool's threads: one fewer than a scan uses, the thread that asked being one.
    helpers: usize,
    /// A run of row groups holding fewer rows than this, by the index, is chosen in turn.
    least_rows: u64,
    /// Started by the first scan that takes a turn. `None` inside: it could not start.
    pool: OnceLock<Option<rayon::ThreadPool>>,
    /// One scan holds the pool.
    taken: AtomicBool,
    /// Ingest bodies in their task.
    bodies: AtomicUsize,
    /// Runs of row groups shared out so far, for `/stats` and for tests.
    fanned: AtomicU64,
    /// The threads `each` shared work out from, for the test that holds a scan to a thread
    /// that may block.
    #[cfg(any(test, feature = "test-support"))]
    entered: Mutex<Vec<std::thread::ThreadId>>,
}

/// The pool, held by one scan for one run of row groups and given back when this goes.
pub(crate) struct Turn<'f>(&'f Fan);

impl Drop for Turn<'_> {
    fn drop(&mut self) {
        self.0.taken.store(false, Ordering::Release);
    }
}

/// An ingest body in its task; while one is held no scan is given the pool, so the
/// ingest path keeps its cores.
pub struct Body<'f>(&'f Fan);

impl Drop for Body<'_> {
    fn drop(&mut self) {
        self.0.bodies.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Fan {
    /// The threads one scan may choose rows on, the asking thread among them; `None` under
    /// two, and no thread is started.
    pub fn new(threads: usize, least_rows: u64) -> Option<Arc<Self>> {
        let helpers = threads.checked_sub(1).filter(|n| *n > 0)?;
        Some(Arc::new(Self {
            helpers,
            least_rows,
            pool: OnceLock::new(),
            taken: AtomicBool::new(false),
            bodies: AtomicUsize::new(0),
            fanned: AtomicU64::new(0),
            #[cfg(any(test, feature = "test-support"))]
            entered: Mutex::new(Vec::new()),
        }))
    }

    pub fn least_rows(&self) -> u64 {
        self.least_rows
    }

    /// Runs of row groups shared out so far.
    pub fn fanned(&self) -> u64 {
        self.fanned.load(Ordering::Relaxed)
    }

    /// Say that an ingest body is in its task, until what is returned is dropped.
    pub fn body(&self) -> Body<'_> {
        self.bodies.fetch_add(1, Ordering::Relaxed);
        Body(self)
    }

    /// The pool for one run of row groups, or `None` without waiting when an ingest body is
    /// in its task, another scan holds it, or it could not start.
    pub(crate) fn turn(&self) -> Option<Turn<'_>> {
        if self.bodies.load(Ordering::Relaxed) != 0 {
            return None;
        }
        // Not a `Mutex`: a panic in a scan would poison it, and every scan after would
        // choose in turn for the life of the process.
        self.taken
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .ok()?;
        // Made before the pool is looked for, so that it is given back on every way out.
        let turn = Turn(self);
        self.pool().is_some().then_some(turn)
    }

    /// The pool, started the first time it is asked for; one that will not start is said
    /// once and is never a failed query.
    fn pool(&self) -> Option<&rayon::ThreadPool> {
        self.pool
            .get_or_init(|| {
                rayon::ThreadPoolBuilder::new()
                    .num_threads(self.helpers)
                    .thread_name(|n| format!("kibbr-scan-{n}"))
                    .build()
                    .map_err(|e| {
                        tracing::warn!(
                            "the scan pool could not start its {} thread(s): {e}; \
                             every query chooses its rows on its own thread",
                            self.helpers
                        );
                    })
                    .ok()
            })
            .as_ref()
    }

    /// The threads `each` shared work out from so far.
    #[cfg(any(test, feature = "test-support"))]
    pub fn entered(&self) -> Vec<std::thread::ThreadId> {
        lock(&self.entered).clone()
    }

    /// Whether the pool has been asked for: a test's way of knowing no thread was started.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn started(&self) -> bool {
        self.pool.get().is_some()
    }
}

impl Turn<'_> {
    /// `work` on every item, each once, on this thread and the pool's; blocks the calling
    /// thread until all are done.
    pub(crate) fn each<T: Send>(&self, items: &[Mutex<T>], work: impl Fn(&mut T) + Sync) {
        let next = AtomicUsize::new(0);
        let claim = || {
            while let Some(item) = items.get(next.fetch_add(1, Ordering::Relaxed)) {
                work(&mut lock(item));
            }
        };
        let Some(pool) = self.0.pool() else {
            return claim();
        };
        #[cfg(any(test, feature = "test-support"))]
        lock(&self.0.entered).push(std::thread::current().id());
        self.0.fanned.fetch_add(1, Ordering::Relaxed);
        let helpers = self.0.helpers.min(items.len().saturating_sub(1));
        pool.in_place_scope(|scope| {
            for _ in 0..helpers {
                scope.spawn(|_| claim());
            }
            claim();
        });
    }
}

#[cfg(test)]
mod tests;
