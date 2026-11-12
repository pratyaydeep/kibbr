// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! What a query's caller tells a scan when the answer is no longer wanted: `Stop`, set by
//! whoever asked and looked at by the scan between two pieces of work.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::engine::store::StoreError;

/// What whoever asked for a scan tells it when the answer is no longer wanted; a scan asks
/// between pieces of work and nobody interrupts it.
#[derive(Default, Clone)]
pub struct Stop(Option<Arc<Told>>);

struct Told {
    set: AtomicBool,
    /// Whether a scan has found it set: `seen` is called by the first that does.
    found: AtomicBool,
    seen: Option<Box<dyn Fn() + Send + Sync>>,
}

impl std::fmt::Debug for Stop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            None => f.write_str("Stop(never)"),
            Some(_) => write!(f, "Stop({})", self.is_set()),
        }
    }
}

/// How many rows the memory store reads between two looks at its `Stop`.
pub(crate) const STOP_EVERY_ROWS: usize = 4096;

impl Stop {
    /// A stop that can be set.
    #[cfg(test)]
    pub fn new() -> Self {
        Self(Some(Arc::new(Told {
            set: AtomicBool::new(false),
            found: AtomicBool::new(false),
            seen: None,
        })))
    }

    /// A stop that can be set, and calls `seen` when a scan first finds it so - on the
    /// scan's thread, between two row groups: it should be quick.
    pub fn watched(seen: impl Fn() + Send + Sync + 'static) -> Self {
        Self(Some(Arc::new(Told {
            set: AtomicBool::new(false),
            found: AtomicBool::new(false),
            seen: Some(Box::new(seen)),
        })))
    }

    pub fn set(&self) {
        if let Some(told) = &self.0 {
            told.set.store(true, Ordering::Relaxed);
        }
    }

    pub fn is_set(&self) -> bool {
        self.0
            .as_ref()
            .is_some_and(|told| told.set.load(Ordering::Relaxed))
    }

    /// `Err(StoreError::Stopped)` once set: what a scan asks between two pieces of work.
    pub fn check(&self) -> Result<(), StoreError> {
        let Some(told) = self
            .0
            .as_ref()
            .filter(|told| told.set.load(Ordering::Relaxed))
        else {
            return Ok(());
        };
        if let Some(seen) = &told.seen {
            if !told.found.swap(true, Ordering::Relaxed) {
                seen();
            }
        }
        Err(StoreError::Stopped)
    }
}
