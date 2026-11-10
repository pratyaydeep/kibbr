// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

/// A store whose leaves will not open while `shut` is set: what the disk store never
/// does today, and what the log's applier has to survive if a store ever does.
struct Refusing(Arc<AtomicBool>);

impl crate::rows::Store for Refusing {
    fn open(
        &self,
        template_id: u64,
        time_column: &str,
    ) -> Result<Arc<dyn crate::rows::LeafStore>, StoreError> {
        if self.0.load(Ordering::Relaxed) {
            return Err(StoreError::Io {
                template_id,
                source: std::io::Error::other("the leaf will not open"),
            });
        }
        crate::rows::MemoryStore::new().open(template_id, time_column)
    }

    fn kind(&self) -> &'static str {
        "refusing"
    }

    fn base_path(&self) -> Option<&Path> {
        None
    }
}

/// A row the store refuses after the log has its line is held, and so is every later row
/// of its template: a row group cut around it would say its line was stored.
#[test]
fn a_row_the_store_refuses_is_held_and_its_template_waits_with_it() {
    let shut = Arc::new(AtomicBool::new(true));
    let stores = StoreManager::new(Box::new(Refusing(Arc::clone(&shut))));
    let held = Held::new();
    let placed = |template_id| ingest::Placed {
        template_id,
        time_column: "ts".to_string(),
        row: crate::rows::Row::new(),
    };
    assert_eq!(held.from(), None);

    assert!(!held.apply(&stores, 40, placed(7)));
    assert_eq!((held.from(), held.rows()), (Some(40), 1));

    shut.store(false, Ordering::Relaxed);
    assert!(
        !held.apply(&stores, 41, placed(7)),
        "the template was let back in"
    );
    assert_eq!(stores.rows(), 0);
    assert!(held.apply(&stores, 42, placed(8)));
    assert_eq!(stores.rows(), 1);
    assert_eq!((held.from(), held.rows()), (Some(40), 2));
}
