// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::templates::trie::ColumnDef;
use crate::model::types::TimeSource;

fn template(columns: &[(&str, Option<ColumnType>)]) -> Arc<Template> {
    let at = chrono::DateTime::from_timestamp(1_705_314_600, 0).expect("a valid timestamp");
    let meta = LeafMeta {
        template_id: 0,
        columns: columns
            .iter()
            .map(|(name, col_type)| ColumnDef {
                name: name.to_string(),
                col_type: *col_type,
                widened_at: None,
            })
            .collect(),
        storage_path: "data/leaf".to_string(),
        first_seen: at,
        last_seen: at,
        time_source: TimeSource::Arrival,
    };
    let path = columns.iter().map(|(name, _)| name.to_string()).collect();
    Arc::new(Template::of(path, &meta))
}

fn ids(list: &[Posting]) -> Vec<u64> {
    list.iter().map(|(id, _)| *id).collect()
}

/// A list is in id order whatever order its templates were put in: a plan reads it
/// from front to back and gives its leaves in that order.
#[test]
fn ids_put_out_of_order_end_in_order() {
    let mut index = KeyIndex::default();
    for id in [105, 101, 109, 103, 102] {
        index.put(id, ["a", "b"], template(&[("a", None), ("b", None)]));
    }
    index.put(104, ["b"], template(&[("b", None)]));
    assert_eq!(ids(index.all()), [101, 102, 103, 104, 105, 109]);
    assert_eq!(
        ids(index.of("a").expect("a list")),
        [101, 102, 103, 105, 109]
    );
    assert_eq!(
        ids(index.of("b").expect("a list")),
        [101, 102, 103, 104, 105, 109]
    );
    assert_eq!(index.edits(), 6);
}

/// A template put a second time is the same entry with a new handle, in every list it
/// was in, and the key it gained has a list.
#[test]
fn a_second_put_replaces_the_handle_everywhere_and_adds_the_new_key() {
    let mut index = KeyIndex::default();
    let int = Some(ColumnType::Int64);
    index.put(101, ["a", "b"], template(&[("a", int), ("b", int)]));
    index.put(102, ["a"], template(&[("a", int)]));
    assert!(index.of("c").is_none());

    let text = Some(ColumnType::String);
    let wider = template(&[("a", text), ("b", int), ("c", int)]);
    index.put(101, ["a", "b", "c"], Arc::clone(&wider));

    assert_eq!(ids(index.all()), [101, 102]);
    assert_eq!(ids(index.of("a").expect("a list")), [101, 102]);
    assert_eq!(ids(index.of("b").expect("a list")), [101]);
    assert_eq!(ids(index.of("c").expect("a list")), [101]);
    for list in [
        index.all(),
        index.of("a").expect("a list"),
        index.of("b").expect("a list"),
        index.of("c").expect("a list"),
    ] {
        assert!(Arc::ptr_eq(&list[0].1, &wider), "an older handle is held");
        assert_eq!(list[0].1.text_columns, ["a"]);
    }
    assert_eq!(index.edits(), 3);
}

/// A key no template has is told apart from a key with templates: there is no list,
/// and never an empty one.
#[test]
fn a_key_never_put_has_no_list() {
    let mut index = KeyIndex::default();
    assert!(index.of("a").is_none());
    assert!(index.all().is_empty());
    index.put(101, [], template(&[]));
    assert!(index.of("a").is_none());
    assert!(index.of("").is_none());
    assert_eq!(ids(index.all()), [101]);
}
