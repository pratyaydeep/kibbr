// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! What a plan reads of the trie without crossing it: one finished description of each
//! template, and for each key the templates that have it as a column.
//!
//! Kept as templates are made, so a plan takes no node's lock and copies no label to learn
//! which templates hold a key.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use super::LeafMeta;
use crate::engine::model::tokens::promoted_of;
use crate::engine::model::types::ColumnType;

/// One template as a plan reads it, made again whole when a column widens or is added and
/// never changed in place, so a plan holding an `Arc` reads it as it was.
#[derive(Debug, PartialEq)]
pub struct Template {
    /// The template's key set as walk tokens, root excluded: its content-derived identity,
    /// which the result order's tiebreak needs and the reference scanner can recompute.
    pub path: Vec<String>,
    /// Carried by a plan and opened by nothing: a store finds a leaf's directory from its
    /// own base and the template's id, and this is the path the leaf was first given.
    pub storage_path: String,
    /// The template's time column: what its rows are ordered, windowed and bucketed by,
    /// falling back to the system column on a row where it holds no instant.
    pub time_column: String,
    /// Columns whose type is now `String`, in the leaf's column order; a `Datetime` stored
    /// in one is rendered back to its ingested text before the row is read.
    pub text_columns: Vec<String>,
    /// Columns whose type is now `Int64`, in the leaf's column order; a `Datetime` stored in
    /// one as epoch digits is shown as the integer it arrived as.
    pub int_columns: Vec<String>,
    /// `(column, value)` for each column the path settles, in key order; empty or one pair
    /// for most templates, so it is searched and not hashed.
    promoted: Vec<(String, String)>,
}

impl Template {
    /// The one function from a leaf to what a plan reads of it, so the plan that reads the
    /// index and the plan that walks the nodes describe a template the same way.
    pub fn of(path: Vec<String>, meta: &LeafMeta) -> Self {
        let columns: BTreeSet<&str> = meta.columns.iter().map(|c| c.name.as_str()).collect();
        let promoted = promoted_of(&path, &columns)
            .into_iter()
            .map(|(key, value)| (key.into_owned(), value.to_string()))
            .collect();
        Self {
            storage_path: meta.storage_path.clone(),
            time_column: meta.time_source.column().to_string(),
            text_columns: meta
                .columns
                .iter()
                .filter(|c| c.col_type == Some(ColumnType::String))
                .map(|c| c.name.clone())
                .collect(),
            int_columns: meta
                .columns
                .iter()
                .filter(|c| c.col_type == Some(ColumnType::Int64))
                .map(|c| c.name.clone())
                .collect(),
            promoted,
            path,
        }
    }

    /// The value this template's path holds for `key`, if the path settles it: every row
    /// of the template then has that value, and a term on the key needs no scan.
    pub fn promoted(&self, key: &str) -> Option<&str> {
        self.promoted
            .iter()
            .find(|(have, _)| have == key)
            .map(|(_, value)| value.as_str())
    }
}

/// A template in a list: its id, held beside the handle so that lists are merged and
/// searched without following it.
pub(crate) type Posting = (u64, Arc<Template>);

/// A template as a walk of the nodes finds it: its id, its column names and what
/// `Template::of` makes of it. What a held index is compared with.
pub(super) type Walked = (u64, Vec<String>, Template);

/// For each key, the templates that have it as a column, and every template; each list
/// is in ascending template id, the order a plan gives its leaves in, so a plan sorts nothing.
#[derive(Default)]
pub struct KeyIndex {
    all: Vec<Posting>,
    by_key: HashMap<String, Vec<Posting>>,
    /// Counts `put`s. Read by the check a debug build makes of every plan and by the
    /// tests, to know that no template was made or changed between two looks.
    edits: u64,
}

impl KeyIndex {
    /// Every template, in id order.
    pub fn all(&self) -> &[Posting] {
        &self.all
    }

    /// The templates that have `key` as a column: `None` when no template has it, never an
    /// empty list.
    pub fn of(&self, key: &str) -> Option<&[Posting]> {
        self.by_key.get(key).map(Vec::as_slice)
    }

    pub fn edits(&self) -> u64 {
        self.edits
    }

    /// Put template `id` under each of `keys` and among all templates: inserted at its
    /// place by id where it is new, the handle replaced where it is there.
    pub(super) fn put<'a>(
        &mut self,
        id: u64,
        keys: impl IntoIterator<Item = &'a str>,
        template: Arc<Template>,
    ) {
        // Wrapping, so that nothing in a write hold of the index's lock can panic.
        self.edits = self.edits.wrapping_add(1);
        for key in keys {
            match self.by_key.get_mut(key) {
                Some(list) => place(list, id, &template),
                None => {
                    self.by_key
                        .insert(key.to_string(), vec![(id, Arc::clone(&template))]);
                }
            }
        }
        place(&mut self.all, id, &template);
    }

    /// Whether this index is the one `walked` (in ascending id) describes - the same templates,
    /// each under exactly its own columns, every list ascending - and the reason where not.
    pub(super) fn agrees_with(&self, walked: &[Walked]) -> Result<(), String> {
        ascending("every template", &self.all)?;
        if self.all.len() != walked.len() {
            return Err(format!(
                "the index holds {} templates and the nodes hold {}",
                self.all.len(),
                walked.len()
            ));
        }
        for ((id, template), (walked_id, _, walked_template)) in self.all.iter().zip(walked) {
            if id != walked_id {
                return Err(format!(
                    "the index holds template {id} where the nodes hold {walked_id}"
                ));
            }
            if **template != *walked_template {
                return Err(format!(
                    "template {id} is {template:?} in the index and {walked_template:?} at \
                     its leaf"
                ));
            }
        }

        let mut expected: BTreeMap<&str, Vec<u64>> = BTreeMap::new();
        for (id, names, _) in walked {
            for name in names {
                let ids = expected.entry(name.as_str()).or_default();
                // A leaf read from a file can name one column twice; it is one key.
                if ids.last() != Some(id) {
                    ids.push(*id);
                }
            }
        }
        if self.by_key.len() != expected.len() {
            return Err(format!(
                "the index holds {} keys and the leaves' columns name {}",
                self.by_key.len(),
                expected.len()
            ));
        }
        for (key, ids) in expected {
            let Some(list) = self.by_key.get(key) else {
                return Err(format!("the index holds no list for the column `{key}`"));
            };
            ascending(key, list)?;
            let held: Vec<u64> = list.iter().map(|(id, _)| *id).collect();
            if held != ids {
                return Err(format!(
                    "the key `{key}` lists templates {held:?} and is a column of {ids:?}"
                ));
            }
            for (id, template) in list {
                let whole = self
                    .all
                    .binary_search_by_key(id, |posting| posting.0)
                    .ok()
                    .map(|at| &self.all[at].1);
                if !whole.is_some_and(|whole| Arc::ptr_eq(whole, template)) {
                    return Err(format!(
                        "the key `{key}` holds a template {id} that is not the one held \
                         among all templates"
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Put one posting at its place by id in a list kept ascending.
fn place(list: &mut Vec<Posting>, id: u64, template: &Arc<Template>) {
    match list.binary_search_by_key(&id, |posting| posting.0) {
        Ok(at) => list[at].1 = Arc::clone(template),
        Err(at) => list.insert(at, (id, Arc::clone(template))),
    }
}

fn ascending(name: &str, list: &[Posting]) -> Result<(), String> {
    match list.windows(2).find(|pair| pair[0].0 >= pair[1].0) {
        Some(pair) => Err(format!(
            "the list of `{name}` holds template {} before template {}",
            pair[0].0, pair[1].0
        )),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests;
