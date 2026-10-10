//! What a plan reads of the trie without crossing it: one finished description of each
//! template, and for each key the templates that have it as a column.
//!
//! A plan that walks the nodes pays for the trie and not for its question. It takes every
//! node's lock and copies every label to learn which templates hold a key, and then copies
//! the path and the columns of each template it keeps. So the answer to "which templates
//! have this key" is kept here as templates are made, and what a plan carries of a template
//! is built once and shared by every plan that names it.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use super::LeafMeta;
use kibbr_core::flatten::promoted_of;
use kibbr_core::types::ColumnType;

/// One template as a plan reads it: everything a planned leaf carries but its id and the
/// terms left to the scan.
///
/// Made when the template is made, made again whole when a column of it widens or is
/// added, and never changed in place: a plan holds an `Arc` of it, and a plan made a moment
/// before a widening goes on reading the template as it was.
#[derive(Debug, PartialEq)]
pub struct Template {
    /// The template's key set as walk tokens - what `ParsedLog::walk_tokens` produces for
    /// any line in it - root excluded.
    ///
    /// Carried because it is the template's *content-derived* identity, where its id is a
    /// counter handed out in the order templates were first seen. The result order needs a
    /// tiebreak for rows a timestamp cannot separate, and a tiebreak the reference scanner
    /// cannot recompute is one the two sides will disagree about - which they did, on
    /// every template whose time column is not called `ts`.
    ///
    /// The first label is the root's edge to this template, which is what a plan's root
    /// filter reads.
    pub path: Vec<String>,
    /// Carried by a plan and opened by nothing: a store finds a leaf's directory from its
    /// own base and the template's id, and this is the path the leaf was first given.
    pub storage_path: String,
    /// The template's time column: what its rows are ordered, windowed and bucketed by,
    /// falling back to the system column on a row where it holds no instant. Every
    /// template has one, so every planned leaf has a time axis.
    pub time_column: String,
    /// Columns whose type is now `String`, in the order the leaf holds its columns. A
    /// `Datetime` stored in one of them is rendered back to its ingested text by the store
    /// before the row is read.
    pub text_columns: Vec<String>,
    /// `(column, value)` for each column the path settles, in key order: what
    /// `flatten::promoted_of` reads from this path and these columns. Empty for most
    /// templates and one pair for most of the rest, so it is searched and not hashed.
    promoted: Vec<(String, String)>,
}

impl Template {
    /// The one function from a leaf to what a plan reads of it. Every site that makes or
    /// changes a template calls this and nothing else builds one, so the plan that reads
    /// the index and the plan that walks the nodes describe a template the same way.
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
pub type Posting = (u64, Arc<Template>);

/// A template as a walk of the nodes finds it: its id, its column names and what
/// `Template::of` makes of it. What a held index is compared with.
pub(super) type Walked = (u64, Vec<String>, Template);

/// For each key, the templates that have it as a column; and every template.
///
/// Each list is in ascending template id, which is the order a plan gives its leaves in,
/// so a plan reads a list from front to back and sorts nothing. A template is in a key's
/// list exactly when that key is one of its leaf's columns: the key is the column's name
/// as a query spells it, never an edge label.
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

    /// The templates that have `key` as a column. `None` when no template has it; never
    /// an empty list.
    pub fn of(&self, key: &str) -> Option<&[Posting]> {
        self.by_key.get(key).map(Vec::as_slice)
    }

    pub fn edits(&self) -> u64 {
        self.edits
    }

    /// Put template `id` under each of `keys` and among all templates: inserted at its
    /// place by id where it is new, the handle replaced where it is there.
    ///
    /// One method for a new template and for a changed one, since a template's columns
    /// are only ever added to: a changed template is in every list it was in, and a
    /// column it gained is one more key to be put under. Ids nearly always arrive
    /// ascending, so finding the place is a search that ends at the back.
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

    /// Whether this index is the one `walked` describes: the same templates in the same
    /// order, each under exactly its own columns, and every list strictly ascending by
    /// id. `walked` is in ascending id. The reason, where it is not.
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
mod tests {
    use super::*;
    use crate::trie::ColumnDef;
    use kibbr_core::types::TimeSource;

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
}
