// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The trie: one token per edge, leaf at depth N, no truncation and no separate
//! column list - the keys that did not branch are the tail of the path.
//!
//! Edges are in walk order - promoted tokens first, then the rest sorted - and not the
//! flattener's identity order; `ParsedLog::walk_tokens` turns one into the other.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use chrono::{DateTime, Utc};
use thiserror::Error;

use crate::engine::templates::catalog::{Catalog, CatalogError, Record, Token};
use crate::engine::parse::flatten::ParsedLog;
use crate::engine::model::types::{widen, ColumnType, TimeSource};

mod keys;

pub(crate) use keys::Template;
pub(crate) use keys::{KeyIndex, Posting};

/// The root carries a label so that a rendered tree and a serialized one agree about
/// what sits above the first edge.
pub const ROOT_LABEL: &str = "__root__";

/// How a `Placeholder` renders and, therefore, the label `walk` falls back to.
pub(crate) const PLACEHOLDER_LABEL: &str = "[*]";

/// Template ids start here rather than at 0 so that a leaf id is never confused with a
/// node id in a log line or a query plan, which prints both.
pub(crate) const FIRST_TEMPLATE_ID: u64 = 101;

/// The largest id a template can have: one short of `u64::MAX`, which is the counter's
/// word for "nothing left" and is never an id.
pub(crate) const LAST_TEMPLATE_ID: u64 = u64::MAX - 1;

/// Where a leaf's columns live when nothing said otherwise - the CLI, which has no data
/// directory because it rebuilds the database per invocation.
pub(crate) const DEFAULT_DATA_DIR: &str = "/var/lib/kibbr/data";

/// How a trie operation fails; a poisoned lock may mean half-inserted children, and the
/// caller decides whether that ends the run.
#[derive(Debug, Error)]
pub enum TrieError {
    #[error("trie node lock poisoned by a panic in another thread")]
    LockPoisoned,
    /// The catalog record a line needed could not be made durable, so the template or
    /// widening was not made in memory either; per line, not fatal.
    #[error("catalog: {0}")]
    Catalog(#[source] CatalogError),
    /// A line needed a new template and every id is spent, which only an id a file names at
    /// the far end of the range can cause; a line of an existing template still lands.
    #[error(
        "no template id is left for a new template: every id up to {LAST_TEMPLATE_ID} is \
         in use or reserved by an id a file names"
    )]
    TemplateIdsSpent,
}

/// `Static` is an exact token; `Placeholder` matches any token in this position, and
/// nothing produces one yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum NodeType {
    Static,
    Placeholder,
}

pub struct Edge {
    pub label: String,
    pub child: Arc<Node>,
}

/// Holds `RwLock` and `AtomicBool`, neither `Serialize`, so do not derive it: persisting
/// the trie is a separate flat form where an `Arc<Node>` has become an integer id.
pub struct Node {
    pub id: u64,
    pub label: String,
    pub node_type: NodeType,
    /// Kept sorted by label, on insert and not on render, so no consumer has to re-sort.
    pub children: RwLock<Vec<Edge>>,
    pub is_leaf: AtomicBool,
    /// Written by `Trie::publish` and `Trie::amend` and nothing else, which put the template
    /// in the key index before this lock is let go; `ingest` alone bumps `last_seen`.
    pub leaf_meta: RwLock<Option<LeafMeta>>,
}

pub struct LeafMeta {
    pub template_id: u64,
    /// Sorted by name, one entry per key on the path - the path *is* the key set.
    pub columns: Vec<ColumnDef>,
    pub storage_path: String,
    pub first_seen: DateTime<Utc>,
    /// Bumped on every ingest.
    pub last_seen: DateTime<Utc>,
    /// Where this template's rows get their time: chosen when the template is created,
    /// recorded in the catalog, never changed.
    pub time_source: TimeSource,
}

pub struct ColumnDef {
    pub name: String,
    /// `None` means the column has been seen but has only ever held nulls - a different
    /// fact from `String`, which is why `ColumnType` has no `Null`.
    pub col_type: Option<ColumnType>,
    /// For a `Datetime` column, whether every value so far was rule 2's epoch digits:
    /// `Some(false)` once one was RFC 3339 text, and `None` on any other column and where
    /// the catalog never said. What decides that an integer makes the column `Int64`
    /// (`types::widen`).
    pub epoch_digits: Option<bool>,
    /// The last ⊔ that changed `col_type`, `None` if it never did: partitions older than it
    /// hold the pre-widening type and need promotion on read, and nothing newer does.
    pub widened_at: Option<DateTime<Utc>>,
}

/// What a completed `walk` found; `None` from `walk` is "no such template", which must
/// stay distinct from "template found, zero rows".
pub struct WalkResult {
    pub node: Arc<Node>,
}

impl Node {
    fn new(id: u64, label: &str, node_type: NodeType) -> Self {
        Self {
            id,
            label: label.to_string(),
            node_type,
            children: RwLock::new(Vec::new()),
            is_leaf: AtomicBool::new(false),
            leaf_meta: RwLock::new(None),
        }
    }

    /// A node rebuilt from `trie.json`, children attached afterwards; the loader validates
    /// the records into a tree first, so no `Arc` cycle can be built here.
    pub fn restored(
        id: u64,
        label: String,
        node_type: NodeType,
        is_leaf: bool,
        leaf_meta: Option<LeafMeta>,
    ) -> Self {
        Self {
            id,
            label,
            node_type,
            children: RwLock::new(Vec::new()),
            is_leaf: AtomicBool::new(is_leaf),
            leaf_meta: RwLock::new(leaf_meta),
        }
    }
}

/// The names a template's time column is chosen from, first match wins.
pub const DEFAULT_TIME_COLUMNS: &[&str] = &[
    "@timestamp",
    "timestamp",
    "time",
    "ts",
    "ts_ms",
    "ts_unix",
    "datetime",
    "event_time",
    "created_at",
];

/// What the type ladder and the trie are told about the data: for now, which key names
/// may carry a template's time.
#[derive(Debug, Clone)]
pub struct Schema {
    /// Ordered: a template's time source is the first of these that is one of its keys
    /// and whose value on the creating line reads as a time (`ParsedLog::reads_as_time`).
    pub time_columns: Vec<String>,
}

impl Default for Schema {
    fn default() -> Self {
        Self {
            time_columns: DEFAULT_TIME_COLUMNS.iter().map(|s| s.to_string()).collect(),
        }
    }
}

pub struct Trie {
    root: Arc<Node>,
    /// Stamped into every new `LeafMeta`. See `DEFAULT_DATA_DIR`.
    data_dir: String,
    /// The names a new template's time column is chosen from, in order.
    time_columns: Vec<String>,
    /// Node ids. The root took 0, so this is also the count of nodes below it.
    next_id: AtomicU64,
    next_template_id: AtomicU64,
    /// Path length summed over templates, not over lines - one per edge, counted once
    /// per template rather than once per line that walked it.
    tokens: AtomicU64,
    /// `catalog.log`, when this trie is durable; `None` for the CLI and `--storage memory`.
    /// A `Mutex`: the lock order is always leaf, then catalog, then `keys`.
    catalog: Mutex<Option<Catalog>>,
    /// For each key the templates that have it: what a plan reads in place of crossing the
    /// nodes. Never take a node's lock while holding it; read and written through poison.
    keys: RwLock<KeyIndex>,
}

impl Default for Trie {
    fn default() -> Self {
        Self::new()
    }
}

impl Trie {
    pub fn new() -> Self {
        Self::new_in(DEFAULT_DATA_DIR)
    }

    pub fn new_in(data_dir: &str) -> Self {
        Self {
            root: Arc::new(Node::new(0, ROOT_LABEL, NodeType::Static)),
            data_dir: data_dir.to_string(),
            time_columns: Schema::default().time_columns,
            next_id: AtomicU64::new(1),
            next_template_id: AtomicU64::new(FIRST_TEMPLATE_ID),
            tokens: AtomicU64::new(0),
            catalog: Mutex::new(None),
            keys: RwLock::new(KeyIndex::default()),
        }
    }

    /// A trie rebuilt from `trie.json`, its counters derived by the loader from the whole
    /// file and its key index built here by one walk of the finished nodes.
    pub fn restored(
        root: Arc<Node>,
        data_dir: &str,
        next_id: u64,
        next_template_id: u64,
        tokens: u64,
    ) -> Self {
        let trie = Self {
            root,
            data_dir: data_dir.to_string(),
            time_columns: Schema::default().time_columns,
            next_id: AtomicU64::new(next_id),
            next_template_id: AtomicU64::new(next_template_id),
            tokens: AtomicU64::new(tokens),
            catalog: Mutex::new(None),
            keys: RwLock::new(KeyIndex::default()),
        };
        let walked = trie.walked();
        {
            let mut keys = trie.keys.write().unwrap_or_else(PoisonError::into_inner);
            for (id, names, template) in walked {
                keys.put(id, names.iter().map(String::as_str), Arc::new(template));
            }
        }
        debug_assert_eq!(trie.keys_agree(), Ok(()));
        trie
    }

    /// The names a new template's time column is chosen from, for templates created after
    /// the call and nothing already recorded.
    pub fn with_time_columns(mut self, time_columns: Vec<String>) -> Self {
        self.time_columns = time_columns;
        self
    }

    pub fn time_columns(&self) -> &[String] {
        &self.time_columns
    }

    /// Make this trie durable: every template and widening from here on is appended to
    /// `catalog` before it exists in memory.
    pub fn attach_catalog(&self, catalog: Catalog) {
        if let Ok(mut slot) = self.catalog.lock() {
            *slot = Some(catalog);
        }
    }

    /// Where the catalog is, if this trie has one.
    pub fn catalog_path(&self) -> Option<std::path::PathBuf> {
        self.catalog
            .lock()
            .ok()
            .and_then(|slot| slot.as_ref().map(|c| c.path().to_path_buf()))
    }

    /// Append to the catalog, or do nothing when there is none.
    fn record(&self, records: &[Record]) -> Result<(), TrieError> {
        let mut slot = self.catalog.lock().map_err(|_| TrieError::LockPoisoned)?;
        match slot.as_mut() {
            Some(catalog) => catalog.append(records).map_err(TrieError::Catalog),
            None => Ok(()),
        }
    }

    /// A trie rebuilt from catalog records, in record order: the last column record wins,
    /// and of two template records with one id or one path the first is kept, with a warning.
    #[cfg(test)]
    pub fn from_catalog(
        data_dir: &str,
        records: &[Record],
        warn: &mut dyn FnMut(String),
    ) -> Result<Self, TrieError> {
        let time_columns = Schema::default().time_columns;
        Ok(Self::from_catalog_with(data_dir, records, time_columns, warn)?.0)
    }

    /// `from_catalog`, with the configured time columns and the records a caller should
    /// append: one `time_source` for every template the catalog holds none for.
    pub fn from_catalog_with(
        data_dir: &str,
        records: &[Record],
        time_columns: Vec<String>,
        warn: &mut dyn FnMut(String),
    ) -> Result<(Self, Vec<Record>), TrieError> {
        struct Pending {
            created_at: DateTime<Utc>,
            last_at: DateTime<Utc>,
            tokens: Vec<Token>,
            columns: BTreeMap<String, ColumnDef>,
            time_source: Option<TimeSource>,
        }
        let mut templates: BTreeMap<u64, Pending> = BTreeMap::new();
        for record in records {
            match record {
                Record::Template {
                    id,
                    created_at,
                    tokens,
                } => {
                    // Intact, and no build wrote it: an id comes from a counter that stops at
                    // `LAST_TEMPLATE_ID`. Skipped with a WARN, and never a failed start.
                    if *id > LAST_TEMPLATE_ID {
                        warn(format!(
                            "a template record has id {id}, past the largest id a template \
                             can have ({LAST_TEMPLATE_ID}); skipped"
                        ));
                        continue;
                    }
                    if templates.contains_key(id) {
                        warn(format!(
                            "template {id} is recorded twice; keeping the first record"
                        ));
                        continue;
                    }
                    templates.insert(
                        *id,
                        Pending {
                            created_at: *created_at,
                            last_at: *created_at,
                            tokens: tokens.clone(),
                            columns: BTreeMap::new(),
                            time_source: None,
                        },
                    );
                }
                Record::Column {
                    template_id,
                    name,
                    col_type,
                    epoch_digits,
                    at,
                    ..
                } => {
                    let Some(pending) = templates.get_mut(template_id) else {
                        warn(format!(
                            "a column record names template {template_id}, which has no \
                             template record before it; skipped"
                        ));
                        continue;
                    };
                    if let Some(at) = at {
                        pending.last_at = pending.last_at.max(*at);
                    }
                    pending.columns.insert(
                        name.clone(),
                        ColumnDef {
                            name: name.clone(),
                            col_type: *col_type,
                            epoch_digits: *epoch_digits,
                            widened_at: *at,
                        },
                    );
                }
                // The first record wins: the source is chosen once and never changes,
                // so a second is a file that has been edited.
                Record::TimeSource {
                    template_id,
                    source,
                } => {
                    let Some(pending) = templates.get_mut(template_id) else {
                        warn(format!(
                            "a time_source record names template {template_id}, which has \
                             no template record before it; skipped"
                        ));
                        continue;
                    };
                    match TimeSource::from_record(source) {
                        Some(_) if pending.time_source.is_some() => warn(format!(
                            "template {template_id} has a second time_source record; \
                             keeping the first"
                        )),
                        Some(parsed) => pending.time_source = Some(parsed),
                        None => warn(format!(
                            "template {template_id}: time_source `{source}` is not a \
                             spelling this build knows; skipped"
                        )),
                    }
                }
            }
        }

        let trie = Self::new_in(data_dir).with_time_columns(time_columns);
        let mut backfill = Vec::new();
        for (id, pending) in templates {
            // `max + 1`, so a gap in the sequence is never refilled, and before the path is looked
            // at, so the id of a record skipped just below is kept from reuse too.
            trie.reserve_template_ids_through(id);
            let labels: Vec<String> = pending.tokens.iter().map(Token::label).collect();
            let borrowed: Vec<&str> = labels.iter().map(String::as_str).collect();
            let leaf = trie.path_to(&borrowed)?;
            let mut slot = write(&leaf.leaf_meta)?;
            if let Some(existing) = slot.as_ref() {
                warn(format!(
                    "template {id} has the same path as template {}; keeping the first",
                    existing.template_id
                ));
                continue;
            }
            let time_source = match pending.time_source {
                Some(source) => source,
                None => {
                    let derived = TimeSource::choose(&trie.time_columns, |name| {
                        pending.columns.get(name).and_then(|c| c.col_type)
                    });
                    backfill.push(Record::TimeSource {
                        template_id: id,
                        source: derived.as_record(),
                    });
                    derived
                }
            };
            let meta = LeafMeta {
                template_id: id,
                columns: pending.columns.into_values().collect(),
                storage_path: format!("{data_dir}/leaf_{id}"),
                first_seen: pending.created_at,
                last_seen: pending.last_at,
                time_source,
            };
            let depth = labels.len() as u64;
            trie.publish(&leaf, &mut slot, labels, meta);
            trie.tokens.fetch_add(depth, Ordering::Relaxed);
        }
        debug_assert_eq!(trie.keys_agree(), Ok(()));
        Ok((trie, backfill))
    }

    /// Every template as catalog records, in id order: what writing a catalog from a
    /// `trie.json` produces, once, on the first start that finds no catalog.
    pub fn catalog_records(&self) -> Vec<Record> {
        let mut leaves: Vec<(Vec<String>, LeafMeta)> = Vec::new();
        for (path, leaf) in self.leaf_paths() {
            let Ok(slot) = leaf.leaf_meta.read() else {
                continue;
            };
            let Some(meta) = slot.as_ref() else {
                continue;
            };
            leaves.push((
                path,
                LeafMeta {
                    template_id: meta.template_id,
                    columns: meta
                        .columns
                        .iter()
                        .map(|c| ColumnDef {
                            name: c.name.clone(),
                            col_type: c.col_type,
                            epoch_digits: c.epoch_digits,
                            widened_at: c.widened_at,
                        })
                        .collect(),
                    storage_path: meta.storage_path.clone(),
                    first_seen: meta.first_seen,
                    last_seen: meta.last_seen,
                    time_source: meta.time_source.clone(),
                },
            ));
        }
        leaves.sort_by_key(|(_, meta)| meta.template_id);

        let mut records = Vec::new();
        for (path, meta) in leaves {
            let tokens = path
                .iter()
                .map(|label| match label.split_once('=') {
                    Some((promoted, value)) => Token::Promoted {
                        promoted: promoted.to_string(),
                        value: value.to_string(),
                    },
                    None => Token::Key { key: label.clone() },
                })
                .collect();
            records.push(Record::Template {
                id: meta.template_id,
                created_at: meta.first_seen,
                tokens,
            });
            for col in &meta.columns {
                records.push(Record::Column {
                    template_id: meta.template_id,
                    name: col.name.clone(),
                    col_type: col.col_type,
                    epoch_digits: col.epoch_digits,
                    widened_from: None,
                    at: col.widened_at,
                });
            }
            records.push(Record::TimeSource {
                template_id: meta.template_id,
                source: meta.time_source.as_record(),
            });
        }
        records
    }

    pub fn root(&self) -> Arc<Node> {
        Arc::clone(&self.root)
    }

    /// Poison the key index's lock, as a panic inside a hold of its write half does, for
    /// the tests of what an ingest and a plan do after one.
    #[cfg(test)]
    pub fn poison_keys(&self) {
        std::thread::scope(|scope| {
            let holder = scope.spawn(|| {
                let _held = self.keys.write().expect("not poisoned yet");
                panic!("a panic inside a hold of the key index's write lock");
            });
            assert!(holder.join().is_err());
        });
    }

    /// The key index, for a plan, held for as long as the guard is: a caller reads what it
    /// needs and lets go before it does anything that could make a template.
    pub fn keys(&self) -> RwLockReadGuard<'_, KeyIndex> {
        self.keys.read().unwrap_or_else(PoisonError::into_inner)
    }

    /// The one place a template comes to exist in memory: its place in the key index, its
    /// meta and its leaf flag in one hold of the index's write lock, so a plan and a walk agree.
    fn publish(&self, leaf: &Node, slot: &mut Option<LeafMeta>, path: Vec<String>, meta: LeafMeta) {
        let template = Arc::new(Template::of(path, &meta));
        let mut keys = self.keys.write().unwrap_or_else(PoisonError::into_inner);
        keys.put(
            meta.template_id,
            meta.columns.iter().map(|col| col.name.as_str()),
            template,
        );
        *slot = Some(meta);
        leaf.is_leaf.store(true, Ordering::Release);
    }

    /// The one place a template's columns change: the changes applied to `meta`, the
    /// template made again whole from it, and that template put in the key index.
    fn amend(
        &self,
        meta: &mut LeafMeta,
        path: &[String],
        changes: Vec<Change>,
        now: DateTime<Utc>,
    ) {
        for (name, col_type, epoch_digits, widened) in changes {
            match meta
                .columns
                .binary_search_by(|col| col.name.as_str().cmp(&name))
            {
                Ok(at) => {
                    meta.columns[at].col_type = col_type;
                    meta.columns[at].epoch_digits = epoch_digits;
                    if widened {
                        meta.columns[at].widened_at = Some(now);
                    }
                }
                Err(at) => meta.columns.insert(
                    at,
                    ColumnDef {
                        name,
                        col_type,
                        epoch_digits,
                        widened_at: None,
                    },
                ),
            }
        }
        let template = Arc::new(Template::of(path.to_vec(), meta));
        self.keys
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .put(
                meta.template_id,
                meta.columns.iter().map(|col| col.name.as_str()),
                template,
            );
    }

    /// Every template a walk of the nodes finds, in id order; a leaf whose lock is poisoned
    /// is left out, as a plan that walks leaves it out.
    fn walked(&self) -> Vec<keys::Walked> {
        let mut walked = Vec::new();
        for (path, leaf) in self.leaf_paths() {
            let Ok(slot) = leaf.leaf_meta.read() else {
                continue;
            };
            let Some(meta) = slot.as_ref() else {
                continue;
            };
            let names = meta.columns.iter().map(|col| col.name.clone()).collect();
            walked.push((meta.template_id, names, Template::of(path, meta)));
        }
        walked.sort_by_key(|(id, ..)| *id);
        walked
    }

    /// Whether the key index is the one a walk of the nodes would build, and the reason
    /// where it is not; it says something only while nothing ingests.
    pub fn keys_agree(&self) -> Result<(), String> {
        let walked = self.walked();
        self.keys().agrees_with(&walked)
    }

    /// The next template id, in the order templates are first seen; checked, so past
    /// `LAST_TEMPLATE_ID` it says so and does not start again from 0 through the ids in use.
    pub fn next_template_id(&self) -> Result<u64, TrieError> {
        let mut next = self.next_template_id.load(Ordering::Relaxed);
        loop {
            let after = next.checked_add(1).ok_or(TrieError::TemplateIdsSpent)?;
            match self.next_template_id.compare_exchange_weak(
                next,
                after,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(next),
                Err(now) => next = now,
            }
        }
    }

    /// Every node in the trie, `__root__` included - ids are handed out from 1 and the
    /// root took 0, so the next id is also the count.
    pub fn node_count(&self) -> u64 {
        self.next_id.load(Ordering::Relaxed)
    }

    /// The leaves that exist, counted - not the ids handed out, which reserving skips ahead of.
    pub fn template_count(&self) -> u64 {
        self.all_leaves().len() as u64
    }

    /// Refuse to hand out a template id at or below one already spent: leaf directories are
    /// named by id, and reissuing one points a new template at another template's rows.
    pub fn reserve_template_ids_through(&self, highest_used: u64) {
        self.next_template_id
            .fetch_max(highest_used.saturating_add(1), Ordering::Relaxed);
    }

    /// What the same templates would cost with no prefix sharing: every path spelled out in
    /// full, the root counted as `node_count` counts it so the difference is the nodes shared.
    pub fn token_count(&self) -> u64 {
        self.tokens.load(Ordering::Relaxed) + 1
    }

    /// Walk the tokens, creating what is missing, and return the last node - not yet a leaf:
    /// `ingest` marks it one only after its catalog record is on disk.
    fn path_to(&self, tokens: &[&str]) -> Result<Arc<Node>, TrieError> {
        let mut node = Arc::clone(&self.root);
        for token in tokens {
            node = self.child_or_create(&node, token)?;
        }
        Ok(node)
    }

    /// Find the child for `token`, creating it if absent: read lock first, then the write
    /// lock and a re-check, or two threads build two nodes wearing one label.
    fn child_or_create(&self, node: &Arc<Node>, token: &str) -> Result<Arc<Node>, TrieError> {
        if let Some(child) = exact_child(&read(&node.children)?, token) {
            return Ok(child);
        }

        let mut children = write(&node.children)?;
        if let Some(child) = exact_child(&children, token) {
            return Ok(child);
        }

        // Exact match only, even though `walk` falls back to `[*]`: matching an existing
        // placeholder here would file a new template under a wildcard and lose the specific one.
        let child = Arc::new(Node::new(
            self.next_id.fetch_add(1, Ordering::Relaxed),
            token,
            NodeType::Static,
        ));
        let at = children
            .binary_search_by(|edge| edge.label.as_str().cmp(token))
            .unwrap_or_else(|at| at);
        children.insert(
            at,
            Edge {
                label: token.to_string(),
                child: Arc::clone(&child),
            },
        );
        Ok(child)
    }

    /// Walk an exact token list; `None` is "no such path" (or a poisoned lock) and must stay
    /// distinguishable from a leaf that holds no rows.
    pub fn walk(&self, tokens: &[&str]) -> Option<WalkResult> {
        let mut node = Arc::clone(&self.root);
        for token in tokens {
            let next = {
                let children = read(&node.children).ok()?;
                find_child(&children, token)?
            };
            node = next;
        }
        // A leaf whose lock is poisoned is no such path, as a poisoned edge is.
        drop(node.leaf_meta.read().ok()?);
        Some(WalkResult { node })
    }

    /// Every leaf in the trie, in path order, so independent of the order lines arrived in;
    /// a poisoned lock truncates the subtree it guards rather than failing.
    pub fn all_leaves(&self) -> Vec<Arc<Node>> {
        let mut leaves = Vec::new();
        let _ = collect_leaves(&self.root, &mut leaves);
        leaves
    }

    /// Every leaf with the edge labels that reach it, root excluded: a template is its path,
    /// and the columns alone cannot say which of them were promoted.
    pub fn leaf_paths(&self) -> Vec<(Vec<String>, Arc<Node>)> {
        let mut out = Vec::new();
        let mut path = Vec::new();
        let _ = collect_leaf_paths(&self.root, &mut path, &mut out);
        out
    }

    /// Put one parsed line into the trie: walk order to a leaf, then join this line's types
    /// into its columns, the catalog record written before the leaf changes.
    /// `now` is a parameter so a test that pins `first_seen` and `last_seen` can fix the clock.
    pub fn ingest(
        &self,
        parsed: &ParsedLog,
        now: DateTime<Utc>,
    ) -> Result<(Arc<Node>, bool), TrieError> {
        let tokens = parsed.walk_tokens();
        let borrowed: Vec<&str> = tokens.iter().map(String::as_str).collect();
        let leaf = self.path_to(&borrowed)?;

        // Scoped, so the write lock is released before the leaf is handed back to a caller
        // that goes straight on to read the columns.
        let mut slot = write(&leaf.leaf_meta)?;
        let is_new = match slot.as_mut() {
            None => {
                let template_id = self.next_template_id()?;
                // `keys` is sorted, so the columns are born in name order - the order
                // `binary_search_by` below depends on.
                let columns: Vec<ColumnDef> = parsed
                    .keys
                    .iter()
                    .map(|key| {
                        let (col_type, epoch_digits) = first_type(parsed, key);
                        ColumnDef {
                            name: key.clone(),
                            col_type,
                            epoch_digits,
                            widened_at: None,
                        }
                    })
                    .collect();
                let mut records = Vec::with_capacity(1 + columns.len());
                records.push(Record::Template {
                    id: template_id,
                    created_at: now,
                    tokens: parsed
                        .promoted
                        .iter()
                        .map(|key| Token::Promoted {
                            promoted: key.clone(),
                            value: parsed.values.get(key).cloned().unwrap_or_default(),
                        })
                        .chain(
                            parsed
                                .value_keys()
                                .map(|key| Token::Key { key: key.clone() }),
                        )
                        .collect(),
                });
                records.extend(columns.iter().map(|col| Record::Column {
                    template_id,
                    name: col.name.clone(),
                    col_type: col.col_type,
                    epoch_digits: col.epoch_digits,
                    widened_from: None,
                    at: None,
                }));
                // The time source: the first configured name among this line's keys that read as a
                // time - a `Datetime`, or a form only a time column reads - chosen once in the
                // same durable unit as the template. The columns keep the types the ladder gave.
                let time_source =
                    TimeSource::choose_by(&self.time_columns, |name| parsed.reads_as_time(name));
                records.push(Record::TimeSource {
                    template_id,
                    source: time_source.as_record(),
                });
                self.record(&records)?;

                if time_source == TimeSource::Arrival {
                    // One WARN per template, naming the keys an operator could add to
                    // `time_columns` to give it a time of its own.
                    tracing::warn!(
                        "template {template_id} has no time column among \
                         [{}]; its rows take their arrival time. Datetime-typed keys: {}",
                        self.time_columns.join(", "),
                        datetime_keys(columns.iter().map(|c| (c.name.as_str(), c.col_type)))
                    );
                }

                let meta = LeafMeta {
                    template_id,
                    columns,
                    storage_path: format!("{}/leaf_{template_id}", self.data_dir),
                    first_seen: now,
                    last_seen: now,
                    time_source,
                };
                let depth = tokens.len() as u64;
                self.publish(&leaf, &mut slot, tokens, meta);
                self.tokens.fetch_add(depth, Ordering::Relaxed);
                true
            }
            Some(meta) => {
                // What this line changes, decided before anything is touched: the record has to be
                // durable first, or a refused change would leave the trie ahead of the file.
                let mut records = Vec::new();
                let mut changes: Vec<Change> = Vec::new();
                for key in &parsed.keys {
                    let incoming = parsed.column_type(key);
                    match meta
                        .columns
                        .binary_search_by(|col| col.name.as_str().cmp(key))
                    {
                        Ok(at) => {
                            let col = &meta.columns[at];
                            // `join`, but digits rule 2 read as an instant and an integer
                            // are one column of integers, not text - unless the column is
                            // known to hold RFC 3339 dates.
                            let epoch = incoming == Some(ColumnType::Datetime)
                                && parsed.is_epoch_digits(key);
                            let (joined, epoch_digits) =
                                widen(col.col_type, col.epoch_digits, incoming, epoch);
                            if joined != col.col_type {
                                // The one place an operator can learn that a column widened.
                                // `None ⊔ T` is a column learning its first type: no warning.
                                if let (Some(from), Some(to)) = (col.col_type, joined) {
                                    tracing::warn!(
                                        "template {} column `{key}` widened \
                                         from {} to {} on value `{}`",
                                        meta.template_id,
                                        from.as_str(),
                                        to.as_str(),
                                        cut(parsed.values.get(key).map_or("", String::as_str), 80)
                                    );
                                }
                                records.push(Record::Column {
                                    template_id: meta.template_id,
                                    name: key.clone(),
                                    col_type: joined,
                                    epoch_digits,
                                    widened_from: col.col_type,
                                    at: Some(now),
                                });
                                changes.push((key.clone(), joined, epoch_digits, true));
                            } else if epoch_digits != col.epoch_digits {
                                // The type stands and what the column says of its values
                                // does not: its first RFC 3339 date. Recorded, so a restart
                                // widens the column as this process would; `at` is the
                                // widening the column already had, which the last record
                                // must not lose.
                                records.push(Record::Column {
                                    template_id: meta.template_id,
                                    name: key.clone(),
                                    col_type: joined,
                                    epoch_digits,
                                    widened_from: None,
                                    at: col.widened_at,
                                });
                                changes.push((key.clone(), joined, epoch_digits, false));
                            }
                        }
                        // A key not on the path: a leaf from before `=` was escaped can hold a
                        // literal `event=login` too. Union the columns, do not drop the value.
                        Err(_) => {
                            let (col_type, epoch_digits) = first_type(parsed, key);
                            records.push(Record::Column {
                                template_id: meta.template_id,
                                name: key.clone(),
                                col_type,
                                epoch_digits,
                                widened_from: None,
                                at: None,
                            });
                            changes.push((key.clone(), col_type, epoch_digits, false));
                        }
                    }
                }
                // Every change has its record, so a line that changes nothing appends
                // nothing and goes nowhere near the key index.
                if !changes.is_empty() {
                    self.record(&records)?;
                    self.amend(meta, &tokens, changes, now);
                }
                meta.last_seen = now;
                false
            }
        };
        drop(slot);
        Ok((leaf, is_new))
    }
}

/// One change to a template's columns: the name, the type, what a `Datetime` column says
/// of its values, and whether the type widened.
type Change = (String, Option<ColumnType>, Option<bool>, bool);

/// What a line's value makes of a column that has no type yet: its type, and for a
/// `Datetime` whether it is rule 2's digits.
fn first_type(parsed: &ParsedLog, key: &str) -> (Option<ColumnType>, Option<bool>) {
    widen(
        None,
        None,
        parsed.column_type(key),
        parsed.is_epoch_digits(key),
    )
}

/// The `Datetime`-typed keys an operator could add to `time_columns`, comma-separated, or
/// `none`: one spelling for the WARN and for the bulk loader's refusal.
pub(crate) fn datetime_keys<'a>(
    columns: impl IntoIterator<Item = (&'a str, Option<ColumnType>)>,
) -> String {
    let dated: Vec<&str> = columns
        .into_iter()
        .filter(|(_, ty)| *ty == Some(ColumnType::Datetime))
        .map(|(name, _)| name)
        .collect();
    if dated.is_empty() {
        "none".to_string()
    } else {
        dated.join(", ")
    }
}

/// The first `n` characters, with an ellipsis where something was cut. On a character
/// boundary, so a multi-byte value never panics the warning that names it.
fn cut(text: &str, n: usize) -> String {
    let mut out: String = text.chars().take(n).collect();
    if text.chars().count() > n {
        out.push('…');
    }
    out
}

/// The exact static label, or nothing.
fn exact_child(children: &[Edge], token: &str) -> Option<Arc<Node>> {
    let at = children
        .binary_search_by(|edge| edge.label.as_str().cmp(token))
        .ok()?;
    Some(Arc::clone(&children[at].child))
}

/// A static child always beats a placeholder: when both could match a token, the more
/// specific template is the right answer, so `[*]` is only the fallback.
fn find_child(children: &[Edge], token: &str) -> Option<Arc<Node>> {
    exact_child(children, token).or_else(|| exact_child(children, PLACEHOLDER_LABEL))
}

fn collect_leaves(node: &Arc<Node>, out: &mut Vec<Arc<Node>>) -> Result<(), TrieError> {
    if node.is_leaf.load(Ordering::Acquire) {
        out.push(Arc::clone(node));
    }
    for edge in read(&node.children)?.iter() {
        collect_leaves(&edge.child, out)?;
    }
    Ok(())
}

fn collect_leaf_paths(
    node: &Arc<Node>,
    path: &mut Vec<String>,
    out: &mut Vec<(Vec<String>, Arc<Node>)>,
) -> Result<(), TrieError> {
    if node.is_leaf.load(Ordering::Acquire) {
        out.push((path.clone(), Arc::clone(node)));
    }
    for edge in read(&node.children)?.iter() {
        path.push(edge.label.clone());
        collect_leaf_paths(&edge.child, path, out)?;
        path.pop();
    }
    Ok(())
}

fn read<T>(lock: &RwLock<T>) -> Result<RwLockReadGuard<'_, T>, TrieError> {
    lock.read().map_err(|_| TrieError::LockPoisoned)
}

fn write<T>(lock: &RwLock<T>) -> Result<RwLockWriteGuard<'_, T>, TrieError> {
    lock.write().map_err(|_| TrieError::LockPoisoned)
}

#[cfg(test)]
#[path = "trie_tests.rs"]
mod tests;
