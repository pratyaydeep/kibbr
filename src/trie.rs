//! The trie. One token per edge, leaf at depth N, no truncation and no separate column
//! list - the keys that did not branch are simply the tail of the path.
//!
//! Two orders run over one token set and must not be confused. *Identity order* is the
//! flattener's - keys sorted, promoted keys rendered `key=value` in place - and it is
//! what `fingerprint` joins and `template_id` hashes; nothing here touches it. *Walk
//! order* is this module's: promoted tokens first, then the rest sorted. Same multiset
//! either way. `ParsedLog::walk_tokens` turns one into the other, and documents why.

// The query engine will call `walk`, `walk_prefix` and `all_leaves`; the column store
// will open `LeafMeta::storage_path`, and crash recovery will read its timestamps.
// Nothing builds a `Placeholder` yet either. Written now because teaching a walk its lock
// discipline later means editing every call site that already exists - the retrofit this
// module is shaped to avoid. Scoped so the allow does not travel.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use chrono::{DateTime, Utc};
use thiserror::Error;

use crate::catalog::{Catalog, CatalogError, Record, Token};
use crate::flatten::ParsedLog;
use crate::types::{join, ColumnType, TimeSource};

/// The root carries a label so that a rendered tree and a serialized one agree about
/// what sits above the first edge.
pub const ROOT_LABEL: &str = "__root__";

/// How a `Placeholder` renders and, therefore, the label `walk` falls back to.
pub const PLACEHOLDER_LABEL: &str = "[*]";

/// Template ids start here rather than at 0 so that a leaf id is never confused with a
/// node id in a log line or a query plan: they are two counters over the same trie, and
/// an explained query prints both.
pub const FIRST_TEMPLATE_ID: u64 = 101;

/// The largest id a template can have: one short of `u64::MAX`.
///
/// The counter holds the id the next template takes, and a start derives it as `max + 1`
/// of the ids in the catalog. `u64::MAX` has no `max + 1`: computed, it was a panic at
/// startup in a debug build, and in a release one it wrapped to 0 and reserved nothing.
/// And a counter that counted past `u64::MAX` went on from 0, through every id already in
/// use. So the counter stops at `u64::MAX`, which is its word for "nothing left" and is
/// never an id, and the last id it gives is this one.
pub const LAST_TEMPLATE_ID: u64 = u64::MAX - 1;

/// Where a leaf's columns live when nothing said otherwise - the CLI, which has no data
/// directory because it rebuilds the database per invocation.
///
/// `LeafMeta::storage_path` is written once per template and persisted with it, so it is
/// documentation and not authority: the store derives a leaf's directory from the
/// configured base and the template id, because a restart under a different `--data-dir`
/// would otherwise send every leaf back to the directory it was first seen under.
pub const DEFAULT_DATA_DIR: &str = "/var/lib/kibbr/data";

/// The only way a trie operation fails.
///
/// A poisoned lock means some thread panicked while holding it, so the children below
/// that node may be half-inserted. There is nothing to recover here - the caller decides
/// whether that ends the run.
#[derive(Debug, Error)]
pub enum TrieError {
    #[error("trie node lock poisoned by a panic in another thread")]
    LockPoisoned,
    /// The catalog record a line needed could not be made durable, so the template or
    /// widening it would have caused was not made in memory either. Per line, not fatal:
    /// the next line that needs no record still lands.
    #[error("catalog: {0}")]
    Catalog(#[source] CatalogError),
    /// A line needed a new template and every id is spent. Counting does not get here -
    /// a template a microsecond takes half a million years - so some file names an id at
    /// the far end of the range, and the ids up to it are kept from reuse like any
    /// other's. A line of a template that already exists still lands.
    #[error(
        "no template id is left for a new template: every id up to {LAST_TEMPLATE_ID} is \
         in use or reserved by an id a file names"
    )]
    TemplateIdsSpent,
}

/// `Static` is an exact token; `Placeholder` matches any token in this position.
///
/// Nothing produces a `Placeholder` yet: schema merging, when it arrives, reuses an
/// existing leaf rather than introducing a wildcard edge, so every node `insert` builds
/// is `Static`. The variant and the preference in `find_child` are the contract for
/// whenever a producer exists - most likely a label whose distinct-value count crosses a
/// bound - and until then the rule is unreachable rather than wrong.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum NodeType {
    Static,
    Placeholder,
}

pub struct Edge {
    pub label: String,
    pub child: Arc<Node>,
}

/// `Node` holds `RwLock` and `AtomicBool`, neither `Serialize`. Do not derive it:
/// persisting the trie means a separate flat representation where an `Arc<Node>` pointer
/// has become an integer id, and there is nothing to persist to yet.
pub struct Node {
    pub id: u64,
    pub label: String,
    pub node_type: NodeType,
    /// Kept sorted by label. Sorted on insert, not on render, so `all_leaves` and the
    /// demo tree are order-independent structurally rather than leaving every consumer
    /// to re-sort and one of them to forget.
    pub children: RwLock<Vec<Edge>>,
    pub is_leaf: AtomicBool,
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
    /// Where this template's rows get their time. Chosen when the template is
    /// created, recorded in the catalog, never changed.
    pub time_source: TimeSource,
}

pub struct ColumnDef {
    pub name: String,
    /// `None` means the column has been seen but has only ever held nulls - a different
    /// fact from `String`, which is why `ColumnType` has no `Null`.
    pub col_type: Option<ColumnType>,
    /// The last ⊔ that *changed* `col_type`; `None` for a column that has held one type
    /// since `first_seen`. This is what makes a mutable column type affordable once there
    /// is data on disk: partitions are day-stamped, so a column that widened at *t* means
    /// every partition older than *t* holds the pre-widening type and needs promotion on
    /// read, and nothing newer does. Without the timestamp, finding the laggards means
    /// opening all of them.
    pub widened_at: Option<DateTime<Utc>>,
}

/// What a completed `walk` found. `None` from `walk` is "no such template", which must
/// stay distinct from "template found, zero rows" - a query that cannot tell them apart
/// reports an unknown field as an empty result.
pub struct WalkResult {
    pub node: Arc<Node>,
    pub is_leaf: bool,
    pub template_id: Option<u64>,
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

    /// A node rebuilt from `trie.json`, children attached afterwards.
    ///
    /// Two-pass, because an adjacency list names children by id and an id may be written
    /// before the node it names. The loader validates the records into a tree *before*
    /// calling this, so no `Arc` cycle can be built here - a cycle in the file would
    /// otherwise be a leak rather than an error.
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
    /// `catalog.log`, when this trie is durable. `None` for the CLI and `--storage memory`,
    /// where a template exists only for the run.
    ///
    /// Owned by the trie because the trie is what a catalog describes: `ingest` writes the
    /// record *before* it changes a leaf, and the two would drift apart the moment they had
    /// two owners. A `Mutex` and not the per-node locks - a record is taken while one
    /// leaf's meta lock is held, and the order is always leaf then catalog, so two lines
    /// creating two templates cannot deadlock on each other.
    catalog: Mutex<Option<Catalog>>,
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
            time_columns: crate::config::Schema::default().time_columns,
            next_id: AtomicU64::new(1),
            next_template_id: AtomicU64::new(FIRST_TEMPLATE_ID),
            tokens: AtomicU64::new(0),
            catalog: Mutex::new(None),
        }
    }

    /// A trie rebuilt from `trie.json`.
    ///
    /// `next_id` and `next_template_id` are handed in rather than recomputed here because
    /// only the loader has seen the whole file - and they are *derived* from it rather than
    /// trusted: a stale counter hands out an id that is already in use, and for a template
    /// id that means two leaves sharing one storage directory.
    pub fn restored(
        root: Arc<Node>,
        data_dir: &str,
        next_id: u64,
        next_template_id: u64,
        tokens: u64,
    ) -> Self {
        Self {
            root,
            data_dir: data_dir.to_string(),
            time_columns: crate::config::Schema::default().time_columns,
            next_id: AtomicU64::new(next_id),
            next_template_id: AtomicU64::new(next_template_id),
            tokens: AtomicU64::new(tokens),
            catalog: Mutex::new(None),
        }
    }

    /// The names a new template's time column is chosen from. Affects templates created
    /// after the call and nothing already recorded.
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

    /// A trie rebuilt from catalog records, in record order.
    ///
    /// The last column record for a `(template, name)` wins, which is how a widening
    /// replays. Two template records with one id, or two with one path, keep the first
    /// and warn: the catalog is append-only and the writer never produces either, so a
    /// second is a file that has been edited or spliced, and the first is the one every
    /// leaf directory was named after.
    pub fn from_catalog(
        data_dir: &str,
        records: &[Record],
        warn: &mut dyn FnMut(String),
    ) -> Result<Self, TrieError> {
        let time_columns = crate::config::Schema::default().time_columns;
        Ok(Self::from_catalog_with(data_dir, records, time_columns, warn)?.0)
    }

    /// `from_catalog`, with the configured time columns and the records a caller should
    /// append: one `time_source` for every template the catalog holds none for.
    ///
    /// A catalog written before the time source existed has no `time_source` records. The
    /// source of such a template is derived here by the same rule, over the column types
    /// the catalog does hold - the nearest thing to what its creating line would have
    /// said - and handed back so it can be made durable, because the choice is one that
    /// must not move between restarts.
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
                    // Intact, and no build wrote it: an id comes from a counter that
                    // stops at `LAST_TEMPLATE_ID`. Skipped with a WARN like any intact
                    // record the format does not describe, and never a failed
                    // start. Its column and time_source records name a template with no
                    // record before them, and are skipped as those are.
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
            // `max + 1`, so a gap in the sequence is never refilled: a leaf directory is
            // named by id whether or not the id's template record survived.
            //
            // Before the path is looked at, so the id of a record skipped just below is
            // kept from reuse too. The record stays in the file, and a new template given
            // its id would be the second record under it: the next start keeps the first
            // of the two, skips it again for its path, and the template that took the id
            // is gone with its rows still stored under it.
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
            *slot = Some(LeafMeta {
                template_id: id,
                columns: pending.columns.into_values().collect(),
                storage_path: format!("{data_dir}/leaf_{id}"),
                first_seen: pending.created_at,
                last_seen: pending.last_at,
                time_source,
            });
            leaf.is_leaf.store(true, Ordering::Release);
            trie.tokens
                .fetch_add(labels.len() as u64, Ordering::Relaxed);
        }
        Ok((trie, backfill))
    }

    /// Every template as catalog records, in id order: what writing a catalog from a
    /// `trie.json` produces, once, on the first start that finds no catalog.
    ///
    /// The snapshot stores a path as edge labels, and a label `event=login` is either a
    /// promoted `event` or a literal key spelled that way - the two render alike and the
    /// file did not say which. Split at the first `=` here: both readings rebuild the same
    /// label, so the trie is the same either way, and only the record's spelling can be
    /// wrong, on a catalog that predates the catalog.
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

    pub fn data_dir(&self) -> &str {
        &self.data_dir
    }

    pub fn root(&self) -> Arc<Node> {
        Arc::clone(&self.root)
    }

    /// Sequential, from an `AtomicU64` fetch-and-increment. Ids are therefore handed out
    /// in the order templates are first seen, which is the order lines arrived in - the
    /// one part of a rendered tree that a reordered file moves.
    ///
    /// Checked, where a bare `fetch_add` wraps: past `LAST_TEMPLATE_ID` the counter has
    /// nothing to give and says so, every time it is asked, and does not start again from
    /// 0 through the ids already in use. A catalog naming `LAST_TEMPLATE_ID` was enough
    /// for that: the next template took `u64::MAX` and the one after it 0.
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

    /// The leaves that exist, counted - not the ids handed out.
    ///
    /// Those were the same number until `reserve_template_ids_through` started skipping
    /// the ids that leaf directories on disk have already spent. A trie recovered from a
    /// lost snapshot holds no templates and may still have reserved a hundred ids, and
    /// every caller of this wants the first number: the banner, `/stats`, and the line
    /// the shutdown prints.
    pub fn template_count(&self) -> u64 {
        self.all_leaves().len() as u64
    }

    /// Refuse to hand out a template id at or below one already spent.
    ///
    /// The counter's floor is normally `trie.json`, which records every id issued. But
    /// leaf directories are *named* by template id, so the ids they hold are spent whether
    /// or not anything still remembers issuing them - and when the snapshot is missing or
    /// unparsable the trie starts empty and the counter restarts from `FIRST_TEMPLATE_ID`
    /// while those directories are still sitting there.
    ///
    /// Reissuing one points a freshly learned template at another template's rows. The
    /// symptom is the bad kind: the two key sets are merged into one directory, so an
    /// unfiltered `MATCH` still returns everything and looks right, while a filtered one
    /// prunes to the template the trie names and silently misses the rows in the directory
    /// it actually opened. A wrong answer, not an error.
    ///
    /// `highest_used` is read from a file - a catalog record, a row group, a record in
    /// the write-ahead log - and may be any `u64`. Saturating, so the largest of them
    /// leaves the counter at `u64::MAX` with no id left to give (`next_template_id`),
    /// where `+ 1` panicked a start in a debug build and, in a release one, wrapped to 0
    /// and reserved nothing.
    pub fn reserve_template_ids_through(&self, highest_used: u64) {
        self.next_template_id
            .fetch_max(highest_used.saturating_add(1), Ordering::Relaxed);
    }

    /// What the same templates would cost with no prefix sharing at all: every template's
    /// path spelled out in full, hanging off the one root they cannot help but share.
    ///
    /// The root is not a token, but it is counted here for the same reason `node_count`
    /// counts it - on both sides of the comparison, so `token_count - node_count` is
    /// exactly the nodes shared prefixes removed and nothing else.
    pub fn token_count(&self) -> u64 {
        self.tokens.load(Ordering::Relaxed) + 1
    }

    /// Walk the tokens, creating what is missing, and return the last node - **not yet a
    /// leaf**. `ingest` marks it one only after the catalog record that describes it is on
    /// disk, so an interior node this creates and then abandons is the whole cost of a
    /// failed append: a few nodes nothing walks to, and no template.
    fn path_to(&self, tokens: &[&str]) -> Result<Arc<Node>, TrieError> {
        let mut node = Arc::clone(&self.root);
        for token in tokens {
            node = self.child_or_create(&node, token)?;
        }
        Ok(node)
    }

    /// Find the child for `token`, creating it if it is genuinely absent.
    ///
    /// Read lock first: the overwhelmingly common case is a child that already exists and
    /// nothing to write. Escalate to the write lock only when the child is missing - and
    /// then re-check under it, because the lock was dropped in between and another thread
    /// may have created the same child in that gap. Without the re-check the two threads
    /// build two nodes wearing one label, and the second silently owns a template's worth
    /// of rows nothing can walk to.
    ///
    /// Built this way while the program is still single-threaded on purpose: the lock is
    /// per node with no global one, and retrofitting that means touching every call site.
    fn child_or_create(&self, node: &Arc<Node>, token: &str) -> Result<Arc<Node>, TrieError> {
        if let Some(child) = exact_child(&read(&node.children)?, token) {
            return Ok(child);
        }

        let mut children = write(&node.children)?;
        if let Some(child) = exact_child(&children, token) {
            return Ok(child);
        }

        // Exact match only, even though `walk` falls back to `[*]`. Insert has to
        // create the static edge the token names; matching an existing placeholder here
        // would file a new template under a wildcard and lose the specific one.
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

    /// Walk an exact token list. `None` is "no such path" - the trie has never seen this
    /// shape - and must stay distinguishable from a leaf that holds no rows.
    ///
    /// `None` on a poisoned lock too: the signature has no room to say more, and an
    /// unwalkable trie and an absent path are the same answer to the caller.
    pub fn walk(&self, tokens: &[&str]) -> Option<WalkResult> {
        let mut node = Arc::clone(&self.root);
        for token in tokens {
            let next = {
                let children = read(&node.children).ok()?;
                find_child(&children, token)?
            };
            node = next;
        }
        let is_leaf = node.is_leaf.load(Ordering::Acquire);
        let template_id = node
            .leaf_meta
            .read()
            .ok()?
            .as_ref()
            .map(|meta| meta.template_id);
        Some(WalkResult {
            node,
            is_leaf,
            template_id,
        })
    }

    /// Every leaf under `prefix`. Omitted keys in a query pattern act as wildcards, so a
    /// partial pattern selects a subtree rather than a single leaf - which works only
    /// because walk order hoists the promoted tokens to the top of the trie.
    ///
    /// An unmatched prefix is an empty result, not an error: nothing went wrong, there is
    /// simply nothing there.
    pub fn walk_prefix(&self, prefix: &[&str]) -> Result<Vec<Arc<Node>>, TrieError> {
        let mut node = Arc::clone(&self.root);
        for token in prefix {
            let next = {
                let children = read(&node.children)?;
                find_child(&children, token)
            };
            match next {
                Some(child) => node = child,
                None => return Ok(Vec::new()),
            }
        }
        let mut leaves = Vec::new();
        collect_leaves(&node, &mut leaves)?;
        Ok(leaves)
    }

    /// Every leaf in the trie, in path order - children are stored sorted, so this does
    /// not depend on the order lines arrived in.
    ///
    /// A poisoned lock truncates the subtree it guards rather than failing; the signature
    /// has nowhere to report it, and a partial list is what the caller gets.
    pub fn all_leaves(&self) -> Vec<Arc<Node>> {
        let mut leaves = Vec::new();
        let _ = collect_leaves(&self.root, &mut leaves);
        leaves
    }

    /// Every leaf with the edge labels that reach it, root excluded.
    ///
    /// `/templates` reports the template, and a template *is* its path: `LeafMeta` holds
    /// the columns but not which of them were promoted into the fingerprint, so the
    /// columns alone cannot say whether a leaf is `event=login_failed, ts, user` or some
    /// other arrangement of the same names. `PlannedLeaf::path` carries the same tokens
    /// for the same reason.
    ///
    /// Depth-first over children already sorted by label, so two runs over one file list
    /// the templates in the same order.
    pub fn leaf_paths(&self) -> Vec<(Vec<String>, Arc<Node>)> {
        let mut out = Vec::new();
        let mut path = Vec::new();
        let _ = collect_leaf_paths(&self.root, &mut path, &mut out);
        out
    }

    /// Put one parsed line into the trie: walk order to a leaf, then fold this line's
    /// types into that leaf's columns.
    ///
    /// The fold is `types::join`'s ⊔, applied on *every* ingest rather than written once
    /// from the line that created the template - which is the point of the lattice.
    /// Commutative and associative, so the column type does not depend on arrival order;
    /// monotonic, so a ten-digit order id the ladder called a `Datetime` widens back to
    /// `String` the first time it meets `AX-99213`.
    ///
    /// **The catalog is written first.** A new template, a widened column or a column
    /// added to an existing leaf is appended and fsynced before the leaf changes, under
    /// the leaf's meta lock so nothing reads the half-made state. If the append fails the
    /// line is refused with `TrieError::Catalog`, the leaf is exactly as it was, and the
    /// template id drawn for a new template is left unused - a gap in the sequence, which
    /// the restart path already tolerates, rather than an id two lines could both hold.
    pub fn ingest(
        &self,
        parsed: &ParsedLog,
        now: DateTime<Utc>,
    ) -> Result<(Arc<Node>, bool), TrieError> {
        let tokens = parsed.walk_tokens();
        let borrowed: Vec<&str> = tokens.iter().map(String::as_str).collect();
        let leaf = self.path_to(&borrowed)?;

        // Scoped, so the write lock is released before the leaf is handed back - a
        // caller that goes straight on to read the columns would otherwise deadlock on
        // a guard it cannot see.
        let mut slot = write(&leaf.leaf_meta)?;
        let is_new = match slot.as_mut() {
            None => {
                let template_id = self.next_template_id()?;
                // `keys` is sorted, so the columns are born in name order - the order
                // `binary_search_by` below depends on.
                let columns: Vec<ColumnDef> = parsed
                    .keys
                    .iter()
                    .map(|key| ColumnDef {
                        name: key.clone(),
                        col_type: parsed.column_type(key),
                        widened_at: None,
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
                    widened_from: None,
                    at: None,
                }));
                // The time source: the first configured name that is one of
                // this line's keys and read as a `Datetime`. Chosen once, in the same
                // durable unit as the template, and never changed.
                let time_source =
                    TimeSource::choose(&self.time_columns, |name| parsed.column_type(name));
                records.push(Record::TimeSource {
                    template_id,
                    source: time_source.as_record(),
                });
                self.record(&records)?;

                if time_source == TimeSource::Arrival {
                    // One WARN per template, naming the keys an operator could add to
                    // `time_columns` to give it a time of its own.
                    eprintln!(
                        "kibbr: WARN template {template_id} has no time column among \
                         [{}]; its rows take their arrival time. Datetime-typed keys: {}",
                        self.time_columns.join(", "),
                        datetime_keys(columns.iter().map(|c| (c.name.as_str(), c.col_type)))
                    );
                }

                *slot = Some(LeafMeta {
                    template_id,
                    columns,
                    storage_path: format!("{}/leaf_{template_id}", self.data_dir),
                    first_seen: now,
                    last_seen: now,
                    time_source,
                });
                leaf.is_leaf.store(true, Ordering::Release);
                self.tokens
                    .fetch_add(tokens.len() as u64, Ordering::Relaxed);
                true
            }
            Some(meta) => {
                // What this line changes, decided before anything is touched: the record
                // has to be durable first, and a change applied and then refused would
                // leave the trie ahead of the file.
                let mut records = Vec::new();
                let mut changes: Vec<(String, Option<ColumnType>, bool)> = Vec::new();
                for key in &parsed.keys {
                    let incoming = parsed.column_type(key);
                    match meta
                        .columns
                        .binary_search_by(|col| col.name.as_str().cmp(key))
                    {
                        Ok(at) => {
                            let col = &meta.columns[at];
                            let joined = join(col.col_type, incoming);
                            if joined != col.col_type {
                                // The moment a column widens: the one place an
                                // operator can learn that `"N/A"` just sent every
                                // time-range query on this column to a full scan.
                                // `None ⊔ T` is a column learning its first type, not a
                                // widening, and is not warned about.
                                if let (Some(from), Some(to)) = (col.col_type, joined) {
                                    eprintln!(
                                        "kibbr: WARN template {} column `{key}` widened \
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
                                    widened_from: col.col_type,
                                    at: Some(now),
                                });
                                changes.push((key.clone(), joined, true));
                            }
                        }
                        // A key not on the path, which one leaf per token multiset makes
                        // unreachable but not impossible: a promoted `event=login` and
                        // the literal key `{"event=login": 1}` share one edge label.
                        // Union the columns rather than drop the value - two schemas in
                        // one leaf is schema merging's to settle, and losing a column
                        // here would hide that it happened.
                        Err(_) => {
                            records.push(Record::Column {
                                template_id: meta.template_id,
                                name: key.clone(),
                                col_type: incoming,
                                widened_from: None,
                                at: None,
                            });
                            changes.push((key.clone(), incoming, false));
                        }
                    }
                }
                if !records.is_empty() {
                    self.record(&records)?;
                }
                for (name, col_type, widened) in changes {
                    match meta
                        .columns
                        .binary_search_by(|col| col.name.as_str().cmp(&name))
                    {
                        Ok(at) => {
                            meta.columns[at].col_type = col_type;
                            if widened {
                                meta.columns[at].widened_at = Some(now);
                            }
                        }
                        Err(at) => meta.columns.insert(
                            at,
                            ColumnDef {
                                name,
                                col_type,
                                widened_at: None,
                            },
                        ),
                    }
                }
                meta.last_seen = now;
                false
            }
        };
        drop(slot);
        Ok((leaf, is_new))
    }
}

/// The keys an operator could add to `time_columns` to give a template a time of its own -
/// those typed `Datetime`, comma-separated, or `none`. One spelling for the WARN a template
/// on arrival time is made with and for the bulk loader's refusal naming it, so the two
/// cannot name different keys for one template.
pub fn datetime_keys<'a>(
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
///
/// Unreachable while nothing builds a `Placeholder`, and here so that whatever eventually
/// builds one is matched by a walk that already exists.
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
mod tests {
    use super::*;
    use crate::flatten::parse_line;

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("kibbr-trie-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("mkdir");
        dir
    }

    fn parsed(line: &str) -> ParsedLog {
        parse_line(line.as_bytes(), 1).expect("a valid line")
    }

    /// The write rule: no record on disk, no template in memory. The line is refused
    /// with the catalog's reason, the trie walks to nothing, and a later line that needs
    /// no record still lands.
    #[test]
    fn a_failed_append_creates_no_template() {
        let dir = scratch("failed-append");
        let trie = Trie::new_in(&dir.display().to_string());
        let good = Catalog::open(&crate::catalog::path_in(&dir), &mut |_| {}).expect("open");
        trie.attach_catalog(good.0);
        let now = Utc::now();

        let first = parsed(r#"{"event":"login","user":7}"#);
        assert!(trie.ingest(&first, now).expect("first line").1);
        assert_eq!(trie.template_count(), 1);

        // Swap in a catalog that cannot be written to.
        trie.attach_catalog(Catalog::read_only(&dir.join("dead.log")).expect("read-only"));

        // A new template: refused, and not there.
        let second = parsed(r#"{"event":"logout","user":7}"#);
        let Err(err) = trie.ingest(&second, now) else {
            panic!("the append must fail");
        };
        assert!(matches!(err, TrieError::Catalog(_)), "{err}");
        assert!(err.to_string().contains("catalog"), "{err}");
        assert_eq!(
            trie.template_count(),
            1,
            "the refused template exists in memory"
        );
        let tokens = second.walk_tokens();
        let borrowed: Vec<&str> = tokens.iter().map(String::as_str).collect();
        let walked = trie.walk(&borrowed).expect("the interior nodes may exist");
        assert!(!walked.is_leaf, "the refused template is a leaf");
        assert_eq!(walked.template_id, None);

        // A widening on the existing template (Int64 ⊔ String): refused, and the column
        // unchanged.
        let widen = parsed(r#"{"event":"login","user":"u1"}"#);
        assert!(matches!(
            trie.ingest(&widen, now),
            Err(TrieError::Catalog(_))
        ));
        let leaf = trie.all_leaves().into_iter().next().expect("one leaf");
        let slot = leaf.leaf_meta.read().expect("meta");
        let user = slot
            .as_ref()
            .expect("meta")
            .columns
            .iter()
            .find(|c| c.name == "user")
            .expect("user column");
        assert_eq!(user.col_type, Some(ColumnType::Int64));
        assert_eq!(user.widened_at, None);
        drop(slot);

        // A repeat of the first line needs no record, so it still goes in.
        assert!(
            !trie
                .ingest(&first, now)
                .expect("a repeat needs no record")
                .1
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// What `ingest` writes is what `from_catalog` rebuilds: same paths, same ids, same
    /// column types including the two the segment format cannot spell, and the widening.
    #[test]
    fn a_trie_round_trips_through_its_catalog() {
        let dir = scratch("round-trip");
        let path = crate::catalog::path_in(&dir);
        let data_dir = dir.display().to_string();
        let trie = Trie::new_in(&data_dir);
        trie.attach_catalog(Catalog::open(&path, &mut |_| {}).expect("open").0);
        let now = Utc::now();

        for line in [
            r#"{"event":"login","session":"123e4567-e89b-12d3-a456-426614174000","ip":"10.0.0.1","n":null}"#,
            r#"{"event":"login","session":"123e4567-e89b-12d3-a456-426614174000","ip":"10.0.0.1","n":7}"#,
            r#"{"event":"login","session":"not-a-uuid","ip":"10.0.0.1","n":7}"#,
            r#"{"event":"pay","amount":12.5}"#,
        ] {
            trie.ingest(&parsed(line), now).expect("ingest");
        }
        assert_eq!(trie.template_count(), 2);

        let (_, records) = Catalog::open(&path, &mut |_| {}).expect("reopen");
        let mut warnings = Vec::new();
        let rebuilt =
            Trie::from_catalog(&data_dir, &records, &mut |w| warnings.push(w)).expect("rebuild");
        assert!(warnings.is_empty(), "{warnings:?}");

        let describe = |t: &Trie| -> Vec<String> {
            t.leaf_paths()
                .into_iter()
                .map(|(path, leaf)| {
                    let slot = leaf.leaf_meta.read().expect("meta");
                    let meta = slot.as_ref().expect("meta");
                    let cols: Vec<String> = meta
                        .columns
                        .iter()
                        .map(|c| {
                            format!(
                                "{}:{}{}",
                                c.name,
                                c.col_type.map_or("null", ColumnType::as_str),
                                if c.widened_at.is_some() { "*" } else { "" }
                            )
                        })
                        .collect();
                    format!(
                        "{} {} [{}]",
                        meta.template_id,
                        path.join("/"),
                        cols.join(",")
                    )
                })
                .collect()
        };
        let before = describe(&trie);
        assert_eq!(before, describe(&rebuilt));
        assert!(
            before
                .iter()
                .any(|d| d.contains("ip:Ip") && d.contains("session:String*")),
            "{before:?}"
        );
        assert!(before.iter().any(|d| d.contains("n:Int64")), "{before:?}");
        assert_eq!(
            rebuilt.next_template_id().expect("an id"),
            trie.next_template_id().expect("an id")
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn template_record(id: u64, key: &str) -> Record {
        Record::Template {
            id,
            created_at: DateTime::from_timestamp(1_705_314_600, 0).expect("a valid timestamp"),
            tokens: vec![Token::Key { key: key.into() }],
        }
    }

    /// A template record whose id has no `max + 1` is skipped with a WARN that names it,
    /// with the column and time_source records that name it, and the start goes on: the
    /// template beside it is rebuilt and the next id follows it. The `max + 1` of that id
    /// panicked the start in a debug build.
    #[test]
    fn a_template_id_with_no_next_id_is_skipped_and_never_fails_a_start() {
        let records = vec![
            template_record(u64::MAX, "ts"),
            Record::Column {
                template_id: u64::MAX,
                name: "ts".into(),
                col_type: Some(ColumnType::Datetime),
                widened_from: None,
                at: None,
            },
            Record::TimeSource {
                template_id: u64::MAX,
                source: "column:ts".into(),
            },
            template_record(FIRST_TEMPLATE_ID, "user"),
        ];
        let mut warnings = Vec::new();
        let rebuilt =
            Trie::from_catalog("data", &records, &mut |w| warnings.push(w)).expect("rebuild");
        assert_eq!(rebuilt.template_count(), 1);
        assert!(rebuilt.walk(&["ts"]).is_none());
        assert_eq!(warnings.len(), 3, "{warnings:?}");
        assert!(
            warnings[0].contains(&format!("id {}", u64::MAX)) && warnings[0].contains("skipped"),
            "{warnings:?}"
        );
        assert_eq!(
            rebuilt.next_template_id().expect("an id"),
            FIRST_TEMPLATE_ID + 1
        );
    }

    /// A template record skipped for a path another template holds still names its id,
    /// and the id is not given again. It was: the next template took 102 here, its record
    /// was the second under that id, and the start after that kept the first of the two
    /// and skipped it for its path - one template where two had been ingested into.
    #[test]
    fn the_id_of_a_template_record_skipped_for_its_path_is_not_given_again() {
        let mut records = vec![
            template_record(FIRST_TEMPLATE_ID, "user"),
            template_record(FIRST_TEMPLATE_ID + 1, "user"),
        ];
        let mut warnings = Vec::new();
        let rebuilt =
            Trie::from_catalog("data", &records, &mut |w| warnings.push(w)).expect("rebuild");
        assert_eq!(rebuilt.template_count(), 1);
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("the same path"), "{warnings:?}");

        let (leaf, is_new) = rebuilt
            .ingest(&parsed(r#"{"other":7}"#), Utc::now())
            .expect("a new template");
        assert!(is_new);
        let id = leaf
            .leaf_meta
            .read()
            .expect("meta")
            .as_ref()
            .map(|meta| meta.template_id)
            .expect("a template");
        assert_eq!(id, FIRST_TEMPLATE_ID + 2);

        // What the catalog holds once that template is recorded, read by the next start.
        records.push(template_record(id, "other"));
        let mut warnings = Vec::new();
        let again =
            Trie::from_catalog("data", &records, &mut |w| warnings.push(w)).expect("rebuild");
        assert_eq!(again.template_count(), 2);
        assert!(again.walk(&["other"]).is_some());
        assert_eq!(warnings.len(), 1, "{warnings:?}");
    }

    /// The last id is handed out once and nothing after it: not 0, not an id again. A
    /// line that needs a new template is refused, the template that holds the last id
    /// still takes its lines, and reserving through `u64::MAX` - an id a WAL record or a
    /// row group could name - is not a panic and frees nothing.
    #[test]
    fn the_last_template_id_is_given_once_and_no_id_is_given_twice() {
        let trie = Trie::new();
        trie.reserve_template_ids_through(LAST_TEMPLATE_ID - 1);
        assert_eq!(
            trie.next_template_id().expect("the last id"),
            LAST_TEMPLATE_ID
        );
        for _ in 0..3 {
            assert!(matches!(
                trie.next_template_id(),
                Err(TrieError::TemplateIdsSpent)
            ));
        }

        let mut warnings = Vec::new();
        let rebuilt = Trie::from_catalog(
            "data",
            &[template_record(LAST_TEMPLATE_ID, "user")],
            &mut |w| warnings.push(w),
        )
        .expect("rebuild");
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(rebuilt.template_count(), 1);
        rebuilt.reserve_template_ids_through(u64::MAX);
        let now = Utc::now();
        let (leaf, is_new) = rebuilt
            .ingest(&parsed(r#"{"user":7}"#), now)
            .expect("a line of the template that exists");
        assert!(!is_new);
        let id = leaf
            .leaf_meta
            .read()
            .expect("meta")
            .as_ref()
            .map(|meta| meta.template_id);
        assert_eq!(id, Some(LAST_TEMPLATE_ID));
        assert!(matches!(
            rebuilt.ingest(&parsed(r#"{"other":7}"#), now),
            Err(TrieError::TemplateIdsSpent)
        ));
        assert_eq!(rebuilt.template_count(), 1);
    }
}
