//! The query engine: two phases, and the result shape every surface above reports.
//!
//! Phase 1 walks the trie and touches no data. Phase 2 opens only the leaves phase 1
//! chose. The split is enforced by the types: `plan` cannot see `Stores`, so `--explain`
//! cannot accidentally read a row.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use crate::fmt::col_text;
use crate::infer::infer;
use crate::store::{Predicate, Row, ScanOptions, StoreError, Stores};
use crate::trie::{Node, Trie};
use crate::types::ColValue;

/// Joins a multi-key group label. A unit separator, because any printable delimiter is
/// something a value is entitled to contain.
pub const GROUP_SEP: char = '\u{1f}';

/// The HTTP `/query` surface and the Elasticsearch translation layer both consume this
/// shape unchanged, so changing it later means changing both.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryMode {
    Exists,
    Fetch,
    Count,
    GroupBy(Vec<String>),
}

#[derive(Debug, Default)]
pub struct QueryResult {
    /// `false` = no such template. Distinct from `exists: true, count: 0`, and
    /// every layer above depends on the difference.
    pub exists: bool,
    pub count: u64,
    pub rows: Vec<Row>,
    pub group_counts: HashMap<String, u64>,
    pub meta: QueryMeta,
}

#[derive(Debug, Default)]
pub struct QueryMeta {
    pub templates_matched: usize,
    pub template_ids: Vec<u64>,
    pub files_opened: usize,
    pub rows_scanned: u64,
    pub rows_returned: u64,
    /// Phase 1 plus phase 2. Loading the file is not query time - at M2 the CLI has to
    /// build the trie first because nothing persists it yet.
    pub duration: Duration,
}

/// One `--match KEY=VALUE`.
#[derive(Debug, Clone, PartialEq)]
pub struct Match {
    pub key: String,
    pub value: MatchValue,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MatchValue {
    /// `*` - the key is present and not null.
    Any,
    Exact(String),
}

#[derive(Debug, Default, Clone, Copy)]
pub struct TimeRange {
    pub since: Option<SystemTime>,
    pub until: Option<SystemTime>,
}

impl TimeRange {
    pub fn is_open(&self) -> bool {
        self.since.is_none() && self.until.is_none()
    }
}

/// A query, already built. Nothing here parses text - the query language is a later
/// milestone, and the CLI constructs this from flags.
#[derive(Debug)]
pub struct Query {
    pub matches: Vec<Match>,
    pub mode: QueryMode,
    pub time: TimeRange,
    /// 0 = unlimited.
    pub limit: usize,
}

impl Query {
    /// The pattern as trie tokens, for `--explain`.
    pub fn tokens(&self) -> Vec<String> {
        self.matches
            .iter()
            .map(|m| match &m.value {
                MatchValue::Any => format!("{}=*", m.key),
                MatchValue::Exact(v) => format!("{}={v}", m.key),
            })
            .collect()
    }

    pub fn scan_options(&self) -> ScanOptions {
        ScanOptions {
            columns: Vec::new(),
            limit: self.limit,
            since: self.time.since,
            until: self.time.until,
            ts_column: None,
        }
    }
}

// ── Phase 1 ────────────────────────────────────────────────────────────────────────

/// What phase 1 decided, and nothing else. No rows, no store handles.
#[derive(Debug, Default)]
pub struct Plan {
    pub leaves: Vec<PlannedLeaf>,
    /// Root subtrees the trie let us skip without looking inside.
    pub skipped: usize,
    pub duration: Duration,
}

#[derive(Debug)]
pub struct PlannedLeaf {
    pub template_id: u64,
    /// Phase 1 collects the storage paths, and nothing opens one while the store is a
    /// `Vec<Row>`. It is what phase 2 will hand to the file layer once there is one.
    #[allow(dead_code)]
    pub storage_path: String,
    /// Terms the path could not settle - phase 2's predicates. Empty means the leaf
    /// answers the pattern by its shape alone and every row in it matches.
    pub residual: Vec<Match>,
}

/// Walk the trie, collect the leaves that could hold a matching row. Reads no data.
///
/// Cost is the trie's size, not the file's - the property `--explain` is timed on.
pub fn plan(trie: &Trie, query: &Query) -> Plan {
    let started = Instant::now();
    let root = trie.root();
    let mut found = Vec::new();
    let mut skipped = 0;

    // A line with no keys at all (`{}`) walks zero tokens, so the root itself can be a
    // leaf. Rare, and the hostile corpus has it.
    if root.is_leaf.load(Ordering::Acquire) {
        found.push((Vec::new(), Arc::clone(&root)));
    }
    if let Ok(children) = root.children.read() {
        for edge in children.iter() {
            if !keeps(&edge.label, query) {
                skipped += 1;
                continue;
            }
            let mut path = vec![edge.label.clone()];
            collect(&edge.child, &mut path, &mut found);
        }
    }

    let mut leaves = Vec::new();
    for (path, node) in found {
        let Ok(slot) = node.leaf_meta.read() else {
            continue;
        };
        let Some(meta) = slot.as_ref() else {
            continue;
        };
        let columns: BTreeSet<&str> = meta.columns.iter().map(|c| c.name.as_str()).collect();
        if let Some(residual) = select(&path, &columns, query) {
            leaves.push(PlannedLeaf {
                template_id: meta.template_id,
                storage_path: meta.storage_path.clone(),
                residual,
            });
        }
    }
    leaves.sort_by_key(|leaf| leaf.template_id);

    Plan {
        leaves,
        skipped,
        duration: started.elapsed(),
    }
}

/// Whether a root subtree can be skipped outright.
///
/// Promoted tokens are hoisted to the front of the path (`ParsedLog::walk_tokens`), so a
/// root label `event=login_ok` is the only thing an exact `--match event=…` can reach.
/// A label that starts with `KEY=` and is not the wanted token belongs either to a
/// different promoted value or to a literal key *named* `KEY=…` - and that leaf's key set
/// then holds `KEY=…` rather than `KEY`, so `select` would reject it anyway.
///
/// A pattern over non-promoted keys prunes nothing and costs one `starts_with` per root
/// child. Correctness never rests on this: `select` re-checks every leaf that survives.
fn keeps(label: &str, query: &Query) -> bool {
    query.matches.iter().all(|m| match &m.value {
        MatchValue::Exact(v) => {
            let token = format!("{}={v}", m.key);
            label == token || !label.starts_with(&format!("{}=", m.key))
        }
        MatchValue::Any => true,
    })
}

fn collect(node: &Arc<Node>, path: &mut Vec<String>, out: &mut Vec<(Vec<String>, Arc<Node>)>) {
    if node.is_leaf.load(Ordering::Acquire) {
        out.push((path.clone(), Arc::clone(node)));
    }
    let Ok(children) = node.children.read() else {
        return;
    };
    for edge in children.iter() {
        path.push(edge.label.clone());
        collect(&edge.child, path, out);
        path.pop();
    }
}

/// Test one leaf against the pattern. `None` = this leaf cannot match; `Some(residual)` =
/// it can, once phase 2 has applied the terms listed.
fn select(path: &[String], columns: &BTreeSet<&str>, query: &Query) -> Option<Vec<Match>> {
    let promoted = promoted_of(path, columns);
    let mut residual = Vec::new();
    for term in &query.matches {
        // The key set is the path, so a template without the key cannot hold a row with
        // it. This is the whole of phase 1's pruning for non-promoted keys.
        if !columns.contains(term.key.as_str()) {
            return None;
        }
        match promoted.get(term.key.as_str()) {
            // The value is in the path, so the leaf settles the term now and every row
            // in it agrees - no predicate, and phase 2 has nothing to check.
            Some(&have) => match &term.value {
                MatchValue::Any => {}
                MatchValue::Exact(want) if want == have => {}
                MatchValue::Exact(_) => return None,
            },
            None => residual.push(term.clone()),
        }
    }
    Some(residual)
}

/// The `key=value` tokens on this path, split back into pairs.
///
/// A plain-key token is exactly a column name and a promoted token is not, so membership
/// in the key set tells the two apart without consulting the promotion rules.
fn promoted_of<'a>(path: &'a [String], columns: &BTreeSet<&str>) -> BTreeMap<&'a str, &'a str> {
    let mut out = BTreeMap::new();
    for label in path {
        if columns.contains(label.as_str()) {
            continue;
        }
        if let Some((key, value)) = label.split_once('=') {
            if columns.contains(key) {
                out.insert(key, value);
            }
        }
    }
    out
}

// ── Phase 2 ────────────────────────────────────────────────────────────────────────

/// Run both phases. `QueryMode::Exists` stops after phase 1 - the question is about the
/// template, and answering it needs no rows.
pub fn execute(trie: &Trie, stores: &Stores, query: &Query) -> Result<QueryResult, StoreError> {
    let plan = plan(trie, query);
    let mut result = QueryResult {
        exists: !plan.leaves.is_empty(),
        meta: QueryMeta {
            templates_matched: plan.leaves.len(),
            template_ids: plan.leaves.iter().map(|l| l.template_id).collect(),
            duration: plan.duration,
            ..QueryMeta::default()
        },
        ..QueryResult::default()
    };
    if query.mode == QueryMode::Exists {
        return Ok(result);
    }

    let started = Instant::now();
    let mut opts = query.scan_options();
    let mut remaining = query.limit;

    for leaf in &plan.leaves {
        // A planned leaf with no store is a known template holding no rows: the second
        // of the two empty answers, reached without an error.
        let Some(store) = stores.lookup(leaf.template_id) else {
            continue;
        };
        result.meta.files_opened += 1;
        opts.limit = remaining;
        let preds: Vec<Predicate> = leaf.residual.iter().map(predicate).collect();

        let scan = match &query.mode {
            QueryMode::Count => store.visit(&opts, &preds, &mut |_| {})?,
            QueryMode::Fetch => {
                let mut scan = store.scan(&opts, &preds)?;
                result.rows.append(&mut scan.rows);
                scan
            }
            QueryMode::GroupBy(keys) => {
                let counts = &mut result.group_counts;
                store.visit(&opts, &preds, &mut |row| {
                    *counts.entry(group_key(row, keys)).or_insert(0) += 1;
                })?
            }
            QueryMode::Exists => unreachable!("returned above"),
        };

        result.meta.rows_scanned += scan.scanned;
        result.count += scan.matched;
        if query.limit != 0 {
            remaining = query.limit.saturating_sub(result.count as usize);
            if remaining == 0 {
                break;
            }
        }
    }

    result.meta.rows_returned = result.count;
    result.meta.duration = plan.duration + started.elapsed();
    Ok(result)
}

/// One residual term as a phase 2 predicate.
fn predicate(term: &Match) -> Predicate {
    let key = term.key.clone();
    let want = term.value.clone();
    Box::new(move |row: &Row| row.get(&key).is_some_and(|have| value_matches(&want, have)))
}

/// Does a stored value satisfy `KEY=VALUE`?
///
/// The contract, shared verbatim with the reference scanner - a differential test is only
/// worth running if both sides are answering the same question.
///
/// Two readings of the flag's text, because a `Row` holds `ColValue` and not the raw line.
/// A `Datetime` column kept nanoseconds and threw the text away, so `ts=2024-01-15T10:30:00Z`
/// can only be matched by putting the flag through the same ladder the value went through.
/// The string reading comes first so a JSON string `"149.00"` is matched by its own text
/// rather than by the float it would infer to.
pub fn value_matches(want: &MatchValue, stored: &ColValue) -> bool {
    match want {
        MatchValue::Any => !matches!(stored, ColValue::Null),
        MatchValue::Exact(text) => {
            *stored == ColValue::String(text.clone()) || *stored == infer(text).value
        }
    }
}

/// A row's group label. Missing and null share one bucket: neither is a value, and
/// splitting them would put two labels on one absent column.
pub fn group_key(row: &Row, keys: &[String]) -> String {
    keys.iter()
        .map(|key| row.get(key).map_or_else(|| col_text(&ColValue::Null), col_text))
        .collect::<Vec<_>>()
        .join(&GROUP_SEP.to_string())
}
