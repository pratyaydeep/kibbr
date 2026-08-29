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

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use chrono::{DateTime, Utc};
use thiserror::Error;

use crate::flatten::ParsedLog;
use crate::types::{join, ColumnType};

/// The root carries a label so that a rendered tree and a serialized one agree about
/// what sits above the first edge.
pub const ROOT_LABEL: &str = "__root__";

/// How a `Placeholder` renders and, therefore, the label `walk` falls back to.
pub const PLACEHOLDER_LABEL: &str = "[*]";

/// Template ids start here rather than at 0 so that a leaf id is never confused with a
/// node id in a log line or a query plan: they are two counters over the same trie, and
/// an explained query prints both.
const FIRST_TEMPLATE_ID: u64 = 101;

/// Where a leaf's columns will live once there is a column store. Recorded now because
/// `LeafMeta` is written once per template and there is no configuration file to read a
/// data directory out of yet; nothing opens it at this point.
const DEFAULT_DATA_DIR: &str = "/var/lib/triedb/data";

/// The only way a trie operation fails.
///
/// A poisoned lock means some thread panicked while holding it, so the children below
/// that node may be half-inserted. There is nothing to recover here - the caller decides
/// whether that ends the run.
#[derive(Debug, Error)]
pub enum TrieError {
    #[error("trie node lock poisoned by a panic in another thread")]
    LockPoisoned,
}

/// `Static` is an exact token; `Placeholder` matches any token in this position.
///
/// Nothing produces a `Placeholder` yet: schema merging, when it arrives, reuses an
/// existing leaf rather than introducing a wildcard edge, so every node `insert` builds
/// is `Static`. The variant and the preference in `find_child` are the contract for
/// whenever a producer exists - most likely a label whose distinct-value count crosses a
/// bound - and until then the rule is unreachable rather than wrong.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
}

pub struct Trie {
    root: Arc<Node>,
    /// Node ids. The root took 0, so this is also the count of nodes below it.
    next_id: AtomicU64,
    next_template_id: AtomicU64,
    /// Path length summed over templates, not over lines - one per edge, counted once
    /// per template rather than once per line that walked it.
    tokens: AtomicU64,
}

impl Default for Trie {
    fn default() -> Self {
        Self::new()
    }
}

impl Trie {
    pub fn new() -> Self {
        Self {
            root: Arc::new(Node::new(0, ROOT_LABEL, NodeType::Static)),
            next_id: AtomicU64::new(1),
            next_template_id: AtomicU64::new(FIRST_TEMPLATE_ID),
            tokens: AtomicU64::new(0),
        }
    }

    pub fn root(&self) -> Arc<Node> {
        Arc::clone(&self.root)
    }

    /// Sequential, from an `AtomicU64` fetch-and-increment. Ids are therefore handed out
    /// in the order templates are first seen, which is the order lines arrived in - the
    /// one part of a rendered tree that a reordered file moves.
    pub fn next_template_id(&self) -> u64 {
        self.next_template_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Every node in the trie, `__root__` included - ids are handed out from 1 and the
    /// root took 0, so the next id is also the count.
    pub fn node_count(&self) -> u64 {
        self.next_id.load(Ordering::Relaxed)
    }

    pub fn template_count(&self) -> u64 {
        self.next_template_id.load(Ordering::Relaxed) - FIRST_TEMPLATE_ID
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

    /// Walk the tokens, creating what is missing, and mark the last node a leaf.
    /// The `bool` is whether this call is the one that turned it into a leaf - the
    /// caller stamps a template id on exactly that call.
    pub fn insert(&self, tokens: &[&str]) -> Result<(Arc<Node>, bool), TrieError> {
        let mut node = Arc::clone(&self.root);
        for token in tokens {
            node = self.child_or_create(&node, token)?;
        }
        // `swap`, not a load followed by a store: two threads inserting the same new
        // template must not both come back holding "I created it".
        let is_new = !node.is_leaf.swap(true, Ordering::AcqRel);
        if is_new {
            self.tokens
                .fetch_add(tokens.len() as u64, Ordering::Relaxed);
        }
        Ok((node, is_new))
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
    pub fn ingest(
        &self,
        parsed: &ParsedLog,
        now: DateTime<Utc>,
    ) -> Result<(Arc<Node>, bool), TrieError> {
        let tokens = parsed.walk_tokens();
        let borrowed: Vec<&str> = tokens.iter().map(String::as_str).collect();
        let (leaf, is_new) = self.insert(&borrowed)?;

        // Scoped, so the write lock is released before the leaf is handed back - a
        // caller that goes straight on to read the columns would otherwise deadlock on
        // a guard it cannot see.
        {
            let mut slot = write(&leaf.leaf_meta)?;
            // `_with`, so an id is drawn only for a leaf without one. Drawing first and
            // discarding would leave a gap in the sequence for every repeat line - which
            // is every line but 869 of them.
            let meta = slot.get_or_insert_with(|| {
                let template_id = self.next_template_id();
                LeafMeta {
                    template_id,
                    columns: Vec::new(),
                    storage_path: format!("{DEFAULT_DATA_DIR}/leaf_{template_id}"),
                    first_seen: now,
                    last_seen: now,
                }
            });
            meta.last_seen = now;

            for key in &parsed.keys {
                let incoming = parsed.column_type(key);
                match meta
                    .columns
                    .binary_search_by(|col| col.name.as_str().cmp(key))
                {
                    Ok(at) => {
                        let col = &mut meta.columns[at];
                        let joined = join(col.col_type, incoming);
                        if joined != col.col_type {
                            col.col_type = joined;
                            col.widened_at = Some(now);
                        }
                    }
                    // The creating line lands here for every key, so a column is born with
                    // the type of the first non-null value it held and `widened_at: None`
                    // - one type since `first_seen`, which is what that field is for.
                    //
                    // A later line reaching this arm means a key not on the path, which
                    // one leaf per token multiset makes unreachable but not impossible: a
                    // promoted `event=login` and the literal key `{"event=login": 1}`
                    // share one edge label. Union the columns rather than drop the value -
                    // two schemas in one leaf is schema merging's to settle, and losing a
                    // column here would hide that it happened.
                    Err(at) => meta.columns.insert(
                        at,
                        ColumnDef {
                            name: key.clone(),
                            col_type: incoming,
                            widened_at: None,
                        },
                    ),
                }
            }
        }

        Ok((leaf, is_new))
    }
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
