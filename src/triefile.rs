//! `trie.json`: the trie as an adjacency list.
//!
//! `Arc<Node>` pointers become integer ids; ids are stable identifiers and `next_id` is
//! restored on load. Written atomically on graceful shutdown - `.tmp`, fsync, rename - so
//! a reader sees the whole file or the previous one.
//!
//! **A `trie.json` that fails to parse logs a WARN and starts with an empty trie. Never a
//! hard failure.** That is not leniency, it is the difference between a bad file and a
//! database that will not start. An operator cannot repair a corrupt trie by hand - it is
//! a graph of integers - so refusing to start leaves them with a crash loop and no way
//! out, while an empty trie is a database that comes up and relearns its templates from
//! the next lines it is sent. Every database that got this wrong learned it the same way.
//!
//! The same reasoning runs one level down, which is why the validation below is as
//! suspicious as it is: a file that parses as JSON but describes a graph that is not a
//! tree would otherwise build an `Arc` cycle that never drops, or a children list that is
//! not sorted and so loses half its edges to a binary search. Both are quieter than a
//! parse error and worse.

// `load` has no caller - `serve` wants the reason, so it calls `load_reporting`. Kept
// because the signature is the policy: a function that returns a `Trie` and not a
// `Result<Trie>` is one no caller can make fatal by accident, and that is the rule this
// module exists to enforce.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::trie::{ColumnDef, LeafMeta, Node, NodeType, Trie, ROOT_LABEL};
use crate::types::{ColumnType, TimeSource};

/// How deep a file may describe before it is refused.
///
/// A hand-edited file must not be able to end the process, and a recursive builder over an
/// adjacency list of a hundred thousand nodes in a line would overflow the stack. The
/// builder below uses an explicit stack, so this is belt and braces - but the whole point
/// of this module is that a bad file is a warning.
const MAX_DEPTH: usize = 4096;

#[derive(Debug, Error)]
pub enum TrieFileError {
    #[error("{path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("{path}: not valid JSON: {source}")]
    Parse {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("node id {id} appears more than once")]
    DuplicateId { id: u64 },
    #[error("there is no node with id 0, so the file describes no root")]
    NoRoot,
    #[error("node {parent} has a child {child}, and there is no node with that id")]
    DanglingChild { parent: u64, child: u64 },
    #[error(
        "{edges} edge(s) over {nodes} node(s): a tree has exactly one fewer edge than it has nodes, \
         so this file describes a node with two parents or one with none"
    )]
    NotATree { edges: usize, nodes: usize },
    #[error("{unreachable} node(s) cannot be reached from the root")]
    Unreachable { unreachable: usize },
    #[error("node {id} has two children labelled `{label}`")]
    DuplicateEdge { id: u64, label: String },
    #[error(
        "template id {template_id} belongs to two leaves, which would share one storage directory"
    )]
    DuplicateTemplate { template_id: u64 },
    #[error("the file describes a path deeper than {MAX_DEPTH} nodes")]
    TooDeep,
    #[error("the trie lock was poisoned by a panic, so part of the tree could not be read")]
    LockPoisoned,
}

// ── The file ───────────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
struct TrieFile {
    next_id: u64,
    nodes: Vec<NodeRecord>,
}

#[derive(Debug, Serialize, Deserialize)]
struct NodeRecord {
    id: u64,
    label: String,
    node_type: NodeType,
    is_leaf: bool,
    leaf_meta: Option<LeafMetaRecord>,
    children: Vec<EdgeRecord>,
}

#[derive(Debug, Serialize, Deserialize)]
struct EdgeRecord {
    label: String,
    child_id: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct LeafMetaRecord {
    template_id: u64,
    columns: Vec<ColumnRecord>,
    storage_path: String,
    first_seen: DateTime<Utc>,
    last_seen: DateTime<Utc>,
    /// `column:<name>` or `arrival`. Absent in a snapshot from before the time source, in
    /// which case the source is derived from the column types with the default list -
    /// the same rule the catalog applies to a record-less template.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    time_source: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ColumnRecord {
    name: String,
    col_type: Option<ColumnType>,
    /// Absent rather than `null` for a column that never widened, so an ordinary file
    /// reads the way the format documents it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    widened_at: Option<DateTime<Utc>>,
}

// ── Saving ─────────────────────────────────────────────────────────────────────────

/// Write the trie, returning the byte length.
///
/// `.tmp` → fsync → rename → fsync the parent directory. The rename is the atomic step:
/// interrupted anywhere before it, the previous file is untouched, and `trie.json.tmp` is
/// overwritten by the next attempt rather than read by anyone.
pub fn save(trie: &Trie, path: &Path) -> Result<u64, TrieFileError> {
    let display = path.display().to_string();
    let io = |source| TrieFileError::Io {
        path: display.clone(),
        source,
    };

    let mut nodes = Vec::new();
    collect(&trie.root(), &mut nodes, 0)?;
    // By id, so one trie produces one file however the walk happened to reach it. A
    // snapshot that differs run to run is a snapshot nothing can be diffed against.
    nodes.sort_by_key(|node| node.id);

    let file = TrieFile {
        next_id: trie.node_count(),
        nodes,
    };
    let body = serde_json::to_vec_pretty(&file).map_err(|source| TrieFileError::Parse {
        path: display.clone(),
        source,
    })?;

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).map_err(io)?;
        }
    }
    let tmp = path.with_extension("json.tmp");
    {
        let mut handle = fs::File::create(&tmp).map_err(io)?;
        handle.write_all(&body).map_err(io)?;
        handle.sync_all().map_err(io)?;
    }
    fs::rename(&tmp, path).map_err(io)?;
    // The rename is durable only once the directory entry is: a failure here is a
    // snapshot that may not be there after a power cut, and the caller logs it as such
    // rather than reporting a file that was written.
    if let Some(parent) = path.parent() {
        let parent = if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        };
        fs::File::open(parent)
            .and_then(|handle| handle.sync_all())
            .map_err(|source| TrieFileError::Io {
                path: parent.display().to_string(),
                source,
            })?;
    }
    Ok(body.len() as u64)
}

fn collect(node: &Arc<Node>, out: &mut Vec<NodeRecord>, depth: usize) -> Result<(), TrieFileError> {
    if depth > MAX_DEPTH {
        return Err(TrieFileError::TooDeep);
    }
    let children = node
        .children
        .read()
        .map_err(|_| TrieFileError::LockPoisoned)?;
    let meta = node
        .leaf_meta
        .read()
        .map_err(|_| TrieFileError::LockPoisoned)?;

    out.push(NodeRecord {
        id: node.id,
        label: node.label.clone(),
        node_type: node.node_type,
        is_leaf: node.is_leaf.load(std::sync::atomic::Ordering::Relaxed),
        leaf_meta: meta.as_ref().map(|meta| LeafMetaRecord {
            template_id: meta.template_id,
            columns: meta
                .columns
                .iter()
                .map(|col| ColumnRecord {
                    name: col.name.clone(),
                    col_type: col.col_type,
                    widened_at: col.widened_at,
                })
                .collect(),
            storage_path: meta.storage_path.clone(),
            first_seen: meta.first_seen,
            last_seen: meta.last_seen,
            time_source: Some(meta.time_source.as_record()),
        }),
        children: children
            .iter()
            .map(|edge| EdgeRecord {
                label: edge.label.clone(),
                child_id: edge.child.id,
            })
            .collect(),
    });
    for edge in children.iter() {
        collect(&edge.child, out, depth + 1)?;
    }
    Ok(())
}

// ── Loading ────────────────────────────────────────────────────────────────────────

/// The trie in `path`, or an empty one.
///
/// The signature is the policy: there is no `Result`, so no caller can accidentally make a
/// bad snapshot fatal. `load_reporting` is the same work with the reason handed back, for
/// the callers that want to say what went wrong.
pub fn load(path: &Path, data_dir: &str) -> Trie {
    load_reporting(path, data_dir).0
}

pub fn load_reporting(path: &Path, data_dir: &str) -> (Trie, Option<TrieFileError>) {
    match read(path, data_dir) {
        Ok(trie) => (trie, None),
        Err(e) => (Trie::new_in(data_dir), Some(e)),
    }
}

fn read(path: &Path, data_dir: &str) -> Result<Trie, TrieFileError> {
    let display = path.display().to_string();
    let bytes = fs::read(path).map_err(|source| TrieFileError::Io {
        path: display.clone(),
        source,
    })?;
    let file: TrieFile = serde_json::from_slice(&bytes).map_err(|source| TrieFileError::Parse {
        path: display,
        source,
    })?;
    build(file, data_dir)
}

/// Validate the records, then build the graph. **In that order, and it is load-bearing:**
/// a cycle turned into `Arc` edges is a reference loop that never drops, so it has to be
/// refused while the file is still a list of integers.
fn build(file: TrieFile, data_dir: &str) -> Result<Trie, TrieFileError> {
    let mut by_id: BTreeMap<u64, &NodeRecord> = BTreeMap::new();
    for record in &file.nodes {
        if by_id.insert(record.id, record).is_some() {
            return Err(TrieFileError::DuplicateId { id: record.id });
        }
    }
    if !by_id.contains_key(&0) {
        return Err(TrieFileError::NoRoot);
    }

    let mut edges = 0;
    for record in &file.nodes {
        let mut seen = HashSet::new();
        for edge in &record.children {
            edges += 1;
            if !by_id.contains_key(&edge.child_id) {
                return Err(TrieFileError::DanglingChild {
                    parent: record.id,
                    child: edge.child_id,
                });
            }
            if !seen.insert(edge.label.as_str()) {
                return Err(TrieFileError::DuplicateEdge {
                    id: record.id,
                    label: edge.label.clone(),
                });
            }
        }
    }
    // A tree has exactly one fewer edge than it has nodes. Together with reachability
    // below, that is the whole proof: the count rules out a second parent and the walk
    // rules out a disconnected cycle, and between them no `Arc` loop can be constructed.
    if edges != file.nodes.len().saturating_sub(1) {
        return Err(TrieFileError::NotATree {
            edges,
            nodes: file.nodes.len(),
        });
    }

    let mut reached = HashSet::new();
    let mut stack = vec![(0u64, 0usize)];
    while let Some((id, depth)) = stack.pop() {
        if depth > MAX_DEPTH {
            return Err(TrieFileError::TooDeep);
        }
        if !reached.insert(id) {
            continue;
        }
        if let Some(record) = by_id.get(&id) {
            for edge in &record.children {
                stack.push((edge.child_id, depth + 1));
            }
        }
    }
    if reached.len() != file.nodes.len() {
        return Err(TrieFileError::Unreachable {
            unreachable: file.nodes.len() - reached.len(),
        });
    }

    let mut templates = HashSet::new();
    for record in &file.nodes {
        if let Some(meta) = &record.leaf_meta {
            if !templates.insert(meta.template_id) {
                return Err(TrieFileError::DuplicateTemplate {
                    template_id: meta.template_id,
                });
            }
        }
    }

    // Past here the file describes a tree and nothing below can fail.
    if by_id.get(&0).is_some_and(|root| root.label != ROOT_LABEL) {
        warn(&format!(
            "the root is labelled `{}` and not `{ROOT_LABEL}`; using it anyway",
            by_id[&0].label
        ));
    }

    let mut built: BTreeMap<u64, Arc<Node>> = BTreeMap::new();
    for record in &file.nodes {
        let mut is_leaf = record.is_leaf;
        let meta = record.leaf_meta.as_ref().map(|meta| LeafMeta {
            template_id: meta.template_id,
            columns: meta
                .columns
                .iter()
                .map(|col| ColumnDef {
                    name: col.name.clone(),
                    col_type: col.col_type,
                    widened_at: col.widened_at,
                })
                .collect(),
            storage_path: meta.storage_path.clone(),
            first_seen: meta.first_seen,
            last_seen: meta.last_seen,
            time_source: meta
                .time_source
                .as_deref()
                .and_then(TimeSource::from_record)
                .unwrap_or_else(|| {
                    let defaults = crate::config::Schema::default().time_columns;
                    TimeSource::choose(&defaults, |name| {
                        meta.columns
                            .iter()
                            .find(|c| c.name == name)
                            .and_then(|c| c.col_type)
                    })
                }),
        });
        // The two halves of "this is a leaf" must agree, and either half alone is
        // recoverable: a leaf with no metadata has no template id and so no store to
        // open, and metadata on a node not marked a leaf is a leaf that lost its flag.
        if is_leaf && meta.is_none() {
            warn(&format!(
                "node {} is marked a leaf and carries no template; treating it as interior",
                record.id
            ));
            is_leaf = false;
        }
        if !is_leaf && meta.is_some() {
            warn(&format!(
                "node {} carries a template and is not marked a leaf; treating it as a leaf",
                record.id
            ));
            is_leaf = true;
        }
        built.insert(
            record.id,
            Arc::new(Node::restored(
                record.id,
                record.label.clone(),
                record.node_type,
                is_leaf,
                meta,
            )),
        );
    }

    for record in &file.nodes {
        let Some(parent) = built.get(&record.id) else {
            continue;
        };
        let Ok(mut children) = parent.children.write() else {
            return Err(TrieFileError::LockPoisoned);
        };
        for edge in &record.children {
            let Some(child) = built.get(&edge.child_id) else {
                continue;
            };
            if edge.label != child.label {
                warn(&format!(
                    "node {} reaches node {} by the label `{}`, which the child spells `{}`; \
                     the edge wins",
                    record.id, edge.child_id, edge.label, child.label
                ));
            }
            children.push(crate::trie::Edge {
                label: edge.label.clone(),
                child: Arc::clone(child),
            });
        }
        // **Sorted on load rather than trusted.** `child_or_create` finds an edge with a
        // binary search, so a file whose children are out of order does not error - it
        // silently fails to find edges that are there, grows duplicates beside them, and
        // splits one template into several. Re-sorting costs nothing and removes the
        // failure mode entirely.
        children.sort_by(|a, b| a.label.cmp(&b.label));
    }

    let max_id = by_id.keys().copied().max().unwrap_or(0);
    // Derived, never trusted: a `next_id` written smaller than the ids in the same file
    // hands out one that is already taken.
    let next_id = file.next_id.max(max_id + 1);
    // `max + 1` and not `first + count`: with a gap in the sequence the count form
    // reissues an id, and for a template that means two leaves in one storage directory.
    let next_template_id = templates
        .iter()
        .copied()
        .max()
        .map_or(crate::trie::FIRST_TEMPLATE_ID, |max| max + 1);

    let root = built.get(&0).cloned().unwrap_or_else(|| {
        Arc::new(Node::restored(
            0,
            ROOT_LABEL.to_string(),
            NodeType::Static,
            false,
            None,
        ))
    });
    let tokens = token_count(&file, &by_id);

    Ok(Trie::restored(
        root,
        data_dir,
        next_id,
        next_template_id,
        tokens,
    ))
}

/// Path length summed over leaves, which is what `Trie::insert` counts.
///
/// Over *leaves* and not over terminal nodes: one template's path can be a prefix of
/// another's, so an interior node is a leaf too and its depth still counted.
fn token_count(file: &TrieFile, by_id: &BTreeMap<u64, &NodeRecord>) -> u64 {
    let mut depth_of: BTreeMap<u64, usize> = BTreeMap::new();
    let mut stack = vec![(0u64, 0usize)];
    while let Some((id, depth)) = stack.pop() {
        if depth_of.insert(id, depth).is_some() {
            continue;
        }
        if let Some(record) = by_id.get(&id) {
            for edge in &record.children {
                stack.push((edge.child_id, depth + 1));
            }
        }
    }
    file.nodes
        .iter()
        .filter(|record| record.is_leaf)
        .map(|record| *depth_of.get(&record.id).unwrap_or(&0) as u64)
        .sum()
}

fn warn(message: &str) {
    eprintln!("triedb: WARN trie.json: {message}");
}
