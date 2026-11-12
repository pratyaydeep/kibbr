// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The plan: which templates a query must read, chosen from the trie's key index without
//! reading a row.

use super::*;

// ── The plan ───────────────────────────────────────────────────────────────────────

/// What the plan decided, and nothing else: no rows, no store handles.
#[derive(Debug, Default)]
pub struct Plan {
    pub leaves: Vec<PlannedLeaf>,
    /// Root subtrees a walk of the trie skips without looking inside: the root's edges
    /// that hold another value of a key the pattern names exactly.
    pub skipped: usize,
    pub duration: Duration,
}

/// One template a plan chose, and what is left for the scan to ask of its rows.
#[derive(Debug, PartialEq)]
pub struct PlannedLeaf {
    pub template_id: u64,
    /// The template's path, storage path, time column and text columns as they were when
    /// the plan was made, shared and not copied.
    pub template: Arc<Template>,
    /// Terms the path could not settle - the scan's predicates; empty means every row in the
    /// leaf matches by its shape alone.
    pub residual: Vec<Match>,
}

/// Choose the templates that could hold a matching row from the trie's key index, reading
/// no data: the plan `plan_by_walk` makes, at the cost of the lists a pattern names.
pub fn plan(trie: &Trie, query: &Query) -> Plan {
    let started = Instant::now();
    let refused = refusals(query);
    let (skipped, edges) = pruned(trie, &refused);
    // The index's lock is let go at the end of this block and before anything else is
    // done: nothing below may wait on a node while it is held.
    let (leaves, edits) = {
        let keys = trie.keys();
        (chosen(&keys, query, &refused), keys.edits())
    };
    let duration = started.elapsed();
    if cfg!(debug_assertions) {
        hold_to_the_walk(trie, query, &leaves, (skipped, edges), edits);
    }
    Plan {
        leaves,
        skipped,
        duration,
    }
}

/// A debug build's check of every plan against the walk. Each half is compared only where
/// no other thread moved it in between: the leaves by `edits`, `skipped` by root edges.
fn hold_to_the_walk(
    trie: &Trie,
    query: &Query,
    leaves: &[PlannedLeaf],
    (skipped, edges): (usize, usize),
    edits: u64,
) {
    let walked = plan_by_walk(trie, query);
    if trie.keys().edits() == edits {
        assert_eq!(
            leaves,
            walked.leaves.as_slice(),
            "the plan read from the key index is not the plan a walk of the trie makes, \
             for the pattern {:?}",
            query.tokens()
        );
    }
    let edges_now = trie
        .root()
        .children
        .read()
        .map_or(0, |children| children.len());
    if edges_now == edges {
        assert_eq!(
            skipped,
            walked.skipped,
            "the root subtrees counted as skipped are not the ones a walk of the trie \
             skips, for the pattern {:?}",
            query.tokens()
        );
    }
}

/// Each exact term's promoted token and the length of its `KEY=` prefix: what the root
/// filter reads, made once for a query rather than for every edge.
fn refusals(query: &Query) -> Vec<(String, usize)> {
    query
        .matches
        .iter()
        .filter_map(|m| match &m.value {
            MatchValue::Exact(v) => {
                let token = promoted_token(&m.key, v);
                let prefix = token.len() - v.len();
                Some((token, prefix))
            }
            MatchValue::Any => None,
        })
        .collect()
}

/// Whether the root filter refuses a root edge with this label. The one predicate both the
/// count of skipped edges and the choice of templates use, so the two cannot disagree.
fn refuses(label: &str, refused: &[(String, usize)]) -> bool {
    refused
        .iter()
        .any(|(token, prefix)| label != token && label.starts_with(&token[..*prefix]))
}

/// `Plan::skipped` and the count of the root's edges, in one hold of the root's lock.
/// Counted at the root because the walk also counts an edge whose subtree has no template.
fn pruned(trie: &Trie, refused: &[(String, usize)]) -> (usize, usize) {
    let root = trie.root();
    let Ok(children) = root.children.read() else {
        return (0, 0);
    };
    let skipped = children
        .iter()
        .filter(|edge| refuses(&edge.label, refused))
        .count();
    (skipped, children.len())
}

/// The templates the pattern could match, in id order, with the terms left to the scan.
/// Applies both of the walk's filters, since `select` does not imply `keeps`.
pub(super) fn chosen(keys: &KeyIndex, query: &Query, refused: &[(String, usize)]) -> Vec<PlannedLeaf> {
    // One list a term, with how far into it the merge has come. A key no template has
    // is a pattern no template can match.
    let mut lists: Vec<(&[Posting], usize)> = Vec::with_capacity(query.matches.len());
    for term in &query.matches {
        match keys.of(&term.key) {
            Some(list) => lists.push((list, 0)),
            None => return Vec::new(),
        }
    }
    // With no term, every template: the root as a leaf has no key and is found here and
    // by no term.
    let driver = lists
        .iter()
        .map(|(list, _)| *list)
        .min_by_key(|list| list.len())
        .unwrap_or(keys.all());

    let mut leaves = Vec::new();
    'next: for (id, template) in driver {
        for (list, at) in &mut lists {
            while *at < list.len() && list[*at].0 < *id {
                *at += 1;
            }
            if list.get(*at).map(|posting| posting.0) != Some(*id) {
                continue 'next;
            }
        }
        // The root as a leaf has no first label and is kept, as the walk keeps it.
        if !refused.is_empty()
            && template
                .path
                .first()
                .is_some_and(|first| refuses(first, refused))
        {
            continue 'next;
        }
        let mut residual = Vec::new();
        for term in &query.matches {
            match template.promoted(&term.key) {
                // The value is in the path, so the template settles the term now and the scan has
                // nothing to check.
                Some(have) => match &term.value {
                    MatchValue::Any => {}
                    MatchValue::Exact(want) if want == have => {}
                    MatchValue::Exact(_) => continue 'next,
                },
                None => residual.push(term.clone()),
            }
        }
        leaves.push(PlannedLeaf {
            template_id: *id,
            template: Arc::clone(template),
            residual,
        });
    }
    leaves
}

/// The plan as a walk of the trie makes it, kept as what `plan` is held to: a change to
/// `keeps`, `select` or `promoted_of` is one to `refuses`, `chosen` and `Template::of` too.
pub(super) fn plan_by_walk(trie: &Trie, query: &Query) -> Plan {
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
                template: Arc::new(Template::of(path, meta)),
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

/// Whether a root subtree is kept: promoted tokens are hoisted to the front of the path,
/// so a root label `KEY=` with another value cannot hold an exact match on that key.
fn keeps(label: &str, query: &Query) -> bool {
    query.matches.iter().all(|m| match &m.value {
        MatchValue::Exact(v) => {
            let token = promoted_token(&m.key, v);
            label == token || !label.starts_with(&token[..token.len() - v.len()])
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
/// it can, once the scan has applied the terms listed.
fn select(path: &[String], columns: &BTreeSet<&str>, query: &Query) -> Option<Vec<Match>> {
    let promoted = promoted_of(path, columns);
    let mut residual = Vec::new();
    for term in &query.matches {
        // The key set is the path, so a template without the key cannot hold a row with
        // it. This is the whole of the walk's pruning for non-promoted keys.
        if !columns.contains(term.key.as_str()) {
            return None;
        }
        match promoted.get(term.key.as_str()) {
            // The value is in the path, so the leaf settles the term now and every row
            // in it agrees - no predicate, and the scan has nothing to check.
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
