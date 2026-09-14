//! `triedb demo` - build the trie over a file and print it as an ASCII tree.
//!
//! The tree is the observable surface of the trie. A non-branching run of edges prints on
//! one line joined by `→`; `●` marks a leaf, and the column list under it carries the
//! types the join settled on. Every key on the path appears again in that list, because
//! the path *is* the key set - the repetition is the point, not an artifact.
//!
//! Everything on stdout is a function of the input lines as a set: children are stored
//! sorted, columns are sorted by name, and the join is commutative, so a different input
//! order produces the same bytes. The one number that cannot be - how long the run took -
//! goes to stderr, outside the artifact whose byte-identity is the milestone's done-when.

use std::io::{BufRead, Write};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use chrono::Utc;

use crate::error::{CliError, DetectorError, DetectorErrorKind};
use crate::flatten::parse_line;
use crate::fmt::commas;
use crate::lines::{read_line, skip_to_eol, strip_bom, strip_eol, Line, Resync, MAX_LINE_BYTES};
use crate::trie::{Node, Trie, TrieError, ROOT_LABEL};
use crate::types::ColumnType;

const BRANCH_MID: &str = "├── ";
const BRANCH_LAST: &str = "└── ";
const GUTTER_MID: &str = "│   ";
const GUTTER_LAST: &str = "    ";
/// The column list hangs one gutter width past the line it belongs to.
const COLUMN_INDENT: &str = "    ";

/// A subtree holding more than this many templates is summarised rather than printed.
/// Six `event=` subtrees of 144 templates each are the file, not a view of it; the handful
/// of shapes that do not branch on `event` are what an operator came to see. `--full`
/// prints all of it.
const COLLAPSE_ABOVE: usize = 8;

/// Blank columns between the widest summarised label in a sibling group and its `…`.
const ELLIPSIS_PAD: usize = 4;

/// Where a column list wraps, counting the gutter. Wide enough that the eight-key
/// templates `--seed 42` produces take two lines rather than eight.
const WRAP: usize = 88;

/// A column that has been seen but has only ever held nulls. Not `Str`: an unknown type
/// and a column of strings are not the same fact, which is why `ColumnType` has no `Null`
/// variant to print here.
const UNKNOWN_TYPE: &str = "?";

pub fn run<R: BufRead, W: Write>(mut input: R, out: &mut W, full: bool) -> Result<(), CliError> {
    let trie = Trie::new();
    let started = Instant::now();

    let mut ingested = 0u64;
    let mut skipped_blank = 0u64;
    let mut rejected = 0u64;
    let mut read_lines = 0u64;
    let mut raw = Vec::new();

    loop {
        raw.clear();
        let read = read_line(&mut input, &mut raw).map_err(|source| CliError::Read {
            path: "<input>".to_string(),
            source,
        })?;
        match read {
            Line::Eof => break,
            // One over-long line costs its own line and nothing else, the same bargain a
            // malformed one gets below.
            Line::TooLong => {
                read_lines += 1;
                rejected += 1;
                // Flushed and reported before the skip, never after: the skip may be the
                // last thing this process does, and a rejection nobody saw is the bug
                // being fixed.
                out.flush().map_err(CliError::Write)?;
                crate::error::report(&DetectorError::new(
                    read_lines as usize,
                    DetectorErrorKind::LineTooLong {
                        limit: MAX_LINE_BYTES,
                    },
                ));
                match skip_to_eol(&mut input).map_err(|source| CliError::Read {
                    path: "<input>".to_string(),
                    source,
                })? {
                    Resync::Done => continue,
                    Resync::NotDelimited { scanned } => {
                        return Err(CliError::NotNewlineDelimited {
                            line: read_lines,
                            scanned,
                            limit: MAX_LINE_BYTES,
                        })
                    }
                }
            }
            Line::Read => {}
        }
        read_lines += 1;
        let line = strip_bom(strip_eol(&raw));

        if std::str::from_utf8(line).is_ok_and(|s| s.trim().is_empty()) {
            skipped_blank += 1;
            continue;
        }

        // One bad line 400 out of 500 must not cost the other 499 their templates.
        let parsed = match parse_line(line, read_lines as usize) {
            Ok(parsed) => parsed,
            Err(e) => {
                rejected += 1;
                // Flush first: stdout is block-buffered when piped and stderr is not,
                // so without this the rejections all arrive before the tree.
                out.flush().map_err(CliError::Write)?;
                crate::error::report(&e);
                continue;
            }
        };

        // Once per line, not once per run: `first_seen`, `last_seen` and `widened_at` are
        // the times things happened, and an operator will read them as such.
        trie.ingest(&parsed, Utc::now())?;
        ingested += 1;
    }

    let elapsed = started.elapsed();

    let root = trie.root();
    let view = snapshot(&root)?;
    render(&view, full, out)?;

    let tokens = trie.token_count();
    let nodes = trie.node_count();
    writeln!(out).map_err(CliError::Write)?;
    writeln!(
        out,
        "{} lines → {} templates; {} tokens → {} nodes, {} saved by shared prefixes{}{}",
        commas(ingested),
        commas(trie.template_count()),
        commas(tokens),
        commas(nodes),
        commas(tokens.saturating_sub(nodes)),
        if skipped_blank > 0 {
            format!("; {skipped_blank} blank lines skipped")
        } else {
            String::new()
        },
        if rejected > 0 {
            format!("; {rejected} lines rejected")
        } else {
            String::new()
        }
    )
    .map_err(CliError::Write)?;

    // Before the timing line, or the tree arrives after it: stdout is buffered here and
    // stderr is not, so an unflushed tree would be printed second however it was written.
    out.flush().map_err(CliError::Write)?;

    // stderr, not stdout: an elapsed time is the one number that can never be
    // byte-identical across runs, so it goes beside the artifact instead of in it.
    eprintln!(
        "triedb demo: {} lines in {} ms",
        commas(ingested),
        elapsed.as_millis()
    );

    if rejected > 0 {
        return Err(CliError::RejectedLines {
            count: rejected,
            total: read_lines,
        });
    }
    Ok(())
}

/// An owned copy of the trie, taken once so rendering holds no locks and each node's leaf
/// count is computed on the way up instead of re-walked per line.
struct View {
    label: String,
    leaf: Option<LeafView>,
    children: Vec<View>,
    /// Leaves at or below this node - the templates a summarised line stands for.
    leaves: usize,
}

struct LeafView {
    template_id: u64,
    /// Pre-rendered `name:Type`, in the sorted order `LeafMeta.columns` holds.
    columns: Vec<String>,
}

fn snapshot(node: &Arc<Node>) -> Result<View, TrieError> {
    let leaf = if node.is_leaf.load(Ordering::Acquire) {
        node.leaf_meta
            .read()
            .map_err(|_| TrieError::LockPoisoned)?
            .as_ref()
            .map(|meta| LeafView {
                template_id: meta.template_id,
                columns: meta
                    .columns
                    .iter()
                    .map(|col| {
                        format!(
                            "{}:{}",
                            col.name,
                            col.col_type.map_or(UNKNOWN_TYPE, ColumnType::short)
                        )
                    })
                    .collect(),
            })
    } else {
        None
    };

    let mut children = Vec::new();
    for edge in node
        .children
        .read()
        .map_err(|_| TrieError::LockPoisoned)?
        .iter()
    {
        children.push(snapshot(&edge.child)?);
    }

    let leaves = usize::from(leaf.is_some()) + children.iter().map(|c| c.leaves).sum::<usize>();
    Ok(View {
        label: node.label.clone(),
        leaf,
        children,
        leaves,
    })
}

fn render<W: Write>(root: &View, full: bool, out: &mut W) -> Result<(), CliError> {
    // The root is not an edge, so it has no branch character and no run to join.
    let mut line = ROOT_LABEL.to_string();
    if let Some(leaf) = &root.leaf {
        line.push_str(&leaf_mark(leaf));
    }
    writeln!(out, "{line}").map_err(CliError::Write)?;
    if let Some(leaf) = &root.leaf {
        render_columns(&leaf.columns, COLUMN_INDENT, out)?;
    }
    render_children(&root.children, "", full, out)
}

/// One sibling group. Laid out in two passes because the `…` of every summarised line in
/// the group is aligned to the widest label among them - one pass cannot know that.
fn render_children<W: Write>(
    children: &[View],
    prefix: &str,
    full: bool,
    out: &mut W,
) -> Result<(), CliError> {
    struct Row<'a> {
        text: String,
        end: &'a View,
        child_prefix: String,
        /// Templates below the run's end, and `None` where they are printed rather than
        /// summarised.
        summarised: Option<usize>,
    }

    let rows: Vec<Row> = children
        .iter()
        .enumerate()
        .map(|(i, child)| {
            let last = i + 1 == children.len();
            let (run, end) = run_of(child);
            let mut text = run;
            if let Some(leaf) = &end.leaf {
                text.push_str(&leaf_mark(leaf));
            }
            let below = end.leaves - usize::from(end.leaf.is_some());
            Row {
                text,
                end,
                child_prefix: format!("{prefix}{}", if last { GUTTER_LAST } else { GUTTER_MID }),
                summarised: (!full && below > COLLAPSE_ABOVE).then_some(below),
            }
        })
        .collect();

    let ellipsis_at = rows
        .iter()
        .filter(|row| row.summarised.is_some())
        .map(|row| row.text.chars().count())
        .max()
        .map_or(0, |widest| widest + ELLIPSIS_PAD);

    for (i, row) in rows.iter().enumerate() {
        let branch = if i + 1 == rows.len() {
            BRANCH_LAST
        } else {
            BRANCH_MID
        };
        match row.summarised {
            Some(below) => {
                let pad = ellipsis_at.saturating_sub(row.text.chars().count());
                writeln!(
                    out,
                    "{prefix}{branch}{}{:pad$}… {} templates",
                    row.text,
                    "",
                    commas(below as u64),
                    pad = pad
                )
                .map_err(CliError::Write)?;
            }
            None => {
                writeln!(out, "{prefix}{branch}{}", row.text).map_err(CliError::Write)?;
                if let Some(leaf) = &row.end.leaf {
                    let indent = format!("{}{COLUMN_INDENT}", row.child_prefix);
                    render_columns(&leaf.columns, &indent, out)?;
                }
                render_children(&row.end.children, &row.child_prefix, full, out)?;
            }
        }
    }
    Ok(())
}

/// Collapse a non-branching run of edges onto one line. A run stops at a leaf even when
/// that leaf has children of its own: the leaf has a template id and a column list to
/// print, and they belong beside their own label.
fn run_of(view: &View) -> (String, &View) {
    let mut labels = vec![view.label.as_str()];
    let mut end = view;
    while end.leaf.is_none() && end.children.len() == 1 {
        end = &end.children[0];
        labels.push(end.label.as_str());
    }
    (labels.join(" → "), end)
}

fn leaf_mark(leaf: &LeafView) -> String {
    format!(" ● #{}", leaf.template_id)
}

fn render_columns<W: Write>(columns: &[String], indent: &str, out: &mut W) -> Result<(), CliError> {
    let width = indent.chars().count();
    let mut line = String::new();
    for column in columns {
        if line.is_empty() {
            line.push_str(column);
        } else if width + line.chars().count() + 1 + column.chars().count() <= WRAP {
            line.push(' ');
            line.push_str(column);
        } else {
            writeln!(out, "{indent}{line}").map_err(CliError::Write)?;
            line.clear();
            line.push_str(column);
        }
    }
    if !line.is_empty() {
        writeln!(out, "{indent}{line}").map_err(CliError::Write)?;
    }
    Ok(())
}
