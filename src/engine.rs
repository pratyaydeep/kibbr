//! The query engine: a walk, a scan, and the result shape every surface above reports.
//!
//! The walk crosses the trie and touches no data. The scan opens only the leaves the walk
//! chose. The split is enforced by the types: `plan` cannot see `Stores`, so `--explain`
//! cannot accidentally read a row.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use thiserror::Error;

use crate::fmt::col_text;
use crate::infer::infer;
use crate::store::{Predicate, Row, ScanOptions, StoreError, Stores, DEFAULT_TS_COLUMN};
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
    /// The limit dropped rows that matched. `count` is the total that matched and
    /// `rows_returned` is how many survived the cap, so this is `rows_returned < count` -
    /// carried as its own field because every surface above has to say so, and a reader
    /// comparing two numbers is a reader who can miss it.
    pub truncated: bool,
    /// Who set the page size; `truncated` says whether it fired. Read together: only
    /// `backstop` *and* `truncated` means "narrow your query", and a caller that asked for
    /// ten rows and got ten needs telling nothing.
    pub limit_source: LimitSource,
    /// Hand this back as `AFTER` to get the next page, or `None` because there is no next
    /// page. Present only when a `LIMIT` both applied and actually stopped short of the
    /// end - `rows_returned == limit` is not the same fact, and a cursor that yields an
    /// empty page is a cursor that lied.
    pub next_cursor: Option<String>,
    /// Walk plus scan. Loading the file is not query time - the CLI has to build the
    /// trie first because nothing persists it yet.
    pub duration: Duration,
}

/// Who set the cap on rows returned. Two fields, not one: see `QueryMeta::limit_source`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum LimitSource {
    /// Nothing capped this answer - no `LIMIT`, no backstop, or a mode returning no rows.
    #[default]
    Unlimited,
    /// The caller's own `LIMIT`.
    Caller,
    /// The server's max_rows limit, because the query named no `LIMIT`.
    Backstop,
}

impl LimitSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unlimited => "unlimited",
            Self::Caller => "caller",
            Self::Backstop => "backstop",
        }
    }
}

/// One term: a `--match KEY=VALUE` flag, or one `"key": "value"` pair of a pattern.
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

/// A query, already built. Nothing here parses text: the flags build this directly and
/// `crate::trieql` builds the same struct from a query string, so the engine has one
/// input whichever surface asked.
#[derive(Debug)]
pub struct Query {
    pub matches: Vec<Match>,
    pub mode: QueryMode,
    pub time: TimeRange,
    /// `None` = every matching row. Resolved by `Query::new`, which drops a limit the mode
    /// cannot use and supplies `max_rows` where the caller named none.
    ///
    /// An unlimited `MATCH` materialises every matching row in the server before a byte of
    /// the response is written: a denial-of-service path on a shared daemon, reachable by
    /// accident from a terminal. The backstop is a *page size* and not a truncation - the
    /// answer says so and carries a cursor - which is what makes capping it an answer
    /// rather than a refusal.
    pub limit: Option<usize>,
    /// Who supplied `limit`.
    pub limit_source: LimitSource,
    /// Where the last page stopped, from `QueryMeta::next_cursor`. `None` = start at the
    /// newest row.
    pub cursor: Option<Cursor>,
}

impl Query {
    /// The one constructor, so the flag surface and TrieQL resolve the limit identically.
    ///
    /// `limit` caps *rows returned*, not rows counted: `MATCH … LIMIT 10` answers with the
    /// newest ten and a `count` of every row that matched, as SQL's LIMIT bounds the result
    /// set and not `COUNT(*)`. `Count` and `GroupBy` return no rows, so a limit has nothing
    /// to cap there and is normalised away rather than quietly halving a total; `cursor`
    /// goes with it, since a mode returning no rows has no sequence to page. `--explain`
    /// prints what survived.
    ///
    /// An `Option`, not a 0-means-absent sentinel: `LIMIT 0` asks for no rows and has to
    /// stay distinguishable from asking for nothing. `ScanOptions.limit` keeps the sentinel
    /// - it is the storage interface, where "unlimited" is a real setting.
    ///
    /// `max_rows` is the server's limit on an unlimited `Fetch`, in rows; `0` means none.
    /// The caller's own limit stands above it and is not clamped; `COUNT`/`GROUP BY`
    /// return no rows for it to bound.
    pub fn new(
        matches: Vec<Match>,
        mode: QueryMode,
        time: TimeRange,
        limit: Option<usize>,
        cursor: Option<Cursor>,
        max_rows: usize,
    ) -> Self {
        let fetching = mode == QueryMode::Fetch;
        let (limit, limit_source) = match (fetching, limit, max_rows) {
            (false, ..) => (None, LimitSource::Unlimited),
            (true, Some(n), _) => (Some(n), LimitSource::Caller),
            (true, None, 0) => (None, LimitSource::Unlimited),
            (true, None, cap) => (Some(cap), LimitSource::Backstop),
        };
        Self {
            matches,
            mode,
            time,
            limit,
            limit_source,
            cursor: if fetching { cursor } else { None },
        }
    }

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
            // The storage layer's spelling of "unlimited", which is not this one's.
            limit: self.limit.unwrap_or(0),
            since: self.time.since,
            until: self.time.until,
            ts_column: None,
        }
    }

    /// A cursor of this query, at a rank this query produced. The one way to mint one -
    /// `Cursor::guard` is the query's, so a cursor cannot be built without a query to
    /// answer for it.
    pub fn cursor_at(&self, rank: Rank) -> Cursor {
        Cursor::new(rank, self)
    }

    /// The column the result is ordered by, and the one a time filter reads.
    pub fn ts_column(&self) -> String {
        self.scan_options()
            .ts_column
            .unwrap_or_else(|| DEFAULT_TS_COLUMN.to_string())
    }
}

// ── The walk ───────────────────────────────────────────────────────────────────────

/// What the walk decided, and nothing else. No rows, no store handles.
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
    /// The leaf's key set as walk tokens - what `ParsedLog::walk_tokens` produces for any
    /// line in it.
    ///
    /// Carried because it is the leaf's *content-derived* identity, where `template_id` is
    /// a counter handed out in the order templates were first seen. The result order needs
    /// a tiebreak for rows a timestamp cannot separate, and a tiebreak the reference
    /// scanner cannot recompute is one the two sides will disagree about - which they did,
    /// on every template whose time column is not called `ts`.
    pub path: Vec<String>,
    /// Collected by the walk; nothing opens one while the store is a `Vec<Row>`. It is
    /// what the scan will hand to the file layer once there is one.
    #[allow(dead_code)]
    pub storage_path: String,
    /// Terms the path could not settle - the scan's predicates. Empty means the leaf
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
                path: path.clone(),
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

// ── Result order ───────────────────────────────────────────────────────────────────

/// Where a row sits in the answer. Smaller is earlier.
///
/// Rows come back **newest first**: "the last 100 errors" is what a limit is nearly always
/// asked for, and an ascending order would hand back the oldest hundred instead. Leaving
/// this unspecified was not a neutral position - it made LIMIT meaningless.
///
/// `Reverse(ts)` puts the newest first and, because `None` is the least `Option`, puts a
/// row with no readable timestamp last. A template with no `ts` column at all has nothing
/// to sort by, so those rows fall back to **insertion order**: `seq` is the order they
/// were appended to their leaf in. The same pair settles a tie between two rows sharing a
/// timestamp.
///
/// Both tiebreak components are facts about the *row*, not about the query that found it -
/// which is what lets a cursor built from one still name the same row later (see `Cursor`).
/// `leaf_key` is the leaf's key set hashed; `seq` is the ordinal the row was given when it
/// was ingested. The order is total and computed from the data alone, so the differential
/// compares the answer exactly instead of as an unordered set.
///
/// This order is also what will let a disk store stop a partition scan once the limit
/// fills: partitioned by day and read newest-first, the first `limit` rows it sees are the
/// answer. Nothing here forecloses that - `Newest` becomes redundant, not wrong.
pub type Rank = (Reverse<Option<i64>>, u64, u64);

/// A leaf's place in the order, from its key set alone.
///
/// Shared with the oracle as `value_matches` is: which number a leaf gets is a contract
/// both sides must land on exactly, not a mechanism worth reimplementing.
///
/// It replaced the leaf's rank among the leaves *this query* matched, which was a fact
/// about the query and not the leaf: one new template matching the pattern renumbered every
/// leaf after it, and a cursor issued before that landed between rows. A key-set hash is
/// content-derived, so it is the same number in the next process and on disk at M5.
///
/// Two paths colliding on 64 bits interleave their tied rows - identically on both sides,
/// and those rows were arbitrarily ordered anyway.
pub fn leaf_key(path: &[String]) -> u64 {
    let mut h = FNV_OFFSET;
    for token in path {
        h = fnv(h, token.as_bytes());
        h = fnv(h, &[0]);
    }
    h
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;

/// FNV-1a, one more slice folded in.
fn fnv(mut h: u64, bytes: &[u8]) -> u64 {
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// A row's timestamp, or `None` for a row that cannot be placed in time.
///
/// The same reading `store::in_window` uses: a column widened to `String` by one bad line
/// is not a timestamp any more, and it should sort to the end rather than be guessed at.
fn ts_of(row: &Row, ts: &str) -> Option<i64> {
    match row.get(ts) {
        Some(ColValue::Datetime(ns)) => Some(*ns),
        _ => None,
    }
}

// ── Cursors ────────────────────────────────────────────────────────────────────────

/// Where a page stopped: the `Rank` of its last row, plus a guard.
///
/// Keyset, not offset. A cursor names a *row*, so nothing ingested elsewhere can shift
/// it; `OFFSET n` names a position in a sequence that moves, so it re-serves rows when
/// anything lands at the head between two calls - and pays for the n rows it skips.
///
/// Nothing stores one. A cursor is a function of the last row handed out, so there is no
/// session and no expiry and the server may restart between two pages. That is the line
/// between `search_after`, which Kibana uses, and `scroll` or a Splunk job, which park a
/// frozen result set server-side.
///
/// **What it names, and what a restart does to it.** All three components are properties of
/// the row - its timestamp, its leaf's key set hashed, the ordinal it was given at ingest -
/// and none is a position in a structure a restart rebuilds. So it resolves to the same row
/// in the next process as long as that row is still stored, which is what M5 needs: rows in
/// day partitions flushed in batches, the ingest ordinal read back from a column.
///
/// It still does not freeze the data. A row ingested ahead of the cursor between two pages
/// is not shown until you start again without one, as under `Cursor::guard`.
///
/// The guard is why `query` is in here. A cursor read against a different pattern is a
/// cursor into a different answer - it would resume from a row that answer never contained
/// and skip everything ranked above it, without complaining. Four bytes is the difference
/// between an error and a wrong answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cursor {
    rank: Rank,
    query: u32,
}

/// Bumped when `Rank`'s components change meaning, and read before anything else in
/// `decode`: a cursor from an older layout would resolve against a layout it never
/// described and answer confidently with the wrong rows. Nothing reads a superseded version
/// and nothing translates one - this check is the whole compatibility story.
const CURSOR_VERSION: &str = "v2";

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CursorError {
    #[error("not a cursor: expected `{CURSOR_VERSION}.` followed by {HEX_LEN} hex digits")]
    Malformed,
    #[error("cursor version `{found}` is not `{CURSOR_VERSION}` - it was issued before the page order changed; re-run the query without `AFTER`")]
    Version { found: String },
    #[error("this cursor came from a different query - a cursor is only valid for the pattern and order that produced it")]
    WrongQuery,
}

/// `[1 present][8 ts][8 leaf_key][8 seq][4 guard]`, big-endian, as hex.
const CURSOR_BYTES: usize = 1 + 8 + 8 + 8 + 4;
const HEX_LEN: usize = CURSOR_BYTES * 2;

impl Cursor {
    /// The fingerprint a cursor is checked against: what was matched, and in what order.
    /// Anything that would renumber the leaves or reorder the rows is in here, and nothing
    /// that would not.
    ///
    /// The time window is deliberately *out*. It renumbers nothing - `select` chooses
    /// leaves from the pattern alone - and putting it in makes the common case impossible:
    /// `SINCE 1h` resolves against `now`, so two calls a millisecond apart produce two
    /// different windows and every cursor from a relative window would be refused by the
    /// query that issued it.
    ///
    /// What that costs, stated rather than hidden: pages are a moving view over live data.
    /// A row ingested between two pages lands ahead of the cursor and is not shown until
    /// you start again without one. Widening the window mid-page has the same shape. This
    /// is how `search_after` and Loki behave; freezing the window into the cursor instead
    /// would make it a snapshot, with the expiry and server-side state that implies.
    pub fn guard(query: &Query) -> u32 {
        let mut h = FNV_OFFSET;
        for token in query.tokens() {
            h = fnv(h, token.as_bytes());
            h = fnv(h, &[0]);
        }
        h = fnv(h, &[1]);
        h = fnv(h, query.ts_column().as_bytes());
        // Folded to 32 bits: this is a guard against a mistake, not against an attacker,
        // and the cursor is already long.
        ((h >> 32) ^ h) as u32
    }

    /// The `Rank` a page stopped at. Read by the scan, and by the oracle, which reaches
    /// the same answer by sorting rather than by filtering as it goes.
    pub fn rank(&self) -> Rank {
        self.rank
    }

    fn new(rank: Rank, query: &Query) -> Self {
        Self {
            rank,
            query: Self::guard(query),
        }
    }

    /// Opaque on purpose. Nobody should be able to write one by hand, because anyone who
    /// can will depend on the layout that `CURSOR_VERSION` exists to change.
    pub fn encode(&self) -> String {
        let (Reverse(ts), leaf, seq) = self.rank;
        let mut out = String::with_capacity(CURSOR_VERSION.len() + 1 + HEX_LEN);
        out.push_str(CURSOR_VERSION);
        out.push('.');
        let mut hex = |bytes: &[u8]| {
            for b in bytes {
                out.push_str(&format!("{b:02x}"));
            }
        };
        // `ts` is an `Option`, and the absent case is a real one - a template with no `ts`
        // column pages like any other. The flag carries it; the eight bytes beside it are
        // then zero and mean nothing.
        match ts {
            Some(ns) => {
                hex(&[1]);
                hex(&ns.to_be_bytes());
            }
            None => {
                hex(&[0]);
                hex(&0i64.to_be_bytes());
            }
        }
        hex(&leaf.to_be_bytes());
        hex(&seq.to_be_bytes());
        hex(&self.query.to_be_bytes());
        out
    }

    /// The text back into a cursor. The guard is *not* checked here - the query it belongs
    /// to is not built yet at parse time - which is what `check` is for.
    pub fn decode(text: &str) -> Result<Self, CursorError> {
        let (version, body) = text.split_once('.').ok_or(CursorError::Malformed)?;
        if version != CURSOR_VERSION {
            return Err(CursorError::Version {
                found: version.to_string(),
            });
        }
        if body.len() != HEX_LEN || !body.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(CursorError::Malformed);
        }
        let mut bytes = [0u8; CURSOR_BYTES];
        for (n, slot) in bytes.iter_mut().enumerate() {
            *slot = u8::from_str_radix(&body[n * 2..n * 2 + 2], 16).map_err(|_| {
                // Unreachable - every byte was checked above. Not an `unwrap`, because
                // `decode` is fed by the CLI and "never panics" is the parser's rule too.
                CursorError::Malformed
            })?;
        }
        let word = |at: usize| {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&bytes[at..at + 8]);
            buf
        };
        let ts = match bytes[0] {
            0 => None,
            _ => Some(i64::from_be_bytes(word(1))),
        };
        let mut guard = [0u8; 4];
        guard.copy_from_slice(&bytes[25..29]);
        Ok(Self {
            rank: (Reverse(ts), u64::from_be_bytes(word(9)), u64::from_be_bytes(word(17))),
            query: u32::from_be_bytes(guard),
        })
    }

    /// Does this cursor belong to this query? Checked once, before the scan.
    pub fn check(&self, query: &Query) -> Result<(), CursorError> {
        if self.query == Self::guard(query) {
            return Ok(());
        }
        Err(CursorError::WrongQuery)
    }
}

/// One candidate row, ordered by `Rank` and by nothing else - `Row` is a `HashMap` and has
/// no ordering of its own.
struct Ranked {
    rank: Rank,
    row: Row,
}

impl Ord for Ranked {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.rank.cmp(&other.rank)
    }
}
impl PartialOrd for Ranked {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for Ranked {
    fn eq(&self, other: &Self) -> bool {
        self.rank == other.rank
    }
}
impl Eq for Ranked {}

/// The best `keep` rows by `Rank`, held in `keep + 1` rows of memory - or all of them,
/// in order, when nothing was capped.
///
/// The bounded case is a max-heap, worst row on top, so one push and one pop per row keeps
/// the best `keep` and drops the rest as they arrive. Collecting every matching row into a
/// `Vec` and sorting afterwards would allocate exactly what the cap was asked to avoid.
///
/// One more than asked for, deliberately: `rows_returned == limit` does not mean there is
/// another page, and a `next_cursor` that yields an empty page is a cursor that lied. The
/// extra row is held only to answer that question and dropped before the answer leaves.
struct Newest {
    /// `None` = keep everything.
    keep: Option<usize>,
    heap: BinaryHeap<Ranked>,
}

impl Newest {
    fn new(keep: Option<usize>) -> Self {
        Self {
            keep,
            heap: BinaryHeap::new(),
        }
    }

    fn offer(&mut self, rank: Rank, row: Row) {
        self.heap.push(Ranked { rank, row });
        if let Some(keep) = self.keep {
            if self.heap.len() > keep + 1 {
                // The greatest `Rank` is the last row in result order, so this drops the
                // oldest candidate and never one that belongs in the answer.
                self.heap.pop();
            }
        }
    }

    /// Newest first, and the `Rank` the next page should resume after.
    ///
    /// `into_sorted_vec` is ascending by `Ord`, and `Rank` is smallest-first. The overflow
    /// row - if the scan produced one - is the proof that a next page exists, and the row
    /// before it is where this one stopped.
    fn finish(self) -> (Vec<Row>, Option<Rank>) {
        let keep = self.keep;
        let mut ranked = self.heap.into_sorted_vec();
        let more = keep.is_some_and(|n| ranked.len() > n);
        if let Some(n) = keep {
            ranked.truncate(n);
        }
        let next = more.then(|| ranked.last().map(|r| r.rank)).flatten();
        (ranked.into_iter().map(|r| r.row).collect(), next)
    }
}

// ── The scan ───────────────────────────────────────────────────────────────────────

/// Both halves.
///
/// `QueryMode::Exists` is two questions wearing one name, and which one was asked is
/// decided by the time clause:
///
/// - **No time clause** - "does this template exist at all?". Answered by the walk, and it
///   reads no data: `files_opened` and `rows_scanned` both stay 0, which is the mode's
///   whole claim.
/// - **With a time clause** - "is this firing *now*?". `--exists --since 5m` is a cheap
///   liveness check, not a contradiction, so it scans - and stops dead on the first row
///   inside the window. `count` is 0 or 1 and `rows_scanned` is what it actually cost.
///
/// Reading `rows_scanned` is how the two are told apart, so both paths report it honestly
/// rather than one of them reporting a plausible zero.
///
/// `exists` keeps its meaning throughout: the template was found. A template that exists
/// and is not firing is `exists: true, count: 0` - already distinct from `exists: false`,
/// and distinguished the same way.
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
    let exists_only = query.mode == QueryMode::Exists;
    if exists_only && query.time.is_open() {
        return Ok(result);
    }

    let started = Instant::now();
    let mut opts = query.scan_options();
    let ts = query.ts_column();
    let mut newest = Newest::new(query.limit);

    for leaf in &plan.leaves {
        // A planned leaf with no store is a known template holding no rows: the second
        // of the two empty answers, reached without an error.
        let Some(store) = stores.lookup(leaf.template_id) else {
            continue;
        };
        result.meta.files_opened += 1;
        let preds: Vec<Predicate> = leaf.residual.iter().map(predicate).collect();

        // The store's own limit stops the read once `limit` rows have matched, and a disk
        // store will lean on it. Not used for `Fetch` here, deliberately: an in-memory
        // `Vec<Row>` is in insertion order, so stopping early would return the *oldest*
        // rows under an ordering that promises the newest. `Newest` bounds the memory
        // instead. Once partitions are read newest-first the early stop becomes correct
        // and this override goes away.
        opts.limit = if exists_only { 1 } else { 0 };

        let scan = match &query.mode {
            // One row is the whole answer, and `visit` stops on it.
            QueryMode::Exists | QueryMode::Count => store.visit(&opts, &preds, &mut |_, _| {})?,
            QueryMode::Fetch => {
                let key = leaf_key(&leaf.path);
                let keep = &mut newest;
                let after = query.cursor.map(|c| c.rank);
                // `seq` comes from the store, so it is the row's ingest ordinal and not a
                // count of what this scan admitted. A row leaving a relative `SINCE` window
                // between two pages used to renumber every row behind it in its leaf.
                store.visit(&opts, &preds, &mut |row, seq| {
                    let rank = (Reverse(ts_of(row, &ts)), key, seq);
                    // Served on an earlier page. `Rank` is a total order, so this is exact
                    // where a `ts` comparison alone is not: rows sharing one nanosecond
                    // split across the page boundary rather than being repeated by an
                    // inclusive bound or dropped by an exclusive one.
                    if after.is_some_and(|stop| rank <= stop) {
                        return;
                    }
                    keep.offer(rank, row.clone());
                })?
            }
            QueryMode::GroupBy(keys) => {
                let counts = &mut result.group_counts;
                store.visit(&opts, &preds, &mut |row, _| {
                    *counts.entry(group_key(row, keys)).or_insert(0) += 1;
                })?
            }
        };

        result.meta.rows_scanned += scan.scanned;
        result.count += scan.matched;
        if exists_only && result.count > 0 {
            break;
        }
    }

    if query.mode == QueryMode::Fetch {
        let (rows, next) = newest.finish();
        result.rows = rows;
        result.meta.next_cursor = next.map(|rank| Cursor::new(rank, query).encode());
    }
    // Rows in the answer, which for every mode but `Fetch` is none: `count` is what those
    // modes return and it is its own field. This used to be a second copy of `count`,
    // which was harmless while nothing could make the two differ - a limit can, and
    // `truncated` is only readable next to a `rows_returned` that means what it says.
    result.meta.rows_returned = result.rows.len() as u64;
    // The limit dropped rows that matched. Only `Fetch` can reach this: the other modes
    // return no rows, so `Query::new` normalised their limit away.
    //
    // Counted against `count`, which is every row matching the pattern and window on every
    // page - it is not reduced by the cursor. That is the point: page 7 of 483 still says
    // how many rows there are in total, so a limit is a statement about this response and
    // never about the answer.
    result.meta.truncated =
        query.mode == QueryMode::Fetch && result.meta.rows_returned < result.count;
    result.meta.limit_source = query.limit_source;
    result.meta.duration = plan.duration + started.elapsed();
    Ok(result)
}

/// One residual term as a scan predicate.
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
/// Two readings of the term's text, because a `Row` holds `ColValue` and not the raw line.
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
