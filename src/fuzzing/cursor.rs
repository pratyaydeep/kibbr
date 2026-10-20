// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The `AFTER` cursor (`engine::Cursor`), held to what a cursor is: a token a client
//! hands back is untrusted, so decoding never panics, a token that is not a cursor of its
//! query is refused, and an accepted one, real or forged, resolves to a suffix of the
//! query's own answer. Answers are compared against an order worked out here from the
//! fixture's lines, not the engine's; nothing reads a clock, a file or a socket.

use std::cmp::Reverse;
use std::collections::HashMap;
use std::sync::OnceLock;

use chrono::{DateTime, Utc};

use crate::config::{Limits, Schema};
use crate::error::{self, CliError};
use crate::cmd::query::{self, Args, Format};
use crate::core::flatten;
use crate::core::types::ColValue;
use crate::engine::engine::{self, Cursor, CursorError, Match, MatchValue, Query, QueryMode, Rank};
use crate::engine::trieql::Kind;
use crate::storage::db::{self, Db};

/// 2024-01-16T00:00:00Z: the instant every query is built against, and the arrival time of
/// every fixture line - the day after their timestamps, so `SINCE 1d` has rows both sides.
const NOW_SECS: i64 = 1_705_363_200;

/// The cursor's layout, written again rather than read off the engine: if the
/// engine's layout moves and its version does not, this is where it shows.
const VERSION: &str = "v3";
const BYTES: usize = 1 + 8 + 8 + 8 + 4;
const GUARD_AT: usize = BYTES - 4;

/// The server's page size for a `MATCH` with no `LIMIT`. Small, so that the backstop pages
/// the fixture and issues cursors as a caller's own limit does.
const BACKSTOP: usize = 4;

/// The bucket caps are small because `foreign` runs whatever `GROUP BY` the input wrote,
/// and the defaults would let one input zero-fill buckets to say nothing about a cursor.
const LIMITS_CONFIG: Limits = Limits {
    max_rows: BACKSTOP,
    max_buckets: 64,
    max_groups: 256,
};

/// The most a rendered refusal may add to the query it refuses: the sentence, a 73
/// character excerpt with each character escaped at its widest, and the caret line.
const RENDER_SLACK: usize = 2048;

/// The `LIMIT` clauses a header byte chooses between; `None` is no clause, which is the
/// backstop. Zero and the largest `usize` are both things a client can write.
const LIMITS: [Option<usize>; 8] = [
    None,
    Some(0),
    Some(1),
    Some(2),
    Some(3),
    Some(6),
    Some(40),
    Some(usize::MAX),
];

/// The most edits one input applies to a real cursor. A cursor is 61 bytes, so this is
/// enough to rewrite one whole field and too few to grow the token past a few lines.
const MAX_EDITS: usize = 24;

/// One fixture line. `n` - its index in `LINES` - is written into the line, so a returned
/// row says which line it is without the harness comparing whole rows.
struct Line {
    at: Option<&'static str>,
    level: &'static str,
    event: &'static str,
    host: Option<&'static str>,
    /// More members, as JSON text ending in a comma, under keys no pattern here names:
    /// what makes two lines two templates where everything a pattern reads is the same.
    more: &'static str,
}

const fn line(
    at: Option<&'static str>,
    level: &'static str,
    event: &'static str,
    host: Option<&'static str>,
) -> Line {
    Line {
        at,
        level,
        event,
        host,
        more: "",
    }
}

/// A `tick` line with members of its own, and so of a template of its own.
const fn keyed(at: &'static str, level: &'static str, more: &'static str) -> Line {
    Line {
        at: Some(at),
        level,
        event: "tick",
        host: None,
        more,
    }
}

/// The cases a cursor has to split correctly: ties inside a leaf, ties across leaves, a
/// late arrival, an instant before 1970, untimed lines, and a NUL in a key.
const LINES: &[Line] = &[
    line(Some("2024-01-15T10:00:00Z"), "ERROR", "tick", None),
    line(Some("2024-01-15T10:00:05Z"), "INFO", "tick", None),
    line(Some("2024-01-15T10:00:05Z"), "INFO", "tick", None),
    line(Some("2024-01-15T10:00:05Z"), "ERROR", "tick", None),
    line(Some("2024-01-15T09:59:00Z"), "ERROR", "tick", None),
    line(Some("2024-01-15T10:00:09Z"), "WARN", "tick", None),
    line(Some("2024-01-15T10:00:05Z"), "INFO", "tick", None),
    line(Some("1969-12-31T23:59:58Z"), "ERROR", "tick", None),
    line(Some("2024-01-15T10:00:09Z"), "ERROR", "tick", None),
    line(Some("2024-01-15T10:00:05Z"), "ERROR", "login", Some("a")),
    line(Some("2024-01-15T10:00:07Z"), "INFO", "login", Some("b")),
    line(Some("2024-01-15T10:00:05Z"), "ERROR", "login", Some("a")),
    line(Some("2024-01-15T10:00:00Z"), "ERROR", "login", Some("c")),
    line(None, "ERROR", "untimed", None),
    line(None, "INFO", "untimed", None),
    line(None, "ERROR", "untimed", None),
    line(None, "ERROR", "untimed", None),
    keyed("2024-01-15T10:00:08Z", "INFO", "\"a\\u0000b\":1,"),
    keyed("2024-01-15T10:00:08Z", "INFO", "\"a\":1,\"b\":1,"),
];

impl Line {
    fn json(&self, n: usize) -> String {
        let mut out = String::from("{");
        out.push_str(self.more);
        if let Some(at) = self.at {
            out.push_str(&format!("\"ts\":\"{at}\","));
        }
        if let Some(host) = self.host {
            out.push_str(&format!("\"host\":\"{host}\","));
        }
        out.push_str(&format!(
            "\"level\":\"{}\",\"event\":\"{}\",\"n\":{n}}}",
            self.level, self.event
        ));
        out
    }

    /// The effective time: the line's own, or the arrival time where it has none.
    fn nanos(&self) -> i64 {
        self.at.map_or(NOW_SECS * 1_000_000_000, nanos)
    }

    fn value(&self, key: &str) -> Option<&'static str> {
        match key {
            "ts" => self.at,
            "level" => Some(self.level),
            "event" => Some(self.event),
            "host" => self.host,
            _ => None,
        }
    }
}

/// A pattern as TrieQL writes it, and as terms the harness can match a `Line` against
/// without the engine: a key, and the value it has to equal or `None` for `*`.
struct Pattern {
    text: &'static str,
    terms: &'static [(&'static str, Option<&'static str>)],
}

/// The queries a cursor can belong to: six different patterns, including one that matches
/// no template, whose answer is empty and whose guard is as good as any other's.
const PATTERNS: &[Pattern] = &[
    Pattern {
        text: "{}",
        terms: &[],
    },
    Pattern {
        text: r#"{"level": "ERROR"}"#,
        terms: &[("level", Some("ERROR"))],
    },
    Pattern {
        text: r#"{"event": "tick"}"#,
        terms: &[("event", Some("tick"))],
    },
    Pattern {
        text: r#"{"host": *}"#,
        terms: &[("host", None)],
    },
    Pattern {
        text: r#"{"event": "tick", "level": "ERROR"}"#,
        terms: &[("event", Some("tick")), ("level", Some("ERROR"))],
    },
    Pattern {
        text: r#"{"event": "absent"}"#,
        terms: &[("event", Some("absent"))],
    },
];

impl Pattern {
    fn matches(&self, line: &Line) -> bool {
        self.terms.iter().all(|(key, want)| match want {
            None => line.value(key).is_some(),
            Some(want) => line.value(key) == Some(*want),
        })
    }
}

/// A time clause, and the half-open range of effective times it admits. The window is
/// not in the guard, so a cursor has to work under all three.
struct Window {
    text: &'static str,
    /// `--since`'s operand where the clause is a `SINCE`; a `BETWEEN` is flagged from the
    /// two instants below.
    ago: Option<&'static str>,
    since: Option<&'static str>,
    until: Option<&'static str>,
}

const WINDOWS: &[Window] = &[
    Window {
        text: "",
        ago: None,
        since: None,
        until: None,
    },
    // A day back from `NOW_SECS`, and open on the right as `SINCE` is.
    Window {
        text: " SINCE 1d",
        ago: Some("1d"),
        since: Some("2024-01-15T00:00:00Z"),
        until: None,
    },
    Window {
        text: " BETWEEN 2024-01-15T10:00:05Z AND 2024-01-15T10:00:09Z",
        ago: None,
        since: Some("2024-01-15T10:00:05Z"),
        until: Some("2024-01-15T10:00:09Z"),
    },
];

impl Window {
    fn admits(&self, at: i64) -> bool {
        self.since.is_none_or(|since| at >= nanos(since))
            && self.until.is_none_or(|until| at < nanos(until))
    }
}

fn nanos(rfc3339: &str) -> i64 {
    DateTime::parse_from_rfc3339(rfc3339)
        .expect("the fixture's instants are RFC 3339")
        .timestamp_nanos_opt()
        .expect("the fixture's instants fit in nanoseconds")
}

// ── The fixture ────────────────────────────────────────────────────────────────────

struct Fixture {
    db: Db,
    now: DateTime<Utc>,
    /// One built query per pattern, for `Cursor::check` to be asked about.
    queries: Vec<Query>,
    /// `Cursor::guard` of each. Taken from the engine: which number a pattern hashes to
    /// is the definition of "the same query", not something to work out twice.
    guards: Vec<u32>,
    /// The rank of each line of `LINES`, by its `n`.
    ranks: Vec<Rank>,
    /// `orders[pattern][window]`: every row that query returns with no cursor and no
    /// limit, in result order, as its rank and its line's `n`.
    orders: Vec<Vec<Vec<(Rank, i64)>>>,
    /// Every cursor the engine issues paging each pattern one row at a time, with the
    /// pattern it belongs to. The real cursors the edit mode starts from.
    issued: Vec<(usize, String)>,
}

/// Built once per process and then only read: a query takes no lock that outlives it.
fn fixture() -> &'static Fixture {
    static FIXTURE: OnceLock<Fixture> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        let fixture = Fixture::build();
        fixture.proves();
        fixture
    })
}

impl Fixture {
    /// The store, and the order worked out from the lines alone. Only `engine::leaf_key` is
    /// called and not written again: which number a key set hashes to is a contract.
    fn build() -> Self {
        let now = DateTime::from_timestamp(NOW_SECS, 0).expect("a representable instant");
        let lines: Vec<String> = LINES.iter().enumerate().map(|(n, l)| l.json(n)).collect();
        let db = db::load(lines.join("\n").as_bytes(), true, now, &Schema::default())
            .expect("the fixture loads");
        assert_eq!(
            db.stats.ingested,
            LINES.len() as u64,
            "every line is stored"
        );

        let mut seen: HashMap<u64, u64> = HashMap::new();
        let ranks: Vec<Rank> = LINES
            .iter()
            .zip(&lines)
            .enumerate()
            .map(|(n, (line, json))| {
                let parsed = flatten::parse_line(json.as_bytes(), n + 1).expect("a JSON line");
                let leaf = engine::leaf_key(&parsed.walk_tokens());
                let seq = seen.entry(leaf).or_insert(0);
                let rank = (Reverse(Some(line.nanos())), leaf, *seq);
                *seq += 1;
                rank
            })
            .collect();

        let orders = PATTERNS
            .iter()
            .map(|pattern| {
                WINDOWS
                    .iter()
                    .map(|window| {
                        let mut order: Vec<(Rank, i64)> = LINES
                            .iter()
                            .enumerate()
                            .filter(|(_, l)| pattern.matches(l) && window.admits(l.nanos()))
                            .map(|(n, _)| (ranks[n], n as i64))
                            .collect();
                        order.sort();
                        order
                    })
                    .collect()
            })
            .collect();

        let queries: Vec<Query> = (0..PATTERNS.len())
            .map(|p| {
                let (src, _) = source(QueryMode::Fetch, p, 0, None, None);
                query::from_trieql(&src, now, LIMITS_CONFIG).expect("a fixture query")
            })
            .collect();
        let guards: Vec<u32> = queries.iter().map(guard_of).collect();

        let mut fixture = Self {
            db,
            now,
            queries,
            guards,
            ranks,
            orders,
            issued: Vec::new(),
        };
        // Every row but the last of an answer is where some page stopped. That the
        // engine issues exactly these is `proves`' first check.
        for p in 0..PATTERNS.len() {
            let order = &fixture.orders[p][0];
            let stops = order.len().saturating_sub(1);
            let tokens: Vec<(usize, String)> = order[..stops]
                .iter()
                .map(|(rank, _)| (p, token(*rank, fixture.guards[p])))
                .collect();
            fixture.issued.extend(tokens);
        }
        fixture
    }

    /// What does not depend on the input, checked once: the order above is the engine's, and
    /// every real cursor is taken by its own pattern, refused by the others and by truncation.
    fn proves(&self) {
        assert_eq!(
            self.orders[0][0].len(),
            LINES.len(),
            "`{{}}` matches every line"
        );
        for (p, guard) in self.guards.iter().enumerate() {
            assert!(
                !self.guards[..p].contains(guard),
                "two fixture patterns share the guard {guard:08x}, so neither can refuse the other's cursor"
            );
        }
        for p in 0..PATTERNS.len() {
            for w in 0..WINDOWS.len() {
                for limit in LIMITS {
                    let (src, _) = source(QueryMode::Fetch, p, w, limit, None);
                    let query = self.built(&src);
                    self.pages(p, w, limit, query, None);
                }
            }
        }

        for (p, text) in &self.issued {
            let cursor = Cursor::decode(text).unwrap_or_else(|e| {
                panic!("the engine's own cursor `{text}` does not decode: {e}")
            });
            assert_eq!(
                &cursor.encode(),
                text,
                "an issued cursor does not round-trip"
            );
            for (other, pattern) in PATTERNS.iter().enumerate() {
                let (src, _) = source(QueryMode::Fetch, other, 0, None, Some(text));
                let built = query::from_trieql(&src, self.now, LIMITS_CONFIG);
                if other == *p {
                    assert_eq!(cursor.check(&self.queries[other]), Ok(()));
                    assert!(built.is_ok(), "`{src}` refuses its own cursor: {built:?}");
                } else {
                    assert_eq!(
                        cursor.check(&self.queries[other]),
                        Err(CursorError::WrongQuery),
                        "a cursor of `{}` is taken by `{}`",
                        PATTERNS[*p].text,
                        pattern.text
                    );
                    assert!(
                        matches!(built, Err(CliError::Cursor(CursorError::WrongQuery))),
                        "`{src}` did not refuse another query's cursor: {built:?}"
                    );
                }
            }
            for w in 0..WINDOWS.len() {
                for limit in LIMITS {
                    let (src, _) = source(QueryMode::Fetch, *p, w, limit, Some(text));
                    let built = query::from_trieql(&src, self.now, LIMITS_CONFIG);
                    assert!(built.is_ok(), "`{src}` refuses its own cursor: {built:?}");
                }
            }
            for cut in 0..text.len() {
                let short = &text[..cut];
                assert!(
                    Cursor::decode(short).is_err(),
                    "the truncated cursor `{short}` decodes"
                );
                let (src, quote) = source(QueryMode::Fetch, *p, 0, None, Some(short));
                match query::from_trieql(&src, self.now, LIMITS_CONFIG) {
                    Err(CliError::Trieql(e)) => {
                        assert!(matches!(e.kind, Kind::BadCursor { .. }), "`{src}`: {e:?}");
                        assert_eq!(e.pos, quote, "`{src}`: the error is not at the cursor");
                    }
                    other => panic!("`{src}` did not refuse a truncated cursor: {other:?}"),
                }
            }
        }
    }

    /// A query the harness wrote itself and knows to be well-formed.
    fn built(&self, src: &str) -> Query {
        query::from_trieql(src, self.now, LIMITS_CONFIG)
            .unwrap_or_else(|e| panic!("`{src}` is refused: {e:?}"))
    }

    /// Run `query` and every page after it, and hold each to the order worked out in `build`.
    /// `after` is a place in that order whether or not any row has that rank, forged or real.
    fn pages(&self, p: usize, w: usize, limit: Option<usize>, query: Query, after: Option<Cursor>) {
        let order = &self.orders[p][w];
        let size = limit.unwrap_or(BACKSTOP);
        let mut at = match after {
            Some(cursor) => order.partition_point(|(rank, _)| *rank <= cursor.rank()),
            None => 0,
        };
        let mut query = query;
        // One page per row at most, and one more for an answer that is empty.
        for _ in 0..=order.len() {
            let result = engine::execute(&self.db.trie, &self.db.stores, &query)
                .unwrap_or_else(|e| panic!("{query:?} failed: {e}"));
            let take = size.min(order.len() - at);
            let want: Vec<i64> = order[at..at + take].iter().map(|(_, n)| *n).collect();
            assert_eq!(
                numbers(&result.rows),
                want,
                "{query:?} did not return rows {at}..{} of its own order",
                at + take
            );
            assert_eq!(result.count, order.len() as u64, "{query:?}: count");
            assert_eq!(
                result.meta.rows_returned, take as u64,
                "{query:?}: rows_returned"
            );
            assert_eq!(
                result.meta.truncated,
                take < order.len(),
                "{query:?}: truncated"
            );
            let more = take > 0 && at + take < order.len();
            let next = more.then(|| token(order[at + take - 1].0, self.guards[p]));
            assert_eq!(result.meta.next_cursor, next, "{query:?}: next_cursor");
            let Some(next) = next else {
                return;
            };
            at += take;
            let (src, _) = source(QueryMode::Fetch, p, w, limit, Some(&next));
            query = self.built(&src);
        }
        panic!("paging `{}` did not end", PATTERNS[p].text);
    }

    /// The same rule as `pages`, for a query the input wrote: pattern `p` in other words. The
    /// window is whatever `alone` resolved it to: it may leave rows out and may not move one.
    fn resumes(&self, p: usize, alone: &Query, query: Query, cursor: Cursor, src: &str) {
        let whole = Query::new(
            alone.matches.clone(),
            QueryMode::Fetch,
            alone.time,
            Some(LINES.len() + 1),
            None,
            alone.now,
            alone.limits,
        );
        let result = engine::execute(&self.db.trie, &self.db.stores, &whole)
            .unwrap_or_else(|e| panic!("`{src}` with no cursor failed: {e}"));
        assert_eq!(
            result.meta.next_cursor, None,
            "`{src}`: more rows than lines"
        );
        let whole = numbers(&result.rows);
        let mut order = self.orders[p][0].iter();
        for n in &whole {
            assert!(
                order.any(|(_, m)| m == n),
                "`{src}` returns line {n} out of the order of `{}`",
                PATTERNS[p].text
            );
        }
        let rest: Vec<i64> = whole
            .iter()
            .copied()
            .filter(|n| self.ranks[*n as usize] > cursor.rank())
            .collect();

        let size = alone.limit.unwrap_or(usize::MAX);
        let (mut at, mut query) = (0, query);
        for _ in 0..=rest.len() {
            let result = engine::execute(&self.db.trie, &self.db.stores, &query)
                .unwrap_or_else(|e| panic!("{query:?} failed: {e}"));
            let take = size.min(rest.len() - at);
            assert_eq!(
                numbers(&result.rows),
                rest[at..at + take],
                "{query:?} did not resume its own order after the cursor"
            );
            assert_eq!(result.count, whole.len() as u64, "{query:?}: count");
            assert_eq!(
                result.meta.truncated,
                take < whole.len(),
                "{query:?}: truncated"
            );
            let more = take > 0 && at + take < rest.len();
            let next =
                more.then(|| token(self.ranks[rest[at + take - 1] as usize], self.guards[p]));
            assert_eq!(result.meta.next_cursor, next, "{query:?}: next_cursor");
            let Some(next) = next else {
                return;
            };
            at += take;
            query = self.built(&format!("{src} AFTER \"{next}\""));
        }
        panic!("paging `{src}` did not end");
    }

    /// A cursor of pattern `p` that the engine issued, chosen by `pick`; the pattern with an
    /// empty answer gets its own guard on the rank of a row it does not hold.
    fn cursor_of(&self, p: usize, pick: usize) -> String {
        let count = self.issued.iter().filter(|(own, _)| *own == p).count();
        if count == 0 {
            return token(self.ranks[0], self.guards[p]);
        }
        self.issued
            .iter()
            .filter(|(own, _)| *own == p)
            .nth(pick % count)
            .map(|(_, text)| text.clone())
            .expect("an index under the count")
    }
}

/// The lines an answer's rows are, by the `n` each was written with.
fn numbers(rows: &[crate::storage::store::Row]) -> Vec<i64> {
    rows.iter()
        .map(|row| match row.get("n") {
            Some(ColValue::Int64(n)) => *n,
            other => panic!("a row with no line number: {other:?}"),
        })
        .collect()
}

// ── The token, written again ───────────────────────────────────────────────────────

/// The canonical token for a rank and a guard: what `Cursor::encode` has to produce.
fn token(rank: Rank, guard: u32) -> String {
    let (Reverse(ts), leaf, seq) = rank;
    let mut bytes = [0u8; BYTES];
    if let Some(ns) = ts {
        bytes[0] = 1;
        bytes[1..9].copy_from_slice(&ns.to_be_bytes());
    }
    bytes[9..17].copy_from_slice(&leaf.to_be_bytes());
    bytes[17..25].copy_from_slice(&seq.to_be_bytes());
    bytes[GUARD_AT..].copy_from_slice(&guard.to_be_bytes());
    spell(&bytes)
}

fn spell(bytes: &[u8; BYTES]) -> String {
    let mut out = String::with_capacity(VERSION.len() + 1 + BYTES * 2);
    out.push_str(VERSION);
    out.push('.');
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// What a token is, by the sentence `CursorError::Malformed` prints: `v3.` followed by 58
/// hex digits, and nothing else.
enum Shape {
    Cursor([u8; BYTES]),
    /// No dot, or a body that is not 58 hex digits.
    Malformed,
    /// Text before the first dot that is not the version.
    Version(String),
}

fn shape(text: &str) -> Shape {
    let Some((version, body)) = text.split_once('.') else {
        return Shape::Malformed;
    };
    if version != VERSION {
        return Shape::Version(version.to_string());
    }
    let digits: Vec<u8> = body
        .chars()
        .map_while(|c| if c.is_ascii() { c.to_digit(16) } else { None })
        .map(|d| d as u8)
        // One past the length is enough to know it is too long.
        .take(BYTES * 2 + 1)
        .collect();
    if digits.len() != BYTES * 2 || body.len() != BYTES * 2 {
        return Shape::Malformed;
    }
    let mut bytes = [0u8; BYTES];
    for (slot, pair) in bytes.iter_mut().zip(digits.chunks_exact(2)) {
        *slot = pair[0] << 4 | pair[1];
    }
    Shape::Cursor(bytes)
}

/// The rank and guard 29 decoded bytes name, and the one token that should spell them:
/// either case of hex, any non-zero flag byte and unread timestamp bytes all fold here.
fn canonical(bytes: &[u8; BYTES]) -> (Rank, u32, String) {
    let word = |at: usize| {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&bytes[at..at + 8]);
        buf
    };
    let ts = (bytes[0] != 0).then(|| i64::from_be_bytes(word(1)));
    let rank = (
        Reverse(ts),
        u64::from_be_bytes(word(9)),
        u64::from_be_bytes(word(17)),
    );
    let mut guard = [0u8; 4];
    guard.copy_from_slice(&bytes[GUARD_AT..]);
    let guard = u32::from_be_bytes(guard);
    (rank, guard, token(rank, guard))
}

/// A query string as a client would send it, and the byte offset of the cursor's opening
/// quote - where a refusal of the cursor has to point.
fn source(
    mode: QueryMode,
    p: usize,
    w: usize,
    limit: Option<usize>,
    after: Option<&str>,
) -> (String, usize) {
    let verb = if mode == QueryMode::Fetch {
        "MATCH"
    } else {
        "COUNT"
    };
    let mut src = format!("{verb} LOG {}{}", PATTERNS[p].text, WINDOWS[w].text);
    if let Some(n) = limit {
        src.push_str(&format!(" LIMIT {n}"));
    }
    let mut quote = src.len();
    if let Some(text) = after {
        quote = resume(&mut src, text);
    }
    (src, quote)
}

/// ` AFTER "<text>"` on the end of a query, and where its opening quote landed; only the
/// two characters a TrieQL string escapes are escaped.
fn resume(src: &mut String, text: &str) -> usize {
    src.push_str(" AFTER ");
    let quote = src.len();
    src.push('"');
    for c in text.chars() {
        if c == '"' || c == '\\' {
            src.push('\\');
        }
        src.push(c);
    }
    src.push('"');
    quote
}

/// The query `source` writes, as the flag surface is handed it; `--after` takes the token
/// as it stands, with nothing to escape.
fn flags(mode: QueryMode, p: usize, w: usize, limit: Option<usize>, after: &str) -> Args {
    let fetching = mode == QueryMode::Fetch;
    let window = &WINDOWS[w];
    Args {
        trieql: None,
        matches: PATTERNS[p]
            .terms
            .iter()
            .map(|(key, want)| format!("{key}={}", want.unwrap_or("*")))
            .collect(),
        count: !fetching,
        fetch: fetching,
        exists: false,
        group_by: None,
        since: window.ago.map(str::to_string),
        between: match (window.ago, window.since, window.until) {
            (None, Some(from), Some(to)) => Some(vec![from.to_string(), to.to_string()]),
            _ => None,
        },
        format: Format::Table,
        explain: false,
        reference: false,
        limit,
        after: Some(after.to_string()),
        limits: LIMITS_CONFIG,
        schema: Schema::default(),
    }
}

// ── One input ──────────────────────────────────────────────────────────────────────

pub fn run(data: &[u8]) {
    let fx = fixture();
    let header = |at: usize| usize::from(data.get(at).copied().unwrap_or(0));
    let p = header(1) % PATTERNS.len();
    let limit = LIMITS[header(2) % LIMITS.len()];
    let w = header(3) % WINDOWS.len();
    let payload = data.get(4..).unwrap_or(&[]);
    // A token reaches the decoder as a `&str` on both surfaces, so bytes that are not UTF-8
    // are decoded lossily, which keeps a byte-level mutation of a cursor alive as a token.
    let text = match header(0) % 4 {
        0 => String::from_utf8_lossy(payload).into_owned(),
        1 => edited(fx, payload),
        2 => forged(fx, p, payload),
        _ => return foreign(fx, p, payload),
    };
    hold(fx, &text, p, w, limit);
}

/// A cursor the engine issued, with the input's edits applied: the first byte picks the
/// cursor, any pattern's, and each three bytes after it are one edit.
fn edited(fx: &Fixture, payload: &[u8]) -> String {
    let (pick, edits) = payload.split_first().unwrap_or((&0, &[]));
    let (_, real) = &fx.issued[usize::from(*pick) % fx.issued.len()];
    let mut text = real.clone().into_bytes();
    for edit in edits.chunks_exact(3).take(MAX_EDITS) {
        let (kind, at, value) = (edit[0], usize::from(edit[1]), edit[2]);
        let len = text.len();
        match kind % 8 {
            0 => text.truncate(at % (len + 1)),
            1 if len > 0 => text[at % len] = value,
            2 if len > 0 => text[at % len] ^= 1 << (value % 8),
            3 => text.insert(at % (len + 1), value),
            4 if len > 0 => {
                text.remove(at % len);
            }
            // One digit of the body set to another digit: the token keeps its shape and
            // names a different rank or guard.
            5 if len == VERSION.len() + 1 + BYTES * 2 => {
                let digit = b"0123456789abcdef"[usize::from(value % 16)];
                text[VERSION.len() + 1 + at % (BYTES * 2)] = digit;
            }
            // Another pattern's guard on this rank: what a client forging a cursor for a
            // query it did not page would write.
            6 if len >= 8 => {
                let guard = fx.guards[usize::from(value) % fx.guards.len()];
                text.truncate(len - 8);
                text.extend_from_slice(format!("{guard:08x}").as_bytes());
            }
            7 if len > 0 => {
                let b = &mut text[at % len];
                *b = if b.is_ascii_lowercase() {
                    b.to_ascii_uppercase()
                } else {
                    b.to_ascii_lowercase()
                };
            }
            _ => {}
        }
    }
    String::from_utf8_lossy(&text).into_owned()
}

/// A well-formed token from raw bytes, so the fuzzer mutates a rank as integers. Bit 0 of
/// the flag byte writes the query's own guard - the forgery the guard does not stop.
fn forged(fx: &Fixture, p: usize, payload: &[u8]) -> String {
    let mut bytes = [0u8; BYTES];
    for (slot, b) in bytes.iter_mut().zip(payload) {
        *slot = *b;
    }
    let flags = payload.get(BYTES).copied().unwrap_or(0);
    if flags & 1 != 0 {
        bytes[GUARD_AT..].copy_from_slice(&fx.guards[p].to_be_bytes());
    }
    let text = spell(&bytes);
    if flags & 2 != 0 {
        // The version's `v` is lower case and has to stay so; only the digits move.
        let (version, body) = text.split_at(VERSION.len() + 1);
        return format!("{version}{}", body.to_ascii_uppercase());
    }
    text
}

/// Everything a token is held to, against pattern `p` under window `w` and `limit`.
fn hold(fx: &Fixture, text: &str, p: usize, w: usize, limit: Option<usize>) {
    // Decoding: a cursor exactly when the text has a cursor's shape, and the refusal the
    // shape calls for when it has not.
    let cursor = match (shape(text), Cursor::decode(text)) {
        (Shape::Cursor(bytes), Ok(cursor)) => Some((bytes, cursor)),
        (Shape::Malformed, Err(CursorError::Malformed)) => None,
        (Shape::Version(want), Err(CursorError::Version { found })) if want == found => None,
        (Shape::Cursor(_), Err(e)) => panic!("`{text}` is a cursor and was refused: {e}"),
        (_, Ok(cursor)) => panic!("`{text}` is not a cursor and decoded to {cursor:?}"),
        (_, Err(e)) => panic!("`{text}` was refused for the wrong reason: {e}"),
    };

    // The modes that return no rows first: `Query::new` drops the cursor unread, but the
    // parser must still refuse text that is not one, and a dropped one must not move the answer.
    let total = fx.orders[p][0].len() as u64;
    let (src, quote) = source(QueryMode::Count, p, 0, None, Some(text));
    match (query::from_trieql(&src, fx.now, LIMITS_CONFIG), &cursor) {
        (Ok(query), Some(_)) => {
            assert_eq!(
                query.cursor, None,
                "`{src}` kept a cursor it cannot page by"
            );
            let result = engine::execute(&fx.db.trie, &fx.db.stores, &query)
                .unwrap_or_else(|e| panic!("`{src}` failed: {e}"));
            assert_eq!(result.count, total, "`{src}`: count");
            assert!(result.rows.is_empty(), "`{src}` returned rows");
        }
        (Err(e), None) => refused_at(&src, &e, quote),
        (other, _) => panic!("`{src}`: {other:?}"),
    }
    let mut src = format!("MATCH LOG {} GROUP BY level", PATTERNS[p].text);
    let quote = resume(&mut src, text);
    match (query::from_trieql(&src, fx.now, LIMITS_CONFIG), &cursor) {
        (Ok(query), Some(_)) => {
            assert_eq!(
                query.cursor, None,
                "`{src}` kept a cursor it cannot page by"
            );
            let result = engine::execute(&fx.db.trie, &fx.db.stores, &query)
                .unwrap_or_else(|e| panic!("`{src}` failed: {e}"));
            let grouped: u64 = result.groups.iter().map(|g| g.count).sum();
            assert_eq!((result.count, grouped), (total, total), "`{src}`: count");
            assert!(result.rows.is_empty(), "`{src}` returned rows");
            assert_eq!(result.meta.next_cursor, None, "`{src}` issued a cursor");
        }
        (Err(e), None) => refused_at(&src, &e, quote),
        (other, _) => panic!("`{src}`: {other:?}"),
    }
    // And `--count --after`: the flags hand the token to the decoder with no parser in
    // front of it, so the refusal is the decoder's own.
    let counted = query::build(&flags(QueryMode::Count, p, 0, None, text), fx.now);
    match (counted, Cursor::decode(text)) {
        (Ok(query), Ok(_)) => assert_eq!(query.cursor, None, "--count kept `{text}`"),
        (Err(CliError::Cursor(got)), Err(want)) => {
            assert_eq!(got, want, "--count --after `{text}`");
        }
        (other, _) => panic!("--count --after `{text}`: {other:?}"),
    }

    let (src, quote) = source(QueryMode::Fetch, p, w, limit, Some(text));
    let built = query::from_trieql(&src, fx.now, LIMITS_CONFIG);
    let by_flags = query::build(&flags(QueryMode::Fetch, p, w, limit, text), fx.now);
    let Some((bytes, cursor)) = cursor else {
        match built {
            Err(e) => refused_at(&src, &e, quote),
            other => panic!("`{src}` took text that is not a cursor: {other:?}"),
        }
        match (by_flags, Cursor::decode(text)) {
            (Err(CliError::Cursor(got)), Err(want)) => {
                assert_eq!(got, want, "--after `{text}`");
                rendered(text, &CliError::Cursor(got));
            }
            (other, _) => panic!("--after took text that is not a cursor: {other:?}"),
        }
        return unquoted(fx, text, p);
    };

    // What it decoded to: the rank and guard its digits spell, under one spelling.
    let (rank, guard, spelling) = canonical(&bytes);
    assert_eq!(cursor.rank(), rank, "`{text}` decoded to another position");
    assert_eq!(
        cursor.encode(),
        spelling,
        "`{text}` does not re-encode canonically"
    );
    assert_eq!(
        Cursor::decode(&spelling),
        Ok(cursor),
        "`{text}` and its own re-encoding `{spelling}` are two cursors"
    );

    // Whose it is: the query whose guard it carries, and no other.
    for (other, query) in fx.queries.iter().enumerate() {
        let want = if guard == fx.guards[other] {
            Ok(())
        } else {
            Err(CursorError::WrongQuery)
        };
        assert_eq!(
            cursor.check(query),
            want,
            "`{text}` against `{}`",
            PATTERNS[other].text
        );
    }

    // And through both doors: refused as another query's, or taken and resolved to a
    // suffix of this query's own order.
    if guard == fx.guards[p] {
        let query = built.unwrap_or_else(|e| panic!("`{src}` refused its own cursor: {e:?}"));
        assert_eq!(query.cursor, Some(cursor), "`{src}` carries another cursor");
        // The flags build the query the text does, cursor included, so what `pages` shows
        // of one it shows of both.
        let flagged = by_flags.unwrap_or_else(|e| panic!("--after refused `{text}`: {e:?}"));
        assert_eq!(
            flagged.matches, query.matches,
            "`{src}`: the flags' pattern"
        );
        assert_eq!(flagged.mode, query.mode, "`{src}`: the flags' mode");
        assert_eq!(
            (flagged.time.since, flagged.time.until),
            (query.time.since, query.time.until),
            "`{src}`: the flags' window"
        );
        assert_eq!(
            (flagged.limit, flagged.limit_source),
            (query.limit, query.limit_source),
            "`{src}`: the flags' limit"
        );
        assert_eq!(flagged.cursor, query.cursor, "`{src}`: the flags' cursor");
        fx.pages(p, w, limit, query, Some(cursor));
    } else {
        for (door, built) in [("AFTER", built), ("--after", by_flags)] {
            match built {
                Err(e @ CliError::Cursor(CursorError::WrongQuery)) => rendered(&src, &e),
                other => panic!("`{src}`: {door} did not refuse another query's cursor: {other:?}"),
            }
        }
    }
    unquoted(fx, text, p);
}

/// A refusal of the cursor's text by the parser: it is `BadCursor`, and it points at the
/// cursor's opening quote - inside the query, at the token that is wrong.
fn refused_at(src: &str, e: &CliError, quote: usize) {
    rendered(src, e);
    let CliError::Trieql(e) = e else {
        panic!("`{src}` was refused by something other than the parser: {e:?}");
    };
    assert!(matches!(e.kind, Kind::BadCursor { .. }), "`{src}`: {e:?}");
    assert_eq!(e.pos, quote, "`{src}`: the error is not at the cursor");
    assert!(e.pos < src.len(), "`{src}`: the error is outside the query");
}

/// The refusal as `POST /query` answers it and the CLI prints it. It quotes the client's
/// text back, so it is held to being a sentence and to costing the query plus a constant.
fn rendered(src: &str, e: &CliError) {
    let line = error::chain(e);
    assert!(!line.is_empty(), "`{src}` was refused with nothing said");
    assert!(
        line.len() <= src.len() + RENDER_SLACK,
        "`{src}` is {} bytes and its refusal is {}",
        src.len(),
        line.len()
    );
}

/// A cursor the engine issued for pattern `p`, sent back with a query the input wrote: `p`
/// respelled takes it, any other pattern refuses it, a `GROUP BY` drops it unread.
fn foreign(fx: &Fixture, p: usize, payload: &[u8]) {
    let (pick, written) = payload.split_first().unwrap_or((&0, &[]));
    let written = String::from_utf8_lossy(written);
    let real = fx.cursor_of(p, usize::from(*pick));
    let cursor = Cursor::decode(&real)
        .unwrap_or_else(|e| panic!("the fixture's own cursor `{real}` does not decode: {e}"));

    let bare = format!("MATCH LOG {written}");
    let with = format!("{bare} AFTER \"{real}\"");
    let resumed = query::from_trieql(&with, fx.now, LIMITS_CONFIG);
    let alone = match query::from_trieql(&bare, fx.now, LIMITS_CONFIG) {
        Ok(alone) => alone,
        Err(e) => {
            rendered(&bare, &e);
            if let Err(e) = resumed {
                rendered(&with, &e);
            }
            return;
        }
    };
    if alone.cursor.is_some() {
        // The text brought a cursor of its own, and the grammar has room for one.
        match resumed {
            Err(e @ CliError::Trieql(_)) => rendered(&with, &e),
            other => panic!("`{with}` has two cursors and was not refused: {other:?}"),
        }
        return;
    }

    if alone.mode != QueryMode::Fetch {
        let query =
            resumed.unwrap_or_else(|e| panic!("`{with}` refused a cursor it does not read: {e:?}"));
        assert_eq!(
            query.cursor, None,
            "`{with}` kept a cursor it cannot page by"
        );
        let run = |q: &Query| engine::execute(&fx.db.trie, &fx.db.stores, q);
        match (run(&alone), run(&query)) {
            (Ok(alone), Ok(resumed)) => {
                assert_eq!(
                    (resumed.count, &resumed.groups),
                    (alone.count, &alone.groups),
                    "`{with}`: the cursor moved a grouped answer"
                );
                assert!(resumed.rows.is_empty(), "`{with}` returned rows");
            }
            (Err(_), Err(_)) => {}
            (alone, resumed) => panic!("`{with}`: {resumed:?} where `{bare}` gave {alone:?}"),
        }
        return;
    }

    confused(fx, &alone, &bare, *pick);

    let own = &fx.queries[p];
    let guard = guard_of(&alone);
    let taken = if guard == fx.guards[p] {
        Ok(())
    } else {
        Err(CursorError::WrongQuery)
    };
    assert_eq!(cursor.check(&alone), taken, "`{real}` against `{bare}`");

    if alone.matches == own.matches {
        let query =
            resumed.unwrap_or_else(|e| panic!("`{with}` refused its own pattern's cursor: {e:?}"));
        assert_eq!(
            query.cursor,
            Some(cursor),
            "`{with}` carries another cursor"
        );
        return fx.resumes(p, &alone, query, cursor, &bare);
    }
    if guard != fx.guards[p] {
        match resumed {
            Err(e @ CliError::Cursor(CursorError::WrongQuery)) => rendered(&with, &e),
            other => panic!("`{with}` did not refuse another query's cursor: {other:?}"),
        }
        return;
    }
    told_apart(own, &alone, &with);
}

/// The pattern the input wrote against its `twin`, sent a cursor of the first. A fuzzer
/// does not find the one neighbour that matters by changing bytes, so it is made here.
fn confused(fx: &Fixture, alone: &Query, bare: &str, pick: u8) {
    let Some(twin) = twin(&alone.matches, pick >> 6) else {
        return;
    };
    assert_ne!(twin, alone.matches, "`{bare}`: a twin is another pattern");
    let src = format!("MATCH LOG {}", pattern_text(&twin));
    let other = match query::from_trieql(&src, fx.now, LIMITS_CONFIG) {
        Ok(other) => other,
        // The one twin that is not a pattern: its `=` was the first character of the key,
        // and a key may not be empty.
        Err(e) => return rendered(&src, &e),
    };
    assert_eq!(other.matches, twin, "`{src}` did not read back as written");

    let cursor = alone.cursor_at(fx.ranks[usize::from(pick) % fx.ranks.len()]);
    let with = format!("{src} AFTER \"{}\"", cursor.encode());
    if guard_of(&other) == guard_of(alone) {
        return told_apart(alone, &other, &with);
    }
    match query::from_trieql(&with, fx.now, LIMITS_CONFIG) {
        Err(e @ CliError::Cursor(CursorError::WrongQuery)) => rendered(&with, &e),
        other => panic!("`{with}` did not refuse the cursor of `{bare}`: {other:?}"),
    }
}

/// A pattern that is not `matches` and that a spelling of terms as `key=value` with a NUL
/// between them cannot tell from it, or `None` where the rewrite has nothing to move.
fn twin(matches: &[Match], rewrite: u8) -> Option<Vec<Match>> {
    let spell = |value: &MatchValue| match value {
        MatchValue::Any => "*".to_string(),
        MatchValue::Exact(text) => text.clone(),
    };
    let mut out = matches.to_vec();
    match rewrite {
        0 => None,
        1 => {
            let mut moved = false;
            for term in &mut out {
                term.value = match &term.value {
                    MatchValue::Any => MatchValue::Exact("*".to_string()),
                    MatchValue::Exact(text) if text == "*" => MatchValue::Any,
                    MatchValue::Exact(_) => continue,
                };
                moved = true;
            }
            moved.then_some(out)
        }
        2 => {
            let [first, second, ..] = matches else {
                return None;
            };
            let folded = Match {
                key: first.key.clone(),
                value: MatchValue::Exact(format!(
                    "{}\0{}={}",
                    spell(&first.value),
                    second.key,
                    spell(&second.value)
                )),
            };
            out.splice(0..2, [folded]);
            Some(out)
        }
        _ => {
            for term in &mut out {
                if let Some((key, rest)) = term.key.split_once('=') {
                    let (key, rest) = (key.to_string(), rest.to_string());
                    term.value = MatchValue::Exact(format!("{rest}={}", spell(&term.value)));
                    term.key = key;
                    return Some(out);
                }
                let MatchValue::Exact(text) = &term.value else {
                    continue;
                };
                if let Some((head, tail)) = text.split_once('=') {
                    term.key = format!("{}={head}", term.key);
                    term.value = MatchValue::Exact(tail.to_string());
                    return Some(out);
                }
            }
            None
        }
    }
}

/// A pattern as TrieQL: each key and each exact value a JSON string - TrieQL's strings
/// take JSON's escapes - and a wildcard the bare `*`.
fn pattern_text(matches: &[Match]) -> String {
    let quoted = |text: &str| serde_json::to_string(text).expect("a string is JSON");
    let terms: Vec<String> = matches
        .iter()
        .map(|term| match &term.value {
            MatchValue::Any => format!("{}: *", quoted(&term.key)),
            MatchValue::Exact(text) => format!("{}: {}", quoted(&term.key), quoted(text)),
        })
        .collect();
    format!("{{{}}}", terms.join(", "))
}

/// Two different patterns with one guard, the second having taken a cursor of the first:
/// fails unless they are different bytes that hashed alike, which is the accepted accident.
fn told_apart(issued_by: &Query, taken_by: &Query, with: &str) {
    assert!(
        spelled(issued_by) != spelled(taken_by),
        "`{with}` takes the cursor of another pattern, {:?}: the guard hashes the same \
         bytes for both, {:?}",
        issued_by.tokens(),
        String::from_utf8_lossy(&spelled(issued_by))
    );
}

/// The bytes `Cursor::guard` hashes of a pattern, taken from the engine and not written
/// again: a second copy would say what the guard ought to hash, not what it does.
fn spelled(query: &Query) -> Vec<u8> {
    Cursor::spelling(query)
}

/// `Cursor::guard` of a query, held to being FNV-1a over `spelled`, folded to 32 bits:
/// `told_apart` is only an answer if the spelling is what was hashed.
fn guard_of(query: &Query) -> u32 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in spelled(query) {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let guard = Cursor::guard(query);
    assert_eq!(
        guard,
        ((h >> 32) ^ h) as u32,
        "the guard of {:?} is not a hash of the bytes it is said to hash",
        query.tokens()
    );
    guard
}

/// The text pasted between the quotes with nothing escaped, as a client concatenating
/// sends it: whatever the parser makes of it, a cursor that comes out resolves as any other.
fn unquoted(fx: &Fixture, text: &str, p: usize) {
    if !text.contains(['"', '\\']) {
        return;
    }
    let src = format!("MATCH LOG {} AFTER \"{text}\"", PATTERNS[p].text);
    let Ok(query) = query::from_trieql(&src, fx.now, LIMITS_CONFIG) else {
        return;
    };
    let cursor = query
        .cursor
        .unwrap_or_else(|| panic!("`{src}` parsed and lost its cursor"));
    assert_eq!(
        cursor.check(&query),
        Ok(()),
        "`{src}` kept a foreign cursor"
    );
    assert_eq!(
        cursor.check(&fx.queries[p]),
        Ok(()),
        "`{src}` is another query"
    );
    fx.pages(p, 0, None, query, Some(cursor));
}
