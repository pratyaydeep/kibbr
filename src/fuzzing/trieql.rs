//! The TrieQL parser (`trieql::parse`), held to the grammar: it never panics, and every input
//! either parses or is refused with an error whose byte position lies inside the input.
//!
//! **What the bytes become.** The parser takes a `&str`, and both surfaces that reach it
//! refuse anything that is not UTF-8 before it runs: `POST /query` decodes its body with
//! `serde_json`, and `kibbr query` takes the string from `argv` through clap. So a client
//! can send any sequence of Unicode scalar values - a NUL included, as `\u0000` in the
//! JSON - and nothing else. An input that is valid UTF-8 is therefore parsed exactly as it
//! is, and that alone covers everything a client can send. An input that is not is decoded
//! lossily rather than skipped: each ill-formed sequence becomes U+FFFD, which keeps the
//! fuzzer's byte-level mutations of a valid query alive as queries, and puts a three-byte
//! character wherever the mutation landed - the case byte offsets go wrong on.
//!
//! **What is asserted.** On a refusal: the position is in `0..=len` and on a character
//! boundary, the message renders and names that position, and the caret under the excerpt
//! points at the character the position names. On a parse: the answer holds none of the
//! things the grammar refuses. Then the same string goes through `query::from_trieql`,
//! which is the whole of what `POST /query` does with it before any storage is touched,
//! and the two have to agree.
//!
//! **Why in bounds is not enough.** `ParseError::new` clamps whatever offset it is handed
//! to the query's length and back to a character boundary, so the two bounds assertions
//! hold whatever the parser's callers pass: a parser that counted characters where it says
//! bytes would never fail them. What such a parser cannot do is point at the right thing.
//! So a refusal is also held to its own sentence - the character at the position has to be
//! the first character of what the error says it found (`points_at`) - and to the query -
//! everything before the position has to be a query that had not gone wrong yet
//! (`nothing_wrong_before`).
//!
//! Nothing here reads a clock, a file or a socket: `now` is fixed, so a crasher replays
//! to the same answer in `tests/fuzz_regressions.rs` as it gave under libFuzzer.

use chrono::{DateTime, Utc};

use crate::config::Limits;
use crate::error::{self, CliError, QueryError};
use crate::cmd::query;
use kibbr_engine::engine::{Cursor, CursorError, ExecError, GroupKey, QueryMode};
use kibbr_engine::trieql::{self, Kind, ParseError, Parsed};

/// The instant `SINCE` is counted back from. Fixed, and the one `crates/kibbr-engine/src/trieql.rs` tests
/// against, so a seed taken from a test parses to the window the test saw.
const NOW_SECS: i64 = 1_700_000_000;

/// The most characters an excerpt line may run to, whatever the size of the query.
///
/// `ParseError` cuts its excerpt at construction "because the query may be a megabyte":
/// 36 characters either side of the position, each at most eight wide once an invisible
/// one is written as `\u{feff}`, and an ellipsis at each cut edge. That is 578. The bound
/// is held loosely on purpose - it is here to catch an excerpt that grew with the input,
/// not to pin the window's width.
const EXCERPT_MAX_CHARS: usize = 1024;

/// The indent `ParseError`'s `Display` puts before the excerpt and before the caret.
const INDENT: &str = "    ";

/// The characters that are a token on their own. A bare word cannot begin with one, so an
/// error that says it found a word and points at one of these is pointing at the wrong
/// place.
const PUNCTUATION: [char; 7] = ['{', '}', '(', ')', ':', ',', '*'];

pub fn run(data: &[u8]) {
    match std::str::from_utf8(data) {
        Ok(src) => check(src),
        Err(_) => check(&String::from_utf8_lossy(data)),
    }
}

fn check(src: &str) {
    let now: DateTime<Utc> =
        DateTime::from_timestamp(NOW_SECS, 0).expect("a representable instant");

    let parsed = trieql::parse(src, now);
    match &parsed {
        Ok(parsed) => parsed_holds(src, parsed),
        Err(e) => refusal_holds(src, e, now),
    }

    // The path a client's string actually takes. A parse error has to come out of it
    // unchanged, and a string that parsed must not turn into a parse error on the way:
    // either would mean the parser gave two answers for one input.
    match (parsed, query::from_trieql(src, now, Limits::default())) {
        (Err(e), Err(CliError::Trieql(f))) => {
            assert_eq!(
                e.pos,
                f.pos,
                "{}: two parses of one query disagree on the position",
                shown(src)
            );
            assert_eq!(
                e.to_string(),
                f.to_string(),
                "{}: two parses of one query disagree on the message",
                shown(src)
            );
            // The body of the 400. `chain` walks the source chain, which is the other
            // thing an error type can get wrong without the parser noticing.
            let _ = error::chain(&CliError::Trieql(f));
        }
        (Err(e), Ok(_)) => panic!(
            "{}: refused by the parser at byte {} and accepted by from_trieql",
            shown(src),
            e.pos
        ),
        (Err(e), Err(other)) => panic!(
            "{}: refused by the parser at byte {} and by from_trieql for something else: {other}",
            shown(src),
            e.pos
        ),
        (Ok(_), Err(CliError::Trieql(f))) => panic!(
            "{}: parsed, and then refused by the parser at byte {} inside from_trieql",
            shown(src),
            f.pos
        ),
        // A query the grammar accepts and the builder's own rules refuse. There are three
        // such rules and each is named, because every other refusal is one this string
        // should not be able to reach: the parser has already said the cursor decodes, so
        // a malformed one here is the two disagreeing about what a cursor is, and a
        // storage or I/O error would mean a string was enough to touch the disk.
        (Ok(_), Err(other)) => {
            assert!(
                matches!(
                    other,
                    CliError::Cursor(CursorError::WrongQuery)
                        | CliError::Exec(ExecError::Buckets(_))
                        | CliError::Query(QueryError::BucketKeyClash)
                ),
                "{}: parsed, and then refused for something a parsed query cannot be: {other}",
                shown(src)
            );
            // Rendered because the client is sent it.
            let _ = error::chain(&other);
        }
        (Ok(_), Ok(_)) => {}
    }
}

/// A refusal: where it says it was has to be a place in the query, and the right one.
fn refusal_holds(src: &str, e: &ParseError, now: DateTime<Utc>) {
    assert!(
        e.pos <= src.len(),
        "{}: error at byte {}, past the end of a {}-byte query",
        shown(src),
        e.pos,
        src.len()
    );
    assert!(
        src.is_char_boundary(e.pos),
        "{}: error at byte {}, which is inside a character",
        shown(src),
        e.pos
    );

    // `Display` is what the CLI prints and what the 400 carries, and it holds the two
    // other things that point into the query: the excerpt and the caret column. Both are
    // private to `ParseError`, so they are read back off the rendering - the last two
    // lines of it, whatever the sentence above them holds.
    let rendered = e.to_string();
    let mut lines = rendered.rsplitn(3, '\n');
    let (Some(caret_line), Some(excerpt_line), Some(sentence)) =
        (lines.next(), lines.next(), lines.next())
    else {
        panic!(
            "{}: the error does not render as three lines: {rendered:?}",
            shown(src)
        );
    };
    // The number a client reads is the one in the sentence, not the field, so the two
    // have to be the same number.
    assert!(
        sentence.ends_with(&format!(" at byte {}", e.pos)),
        "{}: error at byte {}, and the message says otherwise: {sentence:?}",
        shown(src),
        e.pos
    );
    let Some(excerpt) = excerpt_line.strip_prefix(INDENT) else {
        panic!(
            "{}: the excerpt line is not indented: {excerpt_line:?}",
            shown(src)
        );
    };
    let caret = caret_line
        .strip_prefix(INDENT)
        .and_then(|rest| rest.strip_suffix('^'))
        .filter(|spaces| spaces.bytes().all(|b| b == b' '))
        .map(str::len);
    let Some(caret) = caret else {
        panic!(
            "{}: the caret line is not spaces and a caret: {caret_line:?}",
            shown(src)
        );
    };

    let width = excerpt.chars().count();
    assert!(
        width <= EXCERPT_MAX_CHARS,
        "{}: an excerpt {width} characters wide",
        shown(src)
    );
    assert!(
        caret <= width,
        "{}: caret at column {caret} of an excerpt {width} characters wide",
        shown(src)
    );

    // In bounds is not the same as in the right place, and a caret that always sat at
    // column 0 would pass the check above. At the end of the query the caret is one past
    // the excerpt's last character; anywhere else it is under the character the position
    // names, however the excerpt chose to print that character. A multibyte character is
    // checked like any other: it is the one a caret counted in bytes lands wrong on.
    let under: &str = excerpt
        .char_indices()
        .nth(caret)
        .map_or("", |(i, _)| &excerpt[i..]);
    match src[e.pos..].chars().next() {
        None => assert_eq!(
            caret,
            width,
            "{}: error at the end of the query, caret not at the end of the excerpt",
            shown(src)
        ),
        Some(c) => assert!(
            printed_as(under, c),
            "{}: error at byte {} is at {c:?}, and the caret is under something else in {excerpt:?}",
            shown(src),
            e.pos
        ),
    }

    points_at(src, e);
    nothing_wrong_before(src, e, now);
}

/// Does `text` begin with `c` as an error message prints it?
///
/// The parser writes a character that does not render as an escape, and a newline or a
/// tab as one space so the caret still lines up. Which characters it treats so is its own
/// business and is not repeated here: any of the three spellings is accepted for any
/// character it could apply to, so this says "that character", not "that rendering".
fn printed_as(text: &str, c: char) -> bool {
    text.starts_with(c)
        || (matches!(c, '\n' | '\r' | '\t') && text.starts_with(' '))
        || text.starts_with(&format!("\\u{{{:04x}}}", c as u32))
}

/// Is `c` a character a bare word can begin with? Not whitespace, which separates tokens,
/// not a quote, which begins a string, and not a character that is a token by itself.
fn begins_a_word(c: char) -> bool {
    !c.is_whitespace() && c != '"' && !PUNCTUATION.contains(&c)
}

/// The position against the error's own sentence: each kind of refusal accuses one kind
/// of thing, and the character at the position has to be the first character of it.
///
/// This is what an offset that is in bounds and wrong fails. A position counted in
/// characters, or taken one token late, still lands inside the query, and the caret
/// follows it faithfully; it is only against what the error says it found there that it
/// is seen to be somewhere else. The match is exhaustive on purpose: a new kind of
/// refusal has to say here what it points at.
fn points_at(src: &str, e: &ParseError) {
    let rest = &src[e.pos..];
    let at = rest.chars().next();
    let ok = match &e.kind {
        // The token the parser stopped at, named in `found`: the end of the query, a
        // string, a punctuation mark, or a bare word quoted from its first character.
        Kind::Expected { found, .. } => match at {
            None => found.as_str() == "end of query",
            Some('"') => found.starts_with("the string `"),
            Some(c) if PUNCTUATION.contains(&c) => *found == format!("`{c}`"),
            Some(c) => {
                begins_a_word(c)
                    && found
                        .strip_prefix('`')
                        .is_some_and(|word| printed_as(word, c))
            }
        },
        // A string that never closed is reported at the quote that opened it, and a
        // cursor that is not one at the quote that opens the cursor.
        Kind::Unterminated | Kind::BadCursor { .. } => at == Some('"'),
        // The backslash, and the character after it is the one the sentence names.
        Kind::BadEscape { ch } => {
            let mut chars = rest.chars();
            chars.next() == Some('\\') && chars.next() == Some(*ch)
        }
        // The `u` of the escape, which has its backslash immediately before it.
        Kind::BadUnicode => at == Some('u') && src[..e.pos].ends_with('\\'),
        // Operands the grammar spells bare: a quoted one is refused as not a word before
        // its text is ever read as a duration or a number.
        Kind::BadDuration | Kind::BadLimit | Kind::ZeroBucket => at.is_some_and(begins_a_word),
        // A datetime is bare or quoted, and a backwards window is reported at its end.
        Kind::BadDatetime | Kind::Backwards { .. } => {
            at.is_some_and(|c| c == '"' || begins_a_word(c))
        }
        // Only a quoted string can be empty: a bare word holds at least the character
        // that began it.
        Kind::EmptyKey => rest.starts_with("\"\""),
        // The second `time`, which is a bare word whatever its case.
        Kind::TwoTimeBuckets => rest
            .get(..4)
            .is_some_and(|word| word.eq_ignore_ascii_case("time")),
    };
    assert!(
        ok,
        "{}: `{}` reported at byte {}, which is at {:?} - not what that error points at",
        shown(src),
        e.kind,
        e.pos,
        shown(rest)
    );
}

/// The position against the query: what stands before it had not gone wrong yet.
///
/// The parser reads tokens left to right and never looks further than one past the token
/// in hand, so an error at a token is an error the tokens before it did not cause. Cut the
/// query at the position and what is left either parses, or is refused for nothing but
/// having stopped: something was expected, and the end of the query was found, at the cut.
/// Any other refusal of the cut query is one the whole query had too, and should have been
/// reported there: the position was taken too late.
///
/// The kind is checked and not only the position, because a parser that takes its
/// positions a token late takes this one late too, and a late position in a query that
/// has just been cut is the cut.
///
/// Held for the parser's refusals only. The lexer runs over the whole query before the
/// parser sees a token, so an unterminated string at the far end is reported ahead of a
/// misspelled verb at the start, and what stands before a lexer error is not promised to
/// be a query so far.
fn nothing_wrong_before(src: &str, e: &ParseError, now: DateTime<Utc>) {
    if matches!(
        e.kind,
        Kind::Unterminated | Kind::BadEscape { .. } | Kind::BadUnicode
    ) {
        return;
    }
    // At the end of the query the cut is the query, and that parse has been done.
    if e.pos == src.len() {
        return;
    }
    if let Err(earlier) = trieql::parse(&src[..e.pos], now) {
        let ran_out = matches!(
            &earlier.kind,
            Kind::Expected { found, .. } if found == "end of query"
        );
        assert!(
            ran_out && earlier.pos == e.pos,
            "{}: `{}` reported at byte {}, and the query cut there is refused for more than ending, at byte {}: {}",
            shown(src),
            e.kind,
            e.pos,
            earlier.pos,
            earlier.kind
        );
    }
}

/// A parse: the grammar's own refusals, checked on what came out. Each of these is a
/// thing `parse` returns an error for, so one of them in a `Parsed` is a path that got
/// past the check.
fn parsed_holds(src: &str, parsed: &Parsed) {
    for m in &parsed.matches {
        assert!(
            !m.key.is_empty(),
            "{}: parsed with an empty key",
            shown(src)
        );
    }

    match &parsed.mode {
        // The grammar has two verbs and neither is `Exists`.
        QueryMode::Exists => panic!("{}: parsed to a mode no verb names", shown(src)),
        QueryMode::Fetch | QueryMode::Count => {}
        QueryMode::GroupBy(keys) => {
            assert!(!keys.is_empty(), "{}: `GROUP BY` with no key", shown(src));
            let mut buckets = 0;
            for key in keys {
                match key {
                    GroupKey::Column(name) => assert!(
                        !name.is_empty(),
                        "{}: parsed with an empty group key",
                        shown(src)
                    ),
                    GroupKey::Time(width) => {
                        assert!(
                            !width.is_zero(),
                            "{}: parsed a zero-width bucket",
                            shown(src)
                        );
                        buckets += 1;
                    }
                }
            }
            assert!(
                buckets <= 1,
                "{}: parsed {buckets} time buckets",
                shown(src)
            );
        }
    }

    // `SINCE` sets one edge and `BETWEEN` both, in order; nothing sets only the upper.
    if let Some(until) = parsed.time.until {
        match parsed.time.since {
            Some(since) => assert!(since <= until, "{}: parsed a backwards window", shown(src)),
            None => panic!("{}: parsed a window with an end and no start", shown(src)),
        }
    }

    // The parser answers for the cursor's shape and for nothing else about it. Whether
    // one that decodes belongs to this query is the cursor target's to hold.
    if let Some(text) = &parsed.after {
        assert!(
            Cursor::decode(text).is_ok(),
            "{}: parsed with an `AFTER` that is not a cursor",
            shown(src)
        );
    }
}

/// The query as a panic message can carry it: the head of it, escaped, with its length.
/// A fixture is the whole input; this is only so the message says which one.
fn shown(src: &str) -> String {
    let head: String = src.chars().take(120).collect();
    format!("{head:?} ({} bytes)", src.len())
}
