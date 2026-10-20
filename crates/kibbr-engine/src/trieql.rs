// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! TrieQL: a query string in, the `Query` the engine already executes out.
//!
//! The TrieQL grammar and nothing else; adding an operator is a decision in its own right.
//! Nothing here may panic: every offset comes out of `char_indices` and no production
//! recurses.

use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};
use thiserror::Error;

use crate::engine::{Cursor, GroupKey, Match, MatchValue, QueryMode, TimeRange};
use crate::storage::store::system_time;

/// What a query said, before the output flags - which are not in the grammar - are folded
/// in.
#[derive(Debug)]
pub struct Parsed {
    pub matches: Vec<Match>,
    pub mode: QueryMode,
    pub time: TimeRange,
    /// `None` = no `LIMIT` clause, because `LIMIT 0` is writable and has to mean "no rows".
    pub limit: Option<usize>,
    /// The `AFTER` cursor, still text: `query::build` decodes it, where the `Query` it is
    /// checked against exists.
    pub after: Option<String>,
}

/// `now` is a parameter for the same reason it is one in `query::build` - `SINCE` is
/// relative to it, and a caller that wants a reproducible answer has to be able to fix it.
pub fn parse(src: &str, now: DateTime<Utc>) -> Result<Parsed, ParseError> {
    let mut p = Parser {
        toks: lex(src)?,
        at: 0,
        src,
    };
    p.query(now)
}

// ── Tokens ─────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Open,
    Close,
    /// `(` and `)`, which only `time(...)` uses.
    LParen,
    RParen,
    Colon,
    Comma,
    Star,
    /// A quoted string, unescaped.
    Str(String),
    /// A bare run: a keyword, a duration, a datetime, a group key.
    Word(String),
    End,
}

struct Token {
    tok: Tok,
    /// Byte offset of the token's first character.
    pos: usize,
}

/// Characters that end a bare word. `:` is absent on purpose: an RFC 3339 datetime is one
/// word and holds two of them.
const DELIMS: [char; 7] = ['{', '}', '(', ')', ',', '"', '*'];

fn lex(src: &str) -> Result<Vec<Token>, ParseError> {
    let mut out = Vec::new();
    let mut chars = src.char_indices().peekable();
    while let Some(&(pos, c)) = chars.peek() {
        let tok = match c {
            c if c.is_whitespace() => {
                chars.next();
                continue;
            }
            '{' => Tok::Open,
            '}' => Tok::Close,
            '(' => Tok::LParen,
            ')' => Tok::RParen,
            ',' => Tok::Comma,
            '*' => Tok::Star,
            ':' => Tok::Colon,
            '"' => {
                out.push(Token {
                    tok: Tok::Str(string(src, &mut chars, pos)?),
                    pos,
                });
                continue;
            }
            _ => {
                let mut word = String::new();
                while let Some(&(_, c)) = chars.peek() {
                    if c.is_whitespace() || DELIMS.contains(&c) {
                        break;
                    }
                    word.push(c);
                    chars.next();
                }
                out.push(Token {
                    tok: Tok::Word(word),
                    pos,
                });
                continue;
            }
        };
        chars.next();
        out.push(Token { tok, pos });
    }
    out.push(Token {
        tok: Tok::End,
        pos: src.len(),
    });
    Ok(out)
}

type Chars<'a> = std::iter::Peekable<std::str::CharIndices<'a>>;

/// A JSON string. The escapes are JSON's, because the pattern is written as JSON and an
/// operator who has escaped a quote in a log line will escape it the same way here.
fn string(src: &str, chars: &mut Chars<'_>, open: usize) -> Result<String, ParseError> {
    chars.next(); // the opening quote
    let mut s = String::new();
    loop {
        let Some((pos, c)) = chars.next() else {
            return Err(ParseError::new(src, open, Kind::Unterminated));
        };
        match c {
            '"' => return Ok(s),
            '\\' => {
                let Some((esc, c)) = chars.next() else {
                    return Err(ParseError::new(src, open, Kind::Unterminated));
                };
                s.push(match c {
                    '"' => '"',
                    '\\' => '\\',
                    '/' => '/',
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    'b' => '\u{8}',
                    'f' => '\u{c}',
                    'u' => unicode(src, chars, esc)?,
                    other => return Err(ParseError::new(src, pos, Kind::BadEscape { ch: other })),
                });
            }
            c => s.push(c),
        }
    }
}

/// `\uXXXX`. A lone surrogate is rejected rather than replaced: it is not a character, and
/// silently turning it into U+FFFD would match a key nobody wrote.
fn unicode(src: &str, chars: &mut Chars<'_>, at: usize) -> Result<char, ParseError> {
    let bad = || ParseError::new(src, at, Kind::BadUnicode);
    let mut hex = String::new();
    for _ in 0..4 {
        let (_, c) = chars.next().ok_or_else(bad)?;
        hex.push(c);
    }
    let n = u32::from_str_radix(&hex, 16).map_err(|_| bad())?;
    let high = match n {
        0xd800..=0xdbff => n,
        _ => return char::from_u32(n).ok_or_else(bad),
    };
    // A surrogate pair, which is how JSON spells anything above the BMP.
    for expect in ['\\', 'u'] {
        match chars.next() {
            Some((_, c)) if c == expect => {}
            _ => return Err(bad()),
        }
    }
    let mut low_hex = String::new();
    for _ in 0..4 {
        let (_, c) = chars.next().ok_or_else(bad)?;
        low_hex.push(c);
    }
    let low = u32::from_str_radix(&low_hex, 16).map_err(|_| bad())?;
    if !(0xdc00..=0xdfff).contains(&low) {
        return Err(bad());
    }
    char::from_u32(0x1_0000 + ((high - 0xd800) << 10) + (low - 0xdc00)).ok_or_else(bad)
}

// ── Recursive descent ──────────────────────────────────────────────────────────────

struct Parser<'a> {
    toks: Vec<Token>,
    at: usize,
    src: &'a str,
}

impl<'a> Parser<'a> {
    fn query(&mut self, now: DateTime<Utc>) -> Result<Parsed, ParseError> {
        // `word` hands back where the verb was, so a wrong verb is reported where it was
        // written rather than at whatever follows it.
        let (verb, at) = self.word("`MATCH` or `COUNT`")?;
        let mode = match &verb {
            v if v.eq_ignore_ascii_case("MATCH") => QueryMode::Fetch,
            v if v.eq_ignore_ascii_case("COUNT") => QueryMode::Count,
            _ => {
                return Err(ParseError::new(
                    self.src,
                    at,
                    Kind::Expected {
                        expected: "`MATCH` or `COUNT`".to_string(),
                        found: format!("`{}`", clip(&verb)),
                    },
                ))
            }
        };
        self.keyword("LOG")?;

        let matches = self.pattern()?;
        let time = self.time_clause(now)?;
        // Grouping decides the mode whichever verb asked for it, as `--group-by` does to
        // `--fetch`: a `Query` has one mode field, not two.
        let mode = match self.group_clause()? {
            Some(keys) => QueryMode::GroupBy(keys),
            None => mode,
        };
        let limit = self.limit_clause()?;
        let after = self.after_clause()?;

        if self.peek() != &Tok::End {
            return Err(self.expected("end of query"));
        }
        Ok(Parsed {
            matches,
            mode,
            time,
            limit,
            after,
        })
    }

    /// `"{" (pair ("," pair)*)? "}"`. `{}` is the empty pattern - every template, which is
    /// what `kibbr query` with no `--match` already means.
    fn pattern(&mut self) -> Result<Vec<Match>, ParseError> {
        self.eat(&Tok::Open, "`{`")?;
        let mut matches = Vec::new();
        if self.eat_if(&Tok::Close) {
            return Ok(matches);
        }
        loop {
            let (key, pos) = self.string("a quoted key")?;
            if key.is_empty() {
                return Err(ParseError::new(self.src, pos, Kind::EmptyKey));
            }
            self.eat(&Tok::Colon, "`:`")?;
            let value = if self.eat_if(&Tok::Star) {
                MatchValue::Any
            } else {
                // A quoted `"*"` is the literal value, not the wildcard: the grammar lists
                // them as two alternatives, so they are two things.
                MatchValue::Exact(self.string("a quoted value or `*`")?.0)
            };
            matches.push(Match { key, value });
            if !self.eat_if(&Tok::Comma) {
                break;
            }
        }
        self.eat(&Tok::Close, "`}` or `,`")?;
        Ok(matches)
    }

    fn time_clause(&mut self, now: DateTime<Utc>) -> Result<TimeRange, ParseError> {
        if self.eat_keyword("SINCE") {
            // `word`, not `bare`: the grammar spells `duration` out, so `SINCE "1h"` is
            // not one of its shapes however close it looks.
            let (word, pos) = self.word("a duration")?;
            return Ok(TimeRange {
                since: ago(system_time(now), duration(self.src, &word, pos)?),
                until: None,
            });
        }
        if self.eat_keyword("BETWEEN") {
            let from = self.datetime()?;
            self.keyword("AND")?;
            let to = self.datetime()?;
            if from.0 > to.0 {
                return Err(ParseError::new(
                    self.src,
                    to.1,
                    Kind::Backwards {
                        from: from.0.to_rfc3339(),
                        to: to.0.to_rfc3339(),
                    },
                ));
            }
            return Ok(TimeRange {
                since: Some(system_time(from.0)),
                until: Some(system_time(to.0)),
            });
        }
        Ok(TimeRange::default())
    }

    fn group_clause(&mut self) -> Result<Option<Vec<GroupKey>>, ParseError> {
        if !self.eat_keyword("GROUP") {
            return Ok(None);
        }
        self.keyword("BY")?;
        let mut keys: Vec<GroupKey> = Vec::new();
        loop {
            let at = self.pos();
            let key = self.group_key()?;
            // Two time axes is well-formed under the grammar and means nothing: the zero-fill has
            // one axis to fill.
            if matches!(key, GroupKey::Time(_))
                && keys.iter().any(|k| matches!(k, GroupKey::Time(_)))
            {
                return Err(ParseError::new(self.src, at, Kind::TwoTimeBuckets));
            }
            keys.push(key);
            if !self.eat_if(&Tok::Comma) {
                return Ok(Some(keys));
            }
        }
    }

    /// The bucket is a bare `time` followed by a parenthesis; a quoted `"time"`, or a bare
    /// one with no `(`, is the column of that name.
    fn group_key(&mut self) -> Result<GroupKey, ParseError> {
        let is_bucket = matches!(self.peek(), Tok::Word(w) if w.eq_ignore_ascii_case("time"))
            && self.peek_at(1) == &Tok::LParen;
        if is_bucket {
            self.bump();
            self.bump();
            let (word, pos) = self.word("a duration")?;
            let width = duration(self.src, &word, pos)?;
            self.eat(&Tok::RParen, "`)`")?;
            // A zero width has no boundaries to floor against and no bucket count to
            // check: every row would land in the same bucket, or in none.
            if width.is_zero() {
                return Err(ParseError::new(self.src, pos, Kind::ZeroBucket));
            }
            return Ok(GroupKey::Time(width));
        }
        let (key, pos) = self.bare("a group key or `time(<duration>)`")?;
        if key.is_empty() {
            return Err(ParseError::new(self.src, pos, Kind::EmptyKey));
        }
        Ok(GroupKey::Column(key))
    }

    /// `"LIMIT" number`. Last in the query, as it is last in the grammar: it applies to
    /// the whole answer and reads that way written at the end.
    fn limit_clause(&mut self) -> Result<Option<usize>, ParseError> {
        if !self.eat_keyword("LIMIT") {
            return Ok(None);
        }
        // `word`, not `bare`, for the reason `SINCE` takes one: the grammar spells the
        // operand as a number, so `LIMIT "10"` is not one of its shapes.
        let (text, pos) = self.word("a number")?;
        text.parse::<usize>()
            .map(Some)
            .map_err(|_| ParseError::new(self.src, pos, Kind::BadLimit))
    }

    /// `"AFTER" string`. Only shape-checked here: whether it is a cursor for *this* query is
    /// `Cursor::check`, which needs the built `Query`.
    fn after_clause(&mut self) -> Result<Option<String>, ParseError> {
        if !self.eat_keyword("AFTER") {
            return Ok(None);
        }
        // `string`, not `bare`: a cursor is opaque text and quoting it says so. It also
        // keeps the lexer out of a token that is only accidentally free of delimiters.
        let (text, pos) = self.string("a quoted cursor")?;
        match Cursor::decode(&text) {
            Ok(_) => Ok(Some(text)),
            Err(e) => Err(ParseError::new(
                self.src,
                pos,
                Kind::BadCursor { why: e.to_string() },
            )),
        }
    }

    fn datetime(&mut self) -> Result<(DateTime<Utc>, usize), ParseError> {
        let (text, pos) = self.bare("an RFC 3339 datetime")?;
        DateTime::parse_from_rfc3339(&text)
            .map(|dt| (dt.with_timezone(&Utc), pos))
            .map_err(|_| ParseError::new(self.src, pos, Kind::BadDatetime))
    }

    // ── Token plumbing ─────────────────────────────────────────────────────────────

    fn peek(&self) -> &Tok {
        // `lex` always ends the vector with `End`, so this cannot run off it.
        self.toks.get(self.at).map_or(&Tok::End, |t| &t.tok)
    }

    /// One token further on. `group_key` is the only production that needs to look past
    /// the token in hand: `time` is a column name until a `(` follows it.
    fn peek_at(&self, ahead: usize) -> &Tok {
        self.toks.get(self.at + ahead).map_or(&Tok::End, |t| &t.tok)
    }

    fn pos(&self) -> usize {
        self.toks
            .get(self.at)
            .map_or_else(|| self.src.len(), |t| t.pos)
    }

    fn bump(&mut self) {
        if self.at + 1 < self.toks.len() {
            self.at += 1;
        }
    }

    fn eat(&mut self, want: &Tok, expected: &'static str) -> Result<(), ParseError> {
        if self.peek() != want {
            return Err(self.expected(expected));
        }
        self.bump();
        Ok(())
    }

    fn eat_if(&mut self, want: &Tok) -> bool {
        if self.peek() == want {
            self.bump();
            return true;
        }
        false
    }

    /// A bare word, and only a bare word: the verb and the duration, both of which the
    /// grammar spells out as terminals.
    fn word(&mut self, expected: &'static str) -> Result<(String, usize), ParseError> {
        let pos = self.pos();
        match self.peek().clone() {
            Tok::Word(w) => {
                self.bump();
                Ok((w, pos))
            }
            _ => Err(self.expected(expected)),
        }
    }

    /// A group key or a datetime: bare, or the same text quoted.
    fn bare(&mut self, expected: &'static str) -> Result<(String, usize), ParseError> {
        let pos = self.pos();
        match self.peek().clone() {
            Tok::Word(w) | Tok::Str(w) => {
                self.bump();
                Ok((w, pos))
            }
            _ => Err(self.expected(expected)),
        }
    }

    fn string(&mut self, expected: &'static str) -> Result<(String, usize), ParseError> {
        let pos = self.pos();
        match self.peek().clone() {
            Tok::Str(s) => {
                self.bump();
                Ok((s, pos))
            }
            _ => Err(self.expected(expected)),
        }
    }

    fn eat_keyword(&mut self, kw: &str) -> bool {
        match self.peek() {
            Tok::Word(w) if w.eq_ignore_ascii_case(kw) => {
                self.bump();
                true
            }
            _ => false,
        }
    }

    fn keyword(&mut self, kw: &'static str) -> Result<(), ParseError> {
        if self.eat_keyword(kw) {
            return Ok(());
        }
        Err(ParseError::new(
            self.src,
            self.pos(),
            Kind::Expected {
                expected: format!("`{kw}`"),
                found: describe(self.peek()),
            },
        ))
    }

    fn expected(&self, expected: &'static str) -> ParseError {
        ParseError::new(
            self.src,
            self.pos(),
            Kind::Expected {
                expected: expected.to_string(),
                found: describe(self.peek()),
            },
        )
    }
}

/// The unit is case-sensitive where the keywords are not: it is the tail of a literal, and
/// `--since` reads it the same way (`scripts/check/parser_equivalence.sh`).
fn duration(src: &str, text: &str, pos: usize) -> Result<Duration, ParseError> {
    let bad = || ParseError::new(src, pos, Kind::BadDuration);
    // By character, not by `len() - 1`: `len` is bytes, and an index inside a multibyte
    // character would panic where a parse error is the whole contract.
    let (at, unit) = text.char_indices().next_back().ok_or_else(bad)?;
    let n: u64 = text[..at].parse().map_err(|_| bad())?;
    let secs = match unit {
        's' => 1,
        'm' => 60,
        'h' => 3_600,
        'd' => 86_400,
        _ => return Err(bad()),
    };
    Ok(Duration::from_secs(n.saturating_mul(secs)))
}

fn describe(tok: &Tok) -> String {
    match tok {
        Tok::Open => "`{`".to_string(),
        Tok::Close => "`}`".to_string(),
        Tok::LParen => "`(`".to_string(),
        Tok::RParen => "`)`".to_string(),
        Tok::Colon => "`:`".to_string(),
        Tok::Comma => "`,`".to_string(),
        Tok::Star => "`*`".to_string(),
        Tok::Str(s) => format!("the string `{}`", clip(s)),
        Tok::Word(w) => format!("`{}`", clip(w)),
        Tok::End => "end of query".to_string(),
    }
}

/// Bounded, because the token that surprised the parser may be a megabyte of it.
fn clip(s: &str) -> String {
    const MAX: usize = 24;
    let mut out = String::new();
    for (n, c) in s.chars().enumerate() {
        if n == MAX {
            out.push('…');
            break;
        }
        match visible(c) {
            Some(text) => out.push_str(&text),
            None => out.push(c),
        }
    }
    out
}

/// How a character that does not render is written instead, so a leading BOM is not quoted
/// back as a word that looks correct; `\n`, `\r` and `\t` stay one column for the caret.
fn visible(c: char) -> Option<String> {
    match c {
        '\n' | '\r' | '\t' => Some(" ".to_string()),
        c if c.is_control()
            || matches!(c, '\u{a0}' | '\u{200b}'..='\u{200f}' | '\u{2028}'..='\u{202e}' | '\u{2060}' | '\u{feff}') =>
        {
            Some(format!("\\u{{{:04x}}}", c as u32))
        }
        _ => None,
    }
}

// ── Errors ─────────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum Kind {
    #[error("expected {expected}, found {found}")]
    Expected { expected: String, found: String },
    #[error("unterminated string")]
    Unterminated,
    #[error("unknown escape `\\{ch}`")]
    BadEscape { ch: char },
    #[error("invalid `\\u` escape")]
    BadUnicode,
    #[error("expected a number followed by s, m, h or d")]
    BadDuration,
    #[error("expected an RFC 3339 datetime")]
    BadDatetime,
    #[error("expected a whole number of rows")]
    BadLimit,
    #[error("{why}")]
    BadCursor { why: String },
    #[error("`{from}` is after `{to}`")]
    Backwards { from: String, to: String },
    #[error("the key is empty")]
    EmptyKey,
    #[error("`time(0s)` has no width to bucket by")]
    ZeroBucket,
    #[error("`GROUP BY` names two time buckets; it can have one")]
    TwoTimeBuckets,
}

/// A parse failure and the byte offset in `0..=src.len()` it was at; the excerpt is cut at
/// construction because the query may be a megabyte.
#[derive(Debug)]
pub struct ParseError {
    pub pos: usize,
    pub kind: Kind,
    excerpt: String,
    /// Caret column within `excerpt`, in characters.
    caret: usize,
}

impl ParseError {
    fn new(src: &str, pos: usize, kind: Kind) -> Self {
        const WINDOW: usize = 36;
        // Clamped although every caller passes a character boundary: the slicing below is the
        // one place in this file that could panic.
        let mut pos = pos.min(src.len());
        while !src.is_char_boundary(pos) {
            pos -= 1;
        }

        // Both edges walk at most `WINDOW` characters, so neither splits one and an error in a
        // megabyte query costs the window.
        let start = src[..pos]
            .char_indices()
            .rev()
            .nth(WINDOW - 1)
            .map_or(0, |(i, _)| i);
        let end = src[pos..]
            .char_indices()
            .nth(WINDOW)
            .map_or(src.len(), |(i, _)| pos + i);

        // Built character by character because the caret has to land under the character
        // it accuses, and an escaped one is wider on the page than it is in the query.
        let mut excerpt = String::new();
        let mut caret = 0;
        if start > 0 {
            excerpt.push('…');
            caret = 1;
        }
        for (i, c) in src[start..end].char_indices() {
            let width = match visible(c) {
                Some(text) => {
                    excerpt.push_str(&text);
                    text.chars().count()
                }
                None => {
                    excerpt.push(c);
                    1
                }
            };
            if start + i < pos {
                caret += width;
            }
        }
        if end < src.len() {
            excerpt.push('…');
        }
        Self {
            pos,
            kind,
            excerpt,
            caret,
        }
    }
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} at byte {}\n    {}\n    {}^",
            self.kind,
            self.pos,
            self.excerpt,
            " ".repeat(self.caret)
        )
    }
}

impl std::error::Error for ParseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────────

/// `now - d`, or `None` (an open window) when the subtraction leaves the representable
/// range, because `SystemTime - Duration` panics on overflow.
pub fn ago(now: SystemTime, d: Duration) -> Option<SystemTime> {
    now.checked_sub(d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Query;
    use crate::core::rng::Rng;

    fn now() -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).expect("a representable instant")
    }

    /// Not "returns an error" - `parse` returning at all is the whole assertion.
    fn survives(src: &str) {
        let _ = parse(src, now());
    }

    #[test]
    fn nul_bytes_are_just_characters() {
        survives("\0");
        survives("COUNT\0LOG {}");
        survives("COUNT LOG {\"a\\u0000b\": \"c\"}");
        assert!(parse("COUNT LOG {\"a\0b\": *}", now()).is_ok());
    }

    #[test]
    fn megabyte_inputs_return() {
        survives(&"{".repeat(1_000_000));
        survives(&format!(
            r#"COUNT LOG {{"a": "{}"}}"#,
            "x".repeat(1_000_000)
        ));
        survives(&format!(
            "COUNT LOG {{{}}}",
            r#""a": "b", "#.repeat(100_000)
        ));
        survives(&format!("COUNT LOG {{}} SINCE {}h", "9".repeat(1_000_000)));
        survives(&"é".repeat(500_000));
        survives(&r#"\u"#.repeat(500_000));
    }

    /// A deterministic sweep over the characters the grammar cares about: the failure this is
    /// looking for is a panic, so every input is its own assertion.
    #[test]
    fn a_hundred_thousand_random_queries_return() {
        const ALPHABET: [&str; 26] = [
            "COUNT",
            "MATCH",
            "LOG",
            "SINCE",
            "BETWEEN",
            "AND",
            "GROUP",
            "BY",
            "{",
            "}",
            ":",
            ",",
            "*",
            "\"",
            "\\",
            "1h",
            "é",
            "🙂",
            "2024-01-01T00:00:00Z",
            " ",
            "\u{0}",
            "\\u",
            // `time`, its parentheses, and a width of zero - the group key that is parsed
            // rather than read, and the one operand the grammar rejects outright.
            "time",
            "(",
            ")",
            "0s",
        ];
        let mut rng = Rng::new(9);
        for _ in 0..100_000 {
            let mut src = String::new();
            for _ in 0..rng.below(12) {
                src.push_str(ALPHABET[rng.below(ALPHABET.len() as u64) as usize]);
            }
            survives(&src);
        }
    }

    /// The two the grammar prints, parsed rather than merely accepted.
    #[test]
    fn grammar_examples() {
        let a = parse(r#"COUNT LOG {"event": "login_failed"} SINCE 1h"#, now())
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(a.mode, QueryMode::Count);
        assert_eq!(a.matches.len(), 1);
        assert_eq!(a.time.until, None);
        assert!(a.time.since.is_some());

        let b = parse(
            r#"MATCH LOG {"event": "processed"} SINCE 24h GROUP BY level"#,
            now(),
        )
        .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            b.mode,
            QueryMode::GroupBy(vec![GroupKey::Column("level".to_string())])
        );
    }

    /// The flag surface cannot tell these apart: `--match k=*` is always the wildcard, so
    /// `Exact("*")` is a value only TrieQL can ask for.
    #[test]
    fn a_quoted_star_is_a_value_not_the_wildcard() {
        let any = parse(r#"COUNT LOG {"a": *}"#, now()).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(any.matches[0].value, MatchValue::Any);

        let literal = parse(r#"COUNT LOG {"a": "*"}"#, now()).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(literal.matches[0].value, MatchValue::Exact("*".to_string()));
    }

    /// JSON's escapes, because the pattern is written as JSON. A key holding a quote is
    /// otherwise unaskable.
    #[test]
    fn strings_take_json_escapes() {
        let q = parse(
            r#"COUNT LOG {"a\"b": "c\\d", "tab": "x\ty", "astral": "\ud83d\ude42"}"#,
            now(),
        )
        .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(q.matches[0].key, "a\"b");
        assert_eq!(q.matches[0].value, MatchValue::Exact("c\\d".to_string()));
        assert_eq!(q.matches[1].value, MatchValue::Exact("x\ty".to_string()));
        assert_eq!(q.matches[2].value, MatchValue::Exact("🙂".to_string()));
    }

    /// Byte offsets, checked where they can be read as numbers rather than out of a
    /// rendered message.
    #[test]
    fn positions_are_byte_offsets_within_the_input() {
        for src in [
            "",
            "COUNT LOG",
            r#"COUNT LOG {"é": }"#,
            r#"COUNT LOG {"🙂": "x"} SINCE 🙂"#,
            r#"COUNT LOG {"a": "b"} LIMIT ten"#,
            r#"COUNT LOG {"a": "b"} LIMIT -1"#,
        ] {
            let e = parse(src, now()).expect_err("expected a parse error");
            assert!(e.pos <= src.len(), "{src:?} → byte {}", e.pos);
            assert!(src.is_char_boundary(e.pos), "{src:?} → byte {}", e.pos);
        }
        // The exact one, so "inside the input" cannot pass by always answering 0. The
        // `é` is two bytes, so a parser counting characters would say 21.
        let src = r#"COUNT LOG {"é": "b"} ORDER BY ts"#;
        let e = parse(src, now()).unwrap_err();
        assert_eq!(e.pos, 22);
        assert_eq!(&src[e.pos..], "ORDER BY ts");
    }

    /// `limit_clause`. `None` and `Some(0)` are different answers.
    #[test]
    fn limit_is_parsed_and_absence_is_not_zero() {
        let q = |src: &str| parse(src, now()).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(q(r#"MATCH LOG {} LIMIT 10"#).limit, Some(10));
        assert_eq!(q(r#"MATCH LOG {}"#).limit, None);
        assert_eq!(q(r#"MATCH LOG {} LIMIT 0"#).limit, Some(0));
        assert_eq!(
            q(r#"MATCH LOG {"a": *} SINCE 1h GROUP BY b LIMIT 3"#).limit,
            Some(3)
        );
        // Keywords ignore case, as every other one does.
        assert_eq!(q(r#"match log {} limit 7"#).limit, Some(7));

        for bad in [
            r#"MATCH LOG {} LIMIT"#,
            r#"MATCH LOG {} LIMIT -1"#,
            r#"MATCH LOG {} LIMIT 1.5"#,
            r#"MATCH LOG {} LIMIT "10""#,
            r#"MATCH LOG {} LIMIT 99999999999999999999999999"#,
        ] {
            parse(bad, now()).expect_err(bad);
        }
    }

    /// `after_clause`, and the shape check the parser is answerable for. Whether a
    /// well-formed cursor belongs to *this* query is `query::build`'s, not the parser's.
    #[test]
    fn after_takes_a_cursor_and_nothing_else() {
        let mint = |ts: Option<i64>| {
            let q = Query::new(
                Vec::new(),
                QueryMode::Fetch,
                TimeRange::default(),
                None,
                None,
                std::time::SystemTime::UNIX_EPOCH,
                crate::limits::Limits::default(),
            );
            q.cursor_at((std::cmp::Reverse(ts), 3, 17)).encode()
        };
        for ts in [Some(1_700_000_000_000_000_000), None, Some(i64::MIN)] {
            let text = mint(ts);
            let q = parse(&format!(r#"MATCH LOG {{}} LIMIT 5 AFTER "{text}""#), now())
                .unwrap_or_else(|e| panic!("{e}"));
            assert_eq!(q.after.as_deref(), Some(text.as_str()));
            // Round-trips, which is what makes handing `next_cursor` back safe.
            assert_eq!(Cursor::decode(&text).expect("a cursor").encode(), text);
        }
        assert_eq!(parse("MATCH LOG {}", now()).unwrap().after, None);

        let empty = r#"MATCH LOG {} AFTER """#;
        for bad in [
            r#"MATCH LOG {} AFTER"#,
            empty,
            r#"MATCH LOG {} AFTER "v1""#,
            r#"MATCH LOG {} AFTER "v1.zz""#,
            r#"MATCH LOG {} AFTER "v9.0000""#,
            // Right length, wrong alphabet - the hex check is not a length check.
            &format!(r#"MATCH LOG {{}} AFTER "v1.{}""#, "g".repeat(58)),
        ] {
            parse(bad, now()).expect_err(bad);
        }
    }

    /// `group_key`'s two alternatives. `time` is a column name until a `(` follows it,
    /// which is the same rule `pair` applies to `*` and `"*"`.
    #[test]
    fn a_time_bucket_is_a_parenthesis_away_from_a_column_named_time() {
        let q = |src: &str| parse(src, now()).unwrap_or_else(|e| panic!("{src}: {e}"));
        let keys = |src: &str| match q(src).mode {
            QueryMode::GroupBy(keys) => keys,
            other => panic!("{src}: expected a group by, got {other:?}"),
        };

        assert_eq!(
            keys("COUNT LOG {} SINCE 6h GROUP BY time(5m)"),
            vec![GroupKey::Time(Duration::from_secs(300))]
        );
        // Every unit the grammar spells, and the keyword's case ignored as every other
        // keyword's is. The width's unit is not a keyword and keeps its own case.
        assert_eq!(
            keys("count log {} since 6h group by TIME(2d)"),
            vec![GroupKey::Time(Duration::from_secs(172_800))]
        );
        // A bare `time` with nothing after it and a quoted one before a parenthesis both name
        // the column: the second is the escape for a column that really is called `time`.
        assert_eq!(
            keys("COUNT LOG {} GROUP BY time"),
            vec![GroupKey::Column("time".to_string())]
        );
        assert_eq!(
            keys(r#"COUNT LOG {} GROUP BY "time""#),
            vec![GroupKey::Column("time".to_string())]
        );
        // Mixed, in the order written - which is the order every surface prints.
        assert_eq!(
            keys("COUNT LOG {} SINCE 6h GROUP BY time(5m), level"),
            vec![
                GroupKey::Time(Duration::from_secs(300)),
                GroupKey::Column("level".to_string()),
            ]
        );
        assert_eq!(
            keys("COUNT LOG {} SINCE 6h GROUP BY level, time(1h)"),
            vec![
                GroupKey::Column("level".to_string()),
                GroupKey::Time(Duration::from_secs(3_600)),
            ]
        );

        for bad in [
            "COUNT LOG {} GROUP BY time(",
            "COUNT LOG {} GROUP BY time()",
            "COUNT LOG {} GROUP BY time(5m",
            "COUNT LOG {} GROUP BY time(5)",
            "COUNT LOG {} GROUP BY time(\"5m\")",
            "COUNT LOG {} GROUP BY time(-5m)",
            // A zero width has no boundaries to floor against.
            "COUNT LOG {} GROUP BY time(0s)",
            // One time axis. Two is well-formed above and means nothing below.
            "COUNT LOG {} GROUP BY time(5m), time(1h)",
            "COUNT LOG {} GROUP BY time(5m), level, time(5m)",
        ] {
            parse(bad, now()).expect_err(bad);
        }
    }

    /// `{}` is legal syntax and means every template: the pair list is optional.
    #[test]
    fn the_empty_pattern_parses_to_no_terms() {
        for src in [
            "MATCH LOG {}",
            "COUNT LOG {}",
            "MATCH LOG {} SINCE 1h LIMIT 10",
            "MATCH LOG {   }",
        ] {
            let q = parse(src, now()).unwrap_or_else(|e| panic!("{src}: {e}"));
            assert!(q.matches.is_empty(), "{src}");
        }
    }
}
