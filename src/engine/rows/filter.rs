// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! A term, and the time window, put to a decoded column instead of to a row.
//!
//! Building a row for every stored value is wasted on rows thrown away; a typed compare
//! answers with one bit a row (`Sel`). Every compare in `hits` is held to `hits_by_row`
//! and `engine::value_matches`; only `hits_by_row` refuses a value that does not print.

use std::borrow::Cow;

use crate::engine::format::column::{Col, Memo, PieceRef, ValuesRef};
use crate::engine::format::{
    epoch_of, ip_bytes, print_ip, print_uuid, uuid_bytes, Bad, Form, MAX_ROWS, RFC3339,
};
use super::{in_window, instant_of, said_instant, Shown, Term, Want};
use crate::engine::model::infer::time_nanos_of_int;
use crate::engine::model::types::ColValue;

/// A selection's bits, in words of sixty-four.
const WORDS: usize = 128;

// One bit for every row a row group can hold, and no more.
const _: () = assert!(WORDS * 64 == MAX_ROWS as usize);

/// Which rows of one row group are still in, one bit a row: a bitmap because terms are
/// ANDed, counted and walked in row order, and bits at and past `rows` are always clear.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Sel {
    bits: [u64; WORDS],
    rows: usize,
}

/// The rows that are in, as a list: what a failed comparison of two selections prints.
impl std::fmt::Debug for Sel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()?;
        write!(f, " of {} rows", self.rows)
    }
}

impl Sel {
    /// Every row in.
    pub(crate) fn all(rows: usize) -> Self {
        let mut sel = Self::none(rows);
        let whole = sel.rows / 64;
        for word in &mut sel.bits[..whole] {
            *word = u64::MAX;
        }
        if !sel.rows.is_multiple_of(64) {
            sel.bits[whole] = (1 << (sel.rows % 64)) - 1;
        }
        sel
    }

    /// No row in.
    pub(crate) fn none(rows: usize) -> Self {
        Self {
            bits: [0; WORDS],
            rows: rows.min(WORDS * 64),
        }
    }

    /// The rows whose value `hit` says yes to. Sixty-four values make one word with no branch
    /// on the answer, so a compare over a vector runs at the speed of the vector.
    fn of<T>(values: &[T], hit: impl Fn(&T) -> bool) -> Self {
        let mut sel = Self::none(values.len());
        for (word, values) in sel.bits.iter_mut().zip(values.chunks(64)) {
            let mut bits = 0u64;
            for (at, value) in values.iter().enumerate() {
                bits |= u64::from(hit(value)) << at;
            }
            *word = bits;
        }
        sel
    }

    /// `of`, for answers that come one at a time and not out of a slice.
    fn of_each(rows: usize, hits: impl Iterator<Item = bool>) -> Self {
        let mut sel = Self::none(rows);
        for (row, hit) in hits.take(sel.rows).enumerate() {
            sel.bits[row / 64] |= u64::from(hit) << (row % 64);
        }
        sel
    }

    /// How many rows the selection is over, in or out.
    pub(crate) fn rows(&self) -> usize {
        self.rows
    }

    /// Put a row in; a row the selection is not over stays out.
    pub(crate) fn set(&mut self, row: usize) {
        if row < self.rows {
            self.bits[row / 64] |= 1 << (row % 64);
        }
    }

    pub(crate) fn get(&self, row: usize) -> bool {
        row < self.rows && self.bits[row / 64] & (1 << (row % 64)) != 0
    }

    /// Keep only the rows `other` has in too.
    pub(crate) fn and(&mut self, other: &Sel) {
        for (mine, theirs) in self.bits.iter_mut().zip(&other.bits) {
            *mine &= theirs;
        }
    }

    /// How many rows are in.
    pub(crate) fn count(&self) -> u64 {
        self.bits
            .iter()
            .map(|word| u64::from(word.count_ones()))
            .sum()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.bits.iter().all(|word| *word == 0)
    }

    /// The row of the `n`-th row that is in, counting from 0 in row order.
    pub(crate) fn nth(&self, n: u64) -> Option<usize> {
        let mut left = n;
        for (at, word) in self.bits.iter().enumerate() {
            let here = u64::from(word.count_ones());
            if left < here {
                // Drop the lowest set bit once for each match before the one wanted.
                let mut word = *word;
                for _ in 0..left {
                    word &= word - 1;
                }
                return Some(at * 64 + word.trailing_zeros() as usize);
            }
            left -= here;
        }
        None
    }

    /// The rows that are in, in row order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        self.bits.iter().enumerate().flat_map(|(at, word)| {
            let mut left = *word;
            std::iter::from_fn(move || {
                if left == 0 {
                    return None;
                }
                let bit = left.trailing_zeros() as usize;
                left &= left - 1;
                Some(at * 64 + bit)
            })
        })
    }
}

// ── A term over a column ───────────────────────────────────────────────────────────

/// The rows of `column` that `term` matches, by typed compare; a stored `Datetime` is
/// compared as `shown` says the column shows it, and a null matches no term, `*` included.
pub(crate) fn hits(column: Col<'_>, term: &Term, shown: impl Into<Shown>) -> Result<Sel, Bad> {
    let shown = shown.into();
    let rows = column.rows();
    let [a, b] = column.pieces()?;
    let Some(a) = a else {
        return Ok(Sel::none(rows));
    };
    let slots = column.slots();
    let Want::Exact { text, inferred, .. } = term.want() else {
        return Ok(match slots {
            None => Sel::all(rows),
            Some(slots) => Sel::of(slots, |slot| slot.get().is_some()),
        });
    };
    let asked = Asked {
        term,
        text,
        inferred,
        shown,
    };
    let in_a = asked.of(&a)?;
    // With no slots piece A holds every row in row order, and its answer is the column's.
    let Some(slots) = slots else {
        return Ok(in_a);
    };
    let in_b = match &b {
        Some(b) => asked.of(b)?,
        None => Sel::none(0),
    };
    Ok(Sel::of(slots, |slot| match slot.get() {
        None => false,
        Some((false, at)) => in_a.get(at),
        Some((true, at)) => in_b.get(at),
    }))
}

/// The same rows by definition, every row's cell printed as `read::chunk` prints it and
/// the term asked of it: what `hits` is held to.
pub(crate) fn hits_by_row(
    column: Col<'_>,
    term: &Term,
    shown: impl Into<Shown>,
) -> Result<Sel, Bad> {
    let shown = shown.into();
    let mut sel = Sel::none(column.rows());
    let mut memo = Memo::default();
    for row in 0..column.rows() {
        let (value, written) = column.cell_with(row, &mut memo)?;
        if term.matches_cell(&value, written.as_ref(), shown) {
            sel.set(row);
        }
    }
    Ok(sel)
}

/// One term for one value, as it is put to the pieces of one column.
struct Asked<'t> {
    term: &'t Term,
    /// The term's text, and what the ladder reads that text as.
    text: &'t str,
    inferred: &'t ColValue,
    shown: Shown,
}

impl Asked<'_> {
    /// The values of `piece` the term matches, bit `i` for value `i`. The compare is chosen
    /// once, from the piece's form, which fixes what a rendered row would hold.
    fn of(&self, piece: &PieceRef<'_>) -> Result<Sel, Bad> {
        let n = piece.n;
        let every = |hit: bool| if hit { Sel::all(n) } else { Sel::none(n) };
        Ok(match piece.form {
            // A row holds `Int64(x)` whatever digits spelled it, which no string equals:
            // the ladder's reading or nothing. `"0045"`, `"+45"` and `45` are one `x`.
            Form::Int64 { .. } => match (&piece.values, self.inferred) {
                (ValuesRef::Ints(values), ColValue::Int64(want)) => Sel::of(values, |x| x == want),
                (ValuesRef::SameInt(x), ColValue::Int64(want)) => every(x == want),
                (ValuesRef::Ints(_) | ValuesRef::SameInt(_), _) => Sel::none(n),
                _ => self.printed(piece)?,
            },
            // As `ColValue` compares floats: `-0.0` is `0.0`. A term of `200` reads as an
            // integer and does not match `200.0`, as it never has.
            Form::Float64 { .. } => match (&piece.values, self.inferred) {
                (ValuesRef::Floats(values), ColValue::Float64(want)) => {
                    Sel::of(values, |x| x == want)
                }
                (ValuesRef::SameFloat(x), ColValue::Float64(want)) => every(x == want),
                (ValuesRef::Floats(_) | ValuesRef::SameFloat(_), _) => Sel::none(n),
                _ => self.printed(piece)?,
            },
            Form::Bool { .. } => match (&piece.values, self.inferred) {
                (ValuesRef::Bools(values), ColValue::Bool(want)) => Sel::of(values, |x| x == want),
                (ValuesRef::SameBool(x), ColValue::Bool(want)) => every(x == want),
                (ValuesRef::Bools(_) | ValuesRef::SameBool(_), _) => Sel::none(n),
                _ => self.printed(piece)?,
            },
            // In a column that is still `Datetime` a row holds the instant, and every
            // spelling of it the ladder reads - its epoch digits too - finds it. So it does
            // for an RFC 3339 date in a column that is now `Int64`: it is no integer, and
            // stays the date it was.
            Form::Datetime { unit, .. }
                if self.shown == Shown::Stored
                    || (self.shown == Shown::Count && unit == RFC3339) =>
            {
                match (&piece.values, self.inferred) {
                    (ValuesRef::Times(values), ColValue::Datetime(want)) => {
                        Sel::of(values, |ns| ns == want)
                    }
                    (ValuesRef::SameTime(ns), ColValue::Datetime(want)) => every(ns == want),
                    (ValuesRef::Times(_) | ValuesRef::SameTime(_), _) => Sel::none(n),
                    _ => self.printed(piece)?,
                }
            }
            // In a column that is now text a row holds the text the date arrived as, and only that
            // text matches; an RFC 3339 date is printed, and asked.
            Form::Datetime { unit: RFC3339, .. } => self.printed(piece)?,
            // An epoch is the whole seconds or milliseconds in decimal, the one spelling of a
            // number, so the texts are equal exactly when the term's text is that spelling too.
            // In a column that is now `Int64` the row holds that number, and the term matches
            // as the integer its text is, however spelled.
            Form::Datetime { unit, .. } => {
                let whole = match self.shown {
                    Shown::Count => self.term.whole(),
                    _ => self
                        .text
                        .parse::<i64>()
                        .ok()
                        .filter(|units| units.to_string() == self.text),
                };
                match (&piece.values, whole) {
                    (ValuesRef::Times(values), Some(want)) => {
                        Sel::of(values, |ns| epoch_of(*ns, unit) == Some(want))
                    }
                    (ValuesRef::SameTime(ns), Some(want)) => {
                        every(epoch_of(*ns, unit) == Some(want))
                    }
                    (ValuesRef::Times(_) | ValuesRef::SameTime(_), None) => Sel::none(n),
                    _ => self.printed(piece)?,
                }
            }
            // A row holds the id printed in the piece's case. Printing is one-to-one, so the term
            // matches only as the sixteen bytes its text reads as, if they print back as it here.
            Form::Uuid { case } => {
                let want =
                    uuid_bytes(self.text).filter(|bytes| print_uuid(bytes, case == 1) == self.text);
                match (&piece.values, want) {
                    (ValuesRef::Fixed { bytes, width: 16 }, Some(want)) => {
                        Sel::of_each(n, bytes.chunks_exact(16).map(|id| id == want))
                    }
                    (ValuesRef::SameFixed(id), Some(want)) => every(*id == want),
                    (ValuesRef::Fixed { width: 16, .. } | ValuesRef::SameFixed(_), None) => {
                        Sel::none(n)
                    }
                    _ => self.printed(piece)?,
                }
            }
            // An address, by the same argument, in the piece's spelling and this family's width.
            Form::Ip { spelling } => {
                let want = ip_bytes(self.text)
                    .filter(|bytes| print_ip(bytes, spelling).as_deref() == Some(self.text));
                match (&piece.values, want) {
                    (ValuesRef::Fixed { bytes, width }, Some(want)) if want.len() == *width => {
                        Sel::of_each(n, bytes.chunks_exact(*width).map(|ip| ip == want))
                    }
                    (ValuesRef::SameFixed(ip), Some(want)) => every(*ip == want),
                    (ValuesRef::Fixed { .. } | ValuesRef::SameFixed(_), _) => Sel::none(n),
                    _ => self.printed(piece)?,
                }
            }
            Form::String { .. } => self.of_text(piece)?,
        })
    }

    /// A String piece: words the ladder typed `String`, and now and then a value of
    /// another type that no form printed back, kept as the text it arrived as.
    fn of_text(&self, piece: &PieceRef<'_>) -> Result<Sel, Bad> {
        let n = piece.n;
        // The term reads as text: only a row holding a string can match, and that string is its
        // word, so the compare is byte for byte and no ladder runs.
        if matches!(self.inferred, ColValue::String(_)) {
            let want = self.text.as_bytes();
            return Ok(match &piece.values {
                ValuesRef::Words { words, codes } => {
                    let is: Vec<bool> = words.iter().map(|word| *word == want).collect();
                    Sel::of(codes, |code| is.get(usize::from(*code)) == Some(&true))
                }
                ValuesRef::Texts(texts) => Sel::of(texts, |word| *word == want),
                ValuesRef::SameText(word) if *word == want => Sel::all(n),
                ValuesRef::SameText(_) => Sel::none(n),
                _ => self.printed(piece)?,
            });
        }
        // The term reads as a number, a date or a bool: a word matches when the ladder reads it
        // as the same value, asked once for each word and each of the two kinds of row.
        let mut sel = Sel::none(n);
        match &piece.values {
            ValuesRef::Words { words, codes } => {
                let mut known = vec![[None; 2]; words.len()];
                for (at, code) in codes.iter().enumerate() {
                    let lit = piece.lit(at);
                    let code = usize::from(*code);
                    if let (Some(word), Some(known)) = (words.get(code), known.get_mut(code)) {
                        if self.knows(piece, word, lit, &mut known[usize::from(lit)])? {
                            sel.set(at);
                        }
                    }
                }
            }
            ValuesRef::Texts(texts) => {
                for (at, word) in texts.iter().enumerate() {
                    if self.says(piece, word, piece.lit(at))? {
                        sel.set(at);
                    }
                }
            }
            ValuesRef::SameText(word) => {
                let mut known = [None; 2];
                for at in 0..n {
                    let lit = piece.lit(at);
                    if self.knows(piece, word, lit, &mut known[usize::from(lit)])? {
                        sel.set(at);
                    }
                }
            }
            _ => return self.printed(piece),
        }
        Ok(sel)
    }

    /// Does a row that names `word` match? Asked of the cell such a row prints.
    fn says(&self, piece: &PieceRef<'_>, word: &[u8], lit: bool) -> Result<bool, Bad> {
        let (value, written) = piece.text_cell(word, lit)?;
        Ok(self.term.matches_cell(&value, written.as_ref(), self.shown))
    }

    /// `says`, asked once and remembered in `known`.
    fn knows(
        &self,
        piece: &PieceRef<'_>,
        word: &[u8],
        lit: bool,
        known: &mut Option<bool>,
    ) -> Result<bool, Bad> {
        match *known {
            Some(hit) => Ok(hit),
            None => {
                let hit = self.says(piece, word, lit)?;
                *known = Some(hit);
                Ok(hit)
            }
        }
    }

    /// Every value of the piece printed and asked: the answer by construction, where no
    /// typed compare is argued.
    fn printed(&self, piece: &PieceRef<'_>) -> Result<Sel, Bad> {
        let mut sel = Sel::none(piece.n);
        for at in 0..piece.n {
            let (value, written) = piece.cell(at)?;
            if self.term.matches_cell(&value, written.as_ref(), self.shown) {
                sel.set(at);
            }
        }
        Ok(sel)
    }
}

// ── Time over a column ─────────────────────────────────────────────────────────────

/// Each row's instant, as `instant_of` reads the row's cell, borrowed from the column
/// when it is one `Datetime` piece holding every row.
pub(crate) struct Instants<'c> {
    /// A row's instant where it has one. Shorter than the rows only when no row has one.
    ns: Cow<'c, [i64]>,
    /// The rows that have an instant. `None`: every row has.
    has: Option<Sel>,
}

/// What `instant_of` gives each value of one piece.
enum Stamps<'c> {
    Each(&'c [i64]),
    Same(Option<i64>),
    Made(Vec<Option<i64>>),
}

impl Stamps<'_> {
    fn get(&self, at: usize) -> Option<i64> {
        match self {
            Stamps::Each(ns) => ns.get(at).copied(),
            Stamps::Same(ns) => *ns,
            Stamps::Made(ns) => ns.get(at).copied().flatten(),
        }
    }
}

/// A piece's instants. A `Datetime` is one whatever the column's type has since become:
/// the window reads the stored value; text is one when the ladder reads it as one.
fn stamps<'c>(piece: &PieceRef<'c>) -> Result<Stamps<'c>, Bad> {
    let of = |word: &[u8], lit: bool| -> Result<Option<i64>, Bad> {
        Ok(instant_of(&piece.text_cell(word, lit)?.0))
    };
    Ok(match (&piece.form, &piece.values) {
        (Form::Datetime { .. }, ValuesRef::Times(ns)) => Stamps::Each(ns),
        (Form::Datetime { .. }, ValuesRef::SameTime(ns)) => Stamps::Same(Some(*ns)),
        (Form::String { .. }, ValuesRef::Words { words, codes }) => {
            let mut known: Vec<[Option<Option<i64>>; 2]> = vec![[None; 2]; words.len()];
            let mut made = Vec::with_capacity(codes.len());
            for (at, code) in codes.iter().enumerate() {
                let lit = piece.lit(at);
                let code = usize::from(*code);
                let ns = match (words.get(code), known.get_mut(code)) {
                    (Some(word), Some(known)) => match known[usize::from(lit)] {
                        Some(ns) => ns,
                        None => {
                            let ns = of(word, lit)?;
                            known[usize::from(lit)] = Some(ns);
                            ns
                        }
                    },
                    _ => None,
                };
                made.push(ns);
            }
            Stamps::Made(made)
        }
        (Form::String { .. }, ValuesRef::Texts(texts)) => Stamps::Made(
            texts
                .iter()
                .enumerate()
                .map(|(at, word)| of(word, piece.lit(at)))
                .collect::<Result<_, _>>()?,
        ),
        (Form::String { .. }, ValuesRef::SameText(word)) => Stamps::Made(
            (0..piece.n)
                .map(|at| of(word, piece.lit(at)))
                .collect::<Result<_, _>>()?,
        ),
        (
            Form::Int64 { .. }
            | Form::Float64 { .. }
            | Form::Bool { .. }
            | Form::Uuid { .. }
            | Form::Ip { .. },
            _,
        ) => Stamps::Same(None),
        // A pairing no decoder makes: each value printed, and its cell asked.
        _ => Stamps::Made(
            (0..piece.n)
                .map(|at| Ok(instant_of(&piece.cell(at)?.0)))
                .collect::<Result<_, Bad>>()?,
        ),
    })
}

/// A piece's instants as only a template's time column reads them (`said_instant`): what
/// the line said through `time_nanos`, so a decimal of seconds, sixteen digits and a zoned
/// date are instants here. An integer printed in plain decimal is read as its digits with
/// no text made; any other value is printed, and asked.
fn said_stamps<'c>(piece: &PieceRef<'c>) -> Result<Stamps<'c>, Bad> {
    let plain = matches!(
        piece.form,
        Form::Int64 {
            plus: false,
            width: 0,
            ..
        }
    );
    Ok(match (&piece.form, &piece.values) {
        (Form::Datetime { .. }, ValuesRef::Times(ns)) => Stamps::Each(ns),
        (Form::Datetime { .. }, ValuesRef::SameTime(ns)) => Stamps::Same(Some(*ns)),
        (_, ValuesRef::Ints(values)) if plain => {
            Stamps::Made(values.iter().map(|i| time_nanos_of_int(*i)).collect())
        }
        (_, ValuesRef::SameInt(i)) if plain => Stamps::Same(time_nanos_of_int(*i)),
        _ => Stamps::Made(
            (0..piece.n)
                .map(|at| {
                    let (value, written) = piece.cell(at)?;
                    Ok(said_instant(&value, written.as_ref()))
                })
                .collect::<Result<_, Bad>>()?,
        ),
    })
}

/// Each row's instant off a decoded column.
pub(crate) fn instants(column: Col<'_>) -> Result<Instants<'_>, Bad> {
    instants_by(column, stamps)
}

/// Each row's instant off a decoded time column, read as only a time column reads it: the
/// last thing a row's effective time falls back to (`effective_ts`).
pub(crate) fn said_instants(column: Col<'_>) -> Result<Instants<'_>, Bad> {
    instants_by(column, said_stamps)
}

fn instants_by<'c>(
    column: Col<'c>,
    stamps: impl Fn(&PieceRef<'c>) -> Result<Stamps<'c>, Bad>,
) -> Result<Instants<'c>, Bad> {
    let rows = column.rows();
    let [a, b] = column.pieces()?;
    let Some(a) = a else {
        return Ok(Instants::absent(rows));
    };
    let slots = column.slots();
    let in_a = stamps(&a)?;
    let Some(slots) = slots else {
        // Piece A is every row in row order: a time column as it nearly always is.
        if let Stamps::Each(ns) = in_a {
            return Ok(Instants {
                ns: Cow::Borrowed(ns),
                has: None,
            });
        }
        return Ok(Instants::made(rows, |row| in_a.get(row)));
    };
    let in_b = match &b {
        Some(b) => stamps(b)?,
        None => Stamps::Same(None),
    };
    Ok(Instants::made(rows, |row| {
        match slots.get(row).and_then(|slot| slot.get()) {
            None => None,
            Some((false, at)) => in_a.get(at),
            Some((true, at)) => in_b.get(at),
        }
    }))
}

impl<'c> Instants<'c> {
    /// What a column the row group's schema does not have gives: no row an instant.
    pub(crate) fn absent(rows: usize) -> Instants<'static> {
        Instants {
            ns: Cow::Borrowed(&[]),
            has: Some(Sel::none(rows)),
        }
    }

    fn made(rows: usize, of: impl Fn(usize) -> Option<i64>) -> Instants<'static> {
        let mut has = Sel::none(rows);
        let mut ns = vec![0i64; has.rows()];
        for (row, slot) in ns.iter_mut().enumerate() {
            if let Some(at) = of(row) {
                *slot = at;
                has.set(row);
            }
        }
        Instants {
            ns: Cow::Owned(ns),
            has: Some(has),
        }
        .settled()
    }

    /// With `has` dropped when every row has an instant, so that `missing` is one test.
    fn settled(mut self) -> Self {
        if self
            .has
            .as_ref()
            .is_some_and(|has| has.rows() > 0 && has.count() == has.rows() as u64)
        {
            self.has = None;
        }
        self
    }

    /// A row's instant, or `None` for a row that has none.
    pub(crate) fn get(&self, row: usize) -> Option<i64> {
        match &self.has {
            Some(has) if !has.get(row) => None,
            _ => self.ns.get(row).copied(),
        }
    }

    /// Whether some row has no instant, and a fallback is worth decoding.
    pub(crate) fn missing(&self) -> bool {
        self.has.is_some()
    }

    /// These instants, and `fallback`'s for the rows that have none here: a row's effective
    /// time is its time column's instant or else the arrival time it was stored with.
    pub(crate) fn or(self, fallback: &Instants<'_>) -> Instants<'c> {
        let Instants { ns, has } = self;
        let Some(mut has) = has else {
            return Instants { ns, has: None };
        };
        let mut ns = ns.into_owned();
        ns.resize(has.rows(), 0);
        for (row, slot) in ns.iter_mut().enumerate() {
            if !has.get(row) {
                if let Some(at) = fallback.get(row) {
                    *slot = at;
                    has.set(row);
                }
            }
        }
        Instants {
            ns: Cow::Owned(ns),
            has: Some(has),
        }
        .settled()
    }

    /// The rows whose instant is in `[since, until)`; a row with no instant is outside every
    /// window.
    pub(crate) fn window(&self, since: Option<i64>, until: Option<i64>) -> Sel {
        let inside = |ns: &i64| in_window(Some(*ns), since, until);
        match &self.has {
            None => Sel::of(&self.ns, inside),
            Some(has) => {
                let mut sel = Sel::none(has.rows());
                for row in has.iter() {
                    if self.ns.get(row).is_some_and(inside) {
                        sel.set(row);
                    }
                }
                sel
            }
        }
    }
}

#[cfg(test)]
mod tests;
