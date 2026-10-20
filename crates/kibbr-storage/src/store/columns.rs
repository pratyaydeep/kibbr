// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! A term, and the time window, put to a decoded column instead of to a row.
//!
//! Building a row for every stored value is wasted on rows thrown away; a typed compare
//! answers with one bit a row (`Sel`). Every compare in `hits` is held to `hits_by_row`
//! and `engine::value_matches`; only `hits_by_row` refuses a value that does not print.

use std::borrow::Cow;

use super::v3::column::{Col, Memo, PieceRef, ValuesRef};
use super::v3::{
    epoch_of, ip_bytes, print_ip, print_uuid, uuid_bytes, Bad, Form, MAX_ROWS, RFC3339,
};
use super::{in_window, instant_of, Term, Want};
use crate::core::types::ColValue;

/// A selection's bits, in words of sixty-four.
const WORDS: usize = 128;

// One bit for every row a row group can hold, and no more.
const _: () = assert!(WORDS * 64 == MAX_ROWS as usize);

/// Which rows of one row group are still in, one bit a row: a bitmap because terms are
/// ANDed, counted and walked in row order, and bits at and past `rows` are always clear.
#[derive(Clone, PartialEq, Eq)]
pub struct Sel {
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
    pub fn all(rows: usize) -> Self {
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
    pub fn none(rows: usize) -> Self {
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
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Put a row in; a row the selection is not over stays out.
    pub fn set(&mut self, row: usize) {
        if row < self.rows {
            self.bits[row / 64] |= 1 << (row % 64);
        }
    }

    pub fn get(&self, row: usize) -> bool {
        row < self.rows && self.bits[row / 64] & (1 << (row % 64)) != 0
    }

    /// Keep only the rows `other` has in too.
    pub fn and(&mut self, other: &Sel) {
        for (mine, theirs) in self.bits.iter_mut().zip(&other.bits) {
            *mine &= theirs;
        }
    }

    /// How many rows are in.
    pub fn count(&self) -> u64 {
        self.bits
            .iter()
            .map(|word| u64::from(word.count_ones()))
            .sum()
    }

    pub fn is_empty(&self) -> bool {
        self.bits.iter().all(|word| *word == 0)
    }

    /// The row of the `n`-th row that is in, counting from 0 in row order.
    pub fn nth(&self, n: u64) -> Option<usize> {
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
    pub fn iter(&self) -> impl Iterator<Item = usize> + '_ {
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

/// The rows of `column` that `term` matches, by typed compare; under `text_column` a stored
/// `Datetime` is compared as the text it arrived as, and a null matches no term, `*` included.
pub fn hits(column: Col<'_>, term: &Term, text_column: bool) -> Result<Sel, Bad> {
    let rows = column.rows();
    let [a, b] = column.pieces()?;
    let Some(a) = a else {
        return Ok(Sel::none(rows));
    };
    let slots = column.slots();
    let Want::Exact { text, inferred } = term.want() else {
        return Ok(match slots {
            None => Sel::all(rows),
            Some(slots) => Sel::of(slots, |slot| slot.get().is_some()),
        });
    };
    let asked = Asked {
        term,
        text,
        inferred,
        text_column,
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
pub fn hits_by_row(column: Col<'_>, term: &Term, text_column: bool) -> Result<Sel, Bad> {
    let mut sel = Sel::none(column.rows());
    let mut memo = Memo::default();
    for row in 0..column.rows() {
        let (value, written) = column.cell_with(row, &mut memo)?;
        if term.matches_cell(&value, written.as_ref(), text_column) {
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
    text_column: bool,
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
            // spelling of it the ladder reads - its epoch digits too - finds it.
            Form::Datetime { .. } if !self.text_column => match (&piece.values, self.inferred) {
                (ValuesRef::Times(values), ColValue::Datetime(want)) => {
                    Sel::of(values, |ns| ns == want)
                }
                (ValuesRef::SameTime(ns), ColValue::Datetime(want)) => every(ns == want),
                (ValuesRef::Times(_) | ValuesRef::SameTime(_), _) => Sel::none(n),
                _ => self.printed(piece)?,
            },
            // In a column that is now text a row holds the text the date arrived as, and only that
            // text matches; an RFC 3339 date is printed, and asked.
            Form::Datetime { unit: RFC3339, .. } => self.printed(piece)?,
            // An epoch is the whole seconds or milliseconds in decimal, the one spelling of a
            // number, so the texts are equal exactly when the term's text is that spelling too.
            Form::Datetime { unit, .. } => {
                let whole = self
                    .text
                    .parse::<i64>()
                    .ok()
                    .filter(|units| units.to_string() == self.text);
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
        Ok(self
            .term
            .matches_cell(&value, written.as_ref(), self.text_column))
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
            if self
                .term
                .matches_cell(&value, written.as_ref(), self.text_column)
            {
                sel.set(at);
            }
        }
        Ok(sel)
    }
}

// ── Time over a column ─────────────────────────────────────────────────────────────

/// Each row's instant, as `instant_of` reads the row's cell, borrowed from the column
/// when it is one `Datetime` piece holding every row.
pub struct Instants<'c> {
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

/// Each row's instant off a decoded column.
pub fn instants(column: Col<'_>) -> Result<Instants<'_>, Bad> {
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
    pub fn absent(rows: usize) -> Instants<'static> {
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
    pub fn get(&self, row: usize) -> Option<i64> {
        match &self.has {
            Some(has) if !has.get(row) => None,
            _ => self.ns.get(row).copied(),
        }
    }

    /// Whether some row has no instant, and a fallback is worth decoding.
    pub fn missing(&self) -> bool {
        self.has.is_some()
    }

    /// These instants, and `fallback`'s for the rows that have none here: a row's effective
    /// time is its time column's instant or else the arrival time it was stored with.
    pub fn or(self, fallback: &Instants<'_>) -> Instants<'c> {
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
    pub fn window(&self, since: Option<i64>, until: Option<i64>) -> Sel {
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
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;
    use crate::store::v3::column::Column;
    use crate::store::v3::samples::{self, Sample, Stored};
    use crate::store::{effective_ts, pool, ts_of, RECEIVED_AT_COLUMN};
    use crate::core::rng::Rng;

    fn decoded(sample: &Sample, chunk: &Stored) -> Column {
        Column::decode(&chunk.bytes, chunk.at, &chunk.dir, sample.rows)
            .unwrap_or_else(|bad| panic!("{}: {}", sample.name, bad.what))
    }

    /// Column `v` of the layout sample of this name.
    fn layout(name: &str) -> (Sample, usize) {
        let sample = samples::layouts()
            .into_iter()
            .find(|s| s.name == name)
            .unwrap_or_else(|| panic!("no sample named `{name}`"));
        let at = sample.place("v").expect("a column v");
        (sample, at)
    }

    // ── The bitmap ─────────────────────────────────────────────────────────────────

    /// A selection made from a list of answers, held to the list every way it is read.
    fn held(list: &[bool]) -> Sel {
        let rows = list.len();
        let mut sel = Sel::none(rows);
        for (row, hit) in list.iter().enumerate() {
            if *hit {
                sel.set(row);
            }
        }
        // Three ways to make it, one selection.
        assert_eq!(Sel::of(list, |hit| *hit), sel);
        assert_eq!(Sel::of_each(rows, list.iter().copied()), sel);

        let rows_in: Vec<usize> = (0..rows).filter(|row| list[*row]).collect();
        assert_eq!(sel.rows(), rows);
        assert_eq!(sel.count(), rows_in.len() as u64);
        assert_eq!(sel.is_empty(), rows_in.is_empty());
        assert_eq!(sel.iter().collect::<Vec<_>>(), rows_in);
        for (row, hit) in list.iter().enumerate() {
            assert_eq!(sel.get(row), *hit, "row {row} of {rows}");
        }
        for (n, row) in rows_in.iter().enumerate() {
            assert_eq!(sel.nth(n as u64), Some(*row), "match {n} of {rows} rows");
        }
        assert_eq!(sel.nth(rows_in.len() as u64), None);
        assert_eq!(sel.nth(u64::MAX), None);

        // A row the selection is not over is never in, asked or told.
        let before = sel.clone();
        for past in [rows, rows + 1, rows + 64, 8_192, 100_000] {
            assert!(!sel.get(past));
            sel.set(past);
        }
        assert_eq!(sel, before);
        for bit in rows..WORDS * 64 {
            assert_eq!(
                sel.bits[bit / 64] & (1 << (bit % 64)),
                0,
                "bit {bit} of {rows} rows"
            );
        }
        sel
    }

    #[test]
    fn a_selection_holds_the_rows_it_was_given_and_no_others() {
        for rows in [0usize, 1, 63, 64, 65, 8_191, 8_192] {
            let mut rng = Rng::new(rows as u64 + 1);
            let lists: Vec<Vec<bool>> = vec![
                vec![false; rows],
                vec![true; rows],
                (0..rows).map(|_| rng.chance(50)).collect(),
                (0..rows).map(|_| rng.chance(2)).collect(),
                (0..rows).map(|row| row + 1 == rows).collect(),
                (0..rows)
                    .map(|row| row % 64 == 63 || row % 64 == 0)
                    .collect(),
            ];
            let sels: Vec<Sel> = lists.iter().map(|list| held(list)).collect();
            assert_eq!(Sel::none(rows), sels[0]);
            assert_eq!(Sel::all(rows), sels[1]);
            assert_eq!(Sel::all(rows).count(), rows as u64);
            assert!(Sel::none(rows).is_empty());
            // ANDing two selections is ANDing their lists, row by row.
            for (a, list_a) in sels.iter().zip(&lists) {
                for (b, list_b) in sels.iter().zip(&lists) {
                    let both: Vec<bool> =
                        list_a.iter().zip(list_b).map(|(a, b)| *a && *b).collect();
                    let mut and = a.clone();
                    and.and(b);
                    assert_eq!(and, held(&both), "{rows} rows");
                }
            }
        }
        // A row group holds 8,192 rows, and a selection asked for more is over that many.
        let over = Sel::all(9_000);
        assert_eq!((over.rows(), over.count()), (8_192, 8_192));
        assert_eq!(Sel::of(&vec![true; 9_000], |hit| *hit), over);
        // What a failed comparison prints: the rows, not a hundred and twenty-eight words.
        let mut few = Sel::none(8);
        few.set(1);
        few.set(5);
        assert_eq!(format!("{few:?}"), "[1, 5] of 8 rows");
    }

    // ── Time ───────────────────────────────────────────────────────────────────────

    /// Windows that cut a list of instants every way: open, open at each end, around the
    /// middle, holding one instant only, and empty.
    fn windows(instants: &[Option<i64>]) -> Vec<(Option<i64>, Option<i64>)> {
        let mut known: Vec<i64> = instants.iter().flatten().copied().collect();
        known.sort_unstable();
        let mut out = vec![
            (None, None),
            (Some(0), Some(0)),
            (Some(i64::MIN), Some(i64::MAX)),
        ];
        if let (Some(first), Some(last)) = (known.first(), known.last()) {
            let middle = known[known.len() / 2];
            out.extend([
                (Some(middle), None),
                (None, Some(middle)),
                (Some(*first), Some(*last)),
                (Some(*first), Some(last.saturating_add(1))),
                (Some(middle), Some(middle.saturating_add(1))),
                (Some(*last), Some(*first)),
                (Some(last.saturating_add(1)), None),
                (None, Some(*first)),
            ]);
        }
        out
    }

    /// A column's instants, the window over them and a row's effective time are each what a
    /// row a query would build says, over every column of every sample.
    #[test]
    fn a_column_s_instants_are_what_each_row_s_cell_reads_as() {
        let (mut with, mut without, mut borrowed, mut fell_back) = (0, 0, 0, 0);
        for sample in samples::all() {
            let rows = sample.rows;
            let built = sample.built();
            let columns: Vec<Column> = sample
                .chunks
                .iter()
                .map(|chunk| decoded(&sample, chunk))
                .collect();
            let cols: Vec<Col<'_>> = columns
                .iter()
                .zip(&sample.chunks)
                .map(|(column, chunk)| column.over(&chunk.bytes).expect("over its own bytes"))
                .collect();
            for (col, name) in cols.iter().zip(&sample.columns) {
                let got = instants(*col).expect("instants");
                let want: Vec<Option<i64>> = (0..rows)
                    .map(|row| instant_of(&col.cell(row).expect("a cell").0))
                    .collect();
                for (row, want) in want.iter().enumerate() {
                    assert_eq!(got.get(row), *want, "{}, `{name}`, row {row}", sample.name);
                    assert_eq!(ts_of(&built[row], name), *want);
                }
                assert_eq!(got.get(rows), None);
                assert_eq!(got.missing(), want.iter().any(Option::is_none) || rows == 0);
                with += want.iter().flatten().count();
                without += want.iter().filter(|ns| ns.is_none()).count();
                borrowed += usize::from(matches!(got.ns, Cow::Borrowed(_)) && got.has.is_none());

                for (since, until) in windows(&want) {
                    let sel = got.window(since, until);
                    assert_eq!(sel.rows(), rows);
                    for (row, ns) in want.iter().enumerate() {
                        assert_eq!(
                            sel.get(row),
                            in_window(*ns, since, until),
                            "{}, `{name}`, row {row}, window {since:?}..{until:?}",
                            sample.name
                        );
                    }
                }

                // The row's effective time under this column as its time column: the
                // column's instant, or the arrival time where the row has one stored.
                let arrived = match sample.place(RECEIVED_AT_COLUMN) {
                    Some(at) => instants(cols[at]).expect("instants"),
                    None => Instants::absent(rows),
                };
                let effective = instants(*col).expect("instants").or(&arrived);
                for (row, built) in built.iter().enumerate() {
                    assert_eq!(effective.get(row), effective_ts(built, name));
                    fell_back += usize::from(want[row].is_none() && effective.get(row).is_some());
                }
                // And with any column behind it, which is the same rule.
                for (behind, behind_name) in cols.iter().zip(&sample.columns) {
                    let both = instants(*col)
                        .expect("instants")
                        .or(&instants(*behind).expect("instants"));
                    for (row, built) in built.iter().enumerate() {
                        let want = ts_of(built, name).or_else(|| ts_of(built, behind_name));
                        assert_eq!(
                            both.get(row),
                            want,
                            "{}, `{name}` then `{behind_name}`",
                            sample.name
                        );
                    }
                    assert_eq!(both.missing(), (0..rows).any(|row| both.get(row).is_none()));
                }
            }
        }
        // Not agreement on nothing: rows with an instant and rows without, time columns
        // read with nothing copied, and rows whose time came from their arrival.
        assert!(
            with > 300 && without > 300 && borrowed > 30 && fell_back >= 3,
            "{with} {without} {borrowed} {fell_back}"
        );
    }

    /// A column the schema does not have gives no row an instant and puts no row in any
    /// window.
    #[test]
    fn an_absent_column_has_no_instants() {
        let none = Instants::absent(5);
        assert!(none.missing());
        assert_eq!((0..6).filter_map(|row| none.get(row)).count(), 0);
        assert_eq!(none.window(None, None), Sel::none(5));
        let still = Instants::absent(5).or(&Instants::absent(5));
        assert!(still.missing() && still.window(None, None).is_empty());
    }

    // ── The inverse a compare of stored bytes leans on ─────────────────────────────

    /// A term is put to an id or an address column as bytes, which is only right if the text
    /// a row would print names one set of bytes and no other.
    #[test]
    fn an_id_and_an_address_read_back_as_the_bytes_that_printed_them() {
        let both = |bytes: [u8; 16]| {
            for upper in [false, true] {
                let text = print_uuid(&bytes, upper);
                assert_eq!(uuid_bytes(&text), Some(bytes), "{text}");
            }
            for spelling in 6..=9 {
                let text = print_ip(&bytes, spelling).expect("an address prints");
                assert_eq!(ip_bytes(&text).as_deref(), Some(&bytes[..]), "{text}");
            }
            let text = print_ip(&bytes[..4], 4).expect("an address prints");
            assert_eq!(ip_bytes(&text).as_deref(), Some(&bytes[..4]), "{text}");
        };
        let mut rng = Rng::new(7);
        for _ in 0..10_000 {
            let mut bytes = [0u8; 16];
            // Runs of zero groups are what the short spelling folds, so make many.
            for group in bytes.chunks_mut(2) {
                if !rng.chance(40) {
                    group[0] = rng.below(256) as u8;
                    group[1] = rng.below(256) as u8;
                }
            }
            both(bytes);
        }
        // By hand: nothing but zeros, an IPv4-mapped address, one zero group alone, a run
        // at each end, and two runs of one length.
        let groups = |groups: [u16; 8]| {
            let mut bytes = [0u8; 16];
            for (at, group) in groups.iter().enumerate() {
                bytes[2 * at..2 * at + 2].copy_from_slice(&group.to_be_bytes());
            }
            bytes
        };
        for bytes in [
            groups([0; 8]),
            groups([0, 0, 0, 0, 0, 0xffff, 0xc000, 0x0201]),
            groups([0, 0, 0, 0, 0, 0xffff, 0, 0]),
            groups([1, 0, 2, 3, 4, 5, 6, 7]),
            groups([0, 0, 1, 2, 3, 4, 5, 6]),
            groups([1, 2, 3, 4, 5, 6, 0, 0]),
            groups([1, 0, 0, 2, 3, 0, 0, 4]),
            groups([0, 0, 0, 0, 0, 0, 0, 1]),
            groups([0xffff; 8]),
        ] {
            both(bytes);
        }
    }

    // ── A term ─────────────────────────────────────────────────────────────────────

    /// The form of the piece that holds a row, by its type's name; `None` for a null.
    fn kind_of(col: Col<'_>, row: usize) -> Option<&'static str> {
        let (in_b, _) = match col.slots() {
            None => (false, row),
            Some(slots) => slots[row].get()?,
        };
        let pieces = col.pieces().expect("pieces");
        let piece = pieces[usize::from(in_b)].as_ref()?;
        Some(piece.form.column_type().as_str())
    }

    /// Texts a query might ask a value by: what its line wrote, what it prints as, and,
    /// for an instant, each form the ladder reads one in.
    fn spellings(value: &ColValue, written: Option<&serde_json::Value>) -> Vec<String> {
        let text = |json: &serde_json::Value| match json {
            serde_json::Value::String(text) => text.clone(),
            other => other.to_string(),
        };
        let mut out = Vec::new();
        if matches!(value, ColValue::Null) {
            return out;
        }
        out.extend(written.map(text));
        out.push(text(&value.printed()));
        if let ColValue::Datetime(ns) = value {
            out.push((ns / 1_000_000_000).to_string());
            out.push((ns / 1_000_000).to_string());
        }
        out
    }

    fn flipped(text: &str) -> String {
        text.chars()
            .map(|c| {
                if c.is_ascii_lowercase() {
                    c.to_ascii_uppercase()
                } else {
                    c.to_ascii_lowercase()
                }
            })
            .collect()
    }

    /// The typed compare finds the rows the printed cells do, for every column of every
    /// sample, as its own type and as text.
    #[test]
    fn a_typed_compare_finds_the_rows_the_printed_cells_do() {
        let mut matched: BTreeMap<&'static str, [u64; 2]> = BTreeMap::new();
        let (mut present, mut compares) = (0u64, 0u64);
        for sample in samples::all() {
            for (chunk, name) in sample.chunks.iter().zip(&sample.columns) {
                let column = decoded(&sample, chunk);
                let col = column.over(&chunk.bytes).expect("over its own bytes");
                let mut texts: BTreeSet<String> =
                    pool::TEXTS.iter().map(|text| text.to_string()).collect();
                for row in 0..sample.rows {
                    let (value, written) = col.cell(row).expect("a cell");
                    for text in spellings(&value, written.as_ref()) {
                        texts.insert(flipped(&text));
                        texts.insert(text);
                    }
                }
                let mut terms = vec![Term::any("v")];
                terms.extend(texts.iter().map(|text| Term::exact("v", text)));
                for term in &terms {
                    for text_column in [false, true] {
                        let slow = hits_by_row(col, term, text_column).expect("by row");
                        let fast = hits(col, term, text_column).expect("typed");
                        assert_eq!(
                            fast, slow,
                            "{}, `{name}`: {term:?}, text column: {text_column}",
                            sample.name
                        );
                        compares += 1;
                        if matches!(term.want(), Want::Any) {
                            present += fast.count();
                            continue;
                        }
                        for row in fast.iter() {
                            let kind = kind_of(col, row).expect("a null matched a term");
                            matched.entry(kind).or_default()[usize::from(text_column)] += 1;
                        }
                    }
                }
            }
        }
        // Not agreement on nothing: a piece of every type matched a term, as its own
        // type and in a column since widened to text.
        let kinds: Vec<&str> = matched.keys().copied().collect();
        assert_eq!(
            kinds,
            ["Bool", "Datetime", "Float64", "Int64", "Ip", "String", "Uuid"],
            "{matched:?}"
        );
        assert!(
            matched
                .values()
                .all(|[own, text]| *own >= 10 && *text >= 10),
            "{matched:?}"
        );
        assert!(present > 1_000 && compares > 5_000, "{present} {compares}");
    }

    /// The rows of a sample's column `v` that a term finds, by typed compare - held to
    /// the printed cells on the way.
    fn found(name: &str, text: &str, text_column: bool) -> Vec<usize> {
        let (sample, at) = layout(name);
        let chunk = &sample.chunks[at];
        let column = decoded(&sample, chunk);
        let col = column.over(&chunk.bytes).expect("over its own bytes");
        let term = match text {
            "*" => Term::any("v"),
            text => Term::exact("v", text),
        };
        let fast = hits(col, &term, text_column).expect("typed");
        assert_eq!(fast, hits_by_row(col, &term, text_column).expect("by row"));
        fast.iter().collect()
    }

    /// What a term means, case by case, on rows small enough to read: the cases a typed
    /// compare could most easily get wrong.
    #[test]
    fn a_term_finds_the_rows_it_always_found() {
        // One number however it was spelled, and a number kept as text because no form
        // printed it back: `-0` and `0` are one integer, found by either.
        let words = "text the ladder types";
        assert_eq!(found(words, "0", false), [0, 1, 13]);
        assert_eq!(found(words, "-0", false), [0, 1, 13]);
        assert_eq!(found(words, "45", false), [9, 10, 11]);
        assert_eq!(found(words, "0045", false), [9, 10, 11]);
        assert_eq!(found(words, "1000.0", false), [3]);
        assert_eq!(found(words, "0.5", false), [2]);
        // `TRUE` is text, and `true` a bool that `True` spells too.
        assert_eq!(found(words, "TRUE", false), [4]);
        assert_eq!(found(words, "true", false), [5]);
        assert_eq!(found(words, "True", false), [5]);
        assert_eq!(found(words, "inf", false), [6]);
        // A date kept as text is found by any spelling of its instant while the column
        // is not text, and only by its own once it is.
        assert_eq!(found(words, "2024-01-15T10:30:00.000Z", false), [7, 8]);
        assert_eq!(found(words, "1705314600", false), [7, 8]);
        assert_eq!(
            found(words, "2024-01-15T10:30:00.000Z", true),
            [] as [usize; 0]
        );
        assert_eq!(found(words, "2024-01-15T10:30:00Z", true), [7]);
        assert_eq!(found(words, "1705314600", true), [8]);
        assert_eq!(found(words, "INFO", false), [12]);
        assert_eq!(found(words, "info", false), [] as [usize; 0]);
        assert_eq!(found(words, "*", false).len(), 16);

        // Integers: bare, quoted, padded and signed are the number.
        assert_eq!(found("integers, bare and quoted", "200", false), [0, 3]);
        assert_eq!(found("integers, bare and quoted", "0200", false), [0, 3]);
        assert_eq!(
            found("integers, bare and quoted", "200.0", false),
            [] as [usize; 0]
        );
        assert_eq!(found("integers padded with zeros", "45", true), [2]);
        assert_eq!(found("integers with a plus", "0", false), [3]);
        assert_eq!(
            found("one integer spelled two ways", "200", false),
            [0, 1, 2, 3]
        );
        assert_eq!(
            found("one integer spelled two ways", "201", false),
            [] as [usize; 0]
        );

        // Floats compare as floats: a zero of either sign is zero, and `200` is an
        // integer that no float equals.
        assert_eq!(found("a zero of each sign", "0.0", false), [0, 1, 2]);
        assert_eq!(found("a zero of each sign", "-0.0", false), [0, 1, 2]);
        assert_eq!(found("a zero of each sign", "0", false), [] as [usize; 0]);
        assert_eq!(found("floats of mixed digit counts", "0.01", false), [0, 7]);
        assert_eq!(found("floats of mixed digit counts", "1e3", false), [4]);
        assert_eq!(
            found("floats of mixed digit counts", "200", false),
            [] as [usize; 0]
        );
        assert_eq!(found("floats of mixed digit counts", "2e2", false), [8]);

        // A null matches nothing, `*` included; a word beside numbers is found beside them.
        let mixed = "integers beside text and nulls";
        assert_eq!(found(mixed, "*", false), [0, 1, 2, 3, 4, 6]);
        assert_eq!(found(mixed, "N/A", false), [3, 4]);
        assert_eq!(found(mixed, "45", false), [6]);
        assert_eq!(found("nothing but nulls", "*", false), [] as [usize; 0]);
        let level = "a level that is a number, a word and a bool";
        assert_eq!(found(level, "30", false), [1, 2, 6]);
        assert_eq!(found(level, "true", false), [3]);
        assert_eq!(found(level, "WARN", true), [5]);

        // Dates: every spelling of the instant in a `Datetime` column, the spelling that
        // arrived in a column that is now text.
        let dates = "dates of mixed fraction digits";
        assert_eq!(found(dates, "2024-01-15T10:30:00Z", false), [0, 3]);
        assert_eq!(found(dates, "1705314600", false), [0, 3]);
        assert_eq!(found(dates, "2024-01-15T16:00:00+05:30", false), [0, 3]);
        assert_eq!(found(dates, "2024-01-15T10:30:00Z", true), [0]);
        assert_eq!(found(dates, "2024-01-15T10:30:00.000Z", true), [3]);
        assert_eq!(found(dates, "1705314600", true), [] as [usize; 0]);
        // Epoch digits arrived as digits, bare or quoted, and are found by them alone.
        let epoch = "epoch seconds, bare and quoted";
        assert_eq!(found(epoch, "1705314600", false), [0, 3]);
        assert_eq!(found(epoch, "2024-01-15T10:30:00Z", false), [0, 3]);
        assert_eq!(found(epoch, "1705314600", true), [0, 3]);
        assert_eq!(found(epoch, "+1705314600", true), [] as [usize; 0]);
        assert_eq!(found(epoch, "1705314600000", true), [] as [usize; 0]);
        assert_eq!(found(epoch, "2024-01-15T10:30:00Z", true), [] as [usize; 0]);
        assert_eq!(found("epoch milliseconds", "1705314600000", true), [0]);
        assert_eq!(
            found("epoch milliseconds", "1705314600", true),
            [] as [usize; 0]
        );
        assert_eq!(found("one epoch second", "1705314600", true), [0, 1, 2]);

        assert_eq!(found("booleans, bare and quoted", "true", false), [0, 2, 5]);
        assert_eq!(
            found("booleans, bare and quoted", "False", false),
            [1, 3, 4]
        );
        assert_eq!(
            found("booleans, bare and quoted", "TRUE", false),
            [] as [usize; 0]
        );

        // An id is found in the case it was written in, and in no other.
        let lower = "550e8400-e29b-41d4-a716-446655440000";
        let upper = "550E8400-E29B-41D4-A716-446655440000";
        let mixed = "550e8400-E29B-41d4-A716-446655440000";
        assert_eq!(found("ids", lower, false), [0, 2]);
        assert_eq!(found("ids", upper, false), [] as [usize; 0]);
        assert_eq!(found("ids in both cases", lower, false), [0, 4]);
        assert_eq!(found("ids in both cases", upper, false), [1, 3]);
        assert_eq!(found("ids in both cases", mixed, false), [] as [usize; 0]);
        assert_eq!(found("one id", lower, true), [0, 1, 2]);
        assert_eq!(found("one id", upper, true), [] as [usize; 0]);

        // An address is found in the spelling it was written in.
        assert_eq!(found("addresses, v4", "10.0.0.1", false), [0, 2]);
        assert_eq!(found("addresses, v4", "010.0.0.1", false), [] as [usize; 0]);
        assert_eq!(found("addresses, v6", "2001:db8::1", false), [0]);
        assert_eq!(
            found("addresses, v6", "2001:DB8::1", false),
            [] as [usize; 0]
        );
        assert_eq!(found("addresses, v6", "::ffff:192.0.2.1", false), [2]);
        assert_eq!(
            found("addresses, v6", "::ffff:c000:201", false),
            [] as [usize; 0]
        );
        assert_eq!(
            found("addresses, v6 in capitals", "2001:DB8::1", false),
            [0]
        );
        assert_eq!(
            found("addresses, v6 in capitals", "2001:db8::1", false),
            [] as [usize; 0]
        );
        let whole = "2001:0db8:0000:0000:0000:0000:0000:0001";
        assert_eq!(found("addresses, v6 with every group", whole, false), [0]);
        assert_eq!(
            found("addresses, v6 with every group", "2001:db8::1", false),
            [] as [usize; 0]
        );
        assert_eq!(
            found("addresses of both families", "10.0.0.1", false),
            [0, 2, 4]
        );
        assert_eq!(
            found("addresses of both families", "2001:db8::2", false),
            [3]
        );
    }
}
