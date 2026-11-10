// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! One chunk decoded to what its encodings hold, with no cell printed: a filter needs a
//! row's number, word or bytes, not its cell, which `read::cell` prints only when asked.
//! A second reader beside `read::chunk`, held to it: `chunk` is `Ok(cells)` exactly when
//! `Column::decode` and every `Col::cell` are `Ok`, and then the cells are equal.
//! `decode` checks the layout; what only printing finds, `Col::cell` refuses at its row.

use super::read::{self, Cell, ChunkDir, PieceDir, Raw, SOME_NULL};
use super::{
    bit, count_set, encodes, index_width, read_bitmap, unzigzag, Bad, Bytes, Form, JsonKind, BITS,
    CONSTANT, DELTA, DICTIONARY, MAX_BODY, MAX_ROWS, VARINT, ZSTD,
};
use crate::model::types::ColValue;

/// A place in a chunk's body: a word, a constant, a bitmap. Places, not slices, so a
/// column owns everything it holds and can be kept beside the bytes it was decoded from.
#[derive(Debug, Clone, Copy)]
struct Span {
    at: u32,
    len: u32,
}

impl Span {
    /// The `len` bytes starting at `from`; `origin` is the offset of the body's first byte.
    /// Both fit: a body is held to what a `u32` counts before anything in it is read.
    fn at(from: u64, len: usize, origin: u64) -> Self {
        Self {
            at: (from - origin) as u32,
            len: len as u32,
        }
    }

    /// The `len` bytes a reader has just taken.
    fn taken(r: &Bytes<'_>, len: usize, origin: u64) -> Self {
        Self::at(r.offset() - len as u64, len, origin)
    }
}

/// The bytes at a place; one outside the bytes is an error and never a slice out of range.
fn slice(body: &[u8], span: Span, base: u64) -> Result<&[u8], Bad> {
    let from = span.at as usize;
    from.checked_add(span.len as usize)
        .and_then(|to| body.get(from..to))
        .ok_or_else(|| {
            Bad::at(
                base,
                "a decoded column was read over bytes that are not its chunk's",
            )
        })
}

/// Bit `i` of a bitmap, clear past its end.
fn bit_at(map: &[u8], i: usize) -> bool {
    map.get(i / 8)
        .is_some_and(|byte| byte & (1 << (i % 8)) != 0)
}

/// Where one row's value is - piece A or B, and which of its values - or nowhere, for a
/// null: two bytes a row, since an index into a piece takes thirteen bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Slot(u16);

impl Slot {
    const NULL: Slot = Slot(u16::MAX);
    const PIECE_B: u16 = 1 << 15;

    fn of(in_b: bool, at: usize) -> Self {
        let piece = if in_b { Self::PIECE_B } else { 0 };
        Slot(at as u16 | piece)
    }

    /// Whether the value is in piece B, and its place in its piece; `None` for a null.
    pub(crate) fn get(self) -> Option<(bool, usize)> {
        (self != Self::NULL).then_some((
            self.0 & Self::PIECE_B != 0,
            usize::from(self.0 & !Self::PIECE_B),
        ))
    }
}

/// One chunk, decoded and not printed; it owns what it decoded, and is read through `over`
/// with the stored bytes it was decoded from.
#[derive(Debug)]
pub(crate) struct Column {
    rows: usize,
    /// How many stored bytes it was decoded from: `over` takes no other length.
    stored_len: usize,
    /// What the chunk's zstd frame held. `None` when the body is the stored bytes.
    unzstd: Option<Vec<u8>>,
    /// `None` when piece A holds every row, in row order: no nulls and no second piece,
    /// which is most columns, and then a row's number is its value's.
    slots: Option<Vec<Slot>>,
    /// `None` when every row is null.
    a: Option<Piece>,
    b: Option<Piece>,
    /// The chunk's offset in its file: the place an error names.
    base: u64,
}

#[derive(Debug)]
struct Piece {
    form: Form,
    n: usize,
    lits: Lits,
    /// One byte a row, where the form's digit count is per row.
    digits: Option<Span>,
    values: Values,
}

/// Which rows of a piece were JSON numbers or bools: `Form::kind`, with the bitmap's
/// place when the piece holds both kinds.
#[derive(Debug, Clone, Copy)]
enum Lits {
    Str,
    Lit,
    Mixed(Span),
}

#[derive(Debug)]
enum Values {
    Ints(Vec<i64>),
    Floats(Vec<f64>),
    /// Nanoseconds, the differences already added up.
    Times(Vec<i64>),
    Bools(Vec<bool>),
    /// A word list: where each word lies, and the word each row names. A list has no more
    /// words than its piece has rows, so a word's number fits sixteen bits.
    Words {
        words: Vec<Span>,
        codes: Vec<u16>,
    },
    /// Text in plain layout, a place a row: files from before the encodings.
    Texts(Vec<Span>),
    /// A `Uuid`'s or an `Ip`'s bytes in plain layout, as they are stored: every value
    /// `width` bytes, one after the other from `at`.
    Fixed {
        at: u32,
        width: u8,
    },
    /// One value that every row holds.
    Same(One),
}

#[derive(Debug, Clone, Copy)]
enum One {
    Int(i64),
    Float(f64),
    Time(i64),
    Bool(bool),
    Text(Span),
    Fixed(Span),
}

/// What a body lays out, before it is a column.
#[derive(Default)]
struct Laid {
    slots: Option<Vec<Slot>>,
    a: Option<Piece>,
    b: Option<Piece>,
}

impl Column {
    /// A chunk's stored bytes, already CRC-checked, as typed values; `rows` is held to what a
    /// row group holds before anything is made for it.
    pub(crate) fn decode(stored: &[u8], base: u64, dir: &ChunkDir, rows: usize) -> Result<Self, Bad> {
        if rows > MAX_ROWS as usize {
            return Err(read::too_many_rows(base, rows));
        }
        let (unzstd, laid) = match dir.compression {
            ZSTD => {
                let body =
                    read::unzstd(stored, dir.body_len).map_err(|what| Bad::at(base, what))?;
                // An offset inside the body is not one in the file: the chunk's, and where
                // in its body.
                let laid = lay(&body, 0, dir, rows).map_err(|bad| {
                    Bad::at(
                        base,
                        format!("{}, at byte {} of the chunk's body", bad.what, bad.offset),
                    )
                })?;
                (Some(body), laid)
            }
            _ => (None, lay(stored, base, dir, rows)?),
        };
        Ok(Self {
            rows,
            stored_len: stored.len(),
            unzstd,
            slots: laid.slots,
            a: laid.a,
            b: laid.b,
            base,
        })
    }

    /// This column over the bytes it was decoded from; bytes of another length are refused,
    /// so the wrong bytes are an error or the wrong text and never a panic.
    pub(crate) fn over<'c>(&'c self, stored: &'c [u8]) -> Result<Col<'c>, Bad> {
        if stored.len() != self.stored_len {
            return Err(Bad::at(
                self.base,
                format!(
                    "a column decoded from {} stored byte(s) was put over {}",
                    self.stored_len,
                    stored.len()
                ),
            ));
        }
        Ok(Col {
            column: self,
            body: self.unzstd.as_deref().unwrap_or(stored),
        })
    }
}

/// A chunk's body as slots and pieces. `origin` is the offset of the body's first byte:
/// the chunk's own for a body stored as it is, 0 for one out of a zstd frame.
fn lay(body: &[u8], origin: u64, dir: &ChunkDir, rows: usize) -> Result<Laid, Bad> {
    let Some(a_dir) = dir.a else {
        return Ok(Laid::default());
    };
    if u32::try_from(body.len()).is_err() {
        return Err(Bad::at(origin, format!("a chunk of {} bytes", body.len())));
    }
    let mut r = Bytes::new(body, origin);
    let presence = if dir.layout & SOME_NULL != 0 {
        let at = r.offset();
        let map = read_bitmap(&mut r, rows, "presence bitmap")?;
        let set = count_set(map);
        if set == 0 || set == rows {
            return Err(Bad::at(
                at,
                format!("presence bitmap marks {set} of {rows} present"),
            ));
        }
        Some(map)
    } else {
        None
    };
    let present = presence.map_or(rows, count_set);
    let (a, b, selector) = match dir.b {
        Some(b_dir) => {
            let at = r.offset();
            let selector = read_bitmap(&mut r, present, "selector bitmap")?;
            let in_b = count_set(selector);
            if in_b == 0 || in_b == present {
                return Err(Bad::at(at, "a piece of two holds no rows"));
            }
            let a_len = r.u32("piece_a_len")? as usize;
            let a_at = r.offset();
            let a_bytes = r.take(a_len, "piece A")?;
            let b_at = r.offset();
            let b_bytes = r.take(r.remaining(), "piece B")?;
            let a = piece(a_bytes, a_at, origin, &a_dir, present - in_b)?;
            let b = piece(b_bytes, b_at, origin, &b_dir, in_b)?;
            (a, Some(b), Some(selector))
        }
        None => {
            let at = r.offset();
            let bytes = r.take(r.remaining(), "piece A")?;
            (piece(bytes, at, origin, &a_dir, present)?, None, None)
        }
    };

    // With no nulls and one piece a row's number is its value's, and nothing is kept.
    let slots = (presence.is_some() || selector.is_some()).then(|| {
        let mut slots = Vec::with_capacity(rows);
        let (mut nth, mut in_a, mut in_b) = (0usize, 0usize, 0usize);
        for row in 0..rows {
            if presence.is_some_and(|map| !bit(map, row)) {
                slots.push(Slot::NULL);
                continue;
            }
            // The counts were taken from the same bitmaps, so neither piece runs dry.
            let from_b = selector.is_some_and(|map| bit(map, nth));
            nth += 1;
            let next = if from_b { &mut in_b } else { &mut in_a };
            slots.push(Slot::of(from_b, *next));
            *next += 1;
        }
        slots
    });
    Ok(Laid {
        slots,
        a: Some(a),
        b,
    })
}

/// One value in plain layout, kept as its type holds it or as the place of its bytes.
fn one(r: &mut Bytes<'_>, form: &Form, origin: u64) -> Result<One, Bad> {
    Ok(match read::plain_value(r, form)? {
        Raw::Text(text) => One::Text(Span::taken(r, text.len(), origin)),
        Raw::Int(value) => One::Int(value),
        Raw::Float(value) => One::Float(value),
        Raw::Time(ns) => One::Time(ns),
        Raw::Bool(value) => One::Bool(value),
        Raw::Uuid(bytes) => One::Fixed(Span::taken(r, bytes.len(), origin)),
        Raw::Ip(bytes) => One::Fixed(Span::taken(r, bytes.len(), origin)),
    })
}

/// What a value of another type than its piece's is called, here as in `read::cell`.
fn mistyped(at: u64) -> Bad {
    Bad::at(at, "a value of another type than its piece's")
}

/// One piece of `n` rows, read as `read::piece` reads it with nothing printed. `base` is
/// the offset of the piece's first byte and `origin` of its body's.
fn piece(bytes: &[u8], base: u64, origin: u64, dir: &PieceDir, n: usize) -> Result<Piece, Bad> {
    let form = dir.form;
    if !encodes(form.column_type(), dir.encoding) {
        return Err(Bad::at(
            base,
            format!(
                "encoding {} on a {:?} piece, which this build does not read",
                dir.encoding,
                form.column_type()
            ),
        ));
    }
    let mut r = Bytes::new(bytes, base);
    let (kinds, lits) = match form.kind() {
        JsonKind::Str => (None, Lits::Str),
        JsonKind::Lit => (None, Lits::Lit),
        JsonKind::Mixed => {
            let at = r.offset();
            let map = read_bitmap(&mut r, n, "kind bitmap")?;
            let lits = count_set(map);
            if lits == 0 || lits == n {
                return Err(Bad::at(at, "json_kind 2 over rows that are all one kind"));
            }
            (Some(map), Lits::Mixed(Span::at(at, map.len(), origin)))
        }
    };
    let digits = if form.per_row_digits() {
        let at = r.offset();
        let bytes = r.take(n, "per-row digits")?;
        Some((bytes, Span::at(at, n, origin)))
    } else {
        None
    };
    // The one thing a row's spelling can break without being printed: a JSON number has no
    // digit count. Asked row by row as `read::piece` does, so the same broken row is named.
    let spelled = |i: usize, at: u64| -> Result<(), Bad> {
        let Some((digits, _)) = digits else {
            return Ok(());
        };
        let lit = match lits {
            Lits::Str => false,
            Lits::Lit => true,
            Lits::Mixed(_) => kinds.is_some_and(|map| bit(map, i)),
        };
        if lit && digits[i] != 0 {
            return Err(Bad::at(at, "a JSON number with a digits byte"));
        }
        Ok(())
    };
    // A constant or a word list stands for rows it does not store one by one, and a few
    // stored bytes must not stand for more than a body may hold: `read::piece` says why.
    let too_much = |plain: usize, at: u64| {
        Bad::at(
            at,
            format!(
                "{n} rows that would take {plain} bytes written plain, past the {MAX_BODY} \
                 a body may hold"
            ),
        )
    };
    let room = n.min(r.remaining());
    let values = match dir.encoding {
        DICTIONARY => {
            let at = r.offset();
            let count = r.u32("dictionary word count")? as usize;
            if count == 0 || count > n {
                return Err(Bad::at(
                    at,
                    format!("a word list of {count} words for {n} rows"),
                ));
            }
            // Each word takes its four length bytes at least, so this many cannot
            // overstate what is there by much.
            let mut words = Vec::with_capacity(count.min(r.remaining() / 4));
            let mut lens = Vec::with_capacity(words.capacity());
            for _ in 0..count {
                let at = r.offset();
                let One::Text(word) = one(&mut r, &form, origin)? else {
                    return Err(mistyped(at));
                };
                words.push(word);
                lens.push((r.offset() - at) as usize);
            }
            let width = index_width(count);
            let indexes_at = r.offset();
            let indexes = r.take(n * width, "dictionary indexes")?;
            let mut codes = Vec::with_capacity(n);
            let mut plain = 0usize;
            // Numbered in the order rows first name them, and every word named.
            let mut named = 0usize;
            for i in 0..n {
                let mut le = [0u8; 4];
                le[..width].copy_from_slice(&indexes[i * width..(i + 1) * width]);
                let index = u32::from_le_bytes(le) as usize;
                let at = indexes_at + (i * width) as u64;
                let Some(len) = lens.get(index) else {
                    return Err(Bad::at(
                        at,
                        format!("row {i} names word {index} of a list of {count}"),
                    ));
                };
                if index > named {
                    return Err(Bad::at(
                        at,
                        format!("row {i} names word {index} before word {named}"),
                    ));
                }
                if index == named {
                    named += 1;
                }
                plain = plain.saturating_add(*len);
                if plain > MAX_BODY as usize {
                    return Err(too_much(plain, at));
                }
                spelled(i, at)?;
                // Below `count`, which is at most the rows of a row group.
                codes.push(index as u16);
            }
            if named != count {
                return Err(Bad::at(
                    indexes_at,
                    format!("a word list of {count} words whose rows name {named}"),
                ));
            }
            Values::Words { words, codes }
        }
        VARINT => {
            let mut values = Vec::with_capacity(room);
            for i in 0..n {
                let at = r.offset();
                values.push(unzigzag(r.varint("an Int64")?));
                spelled(i, at)?;
            }
            Values::Ints(values)
        }
        BITS => {
            let at = r.offset();
            let bits = read_bitmap(&mut r, n, "Bool bits")?;
            let mut values = Vec::with_capacity(n);
            for i in 0..n {
                spelled(i, at)?;
                values.push(bit(bits, i));
            }
            Values::Bools(values)
        }
        DELTA => {
            let mut values = Vec::with_capacity(room);
            let mut before = 0i64;
            for i in 0..n {
                let at = r.offset();
                let step = unzigzag(r.varint("a Datetime difference")?);
                before = before.wrapping_add(step);
                spelled(i, at)?;
                values.push(before);
            }
            Values::Times(values)
        }
        CONSTANT => {
            let at = r.offset();
            let value = one(&mut r, &form, origin)?;
            let plain = n.saturating_mul((r.offset() - at) as usize);
            if plain > MAX_BODY as usize {
                return Err(too_much(plain, at));
            }
            for i in 0..n {
                spelled(i, at)?;
            }
            Values::Same(value)
        }
        _ => {
            let mut values = match form {
                Form::String { .. } => Values::Texts(Vec::with_capacity(room)),
                Form::Int64 { .. } => Values::Ints(Vec::with_capacity(room)),
                Form::Float64 { .. } => Values::Floats(Vec::with_capacity(room)),
                Form::Datetime { .. } => Values::Times(Vec::with_capacity(room)),
                Form::Bool { .. } => Values::Bools(Vec::with_capacity(room)),
                Form::Uuid { .. } => Values::Fixed {
                    at: Span::at(r.offset(), 0, origin).at,
                    width: 16,
                },
                Form::Ip { spelling } => Values::Fixed {
                    at: Span::at(r.offset(), 0, origin).at,
                    width: if spelling == 4 { 4 } else { 16 },
                },
            };
            for i in 0..n {
                let at = r.offset();
                let value = one(&mut r, &form, origin)?;
                spelled(i, at)?;
                match (&mut values, value) {
                    (Values::Texts(spans), One::Text(span)) => spans.push(span),
                    (Values::Ints(values), One::Int(value)) => values.push(value),
                    (Values::Floats(values), One::Float(value)) => values.push(value),
                    (Values::Times(values), One::Time(ns)) => values.push(ns),
                    (Values::Bools(values), One::Bool(value)) => values.push(value),
                    // One after the other from the first, which is all `Fixed` records.
                    (Values::Fixed { .. }, One::Fixed(_)) => {}
                    _ => return Err(mistyped(at)),
                }
            }
            values
        }
    };
    r.done("a piece")?;
    Ok(Piece {
        form,
        n,
        lits,
        digits: digits.map(|(_, span)| span),
        values,
    })
}

impl Piece {
    fn lit(&self, body: &[u8], base: u64, i: usize) -> Result<bool, Bad> {
        Ok(match self.lits {
            Lits::Str => false,
            Lits::Lit => true,
            Lits::Mixed(map) => bit_at(slice(body, map, base)?, i),
        })
    }

    /// Value `i` as its encoding laid it out, ready for its form to print.
    fn raw<'b>(&self, body: &'b [u8], base: u64, i: usize) -> Result<Raw<'b>, Bad> {
        let gone = || Bad::at(base, format!("value {i} of a piece that holds {}", self.n));
        let fixed = |bytes: &'b [u8]| match self.form {
            Form::Uuid { .. } => <[u8; 16]>::try_from(bytes)
                .map(Raw::Uuid)
                .map_err(|_| mistyped(base)),
            Form::Ip { .. } => Ok(Raw::Ip(bytes)),
            _ => Err(mistyped(base)),
        };
        if i >= self.n {
            return Err(gone());
        }
        match &self.values {
            Values::Ints(values) => values.get(i).map(|v| Raw::Int(*v)).ok_or_else(gone),
            Values::Floats(values) => values.get(i).map(|v| Raw::Float(*v)).ok_or_else(gone),
            Values::Times(values) => values.get(i).map(|v| Raw::Time(*v)).ok_or_else(gone),
            Values::Bools(values) => values.get(i).map(|v| Raw::Bool(*v)).ok_or_else(gone),
            Values::Words { words, codes } => {
                let word = codes
                    .get(i)
                    .and_then(|code| words.get(usize::from(*code)))
                    .ok_or_else(gone)?;
                Ok(Raw::Text(slice(body, *word, base)?))
            }
            Values::Texts(spans) => {
                let span = spans.get(i).ok_or_else(gone)?;
                Ok(Raw::Text(slice(body, *span, base)?))
            }
            Values::Fixed { at, width } => {
                let width = u32::from(*width);
                let span = Span {
                    at: at + i as u32 * width,
                    len: width,
                };
                fixed(slice(body, span, base)?)
            }
            Values::Same(One::Int(value)) => Ok(Raw::Int(*value)),
            Values::Same(One::Float(value)) => Ok(Raw::Float(*value)),
            Values::Same(One::Time(ns)) => Ok(Raw::Time(*ns)),
            Values::Same(One::Bool(value)) => Ok(Raw::Bool(*value)),
            Values::Same(One::Text(span)) => Ok(Raw::Text(slice(body, *span, base)?)),
            Values::Same(One::Fixed(span)) => fixed(slice(body, *span, base)?),
        }
    }

    /// Value `i` printed through the piece's form: `read::cell` on the value, the row's
    /// kind and its digits byte, as `read::piece` calls it.
    fn cell(&self, body: &[u8], base: u64, i: usize) -> Result<Cell, Bad> {
        let raw = self.raw(body, base, i)?;
        let lit = self.lit(body, base, i)?;
        let row_digits = match self.digits {
            Some(span) => slice(body, span, base)?.get(i).copied(),
            None => None,
        };
        read::cell(&raw, &self.form, lit, row_digits, base)
    }

    /// The piece's values as slices of the column and of `body`.
    fn over<'c>(&'c self, body: &'c [u8], base: u64) -> Result<PieceRef<'c>, Bad> {
        let at = |span: &Span| slice(body, *span, base);
        let values = match &self.values {
            Values::Ints(values) => ValuesRef::Ints(values),
            Values::Floats(values) => ValuesRef::Floats(values),
            Values::Times(values) => ValuesRef::Times(values),
            Values::Bools(values) => ValuesRef::Bools(values),
            Values::Words { words, codes } => ValuesRef::Words {
                words: words.iter().map(at).collect::<Result<_, _>>()?,
                codes,
            },
            Values::Texts(spans) => {
                ValuesRef::Texts(spans.iter().map(at).collect::<Result<_, _>>()?)
            }
            Values::Fixed { at, width } => {
                let span = Span {
                    at: *at,
                    len: self.n as u32 * u32::from(*width),
                };
                ValuesRef::Fixed {
                    bytes: slice(body, span, base)?,
                    width: usize::from(*width),
                }
            }
            Values::Same(One::Int(value)) => ValuesRef::SameInt(*value),
            Values::Same(One::Float(value)) => ValuesRef::SameFloat(*value),
            Values::Same(One::Time(ns)) => ValuesRef::SameTime(*ns),
            Values::Same(One::Bool(value)) => ValuesRef::SameBool(*value),
            Values::Same(One::Text(span)) => ValuesRef::SameText(at(span)?),
            Values::Same(One::Fixed(span)) => ValuesRef::SameFixed(at(span)?),
        };
        let kinds = match self.lits {
            Lits::Mixed(map) => Some(slice(body, map, base)?),
            Lits::Str | Lits::Lit => None,
        };
        Ok(PieceRef {
            form: self.form,
            n: self.n,
            values,
            piece: self,
            body,
            kinds,
            base,
        })
    }
}

/// A decoded column and its body: what a filter, a time window and a row build read.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Col<'c> {
    column: &'c Column,
    body: &'c [u8],
}

/// A word or a constant printed once per spelling and copied after, since printing text
/// runs the type ladder and a row group names the same few words thousands of times.
#[derive(Debug, Default)]
pub(crate) struct Memo {
    a: Vec<Option<Cell>>,
    b: Vec<Option<Cell>>,
}

impl<'c> Col<'c> {
    pub(crate) fn rows(&self) -> usize {
        self.column.rows
    }

    /// The piece that holds `row` and the value's place in it. `None` for a null.
    fn place(&self, row: usize) -> Result<Option<(bool, &'c Piece, usize)>, Bad> {
        let column = self.column;
        if row >= column.rows {
            return Err(Bad::at(
                column.base,
                format!("row {row} of a chunk of {} rows", column.rows),
            ));
        }
        let (in_b, at) = match &column.slots {
            None => (false, row),
            Some(slots) => match slots.get(row).and_then(|slot| slot.get()) {
                Some(place) => place,
                None => return Ok(None),
            },
        };
        let piece = if in_b { &column.b } else { &column.a };
        Ok(piece.as_ref().map(|piece| (in_b, piece, at)))
    }

    /// A value printed, its refusal named by the chunk and the row: the value's own
    /// offset went with the bytes it was decoded from.
    fn print(&self, piece: &Piece, at: usize, row: usize) -> Result<Cell, Bad> {
        let base = self.column.base;
        piece
            .cell(self.body, base, at)
            .map_err(|bad| Bad::at(base, format!("{}, at row {row} of the chunk", bad.what)))
    }

    /// The cell `read::chunk` gives this row.
    pub(crate) fn cell(&self, row: usize) -> Result<Cell, Bad> {
        match self.place(row)? {
            None => Ok((ColValue::Null, None)),
            Some((_, piece, at)) => self.print(piece, at, row),
        }
    }

    /// `cell`, printing a word or a constant once for each way rows spell it.
    pub(crate) fn cell_with(&self, row: usize, memo: &mut Memo) -> Result<Cell, Bad> {
        let Some((in_b, piece, at)) = self.place(row)? else {
            return Ok((ColValue::Null, None));
        };
        let spelling = match &piece.values {
            Values::Words { codes, .. } => codes.get(at).map(|code| usize::from(*code) * 2),
            Values::Same(_) if piece.digits.is_none() => Some(0),
            _ => None,
        };
        let Some(spelling) = spelling else {
            return self.print(piece, at, row);
        };
        let key = spelling + usize::from(piece.lit(self.body, self.column.base, at)?);
        let kept = if in_b { &mut memo.b } else { &mut memo.a };
        if let Some(Some(cell)) = kept.get(key) {
            return Ok(cell.clone());
        }
        let cell = self.print(piece, at, row)?;
        if kept.len() <= key {
            kept.resize(key + 1, None);
        }
        kept[key] = Some(cell.clone());
        Ok(cell)
    }

    /// Where each row's value is; `None` when piece A holds every row in row order.
    pub(crate) fn slots(&self) -> Option<&'c [Slot]> {
        self.column.slots.as_deref()
    }

    /// Piece A and piece B, as slices; neither when every row is null.
    pub(crate) fn pieces(&self) -> Result<[Option<PieceRef<'c>>; 2], Bad> {
        let column = self.column;
        let over = |piece: &'c Option<Piece>| {
            piece
                .as_ref()
                .map(|piece| piece.over(self.body, column.base))
                .transpose()
        };
        Ok([over(&column.a)?, over(&column.b)?])
    }
}

/// One piece of a column as borrowed slices: what a typed compare runs over.
#[derive(Debug)]
pub(crate) struct PieceRef<'c> {
    pub form: Form,
    /// How many values the piece holds.
    pub n: usize,
    pub values: ValuesRef<'c>,
    piece: &'c Piece,
    body: &'c [u8],
    kinds: Option<&'c [u8]>,
    base: u64,
}

/// A piece's values, by what its encoding holds.
#[derive(Debug)]
pub(crate) enum ValuesRef<'c> {
    Ints(&'c [i64]),
    Floats(&'c [f64]),
    /// Nanoseconds.
    Times(&'c [i64]),
    Bools(&'c [bool]),
    /// A word list, and the word each value names.
    Words {
        words: Vec<&'c [u8]>,
        codes: &'c [u16],
    },
    /// Text, a value at a time.
    Texts(Vec<&'c [u8]>),
    /// A `Uuid`'s or an `Ip`'s stored bytes, `width` a value.
    Fixed {
        bytes: &'c [u8],
        width: usize,
    },
    SameInt(i64),
    SameFloat(f64),
    SameTime(i64),
    SameBool(bool),
    SameText(&'c [u8]),
    SameFixed(&'c [u8]),
}

impl PieceRef<'_> {
    /// Whether value `i` was a JSON number or bool on its line, and not a string.
    pub(crate) fn lit(&self, i: usize) -> bool {
        match self.form.kind() {
            JsonKind::Str => false,
            JsonKind::Lit => true,
            JsonKind::Mixed => self.kinds.is_some_and(|map| bit_at(map, i)),
        }
    }

    /// Value `i` of the piece printed through its form.
    pub(crate) fn cell(&self, i: usize) -> Result<Cell, Bad> {
        self.piece.cell(self.body, self.base, i)
    }

    /// A word of a String piece, or its constant, printed as a string row or as a literal
    /// row: the cell of every row that names the word and is of that kind.
    pub(crate) fn text_cell(&self, word: &[u8], lit: bool) -> Result<Cell, Bad> {
        read::cell(&Raw::Text(word), &self.form, lit, None, self.base)
    }
}

#[cfg(test)]
mod tests;
