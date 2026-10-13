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
use kibbr_core::types::ColValue;

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
pub struct Slot(u16);

impl Slot {
    const NULL: Slot = Slot(u16::MAX);
    const PIECE_B: u16 = 1 << 15;

    fn of(in_b: bool, at: usize) -> Self {
        let piece = if in_b { Self::PIECE_B } else { 0 };
        Slot(at as u16 | piece)
    }

    /// Whether the value is in piece B, and its place in its piece; `None` for a null.
    pub fn get(self) -> Option<(bool, usize)> {
        (self != Self::NULL).then_some((
            self.0 & Self::PIECE_B != 0,
            usize::from(self.0 & !Self::PIECE_B),
        ))
    }
}

/// One chunk, decoded and not printed; it owns what it decoded, and is read through `over`
/// with the stored bytes it was decoded from.
#[derive(Debug)]
pub struct Column {
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
    pub fn decode(stored: &[u8], base: u64, dir: &ChunkDir, rows: usize) -> Result<Self, Bad> {
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
    pub fn over<'c>(&'c self, stored: &'c [u8]) -> Result<Col<'c>, Bad> {
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
pub struct Col<'c> {
    column: &'c Column,
    body: &'c [u8],
}

/// A word or a constant printed once per spelling and copied after, since printing text
/// runs the type ladder and a row group names the same few words thousands of times.
#[derive(Debug, Default)]
pub struct Memo {
    a: Vec<Option<Cell>>,
    b: Vec<Option<Cell>>,
}

impl<'c> Col<'c> {
    pub fn rows(&self) -> usize {
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
    pub fn cell(&self, row: usize) -> Result<Cell, Bad> {
        match self.place(row)? {
            None => Ok((ColValue::Null, None)),
            Some((_, piece, at)) => self.print(piece, at, row),
        }
    }

    /// `cell`, printing a word or a constant once for each way rows spell it.
    pub fn cell_with(&self, row: usize, memo: &mut Memo) -> Result<Cell, Bad> {
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
    pub fn slots(&self) -> Option<&'c [Slot]> {
        self.column.slots.as_deref()
    }

    /// Piece A and piece B, as slices; neither when every row is null.
    pub fn pieces(&self) -> Result<[Option<PieceRef<'c>>; 2], Bad> {
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
pub struct PieceRef<'c> {
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
pub enum ValuesRef<'c> {
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
    pub fn lit(&self, i: usize) -> bool {
        match self.form.kind() {
            JsonKind::Str => false,
            JsonKind::Lit => true,
            JsonKind::Mixed => self.kinds.is_some_and(|map| bit_at(map, i)),
        }
    }

    /// Value `i` of the piece printed through its form.
    pub fn cell(&self, i: usize) -> Result<Cell, Bad> {
        self.piece.cell(self.body, self.base, i)
    }

    /// A word of a String piece, or its constant, printed as a string row or as a literal
    /// row: the cell of every row that names the word and is of that kind.
    pub fn text_cell(&self, word: &[u8], lit: bool) -> Result<Cell, Bad> {
        read::cell(&Raw::Text(word), &self.form, lit, None, self.base)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::super::encodings::chunk_both;
    use super::super::file::{open_sealed, walk, Reader};
    use super::super::read::{ALL_NULL, TWO_PIECES};
    use super::super::samples::{self, Sample};
    use super::super::{type_byte, NO_COMPRESSION, PER_ROW, PLAIN};
    use super::*;

    fn sample(name: &str) -> Sample {
        samples::layouts()
            .into_iter()
            .find(|s| s.name == name)
            .unwrap_or_else(|| panic!("no sample named `{name}`"))
    }

    /// Column `v` of a sample, decoded.
    fn decoded(sample: &Sample) -> Column {
        let chunk = &sample.chunks[sample.place("v").expect("a column v")];
        Column::decode(&chunk.bytes, chunk.at, &chunk.dir, sample.rows).expect("decodes")
    }

    /// Every file under `testdata/v3/`, read as compaction reads it and as a query does: the
    /// typed decoder prints every row as the cell `Reader::chunks` made.
    #[test]
    fn the_vectors_read_the_same_through_both_readers() {
        let (mut row_groups, mut cells_held) = (0, 0);
        for path in samples::vector_files() {
            let bytes = std::fs::read(&path).expect("read");
            let index = match open_sealed(&path).expect("a vector opens") {
                Some(index) => index,
                None => walk(&path, &bytes).expect("a vector walks").index,
            };
            let mut reader = Reader::open(&path).expect("open for reading");
            for entry in &index.entries {
                let columns = index.schemas[entry.schema_id as usize].len();
                let header = reader.header(entry, columns).expect("header");
                let all: Vec<usize> = (0..columns).collect();
                let (cells, read) = reader.chunks(entry, &header, &all).expect("chunks");
                let (stored, read_stored) = reader.stored(entry, &header, &all).expect("stored");
                assert_eq!(read_stored, read, "{}: the bytes read", path.display());
                assert_eq!(stored.len(), cells.len());
                row_groups += 1;
                for ((chunk, dir), cells) in stored.iter().zip(&header.chunks).zip(&cells) {
                    let from = chunk.at as usize;
                    assert_eq!(chunk.bytes[..], bytes[from..from + dir.stored_len as usize]);
                    let column = Column::decode(&chunk.bytes, chunk.at, dir, cells.len())
                        .expect("a vector's chunk decodes");
                    let col = column.over(&chunk.bytes).expect("over its own bytes");
                    let mut memo = Memo::default();
                    for (row, cell) in cells.iter().enumerate() {
                        assert_eq!(col.cell(row).as_ref(), Ok(cell), "{}", path.display());
                        assert_eq!(col.cell_with(row, &mut memo).as_ref(), Ok(cell));
                        cells_held += 1;
                    }
                }
            }
        }
        // The two-template vector whole and cut, and the encodings' file.
        assert!(
            row_groups >= 6 && cells_held > 100,
            "{row_groups} {cells_held}"
        );
    }

    /// Every layout, decoded both ways and held cell for cell - and the list of samples
    /// held to being every layout.
    #[test]
    fn every_layout_decodes_to_the_cells_the_reader_prints() {
        let mut pairs = BTreeSet::new();
        let mut compressions = BTreeSet::new();
        let mut layouts = BTreeSet::new();
        let mut shapes = BTreeSet::new();
        let (mut mixed, mut per_row, mut slotted, mut bare) = (0, 0, 0, 0);
        for sample in samples::all() {
            for (chunk, name) in sample.chunks.iter().zip(&sample.columns) {
                let cells = chunk_both(&chunk.bytes, chunk.at, &chunk.dir, sample.rows)
                    .unwrap_or_else(|bad| panic!("{}, `{name}`: {}", sample.name, bad.what));
                assert_eq!(cells.len(), sample.rows);

                compressions.insert(chunk.dir.compression);
                layouts.insert(chunk.dir.layout);
                for piece in [chunk.dir.a, chunk.dir.b].iter().flatten() {
                    pairs.insert((type_byte(piece.form.column_type()), piece.encoding));
                    mixed += usize::from(piece.form.kind() == JsonKind::Mixed);
                    per_row += usize::from(piece.form.per_row_digits());
                }
                let column = Column::decode(&chunk.bytes, chunk.at, &chunk.dir, sample.rows)
                    .expect("decodes");
                let col = column.over(&chunk.bytes).expect("over its own bytes");
                match col.slots() {
                    Some(slots) => {
                        slotted += 1;
                        assert_eq!(slots.len(), sample.rows);
                    }
                    None => bare += 1,
                }
                for piece in col.pieces().expect("pieces").iter().flatten() {
                    shapes.insert(match piece.values {
                        ValuesRef::Ints(_) => "ints",
                        ValuesRef::Floats(_) => "floats",
                        ValuesRef::Times(_) => "times",
                        ValuesRef::Bools(_) => "bools",
                        ValuesRef::Words { .. } => "words",
                        ValuesRef::Texts(_) => "texts",
                        ValuesRef::Fixed { .. } => "fixed",
                        ValuesRef::SameInt(_) => "same int",
                        ValuesRef::SameFloat(_) => "same float",
                        ValuesRef::SameTime(_) => "same time",
                        ValuesRef::SameBool(_) => "same bool",
                        ValuesRef::SameText(_) => "same text",
                        ValuesRef::SameFixed(_) => "same fixed",
                    });
                }
            }
        }
        // Plain and constant for each of the seven types, and each type's own encoding.
        let mut every = BTreeSet::new();
        for ty in 0..7u8 {
            every.insert((ty, PLAIN));
            every.insert((ty, CONSTANT));
        }
        every.extend([(0, DICTIONARY), (1, VARINT), (3, DELTA), (4, BITS)]);
        assert_eq!(pairs, every, "the type and encoding pairs the samples hold");
        assert_eq!(compressions, BTreeSet::from([NO_COMPRESSION, ZSTD]));
        assert_eq!(
            layouts,
            BTreeSet::from([0, SOME_NULL, TWO_PIECES, SOME_NULL | TWO_PIECES, ALL_NULL])
        );
        assert_eq!(shapes.len(), 13, "{shapes:?}");
        assert!(
            mixed > 3 && per_row > 2 && slotted > 3 && bare > 3,
            "{mixed} {per_row} {slotted} {bare}"
        );
    }

    /// What a column holds for a few chunks, by hand: the values as their encoding has them,
    /// a slot a row only where a row's number is not its value's.
    #[test]
    fn a_column_holds_what_its_chunk_lays_out() {
        let numbers = sample("integers, bare and quoted");
        let (column, chunk) = (decoded(&numbers), &numbers.chunks[1]);
        let col = column.over(&chunk.bytes).expect("over");
        assert_eq!((col.rows(), col.slots()), (7, None));
        let [Some(a), None] = col.pieces().expect("pieces") else {
            panic!("one piece");
        };
        assert!(
            matches!(a.values, ValuesRef::Ints(v) if v == [200, 404, 503, 200, -17, 0, 45]),
            "{:?}",
            a.values
        );
        let kinds: Vec<bool> = (0..a.n).map(|i| a.lit(i)).collect();
        assert_eq!(kinds, [true, true, false, true, true, true, true]);
        assert_eq!(
            a.cell(2),
            Ok((ColValue::Int64(503), Some(serde_json::json!("503"))))
        );
        assert!(a.cell(7).is_err(), "a value past the piece's last");

        // Two pieces and a null: piece A is the integers, piece B the one word.
        let beside = sample("integers beside text and nulls");
        let (column, chunk) = (decoded(&beside), &beside.chunks[1]);
        let col = column.over(&chunk.bytes).expect("over");
        let slots: Vec<Option<(bool, usize)>> = col
            .slots()
            .expect("a slot a row")
            .iter()
            .map(|slot| slot.get())
            .collect();
        assert_eq!(
            slots,
            [
                Some((false, 0)),
                Some((false, 1)),
                Some((false, 2)),
                Some((true, 0)),
                Some((true, 1)),
                None,
                Some((false, 3)),
            ]
        );
        let [Some(a), Some(b)] = col.pieces().expect("pieces") else {
            panic!("two pieces");
        };
        assert!(matches!(a.values, ValuesRef::Ints(v) if v == [7, 8, 9, 45]));
        assert!(matches!(b.values, ValuesRef::SameText(word) if word == b"N/A"));
        assert_eq!((a.n, b.n), (4, 2));
        assert_eq!(col.cell(5), Ok((ColValue::Null, None)));
        assert_eq!(
            b.text_cell(b"N/A", false),
            Ok((ColValue::String("N/A".into()), None))
        );

        // A word list: the words in the order rows first name them, a number a row.
        let words = sample("words");
        let (column, chunk) = (decoded(&words), &words.chunks[1]);
        let col = column.over(&chunk.bytes).expect("over");
        let [Some(a), None] = col.pieces().expect("pieces") else {
            panic!("one piece");
        };
        let ValuesRef::Words { words, codes } = &a.values else {
            panic!("a word list: {:?}", a.values);
        };
        assert_eq!(words, &[&b"GET"[..], b"POST", b"DELETE", b""]);
        assert_eq!(codes, &[0, 1, 0, 0, 2, 1, 0, 3]);

        // Ids are their sixteen stored bytes, and one id on every row is stored once.
        let ids = sample("ids");
        let (column, chunk) = (decoded(&ids), &ids.chunks[1]);
        let col = column.over(&chunk.bytes).expect("over");
        let [Some(a), None] = col.pieces().expect("pieces") else {
            panic!("one piece");
        };
        let ValuesRef::Fixed { bytes, width } = a.values else {
            panic!("stored bytes: {:?}", a.values);
        };
        assert_eq!(
            (bytes.len(), width, bytes[0], bytes[16]),
            (48, 16, 0x55, 0x6b)
        );
        let one = sample("one id");
        let (column, chunk) = (decoded(&one), &one.chunks[1]);
        let col = column.over(&chunk.bytes).expect("over");
        let [Some(a), None] = col.pieces().expect("pieces") else {
            panic!("one piece");
        };
        assert!(matches!(a.values, ValuesRef::SameFixed(id) if id.len() == 16 && id[0] == 0x55));

        // Nothing but nulls is no piece at all.
        let nulls = sample("nothing but nulls");
        let (column, chunk) = (decoded(&nulls), &nulls.chunks[1]);
        let col = column.over(&chunk.bytes).expect("over");
        assert!(matches!(col.pieces(), Ok([None, None])) && col.slots().is_none());
        assert_eq!(col.cell(3), Ok((ColValue::Null, None)));
    }

    /// A column reads over the bytes it was decoded from and over no others: the wrong
    /// bytes give an error or other text, never a panic.
    #[test]
    fn a_column_is_read_over_its_own_bytes() {
        for sample in samples::all() {
            for chunk in &sample.chunks {
                let column = Column::decode(&chunk.bytes, chunk.at, &chunk.dir, sample.rows)
                    .expect("decodes");
                let mut longer = chunk.bytes.clone();
                longer.push(0);
                let bad = column.over(&longer).expect_err("another length");
                assert_eq!(bad.offset, chunk.at);
                assert!(bad.what.contains("was put over"), "{}", bad.what);
                if !chunk.bytes.is_empty() {
                    assert!(column.over(&chunk.bytes[1..]).is_err());
                }
                let col = column.over(&chunk.bytes).expect("over its own bytes");
                let bad = col.cell(sample.rows).expect_err("a row past the last");
                assert!(bad.what.contains("of a chunk of"), "{}", bad.what);

                let other = vec![0xffu8; chunk.bytes.len()];
                let col = column.over(&other).expect("the same length");
                let mut memo = Memo::default();
                for row in 0..sample.rows {
                    let _ = col.cell(row);
                    let _ = col.cell_with(row, &mut memo);
                }
                let _ = col.pieces();
            }
        }
    }

    /// A value that does not print is refused when its row is printed, by the chunk's
    /// offset and the row; the rows beside it print, and decoding alone refuses nothing.
    #[test]
    fn a_value_that_does_not_print_is_refused_at_its_row() {
        let text = Form::String {
            kind: JsonKind::Str,
        };
        let mut body = 3u32.to_le_bytes().to_vec();
        body.extend_from_slice(b"abc");
        body.extend_from_slice(&2u32.to_le_bytes());
        body.extend_from_slice(&[0xff, 0xfe]);
        let dir = one_piece(text, PLAIN, &body);
        let refused = read::chunk(&body, 40, &dir, 2).expect_err("not UTF-8");
        assert!(refused.what.contains("not UTF-8"), "{}", refused.what);
        assert!(chunk_both(&body, 40, &dir, 2).is_err());

        let column = Column::decode(&body, 40, &dir, 2).expect("the layout holds");
        let col = column.over(&body).expect("over");
        assert_eq!(col.cell(0), Ok((ColValue::String("abc".into()), None)));
        let bad = col.cell(1).expect_err("not UTF-8");
        assert_eq!(bad.offset, 40);
        assert_eq!(
            bad.what,
            "a string value is not UTF-8, at row 1 of the chunk"
        );
    }

    // ── A broken layout ────────────────────────────────────────────────────────────

    /// The directory entry of a chunk of one piece, stored as it is.
    fn one_piece(form: Form, encoding: u8, body: &[u8]) -> ChunkDir {
        ChunkDir {
            layout: 0,
            compression: NO_COMPRESSION,
            stored_len: body.len() as u32,
            stored_crc: 0,
            body_len: body.len() as u32,
            a: Some(PieceDir { form, encoding }),
            b: None,
        }
    }

    /// A word list of these words, then these indexes, one byte each.
    fn words(words: &[&str], indexes: &[u8]) -> Vec<u8> {
        let mut out = (words.len() as u32).to_le_bytes().to_vec();
        for w in words {
            out.extend_from_slice(&(w.len() as u32).to_le_bytes());
            out.extend_from_slice(w.as_bytes());
        }
        out.extend_from_slice(indexes);
        out
    }

    /// Both readers refuse this chunk in the same words at the same offset, stored as it is
    /// and through zstd.
    fn refused_alike(body: &[u8], dir: &ChunkDir, rows: usize, why: &str) {
        const AT: u64 = 40;
        let read = read::chunk(body, AT, dir, rows).expect_err(why);
        let typed = Column::decode(body, AT, dir, rows).expect_err(why);
        assert_eq!(typed, read, "{why}");
        assert!(read.what.contains(why), "{why}: {}", read.what);
        assert!(read.offset >= AT, "{why}: offset {}", read.offset);

        let frame = zstd::bulk::compress(body, 3).expect("compress");
        let zdir = ChunkDir {
            compression: ZSTD,
            stored_len: frame.len() as u32,
            ..dir.clone()
        };
        let read = read::chunk(&frame, AT, &zdir, rows).expect_err(why);
        let typed = Column::decode(&frame, AT, &zdir, rows).expect_err(why);
        assert_eq!(typed, read, "{why}, through zstd");
        assert_eq!(read.offset, AT);
        if rows <= MAX_ROWS as usize {
            assert!(
                read.what.contains(why) && read.what.contains("of the chunk's body"),
                "{why}: {}",
                read.what
            );
        }
    }

    /// The typed decoder makes every check of the layout that `read::chunk` makes, in its
    /// words and at its offset, on chunks whose values would all print.
    #[test]
    fn a_broken_layout_is_refused_in_the_reader_s_words() {
        let text = Form::String {
            kind: JsonKind::Str,
        };
        let int = |kind| Form::Int64 {
            kind,
            plus: false,
            width: 0,
        };
        let float = |kind| Form::Float64 {
            kind,
            digits: PER_ROW,
        };
        let flag = Form::Bool {
            kind: JsonKind::Lit,
            case: 0,
        };
        // Nine fraction digits print any instant, a nanosecond after the epoch included.
        let date = Form::Datetime {
            kind: JsonKind::Str,
            unit: 0,
            separator: b'T',
            zone: b'Z',
            offset: 0,
            digits: 9,
        };
        let floats = |values: &[f64]| -> Vec<u8> {
            values
                .iter()
                .flat_map(|f| f.to_bits().to_le_bytes())
                .collect()
        };

        // A word list.
        for (body, rows, why) in [
            (words(&["a", "b"], &[0, 2, 1]), 3, "names word 2"),
            (words(&[], &[]), 1, "of 0 words"),
            (words(&["a", "b", "c"], &[0, 1]), 2, "of 3 words for 2 rows"),
            (words(&["a", "b"], &[0, 1, 1, 0]), 3, "left over"),
            (
                words(&["a", "b"], &[1, 0, 1]),
                3,
                "names word 1 before word 0",
            ),
            (words(&["a", "b", "c"], &[0, 1, 0]), 3, "whose rows name 2"),
            (words(&["a", "b"], &[0, 1]), 3, "dictionary indexes"),
            (vec![1, 0, 0, 0, 9, 0, 0, 0, b'a'], 1, "string wants 9"),
            (vec![1, 0], 1, "dictionary word count"),
        ] {
            refused_alike(&body, &one_piece(text, DICTIONARY, &body), rows, why);
        }
        // A word that stands for more than a body may hold, as a constant and in a list.
        let word = vec![b'x'; 1 << 20];
        let mut constant = (word.len() as u32).to_le_bytes().to_vec();
        constant.extend_from_slice(&word);
        refused_alike(
            &constant,
            &one_piece(text, CONSTANT, &constant),
            300,
            "past the 268435456",
        );
        let mut list = 1u32.to_le_bytes().to_vec();
        list.extend_from_slice(&constant);
        list.extend(std::iter::repeat_n(0u8, 300));
        refused_alike(
            &list,
            &one_piece(text, DICTIONARY, &list),
            300,
            "past the 268435456",
        );

        // Numbers, bits and differences.
        for (body, rows, why) in [
            (vec![0x80, 0x00], 1, "does not need"),
            ([vec![0xff; 9], vec![0x02]].concat(), 1, "64 bits"),
            (vec![], 1, "only 0 remain"),
            (vec![0x02, 0x04], 1, "left over"),
        ] {
            refused_alike(
                &body,
                &one_piece(int(JsonKind::Lit), VARINT, &body),
                rows,
                why,
            );
            refused_alike(&body, &one_piece(date, DELTA, &body), rows, why);
        }
        refused_alike(
            &[0b1000_0101],
            &one_piece(flag, BITS, &[0]),
            3,
            "past its last row",
        );
        refused_alike(&[], &one_piece(flag, BITS, &[]), 3, "Bool bits wants 1");
        refused_alike(&[1, 2], &one_piece(flag, PLAIN, &[1, 2]), 2, "Bool byte 2");
        refused_alike(
            &[1],
            &one_piece(flag, CONSTANT, &[1]),
            300_000_000,
            "past the 8192",
        );

        // The kind bitmap and the digits bytes.
        refused_alike(
            &[0b111, 2, 4, 6],
            &one_piece(int(JsonKind::Mixed), VARINT, &[0; 4]),
            3,
            "all one kind",
        );
        refused_alike(
            &[0b1001, 2, 4, 6],
            &one_piece(int(JsonKind::Mixed), VARINT, &[0; 4]),
            3,
            "kind bitmap has bits set past",
        );
        let numbers = floats(&[1.5, 2.5]);
        let spelled = [vec![0b01, 2, 1], numbers.clone()].concat();
        refused_alike(
            &spelled,
            &one_piece(float(JsonKind::Mixed), PLAIN, &spelled),
            2,
            "a JSON number with a digits byte",
        );
        let same = [vec![0b10, 1, 3], floats(&[1.5])].concat();
        refused_alike(
            &same,
            &one_piece(float(JsonKind::Mixed), CONSTANT, &same),
            2,
            "a JSON number with a digits byte",
        );
        refused_alike(
            &[1],
            &one_piece(float(JsonKind::Str), PLAIN, &[1]),
            2,
            "per-row digits wants 2",
        );

        // Nulls and a second piece.
        let nullable = |layout: u8, body: &[u8]| ChunkDir {
            layout,
            b: (layout & TWO_PIECES != 0).then_some(PieceDir {
                form: text,
                encoding: CONSTANT,
            }),
            ..one_piece(int(JsonKind::Lit), VARINT, body)
        };
        for (layout, body, why) in [
            (SOME_NULL, vec![0b11, 2, 4], "marks 2 of 2 present"),
            (SOME_NULL, vec![0b00], "marks 0 of 2 present"),
            (
                SOME_NULL,
                vec![0b101, 2],
                "presence bitmap has bits set past",
            ),
            (
                TWO_PIECES,
                vec![0b00, 2, 0, 0, 0, 2, 4],
                "a piece of two holds no rows",
            ),
            (
                TWO_PIECES,
                vec![0b11, 0, 0, 0, 0],
                "a piece of two holds no rows",
            ),
            (TWO_PIECES, vec![0b10, 9, 0, 0, 0, 2], "piece A wants 9"),
            (TWO_PIECES, vec![0b10, 1, 0], "piece_a_len"),
            (
                TWO_PIECES,
                vec![0b10, 1, 0, 0, 0, 2, 1, 0, 0, 0, b'a', 0],
                "left over",
            ),
        ] {
            refused_alike(&body, &nullable(layout, &body), 2, why);
        }

        // What only a zstd frame can break: both refuse it at the chunk, before a body.
        let body = words(&["a", "b"], &[0, 1, 1]);
        let frame = zstd::bulk::compress(&body, 3).expect("compress");
        let len = body.len() as u32;
        let mut longer = frame.clone();
        longer.push(0);
        for (stored, body_len, why) in [
            (longer, len, "after the chunk's zstd frame"),
            (frame.clone(), len + 1, "where body_len says"),
            (
                frame.clone(),
                len - 1,
                "does not decompress to its body_len",
            ),
            (body.clone(), len, "not a zstd frame"),
        ] {
            let dir = ChunkDir {
                compression: ZSTD,
                stored_len: stored.len() as u32,
                body_len,
                ..one_piece(text, DICTIONARY, &body)
            };
            let read = read::chunk(&stored, 40, &dir, 3).expect_err(why);
            assert_eq!(Column::decode(&stored, 40, &dir, 3).expect_err(why), read);
            assert!(read.what.contains(why), "{why}: {}", read.what);
        }
    }

    /// The typed decoder reads an encoding only on the type that has it, as the header
    /// refuses any other pair where the directory names it.
    #[test]
    fn an_encoding_on_another_type_is_refused_as_the_header_refuses_it() {
        let int = Form::Int64 {
            kind: JsonKind::Lit,
            plus: false,
            width: 0,
        };
        let mut body = 1u32.to_le_bytes().to_vec();
        body.extend_from_slice(&7i64.to_le_bytes());
        body.push(0);
        for encoding in [DICTIONARY, DELTA, BITS, 6, 0xff] {
            let dir = one_piece(int, encoding, &body);
            let bad = Column::decode(&body, 40, &dir, 1).expect_err("another type's encoding");
            assert_eq!(bad.offset, 40);
            assert_eq!(
                bad.what,
                format!("encoding {encoding} on a Int64 piece, which this build does not read")
            );
            assert!(!encodes(int.column_type(), encoding));
        }
    }

    /// Every chunk of the samples with a bit changed, a byte cut off or added: both readers
    /// take the same ones, refuse the same ones, and print the same cells.
    #[test]
    fn every_edit_of_a_body_is_taken_or_refused_by_both_readers() {
        let (mut taken, mut refused, mut chunks) = (0u32, 0u32, 0u32);
        let mut both = |body: &[u8], dir: &ChunkDir, rows: usize, zstd: bool| {
            let plain = ChunkDir {
                compression: NO_COMPRESSION,
                stored_len: body.len() as u32,
                body_len: body.len() as u32,
                ..dir.clone()
            };
            match chunk_both(body, 40, &plain, rows) {
                Ok(_) => taken += 1,
                Err(_) => refused += 1,
            }
            if zstd {
                let frame = zstd::bulk::compress(body, 3).expect("compress");
                let dir = ChunkDir {
                    compression: ZSTD,
                    stored_len: frame.len() as u32,
                    ..plain
                };
                let _ = chunk_both(&frame, 40, &dir, rows);
            }
        };
        for sample in samples::all() {
            for chunk in &sample.chunks {
                let body = match chunk.dir.compression {
                    ZSTD => zstd::bulk::decompress(&chunk.bytes, chunk.dir.body_len as usize)
                        .expect("a sample's frame"),
                    _ => chunk.bytes.clone(),
                };
                if body.is_empty() || body.len() > 160 {
                    continue;
                }
                chunks += 1;
                for at in 0..body.len() {
                    for bit in 0..8 {
                        let mut edited = body.clone();
                        edited[at] ^= 1 << bit;
                        both(&edited, &chunk.dir, sample.rows, (at + bit) % 8 == 0);
                    }
                }
                both(&body[..body.len() - 1], &chunk.dir, sample.rows, true);
                let mut longer = body.clone();
                longer.push(0);
                both(&longer, &chunk.dir, sample.rows, true);
                // One row more and one fewer than the body was laid out for.
                both(&body, &chunk.dir, sample.rows + 1, true);
                both(&body, &chunk.dir, sample.rows - 1, true);
            }
        }
        // Not agreement on nothing: thousands of edits read, and thousands refused.
        assert!(
            chunks > 60 && taken > 2_000 && refused > 2_000,
            "{chunks} chunks, {taken} taken, {refused} refused"
        );
    }
}
