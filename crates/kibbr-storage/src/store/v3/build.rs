// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The segment builder: rows of one template in, one encoded row group out.
//! It has no clock, no lock and no knowledge of its caller, so the live door, compaction
//! and the bulk loader share one encoder and one set of encoder bugs.
//! How a column is split into pieces and how a piece is laid out are
//! fixed, so the same rows always make the same bytes.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};

use serde_json::Value;
use thiserror::Error;

use super::{
    bitmap, day_of, epoch_of, float_json, index_width, ip_bytes, print_float, print_int, print_ip,
    print_rfc3339, print_uuid, put_varint, type_byte, uuid_bytes, zigzag, Form, JsonKind, Source,
    BITS, CONSTANT, DELTA, DICTIONARY, EPOCH_MILLIS, EPOCH_SECS, KIND_ROW_GROUP, MAX_BODY,
    MAX_ROWS, NO_COMPRESSION, PER_ROW, PLAIN, RFC3339, VARINT, ZSTD,
};
use crate::store::Row;
use crate::core::types::{ColValue, ColumnType};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum BuildError {
    #[error("a row group needs at least one row")]
    NoRows,
    #[error("{0} rows is more than the {max} a row group holds", max = MAX_ROWS)]
    TooManyRows(usize),
    #[error("{rows} row(s) and {times} effective time(s)")]
    Times { rows: usize, times: usize },
    #[error("a row group's rows must share one UTC day: {min} and {max} do not")]
    TwoDays { min: i64, max: i64 },
    #[error("schema column names must be sorted and distinct: `{0}` is out of place")]
    Schema(String),
    #[error("column `{0}` holds more than a u32 length can describe")]
    TooLong(String),
    #[error("column `{0}`: a typed piece was handed a value its group did not type")]
    Piece(String),
    #[error("column `{0}`: zstd: {1}")]
    Zstd(String, String),
}

/// How hard zstd works on a row group's chunks: fast where the live door closes one, harder
/// where a file is written whole and then read for months.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    Live,
    Finished,
}

impl Level {
    pub fn zstd(self) -> i32 {
        match self {
            Level::Live => 3,
            Level::Finished => 9,
        }
    }
}

/// One encoded row group: the header frame and the chunk area that follows it.
#[derive(Debug, Clone, PartialEq)]
pub struct RowGroup {
    /// The whole frame: length, CRC, payload.
    pub frame: Vec<u8>,
    pub chunks: Vec<u8>,
    pub row_count: u32,
    pub min_time: i64,
    pub max_time: i64,
}

/// The sorted, distinct keys of these rows: the schema a row group of them uses.
pub fn schema_of<'a>(rows: impl IntoIterator<Item = &'a Row>) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    for row in rows {
        for key in row.keys() {
            if let Err(at) = names.binary_search(key) {
                names.insert(at, key.clone());
            }
        }
    }
    names
}

/// Encode `rows` - every one of template `template_id`, all on one UTC day by `times` -
/// as one row group over `columns`, the schema numbered `schema_id` in its file.
pub fn build(
    template_id: u64,
    schema_id: u32,
    columns: &[String],
    rows: &[&Row],
    times: &[i64],
    source: Source,
    level: Level,
) -> Result<RowGroup, BuildError> {
    Ok(encode(template_id, columns, rows, times, source, level)?.frame(schema_id))
}

/// A row group with every column encoded and no schema number yet, split off `build` so a
/// flush can encode on many threads and number them in file order.
#[derive(Debug, Clone, PartialEq)]
pub struct Encoded {
    template_id: u64,
    row_count: u32,
    min_time: i64,
    max_time: i64,
    source: Source,
    directory: Vec<u8>,
    chunks: Vec<u8>,
}

/// The expensive half of `build`: every check, and every column encoded.
pub fn encode(
    template_id: u64,
    columns: &[String],
    rows: &[&Row],
    times: &[i64],
    source: Source,
    level: Level,
) -> Result<Encoded, BuildError> {
    if rows.is_empty() {
        return Err(BuildError::NoRows);
    }
    // What a reader refuses is refused here, before it is written: a file with more rows
    // to a row group is one no start could read.
    let row_count = u32::try_from(rows.len())
        .ok()
        .filter(|n| *n <= MAX_ROWS)
        .ok_or(BuildError::TooManyRows(rows.len()))?;
    if times.len() != rows.len() {
        return Err(BuildError::Times {
            rows: rows.len(),
            times: times.len(),
        });
    }
    for pair in columns.windows(2) {
        if pair[0] >= pair[1] {
            return Err(BuildError::Schema(pair[1].clone()));
        }
    }
    let min_time = times.iter().copied().min().unwrap_or_default();
    let max_time = times.iter().copied().max().unwrap_or_default();
    if day_of(min_time) != day_of(max_time) {
        return Err(BuildError::TwoDays {
            min: min_time,
            max: max_time,
        });
    }

    let mut directory = Vec::new();
    let mut chunks = Vec::new();
    for name in columns {
        let chunk = encode_column(name, rows, level)?;
        directory.extend_from_slice(&chunk.entry);
        chunks.extend_from_slice(&chunk.body);
    }
    u32::try_from(chunks.len()).map_err(|_| BuildError::TooLong(columns.join(",")))?;
    Ok(Encoded {
        template_id,
        row_count,
        min_time,
        max_time,
        source,
        directory,
        chunks,
    })
}

impl Encoded {
    /// The row group whole, as schema `schema_id` of its file; the CRC covers the header
    /// alone, so framing is cheap however large the chunks are.
    pub fn frame(self, schema_id: u32) -> RowGroup {
        let (source_byte, stream, first, last) = self.source.bytes();
        let mut payload = Vec::with_capacity(64 + self.directory.len());
        payload.push(KIND_ROW_GROUP);
        payload.extend_from_slice(&self.template_id.to_le_bytes());
        payload.extend_from_slice(&schema_id.to_le_bytes());
        payload.extend_from_slice(&self.row_count.to_le_bytes());
        payload.extend_from_slice(&self.min_time.to_le_bytes());
        payload.extend_from_slice(&self.max_time.to_le_bytes());
        payload.push(source_byte);
        payload.extend_from_slice(&stream.to_le_bytes());
        payload.extend_from_slice(&first.to_le_bytes());
        payload.extend_from_slice(&last.to_le_bytes());
        // Checked against a `u32` when it was encoded.
        payload.extend_from_slice(&(self.chunks.len() as u32).to_le_bytes());
        payload.extend_from_slice(&self.directory);
        // No sections: bloom filters are not built yet, and a file without them simply has
        // none.
        payload.extend_from_slice(&0u16.to_le_bytes());

        RowGroup {
            frame: crate::frame::frame(&payload),
            chunks: self.chunks,
            row_count: self.row_count,
            min_time: self.min_time,
            max_time: self.max_time,
        }
    }
}

// ── One column ─────────────────────────────────────────────────────────────────────

struct Chunk {
    entry: Vec<u8>,
    body: Vec<u8>,
}

/// A typed value, in the layout plain encoding writes.
#[derive(Debug, Clone)]
enum Val {
    Int(i64),
    Float(f64),
    Time(i64),
    Bool(bool),
    Uuid([u8; 16]),
    Ip(Vec<u8>),
}

/// Which group a value belongs in: the type byte, then the form fields that describe its
/// spelling; ordered, so ties between groups go to the lower bytes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct GroupKey {
    ty: u8,
    key: Vec<u8>,
}

impl GroupKey {
    fn text() -> Self {
        Self {
            ty: type_byte(ColumnType::String),
            key: Vec::new(),
        }
    }

    fn is_text(&self) -> bool {
        self.ty == type_byte(ColumnType::String)
    }

    /// The group a JSON number of this family starts when none of its type is there to
    /// join: every spelling field zero, which is what `json_kind` 1 requires.
    fn default_for(family: (u8, u8)) -> Self {
        let (ty, unit) = family;
        let key = match ty {
            1 => vec![0, 0],
            3 => vec![unit, 0, 0, 0, 0],
            4 => vec![0],
            _ => Vec::new(),
        };
        Self { ty, key }
    }

    /// The type and, for a `Datetime`, the unit: what a JSON number may join.
    fn family(&self) -> (u8, u8) {
        let unit = if self.ty == 3 { self.key[0] } else { 0 };
        (self.ty, unit)
    }
}

/// Where one present value goes.
enum Class {
    /// Printed back exactly by a form of `key`'s group. `lit` for a JSON number or bool,
    /// which joins a group later by `family`.
    Typed {
        key: Option<GroupKey>,
        family: (u8, u8),
        digits: u8,
        val: Val,
    },
    Text,
}

struct Present<'a> {
    /// The row's index in the row group.
    row: usize,
    json: &'a Value,
    lit: bool,
    class: Class,
}

fn encode_column(name: &str, rows: &[&Row], level: Level) -> Result<Chunk, BuildError> {
    // The JSON values, held for the rows that have one; `printed` owns the ones a row
    // does not record separately, so a borrow of either lives as long as this call.
    let mut printed: Vec<Option<Value>> = Vec::with_capacity(rows.len());
    for row in rows {
        printed.push(match row.get(name) {
            None | Some(ColValue::Null) => None,
            Some(value) => match row.written(name) {
                Some(_) => None,
                None => Some(value.printed()),
            },
        });
    }
    let mut present: Vec<Present<'_>> = Vec::new();
    for (i, row) in rows.iter().enumerate() {
        let Some(value) = row.get(name) else { continue };
        if matches!(value, ColValue::Null) {
            continue;
        }
        let Some(json) = row.written(name).or(printed[i].as_ref()) else {
            continue;
        };
        let lit = matches!(json, Value::Number(_) | Value::Bool(_));
        let class = classify(value, json);
        present.push(Present {
            row: i,
            json,
            lit,
            class,
        });
    }

    if present.is_empty() {
        // Every row null: layout bit 2, no pieces and no bytes.
        let mut entry = vec![0b100, NO_COMPRESSION];
        entry.extend_from_slice(&0u32.to_le_bytes());
        entry.extend_from_slice(&0u32.to_le_bytes());
        return Ok(Chunk {
            entry,
            body: Vec::new(),
        });
    }

    let groups = group(&present);
    let pieces = choose(groups, &present);

    // The body: presence, selector and piece A's length where they apply, then the pieces.
    let some_null = present.len() < rows.len();
    let two = pieces.len() == 2;
    let mut body = Vec::new();
    if some_null {
        let mut have = vec![false; rows.len()];
        for p in &present {
            have[p.row] = true;
        }
        body.extend(bitmap(have.into_iter()));
    }
    let mut encoded = Vec::with_capacity(pieces.len());
    for piece in &pieces {
        encoded.push(encode_piece(name, piece, &present)?);
    }
    if two {
        let mut in_b = vec![false; present.len()];
        for at in &pieces[1].members {
            in_b[*at] = true;
        }
        body.extend(bitmap(in_b.into_iter()));
        let a_len =
            u32::try_from(encoded[0].2.len()).map_err(|_| BuildError::TooLong(name.to_string()))?;
        body.extend_from_slice(&a_len.to_le_bytes());
    }
    let mut forms = Vec::new();
    for (form, encoding, bytes) in encoded {
        body.extend_from_slice(&bytes);
        forms.push((form, encoding));
    }

    // zstd once over the whole body, bitmaps included: one frame over a chunk compresses
    // better than one per piece, and is one call to decode.
    let compression = compression_of(
        forms
            .iter()
            .map(|(form, encoding)| (form.column_type(), *encoding)),
    );
    let body_len = u32::try_from(body.len())
        .ok()
        .filter(|len| *len <= MAX_BODY)
        .ok_or_else(|| BuildError::TooLong(name.to_string()))?;
    let stored = match compression {
        ZSTD => zstd::bulk::compress(&body, level.zstd())
            .map_err(|e| BuildError::Zstd(name.to_string(), e.to_string()))?,
        _ => body,
    };
    let stored_len =
        u32::try_from(stored.len()).map_err(|_| BuildError::TooLong(name.to_string()))?;
    let layout = u8::from(some_null) | (u8::from(two) << 1);
    let mut entry = vec![layout, compression];
    entry.extend_from_slice(&stored_len.to_le_bytes());
    entry.extend_from_slice(&crate::frame::crc32c(&stored).to_le_bytes());
    if compression != NO_COMPRESSION {
        entry.extend_from_slice(&body_len.to_le_bytes());
    }
    for (form, encoding) in forms {
        entry.push(type_byte(form.column_type()));
        entry.push(encoding);
        entry.extend(form.bytes());
    }
    Ok(Chunk {
        entry,
        body: stored,
    })
}

/// The one compression a chunk gets: zstd over the whole body, unless every piece is one
/// zstd has nothing to do for - a constant, bits, or a `Uuid`'s random bytes.
fn compression_of(pieces: impl IntoIterator<Item = (ColumnType, u8)>) -> u8 {
    let raw = |(ty, encoding): (ColumnType, u8)| match encoding {
        CONSTANT | BITS => true,
        PLAIN => ty == ColumnType::Uuid,
        _ => false,
    };
    if pieces.into_iter().all(raw) {
        NO_COMPRESSION
    } else {
        ZSTD
    }
}

/// A piece: its group, and the positions in `present` of the rows it holds, in order.
struct Piece {
    key: GroupKey,
    members: Vec<usize>,
}

/// Every present value into a group, with the JSON numbers joining theirs.
fn group(present: &[Present<'_>]) -> BTreeMap<GroupKey, Vec<usize>> {
    let mut groups: BTreeMap<GroupKey, Vec<usize>> = BTreeMap::new();
    let mut floating: BTreeMap<(u8, u8), Vec<usize>> = BTreeMap::new();
    for (at, p) in present.iter().enumerate() {
        match &p.class {
            Class::Typed { key: Some(key), .. } => groups.entry(key.clone()).or_default().push(at),
            Class::Typed {
                key: None, family, ..
            } => floating.entry(*family).or_default().push(at),
            Class::Text => groups.entry(GroupKey::text()).or_default().push(at),
        }
    }
    for (family, members) in floating {
        // The largest group of the family, measured before any number joins; ties to
        // the lower form bytes, which is the map's order.
        let mut best: Option<(&GroupKey, usize)> = None;
        for (key, rows) in &groups {
            if key.is_text() || key.family() != family {
                continue;
            }
            if best.is_none_or(|(_, n)| rows.len() > n) {
                best = Some((key, rows.len()));
            }
        }
        let key = best.map_or_else(|| GroupKey::default_for(family), |(key, _)| key.clone());
        let slot = groups.entry(key).or_default();
        slot.extend(members);
        slot.sort_unstable();
    }
    groups
}

/// At most two pieces out of the groups.
fn choose(groups: BTreeMap<GroupKey, Vec<usize>>, present: &[Present<'_>]) -> Vec<Piece> {
    let mut all: Vec<(Vec<u8>, Piece)> = groups
        .into_iter()
        .map(|(key, members)| {
            let piece = Piece { key, members };
            let form = form_of(&piece, present);
            let mut order = vec![piece.key.ty];
            order.extend(form.bytes());
            (order, piece)
        })
        .collect();
    // The largest first; among equals the lower type number, then the lower form bytes.
    all.sort_by(|(a_order, a), (b_order, b)| {
        b.members
            .len()
            .cmp(&a.members.len())
            .then_with(|| a_order.cmp(b_order))
    });
    let mut all: Vec<Piece> = all.into_iter().map(|(_, piece)| piece).collect();
    let mut rest = all.split_off(1);
    let mut a = all.pop().unwrap_or(Piece {
        key: GroupKey::text(),
        members: Vec::new(),
    });
    match rest.len() {
        0 => vec![a],
        1 => vec![a, rest.remove(0)],
        _ => {
            let mut text: Vec<usize> = rest.into_iter().flat_map(|p| p.members).collect();
            if a.key.is_text() {
                a.members.append(&mut text);
                a.members.sort_unstable();
                vec![a]
            } else {
                text.sort_unstable();
                vec![
                    a,
                    Piece {
                        key: GroupKey::text(),
                        members: text,
                    },
                ]
            }
        }
    }
}

fn digits_of(p: &Present<'_>) -> u8 {
    match &p.class {
        Class::Typed { digits, .. } => *digits,
        Class::Text => 0,
    }
}

/// The form a piece is written with: its group's spelling fields, the JSON kinds its rows
/// had, and one digit count for every string row or one byte per row when they differ.
fn form_of(piece: &Piece, present: &[Present<'_>]) -> Form {
    let rows = piece.members.iter().map(|at| &present[*at]);
    let literals = rows.clone().filter(|p| p.lit).count();
    let kind = JsonKind::of_counts(piece.members.len() - literals, literals);
    let digits = {
        let mut counts = rows.filter(|p| !p.lit).map(digits_of);
        match counts.next() {
            None => 0,
            Some(first) if counts.all(|d| d == first) => first,
            Some(_) => PER_ROW,
        }
    };
    let key = &piece.key.key;
    match piece.key.ty {
        1 => Form::Int64 {
            kind,
            plus: key[0] == 1,
            width: key[1],
        },
        2 => Form::Float64 { kind, digits },
        3 => Form::Datetime {
            kind,
            unit: key[0],
            separator: key[1],
            zone: key[2],
            offset: u16::from_le_bytes([key[3], key[4]]),
            digits,
        },
        4 => Form::Bool { kind, case: key[0] },
        5 => Form::Uuid { case: key[0] },
        6 => Form::Ip { spelling: key[0] },
        _ => Form::String { kind },
    }
}

/// A piece's form, its encoding and its bytes: the kind bitmap, the per-row digits, then
/// the values in that encoding's layout.
fn encode_piece(
    name: &str,
    piece: &Piece,
    present: &[Present<'_>],
) -> Result<(Form, u8, Vec<u8>), BuildError> {
    let rows: Vec<&Present<'_>> = piece.members.iter().map(|at| &present[*at]).collect();
    let form = form_of(piece, present);
    let mut out = Vec::new();
    if form.kind() == JsonKind::Mixed {
        out.extend(bitmap(rows.iter().map(|p| p.lit)));
    }
    if form.per_row_digits() {
        out.extend(rows.iter().map(|p| if p.lit { 0 } else { digits_of(p) }));
    }
    let values = Values::of(&form, &rows).ok_or_else(|| BuildError::Piece(name.to_string()))?;
    let encoding = values.encoding();
    let too_long = || BuildError::TooLong(name.to_string());
    match (encoding, &values) {
        (CONSTANT, _) => values.plain(0..1, &mut out).ok_or_else(too_long)?,
        (DICTIONARY, Values::Text(texts)) => dictionary(texts, &mut out).ok_or_else(too_long)?,
        (VARINT, Values::Int(ints)) => ints.iter().for_each(|i| put_varint(zigzag(*i), &mut out)),
        (DELTA, Values::Time(times)) => delta(times, &mut out),
        (BITS, Values::Bool(bools)) => out.extend(bitmap(bools.iter().copied())),
        _ => values
            .plain(0..values.len(), &mut out)
            .ok_or_else(too_long)?,
    }
    Ok((form, encoding, out))
}

/// A piece's values, as the type its form names: what an encoding lays out.
enum Values<'a> {
    Text(Vec<Cow<'a, str>>),
    Int(Vec<i64>),
    Float(Vec<f64>),
    Time(Vec<i64>),
    Bool(Vec<bool>),
    Uuid(Vec<[u8; 16]>),
    Ip(Vec<&'a [u8]>),
}

impl<'a> Values<'a> {
    /// The rows' values; a row reaching a String piece from a typed group is text here, so
    /// the text fallback is decided in one place.
    fn of(form: &Form, rows: &[&'a Present<'_>]) -> Option<Self> {
        macro_rules! all {
            ($variant:ident, $pat:pat => $out:expr) => {
                Values::$variant(
                    rows.iter()
                        .map(|p| match &p.class {
                            Class::Typed { val: $pat, .. } => Some($out),
                            _ => None,
                        })
                        .collect::<Option<_>>()?,
                )
            };
        }
        Some(match form {
            Form::String { .. } => Values::Text(
                rows.iter()
                    .map(|p| match p.json {
                        Value::String(s) => Cow::Borrowed(s.as_str()),
                        other => Cow::Owned(other.to_string()),
                    })
                    .collect(),
            ),
            Form::Int64 { .. } => all!(Int, Val::Int(i) => *i),
            Form::Float64 { .. } => all!(Float, Val::Float(f) => *f),
            Form::Datetime { .. } => all!(Time, Val::Time(t) => *t),
            Form::Bool { .. } => all!(Bool, Val::Bool(b) => *b),
            Form::Uuid { .. } => all!(Uuid, Val::Uuid(u) => *u),
            Form::Ip { .. } => all!(Ip, Val::Ip(bytes) => bytes.as_slice()),
        })
    }

    fn len(&self) -> usize {
        match self {
            Values::Text(v) => v.len(),
            Values::Int(v) | Values::Time(v) => v.len(),
            Values::Float(v) => v.len(),
            Values::Bool(v) => v.len(),
            Values::Uuid(v) => v.len(),
            Values::Ip(v) => v.len(),
        }
    }

    /// Every row the same value, byte for byte in plain layout - so `-0.0` is not `0.0`.
    fn constant(&self) -> bool {
        fn same<T: PartialEq>(v: &[T]) -> bool {
            v.windows(2).all(|w| w[0] == w[1])
        }
        match self {
            Values::Text(v) => same(v),
            Values::Int(v) | Values::Time(v) => same(v),
            Values::Float(v) => v.windows(2).all(|w| w[0].to_bits() == w[1].to_bits()),
            Values::Bool(v) => same(v),
            Values::Uuid(v) => same(v),
            Values::Ip(v) => same(v),
        }
    }

    /// The one encoding a piece gets: a constant stored once, anything else laid out the way
    /// its type is - never several tried and the smallest kept.
    fn encoding(&self) -> u8 {
        if self.constant() {
            return CONSTANT;
        }
        match self {
            Values::Text(_) => DICTIONARY,
            Values::Int(_) => VARINT,
            Values::Time(_) => DELTA,
            Values::Bool(_) => BITS,
            _ => PLAIN,
        }
    }

    /// The values at `range` in plain layout. `None` for a string past a `u32` length.
    fn plain(&self, range: std::ops::Range<usize>, out: &mut Vec<u8>) -> Option<()> {
        match self {
            Values::Text(v) => {
                for text in &v[range] {
                    out.extend_from_slice(&u32::try_from(text.len()).ok()?.to_le_bytes());
                    out.extend_from_slice(text.as_bytes());
                }
            }
            Values::Int(v) | Values::Time(v) => {
                v[range]
                    .iter()
                    .for_each(|i| out.extend_from_slice(&i.to_le_bytes()));
            }
            Values::Float(v) => v[range]
                .iter()
                .for_each(|f| out.extend_from_slice(&f.to_bits().to_le_bytes())),
            Values::Bool(v) => out.extend(v[range].iter().map(|b| u8::from(*b))),
            Values::Uuid(v) => v[range].iter().for_each(|u| out.extend_from_slice(u)),
            Values::Ip(v) => v[range].iter().for_each(|ip| out.extend_from_slice(ip)),
        }
        Some(())
    }
}

/// A word list: the distinct words in the order they first appear, in plain
/// layout, then each row's word by its number. `None` for a word past a `u32` length.
fn dictionary(texts: &[Cow<'_, str>], out: &mut Vec<u8>) -> Option<()> {
    let mut numbers: HashMap<&str, u32> = HashMap::new();
    let mut words: Vec<&str> = Vec::new();
    let indexes: Vec<u32> = texts
        .iter()
        .map(|text| {
            *numbers.entry(text.as_ref()).or_insert_with(|| {
                words.push(text.as_ref());
                // At most one word per row, and a row group's rows fit a `u32`.
                (words.len() - 1) as u32
            })
        })
        .collect();
    out.extend_from_slice(&u32::try_from(words.len()).ok()?.to_le_bytes());
    for word in &words {
        out.extend_from_slice(&u32::try_from(word.len()).ok()?.to_le_bytes());
        out.extend_from_slice(word.as_bytes());
    }
    let width = index_width(words.len());
    for index in indexes {
        out.extend_from_slice(&index.to_le_bytes()[..width]);
    }
    Some(())
}

/// Differences: the first value, then each value minus the one before it, wrapping
/// in i64, each zigzag and LEB128. Times a few seconds apart are a few bytes each.
fn delta(values: &[i64], out: &mut Vec<u8>) {
    let mut before = 0i64;
    for value in values {
        put_varint(zigzag(value.wrapping_sub(before)), out);
        before = *value;
    }
}

// ── Classifying one value ──────────────────────────────────────────────────────────

/// Which group a value can go in, having printed it through that group's form and got
/// back exactly what was ingested. Anything else is text.
fn classify(value: &ColValue, json: &Value) -> Class {
    match json {
        Value::String(s) => string_row(value, s),
        Value::Number(_) | Value::Bool(_) => literal_row(value, json),
        _ => Class::Text,
    }
}

fn typed(ty: u8, key: Vec<u8>, digits: u8, val: Val) -> Class {
    let key = GroupKey { ty, key };
    let family = key.family();
    Class::Typed {
        key: Some(key),
        family,
        digits,
        val,
    }
}

fn string_row(value: &ColValue, s: &str) -> Class {
    let checked = |ok: bool, class: Class| if ok { class } else { Class::Text };
    match value {
        ColValue::Int64(i) => {
            let (plus, digits) = match s.as_bytes().first() {
                Some(b'+') => (true, &s[1..]),
                Some(b'-') => (false, &s[1..]),
                _ => (false, s),
            };
            let padded = digits.len() > 1 && digits.starts_with('0');
            let Ok(width) = u8::try_from(if padded { digits.len() } else { 0 }) else {
                return Class::Text;
            };
            checked(
                print_int(*i, plus, width) == s,
                typed(1, vec![u8::from(plus), width], 0, Val::Int(*i)),
            )
        }
        ColValue::Float64(f) => {
            let digits = s.split_once('.').map_or(0, |(_, frac)| frac.len());
            let Ok(digits) = u8::try_from(digits) else {
                return Class::Text;
            };
            checked(
                digits < PER_ROW && f.is_finite() && print_float(*f, digits) == s,
                typed(2, Vec::new(), digits, Val::Float(*f)),
            )
        }
        ColValue::Datetime(ns) => {
            if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) {
                let unit = match s.len() {
                    10 => EPOCH_SECS,
                    13 => EPOCH_MILLIS,
                    _ => return Class::Text,
                };
                return checked(
                    epoch_of(*ns, unit).is_some_and(|n| n.to_string() == s),
                    typed(3, vec![unit, 0, 0, 0, 0], 0, Val::Time(*ns)),
                );
            }
            let Some((separator, zone, offset, digits)) = rfc3339_spelling(s) else {
                return Class::Text;
            };
            let o = offset.to_le_bytes();
            checked(
                print_rfc3339(*ns, separator, zone, offset, digits).as_deref() == Some(s),
                typed(
                    3,
                    vec![RFC3339, separator, zone, o[0], o[1]],
                    digits,
                    Val::Time(*ns),
                ),
            )
        }
        ColValue::Bool(b) => {
            let case = match s {
                "true" | "false" => 0,
                "True" | "False" => 1,
                _ => return Class::Text,
            };
            checked(
                (s == "true" || s == "True") == *b,
                typed(4, vec![case], 0, Val::Bool(*b)),
            )
        }
        ColValue::String(_) => {
            if let Some(bytes) = uuid_bytes(s) {
                let upper = s.bytes().any(|b| b.is_ascii_uppercase());
                return checked(
                    print_uuid(&bytes, upper) == s,
                    typed(5, vec![u8::from(upper)], 0, Val::Uuid(bytes)),
                );
            }
            if let Some(bytes) = ip_bytes(s) {
                let spellings: &[u8] = if bytes.len() == 4 {
                    &[4]
                } else {
                    &[6, 7, 8, 9]
                };
                for spelling in spellings {
                    if print_ip(&bytes, *spelling).as_deref() == Some(s) {
                        return typed(6, vec![*spelling], 0, Val::Ip(bytes));
                    }
                }
            }
            Class::Text
        }
        ColValue::Null => Class::Text,
    }
}

/// The separator, zone, offset and fraction digits an RFC 3339 text is spelled with. The
/// print check afterwards is what decides; this only reads the spelling off.
fn rfc3339_spelling(s: &str) -> Option<(u8, u8, u16, u8)> {
    let b = s.as_bytes();
    if b.len() < 20 || !s.is_ascii() {
        return None;
    }
    let separator = b[10];
    let (zone, offset, body_end) = match b[b.len() - 1] {
        z @ (b'Z' | b'z') => (z, 0u16, b.len() - 1),
        _ => {
            let tail = &s[s.len() - 6..];
            let sign = tail.as_bytes()[0];
            if !matches!(sign, b'+' | b'-') || tail.as_bytes()[3] != b':' {
                return None;
            }
            let hours: u16 = tail[1..3].parse().ok()?;
            let minutes: u16 = tail[4..6].parse().ok()?;
            (sign, hours * 60 + minutes, b.len() - 6)
        }
    };
    let digits = match b.get(19) {
        Some(b'.') => body_end.checked_sub(20)?,
        _ => 0,
    };
    Some((separator, zone, offset, u8::try_from(digits).ok()?))
}

fn literal_row(value: &ColValue, json: &Value) -> Class {
    let family = |ty: u8, unit: u8, val: Val| Class::Typed {
        key: None,
        family: (ty, unit),
        digits: 0,
        val,
    };
    match value {
        ColValue::Int64(i) if json.as_i64() == Some(*i) && json.is_i64() => {
            family(1, 0, Val::Int(*i))
        }
        ColValue::Float64(f) if float_json(*f).as_ref() == Some(json) => {
            family(2, 0, Val::Float(*f))
        }
        ColValue::Datetime(ns) => {
            let unit = match json.to_string().len() {
                10 => EPOCH_SECS,
                13 => EPOCH_MILLIS,
                _ => return Class::Text,
            };
            match epoch_of(*ns, unit) {
                Some(n) if json.is_i64() && json.as_i64() == Some(n) => {
                    family(3, unit, Val::Time(*ns))
                }
                _ => Class::Text,
            }
        }
        ColValue::Bool(b) if Value::Bool(*b) == *json => family(4, 0, Val::Bool(*b)),
        _ => Class::Text,
    }
}
