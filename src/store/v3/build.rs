//! **The segment builder**: rows of one template in, one encoded row group out.
//!
//! It has no clock, no lock and no knowledge of who is calling it. Three callers hand it
//! rows the same way - closing a live row group, compaction, and the bulk loader - so
//! there is one encoder and one set of encoder bugs. What it is given is everything it
//! uses: the template id, the schema's number and column names, the rows, each row's
//! effective time, and where the rows came from.
//!
//! **How a column is written.** Each present value falls in a group: its type and the
//! form fields that describe its spelling. A value goes into a typed group only if
//! printing it through that group's form gives back what was ingested - a JSON string byte
//! for byte, a JSON number as the same value - and every value the ladder typed `String`,
//! or that no form prints, is in the one String group, as text. A JSON number or bool has
//! no spelling to describe, so it joins the largest group of its own type (for a
//! `Datetime`, of its unit) or starts one. One group is one piece. Otherwise piece A is
//! the largest group, ties to the lower type number and then the lower form bytes; if the
//! rest are one group that is piece B, and if not they are written as text - in piece B,
//! or in piece A when A is the String group. A reader never needs this rule, but it is
//! fixed so the same rows always make the same bytes.

use std::collections::BTreeMap;

use serde_json::Value;
use thiserror::Error;

use super::{
    bitmap, day_of, epoch_of, float_json, ip_bytes, print_float, print_int, print_ip,
    print_rfc3339, print_uuid, type_byte, uuid_bytes, Form, JsonKind, Source, EPOCH_MILLIS,
    EPOCH_SECS, KIND_ROW_GROUP, NO_COMPRESSION, PER_ROW, PLAIN, RFC3339,
};
use crate::store::Row;
use crate::types::{ColValue, ColumnType};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum BuildError {
    #[error("a row group needs at least one row")]
    NoRows,
    #[error("{0} rows is more than a row group's u32 count holds")]
    TooManyRows(usize),
    #[error("{rows} row(s) and {times} effective time(s)")]
    Times { rows: usize, times: usize },
    #[error("a row group's rows must share one UTC day: {min} and {max} do not")]
    TwoDays { min: i64, max: i64 },
    #[error("schema column names must be sorted and distinct: `{0}` is out of place")]
    Schema(String),
    #[error("column `{0}` holds more than a u32 length can describe")]
    TooLong(String),
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
///
/// A row that falls back to arrival time carries `\_received_at` and one with its own
/// time does not, so one template can use more than one schema; a row group none of whose
/// rows has a column leaves it out.
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
) -> Result<RowGroup, BuildError> {
    Ok(encode(template_id, columns, rows, times, source)?.frame(schema_id))
}

/// A row group with every column encoded and no schema number yet: `build` less the one
/// field only a file's writer can fill.
///
/// **Split off so a flush can encode side by side.** Encoding is nearly all of what a row
/// group costs, and it needs nothing from the file; the schema number does, and a schema
/// record has to go into the file before the first row group that names it. So a flush
/// encodes every row group it has to write at once, on as many threads as it has, and then
/// numbers and frames them one at a time as it appends them.
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
) -> Result<Encoded, BuildError> {
    if rows.is_empty() {
        return Err(BuildError::NoRows);
    }
    let row_count = u32::try_from(rows.len()).map_err(|_| BuildError::TooManyRows(rows.len()))?;
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
        let chunk = encode_column(name, rows)?;
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
    /// The row group whole, as schema `schema_id` of its file: the header frame written
    /// around the directory, and its CRC - which covers the header alone, so framing is
    /// cheap however large the chunks are.
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
/// spelling - everything but `json_kind` and a digit count that can vary per row.
/// Ordered, so ties between groups go to the lower bytes.
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

fn encode_column(name: &str, rows: &[&Row]) -> Result<Chunk, BuildError> {
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
            u32::try_from(encoded[0].1.len()).map_err(|_| BuildError::TooLong(name.to_string()))?;
        body.extend_from_slice(&a_len.to_le_bytes());
    }
    let mut forms = Vec::new();
    for (form, bytes) in encoded {
        body.extend_from_slice(&bytes);
        forms.push(form);
    }

    let stored_len =
        u32::try_from(body.len()).map_err(|_| BuildError::TooLong(name.to_string()))?;
    let layout = u8::from(some_null) | (u8::from(two) << 1);
    let mut entry = vec![layout, NO_COMPRESSION];
    entry.extend_from_slice(&stored_len.to_le_bytes());
    entry.extend_from_slice(&crate::frame::crc32c(&body).to_le_bytes());
    for form in forms {
        entry.push(type_byte(form.column_type()));
        entry.push(PLAIN);
        entry.extend(form.bytes());
    }
    Ok(Chunk { entry, body })
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
/// had, and one digit count for every string row or one byte per row when they differ. A
/// JSON number has no count; it prints the shortest form.
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

/// A piece's form and bytes: the kind bitmap, the per-row digits, then the values.
///
/// A row reaching a String piece from a typed group is written as text here - the JSON it
/// arrived as - so the text fallback is decided in one place.
fn encode_piece(
    name: &str,
    piece: &Piece,
    present: &[Present<'_>],
) -> Result<(Form, Vec<u8>), BuildError> {
    let rows: Vec<&Present<'_>> = piece.members.iter().map(|at| &present[*at]).collect();
    let form = form_of(piece, present);
    let mut out = Vec::new();
    if form.kind() == JsonKind::Mixed {
        out.extend(bitmap(rows.iter().map(|p| p.lit)));
    }
    if form.per_row_digits() {
        out.extend(rows.iter().map(|p| if p.lit { 0 } else { digits_of(p) }));
    }
    for p in rows {
        match (&form, &p.class) {
            (Form::String { .. }, _) | (_, Class::Text) => {
                let text = match p.json {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                let len =
                    u32::try_from(text.len()).map_err(|_| BuildError::TooLong(name.to_string()))?;
                out.extend_from_slice(&len.to_le_bytes());
                out.extend_from_slice(text.as_bytes());
            }
            (_, Class::Typed { val, .. }) => match val {
                Val::Int(i) | Val::Time(i) => out.extend_from_slice(&i.to_le_bytes()),
                Val::Float(f) => out.extend_from_slice(&f.to_bits().to_le_bytes()),
                Val::Bool(b) => out.push(u8::from(*b)),
                Val::Uuid(bytes) => out.extend_from_slice(bytes),
                Val::Ip(bytes) => out.extend_from_slice(bytes),
            },
        }
    }
    Ok((form, out))
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
