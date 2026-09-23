//! A row group's header frame and its chunks, decoded and checked.
//!
//! Every length is compared with the bytes that remain before it is used, every CRC is
//! checked before the bytes it covers are decoded, and every field is held to its closed
//! set of values. What a query reads is what this checked.

use serde_json::Value;

use super::{
    autosi_digits, bit, count_set, epoch_of, print_float, print_int, print_ip, print_rfc3339,
    print_uuid, read_bitmap, type_of_byte, Bad, Bytes, Form, JsonKind, Source, DAY_NS,
    KIND_ROW_GROUP, NO_COMPRESSION, OPTIONAL, PER_ROW, PLAIN, RFC3339,
};
use crate::infer::infer;
use crate::types::ColValue;

/// One chunk's directory entry.
#[derive(Debug, Clone, PartialEq)]
pub struct ChunkDir {
    pub layout: u8,
    pub stored_len: u32,
    pub stored_crc: u32,
    /// Absent when every row is null.
    pub a: Option<Form>,
    pub b: Option<Form>,
}

pub const SOME_NULL: u8 = 0b001;
pub const TWO_PIECES: u8 = 0b010;
pub const ALL_NULL: u8 = 0b100;

#[derive(Debug, Clone, PartialEq)]
pub struct Section {
    pub kind: u8,
    pub column: u32,
    pub len: u32,
    pub crc: u32,
}

/// A row group's header, read.
#[derive(Debug, Clone, PartialEq)]
pub struct Header {
    pub template_id: u64,
    pub schema_id: u32,
    pub row_count: u32,
    pub min_time: i64,
    pub max_time: i64,
    pub source: Source,
    pub chunks_len: u32,
    pub chunks: Vec<ChunkDir>,
    pub sections: Vec<Section>,
}

impl Header {
    /// Where each chunk starts inside the chunk area, and then each section.
    pub fn chunk_offsets(&self) -> Vec<u64> {
        let mut at = 0u64;
        self.chunks
            .iter()
            .map(|c| {
                let here = at;
                at += u64::from(c.stored_len);
                here
            })
            .collect()
    }
}

/// A header frame's payload. `base` is the file offset of the payload's first byte;
/// `columns` says how many columns a schema has, `None` for a schema not yet written.
pub fn header(
    payload: &[u8],
    base: u64,
    columns: impl Fn(u32) -> Option<usize>,
) -> Result<Header, Bad> {
    let mut r = Bytes::new(payload, base);
    let kind = r.u8("row group kind")?;
    if kind != KIND_ROW_GROUP {
        return Err(Bad::at(
            base,
            format!("frame kind {kind} is not a row group"),
        ));
    }
    let template_id = r.u64("template_id")?;
    let at = r.offset();
    let schema_id = r.u32("schema_id")?;
    let Some(column_count) = columns(schema_id) else {
        return Err(Bad::at(
            at,
            format!("row group names schema {schema_id}, which no earlier schema record is"),
        ));
    };
    let at = r.offset();
    let row_count = r.u32("row_count")?;
    if row_count == 0 {
        return Err(Bad::at(at, "row group of 0 rows"));
    }
    let at = r.offset();
    let min_time = r.i64("min_time")?;
    let max_time = r.i64("max_time")?;
    if min_time > max_time || min_time.div_euclid(DAY_NS) != max_time.div_euclid(DAY_NS) {
        return Err(Bad::at(
            at,
            format!("time range {min_time}..{max_time} is not ordered within one UTC day"),
        ));
    }
    let at = r.offset();
    let source_byte = r.u8("source")?;
    let stream = r.u64("stream")?;
    let first = r.u64("first")?;
    let last = r.u64("last")?;
    let source = match source_byte {
        0 if stream == 0 && first == 0 && last == 0 => Source::NotRecorded,
        1 | 2 if stream != 0 && first <= last => {
            if source_byte == 1 {
                Source::Wal {
                    stream,
                    first,
                    last,
                }
            } else {
                Source::Load {
                    stream,
                    first,
                    last,
                }
            }
        }
        _ => {
            return Err(Bad::at(
                at,
                format!("source {source_byte} with stream {stream}, positions {first}..{last}"),
            ))
        }
    };
    let chunks_len_at = r.offset();
    let chunks_len = r.u32("chunks_len")?;
    let mut chunks = Vec::with_capacity(column_count.min(r.remaining()));
    for _ in 0..column_count {
        chunks.push(chunk_dir(&mut r)?);
    }
    let section_count = r.u16("section_count")?;
    let mut sections = Vec::new();
    for _ in 0..section_count {
        let at = r.offset();
        let kind = r.u8("section kind")?;
        let column = r.u32("section column")?;
        let len = r.u32("section len")?;
        let crc = r.u32("section crc")?;
        if kind & OPTIONAL == 0 {
            return Err(Bad::at(
                at,
                format!("section kind 0x{kind:02x} is required and this build does not know it"),
            ));
        }
        if column as usize >= column_count {
            return Err(Bad::at(
                at,
                format!("section names column {column} of {column_count}"),
            ));
        }
        sections.push(Section {
            kind,
            column,
            len,
            crc,
        });
    }
    r.done("the row group header")?;
    let total: u64 = chunks.iter().map(|c| u64::from(c.stored_len)).sum::<u64>()
        + sections.iter().map(|s| u64::from(s.len)).sum::<u64>();
    if total != u64::from(chunks_len) {
        return Err(Bad::at(
            chunks_len_at,
            format!("chunks_len {chunks_len} and the directory's lengths add to {total}"),
        ));
    }
    Ok(Header {
        template_id,
        schema_id,
        row_count,
        min_time,
        max_time,
        source,
        chunks_len,
        chunks,
        sections,
    })
}

fn chunk_dir(r: &mut Bytes<'_>) -> Result<ChunkDir, Bad> {
    let at = r.offset();
    let layout = r.u8("chunk layout")?;
    let compression = r.u8("chunk compression")?;
    let stored_len = r.u32("chunk stored_len")?;
    let stored_crc = r.u32("chunk stored_crc")?;
    if layout & !0b111 != 0 || (layout & ALL_NULL != 0 && layout != ALL_NULL) {
        return Err(Bad::at(at, format!("chunk layout 0x{layout:02x}")));
    }
    if compression != NO_COMPRESSION {
        // 1 is zstd, which this build does not decode; anything else is unassigned.
        return Err(Bad::at(
            at + 1,
            format!("chunk compression {compression}, which this build does not read"),
        ));
    }
    if layout == ALL_NULL {
        if stored_len != 0 || stored_crc != 0 {
            return Err(Bad::at(at, "an all-null chunk with bytes"));
        }
        return Ok(ChunkDir {
            layout,
            stored_len,
            stored_crc,
            a: None,
            b: None,
        });
    }
    let a = Some(piece_form(r)?);
    let b = if layout & TWO_PIECES != 0 {
        Some(piece_form(r)?)
    } else {
        None
    };
    Ok(ChunkDir {
        layout,
        stored_len,
        stored_crc,
        a,
        b,
    })
}

fn piece_form(r: &mut Bytes<'_>) -> Result<Form, Bad> {
    let at = r.offset();
    let ty_byte = r.u8("piece type")?;
    let ty = type_of_byte(ty_byte).ok_or_else(|| Bad::at(at, format!("type {ty_byte}")))?;
    let encoding = r.u8("piece encoding")?;
    if encoding != PLAIN {
        // 1 to 5 are assigned to encodings a later build writes; this one refuses them as
        // it refuses an unknown number.
        return Err(Bad::at(
            at + 1,
            format!("encoding {encoding}, which this build does not read"),
        ));
    }
    Form::read(ty, r)
}

// ── Chunks ─────────────────────────────────────────────────────────────────────────

/// One decoded value: what a query compares against, and - where printing that would not
/// give back what was ingested - the JSON value that was.
pub type Cell = (ColValue, Option<Value>);

/// A chunk's bytes, already CRC-checked, into one cell per row. `base` is the chunk's
/// file offset.
pub fn chunk(body: &[u8], base: u64, dir: &ChunkDir, rows: usize) -> Result<Vec<Cell>, Bad> {
    let Some(a_form) = dir.a else {
        return Ok(vec![(ColValue::Null, None); rows]);
    };
    let mut r = Bytes::new(body, base);
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
    let (a, b) = match dir.b {
        Some(b_form) => {
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
            let a = piece(a_bytes, a_at, &a_form, present - in_b)?;
            let b = piece(b_bytes, b_at, &b_form, in_b)?;
            (a, Some((selector, b)))
        }
        None => {
            let at = r.offset();
            let bytes = r.take(r.remaining(), "piece A")?;
            (piece(bytes, at, &a_form, present)?, None)
        }
    };

    let mut a = a.into_iter();
    let mut b_cells = b.as_ref().map(|(_, cells)| cells.iter());
    let mut out = Vec::with_capacity(rows);
    let mut nth = 0usize;
    for row in 0..rows {
        if presence.is_some_and(|map| !bit(map, row)) {
            out.push((ColValue::Null, None));
            continue;
        }
        let from_b = b.as_ref().is_some_and(|(selector, _)| bit(selector, nth));
        nth += 1;
        let cell = if from_b {
            b_cells.as_mut().and_then(Iterator::next).cloned()
        } else {
            a.next()
        };
        // The counts were taken from the same bitmaps, so a piece cannot run dry.
        out.push(cell.unwrap_or((ColValue::Null, None)));
    }
    Ok(out)
}

/// One piece of `n` rows. The values must use the piece up exactly.
fn piece(bytes: &[u8], base: u64, form: &Form, n: usize) -> Result<Vec<Cell>, Bad> {
    let mut r = Bytes::new(bytes, base);
    let kinds = match form.kind() {
        JsonKind::Mixed => {
            let at = r.offset();
            let map = read_bitmap(&mut r, n, "kind bitmap")?;
            let lits = count_set(map);
            if lits == 0 || lits == n {
                return Err(Bad::at(at, "json_kind 2 over rows that are all one kind"));
            }
            Some(map)
        }
        _ => None,
    };
    let lit = |i: usize| match form.kind() {
        JsonKind::Str => false,
        JsonKind::Lit => true,
        JsonKind::Mixed => kinds.is_some_and(|map| bit(map, i)),
    };
    let digits = if form.per_row_digits() {
        Some(r.take(n, "per-row digits")?)
    } else {
        None
    };
    let mut out = Vec::with_capacity(n.min(r.remaining()));
    for i in 0..n {
        let at = r.offset();
        let row_digits = digits.map(|d| d[i]);
        if lit(i) && row_digits.is_some_and(|d| d != 0) {
            return Err(Bad::at(at, "a JSON number with a digits byte"));
        }
        out.push(value(&mut r, form, lit(i), row_digits)?);
    }
    r.done("a piece")?;
    Ok(out)
}

fn value(r: &mut Bytes<'_>, form: &Form, lit: bool, row_digits: Option<u8>) -> Result<Cell, Bad> {
    let at = r.offset();
    let bad = |what: String| Bad::at(at, what);
    Ok(match *form {
        Form::String { .. } => {
            let len = r.u32("string length")? as usize;
            let raw = r.take(len, "string")?;
            let text = std::str::from_utf8(raw)
                .map_err(|_| bad("a string value is not UTF-8".into()))?
                .to_string();
            if lit {
                let json: Value = serde_json::from_str(&text)
                    .map_err(|_| bad(format!("literal row `{text}` is not JSON")))?;
                if !matches!(json, Value::Number(_) | Value::Bool(_)) {
                    return Err(bad(format!("literal row `{text}` is not a number or bool")));
                }
                let value = infer(&text).value;
                let written = (value.printed() != json).then_some(json);
                (value, written)
            } else {
                let value = if surely_text(&text) {
                    ColValue::String(text)
                } else {
                    infer(&text).value
                };
                match value {
                    ColValue::String(_) => (value, None),
                    other => {
                        // Stored as text because no form printed it: the ladder's value
                        // for matching, and the text it arrived as for the answer.
                        let written = Value::String(String::from_utf8_lossy(raw).into_owned());
                        (other, Some(written))
                    }
                }
            }
        }
        Form::Int64 { plus, width, .. } => {
            let v = r.i64("Int64 value")?;
            if lit {
                (ColValue::Int64(v), None)
            } else {
                let text = print_int(v, plus, width);
                (ColValue::Int64(v), Some(Value::String(text)))
            }
        }
        Form::Float64 { digits, .. } => {
            let f = f64::from_bits(r.u64("Float64 value")?);
            if !f.is_finite() {
                return Err(bad("a Float64 that is not finite".into()));
            }
            if lit {
                (ColValue::Float64(f), None)
            } else {
                let d = row_digits.unwrap_or(digits);
                if d == PER_ROW {
                    return Err(bad("a per-row digits byte of 255".into()));
                }
                (ColValue::Float64(f), Some(Value::String(print_float(f, d))))
            }
        }
        Form::Datetime {
            unit,
            separator,
            zone,
            offset,
            digits,
            ..
        } => {
            let ns = r.i64("Datetime value")?;
            if unit == RFC3339 {
                let d = row_digits.unwrap_or(digits);
                // The canonical spelling needs no second copy: `T`, `Z`, and the digits
                // canonical printing would choose.
                if separator == b'T' && zone == b'Z' && d == autosi_digits(ns) {
                    (ColValue::Datetime(ns), None)
                } else {
                    let text = print_rfc3339(ns, separator, zone, offset, d).ok_or_else(|| {
                        bad(format!(
                            "Datetime {ns} is not printable with {d} fraction digits"
                        ))
                    })?;
                    (ColValue::Datetime(ns), Some(Value::String(text)))
                }
            } else {
                let n = epoch_of(ns, unit)
                    .ok_or_else(|| bad(format!("Datetime {ns} is not a whole epoch unit")))?;
                let json = if lit {
                    Value::from(n)
                } else {
                    Value::String(n.to_string())
                };
                (ColValue::Datetime(ns), Some(json))
            }
        }
        Form::Bool { case, .. } => {
            let b = match r.u8("Bool value")? {
                0 => false,
                1 => true,
                other => return Err(bad(format!("Bool byte {other}"))),
            };
            if lit {
                (ColValue::Bool(b), None)
            } else {
                let text = match (b, case) {
                    (true, 0) => "true",
                    (false, 0) => "false",
                    (true, _) => "True",
                    (false, _) => "False",
                };
                (ColValue::Bool(b), Some(Value::String(text.to_string())))
            }
        }
        Form::Uuid { case } => {
            let bytes = r.id("Uuid value")?;
            (ColValue::String(print_uuid(&bytes, case == 1)), None)
        }
        Form::Ip { spelling } => {
            let width = if spelling == 4 { 4 } else { 16 };
            let bytes = r.take(width, "Ip value")?;
            let text =
                print_ip(bytes, spelling).ok_or_else(|| bad(format!("Ip spelling {spelling}")))?;
            (ColValue::String(text), None)
        }
    })
}

/// Text the ladder certainly reads as `String`, told apart without running it.
///
/// A String piece's rows are text the ladder typed `String` and, rarely, a value of
/// another type that no form printed back. A query needs the ladder's value for each - a
/// stored `"-0"` must still match `0` - and running every rung over every string in a scan
/// costs more than the scan. Every rung but the last needs a value that starts with a
/// digit, a sign, a point, a colon, a hex letter (a UUID, an IPv6 address, `false`) or `t`
/// (`true`); `inf` and `nan` parse as floats and are held to finite values, so they are
/// text too. Anything else is `String` without asking.
pub fn surely_text(text: &str) -> bool {
    let Some(first) = text.bytes().next() else {
        return true;
    };
    match first {
        b'0'..=b'9' | b'+' | b'-' | b'.' | b':' => false,
        b'a'..=b'f' | b'A'..=b'F' => {
            !(text.len() == 36
                || text == "false"
                || text == "False"
                || text
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() || b == b':' || b == b'.'))
        }
        b't' | b'T' => !(text == "true" || text == "True"),
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ColumnType;

    /// The shortcut agrees with the ladder over text built to sit on every rung's edge.
    #[test]
    fn surely_text_never_claims_what_the_ladder_would_type() {
        let samples = [
            "",
            "INFO",
            "GET",
            "error",
            "debug",
            "abc",
            "deadbeef",
            "fe80::1",
            "::1",
            "false",
            "False",
            "true",
            "True",
            "TRUE",
            "inf",
            "NaN",
            "infinity",
            "-inf",
            "1e3",
            ".5",
            "+5",
            "-0",
            "0",
            "2024-01-15",
            "2024-01-15T10:30:00Z",
            "1705312200",
            "550e8400-e29b-41d4-a716-446655440000",
            "10.0.0.1",
            "u42@auth.example.com",
            "Tuesday",
            "a:b",
            "ab.cd",
            "c0ffee",
        ];
        for text in samples {
            if surely_text(text) {
                assert_eq!(
                    infer(text).ty,
                    Some(ColumnType::String),
                    "`{text}` was called text"
                );
            }
        }
    }
}
