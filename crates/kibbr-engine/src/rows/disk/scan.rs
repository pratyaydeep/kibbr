// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! One day of one leaf, read for a query: its row groups, then its rows not yet in one.
//! The window and the terms are put to a row group's columns (`rows::filter`) before any
//! row is built, and a row is built only as far as the visitor reads it (`Wants`). A day's
//! row groups may be chosen on the scan threads, but are read and handed over on the
//! asking thread in storage order, so sharing cannot be told from an answer.

use std::borrow::Cow;
use std::cell::OnceCell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use super::{Stored, View};
use crate::rows::filter::{hits, hits_by_row, instants, said_instants, Instants, Sel};
use crate::rows::scan_threads::{Fan, Turn};
use crate::format::column::{Col, Column, Memo};
use crate::format::file::{Reader, StoredChunk};
use crate::format::read::{Header, Marks, ALL_NULL, SOME_NULL};
use crate::format::{day_of, Bad};
use crate::rows::{
    effective_ts, in_window, instant_of, nanos_of, render_text, said_instant, said_ts, shown_row,
    Row, Scan, ScanOptions, Shown, StoreError, Term, Visitor, Want, Wants, DEFAULT_TS_COLUMN,
    RECEIVED_AT_COLUMN,
};
use crate::model::types::ColValue;

/// What one leaf's scan was asked, worked out once for the leaf; plain data, so it can be
/// shared by reference with whatever chooses a row group's rows.
pub(super) struct Ask<'a> {
    opts: &'a ScanOptions,
    terms: &'a [Term],
    /// For each term, how its key's column shows a stored `Datetime`: as text where the
    /// column's type is now `String`, as a count where it is now `Int64`.
    term_shown: Vec<Shown>,
    since: Option<i64>,
    until: Option<i64>,
    /// The leaf's time column; and whether it, and the arrival time, are columns whose
    /// type is now `String`, in which a date is shown as the text it arrived as.
    ts: &'a str,
    ts_is_text: bool,
    received_is_text: bool,
    wants: Wants,
}

impl<'a> Ask<'a> {
    pub(super) fn new(opts: &'a ScanOptions, terms: &'a [Term], wants: Wants) -> Self {
        let is_text = |name: &str| opts.text_columns.iter().any(|c| c == name);
        let ts = opts.ts_column.as_deref().unwrap_or(DEFAULT_TS_COLUMN);
        Self {
            opts,
            terms,
            term_shown: terms
                .iter()
                .map(|term| {
                    if is_text(term.key()) {
                        Shown::Text
                    } else if opts.int_columns.iter().any(|c| c == term.key()) {
                        Shown::Count
                    } else {
                        Shown::Stored
                    }
                })
                .collect(),
            since: opts.since.map(nanos_of),
            until: opts.until.map(nanos_of),
            ts,
            ts_is_text: is_text(ts),
            received_is_text: is_text(RECEIVED_AT_COLUMN),
            wants,
        }
    }

    fn windowed(&self) -> bool {
        self.since.is_some() || self.until.is_some()
    }

    /// No window and no term: every row passes, and nothing need be decoded to know it.
    fn is_open(&self) -> bool {
        !self.windowed() && self.terms.is_empty()
    }

    /// Is the visitor asked about a row before the row is built? Not where the scan stops at
    /// a number of rows: every one of the first so many is a row the visitor is owed.
    fn gated(&self) -> bool {
        self.wants == Wants::Newest && self.opts.limit == 0
    }

    /// Does `hand` read a row group's times off its time columns before it builds a row? Not
    /// where a time column is now text: that time is read a row at a time, as rendered.
    fn warms(&self) -> bool {
        self.gated() && !(self.ts_is_text || self.received_is_text)
    }

    /// Is there anything in a row group of this scan for a second thread to do? Not in one
    /// that stops at a number of rows, nor where nothing is decoded before the visitor is asked.
    fn fans(&self) -> bool {
        self.opts.limit == 0 && (!self.is_open() || self.warms())
    }

    /// Where a schema's columns are for this question.
    fn shape(&self, schema: &[String]) -> Shape {
        let windowed = self.windowed();
        let columns = &self.opts.columns;
        // Empty means every column. A window always reads the time column, and a term's column
        // is read because the term is asked of it, whether or not the caller named it.
        let wanted: Vec<usize> = (0..schema.len())
            .filter(|at| {
                let name = &schema[*at];
                columns.is_empty()
                    || columns.iter().any(|w| w == name)
                    || (windowed && (name == self.ts || name == RECEIVED_AT_COLUMN))
                    || self.terms.iter().any(|term| term.key() == name)
            })
            .collect();
        let place = |name: &str| wanted.iter().position(|at| schema[*at] == name);
        let row = match &self.wants {
            Wants::Nothing => Vec::new(),
            Wants::Columns(names) => (0..wanted.len())
                .filter(|place| names.iter().any(|name| *name == schema[wanted[*place]]))
                .collect(),
            Wants::Rows | Wants::Newest => (0..wanted.len()).collect(),
        };
        Shape {
            terms: self.terms.iter().map(|term| place(term.key())).collect(),
            ts: place(self.ts),
            received: place(RECEIVED_AT_COLUMN),
            row,
            wanted,
        }
    }

    /// Does a row not yet in a row group pass? The window and each term are asked of the
    /// stored row, read as the rendered one would be, so no row is rendered to be thrown away.
    fn admits(&self, row: &Row) -> bool {
        if self.windowed() && !in_window(effective_ts(row, self.ts), self.since, self.until) {
            return false;
        }
        self.terms
            .iter()
            .zip(&self.term_shown)
            .all(|(term, shown)| term.matches_in(row, *shown))
    }

    /// The effective time of a row not yet in a row group as the rendered row would show it,
    /// read without rendering it: `effective_ts` of `render_row` of the row, and no row made.
    fn seen_ts_of(&self, row: &Row) -> Option<i64> {
        let columns = [
            (self.ts, self.ts_is_text),
            (RECEIVED_AT_COLUMN, self.received_is_text),
        ];
        columns
            .into_iter()
            .find_map(|(column, text)| {
                let value = row.get(column)?;
                match value {
                    ColValue::Datetime(_) if text => {
                        instant_of(&render_text(value, row.written(column))?)
                    }
                    _ => instant_of(value),
                }
            })
            // Neither holds an instant: the time column as only a time column reads it.
            .or_else(|| said_ts(row, self.ts))
    }
}

/// Where one schema's columns are for one question. Made once for a schema and kept while
/// the next row group shares it, which in a day of one template is nearly always.
struct Shape {
    /// The schema's columns that are read off the disk, by their place in the schema,
    /// ascending.
    wanted: Vec<usize>,
    /// For each term, its column's place in `wanted`. `None`: the schema has no such
    /// column, and no row of the row group matches the term.
    terms: Vec<Option<usize>>,
    /// The time column's place in `wanted`, and the arrival time's.
    ts: Option<usize>,
    received: Option<usize>,
    /// The places in `wanted` a built row takes its columns from: all of them, or only
    /// the ones the visitor named.
    row: Vec<usize>,
}

/// One stored row group as a scan holds it. It owns what was read of it, so it can be read
/// in one place and its rows chosen and handed over in another.
struct Unit<'v> {
    at: &'v Stored,
    shape: &'v Shape,
    /// The UTC day it is stored under.
    day: i64,
    /// The position of its first row among its template's rows of the day.
    base: u64,
    rows: usize,
    header: Header,
    /// One for each of `shape.wanted`, and so is `columns`.
    stored: Vec<StoredChunk>,
    columns: Vec<OnceCell<Column>>,
    /// Which rows of each are an array or an empty object, where the row group says so.
    marks: Vec<Option<Marks>>,
}

impl<'v> Unit<'v> {
    /// The header frame and every wanted chunk, read and CRC-checked, and nothing
    /// decoded. The chunk bytes are counted as read here, where they are read.
    fn read(
        at: &'v Stored,
        shape: &'v Shape,
        day: i64,
        base: u64,
        reader: &mut Reader,
        out: &mut Scan,
    ) -> Result<Self, StoreError> {
        let header = reader.header(&at.entry, at.schema.len())?;
        let (stored, read) = reader.stored(&at.entry, &header, &shape.wanted)?;
        out.bytes_read += read;
        let mut marks = Vec::with_capacity(shape.wanted.len());
        for column in &shape.wanted {
            let (found, read) = reader.marks(&at.entry, &header, *column)?;
            out.bytes_read += read;
            marks.push(found);
        }
        Ok(Self {
            at,
            shape,
            day,
            base,
            rows: header.row_count as usize,
            columns: stored.iter().map(|_| OnceCell::new()).collect(),
            header,
            stored,
            marks,
        })
    }

    /// A refusal of something in this row group, named by its file.
    fn refused(&self, bad: Bad) -> StoreError {
        bad.in_file(&self.at.file.path).into()
    }

    /// The chunk at `place` of the wanted ones, decoded.
    fn decode(&self, place: usize) -> Result<Column, StoreError> {
        let chunk = &self.stored[place];
        let dir = &self.header.chunks[self.shape.wanted[place]];
        Column::decode(&chunk.bytes, chunk.at, dir, self.rows)
            .map(|column| column.marked(self.marks[place].clone()))
            .map_err(|bad| self.refused(bad))
    }

    /// A decoded column over the bytes of the chunk at `place`, which it was decoded from.
    fn over<'c>(&'c self, column: &'c Column, place: usize) -> Result<Col<'c>, StoreError> {
        column
            .over(&self.stored[place].bytes)
            .map_err(|bad| self.refused(bad))
    }

    /// The column at `place` of the wanted ones, decoded the first time it is asked for.
    fn column(&self, place: usize) -> Result<Col<'_>, StoreError> {
        let cell = &self.columns[place];
        let column = match cell.get() {
            Some(column) => column,
            None => {
                let made = self.decode(place)?;
                cell.get_or_init(|| made)
            }
        };
        self.over(column, place)
    }

    /// The bytes choosing this row group's rows may leave decoded in it, read off the header
    /// and never less than what is decoded; the same for a count, so a batch is cut alike.
    fn inflates(&self, ask: &Ask<'_>) -> u64 {
        let shape = self.shape;
        let times = ask.windowed() || ask.warms();
        (0..shape.wanted.len())
            .filter(|place| {
                shape.terms.contains(&Some(*place))
                    || (times && (shape.ts == Some(*place) || shape.received == Some(*place)))
            })
            .map(|place| u64::from(self.header.chunks[shape.wanted[place]].body_len))
            .sum()
    }

    /// Each row's effective time, off the stored columns. The arrival time is decoded
    /// only when some row needs it.
    fn times(&self) -> Result<Instants<'_>, StoreError> {
        let ts = match self.shape.ts {
            Some(place) => instants(self.column(place)?).map_err(|bad| self.refused(bad))?,
            None => Instants::absent(self.rows),
        };
        let ts = match self.shape.received {
            Some(place) if ts.missing() && self.shape.ts != Some(place) => {
                let received = instants(self.column(place)?).map_err(|bad| self.refused(bad))?;
                ts.or(&received)
            }
            _ => ts,
        };
        // A row with neither: its time column as only a time column reads it.
        match self.shape.ts {
            Some(place) if ts.missing() => {
                let said = said_instants(self.column(place)?).map_err(|bad| self.refused(bad))?;
                Ok(ts.or(&said))
            }
            _ => Ok(ts),
        }
    }

    /// The effective time of row `r` as the row built of it would show it, for a scan one of
    /// whose time columns is now text: the visitor reads the row's time from the rendered text.
    fn seen_ts(&self, ask: &Ask<'_>, r: usize) -> Result<Option<i64>, StoreError> {
        let columns = [
            (self.shape.ts, ask.ts_is_text),
            (self.shape.received, ask.received_is_text),
        ];
        for (place, text) in columns {
            let Some(place) = place else {
                continue;
            };
            let (value, written) = self
                .column(place)?
                .cell(r)
                .map_err(|bad| self.refused(bad))?;
            let at = match &value {
                ColValue::Datetime(_) if text => render_text(&value, written.as_ref())
                    .as_ref()
                    .and_then(instant_of),
                _ => instant_of(&value),
            };
            if at.is_some() {
                return Ok(at);
            }
        }
        // Neither holds an instant: the time column as only a time column reads it.
        let Some(place) = self.shape.ts else {
            return Ok(None);
        };
        let (value, written) = self
            .column(place)?
            .cell(r)
            .map_err(|bad| self.refused(bad))?;
        Ok(said_instant(&value, written.as_ref()))
    }
}

/// The rows of one row group, built one at a time for a visitor. Their columns are decoded
/// when the first row is built, so a row group none of whose rows is built decodes none.
struct Builder<'u> {
    unit: &'u Unit<'u>,
    ask: &'u Ask<'u>,
    /// One for each of `shape.row`: the column, and what was printed of it so far. A word
    /// or a constant is printed once for the row group and copied after.
    made: Option<(Vec<Col<'u>>, Vec<Memo>)>,
    row: Row,
}

impl<'u> Builder<'u> {
    fn new(unit: &'u Unit<'u>, ask: &'u Ask<'u>) -> Self {
        Self {
            unit,
            ask,
            made: None,
            row: Row::with_capacity(unit.shape.row.len()),
        }
    }

    /// Row `r` as the visitor is handed it: its cells in a row, and the row rendered
    /// where a column that is now text holds a date.
    fn build(&mut self, r: usize) -> Result<Cow<'_, Row>, StoreError> {
        let unit = self.unit;
        let shape = unit.shape;
        let (columns, memos) = match &mut self.made {
            Some(made) => made,
            None => {
                let columns: Vec<Col<'u>> = shape
                    .row
                    .iter()
                    .map(|place| unit.column(*place))
                    .collect::<Result<_, _>>()?;
                let memos = columns.iter().map(|_| Memo::default()).collect();
                self.made.insert((columns, memos))
            }
        };
        self.row.clear();
        for ((place, column), memo) in shape.row.iter().zip(columns.iter()).zip(memos) {
            let (value, written) = column.cell_with(r, memo).map_err(|bad| unit.refused(bad))?;
            let name = unit.at.schema[shape.wanted[*place]].clone();
            self.row.insert_written(name, value, written);
        }
        let seen = match shown_row(&self.row, self.ask.opts) {
            Some(rendered) => Cow::Owned(rendered),
            None => Cow::Borrowed(&self.row),
        };
        // A whole row holds every column a term reads, so the row that was chosen from
        // columns can be asked the terms again as a row.
        debug_assert!(
            !matches!(self.ask.wants, Wants::Rows | Wants::Newest)
                || self.ask.terms.iter().all(|term| term.matches_row(&seen)),
            "row {r} of the row group at offset {} of {} was chosen and does not pass {:?}",
            unit.at.entry.offset,
            unit.at.file.path.display(),
            self.ask.terms
        );
        Ok(seen)
    }
}

/// Which rows of a row group pass the window and the terms, from typed values and stopping
/// at the first term that leaves no row. A debug build checks each against `slow_pick`.
fn pick(unit: &Unit<'_>, ask: &Ask<'_>) -> Result<Sel, StoreError> {
    let rows = unit.rows;
    let mut sel = Sel::all(rows);
    if ask.is_open() {
        return Ok(sel);
    }
    // The window reads the stored value, as it does on a row: a date in a column that is
    // now text is still the instant it was stored as.
    if ask.windowed() {
        sel.and(&unit.times()?.window(ask.since, ask.until));
    }
    let terms = ask.terms.iter().zip(&unit.shape.terms);
    for ((term, place), shown) in terms.zip(&ask.term_shown) {
        if sel.is_empty() {
            break;
        }
        // A row without the term's key matches nothing, `*` included.
        let Some(place) = *place else {
            sel = Sel::none(rows);
            break;
        };
        // `*` is "present and not null", and a chunk's directory entry says that of every row
        // at once when it can; neither case is decoded to find out.
        if matches!(term.want(), Want::Any) {
            let layout = unit.header.chunks[unit.shape.wanted[place]].layout;
            if layout == ALL_NULL {
                sel = Sel::none(rows);
                break;
            }
            if layout & SOME_NULL == 0 {
                continue;
            }
        }
        let hits = hits(unit.column(place)?, term, *shown).map_err(|bad| unit.refused(bad))?;
        sel.and(&hits);
    }
    if cfg!(debug_assertions) {
        // Compared only where the plain way has an answer. It prints every value it asks
        // about and refuses one that does not print, which a typed compare never meets.
        if let Ok(slow) = slow_pick(unit, ask) {
            assert_eq!(
                sel,
                slow,
                "the rows chosen from columns are not the rows chosen cell by cell: {:?} in \
                 [{:?}, {:?}) over the row group at offset {} of {}",
                ask.terms,
                ask.since,
                ask.until,
                unit.at.entry.offset,
                unit.at.file.path.display()
            );
        }
    }
    Ok(sel)
}

/// `pick` by definition, cell by printed cell with no shortcut: what `pick` is held to. It
/// decodes for itself and leaves the unit as found, so checking does not change the scan.
fn slow_pick(unit: &Unit<'_>, ask: &Ask<'_>) -> Result<Sel, StoreError> {
    let rows = unit.rows;
    let mut sel = Sel::all(rows);
    if ask.windowed() {
        // `effective_ts`, of a row that holds these two columns: the time column's
        // instant, and the arrival time's where there is none.
        let places = [unit.shape.ts, unit.shape.received];
        let mut columns = Vec::with_capacity(places.len());
        for place in places {
            columns.push(match place {
                Some(place) => Some((place, unit.decode(place)?, Memo::default())),
                None => None,
            });
        }
        let mut inside = Sel::none(rows);
        for row in 0..rows {
            let mut at = None;
            for (place, column, memo) in columns.iter_mut().flatten() {
                let cell = unit
                    .over(column, *place)?
                    .cell_with(row, memo)
                    .map_err(|bad| unit.refused(bad))?;
                at = instant_of(&cell.0);
                if at.is_some() {
                    break;
                }
            }
            // And where neither holds one, the time column as only a time column reads it.
            if at.is_none() {
                if let Some((place, column, memo)) = columns[0].as_mut() {
                    let cell = unit
                        .over(column, *place)?
                        .cell_with(row, memo)
                        .map_err(|bad| unit.refused(bad))?;
                    at = said_instant(&cell.0, cell.1.as_ref());
                }
            }
            if in_window(at, ask.since, ask.until) {
                inside.set(row);
            }
        }
        sel.and(&inside);
    }
    let terms = ask.terms.iter().zip(&unit.shape.terms);
    for ((term, place), shown) in terms.zip(&ask.term_shown) {
        let Some(place) = *place else {
            sel = Sel::none(rows);
            continue;
        };
        let column = unit.decode(place)?;
        let hits = hits_by_row(unit.over(&column, place)?, term, *shown)
            .map_err(|bad| unit.refused(bad))?;
        sel.and(&hits);
    }
    Ok(sel)
}

/// The chosen rows of a row group to the visitor, counted before any is built; `false` at
/// the limit. A gated visitor is asked first, and told if the row's time is not in its day.
fn hand(
    unit: &Unit<'_>,
    ask: &Ask<'_>,
    sel: &Sel,
    v: &mut dyn Visitor,
    out: &mut Scan,
) -> Result<bool, StoreError> {
    let hits = sel.count();
    let limit = ask.opts.limit as u64;
    let (take, done) = if limit == 0 {
        (hits, false)
    } else {
        // At least one: a scan that arrives with its limit already met - by days counted
        // unread - still counts the next row that passes, and stops on it.
        let room = limit.saturating_sub(out.matched).max(1);
        if hits >= room {
            (room, true)
        } else {
            (hits, false)
        }
    };
    let looked = match take.checked_sub(1).and_then(|last| sel.nth(last)) {
        Some(last) if done => last + 1,
        _ => unit.rows,
    };
    out.scanned += looked as u64;
    out.matched += take;

    if take == 0 || ask.wants == Wants::Nothing {
        return Ok(!done);
    }
    let mut rows = Builder::new(unit, ask);
    if !ask.gated() {
        for r in sel.iter().take(take as usize) {
            let seen = rows.build(r)?;
            v.row(&seen, unit.base + r as u64);
        }
        return Ok(!done);
    }
    let stored = if ask.warms() {
        Some(unit.times()?)
    } else {
        None
    };
    for r in sel.iter() {
        let ts = match &stored {
            Some(times) => times.get(r),
            None => unit.seen_ts(ask, r)?,
        };
        let seq = unit.base + r as u64;
        // A row with no time at all is stored under the day of the epoch.
        let placed = day_of(ts.unwrap_or(0)) == unit.day;
        if !v.wants_row(ts, seq, placed) {
            continue;
        }
        let seen = rows.build(r)?;
        // The visitor ranks the row by the time it reads off it, and was asked with this
        // one: were they two times, a row that belongs in the answer could go unbuilt.
        debug_assert_eq!(
            effective_ts(&seen, ask.ts),
            ts,
            "row {r} of the row group at offset {} of {} was asked about with another time \
             than it shows",
            unit.at.entry.offset,
            unit.at.file.path.display()
        );
        v.row(&seen, seq);
    }
    Ok(!done)
}

/// The rows not yet in a row group, after the stored ones, because that is the position
/// they will have once written. `base` is the day's stored rows; `false` at the limit.
fn tail(view: &View, ask: &Ask<'_>, base: u64, v: &mut dyn Visitor, out: &mut Scan) -> bool {
    let limit = ask.opts.limit;
    let moved = view
        .closing
        .as_ref()
        .map_or(&[][..], |batch| &batch.rows[..]);
    // Every row passes and nobody reads one: they are counted, and not walked.
    if ask.is_open() && ask.wants == Wants::Nothing && limit == 0 {
        let held = (moved.len() + view.buffer.len()) as u64;
        out.scanned += held;
        out.matched += held;
        return true;
    }
    let gated = ask.gated();
    for (k, b) in moved.iter().chain(view.buffer.iter()).enumerate() {
        out.scanned += 1;
        if !ask.admits(&b.row) {
            continue;
        }
        out.matched += 1;
        if ask.wants != Wants::Nothing {
            let seq = base + k as u64;
            // A visitor that keeps the newest rows is asked first here too, so that a row it has
            // no use for is not rendered, and told whether that time is in this day.
            let asked = gated.then(|| ask.seen_ts_of(&b.row));
            if asked.is_none_or(|ts| v.wants_row(ts, seq, day_of(ts.unwrap_or(0)) == view.day)) {
                // A whole row, whatever columns the visitor named: it is a row already.
                let rendered = shown_row(&b.row, ask.opts);
                let seen = rendered.as_ref().unwrap_or(&b.row);
                debug_assert!(
                    asked.is_none_or(|ts| ts == effective_ts(seen, ask.ts)),
                    "row {seq} of a day's rows in memory was asked about with another time \
                     than it shows"
                );
                v.row(seen, seq);
            }
        }
        if limit != 0 && out.matched as usize >= limit {
            return false;
        }
    }
    true
}

/// The most row groups one window holds, and the most chunk area by the index: the bytes
/// as they are stored, which is what the index knows.
const WINDOW_ROW_GROUPS: usize = 256;
const WINDOW_BYTES: u64 = 32 * 1024 * 1024;

/// The most chunk body, once inflated, that the row groups of one batch of a shared-out
/// window may hold decoded: a window's bound is on stored bytes, and chunks inflate.
const BATCH_BODY_BYTES: u64 = 32 * 1024 * 1024;

/// One row group of a window that is chosen on the pool: the unit as it was read, and
/// the rows chosen of it once some thread has chosen them.
struct Slot<'v> {
    unit: Unit<'v>,
    sel: Option<Result<Sel, StoreError>>,
}

/// One day's rows - stored, then in memory - handed to the visitor, its row groups taken a
/// window at a time and shared out where that pays (`together`); `false` at the limit.
pub(super) fn day(
    view: &View,
    ask: &Ask<'_>,
    v: &mut dyn Visitor,
    out: &mut Scan,
    readers: &mut HashMap<PathBuf, Reader>,
    fan: Option<&Fan>,
) -> Result<bool, StoreError> {
    day_by(view, ask, v, out, readers, fan, BATCH_BODY_BYTES)
}

/// `day`, with what a batch of a shared-out window may hold decoded said by the caller:
/// `BATCH_BODY_BYTES` for every scan, and a few bytes for a test that cuts a small window.
fn day_by(
    view: &View,
    ask: &Ask<'_>,
    v: &mut dyn Visitor,
    out: &mut Scan,
    readers: &mut HashMap<PathBuf, Reader>,
    fan: Option<&Fan>,
    batch: u64,
) -> Result<bool, StoreError> {
    let windowed = ask.windowed();
    let stored = &view.stored[..];
    let fan = fan.filter(|_| ask.fans());
    let most = if fan.is_some() { WINDOW_ROW_GROUPS } else { 1 };
    let mut shaped: Option<(Arc<Vec<String>>, Shape)> = None;
    // A row's position is the rows of its template and day before it in storage order,
    // counted from the index: the pruned row groups' counts are added unread.
    let mut base = 0u64;
    let mut next = 0usize;
    // Each row group of the window: where it is in the day, and its first row's position.
    let mut window: Vec<(usize, u64)> = Vec::new();
    loop {
        // Asked before a window is looked at, and below before each row group of one
        // taken in turn: a scan told to stop reads no row group more.
        ask.opts.stop.check()?;
        window.clear();
        let (mut rows, mut bytes) = (0u64, 0u64);
        while next < stored.len() && window.len() < most {
            let s = &stored[next];
            let area = u64::from(s.entry.chunks_len);
            if !window.is_empty() && bytes + area > WINDOW_BYTES {
                break;
            }
            let here = base;
            base += u64::from(s.entry.row_count);
            next += 1;
            if windowed
                && !(ask.since.is_none_or(|from| s.entry.max_time >= from)
                    && ask.until.is_none_or(|to| s.entry.min_time < to))
            {
                out.row_groups_pruned += 1;
                continue;
            }
            rows += u64::from(s.entry.row_count);
            bytes += area;
            window.push((next - 1, here));
        }
        if window.is_empty() {
            break;
        }
        let turn = match fan {
            Some(fan) if window.len() >= 2 && rows >= fan.least_rows() => fan.turn(),
            _ => None,
        };
        if let Some(turn) = turn {
            together(view, ask, &window, &turn, batch, v, out, readers)?;
            continue;
        }
        for (at, here) in &window {
            ask.opts.stop.check()?;
            let s = &stored[*at];
            out.row_groups_read += 1;
            if !matches!(&shaped, Some((schema, _)) if Arc::ptr_eq(schema, &s.schema)) {
                shaped = None;
            }
            let (_, shape) =
                shaped.get_or_insert_with(|| (Arc::clone(&s.schema), ask.shape(&s.schema)));
            let reader = match readers.entry(s.file.path.clone()) {
                std::collections::hash_map::Entry::Occupied(slot) => slot.into_mut(),
                // One of the two places a data file is opened for reading and counted:
                // a file whose every row group the window pruned is not.
                std::collections::hash_map::Entry::Vacant(slot) => {
                    out.files_read += 1;
                    slot.insert(Reader::open(&s.file.path)?)
                }
            };
            let unit = Unit::read(s, &*shape, view.day, *here, reader, out)?;
            let sel = pick(&unit, ask)?;
            if !hand(&unit, ask, &sel, v, out)? {
                return Ok(false);
            }
        }
    }
    ask.opts.stop.check()?;
    Ok(tail(view, ask, base, v, out))
}

/// One window of a day with the pool, a batch at a time: read here, chosen on this thread
/// and the pool's, handed over here in storage order. The error is the first in that order.
#[allow(clippy::too_many_arguments)]
fn together(
    view: &View,
    ask: &Ask<'_>,
    window: &[(usize, u64)],
    turn: &Turn<'_>,
    batch: u64,
    v: &mut dyn Visitor,
    out: &mut Scan,
    readers: &mut HashMap<PathBuf, Reader>,
) -> Result<(), StoreError> {
    let stored = &view.stored[..];
    // The shapes of the window's schemas, made before a row group is read, because a
    // unit borrows its shape for as long as it lives.
    let mut shapes: Vec<(&Arc<Vec<String>>, Shape)> = Vec::new();
    let mut shape_of: Vec<usize> = Vec::with_capacity(window.len());
    for (at, _) in window {
        let schema = &stored[*at].schema;
        let known = shapes
            .iter()
            .position(|(have, _)| Arc::ptr_eq(have, schema));
        shape_of.push(known.unwrap_or_else(|| {
            shapes.push((schema, ask.shape(schema)));
            shapes.len() - 1
        }));
    }

    let mut rest = window.iter().zip(&shape_of);
    let mut stop: Option<StoreError> = None;
    // Whether the last batch was closed by its bytes: only then may the window have more.
    let mut cut = true;
    while cut {
        // A scan told to stop reads no batch more: the batches before this one were
        // handed over whole, and nothing of this one has been read.
        ask.opts.stop.check()?;
        cut = false;
        let mut slots: Vec<Mutex<Slot<'_>>> = Vec::new();
        let mut held = 0u64;
        for ((at, here), shape) in rest.by_ref() {
            let s = &stored[*at];
            out.row_groups_read += 1;
            let reader = match readers.entry(s.file.path.clone()) {
                std::collections::hash_map::Entry::Occupied(slot) => slot.into_mut(),
                // The other place a data file is opened for reading, and counted.
                std::collections::hash_map::Entry::Vacant(slot) => {
                    match Reader::open(&s.file.path) {
                        Ok(reader) => {
                            out.files_read += 1;
                            slot.insert(reader)
                        }
                        Err(e) => {
                            stop = Some(e.into());
                            break;
                        }
                    }
                }
            };
            match Unit::read(s, &shapes[*shape].1, view.day, *here, reader, out) {
                Ok(unit) => {
                    held += unit.inflates(ask);
                    slots.push(Mutex::new(Slot { unit, sel: None }));
                    if held >= batch {
                        cut = true;
                        break;
                    }
                }
                Err(e) => {
                    stop = Some(e);
                    break;
                }
            }
        }

        match &mut slots[..] {
            [] => break,
            [alone] => choose(alone.get_mut().unwrap_or_else(|e| e.into_inner()), ask),
            _ => turn.each(&slots, |slot| choose(slot, ask)),
        }

        for slot in slots {
            let Slot { unit, sel } = slot.into_inner().unwrap_or_else(|e| e.into_inner());
            // Every slot was chosen: `each` returns when every item is done. Were one not,
            // choosing it here is what a scan in turn does.
            let sel = match sel {
                Some(sel) => sel?,
                None => pick(&unit, ask)?,
            };
            let more = hand(&unit, ask, &sel, v, out)?;
            debug_assert!(more, "a scan with a row limit was handed to the pool");
        }
    }
    stop.map_or(Ok(()), Err)
}

/// What is done with one row group of a batch on whichever thread took it: its rows are
/// chosen, then its columns let go if nothing will be built from them, or warmed for a fetch.
fn choose(slot: &mut Slot<'_>, ask: &Ask<'_>) {
    let sel = pick(&slot.unit, ask);
    if let Ok(sel) = &sel {
        if ask.wants == Wants::Nothing || sel.is_empty() {
            for column in &mut slot.unit.columns {
                column.take();
            }
        } else {
            warm(&slot.unit, ask, sel);
        }
    }
    slot.sel = Some(sel);
}

/// The column a gated fetch's `hand` decodes first, decoded where the unit is and left in
/// it. An error here is dropped: `hand` meets the same error in its turn.
fn warm(unit: &Unit<'_>, ask: &Ask<'_>, sel: &Sel) {
    if sel.is_empty() || !ask.warms() {
        return;
    }
    if let Some(place) = unit.shape.ts.or(unit.shape.received) {
        let _ = unit.column(place);
    }
}

#[cfg(test)]
mod tests;
