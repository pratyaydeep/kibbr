//! One day of one leaf, read for a query: its row groups, then its rows not yet in one.
//!
//! **The filter comes before the row.** A scan used to decode every wanted chunk to one
//! printed cell a row, build a `Row` for every stored row, and only then ask the window
//! and the terms of it; nearly every row it built was a row it threw away. Here the window
//! and the terms are put to a row group's columns (`store::columns`), which gives the rows
//! that pass as one bit a row, and a row is built only for a row that passed and only as
//! far as the visitor reads it (`Wants`): no row for a count, the grouping's columns for a
//! grouping, the whole row for a fetch.
//!
//! **What is read off the disk does not move.** The same files are opened, and of each row
//! group the window does not prune the same header frame and the same chunks are read and
//! CRC-checked, every wanted chunk before any is decoded. So a scan counts the files, row
//! groups, bytes and rows it always counted, and a bad CRC in a chunk nothing went on to
//! use still fails the query. What moves is what happens to the bytes afterwards: a column
//! is decoded when the window, a term or a row being built first asks for it, and a value
//! is printed when a row holding it is built. A chunk nothing asks for is held to its CRC
//! and no further, and a value that does not print is refused by the query that prints it.
//!
//! **A term is put to typed values, not to printed cells** (`columns::hits`): an integer
//! against integers, a word against a word list's few words, an id against sixteen stored
//! bytes. What a term means is still said by `engine::value_matches` about the value a
//! rendered row holds, and the compare made here is held to that in three ways: by the
//! tests of `columns` and `engine`, by `slow_pick`, which chooses a row group's rows by
//! printing every cell and which every debug build runs beside `pick`, and by the memory
//! store, which still renders each row and asks it.
//!
//! **One stored row group is one unit** (`Unit`), and it owns what was read of it: its
//! header, its chunks' bytes, each column once decoded. Its work is in three parts that
//! need different things. Reading it needs the file. Choosing its rows (`pick`) needs the
//! unit and the question and nothing else - no reader, no visitor, no counter - so it can
//! be run wherever the unit is. Handing its rows over (`hand`) is the only part that
//! touches the visitor and the counters. A row's position is the rows of its day before
//! it, which the index gives before a byte is read, so no unit waits on another to know
//! where its rows are.
//!
//! **A visitor that keeps the newest few rows is asked before a row is built for it**
//! (`Visitor::wants_row`). A fetch with a limit keeps a few hundred rows of a day of a
//! million, and every other row it was handed was built to be dropped. So such a visitor
//! is told a matched row's time and position, which come off the time column alone, and
//! the row is built only if it says the row could be one it keeps. Rows are still asked
//! about in storage order, and every matched row is still counted. With each ask goes
//! whether the row's time lies in the day it is stored under: see `hand`.
//!
//! **The rows not yet in a row group stay rows** (`tail`). They were built when they
//! arrived, and turning a buffer into columns for each query would cost the very build
//! this saves. They are asked the same terms, on the stored row and without rendering it:
//! a row is rendered only once it has passed and is about to be handed over.

use std::borrow::Cow;
use std::cell::OnceCell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use super::{Stored, View};
use crate::store::columns::{hits, hits_by_row, instants, Instants, Sel};
use crate::store::v3::column::{Col, Column, Memo};
use crate::store::v3::file::{Reader, StoredChunk};
use crate::store::v3::read::{Header, ALL_NULL, SOME_NULL};
use crate::store::v3::{day_of, Bad};
use crate::store::{
    effective_ts, in_window, instant_of, nanos_of, render_row, render_text, Row, Scan, ScanOptions,
    StoreError, Term, Visitor, Want, Wants, DEFAULT_TS_COLUMN, RECEIVED_AT_COLUMN,
};
use crate::types::ColValue;

/// What one leaf's scan was asked, worked out once for the leaf: the options, the terms,
/// the window in nanoseconds, and what the visitor reads of a row.
///
/// Plain data. It holds no closure, no visitor and no counter, so it can be shared by
/// reference with whatever chooses a row group's rows.
pub(super) struct Ask<'a> {
    opts: &'a ScanOptions,
    terms: &'a [Term],
    /// For each term, whether its key is a column whose type is now `String`.
    term_is_text: Vec<bool>,
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
            term_is_text: terms.iter().map(|term| is_text(term.key())).collect(),
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

    /// Is the visitor asked about a row before the row is built? One that keeps the
    /// newest rows is, unless the scan is to stop at a number of rows: that is the first
    /// so many in storage order, and every one of them is a row the visitor is owed.
    fn gated(&self) -> bool {
        self.wants == Wants::Newest && self.opts.limit == 0
    }

    /// Where a schema's columns are for this question.
    fn shape(&self, schema: &[String]) -> Shape {
        let windowed = self.windowed();
        let columns = &self.opts.columns;
        // Empty means every column: a `MATCH` returns whole rows. A window always reads
        // the time column, because it filters on it; and a term's column is read because
        // the term is asked of it, whether or not the caller named it too.
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

    /// Does a row not yet in a row group pass? The window on the stored row, as it is
    /// asked of a stored row everywhere, and then each term - of the stored row too, read
    /// as the rendered one would be, so that no row is rendered to be thrown away.
    fn admits(&self, row: &Row) -> bool {
        if self.windowed() && !in_window(effective_ts(row, self.ts), self.since, self.until) {
            return false;
        }
        self.terms
            .iter()
            .zip(&self.term_is_text)
            .all(|(term, text)| term.matches_in(row, *text))
    }

    /// The effective time of a row not yet in a row group as the row rendered would show
    /// it, read without rendering it: of the time column and then of the arrival time,
    /// the first that holds an instant, a date in a column that is now text being read as
    /// the text it arrived as. `effective_ts` of `render_row` of the row, and no row made.
    fn seen_ts_of(&self, row: &Row) -> Option<i64> {
        let columns = [
            (self.ts, self.ts_is_text),
            (RECEIVED_AT_COLUMN, self.received_is_text),
        ];
        columns.into_iter().find_map(|(column, text)| {
            let value = row.get(column)?;
            match value {
                ColValue::Datetime(_) if text => {
                    instant_of(&render_text(value, row.written(column))?)
                }
                _ => instant_of(value),
            }
        })
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

/// One stored row group as a scan holds it.
///
/// **It owns what was read of it** - the header, each wanted chunk's stored bytes, each
/// column once something has decoded it - and borrows only what outlives the scan of its
/// day: its entry in the frozen view, and the shape made for its schema. So it can be
/// read in one place and its rows chosen and handed over in another.
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
        Ok(Self {
            at,
            shape,
            day,
            base,
            rows: header.row_count as usize,
            columns: stored.iter().map(|_| OnceCell::new()).collect(),
            header,
            stored,
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
        Column::decode(&chunk.bytes, chunk.at, dir, self.rows).map_err(|bad| self.refused(bad))
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

    /// Each row's effective time, off the stored columns: the time column's instant, and
    /// the arrival time's for a row that has none there. The arrival time is decoded only
    /// when some row needs it.
    fn times(&self) -> Result<Instants<'_>, StoreError> {
        let ts = match self.shape.ts {
            Some(place) => instants(self.column(place)?).map_err(|bad| self.refused(bad))?,
            None => Instants::absent(self.rows),
        };
        match self.shape.received {
            Some(place) if ts.missing() && self.shape.ts != Some(place) => {
                let received = instants(self.column(place)?).map_err(|bad| self.refused(bad))?;
                Ok(ts.or(&received))
            }
            _ => Ok(ts),
        }
    }

    /// The effective time of row `r` as the row built of it would show it, for a scan
    /// one of whose time columns is now text. `times` reads the stored value, and a date
    /// in such a column is shown as the text it arrived as, which is what the visitor
    /// will read the row's time from. So the one cell is printed and rendered as the row
    /// would render it, and read as a row's value is read: the time column's, and the
    /// arrival time's where that holds no instant.
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
        Ok(None)
    }
}

/// The rows of one row group, built one at a time for a visitor.
///
/// The columns a row is built of are decoded when the first row is built and not before,
/// so a row group none of whose rows is built has none of them decoded: for a visitor
/// that is asked first, that is most row groups.
struct Builder<'u> {
    unit: &'u Unit<'u>,
    ask: &'u Ask<'u>,
    /// One for each of `shape.row`: the column, and what was printed of it so far. A word
    /// or a constant is printed once for the row group and copied after, as it was when
    /// every chunk was printed whole.
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
        let seen = match render_row(&self.row, &self.ask.opts.text_columns) {
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

/// Which rows of a row group pass the window and the terms.
///
/// A function of the unit and the question alone. It decodes, inside the unit, the
/// columns the window and the terms read, and stops at the first term that leaves no row:
/// a later term's column is then not decoded at all.
///
/// A term is put to its column's typed values (`columns::hits`) and no cell is printed.
/// **A debug build then chooses the rows a second time the plain way** (`slow_pick`) and
/// stops if the two differ, so every scan a test runs is also a check of every compare it
/// made. A release build chooses them once.
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
    for ((term, place), text) in terms.zip(&ask.term_is_text) {
        if sel.is_empty() {
            break;
        }
        // A row without the term's key matches nothing, `*` included.
        let Some(place) = *place else {
            sel = Sel::none(rows);
            break;
        };
        // `*` is "present and not null", and a chunk's directory entry says that of every
        // row at once when it can: no row of an all-null chunk, and every row of a chunk
        // with no presence bitmap. Neither is decoded to find out.
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
        let hits = hits(unit.column(place)?, term, *text).map_err(|bad| unit.refused(bad))?;
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

/// `pick` by definition: every row's effective time read off its printed cells and asked
/// the window, and every term asked of every row's printed cell, with no shortcut and no
/// early way out.
///
/// What `pick` is held to. It decodes the chunks again for itself and leaves the unit as
/// it found it, so that checking a scan does not change what the scan goes on to decode.
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
            if in_window(at, ask.since, ask.until) {
                inside.set(row);
            }
        }
        sel.and(&inside);
    }
    let terms = ask.terms.iter().zip(&unit.shape.terms);
    for ((term, place), text) in terms.zip(&ask.term_is_text) {
        let Some(place) = *place else {
            sel = Sel::none(rows);
            continue;
        };
        let column = unit.decode(place)?;
        let hits = hits_by_row(unit.over(&column, place)?, term, *text)
            .map_err(|bad| unit.refused(bad))?;
        sel.and(&hits);
    }
    Ok(sel)
}

/// The chosen rows of a row group to the visitor, and what the row group adds to the
/// scan's counters. `false` once the limit is reached.
///
/// **Counted before anything is built.** `matched` is the rows that passed, up to the
/// limit, and `scanned` the rows looked at to find them: every row of a row group read to
/// its end, and the rows up to the last one taken where the limit stopped the scan. Both
/// are what a walk of the rows one at a time counted, and neither depends on what the
/// visitor reads or on which rows it takes.
///
/// **A visitor that keeps the newest rows is asked about each row first**, in storage
/// order, with the time the built row would show, and the row is built only on a yes.
/// When neither time column is text that time is the stored one, read off the time column
/// for the whole row group; when one is, it is read a row at a time as the row would
/// render it (`Unit::seen_ts`). A row group with no chosen row decodes no time column for
/// this, and one with no row taken decodes no other column at all.
///
/// With the time goes whether it lies in the day the row group is stored under. A row is
/// filed under the day of its time as it was read when the row arrived, and it is ranked
/// by its time as it is read now. The two are one reading in a store written and read
/// under one setting of the ladder. Where they are not - ten digits stored as an instant
/// and read, under a narrower epoch window, as no instant at all - two rows of different
/// days can come to one time and one position, and which of them a visitor keeps then
/// depends on every row it was offered and in what order. So the visitor is told, and
/// from the first such row on it takes every row, as it was always handed them.
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
    let stored = if ask.ts_is_text || ask.received_is_text {
        None
    } else {
        Some(unit.times()?)
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

/// The rows not yet in a row group - moved aside first, then buffered - after the stored
/// ones, because that is the position they will have once written. `base` is the day's
/// stored rows. `false` once the limit is reached.
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
            // A visitor that keeps the newest rows is asked first here too, with the time
            // the rendered row would show, so that a row it has no use for is not
            // rendered; and told whether that time is in this day, as `hand` tells it.
            let asked = gated.then(|| ask.seen_ts_of(&b.row));
            if asked.is_none_or(|ts| v.wants_row(ts, seq, day_of(ts.unwrap_or(0)) == view.day)) {
                // A whole row, whatever columns the visitor named: it is a row already.
                let rendered = render_row(&b.row, &ask.opts.text_columns);
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

/// One day's rows - stored, then in memory - handed to the visitor. `false` once the
/// limit is reached.
pub(super) fn day(
    view: &View,
    ask: &Ask<'_>,
    v: &mut dyn Visitor,
    out: &mut Scan,
    readers: &mut HashMap<PathBuf, Reader>,
) -> Result<bool, StoreError> {
    let windowed = ask.windowed();
    let mut shaped: Option<(Arc<Vec<String>>, Shape)> = None;
    // A row's position is the rows of its template and day before it in storage order,
    // counted from the index: the pruned row groups' counts are added unread.
    let mut base = 0u64;
    for s in view.stored.iter() {
        let here = base;
        base += u64::from(s.entry.row_count);
        if windowed
            && !(ask.since.is_none_or(|from| s.entry.max_time >= from)
                && ask.until.is_none_or(|to| s.entry.min_time < to))
        {
            out.row_groups_pruned += 1;
            continue;
        }
        out.row_groups_read += 1;
        if !matches!(&shaped, Some((schema, _)) if Arc::ptr_eq(schema, &s.schema)) {
            shaped = None;
        }
        let (_, shape) =
            shaped.get_or_insert_with(|| (Arc::clone(&s.schema), ask.shape(&s.schema)));
        let reader = match readers.entry(s.file.path.clone()) {
            std::collections::hash_map::Entry::Occupied(slot) => slot.into_mut(),
            // The one place a data file is opened for reading, so the one place that
            // counts them: a file whose every row group the window pruned is not.
            std::collections::hash_map::Entry::Vacant(slot) => {
                out.files_read += 1;
                slot.insert(Reader::open(&s.file.path)?)
            }
        };
        let unit = Unit::read(s, &*shape, view.day, here, reader, out)?;
        let sel = pick(&unit, ask)?;
        if !hand(&unit, ask, &sel, v, out)? {
            return Ok(false);
        }
    }
    Ok(tail(view, ask, base, v, out))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::{Duration, SystemTime};

    use super::super::tests::{counters, slow_visit, store_with, thresholds, Scratch, T0};
    use super::super::{DiskLeaf, DiskStore, Frozen};
    use super::*;
    use crate::store::v3::read::TWO_PIECES;
    use crate::store::v3::{day_of, samples, Form, DAY_NS};
    use crate::store::{project, FlushKind, LeafStore, Store};
    use crate::types::ColValue;

    const SECOND: i64 = 1_000_000_000;

    /// The rows of a row group, and of the older day: three row groups, five rows moved
    /// aside and five buffered.
    const UNIT: usize = 16;
    const OLDER: usize = 3 * UNIT + 10;
    /// The newer day: one row group and two rows buffered.
    const NEWER: usize = UNIT + 2;

    /// One row of the leaf `mixed` builds, in one of three shapes. `at` is the row's time
    /// as its line writes it, `n` its number in its day.
    ///
    /// - Shape 0 has no `extra` key, a `note` that is null, and a `when` that is a date
    ///   written with an offset.
    /// - Shape 1 has a `level` that is a word, a number or a null by turns, a `note` that
    ///   is there every other row, and a `when` that is a date or, every fifth row, `N/A`.
    /// - Shape 2 has a `level` that is always the number 30, spelled `0030` every eighth
    ///   row, and a `when` in epoch seconds; and one row in four has no time of its own
    ///   and carries its arrival time instead, `arrived`.
    fn shaped(shape: usize, at: &str, n: usize, arrived: i64) -> Row {
        let line = match shape {
            0 => format!(
                r#"{{"ts":"{at}","n":{n},"level":"{}","note":null,"when":"2024-01-15T16:00:{n:02}+05:30"}}"#,
                if n.is_multiple_of(3) { "WARN" } else { "INFO" }
            ),
            1 => format!(
                r#"{{"ts":"{at}","n":{n},"level":{},"note":{},"when":{},"extra":"e{}"}}"#,
                [r#""INFO""#, "null", "30", "30"][n % 4],
                if n.is_multiple_of(2) {
                    r#""x""#
                } else {
                    "null"
                },
                if n.is_multiple_of(5) {
                    r#""N/A""#.to_string()
                } else {
                    format!(r#""2024-01-15T10:30:{n:02}Z""#)
                },
                n % 2
            ),
            _ => format!(
                r#"{{"ts":"{}","n":{n},"level":{},"note":"y","when":{},"extra":"e2"}}"#,
                if n % 4 == 1 { "N/A" } else { at },
                if n.is_multiple_of(8) {
                    r#""0030""#
                } else {
                    "30"
                },
                1_705_314_600 + n
            ),
        };
        let mut row = samples::row(&line);
        if shape == 2 && n % 4 == 1 {
            row.insert(RECEIVED_AT_COLUMN.into(), ColValue::Datetime(arrived));
        }
        row
    }

    /// Row `n` of the older day, at `T0 + n` seconds: a row group of each shape, and then
    /// the rows in memory, of the three shapes by turns.
    fn older(n: usize) -> Row {
        let shape = if n < 3 * UNIT { n / UNIT } else { n % 3 };
        shaped(
            shape,
            &format!("2024-01-15T10:30:{n:02}Z"),
            n,
            T0 + n as i64 * SECOND,
        )
    }

    /// Row `n` of the newer day, a day later to the second.
    fn newer(n: usize) -> Row {
        shaped(
            1,
            &format!("2024-01-16T10:30:{n:02}Z"),
            n,
            T0 + DAY_NS + n as i64 * SECOND,
        )
    }

    /// One template over two days, with every kind of column a scan has to put a term to.
    ///
    /// The older day is three row groups of sixteen rows - `older` says what is in each -
    /// and, when `in_memory`, five rows a flush has moved aside and five still buffered.
    /// The newer day is one row group, and two buffered rows when `in_memory`. So there is
    /// a column that is null in every row of a row group, a column in two pieces with nulls
    /// beside them, a date in three spellings in a column a query may be told is text, a
    /// time column some rows of which hold no time, and a key one row group's schema does
    /// not have.
    fn mixed(scratch: &Scratch, in_memory: bool) -> (DiskStore, Arc<DiskLeaf>) {
        let s = store_with(scratch, thresholds(UNIT, usize::MAX));
        let opened = s.open(7, "ts").expect("leaf");
        for n in 0..3 * UNIT {
            opened.append(older(n));
        }
        for n in 0..UNIT {
            opened.append(newer(n));
        }
        let done = s.flush(FlushKind::Tick);
        assert!(done.failed.is_empty(), "{:?}", done.failed);
        assert_eq!((done.rows, done.row_groups), (4 * UNIT as u64, 4));

        let leaf = Arc::clone(&s.leaves.read().expect("leaves")[&7]);
        if in_memory {
            for n in 3 * UNIT..3 * UNIT + 5 {
                leaf.append(older(n));
            }
            // The first hold of a flush and not its second: the rows are moved aside and
            // stay there, as they are while the flush is encoding and writing them.
            assert!(
                leaf.move_day(day_of(T0)).is_some(),
                "nothing was moved aside"
            );
            for n in 3 * UNIT + 5..OLDER {
                leaf.append(older(n));
            }
            for n in UNIT..NEWER {
                leaf.append(newer(n));
            }
        }
        (s, leaf)
    }

    /// A stored row group of a frozen leaf with its header, for a test that looks inside.
    fn headers(frozen: &Frozen) -> Vec<(&Stored, Header)> {
        let mut out = Vec::new();
        for view in &frozen.views {
            for s in view.stored.iter() {
                let mut reader = Reader::open(&s.file.path).expect("open");
                let header = reader.header(&s.entry, s.schema.len()).expect("header");
                out.push((s, header));
            }
        }
        out
    }

    /// The leaf holds the layouts it was built to hold. Without this a change to what the
    /// builder makes of these lines would leave the tests below passing over less.
    #[test]
    fn the_mixed_leaf_holds_the_layouts_it_is_for() {
        let scratch = Scratch::new("scan-layouts");
        let (_s, leaf) = mixed(&scratch, true);
        let frozen = leaf.freeze(None, None);
        let mut seen = BTreeSet::new();
        let mut schemas = BTreeSet::new();
        for (s, header) in headers(&frozen) {
            schemas.insert(s.schema.len());
            for (name, dir) in s.schema.iter().zip(&header.chunks) {
                let form = dir.a.map(|piece| piece.form);
                let what = match (name.as_str(), dir.layout, form) {
                    ("note", ALL_NULL, _) => "a column null in every row",
                    ("level", layout, _) if layout == TWO_PIECES | SOME_NULL => {
                        "two pieces beside nulls"
                    }
                    ("when", _, Some(Form::Datetime { .. })) if dir.b.is_none() => {
                        "a date stored as a date"
                    }
                    ("when", layout, _) if layout & TWO_PIECES != 0 => "a date beside text",
                    ("ts", layout, _) if layout & TWO_PIECES != 0 => "a time column in two pieces",
                    (RECEIVED_AT_COLUMN, layout, _) if layout & SOME_NULL != 0 => {
                        "an arrival time some rows have"
                    }
                    _ => continue,
                };
                seen.insert(what);
            }
        }
        assert_eq!(
            seen.into_iter().collect::<Vec<_>>(),
            [
                "a column null in every row",
                "a date beside text",
                "a date stored as a date",
                "a time column in two pieces",
                "an arrival time some rows have",
                "two pieces beside nulls",
            ]
        );
        // One row group has no `extra`, and one has an arrival time: three schemas.
        assert_eq!(schemas.len(), 3, "{schemas:?}");
        let state = leaf.state();
        let part = &state.days[&day_of(T0)];
        assert_eq!(
            (
                part.stored.len(),
                part.closing.as_ref().map_or(0, |batch| batch.rows.len()),
                part.buffer.len(),
            ),
            (3, 5, 5)
        );
    }

    /// Lists of terms to ask the mixed leaf, each with whether some row of it passes them
    /// when nothing else is asked.
    fn term_lists() -> Vec<(Vec<Term>, bool)> {
        vec![
            (Vec::new(), true),
            (vec![Term::exact("level", "INFO")], true),
            // A number in a column that also holds words, bare and padded.
            (vec![Term::exact("level", "30")], true),
            (vec![Term::exact("level", "0030")], true),
            (vec![Term::exact("level", "info")], false),
            (vec![Term::any("level")], true),
            // Null in every row of one row group, in some rows of another.
            (vec![Term::any("note")], true),
            (vec![Term::exact("note", "x")], true),
            // A key one row group's schema does not have, and one no schema has.
            (vec![Term::any("extra")], true),
            (vec![Term::exact("extra", "e1")], true),
            (vec![Term::any("absent")], false),
            // One instant of `when` as each row group spells it, and as none does. Which
            // of them finds a row depends on whether the column is text.
            (vec![Term::exact("when", "2024-01-15T16:00:07+05:30")], true),
            (vec![Term::exact("when", "2024-01-15T10:30:07Z")], true),
            (vec![Term::exact("when", "2024-01-15T10:30:21Z")], true),
            (vec![Term::exact("when", "1705314640")], true),
            // A row in memory by another spelling of its instant: found while `when` is a
            // date, and not once it is text.
            (vec![Term::exact("when", "2024-01-15T10:30:51Z")], true),
            (vec![Term::exact("when", "N/A")], true),
            (vec![Term::any("when")], true),
            // The time column, by an instant and by the text of a row that has none.
            (vec![Term::exact("ts", "2024-01-15T10:30:05Z")], true),
            (vec![Term::exact("ts", "N/A")], true),
            (vec![Term::exact("n", "7")], true),
            // Two terms: the second is asked only of what the first left.
            (vec![Term::any("note"), Term::exact("level", "30")], true),
            (
                vec![Term::exact("level", "INFO"), Term::exact("n", "3")],
                false,
            ),
            (
                vec![Term::any("absent"), Term::exact("level", "INFO")],
                false,
            ),
        ]
    }

    /// Windows over the mixed leaf, in nanoseconds: none; both days whole; one that cuts
    /// the older day and holds the newer whole; one that cuts the older day's second and
    /// third row groups and prunes its first; one that holds only rows in memory; the
    /// newer day alone; and one with no beginning.
    fn windows() -> Vec<(Option<i64>, Option<i64>)> {
        let at = |second: i64| Some(T0 + second * SECOND);
        vec![
            (None, None),
            (at(0), Some(T0 + 2 * DAY_NS)),
            (at(20), Some(T0 + 2 * DAY_NS)),
            (at(20), at(40)),
            (at(50), at(56)),
            (Some(T0 + DAY_NS), None),
            (None, at(10)),
        ]
    }

    fn instant(ns: i64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_nanos(ns as u64)
    }

    /// Every way the tests below ask the mixed leaf: each list of terms, under each
    /// window, with `when` and the time column text and not, reading every column and
    /// reading the ones a query that returns no rows would name.
    fn asks() -> Vec<(ScanOptions, Vec<Term>)> {
        let mut out = Vec::new();
        for (terms, _) in term_lists() {
            for (since, until) in windows() {
                for text_columns in [
                    vec![],
                    vec!["when".to_string()],
                    vec!["ts".into(), "when".into()],
                ] {
                    let mut named: Vec<String> =
                        terms.iter().map(|t| t.key().to_string()).collect();
                    named.extend(["level".to_string(), "ts".into(), RECEIVED_AT_COLUMN.into()]);
                    named.sort();
                    named.dedup();
                    for columns in [Vec::new(), named] {
                        out.push((
                            ScanOptions {
                                columns,
                                since: since.map(instant),
                                until: until.map(instant),
                                ts_column: Some("ts".into()),
                                text_columns: text_columns.clone(),
                                ..ScanOptions::default()
                            },
                            terms.clone(),
                        ));
                    }
                }
            }
        }
        out
    }

    /// A visitor that keeps what it is handed, says it reads what it was told to say, and
    /// takes every day or declines every day. Asked about a row before the row is built,
    /// it keeps what it was asked with, and has no use for the rows `refuses` names.
    struct Keeps {
        wants: Wants,
        days: bool,
        rows: Vec<(Row, u64)>,
        /// Each row it was asked about: its time, its position, and whether the store
        /// said that time is in the day the row is stored under.
        asked: Vec<(Option<i64>, u64, bool)>,
        refuses: fn(u64) -> bool,
    }

    impl Keeps {
        fn new(wants: Wants, days: bool) -> Self {
            Self {
                wants,
                days,
                rows: Vec::new(),
                asked: Vec::new(),
                refuses: |_| false,
            }
        }

        /// One that keeps the newest rows, and so is asked first, and refuses some.
        fn refusing(days: bool, refuses: fn(u64) -> bool) -> Self {
            Self {
                refuses,
                ..Self::new(Wants::Newest, days)
            }
        }
    }

    /// Every third position: the rows a visitor that refuses some has no use for.
    fn third(seq: u64) -> bool {
        seq % 3 == 1
    }

    impl Visitor for Keeps {
        fn row(&mut self, row: &Row, seq: u64) {
            assert!(
                self.wants != Wants::Nothing,
                "a visitor that reads nothing was handed a row"
            );
            // A visitor that is asked first is handed a row straight after its ask, and
            // the row shows the time it was asked with. Every test here names the time
            // column `ts`.
            if let Some((ts, asked, _)) = self.asked.last() {
                assert_eq!(
                    (effective_ts(row, "ts"), seq),
                    (*ts, *asked),
                    "handed {row:?}"
                );
            }
            self.rows.push((row.clone(), seq));
        }

        fn wants_partition(&mut self, _: Option<i64>) -> bool {
            self.days
        }

        fn wants(&self) -> Wants {
            self.wants.clone()
        }

        fn wants_row(&mut self, ts: Option<i64>, seq: u64, placed: bool) -> bool {
            assert_eq!(
                self.wants,
                Wants::Newest,
                "a visitor that did not ask to be asked first was asked about a row"
            );
            self.asked.push((ts, seq, placed));
            !(self.refuses)(seq)
        }
    }

    /// What a visitor that keeps the newest rows was asked and handed, against the rows
    /// the plain scan handed over, `plain`.
    ///
    /// `asked_first`: the scan had no number of rows to stop at. It was then asked about
    /// every row that passed, in storage order, with the time that row shows and with
    /// `placed` true - nothing here reads a row's time otherwise than it was stored - and
    /// it was handed the rows it did not refuse and no others. Otherwise it was asked
    /// about none and handed them all.
    fn held_to_plain(v: &Keeps, plain: &[(Row, u64)], asked_first: bool, said: &str) {
        let asked: Vec<(Option<i64>, u64, bool)> = plain
            .iter()
            .filter(|_| asked_first)
            .map(|(row, seq)| (effective_ts(row, "ts"), *seq, true))
            .collect();
        assert_eq!(v.asked, asked, "{said}");
        let handed: Vec<&(Row, u64)> = plain
            .iter()
            .filter(|(_, seq)| !(asked_first && (v.refuses)(*seq)))
            .collect();
        assert_eq!(v.rows.iter().collect::<Vec<_>>(), handed, "{said}");
    }

    /// What a grouping would name: a column every row group has, the two time columns, one
    /// some schemas lack and one no schema has.
    fn named() -> Vec<String> {
        ["absent", "extra", "level", "ts", RECEIVED_AT_COLUMN]
            .map(String::from)
            .to_vec()
    }

    /// **The scan answers what the plain scan answers**, whatever the visitor reads.
    ///
    /// Over a leaf with every layout, for every list of terms under every window, stopped
    /// by a limit and not: a visitor that reads nothing, one that reads a few columns, one
    /// that reads whole rows and one that keeps the newest are each run beside the plain
    /// scan, which builds every row and asks nobody what they read. All six counters must
    /// be equal - the bytes among them, so nothing more and nothing less is read off the
    /// disk - and so must the rows, their positions and their order: whole for the last
    /// two visitors, and for the one that named columns, in those columns. A visitor that
    /// reads nothing is handed nothing, and its count is still the count.
    ///
    /// The one that keeps the newest is asked about each row first where the scan has no
    /// number of rows to stop at. It is run taking every row and refusing every third:
    /// it must be asked about exactly the rows the plain scan hands over, in that order,
    /// each with the time the row shows, and handed the ones it took. What it refuses is
    /// still counted, so the counters are the plain scan's either way.
    #[test]
    fn the_scan_answers_what_the_plain_scan_answers_whatever_is_read() {
        let scratch = Scratch::new("scan-plain");
        let (_s, leaf) = mixed(&scratch, true);
        let mut found = BTreeSet::new();
        let (mut scans, mut asked, mut refused) = (0usize, 0usize, 0usize);
        for (at, (asking, terms)) in asks().into_iter().enumerate() {
            let frozen = leaf.freeze(asking.since.map(nanos_of), asking.until.map(nanos_of));
            // No limit; the first row; a few rows; and one more row than the newer day has
            // stored, which stops the scan on a row in memory.
            for limit in [0, 1, 5, UNIT + 1] {
                let opts = ScanOptions {
                    limit,
                    ..asking.clone()
                };
                // The visitors, whether each takes the days it is offered, and the rows
                // each refuses when it is asked first. One that
                // declines a day still has it counted: free when the day is whole inside
                // the window and there is no term, and read otherwise.
                let none: fn(u64) -> bool = |_| false;
                let visitors = [
                    (Wants::Nothing, true, none),
                    (Wants::Columns(named()), true, none),
                    (Wants::Rows, true, none),
                    (Wants::Newest, true, none),
                    (Wants::Newest, true, third),
                    (Wants::Nothing, false, none),
                    (Wants::Rows, false, none),
                    (Wants::Newest, false, third),
                ];
                for (wants, days, refuses) in visitors {
                    let said = format!("{terms:?} with {opts:?}, read as {wants:?}, days {days}");
                    let mut plain = Keeps::new(Wants::Rows, days);
                    let slow = slow_visit(&frozen, &opts, &terms, &mut plain).expect("plain scan");
                    let mut v = Keeps {
                        refuses,
                        ..Keeps::new(wants.clone(), days)
                    };
                    let scan = frozen.visit(&opts, &terms, &mut v).expect("scan");
                    assert_eq!(counters(&scan), counters(&slow), "{said}");
                    match &wants {
                        Wants::Nothing => assert!(v.rows.is_empty(), "{said}"),
                        Wants::Rows => assert_eq!(v.rows, plain.rows, "{said}"),
                        Wants::Newest => {
                            held_to_plain(&v, &plain.rows, limit == 0, &said);
                            asked += v.asked.len();
                            refused += v.asked.len() - v.rows.len().min(v.asked.len());
                        }
                        Wants::Columns(names) => {
                            let narrow = |rows: &[(Row, u64)]| -> Vec<(Row, u64)> {
                                rows.iter()
                                    .map(|(row, seq)| (project(row, names), *seq))
                                    .collect()
                            };
                            assert_eq!(narrow(&v.rows), narrow(&plain.rows), "{said}");
                        }
                    }
                    scans += 1;
                    if limit == 0 && days && scan.matched > 0 && asking.since.is_none() {
                        found.insert(at / (windows().len() * 3 * 2));
                    }
                }
            }
        }
        // Not vacuous: the lists of terms meant to find a row found one, and no other did.
        let meant: BTreeSet<usize> = term_lists()
            .iter()
            .enumerate()
            .filter_map(|(at, (_, some))| some.then_some(at))
            .collect();
        assert_eq!(found, meant);
        assert!(scans > 30_000, "{scans}");
        // And the visitor that is asked first was asked, and refused, by the thousand.
        assert!(asked > 20_000 && refused > 5_000, "{asked} {refused}");
    }

    /// One template and one day whose time column a query may be told is text, written
    /// every way a line writes a time: a date ending in `Z`, the same kind of date with
    /// an offset, epoch seconds as a number and as a string of digits, and - one row in
    /// seven - no time at all, the row going by its arrival time. Two row groups of
    /// sixteen rows, then five rows moved aside and five buffered.
    fn spelled(scratch: &Scratch) -> (DiskStore, Arc<DiskLeaf>) {
        let line = |n: usize| {
            let secs = 1_705_314_600 + n;
            let ts = match n % 7 {
                1 | 4 => format!(r#""2024-01-15T16:00:{n:02}+05:30""#),
                2 => secs.to_string(),
                5 => format!(r#""{secs}""#),
                6 => r#""N/A""#.to_string(),
                _ => format!(r#""2024-01-15T10:30:{n:02}Z""#),
            };
            let mut row = samples::row(&format!(r#"{{"ts":{ts},"n":{n}}}"#));
            if n % 7 == 6 {
                row.insert(
                    RECEIVED_AT_COLUMN.into(),
                    ColValue::Datetime(T0 + n as i64 * SECOND),
                );
            }
            row
        };
        let s = store_with(scratch, thresholds(UNIT, usize::MAX));
        let opened = s.open(9, "ts").expect("leaf");
        for n in 0..2 * UNIT {
            opened.append(line(n));
        }
        let done = s.flush(FlushKind::Tick);
        assert!(done.failed.is_empty(), "{:?}", done.failed);
        assert_eq!((done.rows, done.row_groups), (2 * UNIT as u64, 2));
        let leaf = Arc::clone(&s.leaves.read().expect("leaves")[&9]);
        for n in 2 * UNIT..2 * UNIT + 5 {
            leaf.append(line(n));
        }
        assert!(
            leaf.move_day(day_of(T0)).is_some(),
            "nothing was moved aside"
        );
        for n in 2 * UNIT + 5..2 * UNIT + 10 {
            leaf.append(line(n));
        }
        (s, leaf)
    }

    /// **A row is asked about with the time it then shows, and in the day it is stored
    /// under.** A visitor that keeps the newest rows decides on a row from the time it is
    /// asked with and ranks the row by the time it reads off it. Were those ever two
    /// times, a row that belongs in an answer would go unbuilt, and only under a limit,
    /// only once the heap is full.
    ///
    /// Over the mixed leaf and over one whose time column is written five ways, with the
    /// time column text and not, under terms and windows: every row the visitor is handed
    /// shows the time it was asked with (the visitor itself checks each one), in a row
    /// group and in memory alike; it is asked about every row that passed, and handed the
    /// ones it took; and **every row is `placed`**. A store written and read in one
    /// process reads no time two ways, so a visitor is never told to stop trusting the
    /// order - which would cost a fetch nothing but all of its speed, silently.
    #[test]
    fn a_row_is_asked_about_with_the_time_it_shows_and_in_the_day_it_is_stored_under() {
        let scratch = Scratch::new("scan-asked");
        let (_m, mixed) = mixed(&scratch, true);
        let other = Scratch::new("scan-asked-spelled");
        let (_s, spelled) = spelled(&other);

        // The second leaf's questions: nothing, a term, a window that cuts both row
        // groups and the rows in memory, and both; each with the time column a date and
        // with it text.
        let mut spelled_asks = Vec::new();
        for terms in [vec![], vec![Term::any("n")], vec![Term::exact("n", "0012")]] {
            for (since, until) in [
                (None, None),
                (Some(T0 + 9 * SECOND), Some(T0 + 39 * SECOND)),
            ] {
                for text_columns in [vec![], vec!["ts".to_string()]] {
                    spelled_asks.push((
                        ScanOptions {
                            since: since.map(instant),
                            until: until.map(instant),
                            ts_column: Some("ts".into()),
                            text_columns,
                            ..ScanOptions::default()
                        },
                        terms.clone(),
                    ));
                }
            }
        }

        // What the rows handed over showed as their time, where the time column is text:
        // each way of writing one, by how its text ends.
        let mut shown: BTreeSet<&str> = BTreeSet::new();
        let (mut asked, mut text, mut in_memory, mut by_arrival) = (0usize, 0usize, 0usize, 0);
        for (leaf, stored, asks) in [
            (&mixed, 3 * UNIT as u64, asks()),
            (&spelled, 2 * UNIT as u64, spelled_asks),
        ] {
            for (opts, terms) in asks {
                let frozen = leaf.freeze(opts.since.map(nanos_of), opts.until.map(nanos_of));
                let said = format!("{terms:?} with {opts:?}");
                let mut plain = Keeps::new(Wants::Rows, true);
                let slow = slow_visit(&frozen, &opts, &terms, &mut plain).expect("plain scan");
                for refuses in [|_| false, third] {
                    let mut v = Keeps::refusing(true, refuses);
                    let scan = frozen.visit(&opts, &terms, &mut v).expect("scan");
                    assert_eq!(counters(&scan), counters(&slow), "{said}");
                    assert_eq!(v.asked.len() as u64, scan.matched, "{said}");
                    assert!(v.asked.iter().all(|(_, _, placed)| *placed), "{said}");
                    held_to_plain(&v, &plain.rows, true, &said);
                    asked += v.asked.len();
                }
                let ts_is_text = opts.text_columns.iter().any(|c| c == "ts");
                for (row, seq) in &plain.rows {
                    // The older day of the mixed leaf, and the one day of the other, hold
                    // rows in memory after their stored ones; a position says which.
                    let older = effective_ts(row, "ts").is_some_and(|ns| ns < T0 + DAY_NS);
                    in_memory += usize::from(older && *seq >= stored);
                    match row.get("ts") {
                        Some(ColValue::String(at)) if ts_is_text => {
                            text += 1;
                            shown.insert(match at.as_bytes().last() {
                                Some(b'Z') => "a date ending in Z",
                                Some(b'0') if at.ends_with("+05:30") => "a date with an offset",
                                Some(b'A') => {
                                    by_arrival += 1;
                                    "no time"
                                }
                                _ => "digits",
                            });
                        }
                        _ => {}
                    }
                }
            }
        }
        assert_eq!(
            shown.into_iter().collect::<Vec<_>>(),
            [
                "a date ending in Z",
                "a date with an offset",
                "digits",
                "no time"
            ]
        );
        assert!(
            asked > 20_000 && text > 2_000 && in_memory > 1_000 && by_arrival > 200,
            "{asked} {text} {in_memory} {by_arrival}"
        );
    }

    /// A term's column is read because the term is asked of it, whether or not the caller
    /// named it among the columns to read: naming columns says what a row should hold, and
    /// cannot make a term match nothing.
    #[test]
    fn a_term_reads_its_column_whether_or_not_it_was_named() {
        let scratch = Scratch::new("scan-unnamed");
        let (_s, leaf) = mixed(&scratch, true);
        let terms = [Term::exact("level", "INFO")];
        let every = ScanOptions {
            ts_column: Some("ts".into()),
            ..ScanOptions::default()
        };
        let only_ts = ScanOptions {
            columns: vec!["ts".into()],
            ..every.clone()
        };
        let mut whole = Keeps::new(Wants::Rows, true);
        let all = leaf.visit(&every, &terms, &mut whole).expect("scan");
        let mut narrow = Keeps::new(Wants::Rows, true);
        let some = leaf.visit(&only_ts, &terms, &mut narrow).expect("scan");
        assert!(all.matched > 0);
        assert_eq!((some.matched, some.scanned), (all.matched, all.scanned));
        let seqs = |rows: &[(Row, u64)]| rows.iter().map(|(_, seq)| *seq).collect::<Vec<_>>();
        assert_eq!(seqs(&narrow.rows), seqs(&whole.rows));
        assert!(some.bytes_read < all.bytes_read);
    }

    /// What a visitor is promised by what it says it reads.
    ///
    /// One that names columns is handed, of a stored row, exactly the named columns its
    /// row group has: not the terms' columns read beside them, and no entry for a column
    /// the schema lacks. And a row group whose schema has no column for a term's key gives
    /// no row, to `*` as to anything, while every one of its rows is still counted as
    /// looked at.
    #[test]
    fn a_visitor_is_handed_the_columns_it_named_and_no_others() {
        let scratch = Scratch::new("scan-named");
        let (_s, leaf) = mixed(&scratch, false);
        let opts = ScanOptions {
            ts_column: Some("ts".into()),
            ..ScanOptions::default()
        };
        let frozen = leaf.freeze(None, None);
        let mut v = Keeps::new(Wants::Columns(named()), true);
        let scan = frozen
            .visit(&opts, &[Term::any("n")], &mut v)
            .expect("scan");
        let stored = (4 * UNIT) as u64;
        assert_eq!((scan.matched, scan.scanned), (stored, stored));
        let mut kinds = BTreeSet::new();
        for (row, _) in &v.rows {
            let mut keys: Vec<&str> = row.keys().map(String::as_str).collect();
            keys.sort_unstable();
            kinds.insert(keys);
        }
        assert_eq!(
            kinds.into_iter().collect::<Vec<_>>(),
            [
                // The row group of shape 2, some of whose rows carry an arrival time.
                vec!["\\_received_at", "extra", "level", "ts"],
                vec!["extra", "level", "ts"],
                // The row group of shape 0, which has no `extra`.
                vec!["level", "ts"],
            ]
        );

        // The first row group of the older day has no `extra`: none of its sixteen rows
        // is handed over, and all sixteen were looked at.
        let mut v = Keeps::new(Wants::Rows, true);
        let scan = frozen
            .visit(&opts, &[Term::any("extra")], &mut v)
            .expect("scan");
        assert_eq!((scan.matched, scan.scanned), (stored - UNIT as u64, stored));
        assert!(v.rows.iter().all(|(row, _)| row.contains_key("extra")));
        let older: Vec<u64> = v.rows[UNIT..].iter().map(|(_, seq)| *seq).collect();
        assert_eq!(older, (UNIT as u64..3 * UNIT as u64).collect::<Vec<_>>());
    }

    /// The units of a frozen leaf's row groups for one question, each handed to `f` after
    /// its bytes are read and before anything of it is decoded.
    fn each_unit(frozen: &Frozen, ask: &Ask<'_>, mut f: impl FnMut(&Unit<'_>)) {
        let mut out = Scan::default();
        for view in &frozen.views {
            for s in view.stored.iter() {
                let shape = ask.shape(&s.schema);
                let mut reader = Reader::open(&s.file.path).expect("open");
                let unit = Unit::read(s, &shape, view.day, 0, &mut reader, &mut out).expect("read");
                f(&unit);
            }
        }
    }

    /// **The rows chosen from columns are the rows chosen cell by cell.** Over every row
    /// group of the mixed leaf, for every list of terms under every window, with `when`
    /// and the time column text and not: `pick`, which puts each term to its column's
    /// typed values, selects exactly the rows `slow_pick` selects by printing every cell
    /// and asking it.
    ///
    /// A debug build makes this comparison inside `pick`, on every row group of every
    /// scan. Here the test makes it itself, so that it is made in a release build too, and
    /// says how much it compared: selections of some rows, of every row and of none.
    #[test]
    fn the_rows_chosen_from_columns_are_the_rows_chosen_cell_by_cell() {
        let scratch = Scratch::new("scan-pick");
        let (_s, leaf) = mixed(&scratch, false);
        let frozen = leaf.freeze(None, None);
        let (mut some, mut every, mut none) = (0, 0, 0);
        for (opts, terms) in asks() {
            let ask = Ask::new(&opts, &terms, Wants::Rows);
            each_unit(&frozen, &ask, |unit| {
                let fast = pick(unit, &ask).expect("pick");
                let before = decoded(unit);
                let slow = slow_pick(unit, &ask).expect("slow_pick");
                assert_eq!(fast, slow, "{terms:?} with {opts:?}");
                // And the plain way decoded for itself: the unit is as `pick` left it.
                assert_eq!(decoded(unit), before);
                match fast.count() as usize {
                    0 => none += 1,
                    n if n == unit.rows => every += 1,
                    _ => some += 1,
                }
            });
        }
        assert!(
            some > 500 && every > 200 && none > 500,
            "{some} {every} {none}"
        );
    }

    /// The names of the columns a unit has decoded so far.
    fn decoded(unit: &Unit<'_>) -> Vec<String> {
        (0..unit.columns.len())
            .filter(|place| unit.columns[*place].get().is_some())
            .map(|place| unit.at.schema[unit.shape.wanted[place]].clone())
            .collect()
    }

    /// **A column is decoded when something asks for it, and not before.** Choosing the
    /// rows of a row group decodes the columns the window and the terms read; a question
    /// with neither decodes none, and so does `*` wherever the chunk's directory entry
    /// already says which rows are null. Handing rows to a visitor that reads none decodes
    /// nothing more, and asking a visitor about rows it then does not take decodes the
    /// time it is asked with and nothing else. This is where the time goes when it goes
    /// wrong: the answer would be the same with every column decoded, and several times
    /// slower.
    #[test]
    fn a_row_group_decodes_only_the_columns_it_is_asked_about() {
        let scratch = Scratch::new("scan-lazy");
        let (_s, leaf) = mixed(&scratch, false);
        let frozen = leaf.freeze(None, None);
        let base = ScanOptions {
            ts_column: Some("ts".into()),
            ..ScanOptions::default()
        };
        let windowed = ScanOptions {
            since: Some(instant(T0)),
            ..base.clone()
        };
        // What is asked, and the columns each row group may decode to choose its rows.
        let cases: [(&ScanOptions, Vec<Term>, &[&str]); 7] = [
            (&base, vec![], &[]),
            (&base, vec![Term::exact("level", "INFO")], &["level"]),
            // Every row of `n` is there, and its chunk says so.
            (&base, vec![Term::any("n")], &[]),
            // `note` is all null in one row group, has a presence bitmap in another, and
            // is whole in the third: decoded only where the bitmap has to be read.
            (&base, vec![Term::any("note")], &["note"]),
            // The first term leaves no row, so the second's column is left alone.
            (
                &base,
                vec![Term::exact("level", "info"), Term::exact("n", "3")],
                &["level"],
            ),
            // The window reads the time column, and the arrival time only where a row
            // has no time of its own.
            (&windowed, vec![], &["\\_received_at", "ts"]),
            (
                &windowed,
                vec![Term::exact("n", "3")],
                &["\\_received_at", "n", "ts"],
            ),
        ];
        for (opts, terms, may) in cases {
            let ask = Ask::new(opts, &terms, Wants::Nothing);
            let mut all = BTreeSet::new();
            each_unit(&frozen, &ask, |unit| {
                assert!(decoded(unit).is_empty(), "decoded by being read");
                let sel = pick(unit, &ask).expect("pick");
                let chosen = decoded(unit);
                assert!(
                    chosen.iter().all(|name| may.contains(&name.as_str())),
                    "{terms:?}: decoded {chosen:?}"
                );
                all.extend(chosen.clone());
                // A count takes the rows chosen and decodes nothing to do it.
                let mut out = Scan::default();
                hand(
                    unit,
                    &ask,
                    &sel,
                    &mut Keeps::new(Wants::Nothing, true),
                    &mut out,
                )
                .expect("hand");
                assert_eq!(out.matched, sel.count());
                assert_eq!(decoded(unit), chosen, "{terms:?}: decoded by a count");
            });
            // And each of them was decoded by some row group: the list is not slack.
            assert_eq!(
                all.iter().map(String::as_str).collect::<Vec<_>>(),
                may,
                "{terms:?}"
            );
        }

        // Rows for a visitor that named one column decode that column beside the term's,
        // and only in a row group some row of which passed.
        let terms = [Term::exact("level", "WARN")];
        let ask = Ask::new(&base, &terms, Wants::Columns(vec!["n".into()]));
        let mut built = 0;
        each_unit(&frozen, &ask, |unit| {
            let sel = pick(unit, &ask).expect("pick");
            let mut v = Keeps::new(Wants::Columns(vec!["n".into()]), true);
            hand(unit, &ask, &sel, &mut v, &mut Scan::default()).expect("hand");
            let want: &[&str] = if sel.is_empty() {
                &["level"]
            } else {
                &["level", "n"]
            };
            assert_eq!(decoded(unit), want);
            built += v.rows.len();
        });
        assert!(built > 0);

        // A visitor that is asked first and takes no row: the time column is decoded to
        // ask it with - and the arrival time where some row has no time of its own - and
        // no other column is, though each of them would be in a row. Then it takes one
        // row, and the whole row group is decoded to build it.
        let ask = Ask::new(&base, &[], Wants::Newest);
        let mut fell_back = 0;
        each_unit(&frozen, &ask, |unit| {
            let sel = pick(unit, &ask).expect("pick");
            assert!(decoded(unit).is_empty());
            let mut v = Keeps::refusing(true, |_| true);
            hand(unit, &ask, &sel, &mut v, &mut Scan::default()).expect("hand");
            assert_eq!((v.asked.len(), v.rows.len()), (unit.rows, 0));
            let times = decoded(unit);
            assert!(
                times == ["ts"] || times == ["\\_received_at", "ts"],
                "decoded {times:?} to ask"
            );
            fell_back += usize::from(times.len() == 2);

            let mut v = Keeps::refusing(true, |seq| seq != 3);
            hand(unit, &ask, &sel, &mut v, &mut Scan::default()).expect("hand");
            assert_eq!(v.rows.len(), 1);
            assert_eq!(decoded(unit).len(), unit.columns.len());
        });
        assert_eq!(fell_back, 1);

        // And where the terms leave no row of a row group there is nothing to ask about:
        // not even the time column is decoded.
        let ask = Ask::new(&base, &terms, Wants::Newest);
        let (mut asked, mut unasked) = (0, 0);
        each_unit(&frozen, &ask, |unit| {
            let sel = pick(unit, &ask).expect("pick");
            let mut v = Keeps::refusing(true, |_| true);
            hand(unit, &ask, &sel, &mut v, &mut Scan::default()).expect("hand");
            assert_eq!(v.asked.len() as u64, sel.count());
            let want: &[&str] = if sel.is_empty() {
                unasked += 1;
                &["level"]
            } else {
                asked += 1;
                &["level", "ts"]
            };
            assert_eq!(decoded(unit), want);
        });
        assert!(asked > 0 && unasked > 0, "{asked} {unasked}");
    }
}
