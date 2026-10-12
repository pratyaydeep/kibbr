//! A query as it crosses the wire: a TrieQL string in, a `QueryResult` as JSON out.
//!
//! What `POST /query` takes and answers with - and what the binary's `query` command
//! builds its own `Query` through and prints as `--format json`, so the two surfaces
//! cannot drift: `Query::new`'s per-mode limit rules and `finish`'s cursor guard are part
//! of what a TrieQL string means, and the answer's shape is built in one place.

use chrono::{DateTime, Utc};
use serde_json::{Map, Value};

use kibbr_core::fmt::rfc3339;
use kibbr_engine::engine::{
    self, Cursor, Group, GroupKey, Query, QueryMode, QueryResult, BUCKET_COLUMN,
};
use kibbr_engine::limits::Limits;
use kibbr_engine::trieql;
use kibbr_storage::store::{system_time, Stop, STOP_EVERY_ROWS};

use crate::error::{Error, Result};

/// A TrieQL string to a `Query`. The only thing `POST /query` accepts, and the same
/// three lines `--query '<trieql>'` runs.
///
/// Public so the HTTP surface reaches the grammar through this and not through
/// `trieql::parse` directly - `Query::new`'s per-mode limit rules and `finish`'s cursor
/// guard are part of what a TrieQL string means, and a second caller assembling the
/// struct itself would be a second dialect.
pub fn from_trieql(text: &str, now: DateTime<Utc>, limits: Limits) -> Result<Query> {
    let parsed = trieql::parse(text, now)?;
    finish(Query::new(
        parsed.matches,
        parsed.mode,
        parsed.time,
        parsed.limit,
        cursor(parsed.after.as_deref())?,
        system_time(now),
        limits,
    ))
}

/// The cursor text as a `Cursor`, before there is a `Query` to check it against.
pub fn cursor(text: Option<&str>) -> Result<Option<Cursor>> {
    text.map(Cursor::decode).transpose().map_err(Error::from)
}

/// The last thing `build` does on either surface: does this cursor belong to this query?
///
/// After construction, not during: the guard is over the pattern and the order
/// (`Cursor::guard`), so there is nothing to check against until the whole `Query` exists.
/// Refusing here is the difference between an error and a silently wrong page - `Rank`'s
/// leaf ordinal is relative to the leaves *this* pattern matched, so the same cursor read
/// against a different pattern names a different leaf and answers confidently.
pub fn finish(query: Query) -> Result<Query> {
    if let Some(c) = query.cursor {
        c.check(&query)?;
    }
    // The bucket count, against a window that is now resolved. Here rather than in the
    // engine so `SINCE 30d GROUP BY time(1s)` is refused before thirty days of rows are
    // read - the engine checks again for the span a query with no lower bound derives
    // from its rows, which is the only one that cannot be known this early.
    engine::check_buckets(&query).map_err(engine::ExecError::from)?;
    if let QueryMode::GroupBy(keys) = &query.mode {
        // `bucket` is the column a `time(...)` reports under, so a key of that name
        // beside one would put two columns of the same name in the table and collide in
        // the NDJSON object. Refused rather than silently letting one win.
        let (width, columns) = engine::split_keys(keys);
        if width.is_some() && columns.iter().any(|c| c == BUCKET_COLUMN) {
            return Err(Error::BucketKeyClash);
        }
    }
    Ok(query)
}

/// A `QueryResult` as the JSON both `--format json` and `POST /query` answer with.
///
/// One function, because `/query` is specified as answering with "query result JSON" and the
/// two surfaces answering the same question differently is the drift this module's
/// header is about. `engine::QueryMode`'s own comment makes the same promise from the
/// other end: the shape is consumed unchanged, so it is built in one place.
pub fn result_json(query: &Query, result: &QueryResult) -> Value {
    let rows: Vec<Value> = canonical(result).into_iter().map(row_json).collect();
    shaped(query, result, rows)
}

/// `result_json`, for a caller that may stop wanting the answer: `None` once `stop` is
/// set, asked every `STOP_EVERY_ROWS` rows.
///
/// A row is put into JSON at about a third of what it cost to fetch, so a fetch of every
/// row that was stopped only while it scanned would run on for half as long again after
/// it was told - and for a query that had just finished its scan, for all of that.
pub fn result_json_until(query: &Query, result: &QueryResult, stop: &Stop) -> Option<Value> {
    let mut rows = Vec::with_capacity(result.rows.len());
    for (n, row) in canonical(result).into_iter().enumerate() {
        if n % STOP_EVERY_ROWS == 0 && stop.check().is_err() {
            return None;
        }
        rows.push(row_json(row));
    }
    Some(shaped(query, result, rows))
}

/// The answer around its rows: what `result_json` and `result_json_until` share.
fn shaped(query: &Query, result: &QueryResult, rows: Vec<Value>) -> Value {
    let keys = group_keys_of(query);
    let groups: Vec<Value> = result
        .groups
        .iter()
        .map(|group| group_json(keys, group))
        .collect();

    let meta = &result.meta;
    serde_json::json!({
        "exists": result.exists,
        "count": result.count,
        "rows": rows,
        "groups": Value::Array(groups),
        "meta": {
            "templates_matched": meta.templates_matched,
            "template_ids": meta.template_ids,
            // Segment files read, and leaves opened. Two numbers because they are two
            // facts and one of them used to wear the other's name: 870 leaves against
            // 16,694 files read.
            "files_opened": meta.files_opened,
            "leaves_opened": meta.leaves_opened,
            // What the segment format buys, per answer: row groups read against row
            // groups dropped on their timestamps, and the bytes of column chunk that
            // reading them cost. A prune nobody can see is a prune nobody notices
            // stopping.
            "row_groups_read": meta.row_groups_read,
            "row_groups_pruned": meta.row_groups_pruned,
            "bytes_read": meta.bytes_read,
            "rows_scanned": meta.rows_scanned,
            "rows_returned": meta.rows_returned,
            "truncated": meta.truncated,
            // Two fields on purpose: `truncated` says the page is short of the answer,
            // this says who decided that.
            "limit_source": meta.limit_source.as_str(),
            "next_cursor": meta.next_cursor,
            "duration_ms": meta.duration.as_secs_f64() * 1000.0,
        }
    })
}

/// One row as one JSON object. Shared with `ndjson`, so a row serialises identically
/// whether it is a line of its own or an element of `rows`.
///
/// Through `store::present`, so the system column leaves as `_received_at` - or
/// not at all, on a row that ingested a key of that name.
///
/// **Each value is the JSON value that was ingested**: `Row::json`, which is the line's own
/// spelling wherever the stored value would print another - `"0045000012"`, `1705312200`,
/// `"2024-01-15T16:00:00+05:30"` - and the value's canonical print everywhere else. The
/// system column was not ingested, and prints as RFC 3339.
pub fn row_json(row: &kibbr_storage::store::Row) -> Value {
    let mut obj = Map::new();
    for (name, key) in kibbr_storage::store::present(row) {
        obj.insert(name, row.json(key).unwrap_or(Value::Null));
    }
    Value::Object(obj)
}

/// The rows, in the order the answer already has them.
///
/// This used to sort by the rendered row, because the engine returned rows leaf by leaf,
/// the oracle in file order, and nothing promised either. `engine::Rank` promises one now,
/// newest first, so re-sorting here would throw away the ordering the limit depends on and
/// hand back the alphabetically-first rows instead of the newest. Both sides produce the
/// same sequence, so the differential still compares them exactly.
pub fn canonical(result: &QueryResult) -> Vec<&kibbr_storage::store::Row> {
    result.rows.iter().collect()
}

/// The keys a query grouped by, or none for every other mode.
///
/// `result_json` takes the query for this alone: a `Group` carries its values and not the
/// names they go under, because the names are the same for every group in the answer.
pub fn group_keys_of(query: &Query) -> &[GroupKey] {
    match &query.mode {
        QueryMode::GroupBy(keys) => keys,
        _ => &[],
    }
}

/// One group as JSON: the bucket beside the labels rather than among them, because it is
/// the one key that was computed rather than read off a row.
///
/// `bucket` is absent when the clause named no `time(...)` and `null` when it named one
/// and this row could not be placed in time. Two different facts, and a client drawing a
/// chart has to tell them apart.
pub fn group_json(keys: &[GroupKey], group: &Group) -> Value {
    let mut obj = Map::new();
    if keys.iter().any(|key| matches!(key, GroupKey::Time(_))) {
        obj.insert(
            BUCKET_COLUMN.to_string(),
            group
                .bucket
                .map_or(Value::Null, |ns| Value::from(rfc3339(ns))),
        );
    }
    let mut values = group.values.iter();
    let mut labels = Map::new();
    for key in keys {
        if let GroupKey::Column(name) = key {
            let value = values.next().cloned().unwrap_or_default();
            labels.insert(name.clone(), Value::from(value));
        }
    }
    obj.insert("keys".to_string(), Value::Object(labels));
    obj.insert("count".to_string(), Value::from(group.count));
    Value::Object(obj)
}
