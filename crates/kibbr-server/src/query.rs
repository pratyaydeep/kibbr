// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! A query as it crosses the wire: a TrieQL string in, a `QueryResult` as JSON out.
//!
//! What `POST /query` takes and answers with, and what the binary's `query` command
//! builds its `Query` through, so the two surfaces cannot drift.

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

/// A TrieQL string to a `Query`: the one way in for both surfaces, so the limit rules
/// and the cursor guard are not a second dialect.
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

/// Whether this cursor belongs to this query, checked once the whole `Query` exists: a
/// cursor read against another pattern would answer with a silently wrong page.
pub fn finish(query: Query) -> Result<Query> {
    if let Some(c) = query.cursor {
        c.check(&query)?;
    }
    // Here rather than in the engine so `SINCE 30d GROUP BY time(1s)` is refused before
    // thirty days of rows are read.
    engine::check_buckets(&query).map_err(engine::ExecError::from)?;
    if let QueryMode::GroupBy(keys) = &query.mode {
        // `bucket` is the column a `time(...)` reports under, so a key of that name beside
        // one would collide: refused rather than silently letting one win.
        let (width, columns) = engine::split_keys(keys);
        if width.is_some() && columns.iter().any(|c| c == BUCKET_COLUMN) {
            return Err(Error::BucketKeyClash);
        }
    }
    Ok(query)
}

/// A `QueryResult` as the JSON both `--format json` and `POST /query` answer with,
/// built in one place so the two cannot differ.
pub fn result_json(query: &Query, result: &QueryResult) -> Value {
    let rows: Vec<Value> = canonical(result).into_iter().map(row_json).collect();
    shaped(query, result, rows)
}

/// `result_json`, for a caller that may stop wanting the answer: `None` once `stop` is
/// set, asked every `STOP_EVERY_ROWS` rows.
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
            // Segment files read, and leaves opened: two facts, so two numbers.
            "files_opened": meta.files_opened,
            "leaves_opened": meta.leaves_opened,
            // Row groups read against row groups dropped on their timestamps: a prune nobody
            // can see is a prune nobody notices stopping.
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

/// One row as one JSON object, each value the JSON value that was ingested;
/// shared with `ndjson`, so a row serialises the same on every surface.
pub fn row_json(row: &kibbr_storage::store::Row) -> Value {
    let mut obj = Map::new();
    for (name, key) in kibbr_storage::store::present(row) {
        obj.insert(name, row.json(key).unwrap_or(Value::Null));
    }
    Value::Object(obj)
}

/// The rows, in the order the answer already has them: `engine::Rank` promises newest
/// first, and re-sorting would break the ordering the limit depends on.
pub fn canonical(result: &QueryResult) -> Vec<&kibbr_storage::store::Row> {
    result.rows.iter().collect()
}

/// The keys a query grouped by, or none for every other mode.
pub fn group_keys_of(query: &Query) -> &[GroupKey] {
    match &query.mode {
        QueryMode::GroupBy(keys) => keys,
        _ => &[],
    }
}

/// One group as JSON; `bucket` is absent when the clause named no `time(...)` and `null`
/// when this row could not be placed in time, which a chart has to tell apart.
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
