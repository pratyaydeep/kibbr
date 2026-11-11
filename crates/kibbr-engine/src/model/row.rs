// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! One row of values, as the ladder read them, and the instant a row is filed under: below
//! the format, which builds row groups from rows, and the stores, which hold them.

use std::collections::HashMap;

use crate::model::infer::{infer, time_nanos, time_nanos_of_int};
use crate::model::types::{ColValue, RECEIVED_AT_COLUMN};

/// One row: each key's value as the ladder read it and, only where printing that value would
/// not give back what the line said, the JSON the line said.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Row {
    values: HashMap<String, ColValue>,
    written: HashMap<String, serde_json::Value>,
}

impl std::ops::Deref for Row {
    type Target = HashMap<String, ColValue>;

    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

impl<'a> IntoIterator for &'a Row {
    type Item = (&'a String, &'a ColValue);
    type IntoIter = std::collections::hash_map::Iter<'a, String, ColValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.iter()
    }
}

impl FromIterator<(String, ColValue)> for Row {
    fn from_iter<I: IntoIterator<Item = (String, ColValue)>>(iter: I) -> Self {
        Self {
            values: iter.into_iter().collect(),
            written: HashMap::new(),
        }
    }
}

impl Row {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(n: usize) -> Self {
        Self {
            values: HashMap::with_capacity(n),
            written: HashMap::new(),
        }
    }

    /// A value whose canonical print is what the line said.
    pub fn insert(&mut self, key: String, value: ColValue) -> Option<ColValue> {
        self.written.remove(&key);
        self.values.insert(key, value)
    }

    /// A value, and the JSON the line said where that differs from its canonical print, so
    /// every source of rows ends with the same row for the same line.
    pub fn insert_written(
        &mut self,
        key: String,
        value: ColValue,
        written: Option<serde_json::Value>,
    ) {
        match written {
            Some(json) if json != value.printed() => {
                self.written.insert(key.clone(), json);
            }
            _ => {
                self.written.remove(&key);
            }
        }
        self.values.insert(key, value);
    }

    /// Replace the value a query compares against and keep what the line said - the text a
    /// widened date column is matched as, over the JSON the answer still prints.
    pub fn set_value(&mut self, key: &str, value: ColValue) {
        if let Some(slot) = self.values.get_mut(key) {
            let before = std::mem::replace(slot, value);
            if !self.written.contains_key(key) {
                self.written.insert(key.to_string(), before.printed());
            }
        }
    }

    pub fn clear(&mut self) {
        self.values.clear();
        self.written.clear();
    }

    /// The JSON the line said for `key`, when it is not the value's canonical print.
    pub fn written(&self, key: &str) -> Option<&serde_json::Value> {
        self.written.get(key)
    }

    /// The JSON value the line said for `key`: what an answer prints.
    pub fn json(&self, key: &str) -> Option<serde_json::Value> {
        let value = self.values.get(key)?;
        Some(
            self.written
                .get(key)
                .cloned()
                .unwrap_or_else(|| value.printed()),
        )
    }

    /// Roughly the memory this row holds, tables and allocations included: several times its
    /// payload, which is what a memory limit must count.
    pub fn held_bytes(&self) -> usize {
        fn table<K, V>(map: &HashMap<K, V>) -> usize {
            match map.capacity() {
                0 => 0,
                // A table holds seven items for every eight slots, in a power of two, and
                // a control byte a slot beside them.
                n => (n * 8 / 7).next_power_of_two() * (std::mem::size_of::<(K, V)>() + 1),
            }
        }
        fn heap(len: usize) -> usize {
            len.div_ceil(16) * 16
        }
        let keys: usize = self
            .values
            .keys()
            .chain(self.written.keys())
            .map(|key| heap(key.len()))
            .sum();
        let strings: usize = self
            .values
            .values()
            .map(|value| match value {
                ColValue::String(text) => heap(text.len()),
                _ => 0,
            })
            .chain(self.written.values().map(|json| match json {
                serde_json::Value::String(text) => heap(text.len()),
                _ => 0,
            }))
            .sum();
        std::mem::size_of::<Self>() + table(&self.values) + table(&self.written) + keys + strings
    }
}

// ── Reading a row's place in time ──────────────────────────────────────────────────

/// A value as an instant - a `Datetime`, or text the ladder reads as one - or `None`;
/// apart from `ts_of` because a scan reads instants off a decoded chunk, not a row.
pub fn instant_of(value: &ColValue) -> Option<i64> {
    match value {
        ColValue::Datetime(ns) => Some(*ns),
        ColValue::String(text) => match infer(text).value {
            ColValue::Datetime(ns) => Some(ns),
            _ => None,
        },
        _ => None,
    }
}

/// A row's value under `column` as an instant: `instant_of` it, and `None` for a row
/// that has no such column.
pub fn ts_of(row: &Row, column: &str) -> Option<i64> {
    row.get(column).and_then(instant_of)
}

/// What a template's time column reads a value as, from what the line said: a string's
/// characters or a number's digits through `time_nanos`, so a decimal of seconds, sixteen
/// digits and a zoned date are instants here though the ladder typed them otherwise.
///
/// The text is what the line said and not the typed value: `"0000001679711330"` is the
/// integer 1679711330 and is not a count of seconds. A JSON number is its digits as the
/// JSON parser holds them - a double, for one with a point.
pub fn said_instant(value: &ColValue, written: Option<&serde_json::Value>) -> Option<i64> {
    use serde_json::Value;
    match (written, value) {
        (Some(Value::String(text)), _) | (None, ColValue::String(text)) => time_nanos(text),
        (Some(Value::Number(n)), _) => time_nanos(&n.to_string()),
        (Some(_), _) => None,
        (None, ColValue::Int64(i)) => time_nanos_of_int(*i),
        (None, ColValue::Float64(f)) => time_nanos(&serde_json::Number::from_f64(*f)?.to_string()),
        (None, ColValue::Datetime(ns)) => Some(*ns),
        (None, ColValue::Bool(_) | ColValue::Null) => None,
    }
}

/// `said_instant` of a row's value under `column`.
pub fn said_ts(row: &Row, column: &str) -> Option<i64> {
    row.get(column)
        .and_then(|value| said_instant(value, row.written(column)))
}

/// The time a row has of its own under its template's time column, by everything a time
/// column reads: what decides whether the row needs an arrival time.
pub fn own_ts(row: &Row, column: &str) -> Option<i64> {
    ts_of(row, column).or_else(|| said_ts(row, column))
}

/// The effective time of a row: the template's time column where it holds an instant,
/// otherwise the arrival time the row was stored with, and where it has neither, the time
/// column as only a time column reads it. A row is given an arrival time exactly when
/// `own_ts` is `None`, so the last is every row whose own time is in a time-column form;
/// the order keeps a row stored before those forms were read at the arrival time it was
/// filed under. `disk::scan` spells the same rule, so change both.
pub fn effective_ts(row: &Row, column: &str) -> Option<i64> {
    ts_of(row, column)
        .or_else(|| ts_of(row, RECEIVED_AT_COLUMN))
        .or_else(|| said_ts(row, column))
}

#[cfg(test)]
mod tests;
