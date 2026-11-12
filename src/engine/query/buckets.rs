// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! `GROUP BY time(w)`: where a bucket starts, how many a range makes, and the zeros an
//! empty bucket is drawn with.

use super::*;

// ── Time buckets ───────────────────────────────────────────────────────────────────
//
// Boundaries are multiples of the width from the Unix epoch, so overlapping queries agree
// on where the bars go. A range emits every bucket it touches, so unaligned edges add one.

/// What a bucketed `GROUP BY` was refused for; a refusal, not a truncation, and it names
/// both the requested and the permitted count because only the pair is actionable.
#[derive(Debug, PartialEq, Eq)]
pub enum BucketError {
    TooManyBuckets {
        width: String,
        requested: u64,
        permitted: usize,
    },
    TooManyGroups {
        width: String,
        buckets: u64,
        series: usize,
        requested: u64,
        permitted: usize,
    },
}

// Hand-written rather than derived, because every number in these sentences is rendered
// with thousands separators.
impl std::fmt::Display for BucketError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooManyBuckets {
                width,
                requested,
                permitted,
            } => write!(
                f,
                "`GROUP BY time({width})` over this range needs {} buckets; the limit is \
                 {} - widen the bucket or narrow the range",
                commas(*requested),
                commas(*permitted as u64)
            ),
            Self::TooManyGroups {
                width,
                buckets,
                series,
                requested,
                permitted,
            } => write!(
                f,
                "`GROUP BY time({width})` over this range needs {} buckets across {} \
                 distinct groups, which is {} rows; the limit is {} - drop a group key, \
                 widen the bucket, or narrow the range",
                commas(*buckets),
                commas(*series as u64),
                commas(*requested),
                commas(*permitted as u64)
            ),
        }
    }
}

impl std::error::Error for BucketError {}

/// A bucket width in nanoseconds, saturating: `time(1000000000d)` fits no `i64`, and one
/// bucket covering every representable instant is the honest answer to it.
pub(super) fn width_nanos(width: Duration) -> i64 {
    i64::try_from(width.as_nanos()).unwrap_or(i64::MAX).max(1)
}

/// A timestamp's bucket start in Unix nanoseconds, floored on the epoch's grid and not the
/// query's, so a chart does not shift its bars when the range is nudged.
pub fn bucket_of(ns: i64, width: i64) -> i64 {
    ns.div_euclid(width).saturating_mul(width)
}

/// The first bucket start at or after `ns` - the exclusive end of a span covering
/// `[.., ns)`.
fn bucket_after(ns: i64, width: i64) -> i64 {
    bucket_of(ns.saturating_sub(1), width).saturating_add(width)
}

/// The half-open span of bucket starts a query's own window implies, `[lo, hi)`; `None`
/// with no `since`. The right edge is `until`, or `now`, which is not a filter.
pub(super) fn window_span(query: &Query) -> Option<(i64, i64)> {
    let width = width_nanos(query.bucket_width()?);
    let since = query.time.since?;
    let lo = bucket_of(nanos_of(since), width);
    let end = nanos_of(query.time.until.unwrap_or(query.now));
    Some((lo, bucket_after(end, width).max(lo)))
}

/// The window's span widened to hold every bucket that received a row, which keeps
/// `sum(groups) == count` true for a row ahead of `now` or a window with no lower bound.
fn span_with(base: Option<(i64, i64)>, seen: Option<(i64, i64)>, width: i64) -> Option<(i64, i64)> {
    match (base, seen) {
        (Some((lo, hi)), Some((min, max))) => {
            Some((lo.min(min), hi.max(max.saturating_add(width))))
        }
        (Some(span), None) => Some(span),
        (None, Some((min, max))) => Some((min, max.saturating_add(width))),
        (None, None) => None,
    }
}

/// How many buckets a span holds, rounded up so the saturated bottom bucket's rows are
/// not dropped out of the sum.
fn span_len(span: (i64, i64), width: i64) -> u64 {
    let (lo, hi) = span;
    u64::try_from(hi.saturating_sub(lo))
        .unwrap_or(0)
        .div_ceil(width.unsigned_abs().max(1))
}

/// The bucket count against `max_buckets`, before anything is read, so a refused query
/// reads no rows.
pub fn check_buckets(query: &Query) -> Result<(), BucketError> {
    let Some(width) = query.bucket_width() else {
        return Ok(());
    };
    let Some(span) = window_span(query) else {
        return Ok(());
    };
    check_span(span, width, query.limits.max_buckets)
}

/// How many buckets this query's own window implies, for `--explain`; `None` where it is
/// not bucketed or its window has no edge.
pub fn planned_buckets(query: &Query) -> Option<u64> {
    let width = query.bucket_width()?;
    Some(span_len(window_span(query)?, width_nanos(width)))
}

fn check_span(span: (i64, i64), width: Duration, permitted: usize) -> Result<(), BucketError> {
    let requested = span_len(span, width_nanos(width));
    if permitted > 0 && requested > permitted as u64 {
        return Err(BucketError::TooManyBuckets {
            width: duration_text(width),
            requested,
            permitted,
        });
    }
    Ok(())
}

/// What one bucketed group is keyed by while the scan is running: its bucket, and the
/// values of the other keys.
pub(super) type Tally = HashMap<(Option<i64>, Vec<String>), u64>;

/// The tally as the answer, with a zero in every bucket of the span for each observed
/// label combination: a quiet bucket is an observation. Unplaceable rows are not filled.
pub(super) fn zero_fill(
    tally: Tally,
    width: Option<Duration>,
    span: Option<(i64, i64)>,
    columns: usize,
    limits: Limits,
) -> Result<Vec<Group>, BucketError> {
    let Some(width) = width else {
        // No time axis: the groups are what was seen, and nothing is filled in.
        let mut groups: Vec<Group> = tally
            .into_iter()
            .map(|((bucket, values), count)| Group {
                bucket,
                values,
                count,
            })
            .collect();
        groups.sort_by(|a, b| (&a.bucket, &a.values).cmp(&(&b.bucket, &b.values)));
        return Ok(groups);
    };
    let w = width_nanos(width);

    // The rows that could not be placed. Held aside because they are not on the time
    // axis and must not be multiplied across it.
    let mut groups: Vec<Group> = tally
        .iter()
        .filter(|((bucket, _), _)| bucket.is_none())
        .map(|((_, values), count)| Group {
            bucket: None,
            values: values.clone(),
            count: *count,
        })
        .collect();
    groups.sort_by(|a, b| a.values.cmp(&b.values));

    let placed: Vec<(i64, &Vec<String>, u64)> = tally
        .iter()
        .filter_map(|((bucket, values), count)| bucket.map(|b| (b, values, *count)))
        .collect();
    let seen = placed
        .iter()
        .fold(None, |acc: Option<(i64, i64)>, (b, ..)| match acc {
            Some((min, max)) => Some((min.min(*b), max.max(*b))),
            None => Some((*b, *b)),
        });

    let Some(span) = span_with(span, seen, w) else {
        // No window edge and no placed row: nothing names a time axis, so there are no
        // buckets to emit. Not an error - see `execute`.
        return Ok(groups);
    };
    check_span(span, width, limits.max_buckets)?;

    // One series per observed label combination. With no other key there is exactly one,
    // the bare time series.
    let mut series: BTreeSet<&Vec<String>> = placed.iter().map(|(_, values, _)| *values).collect();
    let empty = Vec::new();
    if columns == 0 {
        series.insert(&empty);
    }

    let buckets = span_len(span, w);
    let requested = buckets.saturating_mul(series.len() as u64);
    if limits.max_groups > 0 && requested > limits.max_groups as u64 {
        return Err(BucketError::TooManyGroups {
            width: duration_text(width),
            buckets,
            series: series.len(),
            requested,
            permitted: limits.max_groups,
        });
    }

    let counts: HashMap<(i64, &Vec<String>), u64> = placed
        .iter()
        .map(|(bucket, values, count)| ((*bucket, *values), *count))
        .collect();
    groups.reserve(requested as usize);
    for n in 0..buckets {
        let start = span.0.saturating_add((n as i64).saturating_mul(w));
        for values in &series {
            groups.push(Group {
                bucket: Some(start),
                values: (*values).clone(),
                count: counts.get(&(start, *values)).copied().unwrap_or(0),
            });
        }
    }
    Ok(groups)
}
