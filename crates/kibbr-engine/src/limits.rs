// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! What a query may not exceed. Declared beside the engine that holds a query to it; the
//! flags, variables and file that set it are read by the binary (`kibbr::config`).

/// Default page size for an unlimited `MATCH`.
pub const DEFAULT_MAX_ROWS: usize = 10_000;

/// Default ceiling on the buckets a `GROUP BY time(...)` may emit; past it the query is
/// refused, not truncated, because a chart missing its tail looks like data.
pub const DEFAULT_MAX_BUCKETS: usize = 10_000;

/// Default ceiling on the groups a bucketed `GROUP BY` may emit, since a second key
/// multiplies the bucket count by its distinct labels.
pub const DEFAULT_MAX_GROUPS: usize = 100_000;

/// What a query may not exceed, carried on the `Query` itself so the engine reads the
/// same numbers the CLI and the server were configured with.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// The backstop, in rows. `0` = none.
    pub max_rows: usize,
    /// The ceiling on buckets emitted by `GROUP BY time(...)`. `0` = none.
    pub max_buckets: usize,
    /// The ceiling on groups emitted by a bucketed `GROUP BY`. `0` = none.
    pub max_groups: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_rows: DEFAULT_MAX_ROWS,
            max_buckets: DEFAULT_MAX_BUCKETS,
            max_groups: DEFAULT_MAX_GROUPS,
        }
    }
}
