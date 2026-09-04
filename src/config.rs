//! Configuration, shared between the CLI and the server so they cannot disagree about
//! the backstop for an unlimited query.
//!
//! One struct, because the CLI and the server disagreeing about a backstop would make the
//! same query mean two things.

use thiserror::Error;

/// Default page size for an unlimited `MATCH`.
pub const DEFAULT_MAX_ROWS: usize = 10_000;

/// Default ceiling on the buckets a `GROUP BY time(...)` may emit.
///
/// `SINCE 30d GROUP BY time(1s)` is 2.6M buckets. The query is refused rather than
/// truncated: a chart missing its tail looks like data, where a refusal looks like a
/// refusal.
pub const DEFAULT_MAX_BUCKETS: usize = 10_000;

/// Default ceiling on the groups a bucketed `GROUP BY` may emit.
///
/// A second key multiplies the bucket count by the number of distinct labels it has, so
/// the bucket cap alone does not bound the answer: 10,000 buckets against 800 hosts is
/// 8,000,000 groups. Only bucketed grouping is measured against this - a plain
/// `GROUP BY user_id` emits one row per user and always did.
pub const DEFAULT_MAX_GROUPS: usize = 100_000;

/// Environment variable for the max_rows configuration.
const MAX_ROWS_ENV: &str = "TRIEDB__QUERY__MAX_ROWS";
const MAX_BUCKETS_ENV: &str = "TRIEDB__QUERY__MAX_BUCKETS";
const MAX_GROUPS_ENV: &str = "TRIEDB__QUERY__MAX_GROUPS";

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("{var}=`{value}` is not a whole number")]
    BadEnv { var: &'static str, value: String },
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Config {
    pub query: Limits,
}

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

impl Config {
    /// Load the limits: flag > environment > default, one rule for each.
    pub fn load(
        max_rows: Option<usize>,
        max_buckets: Option<usize>,
        max_groups: Option<usize>,
    ) -> Result<Self, ConfigError> {
        Ok(Self {
            query: Limits {
                max_rows: resolve(max_rows, MAX_ROWS_ENV, DEFAULT_MAX_ROWS)?,
                max_buckets: resolve(max_buckets, MAX_BUCKETS_ENV, DEFAULT_MAX_BUCKETS)?,
                max_groups: resolve(max_groups, MAX_GROUPS_ENV, DEFAULT_MAX_GROUPS)?,
            },
        })
    }
}

fn resolve(flag: Option<usize>, var: &'static str, default: usize) -> Result<usize, ConfigError> {
    match flag {
        Some(n) => Ok(n),
        None => Ok(env_usize(var)?.unwrap_or(default)),
    }
}

/// Unset is absence; unreadable is an error. A limit that silently ignored the number an
/// operator set is worse than not having one - they would believe it was in force.
fn env_usize(var: &'static str) -> Result<Option<usize>, ConfigError> {
    let Ok(text) = std::env::var(var) else {
        return Ok(None);
    };
    text.trim()
        .parse()
        .map(Some)
        .map_err(|_| ConfigError::BadEnv { var, value: text })
}
