//! Configuration, shared between the CLI and the server so they cannot disagree about
//! the backstop for an unlimited query.
//!
//! One struct, because the CLI and the server disagreeing about a backstop would make the
//! same query mean two things.

use thiserror::Error;

/// Default page size for an unlimited `MATCH`.
pub const DEFAULT_MAX_ROWS: usize = 10_000;

/// Environment variable for the max_rows configuration.
const MAX_ROWS_ENV: &str = "TRIEDB__QUERY__MAX_ROWS";

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("{var}=`{value}` is not a row count")]
    BadEnv { var: &'static str, value: String },
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Config {
    pub query: Query,
}

#[derive(Debug, Clone, Copy)]
pub struct Query {
    /// The backstop, in rows. `0` = none.
    pub max_rows: usize,
}

impl Default for Query {
    fn default() -> Self {
        Self {
            max_rows: DEFAULT_MAX_ROWS,
        }
    }
}

impl Config {
    /// Load the backstop: flag > environment > default.
    pub fn load(max_rows: Option<usize>) -> Result<Self, ConfigError> {
        let max_rows = match max_rows {
            Some(n) => n,
            None => env_usize(MAX_ROWS_ENV)?.unwrap_or(DEFAULT_MAX_ROWS),
        };
        Ok(Self {
            query: Query { max_rows },
        })
    }
}

/// Unset is absence; unreadable is an error. A backstop that silently ignored the number
/// an operator set is worse than not having one - they would believe it was in force.
fn env_usize(var: &'static str) -> Result<Option<usize>, ConfigError> {
    let Ok(text) = std::env::var(var) else {
        return Ok(None);
    };
    text.trim()
        .parse()
        .map(Some)
        .map_err(|_| ConfigError::BadEnv { var, value: text })
}
