use thiserror::Error;

/// Errors from flattening and type inference.
#[derive(Debug, Error)]
pub enum DetectorError {
    #[error("nesting deeper than {limit} levels at key `{key}`")]
    NestingTooDeep { key: String, limit: usize },

    #[error("top-level JSON value is {found}, expected an object")]
    NotAnObject { found: &'static str },

    // The cause is not interpolated here: it is a `#[source]`, and the CLI walks the
    // chain. Doing both prints it twice.
    #[error("line {line}: invalid JSON")]
    InvalidJson {
        line: usize,
        #[source]
        source: serde_json::Error,
    },
}

/// Errors from the log generator.
#[derive(Debug, Error)]
pub enum GenError {
    #[error("--templates must be at least 1")]
    NoTemplates,

    #[error("failed to serialize generated line: {0}")]
    Serialize(#[from] serde_json::Error),

    #[error("writing generated output: {0}")]
    Write(#[source] std::io::Error),
}

/// Errors surfaced by the CLI itself.
#[derive(Debug, Error)]
pub enum CliError {
    #[error("reading {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("writing to stdout: {0}")]
    Write(#[source] std::io::Error),

    #[error(transparent)]
    Detector(#[from] DetectorError),

    #[error(transparent)]
    Gen(#[from] GenError),
}
