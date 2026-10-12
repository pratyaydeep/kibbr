//! The commands: one module for each, and what `main.rs` dispatches to.

use std::fs::File;
use std::io::{self, BufRead, BufReader};

use crate::error::CliError;

pub mod ask;
pub mod bench;
pub mod bench_storage;
pub mod compact;
pub mod demo;
pub mod load;
pub mod parse;
pub mod query;

/// The input a command reads: a file by its path, or standard input for `-`.
fn open(input: &str) -> Result<Box<dyn BufRead>, CliError> {
    if input == "-" {
        return Ok(Box::new(BufReader::new(io::stdin())));
    }
    let file = File::open(input).map_err(|source| CliError::Read {
        path: input.to_string(),
        source,
    })?;
    Ok(Box::new(BufReader::new(file)))
}
