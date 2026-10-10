//! Kibbr as a library: the modules the binary in `main.rs` is built from - the commands,
//! the HTTP server and the settings - public so that the fuzz targets under `fuzz/` can
//! call the harnesses in `fuzzing`. Nothing else depends on this crate, and nothing here
//! is a stable interface. What a line is, where it is kept and how it is asked for are
//! the three crates under `crates/`.

pub mod ask;
pub mod bench;
pub mod compact;
pub mod config;
pub mod demo;
pub mod error;
pub mod farm;
pub mod fuzzing;
pub mod gen;
pub mod http;
pub mod load;
pub mod lock;
pub mod logs;
pub mod parse;
pub mod query;
pub mod reference;
pub mod serve;
pub mod speed;
