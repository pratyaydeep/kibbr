//! Kibbr as a library: the modules the binary in `main.rs` is built from, public so that
//! the fuzz targets under `fuzz/` can call the parsers and decoders directly. Nothing
//! else depends on this crate, and nothing here is a stable interface.

pub mod bench;
pub mod catalog;
pub mod compact;
pub mod config;
pub mod db;
pub mod demo;
pub mod engine;
pub mod error;
pub mod farm;
pub mod flatten;
pub mod fmt;
pub mod frame;
pub mod fuzzing;
pub mod gen;
pub mod http;
pub mod infer;
pub mod key;
pub mod lines;
pub mod load;
pub mod lock;
pub mod parse;
pub mod query;
pub mod reference;
pub mod rng;
pub mod serve;
pub mod speed;
pub mod store;
pub mod trie;
pub mod triefile;
pub mod trieql;
pub mod types;
pub mod wal;
