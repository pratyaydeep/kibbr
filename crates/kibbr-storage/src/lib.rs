//! Where templates and rows are kept: the trie and the catalog that makes it durable, the
//! stores under it (memory, and format v3's day files with their manifest), the
//! write-ahead log, and `db` - the write path that puts one parsed line into them. And
//! `lock`: whoever opens a data directory to write takes it first.
//!
//! Above `kibbr-core` and under the engine: nothing here plans or answers a query.

pub mod catalog;
pub mod db;
pub mod frame;
pub mod lock;
pub mod store;
pub mod trie;
pub mod triefile;
pub mod wal;
