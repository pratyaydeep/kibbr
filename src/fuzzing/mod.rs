//! What each fuzz target under `fuzz/` holds its subject to, one module per target, each
//! with one `run(data)`. They live here and not in `fuzz/fuzz_targets/` so that
//! `tests/fuzz_regressions.rs` replays every committed crasher through the same code on
//! the stable toolchain, with no libFuzzer in the build: a crasher that was fixed stays
//! fixed under plain `cargo test`.
//!
//! A `run` returns for every input its subject handles correctly - parsed, or refused -
//! and panics when an invariant breaks. Memory is held by the fuzzer's own limits
//! (`-malloc_limit_mb`, `-rss_limit_mb`, in `fuzz/run.sh`), not from in here.

pub mod cursor;
pub mod record_log;
pub mod trieql;
pub mod v3_segment;
pub mod wal_record;
