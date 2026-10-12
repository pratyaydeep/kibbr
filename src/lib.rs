//! Kibbr as a library: the modules the binary in `main.rs` is built from - the commands
//! and how their settings are resolved - public so that the fuzz targets under `fuzz/`
//! can call the harnesses in `fuzzing`. Nothing else depends on this crate, and nothing
//! here is a stable interface. What a line is, where it is kept, how it is asked for and
//! the server that does all three over HTTP are the four crates under `crates/`.

pub mod client;
pub mod cmd;
pub mod config;
pub mod error;
pub mod fuzzing;
pub mod gen;
pub mod logs;

/// The oracle. Never ships - `naive` is compiled in only so `--reference` can
/// run the same flags through it and a differential test can judge the real engine
/// without a hand-written assertion in sight.
pub mod reference {
    pub mod naive;
}
