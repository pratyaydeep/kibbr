//! The oracle. Never ships - `naive` is compiled in only so `--reference` can
//! run the same flags through it and a differential test can judge the real engine
//! without a hand-written assertion in sight.

pub mod naive;
