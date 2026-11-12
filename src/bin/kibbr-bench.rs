// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! `kibbr-bench`: this build's speed on one farm day (`cargo build --release --features bench`).

fn main() -> std::process::ExitCode {
    kibbr::bench_main()
}
