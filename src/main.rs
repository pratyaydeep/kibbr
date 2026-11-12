// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! `kibbr`, the binary: everything it does is in the library, from `kibbr::run`.

use std::process::ExitCode;

/// mimalloc in place of the system allocator, which aborted inside libmalloc on macOS
/// arm64 with no `unsafe` in src/ and a clean AddressSanitizer run.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> ExitCode {
    kibbr::run()
}
