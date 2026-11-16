// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The `kibbr` binary: the server and the command line, over the library, which is the
//! engine (`src/engine/`). In this crate `kibbr::` is the library and `crate::` is the binary.

mod cli;
mod server;

use std::process::ExitCode;

/// mimalloc in place of the system allocator, which aborted inside libmalloc on macOS
/// arm64 with no `unsafe` in src/ and a clean AddressSanitizer run.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> ExitCode {
    cli::main()
}
