// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| kibbr::fuzzing::cursor::run(data));
