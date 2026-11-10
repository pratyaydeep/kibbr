// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;

/// The flushes a file of `lines` lines is given, when every line reaches the sink.
fn flushes(lines: u64) -> u64 {
    let mut behind = 0;
    (1..=lines)
        .filter(|at| {
            let due = flushes_due(*at, lines);
            std::mem::replace(&mut behind, due) < due
        })
        .count() as u64
}

#[test]
fn a_live_day_is_flushed_1440_times_or_once_a_line_if_it_has_fewer() {
    for lines in [1_440, 1_441, 1_500, 2_880, 2_881, 3_000, 14_401, 200_000] {
        assert_eq!(flushes(lines), FLUSHES_A_DAY, "{lines} lines");
    }
    for lines in [1, 2, 590, 1_439] {
        assert_eq!(flushes(lines), lines, "{lines} lines");
    }
    // The last line is the last flush, so the flush that closes the day finds nothing
    // buffered and adds no row group of its own.
    for lines in [1, 590, 1_441, 200_000] {
        assert_eq!(flushes_due(lines, lines), FLUSHES_A_DAY, "{lines} lines");
        assert!(
            flushes_due(lines - 1, lines) < FLUSHES_A_DAY,
            "{lines} lines"
        );
    }
    assert_eq!(flushes_due(0, 0), 0);
}
