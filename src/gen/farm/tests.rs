// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;

fn farm(seed: u64, lines: u64) -> Farm {
    Farm::new(&Options {
        lines,
        seed,
        day: 19_737,
        services: MAX_SERVICES,
        dirty_ppm: 5_000,
    })
}

/// A line is unique only if its host is: these seeds each drew one pod name twice when
/// names were unchecked.
#[test]
fn no_two_pods_share_a_name() {
    for (seed, lines) in [
        (561_810, 200_000),
        (653_496, 200_000),
        (924_588, 200_000),
        (6, MAX_LINES),
        (11, MAX_LINES),
    ] {
        for names in &farm(seed, lines).hosts {
            let distinct: HashSet<&String> = names.iter().collect();
            assert_eq!(distinct.len(), names.len(), "seed {seed}: {names:?}");
        }
    }
}
