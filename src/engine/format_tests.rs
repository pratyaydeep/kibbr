// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;

#[path = "format/encodings_tests.rs"]
pub(super) mod encodings;
#[path = "format/vectors_tests.rs"]
mod vectors;

#[test]
fn the_forms_print_the_spellings_the_format_names() {
    assert_eq!(print_int(45_000_012, false, 10), "0045000012");
    assert_eq!(print_int(4_500_000_120, true, 0), "+4500000120");
    assert_eq!(print_int(-17, true, 0), "-17");
    assert_eq!(print_int(0, false, 0), "0");
    assert_eq!(print_int(i64::MIN, false, 0), "-9223372036854775808");
    assert_eq!(print_float(0.01, 3), "0.010");
    assert_eq!(print_float(149.0, 2), "149.00");
    assert_eq!(print_float(-0.0, 1), "-0.0");
    assert_eq!(
        print_float(9_223_372_036_854_775_808.0, 0),
        "9223372036854775808"
    );
    let ns = 1_705_314_600_120_000_000;
    assert_eq!(
        print_rfc3339(ns, b'T', b'+', 330, 3).as_deref(),
        Some("2024-01-15T16:00:00.120+05:30")
    );
    assert_eq!(
        print_rfc3339(ns, b' ', b'-', 0, 3).as_deref(),
        Some("2024-01-15 10:30:00.120-00:00")
    );
    assert_eq!(
        print_rfc3339(ns, b'T', b'Z', 0, 1),
        None,
        "drops a nonzero digit"
    );
    assert_eq!(
        epoch_of(1_705_314_600_500_000_000, EPOCH_MILLIS),
        Some(1_705_314_600_500)
    );
    assert_eq!(epoch_of(1_705_314_600_500_000_000, EPOCH_SECS), None);
}

#[test]
fn addresses_print_in_the_spellings_the_form_names() {
    let v6 = |t: &str| ip_bytes(t).expect("an address");
    assert_eq!(
        print_ip(&v6("2001:db8::1"), 6).as_deref(),
        Some("2001:db8::1")
    );
    assert_eq!(
        print_ip(&v6("2001:db8::1"), 7).as_deref(),
        Some("2001:DB8::1")
    );
    assert_eq!(
        print_ip(&v6("2001:db8::1"), 8).as_deref(),
        Some("2001:0db8:0000:0000:0000:0000:0000:0001")
    );
    assert_eq!(
        print_ip(&v6("::ffff:192.0.2.1"), 6).as_deref(),
        Some("::ffff:192.0.2.1")
    );
    assert_eq!(print_ip(&v6("::"), 6).as_deref(), Some("::"));
    assert_eq!(
        print_ip(&v6("1:0:0:2:0:0:0:3"), 6).as_deref(),
        Some("1:0:0:2::3")
    );
    assert_eq!(
        print_ip(&v6("1:0:2:3:4:5:6:7"), 6).as_deref(),
        Some("1:0:2:3:4:5:6:7")
    );
    assert_ne!(
        print_ip(&v6("2001:0db8:0:0:0:0:0:1"), 6).as_deref(),
        Some("2001:0db8:0:0:0:0:0:1")
    );
    let id = "550E8400-E29B-41D4-A716-446655440000";
    let bytes = uuid_bytes(id).expect("a uuid");
    assert_eq!(bytes[0], 0x55);
    assert_eq!(print_uuid(&bytes, true), id);
}
