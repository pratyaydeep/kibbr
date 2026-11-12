// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;

#[test]
fn the_check_value_is_castagnolis() {
    assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    assert_eq!(crc32c(b""), 0);
}

#[test]
fn eight_zero_bytes_are_a_tear_and_not_an_empty_frame() {
    assert_eq!(read(&[0u8; 8], 0, u32::MAX), Next::Tear);
    assert_eq!(read(&[0u8; 64], 0, u32::MAX), Next::Tear);
}

#[test]
fn a_length_past_the_end_is_a_tear_before_anything_is_read() {
    let mut bytes = frame(b"{}");
    bytes[..4].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(read(&bytes, 0, u32::MAX), Next::Tear);
}

#[test]
fn every_cut_of_three_frames_reads_the_whole_ones_before_it() {
    let parts: Vec<Vec<u8>> = vec![b"a".to_vec(), b"bcd".to_vec(), b"efghij".to_vec()];
    let whole: Vec<u8> = parts.iter().flat_map(|p| frame(p)).collect();
    let mut ends = vec![0usize];
    for p in &parts {
        ends.push(ends.last().copied().unwrap_or(0) + HEADER + p.len());
    }
    for cut in 0..=whole.len() {
        let (frames, torn) = read_all(&whole[..cut], u32::MAX);
        let whole_ones = ends.iter().filter(|end| **end <= cut).count() - 1;
        assert_eq!(frames.len(), whole_ones, "cut at {cut}");
        let boundary = ends[whole_ones];
        assert_eq!(
            torn,
            (cut != boundary).then_some(boundary as u64),
            "cut at {cut}"
        );
    }
}
