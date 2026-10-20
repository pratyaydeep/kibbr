// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! `gen-logs --profile farm`: one simulated day of a small estate's logs.
//! Built the other way round from the hostile profile: ten services each written as its
//! logger writes, request ids crossing services, skewed pools, one day in time order with
//! an incident, and dirty values on five named keys. No two lines are equal by
//! construction, and every draw is integer arithmetic on `Rng`, so a seed means the same bytes.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet, VecDeque};
use std::io::Write;

use serde_json::{Map, Value};

use crate::error::GenError;
use crate::gen::civil_from_days;
use crate::core::rng::Rng;

pub struct Options {
    pub lines: u64,
    pub seed: u64,
    /// Days since 1970-01-01.
    pub day: i64,
    /// How many of `SERVICES` run, from the front.
    pub services: usize,
    /// Dirty values per million eligible fields.
    pub dirty_ppm: u32,
}

pub const MAX_SERVICES: usize = 10;

/// The most lines one farm run writes: a minute is planned whole before it is written, so
/// memory is about a 1,440th of the run, and hosts are counted in `u16`.
pub const MAX_LINES: u64 = 1_000_000_000;
pub const DEFAULT_DAY: &str = "2024-01-15";

const DAY_MS: i64 = 86_400_000;
const MINUTE_MS: i64 = 60_000;
const MINUTES: i64 = 1440;

// ---- services ------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Svc {
    Gateway,
    Auth,
    Payments,
    Worker,
    DbProxy,
    Edge,
    Scheduler,
    Notifier,
    Search,
    Cache,
}

/// `--services N` keeps the first N. The gateway comes first so that a small N still has a
/// door and something behind it; the access log arrives at N = 6.
const SERVICES: [Svc; MAX_SERVICES] = [
    Svc::Gateway,
    Svc::Auth,
    Svc::Payments,
    Svc::Worker,
    Svc::DbProxy,
    Svc::Edge,
    Svc::Scheduler,
    Svc::Notifier,
    Svc::Search,
    Svc::Cache,
];

/// Instances at 1M lines a day. Scaled up with volume, so that no host is asked for more
/// than a few lines per millisecond at any size (see `place`).
const BASE_HOSTS: [usize; MAX_SERVICES] = [6, 3, 3, 4, 2, 3, 1, 2, 3, 2];

impl Svc {
    fn idx(self) -> usize {
        self as usize
    }
}

// ---- values and lines ----------------------------------------------------------------

#[derive(Clone, Copy)]
enum TimeForm {
    /// `2024-01-15T10:31:00.123Z`
    IsoMillisZ,
    /// `2024-01-15T10:31:00.123+00:00`, logback's
    IsoMillisOffset,
    /// `2024-01-15T10:31:00+00:00`, nginx's `$time_iso8601`
    IsoSecondsOffset,
    /// `2024-01-15T10:35:00Z`
    IsoSecondsZ,
    /// `2024-01-15T10:31:00.123456Z`
    IsoMicrosZ,
    /// `1705314660123`, a JSON number
    EpochMillis,
    /// `1705314660`, a JSON number
    EpochSeconds,
    /// `"1705314660.123"`, nginx's `$msec`
    MsecText,
}

#[derive(Clone)]
enum V {
    S(String),
    K(&'static str),
    I(i64),
    F(f64),
    B(bool),
    Arr(Vec<&'static str>),
    /// The line's own instant plus an offset in milliseconds, plus sub-millisecond
    /// microseconds for the forms that show them.
    T(TimeForm, i64, u32),
    /// A fixed instant, as milliseconds into the simulated day.
    At(TimeForm, i64),
}

/// A line before it has a time: `dt` is milliseconds after the start of its chain.
struct Line {
    svc: Svc,
    host: u16,
    dt: i64,
    fields: Vec<(&'static str, V)>,
}

impl Line {
    fn new(svc: Svc, host: u16, dt: i64) -> Self {
        Line {
            svc,
            host,
            dt,
            fields: Vec::with_capacity(20),
        }
    }

    fn put(&mut self, key: &'static str, value: V) -> &mut Self {
        self.fields.push((key, value));
        self
    }
}

fn cents(c: i64) -> V {
    V::F(c as f64 / 100.0)
}

// ---- samplers ------------------------------------------------------------------------

/// A draw over integer weights, so no floating-point library call sits between the seed
/// and the output.
struct Weighted {
    cum: Vec<u64>,
}

impl Weighted {
    fn new(weights: impl IntoIterator<Item = u64>) -> Self {
        let mut acc = 0u64;
        let cum = weights
            .into_iter()
            .map(|w| {
                acc += w;
                acc
            })
            .collect();
        Weighted { cum }
    }

    /// Weight 1/(rank + q): Zipf with exponent 1, and `q` sets how steep the head is.
    fn zipf(n: usize, q: u64) -> Self {
        Self::new((0..n as u64).map(|i| (1u64 << 40) / (i + q)))
    }

    fn draw(&self, rng: &mut Rng) -> usize {
        let total = *self.cum.last().expect("a sampler over nothing");
        let r = rng.below(total);
        self.cum.partition_point(|&c| c <= r)
    }
}

fn pick_w<T: Copy>(rng: &mut Rng, items: &[(u64, T)]) -> T {
    let total: u64 = items.iter().map(|x| x.0).sum();
    let mut r = rng.below(total);
    for &(w, v) in items {
        if r < w {
            return v;
        }
        r -= w;
    }
    items[items.len() - 1].1
}

/// A stable per-entity hash, so a user's home address, device and card are the same on
/// every line of the day without being stored.
fn mix(a: u64, b: u64) -> u64 {
    let mut z = a ^ b.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn hex(rng: &mut Rng, nibbles: usize) -> String {
    let mut s = String::with_capacity(nibbles);
    while s.len() < nibbles {
        let word = rng.next_u64();
        for i in 0..16 {
            if s.len() == nibbles {
                break;
            }
            let n = ((word >> (60 - 4 * i)) & 0xf) as u8;
            s.push(char::from(if n < 10 { b'0' + n } else { b'a' + n - 10 }));
        }
    }
    s
}

fn uuid(rng: &mut Rng) -> String {
    let h = hex(rng, 32);
    format!(
        "{}-{}-4{}-{}{}-{}",
        &h[0..8],
        &h[8..12],
        &h[13..16],
        ["8", "9", "a", "b"][rng.below(4) as usize],
        &h[17..20],
        &h[20..32]
    )
}

fn base62(rng: &mut Rng, n: usize) -> String {
    const A: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    (0..n)
        .map(|_| char::from(A[rng.below(62) as usize]))
        .collect()
}

fn ipv4(v: u32) -> String {
    format!(
        "{}.{}.{}.{}",
        v >> 24,
        (v >> 16) & 0xff,
        (v >> 8) & 0xff,
        v & 0xff
    )
}

fn ip_in(a: u8, b: u8, h: u64) -> u32 {
    let c = (h & 0xff) as u32;
    let d = 1 + ((h >> 8) % 254) as u32;
    (a as u32) << 24 | (b as u32) << 16 | c << 8 | d
}

/// Seconds as nginx writes them: `0.012`.
fn secs3(ms: i64) -> String {
    format!("{}.{:03}", ms / 1000, ms % 1000)
}

/// A latency in tenths of a millisecond: skewed toward `lo`, with a slow tail.
fn latency(rng: &mut Rng, lo_ms: i64, hi_ms: i64) -> i64 {
    let a = rng.between(lo_ms * 10, hi_ms * 10);
    let b = rng.between(lo_ms * 10, hi_ms * 10);
    let base = a.min(b);
    if rng.below(1000) < 15 {
        base * rng.between(3, 10)
    } else {
        base
    }
}

// ---- tables --------------------------------------------------------------------------

/// Every browser and app a person arrives with: families times versions times devices, one
/// fixed per user. A flat dozen made the user-agent columns far too compressible.
fn human_agents() -> Vec<(u64, String, bool)> {
    const CHROME: [u32; 13] = [
        121, 120, 119, 118, 117, 116, 115, 114, 113, 112, 111, 110, 109,
    ];
    const IOS: [&str; 14] = [
        "17.3", "17.2.1", "17.2", "17.1.2", "17.1", "17.0.3", "16.7.5", "16.7.2", "16.6.1", "16.6",
        "16.5", "16.3.1", "15.8", "15.7.9",
    ];
    const MAC_SAFARI: [&str; 7] = ["17.2.1", "17.2", "17.1", "17.0", "16.6", "16.5", "15.6.1"];
    const FIREFOX_OS: [&str; 4] = [
        "Windows NT 10.0; Win64; x64",
        "Macintosh; Intel Mac OS X 10.15",
        "X11; Linux x86_64",
        "X11; Ubuntu; Linux x86_64",
    ];
    const EDGE_BUILDS: [(u32, u32); 3] = [(2277, 83), (2277, 112), (2210, 91)];
    const SAMSUNG: [&str; 10] = [
        "SM-S911B", "SM-S918B", "SM-A536B", "SM-G991B", "SM-A546B", "SM-S901B", "SM-A145R",
        "SM-G998B", "SM-A336B", "SM-F936B",
    ];
    const SAMSUNG_VER: [(&str, u32); 3] = [("23.0", 115), ("22.0", 111), ("21.0", 110)];
    const IPHONES: [&str; 7] = [
        "iPhone15,2",
        "iPhone14,5",
        "iPhone15,3",
        "iPhone14,2",
        "iPhone13,2",
        "iPhone16,1",
        "iPhone12,1",
    ];
    const FBAV: [(&str, u32); 4] = [
        ("446.0.0.37.106", 553_614_727),
        ("445.0.0.34.118", 551_738_219),
        ("444.0.0.34.103", 549_816_473),
        ("443.0.0.28.107", 547_402_337),
    ];
    const IG_DEVICES: [(&str, &str, &str, &str, &str); 5] = [
        ("Pixel 7", "panther", "Google/google", "1080x2400", "420dpi"),
        ("Pixel 8", "shiba", "Google/google", "1080x2400", "420dpi"),
        ("SM-S911B", "dm1q", "samsung", "1080x2340", "450dpi"),
        ("SM-A536B", "a53x", "samsung", "1080x2400", "450dpi"),
        ("moto g(30)", "caprip", "motorola", "720x1600", "280dpi"),
    ];
    const IG_VER: [(&str, u32); 3] = [
        ("311.0.0.32.118", 541_635_890),
        ("310.1.0.41.113", 539_210_453),
        ("309.1.0.41.113", 537_288_532),
    ];
    const CHROME_FULL: [&str; 3] = ["120.0.6099.144", "120.0.6099.43", "119.0.6045.193"];
    const ANDROID: [(u32, u32, &str); 4] = [
        (14, 34, "UQ1A.240105.004"),
        (13, 33, "TQ3A.230901.001"),
        (12, 32, "SQ3A.220705.004"),
        (11, 30, "RQ3A.211001.001"),
    ];
    const APP: [&str; 6] = ["5.12.1", "5.12.0", "5.11.2", "5.11.0", "5.10.3", "5.9.0"];
    const APP_IOS: [&str; 6] = ["17.3", "17.2.1", "17.2", "17.1.2", "16.7.5", "16.6"];
    const PHONES: [&str; 8] = [
        "Pixel 8",
        "Pixel 7",
        "SM-S911B",
        "SM-A536B",
        "SM-G991B",
        "moto g(30)",
        "Redmi Note 12",
        "CPH2451",
    ];

    let mut out: Vec<(u64, String, bool)> = Vec::new();
    // A family's share, spread over its variants by rank: 1/(rank + 1) along every axis.
    let mut family = |share: u64, app: bool, variants: Vec<(Vec<usize>, String)>| {
        let raw: Vec<u128> = variants
            .iter()
            .map(|(ranks, _)| ranks.iter().fold(1u128 << 40, |w, r| w / (*r as u128 + 1)))
            .collect();
        let total: u128 = raw.iter().sum();
        for ((_, ua), w) in variants.into_iter().zip(raw) {
            out.push(((share as u128 * 1_000_000 * w / total) as u64 + 1, ua, app));
        }
    };
    let short = |v: &str| v.split('.').take(2).collect::<Vec<_>>().join(".");
    let chrome = |os: &str, tail: &str| -> Vec<(Vec<usize>, String)> {
        CHROME
            .iter()
            .enumerate()
            .map(|(i, c)| {
                (
                    vec![i],
                    format!("Mozilla/5.0 ({os}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{c}.0.0.0 {tail}"),
                )
            })
            .collect()
    };
    family(
        300,
        false,
        chrome("Windows NT 10.0; Win64; x64", "Safari/537.36"),
    );
    family(
        100,
        false,
        chrome("Macintosh; Intel Mac OS X 10_15_7", "Safari/537.36"),
    );
    family(
        140,
        false,
        chrome("Linux; Android 10; K", "Mobile Safari/537.36"),
    );
    let mut v = Vec::new();
    for (i, c) in CHROME.iter().enumerate() {
        for (j, (b, p)) in EDGE_BUILDS.iter().enumerate() {
            v.push((vec![i, j], format!("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{c}.0.0.0 Safari/537.36 Edg/{c}.0.{b}.{p}")));
        }
    }
    family(50, false, v);
    let ios = |device: &str, os: &str| -> Vec<(Vec<usize>, String)> {
        IOS.iter()
            .enumerate()
            .map(|(i, ver)| {
                (
                    vec![i],
                    format!(
                        "Mozilla/5.0 ({device}; CPU {os} {} like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/{} Mobile/15E148 Safari/604.1",
                        ver.replace('.', "_"),
                        short(ver)
                    ),
                )
            })
            .collect()
    };
    family(220, false, ios("iPhone", "iPhone OS"));
    family(20, false, ios("iPad", "OS"));
    family(
        60,
        false,
        MAC_SAFARI
            .iter()
            .enumerate()
            .map(|(i, ver)| (vec![i], format!("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/{ver} Safari/605.1.15")))
            .collect(),
    );
    let mut v = Vec::new();
    for (i, ff) in (110..=122).rev().enumerate() {
        for (j, os) in FIREFOX_OS.iter().enumerate() {
            v.push((
                vec![i, j],
                format!("Mozilla/5.0 ({os}; rv:{ff}.0) Gecko/20100101 Firefox/{ff}.0"),
            ));
        }
    }
    family(40, false, v);
    let mut v = Vec::new();
    for (i, model) in SAMSUNG.iter().enumerate() {
        for (j, (sv, cv)) in SAMSUNG_VER.iter().enumerate() {
            for (k, a) in [14, 13].iter().enumerate() {
                v.push((vec![i, j, k], format!("Mozilla/5.0 (Linux; Android {a}; {model}) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/{sv} Chrome/{cv}.0.0.0 Mobile Safari/537.36")));
            }
        }
    }
    family(40, false, v);
    let mut v = Vec::new();
    for (i, ver) in IOS.iter().enumerate() {
        for (j, dev) in IPHONES.iter().enumerate() {
            for (k, (fbav, fbbv)) in FBAV.iter().enumerate() {
                v.push((vec![i, j, k], format!("Mozilla/5.0 (iPhone; CPU iPhone OS {} like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/21C66 [FBAN/FBIOS;FBAV/{fbav};FBBV/{fbbv};FBDV/{dev};FBMD/iPhone;FBSN/iOS;FBSV/{ver};FBSS/3;FBID/phone;FBLC/en_US;FBOP/5]", ver.replace('.', "_"))));
            }
        }
    }
    family(30, false, v);
    let mut v = Vec::new();
    for (i, (model, dev, brand, res, dpi)) in IG_DEVICES.iter().enumerate() {
        for (j, (igv, igb)) in IG_VER.iter().enumerate() {
            for (k, cv) in CHROME_FULL.iter().enumerate() {
                for (l, (a, api, build)) in ANDROID.iter().take(2).enumerate() {
                    v.push((vec![i, j, k, l], format!("Mozilla/5.0 (Linux; Android {a}; {model} Build/{build}; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/{cv} Mobile Safari/537.36 Instagram {igv} Android ({api}/{a}; {dpi}; {res}; {brand}; {model}; {dev}; {dev}; en_US; {igb})")));
                }
            }
        }
    }
    family(25, false, v);
    let mut v = Vec::new();
    for (i, app) in APP.iter().enumerate() {
        for (j, os) in APP_IOS.iter().enumerate() {
            for (k, scale) in ["3.00", "2.00"].iter().enumerate() {
                v.push((
                    vec![i, j, k],
                    format!("ShopApp/{app} (iPhone; iOS {os}; Scale/{scale})"),
                ));
            }
        }
    }
    family(80, true, v);
    let mut v = Vec::new();
    for (i, app) in APP.iter().enumerate() {
        for (j, (a, _, _)) in ANDROID.iter().enumerate() {
            for (k, phone) in PHONES.iter().enumerate() {
                for (l, ok) in ["4.12.0", "4.11.0"].iter().enumerate() {
                    v.push((
                        vec![i, j, k, l],
                        format!("ShopApp/{app} (Linux; Android {a}; {phone}) okhttp/{ok}"),
                    ));
                }
            }
        }
    }
    family(60, true, v);
    out
}

/// Search terms: a head of short common queries, then a long tail of one-offs, so the
/// column has singletons, as a real site search's does.
fn search_terms(rng: &mut Rng) -> Vec<String> {
    const BRANDS: [&str; 8] = [
        "acme",
        "northwind",
        "contoso",
        "fabrikam",
        "lumen",
        "orbit",
        "kestrel",
        "tandem",
    ];
    const FOR: [&str; 5] = ["men", "women", "kids", "dogs", "travel"];
    let mut head: Vec<String> = ADJ
        .iter()
        .flat_map(|a| NOUN.iter().map(move |n| format!("{a} {n}")))
        .chain(NOUN.iter().map(|n| n.to_string()))
        .collect();
    rng.shuffle(&mut head);
    let tail = (0..6_000).map(|_| {
        let (a, b, n) = (*rng.pick(ADJ), *rng.pick(ADJ), *rng.pick(NOUN));
        match rng.below(10) {
            0..=3 => format!("{a} {b} {n}"),
            4 | 5 => format!("{n} for {}", rng.pick(&FOR)),
            6 => format!("{n} under {}", [25, 50, 100, 200][rng.below(4) as usize]),
            7 => format!("{} {n}", rng.pick(&BRANDS)),
            8 => {
                let term = format!("{a} {n}");
                let cut = 1 + rng.below(term.len() as u64 - 2) as usize;
                format!("{}{}", &term[..cut], &term[cut + 1..])
            }
            _ => format!("{n} {}", 10_000 + rng.below(90_000)),
        }
    });
    head.extend(tail);
    head
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum BotKind {
    Crawler,
    Scanner,
}

/// Weight, user agent, the /16 it comes from, and what it does.
const BOTS: &[(u64, &str, (u8, u8), BotKind)] = &[
    (
        150,
        "Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.216 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
        (66, 249),
        BotKind::Crawler,
    ),
    (
        400,
        "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
        (66, 249),
        BotKind::Crawler,
    ),
    (
        200,
        "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
        (40, 77),
        BotKind::Crawler,
    ),
    (
        100,
        "Mozilla/5.0 (compatible; AhrefsBot/7.0; +http://ahrefs.com/robot/)",
        (54, 36),
        BotKind::Crawler,
    ),
    (70, "python-requests/2.31.0", (34, 201), BotKind::Scanner),
    (50, "curl/8.4.0", (45, 148), BotKind::Scanner),
    (50, "Go-http-client/1.1", (185, 220), BotKind::Scanner),
    (80, "Mozilla/5.0 zgrab/0.x", (193, 118), BotKind::Scanner),
];

/// Home networks, as /16s.
const ISP: &[(u8, u8)] = &[
    (73, 15),
    (73, 222),
    (98, 207),
    (24, 5),
    (71, 178),
    (76, 102),
    (108, 31),
    (174, 53),
    (67, 180),
    (81, 2),
    (86, 140),
    (92, 184),
    (109, 150),
    (2, 96),
    (151, 40),
    (88, 160),
    (49, 205),
    (101, 167),
    (125, 24),
    (186, 212),
];

/// Mobile carriers: few addresses, many users behind each.
const MOBILE: &[(u8, u8)] = &[(172, 56), (172, 58), (107, 77), (166, 137)];

const COUNTRY: &[(u64, (&str, &str))] = &[
    (55, ("US", "USD")),
    (10, ("GB", "GBP")),
    (8, ("DE", "EUR")),
    (6, ("FR", "EUR")),
    (6, ("CA", "CAD")),
    (5, ("AU", "AUD")),
    (4, ("NL", "EUR")),
    (3, ("ES", "EUR")),
    (3, ("IN", "INR")),
];

const CARDS: &[(u64, &str)] = &[
    (55, "visa"),
    (30, "mastercard"),
    (10, "amex"),
    (5, "discover"),
];

const ADJ: &[&str] = &[
    "red",
    "black",
    "wireless",
    "running",
    "leather",
    "organic",
    "vintage",
    "kids",
    "waterproof",
    "mini",
    "smart",
    "cotton",
    "gaming",
    "stainless",
    "linen",
    "ceramic",
];
const NOUN: &[&str] = &[
    "shoes",
    "headphones",
    "jacket",
    "backpack",
    "watch",
    "lamp",
    "mug",
    "sofa",
    "keyboard",
    "dress",
    "tent",
    "bottle",
    "charger",
    "sneakers",
    "blender",
    "desk",
];
const CATEGORIES: &[&str] = &[
    "shoes", "audio", "outdoor", "home", "kitchen", "kids", "sports", "office", "beauty", "garden",
    "toys", "fashion",
];
const IMAGES: &[&str] = &[
    "hero-winter",
    "hero-sale",
    "logo",
    "banner-shoes",
    "banner-audio",
    "promo-free-shipping",
    "icon-cart",
    "icon-user",
    "icon-search",
    "badge-new",
    "badge-sale",
    "footer-payments",
];
const SCAN_PATHS: &[&str] = &[
    "/wp-login.php",
    "/.env",
    "/xmlrpc.php",
    "/.git/config",
    "/phpmyadmin/index.php",
    "/admin/.env",
    "/vendor/phpunit/phpunit/src/Util/PHP/eval-stdin.php",
    "/actuator/env",
    "/server-status",
    "/.aws/credentials",
    "/boaform/admin/formLogin",
    "/cgi-bin/luci/;stok=/locale",
];
const REC_SLOTS: &[&str] = &["home", "pdp", "cart", "checkout"];

#[derive(Clone, Copy, PartialEq, Eq)]
enum Route {
    Static,
    Page,
    Product,
    Search,
    Cart,
    CartAdd,
    Login,
    Refresh,
    Checkout,
    Order,
    Me,
    Recs,
    Logout,
    Signup,
    Refund,
    Scan,
}

const HUMAN_ROUTES: &[(u64, Route)] = &[
    // Stragglers only - the favicon, the manifest, a service worker. A page's own assets
    // come with the page (`page_assets`).
    (250, Route::Static),
    (1000, Route::Page),
    (1800, Route::Product),
    (900, Route::Search),
    (600, Route::Cart),
    (300, Route::CartAdd),
    (200, Route::Login),
    (300, Route::Refresh),
    (120, Route::Checkout),
    (250, Route::Order),
    (400, Route::Me),
    (500, Route::Recs),
    (50, Route::Logout),
    (10, Route::Signup),
    (3, Route::Refund),
];
const CRAWLER_ROUTES: &[(u64, Route)] = &[
    (1500, Route::Static),
    (2500, Route::Page),
    (4000, Route::Product),
    (500, Route::Search),
];
const SCANNER_ROUTES: &[(u64, Route)] = &[(9000, Route::Scan), (1000, Route::Page)];

struct RouteSpec {
    method: &'static str,
    route: &'static str,
    /// The service behind the gateway, by the name the gateway logs.
    upstream: &'static str,
    login: bool,
    lat: (i64, i64),
    bytes: (i64, i64),
}

fn spec(r: Route) -> RouteSpec {
    let s = |method, route, upstream, login, lat, bytes| RouteSpec {
        method,
        route,
        upstream,
        login,
        lat,
        bytes,
    };
    match r {
        Route::Static => s("GET", "/static", "", false, (1, 8), (400, 180_000)),
        Route::Page => s(
            "GET",
            "/{page}",
            "storefront",
            false,
            (40, 400),
            (8_000, 90_000),
        ),
        Route::Product => s(
            "GET",
            "/api/v1/products/{id}",
            "catalog",
            false,
            (15, 120),
            (800, 6_000),
        ),
        Route::Search => s(
            "GET",
            "/api/v1/search",
            "search",
            false,
            (40, 400),
            (2_000, 40_000),
        ),
        Route::Cart => s("GET", "/api/v1/cart", "cart", false, (8, 60), (200, 4_000)),
        Route::CartAdd => s(
            "POST",
            "/api/v1/cart/items",
            "cart",
            false,
            (15, 90),
            (150, 900),
        ),
        Route::Login => s(
            "POST",
            "/api/v1/auth/login",
            "auth",
            false,
            (150, 500),
            (300, 900),
        ),
        Route::Refresh => s(
            "POST",
            "/api/v1/auth/refresh",
            "auth",
            false,
            (8, 50),
            (300, 700),
        ),
        Route::Checkout => s(
            "POST",
            "/api/v1/checkout",
            "payments",
            true,
            (500, 2000),
            (400, 1_200),
        ),
        Route::Order => s(
            "GET",
            "/api/v1/orders/{id}",
            "orders",
            true,
            (15, 120),
            (600, 3_000),
        ),
        Route::Me => s(
            "GET",
            "/api/v1/users/me",
            "profile",
            true,
            (8, 50),
            (300, 1_500),
        ),
        Route::Recs => s(
            "GET",
            "/api/v1/recommendations",
            "recs",
            false,
            (40, 250),
            (1_500, 12_000),
        ),
        Route::Logout => s(
            "POST",
            "/api/v1/auth/logout",
            "auth",
            true,
            (4, 20),
            (0, 60),
        ),
        Route::Signup => s(
            "POST",
            "/api/v1/users",
            "auth",
            false,
            (250, 800),
            (200, 600),
        ),
        Route::Refund => s(
            "POST",
            "/api/v1/orders/{id}/refund",
            "payments",
            true,
            (300, 1200),
            (200, 600),
        ),
        Route::Scan => s("GET", "/scan", "", false, (0, 2), (150, 600)),
    }
}

/// Statements as the proxy logs them, by the role that runs them.
const SQL_PRODUCT: &str = "SELECT id, name, price_cents, stock FROM products WHERE id = $1";
const SQL_CART_READ: &str = "SELECT product_id, qty FROM cart_items WHERE cart_id = $1";
const SQL_CART_ADD: &str = "INSERT INTO cart_items (cart_id, product_id, qty) VALUES ($1, $2, $3)";
const SQL_USER_BY_EMAIL: &str = "SELECT id, password_hash, mfa_enabled FROM users WHERE email = $1";
const SQL_LAST_LOGIN: &str = "UPDATE users SET last_login_at = now() WHERE id = $1";
const SQL_USER_INSERT: &str =
    "INSERT INTO users (email, password_hash) VALUES ($1, $2) RETURNING id";
const SQL_ORDER_INSERT: &str =
    "INSERT INTO orders (user_id, total_cents, currency) VALUES ($1, $2, $3) RETURNING id";
const SQL_RESERVE: &str = "UPDATE inventory SET reserved = reserved + $1 WHERE product_id = $2";
const SQL_PAYMENT_INSERT: &str = "INSERT INTO payments (order_id, psp, status) VALUES ($1, $2, $3)";
const SQL_PAYMENT_UPDATE: &str =
    "UPDATE payments SET status = $1, psp_ref = $2 WHERE order_id = $3";
const SQL_ORDER_READ: &str =
    "SELECT id, status, total_cents, created_at FROM orders WHERE id = $1 AND user_id = $2";
const SQL_PROFILE: &str =
    "SELECT id, display_name, locale, marketing_opt_in FROM users WHERE id = $1";
const SQL_STOCK_SYNC: &str =
    "UPDATE inventory SET on_hand = $1 WHERE product_id = $2 AND warehouse = $3";
const SQL_SESSION_GC: &str = "DELETE FROM sessions WHERE expires_at < now() AND shard = $1";
const SQL_CART_GC: &str =
    "DELETE FROM cart_items WHERE updated_at < now() - interval '30 days' AND shard = $1";
const SQL_ROLLUP: &str = "INSERT INTO metrics_1m (tenant_id, bucket, n) SELECT $1, $2, count(*) FROM events WHERE tenant_id = $1";
const SQL_EXPORT: &str =
    "SELECT * FROM orders WHERE created_at >= $1 AND created_at < $2 ORDER BY id";

const PG_ERRORS: &[(u64, &str, &str)] = &[
    (70, "no more connections allowed (max_client_conn)", "08P01"),
    (25, "query_wait_timeout", "08P01"),
    (5, "canceling statement due to statement timeout", "57014"),
];

const HIKARI_TRACE: &str = "java.sql.SQLTransientConnectionException: HikariPool-1 - Connection is not available, request timed out after 10000ms.\n\tat com.zaxxer.hikari.pool.HikariPool.createTimeoutException(HikariPool.java:696)\n\tat com.zaxxer.hikari.pool.HikariPool.getConnection(HikariPool.java:197)\n\tat com.shop.payments.PaymentRepository.save(PaymentRepository.java:88)\n\tat com.shop.payments.PaymentService.authorize(PaymentService.java:142)\n\tat com.shop.payments.CheckoutController.checkout(CheckoutController.java:61)";
const PSP_TRACE: &str = "com.stripe.exception.ApiConnectionException: IOException during API request to Stripe (https://api.stripe.com): Read timed out\n\tat com.stripe.net.HttpURLConnectionClient.request(HttpURLConnectionClient.java:103)\n\tat com.stripe.net.LiveStripeResponseGetter.request(LiveStripeResponseGetter.java:62)\n\tat com.shop.payments.psp.StripeGateway.authorize(StripeGateway.java:77)\n\tat com.shop.payments.PaymentService.authorize(PaymentService.java:151)";

const DECLINES: &[(u64, &str)] = &[
    (45, "insufficient_funds"),
    (25, "do_not_honor"),
    (15, "expired_card"),
    (10, "incorrect_cvc"),
    (5, "fraudulent"),
];

const LOGIN_FAIL: &[(u64, &str)] = &[
    (70, "invalid_password"),
    (20, "unknown_user"),
    (5, "account_locked"),
    (5, "mfa_failed"),
];

/// A background job, drawn by `weight`; `ms` is its duration range.
struct Job {
    weight: u64,
    queue: &'static str,
    kind: &'static str,
    ms: (i64, i64),
    fail_per_mille: u64,
}

const fn job(
    weight: u64,
    queue: &'static str,
    kind: &'static str,
    ms: (i64, i64),
    fail_per_mille: u64,
) -> Job {
    Job {
        weight,
        queue,
        kind,
        ms,
        fail_per_mille,
    }
}

const JOBS: &[Job] = &[
    job(40, "webhooks", "deliver_webhook", (40, 1800), 40),
    job(25, "media", "resize_image", (150, 1400), 8),
    job(15, "inventory", "sync_inventory_item", (20, 300), 5),
    job(12, "search", "reindex_product", (30, 400), 3),
    job(6, "emails", "send_digest", (80, 600), 10),
    job(2, "exports", "generate_export", (4_000, 45_000), 30),
];

const WORKER_ERRORS: &[(u64, &str)] = &[
    (40, "context deadline exceeded"),
    (25, "http 503 from partner endpoint"),
    (15, "dial tcp 10.2.1.4:5432: connect: connection refused"),
    (10, "image decode: unexpected EOF"),
    (10, "upstream returned 429, retry-after 30"),
];

/// A scheduled job: due every `period` minutes from minute `offset` of the day, run once
/// per shard, each run lasting somewhere in `ms`.
struct Cron {
    name: &'static str,
    schedule: &'static str,
    period: i64,
    offset: i64,
    shards: u32,
    ms: (i64, i64),
}

const fn cron(
    name: &'static str,
    schedule: &'static str,
    period: i64,
    offset: i64,
    shards: u32,
    ms: (i64, i64),
) -> Cron {
    Cron {
        name,
        schedule,
        period,
        offset,
        shards,
        ms,
    }
}

const CRON: &[Cron] = &[
    cron("cleanup-sessions", "*/5 * * * *", 5, 0, 4, (400, 6_000)),
    cron("expire-carts", "*/10 * * * *", 10, 0, 2, (800, 12_000)),
    cron("sync-inventory", "*/15 * * * *", 15, 0, 6, (2_000, 25_000)),
    cron("rotate-signing-keys", "0 * * * *", 60, 0, 1, (100, 800)),
    cron(
        "nightly-report",
        "0 2 * * *",
        1440,
        120,
        1,
        (30_000, 90_000),
    ),
    cron(
        "reindex-catalog",
        "30 3 * * *",
        1440,
        210,
        4,
        (20_000, 60_000),
    ),
];
/// The every-minute job, one run per tenant, which fills whatever the table above leaves.
const ROLLUP: (&str, &str) = ("rollup-metrics", "* * * * *");
const TENANTS: u32 = 400;

/// What the gateway waits for an upstream, and what payments' connection pool waits for
/// a connection - the same ten seconds, so in the incident both give up together.
const GATEWAY_TIMEOUT_MS: i64 = 10_000;

const PROBE_UA: &str = "kube-probe/1.28";

/// Where a browser's first page came from, when it says.
const EXTERNAL_REFERERS: &[(u64, &str)] = &[
    (60, "https://www.google.com/"),
    (15, "https://www.bing.com/"),
    (12, "https://l.instagram.com/"),
    (8, "https://l.facebook.com/"),
    (5, "https://duckduckgo.com/"),
];

/// Index of the kubelet's user agent in `Farm::agents`; the bots follow it, then people.
const PROBE: usize = 0;

/// The worker's queues, in the order its stats lines take them.
const QUEUES: [&str; 7] = [
    "webhooks",
    "media",
    "inventory",
    "search",
    "emails",
    "exports",
    "payments",
];

const GO_PANIC: &str = "goroutine 4817 [running]:\nruntime/debug.Stack()\n\t/usr/local/go/src/runtime/debug/stack.go:24 +0x5e\ngithub.com/shop/gateway/internal/middleware.Recover.func1.1()\n\t/src/internal/middleware/recover.go:31 +0x8b\npanic({0x1a2b3c0?, 0x2d4e5f0?})\n\t/usr/local/go/src/runtime/panic.go:914 +0x21f\ngithub.com/shop/gateway/internal/route.(*Table).Match(0x0, {0xc0004a2000, 0x1c})\n\t/src/internal/route/table.go:88 +0x3a";

const PARTNERS: &[&str] = &[
    "hooks.erp.example.net",
    "api.3pl.example.com",
    "webhooks.crm.example.org",
    "ingest.analytics.example.io",
    "notify.marketplace.example.com",
];

/// How many taken milliseconds are remembered before the stale ones are dropped.
const USED_FLOOR: usize = 50_000;

// ---- the simulation ------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum Src {
    Web,
    Probe,
    Jobs,
    Cron,
    Stats,
    Storm,
}

/// Shares of the day's lines, in per mille. Probes and housekeeping do not follow the daily
/// curve; `Storm` is the incident's own, silent all day and then among the busiest.
const SOURCES: &[(Src, u64)] = &[
    (Src::Web, 784),
    (Src::Probe, 85),
    (Src::Jobs, 65),
    (Src::Cron, 25),
    (Src::Stats, 35),
    (Src::Storm, 6),
];

/// Traffic by hour of day, UTC, at each hour's midpoint: a trough near 04:00, a peak mid
/// afternoon, a smaller evening shoulder.
const HOURLY: [i64; 24] = [
    34, 28, 24, 22, 21, 23, 28, 38, 52, 66, 78, 86, 90, 93, 96, 100, 97, 90, 82, 76, 70, 62, 52, 42,
];

#[derive(Clone)]
struct Session {
    uid: i64,
    ip: u32,
    /// Index into `Farm::agents`.
    ua: usize,
    bot: Option<BotKind>,
    /// The shop's own app: no HTML pages, no static bundle, no referer.
    app: bool,
    logged_in: bool,
    wants_login: bool,
    /// Holds the static bundle already, so a page brings its images and not the bundle.
    warm: bool,
    /// The last page this browser loaded - the referer of whatever it asks for next.
    page: Option<String>,
    left: u16,
}

/// What one minute of the day is like.
#[derive(Clone, Copy)]
struct Minute {
    m: i64,
    incident: bool,
    /// Percent of gateway requests on the new pods: 0 before the rollout, 100 from ten
    /// minutes after it, rising 10 a minute between.
    gw_new_pct: u64,
}

/// The per-line state that must not repeat: which (service, host, millisecond) is taken.
type Slot = (u8, u16, i64);

struct Pending {
    t: i64,
    seq: u64,
    line: Line,
}

impl PartialEq for Pending {
    fn eq(&self, other: &Self) -> bool {
        (self.t, self.seq) == (other.t, other.seq)
    }
}
impl Eq for Pending {}
impl PartialOrd for Pending {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Pending {
    // Reversed: `BinaryHeap` is a max-heap and the earliest line leaves first.
    fn cmp(&self, other: &Self) -> Ordering {
        (other.t, other.seq).cmp(&(self.t, self.seq))
    }
}

struct Farm {
    rng: Rng,
    seed: u64,
    on: [bool; MAX_SERVICES],
    dirty_ppm: u64,
    day_ms: i64,

    hosts: Vec<Vec<String>>,
    host_ip: Vec<Vec<u32>>,
    node_ip: Vec<Vec<u32>>,
    notifier_pid: i64,
    /// Gateway pods per generation; the second generation replaces the first at rollout.
    gw_pods: usize,
    rollout: i64,
    incident: (i64, i64),
    bursts: Vec<(i64, i64, i64)>,
    static_build: [String; 2],
    storefront_deploy: i64,

    users: Weighted,
    products: Weighted,
    product_ids: Vec<i64>,
    terms: Vec<String>,
    term_w: Weighted,
    /// Every user agent: the kubelet's, the bots', then people's (`human_agents`).
    agents: Vec<String>,
    app_agent: Vec<bool>,
    human_agents: Weighted,
    bots: Weighted,
    human_routes: Vec<(u64, Route)>,
    crawler_routes: Vec<(u64, Route)>,
    scanner_routes: Vec<(u64, Route)>,
    upstream_ip: Vec<(&'static str, u32)>,
    images: Weighted,
    scans: Weighted,
    categories: Weighted,
    threads: Weighted,
    jobs: Weighted,
    partners: Weighted,
    pg_errors: Weighted,
    key_rotation: i64,

    sessions: VecDeque<Session>,
    session_cap: usize,
    next_order: i64,
    next_message: i64,
    tenant_cursor: u32,
    cron_minute: i64,
    cron_cursor: usize,
    new_users: i64,
    /// Per cache host: keyspace hits, misses and evictions, which only ever grow.
    cache_counters: Vec<[i64; 3]>,
    probe_rr: usize,
    stats_rr: usize,
    rollout_logged: usize,
    used: HashSet<Slot>,
    used_limit: usize,
}

pub fn generate<W: Write>(out: &mut W, opts: &Options) -> Result<(), GenError> {
    let mut farm = Farm::new(opts);
    farm.run(out, opts.lines)
}

impl Farm {
    fn new(opts: &Options) -> Self {
        let mut rng = Rng::new(opts.seed);
        let mut on = [false; MAX_SERVICES];
        for s in SERVICES.iter().take(opts.services) {
            on[s.idx()] = true;
        }
        let scale = 1 + (opts.lines / 5_000_000) as usize;

        // Pods: a deployment shares its ReplicaSet hash, and each pod sits on a node.
        let nodes: Vec<u32> = (0..6)
            .map(|i| (10 << 24) | ((1 + i / 2) << 8) | (20 + rng.below(200) as u32))
            .collect();
        let mut hosts = Vec::new();
        let mut host_ip = Vec::new();
        let mut node_ip = Vec::new();
        let gw_pods = BASE_HOSTS[Svc::Gateway.idx()] * scale;
        for (i, svc) in SERVICES.iter().enumerate() {
            let n = BASE_HOSTS[i] * scale;
            // The gateway runs two generations of pods over the day, one after the other.
            let generations = if *svc == Svc::Gateway { 2 } else { 1 };
            let mut names = Vec::new();
            // A pod name is a line's host, and a line is only unique if its host is: a
            // repeated suffix is drawn again.
            let mut taken = HashSet::new();
            for _ in 0..generations {
                let rs = pod_token(&mut rng, 10);
                for k in 0..n {
                    names.push(match svc {
                        Svc::Edge => format!("edge-{}", k + 1),
                        Svc::DbProxy => format!("pgbouncer-{k}"),
                        Svc::Scheduler => format!("scheduler-{k}"),
                        Svc::Cache => format!("redis-proxy-{k}"),
                        _ => loop {
                            let name =
                                format!("{}-{rs}-{}", deployment(*svc), pod_token(&mut rng, 5));
                            if taken.insert(name.clone()) {
                                break name;
                            }
                        },
                    });
                }
            }
            let ips = names
                .iter()
                .map(|_| {
                    (10 << 24)
                        | (1 << 16)
                        | ((rng.below(8) as u32) << 8)
                        | (2 + rng.below(250) as u32)
                })
                .collect();
            let on_node = names.iter().map(|_| *rng.pick(&nodes)).collect();
            hosts.push(names);
            host_ip.push(ips);
            node_ip.push(on_node);
        }
        let notifier_pid = rng.between(7, 90);

        let rollout = rng.between(9 * 60, 16 * 60);
        let incident_start = rng.between(10 * 60, 19 * 60);
        let incident = (incident_start, incident_start + rng.between(35, 50));
        let bursts = (0..5)
            .map(|_| {
                let start = rng.between(7 * 60, 23 * 60);
                (start, rng.between(3, 12), rng.between(200, 400))
            })
            .collect();
        let static_build = [hex(&mut rng, 8), hex(&mut rng, 8)];
        let storefront_deploy = rng.between(8 * 60, 18 * 60);
        let key_rotation = rng.between(0, 23) * 60;

        let n_products = 20_000;
        let product_ids = (0..n_products)
            .map(|i| 10_000 + ((i as i64 * 7_919) % 90_000))
            .collect();
        let terms = search_terms(&mut rng);
        let term_w = Weighted::zipf(terms.len(), 4);
        let people = human_agents();
        let mut agents = vec![PROBE_UA.to_string()];
        agents.extend(BOTS.iter().map(|b| b.1.to_string()));
        let mut app_agent = vec![false; agents.len()];
        app_agent.extend(people.iter().map(|p| p.2));
        let human_agents = Weighted::new(people.iter().map(|p| p.0));
        agents.extend(people.into_iter().map(|p| p.1));
        let cache_hosts = BASE_HOSTS[Svc::Cache.idx()] * scale;

        let keep = |routes: &[(u64, Route)]| -> Vec<(u64, Route)> {
            routes
                .iter()
                .copied()
                .filter(|(_, r)| on[Svc::Edge.idx()] || !matches!(r, Route::Static | Route::Scan))
                .collect()
        };
        let upstream_ip = ["storefront", "catalog", "cart", "orders", "profile", "recs"]
            .iter()
            .map(|n| {
                (
                    *n,
                    (10 << 24)
                        | (2 << 16)
                        | ((rng.below(8) as u32) << 8)
                        | (2 + rng.below(250) as u32),
                )
            })
            .collect();

        Farm {
            seed: opts.seed,
            on,
            dirty_ppm: opts.dirty_ppm as u64,
            day_ms: opts.day * DAY_MS,
            hosts,
            host_ip,
            node_ip,
            notifier_pid,
            gw_pods,
            rollout,
            incident,
            bursts,
            static_build,
            storefront_deploy,
            users: Weighted::zipf(60_000, 20),
            products: Weighted::zipf(n_products, 10),
            product_ids,
            terms,
            term_w,
            agents,
            app_agent,
            human_agents,
            bots: Weighted::new(BOTS.iter().map(|x| x.0)),
            human_routes: keep(HUMAN_ROUTES),
            crawler_routes: keep(CRAWLER_ROUTES),
            scanner_routes: keep(SCANNER_ROUTES),
            upstream_ip,
            images: Weighted::zipf(IMAGES.len(), 2),
            scans: Weighted::zipf(SCAN_PATHS.len(), 2),
            categories: Weighted::zipf(CATEGORIES.len(), 2),
            threads: Weighted::zipf(200, 8),
            jobs: Weighted::new(JOBS.iter().map(|j| j.weight)),
            partners: Weighted::zipf(PARTNERS.len(), 1),
            pg_errors: Weighted::new(PG_ERRORS.iter().map(|e| e.0)),
            key_rotation,
            sessions: VecDeque::new(),
            session_cap: 48 * scale,
            next_order: 4_512_000_000 + rng.between(0, 99_999),
            next_message: 1_690_000_000 + rng.between(0, 999_999),
            tenant_cursor: 0,
            cron_minute: -1,
            cron_cursor: 0,
            new_users: 0,
            cache_counters: vec![[0; 3]; cache_hosts],
            probe_rr: 0,
            stats_rr: 0,
            rollout_logged: 0,
            used: HashSet::new(),
            used_limit: USED_FLOOR,
            rng,
        }
    }

    fn is_on(&self, svc: Svc) -> bool {
        self.on[svc.idx()]
    }

    // ---- the day -------------------------------------------------------------------

    fn run<W: Write>(&mut self, out: &mut W, lines: u64) -> Result<(), GenError> {
        let sources: Vec<(Src, u64)> = SOURCES
            .iter()
            .copied()
            .filter(|(s, _)| self.source_on(*s))
            .collect();
        let share_total: u64 = sources.iter().map(|x| x.1).sum();

        // Each source's lines, then each source's lines per minute, both by cumulative
        // rounding so every total is exact.
        let mut quotas: Vec<Vec<u64>> = Vec::new();
        let mut acc = 0u64;
        for (src, share) in &sources {
            let before = lines as u128 * acc as u128 / share_total as u128;
            acc += share;
            let n = (lines as u128 * acc as u128 / share_total as u128 - before) as u64;
            let weights: Vec<u64> = (0..MINUTES).map(|m| self.weight(*src, m)).collect();
            let total: u128 = weights.iter().map(|&w| w as u128).sum();
            let mut run = 0u128;
            let mut prev = 0u64;
            quotas.push(
                weights
                    .iter()
                    .map(|&w| {
                        run += w as u128;
                        let upto = (n as u128 * run / total) as u64;
                        let q = upto - prev;
                        prev = upto;
                        q
                    })
                    .collect(),
            );
        }

        let mut heap: BinaryHeap<Pending> = BinaryHeap::new();
        let mut over = vec![0u64; sources.len()];
        let mut placed = 0u64;
        let mut seq = 0u64;
        let mut text = String::new();

        for m in 0..MINUTES {
            let mi = self.minute(m);
            // Plan the minute: whole chains, until each source has its lines. A chain
            // that overshoots is paid back out of the source's next minute.
            let mut chains: Vec<(usize, i64, Vec<Line>)> = Vec::new();
            for (k, (src, _)) in sources.iter().enumerate() {
                let need = quotas[k][m as usize];
                if over[k] >= need {
                    over[k] -= need;
                    continue;
                }
                let need = need - over[k];
                let mut got = 0u64;
                while got < need {
                    let chain = self.plan(*src, &mi);
                    got += chain.len() as u64;
                    chains.push((k, 0, chain));
                }
                over[k] = got - need;
            }
            // Then give each chain its start, and hand the lines out in time order. Cron fires on
            // the minute, probes and stats on a steady period; everything else arrives at random.
            let mut per_source = vec![0i64; sources.len()];
            for (k, _, _) in &chains {
                per_source[*k] += 1;
            }
            let mut nth = vec![0i64; sources.len()];
            for (k, start, chain) in chains.iter_mut() {
                let span = chain.iter().map(|l| l.dt).max().unwrap_or(0);
                let base = m * MINUTE_MS;
                let room = (DAY_MS - 1 - span - base).clamp(1, MINUTE_MS);
                let off = match sources[*k].0 {
                    Src::Cron => self.rng.below(40.min(room as u64)) as i64,
                    Src::Probe | Src::Stats => {
                        let even = (2 * nth[*k] + 1) * MINUTE_MS / (2 * per_source[*k]);
                        nth[*k] += 1;
                        (even + self.rng.below(20) as i64).min(room - 1)
                    }
                    _ => self.rng.below(room as u64) as i64,
                };
                *start = base + off;
                // A chain too long for what is left of the day is squeezed, in order,
                // rather than stamped on the next one.
                let left = DAY_MS - 1 - *start;
                if span > left {
                    for l in chain.iter_mut() {
                        l.dt = l.dt * left / span;
                    }
                }
            }
            chains.sort_by_key(|(_, start, _)| *start);

            for (_, start, chain) in chains {
                while heap.peek().is_some_and(|p| p.t <= start) {
                    let p = heap.pop().expect("peeked");
                    self.emit(out, &p, &mut text)?;
                }
                // Nothing placed from here on can be earlier than `start`.
                if self.used.len() > self.used_limit {
                    self.used.retain(|slot| slot.2 >= start);
                    self.used_limit = (2 * self.used.len()).max(USED_FLOOR);
                }
                for line in chain {
                    if placed == lines {
                        break;
                    }
                    let t = self.place(&line, start);
                    heap.push(Pending { t, seq, line });
                    seq += 1;
                    placed += 1;
                }
            }
        }
        while let Some(p) = heap.pop() {
            self.emit(out, &p, &mut text)?;
        }
        Ok(())
    }

    /// The millisecond this line is stamped with, moved on past any its host already has: two
    /// lines of one host differ in time, two hosts in name, so every line is distinct.
    fn place(&mut self, line: &Line, start: i64) -> i64 {
        let svc = line.svc.idx() as u8;
        let mut t = start + line.dt;
        while self.used.contains(&(svc, line.host, t)) {
            t += 1;
        }
        if t > DAY_MS - 1 {
            // Past midnight only in a pathological last millisecond; look back instead,
            // never before the chain's start, so the heap's order still holds.
            t = start + line.dt;
            while self.used.contains(&(svc, line.host, t)) && t > start {
                t -= 1;
            }
        }
        self.used.insert((svc, line.host, t));
        t
    }

    fn emit<W: Write>(&self, out: &mut W, p: &Pending, text: &mut String) -> Result<(), GenError> {
        let mut obj = Map::new();
        for (key, value) in &p.line.fields {
            let v = match value {
                V::S(s) => Value::String(s.clone()),
                V::K(s) => Value::String((*s).to_string()),
                V::I(i) => Value::from(*i),
                V::F(f) => Value::from(*f),
                V::B(b) => Value::Bool(*b),
                V::Arr(items) => Value::Array(
                    items
                        .iter()
                        .map(|s| Value::String((*s).to_string()))
                        .collect(),
                ),
                V::T(form, offset, micros) => {
                    time_value(*form, self.day_ms + p.t + offset, *micros)
                }
                V::At(form, ms) => time_value(*form, self.day_ms + ms, 0),
            };
            insert(&mut obj, key, v);
        }
        text.clear();
        text.push_str(&serde_json::to_string(&Value::Object(obj))?);
        text.push('\n');
        out.write_all(text.as_bytes()).map_err(GenError::Write)
    }

    fn source_on(&self, src: Src) -> bool {
        match src {
            Src::Web | Src::Probe => true,
            Src::Jobs => self.is_on(Svc::Worker),
            Src::Cron => self.is_on(Svc::Scheduler),
            Src::Stats => [Svc::DbProxy, Svc::Cache, Svc::Worker]
                .iter()
                .any(|s| self.is_on(*s)),
            Src::Storm => self.is_on(Svc::DbProxy),
        }
    }

    fn diurnal(m: i64) -> i64 {
        // Linear between hour midpoints, in sixtieths.
        let pos = (m - 30).rem_euclid(MINUTES);
        let h = (pos / 60) as usize;
        let frac = pos % 60;
        HOURLY[h] * (60 - frac) + HOURLY[(h + 1) % 24] * frac
    }

    fn weight(&self, src: Src, m: i64) -> u64 {
        let d = Self::diurnal(m);
        let w = match src {
            Src::Web => {
                let mut w = d * 100;
                for &(start, len, peak) in &self.bursts {
                    if m >= start && m < start + len {
                        // Up at once, then a decay back over the burst.
                        w = w * (100 + (peak - 100) * (start + len - m) / len) / 100;
                    }
                }
                if m >= self.incident.0 && m < self.incident.1 {
                    // Clients retry: the incident is louder, not just angrier.
                    w = w * 140 / 100;
                }
                w
            }
            Src::Jobs => (3000 + d) * 50,
            // Five lines for every run the schedule has due this minute, plus the rollups
            // that fill the rest: the scheduler's volume follows its crontab.
            Src::Cron => 5 * cron_due(m).len() as i64 + 4,
            Src::Probe | Src::Stats => 6000 * 100,
            Src::Storm => u64::from(m >= self.incident.0 && m < self.incident.1) as i64,
        };
        w as u64
    }

    fn minute(&self, m: i64) -> Minute {
        let since = m - self.rollout;
        Minute {
            m,
            incident: m >= self.incident.0 && m < self.incident.1,
            gw_new_pct: since.clamp(0, 10) as u64 * 10,
        }
    }

    fn plan(&mut self, src: Src, mi: &Minute) -> Vec<Line> {
        match src {
            Src::Web => self.plan_web(mi),
            Src::Probe => self.plan_probe(mi),
            Src::Jobs => self.plan_job(),
            Src::Cron => self.plan_cron(mi),
            Src::Stats => self.plan_stats(mi),
            Src::Storm => self.plan_storm(),
        }
    }

    // ---- hosts and faults ------------------------------------------------------------

    fn host(&mut self, svc: Svc, mi: &Minute) -> u16 {
        if svc == Svc::Gateway {
            let gen = u64::from(self.rng.below(100) < mi.gw_new_pct) as usize;
            return (gen * self.gw_pods + self.rng.below(self.gw_pods as u64) as usize) as u16;
        }
        self.rng.below(self.hosts[svc.idx()].len() as u64) as u16
    }

    fn host_name(&self, svc: Svc, host: u16) -> String {
        self.hosts[svc.idx()][host as usize].clone()
    }

    /// A numeric field that sometimes arrives as `""` or `"N/A"`. The draws happen whether or
    /// not the fault does, so `--dirty-rate` changes those values and nothing else.
    fn dirty_num(&mut self, clean: V) -> V {
        let hit = self.rng.below(1_000_000) < self.dirty_ppm;
        let empty = self.rng.chance(50);
        if hit {
            V::K(if empty { "" } else { "N/A" })
        } else {
            clean
        }
    }

    /// A string field that sometimes arrives as a number.
    fn dirty_str(&mut self, clean: V) -> V {
        let hit = self.rng.below(1_000_000) < self.dirty_ppm;
        let n = self.rng.between(1, 12);
        if hit {
            V::I(n)
        } else {
            clean
        }
    }

    // ---- sessions --------------------------------------------------------------------

    fn session(&mut self) -> usize {
        if !self.sessions.is_empty() && self.rng.chance(82) {
            return self.rng.below(self.sessions.len() as u64) as usize;
        }
        let s = if self.rng.chance(6) {
            let i = self.bots.draw(&mut self.rng);
            let b = BOTS[i];
            let h = self.rng.next_u64();
            Session {
                uid: 0,
                ip: ip_in(b.2 .0, b.2 .1, h),
                ua: 1 + i,
                bot: Some(b.3),
                app: false,
                logged_in: false,
                wants_login: false,
                warm: false,
                page: None,
                left: self.rng.between(3, 40) as u16,
            }
        } else {
            let u = self.users.draw(&mut self.rng) as u64;
            let uid = 100_000 + ((u * 2_654_435_761) % 900_000) as i64;
            let h = mix(self.seed, uid as u64);
            // Home broadband mostly; a phone on a carrier network now and then, whose
            // addresses a great many users share.
            let ip = if self.rng.chance(20) {
                let (a, b) = MOBILE[(h % MOBILE.len() as u64) as usize];
                ip_in(a, b, self.rng.below(4096))
            } else {
                let (a, b) = ISP[(h >> 8) as usize % ISP.len()];
                ip_in(a, b, h >> 16)
            };
            // The same device all day: drawn from the user, not from the stream.
            let pick = mix(h, 1) % *self.human_agents.cum.last().expect("agents");
            let ua = 1 + BOTS.len() + self.human_agents.cum.partition_point(|&c| c <= pick);
            Session {
                uid,
                ip,
                ua,
                bot: None,
                app: self.app_agent[ua],
                logged_in: self.rng.chance(55),
                wants_login: false,
                warm: self.rng.chance(40),
                page: None,
                left: 1 + self.rng.between(0, 12) as u16,
            }
        };
        self.sessions.push_back(s);
        if self.sessions.len() > self.session_cap {
            self.sessions.pop_front();
        }
        self.sessions.len() - 1
    }

    fn user_facts(&self, uid: i64) -> (&'static str, &'static str, &'static str, i64) {
        let h = mix(self.seed ^ 0xC0FFEE, uid as u64);
        let total: u64 = COUNTRY.iter().map(|c| c.0).sum();
        let mut r = h % total;
        let mut place = COUNTRY[0].1;
        for (w, c) in COUNTRY {
            if r < *w {
                place = *c;
                break;
            }
            r -= w;
        }
        let total: u64 = CARDS.iter().map(|c| c.0).sum();
        let mut r = (h >> 20) % total;
        let mut card = CARDS[0].1;
        for (w, c) in CARDS {
            if r < *w {
                card = c;
                break;
            }
            r -= w;
        }
        (place.0, place.1, card, 1000 + ((h >> 40) % 9000) as i64)
    }

    // ---- web requests ----------------------------------------------------------------

    fn plan_web(&mut self, mi: &Minute) -> Vec<Line> {
        let si = self.session();
        let mut sess = self.sessions[si].clone();
        let mut route = match sess.bot {
            Some(BotKind::Crawler) => pick_w(&mut self.rng, &self.crawler_routes),
            Some(BotKind::Scanner) => pick_w(&mut self.rng, &self.scanner_routes),
            None if sess.wants_login => Route::Login,
            None => pick_w(&mut self.rng, &self.human_routes),
        };
        // The app has no HTML and no static bundle: it asks the API for what a page shows.
        if sess.app && matches!(route, Route::Page | Route::Static) {
            route = Route::Recs;
        }
        let req_id = hex(&mut self.rng, 32);
        let sp = spec(route);
        let uid = (sess.logged_in && sess.bot.is_none()).then_some(sess.uid);
        let referer = self.referer(&sess);
        let mut lines = Vec::new();
        // What happens after the response: jobs, receipts, a page's assets. Kept apart so
        // the gateway's latency is measured over what it actually waited for.
        let mut later = Vec::new();

        // Answered by nginx itself: nothing behind the edge hears of it.
        if matches!(route, Route::Static | Route::Scan) {
            let (uri, status) = if route == Route::Static {
                let uri = match self.rng.below(10) {
                    _ if sess.bot.is_some() => "/robots.txt",
                    0..=5 => "/favicon.ico",
                    6..=7 => "/manifest.webmanifest",
                    _ => "/sw.js",
                };
                (uri.to_string(), if self.rng.chance(35) { 304 } else { 200 })
            } else {
                let path = SCAN_PATHS[self.scans.draw(&mut self.rng)];
                (
                    path.to_string(),
                    if self.rng.chance(15) { 403 } else { 404 },
                )
            };
            let bytes = self.body_bytes(&uri, status, sp.bytes);
            let ms = latency(&mut self.rng, sp.lat.0, sp.lat.1) / 10 + 1;
            lines.extend(self.edge(
                ms, None, &sess, &req_id, "GET", uri, status, bytes, None, referer,
            ));
            self.finish_session(si, sess);
            return lines;
        }

        let (path, route_label, query) = self.path_for(route);
        let gw_host = self.host(Svc::Gateway, mi);
        let gw_ip = self.host_ip[Svc::Gateway.idx()][gw_host as usize];

        // Refused at the door: no token, or too many requests.
        let limited = self.rng.below(1000) < if sess.bot.is_some() { 40 } else { 6 };
        if (sp.login && uid.is_none()) || limited {
            let (event, status, err) = if limited {
                (
                    "rate_limited",
                    429,
                    ("rate_limit", "rate limit exceeded for client"),
                )
            } else {
                sess.wants_login = sess.bot.is_none();
                (
                    "auth_rejected",
                    401,
                    ("unauthorized", "missing or invalid bearer token"),
                )
            };
            let dur = self.rng.between(2, 15);
            let total = dur / 10 + 1;
            let bytes = self.body_bytes(&path, status, sp.bytes);
            let gw = Gw {
                event,
                level: "warn",
                status,
                method: sp.method,
                path: path.clone(),
                route: route_label,
                upstream: None,
                dur,
                bytes,
                err: Some(err),
                stack: None,
            };
            lines.push(self.gw_line(total, gw_host, &sess, &req_id, uid, gw));
            let up = Some((vec![gw_ip], vec![total]));
            lines.extend(self.edge(
                total + 1,
                None,
                &sess,
                &req_id,
                sp.method,
                path,
                status,
                bytes,
                up,
                referer,
            ));
            self.finish_session(si, sess);
            return lines;
        }

        // Behind the gateway. Everything it calls is logged before it logs the request.
        let mut lat = latency(&mut self.rng, sp.lat.0, sp.lat.1);
        let inner = (lat / 10).max(3);
        let mut status = match route {
            Route::CartAdd | Route::Signup => 201,
            Route::Logout => 204,
            _ => 200,
        };
        let mut timed_out = false;
        let mut edge_tries = 1i64;
        let upstream_ip = self.upstream_addr(sp.upstream);

        // What moves money checks the token with auth first, so a checkout's id is in the
        // auth line as well as in the payment one.
        if matches!(route, Route::Checkout | Route::Refund) && self.is_on(Svc::Auth) {
            let kid = self.kid(mi);
            let mut l = self.auth_line(
                1,
                "token_verified",
                "debug",
                "token verified",
                &req_id,
                Some(sess.uid),
                &sess,
            );
            l.put("token.kid", V::K(kid));
            lines.push(l);
        }

        match route {
            Route::Page if self.rng.chance(1) => status = 404,
            Route::Product => {
                let pid = self.product();
                if !self.rng.chance(85) {
                    self.cache_miss(&mut lines, 1, "GET", "product", pid, upstream_ip);
                    self.db(
                        &mut lines,
                        2,
                        "catalog_ro",
                        SQL_PRODUCT,
                        Some(1),
                        Some(&req_id),
                        upstream_ip,
                        false,
                    );
                }
                if self.rng.chance(2) {
                    status = 404;
                }
            }
            Route::Search => {
                let (term, page) = query.expect("a search carries its query");
                self.search(&mut lines, 2, &req_id, uid, term, page);
            }
            Route::Cart => {
                if let Some(u) = uid.filter(|_| self.rng.chance(12)) {
                    self.cache_miss(&mut lines, 1, "HGETALL", "cart", u, upstream_ip);
                    let fail = mi.incident && self.rng.chance(15);
                    let rows = self.rng.between(0, 9);
                    self.db(
                        &mut lines,
                        2,
                        "cart_rw",
                        SQL_CART_READ,
                        Some(rows),
                        Some(&req_id),
                        upstream_ip,
                        fail,
                    );
                    if fail {
                        timed_out = true;
                        edge_tries = 2;
                    }
                }
            }
            Route::CartAdd => {
                // The pool is pgbouncer's, not payments': every writer waits on it.
                let fail = mi.incident && self.rng.chance(15);
                self.db(
                    &mut lines,
                    2,
                    "cart_rw",
                    SQL_CART_ADD,
                    None,
                    Some(&req_id),
                    upstream_ip,
                    fail,
                );
                timed_out = fail;
            }
            Route::Login => {
                if !self.login(&mut lines, &mut sess, &req_id, upstream_ip) {
                    status = 401;
                }
            }
            Route::Refresh => {
                if !self.refresh(&mut lines, inner, &sess, &req_id, mi) {
                    status = 401;
                    sess.logged_in = false;
                }
            }
            Route::Checkout => {
                let (s, failed) =
                    self.checkout(&mut lines, &mut later, &sess, &req_id, upstream_ip, mi);
                status = s;
                timed_out = failed;
            }
            Route::Order => {
                let fail = mi.incident && self.rng.chance(20);
                self.db(
                    &mut lines,
                    2,
                    "orders_ro",
                    SQL_ORDER_READ,
                    Some(1),
                    Some(&req_id),
                    upstream_ip,
                    fail,
                );
                if fail {
                    // A GET is safe to repeat, so the edge tries a second gateway pod.
                    timed_out = true;
                    edge_tries = 2;
                }
            }
            Route::Me => {
                if let Some(u) = uid.filter(|_| self.rng.chance(20)) {
                    self.cache_miss(&mut lines, 1, "GET", "user", u, upstream_ip);
                    self.db(
                        &mut lines,
                        2,
                        "profile_ro",
                        SQL_PROFILE,
                        Some(1),
                        Some(&req_id),
                        upstream_ip,
                        false,
                    );
                }
            }
            Route::Logout => {
                if self.is_on(Svc::Auth) {
                    let l = self.auth_line(
                        inner,
                        "logout",
                        "info",
                        "session ended",
                        &req_id,
                        Some(sess.uid),
                        &sess,
                    );
                    lines.push(l);
                }
                sess.logged_in = false;
                sess.left = 1;
            }
            Route::Signup => self.signup(&mut lines, &mut later, &mut sess, &req_id, upstream_ip),
            Route::Refund => self.refund(&mut lines, &mut later, &sess, &req_id, upstream_ip),
            _ => {}
        }

        // The gateway took at least as long as the slowest thing it waited for.
        let waited = lines.iter().map(|l| l.dt).max().unwrap_or(0);
        if lat / 10 <= waited {
            lat = (waited + self.rng.between(1, 4)) * 10 + self.rng.below(10) as i64;
        }

        // A baseline of failures all day, not only in the incident. Each is a rare template of
        // its own that no overlap rule would fold into the common one.
        let fault = self.rng.below(100_000);
        let (event, level, err, stack) = if timed_out {
            lat = GATEWAY_TIMEOUT_MS * 10 + self.rng.between(5, 90);
            status = 504;
            (
                "upstream_timeout",
                "warn",
                Some(("timeout", "upstream request timeout")),
                None,
            )
        } else if fault < 60 {
            status = 502;
            let e = ("upstream_reset", "upstream connect error or disconnect/reset before headers. reset reason: connection termination");
            ("upstream_error", "error", Some(e), None)
        } else if fault < 160 {
            status = 499;
            (
                "client_closed",
                "info",
                Some(("downstream_reset", "downstream remote disconnect")),
                None,
            )
        } else if fault < 163 {
            status = 500;
            let e = (
                "panic",
                "runtime error: invalid memory address or nil pointer dereference",
            );
            ("panic_recovered", "error", Some(e), Some(GO_PANIC))
        } else if status >= 500 {
            (
                "upstream_error",
                "error",
                Some(("upstream", "upstream returned an error")),
                None,
            )
        } else if status >= 400 {
            ("request", "warn", None, None)
        } else {
            ("request", "info", None, None)
        };
        let total = lat / 10 + 1;
        let bytes = self.body_bytes(&path, status, sp.bytes);
        let spent = if timed_out {
            lat
        } else {
            lat - self.rng.between(1, 8).min(lat)
        };
        let gw = Gw {
            event,
            level,
            status,
            method: sp.method,
            path: path.clone(),
            route: route_label,
            upstream: Some((sp.upstream, upstream_ip, spent)),
            dur: lat,
            bytes,
            err,
            stack,
        };
        lines.push(self.gw_line(total, gw_host, &sess, &req_id, uid, gw));
        let mut addrs = vec![gw_ip];
        let mut times = vec![total];
        if edge_tries == 2 {
            // The retry lands on another pod, which times out the same way.
            let second = self.host(Svc::Gateway, mi);
            let gw = Gw {
                event,
                level,
                status,
                method: sp.method,
                path: path.clone(),
                route: route_label,
                upstream: Some((sp.upstream, upstream_ip, lat)),
                dur: lat,
                bytes,
                err,
                stack: None,
            };
            lines.push(self.gw_line(total * 2, second, &sess, &req_id, uid, gw));
            addrs.push(self.host_ip[Svc::Gateway.idx()][second as usize]);
            times.push(total);
        }
        let edge_total = total * edge_tries + 1;
        let page = path.clone();
        lines.extend(self.edge(
            edge_total,
            None,
            &sess,
            &req_id,
            sp.method,
            path,
            status,
            bytes,
            Some((addrs, times)),
            referer,
        ));
        if route == Route::Page && status == 200 && sess.bot.is_none() {
            self.page_assets(&mut lines, edge_total, &sess, &page, mi);
            sess.page = Some(page);
            sess.warm = true;
        }
        lines.append(&mut later);
        self.finish_session(si, sess);
        lines
    }

    /// Where the browser says it came from: the page it is on, or for its first page a
    /// search engine or a social app now and then. Bots and the app send none.
    fn referer(&mut self, sess: &Session) -> V {
        if sess.bot.is_some() || sess.app {
            return V::K("-");
        }
        match &sess.page {
            Some(p) => V::S(format!("https://shop.example.com{p}")),
            None if self.rng.chance(45) => V::K(pick_w(&mut self.rng, EXTERNAL_REFERERS)),
            None => V::K("-"),
        }
    }

    /// The size of a response, from what was asked for: a static file is the same size every
    /// time, an error page one size per status, and a 204 or 304 nothing at all.
    fn body_bytes(&mut self, uri: &str, status: i64, range: (i64, i64)) -> i64 {
        if status == 204 || status == 304 || status == 499 {
            return 0;
        }
        if status >= 400 {
            return 60 + (mix(self.seed, status as u64) % 140) as i64;
        }
        let h = mix(self.seed, fnv(uri));
        if uri.starts_with("/static/")
            || uri.starts_with("/favicon")
            || uri.starts_with("/sw.js")
            || uri.starts_with("/manifest")
            || uri.starts_with("/robots")
        {
            let (lo, hi) = if uri.ends_with(".js") {
                (30_000, 420_000)
            } else if uri.ends_with(".css") {
                (8_000, 90_000)
            } else if uri.ends_with(".woff2") {
                (40_000, 110_000)
            } else if uri.ends_with(".webp") {
                (4_000, 180_000)
            } else {
                (300, 16_000)
            };
            return lo + (h % (hi - lo) as u64) as i64;
        }
        let base = range.0 + (h % (range.1 - range.0 + 1) as u64) as i64;
        base + base * self.rng.between(-30, 30) / 1000
    }

    /// What a browser fetches after an HTML page: the bundle if it does not hold it, then the
    /// images - the runs of equal address, agent and referer an access log is made of.
    fn page_assets(
        &mut self,
        lines: &mut Vec<Line>,
        after: i64,
        sess: &Session,
        page: &str,
        mi: &Minute,
    ) {
        if !self.is_on(Svc::Edge) {
            return;
        }
        let build = self.static_build[usize::from(mi.m >= self.storefront_deploy)].clone();
        let mut assets: Vec<String> = Vec::new();
        if !sess.warm {
            for name in ["runtime", "vendor", "app"] {
                assets.push(format!("/static/js/{name}.{build}.js"));
            }
            assets.push(format!("/static/css/main.{build}.css"));
            assets.push("/static/fonts/inter-var.woff2".to_string());
            assets.push("/static/img/logo.svg".to_string());
        }
        if let Some(pid) = page
            .strip_prefix("/product/")
            .and_then(|p| p.rsplit('-').next())
        {
            for w in [800, 400, 200].iter().take(1 + self.rng.below(3) as usize) {
                assets.push(format!("/static/img/p/{pid}-{w}.webp"));
            }
        } else if page.starts_with("/category/") {
            for _ in 0..self.rng.between(6, 16) {
                let pid = self.product();
                assets.push(format!("/static/img/p/{pid}-200.webp"));
            }
        } else {
            for _ in 0..self.rng.between(2, 4) {
                assets.push(format!(
                    "/static/img/{}.webp",
                    IMAGES[self.images.draw(&mut self.rng)]
                ));
            }
        }
        let referer = format!("https://shop.example.com{page}");
        let mut t = after;
        for uri in assets {
            t += self.rng.between(1, 30);
            // A warm browser mostly revalidates what it has; a cold one downloads it.
            let status = if sess.warm && self.rng.chance(60) {
                304
            } else {
                200
            };
            let bytes = self.body_bytes(&uri, status, (0, 0));
            let ms = latency(&mut self.rng, 1, 12) / 10 + 1;
            let req_id = hex(&mut self.rng, 32);
            lines.extend(self.edge(
                t + ms,
                None,
                sess,
                &req_id,
                "GET",
                uri,
                status,
                bytes,
                None,
                V::S(referer.clone()),
            ));
        }
    }

    /// The signing key a token carries: rotated once a day, by the cron job of that name.
    fn kid(&self, mi: &Minute) -> &'static str {
        if mi.m >= self.key_rotation {
            "k-2024-02"
        } else {
            "k-2024-01"
        }
    }

    fn finish_session(&mut self, si: usize, mut sess: Session) {
        sess.left = sess.left.saturating_sub(1);
        if sess.left == 0 {
            self.sessions.remove(si);
        } else {
            self.sessions[si] = sess;
        }
    }

    fn product(&mut self) -> i64 {
        self.product_ids[self.products.draw(&mut self.rng)]
    }

    /// The path as requested and the route it matched; for a search, the term and page too.
    fn path_for(&mut self, route: Route) -> (String, &'static str, Option<(usize, i64)>) {
        let (path, label) = match route {
            Route::Page => match self.rng.below(10) {
                0..=3 => ("/".to_string(), "/"),
                4..=7 => {
                    let pid = self.product();
                    let h = mix(self.seed, pid as u64);
                    let slug = format!(
                        "{}-{}",
                        ADJ[(h % 16) as usize],
                        NOUN[((h >> 8) % 16) as usize]
                    );
                    (format!("/product/{slug}-{pid}"), "/product/{slug}")
                }
                _ => {
                    let c = CATEGORIES[self.categories.draw(&mut self.rng)];
                    (format!("/category/{c}"), "/category/{name}")
                }
            },
            Route::Product => (
                format!("/api/v1/products/{}", self.product()),
                "/api/v1/products/{id}",
            ),
            Route::Search => {
                let term = self.term_w.draw(&mut self.rng);
                let page = if self.rng.chance(80) {
                    1
                } else {
                    self.rng.between(2, 5)
                };
                let q = self.terms[term].replace(' ', "+");
                return (
                    format!("/api/v1/search?q={q}&page={page}"),
                    "/api/v1/search",
                    Some((term, page)),
                );
            }
            Route::Order => {
                let oid = self.next_order - self.rng.between(0, 5000);
                (format!("/api/v1/orders/{oid}"), "/api/v1/orders/{id}")
            }
            Route::Refund => {
                let oid = self.next_order - self.rng.between(100, 50_000);
                (
                    format!("/api/v1/orders/{oid}/refund"),
                    "/api/v1/orders/{id}/refund",
                )
            }
            Route::Recs => {
                let slot = REC_SLOTS[self.rng.below(REC_SLOTS.len() as u64) as usize];
                (
                    format!("/api/v1/recommendations?slot={slot}"),
                    "/api/v1/recommendations",
                )
            }
            _ => {
                let r = spec(route).route;
                (r.to_string(), r)
            }
        };
        (path, label, None)
    }

    /// Where the gateway sends a route: a pod, when the service is one of ours.
    fn upstream_addr(&mut self, upstream: &'static str) -> u32 {
        let svc = match upstream {
            "auth" => Svc::Auth,
            "payments" => Svc::Payments,
            "search" => Svc::Search,
            _ => {
                return self
                    .upstream_ip
                    .iter()
                    .find(|(n, _)| *n == upstream)
                    .map_or(0x0A02_0002, |x| x.1)
            }
        };
        let h = self.rng.below(self.host_ip[svc.idx()].len() as u64) as usize;
        self.host_ip[svc.idx()][h]
    }

    // ---- the gateway -----------------------------------------------------------------

    /// Go, with an ECS encoder: `log.level` and `ecs.version` are flat names with a dot in
    /// them. The rollout's new pods also log the TLS version: a new template per call site.
    fn gw_line(
        &mut self,
        dt: i64,
        host: u16,
        sess: &Session,
        req_id: &str,
        uid: Option<i64>,
        g: Gw,
    ) -> Line {
        let new_pod = host as usize >= self.gw_pods;
        let tls = if self.rng.chance(85) { "1.3" } else { "1.2" };
        let ns = |rng: &mut Rng, tenths: i64| {
            V::F((tenths * 100_000 + rng.below(100_000) as i64) as f64 / 1e6)
        };
        let mut l = Line::new(Svc::Gateway, host, dt);
        l.put("@timestamp", V::T(TimeForm::IsoMillisZ, 0, 0))
            .put("log\\.level", V::K(g.level))
            .put(
                "message",
                V::S(format!("{} {} {}", g.method, g.route, g.status)),
            )
            .put("ecs\\.version", V::K("1.6.0"))
            .put("event", V::K(g.event))
            .put("service.name", V::K("gateway"))
            .put(
                "service.version",
                V::K(if new_pod { "2.15.0" } else { "2.14.3" }),
            )
            .put("host.name", V::S(self.host_name(Svc::Gateway, host)))
            .put("http.request.id", V::S(req_id.to_string()))
            .put("http.request.method", V::K(g.method))
            .put("http.response.status_code", V::I(g.status))
            .put("http.response.bytes", V::I(g.bytes))
            .put("url.path", V::S(g.path))
            .put("url.route", V::K(g.route))
            .put("client.ip", V::S(ipv4(sess.ip)))
            .put("user_agent.original", V::S(self.agents[sess.ua].clone()));
        if new_pod && sess.ua != PROBE {
            l.put("tls.version", V::K(tls));
        }
        if let Some(u) = uid {
            l.put("user.id", V::I(u));
        }
        if let Some((name, addr, spent)) = g.upstream {
            let spent = ns(&mut self.rng, spent);
            l.put("upstream.name", V::K(name))
                .put("upstream.address", V::S(format!("{}:8080", ipv4(addr))))
                .put("upstream.duration_ms", spent);
        }
        let dur = ns(&mut self.rng, g.dur);
        l.put("duration_ms", dur);
        if let Some((kind, message)) = g.err {
            l.put("error.type", V::K(kind))
                .put("error.message", V::K(message));
        }
        if let Some(trace) = g.stack {
            l.put("error.stack_trace", V::K(trace));
        }
        l
    }

    // ---- auth ------------------------------------------------------------------------

    /// zap's production encoder: level, ts, logger, caller, msg, then the fields.
    #[allow(clippy::too_many_arguments)]
    fn auth_line(
        &mut self,
        dt: i64,
        event: &'static str,
        level: &'static str,
        msg: &'static str,
        req_id: &str,
        uid: Option<i64>,
        sess: &Session,
    ) -> Line {
        let host = self.rng.below(self.hosts[Svc::Auth.idx()].len() as u64) as u16;
        let (logger, caller) = match event {
            "login_ok" | "login_failed" | "mfa_challenge" | "account_locked" => {
                ("auth.login", "login/handler.go:142")
            }
            "token_refresh" | "token_invalid" => ("auth.token", "token/refresh.go:88"),
            "token_verified" => ("auth.token", "token/verify.go:41"),
            "logout" => ("auth.session", "session/logout.go:31"),
            "user_created" => ("auth.signup", "signup/create.go:57"),
            _ => ("auth.keys", "keys/jwks.go:203"),
        };
        let mut l = Line::new(Svc::Auth, host, dt);
        l.put("level", V::K(level))
            .put("ts", V::T(TimeForm::IsoMillisZ, 0, 0))
            .put("logger", V::K(logger))
            .put("caller", V::K(caller))
            .put("msg", V::K(msg))
            .put("hostname", V::S(self.host_name(Svc::Auth, host)))
            .put("event", V::K(event));
        if !req_id.is_empty() {
            l.put("request_id", V::S(req_id.to_string()));
        }
        if let Some(u) = uid {
            let v = self.dirty_num(V::I(u));
            l.put("user_id", v);
        }
        if sess.ip != 0 {
            l.put("ip", V::S(ipv4(sess.ip)));
        }
        l
    }

    fn login(
        &mut self,
        lines: &mut Vec<Line>,
        sess: &mut Session,
        req_id: &str,
        auth_ip: u32,
    ) -> bool {
        let lookup = 1 + self.rng.below(3) as i64;
        let found = self.db(
            lines,
            lookup,
            "auth_rw",
            SQL_USER_BY_EMAIL,
            Some(1),
            Some(req_id),
            auth_ip,
            false,
        );
        // The password hash is the slow part.
        let at = found + self.rng.between(120, 400);
        let roll = self.rng.below(100);
        let ok = roll < 89;
        if self.is_on(Svc::Auth) {
            if roll < 86 {
                let method = pick_w(
                    &mut self.rng,
                    &[(85, "password"), (10, "google"), (5, "apple")],
                );
                let mfa = self.rng.chance(20);
                let session_id = uuid(&mut self.rng);
                let mut l = self.auth_line(
                    at,
                    "login_ok",
                    "info",
                    "login succeeded",
                    req_id,
                    Some(sess.uid),
                    sess,
                );
                l.put("method", V::K(method))
                    .put("mfa", V::B(mfa))
                    .put("session_id", V::S(session_id));
                lines.push(l);
            } else if roll < 89 {
                let mfa = pick_w(&mut self.rng, &[(70, "totp"), (30, "sms")]);
                let mut l = self.auth_line(
                    at,
                    "mfa_challenge",
                    "info",
                    "mfa challenge issued",
                    req_id,
                    Some(sess.uid),
                    sess,
                );
                l.put("mfa.method", V::K(mfa));
                lines.push(l);
            } else {
                let reason = pick_w(&mut self.rng, LOGIN_FAIL);
                let attempt = self.rng.between(1, 5);
                let who = (reason != "unknown_user").then_some(sess.uid);
                let mut l = self.auth_line(
                    at,
                    "login_failed",
                    "warn",
                    "login failed",
                    req_id,
                    who,
                    sess,
                );
                l.put("reason", V::K(reason)).put("attempt", V::I(attempt));
                lines.push(l);
                if attempt == 5 && reason == "invalid_password" {
                    let mut l = self.auth_line(
                        at + 1,
                        "account_locked",
                        "warn",
                        "account locked after repeated failures",
                        req_id,
                        Some(sess.uid),
                        sess,
                    );
                    l.put("failed_attempts", V::I(5))
                        .put("locked_for_s", V::I(900));
                    lines.push(l);
                }
            }
        }
        if ok {
            self.db(
                lines,
                at + 1,
                "auth_rw",
                SQL_LAST_LOGIN,
                None,
                Some(req_id),
                auth_ip,
                false,
            );
            sess.logged_in = true;
            sess.wants_login = false;
        } else {
            sess.wants_login = self.rng.chance(60);
        }
        ok
    }

    fn refresh(
        &mut self,
        lines: &mut Vec<Line>,
        inner: i64,
        sess: &Session,
        req_id: &str,
        mi: &Minute,
    ) -> bool {
        let ok = !self.rng.chance(5);
        if !self.is_on(Svc::Auth) {
            return ok;
        }
        if ok {
            let kid = self.kid(mi);
            let mut l = self.auth_line(
                inner,
                "token_refresh",
                "info",
                "token refreshed",
                req_id,
                Some(sess.uid),
                sess,
            );
            l.put("token.kid", V::K(kid))
                .put("token.exp", V::T(TimeForm::EpochSeconds, 3_600_000, 0));
            lines.push(l);
        } else {
            let reason = pick_w(
                &mut self.rng,
                &[(80, "expired"), (15, "bad_signature"), (5, "revoked")],
            );
            let mut l = self.auth_line(
                inner,
                "token_invalid",
                "warn",
                "refresh token rejected",
                req_id,
                None,
                sess,
            );
            l.put("reason", V::K(reason));
            lines.push(l);
        }
        ok
    }

    fn signup(
        &mut self,
        lines: &mut Vec<Line>,
        later: &mut Vec<Line>,
        sess: &mut Session,
        req_id: &str,
        auth_ip: u32,
    ) {
        self.new_users += 1;
        let uid = 1_000_000 + self.new_users;
        sess.uid = uid;
        sess.logged_in = true;
        let stored = self.db(
            lines,
            2,
            "auth_rw",
            SQL_USER_INSERT,
            None,
            Some(req_id),
            auth_ip,
            false,
        );
        let at = stored + self.rng.between(120, 400);
        if self.is_on(Svc::Auth) {
            let source = pick_w(&mut self.rng, &[(60, "web"), (25, "ios"), (15, "android")]);
            let mut l = self.auth_line(
                at,
                "user_created",
                "info",
                "user created",
                req_id,
                Some(uid),
                sess,
            );
            l.put("signup.source", V::K(source));
            if self.rng.chance(20) {
                let code = format!("REF-{}", base62(&mut self.rng, 6).to_uppercase());
                l.put("signup.referral", V::S(code));
            }
            lines.push(l);
        }
        let start = at + self.rng.between(200, 2_000);
        let args = vec![
            ("job.args.user_id", V::I(uid)),
            ("job.args.template", V::K("welcome")),
        ];
        let (end, _) = self.job(
            later,
            start,
            "emails",
            "send_welcome",
            args,
            Some(req_id),
            (80, 600),
            10,
            false,
        );
        self.notify(later, end - 1, "email", "welcome", uid, Some(req_id));
    }

    // ---- payments --------------------------------------------------------------------

    /// logstash-logback-encoder: `@timestamp` with an offset, `@version`, the MDC fields by
    /// their Java names. `ids` is the request id and the one span payments logs it under.
    #[allow(clippy::too_many_arguments)]
    fn pay_line(
        &mut self,
        dt: i64,
        event: &'static str,
        level: &'static str,
        logger: &'static str,
        message: String,
        ids: Option<(&str, &str)>,
        thread: String,
    ) -> Line {
        let host = self.rng.below(self.hosts[Svc::Payments.idx()].len() as u64) as u16;
        let level_value = match level {
            "ERROR" => 40_000,
            "WARN" => 30_000,
            "DEBUG" => 10_000,
            _ => 20_000,
        };
        let mut l = Line::new(Svc::Payments, host, dt);
        l.put("@timestamp", V::T(TimeForm::IsoMillisOffset, 0, 0))
            .put("@version", V::K("1"))
            .put("message", V::S(message))
            .put("logger_name", V::K(logger))
            .put("thread_name", V::S(thread))
            .put("level", V::K(level))
            .put("level_value", V::I(level_value))
            .put("HOSTNAME", V::S(self.host_name(Svc::Payments, host)));
        if let Some((request, span)) = ids {
            l.put("requestId", V::S(request.to_string()))
                .put("spanId", V::S(span.to_string()));
        }
        l.put("event", V::K(event));
        l
    }

    fn http_thread(&mut self) -> String {
        format!(
            "http-nio-8080-exec-{}",
            1 + self.threads.draw(&mut self.rng)
        )
    }

    fn amount_cents(&mut self) -> i64 {
        let a = self.rng.between(5, 400);
        let b = self.rng.between(5, 400);
        let dollars = a.min(b);
        dollars * 100 + pick_w(&mut self.rng, &[(40, 99), (30, 0), (15, 49), (15, 95)])
    }

    /// Returns the status the gateway sees, and whether it timed out waiting.
    fn checkout(
        &mut self,
        lines: &mut Vec<Line>,
        later: &mut Vec<Line>,
        sess: &Session,
        req_id: &str,
        pay_ip: u32,
        mi: &Minute,
    ) -> (i64, bool) {
        self.next_order += self.rng.between(1, 3);
        let oid = self.next_order;
        let (country, currency, brand, last4) = self.user_facts(sess.uid);
        let amount = self.amount_cents();
        let on = self.is_on(Svc::Payments);
        let thread = self.http_thread();
        let span = hex(&mut self.rng, 16);
        let ids = Some((req_id, span.as_str()));

        let mut t = 3 + self.rng.between(2, 20);
        let score = self.rng.between(0, 1000).min(self.rng.between(0, 1000)) / 10;
        let decision = if score > 92 {
            "deny"
        } else if score > 75 {
            "review"
        } else {
            "allow"
        };
        if on {
            let mut l = self.pay_line(
                t,
                "fraud_check",
                "INFO",
                "com.shop.payments.fraud.FraudScreen",
                format!("Fraud screen {decision} for order {oid}"),
                ids,
                thread.clone(),
            );
            l.put("order_id", V::I(oid))
                .put("customer.id", V::I(sess.uid))
                .put("customer.ip", V::S(ipv4(sess.ip)))
                .put("score", V::I(score))
                .put("decision", V::K(decision));
            if score > 60 {
                let rules = if score > 80 {
                    vec!["velocity", "geo_mismatch"]
                } else {
                    vec!["velocity"]
                };
                l.put("rules_hit", V::Arr(rules));
            }
            lines.push(l);
        }

        if mi.incident && self.rng.chance(35) {
            // The pool behind payments is exhausted: every wait ends at the timeout.
            for _ in 0..self.rng.between(1, 2) {
                let at = t + self.rng.below(GATEWAY_TIMEOUT_MS as u64 - 200) as i64;
                self.db(
                    lines,
                    at,
                    "payments_rw",
                    SQL_ORDER_INSERT,
                    None,
                    Some(req_id),
                    pay_ip,
                    true,
                );
            }
            if on {
                let at = GATEWAY_TIMEOUT_MS - self.rng.between(1, 40);
                let mut l = self.pay_line(
                    at,
                    "payment_failed",
                    "ERROR",
                    "com.shop.payments.PaymentService",
                    format!("Payment failed for order {oid}"),
                    ids,
                    thread,
                );
                l.put("order_id", V::I(oid))
                    .put("error.class", V::K("java.sql.SQLTransientConnectionException"))
                    .put("error.message", V::K("HikariPool-1 - Connection is not available, request timed out after 10000ms."))
                    .put("stack_trace", V::K(HIKARI_TRACE));
                lines.push(l);
            }
            return (504, true);
        }

        t = self.db(
            lines,
            t + 1,
            "payments_rw",
            SQL_ORDER_INSERT,
            None,
            Some(req_id),
            pay_ip,
            false,
        );
        let amount_v = self.dirty_num(cents(amount));
        let psp = if mix(self.seed, sess.uid as u64) % 10 < 7 {
            "stripe"
        } else {
            "adyen"
        };
        let roll = self.rng.below(1000);
        let psp_ms = self.rng.between(120, 900);
        // Screened out: the card is never sent to the processor.
        let fraud = decision == "deny";
        if fraud || (3..63).contains(&roll) {
            let code = if fraud {
                "fraudulent"
            } else {
                pick_w(&mut self.rng, DECLINES)
            };
            if on {
                let at = if fraud { t + 1 } else { t + psp_ms };
                let mut l = self.pay_line(
                    at,
                    "payment_declined",
                    "WARN",
                    "com.shop.payments.PaymentService",
                    format!("Payment declined for order {oid}: {code}"),
                    ids,
                    thread,
                );
                l.put("order_id", V::I(oid))
                    .put("customer.id", V::I(sess.uid))
                    .put("customer.country", V::K(country))
                    .put("amount", amount_v)
                    .put("currency", V::K(currency))
                    .put("card.brand", V::K(brand))
                    .put("card.last4", V::S(format!("{last4:04}")))
                    .put("psp", V::K(psp))
                    .put("decline_code", V::K(code));
                lines.push(l);
            }
            return (402, false);
        }
        if roll < 3 {
            // The processor never answered, and the client's read timeout ended it.
            if on {
                let at = t + self.rng.between(5_000, 5_100);
                let mut l = self.pay_line(
                    at,
                    "payment_failed",
                    "ERROR",
                    "com.shop.payments.PaymentService",
                    format!("Payment failed for order {oid}"),
                    ids,
                    thread,
                );
                l.put("order_id", V::I(oid))
                    .put("psp", V::K(psp))
                    .put("error.class", V::K("com.stripe.exception.ApiConnectionException"))
                    .put("error.message", V::K("IOException during API request to Stripe (https://api.stripe.com): Read timed out"))
                    .put("stack_trace", V::K(PSP_TRACE));
                lines.push(l);
            }
            return (502, false);
        }

        for _ in 0..self.rng.between(1, 3) {
            t = self.db(
                lines,
                t + 1,
                "payments_rw",
                SQL_RESERVE,
                None,
                Some(req_id),
                pay_ip,
                false,
            );
        }
        t = self.db(
            lines,
            t + 1,
            "payments_rw",
            SQL_PAYMENT_INSERT,
            None,
            Some(req_id),
            pay_ip,
            false,
        );
        let at = t + psp_ms;
        if on {
            let three_ds = self.rng.chance(30);
            let mut l = self.pay_line(
                at,
                "payment_authorized",
                "INFO",
                "com.shop.payments.PaymentService",
                format!("Payment authorized for order {oid}"),
                ids,
                thread,
            );
            l.put("order_id", V::I(oid))
                .put("customer.id", V::I(sess.uid))
                .put("customer.country", V::K(country))
                .put("amount", amount_v)
                .put("currency", V::K(currency))
                .put("card.brand", V::K(brand))
                .put("card.last4", V::S(format!("{last4:04}")))
                .put("psp", V::K(psp))
                .put("latency_ms", V::I(psp_ms));
            if three_ds {
                l.put("three_ds", V::B(true));
            }
            lines.push(l);
        }

        // After the response: capture the payment, then send the receipt. The capture job
        // is a span of its own.
        let start = at + self.rng.between(300, 3_000);
        let args = vec![("job.args.order_id", V::I(oid))];
        let (end, _) = self.job(
            later,
            start,
            "payments",
            "capture_payment",
            args,
            Some(req_id),
            (150, 1_200),
            5,
            mi.incident,
        );
        if on {
            let thread = format!("capture-worker-{}", 1 + self.rng.below(8));
            let psp_ref = format!("ch_{}", base62(&mut self.rng, 24));
            let amount_v = self.dirty_num(cents(amount));
            let span = hex(&mut self.rng, 16);
            let psp_ms = self.rng.between(90, 700);
            let mut l = self.pay_line(
                end - 1,
                "payment_captured",
                "INFO",
                "com.shop.payments.CaptureJob",
                format!("Payment captured for order {oid}"),
                Some((req_id, span.as_str())),
                thread,
            );
            l.put("order_id", V::I(oid))
                .put("amount", amount_v)
                .put("currency", V::K(currency))
                .put("psp", V::K(psp))
                .put("psp_ref", V::S(psp_ref))
                .put("latency_ms", V::I(psp_ms));
            later.push(l);
        }
        self.db(
            later,
            end - 60,
            "payments_rw",
            SQL_PAYMENT_UPDATE,
            None,
            Some(req_id),
            pay_ip,
            false,
        );
        let start = end + self.rng.between(50, 800);
        let args = vec![
            ("job.args.order_id", V::I(oid)),
            ("job.args.template", V::K("order_receipt")),
        ];
        let (end, _) = self.job(
            later,
            start,
            "emails",
            "send_receipt",
            args,
            Some(req_id),
            (80, 600),
            10,
            false,
        );
        self.notify(
            later,
            end - 1,
            "email",
            "order_receipt",
            sess.uid,
            Some(req_id),
        );
        (200, false)
    }

    fn refund(
        &mut self,
        lines: &mut Vec<Line>,
        later: &mut Vec<Line>,
        sess: &Session,
        req_id: &str,
        pay_ip: u32,
    ) {
        let oid = self.next_order - self.rng.between(100, 50_000);
        let (_, currency, _, _) = self.user_facts(sess.uid);
        let amount = self.amount_cents();
        let span = hex(&mut self.rng, 16);
        let t = self.db(
            lines,
            3,
            "payments_rw",
            SQL_PAYMENT_UPDATE,
            None,
            Some(req_id),
            pay_ip,
            false,
        );
        let at = t + self.rng.between(200, 900);
        if self.is_on(Svc::Payments) {
            let reason = pick_w(
                &mut self.rng,
                &[
                    (70, "requested_by_customer"),
                    (15, "not_received"),
                    (10, "duplicate"),
                    (5, "fraudulent"),
                ],
            );
            let psp_ref = format!("re_{}", base62(&mut self.rng, 24));
            let thread = self.http_thread();
            let amount_v = self.dirty_num(cents(amount));
            let mut l = self.pay_line(
                at,
                "refund_issued",
                "INFO",
                "com.shop.payments.RefundService",
                format!("Refund issued for order {oid}"),
                Some((req_id, span.as_str())),
                thread,
            );
            l.put("order_id", V::I(oid))
                .put("amount", amount_v)
                .put("currency", V::K(currency))
                .put("reason", V::K(reason))
                .put("psp_ref", V::S(psp_ref));
            lines.push(l);
        }
        let start = at + self.rng.between(200, 2_000);
        let args = vec![
            ("job.args.order_id", V::I(oid)),
            ("job.args.template", V::K("refund_issued")),
        ];
        let (end, _) = self.job(
            later,
            start,
            "emails",
            "send_refund_notice",
            args,
            Some(req_id),
            (80, 600),
            10,
            false,
        );
        self.notify(
            later,
            end - 1,
            "email",
            "refund_issued",
            sess.uid,
            Some(req_id),
        );
    }

    // ---- worker ----------------------------------------------------------------------

    /// A queued job from `job_started` to its end; returns when it ended and whether it
    /// succeeded. Logged on log15's level names, which really are four letters: `eror`, `dbug`.
    #[allow(clippy::too_many_arguments)]
    fn job(
        &mut self,
        lines: &mut Vec<Line>,
        start: i64,
        queue: &'static str,
        kind: &'static str,
        args: Vec<(&'static str, V)>,
        req_id: Option<&str>,
        dur: (i64, i64),
        fail_pm: u64,
        stressed: bool,
    ) -> (i64, bool) {
        if !self.is_on(Svc::Worker) {
            return (start + dur.0, true);
        }
        let host = self.rng.below(self.hosts[Svc::Worker.idx()].len() as u64) as u16;
        let id = uuid(&mut self.rng);
        let fail_pm = if stressed { fail_pm + 250 } else { fail_pm };
        let mut t = start;
        for attempt in 1..=3 {
            let l = self.job_line(
                t,
                host,
                "job_started",
                "info",
                "job started",
                queue,
                kind,
                &id,
                attempt,
                req_id,
                &args,
            );
            lines.push(l);
            let d = latency(&mut self.rng, dur.0, dur.1) / 10;
            t += d.max(1);
            if self.rng.below(1000) >= fail_pm {
                let dv = self.dirty_num(V::I(d));
                let mut l = self.job_line(
                    t,
                    host,
                    "job_succeeded",
                    "info",
                    "job finished",
                    queue,
                    kind,
                    &id,
                    attempt,
                    req_id,
                    &args,
                );
                l.put("duration_ms", dv);
                lines.push(l);
                return (t, true);
            }
            let err = pick_w(&mut self.rng, WORKER_ERRORS);
            let dv = self.dirty_num(V::I(d));
            let last = attempt == 3;
            let (event, msg) = if last {
                ("job_dead_lettered", "job moved to dead letter queue")
            } else {
                ("job_failed", "job failed")
            };
            let mut l = self.job_line(
                t, host, event, "eror", msg, queue, kind, &id, attempt, req_id, &args,
            );
            l.put("duration_ms", dv).put("error", V::K(err));
            lines.push(l);
            if last {
                return (t, false);
            }
            let backoff = 1_000 * (1 << attempt) + self.rng.between(0, 999);
            let mut l = self.job_line(
                t + 1,
                host,
                "job_retry",
                "warn",
                "job scheduled for retry",
                queue,
                kind,
                &id,
                attempt,
                req_id,
                &args,
            );
            l.put("backoff_ms", V::I(backoff));
            lines.push(l);
            t += backoff;
        }
        (t, false)
    }

    #[allow(clippy::too_many_arguments)]
    fn job_line(
        &mut self,
        dt: i64,
        host: u16,
        event: &'static str,
        lvl: &'static str,
        msg: &'static str,
        queue: &'static str,
        kind: &'static str,
        id: &str,
        attempt: i64,
        req_id: Option<&str>,
        args: &[(&'static str, V)],
    ) -> Line {
        let mut l = Line::new(Svc::Worker, host, dt);
        l.put("ts_ms", V::T(TimeForm::EpochMillis, 0, 0))
            .put("lvl", V::K(lvl))
            .put("msg", V::K(msg))
            .put("event", V::K(event))
            .put("queue", V::K(queue))
            .put("job.id", V::S(id.to_string()))
            .put("job.type", V::K(kind))
            .put("job.attempt", V::I(attempt));
        for (k, v) in args {
            l.put(k, v.clone());
        }
        l.put("worker", V::S(self.host_name(Svc::Worker, host)));
        if let Some(r) = req_id {
            l.put("req_id", V::S(r.to_string()));
        }
        l
    }

    fn plan_job(&mut self) -> Vec<Line> {
        let j = &JOBS[self.jobs.draw(&mut self.rng)];
        let (queue, kind, dur, fail_pm) = (j.queue, j.kind, j.ms, j.fail_per_mille);
        let mut lines = Vec::new();
        let args = match kind {
            "deliver_webhook" => {
                let endpoint = PARTNERS[self.partners.draw(&mut self.rng)];
                let topic = pick_w(
                    &mut self.rng,
                    &[
                        (60, "order.created"),
                        (30, "order.shipped"),
                        (10, "payment.refunded"),
                    ],
                );
                vec![
                    ("job.args.endpoint", V::K(endpoint)),
                    ("job.args.topic", V::K(topic)),
                ]
            }
            "resize_image" => {
                let width = pick_w(&mut self.rng, &[(50, 320), (30, 640), (20, 1280)]);
                vec![
                    (
                        "job.args.image_id",
                        V::I(self.rng.between(100_000, 999_999)),
                    ),
                    ("job.args.width", V::I(width)),
                ]
            }
            "sync_inventory_item" | "reindex_product" => {
                let wh = pick_w(&mut self.rng, &[(50, "ams1"), (35, "iad2"), (15, "sin1")]);
                let mut a = vec![("job.args.product_id", V::I(self.product()))];
                if kind == "sync_inventory_item" {
                    a.push(("job.args.warehouse", V::K(wh)));
                }
                a
            }
            "send_digest" => vec![
                (
                    "job.args.user_id",
                    V::I(100_000 + self.rng.between(0, 899_999)),
                ),
                ("job.args.template", V::K("weekly_digest")),
            ],
            _ => {
                let format = if self.rng.chance(70) { "csv" } else { "xlsx" };
                vec![
                    ("job.args.export_id", V::S(uuid(&mut self.rng))),
                    ("job.args.format", V::K(format)),
                ]
            }
        };
        let uid = match &args[0] {
            (_, V::I(u)) if kind == "send_digest" => *u,
            _ => 0,
        };
        let pid = match &args[0] {
            (_, V::I(p)) => *p,
            _ => 0,
        };
        let (end, ok) = self.job(&mut lines, 0, queue, kind, args, None, dur, fail_pm, false);
        if ok {
            let worker_ip = self.host_ip[Svc::Worker.idx()][0];
            match kind {
                "sync_inventory_item" => {
                    self.db(
                        &mut lines,
                        end / 2,
                        "worker_rw",
                        SQL_STOCK_SYNC,
                        None,
                        None,
                        worker_ip,
                        false,
                    );
                }
                "generate_export" => {
                    let rows = self.rng.between(200, 90_000);
                    self.db(
                        &mut lines,
                        end / 2,
                        "worker_rw",
                        SQL_EXPORT,
                        Some(rows),
                        None,
                        worker_ip,
                        false,
                    );
                }
                "reindex_product" if self.is_on(Svc::Search) => {
                    let host = self.rng.below(self.hosts[Svc::Search.idx()].len() as u64) as u16;
                    let mut l =
                        self.search_head(end - 1, host, "INFO", "search.indexer", "reindexed");
                    l.put("product_id", V::I(pid))
                        .put("index", V::K("products-v7"))
                        .put("took_ms", V::I(self.rng.between(4, 60)));
                    lines.push(l);
                }
                "send_digest" => {
                    self.notify(&mut lines, end - 1, "email", "weekly_digest", uid, None)
                }
                _ => {}
            }
        }
        // The worker is on whenever this source is, and a job always starts.
        debug_assert!(!lines.is_empty());
        lines
    }

    // ---- database proxy --------------------------------------------------------------

    /// One statement through pgbouncer, timed to the microsecond as Postgres prints it, with
    /// the request id in `application_name`. Logged when it ends; returns that offset.
    #[allow(clippy::too_many_arguments)]
    fn db(
        &mut self,
        lines: &mut Vec<Line>,
        start: i64,
        role: &'static str,
        sql: &'static str,
        rows: Option<i64>,
        req_id: Option<&str>,
        client: u32,
        fail: bool,
    ) -> i64 {
        let host = self.rng.below(self.hosts[Svc::DbProxy.idx()].len() as u64) as u16;
        let micros = self.rng.below(1000) as u32;
        let slow = !fail && self.rng.below(1000) < 3;
        let dur_us = if slow {
            self.rng.between(500_000, 3_000_000)
        } else {
            200 + latency(&mut self.rng, 0, 6) * 100 + self.rng.below(100) as i64
        };
        let end = if fail {
            start
        } else {
            start + dur_us / 1000 + 1
        };
        if !self.is_on(Svc::DbProxy) {
            return end;
        }
        let (severity, message) = if fail {
            ("ERROR", "query failed")
        } else if slow {
            ("WARNING", "slow query")
        } else {
            ("LOG", "query")
        };
        let pool = if role.ends_with("_ro") {
            "shop_ro"
        } else {
            "shop_rw"
        };
        let mut l = Line::new(Svc::DbProxy, host, end.max(0));
        l.put("time", V::T(TimeForm::IsoMicrosZ, 0, micros))
            .put("severity", V::K(severity))
            .put("host", V::S(self.host_name(Svc::DbProxy, host)))
            .put("message", V::K(message))
            .put("db", V::K("shop"))
            .put("user", V::K(role))
            .put("pool", V::K(pool))
            .put("client_addr", V::S(ipv4(client)));
        if let Some(r) = req_id {
            l.put("request_id", V::S(r.to_string()));
        }
        if fail {
            let (_, error, state) = PG_ERRORS[self.pg_errors.draw(&mut self.rng)];
            l.put("error", V::K(error)).put("sqlstate", V::K(state));
        } else {
            l.put("duration_ms", V::F(dur_us as f64 / 1000.0));
            if let Some(n) = rows {
                let v = self.dirty_num(V::I(n));
                l.put("rows", v);
            }
            if slow {
                l.put("threshold_ms", V::I(500));
            }
        }
        l.put("statement", V::K(sql));
        lines.push(l);
        end
    }

    // ---- notifier --------------------------------------------------------------------

    /// bunyan: name, hostname, pid, a numeric level, the fields, then msg, time and v.
    fn notify(
        &mut self,
        lines: &mut Vec<Line>,
        dt: i64,
        channel: &'static str,
        template: &'static str,
        uid: i64,
        req_id: Option<&str>,
    ) {
        if !self.is_on(Svc::Notifier) {
            return;
        }
        let host = self.rng.below(self.hosts[Svc::Notifier.idx()].len() as u64) as u16;
        let provider = if mix(self.seed, uid as u64).is_multiple_of(3) {
            "ses"
        } else {
            "sendgrid"
        };
        let roll = self.rng.below(1000);
        let level = if roll < 985 {
            30
        } else if roll < 995 {
            50
        } else {
            40
        };
        let mut l = Line::new(Svc::Notifier, host, dt.max(0));
        l.put("name", V::K("notifier"))
            .put("hostname", V::S(self.host_name(Svc::Notifier, host)))
            .put("pid", V::I(self.notifier_pid + host as i64))
            .put("level", V::I(level));
        if let Some(r) = req_id {
            l.put("req_id", V::S(r.to_string()));
        }
        let template_v = self.dirty_str(V::K(template));
        l.put("user", V::I(uid))
            .put("channel", V::K(channel))
            .put("template", template_v)
            .put("provider", V::K(provider));
        let msg = match level {
            30 => {
                // Ten digits, and not a time: a provider's message number that happens to
                // fall inside this century when read as seconds.
                self.next_message += self.rng.between(1, 9);
                l.put("message_id", V::I(self.next_message))
                    .put("latency_ms", V::I(self.rng.between(60, 450)));
                "notification sent"
            }
            50 => {
                let (name, message) = if provider == "ses" {
                    ("MessageRejected", "Email address is not verified.")
                } else {
                    ("ResponseError", "Too Many Requests")
                };
                l.put("err.name", V::K(name))
                    .put("err.message", V::K(message))
                    .put("err.code", V::I(if provider == "ses" { 400 } else { 429 }));
                "notification failed"
            }
            _ => {
                l.put("retry_in_ms", V::I(self.rng.between(1_000, 30_000)));
                "provider throttled, retrying"
            }
        };
        l.put("msg", V::K(msg))
            .put("time", V::T(TimeForm::IsoMillisZ, 0, 0))
            .put("v", V::I(0));
        lines.push(l);
    }

    // ---- search ----------------------------------------------------------------------

    fn search_head(
        &mut self,
        dt: i64,
        host: u16,
        level: &'static str,
        logger: &'static str,
        event: &'static str,
    ) -> Line {
        let mut l = Line::new(Svc::Search, host, dt);
        l.put("@timestamp", V::T(TimeForm::IsoMillisZ, 0, 0))
            .put("level", V::K(level))
            .put("logger", V::K(logger))
            .put("event", V::K(event))
            .put("host.name", V::S(self.host_name(Svc::Search, host)));
        l
    }

    /// structlog: `event` is the call site's name and every filter the user set is a key.
    /// Filters come as the sets a search page offers; independent coins explode the templates.
    #[allow(clippy::too_many_arguments)]
    fn search(
        &mut self,
        lines: &mut Vec<Line>,
        start: i64,
        req_id: &str,
        uid: Option<i64>,
        term: usize,
        page: i64,
    ) {
        let host = self.rng.below(self.hosts[Svc::Search.idx()].len() as u64) as u16;
        let roll = self.rng.below(100);
        let (event, level, hits, took) = if roll < 2 {
            (
                "slow_query",
                "WARNING",
                self.rng.between(1, 3000),
                self.rng.between(500, 3000),
            )
        } else if roll < 10 {
            ("no_results", "INFO", 0, self.rng.between(3, 40))
        } else {
            let a = self.rng.between(1, 3000);
            let b = self.rng.between(1, 3000);
            ("query", "INFO", a.min(b), self.rng.between(4, 120))
        };
        let facets = pick_w(
            &mut self.rng,
            &[(700, 0u8), (180, 1), (70, 2), (30, 3), (20, 4)],
        );
        let category = CATEGORIES[self.categories.draw(&mut self.rng)];
        let price = 25 * self.rng.between(1, 20);
        let suggestion = self.terms[self.term_w.draw(&mut self.rng)].clone();
        if !self.is_on(Svc::Search) {
            return;
        }
        let mut l = self.search_head(start + took, host, level, "search.api", event);
        l.put("request_id", V::S(req_id.to_string()));
        if let Some(u) = uid {
            l.put("user_id", V::I(u));
        }
        l.put("query.text", V::S(self.terms[term].clone()))
            .put("query.page", V::I(page));
        if matches!(facets, 1..=3) {
            l.put("query.filters.category", V::K(category));
        }
        if matches!(facets, 2 | 4) {
            l.put("query.filters.price_max", V::I(price));
        }
        if facets == 3 {
            l.put("query.filters.in_stock", V::B(true));
        }
        l.put("index", V::K("products-v7"))
            .put("hits", V::I(hits))
            .put("took_ms", V::I(took));
        if event == "slow_query" {
            l.put("threshold_ms", V::I(500));
        }
        if event == "no_results" {
            l.put("suggestion", V::S(suggestion));
        }
        lines.push(l);
    }

    // ---- cache -----------------------------------------------------------------------

    fn cache_miss(
        &mut self,
        lines: &mut Vec<Line>,
        dt: i64,
        cmd: &'static str,
        kind: &'static str,
        id: i64,
        client: u32,
    ) {
        if !self.is_on(Svc::Cache) {
            return;
        }
        let host = self.rng.below(self.hosts[Svc::Cache.idx()].len() as u64) as u16;
        let micros = self.rng.below(1000) as u32;
        let mut l = Line::new(Svc::Cache, host, dt);
        l.put("ts", V::T(TimeForm::IsoMicrosZ, 0, micros))
            .put("level", V::K("debug"))
            .put("host", V::S(self.host_name(Svc::Cache, host)))
            .put("cmd", V::K(cmd))
            .put("key", V::S(format!("{kind}:{id}")))
            .put("result", V::K("miss"))
            .put("latency_us", V::I(self.rng.between(60, 900)))
            .put("client", V::S(ipv4(client)));
        lines.push(l);
    }

    // ---- probes, cron and housekeeping ------------------------------------------------

    /// Kubelet health checks, flat through the day: round robin over every probed pod, so each
    /// is hit on a steady period, as `periodSeconds` does; the period follows `--lines`.
    fn plan_probe(&mut self, mi: &Minute) -> Vec<Line> {
        let gateways = self.gw_pods;
        let edges = if self.is_on(Svc::Edge) {
            self.hosts[Svc::Edge.idx()].len()
        } else {
            0
        };
        let searches = if self.is_on(Svc::Search) {
            self.hosts[Svc::Search.idx()].len()
        } else {
            0
        };
        let slot = self.probe_rr % (gateways + edges + searches);
        self.probe_rr += 1;
        let req_id = hex(&mut self.rng, 32);
        let mut lines = Vec::new();
        if slot < gateways {
            // Whichever generation of pods is serving this minute is the one probed.
            let host = (usize::from(mi.gw_new_pct >= 50) * gateways + slot) as u16;
            let probe = Session {
                ip: self.node_ip[Svc::Gateway.idx()][host as usize],
                ..self.quiet_session()
            };
            let gw = Gw {
                event: "request",
                level: "info",
                status: 200,
                method: "GET",
                path: "/healthz".to_string(),
                route: "/healthz",
                upstream: None,
                dur: self.rng.between(1, 6),
                bytes: 2,
                err: None,
                stack: None,
            };
            lines.push(self.gw_line(0, host, &probe, &req_id, None, gw));
        } else if slot < gateways + edges {
            let host = (slot - gateways) as u16;
            let probe = Session {
                ip: self.node_ip[Svc::Edge.idx()][host as usize],
                bot: Some(BotKind::Crawler),
                ..self.quiet_session()
            };
            lines.extend(self.edge(
                0,
                Some(host),
                &probe,
                &req_id,
                "GET",
                "/nginx-health".to_string(),
                200,
                2,
                None,
                V::K("-"),
            ));
        } else {
            let host = (slot - gateways - edges) as u16;
            let status = if self.rng.chance(1) {
                "yellow"
            } else {
                "green"
            };
            let took = self.rng.between(1, 5);
            let mut l = self.search_head(0, host, "INFO", "search.health", "health_check");
            l.put("cluster.status", V::K(status))
                .put("cluster.nodes", V::I(3))
                .put("took_ms", V::I(took));
            lines.push(l);
        }
        lines
    }

    fn plan_cron(&mut self, mi: &Minute) -> Vec<Line> {
        let m = mi.m;
        if self.cron_minute != m {
            self.cron_minute = m;
            self.cron_cursor = 0;
        }
        let due = cron_due(m);
        let cursor = self.cron_cursor;
        self.cron_cursor += 1;
        let (job, schedule, period, shard, dur) = match due.get(cursor) {
            Some(&(i, s)) => (
                CRON[i].name,
                CRON[i].schedule,
                CRON[i].period,
                s,
                CRON[i].ms,
            ),
            None => {
                self.tenant_cursor = (self.tenant_cursor + 1) % TENANTS;
                (ROLLUP.0, ROLLUP.1, 1, self.tenant_cursor, (60, 2_500))
            }
        };
        let (y, mo, d) = civil_from_days(self.day_ms.div_euclid(DAY_MS));
        let run_id = format!(
            "{job}-{shard}-{y:04}{mo:02}{d:02}T{:02}{:02}",
            m / 60,
            m % 60
        );
        let host = self
            .rng
            .below(self.hosts[Svc::Scheduler.idx()].len() as u64) as u16;
        let mut lines = Vec::new();

        if self.rng.below(1000) < 15 {
            let mut l = self.sched_line(
                0,
                host,
                "WARN",
                "job skipped",
                "skipped",
                job,
                schedule,
                &run_id,
                shard,
            );
            l.put("reason", V::K("previous run still active"));
            lines.push(l);
            return lines;
        }
        let mut l = self.sched_line(
            0,
            host,
            "INFO",
            "job triggered",
            "triggered",
            job,
            schedule,
            &run_id,
            shard,
        );
        l.put(
            "next_run",
            V::At(TimeForm::IsoSecondsZ, (m + period) * MINUTE_MS),
        );
        lines.push(l);

        let start = self.rng.between(5, 60);
        let args = vec![
            ("job.args.run_id", V::S(run_id.clone())),
            ("job.args.shard", V::I(shard as i64)),
        ];
        let (end, ok) = self.job(&mut lines, start, "cron", job, args, None, dur, 8, false);
        let sql = match job {
            "cleanup-sessions" => Some((SQL_SESSION_GC, None)),
            "expire-carts" => Some((SQL_CART_GC, None)),
            "sync-inventory" => Some((SQL_STOCK_SYNC, None)),
            "nightly-report" => Some((SQL_EXPORT, Some(self.rng.between(50_000, 400_000)))),
            "rollup-metrics" => Some((SQL_ROLLUP, None)),
            _ => None,
        };
        if let Some((sql, rows)) = sql {
            let worker_ip = self.host_ip[Svc::Worker.idx()][0];
            let at = start + (end - start) / 3;
            self.db(
                &mut lines,
                at,
                "worker_rw",
                sql,
                rows,
                None,
                worker_ip,
                false,
            );
        }
        if job == "rotate-signing-keys" && self.is_on(Svc::Auth) {
            let quiet = self.quiet_session();
            let mut l = self.auth_line(
                end - 1,
                "keys_rotated",
                "info",
                "signing keys rotated",
                "",
                None,
                &quiet,
            );
            l.put("keys.active", V::I(2)).put("keys.retired", V::I(1));
            lines.push(l);
        }
        let took = end - start;
        let (level, msg, event) = if ok {
            ("INFO", "job completed", "completed")
        } else {
            ("ERROR", "job failed", "failed")
        };
        let mut l = self.sched_line(
            end + 3,
            host,
            level,
            msg,
            event,
            job,
            schedule,
            &run_id,
            shard,
        );
        l.put("duration_ms", V::I(took));
        lines.push(l);
        lines
    }

    /// A session for a line no client caused: no address, no agent.
    fn quiet_session(&self) -> Session {
        Session {
            uid: 0,
            ip: 0,
            ua: PROBE,
            bot: None,
            app: false,
            logged_in: false,
            wants_login: false,
            warm: false,
            page: None,
            left: 1,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn sched_line(
        &mut self,
        dt: i64,
        host: u16,
        level: &'static str,
        msg: &'static str,
        event: &'static str,
        job: &'static str,
        schedule: &'static str,
        run_id: &str,
        shard: u32,
    ) -> Line {
        let mut l = Line::new(Svc::Scheduler, host, dt);
        l.put("time", V::T(TimeForm::IsoMillisZ, 0, 0))
            .put("level", V::K(level))
            .put("node", V::S(self.host_name(Svc::Scheduler, host)))
            .put("msg", V::K(msg))
            .put("event", V::K(event))
            .put("job", V::K(job))
            .put("schedule", V::K(schedule))
            .put("run_id", V::S(run_id.to_string()))
            .put("shard", V::I(shard as i64));
        l
    }

    fn plan_stats(&mut self, mi: &Minute) -> Vec<Line> {
        let mut lines = Vec::new();
        // The rollout: each old gateway pod drains and stops, and its replacement starts.
        // Once per pod, spread over the stats chains that follow the rollout.
        if mi.m >= self.rollout && self.rollout_logged < self.gw_pods {
            let old = self.rollout_logged as u16;
            self.rollout_logged += 1;
            lines.extend(self.lifecycle(old, old + self.gw_pods as u16));
        }
        lines.push(self.stats_line(mi));
        lines
    }

    fn lifecycle(&mut self, old: u16, new: u16) -> Vec<Line> {
        let head = |f: &mut Self,
                    dt: i64,
                    host: u16,
                    level: &'static str,
                    message: &'static str,
                    event: &'static str| {
            let mut l = Line::new(Svc::Gateway, host, dt);
            let version = if host >= f.gw_pods as u16 {
                "2.15.0"
            } else {
                "2.14.3"
            };
            l.put("@timestamp", V::T(TimeForm::IsoMillisZ, 0, 0))
                .put("log\\.level", V::K(level))
                .put("message", V::K(message))
                .put("ecs\\.version", V::K("1.6.0"))
                .put("event", V::K(event))
                .put("service.name", V::K("gateway"))
                .put("service.version", V::K(version))
                .put("host.name", V::S(f.host_name(Svc::Gateway, host)));
            l
        };
        let drain = self.rng.between(800, 9_000);
        let inflight = self.rng.between(0, 40);
        let mut lines = Vec::new();
        let mut l = head(
            self,
            0,
            old,
            "info",
            "received SIGTERM, draining",
            "shutdown_started",
        );
        l.put("signal", V::K("SIGTERM"))
            .put("inflight", V::I(inflight));
        lines.push(l);
        let mut l = head(
            self,
            drain,
            old,
            "info",
            "drained, exiting",
            "shutdown_complete",
        );
        l.put("drain_ms", V::I(drain));
        lines.push(l);
        let mut l = head(self, 5, new, "info", "starting gateway", "startup");
        l.put("config.version", V::K("routes-2024-01-15.3"))
            .put("go.version", V::K("go1.21.6"));
        lines.push(l);
        let up = 300 + self.rng.between(0, 400);
        let mut l = head(self, up, new, "info", "listening", "listening");
        l.put("listen.address", V::K(":8080"));
        lines.push(l);
        lines
    }

    /// Counters and gauges on a timer, one emitter after another: the most compressible
    /// lines in any estate. Now and then a rare line takes the slot instead.
    fn stats_line(&mut self, mi: &Minute) -> Line {
        let roll = self.rng.below(1000);
        if roll < 10 && self.is_on(Svc::Auth) {
            let quiet = self.quiet_session();
            let mut l = self.auth_line(
                0,
                "jwks_refreshed",
                "info",
                "signing keys refreshed",
                "",
                None,
                &quiet,
            );
            l.put("keys", V::I(2));
            return l;
        }
        if roll < 30 && self.is_on(Svc::Payments) {
            let psp = if self.rng.chance(70) {
                "stripe"
            } else {
                "adyen"
            };
            let degraded = mi.incident && self.rng.chance(50);
            let thread = format!("psp-health-{}", 1 + self.rng.below(2));
            let (level, state) = if degraded {
                ("WARN", "degraded")
            } else {
                ("DEBUG", "up")
            };
            let latency_ms = self.rng.between(40, 400);
            let mut l = self.pay_line(
                0,
                "psp_health",
                level,
                "com.shop.payments.psp.HealthCheck",
                format!("PSP {psp} {state}"),
                None,
                thread,
            );
            l.put("psp", V::K(psp)).put("latency_ms", V::I(latency_ms));
            return l;
        }
        let mut emitters: Vec<(Svc, usize)> = Vec::new();
        for svc in [Svc::DbProxy, Svc::Cache] {
            if self.is_on(svc) {
                emitters.extend((0..self.hosts[svc.idx()].len()).map(|h| (svc, h)));
            }
        }
        if self.is_on(Svc::Worker) {
            emitters.extend((0..QUEUES.len()).map(|q| (Svc::Worker, q)));
        }
        let (svc, which) = emitters[self.stats_rr % emitters.len()];
        self.stats_rr += 1;
        let load = Self::diurnal(mi.m);
        match svc {
            Svc::DbProxy => {
                let host = which as u16;
                let waiting = if mi.incident {
                    self.rng.between(40, 400)
                } else {
                    self.rng.between(0, 2)
                };
                let wait_us = if mi.incident {
                    self.rng.between(200_000, 9_000_000)
                } else {
                    self.rng.between(0, 80)
                };
                let mut l = Line::new(svc, host, 0);
                l.put(
                    "time",
                    V::T(TimeForm::IsoMicrosZ, 0, self.rng.below(1000) as u32),
                )
                .put("severity", V::K("LOG"))
                .put("host", V::S(self.host_name(svc, host)))
                .put("message", V::K("stats"))
                .put(
                    "stats.xact_per_s",
                    V::I(load / 20 + self.rng.between(0, 40)),
                )
                .put(
                    "stats.query_per_s",
                    V::I(load / 8 + self.rng.between(0, 90)),
                )
                .put("stats.avg_query_us", V::I(self.rng.between(300, 900)))
                .put("stats.avg_wait_us", V::I(wait_us))
                .put("stats.cl_active", V::I(self.rng.between(20, 60)))
                .put("stats.cl_waiting", V::I(waiting))
                .put("stats.sv_active", V::I(self.rng.between(5, 20)))
                .put(
                    "stats.sv_idle",
                    V::I(if mi.incident {
                        0
                    } else {
                        self.rng.between(2, 15)
                    }),
                );
                l
            }
            Svc::Cache => {
                let host = which as u16;
                let c = &mut self.cache_counters[which];
                c[0] += 500 + load * 3;
                c[1] += 60 + load / 2;
                c[2] += load / 90;
                let [hits, misses, evicted] = *c;
                let mut l = Line::new(svc, host, 0);
                l.put(
                    "ts",
                    V::T(TimeForm::IsoMicrosZ, 0, self.rng.below(1000) as u32),
                )
                .put("level", V::K("info"))
                .put("host", V::S(self.host_name(svc, host)))
                .put("msg", V::K("stats"))
                .put("used_memory_mb", V::I(2_048 + self.rng.between(0, 600)))
                .put("connected_clients", V::I(self.rng.between(80, 140)))
                .put("keyspace_hits", V::I(hits))
                .put("keyspace_misses", V::I(misses))
                .put("evicted_keys", V::I(evicted))
                .put("ops_per_sec", V::I(load * 2 + self.rng.between(0, 300)));
                l
            }
            _ => {
                let queue = QUEUES[which];
                let host = (which % self.hosts[Svc::Worker.idx()].len()) as u16;
                let depth = if mi.incident && matches!(queue, "emails" | "payments") {
                    self.rng.between(500, 5000)
                } else {
                    self.rng.between(0, 40)
                };
                let mut l = Line::new(Svc::Worker, host, 0);
                l.put("ts_ms", V::T(TimeForm::EpochMillis, 0, 0))
                    .put("lvl", V::K("dbug"))
                    .put("msg", V::K("queue stats"))
                    .put("event", V::K("queue_stats"))
                    .put("queue", V::K(queue))
                    .put("depth", V::I(depth))
                    .put("inflight", V::I(self.rng.between(0, 16)))
                    .put("oldest_age_ms", V::I(depth * self.rng.between(5, 40)))
                    .put("worker", V::S(self.host_name(Svc::Worker, host)));
                l
            }
        }
    }

    /// A client dropped from pgbouncer's wait queue. Only the incident has these.
    fn plan_storm(&mut self) -> Vec<Line> {
        let host = self.rng.below(self.hosts[Svc::DbProxy.idx()].len() as u64) as u16;
        let (role, svc) = pick_w(
            &mut self.rng,
            &[
                (50, ("payments_rw", Some(Svc::Payments))),
                (20, ("cart_rw", None)),
                (20, ("auth_rw", Some(Svc::Auth))),
                (10, ("worker_rw", Some(Svc::Worker))),
            ],
        );
        let client = match svc {
            Some(s) => {
                let i = self.rng.below(self.host_ip[s.idx()].len() as u64) as usize;
                self.host_ip[s.idx()][i]
            }
            None => self.upstream_addr("cart"),
        };
        let conn = format!("C-0x{}", hex(&mut self.rng, 12));
        let port = self.rng.between(32_768, 60_999);
        let mut l = Line::new(Svc::DbProxy, host, 0);
        l.put(
            "time",
            V::T(TimeForm::IsoMicrosZ, 0, self.rng.below(1000) as u32),
        )
        .put("severity", V::K("LOG"))
        .put("host", V::S(self.host_name(Svc::DbProxy, host)))
        .put("message", V::K("closing because: query_wait_timeout"))
        .put("db", V::K("shop"))
        .put("user", V::K(role))
        .put("pool", V::K("shop_rw"))
        .put("client", V::S(conn))
        .put("client_addr", V::S(ipv4(client)))
        .put("client_port", V::I(port))
        .put("age_s", V::I(120));
        vec![l]
    }

    // ---- the access log --------------------------------------------------------------

    /// nginx, `log_format json escape=json`: every value a string, `-` for what is missing.
    /// Logged when the response ends; the last edge host still runs last year's format.
    #[allow(clippy::too_many_arguments)]
    fn edge(
        &mut self,
        dt: i64,
        host: Option<u16>,
        sess: &Session,
        req_id: &str,
        method: &'static str,
        uri: String,
        status: i64,
        bytes: i64,
        upstream: Option<(Vec<u32>, Vec<i64>)>,
        referer: V,
    ) -> Option<Line> {
        if !self.is_on(Svc::Edge) {
            return None;
        }
        let n = self.hosts[Svc::Edge.idx()].len();
        let host = host.unwrap_or_else(|| self.rng.below(n as u64) as u16);
        let legacy = host as usize == n - 1;
        let proto = if sess.bot.is_some() || self.rng.chance(30) {
            "HTTP/1.1"
        } else {
            "HTTP/2.0"
        };
        let mut l = Line::new(Svc::Edge, host, dt);
        l.put("time", V::T(TimeForm::IsoSecondsOffset, 0, 0))
            .put("msec", V::T(TimeForm::MsecText, 0, 0))
            .put("hostname", V::S(self.host_name(Svc::Edge, host)))
            .put("remote_addr", V::S(ipv4(sess.ip)));
        if !legacy {
            l.put("request_id", V::S(req_id.to_string()));
        }
        l.put("request_method", V::K(method))
            .put("request_uri", V::S(uri));
        if !legacy {
            l.put("server_protocol", V::K(proto));
        }
        l.put("status", V::S(status.to_string()))
            .put("body_bytes_sent", V::S(bytes.to_string()))
            .put("request_time", V::S(secs3(dt)));
        let (addr, time) = match &upstream {
            Some((addrs, times)) => (
                addrs
                    .iter()
                    .map(|a| format!("{}:8080", ipv4(*a)))
                    .collect::<Vec<_>>()
                    .join(", "),
                times
                    .iter()
                    .map(|t| secs3(*t))
                    .collect::<Vec<_>>()
                    .join(", "),
            ),
            None => ("-".to_string(), "-".to_string()),
        };
        if !legacy {
            l.put("upstream_addr", V::S(addr));
        }
        l.put("upstream_response_time", V::S(time))
            .put("http_referer", referer)
            .put("http_user_agent", V::S(self.agents[sess.ua].clone()));
        Some(l)
    }
}

/// The scheduled runs due at minute `m`, rarest job first, so a budget too small for the
/// whole table drops shards of the busiest job rather than every job below it.
fn cron_due(m: i64) -> Vec<(usize, u32)> {
    let mut due: Vec<(usize, u32)> = Vec::new();
    for (i, c) in CRON.iter().enumerate() {
        if m >= c.offset && (m - c.offset) % c.period == 0 {
            due.extend((0..c.shards).map(|s| (i, s)));
        }
    }
    due.sort_by_key(|&(i, s)| (-CRON[i].period, s, i));
    due
}

/// What the gateway knows about a request when it logs it.
struct Gw {
    event: &'static str,
    level: &'static str,
    status: i64,
    method: &'static str,
    path: String,
    route: &'static str,
    /// Name, address, and time spent there in tenths of a millisecond.
    upstream: Option<(&'static str, u32, i64)>,
    /// Tenths of a millisecond.
    dur: i64,
    bytes: i64,
    err: Option<(&'static str, &'static str)>,
    stack: Option<&'static str>,
}

/// FNV-1a: a stable hash of a path, so a resource keeps its size all day.
fn fnv(s: &str) -> u64 {
    s.bytes().fold(0xcbf2_9ce4_8422_2325, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

fn pod_token(rng: &mut Rng, n: usize) -> String {
    const ALNUM: &[u8] = b"bcdfghjklmnpqrstvwxz2456789";
    (0..n)
        .map(|_| char::from(ALNUM[rng.below(ALNUM.len() as u64) as usize]))
        .collect()
}

fn deployment(svc: Svc) -> &'static str {
    match svc {
        Svc::Gateway => "gateway",
        Svc::Auth => "auth",
        Svc::Payments => "payments",
        Svc::Worker => "worker",
        Svc::DbProxy => "pgbouncer",
        Svc::Edge => "edge",
        Svc::Scheduler => "scheduler",
        Svc::Notifier => "notifier",
        Svc::Search => "search-api",
        Svc::Cache => "redis-proxy",
    }
}

/// Dotted path into nested objects. `\.` is a literal dot inside one member name, the way
/// ECS loggers write `log.level` - and the way `kibbr` renders such a key back.
fn insert(root: &mut Map<String, Value>, path: &str, value: Value) {
    let segments: Vec<String> = if path.contains('\\') {
        let mut out = vec![String::new()];
        let mut chars = path.chars();
        while let Some(c) = chars.next() {
            match c {
                '\\' => {
                    if let Some(next) = chars.next() {
                        out.last_mut().expect("one segment").push(next);
                    }
                }
                '.' => out.push(String::new()),
                c => out.last_mut().expect("one segment").push(c),
            }
        }
        out
    } else {
        path.split('.').map(str::to_string).collect()
    };
    let (last, parents) = segments.split_last().expect("a path has a segment");
    let mut cur = root;
    for p in parents {
        cur = cur
            .entry(p.clone())
            .or_insert_with(|| Value::Object(Map::new()))
            .as_object_mut()
            .expect("no farm line writes a value where it later nests");
    }
    cur.insert(last.clone(), value);
}

fn time_value(form: TimeForm, ms: i64, micros: u32) -> Value {
    let secs = ms.div_euclid(1000);
    let milli = ms.rem_euclid(1000);
    let stamp = || {
        let (y, mo, d) = civil_from_days(secs.div_euclid(86_400));
        let r = secs.rem_euclid(86_400);
        format!(
            "{y:04}-{mo:02}-{d:02}T{:02}:{:02}:{:02}",
            r / 3600,
            r % 3600 / 60,
            r % 60
        )
    };
    match form {
        TimeForm::IsoMillisZ => Value::String(format!("{}.{milli:03}Z", stamp())),
        TimeForm::IsoMillisOffset => Value::String(format!("{}.{milli:03}+00:00", stamp())),
        TimeForm::IsoSecondsOffset => Value::String(format!("{}+00:00", stamp())),
        TimeForm::IsoSecondsZ => Value::String(format!("{}Z", stamp())),
        TimeForm::IsoMicrosZ => Value::String(format!("{}.{milli:03}{micros:03}Z", stamp())),
        TimeForm::EpochMillis => Value::from(ms),
        TimeForm::EpochSeconds => Value::from(secs),
        TimeForm::MsecText => Value::String(format!("{secs}.{milli:03}")),
    }
}

#[cfg(test)]
mod tests {
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
}
