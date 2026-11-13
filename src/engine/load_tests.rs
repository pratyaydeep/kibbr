// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use std::io::Write;

struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "kibbr-load-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("mkdir");
        Self(dir)
    }

    fn write(&self, rel: &str, bytes: &[u8]) -> PathBuf {
        let path = self.0.join(rel);
        fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
        fs::write(&path, bytes).expect("write");
        path
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

const LINES: &[u8] = b"{\"a\":1}\n{\"a\":2}\n";

/// The id, against values worked out apart from this code: FNV-1a 64 over the name, a
/// zero byte, the length and the content's own FNV-1a 64, input by input in name order.
#[test]
fn the_load_id_is_the_documented_hash_in_name_order() {
    let scratch = Scratch::new("id");
    let x = scratch.write("one/x.ndjson", LINES);
    assert_eq!(
        load_id(&resolve(std::slice::from_ref(&x), None).expect("resolve")).expect("id"),
        0xf4ac_ae0a_82d2_f931
    );
    let a = scratch.write("two/a.ndjson", b"{\"b\":1}\n");
    let want = 0xa7c9_adc7_d15f_1550;
    assert_eq!(
        load_id(&resolve(&[x.clone(), a.clone()], None).expect("resolve")).expect("id"),
        want
    );
    assert_eq!(
        load_id(&resolve(&[a, x], None).expect("resolve")).expect("id"),
        want,
        "the order given changed the id"
    );
}

/// The same name and the same bytes after decompression are the same load, whatever
/// compressed them and wherever they sit.
#[test]
fn a_recompressed_file_elsewhere_is_the_same_load() {
    let scratch = Scratch::new("recompressed");
    let plain = scratch.write("plain/x.ndjson", LINES);
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::new(9));
    gz.write_all(LINES).expect("gzip");
    let gz = scratch.write("gz/x.ndjson", &gz.finish().expect("gzip"));
    let zst = scratch.write(
        "zst/x.ndjson",
        &zstd::stream::encode_all(LINES, 3).expect("zstd"),
    );
    let id = |p: &PathBuf| {
        load_id(&resolve(std::slice::from_ref(p), None).expect("resolve")).expect("id")
    };
    assert_eq!(id(&plain), id(&gz));
    assert_eq!(id(&plain), id(&zst));
    let renamed = scratch.write("plain/y.ndjson", LINES);
    assert_ne!(id(&plain), id(&renamed), "the name is part of the id");
}

#[test]
fn a_directory_is_its_files_in_name_order_without_hidden_ones() {
    let scratch = Scratch::new("dir");
    scratch.write("in/b.ndjson", LINES);
    scratch.write("in/deeper/a.ndjson", LINES);
    scratch.write("in/.partial", LINES);
    let names: Vec<String> = resolve(&[scratch.0.join("in")], None)
        .expect("resolve")
        .into_iter()
        .map(|i| i.name)
        .collect();
    assert_eq!(names, vec!["a.ndjson", "b.ndjson"]);
    assert!(resolve(&[scratch.0.join("missing")], None).is_err());
}

// ── Loads into a data directory of the test's own ──

/// A data directory opened as `kibbr load` alone opens one. The lock is dropped last,
/// after the stores that write under it.
struct Store {
    trie: Trie,
    stores: StoreManager,
    _lock: crate::engine::lock::DataLock,
}

impl Store {
    fn open(scratch: &Scratch) -> Self {
        // What the binary's settings make of `--data-dir`: the directory made, its
        // path absolute, and the trie's snapshot named inside it.
        let base = scratch.0.join("store");
        fs::create_dir_all(&base).expect("mkdir");
        let base = fs::canonicalize(&base).expect("the directory just made");
        let storage = crate::engine::Storage {
            base_path: Some(base.clone()),
            trie_file: Some(base.join("trie.json")),
            ..crate::engine::Storage::default()
        };
        let lock = crate::engine::lock::DataLock::acquire(&base).expect("lock");
        let (trie, stores) = crate::engine::database::lifecycle::open_durable(
            &storage,
            &crate::engine::database::Schema::default(),
        )
        .expect("open the data directory");
        Self {
            trie,
            stores,
            _lock: lock,
        }
    }

    fn load(&self, opts: &Options) -> Result<(Report, Measured), LoadError> {
        run_measured(
            &self.trie,
            &self.stores,
            opts,
            &Control::default(),
            &mut |_| {},
        )
    }

    /// Every template's rows, as a scan hands them out: each day in storage order.
    fn rows(&self) -> Vec<(u64, Row)> {
        let mut out = Vec::new();
        for leaf in self.trie.all_leaves() {
            let Some(id) = leaf
                .leaf_meta
                .read()
                .expect("leaf meta")
                .as_ref()
                .map(|m| m.template_id)
            else {
                continue;
            };
            let Some(store) = self.stores.lookup(id) else {
                continue;
            };
            let scan = store
                .scan(&crate::engine::store::ScanOptions::default(), &[])
                .expect("scan");
            out.extend(scan.rows.into_iter().map(|row| (id, row)));
        }
        out
    }
}

fn options(inputs: Vec<Input>) -> Options {
    Options {
        inputs,
        replace_day: None,
        max_bytes_per_sec: None,
        threads: 2,
        load_buffer_bytes: DEFAULT_LOAD_BUFFER_BYTES,
        received_at: "2024-02-20T12:00:00Z".parse().expect("an instant"),
        proceed: false,
        max_arrival_share: DEFAULT_MAX_ARRIVAL_SHARE,
    }
}

/// `count` timed lines across `templates` key sets and `days` days, interleaved so every
/// template-day is open at once; line `i` carries `n` = `i`.
fn spread(count: usize, templates: usize, days: usize) -> Vec<u8> {
    (0..count)
        .map(|i| {
            format!(
                "{{\"ts\":\"2024-01-{:02}T10:00:00Z\",\"t{}\":\"x\",\"n\":{i}}}\n",
                1 + i * 13 % days,
                i * 7 % templates
            )
        })
        .collect::<String>()
        .into_bytes()
}

/// `count` lines of template `k` with no time of their own; `logged_at` is a Datetime,
/// but no name on the default time columns.
fn untimed(count: usize, k: usize) -> Vec<u8> {
    (0..count)
        .map(|i| format!("{{\"logged_at\":\"2023-06-01T09:00:00Z\",\"u{k}\":{i}}}\n"))
        .collect::<String>()
        .into_bytes()
}

/// A hard bound: open batches and closed ones not yet written together never hold more
/// than `--load-buffer-bytes`, and every row is still stored once, in line order.
#[test]
fn a_tiny_load_buffer_holds_its_rows_to_the_bound_and_loses_none() {
    const LINES: usize = 6_000;
    const BOUND: usize = 16 * 1024;
    let scratch = Scratch::new("bound");
    let input = scratch.write("in/spread.ndjson", &spread(LINES, 12, 7));
    let store = Store::open(&scratch);
    let opts = Options {
        load_buffer_bytes: BOUND,
        ..options(resolve(&[input], None).expect("resolve"))
    };
    let (report, measured) = store.load(&opts).expect("load");
    assert_eq!(report.rows, LINES as u64);
    assert!(
        measured.peak_held <= BOUND,
        "held {} bytes of rows against a bound of {BOUND}",
        measured.peak_held
    );

    let mut seen = vec![false; LINES];
    let mut last: HashMap<(u64, i64), i64> = HashMap::new();
    for (template, row) in store.rows() {
        let Some(crate::engine::model::types::ColValue::Int64(n)) = row.get("n") else {
            panic!("a row with no n: {row:?}");
        };
        let day = day_of(effective_ts(&row, "ts").expect("a time"));
        let before = last.insert((template, day), *n);
        assert!(
            before.is_none_or(|b| b < *n),
            "template {template} on {}: n {n} after {before:?}",
            day_name(day)
        );
        assert!(
            !std::mem::replace(&mut seen[*n as usize], true),
            "n {n} twice"
        );
    }
    assert!(seen.iter().all(|s| *s), "rows missing");
    assert_eq!(last.len(), 84, "not every template-day was stored");
}

/// The paced pass reads each input byte once, so its pace is charged the inputs'
/// decompressed length exactly, not the id's digest or the head check's reads as well.
#[test]
fn the_pace_is_charged_each_input_byte_once() {
    let scratch = Scratch::new("paced");
    let plain = spread(3_000, 5, 3);
    let other = spread(2_000, 4, 2);
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::new(6));
    gz.write_all(&other).expect("gzip");
    let a = scratch.write("in/a.ndjson", &plain);
    let b = scratch.write("in/b.ndjson.gz", &gz.finish().expect("gzip"));
    let store = Store::open(&scratch);
    let opts = Options {
        // A rate no load reaches: charged, never waited on.
        max_bytes_per_sec: Some(1e15),
        ..options(resolve(&[a, b], None).expect("resolve"))
    };
    let (_, measured) = store.load(&opts).expect("load");
    assert_eq!(measured.paced, (plain.len() + other.len()) as u64);
}

/// The rate is what the load delivers: bytes at N a second take about bytes / N seconds.
/// Wide margins, for a debug build on a busy machine.
#[test]
fn a_paced_load_takes_about_its_bytes_over_the_rate() {
    let scratch = Scratch::new("rate");
    let bytes = spread(1_500, 5, 3);
    let input = scratch.write("in/a.ndjson", &bytes);
    let store = Store::open(&scratch);
    let opts = Options {
        max_bytes_per_sec: Some(bytes.len() as f64),
        ..options(resolve(&[input], None).expect("resolve"))
    };
    let (report, _) = store.load(&opts).expect("load");
    assert!(
        (0.8..2.5).contains(&report.seconds),
        "{} bytes at {} a second took {}s",
        bytes.len(),
        bytes.len(),
        report.seconds
    );
}

/// The head check makes nothing, since a template made while the load is refused would
/// keep arrival time, so it names a new template by the line it first met it on.
#[test]
fn the_head_check_names_a_new_template_by_its_line_and_makes_none() {
    let scratch = Scratch::new("head");
    let mut body = spread(50, 2, 2);
    body.extend_from_slice(&untimed(150, 0));
    let input = scratch.write("in/a.ndjson", &body);
    let store = Store::open(&scratch);
    let err = store
        .load(&options(resolve(&[input], None).expect("resolve")))
        .expect_err("refused");
    let text = err.to_string();
    for phrase in [
        "75.0% of the first 200 rows (150)",
        "2024-02-20",
        "a new template first on line 51 (150 rows; Datetime-typed keys: logged_at)",
        "--proceed",
    ] {
        assert!(text.contains(phrase), "no `{phrase}`: {text}");
    }
    assert!(
        store.trie.all_leaves().is_empty(),
        "the check made a template"
    );
}

/// The head check reads at most `HEAD_LINES` lines, rows or not; here it finds no row and
/// lets the load through to the count over every row, which stops it.
#[test]
fn the_head_check_stops_at_its_line_cap_and_the_count_over_every_row_decides() {
    let scratch = Scratch::new("head-lines");
    let mut body: Vec<u8> = (0..HEAD_LINES + 5)
        .flat_map(|i| format!("not json {i}\n").into_bytes())
        .collect();
    body.extend_from_slice(&untimed(50, 0));
    let input = scratch.write("in/a.ndjson", &body);
    let store = Store::open(&scratch);
    let text = store
        .load(&options(resolve(&[input], None).expect("resolve")))
        .expect_err("refused")
        .to_string();
    assert!(
        text.contains("100.0% of the 50 rows (50)"),
        "not the count over every row: {text}"
    );
}

/// A template with a time column whose rows held no instant there is named by that
/// column: no time key an operator could add would change where its rows go.
#[test]
fn a_template_with_a_time_column_is_named_by_it_when_its_rows_fall_back() {
    let scratch = Scratch::new("column-fell-back");
    let mut body = spread(10_500, 1, 1);
    for i in 0..500 {
        body.extend_from_slice(
            format!("{{\"ts\":\"N/A\",\"t0\":\"x\",\"n\":{i}}}\n").as_bytes(),
        );
    }
    let input = scratch.write("in/a.ndjson", &body);
    let store = Store::open(&scratch);
    let text = store
        .load(&options(resolve(&[input], None).expect("resolve")))
        .expect_err("refused")
        .to_string();
    assert!(
        text.contains("(500 rows; its time column `ts` held no time)"),
        "{text}"
    );
}

/// Past the head's limit and within it over every row, `--proceed` loads - and the
/// summary says the load was within the limit, not past it.
#[test]
fn a_load_past_only_the_head_says_it_was_within_the_limit() {
    let scratch = Scratch::new("head-only");
    let mut body = untimed(150, 0);
    body.extend_from_slice(&spread(49_850, 2, 2));
    let input = scratch.write("in/a.ndjson", &body);
    let store = Store::open(&scratch);
    let refused = store
        .load(&options(
            resolve(std::slice::from_ref(&input), None).expect("resolve"),
        ))
        .expect_err("the head is past the limit");
    assert!(
        refused.to_string().contains("of the first 10000 rows"),
        "{refused}"
    );
    let (report, _) = store
        .load(&Options {
            proceed: true,
            ..options(resolve(&[input], None).expect("resolve"))
        })
        .expect("loaded with --proceed");
    let summary = report.summary();
    assert!(
        summary.contains("150 of 50,000 rows (0.3%)")
            && summary.contains("within --max-arrival-share 1% over every row"),
        "{summary}"
    );
}

/// Past a clean head, the count is over every row, and the refusal names the templates
/// that fell back by id, most rows first, ten at most.
#[test]
fn the_whole_input_check_names_ten_templates_most_rows_first() {
    let scratch = Scratch::new("whole");
    let mut body = spread(10_500, 3, 2);
    for k in 0..12 {
        body.extend_from_slice(&untimed(100 + k, k));
    }
    let input = scratch.write("in/a.ndjson", &body);
    let store = Store::open(&scratch);
    let err = store
        .load(&options(resolve(&[input], None).expect("resolve")))
        .expect_err("refused");
    let text = err.to_string();
    let fell_back: usize = (0..12).map(|k| 100 + k).sum();
    let rows = 10_500 + fell_back;
    for phrase in [
        format!("of the {rows} rows ({fell_back})"),
        "(111 rows; Datetime-typed keys: logged_at)".to_string(),
        "and 2 more".to_string(),
    ] {
        assert!(text.contains(&phrase), "no `{phrase}`: {text}");
    }
    assert!(
        !text.contains("(100 rows;"),
        "the fewest rows named first: {text}"
    );
    let staging = scratch
        .0
        .join("store")
        .join(crate::engine::store::disk::STAGING_DIR);
    let left: Vec<_> = fs::read_dir(&staging)
        .map(|d| d.flatten().map(|e| e.path()).collect())
        .unwrap_or_default();
    assert!(left.is_empty(), "staging left: {left:?}");
}
