// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! Kibbr. Generate hostile logs, parse them, build the trie, query it.

use kibbr::cmd::{ask, bench, bench_storage, compact, demo, load, parse, query};
use kibbr::gen::{self, Profile};
use kibbr::{config, error, logs};
use kibbr_core::infer;

use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use tokio_util::sync::CancellationToken;

use error::CliError;

/// mimalloc in place of the system allocator, which aborted inside libmalloc on macOS
/// arm64 with no `unsafe` in src/ and a clean AddressSanitizer run.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser)]
#[command(name = "kibbr", version, about = "kibbr - log store", long_about = None)]
struct Cli {
    /// A TOML file of settings (see kibbr.toml.example); a key is named as its variable is.
    /// Flag > variable > file > default; an unknown key is refused. No --config, no file.
    #[arg(long, global = true, value_name = "FILE")]
    config: Option<PathBuf>,
    /// How much of kibbr's own log is written, to stderr: trace, debug, info, warn or
    /// error. Overrides `KIBBR__LOG__LEVEL` and `[log] level`. [default: info]
    #[arg(long, global = true, value_enum, value_name = "LEVEL")]
    log_level: Option<config::LogLevel>,
    /// How a log line is written, to stderr: `text`, or `json`, one object a line.
    /// Overrides `KIBBR__LOG__FORMAT` and `[log] format`. [default: text]
    #[arg(long, global = true, value_enum, value_name = "FORMAT")]
    log_format: Option<config::LogFormat>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Generate reproducible NDJSON logs on stdout.
    GenLogs {
        /// `hostile`: every case the parser must survive, over thirty days. `farm`: one day of a
        /// ten-service estate, in time order, shaped like real traffic.
        #[arg(long, value_enum, default_value = "hostile")]
        profile: Profile,
        /// How many NDJSON lines to write.
        #[arg(long, default_value_t = 1000)]
        lines: u64,
        /// `hostile` only: how many log shapes to draw from; optional fields split each into
        /// several templates. [default: 12]
        #[arg(long)]
        templates: Option<usize>,
        /// Same seed, byte-identical output. Nothing here reads the wall clock.
        #[arg(long, default_value_t = 42)]
        seed: u64,
        /// `farm` only: the simulated day, YYYY-MM-DD, UTC. [default: 2024-01-15]
        #[arg(long)]
        day: Option<String>,
        /// `farm` only: how many services run, 1 to 10; below 6 there is no access log.
        /// [default: 10]
        #[arg(long)]
        services: Option<usize>,
        /// `farm` only: percent of the values of five named keys that arrive dirty -
        /// `""`, `"N/A"`, or a number where a string was. [default: 0.5]
        #[arg(long, value_name = "PERCENT")]
        dirty_rate: Option<f64>,
    },
    /// Print the fingerprint and inferred types of each line.
    Parse {
        /// Path to an NDJSON file, or `-` for stdin.
        input: String,
        /// Stop after this many lines.
        #[arg(long)]
        limit: Option<u64>,
        /// Emit the `ParsedLog` as NDJSON instead of the human table.
        #[arg(long)]
        json: bool,
    },
    /// Build the trie over a file and print it as an ASCII tree.
    Demo {
        /// Path to an NDJSON file, or `-` for stdin.
        input: String,
        /// Print every subtree instead of summarising the large ones as `… N templates`.
        #[arg(long)]
        full: bool,
    },
    /// Serve the HTTP API and the log viewer. Rows are written to `--data-dir` and survive a
    /// restart; `--storage memory` keeps nothing past the process.
    Serve {
        /// The address to listen on. Loopback by default: `0.0.0.0` publishes a write endpoint with
        /// no authentication. Overrides the host of `KIBBR__API__NATIVE_ADDR`. [default: 127.0.0.1]
        #[arg(long)]
        host: Option<String>,
        /// Overrides the port of `KIBBR__API__NATIVE_ADDR` and of `[api] native_addr`.
        /// [default: 7700]
        #[arg(long)]
        port: Option<u16>,
        /// Rows a query that names no `LIMIT` is cut at.
        /// `0` disables it. Overrides `KIBBR__QUERY__MAX_ROWS`.
        #[arg(long, value_name = "ROWS")]
        max_rows: Option<usize>,
        /// Buckets a `GROUP BY time(...)` may emit before the query is refused.
        /// `0` disables it. Overrides `KIBBR__QUERY__MAX_BUCKETS`.
        #[arg(long, value_name = "BUCKETS")]
        max_buckets: Option<usize>,
        /// Groups a bucketed `GROUP BY` may emit before the query is refused.
        /// `0` disables it. Overrides `KIBBR__QUERY__MAX_GROUPS`.
        #[arg(long, value_name = "GROUPS")]
        max_groups: Option<usize>,

        // The three below are properties of the server, not of a query: a file has no wire, no
        // encoding and no second caller competing for the machine.
        /// The largest `/ingest` body accepted, measured on the wire *before* any
        /// decompression. Overrides `KIBBR__API__MAX_BODY_BYTES`.
        #[arg(long, value_name = "BYTES")]
        max_body_bytes: Option<usize>,
        /// The largest a gzipped `/ingest` body may become once decompressed. Cannot
        /// exceed the 64 MiB line limit. Overrides `KIBBR__API__MAX_DECOMPRESSED_BYTES`.
        #[arg(long, value_name = "BYTES")]
        max_decompressed_bytes: Option<usize>,
        /// How many `/ingest` bodies may be parsed at once; past it a body waits --ingest-wait-ms,
        /// then answered `503`. Overrides `KIBBR__API__INGEST_CONCURRENCY`. [default: one a core]
        #[arg(long, value_name = "N")]
        ingest_concurrency: Option<usize>,
        /// How long an `/ingest` body may wait, in milliseconds, before it is answered `503`; at
        /// most 30000. Overrides `KIBBR__API__INGEST_WAIT_MS`. [default: 100]
        #[arg(long, value_name = "MS")]
        ingest_wait_ms: Option<usize>,
        /// How long after a stop signal requests in flight are given to finish: a number and ms, s,
        /// m or h; `0s` waits for all. Overrides `KIBBR__API__SHUTDOWN_DEADLINE`. [default: 25s]
        #[arg(long, value_name = "DURATION", value_parser = config::parse_duration)]
        shutdown_deadline: Option<std::time::Duration>,
        /// How long one request may take: `408` for a body not yet arrived, `504` for a query,
        /// whose scan stops; `0s`: none. Overrides `KIBBR__API__REQUEST_TIMEOUT`. [default: 30s]
        #[arg(long, value_name = "DURATION", value_parser = config::parse_duration)]
        request_timeout: Option<std::time::Duration>,
        /// Where rows go: `disk`, day files under --data-dir, or `memory`, which writes nothing.
        #[arg(long, value_name = "KIND", default_value = "disk")]
        storage: StorageKind,
        /// The data directory. Created if absent. Overrides `KIBBR__STORAGE__BASE_PATH`.
        #[arg(long, value_name = "PATH")]
        data_dir: Option<PathBuf>,
        /// The trie snapshot. Defaults to `trie.json` inside the data directory.
        /// Overrides `KIBBR__STORAGE__TRIE_FILE`.
        #[arg(long, value_name = "PATH")]
        trie_file: Option<PathBuf>,
        /// How often the row group thresholds are checked, in milliseconds; a row is queryable at
        /// once either way. Overrides `KIBBR__INGESTOR__FLUSH_INTERVAL_MS`. [default: 500]
        #[arg(long, value_name = "MS")]
        flush_interval_ms: Option<usize>,
        /// How long a row group may stay open with rows in it, in milliseconds; they are in the log
        /// meanwhile. Overrides `KIBBR__INGESTOR__ROW_GROUP_TIMEOUT_MS`. [default: 300000]
        #[arg(long, value_name = "MS")]
        row_group_timeout_ms: Option<usize>,
        /// A segment of the write-ahead log rolls to the next number at this many bytes.
        /// Overrides `KIBBR__STORAGE__WAL_SEGMENT_BYTES`. [default: 64 MiB]
        #[arg(long, value_name = "BYTES")]
        wal_segment_bytes: Option<usize>,
        /// Lines at which one flush of the write-ahead log stops taking more bodies; a body is
        /// never split. Overrides `KIBBR__INGESTOR__BATCH_SIZE`. [default: 500]
        #[arg(long, value_name = "LINES")]
        batch_size: Option<usize>,
        /// The most bytes of lines that may be in the write-ahead log and not yet in the store;
        /// past it a body waits. Overrides `KIBBR__STORAGE__WAL_QUEUE_BYTES`. [default: 256 MiB]
        #[arg(long, value_name = "BYTES")]
        wal_queue_bytes: Option<usize>,
        /// How many day files may be open for writing at once; keep it under `ulimit -n`.
        /// Overrides `KIBBR__STORAGE__MAX_OPEN_SEGMENTS`. [default: 512]
        #[arg(long, value_name = "N")]
        max_open_segments: Option<usize>,
        /// A day's data file is sealed at this many bytes and the next one opened.
        /// Overrides `KIBBR__STORAGE__SEGMENT_MAX_BYTES`. [default: 512 MiB]
        #[arg(long, value_name = "BYTES")]
        segment_max_bytes: Option<usize>,
        /// Memory all row buffers together may hold before the largest are closed early; at twice
        /// it `/ingest` waits. Overrides `KIBBR__STORAGE__MAX_BUFFER_BYTES`. [default: 512 MiB]
        #[arg(long, value_name = "BYTES")]
        max_buffer_bytes: Option<usize>,
        /// How often to look for a finished day and compact it, in milliseconds; `0` never looks.
        /// Overrides `KIBBR__STORAGE__COMPACT_INTERVAL_MS`. [default: 60000]
        #[arg(long, value_name = "MS")]
        compact_interval_ms: Option<usize>,
        /// Threads one query may scan a day on; more than the machine has cores is refused.
        /// Overrides `KIBBR__QUERY__SCAN_THREADS`. [default: half the cores, at most 4]
        #[arg(long, visible_alias = "threads", value_name = "N")]
        scan_threads: Option<usize>,
        /// A run of row groups holding fewer rows than this is scanned on one thread.
        /// Overrides `KIBBR__QUERY__SCAN_FAN_ROWS`. [default: 32768]
        #[arg(long, value_name = "ROWS")]
        scan_fan_rows: Option<usize>,
        /// The key names a new template's time column is chosen from, in order, comma-separated;
        /// one holding none takes arrival time. Overrides `KIBBR__SCHEMA__TIME_COLUMNS`.
        #[arg(long, value_name = "NAMES", value_delimiter = ',')]
        time_columns: Option<Vec<String>>,
    },
    /// Load closed days of logs into a data directory: NDJSON, gzip or zstd, from files,
    /// directories or stdin. Every row or none, and the same inputs are never loaded twice.
    Load {
        /// Files, directories - every file under them, names starting with `.` skipped - or
        /// `-` for standard input. Read in order of file name, whatever order given.
        #[arg(required = true, value_name = "INPUT")]
        inputs: Vec<PathBuf>,
        /// The data directory. Overrides `KIBBR__STORAGE__BASE_PATH`. [default: kibbr-data]
        #[arg(long, value_name = "PATH")]
        data_dir: Option<PathBuf>,
        /// Publish these rows as the whole of this UTC day, retiring every file it held, in one
        /// step. Every row must be on that day.
        #[arg(long, value_name = "YYYY-MM-DD")]
        replace_day: Option<String>,
        /// Parse and encode at most this many megabytes of input a second, after decompression,
        /// so a load leaves the machine to the live door.
        #[arg(long, value_name = "MB")]
        max_mb_per_sec: Option<f64>,
        /// Threads to parse and encode on, at most one per core. [default: one per core up
        /// to 8 alone, half that inside a server]
        #[arg(long, value_name = "N")]
        threads: Option<usize>,
        /// Memory the load may hold rows in before it writes them; past it the largest row
        /// groups are written early. [default: 64 MiB]
        #[arg(long, value_name = "BYTES")]
        load_buffer_bytes: Option<usize>,
        /// The arrival time given to rows with no time of their own. [default: now]
        #[arg(long, value_name = "RFC3339")]
        received_at: Option<String>,
        /// Load even when more than --max-arrival-share of the rows have no time of their
        /// own and would be stored at their arrival time - now, or --received-at.
        #[arg(long)]
        proceed: bool,
        /// The percentage of rows that may fall back to arrival time before the load stops,
        /// loading nothing. [default: 1]
        #[arg(long, value_name = "PERCENT")]
        max_arrival_share: Option<f64>,
        /// As `serve --time-columns`; used only when no server holds the directory.
        /// Overrides `KIBBR__SCHEMA__TIME_COLUMNS`.
        #[arg(long, value_name = "NAMES", value_delimiter = ',')]
        time_columns: Option<Vec<String>>,
        /// A data file is sealed at this many bytes, when no server holds the directory.
        /// Overrides `KIBBR__STORAGE__SEGMENT_MAX_BYTES`. [default: 512 MiB]
        #[arg(long, value_name = "BYTES")]
        segment_max_bytes: Option<usize>,
        /// Print the report as one JSON object instead of a line.
        #[arg(long)]
        json: bool,
    },
    /// Compact a finished day: its small row groups rebuilt into full ones, published in one
    /// step. Nothing a query can see moves. A server holding the directory is asked to run it.
    Compact {
        /// Compact this UTC day now, whatever its age. Without it, every day that is due:
        /// each day before today whose last live row was written before today.
        #[arg(long, value_name = "YYYY-MM-DD")]
        day: Option<String>,
        /// The data directory, which must be one already; nothing is created.
        /// Overrides `KIBBR__STORAGE__BASE_PATH`. [default: kibbr-data]
        #[arg(long, value_name = "PATH")]
        data_dir: Option<PathBuf>,
        /// Threads to encode on, at most one per core. [default: one per core up to 8
        /// alone, half that inside a server]
        #[arg(long, value_name = "N")]
        threads: Option<usize>,
        /// A rebuilt file is sealed at this many bytes, when no server holds the directory.
        /// Overrides `KIBBR__STORAGE__SEGMENT_MAX_BYTES`. [default: 512 MiB]
        #[arg(long, value_name = "BYTES")]
        segment_max_bytes: Option<usize>,
        /// Print the report as one JSON object instead of a line a day.
        #[arg(long)]
        json: bool,
    },
    /// Measure what a file costs to store: raw, gzip -9, zstd -19 and as kibbr holds it, in
    /// bytes and disk blocks; then the same lines as a live day, before compaction and after.
    BenchStorage {
        /// NDJSON, gzip or zstd.
        #[arg(value_name = "FILE")]
        input: PathBuf,
        /// Also index the same lines into this Elasticsearch - a scratch index, deleted
        /// after - and print its store size beside kibbr's.
        #[arg(long, value_name = "URL")]
        es: Option<String>,
        /// Where to load it. Must be absent or empty. [default: a directory under the
        /// system's temporary directory]
        #[arg(long, value_name = "PATH")]
        scratch: Option<PathBuf>,
        /// Leave the scratch directory in place, to look at.
        #[arg(long)]
        keep: bool,
        /// As `load --time-columns`. Overrides `KIBBR__SCHEMA__TIME_COLUMNS`.
        #[arg(long, value_name = "NAMES", value_delimiter = ',')]
        time_columns: Option<Vec<String>>,
    },
    /// Measure how fast this build is on one farm day: ingest lines a second, the four
    /// standard queries idle and under ingest, scan rows per CPU-second, and the plan step.
    Bench {
        /// Lines in the farm day that is loaded (seed 42), and in the live stream sent to
        /// the servers (seed 43). The measurement is `--lines 1000000`.
        #[arg(
            long,
            value_parser = clap::value_parser!(u64).range(1..),
            required_unless_present_any = ["table", "plan_only"]
        )]
        lines: Option<u64>,
        /// Only `farm` is measured: the queries and their expected answers are the
        /// farm's. `hostile` is refused.
        #[arg(long, value_enum, required_unless_present_any = ["table", "plan_only"])]
        profile: Option<Profile>,
        /// Where the result is saved, the raw log beside it as `.log`; a file that exists is
        /// refused. [default: <dir>/results/<UTC time>-<commit>.json]
        #[arg(long, value_name = "FILE")]
        out: Option<PathBuf>,
        /// A saved result to compare with, held to be the same work before anything is measured;
        /// each number is printed with its speed-up on it.
        #[arg(long, value_name = "FILE")]
        baseline: Option<PathBuf>,
        /// The heading of this result's column, saved in the file.
        /// [default: baseline, or `this run` with --baseline]
        #[arg(long, value_name = "TEXT")]
        label: Option<String>,
        /// Holds the loaded directory, each server's copy of it, and the default results.
        #[arg(long, value_name = "PATH", default_value = bench::DEFAULT_DIR)]
        dir: PathBuf,
        /// How many times the whole sequence is run; each printed number is the middle
        /// one. Odd. Two results compare only at the same value.
        #[arg(long, value_name = "R", default_value_t = bench::DEFAULT_RUNS, value_parser = odd)]
        runs: usize,
        /// Timed asks of each query, and timed plans of each shape. Two results compare only at
        /// the same value.
        #[arg(
            long,
            value_name = "K",
            default_value_t = bench::DEFAULT_SAMPLES,
            value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..)
        )]
        samples: usize,
        /// Lines in one `/ingest` body. Two results compare only at the same value.
        #[arg(
            long,
            value_name = "L",
            default_value_t = bench::DEFAULT_BODY_LINES,
            value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..)
        )]
        body_lines: usize,
        /// Scan threads of every server measured: each is started with `--scan-threads T`.
        /// Two results compare only at the same value.
        #[arg(
            long,
            value_name = "T",
            value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..)
        )]
        threads: Option<usize>,
        /// Skip the checks that make a run a measurement, and allow a debug build; the result is
        /// marked `UNCHECKED - not a measurement`. For tests.
        #[arg(long)]
        unchecked: bool,
        /// Measure nothing: print the table from saved results, the first as the baseline
        /// and one column a file, headed by its label.
        #[arg(
            long,
            value_name = "FILE",
            num_args = 2..,
            conflicts_with_all = [
                "lines", "profile", "out", "baseline", "label", "dir", "runs", "samples",
                "body_lines", "unchecked", "plan_only"
            ]
        )]
        table: Vec<PathBuf>,
        // What a run starts as a child, once a run. Hidden, because its one JSON line is read
        // by the bench and not by a person.
        #[arg(
            long,
            hide = true,
            conflicts_with_all = [
                "lines", "profile", "out", "baseline", "label", "dir", "runs", "body_lines",
                "unchecked"
            ]
        )]
        plan_only: bool,
    },
    /// List every template a running server holds: its id, its rows, where its time
    /// comes from, and its path. What `GET /templates` answers, asked for you.
    Templates {
        /// The server to ask, `host:port`. [default: the server holding --data-dir; with none,
        /// `[api] native_addr`, 127.0.0.1:7700]
        #[arg(long, value_name = "HOST:PORT")]
        addr: Option<String>,
        /// The data directory whose server is asked.
        /// Overrides `KIBBR__STORAGE__BASE_PATH`. [default: kibbr-data]
        #[arg(long, value_name = "PATH")]
        data_dir: Option<PathBuf>,
        /// Print the server's answer as it sent it, one JSON object, instead of a table.
        #[arg(long)]
        json: bool,
    },
    /// Print what a running server says of itself: uptime, storage, the trie, ingest,
    /// the write-ahead log. What `GET /stats` answers, asked for you - a line a number.
    Stats {
        /// The server to ask, `host:port`. [default: the server holding --data-dir; with none,
        /// `[api] native_addr`, 127.0.0.1:7700]
        #[arg(long, value_name = "HOST:PORT")]
        addr: Option<String>,
        /// The data directory whose server is asked.
        /// Overrides `KIBBR__STORAGE__BASE_PATH`. [default: kibbr-data]
        #[arg(long, value_name = "PATH")]
        data_dir: Option<PathBuf>,
        /// Print the server's answer as it sent it, one JSON object, instead of a line a
        /// number.
        #[arg(long)]
        json: bool,
    },
    /// Ask a running server whether it is well: `GET /health`. Prints `ok`, or `degraded` and
    /// why, and exits 0; exits 1 when no server answers. What the image's HEALTHCHECK runs.
    Health {
        /// The server to ask, `host:port`. [default: the server holding --data-dir; with none,
        /// `[api] native_addr`, 127.0.0.1:7700]
        #[arg(long, value_name = "HOST:PORT")]
        addr: Option<String>,
        /// The data directory whose server is asked.
        /// Overrides `KIBBR__STORAGE__BASE_PATH`. [default: kibbr-data]
        #[arg(long, value_name = "PATH")]
        data_dir: Option<PathBuf>,
        /// Print the server's answer as it sent it, one JSON object, instead of a word.
        #[arg(long)]
        json: bool,
    },
    /// Parse a TrieQL query and print what it means. Opens no file and reads no data;
    /// `query <file> '<trieql>' --explain` is the half that prints the execution plan.
    Explain {
        /// e.g. `COUNT LOG {"event": "login_failed"} SINCE 1h`.
        query: String,
        /// As `serve --max-rows`: what an unlimited `MATCH` would be cut at.
        #[arg(long, value_name = "ROWS")]
        max_rows: Option<usize>,
        /// Buckets a `GROUP BY time(...)` may emit before the query is refused.
        /// `0` disables it. Overrides `KIBBR__QUERY__MAX_BUCKETS`.
        #[arg(long, value_name = "BUCKETS")]
        max_buckets: Option<usize>,
        /// Groups a bucketed `GROUP BY` may emit before the query is refused.
        /// `0` disables it. Overrides `KIBBR__QUERY__MAX_GROUPS`.
        #[arg(long, value_name = "GROUPS")]
        max_groups: Option<usize>,
    },
    /// Query a file, with TrieQL or with the flags.
    Query {
        /// Path to an NDJSON file, or `-` for stdin.
        input: String,

        /// A TrieQL query. Give this or the flags below, not both.
        #[arg(
            value_name = "TRIEQL",
            conflicts_with_all = [
                "matches", "count", "fetch", "exists", "group_by", "since", "between",
                "limit", "after"
            ]
        )]
        query: Option<String>,

        /// `KEY=VALUE`, repeatable. `VALUE` may be `*` for "present and not null".
        #[arg(long = "match", value_name = "KEY=VALUE")]
        matches: Vec<String>,

        // The four verbs are mutually exclusive. Spelled out rather than put in an `ArgGroup`,
        // which reports the pair as its first member repeated.
        /// How many rows match. The default when no other verb is given.
        #[arg(long, conflicts_with_all = ["fetch", "exists", "group_by"])]
        count: bool,
        /// The matching rows themselves.
        #[arg(long, conflicts_with_all = ["exists", "group_by"])]
        fetch: bool,
        /// Whether the template exists at all - the trie walk only, no rows read.
        #[arg(long, conflicts_with_all = ["group_by"])]
        exists: bool,
        /// Count per distinct value of these keys. A key may be `time(<duration>)`, which
        /// buckets by the timestamp instead - TrieQL spells it `GROUP BY time(5m)`.
        #[arg(long = "group-by", value_name = "KEY[,KEY]")]
        group_by: Option<String>,

        /// Relative window ending now: a number followed by s, m, h or d.
        #[arg(long, value_name = "DURATION", conflicts_with = "between")]
        since: Option<String>,
        /// Absolute window `[FROM, TO)`, both RFC 3339.
        #[arg(long, num_args = 2, value_names = ["FROM", "TO"])]
        between: Option<Vec<String>>,

        #[arg(long, value_enum, default_value = "table")]
        format: query::Format,

        /// Print the execution plan and execute nothing.
        #[arg(long)]
        explain: bool,

        /// Answer with the brute-force reference scanner instead of the engine.
        #[arg(long)]
        reference: bool,

        /// Return at most this many rows, newest first; TrieQL spells it `LIMIT n`. Omitted,
        /// every matching row comes back; `--limit 0` asks for none.
        #[arg(long)]
        limit: Option<usize>,

        /// Resume after a previous answer's `next_cursor`. TrieQL spells it `AFTER "…"`.
        /// Only valid for the query that produced it.
        #[arg(long, value_name = "CURSOR")]
        after: Option<String>,

        /// Rows a query that names no `LIMIT` is cut at; `0` disables it.
        /// Overrides `KIBBR__QUERY__MAX_ROWS`.
        #[arg(long, value_name = "ROWS")]
        max_rows: Option<usize>,
        /// Buckets a `GROUP BY time(...)` may emit before the query is refused.
        /// `0` disables it. Overrides `KIBBR__QUERY__MAX_BUCKETS`.
        #[arg(long, value_name = "BUCKETS")]
        max_buckets: Option<usize>,
        /// Groups a bucketed `GROUP BY` may emit before the query is refused.
        /// `0` disables it. Overrides `KIBBR__QUERY__MAX_GROUPS`.
        #[arg(long, value_name = "GROUPS")]
        max_groups: Option<usize>,
        /// As `serve --time-columns`: the names a template's time column is chosen from.
        /// Overrides `KIBBR__SCHEMA__TIME_COLUMNS`.
        #[arg(long, value_name = "NAMES", value_delimiter = ',')]
        time_columns: Option<Vec<String>>,
    },
}

/// A flag rather than inferred from `--data-dir`, so that `--storage memory --data-dir /x`
/// reads as an instruction and not as a contradiction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum StorageKind {
    Disk,
    Memory,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli) {
        Ok(code) => code,
        Err(e) => {
            logs::failed(&e);
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<ExitCode, CliError> {
    // The file first, and for every command alike: a bad one stops the process before
    // anything is done under settings its writer did not ask for.
    let file = config::File::read(cli.config.as_deref())?;
    let file = &file;
    // Then the log, so that everything after this line is said the way it was asked for.
    let log = config::log(file, cli.log_level, cli.log_format)?;
    logs::install(log);
    // Before any command types a value, and for every command alike: a bad window stops
    // the process here rather than after it has typed lines under the default.
    infer::set_epoch_window(config::epoch_window(file)?);

    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    // What every command but `serve` ends on once it has not failed.
    let mut code = ExitCode::SUCCESS;

    let result = match cli.command {
        Command::GenLogs {
            profile,
            lines,
            templates,
            seed,
            day,
            services,
            dirty_rate,
        } => gen::gen_logs(
            &mut out, profile, lines, templates, seed, day, services, dirty_rate,
        )
        .map_err(CliError::from),
        Command::Parse { input, limit, json } => parse::cli(&input, &mut out, limit, json),
        Command::Demo { input, full } => demo::cli(&input, &mut out, full),
        // The server is a library (`kibbr_server`), and this arm is its command line. Writes
        // nothing to `out`: the banner and the lifecycle log go to stderr through `logs`.
        Command::Serve {
            host,
            port,
            max_rows,
            max_buckets,
            max_groups,
            max_body_bytes,
            max_decompressed_bytes,
            ingest_concurrency,
            ingest_wait_ms,
            shutdown_deadline,
            request_timeout,
            storage,
            data_dir,
            trie_file,
            flush_interval_ms,
            row_group_timeout_ms,
            wal_segment_bytes,
            batch_size,
            wal_queue_bytes,
            max_open_segments,
            segment_max_bytes,
            max_buffer_bytes,
            compact_interval_ms,
            scan_threads,
            scan_fan_rows,
            time_columns,
        } => {
            let flags = config::ServeFlags {
                host,
                port,
                max_rows,
                max_buckets,
                max_groups,
                max_body_bytes,
                max_decompressed_bytes,
                ingest_concurrency,
                ingest_wait_ms,
                shutdown_deadline,
                request_timeout,
                memory: storage == StorageKind::Memory,
                data_dir,
                trie_file,
                flush_interval_ms,
                row_group_timeout_ms,
                wal_segment_bytes,
                batch_size,
                wal_queue_bytes,
                max_open_segments,
                segment_max_bytes,
                max_buffer_bytes,
                compact_interval_ms,
                scan_threads,
                scan_fan_rows,
                time_columns,
            };
            let settings = config::settings(file, flags, log)?;
            // Nothing here cancels the token: a server started from the command line is
            // stopped by SIGTERM or SIGINT, which it hears for itself.
            let report = kibbr_server::run(settings, CancellationToken::new())?;
            code = exit_code(&report);
            Ok(())
        }
        Command::Load {
            inputs,
            data_dir,
            replace_day,
            max_mb_per_sec,
            threads,
            load_buffer_bytes,
            received_at,
            proceed,
            max_arrival_share,
            time_columns,
            segment_max_bytes,
            json,
        } => load::cli(
            file,
            &mut out,
            inputs,
            data_dir,
            replace_day,
            max_mb_per_sec,
            threads,
            load_buffer_bytes,
            received_at,
            proceed,
            max_arrival_share,
            time_columns,
            segment_max_bytes,
            json,
        ),
        Command::Compact {
            day,
            data_dir,
            threads,
            segment_max_bytes,
            json,
        } => compact::cli(
            file,
            &mut out,
            day,
            data_dir,
            threads,
            segment_max_bytes,
            json,
        ),
        Command::BenchStorage {
            input,
            es,
            scratch,
            keep,
            time_columns,
        } => bench_storage::run(
            &bench_storage::Args {
                input,
                es,
                scratch,
                keep,
                settings: file.clone(),
            },
            config::schema(file, time_columns),
            &mut out,
        ),
        Command::Bench {
            lines,
            profile,
            out: save,
            baseline,
            label,
            dir,
            runs,
            samples,
            body_lines,
            threads,
            unchecked,
            table,
            plan_only,
        } => bench::cli(
            &mut out,
            lines,
            profile,
            save,
            baseline,
            label,
            dir,
            runs,
            samples,
            body_lines,
            threads,
            unchecked,
            table,
            plan_only,
        ),
        Command::Explain {
            query,
            max_rows,
            max_buckets,
            max_groups,
        } => query::describe(
            &mut out,
            &query,
            config::limits(file, max_rows, max_buckets, max_groups)?,
        ),
        Command::Templates {
            addr,
            data_dir,
            json,
        } => ask::templates(&mut out, &ask::server(file, addr, data_dir)?, json),
        Command::Stats {
            addr,
            data_dir,
            json,
        } => ask::stats(&mut out, &ask::server(file, addr, data_dir)?, json),
        Command::Health {
            addr,
            data_dir,
            json,
        } => ask::health(&mut out, &ask::server(file, addr, data_dir)?, json),
        Command::Query {
            input,
            query: trieql,
            matches,
            count,
            fetch,
            exists,
            group_by,
            since,
            between,
            format,
            explain,
            reference,
            limit,
            after,
            max_rows,
            max_buckets,
            max_groups,
            time_columns,
        } => query::cli(
            file,
            &mut out,
            &input,
            trieql,
            matches,
            count,
            fetch,
            exists,
            group_by,
            since,
            between,
            format,
            explain,
            reference,
            limit,
            after,
            max_rows,
            max_buckets,
            max_groups,
            time_columns,
        ),
    };

    // A closed downstream pipe (`| head`) is how the command ends, not a failure.
    match result {
        Err(e) if is_broken_pipe(&e) => return Ok(code),
        other => other?,
    }
    match out.flush() {
        Err(e) if e.kind() == io::ErrorKind::BrokenPipe => Ok(code),
        Err(e) => Err(CliError::Write(e)),
        Ok(()) => Ok(code),
    }
}

/// What `kibbr serve` exits with: 0 for every run that ended in a report, the deadline's
/// cut included, because every acknowledged line is in the write-ahead log.
fn exit_code(report: &kibbr_server::Report) -> ExitCode {
    // Every field named, so that one added to the report is looked at here.
    let kibbr_server::Report { addr: _, cut: _ } = report;
    ExitCode::SUCCESS
}

/// `bench --runs`: an odd count, so that the median of the runs is one of them and not a
/// number between two.
fn odd(text: &str) -> Result<usize, String> {
    match text.parse::<usize>() {
        Ok(runs) if runs % 2 == 1 => Ok(runs),
        Ok(runs) => Err(format!(
            "{runs} is not odd; each printed number is the middle one of the runs"
        )),
        Err(e) => Err(e.to_string()),
    }
}

fn is_broken_pipe(e: &CliError) -> bool {
    let io_error = match e {
        CliError::Write(inner) => Some(inner),
        CliError::Read { source, .. } => Some(source),
        CliError::Input(kibbr_storage::db::ReadError::Read { source, .. }) => Some(source),
        CliError::Gen(error::GenError::Write(inner)) => Some(inner),
        _ => None,
    };
    io_error.is_some_and(|inner| inner.kind() == io::ErrorKind::BrokenPipe)
}
