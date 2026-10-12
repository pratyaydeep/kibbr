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
    /// A TOML file of settings, read by every command: `[storage]`, `[ingestor]`,
    /// `[api]`, `[query]`, `[schema]` and `[log]`, each key named as its variable is -
    /// `[storage] base_path` is `KIBBR__STORAGE__BASE_PATH`. A flag wins over its
    /// variable and the variable over the file; a key this build has no setting for is
    /// refused. With no --config no file is read.
    #[arg(long, global = true, value_name = "FILE")]
    config: Option<PathBuf>,
    /// How much of kibbr's own log is written, to stderr: trace, debug, info, warn or
    /// error. Overrides `KIBBR__LOG__LEVEL` and `[log] level`. [default: info]
    #[arg(long, global = true, value_enum, value_name = "LEVEL")]
    log_level: Option<config::LogLevel>,
    /// How a log line is written: `text`, a line for a person, or `json`, one object a
    /// line. A command's data is on stdout either way.
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
        /// `hostile` is every case the parser must survive, spread flat over a few shapes
        /// and thirty days; fixtures depend on its bytes. `farm` is one day of a
        /// ten-service estate, in time order, shaped like real traffic.
        #[arg(long, value_enum, default_value = "hostile")]
        profile: Profile,
        /// How many NDJSON lines to write.
        #[arg(long, default_value_t = 1000)]
        lines: u64,
        /// `hostile` only: how many distinct log shapes to draw from. Optional fields
        /// split each shape into several fingerprints, so `kibbr parse` reports more
        /// templates than this. [default: 12]
        #[arg(long)]
        templates: Option<usize>,
        /// Same seed, byte-identical output. Nothing here reads the wall clock.
        #[arg(long, default_value_t = 42)]
        seed: u64,
        /// `farm` only: the simulated day, YYYY-MM-DD, UTC. [default: 2024-01-15]
        #[arg(long)]
        day: Option<String>,
        /// `farm` only: how many services run, 1 to 10 - the first N of gateway, auth,
        /// payments, worker, dbproxy, access log, scheduler, notifier, search, cache.
        /// Below 6 there is no access log in front of the gateway. [default: 10]
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
        /// Print every subtree instead of summarising the large ones. The default view
        /// hides most of the templates behind `… N templates`, which is what an operator
        /// wants and the opposite of what a test comparing two orderings wants.
        #[arg(long)]
        full: bool,
    },
    /// Serve the native HTTP API. Rows are written to `--data-dir` and survive a restart;
    /// `--storage memory` opts out, and everything ingested is then lost when the process
    /// exits - which the startup banner says in as many words either way.
    Serve {
        /// Loopback by default. A container needs `--host 0.0.0.0`, and that is a
        /// decision to publish an unauthenticated write endpoint - so it is typed, not
        /// defaulted into. Overrides the host of `KIBBR__API__NATIVE_ADDR` and of
        /// `[api] native_addr`. [default: 127.0.0.1]
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
        /// Groups a bucketed `GROUP BY` may emit before the query is refused - the bucket
        /// count times the distinct values of the other keys.
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
        /// How many `/ingest` bodies may be parsed at once. Beyond it, a body waits
        /// for --ingest-wait-ms and is then answered `503` with a `Retry-After` rather
        /// than left to time out. Overrides `KIBBR__API__INGEST_CONCURRENCY`.
        #[arg(long, value_name = "N")]
        ingest_concurrency: Option<usize>,
        /// How long an `/ingest` body may be kept waiting, in milliseconds, before it is
        /// answered `503` with a `Retry-After`: at the door, and then for room in the
        /// write-ahead log - each wait is at most this long. `0` waits for nothing; more
        /// than 30000 is refused.
        /// Overrides `KIBBR__API__INGEST_WAIT_MS`. [default: 100]
        #[arg(long, value_name = "MS")]
        ingest_wait_ms: Option<usize>,
        /// How long after SIGTERM or SIGINT the requests in flight are given to finish:
        /// a number and ms, s, m or h. What is still in flight then is cut off, and the
        /// final flush, the checkpoint and the trie snapshot follow - so keep it a few
        /// seconds under whatever kills the process next (Kubernetes: 30s). `0s` waits
        /// for every request, however long it takes.
        /// Overrides `KIBBR__API__SHUTDOWN_DEADLINE`. [default: 25s]
        #[arg(long, value_name = "DURATION", value_parser = config::parse_duration)]
        shutdown_deadline: Option<std::time::Duration>,
        /// How long one request may take from its first line to its answer: a number and
        /// ms, s, m or h. A request whose body has not arrived by then is answered `408`;
        /// a query still running is answered `504` and its scan stops. An `/ingest` body
        /// that has been read whole is not timed out - its waits are --ingest-wait-ms's.
        /// `0s` is no timeout.
        /// Overrides `KIBBR__API__REQUEST_TIMEOUT`. [default: 30s]
        #[arg(long, value_name = "DURATION", value_parser = config::parse_duration)]
        request_timeout: Option<std::time::Duration>,
        /// Where rows go. `disk` writes day-partitioned column files and snapshots the
        /// trie on a clean shutdown; `memory` writes nothing, and is what the
        /// memory-versus-disk differential runs against.
        #[arg(long, value_name = "KIND", default_value = "disk")]
        storage: StorageKind,
        /// The data directory. Created if absent. Overrides `KIBBR__STORAGE__BASE_PATH`.
        #[arg(long, value_name = "PATH")]
        data_dir: Option<PathBuf>,
        /// The trie snapshot. Defaults to `trie.json` inside the data directory.
        /// Overrides `KIBBR__STORAGE__TRIE_FILE`.
        #[arg(long, value_name = "PATH")]
        trie_file: Option<PathBuf>,
        /// How often the row group thresholds are checked, in milliseconds. This is not
        /// when a row reaches the disk - see --row-group-timeout-ms - and not when it
        /// becomes queryable, which is at once.
        /// Overrides `KIBBR__INGESTOR__FLUSH_INTERVAL_MS`.
        #[arg(long, value_name = "MS")]
        flush_interval_ms: Option<usize>,
        /// How long a row group may stay open with rows in it, in milliseconds, when
        /// neither 8,192 rows nor 8MB got there first. The rows that wait are in the
        /// write-ahead log, so this is replay time after a crash and not rows. Floored at
        /// the flush interval.
        /// Overrides `KIBBR__INGESTOR__ROW_GROUP_TIMEOUT_MS`. [default: 300000]
        #[arg(long, value_name = "MS")]
        row_group_timeout_ms: Option<usize>,
        /// A segment of the write-ahead log rolls to the next number at this many bytes.
        /// Overrides `KIBBR__STORAGE__WAL_SEGMENT_BYTES`. [default: 64 MiB]
        #[arg(long, value_name = "BYTES")]
        wal_segment_bytes: Option<usize>,
        /// Where one flush of the write-ahead log stops taking more bodies: once it holds
        /// this many lines. A body is never split, so a flush can hold more; and a flush
        /// waits for nothing - it takes whatever queued while the one before it ran.
        /// Overrides `KIBBR__INGESTOR__BATCH_SIZE`. [default: 500]
        #[arg(long, value_name = "LINES")]
        batch_size: Option<usize>,
        /// The most bytes of lines that may be in the write-ahead log and not yet in the
        /// store: what a stalled disk under the log, or a store slow to take rows, may
        /// cost in memory. A body past it waits for room, up to --ingest-wait-ms, and is
        /// then answered 503. Counted as records, forty bytes and the line for each.
        /// Overrides `KIBBR__STORAGE__WAL_QUEUE_BYTES`. [default: 256 MiB]
        #[arg(long, value_name = "BYTES")]
        wal_queue_bytes: Option<usize>,
        /// How many segment files may be open for writing at once, across every leaf.
        /// Past it the least recently written one is sealed, which is always safe - the
        /// next row for that partition opens the next segment number. Set it below
        /// `ulimit -n`: a cap above the descriptor limit is not a cap.
        /// Overrides `KIBBR__STORAGE__MAX_OPEN_SEGMENTS`.
        #[arg(long, value_name = "N")]
        max_open_segments: Option<usize>,
        /// A day's data file is sealed at this many bytes and the next one opened.
        /// Overrides `KIBBR__STORAGE__SEGMENT_MAX_BYTES`. [default: 512 MiB]
        #[arg(long, value_name = "BYTES")]
        segment_max_bytes: Option<usize>,
        /// Memory every buffer together may hold before the largest are closed early - an
        /// emergency brake - and at twice which `/ingest` keeps a body waiting at the door
        /// for the flush to catch up, and answers 503 if it has not within
        /// --ingest-wait-ms. Estimated per row, hash tables and allocations included,
        /// which is about eight times a row's keys and values.
        /// Overrides `KIBBR__STORAGE__MAX_BUFFER_BYTES`. [default: 512 MiB]
        #[arg(long, value_name = "BYTES")]
        max_buffer_bytes: Option<usize>,
        /// How often to look for a day that has ended and compact it, in milliseconds: a
        /// day is due the day after its last live row. `0` never looks, and `kibbr compact`
        /// still compacts on demand.
        /// Overrides `KIBBR__STORAGE__COMPACT_INTERVAL_MS`. [default: 60000]
        #[arg(long, value_name = "MS")]
        compact_interval_ms: Option<usize>,
        /// Threads one query may choose a day's rows on, its own among them. 0 or 1: its
        /// own alone. Defaults to half the cores, at most 4; more than the machine has
        /// cores is refused. The other threads are started by the first query with enough
        /// rows to share, and are left alone while any `/ingest` body is between the door
        /// and its answer, the wait for the log's flush included.
        /// Overrides `KIBBR__QUERY__SCAN_THREADS`.
        #[arg(long, visible_alias = "threads", value_name = "N")]
        scan_threads: Option<usize>,
        /// A run of a day's row groups holding fewer rows than this is never shared
        /// between those threads: waking them costs more than they would save.
        /// Overrides `KIBBR__QUERY__SCAN_FAN_ROWS`. [default: 32768]
        #[arg(long, value_name = "ROWS")]
        scan_fan_rows: Option<usize>,
        /// The key names a new template's time column is chosen from, in order; the
        /// first one the template holds as a Datetime wins, and a template holding none
        /// takes its rows' arrival time. Comma-separated.
        /// Overrides `KIBBR__SCHEMA__TIME_COLUMNS`.
        #[arg(long, value_name = "NAMES", value_delimiter = ',')]
        time_columns: Option<Vec<String>>,
    },
    /// Load closed days of logs into a data directory whole: NDJSON, gzip or zstd, from
    /// files, directories or standard input. The load is published in one step - every row
    /// or none - and the same inputs are never loaded twice. A server holding the directory
    /// is asked to run it; with none, this command takes the directory and runs it alone.
    Load {
        /// Files, directories - every file under them, names starting with `.` skipped - or
        /// `-` for standard input. Read in order of file name, whatever order given.
        #[arg(required = true, value_name = "INPUT")]
        inputs: Vec<PathBuf>,
        /// The data directory. Overrides `KIBBR__STORAGE__BASE_PATH`. [default: kibbr-data]
        #[arg(long, value_name = "PATH")]
        data_dir: Option<PathBuf>,
        /// Publish these rows as the whole of this UTC day, retiring every file it held, in
        /// the same step: how a day's source is changed, say from what arrived live to the
        /// archive's copy. Every row must be on that day. A load of inputs loaded before is
        /// allowed with this, since it replaces them.
        #[arg(long, value_name = "YYYY-MM-DD")]
        replace_day: Option<String>,
        /// Parse and encode at most this many megabytes of input a second, after
        /// decompression, over the whole load - so a load leaves the machine to the live
        /// door. The quick reads before it, for the load id and the arrival check, are not
        /// held to it; their time counts toward the rate, up to a second.
        #[arg(long, value_name = "MB")]
        max_mb_per_sec: Option<f64>,
        /// Threads to parse and encode on, at most one per core. [default: one per core up
        /// to 8 alone, half that inside a server]
        #[arg(long, value_name = "N")]
        threads: Option<usize>,
        /// Memory the load may hold rows in before it writes them - open row groups and
        /// closed ones not yet written, together. Past it the largest are written early, as
        /// smaller row groups, so a load inside a server cannot crowd the live door out of
        /// memory. [default: 64 MiB]
        #[arg(long, value_name = "BYTES")]
        load_buffer_bytes: Option<usize>,
        /// The arrival time given to rows with no time of their own. [default: now]
        #[arg(long, value_name = "RFC3339")]
        received_at: Option<String>,
        /// Load even when more than --max-arrival-share of the rows have no time of their
        /// own and would be stored at their arrival time - now, or --received-at.
        #[arg(long)]
        proceed: bool,
        /// The percentage of rows that may fall back to arrival time before the load
        /// stops, loading nothing: of every row, counted before the commit - and of the
        /// first 10,000, checked before anything is written, so a file with no time from
        /// its first line stops at once. [default: 1]
        #[arg(long, value_name = "PERCENT")]
        max_arrival_share: Option<f64>,
        /// As `serve --time-columns`: the key names a new template's time column is chosen
        /// from. Used only when no server holds the directory - a server uses its own.
        /// Overrides `KIBBR__SCHEMA__TIME_COLUMNS`.
        #[arg(long, value_name = "NAMES", value_delimiter = ',')]
        time_columns: Option<Vec<String>>,
        /// A data file is sealed at this many bytes and the next one opened, when no
        /// server holds the directory. Overrides `KIBBR__STORAGE__SEGMENT_MAX_BYTES`.
        /// [default: 512 MiB]
        #[arg(long, value_name = "BYTES")]
        segment_max_bytes: Option<usize>,
        /// Print the report as one JSON object instead of a line.
        #[arg(long)]
        json: bool,
    },
    /// Compact a day the live door has finished with: its many small row groups rebuilt
    /// into full ones, in finished files, published in one step. Nothing a query can see
    /// moves - not a row, not its position - and a day with nothing to compact is left
    /// untouched. A server holding the directory is asked to run it; with none, this command
    /// takes the directory and runs it alone - and then reads values back under this
    /// process's own settings, so give it the `KIBBR__SCHEMA__EPOCH_WINDOW` the server that
    /// wrote the day runs with, as for `kibbr load`.
    Compact {
        /// Compact this UTC day now, whatever its age. Without it, every day that is due:
        /// each day before today whose last live row was written before today.
        #[arg(long, value_name = "YYYY-MM-DD")]
        day: Option<String>,
        /// The data directory, which must be one already: a path that is not a kibbr data
        /// directory is refused, and nothing is created there.
        /// Overrides `KIBBR__STORAGE__BASE_PATH`. [default: kibbr-data]
        #[arg(long, value_name = "PATH")]
        data_dir: Option<PathBuf>,
        /// Threads to encode on, at most one per core. [default: one per core up to 8
        /// alone, half that inside a server]
        #[arg(long, value_name = "N")]
        threads: Option<usize>,
        /// A rebuilt file is sealed at this many bytes and the next one opened, when no
        /// server holds the directory - a server uses its own.
        /// Overrides `KIBBR__STORAGE__SEGMENT_MAX_BYTES`. [default: 512 MiB]
        #[arg(long, value_name = "BYTES")]
        segment_max_bytes: Option<usize>,
        /// Print the report as one JSON object instead of a line a day.
        #[arg(long)]
        json: bool,
    },
    /// Measure what a file costs to store: load it into a scratch data directory and print
    /// its size raw, under gzip -9 and zstd -19, and as kibbr holds it - data files,
    /// catalog and manifest - in bytes and in disk blocks. Then the same lines as a live
    /// day - through the live door, flushed 1,440 times over the file, or once a line if
    /// it has fewer - as it is before compaction and after.
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
    /// Measure how fast this build is on one farm day, and print the numbers a change to
    /// it is held to: ingest lines a second from four senders, the four standard queries
    /// on an idle server and under that ingest, scan rows per CPU-second, and the plan
    /// step at 10,000 templates. Each server measured is this binary, started on its own
    /// copy of one loaded directory and killed when its part is done; every number is the
    /// median over the runs. The result is saved with every sample behind it, and the raw
    /// log beside it. `--baseline` prints each number with its speed-up on a saved
    /// result; `--table` prints saved results side by side and measures nothing.
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
        /// Where the result is saved, with the raw log beside it under the same name
        /// ending `.log`. A file that exists is refused before anything runs: a saved
        /// result is not written over. So is a place no file can be made in; the
        /// result's directory is made then, if it is not there.
        /// [default: <dir>/results/<UTC time>-<commit>.json]
        #[arg(long, value_name = "FILE")]
        out: Option<PathBuf>,
        /// A saved result to compare with. It is read and held to be the same work - the
        /// same flags, loaded bytes, queries, answers, server command line and machine -
        /// before anything is measured; each number is then printed with its speed-up on
        /// it, above 1 when this build is faster.
        #[arg(long, value_name = "FILE")]
        baseline: Option<PathBuf>,
        /// The heading of this result's column, saved in the file.
        /// [default: baseline, or `this run` with --baseline]
        #[arg(long, value_name = "TEXT")]
        label: Option<String>,
        /// Holds the loaded directory - built once, and never opened by a server - each
        /// server's copy of it, and the default results. One volume, so that a copy can
        /// be a clone.
        #[arg(long, value_name = "PATH", default_value = bench::DEFAULT_DIR)]
        dir: PathBuf,
        /// How many times the whole sequence is run; each printed number is the middle
        /// one. Odd. Two results compare only at the same value.
        #[arg(long, value_name = "R", default_value_t = bench::DEFAULT_RUNS, value_parser = odd)]
        runs: usize,
        /// Timed asks of each query on the idle server, servers each query is asked on
        /// once under ingest, and timed plans of each shape; the scan rate is read off a
        /// quarter as many asks. Under 100, the p99 is of that many samples and the table
        /// says so. Two results compare only at the same value.
        #[arg(
            long,
            value_name = "K",
            default_value_t = bench::DEFAULT_SAMPLES,
            value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..)
        )]
        samples: usize,
        /// Lines in one `/ingest` body. A body is one flush of the write-ahead log, so
        /// this is part of what the ingest number means. Two results compare only at the
        /// same value.
        #[arg(
            long,
            value_name = "L",
            default_value_t = bench::DEFAULT_BODY_LINES,
            value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..)
        )]
        body_lines: usize,
        /// Scan threads of every server measured: each is started with `--scan-threads
        /// T`. Without it no server is told, and each takes this build's default (half
        /// the cores, at most 4); no `KIBBR__` variable reaches one either way. More
        /// than the machine has cores is refused. Two results compare only at the same
        /// value, and one taken without the flag only with another taken without it.
        #[arg(
            long,
            value_name = "T",
            value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..)
        )]
        threads: Option<usize>,
        /// Skip what makes a run a measurement - the pause before each run, the waits for
        /// a machine with no other kibbr process and no swapping, the look after each
        /// server that throws a disturbed one's numbers away - and allow a debug build.
        /// The result is marked `UNCHECKED - not a measurement`. For tests, which run
        /// beside other tests' servers.
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
        /// The server to ask, `host:port`. [default: the server holding --data-dir, by
        /// its LOCK file; with none, `KIBBR__API__NATIVE_ADDR`, `[api] native_addr`,
        /// 127.0.0.1:7700]
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
        /// The server to ask, `host:port`. [default: the server holding --data-dir, by
        /// its LOCK file; with none, `KIBBR__API__NATIVE_ADDR`, `[api] native_addr`,
        /// 127.0.0.1:7700]
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
    /// Ask a running server whether it is well: `GET /health`, asked for you. Prints
    /// `ok`, or `degraded` and why, and exits 0; exits 1 when no server answers or the
    /// answer is not `"ok":true`. What the image's HEALTHCHECK runs - a degraded server
    /// cannot write and still answers queries, so it is not one to restart.
    Health {
        /// The server to ask, `host:port`. [default: the server holding --data-dir, by
        /// its LOCK file; with none, `KIBBR__API__NATIVE_ADDR`, `[api] native_addr`,
        /// 127.0.0.1:7700]
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
        /// Groups a bucketed `GROUP BY` may emit before the query is refused - the bucket
        /// count times the distinct values of the other keys.
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

        /// Return at most this many rows, newest first. TrieQL spells it `LIMIT n`, and
        /// the two are exclusive for the reason `--since` and `SINCE` are. Omitted, every
        /// matching row comes back; `--limit 0` is a request for no rows, and the two are
        /// not the same thing.
        #[arg(long)]
        limit: Option<usize>,

        /// Resume after a previous answer's `next_cursor`. TrieQL spells it `AFTER "…"`.
        /// Only valid for the query that produced it.
        #[arg(long, value_name = "CURSOR")]
        after: Option<String>,

        /// Rows a query that names no `LIMIT` is cut at.
        /// `0` disables it. Overrides `KIBBR__QUERY__MAX_ROWS`. Not exclusive with a
        /// TrieQL string, unlike `--limit`: the grammar cannot state it, and it is a
        /// property of the server rather than of the query.
        #[arg(long, value_name = "ROWS")]
        max_rows: Option<usize>,
        /// Buckets a `GROUP BY time(...)` may emit before the query is refused.
        /// `0` disables it. Overrides `KIBBR__QUERY__MAX_BUCKETS`.
        #[arg(long, value_name = "BUCKETS")]
        max_buckets: Option<usize>,
        /// Groups a bucketed `GROUP BY` may emit before the query is refused - the bucket
        /// count times the distinct values of the other keys.
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
