//! TrieDB. Generate hostile logs, parse them, build the trie, query it.

mod bench;
mod catalog;
mod config;
mod db;
mod demo;
mod engine;
mod error;
mod farm;
mod flatten;
mod fmt;
mod frame;
mod gen;
mod http;
mod infer;
mod key;
mod lines;
mod load;
mod lock;
mod parse;
mod query;
mod reference;
mod rng;
mod serve;
mod store;
mod trie;
mod triefile;
mod trieql;
mod types;

use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use error::CliError;

/// mimalloc in place of the system allocator. On macOS arm64 the server aborted inside
/// libmalloc - "pointer being freed was not allocated" - at three unrelated free sites, with
/// no `unsafe` in src/ and a clean AddressSanitizer run, which left the allocator itself as
/// the suspect. This takes it out of the picture.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser)]
#[command(name = "triedb", version, about = "TrieDB - log store", long_about = None)]
struct Cli {
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
        /// split each shape into several fingerprints, so `triedb parse` reports more
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
        /// defaulted into.
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        #[arg(long, default_value_t = serve::DEFAULT_PORT)]
        port: u16,
        /// Rows a query that names no `LIMIT` is cut at.
        /// `0` disables it. Overrides `TRIEDB__QUERY__MAX_ROWS`.
        #[arg(long, value_name = "ROWS")]
        max_rows: Option<usize>,
        /// Buckets a `GROUP BY time(...)` may emit before the query is refused.
        /// `0` disables it. Overrides `TRIEDB__QUERY__MAX_BUCKETS`.
        #[arg(long, value_name = "BUCKETS")]
        max_buckets: Option<usize>,
        /// Groups a bucketed `GROUP BY` may emit before the query is refused - the bucket
        /// count times the distinct values of the other keys.
        /// `0` disables it. Overrides `TRIEDB__QUERY__MAX_GROUPS`.
        #[arg(long, value_name = "GROUPS")]
        max_groups: Option<usize>,

        // The three below are properties of the server rather than of a query, so they
        // are here and not on `query`/`explain`: a file has no wire, no encoding and no
        // second caller competing for the machine.
        /// The largest `/ingest` body accepted, measured on the wire *before* any
        /// decompression. Overrides `TRIEDB__API__MAX_BODY_BYTES`.
        #[arg(long, value_name = "BYTES")]
        max_body_bytes: Option<usize>,
        /// The largest a gzipped `/ingest` body may become once decompressed. Cannot
        /// exceed the 64 MiB line limit. Overrides `TRIEDB__API__MAX_DECOMPRESSED_BYTES`.
        #[arg(long, value_name = "BYTES")]
        max_decompressed_bytes: Option<usize>,
        /// How many `/ingest` bodies may be parsed at once. Beyond it, a body waits
        /// briefly and is then answered `503` with a `Retry-After` rather than left to
        /// time out. Overrides `TRIEDB__API__INGEST_CONCURRENCY`.
        #[arg(long, value_name = "N")]
        ingest_concurrency: Option<usize>,
        /// Where rows go. `disk` writes day-partitioned column files and snapshots the
        /// trie on a clean shutdown; `memory` writes nothing, and is what the
        /// memory-versus-disk differential runs against.
        #[arg(long, value_name = "KIND", default_value = "disk")]
        storage: StorageKind,
        /// The data directory. Created if absent. Overrides `TRIEDB__STORAGE__BASE_PATH`.
        #[arg(long, value_name = "PATH")]
        data_dir: Option<PathBuf>,
        /// The trie snapshot. Defaults to `trie.json` inside the data directory.
        /// Overrides `TRIEDB__STORAGE__TRIE_FILE`.
        #[arg(long, value_name = "PATH")]
        trie_file: Option<PathBuf>,
        /// How often the row group thresholds are checked, in milliseconds. This is not
        /// when a row reaches the disk - see --row-group-timeout-ms - and not when it
        /// becomes queryable, which is at once.
        /// Overrides `TRIEDB__INGESTOR__FLUSH_INTERVAL_MS`.
        #[arg(long, value_name = "MS")]
        flush_interval_ms: Option<usize>,
        /// How long a row group may stay open with rows in it, in milliseconds, when
        /// neither 8,192 rows nor 8MB got there first. Until there is a write-ahead log
        /// this is also what a kill -9 costs. Floored at the flush interval.
        /// Overrides `TRIEDB__INGESTOR__ROW_GROUP_TIMEOUT_MS`.
        #[arg(long, value_name = "MS")]
        row_group_timeout_ms: Option<usize>,
        /// How many segment files may be open for writing at once, across every leaf.
        /// Past it the least recently written one is sealed, which is always safe - the
        /// next row for that partition opens the next segment number. Set it below
        /// `ulimit -n`: a cap above the descriptor limit is not a cap.
        /// Overrides `TRIEDB__STORAGE__MAX_OPEN_SEGMENTS`.
        #[arg(long, value_name = "N")]
        max_open_segments: Option<usize>,
        /// A day's data file is sealed at this many bytes and the next one opened.
        /// Overrides `TRIEDB__STORAGE__SEGMENT_MAX_BYTES`. [default: 512 MiB]
        #[arg(long, value_name = "BYTES")]
        segment_max_bytes: Option<usize>,
        /// Memory every buffer together may hold before the largest are closed early - an
        /// emergency brake - and at twice which `/ingest` answers 503 until the flush
        /// catches up. Estimated per row, hash tables and allocations included, which is
        /// about eight times a row's keys and values.
        /// Overrides `TRIEDB__STORAGE__MAX_BUFFER_BYTES`. [default: 512 MiB]
        #[arg(long, value_name = "BYTES")]
        max_buffer_bytes: Option<usize>,
        /// The key names a new template's time column is chosen from, in order; the
        /// first one the template holds as a Datetime wins, and a template holding none
        /// takes its rows' arrival time. Comma-separated.
        /// Overrides `TRIEDB__SCHEMA__TIME_COLUMNS`.
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
        /// The data directory. Overrides `TRIEDB__STORAGE__BASE_PATH`. [default: triedb-data]
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
        /// Overrides `TRIEDB__SCHEMA__TIME_COLUMNS`.
        #[arg(long, value_name = "NAMES", value_delimiter = ',')]
        time_columns: Option<Vec<String>>,
        /// A data file is sealed at this many bytes and the next one opened, when no
        /// server holds the directory. Overrides `TRIEDB__STORAGE__SEGMENT_MAX_BYTES`.
        /// [default: 512 MiB]
        #[arg(long, value_name = "BYTES")]
        segment_max_bytes: Option<usize>,
        /// Print the report as one JSON object instead of a line.
        #[arg(long)]
        json: bool,
    },
    /// Measure what a file costs to store: load it into a scratch data directory and print
    /// its size raw, under gzip -9 and zstd -19, and as TrieDB holds it - data files,
    /// catalog and manifest - in bytes and in disk blocks.
    BenchStorage {
        /// NDJSON, gzip or zstd.
        #[arg(value_name = "FILE")]
        input: PathBuf,
        /// Also index the same lines into this Elasticsearch - a scratch index, deleted
        /// after - and print its store size beside TrieDB's.
        #[arg(long, value_name = "URL")]
        es: Option<String>,
        /// Where to load it. Must be absent or empty. [default: a directory under the
        /// system's temporary directory]
        #[arg(long, value_name = "PATH")]
        scratch: Option<PathBuf>,
        /// Leave the scratch directory in place, to look at.
        #[arg(long)]
        keep: bool,
        /// As `load --time-columns`. Overrides `TRIEDB__SCHEMA__TIME_COLUMNS`.
        #[arg(long, value_name = "NAMES", value_delimiter = ',')]
        time_columns: Option<Vec<String>>,
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
        /// `0` disables it. Overrides `TRIEDB__QUERY__MAX_BUCKETS`.
        #[arg(long, value_name = "BUCKETS")]
        max_buckets: Option<usize>,
        /// Groups a bucketed `GROUP BY` may emit before the query is refused - the bucket
        /// count times the distinct values of the other keys.
        /// `0` disables it. Overrides `TRIEDB__QUERY__MAX_GROUPS`.
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

        // The four verbs are mutually exclusive. Spelled out rather than put in an
        // `ArgGroup`, which rejects the pair but reports it as one flag repeated -
        // clap names a group's first member, so `--count --fetch` came back as
        // "--count cannot be used multiple times".
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
        /// `0` disables it. Overrides `TRIEDB__QUERY__MAX_ROWS`. Not exclusive with a
        /// TrieQL string, unlike `--limit`: the grammar cannot state it, and it is a
        /// property of the server rather than of the query.
        #[arg(long, value_name = "ROWS")]
        max_rows: Option<usize>,
        /// Buckets a `GROUP BY time(...)` may emit before the query is refused.
        /// `0` disables it. Overrides `TRIEDB__QUERY__MAX_BUCKETS`.
        #[arg(long, value_name = "BUCKETS")]
        max_buckets: Option<usize>,
        /// Groups a bucketed `GROUP BY` may emit before the query is refused - the bucket
        /// count times the distinct values of the other keys.
        /// `0` disables it. Overrides `TRIEDB__QUERY__MAX_GROUPS`.
        #[arg(long, value_name = "GROUPS")]
        max_groups: Option<usize>,
        /// As `serve --time-columns`: the names a template's time column is chosen from.
        /// Overrides `TRIEDB__SCHEMA__TIME_COLUMNS`.
        #[arg(long, value_name = "NAMES", value_delimiter = ',')]
        time_columns: Option<Vec<String>>,
    },
}

/// Spelled out as a flag rather than inferred from whether `--data-dir` was given, so
/// that `--storage memory --data-dir /x` reads as an instruction and not as a
/// contradiction settled by whichever the code looked at first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum StorageKind {
    Disk,
    Memory,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error::report(&e);
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<(), CliError> {
    // Before any command types a value, and for every command alike: a bad window stops
    // the process here rather than after it has typed lines under the default.
    infer::set_epoch_window(config::epoch_window()?);

    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());

    let result = match cli.command {
        Command::GenLogs {
            profile,
            lines,
            templates,
            seed,
            day,
            services,
            dirty_rate,
        } => gen_logs(
            &mut out, profile, lines, templates, seed, day, services, dirty_rate,
        )
        .map_err(CliError::from),
        Command::Parse { input, limit, json } => {
            let reader = open(&input)?;
            parse::run(reader, &mut out, limit, json)
        }
        Command::Demo { input, full } => {
            let reader = open(&input)?;
            demo::run(reader, &mut out, full)
        }
        // Writes nothing to `out`: the banner and the lifecycle log are stderr (see
        // `serve::banner`), and everything else this command produces leaves over HTTP.
        Command::Serve {
            host,
            port,
            max_rows,
            max_buckets,
            max_groups,
            max_body_bytes,
            max_decompressed_bytes,
            ingest_concurrency,
            storage,
            data_dir,
            trie_file,
            flush_interval_ms,
            row_group_timeout_ms,
            max_open_segments,
            segment_max_bytes,
            max_buffer_bytes,
            time_columns,
        } => serve::run(
            &host,
            port,
            config::Limits::load(max_rows, max_buckets, max_groups)?,
            config::ApiLimits::load(max_body_bytes, max_decompressed_bytes, ingest_concurrency)?,
            config::Storage::load(
                storage == StorageKind::Disk,
                data_dir,
                trie_file,
                flush_interval_ms,
                row_group_timeout_ms,
                max_open_segments,
                segment_max_bytes,
                max_buffer_bytes,
            )?,
            config::Schema::load(time_columns),
        ),
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
        } => {
            let storage = config::Storage::load(
                true,
                data_dir,
                None,
                None,
                None,
                None,
                segment_max_bytes,
                None,
            )?;
            let schema = config::Schema::load(time_columns.clone());
            let args = load::Args {
                inputs,
                replace_day,
                max_mb_per_sec,
                threads,
                load_buffer_bytes,
                received_at,
                proceed,
                max_arrival_share,
                time_columns,
                json,
            };
            load::command(args, storage, schema, &mut out).map_err(CliError::from)
        }
        Command::BenchStorage {
            input,
            es,
            scratch,
            keep,
            time_columns,
        } => bench::run(
            &bench::Args {
                input,
                es,
                scratch,
                keep,
            },
            config::Schema::load(time_columns),
            &mut out,
        ),
        Command::Explain {
            query,
            max_rows,
            max_buckets,
            max_groups,
        } => query::describe(
            &mut out,
            &query,
            config::Limits::load(max_rows, max_buckets, max_groups)?,
        ),
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
        } => {
            let limits = config::Limits::load(max_rows, max_buckets, max_groups)?;
            let reader = open(&input)?;
            let args = query::Args {
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
                limits,
                time_columns,
            };
            query::run(reader, &mut out, &args)
        }
    };

    // A closed downstream pipe (`| head`) is how the command ends, not a failure.
    match result {
        Err(e) if is_broken_pipe(&e) => return Ok(()),
        other => other?,
    }
    match out.flush() {
        Err(e) if e.kind() == io::ErrorKind::BrokenPipe => Ok(()),
        Err(e) => Err(CliError::Write(e)),
        Ok(()) => Ok(()),
    }
}

#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum Profile {
    Hostile,
    Farm,
}

/// A flag that belongs to the other profile is refused rather than ignored: a `--day` that
/// silently did nothing would leave its caller measuring the wrong file.
#[allow(clippy::too_many_arguments)]
fn gen_logs<W: Write>(
    out: &mut W,
    profile: Profile,
    lines: u64,
    templates: Option<usize>,
    seed: u64,
    day: Option<String>,
    services: Option<usize>,
    dirty_rate: Option<f64>,
) -> Result<(), error::GenError> {
    use error::GenError;
    match profile {
        Profile::Hostile => {
            for (given, flag) in [
                (day.is_some(), "--day"),
                (services.is_some(), "--services"),
                (dirty_rate.is_some(), "--dirty-rate"),
            ] {
                if given {
                    return Err(GenError::WrongProfile {
                        flag,
                        profile: "farm",
                    });
                }
            }
            gen::generate(out, lines, templates.unwrap_or(12), seed)
        }
        Profile::Farm => {
            if templates.is_some() {
                return Err(GenError::WrongProfile {
                    flag: "--templates",
                    profile: "hostile",
                });
            }
            if lines > farm::MAX_LINES {
                return Err(GenError::TooManyLines(farm::MAX_LINES));
            }
            // Ten characters, 1970 to 9999: every time form the farm writes - RFC 3339 with
            // a four-digit year, epoch seconds and milliseconds - is valid on that range
            // and on no wider one.
            let day_text = day.unwrap_or_else(|| farm::DEFAULT_DAY.to_string());
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("a valid date");
            let last = chrono::NaiveDate::from_ymd_opt(9999, 12, 31).expect("a valid date");
            let day = chrono::NaiveDate::parse_from_str(&day_text, "%Y-%m-%d")
                .ok()
                .filter(|d| day_text.len() == 10 && (epoch..=last).contains(d))
                .ok_or_else(|| GenError::BadDay(day_text.clone()))?;
            let services = services.unwrap_or(farm::MAX_SERVICES);
            if !(1..=farm::MAX_SERVICES).contains(&services) {
                return Err(GenError::BadServices(services));
            }
            let rate = dirty_rate.unwrap_or(0.5);
            if !(0.0..=100.0).contains(&rate) {
                return Err(GenError::BadDirtyRate(rate));
            }
            let opts = farm::Options {
                lines,
                seed,
                day: (day - epoch).num_days(),
                services,
                // Parts per million: the rate is read once, and every draw after it is an
                // integer comparison.
                dirty_ppm: (rate * 10_000.0).round() as u32,
            };
            farm::generate(out, &opts)
        }
    }
}

fn open(input: &str) -> Result<Box<dyn BufRead>, CliError> {
    if input == "-" {
        return Ok(Box::new(BufReader::new(io::stdin())));
    }
    let file = File::open(input).map_err(|source| CliError::Read {
        path: input.to_string(),
        source,
    })?;
    Ok(Box::new(BufReader::new(file)))
}

fn is_broken_pipe(e: &CliError) -> bool {
    let io_error = match e {
        CliError::Write(inner) => Some(inner),
        CliError::Read { source, .. } => Some(source),
        CliError::Gen(error::GenError::Write(inner)) => Some(inner),
        _ => None,
    };
    io_error.is_some_and(|inner| inner.kind() == io::ErrorKind::BrokenPipe)
}
