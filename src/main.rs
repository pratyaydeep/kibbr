//! TrieDB. Generate hostile logs, parse them, build the trie, query it.

mod config;
mod db;
mod demo;
mod engine;
mod error;
mod flatten;
mod fmt;
mod gen;
mod infer;
mod key;
mod lines;
mod parse;
mod query;
mod reference;
mod rng;
mod serve;
mod store;
mod trie;
mod trieql;
mod types;

use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use error::CliError;

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
        /// How many NDJSON lines to write.
        #[arg(long, default_value_t = 1000)]
        lines: u64,
        /// How many distinct log shapes to draw from. Optional fields split each
        /// shape into several fingerprints, so `triedb parse` reports more
        /// templates than this.
        #[arg(long, default_value_t = 12)]
        templates: usize,
        /// Same seed, byte-identical output. Nothing here reads the wall clock.
        #[arg(long, default_value_t = 42)]
        seed: u64,
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
    /// Serve the native HTTP API. Storage is in memory: everything ingested is
    /// lost when the process exits, which the startup banner says in as many words.
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
    },
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
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());

    let result = match cli.command {
        Command::GenLogs {
            lines,
            templates,
            seed,
        } => gen::generate(&mut out, lines, templates, seed).map_err(CliError::from),
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
        } => serve::run(
            &host,
            port,
            config::Config::load(max_rows, max_buckets, max_groups)?.query,
        ),
        Command::Explain {
            query,
            max_rows,
            max_buckets,
            max_groups,
        } => query::describe(
            &mut out,
            &query,
            config::Config::load(max_rows, max_buckets, max_groups)?.query,
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
        } => {
            let config = config::Config::load(max_rows, max_buckets, max_groups)?;
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
                limits: config.query,
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
