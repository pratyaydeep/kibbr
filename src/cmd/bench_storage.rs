// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! `kibbr bench-storage <file>`: what a file costs to store, by one command, so every
//! storage number is measured the same way. The file goes through the bulk door into a
//! scratch directory and is printed beside the raw bytes, `gzip -9`, `zstd -19` and, with
//! `--es`, Elasticsearch. The same lines then go through the live door as a day flushed once
//! a minute, measured before and after compaction: the bulk door alone cannot show that.

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::config::{LoadStorage as _, Schema, Storage};
use crate::error::{CliError, DetectorErrorKind};
use crate::cmd::load::{self, Asked, LoadError};
use crate::core::fmt::commas;
use crate::storage::db::{self, ReadError, SinkError};
use crate::storage::store::disk::{self, CompactOptions};
use crate::storage::store::v3::{HEADER_LEN, KIND_FOOTER, KIND_ROW_GROUP, KIND_SCHEMA, TRAILER_LEN};
use crate::storage::store::{FlushKind, FlushSummary};

pub struct Args {
    pub input: PathBuf,
    pub es: Option<String>,
    pub scratch: Option<PathBuf>,
    pub keep: bool,
    /// The settings file, for what a scratch directory's storage is opened with.
    pub settings: crate::config::File,
}

/// One file's size, and what it takes on disk.
#[derive(Debug, Default, Clone, Copy)]
struct Size {
    bytes: u64,
    /// Whole blocks, as the file system allocated them.
    blocks: u64,
}

impl std::ops::AddAssign for Size {
    fn add_assign(&mut self, other: Self) {
        self.bytes += other.bytes;
        self.blocks += other.blocks;
    }
}

/// The unit `st_blocks` counts in, whatever the file system's block size.
#[cfg(unix)]
const STAT_BLOCK: u64 = 512;

fn size_of(path: &Path, block: u64) -> io::Result<Size> {
    let meta = fs::metadata(path)?;
    #[cfg(unix)]
    let allocated = {
        use std::os::unix::fs::MetadataExt;
        meta.blocks() * STAT_BLOCK
    };
    #[cfg(not(unix))]
    let allocated = meta.len().div_ceil(block) * block;
    Ok(Size {
        bytes: meta.len(),
        blocks: allocated.div_ceil(block),
    })
}

/// Where a data file's bytes go.
#[derive(Debug, Default, Clone, Copy)]
struct Parts {
    columns: u64,
    row_group_headers: u64,
    schemas: u64,
    footers: u64,
    file_headers: u64,
    /// Optional frames: none is written today, and a reader steps over them.
    other: u64,
}

/// Walk a sealed data file's frames from its header to its trailer, and sort its bytes.
fn parts_of(path: &Path) -> Result<Parts, LoadError> {
    let bytes = fs::read(path).map_err(|source| LoadError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let corrupt = |at: usize, what: &str| {
        LoadError::Input(format!("{}: offset {at}: {what}", path.display()))
    };
    let mut parts = Parts {
        file_headers: HEADER_LEN as u64,
        ..Parts::default()
    };
    let mut at = HEADER_LEN;
    loop {
        match crate::storage::frame::read(&bytes, at, u32::MAX) {
            crate::storage::frame::Next::Frame { payload, end } => {
                let frame = (end - at) as u64;
                match payload[0] {
                    KIND_SCHEMA => parts.schemas += frame,
                    KIND_ROW_GROUP => {
                        // The fields before the chunk area's length.
                        const CHUNKS_LEN_AT: usize = 1 + 8 + 4 + 4 + 8 + 8 + 1 + 8 + 8 + 8;
                        let len = payload
                            .get(CHUNKS_LEN_AT..CHUNKS_LEN_AT + 4)
                            .ok_or_else(|| corrupt(at, "a row group header cut short"))?;
                        let chunks =
                            u64::from(u32::from_le_bytes([len[0], len[1], len[2], len[3]]));
                        parts.row_group_headers += frame;
                        parts.columns += chunks;
                        at = end + chunks as usize;
                        continue;
                    }
                    KIND_FOOTER => {
                        parts.footers += frame + TRAILER_LEN as u64;
                        return Ok(parts);
                    }
                    _ => parts.other += frame,
                }
                at = end;
            }
            _ => return Err(corrupt(at, "not a sealed data file")),
        }
    }
}

/// What a data directory takes: its data files, sorted into their parts, and its two logs.
struct Held {
    files: Size,
    parts: Parts,
    catalog: Size,
    manifest: Size,
    total: Size,
}

/// Measure a data directory no process has open. Only once its stores are dropped: a file
/// a compaction replaced is unlinked when its last reader lets go, and would count twice.
fn held(data: &Path, block: u64) -> Result<Held, LoadError> {
    let io_at = |path: &Path| {
        let path = path.to_path_buf();
        move |source| LoadError::Read { path, source }
    };
    let mut files = Size::default();
    let mut parts = Parts::default();
    for path in disk::data_files(data) {
        files += size_of(&path, block).map_err(io_at(&path))?;
        let p = parts_of(&path)?;
        parts.columns += p.columns;
        parts.row_group_headers += p.row_group_headers;
        parts.schemas += p.schemas;
        parts.footers += p.footers;
        parts.file_headers += p.file_headers;
        parts.other += p.other;
    }
    let catalog_path = crate::storage::catalog::path_in(data);
    let manifest_path = crate::storage::store::manifest::path_in(data);
    let catalog = size_of(&catalog_path, block).map_err(io_at(&catalog_path))?;
    let manifest = size_of(&manifest_path, block).map_err(io_at(&manifest_path))?;
    let mut total = files;
    total += catalog;
    total += manifest;
    Ok(Held {
        files,
        parts,
        catalog,
        manifest,
        total,
    })
}

impl Held {
    /// The table's rows for this directory, its total under `name`.
    fn rows(&self, name: &str, row: &dyn Fn(&str, Option<Size>, u64) -> String) -> Vec<String> {
        let parts = &self.parts;
        let mut rows = vec![
            row(name, Some(self.total), self.total.bytes),
            row("  data files", Some(self.files), self.files.bytes),
            row("    column data", None, parts.columns),
            row("    row group headers", None, parts.row_group_headers),
            row("    schema records", None, parts.schemas),
            row("    footers", None, parts.footers),
            row("    file headers", None, parts.file_headers),
        ];
        if parts.other > 0 {
            rows.push(row("    optional frames", None, parts.other));
        }
        rows.push(row("  catalog.log", Some(self.catalog), self.catalog.bytes));
        rows.push(row(
            "  manifest.log",
            Some(self.manifest),
            self.manifest.bytes,
        ));
        rows
    }
}

/// A data directory's files, counted from their footers.
#[derive(Debug, Default)]
struct Counts {
    files: u64,
    /// Files written whole: every one of them, once each day is compacted.
    finished: u64,
    row_groups: u64,
    rows: u64,
    /// The day directories that hold a file, by name.
    days: BTreeSet<String>,
}

fn counts_of(data: &Path) -> Result<Counts, LoadError> {
    let mut counts = Counts::default();
    for path in disk::data_files(data) {
        let index = crate::storage::store::v3::file::open_sealed(&path)?.ok_or_else(|| {
            LoadError::Input(format!("{}: not a sealed data file", path.display()))
        })?;
        counts.files += 1;
        counts.finished += u64::from(index.finished);
        counts.row_groups += index.entries.len() as u64;
        counts.rows += index.row_count();
        if let Some(day) = path.parent().and_then(Path::file_name) {
            counts.days.insert(day.to_string_lossy().into_owned());
        }
    }
    Ok(counts)
}

/// `part` as a share of `whole`, to three places.
fn share(part: u64, whole: u64) -> String {
    if whole == 0 {
        "-".to_string()
    } else {
        format!("{:.3}", part as f64 / whole as f64)
    }
}

/// A compressor's output, written where its blocks can be measured.
fn compressed(
    input: &Path,
    to: &Path,
    compress: impl FnOnce(&mut dyn Read, File) -> io::Result<()>,
) -> Result<(), LoadError> {
    let mut reader = load::open_decoded(input)?;
    let file = File::create(to).map_err(|source| LoadError::Read {
        path: to.to_path_buf(),
        source,
    })?;
    compress(&mut reader, file).map_err(|source| LoadError::Read {
        path: to.to_path_buf(),
        source,
    })
}

pub fn run(args: &Args, schema: Schema, out: &mut dyn Write) -> Result<(), CliError> {
    run_inner(args, schema, out).map_err(CliError::from)
}

fn run_inner(args: &Args, schema: Schema, out: &mut dyn Write) -> Result<(), LoadError> {
    let (scratch, ours) = match &args.scratch {
        Some(dir) => {
            if fs::read_dir(dir).is_ok_and(|mut d| d.next().is_some()) {
                return Err(LoadError::Input(format!(
                    "--scratch {}: not empty; bench-storage loads into a directory of its own",
                    dir.display()
                )));
            }
            (dir.clone(), false)
        }
        None => {
            let dir = std::env::temp_dir().join(format!("kibbr-bench-{}", std::process::id()));
            let _ = fs::remove_dir_all(&dir);
            (dir, true)
        }
    };
    let result = measure(args, schema, &scratch, out);
    if !args.keep {
        let _ = fs::remove_dir_all(&scratch);
    } else if ours {
        tracing::info!("kept {}", scratch.display());
    }
    result
}

fn measure(
    args: &Args,
    schema: Schema,
    scratch: &Path,
    out: &mut dyn Write,
) -> Result<(), LoadError> {
    let data = scratch.join("data");
    fs::create_dir_all(&data).map_err(|source| LoadError::Read {
        path: data.clone(),
        source,
    })?;
    let block = {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            fs::metadata(&data).map_or(4096, |m| m.blksize().max(512))
        }
        #[cfg(not(unix))]
        {
            4096
        }
    };

    // ── kibbr: the file through the bulk door, alone ──
    let storage = Storage::load(
        &args.settings,
        true,
        Some(data.clone()),
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .map_err(CliError::from)?;
    let lock = crate::storage::lock::DataLock::acquire(&data)?;
    let (trie, stores) =
        crate::server::open_durable(&storage, &schema).map_err(crate::error::CliError::from)?;
    let opts = load::Options {
        inputs: load::resolve(std::slice::from_ref(&args.input), None)?,
        replace_day: None,
        max_bytes_per_sec: None,
        threads: load::alone_threads(),
        load_buffer_bytes: load::DEFAULT_LOAD_BUFFER_BYTES,
        received_at: chrono::Utc::now(),
        // What it costs to store, not whether its times are right: a file whose rows take
        // their arrival time is stored all the same.
        proceed: true,
        max_arrival_share: load::DEFAULT_MAX_ARRIVAL_SHARE,
    };
    let report = load::run(&trie, &stores, &opts, &load::Control::default(), &mut |e| {
        crate::error::report(e)
    })?;
    drop(stores);
    drop(trie);
    drop(lock);

    let bulk = held(&data, block)?;
    let io_at = |path: &Path| {
        let path = path.to_path_buf();
        move |source| LoadError::Read { path, source }
    };

    // ── The raw bytes, and the two compressors ──
    let raw_copy = scratch.join("raw.ndjson");
    let raw = {
        let plain = {
            let mut head = [0u8; 4];
            let n = File::open(&args.input)
                .and_then(|mut f| f.read(&mut head))
                .map_err(io_at(&args.input))?;
            !(head[..n].starts_with(&[0x1f, 0x8b])
                || head[..n].starts_with(&[0x28, 0xb5, 0x2f, 0xfd]))
        };
        if plain {
            size_of(&args.input, block).map_err(io_at(&args.input))?
        } else {
            compressed(&args.input, &raw_copy, |r, mut f| {
                io::copy(r, &mut f)?;
                f.sync_all()
            })?;
            size_of(&raw_copy, block).map_err(io_at(&raw_copy))?
        }
    };
    let gz_path = scratch.join("raw.ndjson.gz");
    let zst_path = scratch.join("raw.ndjson.zst");
    // Side by side: zstd -19 is the slow one, and the two read the input independently.
    let (gz, zst) = std::thread::scope(|scope| {
        let gz = scope.spawn(|| {
            compressed(&args.input, &gz_path, |r, f| {
                let mut enc = flate2::write::GzEncoder::new(f, flate2::Compression::new(9));
                io::copy(r, &mut enc)?;
                enc.finish()?.sync_all()
            })
        });
        let zst = scope.spawn(|| {
            compressed(&args.input, &zst_path, |r, f| {
                let mut enc = zstd::stream::write::Encoder::new(f, 19)?;
                io::copy(r, &mut enc)?;
                enc.finish()?.sync_all()
            })
        });
        (gz.join(), zst.join())
    });
    let joined = |r: std::thread::Result<Result<(), LoadError>>| {
        r.unwrap_or_else(|_| Err(LoadError::Input("a compressor panicked".to_string())))
    };
    joined(gz)?;
    joined(zst)?;
    let gz = size_of(&gz_path, block).map_err(io_at(&gz_path))?;
    let zst = size_of(&zst_path, block).map_err(io_at(&zst_path))?;

    let es = match &args.es {
        Some(url) => Some(elasticsearch(url, &args.input)?),
        None => None,
    };

    // ── The table ──
    let ratio = |bytes: u64| share(bytes, raw.bytes);
    let heading = format!(
        "  {:<24}{:>14}{:>14}{:>9}",
        "", "bytes", "disk blocks", "x raw"
    );
    let row = |name: &str, size: Option<Size>, bytes: u64| {
        format!(
            "  {:<24}{:>14}{:>14}{:>9}",
            name,
            commas(bytes),
            size.map_or_else(String::new, |s| commas(s.blocks)),
            ratio(bytes)
        )
    };
    let mut lines = vec![
        format!("bench-storage {}", args.input.display()),
        format!(
            "  {} lines, {} rows as load {}: {} row groups in {} file(s) over {} day(s), \
             loaded in {:.1}s",
            commas(report.lines),
            commas(report.rows),
            report.load_id,
            commas(report.row_groups),
            report.files.len(),
            report.days,
            report.seconds
        ),
        String::new(),
        heading.clone(),
        row("raw", Some(raw), raw.bytes),
        row("gzip -9", Some(gz), gz.bytes),
        row("zstd -19", Some(zst), zst.bytes),
    ];
    lines.extend(bulk.rows("kibbr total", &row));
    if let Some((bytes, rejected)) = es {
        lines.push(row("Elasticsearch store", None, bytes));
        if rejected > 0 {
            lines.push(format!(
                "  ({} line(s) Elasticsearch refused are not in its number)",
                commas(rejected)
            ));
        }
    }
    lines.push(String::new());
    // How far kibbr's total is from the two compressors - which keep no index and answer
    // no query - in bytes: what the encodings are measured by.
    if gz.bytes > 0 && zst.bytes > 0 {
        lines.push(format!(
            "  kibbr total is {:.3} of gzip -9 and {:.3} of zstd -19",
            bulk.total.bytes as f64 / gz.bytes as f64,
            bulk.total.bytes as f64 / zst.bytes as f64
        ));
    }
    lines.push(format!(
        "  disk blocks are {} bytes: what each file takes on this disk, rounded up to whole \
         blocks",
        commas(block)
    ));
    // Written before the live day is tried, and not with it: the compressors are most of a
    // run, and a live day that cannot be measured must not cost the table they went into.
    writeln!(out, "{}", lines.join("\n"))
        .and_then(|()| out.flush())
        .map_err(CliError::Write)?;

    // ── The same lines as a live day, before and after compaction ──
    let live =
        live_day(args, &schema, &scratch.join("live"), block, report.lines).map_err(|e| {
            LoadError::Input(format!(
                "the table above stands, but the same lines could not be measured as a live \
                 day: {}",
                crate::error::chain(&e)
            ))
        })?;
    let (before, was) = &live.before;
    let (after, is) = &live.after;
    let mut lines = vec![
        String::new(),
        format!(
            "the same lines as a live day: flushed {} time(s) over the file, as a day that \
             arrives evenly is flushed once a minute, then compacted",
            commas(live.flushes)
        ),
        format!(
            "  before compaction: {} lines, {} rows, {} row groups in {} file(s) over {} \
             day(s), ingested in {:.1}s",
            commas(live.lines),
            commas(was.rows),
            commas(was.row_groups),
            commas(was.files),
            was.days.len(),
            live.ingested_in
        ),
        format!(
            "  after compaction: {} rows, {} row groups in {} file(s) over {} day(s), \
             compacted in {:.1}s",
            commas(is.rows),
            commas(is.row_groups),
            commas(is.files),
            is.days.len(),
            live.compacted_in
        ),
        String::new(),
        heading,
    ];
    lines.extend(before.rows("kibbr total, before", &row));
    lines.extend(after.rows("kibbr total, after", &row));
    lines.push(String::new());
    lines.push(format!(
        "  after compaction is {} of before in bytes, {} in files and {} in row groups",
        share(after.total.bytes, before.total.bytes),
        share(is.files, was.files),
        share(is.row_groups, was.row_groups)
    ));
    writeln!(out, "{}", lines.join("\n")).map_err(|e| CliError::Write(e).into())
}

/// Flushes in a day that is flushed once a minute: the runs the live day's lines are cut
/// into.
const FLUSHES_A_DAY: u64 = 24 * 60;

/// How many of a day's flushes are behind line `at` of `lines`, counted from 1. Placed by
/// share of the file, so any file of at least that many lines gets exactly `FLUSHES_A_DAY`.
fn flushes_due(at: u64, lines: u64) -> u64 {
    at.saturating_mul(FLUSHES_A_DAY) / lines.max(1)
}

/// The input as a day the live door took: what it left, and what compaction made of it.
struct LiveDay {
    /// Flushes made over the file, before the one that closes it.
    flushes: u64,
    lines: u64,
    before: (Held, Counts),
    ingested_in: f64,
    after: (Held, Counts),
    compacted_in: f64,
}

/// A flush that left rows unwritten ends the live day: its sizes would be those of a day
/// with rows missing. The flush has said which, and why, on stderr.
fn written(flush: FlushSummary) -> Result<(), LoadError> {
    if flush.failed == 0 {
        return Ok(());
    }
    Err(LoadError::Input(format!(
        "the live door could not write the rows of {} template(s); each is reported above",
        commas(flush.failed as u64)
    )))
}

/// The input through the live door, measured, compacted and measured again. Flushes are
/// counted in lines, not left to a clock, so a file gives the same row groups every run.
fn live_day(
    args: &Args,
    schema: &Schema,
    data: &Path,
    block: u64,
    lines: u64,
) -> Result<LiveDay, LoadError> {
    fs::create_dir_all(data).map_err(|source| LoadError::Read {
        path: data.to_path_buf(),
        source,
    })?;
    let mut storage = Storage::load(
        &args.settings,
        true,
        Some(data.to_path_buf()),
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .map_err(CliError::from)?;
    // No row waits out a timeout, whatever the environment sets one to: a flush closes
    // every buffer that holds a row.
    storage.row_group_timeout = Duration::ZERO;

    let lock = crate::storage::lock::DataLock::acquire(data)?;
    let (trie, stores) = crate::server::open_durable(&storage, schema).map_err(CliError::from)?;
    // One arrival time for the whole file, as the bulk door gives it one: a clock read per
    // line could split the file over midnight.
    let now = chrono::Utc::now();
    let started = Instant::now();
    // Compared against the flushes already behind, not a line number: a flush that falls after
    // a blank or unparsed line is made at the next line that reaches this.
    let (mut behind, mut flushes) = (0u64, 0u64);
    let stats = db::read_lines(
        BufReader::new(load::open_decoded(&args.input)?),
        &mut |at, parsed| {
            db::ingest_line(&trie, Some(&stores), &parsed, now)?;
            let due = flushes_due(at as u64, lines);
            if due > behind {
                behind = due;
                flushes += 1;
                written(stores.flush_all(FlushKind::Tick))
                    .map_err(|e| SinkError::Fatal(ReadError::Sink(Box::new(e))))?;
            }
            Ok(())
        },
        // A line that does not parse was reported when the bulk door read it. One that
        // parsed and this door could not keep is new.
        &mut |e| {
            if matches!(
                e.kind,
                DetectorErrorKind::NotDurable { .. } | DetectorErrorKind::NotStored { .. }
            ) {
                crate::error::report(&e);
            }
        },
    )
    .map_err(CliError::from)?;
    written(stores.flush_all(FlushKind::Final))?;
    let ingested_in = started.elapsed().as_secs_f64();
    drop(stores);
    drop(trie);
    drop(lock);
    let before = (held(data, block)?, counts_of(data)?);
    if before.1.rows != stats.ingested {
        return Err(LoadError::Input(format!(
            "the live door took {} rows and its files hold {}",
            commas(stats.ingested),
            commas(before.1.rows)
        )));
    }

    let lock = crate::storage::lock::DataLock::acquire(data)?;
    let (trie, stores) = crate::server::open_durable(&storage, schema).map_err(CliError::from)?;
    let store = stores.disk_store().ok_or(LoadError::NoDisk)?;
    let opts = CompactOptions {
        threads: load::alone_threads(),
        cancel: Arc::default(),
    };
    let started = Instant::now();
    for name in &before.1.days {
        let day = disk::day_of_name(name).ok_or_else(|| {
            LoadError::Input(format!(
                "{name}: a directory of data files that is not named for a day"
            ))
        })?;
        store.compact_day(day, &opts)?;
    }
    let compacted_in = started.elapsed().as_secs_f64();
    drop(stores);
    drop(trie);
    drop(lock);
    let after = (held(data, block)?, counts_of(data)?);
    // The two a reader takes on trust when comparing the blocks: the same rows, and no
    // file left as the live door wrote it.
    if after.1.rows != before.1.rows || after.1.finished != after.1.files {
        return Err(LoadError::Input(format!(
            "compaction left {} of {} rows, and {} of {} file(s) finished",
            commas(after.1.rows),
            commas(before.1.rows),
            commas(after.1.finished),
            commas(after.1.files)
        )));
    }
    Ok(LiveDay {
        flushes,
        lines: stats.read,
        before,
        ingested_in,
        after,
        compacted_in,
    })
}

/// The same lines in a scratch Elasticsearch index: its primary store size once
/// force-merged, and how many lines it refused.
fn elasticsearch(url: &str, input: &Path) -> Result<(u64, u64), LoadError> {
    let url = crate::client::parse_url(url).map_err(LoadError::Input)?;
    let index = format!("kibbr-bench-{}", std::process::id());
    let at = |path: &str| format!("{}/{index}{path}", url.prefix);
    let call = |method: &str, path: String, body: Option<(&str, &[u8])>| {
        let (status, answer) = crate::client::request(
            &url.address,
            method,
            &path,
            &[],
            body,
            Duration::from_secs(600),
        )
        .map_err(|e| LoadError::from(Asked::Server {
            address: url.address.clone(),
            detail: format!("{method} {path}: {e}"),
        }))?;
        if status >= 300 {
            return Err(LoadError::from(Asked::Server {
                address: url.address.clone(),
                detail: format!(
                    "{method} {path} answered {status}: {}",
                    String::from_utf8_lossy(&answer[..answer.len().min(500)])
                ),
            }));
        }
        serde_json::from_slice::<serde_json::Value>(&answer).map_err(|e| LoadError::from(Asked::Server {
            address: url.address.clone(),
            detail: format!("{method} {path}: not JSON ({e})"),
        }))
    };
    let settings = br#"{"settings":{"number_of_shards":1,"number_of_replicas":0}}"#;
    call("PUT", at(""), Some(("application/json", settings)))?;
    let result = (|| {
        let mut rejected = 0u64;
        let mut body = Vec::with_capacity(8 << 20);
        let mut send = |body: &mut Vec<u8>| -> Result<(), LoadError> {
            if body.is_empty() {
                return Ok(());
            }
            let answer = call("POST", at("/_bulk"), Some(("application/x-ndjson", body)))?;
            if answer["errors"].as_bool() == Some(true) {
                rejected += answer["items"].as_array().map_or(0, |items| {
                    items
                        .iter()
                        .filter(|i| i["index"]["error"].is_object())
                        .count()
                }) as u64;
            }
            body.clear();
            Ok(())
        };
        let reader = BufReader::new(load::open_decoded(input)?);
        for line in reader.split(b'\n') {
            let line = line.map_err(|source| LoadError::Read {
                path: input.to_path_buf(),
                source,
            })?;
            let line = crate::core::lines::strip_eol(&line);
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            body.extend_from_slice(b"{\"index\":{}}\n");
            body.extend_from_slice(line);
            body.push(b'\n');
            if body.len() >= 8 << 20 {
                send(&mut body)?;
            }
        }
        send(&mut body)?;
        call("POST", at("/_refresh"), None)?;
        call("POST", at("/_forcemerge?max_num_segments=1"), None)?;
        call("POST", at("/_refresh"), None)?;
        let stats = call("GET", at("/_stats/store"), None)?;
        let bytes = stats["indices"][&index]["primaries"]["store"]["size_in_bytes"]
            .as_u64()
            .ok_or_else(|| LoadError::from(Asked::Server {
                address: url.address.clone(),
                detail: "no primaries.store.size_in_bytes in its stats".to_string(),
            }))?;
        Ok((bytes, rejected))
    })();
    let _ = call("DELETE", at(""), None);
    result
}

#[cfg(test)]
mod tests {
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
}
