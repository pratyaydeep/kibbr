//! `triedb bench-storage <file>`: what a file costs to store, by one command.
//!
//! Every storage number before this went through `POST /ingest` and a hand-run `du`, so no
//! two were measured the same way. This loads the file through the bulk door into a
//! scratch data directory - full row groups, the way a closed day is stored - and prints,
//! beside the raw bytes and the two compressors an operator would otherwise reach for:
//!
//! - **TrieDB's total**: every data file, the catalog and the manifest - everything the
//!   directory needs to answer a query, and nothing it does not (`LOCK` is a few bytes of
//!   hint, `staging/` is empty after a commit).
//! - **Where the data files' bytes go**: column data, row group headers, schema records,
//!   footers and file headers - found by walking each file's frames, so the parts add up to
//!   the files exactly. The per-row-group overhead is the number compaction exists for.
//! - **Bytes and disk blocks**: what a file holds, and what it takes on this disk once
//!   rounded up to whole blocks - the 4KB per file that made one file per template a day
//!   cost more than its data.
//!
//! `gzip -9` and `zstd -19` are computed in this process, with flate2 and libzstd at those
//! levels, and written to the scratch directory so their blocks are measured like the rest.
//! With `--es`, the same lines are indexed into a scratch Elasticsearch index - one shard,
//! no replica, force-merged to one segment so the number does not depend on when it was
//! read - and its primary store size is printed beside the rest.

use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::config::{Schema, Storage};
use crate::error::CliError;
use crate::fmt::commas;
use crate::load::{self, LoadError};
use crate::store::v3::{HEADER_LEN, KIND_FOOTER, KIND_ROW_GROUP, KIND_SCHEMA, TRAILER_LEN};

pub struct Args {
    pub input: PathBuf,
    pub es: Option<String>,
    pub scratch: Option<PathBuf>,
    pub keep: bool,
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
        match crate::frame::read(&bytes, at, u32::MAX) {
            crate::frame::Next::Frame { payload, end } => {
                let frame = (end - at) as u64;
                match payload[0] {
                    KIND_SCHEMA => parts.schemas += frame,
                    KIND_ROW_GROUP => {
                        // kind, template, schema, rows, min, max, source, stream, first,
                        // last - then the chunk area's length.
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
            let dir = std::env::temp_dir().join(format!("triedb-bench-{}", std::process::id()));
            let _ = fs::remove_dir_all(&dir);
            (dir, true)
        }
    };
    let result = measure(args, schema, &scratch, out);
    if !args.keep {
        let _ = fs::remove_dir_all(&scratch);
    } else if ours {
        eprintln!("triedb: kept {}", scratch.display());
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

    // ── TrieDB: the file through the bulk door, alone ──
    let storage = Storage::load(true, Some(data.clone()), None, None, None, None, None, None)
        .map_err(CliError::from)?;
    let lock = crate::lock::DataLock::acquire(&data)?;
    let (trie, stores) =
        crate::serve::open_durable(&storage, &schema).map_err(crate::error::CliError::from)?;
    let opts = load::Options {
        inputs: load::resolve(std::slice::from_ref(&args.input), None)?,
        replace_day: None,
        max_bytes_per_sec: None,
        threads: load::alone_threads(),
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

    let io_at = |path: &Path| {
        let path = path.to_path_buf();
        move |source| LoadError::Read { path, source }
    };
    let mut files = Size::default();
    let mut parts = Parts::default();
    for path in crate::store::disk::data_files(&data) {
        files += size_of(&path, block).map_err(io_at(&path))?;
        let p = parts_of(&path)?;
        parts.columns += p.columns;
        parts.row_group_headers += p.row_group_headers;
        parts.schemas += p.schemas;
        parts.footers += p.footers;
        parts.file_headers += p.file_headers;
        parts.other += p.other;
    }
    let catalog_path = crate::catalog::path_in(&data);
    let manifest_path = crate::store::manifest::path_in(&data);
    let catalog = size_of(&catalog_path, block).map_err(io_at(&catalog_path))?;
    let manifest = size_of(&manifest_path, block).map_err(io_at(&manifest_path))?;
    let mut total = files;
    total += catalog;
    total += manifest;

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
    let ratio = |bytes: u64| {
        if raw.bytes == 0 {
            "-".to_string()
        } else {
            format!("{:.3}", bytes as f64 / raw.bytes as f64)
        }
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
        format!(
            "  {:<24}{:>14}{:>14}{:>9}",
            "", "bytes", "disk blocks", "x raw"
        ),
    ];
    let mut row = |name: &str, size: Option<Size>, bytes: u64| {
        lines.push(format!(
            "  {:<24}{:>14}{:>14}{:>9}",
            name,
            commas(bytes),
            size.map_or_else(String::new, |s| commas(s.blocks)),
            ratio(bytes)
        ));
    };
    row("raw", Some(raw), raw.bytes);
    row("gzip -9", Some(gz), gz.bytes);
    row("zstd -19", Some(zst), zst.bytes);
    row("TrieDB total", Some(total), total.bytes);
    row("  data files", Some(files), files.bytes);
    row("    column data", None, parts.columns);
    row("    row group headers", None, parts.row_group_headers);
    row("    schema records", None, parts.schemas);
    row("    footers", None, parts.footers);
    row("    file headers", None, parts.file_headers);
    if parts.other > 0 {
        row("    optional frames", None, parts.other);
    }
    row("  catalog.log", Some(catalog), catalog.bytes);
    row("  manifest.log", Some(manifest), manifest.bytes);
    if let Some((bytes, rejected)) = es {
        row("Elasticsearch store", None, bytes);
        if rejected > 0 {
            lines.push(format!(
                "  ({} line(s) Elasticsearch refused are not in its number)",
                commas(rejected)
            ));
        }
    }
    lines.push(String::new());
    lines.push(format!(
        "  disk blocks are {} bytes: what each file takes on this disk, rounded up to whole \
         blocks",
        commas(block)
    ));
    writeln!(out, "{}", lines.join("\n")).map_err(|e| CliError::Write(e).into())
}

/// The same lines in a scratch Elasticsearch index: its primary store size once
/// force-merged, and how many lines it refused.
fn elasticsearch(url: &str, input: &Path) -> Result<(u64, u64), LoadError> {
    let url = crate::http::parse_url(url).map_err(LoadError::Input)?;
    let index = format!("triedb-bench-{}", std::process::id());
    let at = |path: &str| format!("{}/{index}{path}", url.prefix);
    let call = |method: &str, path: String, body: Option<(&str, &[u8])>| {
        let (status, answer) = crate::http::request(
            &url.address,
            method,
            &path,
            &[],
            body,
            Duration::from_secs(600),
        )
        .map_err(|e| LoadError::Server {
            address: url.address.clone(),
            detail: format!("{method} {path}: {e}"),
        })?;
        if status >= 300 {
            return Err(LoadError::Server {
                address: url.address.clone(),
                detail: format!(
                    "{method} {path} answered {status}: {}",
                    String::from_utf8_lossy(&answer[..answer.len().min(500)])
                ),
            });
        }
        serde_json::from_slice::<serde_json::Value>(&answer).map_err(|e| LoadError::Server {
            address: url.address.clone(),
            detail: format!("{method} {path}: not JSON ({e})"),
        })
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
            let line = crate::lines::strip_eol(&line);
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
            .ok_or_else(|| LoadError::Server {
                address: url.address.clone(),
                detail: "no primaries.store.size_in_bytes in its stats".to_string(),
            })?;
        Ok((bytes, rejected))
    })();
    let _ = call("DELETE", at(""), None);
    result
}
