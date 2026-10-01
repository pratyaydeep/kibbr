//! B11 - merging 200 small files into one, and whether the swap is crash safe.
//!
//! 200 small files, each 8 row groups of 512 rows (zstd 3), merged into one file:
//!   copy_row_groups      the row group bytes are copied across untouched
//!   rebuild_8192_zstd19  every row group is decoded and re-cut into 8,192-row groups
//!                        at zstd 19
//! Reports seconds, output size, row group count.
//!
//! CRASH SAFETY, simulated: the merged file's footer names the files it replaces. A
//! child process (this binary with --crash-at K) stops dead at: (1) new file half
//! written; (2) new file complete but not renamed into place; (3) renamed, no old file
//! removed; (4) some old files removed. The parent then runs the startup rule - delete
//! any file named as a parent of a complete file; delete any incomplete file - and
//! checks that every row exists exactly once.
//!
//! Simplifies: a row is an id and 12 synthetic columns, a file's "index" is its footer,
//! no fsync ordering is tested (a crash here is process exit, not power loss). Decides
//! copy versus rebuild, and whether the parent-name swap rule recovers every stop point.

use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;
use kibbr_lab_rs::workload::{Columns, Gen};
use kibbr_lab_rs::*;

const FILES: usize = 200;
const GROUPS_PER_FILE: usize = 8;
const ROWS_PER_GROUP: usize = 512;
const MAGIC: &[u8; 8] = b"LABSEGOK";

// Record: [len u32][first_id u64][count u32][payload]. Footer: [parents json][parents_len
// u32][MAGIC], so a reader finds the length next to the magic.
fn write_group<W: Write + ?Sized>(f: &mut W, first_id: u64, count: u32, payload: &[u8]) {
    f.write_all(&(payload.len() as u32).to_le_bytes()).unwrap();
    f.write_all(&first_id.to_le_bytes()).unwrap();
    f.write_all(&count.to_le_bytes()).unwrap();
    f.write_all(payload).unwrap();
}
fn write_footer(f: &mut impl Write, parents: &[String]) {
    let j = serde_json::to_vec(parents).unwrap();
    f.write_all(&j).unwrap();
    f.write_all(&(j.len() as u32).to_le_bytes()).unwrap();
    f.write_all(MAGIC).unwrap();
}

struct Group {
    first_id: u64,
    count: u32,
    payload: Vec<u8>,
}
struct Parsed {
    groups: Vec<Group>,
    parents: Vec<String>,
}

/// None when the file is incomplete (no valid footer, or a torn body).
fn parse(path: &Path) -> Option<Parsed> {
    let mut b = Vec::new();
    File::open(path).ok()?.read_to_end(&mut b).ok()?;
    if b.len() < 12 || &b[b.len() - 8..] != MAGIC {
        return None;
    }
    let plen = u32::from_le_bytes(b[b.len() - 12..b.len() - 8].try_into().unwrap()) as usize;
    let pstart = b.len().checked_sub(12 + plen)?;
    let parents: Vec<String> = serde_json::from_slice(&b[pstart..b.len() - 12]).ok()?;
    let mut groups = Vec::new();
    let mut off = 0;
    while off < pstart {
        if off + 16 > pstart {
            return None;
        }
        let len = u32::from_le_bytes(b[off..off + 4].try_into().unwrap()) as usize;
        let first_id = u64::from_le_bytes(b[off + 4..off + 12].try_into().unwrap());
        let count = u32::from_le_bytes(b[off + 12..off + 16].try_into().unwrap());
        if off + 16 + len > pstart {
            return None;
        }
        groups.push(Group { first_id, count, payload: b[off + 16..off + 16 + len].to_vec() });
        off += 16 + len;
    }
    Some(Parsed { groups, parents })
}

fn build_inputs(dir: &Path) -> u64 {
    let _ = fs::remove_dir_all(dir);
    fs::create_dir_all(dir).unwrap();
    let mut g = Gen::new(500);
    let mut id = 0u64;
    for i in 0..FILES {
        let mut f = std::io::BufWriter::new(File::create(dir.join(format!("small_{i:03}.seg"))).unwrap());
        for _ in 0..GROUPS_PER_FILE {
            let rows = g.rows(ROWS_PER_GROUP);
            let mut c = Columns::with_capacity(ROWS_PER_GROUP);
            for r in &rows {
                c.push(r);
            }
            let p = zstd::bulk::compress(&c.to_bytes(), 3).unwrap();
            write_group(&mut f, id, ROWS_PER_GROUP as u32, &p);
            id += ROWS_PER_GROUP as u64;
        }
        write_footer(&mut f, &[]);
    }
    id
}

fn inputs(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = fs::read_dir(dir).unwrap().map(|e| e.unwrap().path()).filter(|p| p.file_name().unwrap().to_str().unwrap().starts_with("small_")).collect();
    v.sort();
    v
}

/// The copy merge, with the crash points. `crash_at` 0 = run to completion.
fn merge_copy(dir: &Path, crash_at: u32) -> (usize, u64) {
    let ins = inputs(dir);
    let tmp = dir.join("merged.tmp");
    let fin = dir.join("merged.seg");
    let parents: Vec<String> = ins.iter().map(|p| p.file_name().unwrap().to_str().unwrap().to_string()).collect();
    let mut f = std::io::BufWriter::new(File::create(&tmp).unwrap());
    let mut n = 0;
    for (i, p) in ins.iter().enumerate() {
        if crash_at == 1 && i == ins.len() / 2 {
            f.flush().unwrap();
            std::process::exit(0);
        }
        let parsed = parse(p).unwrap();
        for g in parsed.groups {
            write_group(&mut f, g.first_id, g.count, &g.payload);
            n += 1;
        }
    }
    write_footer(&mut f, &parents);
    f.flush().unwrap();
    f.get_ref().sync_all().unwrap();
    drop(f);
    if crash_at == 2 {
        std::process::exit(0);
    }
    fs::rename(&tmp, &fin).unwrap();
    if crash_at == 3 {
        std::process::exit(0);
    }
    for (i, p) in ins.iter().enumerate() {
        if crash_at == 4 && i == ins.len() / 2 {
            std::process::exit(0);
        }
        fs::remove_file(p).unwrap();
    }
    (n, fs::metadata(&fin).unwrap().len())
}

fn merge_rebuild(dir: &Path) -> (usize, u64) {
    let ins = inputs(dir);
    let fin = dir.join("merged.seg");
    let parents: Vec<String> = ins.iter().map(|p| p.file_name().unwrap().to_str().unwrap().to_string()).collect();
    let mut f = std::io::BufWriter::new(File::create(&fin).unwrap());
    let mut acc = Columns::with_capacity(8192);
    let mut first = 0u64;
    let mut n = 0;
    let flush = |acc: &mut Columns, first: &mut u64, f: &mut dyn Write, n: &mut usize| {
        if acc.is_empty() {
            return;
        }
        let p = zstd::bulk::compress(&acc.to_bytes(), 19).unwrap();
        write_group(f, *first, acc.len() as u32, &p);
        *first += acc.len() as u64;
        *n += 1;
        acc.clear();
    };
    for p in &ins {
        for g in parse(p).unwrap().groups {
            let c = Columns::from_bytes(&zstd::bulk::decompress(&g.payload, 64 << 20).unwrap());
            acc.append(&c);
            if acc.len() >= 8192 {
                flush(&mut acc, &mut first, &mut f, &mut n);
            }
        }
    }
    flush(&mut acc, &mut first, &mut f, &mut n);
    write_footer(&mut f, &parents);
    f.flush().unwrap();
    for p in &ins {
        fs::remove_file(p).unwrap();
    }
    (n, fs::metadata(&fin).unwrap().len())
}

/// The startup rule. Returns (files deleted as parents, incomplete files deleted).
fn startup_rule(dir: &Path) -> (usize, usize) {
    let files: Vec<PathBuf> = fs::read_dir(dir).unwrap().map(|e| e.unwrap().path()).collect();
    let mut parents = std::collections::HashSet::new();
    let mut incomplete = Vec::new();
    for f in &files {
        match parse(f) {
            Some(p) => parents.extend(p.parents),
            None => incomplete.push(f.clone()),
        }
    }
    let mut a = 0;
    for f in &files {
        if parents.contains(f.file_name().unwrap().to_str().unwrap()) && f.exists() {
            fs::remove_file(f).unwrap();
            a += 1;
        }
    }
    let mut b = 0;
    for f in incomplete {
        if f.exists() {
            fs::remove_file(f).unwrap();
            b += 1;
        }
    }
    (a, b)
}

/// Every id in 0..total exactly once across the complete files?
fn check_rows(dir: &Path, total: u64) -> Result<usize, String> {
    let mut ranges = Vec::new();
    let mut files = 0;
    for e in fs::read_dir(dir).unwrap() {
        let p = e.unwrap().path();
        let parsed = parse(&p).ok_or_else(|| format!("incomplete file left: {}", p.display()))?;
        files += 1;
        for g in parsed.groups {
            ranges.push((g.first_id, g.count as u64));
        }
    }
    ranges.sort_unstable();
    let mut expect = 0;
    for (first, n) in ranges {
        if first != expect {
            return Err(format!("expected id {expect}, found range starting {first}"));
        }
        expect += n;
    }
    if expect != total {
        return Err(format!("{expect} rows found, {total} expected"));
    }
    Ok(files)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if let Some(i) = args.iter().position(|a| a == "--crash-at") {
        let k: u32 = args[i + 1].parse().unwrap();
        let dir = PathBuf::from(&args[i + 2]);
        merge_copy(&dir, k);
        return;
    }
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("out/b11");
    let dir = base.join("work");
    println!("B11 compaction_cost: {FILES} files x {GROUPS_PER_FILE} row groups x {ROWS_PER_GROUP} rows; 3 runs");
    let mut rec = Recorder::new("B11_compaction_cost");
    let mut out = Vec::new();
    let mut total = 0;
    for (name, rebuild) in [("copy_row_groups_untouched", false), ("rebuild_8192_row_groups_zstd19", true)] {
        let mut secs = vec![];
        let mut size = 0;
        let mut groups = 0;
        for _ in 0..3 {
            total = build_inputs(&dir);
            let in_bytes: u64 = inputs(&dir).iter().map(|p| fs::metadata(p).unwrap().len()).sum();
            let t = Instant::now();
            let (n, sz) = if rebuild { merge_rebuild(&dir) } else { merge_copy(&dir, 0) };
            secs.push(t.elapsed().as_secs_f64());
            size = sz;
            groups = n;
            check_rows(&dir, total).unwrap();
            rec.add(name, "input_bytes", in_bytes as f64, "bytes", "");
        }
        let s = Stat::of(&secs);
        record(&mut rec, name, "seconds", s, "s", "");
        rec.add(name, "output_bytes", size as f64, "bytes", "");
        rec.add(name, "row_groups", groups as f64, "count", "");
        out.push(vec![name.into(), format!("{:.3} ±{:.0}%", s.median, s.spread_pct()), fmt_bytes(size as f64), groups.to_string()]);
    }
    table(&["merge", "seconds", "output", "row groups"], &out);

    println!("\ncrash safety of the swap (copy merge, child process stops dead):");
    let exe = std::env::current_exe().unwrap();
    let mut crash_rows = Vec::new();
    for (k, what) in [(1, "new file half written"), (2, "new file complete, not renamed"), (3, "renamed, no old file removed"), (4, "half the old files removed")] {
        build_inputs(&dir);
        let st = std::process::Command::new(&exe).args(["--crash-at", &k.to_string(), dir.to_str().unwrap()]).status().unwrap();
        assert!(st.success());
        let before = fs::read_dir(&dir).unwrap().count();
        let (as_parent, as_incomplete) = startup_rule(&dir);
        let verdict = check_rows(&dir, total);
        let ok = verdict.is_ok();
        let files_after = *verdict.as_ref().unwrap_or(&0);
        let err = verdict.err().unwrap_or_default();
        rec.add(&format!("crash_{k}_{}", what.replace(' ', "_").replace(',', "")), "every_row_exactly_once", ok as u8 as f64, "bool", &format!("{before} files before startup rule; deleted {as_parent} named as parents, {as_incomplete} incomplete; {files_after} files after; {err}"));
        crash_rows.push(vec![k.to_string(), what.into(), before.to_string(), as_parent.to_string(), as_incomplete.to_string(), files_after.to_string(), if ok { "OK".into() } else { format!("VIOLATION: {err}") }]);
    }
    table(&["stop", "point", "files", "deleted as parent", "deleted incomplete", "files after", "every row exactly once"], &crash_rows);
    let _ = fs::remove_dir_all(&base);
    rec.flush();
}
