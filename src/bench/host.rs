// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The machine the bench runs on: what it is, and whether it is fit to measure on.
//! Two gates and no more - another kibbr process alive, the swap-out counter moving; the
//! rest is recorded and decides nothing. A child's CPU time is checked against `getrusage`
//! on every start: macOS counts it in Mach ticks, not nanoseconds. System programs are run
//! as children with `LC_ALL=C` and their text parsed, so each parser is tested on real text.

use std::ffi::OsStr;
use std::fs::{self, File};
use std::hint::black_box;
use std::io::{self, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use chrono::{DateTime, Local, SecondsFormat};

use super::corpus::digest;
use super::report::{Binary, Commit, Machine, Power};
use super::{crc_hex, SpeedError, FLUSH_REF_BYTES, FLUSH_REF_WRITES};

/// Rounds of the spin reference: long enough that the scheduler's choice of core shows,
/// short enough to take three.
const SPIN_ROUNDS: u64 = 1 << 27;

/// Where the spin's arithmetic starts. Any odd number would do; it is fixed so that the
/// rounds are the same work in every build.
const SPIN_SEED: u64 = 0x9E37_79B9_7F4A_7C15;

/// A reference is taken this many times and the least kept: the least is the one nothing
/// else on the machine was in.
const REFERENCE_TRIES: usize = 3;

/// CPU time this process uses, by `getrusage`, while its clock is checked.
const CLOCK_CHECK_S: f64 = 0.2;

/// How far from `getrusage` the child's reading of the same spin may be, as a share.
const CLOCK_CHECK_SHARE: f64 = 0.02;

/// How `Commit::how` reads when git named a commit.
const HOW: &str = "git rev-parse HEAD in the working directory when the bench ran";

/// A process's CPU time and memory, read from the system by pid.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Cpu {
    pub user_s: f64,
    pub system_s: f64,
    pub resident_bytes: u64,
    pub pageins: u64,
}

impl Cpu {
    /// User plus system: one core for one second is 1.
    pub fn total_s(&self) -> f64 {
        self.user_s + self.system_s
    }
}

/// Pages swapped in and out since boot, where only a difference between two reads means
/// anything and only `outs` is a gate.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Swap {
    pub ins: u64,
    pub outs: u64,
}

// ── A process's CPU time ───────────────────────────────────────────────────────────

/// The CPU time and memory of process `pid`, every thread since it started, or `None`
/// where this system has no source the bench reads or the process is gone.
pub fn cpu(pid: u32) -> Option<Cpu> {
    os::cpu(pid)
}

/// Hold `cpu` to `getrusage` over a spin of this process's own, and not to the wall clock,
/// so that a tick read as another unit fails and a busy machine does not.
pub fn cpu_clock_check() -> Result<Option<f64>, SpeedError> {
    let pid = std::process::id();
    if cpu(pid).is_none() {
        return Ok(None);
    }
    let own = || {
        own_cpu_s().ok_or_else(|| {
            SpeedError::Host(
                "getrusage gave this process no CPU time, so the clock a server's CPU time is \
                 read from cannot be checked against it, and no scan rate can be trusted"
                    .to_string(),
            )
        })
    };
    // `getrusage` first and the system's reading second, at both ends: what passes between
    // the two reads is then on the same side of both differences.
    let own_before = own()?;
    let Some(read_before) = cpu(pid) else {
        return Ok(None);
    };
    let mut x = SPIN_SEED;
    let own_after = loop {
        x = spin_rounds(x, 1 << 16);
        let now = own()?;
        if now - own_before >= CLOCK_CHECK_S {
            break now;
        }
    };
    let Some(read_after) = cpu(pid) else {
        return Ok(None);
    };
    black_box(x);

    let by_own = own_after - own_before;
    let by_read = read_after.total_s() - read_before.total_s();
    let allowed = by_own * CLOCK_CHECK_SHARE + 2.0 * os::cpu_step_s();
    if (by_read - by_own).abs() > allowed {
        return Err(SpeedError::Host(format!(
            "the clock a server's CPU time is read from does not agree with getrusage: over \
             one spin of this process, getrusage counted {by_own:.4} s of CPU and {} counted \
             {by_read:.4} s, {:.3} of it, where 0.98 to 1.02 is allowed. Every scan rate \
             would be out by as much, so nothing was measured; this is a fault in how \
             kibbr-bench reads this system's clock, and waiting does not mend it",
            os::cpu_clock().unwrap_or_else(|| "the system".to_string()),
            by_read / by_own,
        )));
    }
    Ok(Some(by_read / by_own))
}

/// This process's user plus system CPU time by `getrusage`, in seconds.
#[cfg(unix)]
fn own_cpu_s() -> Option<f64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: `usage` is room for one `rusage`, which the call fills when it returns 0 -
    // the only case in which it is read. `RUSAGE_SELF` names this process and no memory.
    let usage = unsafe {
        if libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) != 0 {
            return None;
        }
        usage.assume_init()
    };
    let seconds = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    Some(seconds(usage.ru_utime) + seconds(usage.ru_stime))
}

#[cfg(not(unix))]
fn own_cpu_s() -> Option<f64> {
    None
}

// ── The two gates ──────────────────────────────────────────────────────────────────

/// The swap counters, or `Ok(None)` where this system has none the bench reads; one that
/// is there and cannot be read is an error, since a gate that cannot be read cannot pass.
pub fn swap() -> Result<Option<Swap>, SpeedError> {
    os::swap()
}

/// The pids of every process whose command's file name is exactly `kibbr`, less this
/// process and its children; a process list that cannot be read is `SpeedError::Host`.
pub fn other_kibbr() -> Result<Vec<u32>, SpeedError> {
    let cannot = |why: String| {
        SpeedError::Host(format!(
            "the check for another kibbr process cannot pass, because the process list \
             cannot be read: `ps -axo pid=,ppid=,comm=` {why}. --unchecked runs without the \
             check, and marks the result as not a measurement"
        ))
    };
    let text = asked("ps", ["-axo", "pid=,ppid=,comm="]).map_err(cannot)?;
    others(&text, std::process::id())
        .ok_or_else(|| cannot("printed a list that does not hold this process".to_string()))
}

/// The `kibbr` processes in what `ps` printed, less process `own` and its children. `None`
/// when `own` is not in the list: such a list would say "no other kibbr" of any machine.
fn others(ps: &str, own: u32) -> Option<Vec<u32>> {
    let mut seen_own = false;
    let mut found = Vec::new();
    for (pid, parent, command) in ps.lines().filter_map(ps_row) {
        if pid == own {
            seen_own = true;
        } else if parent != own && command.rsplit('/').next() == Some("kibbr") {
            found.push(pid);
        }
    }
    seen_own.then_some(found)
}

/// One row of `ps -axo pid=,ppid=,comm=`: two numbers, then the command to the end of the
/// line - on macOS the executable's whole path, which may hold spaces.
fn ps_row(line: &str) -> Option<(u32, u32, &str)> {
    let (pid, rest) = line.trim_start().split_once(char::is_whitespace)?;
    let (parent, command) = rest.trim_start().split_once(char::is_whitespace)?;
    Some((pid.parse().ok()?, parent.parse().ok()?, command.trim()))
}

// ── Printed and recorded, never a gate ─────────────────────────────────────────────

/// The 1-minute load average: printed, never a gate.
pub fn load_average() -> Option<f64> {
    os::load_average()
}

/// Memory an application could be given without swapping, in bytes: printed, never a
/// gate.
pub fn memory_available() -> Option<u64> {
    os::memory_available()
}

/// How the machine is powered and whether Low Power Mode is on: recorded beside the
/// references to say why a spin read slower, never a gate.
pub fn power() -> Option<Power> {
    os::power()
}

/// The spin reference, the least of three runs of fixed arithmetic on one thread: the
/// same on every commit, so a throttled run or one on the efficiency cores shows here.
pub fn spin() -> Duration {
    (0..REFERENCE_TRIES)
        .map(|_| {
            let start = Instant::now();
            black_box(spin_rounds(SPIN_SEED, SPIN_ROUNDS));
            start.elapsed()
        })
        .min()
        .unwrap_or_default()
}

/// `rounds` rounds of xorshift64 from `x`, each through `black_box`: a reference that a
/// compiler upgrade made faster would read as a cooler machine.
fn spin_rounds(mut x: u64, rounds: u64) -> u64 {
    for _ in 0..rounds {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x = black_box(x);
    }
    x
}

/// The flush reference, the least of three: `FLUSH_REF_WRITES` writes to one file in
/// `dir`, each followed by `sync_all`, which is what the server's log calls after each body.
pub fn flush_ref(dir: &Path) -> io::Result<Duration> {
    let path = dir.join(format!("flush-reference-{}", std::process::id()));
    // Not zeros: a volume that stores a run of zeros as a hole would flush nothing.
    let mut x = SPIN_SEED;
    let block: Vec<u8> = (0..FLUSH_REF_BYTES)
        .map(|_| {
            x = spin_rounds(x, 1);
            x as u8
        })
        .collect();

    let timed = flush_least(&path, &block);
    // Removed whether or not the writes went through; of two failures the first is told.
    let removed = match fs::remove_file(&path) {
        Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    };
    let least = timed?;
    removed?;
    Ok(least)
}

/// The least of the tries of the flush reference.
fn flush_least(path: &Path, block: &[u8]) -> io::Result<Duration> {
    let mut least = flush_once(path, block)?;
    for _ in 1..REFERENCE_TRIES {
        least = least.min(flush_once(path, block)?);
    }
    Ok(least)
}

/// One try of the flush reference: the file made anew, then the writes and flushes timed.
fn flush_once(path: &Path, block: &[u8]) -> io::Result<Duration> {
    let mut file = File::create(path)?;
    let start = Instant::now();
    for _ in 0..FLUSH_REF_WRITES {
        file.write_all(block)?;
        file.sync_all()?;
    }
    Ok(start.elapsed())
}

// ── What the machine is ────────────────────────────────────────────────────────────

/// What the machine is, less `disk_free_bytes` and `cpu_clock_check`, which the caller
/// fills: the one needs a path and the other can fail.
pub fn machine() -> Machine {
    let (cores_performance, cores_efficiency) = os::core_kinds();
    Machine {
        os: std::env::consts::OS.to_string(),
        os_version: os::version(),
        arch: std::env::consts::ARCH.to_string(),
        cpu: os::cpu_name(),
        cores_logical: std::thread::available_parallelism().map_or(1, NonZeroUsize::get),
        cores_performance,
        cores_efficiency,
        memory_bytes: os::memory(),
        disk_free_bytes: 0,
        cpu_clock: cpu(std::process::id()).and_then(|_| os::cpu_clock()),
        cpu_clock_check: None,
        swap_counter: match swap() {
            Ok(Some(_)) => os::SWAP_COUNTER.map(str::to_string),
            _ => None,
        },
    }
}

/// Free bytes on the volume that holds `dir`, or its nearest parent that exists: what
/// `df -Pk` prints as available to a writer that is not root.
pub fn disk_free(dir: &Path) -> Result<u64, SpeedError> {
    let at = nearest_existing(dir);
    let cannot = |why: String| {
        SpeedError::Host(format!(
            "the free space of the volume that holds {} cannot be read: `df -Pk {}` {why}",
            dir.display(),
            at.display(),
        ))
    };
    let text = asked("df", [OsStr::new("-Pk"), at.as_os_str()]).map_err(cannot)?;
    df_available_bytes(&text).ok_or_else(|| {
        cannot(format!(
            "printed no row the bench can read: {}",
            text.trim()
        ))
    })
}

/// `dir`, or the nearest directory above it that exists: a `--dir` yet to be made is on
/// that volume. A whole path, so that a name beginning with `-` is not read by `df` as a flag.
fn nearest_existing(dir: &Path) -> PathBuf {
    let mut at = dir;
    let found = loop {
        if at.as_os_str().is_empty() {
            break Path::new(".");
        }
        if at.exists() {
            break at;
        }
        match at.parent() {
            Some(parent) => at = parent,
            None => break Path::new("."),
        }
    };
    fs::canonicalize(found).unwrap_or_else(|_| found.to_path_buf())
}

/// The available bytes in what `df -Pk` printed for one path. Found from the one column
/// that ends in `%`, not by counting from the left: a file system's name may hold spaces.
fn df_available_bytes(text: &str) -> Option<u64> {
    let row: Vec<&str> = text
        .lines()
        .skip(1)
        .flat_map(str::split_whitespace)
        .collect();
    let number = |at: usize| row.get(at)?.parse::<u64>().ok();
    (3..row.len())
        .find(|at| row[*at].ends_with('%') && (1..=3).all(|back| number(at - back).is_some()))
        .and_then(|at| number(at - 1))
        .map(|kib| kib.saturating_mul(1024))
}

// ── The checkout and the binary ────────────────────────────────────────────────────

/// The checkout the bench runs in, as git tells it; `src_dirty` asks only about `src`,
/// `Cargo.toml` and `Cargo.lock`, since the whole tree is dirty in every result.
pub fn commit() -> Commit {
    let git = |args: &[&str]| {
        asked("git", args)
            .ok()
            .map(|text| text.trim().to_string())
            .filter(|text| !text.is_empty())
    };
    let Some(sha) = git(&["rev-parse", "HEAD"]) else {
        return Commit {
            sha: None,
            src_dirty: None,
            committed_at: None,
            how: "git rev-parse HEAD named no commit in the working directory when the bench ran"
                .to_string(),
        };
    };
    // `:/` is the checkout's root, wherever under it the bench was started.
    let differs = Command::new("git")
        .args(["diff", "--quiet", "HEAD", "--"])
        .args([":/src", ":/Cargo.toml", ":/Cargo.lock"])
        .env("LC_ALL", "C")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .ok()
        .and_then(|status| status.code());
    Commit {
        sha: Some(sha),
        src_dirty: match differs {
            Some(0) => Some(false),
            Some(1) => Some(true),
            _ => None,
        },
        committed_at: git(&["log", "-1", "--format=%cI", "HEAD"]),
        how: HOW.to_string(),
    }
}

/// The binary at `exe`: its length, CRC-32C and modification time, whether it is a
/// release build, and `stale` if it is older than `commit` and so not built from it.
pub fn binary(exe: &Path, commit: &Commit) -> Result<Binary, SpeedError> {
    let (bytes, crc) = digest(exe).map_err(|source| SpeedError::Io {
        path: exe.to_path_buf(),
        source,
    })?;
    let modified = fs::metadata(exe)
        .and_then(|meta| meta.modified())
        .ok()
        .map(DateTime::<Local>::from);
    let committed = commit
        .committed_at
        .as_deref()
        .and_then(|at| DateTime::parse_from_rfc3339(at).ok());
    Ok(Binary {
        path: exe.display().to_string(),
        bytes,
        crc32c: crc_hex(crc),
        modified_at: modified.map(|at| at.to_rfc3339_opts(SecondsFormat::Secs, false)),
        build: if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        }
        .to_string(),
        stale: match (modified, committed) {
            (Some(modified), Some(committed)) => Some(modified < committed),
            _ => None,
        },
    })
}

/// Whether the binary is still the bytes `was` recorded, since a build beside the bench
/// replaces the binary being measured.
pub fn binary_unchanged(was: &Binary) -> Result<(), SpeedError> {
    let changed = |now: String| {
        SpeedError::Host(format!(
            "the binary changed while it was being measured: {} was {} bytes, crc32c {}, when \
             the bench started, and {now}. The servers of this run were not all one build, so \
             nothing was saved; a `cargo build --release` beside the bench does this. Start \
             it again and leave the binary alone until it ends",
            was.path, was.bytes, was.crc32c,
        ))
    };
    match digest(Path::new(&was.path)) {
        Ok((bytes, crc)) if bytes == was.bytes && crc_hex(crc) == was.crc32c => Ok(()),
        Ok((bytes, crc)) => Err(changed(format!(
            "is now {bytes} bytes, crc32c {}",
            crc_hex(crc)
        ))),
        Err(e) => Err(changed(format!("cannot be read now ({e})"))),
    }
}

// ── Children, and the text they print ──────────────────────────────────────────────

/// Run a program of the system and return what it printed. The error is why not, in
/// words that end a sentence: it could not be started, or how it exited.
fn asked<I, S>(program: &str, args: I) -> Result<String, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = Command::new(program)
        .args(args)
        .env("LC_ALL", "C")
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .map_err(|e| format!("could not be started ({e})"))?;
    if !output.status.success() {
        return Err(match output.status.code() {
            Some(code) => format!("exited {code}"),
            None => "was ended by a signal".to_string(),
        });
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// The first number in a line of numbers: the 1-minute load average, in `{ 1.75 1.81
/// 1.70 }` as `sysctl -n vm.loadavg` prints it and in `/proc/loadavg`.
#[cfg(any(target_os = "macos", target_os = "linux", test))]
fn first_number(text: &str) -> Option<f64> {
    text.split_whitespace().find_map(|word| word.parse().ok())
}

/// The count `vm_stat` prints beside `name`, as in `Swapouts:      7983780.` - the name,
/// a colon, the number and a full stop.
#[cfg(any(target_os = "macos", test))]
fn vm_stat_count(text: &str, name: &str) -> Option<u64> {
    text.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        if key.trim().trim_matches('"') != name {
            return None;
        }
        value.trim().trim_end_matches('.').parse().ok()
    })
}

/// The swap counters in what `vm_stat` printed; `None` if it names no `Swapouts`.
#[cfg(any(target_os = "macos", test))]
fn vm_stat_swap(text: &str) -> Option<Swap> {
    Some(Swap {
        ins: vm_stat_count(text, "Swapins").unwrap_or(0),
        outs: vm_stat_count(text, "Swapouts")?,
    })
}

/// Memory an application could be given without swapping, from what `vm_stat` printed:
/// the free, inactive, speculative and purgeable pages.
#[cfg(any(target_os = "macos", test))]
fn vm_stat_available(text: &str) -> Option<u64> {
    let (_, after) = text.split_once("page size of ")?;
    let page: u64 = after.split_whitespace().next()?.parse().ok()?;
    let free = vm_stat_count(text, "Pages free")?;
    let reclaimed: u64 = ["inactive", "speculative", "purgeable"]
        .iter()
        .map(|kind| vm_stat_count(text, &format!("Pages {kind}")).unwrap_or(0))
        .sum();
    Some((free + reclaimed).saturating_mul(page))
}

/// Whether Low Power Mode is on, in what `pmset -g` printed: the number beside
/// `lowpowermode`. `None` where the system names no such setting.
#[cfg(any(target_os = "macos", test))]
fn pmset_low_power(text: &str) -> Option<bool> {
    text.lines().find_map(|line| {
        let mut words = line.split_whitespace();
        if words.next()? != "lowpowermode" {
            return None;
        }
        match words.next()? {
            "1" => Some(true),
            "0" => Some(false),
            _ => None,
        }
    })
}

/// What the machine draws from, in what `pmset -g batt` printed: the name in quotes on
/// its first line, `Now drawing from 'AC Power'`. Kept as the system spells it.
#[cfg(any(target_os = "macos", test))]
fn pmset_source(text: &str) -> Option<String> {
    text.lines().find_map(|line| {
        let named = line.trim().strip_prefix("Now drawing from '")?;
        let (name, _) = named.split_once('\'')?;
        (!name.is_empty()).then(|| name.to_string())
    })
}

/// The number beside `name` in a file of `name value` lines, as `/proc/vmstat` is.
#[cfg(any(target_os = "linux", test))]
fn proc_count(text: &str, name: &str) -> Option<u64> {
    text.lines().find_map(|line| {
        let mut words = line.split_whitespace();
        if words.next()? != name {
            return None;
        }
        words.next()?.parse().ok()
    })
}

/// The swap counters in `/proc/vmstat`; `None` if it names no `pswpout`, as on a kernel
/// built without swap.
#[cfg(any(target_os = "linux", test))]
fn proc_vmstat_swap(text: &str) -> Option<Swap> {
    Some(Swap {
        ins: proc_count(text, "pswpin").unwrap_or(0),
        outs: proc_count(text, "pswpout")?,
    })
}

/// The bytes beside `name` in `/proc/meminfo`, whose lines are `MemAvailable:  123 kB`.
#[cfg(any(target_os = "linux", test))]
fn meminfo_bytes(text: &str, name: &str) -> Option<u64> {
    text.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        if key.trim() != name {
            return None;
        }
        let kib: u64 = value.split_whitespace().next()?.parse().ok()?;
        Some(kib.saturating_mul(1024))
    })
}

/// What the bench reads of `/proc/<pid>/stat`.
#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProcStat {
    /// Faults that needed a page read from the disk.
    major_faults: u64,
    user_ticks: u64,
    system_ticks: u64,
    resident_pages: u64,
}

/// Read `/proc/<pid>/stat`. The command's name may hold spaces and brackets, so the
/// fields are counted from the last `)`.
#[cfg(any(target_os = "linux", test))]
fn proc_stat(text: &str) -> Option<ProcStat> {
    let (_, after) = text.rsplit_once(')')?;
    let fields: Vec<&str> = after.split_whitespace().collect();
    let field = |number: usize| fields.get(number - 3)?.parse::<u64>().ok();
    Some(ProcStat {
        major_faults: field(12)?,
        user_ticks: field(14)?,
        system_ticks: field(15)?,
        resident_pages: field(24)?,
    })
}

/// Ticks of the Mach clock as seconds: times `numer` over `denom` they are nanoseconds.
/// In whole numbers first, so that a long-lived server's count is not rounded on the way.
#[cfg(any(target_os = "macos", test))]
fn mach_seconds(ticks: u64, numer: u32, denom: u32) -> f64 {
    (u128::from(ticks) * u128::from(numer) / u128::from(denom.max(1))) as f64 / 1e9
}

// ── What each system is asked ──────────────────────────────────────────────────────

#[cfg(target_os = "macos")]
mod os {
    use std::mem::MaybeUninit;
    use std::sync::OnceLock;

    use super::{asked, first_number, mach_seconds, vm_stat_available, vm_stat_swap};
    use super::{pmset_low_power, pmset_source};
    use super::{Cpu, Power, SpeedError, Swap};

    /// Whole paths: `sysctl` is in a directory that is not on every `PATH`.
    const VM_STAT: &str = "/usr/bin/vm_stat";
    const SYSCTL: &str = "/usr/sbin/sysctl";
    const PMSET: &str = "/usr/bin/pmset";

    pub const SWAP_COUNTER: Option<&str> = Some("vm_stat");

    /// What turns a tick of the Mach clock into nanoseconds: times `numer`, over `denom`.
    /// Asked once; it does not change while the system is up.
    // libc marks this call and its structure as deprecated in favour of the `mach2`
    // crate, which this build does not have and the bench does not add for one call.
    #[allow(deprecated)]
    fn timebase() -> Option<(u32, u32)> {
        static TIMEBASE: OnceLock<Option<(u32, u32)>> = OnceLock::new();
        *TIMEBASE.get_or_init(|| {
            let mut info = MaybeUninit::<libc::mach_timebase_info>::uninit();
            // SAFETY: `info` is room for one `mach_timebase_info`, which the call fills
            // when it returns 0 - the only case in which it is read.
            let info = unsafe {
                if libc::mach_timebase_info(info.as_mut_ptr()) != 0 {
                    return None;
                }
                info.assume_init()
            };
            (info.numer != 0 && info.denom != 0).then_some((info.numer, info.denom))
        })
    }

    pub fn cpu(pid: u32) -> Option<Cpu> {
        let (numer, denom) = timebase()?;
        let pid = libc::c_int::try_from(pid).ok()?;
        let mut info = MaybeUninit::<libc::rusage_info_v2>::uninit();
        // SAFETY: `info` is room for one `rusage_info_v2`, filled when the call returns 0 - the
        // only case in which it is read. The cast is to the call's own `rusage_info_t` pointer.
        let info = unsafe {
            if libc::proc_pid_rusage(pid, libc::RUSAGE_INFO_V2, info.as_mut_ptr().cast()) != 0 {
                return None;
            }
            info.assume_init()
        };
        Some(Cpu {
            user_s: mach_seconds(info.ri_user_time, numer, denom),
            system_s: mach_seconds(info.ri_system_time, numer, denom),
            resident_bytes: info.ri_resident_size,
            pageins: info.ri_pageins,
        })
    }

    /// Where `cpu` reads from, in words, for the result.
    pub fn cpu_clock() -> Option<String> {
        let (numer, denom) = timebase()?;
        Some(format!("proc_pid_rusage, mach timebase {numer}/{denom}"))
    }

    /// The step `cpu` counts in, in seconds: nothing beside the check's share.
    pub fn cpu_step_s() -> f64 {
        0.0
    }

    pub fn swap() -> Result<Option<Swap>, SpeedError> {
        let text = asked(VM_STAT, [] as [&str; 0]).map_err(|why| {
            SpeedError::Host(format!(
                "the swap counter cannot be read, so the check that the machine is not \
                 swapping cannot pass: `vm_stat` {why}. --unchecked runs without the check, \
                 and marks the result as not a measurement"
            ))
        })?;
        Ok(vm_stat_swap(&text))
    }

    pub fn load_average() -> Option<f64> {
        first_number(&sysctl("vm.loadavg")?)
    }

    pub fn memory_available() -> Option<u64> {
        vm_stat_available(&asked(VM_STAT, [] as [&str; 0]).ok()?)
    }

    /// Two children, one for each half: the settings in use name the mode and not the
    /// source, the battery's status the source and not the mode.
    pub fn power() -> Option<Power> {
        let read = |args: &[&str]| asked(PMSET, args).ok();
        let power = Power {
            source: read(&["-g", "batt"]).and_then(|text| pmset_source(&text)),
            low_power_mode: read(&["-g"]).and_then(|text| pmset_low_power(&text)),
        };
        (power != Power::default()).then_some(power)
    }

    pub fn version() -> Option<String> {
        sysctl("kern.osproductversion")
    }

    pub fn cpu_name() -> Option<String> {
        sysctl("machdep.cpu.brand_string")
    }

    /// Performance and efficiency cores, where the machine has the two kinds.
    pub fn core_kinds() -> (Option<usize>, Option<usize>) {
        let count = |name: &str| sysctl(name)?.parse().ok();
        (
            count("hw.perflevel0.logicalcpu"),
            count("hw.perflevel1.logicalcpu"),
        )
    }

    pub fn memory() -> Option<u64> {
        sysctl("hw.memsize")?.parse().ok()
    }

    /// One name a child: asked for several, `sysctl` leaves out a name it does not know
    /// and the answers that follow would be read as their neighbours'.
    fn sysctl(name: &str) -> Option<String> {
        let text = asked(SYSCTL, ["-n", name]).ok()?;
        let text = text.trim();
        (!text.is_empty()).then(|| text.to_string())
    }
}

#[cfg(target_os = "linux")]
mod os {
    use std::fs;
    use std::io;

    use super::{first_number, meminfo_bytes, proc_stat, proc_vmstat_swap};
    use super::{Cpu, Power, SpeedError, Swap};

    pub const SWAP_COUNTER: Option<&str> = Some("/proc/vmstat");

    pub fn cpu(pid: u32) -> Option<Cpu> {
        let stat = proc_stat(&fs::read_to_string(format!("/proc/{pid}/stat")).ok()?)?;
        let ticks = sysconf(libc::_SC_CLK_TCK)? as f64;
        Some(Cpu {
            user_s: stat.user_ticks as f64 / ticks,
            system_s: stat.system_ticks as f64 / ticks,
            resident_bytes: stat
                .resident_pages
                .saturating_mul(sysconf(libc::_SC_PAGESIZE)?),
            pageins: stat.major_faults,
        })
    }

    /// Where `cpu` reads from, in words, for the result.
    pub fn cpu_clock() -> Option<String> {
        let ticks = sysconf(libc::_SC_CLK_TCK)?;
        Some(format!("/proc/<pid>/stat, {ticks} ticks a second"))
    }

    /// The step `cpu` counts in, in seconds.
    pub fn cpu_step_s() -> f64 {
        sysconf(libc::_SC_CLK_TCK).map_or(0.0, |ticks| 1.0 / ticks as f64)
    }

    /// One of the system's numbers, or `None` where it has none.
    fn sysconf(name: libc::c_int) -> Option<u64> {
        // SAFETY: `sysconf` takes a number and returns one, and reads no memory of this
        // process; a name it does not know is answered with -1.
        let value = unsafe { libc::sysconf(name) };
        u64::try_from(value).ok().filter(|value| *value > 0)
    }

    pub fn swap() -> Result<Option<Swap>, SpeedError> {
        match fs::read_to_string("/proc/vmstat") {
            Ok(text) => Ok(proc_vmstat_swap(&text)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(SpeedError::Host(format!(
                "the swap counter cannot be read, so the check that the machine is not \
                 swapping cannot pass: /proc/vmstat: {e}. --unchecked runs without the \
                 check, and marks the result as not a measurement"
            ))),
        }
    }

    pub fn load_average() -> Option<f64> {
        first_number(&fs::read_to_string("/proc/loadavg").ok()?)
    }

    pub fn memory_available() -> Option<u64> {
        meminfo_bytes(&fs::read_to_string("/proc/meminfo").ok()?, "MemAvailable")
    }

    /// Not read here: Linux machines differ in where they say what they draw from, and the
    /// spin reference is what shows a slower machine on this system.
    pub fn power() -> Option<Power> {
        None
    }

    pub fn version() -> Option<String> {
        let text = fs::read_to_string("/proc/sys/kernel/osrelease").ok()?;
        let text = text.trim();
        (!text.is_empty()).then(|| text.to_string())
    }

    /// The first processor's `model name`; `/proc/cpuinfo` has none on some
    /// architectures.
    pub fn cpu_name() -> Option<String> {
        let text = fs::read_to_string("/proc/cpuinfo").ok()?;
        text.lines().find_map(|line| {
            let (key, value) = line.split_once(':')?;
            (key.trim() == "model name").then(|| value.trim().to_string())
        })
    }

    /// Linux does not say which cores are which kind in a form the bench reads.
    pub fn core_kinds() -> (Option<usize>, Option<usize>) {
        (None, None)
    }

    pub fn memory() -> Option<u64> {
        meminfo_bytes(&fs::read_to_string("/proc/meminfo").ok()?, "MemTotal")
    }
}

/// A system the bench has no readers for: it runs, with no CPU time for a child - so no
/// scan rate - and no swap gate, and says both in its log.
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
mod os {
    use super::{Cpu, Power, SpeedError, Swap};

    pub const SWAP_COUNTER: Option<&str> = None;

    pub fn cpu(_pid: u32) -> Option<Cpu> {
        None
    }

    pub fn cpu_clock() -> Option<String> {
        None
    }

    pub fn cpu_step_s() -> f64 {
        0.0
    }

    pub fn swap() -> Result<Option<Swap>, SpeedError> {
        Ok(None)
    }

    pub fn load_average() -> Option<f64> {
        None
    }

    pub fn memory_available() -> Option<u64> {
        None
    }

    pub fn power() -> Option<Power> {
        None
    }

    pub fn version() -> Option<String> {
        None
    }

    pub fn cpu_name() -> Option<String> {
        None
    }

    pub fn core_kinds() -> (Option<usize>, Option<usize>) {
        (None, None)
    }

    pub fn memory() -> Option<u64> {
        None
    }
}

#[cfg(test)]
#[path = "host_tests.rs"]
mod tests;
