//! The machine the bench runs on: what it is, and whether it is fit to measure on.
//!
//! This file holds the machine's facts, a child's CPU time and resident size, the swap
//! counters, the look for other kibbr processes, the load average and available memory,
//! how the machine is powered, the two references - a spin on one core and a flush to the
//! disk, neither of which touches kibbr - the commit of the checkout, and the digest of
//! the binary.
//!
//! **Two gates and no more.** A run waits while another kibbr process is alive and while
//! the swap-out counter moves; a unit that either disturbed is thrown away and done again.
//! Everything else here is printed and recorded, and decides nothing: a gate this machine
//! fails at rest would stop the one run that cannot be taken again.
//!
//! **A child's CPU time is checked before it is believed.** The scan rate is rows over the
//! CPU-seconds a server used, and on macOS the system counts those in ticks of its own
//! clock, which are not nanoseconds: read as nanoseconds they are 41.7 times out on this
//! machine and every scan rate with them. So on every start this process spins, and what
//! the system says it used by the child's reading must agree with what `getrusage` says.
//!
//! **Programs and files, not calls, wherever a program or a file will do.** `ps`,
//! `vm_stat`, `sysctl`, `pmset`, `df` and `git` are run as children and `/proc` is read as
//! text; what they print is parsed by functions that take the text, so each is tested on
//! text pasted from a real machine. Each child is run with `LC_ALL=C`: `sysctl` prints a
//! load average with the locale's decimal sign. The only `unsafe` in the bench is here -
//! the calls std has none for: a process's CPU time by pid, the clock those ticks are in,
//! `getrusage`, and `sysconf` for the tick `/proc` counts in.

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

/// Rounds of the spin reference: about a quarter of a second on a performance core of the
/// machine this was written on (267 ms, optimised), long enough that the scheduler's
/// choice of core shows and short enough to take three.
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

/// Pages swapped in and out since boot. Only a difference between two reads means
/// anything, and only `outs` is a gate.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Swap {
    pub ins: u64,
    pub outs: u64,
}

// ── A process's CPU time ───────────────────────────────────────────────────────────

/// The CPU time and memory of process `pid`, or `None` where this system has no source
/// the bench reads, or the process is gone.
///
/// The whole process, every thread of it, since it started: what a server used over a
/// block of asks is the difference of two reads. On Linux the times come in ticks of
/// 10 ms, so a short block can read as no CPU at all.
pub fn cpu(pid: u32) -> Option<Cpu> {
    os::cpu(pid)
}

/// Hold `cpu` to `getrusage`: spin until this process has used 200 ms of CPU by
/// `getrusage`, and return the same spin by `cpu(own pid)` over it. Outside 2% of 1 is
/// `SpeedError::Host` with both numbers - a tick count read as nanoseconds is 41.7 times
/// out. Against `getrusage` and not the wall clock, so a busy machine cannot fail it.
/// `Ok(None)` where `cpu` has no source.
///
/// Where the source counts in coarse ticks, two of them are allowed on top of the 2%:
/// over 200 ms a clock of 10 ms steps is off by up to a tenth with nothing wrong, and
/// what the check is for - a unit read as another - is out by a factor, not a tenth.
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
             would be out by as much, so nothing was measured; this is a fault in how kibbr \
             bench reads this system's clock, and waiting does not mend it",
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

/// The swap counters. `Ok(None)`: this system has no counter the bench reads, which is
/// said in the log and is not a gate. A counter that is there and cannot be read is
/// `SpeedError::Host`: a gate that cannot be read cannot pass.
pub fn swap() -> Result<Option<Swap>, SpeedError> {
    os::swap()
}

/// The pids of every process whose command's file name is exactly `kibbr`, less this
/// process and its children. A process list that cannot be read is `SpeedError::Host`:
/// without it the check cannot pass.
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

/// The `kibbr` processes in what `ps` printed, less process `own` and its children - the
/// servers and the plan step this bench started itself. `None` when `own` is not in the
/// list: a list this process is missing from was not read the way it is written, and
/// would say "no other kibbr" of any machine.
///
/// The name is held exactly: `kibbr-helper` is another program, and so is a test binary,
/// which cargo names `kibbr-<hash>`. A process that has exited and not been reaped prints
/// in brackets or with `<defunct>` after it, and is not counted - it uses no CPU.
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

/// The 1-minute load average. Printed, never a gate.
pub fn load_average() -> Option<f64> {
    os::load_average()
}

/// Memory an application could be given without swapping, in bytes. Printed, never a
/// gate.
pub fn memory_available() -> Option<u64> {
    os::memory_available()
}

/// How the machine is powered at this moment: what it draws from, and whether Low Power
/// Mode is on. Printed and recorded beside the references, never a gate: it says why a
/// spin read slower, and a run on a battery is still a run. `None` on a system the bench
/// has no reading for, or one whose `pmset` could not be run.
pub fn power() -> Option<Power> {
    os::power()
}

/// The spin reference: a fixed count of rounds of arithmetic on one thread, the least of
/// three. It is the bench's own code and reads the same on every commit, so a run that
/// was throttled, or put on the efficiency cores, shows here.
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

/// `rounds` rounds of xorshift64 from `x`. Each round's value goes through `black_box`,
/// so that the loop is the same loads, shifts and stores whatever a later compiler could
/// make of it: a reference that a compiler upgrade made faster would read as a cooler
/// machine.
fn spin_rounds(mut x: u64, rounds: u64) -> u64 {
    for _ in 0..rounds {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x = black_box(x);
    }
    x
}

/// The flush reference, the same idea for the disk: `FLUSH_REF_WRITES` writes of
/// `FLUSH_REF_BYTES` bytes to one file in `dir`, each followed by `sync_all`, timed as
/// one sum; the least of three. The file is removed.
///
/// `sync_all` because that is what the server's log calls after each body, and on macOS
/// it is the full flush that waits for the disk. Making the file is outside the clock:
/// the reference is the flushes, which are what a sender's cycle waits on.
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

/// What the machine is. `disk_free_bytes` and `cpu_clock_check` are left for the caller,
/// from `disk_free` and `cpu_clock_check`: the one needs a path and the other can fail.
///
/// `cores_logical` is what a server started with no `--ingest-concurrency` would take,
/// by the same call, and is what every server of the bench is given by name.
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
/// `df -Pk` prints as available, which is what a writer that is not root can take.
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

/// `dir`, or the nearest directory above it that exists, as a whole path: a `--dir` that
/// is yet to be made is on the volume of the directory it will be made in. A whole path,
/// so that a name beginning with `-` is not read by `df` as a flag.
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

/// The available bytes in what `df -Pk` printed for one path: the number before the
/// capacity, which is the one column that ends in `%`. Found from that column and not by
/// counting from the left, because a file system's name may hold spaces.
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

/// The checkout the bench runs in: what git says of the working directory. Without git,
/// or outside a checkout, every field but `how` is `None`.
///
/// `src_dirty` asks about `src`, `Cargo.toml` and `Cargo.lock` at the checkout's root and
/// nothing else: over the whole tree it is true for a file manager's leavings, and would
/// be true of every result. It cannot see a file git does not track yet.
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

/// The binary at `exe`: its length, CRC-32C and modification time, whether this is a
/// release build, and whether it is older than `commit`.
///
/// The commit is the checkout's and the binary may have been built before it was made;
/// `stale` is the one sign of that a result can carry.
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

/// Whether the binary is still the bytes `was` recorded. A build beside the bench
/// replaces the binary being measured; that is `SpeedError::Host`.
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
/// the free, inactive, speculative and purgeable pages, at the page size its first line
/// names.
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
/// `lowpowermode` among the settings in use, as in ` lowpowermode         0`. `None` where
/// the system names no such setting.
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

/// Read `/proc/<pid>/stat`. Its second field is the command's name in brackets, which
/// may itself hold spaces and brackets, so the fields are counted from the last `)`: the
/// one after it is the third. Major faults are the 12th, user and system time the 14th
/// and 15th, the resident pages the 24th.
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
    /// 125/3 on Apple silicon, 1/1 on Intel. Asked once; it does not change while the
    /// system is up.
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
        // SAFETY: `info` is room for one `rusage_info_v2`, the structure the call writes
        // for `RUSAGE_INFO_V2`. The call declares its buffer as a pointer to a
        // `rusage_info_t`, itself a pointer to anything, which is why the pointer is
        // cast and the structure is not. It fills the buffer when it returns 0 - the
        // only case in which it is read.
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

    /// The step `cpu` counts in, in seconds: 42 ns here, which is nothing beside 2%.
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
    /// source, and the battery's status names the source and not the mode. A half that
    /// could not be read is `None` beside the other; with neither there is no reading.
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

    /// Performance and efficiency cores, where the machine has the two kinds; an Intel
    /// Mac names neither.
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

    /// The step `cpu` counts in, in seconds: 10 ms on every kernel anyone runs.
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

    /// Not read here: where a Linux machine says what it draws from, and whether it is
    /// holding its cores back, differs from one to the next. The spin reference is what
    /// shows a slower machine on this system.
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
mod tests {
    use super::*;

    /// A scratch directory of one test, removed when the test ends.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            let root = std::env::temp_dir()
                .join(format!("kibbr-speed-host-{name}-{}", std::process::id()));
            let _ = fs::remove_dir_all(&root);
            fs::create_dir_all(&root).expect("create the scratch directory");
            Self(root)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    /// `vm_stat` on the machine the bench was written on, as it printed it.
    const VM_STAT: &str = "\
Mach Virtual Memory Statistics: (page size of 16384 bytes)
Pages free:                               35829.
Pages active:                            175543.
Pages inactive:                          159227.
Pages speculative:                        15547.
Pages throttled:                              0.
Pages wired down:                         78171.
Pages purgeable:                           1151.
\"Translation faults\":                 702124312.
Pages copy-on-write:                   21608195.
Pages zero filled:                    211922291.
Pages reactivated:                    120544197.
Pages purged:                           2849898.
File-backed pages:                       167929.
Anonymous pages:                         182388.
Pages stored in compressor:              107195.
Pages occupied by compressor:             23695.
Decompressions:                       150492297.
Compressions:                         171111890.
Pageins:                               19897118.
Pageouts:                                912630.
Swapins:                                7391248.
Swapouts:                               7983780.
";

    #[test]
    fn the_cpu_time_read_by_pid_is_the_cpu_time_getrusage_counts() {
        // The test that catches a tick of another clock read as a nanosecond: this
        // process spins for 200 ms of its own CPU time, and the two sources must agree.
        let ratio = cpu_clock_check().expect("the two clocks agree");
        if cfg!(target_os = "macos") {
            let ratio = ratio.expect("macOS has a source for a process's CPU time");
            assert!(
                (ratio - 1.0).abs() <= 0.02,
                "{ratio} of getrusage, where 0.98 to 1.02 is allowed"
            );
        }
        if cfg!(any(target_os = "macos", target_os = "linux")) {
            let own = cpu(std::process::id()).expect("this process can be read");
            assert!(own.total_s() >= CLOCK_CHECK_S, "{own:?}");
            assert!(own.resident_bytes > 0, "{own:?}");
        }
    }

    #[test]
    fn a_process_that_is_not_there_has_no_cpu_time() {
        // No system gives a process this number: pids stop far below it.
        assert_eq!(cpu(u32::MAX), None);
        assert_eq!(cpu(i32::MAX as u32 - 1), None);
    }

    #[test]
    fn mach_ticks_are_scaled_by_the_timebase_and_are_not_nanoseconds() {
        // 125/3 ns a tick, as on Apple silicon: 24,000,000 ticks are one second.
        assert_eq!(mach_seconds(24_000_000, 125, 3), 1.0);
        assert_eq!(mach_seconds(1_000_000_000, 1, 1), 1.0);
        assert_eq!(mach_seconds(0, 125, 3), 0.0);
        // A server that has used a month of CPU is still counted to the tick.
        let month = 30 * 86_400 * 24_000_000_u64;
        assert_eq!(mach_seconds(month + 1, 125, 3), 2_592_000.000_000_041);
    }

    #[test]
    fn proc_stat_is_read_from_the_last_bracket_whatever_the_command_is_called() {
        let stat = "4242 (kibbr) S 4000 4242 4000 34816 4242 4194304 1201 0 7 0 1534 289 0 0 \
                    20 0 12 0 8841212 912345088 20480 18446744073709551615 1 1 0 0 0 0 0 4096 \
                    17922 0 0 0 17 3 0 0 0 0 0";
        let want = ProcStat {
            major_faults: 7,
            user_ticks: 1534,
            system_ticks: 289,
            resident_pages: 20480,
        };
        assert_eq!(proc_stat(stat), Some(want));

        // A name with a space and a closing bracket in it, and numbers of its own.
        let odd = stat.replace("(kibbr)", "(my) 1 2 3 4 5 6 7 8 9 10 11 12 13 k)");
        assert_eq!(proc_stat(&odd), Some(want));

        assert_eq!(proc_stat(""), None);
        assert_eq!(proc_stat("4242 (kibbr) S 4000"), None);
        assert_eq!(proc_stat("no bracket at all"), None);
    }

    #[test]
    fn swap_counts_are_read_from_vm_stat_as_it_prints_them() {
        assert_eq!(
            vm_stat_swap(VM_STAT),
            Some(Swap {
                ins: 7_391_248,
                outs: 7_983_780
            })
        );
        assert_eq!(vm_stat_count(VM_STAT, "Pageins"), Some(19_897_118));
        assert_eq!(
            vm_stat_count(VM_STAT, "Translation faults"),
            Some(702_124_312)
        );
        // `Pages free` is not `Pages`, and `Swapouts` is not `Swapout`.
        assert_eq!(vm_stat_count(VM_STAT, "Pages"), None);
        assert_eq!(vm_stat_count(VM_STAT, "Swapout"), None);
        // A system whose `vm_stat` names no swap-outs has no counter, and is not a gate.
        assert_eq!(vm_stat_swap("Pages free:   12.\nSwapins:  3.\n"), None);
        assert_eq!(vm_stat_swap(""), None);
    }

    #[test]
    fn available_memory_is_four_kinds_of_page_at_the_size_vm_stat_names() {
        assert_eq!(
            vm_stat_available(VM_STAT),
            Some((35_829 + 159_227 + 15_547 + 1_151) * 16_384)
        );
        assert_eq!(vm_stat_available("Pages free:  12.\n"), None);
    }

    /// `pmset -g` on the machine the bench was written on, on mains power with Low Power
    /// Mode off, as it printed it.
    const PMSET: &str = "\
System-wide power settings:
Currently in use:
 standby              1
 Sleep On Power Button 1
 hibernatefile        /var/vm/sleepimage
 powernap             1
 networkoversleep     0
 disksleep            10
 sleep                0 (sleep prevented by caffeinate, powerd)
 hibernatemode        3
 ttyskeepawake        1
 displaysleep         90
 tcpkeepalive         1
 lowpowermode         0
 womp                 1
";

    /// `pmset -g batt` on the same machine at the same moment, as it printed it.
    const PMSET_BATT: &str = "\
Now drawing from 'AC Power'
 -InternalBattery-0 (id=7929955)\t94%; charging; 0:33 remaining present: true
";

    /// Both again on the same machine on its battery in Low Power Mode, as it printed
    /// them: the state in which the spin reference takes 1.7 times as long.
    const PMSET_LOW: &str = "\
System-wide power settings:
Currently in use:
 standby              1
 Sleep On Power Button 1
 hibernatefile        /var/vm/sleepimage
 powernap             1
 networkoversleep     0
 disksleep            10
 sleep                1 (sleep prevented by powerd, sharingd, useractivityd, coreaudiod, Google Chrome, caffeinate)
 hibernatemode        3
 ttyskeepawake        1
 displaysleep         180
 tcpkeepalive         1
 lowpowermode         1
 womp                 0
";
    const PMSET_BATT_LOW: &str = "\
Now drawing from 'Battery Power'
 -InternalBattery-0 (id=7929955)\t97%; discharging; 9:59 remaining present: true
";

    #[test]
    fn the_machine_on_its_battery_in_low_power_mode_reads_as_both() {
        assert_eq!(pmset_low_power(PMSET_LOW), Some(true));
        assert_eq!(
            pmset_source(PMSET_BATT_LOW).as_deref(),
            Some("Battery Power")
        );
        // Neither text is a reading of the other half.
        assert_eq!(pmset_low_power(PMSET_BATT_LOW), None);
        assert_eq!(pmset_source(PMSET_LOW), None);
    }

    #[test]
    fn low_power_mode_is_the_number_beside_its_name_among_the_settings_in_use() {
        assert_eq!(pmset_low_power(PMSET), Some(false));
        // The same text with the one digit Low Power Mode changes.
        let on = PMSET.replace("lowpowermode         0", "lowpowermode         1");
        assert_ne!(on, PMSET);
        assert_eq!(pmset_low_power(&on), Some(true));
        // The name is a whole word, and the first on its line: `powernap 1` and
        // `hibernatemode 3` are other settings, and so would `lowpowermodeschedule 1` be.
        let other = PMSET.replace("lowpowermode ", "lowpowermodeschedule ");
        assert_eq!(pmset_low_power(&other), None);
        // A Mac that names no such setting has no reading, and neither does a value that
        // is not one of the two.
        let without: String = PMSET
            .lines()
            .filter(|line| !line.contains("lowpowermode"))
            .map(|line| format!("{line}\n"))
            .collect();
        assert_eq!(pmset_low_power(&without), None);
        assert_eq!(pmset_low_power(" lowpowermode         2\n"), None);
        assert_eq!(pmset_low_power(" lowpowermode\n"), None);
        assert_eq!(pmset_low_power(""), None);
    }

    #[test]
    fn the_power_source_is_the_name_in_quotes_as_the_system_spells_it() {
        assert_eq!(pmset_source(PMSET_BATT).as_deref(), Some("AC Power"));
        // The same text as a machine on its battery prints it: another name in the
        // quotes, and `discharging` where it charged.
        let battery = PMSET_BATT
            .replace("'AC Power'", "'Battery Power'")
            .replace("charging", "discharging");
        assert_eq!(pmset_source(&battery).as_deref(), Some("Battery Power"));
        // A Mac with no battery prints the first line alone.
        assert_eq!(
            pmset_source("Now drawing from 'AC Power'\n").as_deref(),
            Some("AC Power")
        );
        // What is not that line, or names nothing, is no reading.
        assert_eq!(
            pmset_source(" -InternalBattery-0 (id=7929955)\t94%; charging\n"),
            None
        );
        assert_eq!(pmset_source("Now drawing from ''\n"), None);
        assert_eq!(pmset_source("Now drawing from 'AC Power\n"), None);
        assert_eq!(pmset_source(PMSET), None);
        assert_eq!(pmset_source(""), None);
    }

    #[test]
    fn the_power_state_of_this_machine_is_read_where_the_system_has_one() {
        let power = power();
        if cfg!(target_os = "macos") {
            let power = power.expect("pmset can be run on macOS");
            let source = power
                .source
                .expect("pmset names what the machine draws from");
            assert!(!source.is_empty());
        } else {
            assert_eq!(power, None, "no other system is read");
        }
    }

    #[test]
    fn swap_counts_are_read_from_proc_vmstat() {
        let vmstat = "nr_free_pages 123456\npgpgin 5512\npgpgout 99120\npswpin 17\n\
                      pswpout 4021\npgalloc_dma 0\n";
        assert_eq!(
            proc_vmstat_swap(vmstat),
            Some(Swap {
                ins: 17,
                outs: 4_021
            })
        );
        // `pswpout` is a whole word: `pswpout_extra 9` is another counter.
        assert_eq!(proc_vmstat_swap("pswpin 1\npswpout_extra 9\n"), None);
        assert_eq!(proc_vmstat_swap("nr_free_pages 1\n"), None);
    }

    #[test]
    fn memory_is_read_from_proc_meminfo_in_bytes() {
        let meminfo = "MemTotal:       16303428 kB\nMemFree:         1203220 kB\n\
                       MemAvailable:    9876544 kB\nSwapTotal:             0 kB\n";
        assert_eq!(meminfo_bytes(meminfo, "MemTotal"), Some(16_303_428 * 1024));
        assert_eq!(
            meminfo_bytes(meminfo, "MemAvailable"),
            Some(9_876_544 * 1024)
        );
        assert_eq!(meminfo_bytes(meminfo, "Mem"), None);
    }

    #[test]
    fn the_load_average_is_the_first_number_in_either_spelling() {
        assert_eq!(first_number("{ 1.75 1.81 1.70 }\n"), Some(1.75));
        assert_eq!(first_number("0.20 0.18 0.12 1/80 11206\n"), Some(0.20));
        assert_eq!(first_number("{ }"), None);
    }

    #[test]
    fn another_kibbr_is_one_named_exactly_that_which_this_process_did_not_start() {
        let ps = "\
    1     0 /sbin/launchd
  107     1 /usr/libexec/logd
 4000   900 /Users/someone/repo/kibbr/target/release/kibbr
 4001  4000 /Users/someone/repo/kibbr/target/release/kibbr
 4002  4000 /Users/someone/repo/kibbr/target/release/kibbr
 5000     1 /Users/someone/repo/kibbr/target/release/kibbr
 5001  5000 /Users/someone/repo/kibbr/target/release/kibbr
 6000     1 kibbr
 6100     1 /opt/tools/kibbr-helper
 6200     1 /Users/someone/repo/kibbr/target/debug/deps/kibbr-0f3c2a91d5e6b7a8
 6300     1 /Applications/Some App.app/Contents/MacOS/Some App
 6400     1 /Users/someone/my tools/kibbr
 6500     1 (kibbr)
 6600     1 /Users/someone/repo/kibbr/scripts/check/agree.sh
";
        // 4000 is this process; 4001 and 4002 are its servers. 5000 is another bench and
        // 5001 that one's server; 6000 is a kibbr started by name, 6400 one under a
        // directory with a space in its name.
        assert_eq!(others(ps, 4000), Some(vec![5000, 5001, 6000, 6400]));
        // Seen from the other bench, this one and its two servers are the others.
        assert_eq!(others(ps, 5000), Some(vec![4000, 4001, 4002, 6000, 6400]));
        // A list this process is not in was not read right, and proves nothing.
        assert_eq!(others(ps, 7777), None);
        assert_eq!(others("", 4000), None);
    }

    #[test]
    fn a_row_of_ps_is_two_numbers_and_the_rest_of_the_line() {
        assert_eq!(
            ps_row("  812     1 /Applications/Some App.app/Contents/MacOS/Some App  "),
            Some((812, 1, "/Applications/Some App.app/Contents/MacOS/Some App"))
        );
        assert_eq!(ps_row("812 1 kibbr"), Some((812, 1, "kibbr")));
        assert_eq!(ps_row("  PID  PPID COMM"), None);
        assert_eq!(ps_row("812 1"), None);
        assert_eq!(ps_row(""), None);
    }

    #[test]
    fn the_process_list_of_this_machine_can_be_read_and_holds_this_process() {
        // The test binary is `kibbr-<hash>`, which is not `kibbr`: whatever the answer
        // holds, it is not this process.
        let found = other_kibbr().expect("ps can be run and lists this process");
        assert!(!found.contains(&std::process::id()), "{found:?}");
    }

    #[test]
    fn free_space_is_the_number_before_the_capacity_whatever_the_volume_is_called() {
        let df = "Filesystem   1024-blocks      Used Available Capacity  Mounted on\n\
                  /dev/disk3s5   482797652 420655560  22611024    95%    /System/Volumes/Data\n";
        assert_eq!(df_available_bytes(df), Some(22_611_024 * 1024));

        let spaced = "Filesystem 1024-blocks Used Available Capacity Mounted on\n\
                      //someone@nas/my share 1000 400 600 40% /Volumes/my share\n";
        assert_eq!(df_available_bytes(spaced), Some(600 * 1024));

        // A name that ends in `%` is not the capacity: three numbers do not stand before it.
        let odd = "Filesystem 1024-blocks Used Available Capacity Mounted on\n\
                   map 50% 2000 1500 500 75% /net\n";
        assert_eq!(df_available_bytes(odd), Some(500 * 1024));

        assert_eq!(df_available_bytes("Filesystem 1024-blocks Used\n"), None);
        assert_eq!(df_available_bytes(""), None);
    }

    #[test]
    fn free_space_of_a_directory_not_yet_made_is_that_of_the_one_it_will_be_made_in() {
        let scratch = Scratch::new("free");
        let there = disk_free(&scratch.0).expect("df can be run");
        assert!(there > 0);
        let deep = scratch.0.join("not/made/yet");
        assert_eq!(nearest_existing(&deep), nearest_existing(&scratch.0));
        assert!(disk_free(&deep).expect("df can be run on the parent") > 0);
        // A relative name with nothing above it is in the working directory.
        assert_eq!(
            nearest_existing(Path::new("no-such-directory-of-kibbr-bench")),
            nearest_existing(Path::new("."))
        );
    }

    #[test]
    fn the_spin_is_the_same_arithmetic_in_every_build() {
        // The reference is only a reference while its rounds are the same work: these
        // values move if the loop does.
        assert_eq!(spin_rounds(SPIN_SEED, 0), SPIN_SEED);
        assert_eq!(spin_rounds(1, 1), 0x4082_2041);
        assert_eq!(
            spin_rounds(SPIN_SEED, 1_000),
            spin_rounds(spin_rounds(SPIN_SEED, 400), 600)
        );
        assert_eq!(spin_rounds(SPIN_SEED, 1_000), 0x56b6_6321_9f6e_38f5);
        assert_eq!(SPIN_ROUNDS, 134_217_728);
    }

    #[test]
    fn the_flush_reference_takes_time_and_leaves_no_file_behind() {
        let scratch = Scratch::new("flush");
        let took = flush_ref(&scratch.0).expect("the scratch directory can be written");
        assert!(took > Duration::ZERO);
        let left: Vec<_> = fs::read_dir(&scratch.0)
            .expect("list the scratch directory")
            .collect();
        assert!(left.is_empty(), "{left:?}");

        // A directory that is not there is an error, and still nothing is left.
        let missing = scratch.0.join("missing");
        assert!(flush_ref(&missing).is_err());
        assert!(!missing.exists());
    }

    #[test]
    fn the_machine_says_what_it_is_and_where_its_numbers_come_from() {
        let machine = machine();
        assert_eq!(machine.os, std::env::consts::OS);
        assert_eq!(machine.arch, std::env::consts::ARCH);
        assert!(machine.cores_logical >= 1);
        assert_eq!(machine.disk_free_bytes, 0, "left for the caller");
        assert_eq!(machine.cpu_clock_check, None, "left for the caller");
        if cfg!(target_os = "macos") {
            assert!(machine.cpu.is_some(), "{machine:?}");
            assert!(machine.os_version.is_some(), "{machine:?}");
            assert!(machine.memory_bytes.is_some_and(|bytes| bytes > 0));
            let clock = machine.cpu_clock.expect("macOS has a source");
            assert!(
                clock.starts_with("proc_pid_rusage, mach timebase "),
                "{clock}"
            );
            assert_eq!(machine.swap_counter.as_deref(), Some("vm_stat"));
            assert!(load_average().is_some_and(|load| load >= 0.0));
            assert!(memory_available().is_some_and(|bytes| bytes > 0));
            assert!(swap().expect("vm_stat can be run").is_some());
        }
        // Two looks at one machine are one machine: a result is compared on these.
        let again = super::machine();
        assert_eq!(
            (&machine.cpu, machine.cores_logical, machine.memory_bytes),
            (&again.cpu, again.cores_logical, again.memory_bytes)
        );
    }

    #[test]
    fn a_binary_that_was_built_again_is_not_the_one_that_started() {
        let scratch = Scratch::new("binary");
        let exe = scratch.0.join("kibbr");
        fs::write(&exe, b"the build that started").expect("write the stand-in binary");

        let commit = Commit {
            sha: Some("1bfe096e45ef729cb24d60eb80f6d2267c76d082".to_string()),
            src_dirty: Some(false),
            // Long before the file was written: the binary is the newer of the two.
            committed_at: Some("2001-01-01T00:00:00+05:30".to_string()),
            how: HOW.to_string(),
        };
        let was = binary(&exe, &commit).expect("the stand-in can be read");
        assert_eq!(was.bytes, 22);
        assert_eq!(
            was.crc32c,
            crc_hex(crc32c::crc32c(b"the build that started"))
        );
        assert_eq!(was.path, exe.display().to_string());
        assert_eq!(was.stale, Some(false));
        assert!(was.build == "debug" || was.build == "release");
        let modified = was.modified_at.clone().expect("the file has a time");
        assert!(
            DateTime::parse_from_rfc3339(&modified).is_ok(),
            "{modified}"
        );
        binary_unchanged(&was).expect("nothing has touched it");

        // A commit made after the binary was built: the sha is not the binary's.
        let later = Commit {
            committed_at: Some("2999-01-01T00:00:00Z".to_string()),
            ..commit.clone()
        };
        assert_eq!(binary(&exe, &later).expect("read").stale, Some(true));
        // No commit time, no verdict.
        assert_eq!(binary(&exe, &Commit::default()).expect("read").stale, None);

        // The same length, other bytes: the length alone would not have seen it.
        fs::write(&exe, b"the build that came in").expect("write over the stand-in");
        let told = binary_unchanged(&was).expect_err("it changed").to_string();
        assert!(
            told.starts_with("the binary changed while it was being measured"),
            "{told}"
        );
        assert!(told.contains("is now 22 bytes"), "{told}");

        fs::remove_file(&exe).expect("remove the stand-in");
        let told = binary_unchanged(&was).expect_err("it is gone").to_string();
        assert!(told.contains("cannot be read now"), "{told}");
        assert!(matches!(binary(&exe, &commit), Err(SpeedError::Io { .. })));
    }

    #[test]
    fn the_commit_is_what_git_says_or_nothing_with_the_reason() {
        let commit = commit();
        match &commit.sha {
            Some(sha) => {
                assert_eq!(sha.len(), 40, "{sha}");
                assert!(sha.bytes().all(|b| b.is_ascii_hexdigit()), "{sha}");
                assert_eq!(commit.how, HOW);
                let at = commit.committed_at.as_deref().expect("a commit has a time");
                assert!(DateTime::parse_from_rfc3339(at).is_ok(), "{at}");
            }
            None => {
                assert_eq!((commit.src_dirty, &commit.committed_at), (None, &None));
                assert!(commit.how.contains("named no commit"), "{}", commit.how);
            }
        }
    }
}
