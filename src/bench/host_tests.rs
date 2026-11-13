// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;

/// A scratch directory of one test, removed when the test ends.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let root =
            std::env::temp_dir().join(format!("kibbr-speed-host-{name}-{}", std::process::id()));
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

/// Both again on the same machine on its battery in Low Power Mode, as it printed them.
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
    // 4000 is this process, 4001 and 4002 its servers; 5000 another bench, 5001 its server;
    // 6000 a kibbr started by name, 6400 one under a directory with a space in its name.
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
