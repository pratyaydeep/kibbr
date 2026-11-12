// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::server::settings::duration_words;

/// Variables as a test found them, cleared for it and put back when it ends - one such
/// test at a time, because the variables are the process's.
struct Cleared(
    Vec<(&'static str, Option<String>)>,
    #[allow(dead_code)] std::sync::MutexGuard<'static, ()>,
);

static ONE_AT_A_TIME: std::sync::Mutex<()> = std::sync::Mutex::new(());

impl Cleared {
    fn of(vars: &[&'static str]) -> Self {
        // Through a poisoning: a test that failed holding it has put its variables
        // back in `drop`, and the next one clears its own.
        let turn = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
        let kept = vars.iter().map(|var| (*var, std::env::var(var).ok()));
        let kept: Vec<_> = kept.collect();
        for (var, _) in &kept {
            std::env::remove_var(var);
        }
        Self(kept, turn)
    }
}

impl Drop for Cleared {
    fn drop(&mut self) {
        for (var, was) in &self.0 {
            match was {
                Some(value) => std::env::set_var(var, value),
                None => std::env::remove_var(var),
            }
        }
    }
}

/// One test, because the variables are the process's: nothing else in this crate's
/// tests reads these two, and this sets and clears them in turn.
/// `kibbr.toml.example` is held to this reader: a key added here and not there, or
/// renamed, fails this and not an operator's first start.
#[test]
fn the_example_file_is_read_and_sets_every_key() {
    let text = include_str!("../../../kibbr.toml.example");
    let file = File::parse(text).expect("kibbr.toml.example is read");
    let all = format!("{file:?}");
    assert!(!all.contains("None"), "a key the example leaves out: {all}");
}

#[test]
fn scan_threads_are_the_flag_then_the_variable_then_the_default_and_never_above_the_cores() {
    let _cleared = Cleared::of(&[SCAN_THREADS_ENV, SCAN_FAN_ROWS_ENV]);
    let cores = cores();
    let scan = |threads: Option<usize>, fan_rows: Option<usize>| {
        Storage::default()
            .with_scan(&File::default(), threads, fan_rows)
            .map(|storage| (storage.scan_threads, storage.scan_fan_rows))
    };

    // Nobody but `with_scan` asks for a thread: a load and a compaction have none.
    let untouched = Storage::default();
    assert_eq!(
        (untouched.scan_threads, untouched.scan_fan_rows),
        (0, crate::engine::database::DEFAULT_FAN_ROWS)
    );
    assert_eq!(untouched.disk_options().scan_threads, 0);

    // The default: half the cores, at least one, at most four, never above the cores.
    let default = default_scan_threads();
    assert!(
        (1..=4).contains(&default) && default <= cores,
        "{default} of {cores}"
    );
    assert_eq!(default, (cores / 2).clamp(1, 4));
    assert_eq!(
        scan(None, None).expect("the defaults"),
        (default, crate::engine::database::DEFAULT_FAN_ROWS)
    );

    // The variable over the default, and the flag over the variable.
    std::env::set_var(SCAN_THREADS_ENV, "1");
    std::env::set_var(SCAN_FAN_ROWS_ENV, " 4096 ");
    assert_eq!(scan(None, None).expect("the variables"), (1, 4096));
    assert_eq!(scan(Some(0), None).expect("a flag"), (0, 4096));
    assert_eq!(scan(None, Some(0)).expect("a flag"), (1, 0));
    assert_eq!(scan(Some(cores), Some(7)).expect("both flags"), (cores, 7));

    // An unreadable variable is refused, unless the flag that overrides it is given.
    std::env::set_var(SCAN_THREADS_ENV, "four");
    assert!(matches!(
        scan(None, Some(7)),
        Err(ConfigError::BadEnv { var, value }) if var == SCAN_THREADS_ENV && value == "four"
    ));
    assert_eq!(scan(Some(1), None).expect("the flag"), (1, 4096));
    std::env::set_var(SCAN_THREADS_ENV, "1");
    std::env::set_var(SCAN_FAN_ROWS_ENV, "-1");
    assert!(matches!(
        scan(None, None),
        Err(ConfigError::BadEnv { var, .. }) if var == SCAN_FAN_ROWS_ENV
    ));
    assert_eq!(scan(None, Some(9)).expect("the flag"), (1, 9));
    std::env::remove_var(SCAN_FAN_ROWS_ENV);

    // As many threads as there are cores are taken; one more is refused, by the flag
    // and by the variable, and the refusal names both numbers. None and one are taken.
    std::env::remove_var(SCAN_THREADS_ENV);
    for few in [0, 1, cores] {
        assert_eq!(scan(Some(few), None).expect("taken").0, few);
        std::env::set_var(SCAN_THREADS_ENV, few.to_string());
        assert_eq!(scan(None, None).expect("taken").0, few);
        std::env::remove_var(SCAN_THREADS_ENV);
    }
    let refused = |got: Result<(usize, u64), ConfigError>, asked: usize| match got {
        Err(e @ ConfigError::ScanThreadsAboveCores { .. }) => {
            let said = e.to_string();
            assert!(
                said.contains(&format!(
                    "scan threads ({asked}) is above the {cores} core(s)"
                )),
                "{said}"
            );
        }
        other => panic!("{asked} threads on {cores} cores: {other:?}"),
    };
    for many in [cores + 1, 512, 100_000] {
        refused(scan(Some(many), None), many);
        std::env::set_var(SCAN_THREADS_ENV, many.to_string());
        refused(scan(None, None), many);
        // And the flag, when it is in range, is not refused for the variable.
        assert_eq!(scan(Some(1), None).expect("the flag").0, 1);
        std::env::remove_var(SCAN_THREADS_ENV);
    }

    // What a server opens its disk store with is what was resolved.
    let options = Storage::default()
        .with_scan(&File::default(), Some(1), Some(12))
        .expect("taken")
        .disk_options();
    assert_eq!((options.scan_threads, options.fan_rows), (1, 12));
}

/// One test for the two, because the variables are the process's; the five beside them
/// are cleared too, so a shell with one set to something unreadable does not fail this.
#[test]
fn the_ingest_wait_and_the_logs_bound_are_the_flag_then_the_variable_then_the_default() {
    let _cleared = Cleared::of(&[
        INGEST_WAIT_ENV,
        WAL_QUEUE_BYTES_ENV,
        WAL_SEGMENT_BYTES_ENV,
        BATCH_SIZE_ENV,
        MAX_BODY_BYTES_ENV,
        MAX_DECOMPRESSED_BYTES_ENV,
        INGEST_CONCURRENCY_ENV,
    ]);
    let wait = |flag: Option<usize>| {
        ApiLimits::default()
            .with_ingest_wait(&File::default(), flag)
            .map(|api| api.ingest_wait)
    };
    let ms = |ms: usize| Duration::from_millis(ms as u64);

    // Nobody but `with_ingest_wait` reads the variable: `load` gives the default.
    std::env::set_var(INGEST_WAIT_ENV, "7");
    let loaded = ApiLimits::load(&File::default(), None, None, None).expect("the defaults");
    assert_eq!(loaded.ingest_wait, ms(DEFAULT_INGEST_WAIT_MS));
    assert_eq!(ApiLimits::default().ingest_wait, ms(100));
    std::env::remove_var(INGEST_WAIT_ENV);

    assert_eq!(wait(None).expect("the default"), ms(100));
    std::env::set_var(INGEST_WAIT_ENV, " 250 ");
    assert_eq!(wait(None).expect("the variable"), ms(250));
    assert_eq!(wait(Some(5)).expect("the flag"), ms(5));
    // No wait at all is a setting, by the flag and by the variable.
    assert_eq!(wait(Some(0)).expect("the flag"), Duration::ZERO);
    std::env::set_var(INGEST_WAIT_ENV, "0");
    assert_eq!(wait(None).expect("the variable"), Duration::ZERO);

    // Thirty seconds is taken, and a millisecond more is refused with both numbers.
    std::env::remove_var(INGEST_WAIT_ENV);
    assert_eq!(wait(Some(MAX_INGEST_WAIT_MS)).expect("taken"), ms(30_000));
    let refused = |got: Result<Duration, ConfigError>| match got {
        Err(e @ ConfigError::IngestWaitTooLong { .. }) => {
            let said = e.to_string();
            assert!(
                said.contains("ingest wait (30001ms) is above the 30000ms limit"),
                "{said}"
            );
        }
        other => panic!("a wait of 30001ms: {other:?}"),
    };
    refused(wait(Some(30_001)));
    std::env::set_var(INGEST_WAIT_ENV, "30001");
    refused(wait(None));
    // The flag, when it is in range, is not refused for the variable it overrides,
    // and an unreadable variable is refused unless the flag is given.
    assert_eq!(wait(Some(1)).expect("the flag"), ms(1));
    std::env::set_var(INGEST_WAIT_ENV, "soon");
    assert!(matches!(
        wait(None),
        Err(ConfigError::BadEnv { var, value }) if var == INGEST_WAIT_ENV && value == "soon"
    ));
    assert_eq!(wait(Some(1)).expect("the flag"), ms(1));
    std::env::remove_var(INGEST_WAIT_ENV);

    // The log's bound.
    let bound = |flag: Option<usize>| {
        Storage::default()
            .with_wal(&File::default(), None, None, flag)
            .map(|storage| (storage.wal_queue_bytes, storage.wal_options().queue_bytes))
    };
    let default = crate::engine::database::DEFAULT_WAL_QUEUE_BYTES;
    assert_eq!(default, 256 * 1024 * 1024);
    assert_eq!(Storage::default().wal_queue_bytes, default);
    assert_eq!(bound(None).expect("the default"), (default, default));
    std::env::set_var(WAL_QUEUE_BYTES_ENV, "4096");
    assert_eq!(bound(None).expect("the variable"), (4096, 4096));
    assert_eq!(bound(Some(1)).expect("the flag"), (1, 1));
    let no_bytes = |got: Result<(usize, usize), ConfigError>| match got {
        Err(e @ ConfigError::NoBytes { .. }) => {
            let said = e.to_string();
            assert!(
                said.starts_with("wal queue bytes must be at least 1 byte"),
                "{said}"
            );
        }
        other => panic!("a bound of nothing: {other:?}"),
    };
    no_bytes(bound(Some(0)));
    std::env::set_var(WAL_QUEUE_BYTES_ENV, "0");
    no_bytes(bound(None));
    assert_eq!(bound(Some(2)).expect("the flag"), (2, 2));
}

/// Every variable a key of the file has, so a test of the file is not answered by
/// whatever the machine it runs on has set.
const FILE_VARS: &[&str] = &[
    BASE_PATH_ENV,
    TRIE_FILE_ENV,
    MAX_OPEN_SEGMENTS_ENV,
    SEGMENT_MAX_BYTES_ENV,
    MAX_BUFFER_BYTES_ENV,
    COMPACT_INTERVAL_ENV,
    WAL_SEGMENT_BYTES_ENV,
    WAL_QUEUE_BYTES_ENV,
    FLUSH_INTERVAL_ENV,
    ROW_GROUP_TIMEOUT_ENV,
    BATCH_SIZE_ENV,
    NATIVE_ADDR_ENV,
    MAX_BODY_BYTES_ENV,
    MAX_DECOMPRESSED_BYTES_ENV,
    INGEST_CONCURRENCY_ENV,
    INGEST_WAIT_ENV,
    SHUTDOWN_DEADLINE_ENV,
    REQUEST_TIMEOUT_ENV,
    SCAN_THREADS_ENV,
    SCAN_FAN_ROWS_ENV,
    TIME_COLUMNS_ENV,
    EPOCH_WINDOW_ENV,
    LOG_LEVEL_ENV,
    LOG_FORMAT_ENV,
];

/// On the query limits, whose three variables no other test of this crate sets.
#[test]
fn a_setting_is_the_flag_then_the_variable_then_the_file_then_the_default() {
    let _cleared = Cleared::of(&[MAX_ROWS_ENV, MAX_BUCKETS_ENV, MAX_GROUPS_ENV]);
    let file = File::parse("[query]\nmax_rows = 11\nmax_buckets = 12\n").expect("a file");

    // The file over the default, key by key: `max_groups` is not in it.
    let read = limits(&file, None, None, None).expect("the file");
    assert_eq!(
        (read.max_rows, read.max_buckets, read.max_groups),
        (11, 12, DEFAULT_MAX_GROUPS)
    );

    // The variable over the file.
    std::env::set_var(MAX_ROWS_ENV, "21");
    let read = limits(&file, None, None, None).expect("the variable");
    assert_eq!((read.max_rows, read.max_buckets), (21, 12));

    // The flag over both.
    let read = limits(&file, Some(31), Some(32), None).expect("the flag");
    assert_eq!((read.max_rows, read.max_buckets), (31, 32));

    // And a variable that cannot be read is an error with a file there to fall back
    // on, as it is with none: its writer believes it is in force.
    std::env::set_var(MAX_ROWS_ENV, "many");
    assert!(matches!(
        limits(&file, None, None, None),
        Err(ConfigError::BadEnv { var, .. }) if var == MAX_ROWS_ENV
    ));

    // No file is every key absent.
    std::env::remove_var(MAX_ROWS_ENV);
    let none = File::read(None).expect("no file");
    assert_eq!(
        limits(&none, None, None, None).expect("defaults").max_rows,
        DEFAULT_MAX_ROWS
    );
}

/// One file holding all of them, each at a value that is not its default, read with no
/// flag and no variable.
#[test]
fn every_key_of_the_file_reaches_its_setting() {
    let _cleared = Cleared::of(FILE_VARS);
    let dir = std::env::temp_dir().join(format!("kibbr-config-keys-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let text = format!(
        r#"
[storage]
base_path = "{base}"
trie_file = "{base}/elsewhere.json"
max_open_segments = 7
segment_max_bytes = 1048576
max_buffer_bytes = 2097152
compact_interval_ms = 0
wal_segment_bytes = 131072
wal_queue_bytes = 4194304

[ingestor]
flush_interval_ms = 20
row_group_timeout_ms = 60
batch_size = 250

[api]
native_addr = "0.0.0.0:7811"
max_body_bytes = 1024
max_decompressed_bytes = 2048
ingest_concurrency = 3
ingest_wait_ms = 250
shutdown_deadline = "1500ms"
request_timeout = "45s"

[query]
scan_threads = 1
scan_fan_rows = 99

[schema]
time_columns = ["when", "ts"]
epoch_window = ["2001-01-01T00:00:00Z", "2002-01-01T00:00:00Z"]

[log]
level = "warn"
format = "json"
"#,
        base = dir.display()
    );
    let file = File::parse(&text).expect("every key is a setting");

    let storage = Storage::load(&file, true, None, None, None, None, None, None, None)
        .and_then(|storage| storage.with_compact_interval(&file, None))
        .and_then(|storage| storage.with_wal(&file, None, None, None))
        .and_then(|storage| storage.with_scan(&file, None, None))
        .expect("the storage settings");
    let base = std::fs::canonicalize(&dir).expect("the directory was made");
    assert_eq!(storage.base_path.as_deref(), Some(base.as_path()));
    assert_eq!(storage.trie_file, Some(dir.join("elsewhere.json")));
    assert_eq!(storage.max_open_segments, 7);
    assert_eq!(storage.segment_max_bytes, 1_048_576);
    assert_eq!(storage.max_buffer_bytes, 2_097_152);
    assert_eq!(storage.compact_interval, Duration::ZERO);
    assert_eq!(storage.wal_segment_bytes, 131_072);
    assert_eq!(storage.wal_queue_bytes, 4_194_304);
    assert_eq!(storage.flush_interval, Duration::from_millis(20));
    assert_eq!(storage.row_group_timeout, Duration::from_millis(60));
    assert_eq!(storage.wal_batch_lines, 250);
    assert_eq!((storage.scan_threads, storage.scan_fan_rows), (1, 99));
    let _ = std::fs::remove_dir_all(&dir);

    let api = ApiLimits::load(&file, None, None, None)
        .and_then(|api| api.with_ingest_wait(&file, None))
        .and_then(|api| api.with_shutdown_deadline(&file, None))
        .and_then(|api| api.with_request_timeout(&file, None))
        .expect("the request settings");
    assert_eq!(api.max_body_bytes, 1024);
    assert_eq!(api.max_decompressed_bytes, 2048);
    assert_eq!(api.ingest_concurrency, 3);
    assert_eq!(api.ingest_wait, Duration::from_millis(250));
    assert_eq!(api.shutdown_deadline, Some(Duration::from_millis(1500)));
    assert_eq!(api.request_timeout, Some(Duration::from_secs(45)));

    assert_eq!(
        listen(&file, None, None).expect("an address"),
        ("0.0.0.0".to_string(), 7811)
    );
    // Each flag overrides its half of the address and leaves the other.
    assert_eq!(
        listen(&file, Some("127.0.0.1".to_string()), None).expect("a host"),
        ("127.0.0.1".to_string(), 7811)
    );
    assert_eq!(
        listen(&file, None, Some(0)).expect("a port"),
        ("0.0.0.0".to_string(), 0)
    );

    assert_eq!(schema(&file, None).time_columns, ["when", "ts"]);
    let window = epoch_window(&file).expect("a window");
    assert!(format!("{window:?}") != format!("{:?}", EpochWindow::default()));

    assert_eq!(
        log(&file, None, None).expect("the log settings"),
        Log {
            level: LogLevel::Warn,
            format: LogFormat::Json
        }
    );
    assert_eq!(
        log(&file, Some(LogLevel::Debug), Some(LogFormat::Text)).expect("the flags"),
        Log {
            level: LogLevel::Debug,
            format: LogFormat::Text
        }
    );
}

/// Read and dropped, a key the design names and nothing implements would be a setting its
/// writer believes is in force.
#[test]
fn a_key_with_no_setting_behind_it_is_refused() {
    for (text, says) in [
        ("[storage]\nwal_dir = \"/var/lib/kibbr/wal\"\n", "wal_dir"),
        (
            "[api]\nes_compat_addr = \"0.0.0.0:9200\"\n",
            "es_compat_addr",
        ),
        (
            "[schema]\nmerge_threshold_pct = 80\n",
            "merge_threshold_pct",
        ),
        ("[schema]\npromoted_keys = [\"event\"]\n", "promoted_keys"),
        ("[cold_tier]\nenabled = false\n", "cold_tier"),
        ("[ingestor]\nflush_interval = \"500ms\"\n", "flush_interval"),
        ("[query]\nmax_rows = \"many\"\n", "max_rows"),
        ("[query]\nmax_rows = -1\n", "max_rows"),
        ("max_rows = 5\n", "max_rows"),
    ] {
        let Err(detail) = File::parse(text) else {
            panic!("read: {text}");
        };
        assert!(detail.contains(says), "{text}: {detail}");
        assert!(detail.contains("line "), "{text}: {detail}");
    }
    // What is not TOML at all is refused by the same road.
    assert!(File::parse("[storage\n").is_err());
    // And a file of nothing, or of comments, is every key absent.
    assert!(File::parse("# nothing yet\n").is_ok());

    // A file that is named and is not there is an error, never "no file".
    let gone = std::env::temp_dir().join(format!("kibbr-no-such-{}.toml", std::process::id()));
    assert!(matches!(
        File::read(Some(&gone)),
        Err(ConfigError::FileRead { .. })
    ));
}

#[test]
fn a_duration_is_a_whole_number_and_a_unit() {
    for (text, millis) in [
        ("25s", 25_000),
        ("500ms", 500),
        ("5m", 300_000),
        ("1h", 3_600_000),
        ("0s", 0),
        (" 2s ", 2_000),
    ] {
        assert_eq!(
            parse_duration(text),
            Ok(Duration::from_millis(millis)),
            "{text}"
        );
    }
    for text in [
        "25",
        "s",
        "",
        "2.5s",
        "-1s",
        "25 s",
        "25sec",
        "1d",
        "99999999999999999999s",
    ] {
        assert!(parse_duration(text).is_err(), "{text}");
    }
    assert!(parse_duration("18446744073709551615h").is_err());
    assert_eq!(duration_words(Duration::from_secs(25)), "25s");
    assert_eq!(duration_words(Duration::from_millis(1500)), "1500ms");
    assert_eq!(duration_words(Duration::ZERO), "0s");
}

/// One that cannot be read is an error that says where it was written.
#[test]
fn the_shutdown_deadline_is_25s_by_default_and_none_at_zero() {
    let _cleared = Cleared::of(&[SHUTDOWN_DEADLINE_ENV]);
    let deadline = |file: &File, flag: Option<Duration>| {
        ApiLimits::default()
            .with_shutdown_deadline(file, flag)
            .map(|api| api.shutdown_deadline)
    };
    let none = File::default();
    assert_eq!(
        ApiLimits::default().shutdown_deadline,
        Some(Duration::from_secs(25))
    );
    assert_eq!(
        deadline(&none, None).expect("default"),
        Some(Duration::from_secs(25))
    );

    let file = File::parse("[api]\nshutdown_deadline = \"40s\"\n").expect("a file");
    assert_eq!(
        deadline(&file, None).expect("file"),
        Some(Duration::from_secs(40))
    );
    std::env::set_var(SHUTDOWN_DEADLINE_ENV, "300ms");
    assert_eq!(
        deadline(&file, None).expect("variable"),
        Some(Duration::from_millis(300))
    );
    let flag = Some(Duration::from_secs(2));
    assert_eq!(deadline(&file, flag).expect("flag"), flag);

    // Zero, in any unit and from anywhere, waits for every request.
    assert_eq!(deadline(&file, Some(Duration::ZERO)).expect("flag 0"), None);
    std::env::set_var(SHUTDOWN_DEADLINE_ENV, "0ms");
    assert_eq!(deadline(&file, None).expect("variable 0"), None);

    std::env::set_var(SHUTDOWN_DEADLINE_ENV, "25");
    let Err(e) = deadline(&file, None) else {
        panic!("a bare number was read");
    };
    assert_eq!(
        e.to_string(),
        "KIBBR__API__SHUTDOWN_DEADLINE `25`: expected a whole number and a unit - ms, s, \
             m or h - as in `25s`"
    );
    std::env::remove_var(SHUTDOWN_DEADLINE_ENV);
    let file = File::parse("[api]\nshutdown_deadline = \"soon\"\n").expect("a file");
    let Err(e) = deadline(&file, None) else {
        panic!("`soon` was read");
    };
    assert!(
        e.to_string()
            .starts_with("[api] shutdown_deadline `soon`: "),
        "{e}"
    );
}

/// One that cannot be read is an error that says where it was written.
#[test]
fn the_request_timeout_is_30s_by_default_and_none_at_zero() {
    let _cleared = Cleared::of(&[REQUEST_TIMEOUT_ENV]);
    let timeout = |file: &File, flag: Option<Duration>| {
        ApiLimits::default()
            .with_request_timeout(file, flag)
            .map(|api| api.request_timeout)
    };
    let none = File::default();
    assert_eq!(
        ApiLimits::default().request_timeout,
        Some(Duration::from_secs(30))
    );
    assert_eq!(
        timeout(&none, None).expect("default"),
        Some(Duration::from_secs(30))
    );

    let file = File::parse("[api]\nrequest_timeout = \"2m\"\n").expect("a file");
    assert_eq!(
        timeout(&file, None).expect("file"),
        Some(Duration::from_secs(120))
    );
    std::env::set_var(REQUEST_TIMEOUT_ENV, "750ms");
    assert_eq!(
        timeout(&file, None).expect("variable"),
        Some(Duration::from_millis(750))
    );
    let flag = Some(Duration::from_secs(5));
    assert_eq!(timeout(&file, flag).expect("flag"), flag);

    assert_eq!(timeout(&file, Some(Duration::ZERO)).expect("flag 0"), None);
    std::env::set_var(REQUEST_TIMEOUT_ENV, "0s");
    assert_eq!(timeout(&file, None).expect("variable 0"), None);

    std::env::set_var(REQUEST_TIMEOUT_ENV, "30");
    let Err(e) = timeout(&file, None) else {
        panic!("a bare number was read");
    };
    assert_eq!(
        e.to_string(),
        "KIBBR__API__REQUEST_TIMEOUT `30`: expected a whole number and a unit - ms, s, \
             m or h - as in `25s`"
    );
    std::env::remove_var(REQUEST_TIMEOUT_ENV);
    let file = File::parse("[api]\nrequest_timeout = \"never\"\n").expect("a file");
    let Err(e) = timeout(&file, None) else {
        panic!("`never` was read");
    };
    assert!(
        e.to_string().starts_with("[api] request_timeout `never`: "),
        "{e}"
    );
}

#[test]
fn an_address_or_a_log_setting_that_cannot_be_read_says_where_it_was_written() {
    let _cleared = Cleared::of(&[NATIVE_ADDR_ENV, LOG_LEVEL_ENV, LOG_FORMAT_ENV]);
    let none = File::default();
    assert_eq!(
        listen(&none, None, None).expect("defaults"),
        ("127.0.0.1".to_string(), DEFAULT_PORT)
    );
    assert_eq!(log(&none, None, None).expect("defaults"), Log::default());

    for text in [
        "localhost",
        "localhost:",
        "localhost:http",
        ":7700",
        "localhost:70000",
    ] {
        let file = File {
            api: ApiKeys {
                native_addr: Some(text.to_string()),
                ..ApiKeys::default()
            },
            ..File::default()
        };
        let Err(e) = listen(&file, None, None) else {
            panic!("{text} was read");
        };
        assert!(e.to_string().starts_with("[api] native_addr `"), "{e}");
    }
    // An address with colons of its own keeps them: the port is after the last.
    std::env::set_var(NATIVE_ADDR_ENV, "[::1]:7800");
    assert_eq!(
        listen(&none, None, None).expect("an IPv6 address"),
        ("[::1]".to_string(), 7800)
    );
    std::env::remove_var(NATIVE_ADDR_ENV);

    std::env::set_var(LOG_LEVEL_ENV, "loud");
    let Err(e) = log(&none, None, None) else {
        panic!("`loud` was read");
    };
    assert_eq!(
        e.to_string(),
        "KIBBR__LOG__LEVEL `loud`: expected one of trace, debug, info, warn, error"
    );
    // The flag is not refused for a variable it overrides.
    assert_eq!(
        log(&none, Some(LogLevel::Error), None)
            .expect("the flag")
            .level,
        LogLevel::Error
    );
    std::env::set_var(LOG_LEVEL_ENV, "DEBUG");
    assert_eq!(
        log(&none, None, None).expect("any case").level,
        LogLevel::Debug
    );
}
