// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;

/// A scratch directory of one test, removed when the test ends.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let root =
            std::env::temp_dir().join(format!("kibbr-speed-corpus-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("create the scratch directory");
        Self(root)
    }

    fn write(&self, path: &str, bytes: &[u8]) {
        let path = self.0.join(path);
        fs::create_dir_all(path.parent().expect("a file has a directory"))
            .expect("create the directories");
        fs::write(path, bytes).expect("write the file");
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn file(path: &str, bytes: &[u8]) -> FileDigest {
    FileDigest {
        path: path.to_string(),
        bytes: bytes.len() as u64,
        crc32c: crc_hex(crc32c::crc32c(bytes)),
    }
}

/// A directory as `kibbr load` leaves one, and what a file manager and the lock add.
fn planted(scratch: &Scratch) -> PathBuf {
    scratch.write("loaded/catalog.log", b"catalog");
    scratch.write("loaded/manifest.log", b"manifest");
    scratch.write(
        "loaded/data/2024-01-15/seg_0_000001.col",
        b"columns of one day",
    );
    fs::create_dir_all(scratch.0.join("loaded/staging")).expect("create staging");
    scratch.write("loaded/LOCK", br#"{"pid":1,"load":true}"#);
    scratch.write("loaded/.DS_Store", b"finder");
    scratch.write("loaded/data/.DS_Store", b"finder");
    scratch.write("loaded/data/2024-01-15/.DS_Store", b"finder");
    scratch.write("loaded/.Trashes/501/seg_9_000009.col", b"thrown away");
    scratch.0.join("loaded")
}

fn loaded_files() -> Vec<FileDigest> {
    vec![
        file("catalog.log", b"catalog"),
        file("data/2024-01-15/seg_0_000001.col", b"columns of one day"),
        file("manifest.log", b"manifest"),
    ]
}

#[test]
fn a_listing_is_the_loaded_files_in_path_order_and_names_what_it_left_out() {
    let scratch = Scratch::new("listing");
    let loaded = planted(&scratch);

    let listed = listing(&loaded).expect("list the planted directory");
    assert_eq!(listed.files, loaded_files());
    // The lock, a dot file at each depth, and a dot directory once - not its files.
    assert_eq!(
        listed.left_out,
        [
            ".DS_Store",
            ".Trashes",
            "LOCK",
            "data/.DS_Store",
            "data/2024-01-15/.DS_Store",
        ]
        .map(PathBuf::from)
    );

    // Only the root's `LOCK` is the lock: a file of that name further down is data.
    scratch.write("loaded/data/LOCK", b"not the lock");
    let listed = listing(&loaded).expect("list again");
    assert!(listed.files.contains(&file("data/LOCK", b"not the lock")));
    assert!(!listed.left_out.contains(&PathBuf::from("data/LOCK")));

    assert!(listing(&scratch.0.join("not-there")).is_err());
}

#[test]
fn a_digest_is_of_every_byte_whatever_the_size_of_a_read() {
    let scratch = Scratch::new("digest");
    // Longer than one read, and not a whole number of them.
    let long: Vec<u8> = (0..CHUNK * 2 + 4_321).map(|i| (i % 251) as u8).collect();
    scratch.write("long", &long);
    scratch.write("empty", b"");
    assert_eq!(
        digest(&scratch.0.join("long")).expect("read the file"),
        (long.len() as u64, crc32c::crc32c(&long))
    );
    assert_eq!(
        digest(&scratch.0.join("empty")).expect("read the file"),
        (0, 0)
    );
    assert!(digest(&scratch.0.join("not-there")).is_err());
}

#[cfg(unix)]
#[test]
fn a_link_in_a_loaded_directory_is_an_error_and_not_a_file() {
    let scratch = Scratch::new("link");
    let loaded = planted(&scratch);
    std::os::unix::fs::symlink(loaded.join("catalog.log"), loaded.join("data/link.col"))
        .expect("make the link");
    let told = listing(&loaded)
        .expect_err("a link is not listed")
        .to_string();
    assert!(
        told.ends_with("link.col is neither a file nor a directory"),
        "{told}"
    );
}

#[test]
fn a_copy_is_the_listed_files_and_every_directory_and_nothing_else() {
    let scratch = Scratch::new("copy");
    let loaded = planted(&scratch);
    let listed = listing(&loaded).expect("list the planted directory");
    let copy = scratch.0.join("work/copy-1");

    let (files, bytes) = copy_tree(&loaded, &listed.files, &copy).expect("copy");
    assert_eq!((files, bytes), (3, 7 + 18 + 8));

    // The same list, and nothing left out: no lock and no dot name reached the copy.
    let copied = listing(&copy).expect("list the copy");
    assert_eq!(copied.files, listed.files);
    assert_eq!(copied.left_out, Vec::<PathBuf>::new());
    assert!(!copy.join("LOCK").exists());
    assert!(!copy.join(".Trashes").exists());
    assert!(!copy.join("data/.DS_Store").exists());
    // The empty directory is made: a server expects to find it.
    assert!(copy.join("staging").is_dir());
    assert_eq!(
        fs::read(copy.join("data/2024-01-15/seg_0_000001.col")).expect("read the copy"),
        b"columns of one day"
    );
    // The loaded directory is as it was.
    assert_eq!(listing(&loaded).expect("list again"), listed);

    // A copy is a new directory: one that is there is not written into.
    let again = copy_tree(&loaded, &listed.files, &copy).expect_err("it is there");
    assert_eq!(again.kind(), io::ErrorKind::AlreadyExists);
}

#[test]
fn a_copy_refuses_a_listed_path_that_leaves_the_directory_or_is_not_there() {
    let scratch = Scratch::new("copy-refused");
    let loaded = planted(&scratch);
    scratch.write("outside.col", b"not the bench's");

    for (n, path) in ["../outside.col", "/etc/hosts", "data/../../outside.col", ""]
        .into_iter()
        .enumerate()
    {
        let copy = scratch.0.join(format!("copy-{n}"));
        let told = copy_tree(&loaded, &[file(path, b"")], &copy).expect_err(path);
        assert_eq!(told.kind(), io::ErrorKind::InvalidInput, "{path}: {told}");
    }

    // A file the list names and the directory does not hold is an error that names it.
    let copy = scratch.0.join("copy-missing");
    let told = copy_tree(&loaded, &[file("data/gone.col", b"")], &copy)
        .expect_err("the file is not there")
        .to_string();
    assert!(told.contains("gone.col"), "{told}");
}

#[test]
fn a_directory_differs_from_its_marker_by_a_file_a_length_or_a_byte() {
    let recorded = loaded_files();
    assert_eq!(differences(&recorded, &recorded), Vec::<String>::new());

    let mut listed = recorded.clone();
    listed[0] = file("catalog.log", b"catalOg");
    listed[1] = file(
        "data/2024-01-15/seg_0_000001.col",
        b"columns of one day, and more",
    );
    listed.remove(2);
    listed.push(file("wal/000001.log", b"a server opened it"));
    assert_eq!(
        differences(&listed, &recorded),
        [
            format!(
                "catalog.log: crc32c {}, recorded {}",
                listed[0].crc32c, recorded[0].crc32c
            ),
            "data/2024-01-15/seg_0_000001.col: 28 bytes, recorded 18".to_string(),
            "manifest.log: not there, recorded with 8 bytes".to_string(),
            "wal/000001.log: not recorded".to_string(),
        ]
    );
}

/// Stand a directory and a marker that lists it under `scratch`, as a load left them.
fn marked(scratch: &Scratch, lines: u64) -> (PathBuf, Marker) {
    let loaded = planted(scratch);
    fs::rename(&loaded, loaded_dir(&scratch.0, lines)).expect("name the directory");
    let marker = Marker {
        lines,
        seed: LOADED_SEED,
        profile: PROFILE.to_string(),
        input: InputDigest {
            bytes: 489_036_605,
            crc32c: "0c77ab10".to_string(),
        },
        load: LoadFacts {
            load_id: Some("8eea6021fce6b677".to_string()),
            rows: lines,
            rejected: Some(0),
            row_groups: Some(231),
            days: 1,
            bytes: Some(36_212_480),
            seconds: Some(16.8),
        },
        files: loaded_files(),
        s3_id: "20834abd-1e3d-462e-b89b-fb1d4643e6bb".to_string(),
        built_at: "2026-10-07T15:58:40Z".to_string(),
        built_by: BuiltBy {
            sha: Some("1bfe096e45ef729cb24d60eb80f6d2267c76d082".to_string()),
            binary_crc32c: "9a3f01c2".to_string(),
        },
    };
    write_marker(&marker_path(&scratch.0, lines), &marker).expect("write the marker");
    (loaded_dir(&scratch.0, lines), marker)
}

/// `prepare` where no child may be started: the binary named is not there, so a call
/// that tried to generate or load fails on it.
fn prepared(scratch: &Scratch, lines: u64) -> Result<Corpus, SpeedError> {
    prepare(
        &scratch.0.join("no-such-binary"),
        &scratch.0,
        lines,
        &BuiltBy::default(),
    )
}

#[test]
fn a_loaded_directory_with_its_marker_is_reused_and_dot_files_do_not_stop_it() {
    let scratch = Scratch::new("reuse");
    let (loaded, marker) = marked(&scratch, 1_000);
    let marker_bytes = fs::read(marker_path(&scratch.0, 1_000)).expect("read the marker");

    let corpus = prepared(&scratch, 1_000).expect("the directory is what its marker lists");
    assert!(corpus.reused);
    assert_eq!(corpus.dir, loaded);
    assert_eq!(corpus.marker_path, marker_path(&scratch.0, 1_000));
    assert_eq!(corpus.marker, marker);
    assert_eq!(
        corpus.left_out,
        [
            ".DS_Store",
            ".Trashes",
            "LOCK",
            "data/.DS_Store",
            "data/2024-01-15/.DS_Store",
        ]
        .map(PathBuf::from)
    );
    // Reusing writes nothing: the marker is the bytes it was, and no `.tmp` is left.
    assert_eq!(
        fs::read(marker_path(&scratch.0, 1_000)).expect("read the marker"),
        marker_bytes
    );
    assert!(!with_tmp(&marker_path(&scratch.0, 1_000)).exists());
}

#[test]
fn a_loaded_directory_that_is_not_its_marker_s_bytes_is_named_and_left_alone() {
    let scratch = Scratch::new("differs");
    let (loaded, _) = marked(&scratch, 1_000);
    fs::write(loaded.join("manifest.log"), b"manifesT").expect("change one byte");

    let told = prepared(&scratch, 1_000)
        .expect_err("one byte differs")
        .to_string();
    assert!(
        told.starts_with(
            "the loaded directory is not the bytes that were loaded (manifest.log: crc32c "
        ),
        "{told}"
    );
    assert!(
        told.ends_with(&format!(
            "). Move {} and {} away to load again - a baseline taken on the old bytes \
                 will then not compare",
            loaded.display(),
            marker_path(&scratch.0, 1_000).display()
        )),
        "{told}"
    );
    // Nothing was removed and nothing was mended.
    assert_eq!(
        fs::read(loaded.join("manifest.log")).expect("read"),
        b"manifesT"
    );
    assert!(marker_path(&scratch.0, 1_000).exists());
}

#[test]
fn a_marker_of_other_data_or_one_that_is_not_a_marker_is_refused() {
    let scratch = Scratch::new("other-marker");
    let (_, mut marker) = marked(&scratch, 1_000);

    marker.seed = 7;
    write_marker(&marker_path(&scratch.0, 1_000), &marker).expect("write the marker");
    let told = prepared(&scratch, 1_000)
        .expect_err("another seed")
        .to_string();
    assert!(
        told.contains("is the marker of other data: 1,000 lines"),
        "{told}"
    );
    assert!(told.contains("at seed 7"), "{told}");

    fs::write(marker_path(&scratch.0, 1_000), b"{\"lines\":").expect("cut the marker");
    let told = prepared(&scratch, 1_000)
        .expect_err("half a marker")
        .to_string();
    assert!(told.contains("cannot be read as the marker"), "{told}");
    assert!(told.contains("away to load again"), "{told}");
}

#[test]
fn a_directory_without_its_marker_or_a_marker_without_its_directory_is_not_built_over() {
    let scratch = Scratch::new("half");
    let (loaded, _) = marked(&scratch, 1_000);
    let marker = marker_path(&scratch.0, 1_000);

    fs::rename(&marker, scratch.0.join("marker.aside")).expect("move the marker");
    let told = match prepared(&scratch, 1_000) {
        Err(SpeedError::Corpus(told)) => told,
        other => panic!("a directory with no marker: {other:?}"),
    };
    assert!(told.contains(&loaded.display().to_string()), "{told}");
    assert!(told.contains(&marker.display().to_string()), "{told}");
    assert!(told.contains("Nothing was removed"), "{told}");
    assert!(loaded.join("catalog.log").exists());

    fs::rename(scratch.0.join("marker.aside"), &marker).expect("put the marker back");
    fs::rename(&loaded, scratch.0.join("loaded.aside")).expect("move the directory");
    let told = match prepared(&scratch, 1_000) {
        Err(SpeedError::Corpus(told)) => told,
        other => panic!("a marker with no directory: {other:?}"),
    };
    assert!(
        told.contains("is there and the directory it describes"),
        "{told}"
    );
    assert!(marker.exists());
}

#[test]
fn with_neither_there_the_directory_is_built_which_needs_the_binary() {
    let scratch = Scratch::new("build");
    // The first step of a build is the generator, and the binary named is not there.
    match prepared(&scratch, 1_000) {
        Err(SpeedError::Io { path, .. }) => {
            assert_eq!(path, scratch.0.join("no-such-binary"))
        }
        other => panic!("a build without a binary: {other:?}"),
    }
    assert!(!loaded_dir(&scratch.0, 1_000).exists());
    assert!(!marker_path(&scratch.0, 1_000).exists());
}

#[test]
fn the_marker_is_written_whole_under_the_names_a_result_records() {
    let scratch = Scratch::new("marker");
    let (_, marker) = marked(&scratch, 1_000_000);
    let path = marker_path(&scratch.0, 1_000_000);
    assert_eq!(path, scratch.0.join("farm-1000000-seed42.json"));
    assert_eq!(
        loaded_dir(&scratch.0, 1_000_000),
        scratch.0.join("farm-1000000-seed42")
    );
    assert_eq!(
        load_log(&scratch.0, 1_000_000),
        scratch.0.join("farm-1000000-seed42.load.log")
    );

    let text = fs::read_to_string(&path).expect("read the marker");
    assert!(text.ends_with("}\n"), "{text}");
    let value: serde_json::Value = serde_json::from_str(&text).expect("the marker is JSON");
    let keys: Vec<&str> = value
        .as_object()
        .expect("an object")
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        ["lines", "seed", "profile", "input", "load", "files", "s3_id", "built_at", "built_by"]
    );
    assert_eq!(value["input"]["crc32c"], "0c77ab10");
    assert_eq!(value["load"]["row_groups"], 231);
    assert_eq!(
        value["files"][1]["path"],
        "data/2024-01-15/seg_0_000001.col"
    );
    assert_eq!(value["built_by"]["binary_crc32c"], "9a3f01c2");
    assert_eq!(
        serde_json::from_str::<Marker>(&text).expect("read back"),
        marker
    );
}

#[test]
fn the_third_query_s_id_is_the_first_string_at_job_id_in_the_stream() {
    let stream = concat!(
        r#"{"ts":"2024-01-15T00:00:00.000Z","msg":"sync","run_id":"sync-inventory-1"}"#,
        "\n",
        "not json at all\n",
        r#"{"job":"resize_image","id":"not-this-one"}"#,
        "\n",
        r#"{"job":{"type":"resize_image"},"job.id":"nor-this"}"#,
        "\n",
        r#"{"job":{"id":17}}"#,
        "\n",
        r#"{"event":"job_started","job":{"id":"491ec485-15a8-4e46-b5ee-506af57ab1e2","attempt":1}}"#,
        "\n",
        r#"{"event":"job_finished","job":{"id":"00000000-0000-4000-8000-000000000000"}}"#,
        "\n",
    );
    assert_eq!(
        first_job_id(stream.as_bytes()).expect("read"),
        Some("491ec485-15a8-4e46-b5ee-506af57ab1e2".to_string())
    );
    // The last line need not end in a newline.
    assert_eq!(
        first_job_id(&br#"{"job":{"id":"a-b"}}"#[..]).expect("read"),
        Some("a-b".to_string())
    );
    assert_eq!(
        first_job_id(&b"{\"msg\":\"no job\"}\n"[..]).expect("read"),
        None
    );
    assert_eq!(first_job_id(&b""[..]).expect("read"), None);
}

#[test]
fn a_stream_with_no_job_or_an_id_a_query_cannot_hold_is_refused_before_a_load() {
    let scratch = Scratch::new("s3");
    scratch.write("none.ndjson", b"{\"msg\":\"no job\"}\n");
    let told = s3_id(&scratch.0.join("none.ndjson"), 3)
        .expect_err("no id")
        .to_string();
    assert!(told.contains("--lines 3 --seed 42"), "{told}");
    assert!(told.contains("more --lines"), "{told}");

    for id in [r#"a\"b"#, r#"a\\b"#, "", r#"a\nb"#] {
        scratch.write(
            "odd.ndjson",
            format!("{{\"job\":{{\"id\":\"{id}\"}}}}\n").as_bytes(),
        );
        let told = s3_id(&scratch.0.join("odd.ndjson"), 3)
            .expect_err("an id that cannot be quoted")
            .to_string();
        assert!(
            told.contains("cannot stand in a query's text"),
            "{id}: {told}"
        );
    }

    scratch.write("good.ndjson", b"{\"job\":{\"id\":\"491ec485-15a8\"}}\n");
    assert_eq!(
        s3_id(&scratch.0.join("good.ndjson"), 3).expect("an id"),
        "491ec485-15a8"
    );
}

#[test]
fn what_the_load_printed_is_held_to_ok_one_day_and_a_row() {
    let printed = br#"{"ok":true,"load_id":"9e53627f92fccb15","lines":2000,"blank":0,"rejected":0,"rows":2000,"row_groups":80,"files":["2024-01-15/seg_0_000001.col"],"days":1,"bytes":164117,"replaced":[],"arrival":{"head":2000,"rows":2000,"fell_back":0,"share":0.0,"day":"2024-01-15","limit":1.0,"proceeded":false},"seconds":0.289029292,"errors":[]}
"#;
    assert_eq!(
        load_facts(printed),
        Ok(LoadFacts {
            load_id: Some("9e53627f92fccb15".to_string()),
            rows: 2_000,
            rejected: Some(0),
            row_groups: Some(80),
            days: 1,
            bytes: Some(164_117),
            seconds: Some(0.289_029_292),
        })
    );

    // A field the bench only records may be missing, or of another type.
    assert_eq!(
        load_facts(br#"{"ok":true,"rows":5,"days":1,"bytes":"many","load_id":7}"#),
        Ok(LoadFacts {
            rows: 5,
            days: 1,
            ..LoadFacts::default()
        })
    );

    let refused = |printed: &[u8]| load_facts(printed).expect_err("refused");
    assert!(refused(br#"{"ok":false,"rows":5,"days":1}"#).contains("\"ok\":true"));
    assert!(refused(br#"{"rows":5,"days":1}"#).contains("\"ok\":true"));
    assert!(refused(br#"{"ok":true,"rows":0,"days":1}"#).contains("0 rows over 1 days"));
    assert!(refused(br#"{"ok":true,"rows":9,"days":2}"#).contains("9 rows over 2 days"));
    assert!(refused(br#"{"ok":true,"rows":9}"#).contains("`rows` and `days`"));
    assert!(refused(br#"{"ok":true,"rows":9,"days":["2024-01-15"]}"#).contains("`rows` and `days`"));
    assert!(refused(b"loaded 2,000 rows from 2,000 lines").contains("not the one JSON object"));
    assert!(refused(b"").contains("not the one JSON object"));
}

#[test]
fn a_child_s_failure_quotes_what_was_wrong_and_the_end_of_what_it_wrote() {
    let scratch = Scratch::new("said");
    let log = scratch.0.join("child.log");
    let wrong = || Some("2,000 lines were asked of it and it wrote 1,999".to_string());

    // No file, and an empty one: only what was wrong, or that nothing was written.
    assert_eq!(
        said(wrong(), &log),
        "2,000 lines were asked of it and it wrote 1,999"
    );
    assert_eq!(said(None, &log), "it wrote nothing to standard error");

    let lines: Vec<String> = (1..=30).map(|n| format!("kibbr: WARN line {n}")).collect();
    fs::write(&log, lines.join("\n") + "\n").expect("write the log");
    let told = said(None, &log);
    assert_eq!(told.lines().count(), QUOTED_LINES);
    assert!(told.starts_with("kibbr: WARN line 11\n"), "{told}");
    assert!(told.ends_with("kibbr: WARN line 30"), "{told}");
    let told = said(wrong(), &log);
    assert!(
        told.starts_with("2,000 lines were asked of it and it wrote 1,999\nkibbr: WARN line 11"),
        "{told}"
    );
}

#[test]
fn a_stream_is_cut_at_line_ends_and_the_last_body_is_what_is_left() {
    let stream: Vec<u8> = (0..10)
        .flat_map(|n| format!("{{\"n\":{n},\"pad\":\"{}\"}}\n", "x".repeat(n)).into_bytes())
        .collect();

    let bodies = cut(&stream, 3);
    assert_eq!(
        bodies.iter().map(|body| body.lines).collect::<Vec<_>>(),
        [3, 3, 3, 1],
        "the last is short"
    );
    // Every byte is in exactly one body, in order: each begins where the last ended.
    let mut next = 0;
    for body in &bodies {
        assert_eq!(body.range.start, next);
        assert!(body.range.end > body.range.start);
        let bytes = &stream[body.range.clone()];
        assert_eq!(bytes.last(), Some(&b'\n'), "a body ends at a line's end");
        assert_eq!(
            bytes.iter().filter(|byte| **byte == b'\n').count(),
            body.lines
        );
        next = body.range.end;
    }
    assert_eq!(next, stream.len());

    // A count that divides the stream leaves no short body, and one line a body is
    // one body a line.
    assert_eq!(cut(&stream, 5).len(), 2);
    assert_eq!(cut(&stream, 10).len(), 1);
    assert_eq!(cut(&stream, 11).len(), 1);
    assert_eq!(cut(&stream, 1).len(), 10);
    assert_eq!(cut(b"", 3), Vec::<Body>::new());
}

#[test]
fn a_last_line_the_stream_did_not_end_is_still_a_line_of_the_last_body() {
    let bodies = cut(b"a\nb\nc", 2);
    assert_eq!(
        bodies,
        [
            Body {
                range: 0..4,
                lines: 2
            },
            Body {
                range: 4..5,
                lines: 1
            }
        ]
    );
    assert_eq!(
        cut(b"a", 2),
        [Body {
            range: 0..1,
            lines: 1
        }]
    );

    // The count a generator is held to is the same count, however the stream is read.
    let streams: [(&[u8], usize); 4] = [(b"a\nb\nc", 3), (b"a\nb\n", 2), (b"", 0), (b"\n", 1)];
    for (stream, lines) in streams {
        let cut_lines: usize = cut(stream, 2).iter().map(|body| body.lines).sum();
        assert_eq!(cut_lines, lines);
        for step in 1..=stream.len().max(1) {
            let mut counted = Lines::default();
            stream.chunks(step).for_each(|chunk| counted.eat(chunk));
            assert_eq!(
                counted.count(),
                lines as u64,
                "{stream:?} in reads of {step}"
            );
        }
    }
}

#[test]
fn the_four_shares_are_disjoint_and_together_are_every_body() {
    for bodies in [0, 1, 3, 4, 5, 10, 500] {
        let mut seen = vec![0; bodies];
        for sender in 0..4 {
            let share = share(bodies, sender, 4);
            assert!(
                share.windows(2).all(|pair| pair[1] == pair[0] + 4),
                "in order"
            );
            assert_eq!(share.first().copied(), (sender < bodies).then_some(sender));
            share.iter().for_each(|body| seen[*body] += 1);
        }
        assert!(
            seen.iter().all(|times| *times == 1),
            "{bodies} bodies: {seen:?}"
        );
    }
    // With fewer bodies than senders, a sender has none.
    assert_eq!(share(2, 3, 4), Vec::<usize>::new());
}

#[test]
fn a_stream_has_one_request_head_for_each_of_its_bodies() {
    let bytes = b"a\nbb\nccc\n".to_vec();
    let stream = LiveStream {
        bodies: cut(&bytes, 2),
        lines: 3,
        crc32c: crc_hex(crc32c::crc32c(&bytes)),
        bytes,
    };
    let heads = heads(&stream);
    assert_eq!(heads.len(), stream.bodies.len());
    for (head, body) in heads.iter().zip(&stream.bodies) {
        assert_eq!(*head, wire::ingest_head(body.range.len()));
    }
}

#[test]
fn a_volume_must_have_room_for_every_line_before_anything_is_built() {
    let dir = Path::new("target/scratch/bench");
    // 2,000 bytes a line: 2 GB at the measurement's size.
    assert!(enough(dir, 2_000_000_000, 1_000_000).is_ok());
    let told = enough(dir, 1_999_999_999, 1_000_000)
        .expect_err("one byte short")
        .to_string();
    assert_eq!(
        told,
        "the volume that holds target/scratch/bench has 1,999,999,999 bytes free, and \
             kibbr-bench --lines 1,000,000 needs 2,000,000,000 (2,000 a line) before it builds \
             anything. Make room on it, or name a directory on another volume with --dir"
    );
    assert!(
        enough(dir, u64::MAX, u64::MAX).is_ok(),
        "the need does not wrap"
    );
}

#[test]
fn work_directories_another_invocation_left_are_named_and_this_one_s_is_not() {
    let scratch = Scratch::new("stale");
    fs::create_dir_all(work_dir(&scratch.0)).expect("this invocation's");
    fs::create_dir_all(scratch.0.join("work-1.noindex")).expect("another's");
    fs::create_dir_all(scratch.0.join("work-77.noindex")).expect("another's");
    fs::create_dir_all(scratch.0.join("results")).expect("not a work directory");
    fs::create_dir_all(scratch.0.join("work-in-progress")).expect("nor this");

    assert_eq!(
        stale_work(&scratch.0),
        [
            scratch.0.join("work-1.noindex"),
            scratch.0.join("work-77.noindex")
        ]
    );
    assert_eq!(
        work_dir(&scratch.0),
        scratch
            .0
            .join(format!("work-{}.noindex", std::process::id()))
    );
    assert_eq!(
        stale_work(&scratch.0.join("not-there")),
        Vec::<PathBuf>::new()
    );
    // Naming them removes none.
    assert!(scratch.0.join("work-1.noindex").is_dir());
}
