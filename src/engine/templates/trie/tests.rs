// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::engine::parse::flatten::parse_line;

fn scratch(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("kibbr-trie-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("mkdir");
    dir
}

fn parsed(line: &str) -> ParsedLog {
    parse_line(line.as_bytes(), 1).expect("a valid line")
}

/// The ids the key index lists under `key`; none where it has no list.
fn listed(trie: &Trie, key: &str) -> Vec<u64> {
    trie.keys()
        .of(key)
        .map(|list| list.iter().map(|(id, _)| *id).collect())
        .unwrap_or_default()
}

/// Every id the key index holds, in the order it holds them.
fn all_ids(trie: &Trie) -> Vec<u64> {
    trie.keys().all().iter().map(|(id, _)| *id).collect()
}

/// Template `id` as the key index hands it to a plan.
fn held(trie: &Trie, id: u64) -> Arc<Template> {
    let keys = trie.keys();
    let (_, template) = keys
        .all()
        .iter()
        .find(|(have, _)| *have == id)
        .expect("the template is in the index");
    Arc::clone(template)
}

/// The write rule: no record on disk, no template in memory, and a later line that
/// needs no record still lands.
#[test]
fn a_failed_append_creates_no_template() {
    let dir = scratch("failed-append");
    let trie = Trie::new_in(&dir.display().to_string());
    let good = Catalog::open(&crate::engine::templates::catalog::path_in(&dir), &mut |_| {}).expect("open");
    trie.attach_catalog(good.0);
    let now = Utc::now();

    let first = parsed(r#"{"event":"login","user":7}"#);
    assert!(trie.ingest(&first, now).expect("first line").1);
    assert_eq!(trie.template_count(), 1);
    assert_eq!(trie.keys_agree(), Ok(()));
    let edits = trie.keys().edits();

    // Swap in a catalog that cannot be written to.
    trie.attach_catalog(Catalog::read_only(&dir.join("dead.log")).expect("read-only"));

    // A new template: refused, and not there.
    let second = parsed(r#"{"event":"logout","user":7}"#);
    let Err(err) = trie.ingest(&second, now) else {
        panic!("the append must fail");
    };
    assert!(matches!(err, TrieError::Catalog(_)), "{err}");
    assert!(err.to_string().contains("catalog"), "{err}");
    assert_eq!(
        trie.template_count(),
        1,
        "the refused template exists in memory"
    );
    let tokens = second.walk_tokens();
    let borrowed: Vec<&str> = tokens.iter().map(String::as_str).collect();
    let walked = trie.walk(&borrowed).expect("the interior nodes may exist");
    assert!(!walked.is_leaf, "the refused template is a leaf");
    assert_eq!(walked.template_id, None);
    // Nor is it anywhere a plan looks: the index holds the one template, untouched.
    assert_eq!(trie.keys_agree(), Ok(()));
    assert_eq!(all_ids(&trie), [FIRST_TEMPLATE_ID]);
    assert_eq!(listed(&trie, "event"), [FIRST_TEMPLATE_ID]);
    assert_eq!(listed(&trie, "user"), [FIRST_TEMPLATE_ID]);
    assert_eq!(trie.keys().edits(), edits);

    // A widening on the existing template (Int64 ⊔ String): refused, and the column
    // unchanged.
    let widen = parsed(r#"{"event":"login","user":"u1"}"#);
    assert!(matches!(
        trie.ingest(&widen, now),
        Err(TrieError::Catalog(_))
    ));
    let leaf = trie.all_leaves().into_iter().next().expect("one leaf");
    let slot = leaf.leaf_meta.read().expect("meta");
    let user = slot
        .as_ref()
        .expect("meta")
        .columns
        .iter()
        .find(|c| c.name == "user")
        .expect("user column");
    assert_eq!(user.col_type, Some(ColumnType::Int64));
    assert_eq!(user.widened_at, None);
    drop(slot);
    // And a plan is handed the columns as they were: `user` is not text.
    assert_eq!(trie.keys_agree(), Ok(()));
    assert_eq!(trie.keys().edits(), edits);
    assert_eq!(held(&trie, FIRST_TEMPLATE_ID).text_columns, ["event"]);

    // A repeat of the first line needs no record, so it still goes in.
    assert!(
        !trie
            .ingest(&first, now)
            .expect("a repeat needs no record")
            .1
    );
    assert_eq!(trie.keys_agree(), Ok(()));
    assert_eq!(trie.keys().edits(), edits);
    let _ = std::fs::remove_dir_all(&dir);
}

/// What `ingest` writes is what `from_catalog` rebuilds: same paths, same ids, same
/// column types including the two the segment format cannot spell, and the widening.
#[test]
fn a_trie_round_trips_through_its_catalog() {
    let dir = scratch("round-trip");
    let path = crate::engine::templates::catalog::path_in(&dir);
    let data_dir = dir.display().to_string();
    let trie = Trie::new_in(&data_dir);
    trie.attach_catalog(Catalog::open(&path, &mut |_| {}).expect("open").0);
    let now = Utc::now();

    for line in [
        r#"{"event":"login","session":"123e4567-e89b-12d3-a456-426614174000","ip":"10.0.0.1","n":null}"#,
        r#"{"event":"login","session":"123e4567-e89b-12d3-a456-426614174000","ip":"10.0.0.1","n":7}"#,
        r#"{"event":"login","session":"not-a-uuid","ip":"10.0.0.1","n":7}"#,
        r#"{"event":"pay","amount":12.5}"#,
    ] {
        trie.ingest(&parsed(line), now).expect("ingest");
    }
    assert_eq!(trie.template_count(), 2);

    let (_, records) = Catalog::open(&path, &mut |_| {}).expect("reopen");
    let mut warnings = Vec::new();
    let rebuilt =
        Trie::from_catalog(&data_dir, &records, &mut |w| warnings.push(w)).expect("rebuild");
    assert!(warnings.is_empty(), "{warnings:?}");

    let describe = |t: &Trie| -> Vec<String> {
        t.leaf_paths()
            .into_iter()
            .map(|(path, leaf)| {
                let slot = leaf.leaf_meta.read().expect("meta");
                let meta = slot.as_ref().expect("meta");
                let cols: Vec<String> = meta
                    .columns
                    .iter()
                    .map(|c| {
                        format!(
                            "{}:{}{}",
                            c.name,
                            c.col_type.map_or("null", ColumnType::as_str),
                            if c.widened_at.is_some() { "*" } else { "" }
                        )
                    })
                    .collect();
                format!(
                    "{} {} [{}]",
                    meta.template_id,
                    path.join("/"),
                    cols.join(",")
                )
            })
            .collect()
    };
    let before = describe(&trie);
    assert_eq!(before, describe(&rebuilt));
    assert_eq!(trie.keys_agree(), Ok(()));
    assert_eq!(rebuilt.keys_agree(), Ok(()));
    assert!(
        before
            .iter()
            .any(|d| d.contains("ip:Ip") && d.contains("session:String*")),
        "{before:?}"
    );
    assert!(before.iter().any(|d| d.contains("n:Int64")), "{before:?}");
    assert_eq!(
        rebuilt.next_template_id().expect("an id"),
        trie.next_template_id().expect("an id")
    );
    let _ = std::fs::remove_dir_all(&dir);
}

fn template_record(id: u64, key: &str) -> Record {
    Record::Template {
        id,
        created_at: DateTime::from_timestamp(1_705_314_600, 0).expect("a valid timestamp"),
        tokens: vec![Token::Key { key: key.into() }],
    }
}

fn column_record(id: u64, name: &str, col_type: Option<ColumnType>) -> Record {
    Record::Column {
        template_id: id,
        name: name.into(),
        col_type,
        epoch_digits: None,
        widened_from: None,
        at: None,
    }
}

/// A template is listed under each of its columns from the line that makes it, a widening
/// shows in the next template handed out, and an earlier handle is the template as it was.
#[test]
fn the_key_index_follows_a_new_template_and_a_widening() {
    let trie = Trie::new();
    let now = Utc::now();
    trie.ingest(&parsed(r#"{"event":"login","user":7}"#), now)
        .expect("a new template");
    trie.ingest(&parsed(r#"{"level":"warn","user":8}"#), now)
        .expect("a new template");
    assert_eq!(trie.keys_agree(), Ok(()));
    assert_eq!(all_ids(&trie), [101, 102]);
    assert_eq!(listed(&trie, "user"), [101, 102]);
    assert_eq!(listed(&trie, "event"), [101]);
    assert_eq!(listed(&trie, "level"), [102]);
    assert!(trie.keys().of("nosuch").is_none());
    // An edge label is not a key.
    assert!(trie.keys().of("event=login").is_none());

    let before = held(&trie, 101);
    assert_eq!(before.path, ["event=login", "user"]);
    assert_eq!(before.storage_path, format!("{DEFAULT_DATA_DIR}/leaf_101"));
    assert_eq!(before.text_columns, ["event"]);
    assert_eq!(before.promoted("event"), Some("login"));
    assert_eq!(before.promoted("user"), None);

    let edits = trie.keys().edits();
    trie.ingest(&parsed(r#"{"event":"login","user":9}"#), now)
        .expect("a line of a template that exists");
    assert_eq!(trie.keys().edits(), edits, "a line that changes nothing");
    assert!(Arc::ptr_eq(&before, &held(&trie, 101)));

    // Int64 ⊔ String.
    trie.ingest(&parsed(r#"{"event":"login","user":"u1"}"#), now)
        .expect("a widening");
    assert_eq!(trie.keys().edits(), edits + 1);
    let after = held(&trie, 101);
    assert_eq!(after.text_columns, ["event", "user"]);
    assert_eq!(before.text_columns, ["event"]);
    for key in ["event", "user"] {
        let keys = trie.keys();
        let list = keys.of(key).expect("a list");
        assert!(
            Arc::ptr_eq(&list[0].1, &after),
            "`{key}` still lists the template as it was"
        );
    }
    assert_eq!(listed(&trie, "user"), [101, 102]);
    assert_eq!(trie.keys_agree(), Ok(()));
}

/// A leaf from before `=` was escaped in a key's token: a line whose `event` promotes to
/// its label adds the column `event`, and the template is then listed under that key too.
#[test]
fn a_column_added_to_a_leaf_lists_its_template_under_the_new_key() {
    let records = vec![
        Record::Template {
            id: 101,
            created_at: DateTime::from_timestamp(1_705_314_600, 0).expect("a valid timestamp"),
            tokens: vec![Token::Promoted {
                promoted: "event".into(),
                value: "login".into(),
            }],
        },
        column_record(101, "event=login", Some(ColumnType::Int64)),
    ];
    let trie = Trie::from_catalog("data", &records, &mut |_| {}).expect("rebuild");
    assert_eq!(listed(&trie, "event=login"), [101]);
    assert!(trie.keys().of("event").is_none());

    let (_, is_new) = trie
        .ingest(&parsed(r#"{"event":"login"}"#), Utc::now())
        .expect("a line of the leaf that exists");
    assert!(!is_new);
    assert_eq!(listed(&trie, "event"), [101]);
    assert_eq!(listed(&trie, "event=login"), [101]);
    let template = held(&trie, 101);
    assert_eq!(template.path, ["event=login"]);
    // The label is also a column, so it speaks for no row's `event`.
    assert_eq!(template.promoted("event"), None);
    assert_eq!(template.text_columns, ["event"]);
    assert_eq!(trie.keys_agree(), Ok(()));
}

/// The three records a replay skips - an id past the last one, a second record under
/// an id, a second template on a path - make no template, so none is in the index.
#[test]
fn a_record_a_replay_skips_leaves_nothing_in_the_key_index() {
    let int = Some(ColumnType::Int64);
    let records = vec![
        template_record(u64::MAX, "past"),
        column_record(u64::MAX, "past", int),
        template_record(101, "user"),
        column_record(101, "user", int),
        template_record(101, "twice"),
        template_record(102, "user"),
        column_record(102, "other", int),
    ];
    let mut warnings = Vec::new();
    let trie =
        Trie::from_catalog("data", &records, &mut |w| warnings.push(w)).expect("rebuild");
    assert_eq!(warnings.len(), 4, "{warnings:?}");
    assert_eq!(trie.template_count(), 1);
    assert_eq!(all_ids(&trie), [101]);
    assert_eq!(listed(&trie, "user"), [101]);
    for key in ["past", "twice", "other"] {
        assert!(trie.keys().of(key).is_none(), "`{key}` has a list");
    }
    assert_eq!(trie.keys().edits(), 1);
    assert_eq!(trie.keys_agree(), Ok(()));
}

/// A line with no keys walks no token, so the root is its leaf: a template with an
/// empty path, among all templates and under no key.
#[test]
fn a_line_with_no_keys_is_a_template_under_no_key() {
    let trie = Trie::new();
    let now = Utc::now();
    trie.ingest(&parsed("{}"), now).expect("the root as a leaf");
    trie.ingest(&parsed(r#"{"user":7}"#), now)
        .expect("a new template");
    assert_eq!(all_ids(&trie), [101, 102]);
    assert!(held(&trie, 101).path.is_empty());
    assert_eq!(listed(&trie, "user"), [102]);
    assert!(trie.keys().of("").is_none());
    assert_eq!(trie.keys_agree(), Ok(()));
}

/// Nothing is refused for a poisoned index lock: a line refused here would be one
/// whose record the catalog already holds.
#[test]
fn a_poisoned_key_index_still_takes_templates_and_answers_plans() {
    let trie = Trie::new();
    let now = Utc::now();
    trie.ingest(&parsed(r#"{"user":7}"#), now)
        .expect("a new template");

    trie.poison_keys();
    assert!(trie.keys.is_poisoned());

    let (_, is_new) = trie
        .ingest(&parsed(r#"{"host":"a","n":1}"#), now)
        .expect("a new template through a poisoned lock");
    assert!(is_new);
    assert_eq!(all_ids(&trie), [101, 102]);
    assert_eq!(listed(&trie, "host"), [102]);
    assert_eq!(held(&trie, 102).text_columns, ["host"]);

    trie.ingest(&parsed(r#"{"host":"a","n":"x"}"#), now)
        .expect("a widening through a poisoned lock");
    assert_eq!(held(&trie, 102).text_columns, ["host", "n"]);
    assert_eq!(trie.keys_agree(), Ok(()));
}

/// A template record whose id has no `max + 1` is skipped with a WARN that names it,
/// with the records that name it, and the start goes on.
#[test]
fn a_template_id_with_no_next_id_is_skipped_and_never_fails_a_start() {
    let records = vec![
        template_record(u64::MAX, "ts"),
        Record::Column {
            template_id: u64::MAX,
            name: "ts".into(),
            col_type: Some(ColumnType::Datetime),
            epoch_digits: None,
            widened_from: None,
            at: None,
        },
        Record::TimeSource {
            template_id: u64::MAX,
            source: "column:ts".into(),
        },
        template_record(FIRST_TEMPLATE_ID, "user"),
    ];
    let mut warnings = Vec::new();
    let rebuilt =
        Trie::from_catalog("data", &records, &mut |w| warnings.push(w)).expect("rebuild");
    assert_eq!(rebuilt.template_count(), 1);
    assert!(rebuilt.walk(&["ts"]).is_none());
    assert_eq!(warnings.len(), 3, "{warnings:?}");
    assert!(
        warnings[0].contains(&format!("id {}", u64::MAX)) && warnings[0].contains("skipped"),
        "{warnings:?}"
    );
    assert_eq!(
        rebuilt.next_template_id().expect("an id"),
        FIRST_TEMPLATE_ID + 1
    );
}

/// A template record skipped for a path another template holds still names its id, and
/// the id is not given again, or the next start would lose the template that took it.
#[test]
fn the_id_of_a_template_record_skipped_for_its_path_is_not_given_again() {
    let mut records = vec![
        template_record(FIRST_TEMPLATE_ID, "user"),
        template_record(FIRST_TEMPLATE_ID + 1, "user"),
    ];
    let mut warnings = Vec::new();
    let rebuilt =
        Trie::from_catalog("data", &records, &mut |w| warnings.push(w)).expect("rebuild");
    assert_eq!(rebuilt.template_count(), 1);
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(warnings[0].contains("the same path"), "{warnings:?}");

    let (leaf, is_new) = rebuilt
        .ingest(&parsed(r#"{"other":7}"#), Utc::now())
        .expect("a new template");
    assert!(is_new);
    let id = leaf
        .leaf_meta
        .read()
        .expect("meta")
        .as_ref()
        .map(|meta| meta.template_id)
        .expect("a template");
    assert_eq!(id, FIRST_TEMPLATE_ID + 2);

    // What the catalog holds once that template is recorded, read by the next start.
    records.push(template_record(id, "other"));
    let mut warnings = Vec::new();
    let again =
        Trie::from_catalog("data", &records, &mut |w| warnings.push(w)).expect("rebuild");
    assert_eq!(again.template_count(), 2);
    assert!(again.walk(&["other"]).is_some());
    assert_eq!(warnings.len(), 1, "{warnings:?}");
}

/// The last id is handed out once and nothing after it: not 0, not an id again, and
/// reserving through `u64::MAX` - an id a file could name - is not a panic.
#[test]
fn the_last_template_id_is_given_once_and_no_id_is_given_twice() {
    let trie = Trie::new();
    trie.reserve_template_ids_through(LAST_TEMPLATE_ID - 1);
    assert_eq!(
        trie.next_template_id().expect("the last id"),
        LAST_TEMPLATE_ID
    );
    for _ in 0..3 {
        assert!(matches!(
            trie.next_template_id(),
            Err(TrieError::TemplateIdsSpent)
        ));
    }

    let mut warnings = Vec::new();
    let rebuilt = Trie::from_catalog(
        "data",
        &[template_record(LAST_TEMPLATE_ID, "user")],
        &mut |w| warnings.push(w),
    )
    .expect("rebuild");
    assert!(warnings.is_empty(), "{warnings:?}");
    assert_eq!(rebuilt.template_count(), 1);
    rebuilt.reserve_template_ids_through(u64::MAX);
    let now = Utc::now();
    let (leaf, is_new) = rebuilt
        .ingest(&parsed(r#"{"user":7}"#), now)
        .expect("a line of the template that exists");
    assert!(!is_new);
    let id = leaf
        .leaf_meta
        .read()
        .expect("meta")
        .as_ref()
        .map(|meta| meta.template_id);
    assert_eq!(id, Some(LAST_TEMPLATE_ID));
    assert!(matches!(
        rebuilt.ingest(&parsed(r#"{"other":7}"#), now),
        Err(TrieError::TemplateIdsSpent)
    ));
    assert_eq!(rebuilt.template_count(), 1);
}

// ── Epochs and integers, and what a time column reads ──────────────────────────────

/// The type of one column of the template a line belongs to, and the template's time source.
fn column_of(trie: &Trie, line: &str, name: &str) -> (Option<ColumnType>, TimeSource) {
    let parsed = parsed(line);
    let tokens = parsed.walk_tokens();
    let borrowed: Vec<&str> = tokens.iter().map(String::as_str).collect();
    let leaf = trie.walk(&borrowed).expect("the line's template").node;
    let slot = leaf.leaf_meta.read().expect("the leaf");
    let meta = slot.as_ref().expect("a template");
    let ty = meta
        .columns
        .iter()
        .find(|c| c.name == name)
        .unwrap_or_else(|| panic!("no column `{name}`"))
        .col_type;
    (ty, meta.time_source.clone())
}

fn feed(trie: &Trie, lines: &[&str]) {
    for line in lines {
        trie.ingest(&parsed(line), Utc::now()).expect("a line");
    }
}

/// **MongoDB's `attr.Available`: a column of epoch digits that takes `999999999` is
/// `Int64`**, where it used to be `String`; the catalog records the widening.
#[test]
fn a_datetime_column_of_epoch_digits_that_meets_an_integer_becomes_int64() {
    let dir = scratch("epoch-then-int");
    let trie = Trie::new_in(&dir.display().to_string());
    let path = crate::engine::templates::catalog::path_in(&dir);
    trie.attach_catalog(Catalog::open(&path, &mut |_| {}).expect("open").0);
    let first = r#"{"attr":{"Available":1679711330},"msg":"a"}"#;
    feed(&trie, &[first]);
    assert_eq!(
        column_of(&trie, first, "attr.Available").0,
        Some(ColumnType::Datetime)
    );
    feed(&trie, &[r#"{"attr":{"Available":999999999},"msg":"b"}"#]);
    assert_eq!(
        column_of(&trie, first, "attr.Available").0,
        Some(ColumnType::Int64)
    );
    // And it stays a column of integers whichever arrives next.
    feed(
        &trie,
        &[
            r#"{"attr":{"Available":1679711331},"msg":"c"}"#,
            r#"{"attr":{"Available":"1679711332000"},"msg":"d"}"#,
            r#"{"attr":{"Available":7},"msg":"e"}"#,
        ],
    );
    assert_eq!(
        column_of(&trie, first, "attr.Available").0,
        Some(ColumnType::Int64)
    );
    assert_eq!(
        held(&trie, FIRST_TEMPLATE_ID).int_columns,
        ["attr.Available"]
    );
    assert_eq!(held(&trie, FIRST_TEMPLATE_ID).text_columns, ["msg"]);

    // One widening on record, from `Datetime` to `Int64`; a restart reads it back.
    drop(trie);
    let (_, records) = Catalog::open(&path, &mut |_| {}).expect("reopen");
    let widened: Vec<&Record> = records
        .iter()
        .filter(|r| {
            matches!(
                r,
                Record::Column {
                    widened_from: Some(_),
                    ..
                }
            )
        })
        .collect();
    assert!(
        matches!(
            widened[..],
            [Record::Column {
                col_type: Some(ColumnType::Int64),
                epoch_digits: None,
                widened_from: Some(ColumnType::Datetime),
                ..
            }]
        ),
        "{widened:?}"
    );
    let again = Trie::from_catalog("data", &records, &mut |w| panic!("{w}")).expect("rebuild");
    assert_eq!(
        column_of(&again, first, "attr.Available").0,
        Some(ColumnType::Int64)
    );
}

/// **CockroachDB's `event.GoAllocBytes`: a column of integers that takes `1220305536`,
/// ten digits inside the epoch window, stays `Int64`**, and no record is written, because
/// nothing changed.
#[test]
fn an_int64_column_that_meets_epoch_digits_stays_int64() {
    let dir = scratch("int-then-epoch");
    let trie = Trie::new_in(&dir.display().to_string());
    let path = crate::engine::templates::catalog::path_in(&dir);
    trie.attach_catalog(Catalog::open(&path, &mut |_| {}).expect("open").0);
    let first = r#"{"GoAllocBytes":999999999,"msg":"a"}"#;
    feed(&trie, &[first]);
    assert_eq!(
        column_of(&trie, first, "GoAllocBytes").0,
        Some(ColumnType::Int64)
    );
    let before = std::fs::metadata(&path).expect("the catalog").len();
    feed(
        &trie,
        &[
            r#"{"GoAllocBytes":1220305536,"msg":"b"}"#,
            r#"{"GoAllocBytes":"1220305536","msg":"c"}"#,
            r#"{"GoAllocBytes":1679711330570,"msg":"d"}"#,
        ],
    );
    assert_eq!(
        column_of(&trie, first, "GoAllocBytes").0,
        Some(ColumnType::Int64)
    );
    assert_eq!(std::fs::metadata(&path).expect("the catalog").len(), before);
}

/// **Every other widening is what it was**: an RFC 3339 date in a column of integers, a
/// float or a word in a column of dates, and epoch digits in a column of floats are
/// `String`, and a `String` column stays one.
#[test]
fn the_other_widenings_of_dates_and_integers_are_unchanged() {
    let cases: [(&str, &str, Option<ColumnType>); 8] = [
        ("5", r#""2024-01-15T10:30:00Z""#, Some(ColumnType::String)),
        ("1679711330", "1.5", Some(ColumnType::String)),
        ("1679711330", r#""N/A""#, Some(ColumnType::String)),
        ("1.5", "1679711330", Some(ColumnType::String)),
        (r#""N/A""#, "1679711330", Some(ColumnType::String)),
        (r#""N/A""#, "5", Some(ColumnType::String)),
        ("5", "1.5", Some(ColumnType::Float64)),
        (
            "1679711330",
            r#""2024-01-15T10:30:00Z""#,
            Some(ColumnType::Datetime),
        ),
    ];
    for (first, second, want) in cases {
        let trie = Trie::new();
        let line = format!(r#"{{"v":{first}}}"#);
        feed(&trie, &[&line, &format!(r#"{{"v":{second}}}"#)]);
        assert_eq!(
            column_of(&trie, &line, "v").0,
            want,
            "{first} then {second}"
        );
    }
    // A column of RFC 3339 dates is no column of counts: an integer makes it `String`,
    // whether or not epoch digits arrived beside the dates.
    let (rfc, digits, int) = (
        r#"{"v":"2024-01-15T10:30:00Z"}"#,
        r#"{"v":1679711330}"#,
        r#"{"v":5}"#,
    );
    for lines in [
        vec![rfc, int],
        vec![rfc, digits, int],
        vec![digits, rfc, int],
        vec![digits, int, rfc],
        vec![int, digits, rfc],
    ] {
        let trie = Trie::new();
        feed(&trie, &lines);
        assert_eq!(
            column_of(&trie, lines[0], "v").0,
            Some(ColumnType::String),
            "{lines:?}"
        );
    }
}

/// What the catalog's records say `v` is, last record winning: its type and what it says
/// of its values.
fn recorded(records: &[Record]) -> (Option<ColumnType>, Option<bool>) {
    records
        .iter()
        .filter_map(|record| match record {
            Record::Column {
                name,
                col_type,
                epoch_digits,
                ..
            } if name == "v" => Some((*col_type, *epoch_digits)),
            _ => None,
        })
        .next_back()
        .expect("a record of `v`")
}

/// **A `Datetime` column records whether its values were all epoch digits, and a restart
/// reads it back**: so the column widens after a restart as it would have before it -
/// digits and an integer to `Int64`, an RFC 3339 date and an integer to `String`.
#[test]
fn a_datetime_column_records_what_its_values_were_across_a_restart() {
    let (rfc, digits, millis) = (
        r#"{"v":"2024-01-15T10:30:00Z"}"#,
        r#"{"v":1679711330}"#,
        r#"{"v":"1679711331000"}"#,
    );
    // The lines before the restart, what the catalog then says of `v`, and the type an
    // integer arriving after the restart makes it.
    let cases: [(Vec<&str>, Option<bool>, ColumnType); 4] = [
        (vec![digits], Some(true), ColumnType::Int64),
        (vec![digits, millis], Some(true), ColumnType::Int64),
        (vec![rfc], Some(false), ColumnType::String),
        // The first RFC 3339 date in a column of digits is a record of its own.
        (vec![digits, rfc], Some(false), ColumnType::String),
    ];
    for (at, (lines, says, then)) in cases.into_iter().enumerate() {
        let dir = scratch(&format!("epoch-digits-{at}"));
        let path = crate::engine::templates::catalog::path_in(&dir);
        let trie = Trie::new_in(&dir.display().to_string());
        trie.attach_catalog(Catalog::open(&path, &mut |_| {}).expect("open").0);
        feed(&trie, &lines);
        let before = std::fs::metadata(&path).expect("the catalog").len();
        // More of what it has already seen changes nothing, and writes nothing.
        feed(&trie, &lines[lines.len() - 1..]);
        assert_eq!(std::fs::metadata(&path).expect("the catalog").len(), before);
        drop(trie);

        let (catalog, records) = Catalog::open(&path, &mut |_| {}).expect("reopen");
        assert_eq!(
            recorded(&records),
            (Some(ColumnType::Datetime), says),
            "{lines:?}"
        );
        let again = Trie::from_catalog("data", &records, &mut |w| panic!("{w}")).expect("rebuild");
        again.attach_catalog(catalog);
        feed(&again, &[r#"{"v":5}"#]);
        assert_eq!(column_of(&again, lines[0], "v").0, Some(then), "{lines:?}");
        drop(again);
        // The widened column says nothing of its values: it is not a `Datetime`.
        let (_, records) = Catalog::open(&path, &mut |_| {}).expect("reopen");
        assert_eq!(recorded(&records), (Some(then), None), "{lines:?}");
    }
}

/// **A catalog from before the fact was kept says nothing of a `Datetime` column's
/// values**: an integer is then taken as the sign that they are counts, and the first RFC
/// 3339 date settles that they are not - and is recorded, so it stays settled.
#[test]
fn a_datetime_column_from_an_older_catalog_learns_what_its_values_are() {
    let older = || {
        vec![
            template_record(101, "v"),
            column_record(101, "v", Some(ColumnType::Datetime)),
        ]
    };
    let line = r#"{"v":1679711330}"#;

    let trie = Trie::from_catalog("data", &older(), &mut |w| panic!("{w}")).expect("rebuild");
    feed(&trie, &[line, r#"{"v":5}"#]);
    assert_eq!(column_of(&trie, line, "v").0, Some(ColumnType::Int64));

    let dir = scratch("epoch-digits-older");
    let path = crate::engine::templates::catalog::path_in(&dir);
    let trie = Trie::from_catalog("data", &older(), &mut |w| panic!("{w}")).expect("rebuild");
    trie.attach_catalog(Catalog::open(&path, &mut |_| {}).expect("open").0);
    // Digits say nothing of the values before them: no record. A date in text does.
    feed(&trie, &[line]);
    assert_eq!(std::fs::metadata(&path).expect("the catalog").len(), 0);
    feed(&trie, &[r#"{"v":"2024-01-15T10:30:00Z"}"#]);
    drop(trie);
    let (_, records) = Catalog::open(&path, &mut |_| {}).expect("reopen");
    assert_eq!(
        recorded(&records),
        (Some(ColumnType::Datetime), Some(false))
    );
    let mut all = older();
    all.extend(records);
    let trie = Trie::from_catalog("data", &all, &mut |w| panic!("{w}")).expect("rebuild");
    feed(&trie, &[r#"{"v":5}"#]);
    assert_eq!(column_of(&trie, line, "v").0, Some(ColumnType::String));
}

/// **A time column in a form only a time column reads is the template's time source,
/// and its column keeps the type the ladder gave it**: CockroachDB's decimal seconds are
/// `Float64`, Bluesky's microseconds `Int64`, PostgreSQL's zoned date `String`.
#[test]
fn a_time_column_form_makes_the_time_source_and_leaves_the_column_s_type() {
    let names = vec!["timestamp".to_string(), "time_us".into()];
    let cases = [
        (
            r#"{"timestamp":"1679711330.570420890","channel":"OPS"}"#,
            "timestamp",
            ColumnType::Float64,
        ),
        (
            r#"{"timestamp":1679711330.57042,"channel":"OPS"}"#,
            "timestamp",
            ColumnType::Float64,
        ),
        (
            r#"{"time_us":1732206349000167,"kind":"commit"}"#,
            "time_us",
            ColumnType::Int64,
        ),
        (
            r#"{"time_us":"1679711330570420890","kind":"commit"}"#,
            "time_us",
            ColumnType::Int64,
        ),
        (
            r#"{"timestamp":"2023-03-27 00:26:35.719 EDT","pid":27}"#,
            "timestamp",
            ColumnType::String,
        ),
    ];
    for (line, key, ty) in cases {
        let trie = Trie::new().with_time_columns(names.clone());
        feed(&trie, &[line]);
        assert_eq!(
            column_of(&trie, line, key),
            (Some(ty), TimeSource::Column(key.to_string())),
            "{line}"
        );
    }
}

/// **Only a configured time column is read that way**: a sixteen-digit id under any
/// other key is an `Int64` and makes no time source, and a time column whose value is in
/// no form a time column reads - `IST`, a tenth fraction digit - leaves the template on
/// arrival time.
#[test]
fn a_form_outside_a_time_column_or_one_not_read_makes_no_time_source() {
    let names = vec!["timestamp".to_string(), "time_us".into()];
    let cases = [
        (
            r#"{"order_id":1732206349000167,"kind":"commit"}"#,
            "order_id",
            ColumnType::Int64,
        ),
        (
            r#"{"elapsed":"1679711330.570420890","kind":"commit"}"#,
            "elapsed",
            ColumnType::Float64,
        ),
        (
            r#"{"note":"2023-03-27 00:26:35.719 EDT","kind":"commit"}"#,
            "note",
            ColumnType::String,
        ),
        (
            r#"{"timestamp":"2023-03-27 00:26:35.719 IST","pid":27}"#,
            "timestamp",
            ColumnType::String,
        ),
        (
            r#"{"timestamp":"1679711330.5704208901","pid":27}"#,
            "timestamp",
            ColumnType::Float64,
        ),
        (
            r#"{"time_us":4102444800000000,"kind":"commit"}"#,
            "time_us",
            ColumnType::Int64,
        ),
        (
            r#"{"time_us":173220634900016,"kind":"commit"}"#,
            "time_us",
            ColumnType::Int64,
        ),
    ];
    for (line, key, ty) in cases {
        let trie = Trie::new().with_time_columns(names.clone());
        feed(&trie, &[line]);
        assert_eq!(
            column_of(&trie, line, key),
            (Some(ty), TimeSource::Arrival),
            "{line}"
        );
    }
    // The default names do not include `time_us`: the same line, not configured.
    let trie = Trie::new();
    let line = r#"{"time_us":1732206349000167,"kind":"commit"}"#;
    feed(&trie, &[line]);
    assert_eq!(
        column_of(&trie, line, "time_us"),
        (Some(ColumnType::Int64), TimeSource::Arrival)
    );
    // The first configured name that reads as a time wins, a `Datetime` or a form alike.
    let trie = Trie::new().with_time_columns(names.clone());
    let line = r#"{"time_us":1732206349000167,"timestamp":"N/A"}"#;
    feed(&trie, &[line]);
    assert_eq!(
        column_of(&trie, line, "time_us").1,
        TimeSource::Column("time_us".into())
    );
    // A null is no time.
    let trie = Trie::new().with_time_columns(names);
    let line = r#"{"time_us":null,"kind":"commit"}"#;
    feed(&trie, &[line]);
    assert_eq!(
        column_of(&trie, line, "time_us"),
        (None, TimeSource::Arrival)
    );
}

/// **`trie.json` keeps what a `Datetime` column says of its values too**, so a trie
/// restored from the snapshot alone widens as the one that was saved would.
#[test]
fn a_trie_restored_from_its_snapshot_knows_what_its_datetime_columns_held() {
    let dir = scratch("epoch-digits-snapshot");
    let file = dir.join("trie.json");
    let (counts, dated) = (r#"{"a":1679711330}"#, r#"{"b":"2024-01-15T10:30:00Z"}"#);
    let trie = Trie::new();
    feed(&trie, &[counts, dated]);
    crate::engine::templates::snapshot::save(&trie, &file).expect("save");

    let (back, failed) = crate::engine::templates::snapshot::load_reporting(&file, "data");
    assert!(failed.is_none(), "the snapshot did not load");
    feed(&back, &[r#"{"a":5}"#, r#"{"b":5}"#]);
    assert_eq!(column_of(&back, counts, "a").0, Some(ColumnType::Int64));
    assert_eq!(column_of(&back, dated, "b").0, Some(ColumnType::String));
}
