//! The trie, tested through the binary. The load-bearing test is
//! `the_tree_is_stable_across_input_orderings`: re-running the same file passes without the
//! type join, and feeding the same lines in a different order is what makes it observable.

use std::process::{Command, Stdio};

fn kibbr() -> Command {
    Command::new(env!("CARGO_BIN_EXE_kibbr"))
}

/// Feeds stdin from a separate thread. Writing inline deadlocks as soon as the input
/// outgrows a pipe buffer: the child blocks writing stdout while we block writing stdin.
fn run_stdin(args: &[&str], input: &str) -> String {
    use std::io::Write;

    let mut child = kibbr()
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("failed to spawn kibbr");
    let mut stdin = child.stdin.take().expect("stdin was not piped");
    let owned = input.as_bytes().to_vec();
    let writer = std::thread::spawn(move || {
        let _ = stdin.write_all(&owned);
    });
    let out = child.wait_with_output().expect("failed to wait on kibbr");
    writer.join().expect("stdin writer thread panicked");
    assert!(
        out.status.success(),
        "kibbr {args:?} exited {:?}\nstderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("stdout was not utf-8")
}

fn gen(lines: u64, templates: u64, seed: u64) -> String {
    let out = kibbr()
        .args([
            "gen-logs",
            "--lines",
            &lines.to_string(),
            "--templates",
            &templates.to_string(),
            "--seed",
            &seed.to_string(),
        ])
        .output()
        .expect("failed to run gen-logs");
    assert!(out.status.success(), "gen-logs failed");
    String::from_utf8(out.stdout).expect("generated output was not utf-8")
}

fn demo(input: &str) -> String {
    run_stdin(&["demo", "-"], input)
}

/// The tree with every leaf's template id blanked: ids are handed out in the order
/// templates were first seen, so a reordered file moves them and nothing else.
fn without_ids(tree: &str) -> String {
    let mut out = String::with_capacity(tree.len());
    for line in tree.lines() {
        match line.split_once("● #") {
            Some((head, tail)) => {
                out.push_str(head);
                out.push_str("● #");
                out.push_str(tail.trim_start_matches(|c: char| c.is_ascii_digit()));
            }
            None => out.push_str(line),
        }
        out.push('\n');
    }
    out
}

fn summary(tree: &str) -> &str {
    tree.lines()
        .rev()
        .find(|line| line.contains(" lines → "))
        .expect("no summary line in demo output")
}

/// Every leaf line, id blanked, sorted - the shape as a set, independent of how the tree
/// was walked to print it.
fn leaves(tree: &str) -> Vec<String> {
    let mut out: Vec<String> = without_ids(tree)
        .lines()
        .filter(|line| line.contains("● #"))
        .map(str::to_string)
        .collect();
    out.sort();
    out
}

/// What the trie has to guarantee: the same lines, three orders, one tree.
#[test]
fn the_tree_is_stable_across_input_orderings() {
    let generated = gen(20_000, 12, 42);
    let lines: Vec<&str> = generated.lines().collect();

    let forwards = lines.join("\n") + "\n";
    let backwards = lines.iter().rev().copied().collect::<Vec<_>>().join("\n") + "\n";

    // Every seventh line, seven times over: reversing alone is a weak shuffle, still
    // delivering each template's lines in the same runs.
    let mut interleaved: Vec<&str> = Vec::with_capacity(lines.len());
    for offset in 0..7 {
        interleaved.extend(lines.iter().skip(offset).step_by(7).copied());
    }
    assert_eq!(
        lines.len(),
        interleaved.len(),
        "the strided file lost lines"
    );
    let strided = interleaved.join("\n") + "\n";

    let a = demo(&forwards);
    let b = demo(&backwards);
    let c = demo(&strided);

    // Shape and column types both, to the byte.
    assert_eq!(
        without_ids(&a),
        without_ids(&b),
        "reversed input built a different tree"
    );
    assert_eq!(
        without_ids(&a),
        without_ids(&c),
        "strided input built a different tree"
    );
    // And the counts, which the tree's summarised subtrees would otherwise hide.
    assert_eq!(summary(&a), summary(&b));
    assert_eq!(summary(&a), summary(&c));
}

/// Same, on the whole trie rather than the operator's view of it: a tree that agrees about
/// what it is not showing is not evidence of much.
#[test]
fn the_full_tree_is_stable_across_input_orderings() {
    let generated = gen(20_000, 12, 42);
    let lines: Vec<&str> = generated.lines().collect();
    let forwards = lines.join("\n") + "\n";
    let backwards = lines.iter().rev().copied().collect::<Vec<_>>().join("\n") + "\n";

    let a = run_stdin(&["demo", "--full", "-"], &forwards);
    let b = run_stdin(&["demo", "--full", "-"], &backwards);

    assert_eq!(without_ids(&a), without_ids(&b));
    // Not vacuous: the tree has to have had something in it to be stable about.
    assert!(leaves(&a).len() > 500, "only {} leaves", leaves(&a).len());
}

/// One key on four lines of one template, in all twenty-four orders: the column is `String`
/// whichever way they arrive, so the join is commutative and associative.
#[test]
fn the_column_join_does_not_depend_on_arrival_order() {
    let lines = [
        r#"{"k":"a","order_id":4500000123}"#,
        r#"{"k":"a","order_id":1700000123}"#,
        r#"{"k":"a","order_id":9900001234567}"#,
        r#"{"k":"a","order_id":"AX-99213"}"#,
    ];

    for order in permutations(&lines) {
        let tree = demo(&(order.join("\n") + "\n"));
        assert!(
            tree.contains("order_id:Str"),
            "order {order:?} settled on something else:\n{tree}"
        );
    }
}

/// A column never narrows. `Int64 ⊔ Float64` is `Float64` from either side, and anything
/// else joins to `String`.
#[test]
fn a_column_widens_and_never_narrows() {
    let int = r#"{"k":"a","n":7}"#;
    let float = r#"{"k":"a","n":7.5}"#;
    let text = r#"{"k":"a","n":"seven"}"#;

    for order in permutations(&[int, float]) {
        let tree = demo(&(order.join("\n") + "\n"));
        assert!(tree.contains("n:F64"), "Int64 ⊔ Float64 gave:\n{tree}");
    }
    for order in permutations(&[int, float, text]) {
        let tree = demo(&(order.join("\n") + "\n"));
        assert!(tree.contains("n:Str"), "order {order:?} gave:\n{tree}");
    }
}

/// A column that has only ever held nulls has no type, which is a different statement
/// from `String` - and the reason `ColumnType` has no `Null` variant to print.
#[test]
fn a_column_that_has_only_held_nulls_has_no_type() {
    let tree = demo("{\"a\":null,\"b\":1}\n{\"a\":null,\"b\":2}\n");
    assert!(tree.contains("a:? b:I64"), "{tree}");

    // One non-null value is all it takes; the null before it claimed nothing.
    let tree = demo("{\"a\":null,\"b\":1}\n{\"a\":\"x\",\"b\":2}\n");
    assert!(tree.contains("a:Str b:I64"), "{tree}");
}

/// The path is walk order; the fingerprint is identity order; neither moved the other.
/// Walk order hoists `event=` to the first edge, so a pattern over it is walked.
#[test]
fn the_path_is_walk_order_and_the_fingerprint_is_still_identity_order() {
    let line = r#"{"amount":1.5,"event":"processed","zone":"eu"}"#;
    let input = format!("{line}\n");

    let tree = demo(&input);
    assert!(
        tree.contains("└── event=processed → amount → zone ● #101"),
        "{tree}"
    );

    let parsed = run_stdin(&["parse", "-"], &input);
    assert!(
        parsed.contains("amount | event=processed | zone"),
        "identity order moved:\n{parsed}"
    );
}

/// What the summary counts. Two templates, one a prefix of the other: five tokens plus
/// the root they share is six nodes unshared, four nodes shared, two saved.
#[test]
fn the_saving_is_the_nodes_the_shared_prefix_removed() {
    let tree = demo("{\"a\":1,\"b\":2}\n{\"a\":1,\"b\":2,\"c\":3}\n");
    assert_eq!(
        summary(&tree),
        "2 lines → 2 templates; 6 tokens → 4 nodes, 2 saved by shared prefixes",
        "{tree}"
    );
    // The shorter template's leaf is an interior node of the longer one, and keeps its own
    // id and its own key set: `c` is absent from one and present in the other.
    assert!(tree.contains("└── a → b ● #101"), "{tree}");
    assert!(tree.contains("    └── c ● #102"), "{tree}");
}

/// The reference numbers, on the file they were measured over. The template count is far
/// above `--templates`: optional fields split each archetype by present-key subset.
#[test]
fn seed_42_matches_the_reference_numbers() {
    let tree = demo(&gen(100_000, 12, 42));

    assert_eq!(
        summary(&tree),
        "100,000 lines → 869 templates; 11,739 tokens → 4,026 nodes, 7,713 saved by shared prefixes",
        "{tree}"
    );

    // The two archetypes with no promoted `event` are the whole of the printed tree. The pair
    // below, alike but for `shard.checkpoint`, is the case schema merging folds back.
    for expected in [
        "├── err.cause.code",
        "│   ├── err.cause.detail → err.code → err.trace_id → level → msg → service → ts_unix ● #101",
        "│   └── err.code → err.trace_id → level → msg → service → ts_unix ● #169",
        "├── event=login_failed        … 143 templates",
        "└── level → region → service",
        "    ├── shard.checkpoint → shard.id → shard.leader → shard.rows → ts ● #118",
        "    │       level:Str region:Str service:Str shard.checkpoint:DT shard.id:I64",
        "    │       shard.leader:Bool shard.rows:I64 ts:DT",
        "    └── shard.id → shard.leader → shard.rows → ts ● #105",
    ] {
        assert!(
            tree.lines().any(|line| line == expected),
            "missing from the tree: {expected:?}\n{tree}"
        );
    }
}

/// Every ordering of `items`, by repeated insertion - small inputs only, which is all
/// these tests feed it.
fn permutations<'a>(items: &[&'a str]) -> Vec<Vec<&'a str>> {
    let Some((first, rest)) = items.split_first() else {
        return vec![Vec::new()];
    };
    let mut out = Vec::new();
    for shorter in permutations(rest) {
        for at in 0..=shorter.len() {
            let mut one = shorter.clone();
            one.insert(at, first);
            out.push(one);
        }
    }
    out
}
