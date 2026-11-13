// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::Path;
use std::time::Duration;

use super::*;

/// Every `.rs` file under `dir`, by its path below `root` with `/` between the
/// parts, and its text.
fn sources(root: &Path, dir: &Path, out: &mut Vec<(String, String)>) {
    for entry in std::fs::read_dir(dir).expect("a source directory") {
        let path = entry.expect("an entry").path();
        if path.is_dir() {
            sources(root, &path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            let below = path.strip_prefix(root).expect("under the root");
            let name: Vec<String> = below
                .components()
                .map(|part| part.as_os_str().to_string_lossy().into_owned())
                .collect();
            let text = std::fs::read_to_string(&path).expect("a source file");
            out.push((name.join("/"), text));
        }
    }
}

/// A `par_iter` or a turn in an `async fn` handler would stall every request in flight;
/// neither can be written without one of these two words.
#[test]
fn rayon_is_named_in_this_file_alone_and_a_turn_is_taken_in_the_scan_alone() {
    // Every source file of the crate: the handlers this is about are in the server, not
    // in the engine.
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    sources(root, &root.join("src"), &mut files);
    assert!(files.len() > 40, "{} source files", files.len());
    assert!(
        files.iter().any(|(name, _)| name == "src/server/handlers.rs"),
        "the server's handlers are among the files read"
    );
    let holding = |word: &str| -> Vec<&str> {
        let mut names: Vec<&str> = files
            .iter()
            .filter(|(_, text)| text.contains(word))
            .map(|(name, _)| name.as_str())
            .collect();
        names.sort_unstable();
        names
    };
    // Each module's tests are a file of their own beside its code, and name these words too:
    // these in asking, the scan's in taking a turn.
    assert_eq!(
        holding("rayon"),
        [
            "src/engine/store/scan_threads.rs",
            "src/engine/store/scan_threads_tests.rs"
        ]
    );
    assert_eq!(
        holding(".turn("),
        [
            "src/engine/store/disk/scan.rs",
            "src/engine/store/disk/scan_tests.rs",
            "src/engine/store/scan_threads_tests.rs"
        ]
    );
}

/// Every item, each once, however many there are and however long each takes.
#[test]
fn each_does_the_work_on_every_item_once() {
    for threads in [2, 3, 4] {
        let fan = Fan::new(threads, 0).expect("a fan");
        for n in [0usize, 1, 2, 3, 4, 5, 1_000] {
            let items: Vec<Mutex<(usize, u32)>> =
                (0..n).map(|at| Mutex::new((at, 0))).collect();
            let turn = fan.turn().expect("a turn");
            turn.each(&items, |(at, times)| {
                // Uneven: one item in seven holds its thread while the rest go by.
                if *at % 7 == 3 {
                    std::thread::sleep(Duration::from_micros(200));
                }
                *times += 1;
                *at *= 2;
            });
            drop(turn);
            let got: Vec<(usize, u32)> = items.iter().map(|item| *lock(item)).collect();
            let want: Vec<(usize, u32)> = (0..n).map(|at| (at * 2, 1)).collect();
            assert_eq!(got, want, "{n} items on {threads} threads");
        }
        assert_eq!(fan.fanned(), 7);
    }
}

/// The asking thread works too, and so does the pool.
#[test]
fn the_asking_thread_and_the_pool_both_take_items() {
    let fan = Fan::new(4, 0).expect("a fan");
    let items: Vec<Mutex<Option<std::thread::ThreadId>>> =
        (0..1_000).map(|_| Mutex::new(None)).collect();
    let turn = fan.turn().expect("a turn");
    turn.each(&items, |by| {
        std::thread::sleep(Duration::from_micros(20));
        *by = Some(std::thread::current().id());
    });
    let me = std::thread::current().id();
    let mine = items.iter().filter(|by| *lock(by) == Some(me)).count();
    assert!(mine > 0, "the asking thread took nothing");
    assert!(mine < items.len(), "the pool took nothing");
    assert_eq!(fan.entered(), [me]);
}

/// Under two threads there is nobody to share with, and no `Fan`.
#[test]
fn under_two_threads_there_is_no_fan() {
    assert!(Fan::new(0, 0).is_none());
    assert!(Fan::new(1, 0).is_none());
    let fan = Fan::new(2, 7).expect("a fan");
    assert_eq!((fan.least_rows(), fan.fanned()), (7, 0));
}

/// A store that is never asked a query worth sharing out starts no thread.
#[test]
fn no_thread_is_started_before_the_first_turn() {
    let fan = Fan::new(4, 0).expect("a fan");
    assert!(!fan.started());
    drop(fan.body());
    assert!(!fan.started());
    // Refused for a body in its task: still none.
    let body = fan.body();
    assert!(fan.turn().is_none());
    assert!(!fan.started());
    drop(body);
    assert!(fan.turn().is_some());
    assert!(fan.started());
}

/// One scan holds the pool at a time, and none does while an ingest body is in its
/// task. Neither refusal waits, and each ends when what caused it is dropped.
#[test]
fn a_turn_is_refused_while_one_is_held_or_a_body_is_in_its_task() {
    let fan = Fan::new(3, 0).expect("a fan");
    let first = fan.turn().expect("the first turn");
    assert!(fan.turn().is_none(), "two scans held the pool");
    drop(first);
    let again = fan.turn().expect("a turn after the first was given back");
    drop(again);

    let one = fan.body();
    let two = fan.body();
    assert!(fan.turn().is_none(), "a turn beside two bodies");
    drop(one);
    assert!(fan.turn().is_none(), "a turn beside one body");
    drop(two);
    assert!(fan.turn().is_some());
    // A body that arrives while a turn is held does not take the turn back: the run
    // of row groups that began in a gap is finished.
    let turn = fan.turn().expect("a turn");
    let late = fan.body();
    let items = [Mutex::new(0), Mutex::new(0)];
    turn.each(&items, |n| *n += 1);
    assert_eq!((*lock(&items[0]), *lock(&items[1])), (1, 1));
    drop((turn, late));
}

/// A panic in the work reaches the thread that asked, and the pool is given back, so
/// the next scan is not refused for the life of the process.
#[test]
fn a_panic_in_the_work_leaves_each_as_a_panic_and_gives_the_pool_back() {
    let fan = Fan::new(4, 0).expect("a fan");
    for bad in [0usize, 1, 37, 99] {
        let items: Vec<Mutex<usize>> = (0..100).map(Mutex::new).collect();
        let ran = catch_unwind(AssertUnwindSafe(|| {
            let turn = fan.turn().expect("a turn");
            turn.each(&items, |at| {
                if *at == bad {
                    panic!("item {at} is the bad one");
                }
            });
        }));
        assert!(ran.is_err(), "the panic of item {bad} was lost");
        let turn = fan.turn().expect("a turn after a panic");
        let items: Vec<Mutex<usize>> = (0..10).map(Mutex::new).collect();
        turn.each(&items, |at| *at += 1);
        let got: Vec<usize> = items.iter().map(|item| *lock(item)).collect();
        assert_eq!(got, (1..=10).collect::<Vec<_>>());
    }
}
