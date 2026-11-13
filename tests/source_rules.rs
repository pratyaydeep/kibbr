// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! What the compiler does not hold the source to, held here by reading it: which layer
//! may name which, and the licence header on every file.
//!
//! The engine's machinery is private to it, and that the compiler enforces (E0603). What
//! it cannot enforce is the order of the layers in one crate: the engine (`src/engine/`)
//! never names the async runtime, HTTP, the command line or anything above it, so it stays
//! a database with no runtime in it; the server (`src/server/`) never names the command line,
//! nor `engine::inspect`, the door the command line's diagnostics read past the handle by.
//! The server's own tests (`src/server_tests.rs`) may use that door, and only that one.
//! Comments are not read; a `use` is read with its braces expanded, so
//! `use crate::{cli, server}` names `crate::cli`.

use std::fs;
use std::path::{Path, PathBuf};

/// The licence header every `.rs` file starts with.
const HEADER: [&str; 2] = [
    "// Copyright (C) 2026 Pratyaydeep Ghanta",
    "// SPDX-License-Identifier: AGPL-3.0-only",
];

/// What a layer's code must never name, and the files of it that may name those anyway.
const RULES: &[(&str, &[&str], &[&str])] = &[
    (
        "src/engine/",
        &[
            "tokio::",
            "tokio_util::",
            "axum::",
            "clap::",
            "toml::",
            "mimalloc::",
            "crate::server",
            "crate::cli",
            "crate::bench",
        ],
        &[],
    ),
    (
        "src/engine.rs",
        &["crate::server", "crate::cli", "crate::bench"],
        &[],
    ),
    ("src/server", &["clap::", "crate::cli", "crate::bench"], &[]),
    ("src/server", &["engine::inspect"], &["src/server_tests.rs"]),
];

fn root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

/// Every `.rs` file under `dir`, by its path from the crate root with `/` between the
/// parts; build and fuzzer output directories are not walked.
fn rust_files(dir: &Path, out: &mut Vec<(String, PathBuf)>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries {
        let path = entry.expect("a directory entry").path();
        let name = path.file_name().map(|n| n.to_string_lossy().into_owned());
        if path.is_dir() {
            if !matches!(
                name.as_deref(),
                Some("target" | "corpus" | "artifacts" | "coverage")
            ) {
                rust_files(&path, out);
            }
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            let below = path.strip_prefix(root()).expect("under the crate root");
            let parts: Vec<String> = below
                .components()
                .map(|part| part.as_os_str().to_string_lossy().into_owned())
                .collect();
            out.push((parts.join("/"), path));
        }
    }
}

/// The code of a file with every `//` comment cut off its line.
fn code_of(text: &str) -> String {
    text.lines()
        .map(|line| line.split("//").next().unwrap_or(""))
        .collect::<Vec<_>>()
        .join("\n")
}

/// The paths a `use` tree names, its braces expanded: `a::{b, c::{d, e}}` is `a::b`,
/// `a::c::d` and `a::c::e`. Spaces and line breaks are dropped.
fn expand(tree: &str) -> Vec<String> {
    let tree: String = tree.chars().filter(|c| !c.is_whitespace()).collect();
    // Crude on purpose: what follows `use ` in a string literal is not a tree, and comes
    // back whole.
    let Some(open) = tree.find('{') else {
        return vec![tree];
    };
    let Some(close) = tree.rfind('}').filter(|close| *close > open) else {
        return vec![tree];
    };
    let (prefix, body) = (&tree[..open], &tree[open + 1..close]);
    let mut parts = Vec::new();
    let (mut depth, mut start) = (0, 0);
    for (at, c) in body.char_indices() {
        match c {
            '{' => depth += 1,
            '}' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(&body[start..at]);
                start = at + 1;
            }
            _ => {}
        }
    }
    parts.push(&body[start..]);
    parts
        .into_iter()
        .filter(|part| !part.is_empty())
        .flat_map(expand)
        .map(|path| format!("{prefix}{path}"))
        .collect()
}

/// The code of a file, comments cut, followed by every path its `use` declarations name,
/// one a line: what a word is looked for in.
fn named_in(text: &str) -> String {
    let code = code_of(text);
    let mut out = code.clone();
    for (at, _) in code.match_indices("use ") {
        if code[..at]
            .chars()
            .next_back()
            .is_some_and(|c| c.is_alphanumeric() || c == '_')
        {
            continue;
        }
        let Some(end) = code[at..].find(';') else {
            continue;
        };
        for path in expand(&code[at + "use ".len()..at + end]) {
            out.push('\n');
            out.push_str(&path);
        }
    }
    out
}

/// Whether `code` holds `word` with no identifier character just before it.
fn names(code: &str, word: &str) -> bool {
    code.match_indices(word).any(|(at, _)| {
        !code[..at]
            .chars()
            .next_back()
            .is_some_and(|c| c.is_alphanumeric() || c == '_')
    })
}

/// What the file `name`, holding `text`, names that its layer may not.
fn broken(name: &str, text: &str) -> Vec<String> {
    let named = named_in(text);
    let mut out = Vec::new();
    for (layer, banned, exempt) in RULES {
        if !name.starts_with(layer) || exempt.contains(&name) {
            continue;
        }
        for word in banned.iter().filter(|word| names(&named, word)) {
            out.push(format!("{name} names `{word}`, which {layer} may not"));
        }
    }
    out
}

#[test]
fn each_layer_names_only_the_layers_under_it() {
    let mut files = Vec::new();
    rust_files(&root().join("src"), &mut files);
    assert!(files.len() > 100, "{} source files", files.len());
    assert!(files
        .iter()
        .any(|(name, _)| name == "src/server/handlers.rs"));
    let mut found = Vec::new();
    for (name, path) in &files {
        found.extend(broken(
            name,
            &fs::read_to_string(path).expect("a source file"),
        ));
    }
    assert!(found.is_empty(), "\n{}", found.join("\n"));
}

/// The rule holds on what it is held to: a word is found where it is code, and not where
/// it is part of a longer name or in a comment.
#[test]
fn the_rule_reads_code_and_not_comments() {
    assert!(names(&code_of("use tokio::sync::Notify;"), "tokio::"));
    assert!(!names(&code_of("// tokio::spawn would block"), "tokio::"));
    assert!(!names(&code_of("use my_tokio::Thing;"), "tokio::"));
    assert!(names(&code_of("x(crate::cli::run)"), "crate::cli"));
}

/// A grouped `use` is caught as a plain one is, the server's tests may name
/// `engine::inspect` and nothing else the server may not, and a comment is not code.
#[test]
fn the_rule_catches_a_name_inside_braces() {
    let flagged = [
        (
            "src/server/handlers.rs",
            "use crate::{cli, server::Report};",
        ),
        (
            "src/server.rs",
            "use crate::{\n    engine,\n    cli::config::File,\n};",
        ),
        (
            "src/server/routes.rs",
            "use crate::engine::{self, inspect, Database};",
        ),
        (
            "src/server/body.rs",
            "use crate::engine::{Database, inspect::{Trie, Node}};",
        ),
        (
            "src/server_tests.rs",
            "use crate::{cli::config, engine::inspect};",
        ),
        (
            "src/engine/wal.rs",
            "pub(crate) use crate::{server::Report};",
        ),
        ("src/engine.rs", "use crate::{bench};"),
    ];
    for (name, text) in flagged {
        assert!(!broken(name, text).is_empty(), "{name}: {text}");
    }
    let clean = [
        (
            "src/server_tests.rs",
            "use crate::engine::{inspect::{self as db, Wal}, Database};",
        ),
        (
            "src/server/routes.rs",
            "use crate::engine::{self, Database};",
        ),
        ("src/server.rs", "// use crate::{cli};"),
        ("src/cli/query.rs", "use crate::{engine::inspect, server};"),
    ];
    for (name, text) in clean {
        assert!(broken(name, text).is_empty(), "{name}: {text}");
    }
    assert_eq!(
        expand("a::{b, c::{d, e}, self as f}"),
        ["a::b", "a::c::d", "a::c::e", "a::selfasf"]
    );
}

/// Every `.rs` file under src/, tests/ and fuzz/ starts with the two header lines.
#[test]
fn every_source_file_carries_the_licence_header() {
    let mut files = Vec::new();
    for dir in ["src", "tests", "fuzz"] {
        rust_files(&root().join(dir), &mut files);
    }
    assert!(files.len() > 100, "{} source files", files.len());
    let missing: Vec<&str> = files
        .iter()
        .filter(|(_, path)| {
            let text = fs::read_to_string(path).expect("a source file");
            let mut lines = text.lines();
            !HEADER.iter().all(|want| lines.next() == Some(*want))
        })
        .map(|(name, _)| name.as_str())
        .collect();
    assert!(
        missing.is_empty(),
        "no header (the first two lines must be `{}` and `{}`):\n{}",
        HEADER[0],
        HEADER[1],
        missing.join("\n")
    );
}
