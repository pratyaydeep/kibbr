// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The rules of the layers that the compiler does not hold, read off the source as text.
//!
//! The compiler holds the main rule: the engine is the library, and a library cannot see
//! the binary that uses it. What is left is here, and each rule has a number:
//!
//! 1. Every dependency in `Cargo.toml` is in the list of at least one layer.
//! 2. A layer names only the external crates in its own list.
//! 3. The server names neither the command line nor the engine's `internals`.
//! 4. The engine does not write `kibbr::`, opens no socket, and has two features only.
//! 5. `test-support` only adds whole items.
//! 6. Every `.rs` file starts with the two licence lines.
//! 8. A production command never names the tools.
//!
//! (Rule 7 is about where unit tests sit, and is not here yet.)
//!
//! The test reads lines of text and has no parser: it does not follow a path inside the
//! crate or open a `use` tree. Each rule also has a test of its own below, which gives the
//! rule a line that breaks it and makes sure that the rule catches it.

use std::fs;
use std::path::Path;

// ── The lists ──────────────────────────────────────────────────────────────────────

/// What each layer may name of the dependencies. No list says "anything", or the test
/// could never fail. `kibbr` is the library itself, which the two layers of the binary name.
const ENGINE: &[&str] = &[
    "chrono",
    "crc32c",
    "flate2",
    "libc",
    "rayon",
    "serde",
    "serde_json",
    "thiserror",
    "tracing",
    "zstd",
];
const SERVER: &[&str] = &[
    "kibbr",
    "axum",
    "chrono",
    "flate2",
    "http-body",
    "serde",
    "serde_json",
    "thiserror",
    "tokio",
    "tokio-util",
    "tracing",
];
const COMMAND_LINE: &[&str] = &[
    "kibbr",
    "chrono",
    "clap",
    "flate2",
    "mimalloc",
    "serde",
    "serde_json",
    "thiserror",
    "toml",
    "tracing",
    "tokio-util",
    "zstd",
];

/// The only features the engine's code may ask about.
const FEATURES: &[&str] = &["test-support", "fuzzing"];

/// The two lines every `.rs` file starts with.
const LICENCE: &str =
    "// Copyright (C) 2026 Pratyaydeep Ghanta\n// SPDX-License-Identifier: AGPL-3.0-only\n";

/// The words an item starts with, after `pub` if it has one.
const ITEMS: &[&str] = &[
    "fn", "impl", "mod", "struct", "enum", "const", "static", "use",
];

/// Where `test-support` is on something that is not a whole item: the file, and the line
/// it is on, as text. One thing today: the list of threads a scan entered, which is a
/// field, the value it starts with, and the one statement that adds to it.
const NOT_AN_ITEM: &[(&str, &str)] = &[
    (
        "src/engine/rows/scan_threads.rs",
        "entered: Mutex<Vec<std::thread::ThreadId>>,",
    ),
    (
        "src/engine/rows/scan_threads.rs",
        "entered: Mutex::new(Vec::new()),",
    ),
    (
        "src/engine/rows/scan_threads.rs",
        "lock(&self.0.entered).push(std::thread::current().id());",
    ),
];

// ── A file, and its layer ──────────────────────────────────────────────────────────

/// One source file: its path from the repository's root, with `/`, and its text.
struct Source {
    path: String,
    text: String,
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Layer {
    Engine,
    Server,
    CommandLine,
}

impl Layer {
    /// The layer a file under `src/` is in, and `None` for a file anywhere else.
    fn of(path: &str) -> Option<Layer> {
        if path.starts_with("src/engine/") {
            Some(Layer::Engine)
        } else if path == "src/server.rs" || path.starts_with("src/server/") {
            Some(Layer::Server)
        } else if path.starts_with("src/") {
            Some(Layer::CommandLine)
        } else {
            None
        }
    }

    fn may_name(self) -> &'static [&'static str] {
        match self {
            Layer::Engine => ENGINE,
            Layer::Server => SERVER,
            Layer::CommandLine => COMMAND_LINE,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Layer::Engine => "the engine",
            Layer::Server => "the server",
            Layer::CommandLine => "the command line",
        }
    }
}

// ── Reading a line ─────────────────────────────────────────────────────────────────

/// A line without its `//` comment. A `//` inside a string ends the line too, which only
/// means that the rest of that line is not read.
fn code(line: &str) -> &str {
    match line.find("//") {
        Some(at) => &line[..at],
        None => line,
    }
}

fn is_word(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// Whether `word` is in `text` with no letter, digit or `_` on either side of it.
fn has_word(text: &str, word: &str) -> bool {
    let mut from = 0;
    while let Some(at) = text[from..].find(word) {
        let start = from + at;
        let end = start + word.len();
        let before = text[..start].chars().next_back();
        let after = text[end..].chars().next();
        let alone = |c: Option<char>| match c {
            Some(c) => !is_word(c),
            None => true,
        };
        if alone(before) && alone(after) {
            return true;
        }
        from = end;
    }
    false
}

/// Whether `text` names the path `path` (`tokio::`, `crate::cmd`): the path with no
/// letter, digit or `_` before it, and none after it unless it ends in `::`.
fn has_path(text: &str, path: &str) -> bool {
    if path.ends_with("::") {
        let mut from = 0;
        while let Some(at) = text[from..].find(path) {
            let start = from + at;
            let before = text[..start].chars().next_back();
            let free = match before {
                Some(c) => !is_word(c),
                None => true,
            };
            if free {
                return true;
            }
            from = start + path.len();
        }
        false
    } else {
        has_word(text, path)
    }
}

/// The lines of a file that hold code, each with its number, counted from 1.
fn code_lines(text: &str) -> Vec<(usize, &str)> {
    let mut lines = Vec::new();
    for (at, line) in text.lines().enumerate() {
        let code = code(line);
        if !code.trim().is_empty() {
            lines.push((at + 1, code));
        }
    }
    lines
}

fn broken(source: &Source, line: usize, what: &str) -> String {
    format!("{}:{line}: {what}", source.path)
}

// ── Rule 1: every dependency is in a list ──────────────────────────────────────────

/// The names under `[dependencies]` in a manifest, as they are written there.
fn dependencies(manifest: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut inside = false;
    for line in manifest.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            inside = line == "[dependencies]";
            continue;
        }
        if !inside || line.is_empty() || line.starts_with('#') {
            continue;
        }
        let end = line
            .find(|c: char| c == '.' || c == '=' || c.is_whitespace())
            .unwrap_or(line.len());
        names.push(line[..end].to_string());
    }
    names
}

/// The dependencies that no layer lists: adding one must be a decision about a layer.
fn in_no_list(dependencies: &[String]) -> Vec<String> {
    let mut missing = Vec::new();
    for name in dependencies {
        let listed = [ENGINE, SERVER, COMMAND_LINE]
            .iter()
            .any(|list| list.contains(&name.as_str()));
        if !listed {
            missing.push(format!(
                "Cargo.toml: the dependency `{name}` is in no layer's list in \
                 tests/source_rules.rs; say which layer may name it"
            ));
        }
    }
    missing
}

// ── Rule 2: a layer names only its own crates ──────────────────────────────────────

/// A crate is always written `name::` where it is used, in a `use` or inline, so one
/// search a name is enough. `tokio-util` is `tokio_util` in code.
fn other_layers_crates(source: &Source, dependencies: &[String]) -> Vec<String> {
    let Some(layer) = Layer::of(&source.path) else {
        return Vec::new();
    };
    let mut found = Vec::new();
    for (number, line) in code_lines(&source.text) {
        for name in dependencies {
            if layer.may_name().contains(&name.as_str()) {
                continue;
            }
            let written = format!("{}::", name.replace('-', "_"));
            if has_path(line, &written) {
                found.push(broken(
                    source,
                    number,
                    &format!("{} names `{name}`, which is not in its list", layer.name()),
                ));
            }
        }
    }
    found
}

// ── Rule 3: the server names neither the command line nor `internals` ──────────────

/// The command line's modules: every `mod` of `src/main.rs` but the server.
fn command_line_modules(main: &str) -> Vec<String> {
    let mut names = Vec::new();
    for (_, line) in code_lines(main) {
        let Some(rest) = line.trim().strip_prefix("mod ") else {
            continue;
        };
        let Some(name) = rest.strip_suffix(';') else {
            continue;
        };
        if name != "server" {
            names.push(name.trim().to_string());
        }
    }
    names
}

/// The text between `crate::{` and the `}` that closes it, over as many lines as it takes.
fn crate_groups(text: &str) -> Vec<(usize, String)> {
    let lines = code_lines(text);
    let mut groups = Vec::new();
    for (at, (number, line)) in lines.iter().enumerate() {
        let Some(start) = line.find("crate::{") else {
            continue;
        };
        let mut group = String::new();
        let mut depth = 0;
        let mut closed = false;
        let first = &line[start + "crate::".len()..];
        let rest = lines[at + 1..].iter().map(|(_, line)| *line);
        for piece in std::iter::once(first).chain(rest) {
            for c in piece.chars() {
                if c == '{' {
                    depth += 1;
                } else if c == '}' {
                    depth -= 1;
                }
                if depth == 0 {
                    closed = true;
                    break;
                }
                group.push(c);
            }
            if closed {
                break;
            }
            group.push(' ');
        }
        groups.push((*number, group));
    }
    groups
}

fn server_reaches_up(source: &Source, command_line: &[String]) -> Vec<String> {
    if Layer::of(&source.path) != Some(Layer::Server) {
        return Vec::new();
    }
    // From the server's root file `super` is the crate; from a file under it, the server.
    let up = if source.path == "src/server.rs" {
        "super::"
    } else {
        "super::super::"
    };
    let mut found = Vec::new();
    for (number, line) in code_lines(&source.text) {
        if has_word(line, "internals") {
            found.push(broken(
                source,
                number,
                "the server names the engine's `internals`; it may use the engine's public \
                 items only",
            ));
        }
        for module in command_line {
            let named = has_path(line, &format!("crate::{module}"))
                || has_path(line, &format!("{up}{module}"));
            if named {
                found.push(broken(
                    source,
                    number,
                    &format!("the server names the command line's `{module}`"),
                ));
            }
        }
    }
    for (number, group) in crate_groups(&source.text) {
        // `crate::{server::x, config}`: a name is the command line's when it starts a path
        // of the group, which is when no `::` is before it.
        for module in command_line {
            let starts_a_path = group
                .split(',')
                .any(|path| has_word(path.trim().split("::").next().unwrap_or(""), module));
            if starts_a_path {
                found.push(broken(
                    source,
                    number,
                    &format!("the server names the command line's `{module}`"),
                ));
            }
        }
    }
    found
}

// ── Rule 4: what the engine must not write ─────────────────────────────────────────

fn engine_reaches_out(source: &Source) -> Vec<String> {
    if Layer::of(&source.path) != Some(Layer::Engine) {
        return Vec::new();
    }
    let mut found = Vec::new();
    for (number, line) in code_lines(&source.text) {
        // Inside the library the library is `crate::`. In its own unit tests `kibbr::` is a
        // second copy of the crate, whose types are not this copy's.
        if has_path(line, "kibbr::") {
            found.push(broken(
                source,
                number,
                "the engine writes `kibbr::`; inside the library it is `crate::`",
            ));
        }
        for socket in ["TcpListener", "TcpStream", "UdpSocket"] {
            if has_word(line, socket) {
                found.push(broken(
                    source,
                    number,
                    &format!("the engine names `{socket}`; it has no network"),
                ));
            }
        }
        let mut rest = line;
        while let Some(at) = rest.find("feature = \"") {
            rest = &rest[at + "feature = \"".len()..];
            let name = rest.split('"').next().unwrap_or("");
            if !FEATURES.contains(&name) {
                found.push(broken(
                    source,
                    number,
                    &format!("the engine asks about the feature `{name}`; it has two"),
                ));
            }
        }
    }
    found
}

// ── Rule 5: `test-support` only adds whole items ───────────────────────────────────

/// Whether a line starts an item: one of `ITEMS`, after `pub` or `pub(...)` if it has one.
fn starts_an_item(line: &str) -> bool {
    let mut line = line.trim();
    if let Some(rest) = line.strip_prefix("pub") {
        let rest = rest.trim_start();
        line = match rest.strip_prefix('(') {
            Some(scope) => match scope.find(')') {
                Some(end) => scope[end + 1..].trim_start(),
                None => rest,
            },
            None => rest,
        };
    }
    let first = line.split(|c: char| !is_word(c)).next().unwrap_or("");
    ITEMS.contains(&first)
}

/// The feature must never change what shipped code does: a build with it on and a build
/// with it off differ only in items the first one has and the second has not.
fn test_support_on_less_than_an_item(source: &Source) -> Vec<String> {
    if Layer::of(&source.path).is_none() {
        return Vec::new();
    }
    let lines = code_lines(&source.text);
    let mut found = Vec::new();
    for (at, (number, line)) in lines.iter().enumerate() {
        if !line.contains("test-support") {
            continue;
        }
        if line.contains("cfg!(") {
            found.push(broken(
                source,
                *number,
                "`cfg!` asks about `test-support`; the feature may only add whole items",
            ));
            continue;
        }
        if !line.trim().starts_with('#') || !line.contains("cfg") {
            continue;
        }
        // What the attribute is on: the next line that is not an attribute itself.
        let on = lines[at + 1..]
            .iter()
            .find(|(_, next)| !next.trim().starts_with('#'));
        let Some((_, on)) = on else {
            continue;
        };
        let listed = NOT_AN_ITEM
            .iter()
            .any(|(path, text)| *path == source.path && *text == on.trim());
        if !starts_an_item(on) && !listed {
            found.push(broken(
                source,
                *number,
                &format!(
                    "`test-support` is on `{}`, which is not a whole item (fn, impl, mod, \
                     struct, enum, const, static or use)",
                    on.trim()
                ),
            ));
        }
    }
    found
}

// ── Rule 6: the licence lines ──────────────────────────────────────────────────────

fn no_licence(source: &Source) -> Vec<String> {
    if source.text.starts_with(LICENCE) {
        Vec::new()
    } else {
        vec![broken(
            source,
            1,
            "the file does not start with the copyright line and the SPDX line",
        )]
    }
}

// ── Rule 8: a production command never names the tools ─────────────────────────────

/// `tools` as a module: the word, then `::`, or the `;`, `}` or `,` that ends it in a
/// `use`, or the `/` of a `#[path]` into its folder. A line that is all comment is not read.
fn names_the_tools(line: &str) -> bool {
    if line.trim_start().starts_with("//") {
        return false;
    }
    let mut from = 0;
    while let Some(at) = line[from..].find("tools") {
        let start = from + at;
        let end = start + "tools".len();
        from = end;
        if line[..start].chars().next_back().is_some_and(is_word) {
            continue;
        }
        let after = &line[end..];
        if after.starts_with('/') {
            return true;
        }
        let after = after.trim_start();
        if after.starts_with("::") || after.starts_with([';', '}', ',']) {
            return true;
        }
    }
    false
}

/// A production command is a file under `src/cli/` that is not the tools: not under
/// `src/cli/tools/`, and not `src/cli/tools.rs`, the tools' own root.
fn is_a_production_command(path: &str) -> bool {
    path.starts_with("src/cli/")
        && !path.starts_with("src/cli/tools/")
        && path != "src/cli/tools.rs"
}

fn command_names_the_tools(source: &Source) -> Vec<String> {
    if !is_a_production_command(&source.path) {
        return Vec::new();
    }
    let mut found = Vec::new();
    for (at, line) in source.text.lines().enumerate() {
        if names_the_tools(line) {
            found.push(broken(
                source,
                at + 1,
                "a production command names `tools`; it may use the engine, the server and \
                 the rest of the command line",
            ));
        }
    }
    found
}

// ── Every rule, over every file ────────────────────────────────────────────────────

fn check(source: &Source, dependencies: &[String], command_line: &[String]) -> Vec<String> {
    let mut found = other_layers_crates(source, dependencies);
    found.extend(server_reaches_up(source, command_line));
    found.extend(engine_reaches_out(source));
    found.extend(test_support_on_less_than_an_item(source));
    found.extend(no_licence(source));
    found.extend(command_names_the_tools(source));
    found
}

/// Every `.rs` file under `dir`, which is `below` from the root. A `target` folder is a
/// build's, and is not walked.
fn walk(dir: &Path, below: &str, found: &mut Vec<Source>) {
    let mut entries: Vec<_> = fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
        .map(|entry| entry.expect("a directory entry"))
        .collect();
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let name = entry.file_name().to_string_lossy().into_owned();
        let path = format!("{below}/{name}");
        if entry.file_type().expect("a file type").is_dir() {
            if name != "target" {
                walk(&entry.path(), &path, found);
            }
        } else if name.ends_with(".rs") {
            let text =
                fs::read_to_string(entry.path()).unwrap_or_else(|e| panic!("read {path}: {e}"));
            found.push(Source { path, text });
        }
    }
}

#[test]
fn the_sources_keep_the_rules_the_compiler_does_not_hold() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let read = |name: &str| {
        fs::read_to_string(root.join(name)).unwrap_or_else(|e| panic!("read {name}: {e}"))
    };
    let dependencies = dependencies(&read("Cargo.toml"));
    let command_line = command_line_modules(&read("src/main.rs"));
    let mut sources = Vec::new();
    for folder in ["src", "crates", "tests", "fuzz/fuzz_targets"] {
        walk(&root.join(folder), folder, &mut sources);
    }

    // A test that read nothing would pass: hold it to having read what it is about.
    assert!(dependencies.len() >= 10, "{dependencies:?}");
    assert_eq!(command_line, ["cli"], "the command line is one module");
    for (layer, least) in [
        (Layer::Engine, 60),
        (Layer::Server, 8),
        (Layer::CommandLine, 15),
    ] {
        let files = sources
            .iter()
            .filter(|source| Layer::of(&source.path) == Some(layer))
            .count();
        assert!(
            files >= least,
            "{} has {files} files where at least {least} were read before",
            layer.name()
        );
    }
    for folder in ["crates/", "tests/", "fuzz/fuzz_targets/"] {
        assert!(
            sources.iter().any(|source| source.path.starts_with(folder)),
            "no file was read under {folder}"
        );
    }

    let mut found = in_no_list(&dependencies);
    for source in &sources {
        found.extend(check(source, &dependencies, &command_line));
    }
    assert!(
        found.is_empty(),
        "{} line(s) break a layer rule:\n{}",
        found.len(),
        found.join("\n")
    );
}

// ── Each rule catches a line that breaks it ────────────────────────────────────────

#[cfg(test)]
mod planted {
    use super::*;

    fn source(path: &str, body: &str) -> Source {
        Source {
            path: path.to_string(),
            text: format!("{LICENCE}\n{body}\n"),
        }
    }

    fn deps() -> Vec<String> {
        ["axum", "clap", "serde", "tokio", "tokio-util", "zstd"]
            .map(String::from)
            .to_vec()
    }

    fn cli() -> Vec<String> {
        vec!["cli".to_string()]
    }

    /// How many lines of `body`, as the file `path`, break any rule.
    fn caught(path: &str, body: &str) -> usize {
        check(&source(path, body), &deps(), &cli()).len()
    }

    #[test]
    fn rule_1_a_dependency_that_no_layer_lists_is_caught() {
        let manifest = "[package]\nname = \"kibbr\"\n\n[dependencies]\n# why\nserde.workspace = true\nreqwest = \"0.12\"\ntokio-util = { version = \"0.7\" }\n\n[features]\nfuzzing = []\n";
        let names = dependencies(manifest);
        assert_eq!(names, ["serde", "reqwest", "tokio-util"]);
        let missing = in_no_list(&names);
        assert_eq!(missing.len(), 1, "{missing:?}");
        assert!(missing[0].contains("`reqwest`"), "{missing:?}");
    }

    #[test]
    fn rule_2_a_layer_that_names_a_crate_outside_its_list_is_caught() {
        assert_eq!(caught("src/engine/wal.rs", "use tokio::sync::Notify;"), 1);
        assert_eq!(caught("src/engine/wal.rs", "let x = axum::Json(1);"), 1);
        assert_eq!(caught("src/server/run.rs", "use clap::Parser;"), 1);
        assert_eq!(caught("src/cli/config.rs", "use axum::Router;"), 1);
        assert_eq!(caught("src/cli/config.rs", "use tokio::time::sleep;"), 1);
        // `tokio-util` is not `tokio`, and is in the command line's list.
        assert_eq!(
            caught(
                "src/cli/config.rs",
                "use tokio_util::sync::CancellationToken;"
            ),
            0
        );
        // In its own list, in a comment, or as a part of a longer name: not a use of it.
        assert_eq!(caught("src/engine/wal.rs", "use zstd::stream;"), 0);
        assert_eq!(
            caught("src/engine/wal.rs", "// tokio::spawn would stall this"),
            0
        );
        assert_eq!(caught("src/engine/wal.rs", "let n = my_tokio::count();"), 0);
        // A file outside `src/` is in no layer.
        assert_eq!(caught("tests/serve.rs", "use tokio::sync::Notify;"), 0);
    }

    #[test]
    fn rule_3_the_server_naming_the_command_line_or_internals_is_caught() {
        assert_eq!(caught("src/server/run.rs", "use crate::cli::serve;"), 1);
        assert_eq!(
            caught(
                "src/server/run.rs",
                "let e = crate::cli::error::CliError::Write;"
            ),
            1
        );
        assert_eq!(
            caught("src/server/run.rs", "use crate::{server::settings, cli};"),
            1
        );
        assert_eq!(
            caught(
                "src/server/run.rs",
                "use crate::{\n    server::settings,\n    cli::config,\n};"
            ),
            1
        );
        assert_eq!(
            caught("src/server/run.rs", "use super::super::cli::config::File;"),
            1
        );
        assert_eq!(caught("src/server.rs", "use super::cli::config::File;"), 1);
        assert_eq!(
            caught(
                "src/server/run.rs",
                "let (trie, _) = kibbr::internals::parts(db);"
            ),
            1
        );
        // Its own modules, and a longer word that starts as the command line's module does.
        assert_eq!(
            caught(
                "src/server/run.rs",
                "use crate::server::settings::Settings;"
            ),
            0
        );
        assert_eq!(
            caught("src/server/run.rs", "use crate::server::error::Error;"),
            0
        );
        assert_eq!(
            caught(
                "src/server/run.rs",
                "use crate::{server::error, server::settings};"
            ),
            0
        );
        assert_eq!(caught("src/server/run.rs", "use super::error::Error;"), 0);
        assert_eq!(
            caught("src/server/run.rs", "let n = crate::clients::count();"),
            0
        );
        // The command line may name all of these.
        assert_eq!(
            caught("src/cli/serve.rs", "use crate::cli::config::File;"),
            0
        );
        assert_eq!(
            caught("src/cli/tools/demo.rs", "use kibbr::internals::parts;"),
            0
        );
    }

    #[test]
    fn rule_4_the_engine_naming_kibbr_a_socket_or_a_third_feature_is_caught() {
        assert_eq!(
            caught(
                "src/engine/wal.rs",
                "let db = kibbr::Database::in_memory(o);"
            ),
            1
        );
        assert_eq!(caught("src/engine/wal.rs", "use std::net::TcpStream;"), 1);
        assert_eq!(
            caught(
                "src/engine/wal.rs",
                "let l = std::net::TcpListener::bind(a)?;"
            ),
            1
        );
        assert_eq!(
            caught(
                "src/engine/wal.rs",
                "let s = std::net::UdpSocket::bind(a)?;"
            ),
            1
        );
        assert_eq!(
            caught(
                "src/engine/wal.rs",
                "#[cfg(feature = \"bench\")]\nfn learn() {}"
            ),
            1
        );
        assert_eq!(
            caught(
                "src/engine/wal.rs",
                "#[cfg(any(feature = \"fuzzing\", feature = \"slow\"))]\nfn x() {}"
            ),
            1
        );
        // An address is a value, and the two features are the engine's own.
        assert_eq!(caught("src/engine/wal.rs", "use std::net::IpAddr;"), 0);
        assert_eq!(
            caught(
                "src/engine/wal.rs",
                "#[cfg(feature = \"fuzzing\")]\npub mod trieql;"
            ),
            0
        );
        assert_eq!(
            caught(
                "src/engine/wal.rs",
                "let db = crate::Database::in_memory(o);"
            ),
            0
        );
        // The binary writes `kibbr::`, and opens sockets.
        assert_eq!(
            caught(
                "src/cli/http.rs",
                "use std::net::TcpStream;\nuse kibbr::Database;"
            ),
            0
        );
    }

    #[test]
    fn rule_5_test_support_on_less_than_a_whole_item_is_caught() {
        let on = |what: &str| format!("#[cfg(any(test, feature = \"test-support\"))]\n{what}");
        assert_eq!(caught("src/engine/wal.rs", &on("let limit = 1;")), 1);
        assert_eq!(caught("src/engine/wal.rs", &on("self.count += 1;")), 1);
        assert_eq!(caught("src/engine/wal.rs", &on("limit: usize,")), 1);
        assert_eq!(
            caught(
                "src/engine/wal.rs",
                "let n = if cfg!(feature = \"test-support\") { 1 } else { 2 };"
            ),
            1
        );
        // The one listed exception is an exception in its own file only.
        let entered = on("    entered: Mutex<Vec<std::thread::ThreadId>>,");
        assert_eq!(caught("src/engine/rows/scan_threads.rs", &entered), 0);
        assert_eq!(caught("src/engine/wal.rs", &entered), 1);
        // Whole items, with `pub` or without, and under a second attribute.
        for item in [
            "fn counted() {}",
            "pub fn counted() {}",
            "pub(crate) mod samples;",
            "impl Database {}",
            "pub struct Hook;",
            "enum Kind { A }",
            "const N: usize = 1;",
            "static N: usize = 1;",
            "use crate::wal::Wal;",
            "#[doc(hidden)]\npub mod testing {}",
        ] {
            assert_eq!(caught("src/engine/wal.rs", &on(item)), 0, "{item}");
        }
        // The binary is held to it too: the feature is the package's.
        assert_eq!(caught("src/server/run.rs", &on("let limit = 1;")), 1);
    }

    #[test]
    fn rule_6_a_file_without_the_licence_lines_is_caught() {
        let without = |path: &str, text: &str| {
            let source = Source {
                path: path.to_string(),
                text: text.to_string(),
            };
            check(&source, &deps(), &cli()).len()
        };
        assert_eq!(without("src/engine/wal.rs", "fn x() {}\n"), 1);
        assert_eq!(without("tests/serve.rs", "//! A test.\n"), 1);
        assert_eq!(without("fuzz/fuzz_targets/cursor.rs", "#![no_main]\n"), 1);
        // The second line alone, or the two in the other order, is not the header.
        assert_eq!(
            without(
                "crates/kibbr-devtools/src/main.rs",
                "// SPDX-License-Identifier: AGPL-3.0-only\n"
            ),
            1
        );
        assert_eq!(
            without("src/engine/wal.rs", &format!("{LICENCE}\nfn x() {{}}\n")),
            0
        );
    }

    #[test]
    fn rule_8_a_production_command_that_names_the_tools_is_caught() {
        assert_eq!(
            caught("src/cli/load.rs", "use crate::cli::tools::gen_logs;"),
            1
        );
        assert_eq!(
            caught("src/cli/load.rs", "use crate::cli::{config, tools};"),
            1
        );
        assert_eq!(caught("src/cli/load.rs", "use super::{tools, config};"), 1);
        assert_eq!(
            caught("src/cli/load.rs", "let p = super::tools::parse::cli;"),
            1
        );
        assert_eq!(
            caught("src/cli/query/oracle.rs", "use super::super::tools::demo;"),
            1
        );
        assert_eq!(
            caught("src/cli/load.rs", "#[path = \"tools/demo.rs\"]\nmod demo;"),
            1
        );
        // A comment, and a longer word.
        assert_eq!(
            caught("src/cli/load.rs", "// the tools; never named here"),
            0
        );
        assert_eq!(
            caught(
                "src/cli/load.rs",
                "let tools_dir = 1;\nlet n = subtools::count();"
            ),
            0
        );
        // The tools themselves, and the file that dispatches to every command.
        assert_eq!(
            caught("src/cli/tools/demo.rs", "use crate::cli::tools::gen_logs;"),
            0
        );
        assert_eq!(caught("src/cli/tools.rs", "pub use self::tools::x;"), 0);
        assert_eq!(
            caught("src/cli.rs", "use tools::{bench_storage, demo, parse};"),
            0
        );
    }
}
