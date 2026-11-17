# kibbr-storage is discontinued

This crate was the trie, catalog, day files and write-ahead log of
[kibbr](https://crates.io/crates/kibbr), a database for structured JSON logs. At 0.3.0 it
became part of `kibbr-engine`, which is itself part of the `kibbr` crate from kibbr 0.5.0,
and this crate gets no more releases.

- To run kibbr: `cargo install --locked kibbr`
- Source and issues: https://github.com/pratyaydeep/kibbr
- This crate never promised a stable API. Earlier versions stay available for the kibbr
  releases that depend on them.
