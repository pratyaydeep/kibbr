# kibbr's docs

- [serve.md](serve.md): `kibbr serve`, the HTTP API, TrieQL, stopping, and asking a running server (`templates`, `stats`, `health`).
- [load.md](load.md): `kibbr load`, the bulk door for closed days.
- [compact.md](compact.md): `kibbr compact`, rebuilding a finished day into full row groups.
- [settings.md](settings.md): every setting, as a flag, a `KIBBR__` variable or a line of the `--config` file.
- [viewer.md](viewer.md): the web console served at `/`.
- [generate-and-parse.md](generate-and-parse.md): `gen-logs` and `parse`, and things to try with the type ladder.
- [benchmarks.md](benchmarks.md): the storage results in full, and how to reproduce them.
- [glossary.md](glossary.md): kibbr's own words (template, leaf, door, shed, backstop) and their names in the code.

Elsewhere: running it in a container or on Kubernetes is [deploy/README.md](../deploy/README.md); the
code's map is [ARCHITECTURE.md](../ARCHITECTURE.md).
