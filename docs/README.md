# kibbr's docs

Each page starts with the commands, then gives the facts.

## Using kibbr

| Page | What it covers |
|---|---|
| [serve.md](serve.md) | The server: `/ingest`, `/query`, TrieQL, stopping, and asking a running server (`kibbr stats`, `templates`, `health`) |
| [load.md](load.md) | `kibbr load`: a closed day of logs loaded whole |
| [compact.md](compact.md) | `kibbr compact`: a finished day made smaller |
| [settings.md](settings.md) | Every setting: flag, `KIBBR__` variable, file, default; and kibbr's own log |
| [viewer.md](viewer.md) | The web console at `/`, and how it is built |
| [generate-and-parse.md](generate-and-parse.md) | `kibbr gen-logs` and `kibbr parse`, and things to try with the type ladder |
| [benchmarks.md](benchmarks.md) | What was measured, how, and where kibbr loses |
| [../deploy/README.md](../deploy/README.md) | The container image, and the Kubernetes manifests |

## Working on kibbr

| Page | What it covers |
|---|---|
| [../ARCHITECTURE.md](../ARCHITECTURE.md) | The map of the code: the layers, where each thing is, the rules and what holds them |
| [glossary.md](glossary.md) | kibbr's own words (template, leaf, door, shed, backstop, ...) and where each is in the code |
| [../CONTRIBUTING.md](../CONTRIBUTING.md) | Build, test, where a change goes, and how a release is made |
| [../scripts/README.md](../scripts/README.md) | The check, fault and measure scripts, and what each prints when it passes |
| [../scripts/bench/README.md](../scripts/bench/README.md) | `kibbr-bench`, and the compression measurements |
| [../tests/data/README.md](../tests/data/README.md) | The data the tests read |
