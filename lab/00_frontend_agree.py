#!/usr/bin/env python3
"""Does the Python frontend agree with the binary? Nothing else is trusted until it does.

5,000 lines through `triedb parse --json` and through `common/frontend.py`, comparing
template id, fingerprint, the key list, and the inferred type of every key. Zero
disagreements is the pass condition. A non-zero count is itself a finding and is printed
case by case, because every later script reads types and template ids out of the port.
"""

from __future__ import annotations

import json
import subprocess
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lab.common import datasets, measure
from lab.common.frontend import parse_line

LIMIT = 5_000


def oracle(path: Path, payloads: list) -> list:
    """`parse --json` over the payload lines, fed on stdin so nothing is written to disk."""
    body = b"\n".join(payloads) + b"\n"
    out = subprocess.run(
        [str(datasets.BINARY), "parse", "-", "--json"],
        input=body,
        capture_output=True,
        check=True,
    ).stdout
    return [json.loads(line) for line in out.splitlines() if line.strip()]


def compare(name: str, payloads: list) -> tuple:
    want = oracle(name, payloads)
    disagreements = []
    checked = Counter()

    for i, (raw, exp) in enumerate(zip(payloads, want)):
        got = parse_line(raw)
        checked["lines"] += 1

        if got.template_id != exp["template_id"]:
            disagreements.append((i, "template_id", exp["template_id"], got.template_id))
        if got.fingerprint != exp["fingerprint"]:
            disagreements.append((i, "fingerprint", exp["fingerprint"], got.fingerprint))
        if got.keys != exp["keys"]:
            disagreements.append((i, "keys", exp["keys"], got.keys))

        exp_types = exp["types"]
        got_types = got.types
        checked["keys"] += len(exp_types)
        for key in sorted(set(exp_types) | set(got_types)):
            a, b = exp_types.get(key, "<absent>"), got_types.get(key, "<absent>")
            if a != b:
                disagreements.append((i, f"type[{key}]", a, b))
        # `values` is the raw text of every key, promoted ones included - this is where a
        # float rendered differently by ryu and by Python's repr would show up.
        for key in sorted(set(exp["values"]) | set(got.values)):
            a, b = exp["values"].get(key, "<absent>"), got.values.get(key, "<absent>")
            if a != b:
                disagreements.append((i, f"value[{key}]", a, b))
            checked["values"] += 1

    return checked, disagreements


def main() -> int:
    ap = measure.parse_args("00_frontend_agree")
    ap.add_argument("--limit", type=int, default=LIMIT)
    args = ap.parse_args()

    res = measure.Results("00_frontend_agree", "all")
    measure.print_machine()

    total_bad = 0
    rows = []
    for name in datasets.available(args.size):
        ds = datasets.ensure(name, args.size)
        payloads = []
        for _, p in datasets.iter_payloads(ds.path):
            payloads.append(p)
            if len(payloads) >= args.limit:
                break
        checked, bad = compare(name, payloads)
        total_bad += len(bad)
        rows.append(
            [
                name,
                checked["lines"],
                checked["keys"],
                checked["values"],
                len(bad),
            ]
        )
        res.add(name, "lines_compared", checked["lines"], "lines", dataset=name)
        res.add(name, "disagreements", len(bad), "count", ds.note, dataset=name)

        if bad:
            print(f"\n!!! {name}: {len(bad)} disagreements, first 25:")
            for line_no, what, want, got in bad[:25]:
                print(f"  line {line_no:>5}  {what}")
                print(f"      binary: {want!r}")
                print(f"      python: {got!r}")

    measure.table(
        rows,
        headers=["dataset", "lines", "types checked", "values checked", "disagreements"],
        title="binary `parse --json` vs common/frontend.py",
    )
    res.flush()

    if total_bad:
        print(
            f"\nFAIL: {total_bad} disagreements. That list is the finding - no later "
            "result is trusted until it is zero."
        )
        return 1
    print("\nPASS: zero disagreements. The Python port may stand in for the binary.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
