#!/usr/bin/env python3
# What kibbr's storage makes of real log corpora, one source at a time, with each result
# kept in one JSON file. Each source is downloaded, read out of its archive and run through
# `kibbr bench-storage`, which prints the raw bytes, `gzip -9`, `zstd -19`, kibbr's bulk load,
# and the same lines as a live day before and after compaction. This script reads that table
# back into numbers.
#
# **The input** is a JSON file of sources, each with a `tag` that names it:
#
#   {"sources": [
#     {"tag": "postgresql",
#      "url": "https://zenodo.org/records/10516402/files/postgresql.tar.gz?download=1",
#      "files": "*.log",               optional: a glob over the archive's paths    [*]
#      "time_columns": ["timestamp"],  optional: as `bench-storage --time-columns`
#      "checksum": "md5:...",          optional: md5:<hex> or sha256:<hex>, as Zenodo gives it;
#                                      the download must have this hash
#      "sha256": "...",                optional: the same, for a sha256
#      "zstd_threads": 1,              optional: as `bench-storage --zstd-threads`; this
#                                      source's own wins over --zstd-threads
#      "note": "..."}                  optional: copied into the result
#   ]}
#
# `path` can stand in for `url`, for a file already on this machine. A download is a
# `.tar.gz`, `.tgz`, `.tar` or `.zip`, or one NDJSON file, plain or gzip or zstd. The files of
# an archive that `files` matches are measured as one NDJSON file, a gzip member decoded and
# a missing last newline added. A tar is read straight into that file, in the archive's own
# order, and never unpacked: the disk holds one copy of the logs. A zip is unpacked, and its
# files joined in path order.
#
# **The output** holds one result a tag, under `results`. A tag already in it is not run
# again: the script says so and goes on to the next. Delete the entry to measure it again.
# The file is rewritten after each source, so a run that stops keeps what it measured.
# Each result is also written as <tag>.md, in a directory named for the output without its
# `.json`: bench/results/compression/postgresql.md beside bench/results/compression.json.
#
# Downloads, the logs read out of them and each run's raw output go under testdata/compression/<tag>/,
# which is git-ignored scratch. A download that is there already is used, not fetched again.
#
#   bench/compression.py                                   # every source in bench/compression.json
#   bench/compression.py --tag postgresql                  # that one only
#   bench/compression.py --input mine.json --output out.json --no-build
#
#   --input FILE    the sources                                 [bench/compression.json]
#   --output FILE   the results                                 [bench/results/compression.json]
#   --tag TAG       measure only this source; given more than once, these sources
#   --bin PATH      the kibbr to run; nothing is built          [target/release/kibbr, built]
#   --no-build      use target/release/kibbr as it is
#   --md-dir DIR    where each tag's .md goes                   [the output without .json]
#   --no-zstd       leave zstd -19 out, as `bench-storage --no-zstd`: at about 3 MB/s on one
#                   thread it is most of the run on a big corpus (7 hours of 69.6 GB of MongoDB)
#   --zstd-threads N  zstd -19's threads, as `bench-storage --zstd-threads`: 1 is the plain,
#                   smallest zstd -19; more are a little larger and close to N times faster.
#                   A source's own "zstd_threads" wins. [bench-storage's: every CPU]

import argparse
import datetime
import fnmatch
import gzip
import hashlib
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tarfile
import time
import urllib.parse
import urllib.request
import zipfile

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
WORK = os.path.join(ROOT, "testdata", "compression")
TAG = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"
GZIP_MAGIC = b"\x1f\x8b"


def shown(path):
    """A path under the repository from its root, any other as it is."""
    return os.path.relpath(path, ROOT) if path.startswith(ROOT + os.sep) else path


def say(msg):
    print(f"compression: {msg}", flush=True)


def fail(msg):
    sys.exit(f"compression: {msg}")


def load_sources(path):
    with open(path) as f:
        doc = json.load(f)
    sources = doc["sources"] if isinstance(doc, dict) else doc
    seen = set()
    for s in sources:
        tag = s.get("tag")
        if not isinstance(tag, str) or not TAG.match(tag):
            fail(f"{path}: a source's tag must be letters, digits, `.`, `_` or `-`: {tag!r}")
        if tag in seen:
            fail(f"{path}: tag {tag} is given twice")
        seen.add(tag)
        if ("url" in s) == ("path" in s):
            fail(f"{path}: source {tag} needs one of `url` and `path`")
        t = s.get("zstd_threads")
        if t is not None and (not isinstance(t, int) or isinstance(t, bool) or t < 1):
            fail(f"{path}: source {tag}'s zstd_threads must be a whole number, 1 or more: {t!r}")
    return sources


def load_results(path):
    if not os.path.exists(path):
        return {"results": {}}
    with open(path) as f:
        doc = json.load(f)
    doc.setdefault("results", {})
    return doc


def save_results(path, doc):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    part = path + ".part"
    with open(part, "w") as f:
        json.dump(doc, f, indent=2)
        f.write("\n")
    os.replace(part, path)


def hashes_of(path):
    """The file's sha256 and md5, in one read: Zenodo gives the md5."""
    sha, md5 = hashlib.sha256(), hashlib.md5()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            sha.update(block)
            md5.update(block)
    return {"sha256": sha.hexdigest(), "md5": md5.hexdigest()}


def wanted_hashes(source):
    """What the source says the download must hash to, by algorithm."""
    want = {}
    if source.get("sha256"):
        want["sha256"] = source["sha256"].lower()
    if source.get("checksum"):
        algo, _, digest = source["checksum"].partition(":")
        if algo.lower() not in ("md5", "sha256") or not digest:
            fail(f"source {source['tag']}: checksum must be md5:<hex> or sha256:<hex>, not {source['checksum']!r}")
        want[algo.lower()] = digest.lower()
    return want


def download(url, into):
    """The file at `url`, under `into`: fetched once, and kept for the next run."""
    name = os.path.basename(urllib.parse.urlparse(url).path) or "download"
    path = os.path.join(into, name)
    if os.path.exists(path):
        say(f"using {shown(path)}, downloaded before")
        return path
    os.makedirs(into, exist_ok=True)
    part = path + ".part"
    say(f"downloading {url}")
    req = urllib.request.Request(url, headers={"User-Agent": "kibbr-bench-compression"})
    with urllib.request.urlopen(req) as r, open(part, "wb") as f:
        total = int(r.headers.get("Content-Length") or 0)
        got, told = 0, time.monotonic()
        for block in iter(lambda: r.read(1 << 20), b""):
            f.write(block)
            got += len(block)
            if time.monotonic() - told > 5:
                of = f" of {total:,}" if total else ""
                say(f"  {got:,}{of} bytes")
                told = time.monotonic()
    os.replace(part, path)
    say(f"downloaded {os.path.getsize(path):,} bytes")
    return path


def is_archive(path):
    lower = path.lower()
    return lower.endswith((".tar.gz", ".tgz", ".tar", ".tar.bz2", ".tar.xz", ".zip"))


def unpack(archive, into):
    """Unpack once: a marker says a whole unpack is there, and a partial one is redone."""
    marker = os.path.join(into, ".unpacked")
    if os.path.exists(marker):
        return
    if os.path.exists(into):
        fail(f"{shown(into)} holds a partial unpack; remove it and run again")
    os.makedirs(into)
    say(f"unpacking {os.path.basename(archive)}")
    if archive.lower().endswith(".zip"):
        with zipfile.ZipFile(archive) as z:
            for name in z.namelist():
                target = os.path.realpath(os.path.join(into, name))
                if not target.startswith(os.path.realpath(into) + os.sep):
                    fail(f"{archive}: {name} would land outside the unpack directory")
            z.extractall(into)
    else:
        with tarfile.open(archive) as t:
            # The `data` filter refuses links and paths that leave the directory.
            t.extractall(into, filter="data")
    open(marker, "w").close()


def matches(rel, pattern):
    """Whether an archive's file is one the source asks for: by its path or its name."""
    name = os.path.basename(rel)
    if name.startswith("."):
        return False
    return fnmatch.fnmatch(rel, pattern) or fnmatch.fnmatch(name, pattern)


def picked(root, pattern):
    """The unpacked files the source asks for, by path under `root`, in order."""
    out = []
    for d, dirs, files in os.walk(root):
        dirs.sort()
        for name in sorted(files):
            rel = os.path.relpath(os.path.join(d, name), root)
            if matches(rel, pattern):
                out.append(os.path.join(d, name))
    return out


def streamed(archive, pattern, to):
    """A tar's matching files as one NDJSON file, read straight out of the archive in its own
    order, a gzip member decoded. Nothing is unpacked, so the disk holds one copy of the logs,
    not two: a 70 GB corpus would not fit twice. The names go beside it, as `.files`."""
    names_at = to + ".files"
    if os.path.exists(to) and os.path.exists(names_at):
        with open(names_at) as f:
            return json.load(f)
    say(f"reading {os.path.basename(archive)} into {shown(to)}")
    os.makedirs(os.path.dirname(to), exist_ok=True)
    part = to + ".part"
    names, written, told = [], 0, time.monotonic()
    with tarfile.open(archive, "r|*") as t, open(part, "wb") as out:
        for member in t:
            if not member.isfile() or not matches(member.name, pattern):
                continue
            src = t.extractfile(member)
            head = src.peek(4)[:4]
            if head.startswith(ZSTD_MAGIC):
                fail(f"{archive}: {member.name} is zstd inside a tar; that is not read here")
            if head.startswith(GZIP_MAGIC):
                src = gzip.GzipFile(fileobj=src)
            last = b"\n"
            for block in iter(lambda: src.read(1 << 20), b""):
                out.write(block)
                written += len(block)
                last = block[-1:]
            if last != b"\n":
                out.write(b"\n")
            names.append(member.name)
            if time.monotonic() - told > 30:
                say(f"  {len(names)} files, {written:,} bytes")
                told = time.monotonic()
    if not names:
        os.remove(part)
        return names
    os.replace(part, to)
    with open(names_at, "w") as f:
        json.dump(names, f)
    say(f"read {len(names)} files, {written:,} bytes")
    return names


def open_decoded(path):
    with open(path, "rb") as f:
        head = f.read(4)
    if head.startswith(GZIP_MAGIC):
        return gzip.open(path, "rb"), None
    if head.startswith(ZSTD_MAGIC):
        p = subprocess.Popen(["zstd", "-dc", path], stdout=subprocess.PIPE)
        return p.stdout, p
    return open(path, "rb"), None


def joined(files, to):
    """Several files as one NDJSON file, each ending in a newline."""
    if os.path.exists(to):
        return to
    say(f"joining {len(files)} files into {shown(to)}")
    part = to + ".part"
    with open(part, "wb") as out:
        for path in files:
            src, proc = open_decoded(path)
            last = b"\n"
            with src:
                for block in iter(lambda: src.read(1 << 20), b""):
                    out.write(block)
                    last = block[-1:]
            if proc is not None and proc.wait() != 0:
                fail(f"zstd could not decode {path}")
            if last != b"\n":
                out.write(b"\n")
    os.replace(part, to)
    return to


def input_file(source, work):
    """The one file bench-storage is given for this source, and where it came from."""
    if "path" in source:
        got = os.path.abspath(os.path.expanduser(source["path"]))
        if not os.path.isfile(got):
            fail(f"source {source['tag']}: {got} is not a file")
    else:
        got = download(source["url"], os.path.join(work, "download"))
    fetched = {"file": os.path.basename(got), "bytes": os.path.getsize(got), **hashes_of(got)}
    for algo, digest in wanted_hashes(source).items():
        if digest != fetched[algo]:
            fail(f"source {source['tag']}: {algo} of {shown(got)} is {fetched[algo]}, not {digest}")
        say(f"{source['tag']}: {algo} matches")
    if not is_archive(got):
        return got, fetched, [fetched["file"]]
    pattern = source.get("files", "*")
    nothing = f"source {source['tag']}: no file in the archive matches {pattern!r}"
    if not got.lower().endswith(".zip"):
        # Named for the archive and the pattern, so a source whose `files` changed is read again.
        key = hashlib.sha256(f"{fetched['sha256']}\n{pattern}".encode()).hexdigest()[:12]
        to = os.path.join(work, f"logs-{key}.ndjson")
        names = streamed(got, pattern, to)
        if not names:
            fail(nothing)
        return to, fetched, names
    unpacked = os.path.join(work, "unpacked")
    unpack(got, unpacked)
    files = picked(unpacked, pattern)
    if not files:
        fail(nothing)
    names = [os.path.relpath(f, unpacked) for f in files]
    if len(files) == 1:
        return files[0], fetched, names
    # Named for the files in it, so a source whose `files` changed is joined again.
    key = hashlib.sha256("\n".join(names).encode()).hexdigest()[:12]
    return joined(files, os.path.join(work, f"joined-{key}.ndjson")), fetched, names


# ── Reading bench-storage's table back ──

NUM = r"[\d,]+"
ROW = re.compile(rf"^(?P<indent> +)(?P<name>\S.*?) {{2,}}(?P<bytes>{NUM})(?: +(?P<blocks>{NUM}))? +(?P<ratio>[\d.]+|-)$")
TIMES = re.compile(
    r"^  gzip -9 ran on 1 thread in (?P<gz>[\d.]+)s(?:; zstd -19 on (?P<n>\d+) thread\(s\) in (?P<zst>[\d.]+)s)?$")
BULK = re.compile(
    rf"^  (?P<lines>{NUM}) lines, (?P<rows>{NUM}) rows as load \S+: (?P<groups>{NUM}) row groups "
    rf"in (?P<files>{NUM}) file\(s\) over (?P<days>{NUM}) day\(s\), loaded in (?P<s>[\d.]+)s$")
BEFORE = re.compile(
    rf"^  before compaction: (?P<lines>{NUM}) lines, (?P<rows>{NUM}) rows, (?P<groups>{NUM}) row groups "
    rf"in (?P<files>{NUM}) file\(s\) over (?P<days>{NUM}) day\(s\), ingested in (?P<s>[\d.]+)s$")
AFTER = re.compile(
    rf"^  after compaction: (?P<rows>{NUM}) rows, (?P<groups>{NUM}) row groups "
    rf"in (?P<files>{NUM}) file\(s\) over (?P<days>{NUM}) day\(s\), compacted in (?P<s>[\d.]+)s$")


def n(text):
    return int(text.replace(",", ""))


def slug(name):
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def counts(m):
    d = m.groupdict()
    out = {k: n(d[k]) for k in ("lines", "rows", "groups", "files", "days") if d.get(k) is not None}
    out["row_groups"] = out.pop("groups")
    out["seconds"] = float(d["s"])
    return out


def parse_report(text):
    """bench-storage's stdout as numbers: each table row by name, nested by its indent."""
    tables = {}
    stack = []  # (indent, dict) of the rows the next one may sit under
    loads = {}
    for line in text.splitlines():
        for key, pattern in (("bulk", BULK), ("live_before", BEFORE), ("live_after", AFTER)):
            m = pattern.match(line)
            if m:
                loads[key] = counts(m)
        m = ROW.match(line)
        if not m:
            continue
        indent = len(m["indent"])
        row = {"bytes": n(m["bytes"])}
        if m["blocks"]:
            row["disk_blocks"] = n(m["blocks"])
        if m["ratio"] != "-":
            row["x_raw"] = float(m["ratio"])
        while stack and stack[-1][0] >= indent:
            stack.pop()
        if stack:
            stack[-1][1][slug(m["name"])] = row
        else:
            tables[slug(m["name"])] = row
        stack.append((indent, row))
    times = next((m for m in map(TIMES.match, text.splitlines()) if m), None)
    gz_ratio = re.search(r"kibbr total is ([\d.]+) of gzip -9", text)
    zst_ratio = re.search(r"and ([\d.]+) of zstd -19", text)
    # zstd -19 is absent from a run under --no-zstd.
    need = ("raw", "gzip_9", "kibbr_total")
    if not all(k in tables for k in need) or "bulk" not in loads:
        raise ValueError("bench-storage's table did not read back: has its format changed?")
    raw = tables["raw"]["bytes"]
    zst = tables.get("zstd_19")
    block = re.search(r"disk blocks are ([\d,]+) bytes", text)
    block = n(block[1]) if block else None
    # What each takes on this disk: its whole blocks, in bytes.
    disk = lambda row: row["disk_blocks"] * block if block and row and "disk_blocks" in row else None
    summary = {
        "ingested_bytes": raw,
        "ingested_disk_bytes": disk(tables["raw"]),
        "kibbr_disk_bytes": disk(tables["kibbr_total"]),
        "gzip_9_disk_bytes": disk(tables["gzip_9"]),
        "zstd_19_disk_bytes": disk(zst),
        "disk_block_bytes": block,
        "raw_bytes": raw,
        "gzip_9_bytes": tables["gzip_9"]["bytes"],
        "zstd_19_bytes": zst["bytes"] if zst else None,
        "kibbr_bytes": tables["kibbr_total"]["bytes"],
        "gzip_9_x_raw": tables["gzip_9"].get("x_raw"),
        "zstd_19_x_raw": zst.get("x_raw") if zst else None,
        "kibbr_x_raw": tables["kibbr_total"].get("x_raw"),
        "kibbr_x_gzip_9": float(gz_ratio[1]) if gz_ratio else None,
        "kibbr_x_zstd_19": float(zst_ratio[1]) if zst_ratio else None,
        # A bench-storage from before --zstd-threads printed no timings, and ran zstd on one.
        "zstd_19_threads": (int(times["n"]) if times and times["n"] else 1) if zst else None,
        "gzip_9_seconds": float(times["gz"]) if times else None,
        "zstd_19_seconds": float(times["zst"]) if times and times["zst"] else None,
    }
    after = tables.get("kibbr_total_after")
    if after:
        summary["kibbr_compacted_bytes"] = after["bytes"]
        summary["kibbr_compacted_x_raw"] = after.get("x_raw")
        summary["kibbr_compacted_x_gzip_9"] = round(after["bytes"] / summary["gzip_9_bytes"], 3)
        summary["kibbr_compacted_x_zstd_19"] = (
            round(after["bytes"] / summary["zstd_19_bytes"], 3) if summary["zstd_19_bytes"] else None)
        summary["kibbr_compacted_disk_bytes"] = disk(after)
    if summary["ingested_disk_bytes"] and summary["kibbr_disk_bytes"]:
        summary["kibbr_disk_saved_share"] = round(1 - summary["kibbr_disk_bytes"] / summary["ingested_disk_bytes"], 4)
    return {"summary": summary, "loads": loads, "tables": tables}


# ── What it was measured with ──

def git(*args):
    r = subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True)
    return r.stdout.strip() if r.returncode == 0 else None


def machine():
    m = {"os": platform.system(), "os_version": platform.release(), "arch": platform.machine()}
    if platform.system() == "Darwin":
        for key, name in (("cpu", "machdep.cpu.brand_string"), ("cores", "hw.ncpu"), ("memory_bytes", "hw.memsize")):
            r = subprocess.run(["sysctl", "-n", name], capture_output=True, text=True)
            if r.returncode == 0:
                m[key] = int(r.stdout) if key != "cpu" else r.stdout.strip()
        r = subprocess.run(["sysctl", "-n", "hw.physicalcpu"], capture_output=True, text=True)
        if r.returncode == 0:
            m["physical_cores"] = int(r.stdout)
    else:
        m["cores"] = os.cpu_count()
        try:
            # Physical cores: the unique (physical id, core id) pairs, one per core's threads.
            cores, phys = set(), None
            with open("/proc/cpuinfo") as f:
                for line in f:
                    key, _, value = line.partition(":")
                    key, value = key.strip(), value.strip()
                    if key == "model name" and "cpu" not in m:
                        m["cpu"] = value
                    elif key == "physical id":
                        phys = value
                    elif key == "core id":
                        cores.add((phys, value))
            if cores:
                m["physical_cores"] = len(cores)
            with open("/proc/meminfo") as f:
                for line in f:
                    if line.startswith("MemTotal:"):
                        m["memory_bytes"] = int(line.split()[1]) * 1024
                        break
        except OSError:
            pass
    # What the box is, as whoever started it says: bench/cloud/ sets it to the instance type.
    if os.environ.get("KIBBR_MACHINE"):
        m["host"] = os.environ["KIBBR_MACHINE"]
    return m


def measure(source, bin_path, no_zstd=False, zstd_threads=None):
    tag = source["tag"]
    work = os.path.join(WORK, tag)
    began = time.monotonic()
    path, fetched, members = input_file(source, work)
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    # bench-storage wants a directory of its own, and removes it when it is done.
    scratch = os.path.join(work, f"scratch-{stamp}")
    cmd = [bin_path, "bench-storage", path, "--scratch", scratch]
    if source.get("time_columns"):
        cmd += ["--time-columns", ",".join(source["time_columns"])]
    if no_zstd:
        cmd.append("--no-zstd")
    threads = source.get("zstd_threads", zstd_threads)
    if threads is not None and not no_zstd:
        cmd += ["--zstd-threads", str(threads)]
    say(f"{tag}: {' '.join(shown(c) for c in cmd)}")
    os.makedirs(work, exist_ok=True)
    log =os.path.join(work, f"bench-storage-{stamp}.log")
    with open(log, "w") as err:
        r = subprocess.run(cmd, cwd=ROOT, stdout=subprocess.PIPE, stderr=err, text=True)
    with open(os.path.join(work, f"bench-storage-{stamp}.txt"), "w") as f:
        f.write(r.stdout)
    print(r.stdout, end="", flush=True)
    if r.returncode != 0:
        tail = open(log).read().splitlines()[-5:]
        say(f"{tag}: bench-storage exited {r.returncode}; nothing written. Last of its log:")
        for line in tail:
            print(f"  {line}")
        return None
    with open(log) as f:
        warnings = sum(1 for line in f if " WARN " in line)
    result = parse_report(r.stdout)
    result.update({
        "tag": tag,
        "source": {k: source[k] for k in ("url", "path", "files", "time_columns", "zstd_threads", "checksum", "note") if k in source},
        "download": fetched,
        "measured_files": members,
        "measured_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "seconds": round(time.monotonic() - began, 1),
        "kibbr": {
            "version": subprocess.run([bin_path, "--version"], capture_output=True, text=True).stdout.strip(),
            "commit": git("rev-parse", "HEAD"),
            "dirty": git("status", "--porcelain", "--", "src", "crates", "Cargo.toml", "Cargo.lock") not in ("", None),
            "argv": cmd[1:2] + [os.path.basename(path)] + cmd[3:4] + ["<scratch>"] + cmd[5:],
        },
        "machine": machine(),
        "warnings": warnings,
        "report": r.stdout.splitlines(),
    })
    return result


def write_markdown(result, into):
    """The result as <tag>.md: the headline numbers, then bench-storage's own table."""
    os.makedirs(into, exist_ok=True)
    path = os.path.join(into, f"{result['tag']}.md")
    sm, src, k, m = result["summary"], result["source"], result["kibbr"], result["machine"]
    num = lambda v: "-" if v is None else f"{v:,}"
    get = sm.get
    rows = [
        ("ingested, as a plain file", sm["raw_bytes"], get("ingested_disk_bytes"), 1.0, None, None),
        ("gzip -9", sm["gzip_9_bytes"], get("gzip_9_disk_bytes"), sm["gzip_9_x_raw"], None, None),
        *([(f"zstd -19, {get('zstd_19_threads') or 1} thread(s)", sm["zstd_19_bytes"], get("zstd_19_disk_bytes"), sm["zstd_19_x_raw"], None, None)]
          if sm.get("zstd_19_bytes") is not None else []),
        ("kibbr, bulk load", sm["kibbr_bytes"], get("kibbr_disk_bytes"), sm["kibbr_x_raw"],
         sm["kibbr_x_gzip_9"], sm["kibbr_x_zstd_19"]),
    ]
    if "kibbr_compacted_bytes" in sm:
        rows.append(("kibbr, live day compacted", sm["kibbr_compacted_bytes"], get("kibbr_compacted_disk_bytes"),
                     sm["kibbr_compacted_x_raw"], sm["kibbr_compacted_x_gzip_9"], sm["kibbr_compacted_x_zstd_19"]))
    headline = []
    if get("ingested_disk_bytes") and get("kibbr_disk_bytes"):
        headline = [
            f"**{num(sm['raw_bytes'])} bytes ingested.** As a plain file that takes "
            f"{num(sm['ingested_disk_bytes'])} bytes on this disk; kibbr keeps it in "
            f"{num(sm['kibbr_disk_bytes'])}, {100 * sm['kibbr_disk_saved_share']:.1f}% less.",
            "",
        ]
    bulk = result["loads"]["bulk"]
    members = result["measured_files"]
    listed = ", ".join(f"`{f}`" for f in members[:10])
    if len(members) > 10:
        listed += f" and {len(members) - 10:,} more ({len(members):,} files, joined in this order)"
    elif len(members) > 1:
        listed += f" ({len(members)} files, joined in this order)"
    lines = [
        f"# kibbr storage on {result['tag']}, {result['measured_at'][:10]}",
        "",
        *([src["note"], ""] if src.get("note") else []),
        f"- **Source:** {src.get('url') or src.get('path')}"
        + (f", files matching `{src['files']}`" if src.get("files") else ""),
        f"- **Download:** `{result['download']['file']}`, {num(result['download']['bytes'])} bytes, "
        f"sha256 `{result['download']['sha256']}`",
        f"- **Measured:** {listed}: "
        f"{num(bulk['lines'])} lines, {num(bulk['rows'])} rows",
        "",
        *headline,
        "| store | bytes | on disk | x raw | x gzip -9 | x zstd -19 |",
        "|---|---:|---:|---:|---:|---:|",
        *(f"| {name} | {num(b)} | {num(d)} | {r:.3f} | {'-' if g is None else f'{g:.3f}'} | "
          f"{'-' if z is None else f'{z:.3f}'} |" for name, b, d, r, g, z in rows),
        "",
        f"On disk is whole {num(get('disk_block_bytes'))}-byte blocks, as `bench-storage` measures "
        "them. kibbr's is its whole data directory: the data files, `catalog.log` and `manifest.log`.",
        "",
        "## `kibbr bench-storage`",
        "",
        "```",
        *result["report"],
        "```",
        "",
        "## What it was measured with",
        "",
        f"- {k['version']}, commit `{k['commit']}`" + (", **with uncommitted changes**" if k["dirty"] else ""),
        f"- `kibbr {' '.join(k['argv'])}`",
        f"- {m.get('os')} {m.get('os_version')}, {m.get('arch')}, {m.get('cpu', '-')}, "
        f"{m.get('cores', '-')} cores"
        + (f" ({m['physical_cores']} physical)" if m.get("physical_cores") else "")
        + f", {num(m.get('memory_bytes'))} bytes of memory"
        + (f", on {m['host']}" if m.get("host") else ""),
        f"- {result['warnings']} warning(s) in bench-storage's log; {result['seconds']} s start to end, "
        "download included",
        "",
    ]
    with open(path, "w") as f:
        f.write("\n".join(lines))
    return path


def main():
    p = argparse.ArgumentParser(description="kibbr bench-storage over downloaded log corpora")
    p.add_argument("--input", default=os.path.join(ROOT, "bench", "compression.json"))
    p.add_argument("--output", default=os.path.join(ROOT, "bench", "results", "compression.json"))
    p.add_argument("--tag", action="append", help="measure only this source (repeatable)")
    p.add_argument("--bin", help="the kibbr to run; nothing is built")
    p.add_argument("--no-build", action="store_true", help="use target/release/kibbr as it is")
    p.add_argument("--no-zstd", action="store_true", help="as bench-storage --no-zstd: hours saved on a big corpus")
    p.add_argument("--zstd-threads", type=int, metavar="N",
                   help="as bench-storage --zstd-threads; a source's own zstd_threads wins")
    p.add_argument("--md-dir", help="where each tag's .md goes [the output's path without .json]")
    args = p.parse_args()
    if args.zstd_threads is not None and args.zstd_threads < 1:
        fail(f"--zstd-threads must be 1 or more: {args.zstd_threads}")
    md_dir = args.md_dir or os.path.splitext(os.path.abspath(args.output))[0]

    sources = load_sources(args.input)
    if args.tag:
        known = {s["tag"] for s in sources}
        unknown = [t for t in args.tag if t not in known]
        if unknown:
            fail(f"no source tagged {', '.join(unknown)} in {args.input}; tags: {', '.join(sorted(known))}")
        sources = [s for s in sources if s["tag"] in args.tag]

    doc = load_results(args.output)
    todo = []
    for s in sources:
        if s["tag"] in doc["results"]:
            at = doc["results"][s["tag"]].get("measured_at", "an earlier run")
            say(f"{s['tag']}: result already present in {shown(os.path.abspath(args.output))} ({at}); not run")
        else:
            todo.append(s)
    if not todo:
        return 0

    bin_path = os.path.abspath(args.bin) if args.bin else os.path.join(ROOT, "target", "release", "kibbr")
    if not args.bin and not args.no_build:
        say("cargo build --release")
        if subprocess.run(["cargo", "build", "--release", "--quiet"], cwd=ROOT).returncode != 0:
            fail("the build failed")
    if not os.access(bin_path, os.X_OK):
        fail(f"{bin_path}: no kibbr there")

    failed = 0
    for s in todo:
        result = measure(s, bin_path, args.no_zstd, args.zstd_threads)
        if result is None:
            failed += 1
            continue
        doc["results"][s["tag"]] = result
        save_results(args.output, doc)
        md = write_markdown(result, md_dir)
        sm = result["summary"]
        zst = "not run" if sm["zstd_19_x_raw"] is None else f"{sm['zstd_19_x_raw']}x"
        say(f"{s['tag']}: kibbr {sm['kibbr_x_raw']}x raw, gzip -9 {sm['gzip_9_x_raw']}x, "
            f"zstd -19 {zst} - written to {shown(os.path.abspath(args.output))} "
            f"and {shown(md)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
