#!/usr/bin/env python3
# How big a source in scripts/bench/compression.json is before anything is downloaded: one request
# a file for its last four bytes. The answer's Content-Range gives the file's size, and a gzip
# file's last four bytes are its decoded size modulo 2^32 (RFC 1952's ISIZE) - exact for a
# file of one gzip member under 4 GiB, as JSONBench's are. A file large enough that it could
# decode past 4 GiB - more than 4 GiB / MAX_RATIO - is shown as its trailer's value modulo 4 GiB
# and counted as unknown: Quickwit's HDFS logs are 470 MB, whose trailer says 2.9 GB of what is
# 7.2 GB. A plain file decodes to itself. A server that will not send a range gives the size
# alone, from Content-Length.
#
#   scripts/bench/sizes.py --tag bluesky-1b                 # every file of the source
#   scripts/bench/sizes.py --tag bluesky-1b --first 100     # its first 100: the 100M of JSONBench
#   scripts/bench/sizes.py --tag bluesky-1b --first 3 --each
#
# To check the trailer is telling the truth, bluesky-1m's file_0001.json.gz decodes to
# 480,778,277 bytes (scripts/bench/results/compression/bluesky-1m.md).

import argparse
import concurrent.futures
import importlib.util
import os
import re
import sys
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
# The most gzip -9 is taken to shrink NDJSON logs: past 4 GiB / this, ISIZE may have wrapped.
MAX_RATIO = 25
WRAP = 1 << 32
# compression.py is loaded, not imported as a package: no __pycache__ beside it.
sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location("compression", os.path.join(HERE, "compression.py"))
compression = importlib.util.module_from_spec(spec)
spec.loader.exec_module(compression)


def size_of(url):
    """(bytes, decoded bytes or None, the gzip trailer's value when it may have wrapped)."""
    req = urllib.request.Request(url, headers={"User-Agent": "kibbr-bench-sizes", "Range": "bytes=-4"})
    with urllib.request.urlopen(req, timeout=60) as r:
        if r.status == 206:
            total = re.search(r"/(\d+)$", r.headers.get("Content-Range", ""))
            tail = r.read()
            if not total:
                raise OSError(f"{url}: 206 without a total in Content-Range")
            size = int(total.group(1))
            # The first two bytes say whether it is gzip at all; the tail alone cannot.
            head = urllib.request.Request(url, headers={"User-Agent": "kibbr-bench-sizes", "Range": "bytes=0-1"})
            with urllib.request.urlopen(head, timeout=60) as h:
                magic = h.read(2)
            if magic == compression.GZIP_MAGIC:
                isize = int.from_bytes(tail, "little") if len(tail) == 4 else None
                if isize is not None and size * MAX_RATIO >= WRAP:
                    return size, None, isize
                return size, isize, None
            # zstd (or anything else not NDJSON) is not sized here; plain NDJSON is its own size.
            return size, (None if magic == compression.ZSTD_MAGIC[:2] else size), None
        # No range: the size is in the headers, and the body is not read.
        return int(r.headers.get("Content-Length") or 0), None, None


def main():
    p = argparse.ArgumentParser(description="a compression source's sizes, before downloading it")
    p.add_argument("--tag", required=True)
    p.add_argument("--input", default=os.path.join(HERE, "compression.json"))
    p.add_argument("--first", type=int, metavar="N", help="only the source's first N files")
    p.add_argument("--each", action="store_true", help="a line for each file")
    p.add_argument("--jobs", type=int, default=16, help="requests at once [16]")
    args = p.parse_args()
    found = [s for s in compression.load_sources(args.input) if s["tag"] == args.tag]
    if not found:
        sys.exit(f"sizes: no source tagged {args.tag} in {args.input}")
    s = found[0]
    if "path" in s:
        sys.exit(f"sizes: {args.tag} is a local path; ls -l says its size")
    urls = compression.url_list(s) if "urls" in s else [s["url"]]
    if args.first is not None:
        urls = urls[: args.first]
    with concurrent.futures.ThreadPoolExecutor(max(1, args.jobs)) as pool:
        got = list(pool.map(size_of, urls))
    total = sum(g[0] for g in got)
    decoded = [g[1] for g in got]
    if args.each:
        for url, (size, dec, wrapped) in zip(urls, got):
            shown = f"{dec:,}" if dec is not None else "-" if wrapped is None else f"{wrapped:,} + k x 4 GiB"
            print(f"{size:>16,}  {shown:>16}  {os.path.basename(url.split('?')[0])}")
    print(f"{len(urls):,} file(s), {total:,} bytes to download ({total / 1e9:,.1f} GB)")
    if all(d is not None for d in decoded):
        whole = sum(decoded)
        print(f"{whole:,} bytes decoded ({whole / 1e9:,.1f} GB), {whole / max(total, 1):.2f}x the download, "
              "from each gzip trailer (a plain file is its own size): exact for single-member gzip files under 4 GiB")
    else:
        unknown = sum(d is None for d in decoded)
        print(f"decoded size not known for {unknown:,} of {len(urls):,} file(s): over 4 GiB / {MAX_RATIO} "
              "compressed, so its gzip trailer may have wrapped; zstd; or a server without ranges")
        for url, (size, _, wrapped) in zip(urls, got):
            if wrapped is not None:
                fits = [wrapped + k * WRAP for k in range(MAX_RATIO + 1) if size <= wrapped + k * WRAP <= size * MAX_RATIO]
                print(f"  {os.path.basename(url.split('?')[0])}: {', or '.join(f'{f:,}' for f in fits)} bytes; "
                      "decode it once to know")
    return 0


if __name__ == "__main__":
    sys.exit(main())
