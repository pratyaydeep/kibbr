#!/usr/bin/env python3
"""strip_comments.py - what is left of Rust source when the comments are gone.

    strip_comments.py FILE.rs...               the code, comments taken out
    strip_comments.py --signatures FILE.rs...  one line per fn, sorted

Two uses:

  comments only changed:  stripped output before == after, byte for byte
  code only moved:        --signatures before == after as a sorted multiset

Default mode, for each file in the order given, prints `== PATH ==` and then
the file with
  - every line comment (//, ///, //!) and block comment (nested ones too) removed;
  - `use` and `extern crate` items dropped, through their `;`;
  - `mod name;` and the `mod name {` opener dropped;
  - `#[...]` and `#![...]` attributes dropped, through the closing bracket;
  - whitespace runs made one space, line ends trimmed, empty lines dropped.

--signatures prints, for every `fn` in all the files together, `name(params)`:
no visibility, qualifiers, generics, return type or body; the parameter list on
one line with no trailing comma. Sorted by byte, duplicates kept, no paths - a
fn that moved to another file or another impl prints the same line.

A `//` or `/*` inside a literal is left alone, and so is the whitespace in it.
Literals understood: "..." with escapes (on one line or over several),
r"..." / r#"..."# with b and c prefixes, and char literals ('/', '"', '\\'').

What it does not handle:
  - It is a scanner, not a parser. Macro bodies are read as ordinary code: a
    `'` or `"` that is a bare token in a macro_rules pattern can be taken for
    the start of a literal.
  - `use`, `mod`, `extern crate` and attributes are dropped only when they
    lead a line (after an optional `pub`, `pub(...)`). One that follows other
    code on its line stays.
  - The closing `}` of an inline `mod name { ... }` stays; only the opener goes.
  - --signatures skips `fn $name` in macro_rules bodies and fn-pointer types,
    and misreads a generic list holding a bare `>` or `<` comparison
    (`<const N: usize = { A > B }>`).
  - --signatures does not say which impl, trait or module a fn is in. A fn
    moved between two impls with the same parameter text is not seen.
  - A file with a NUL byte, or an unterminated literal or block comment, is
    refused: exit 2, nothing printed for any file.
"""

import argparse
import re
import sys

CHAR = re.compile(r"'(?:\\(?:u\{[0-9a-fA-F_]+\}|x[0-9a-fA-F]{2}|.)|[^\\'\n])'")
RAW = re.compile(r'[bc]?r(#*)"')
IDENT = re.compile(r"[^\W\d]\w*")
PLACE = re.compile(r"\0(\d+)\0")

VIS = r"(?:pub(?:\s*\([^)]*\))?\s+)?"
USE = re.compile(r"^[ \t]*" + VIS + r"(?:use\b(?!\s*<)|extern\s+crate\b)[^;]*;", re.M)
MOD = re.compile(r"^[ \t]*" + VIS + r"mod\s+\w+\s*[;{]", re.M)
ATTR = re.compile(r"^[ \t]*#!?\[", re.M)
ATTR_NEXT = re.compile(r"[ \t]*#!?\[")
FN = re.compile(r"\bfn\s+((?:r#)?[^\W\d]\w*)")


class Refused(Exception):
    pass


def scan(src, path):
    """Comments out, literals replaced by \\0N\\0. Returns (code, literals)."""
    if "\0" in src:
        raise Refused(f"{path}: NUL byte")
    out, lits = [], []
    n = len(src)

    def lit(s):
        out.append(f"\0{len(lits)}\0")
        lits.append(s)

    def unterminated(at, what):
        line = src.count("\n", 0, at) + 1
        return Refused(f"{path}:{line}: unterminated {what}")

    i = 0
    while i < n:
        c = src[i]
        if src.startswith("//", i):
            j = src.find("\n", i)
            i = n if j < 0 else j
        elif src.startswith("/*", i):
            depth, j = 1, i + 2
            while depth:
                opened, closed = src.find("/*", j), src.find("*/", j)
                if closed < 0:
                    raise unterminated(i, "block comment")
                if 0 <= opened < closed:
                    depth, j = depth + 1, opened + 2
                else:
                    depth, j = depth - 1, closed + 2
            out.append(" ")
            i = j
        elif c == '"':
            j = i + 1
            while j < n and src[j] != '"':
                j += 2 if src[j] == "\\" else 1
            if j >= n:
                raise unterminated(i, "string")
            lit(src[i : j + 1])
            i = j + 1
        elif c == "'":
            m = CHAR.match(src, i)
            if m:
                lit(m.group())
                i = m.end()
            else:
                out.append(c)
                i += 1
        elif c == "_" or c.isalpha():
            m = RAW.match(src, i)
            if m:
                close = '"' + m.group(1)
                j = src.find(close, m.end())
                if j < 0:
                    raise unterminated(i, "raw string")
                lit(src[i : j + len(close)])
                i = j + len(close)
            else:
                m = IDENT.match(src, i)
                out.append(m.group())
                i = m.end()
        else:
            out.append(c)
            i += 1
    return "".join(out), lits


def restore(text, lits):
    return PLACE.sub(lambda m: lits[int(m.group(1))], text)


def drop_attrs(code):
    out, pos = [], 0
    m = ATTR.search(code)
    while m:
        out.append(code[pos : m.start()])
        while m:
            depth, j = 1, m.end()
            while depth and j < len(code):
                depth += (code[j] == "[") - (code[j] == "]")
                j += 1
            m = ATTR_NEXT.match(code, j)
        pos = j
        m = ATTR.search(code, pos)
    out.append(code[pos:])
    return "".join(out)


def stripped(code, lits):
    code = MOD.sub("", USE.sub("", drop_attrs(code)))
    lines = (" ".join(line.split()) for line in code.split("\n"))
    return [restore(line, lits) for line in lines if line]


def signatures(code, lits):
    n = len(code)

    def blanks(j):
        while j < n and code[j].isspace():
            j += 1
        return j

    for m in FN.finditer(code):
        j = blanks(m.end())
        if j < n and code[j] == "<":
            depth = 0
            while j < n:
                if code.startswith("->", j):
                    j += 2
                    continue
                depth += (code[j] == "<") - (code[j] == ">")
                j += 1
                if not depth:
                    break
            j = blanks(j)
        if j >= n or code[j] != "(":
            continue
        depth, k = 1, j + 1
        while depth and k < n:
            depth += (code[k] == "(") - (code[k] == ")")
            k += 1
        params = " ".join(code[j + 1 : k - 1].split()).rstrip(", ")
        yield f"{m.group(1)}({restore(params, lits)})"


def main():
    ap = argparse.ArgumentParser(
        description="Rust source with the comments gone, or its fn signatures.",
        epilog="See the header of this file for the two uses and the limits.",
    )
    ap.add_argument("--signatures", action="store_true", help="one line per fn, sorted")
    ap.add_argument("paths", nargs="+", metavar="FILE.rs")
    args = ap.parse_args()

    lines = []
    try:
        for path in args.paths:
            with open(path, encoding="utf-8", newline="") as f:
                code, lits = scan(f.read().replace("\r\n", "\n"), path)
            if args.signatures:
                lines.extend(signatures(code, lits))
            else:
                lines.append(f"== {path} ==")
                lines.extend(stripped(code, lits))
    except (Refused, OSError, UnicodeDecodeError) as e:
        print(f"strip_comments.py: {e}", file=sys.stderr)
        return 2
    if args.signatures:
        lines.sort(key=lambda s: s.encode("utf-8"))
    sys.stdout.reconfigure(encoding="utf-8", newline="\n")
    for line in lines:
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
