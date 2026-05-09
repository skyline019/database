#!/usr/bin/env python3
"""Count translation units under newdb/ that #include cli/shell/state/shell_state.h.

Used as a compile-coupling baseline (Wave 9+). Run from repo root or newdb/:
  python newdb/tools/count_shell_state_includes.py
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

INCLUDE_RE = re.compile(
    r'^\s*#\s*include\s+"(?:cli/shell/state/shell_state\.h|cli\\\\shell\\\\state\\\\shell_state\.h)"',
    re.MULTILINE,
)


def collect_hits(newdb_root: Path) -> list[Path]:
    hits: list[Path] = []
    for ext in (".cc", ".cpp"):
        for p in newdb_root.rglob(f"*{ext}"):
            if "build" in p.parts or "_deps" in p.parts:
                continue
            try:
                text = p.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            if INCLUDE_RE.search(text):
                try:
                    rel = p.relative_to(newdb_root.parent)
                except ValueError:
                    rel = p
                hits.append(rel)
    hits.sort()
    return hits


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fail-if-count-above",
        type=int,
        metavar="N",
        default=None,
        help="Exit with status 1 if the TU count is greater than N (CI coupling ceiling).",
    )
    args = parser.parse_args()

    here = Path(__file__).resolve().parent
    newdb_root = here.parent
    hits = collect_hits(newdb_root)
    print(f"TU count with #include shell_state.h: {len(hits)}")
    for p in hits:
        print(f"  {p}")
    if args.fail_if_count_above is not None and len(hits) > args.fail_if_count_above:
        print(
            f"error: count {len(hits)} exceeds ceiling {args.fail_if_count_above}",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
