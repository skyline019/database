#!/usr/bin/env python3
"""Parse an LCOV .info file and enforce minimum line-hit rates for path substrings.

Example:
  geninfo build_cov --ignore-errors mismatch -o raw.info
  python3 scripts/ci/check_coverage_threshold.py raw.info \\
    --require new/src/:88 --require new/demo/:78
"""

from __future__ import annotations

import argparse
import re
import sys
from typing import Dict, List, Tuple


def parse_lcov(path: str) -> Dict[str, List[int]]:
    per_file: Dict[str, List[int]] = {}
    cur: str | None = None
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            if line.startswith("SF:"):
                cur = line[3:].strip()
                per_file.setdefault(cur, [])
            elif line.startswith("DA:") and cur is not None:
                m = re.match(r"DA:(\d+),(\d+)", line)
                if m:
                    per_file[cur].append(int(m.group(2)))
            elif line.startswith("end_of_record"):
                cur = None
    return per_file


def aggregate(files: Dict[str, List[int]], path_substr: str, exclude_test_paths: bool) -> Tuple[int, int]:
    hit = 0
    total = 0
    norm_sub = path_substr.replace("\\", "/")
    for sf, hits in files.items():
        sf_n = sf.replace("\\", "/")
        if norm_sub not in sf_n:
            continue
        if exclude_test_paths and "/tests/" in sf_n:
            continue
        for h in hits:
            total += 1
            if h > 0:
                hit += 1
    return hit, total


def main() -> int:
    ap = argparse.ArgumentParser(description="LCOV line coverage gate for path prefixes.")
    ap.add_argument("lcov_info", help="Path to .info from lcov/geninfo")
    ap.add_argument(
        "--require",
        action="append",
        default=[],
        metavar="SUBSTR:MIN_PCT",
        help="Substring that must appear in SF paths (e.g. new/src/), minimum line %% hit. Repeatable.",
    )
    ap.add_argument(
        "--include-tests",
        action="store_true",
        help="Count lines in paths containing /tests/ (default: skip them in totals).",
    )
    args = ap.parse_args()

    requires = args.require
    if not requires:
        requires = ["new/src/:85"]

    files = parse_lcov(args.lcov_info)
    if not files:
        print("No LCOV records found.", file=sys.stderr)
        return 2

    fail = False
    skip_tests = not args.include_tests
    for spec in requires:
        if ":" not in spec:
            print(f"Invalid --require (need substr:pct): {spec}", file=sys.stderr)
            return 2
        sub, pct_s = spec.rsplit(":", 1)
        try:
            min_pct = float(pct_s)
        except ValueError:
            print(f"Invalid percentage in: {spec}", file=sys.stderr)
            return 2
        h, t = aggregate(files, sub, skip_tests)
        pct = 100.0 * h / t if t else 100.0
        print(f"[{sub}] line coverage: {pct:.2f}%  ({h}/{t} instrumented lines)")
        if t == 0:
            print(f"No matching lines for substring {sub!r}", file=sys.stderr)
            fail = True
        elif pct + 1e-9 < min_pct:
            print(f"Below required {min_pct:.2f}%", file=sys.stderr)
            fail = True

    return 1 if fail else 0


if __name__ == "__main__":
    sys.exit(main())
