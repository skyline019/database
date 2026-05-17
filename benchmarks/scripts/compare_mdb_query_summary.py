#!/usr/bin/env python3
"""Compare mdb_query_summary / mdb_query_complex_summary JSON (per-query ms_p95)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple


def _load(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        doc = json.load(f)
    if not isinstance(doc, dict):
        raise SystemExit(f"expected object JSON: {path}")
    return doc


def _query_map(doc: Dict[str, Any], phase_key: str) -> Dict[str, float]:
    out: Dict[str, float] = {}
    rows = doc.get(phase_key)
    if not isinstance(rows, list):
        return out
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = row.get("name")
        p95 = row.get("ms_p95")
        if isinstance(name, str) and isinstance(p95, (int, float)):
            out[name] = float(p95)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--baseline", type=Path, required=True)
    ap.add_argument("--current", type=Path, required=True)
    ap.add_argument(
        "--phase",
        default="queries_warm",
        help="JSON key for query stats (default queries_warm)",
    )
    ap.add_argument(
        "--max-p95-ratio",
        type=float,
        default=1.25,
        help="Fail if current ms_p95 > baseline * ratio (default 1.25)",
    )
    ap.add_argument(
        "--min-baseline-ms",
        type=float,
        default=0.05,
        help="Skip ratio check when baseline ms_p95 below this (noise)",
    )
    ap.add_argument(
        "--ignore-queries",
        default="",
        help="Comma-separated query names to skip in ratio check (e.g. soak cases)",
    )
    args = ap.parse_args()
    ignore = {q.strip() for q in args.ignore_queries.split(",") if q.strip()}

    base = _load(args.baseline)
    cur = _load(args.current)
    bm = _query_map(base, args.phase)
    cm = _query_map(cur, args.phase)
    if not bm:
        sys.stderr.write(f"baseline has no {args.phase} entries\n")
        return 2
    if not cm:
        sys.stderr.write(f"current has no {args.phase} entries\n")
        return 2

    failures: List[Tuple[str, float, float, float]] = []
    missing: List[str] = []
    for name, b95 in sorted(bm.items()):
        if name not in cm:
            missing.append(name)
            continue
        if name in ignore:
            print(f"  {name}: (skip, --ignore-queries)")
            continue
        c95 = cm[name]
        if b95 < args.min_baseline_ms:
            print(f"  {name}: baseline_p95={b95:.3f}ms current_p95={c95:.3f}ms (skip, tiny baseline)")
            continue
        ratio = c95 / b95 if b95 > 0 else 0.0
        print(f"  {name}: baseline_p95={b95:.3f}ms current_p95={c95:.3f}ms ratio={ratio:.3f}")
        if ratio > args.max_p95_ratio:
            failures.append((name, b95, c95, ratio))

    if missing:
        sys.stderr.write("missing queries in current: " + ", ".join(missing) + "\n")
        return 1

    if failures:
        sys.stderr.write(
            f"{len(failures)} queries exceeded max_p95_ratio={args.max_p95_ratio}:\n"
        )
        for name, b95, c95, ratio in failures:
            sys.stderr.write(f"  {name}: {b95:.3f} -> {c95:.3f} (ratio {ratio:.3f})\n")
        return 1

    print("compare_mdb_query_summary: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
