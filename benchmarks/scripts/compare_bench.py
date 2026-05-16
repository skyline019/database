#!/usr/bin/env python3
"""
Compare two Google Benchmark JSON exports (real_time per iteration, ns).

Exit code 0 if every benchmark name present in the baseline exists in the
current run and current.real_time <= baseline.real_time * max_ratio.

Requires Python 3.8+ (stdlib only).
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict


def _bench_map(doc: Dict[str, Any]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for row in doc.get("benchmarks", []):
        if not isinstance(row, dict):
            continue
        name = row.get("name")
        if not isinstance(name, str):
            continue
        rt = row.get("real_time")
        if isinstance(rt, (int, float)):
            out[name] = float(rt)
    return out


def _load(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _run_bench_to_json(bench_exe: Path, out_path: Path) -> None:
    cmd = [
        str(bench_exe),
        "--benchmark_format=json",
        f"--benchmark_out={out_path}",
        "--benchmark_out_format=json",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.stderr.write(proc.stdout or "")
        sys.stderr.write(proc.stderr or "")
        raise SystemExit(
            f"structdb_bench failed (exit {proc.returncode}): {bench_exe}"
        )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--baseline", type=Path, required=True, help="Baseline JSON")
    ap.add_argument(
        "--current",
        type=Path,
        help="Current run JSON (omit with --bench-exe)",
    )
    ap.add_argument(
        "--bench-exe",
        type=Path,
        help="Run this structdb_bench and compare stdout JSON to --baseline",
    )
    ap.add_argument(
        "--max-ratio",
        type=float,
        default=1.5,
        help="Max allowed current/baseline for real_time (default: 1.5)",
    )
    args = ap.parse_args()

    if bool(args.current) == bool(args.bench_exe):
        ap.error("Specify exactly one of --current or --bench-exe")

    tmp: Path | None = None
    if args.bench_exe:
        fd, tmp_name = tempfile.mkstemp(prefix="structdb_bench_", suffix=".json")
        os.close(fd)
        tmp = Path(tmp_name)
        try:
            _run_bench_to_json(args.bench_exe, tmp)
            current_path = tmp
        except SystemExit:
            tmp.unlink(missing_ok=True)
            raise
    else:
        current_path = args.current  # type: ignore[assignment]

    try:
        base_doc = _load(args.baseline)
        cur_doc = _load(current_path)
        base_m = _bench_map(base_doc)
        cur_m = _bench_map(cur_doc)

        if not base_m:
            print("baseline has no benchmarks[] entries", file=sys.stderr)
            return 2

        failed = False
        for name, base_ns in sorted(base_m.items()):
            if name not in cur_m:
                print(f"MISSING in current: {name}", file=sys.stderr)
                failed = True
                continue
            cur_ns = cur_m[name]
            ratio = cur_ns / base_ns if base_ns > 0 else float("inf")
            limit = base_ns * args.max_ratio
            ok = cur_ns <= limit + 1e-9  # float tolerance
            status = "OK" if ok else "FAIL"
            print(f"{status}  {name}:  current={cur_ns:.3f} ns  baseline={base_ns:.3f} ns  ratio={ratio:.3f}")
            if not ok:
                failed = True

        extra = set(cur_m) - set(base_m)
        for name in sorted(extra):
            print(f"NOTE  extra benchmark in current (ignored): {name}")

        return 1 if failed else 0
    finally:
        if tmp is not None:
            tmp.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
