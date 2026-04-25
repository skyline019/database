#!/usr/bin/env python3
"""CI gate: smoke binary + microbench gtest (through ctest).

Note: `newdb_smoke` requires a subcommand and returns non-zero on usage output,
so the gate runs a minimal create/append/load flow on a temp `.bin`.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from typing import Optional


def _resolve_build_config(build_dir: str, requested: str) -> Optional[str]:
    if requested:
        return requested
    for cfg in ("RelWithDebInfo", "Release", "Debug", "MinSizeRel"):
        if os.path.isdir(os.path.join(build_dir, cfg)):
            return cfg
    return None


def _resolve_smoke_binary(build_dir: str, build_config: Optional[str]) -> str:
    binary_name = "newdb_smoke.exe" if os.name == "nt" else "newdb_smoke"
    candidates = [os.path.join(build_dir, binary_name)]
    if build_config:
        candidates.insert(0, os.path.join(build_dir, build_config, binary_name))
    for path in candidates:
        if os.path.isfile(path):
            return path
    return candidates[0]


def _resolve_runtime_report_binary(build_dir: str, build_config: Optional[str]) -> str:
    binary_name = "newdb_runtime_report.exe" if os.name == "nt" else "newdb_runtime_report"
    candidates = [os.path.join(build_dir, binary_name)]
    if build_config:
        candidates.insert(0, os.path.join(build_dir, build_config, binary_name))
    for path in candidates:
        if os.path.isfile(path):
            return path
    return candidates[0]


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument(
        "build_dir",
        nargs="?",
        default="build",
        help="CMake build directory (relative to repo root when not absolute)",
    )
    p.add_argument(
        "--build-config",
        default="",
        help="CMake multi-config profile (e.g. RelWithDebInfo). Auto-detected when omitted.",
    )
    p.add_argument(
        "--runtime-jsonl",
        default="",
        help="Path to runtime snapshot jsonl for extra gate checks (optional).",
    )
    p.add_argument(
        "--runtime-last-n",
        type=int,
        default=2,
        help="Use only last N snapshots for gate delta (must be >=2).",
    )
    p.add_argument(
        "--runtime-label-prefix",
        default="",
        help="Optional label prefix filter before computing runtime deltas.",
    )
    p.add_argument(
        "--min-vacuum-efficiency",
        type=float,
        default=-1.0,
        help="Fail gate when vacuum_efficiency is below this threshold (disabled when <0).",
    )
    p.add_argument(
        "--max-conflict-rate",
        type=float,
        default=-1.0,
        help="Fail gate when conflict_rate is above this threshold (disabled when <0).",
    )
    args = p.parse_args()
    here = os.path.dirname(os.path.abspath(__file__))
    # scripts/ci/ci_bench_gate.py -> repo root is two levels up.
    repo_root = os.path.dirname(os.path.dirname(here))
    b = args.build_dir if os.path.isabs(args.build_dir) else os.path.join(repo_root, args.build_dir)
    if not os.path.isdir(b):
        print(f"ERROR: build dir not found: {b}", file=sys.stderr)
        return 2

    build_config = _resolve_build_config(b, args.build_config)
    smoke = _resolve_smoke_binary(b, build_config)
    if os.path.isfile(smoke):
        with tempfile.TemporaryDirectory(prefix="newdb_smoke_") as td:
            bin_path = os.path.join(td, "smoke_ci.bin")
            subprocess.check_call([smoke, "create", bin_path], cwd=b)
            subprocess.check_call([smoke, "append", bin_path, "1"], cwd=b)
            subprocess.check_call([smoke, "load", bin_path], cwd=b)
    else:
        print(f"NOTE: skip newdb_smoke (not built): {smoke}")

    ctest_cmd = ["ctest", "-R", "CiMicrobench", "--output-on-failure"]
    if build_config:
        ctest_cmd.extend(["-C", build_config])
    subprocess.check_call(ctest_cmd, cwd=b)

    runtime_jsonl = args.runtime_jsonl
    if runtime_jsonl:
        if args.runtime_last_n < 2:
            print("ERROR: --runtime-last-n must be >= 2", file=sys.stderr)
            return 8
        if not os.path.isabs(runtime_jsonl):
            runtime_jsonl = os.path.join(repo_root, runtime_jsonl)
        if not os.path.isfile(runtime_jsonl):
            print(f"ERROR: runtime jsonl not found: {runtime_jsonl}", file=sys.stderr)
            return 6

        reporter = _resolve_runtime_report_binary(b, build_config)
        if not os.path.isfile(reporter):
            print(f"ERROR: runtime report tool not found: {reporter}", file=sys.stderr)
            return 7

        cmd = [reporter, "--input", runtime_jsonl, "--last-n", str(args.runtime_last_n)]
        if args.runtime_label_prefix:
            cmd.extend(["--label-prefix", args.runtime_label_prefix])
        if args.min_vacuum_efficiency >= 0.0:
            cmd.extend(["--min-vacuum-efficiency", str(args.min_vacuum_efficiency)])
        if args.max_conflict_rate >= 0.0:
            cmd.extend(["--max-conflict-rate", str(args.max_conflict_rate)])
        out = subprocess.check_output(cmd, cwd=b, text=True).strip()
        # Keep one machine-readable summary line in CI logs.
        try:
            parsed = json.loads(out)
            print("RUNTIME_GATE_SUMMARY " + json.dumps(parsed, sort_keys=True))
        except Exception:
            print("RUNTIME_GATE_SUMMARY_RAW " + out)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
