#!/usr/bin/env python3
"""CI gate: smoke binary + microbench gtest (through ctest).

Note: `newdb_smoke` requires a subcommand and returns non-zero on usage output,
so the gate runs a minimal create/append/load flow on a temp `.bin`.
"""

from __future__ import annotations

import argparse
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
