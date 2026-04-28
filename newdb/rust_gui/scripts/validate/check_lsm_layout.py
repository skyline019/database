#!/usr/bin/env python3
"""Validate LSM-lite on-disk layout conventions.

This is a lightweight gate used by local runs and CI scripts.
It validates that:
  - <data>.lsm directory exists (when requested)
  - segment files follow L0_/L1_ naming and .log extension
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


def fail(msg: str) -> int:
    print(f"LSM_LAYOUT_INVALID: {msg}", file=sys.stderr)
    return 2


L0_RE = re.compile(r"^L0_\d+\.log$")
L1_RE = re.compile(r"^L1_\d+\.log$")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("data_file", help="Path to <table>.bin")
    p.add_argument(
        "--require-non-empty",
        action="store_true",
        help="Fail when segment dir exists but has no segment logs",
    )
    args = p.parse_args()

    data = Path(args.data_file)
    seg_dir = Path(str(data) + ".lsm")
    if not seg_dir.is_dir():
        return fail(f"segment dir not found: {seg_dir}")

    logs = [p for p in seg_dir.iterdir() if p.is_file() and p.suffix == ".log"]
    if args.require_non_empty and not logs:
        return fail(f"no segment logs found under: {seg_dir}")

    bad = []
    for pth in logs:
        name = pth.name
        if not (L0_RE.match(name) or L1_RE.match(name)):
            bad.append(name)
    if bad:
        return fail(f"unexpected segment filenames: {bad}")

    print(f"LSM_LAYOUT_VALID: dir={seg_dir} logs={len(logs)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

