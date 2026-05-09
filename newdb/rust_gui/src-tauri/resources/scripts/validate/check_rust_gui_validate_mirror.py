#!/usr/bin/env python3
"""Fail if rust_gui packaged validate copies drift from canonical newdb/scripts/validate."""

from __future__ import annotations

import argparse
import pathlib
import sys


def _read_bytes(p: pathlib.Path) -> bytes:
    return p.read_bytes()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    root = pathlib.Path(__file__).resolve().parents[2]
    canonical = root / "scripts" / "validate"
    default_mirrors = [
        root / "rust_gui" / "scripts" / "validate",
        root / "rust_gui" / "src-tauri" / "resources" / "scripts" / "validate",
    ]
    ap.add_argument("--canonical", type=pathlib.Path, default=canonical)
    ap.add_argument(
        "--mirror",
        type=pathlib.Path,
        action="append",
        default=[],
        help="May repeat; defaults to rust_gui/scripts/validate and src-tauri/resources/scripts/validate.",
    )
    args = ap.parse_args()
    mirrors = list(args.mirror) if args.mirror else default_mirrors

    rel_paths = [
        pathlib.Path("validate_runtime_stats.py"),
        pathlib.Path("RUNTIME_STATS_SCHEMA.md"),
        pathlib.Path("check_runtime_stats_contract_parity.py"),
        pathlib.Path("contract") / "runtime_stats.v1.required.json",
    ]
    failed = False
    pairs = 0
    for mirror in mirrors:
        for rel in rel_paths:
            a = args.canonical / rel
            b = mirror / rel
            pairs += 1
            if not a.is_file():
                print(f"MIRROR_CHECK_SKIP missing canonical {a}", file=sys.stderr)
                continue
            if not b.is_file():
                print(f"MIRROR_CHECK_FAIL missing mirror {b}", file=sys.stderr)
                failed = True
                continue
            if _read_bytes(a) != _read_bytes(b):
                print(f"MIRROR_CHECK_FAIL drift: {mirror.name}/{rel}", file=sys.stderr)
                failed = True
    if failed:
        return 1
    print(f"MIRROR_CHECK_OK compared {pairs} file byte-pairs across {len(mirrors)} mirror roots")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
