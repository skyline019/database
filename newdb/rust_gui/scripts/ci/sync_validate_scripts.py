#!/usr/bin/env python3
"""Phase-0 sync helper: keep `scripts/validate/` mirrors in lock-step.

Canonical:   newdb/scripts/validate/
Mirrors:     newdb/rust_gui/scripts/validate/
             newdb/rust_gui/src-tauri/resources/scripts/validate/

Why three copies?
  - canonical: used by repo-root CI / dev shell.
  - rust_gui/scripts: used by GUI dev pipelines (npm scripts).
  - src-tauri/resources/scripts: bundled into the Tauri build per
    `tauri.conf.json` `bundle.resources` ("resources/scripts").

Usage:
  python scripts/ci/sync_validate_scripts.py          # check; non-zero exit on drift
  python scripts/ci/sync_validate_scripts.py --apply  # copy canonical -> mirrors
"""

from __future__ import annotations

import argparse
import filecmp
import os
import shutil
import sys
from pathlib import Path

CANONICAL_REL = "scripts/validate"
MIRROR_RELS = [
    "rust_gui/scripts/validate",
    "rust_gui/src-tauri/resources/scripts/validate",
]
TRACKED_FILES = [
    "RUNTIME_STATS_SCHEMA.md",
    "contract/runtime_stats.v1.required.json",
    "contract/runtime_stats.v1.line.schema.json",
    "validate_runtime_stats.py",
    "test_validate_runtime_stats_compat.py",
    "test_runtime_report_compact_gates.py",
    "validate_runtime_trend_dashboard.py",
    "validate_telemetry_event.py",
    "validate_perf_summary.py",
    "check_lsm_layout.py",
    "check_c_api_abi.py",
    "check_runtime_stats_contract_parity.py",
    "c_api_expected_symbols.txt",
]


def _newdb_root(here: Path) -> Path:
    """Project root is the `newdb/` directory (same convention as capture_baseline.py)."""
    # scripts/ci/<this>.py -> newdb/ is three parents up
    return Path(os.environ.get("NEWDB_REPO_ROOT", here.resolve().parents[2]))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Copy canonical files into each mirror; otherwise drift-check only.",
    )
    args = parser.parse_args()

    root = _newdb_root(Path(__file__))
    canonical = root / CANONICAL_REL
    mirrors = [root / m for m in MIRROR_RELS]

    if not canonical.is_dir():
        print(f"[sync_validate_scripts] canonical dir missing: {canonical}", file=sys.stderr)
        return 2

    drift: list[str] = []
    missing: list[str] = []
    for name in TRACKED_FILES:
        src = canonical / name
        if not src.is_file():
            print(f"[sync_validate_scripts] WARN canonical missing: {src}", file=sys.stderr)
            continue
        for m in mirrors:
            dst = m / name
            if not dst.is_file():
                missing.append(f"{m}/{name}")
                if args.apply:
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(src, dst)
                continue
            if not filecmp.cmp(src, dst, shallow=False):
                drift.append(f"{m}/{name}")
                if args.apply:
                    shutil.copy2(src, dst)

    if missing:
        print("Missing in mirrors:")
        for p in missing:
            print(f"  - {p}")
    if drift:
        print("Drifted from canonical:")
        for p in drift:
            print(f"  - {p}")

    if not missing and not drift:
        print("[sync_validate_scripts] OK: all mirrors in sync with canonical.")
        return 0
    if args.apply:
        print(f"[sync_validate_scripts] APPLIED: copied canonical -> {len(missing) + len(drift)} mirror file(s).")
        return 0
    print(
        "[sync_validate_scripts] DRIFT detected; rerun with --apply to fix.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
