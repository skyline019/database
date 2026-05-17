#!/usr/bin/env python3
"""Compare two mega_data_summary JSON files (runtime_pressure_tps_est)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict


def _load(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        doc = json.load(f)
    if not isinstance(doc, dict):
        raise SystemExit(f"expected object JSON: {path}")
    return doc


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--baseline", type=Path, required=True)
    ap.add_argument("--current", type=Path, required=True)
    ap.add_argument(
        "--max-tps-ratio-delta",
        type=float,
        default=0.05,
        help="Max relative TPS difference |a-b|/max(a,b) (default 0.05)",
    )
    args = ap.parse_args()
    base = _load(args.baseline)
    cur = _load(args.current)
    btps = float(base.get("runtime_pressure_tps_est", 0))
    ctps = float(cur.get("runtime_pressure_tps_est", 0))
    if btps <= 0 or ctps <= 0:
        sys.stderr.write("missing or zero runtime_pressure_tps_est\n")
        return 2
    denom = max(btps, ctps)
    delta = abs(btps - ctps) / denom
    print(f"baseline_tps={btps} current_tps={ctps} relative_delta={delta:.4f}")
    if delta > args.max_tps_ratio_delta:
        sys.stderr.write(
            f"TPS delta {delta:.4f} exceeds max {args.max_tps_ratio_delta}\n"
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
