#!/usr/bin/env python3
"""Copy queries_warm from a mdb_query_complex_summary JSON into the repo baseline file."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--from", dest="src", type=Path, required=True, help="mdb_query_*_summary.json")
    ap.add_argument(
        "--out",
        type=Path,
        default=Path("benchmarks/baselines/mdb_query_complex_baseline.json"),
        help="Baseline output path (default: benchmarks/baselines/...)",
    )
    ap.add_argument("--phase", default="queries_warm", help="Phase key to copy (default queries_warm)")
    args = ap.parse_args()

    with args.src.open("r", encoding="utf-8") as f:
        doc = json.load(f)
    if not isinstance(doc, dict):
        raise SystemExit(f"expected object: {args.src}")

    phase = doc.get(args.phase)
    if not isinstance(phase, list) or not phase:
        raise SystemExit(f"missing or empty {args.phase!r} in {args.src}")

    out = {
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "benchmark_profile": doc.get("benchmark_profile", "mdb_query_complex_v1"),
        "table": doc.get("table", "qcx"),
        "load_rows": doc.get("load_rows", doc.get("row_count_hint")),
        "row_count_hint": doc.get("row_count_hint"),
        "bench_page_size": doc.get("bench_page_size", 100),
        "bench_warmup": doc.get("bench_warmup"),
        "bench_iters": doc.get("bench_iters"),
        "bench_profile": doc.get("bench_profile", "all"),
        "schema": doc.get("schema", "id,dept,val,k + INDEX ik(k)"),
        "load_wall_ms": doc.get("load_wall_ms"),
        "load_tps_est": doc.get("load_tps_est"),
        "query_wall_ms": doc.get("query_wall_ms"),
        "engine_bulk_import": doc.get("engine_bulk_import"),
        "source_summary": str(args.src.as_posix()),
        "note": "Promoted via promote_mdb_query_baseline.py; compare with compare_mdb_query_summary.py",
        args.phase: phase,
    }
    cold = doc.get("queries_cold")
    if isinstance(cold, list) and cold:
        out["queries_cold"] = cold

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(out, f, indent=2)
        f.write("\n")

    print(f"wrote {args.out} ({len(phase)} queries from {args.phase})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
