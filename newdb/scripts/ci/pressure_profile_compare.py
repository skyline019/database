#!/usr/bin/env python3
"""Compare two concurrent pressure summaries for CI/nightly."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def _load(path: str) -> dict:
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(f"summary not found: {p}")
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--leveldb-summary", required=True)
    ap.add_argument("--innodb-summary", required=True)
    ap.add_argument("--hybrid-summary", default="")
    ap.add_argument("--output", required=True)
    ap.add_argument("--min-tps-ratio", type=float, default=1.05)
    ap.add_argument("--max-p95-ratio", type=float, default=1.15)
    ap.add_argument("--max-hybrid-flips", type=int, default=3)
    ap.add_argument("--hybrid-tps-lower-ratio", type=float, default=0.9)
    ap.add_argument("--hybrid-tps-upper-ratio", type=float, default=1.1)
    args = ap.parse_args()

    level = _load(args.leveldb_summary)
    innodb = _load(args.innodb_summary)
    hybrid = _load(args.hybrid_summary) if args.hybrid_summary else None

    level_tps = float(level.get("runtime_pressure_tps_est", 0.0))
    innodb_tps = float(innodb.get("runtime_pressure_tps_est", 0.0))
    level_p95 = float(level.get("runtime_pressure_batch_ms_p95", 0.0))
    innodb_p95 = float(innodb.get("runtime_pressure_batch_ms_p95", 0.0))
    hybrid_tps = float(hybrid.get("runtime_pressure_tps_est", 0.0)) if hybrid else 0.0
    hybrid_p95 = float(hybrid.get("runtime_pressure_batch_ms_p95", 0.0)) if hybrid else 0.0
    hybrid_flips = int(hybrid.get("runtime_hybrid_mode_switch_count", 0)) if hybrid else 0

    tps_ratio = (level_tps / innodb_tps) if innodb_tps > 0 else 0.0
    p95_ratio = (innodb_p95 / level_p95) if level_p95 > 0 else 0.0

    verdict = "warning"
    reasons = []
    if tps_ratio >= args.min_tps_ratio:
        reasons.append("leveldb-like throughput advantage meets threshold")
    else:
        reasons.append("leveldb-like throughput advantage below threshold")
    if p95_ratio <= args.max_p95_ratio:
        reasons.append("innodb-like p95 degradation within threshold")
    else:
        reasons.append("innodb-like p95 degradation too high")
    hybrid_ok = True
    if hybrid:
        lower_tps = min(level_tps, innodb_tps) * args.hybrid_tps_lower_ratio
        upper_tps = max(level_tps, innodb_tps) * args.hybrid_tps_upper_ratio
        if not (lower_tps <= hybrid_tps <= upper_tps):
            hybrid_ok = False
            reasons.append("hybrid-balanced tps outside leveldb/innodb band")
        else:
            reasons.append("hybrid-balanced tps within leveldb/innodb band")
        if hybrid_p95 > max(level_p95, innodb_p95):
            hybrid_ok = False
            reasons.append("hybrid-balanced p95 worse than both anchors")
        else:
            reasons.append("hybrid-balanced p95 within anchor ceiling")
        if hybrid_flips > args.max_hybrid_flips:
            hybrid_ok = False
            reasons.append("hybrid-balanced mode flips exceed threshold")
        else:
            reasons.append("hybrid-balanced mode flips within threshold")
    if tps_ratio >= args.min_tps_ratio and p95_ratio <= args.max_p95_ratio and hybrid_ok:
        verdict = "ok"

    result = {
        "schema_version": "newdb.pressure_profile_compare.v1",
        "verdict": verdict,
        "thresholds": {
            "min_tps_ratio": args.min_tps_ratio,
            "max_p95_ratio": args.max_p95_ratio,
        },
        "leveldb_like": {
            "summary": args.leveldb_summary,
            "benchmark_profile": level.get("benchmark_profile", ""),
            "runtime_walsync_mode": level.get("runtime_walsync_mode", ""),
            "runtime_pressure_tps_est": level_tps,
            "runtime_pressure_batch_ms_p95": level_p95,
        },
        "innodb_like": {
            "summary": args.innodb_summary,
            "benchmark_profile": innodb.get("benchmark_profile", ""),
            "runtime_walsync_mode": innodb.get("runtime_walsync_mode", ""),
            "runtime_pressure_tps_est": innodb_tps,
            "runtime_pressure_batch_ms_p95": innodb_p95,
        },
        "hybrid_balanced": {
            "summary": args.hybrid_summary if hybrid else "",
            "benchmark_profile": hybrid.get("benchmark_profile", "") if hybrid else "",
            "runtime_walsync_mode": hybrid.get("runtime_walsync_mode", "") if hybrid else "",
            "runtime_pressure_tps_est": float(hybrid.get("runtime_pressure_tps_est", 0.0)) if hybrid else 0.0,
            "runtime_pressure_batch_ms_p95": float(hybrid.get("runtime_pressure_batch_ms_p95", 0.0)) if hybrid else 0.0,
            "runtime_hybrid_mode_switch_count": hybrid_flips,
        },
        "derived": {
            "tps_ratio_leveldb_over_innodb": tps_ratio,
            "p95_ratio_innodb_over_leveldb": p95_ratio,
            "hybrid_tps": hybrid_tps,
            "hybrid_p95": hybrid_p95,
            "hybrid_mode_switch_count": hybrid_flips,
            "reasons": reasons,
        },
    }

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, ensure_ascii=True, indent=2), encoding="utf-8")
    print("PRESSURE_PROFILE_COMPARE " + json.dumps(result["derived"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
