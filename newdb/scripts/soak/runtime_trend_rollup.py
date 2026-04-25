#!/usr/bin/env python3
"""Build a dashboard-friendly runtime trend rollup from soak trend JSONL files."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.is_file():
        return rows
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if isinstance(obj, dict):
            rows.append(obj)
    return rows


def _series(values: list[float]) -> dict[str, Any]:
    if not values:
        return {"count": 0, "min": None, "max": None, "avg": None}
    return {
        "count": len(values),
        "min": min(values),
        "max": max(values),
        "avg": (sum(values) / float(len(values))),
    }


def _to_float(x: Any) -> float | None:
    if isinstance(x, bool):
        return None
    if isinstance(x, (int, float)):
        return float(x)
    return None


def _parse_ts(ts: Any) -> datetime | None:
    if not isinstance(ts, str) or not ts.strip():
        return None
    text = ts.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except Exception:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _recent_entries(test_rows: list[dict[str, Any]], nightly_rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    for row in test_rows:
        merged.append(
            {
                "source": "test_loop",
                "timestamp": row.get("timestamp"),
                "runtime_run_id": row.get("runtime_run_id"),
                "runtime_vacuum_efficiency_p50": _to_float(row.get("runtime_vacuum_efficiency_p50")),
                "runtime_conflict_rate_p95": _to_float(row.get("runtime_conflict_rate_p95")),
                "runtime_txn_begin_lock_conflict_delta": _to_float(row.get("runtime_txn_begin_lock_conflict_delta")),
                "runtime_wal_compact_delta": _to_float(row.get("runtime_wal_compact_delta")),
                "status": None,
            }
        )
    for row in nightly_rows:
        merged.append(
            {
                "source": "nightly",
                "timestamp": row.get("timestamp"),
                "runtime_run_id": row.get("runtime_run_id"),
                "runtime_vacuum_efficiency_p50": _to_float(row.get("runtime_vacuum_efficiency_p50")),
                "runtime_conflict_rate_p95": _to_float(row.get("runtime_conflict_rate_p95")),
                "runtime_txn_begin_lock_conflict_delta": _to_float(row.get("runtime_txn_begin_lock_conflict_delta")),
                "runtime_wal_compact_delta": _to_float(row.get("runtime_wal_compact_delta")),
                "status": row.get("status"),
            }
        )
    if limit < 1:
        return []
    merged.sort(key=lambda x: (_parse_ts(x.get("timestamp")) is None, _parse_ts(x.get("timestamp")) or datetime.min.replace(tzinfo=timezone.utc)))
    return merged[-limit:]


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--test-loop-trend", required=True)
    p.add_argument("--nightly-trend", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--recent-limit", type=int, default=30)
    p.add_argument("--require-nightly-samples", action="store_true")
    p.add_argument("--max-latest-nightly-age-hours", type=float, default=-1.0)
    args = p.parse_args()

    test_path = Path(args.test_loop_trend)
    nightly_path = Path(args.nightly_trend)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    test_rows = _load_jsonl(test_path)
    nightly_rows = _load_jsonl(nightly_path)

    vac_p50: list[float] = []
    conf_p95: list[float] = []
    begin_lock_delta: list[float] = []
    wal_compact_delta: list[float] = []
    for row in (test_rows + nightly_rows):
        for src, bucket in (
            ("runtime_vacuum_efficiency_p50", vac_p50),
            ("runtime_conflict_rate_p95", conf_p95),
            ("runtime_txn_begin_lock_conflict_delta", begin_lock_delta),
            ("runtime_wal_compact_delta", wal_compact_delta),
        ):
            val = _to_float(row.get(src))
            if val is not None:
                bucket.append(val)

    nightly_total = len(nightly_rows)
    nightly_passed = sum(1 for r in nightly_rows if r.get("status") == "passed")
    nightly_failed = sum(1 for r in nightly_rows if r.get("status") == "failed")
    pass_rate = (float(nightly_passed) / float(nightly_total)) if nightly_total > 0 else None
    latest_nightly_dt = _parse_ts(nightly_rows[-1].get("timestamp")) if nightly_rows else None
    nightly_age_hours = None
    if latest_nightly_dt is not None:
        nightly_age_hours = (datetime.now(timezone.utc) - latest_nightly_dt).total_seconds() / 3600.0

    dashboard = {
        "schema_version": "newdb.runtime_trend_dashboard.v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources": {
            "test_loop_trend_path": str(test_path),
            "nightly_soak_trend_path": str(nightly_path),
            "test_loop_rows": len(test_rows),
            "nightly_rows": nightly_total,
        },
        "overview": {
            "total_rows": len(test_rows) + nightly_total,
            "latest_test_loop_timestamp": test_rows[-1].get("timestamp") if test_rows else None,
            "latest_nightly_timestamp": nightly_rows[-1].get("timestamp") if nightly_rows else None,
            "latest_runtime_run_id": (
                (nightly_rows[-1].get("runtime_run_id") if nightly_rows and nightly_rows[-1].get("runtime_run_id") else None)
                or (test_rows[-1].get("runtime_run_id") if test_rows else None)
            ),
        },
        "nightly_status": {
            "total": nightly_total,
            "passed": nightly_passed,
            "failed": nightly_failed,
            "pass_rate": pass_rate,
        },
        "data_quality": {
            "has_nightly_samples": nightly_total > 0,
            "latest_nightly_age_hours": nightly_age_hours,
        },
        "runtime_metrics": {
            "vacuum_efficiency_p50": _series(vac_p50),
            "conflict_rate_p95": _series(conf_p95),
            "txn_begin_lock_conflict_delta": _series(begin_lock_delta),
            "wal_compact_delta": _series(wal_compact_delta),
        },
        "recent_runs": _recent_entries(test_rows, nightly_rows, args.recent_limit),
    }

    out_path.write_text(json.dumps(dashboard, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"RUNTIME_TREND_DASHBOARD_WRITTEN: {out_path}")
    if args.require_nightly_samples and nightly_total == 0:
        print("RUNTIME_TREND_QUALITY_GATE_FAILED: nightly samples required but none found")
        return 4
    if args.max_latest_nightly_age_hours >= 0.0 and nightly_age_hours is not None:
        if nightly_age_hours > args.max_latest_nightly_age_hours:
            print(
                "RUNTIME_TREND_QUALITY_GATE_FAILED: "
                f"latest nightly age {nightly_age_hours:.3f}h exceeds "
                f"threshold {args.max_latest_nightly_age_hours:.3f}h"
            )
            return 5
    if args.max_latest_nightly_age_hours >= 0.0 and nightly_age_hours is None:
        print("RUNTIME_TREND_QUALITY_GATE_FAILED: latest nightly age unavailable")
        return 6
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

