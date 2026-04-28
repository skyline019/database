#!/usr/bin/env python3
"""Build a dashboard-friendly runtime trend rollup from soak trend JSONL files."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


_HEALTH_ORDER = {"healthy": 0, "warning": 1, "critical": 2}


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
                "txn_normal_avg_ms": _to_float(row.get("txn_normal_avg_ms")),
                "query_avg_ms_max": _to_float(row.get("query_avg_ms_max")),
                "cm_tps_min": _to_float(row.get("cm_tps_min")),
                "hp_max_query_avg_ms": _to_float(row.get("hp_max_query_avg_ms")),
                "status": None,
                "dashboard_quality_gate_status": None,
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
                "txn_normal_avg_ms": _to_float(row.get("txn_normal_avg_ms")),
                "query_avg_ms_max": _to_float(row.get("query_avg_ms_max")),
                "cm_tps_min": _to_float(row.get("cm_tps_min")),
                "hp_max_query_avg_ms": _to_float(row.get("hp_max_query_avg_ms")),
                "status": row.get("status"),
                "dashboard_quality_gate_status": row.get("dashboard_quality_gate_status"),
            }
        )
    if limit < 1:
        return []
    merged.sort(key=lambda x: (_parse_ts(x.get("timestamp")) is None, _parse_ts(x.get("timestamp")) or datetime.min.replace(tzinfo=timezone.utc)))
    return merged[-limit:]


def _latest_metric(rows: list[dict[str, Any]], key: str) -> float | None:
    for row in reversed(rows):
        val = _to_float(row.get(key))
        if val is not None:
            return val
    return None


def _health_tier_from_reasons(warning_reasons: list[str], critical_reasons: list[str]) -> str:
    if critical_reasons:
        return "critical"
    if warning_reasons:
        return "warning"
    return "healthy"


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--test-loop-trend", required=True)
    p.add_argument("--nightly-trend", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--recent-limit", type=int, default=30)
    p.add_argument("--require-nightly-samples", action="store_true")
    p.add_argument("--max-latest-nightly-age-hours", type=float, default=-1.0)
    p.add_argument("--warn-max-query-avg-ms", type=float, default=100.0)
    p.add_argument("--critical-max-query-avg-ms", type=float, default=300.0)
    p.add_argument("--warn-min-cm-tps", type=float, default=25000.0)
    p.add_argument("--critical-min-cm-tps", type=float, default=15000.0)
    p.add_argument("--warn-min-nightly-pass-rate", type=float, default=0.90)
    p.add_argument("--critical-min-nightly-pass-rate", type=float, default=0.70)
    p.add_argument("--max-health-tier", choices=("healthy", "warning", "critical"), default="critical")
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
    txn_normal_avg_ms: list[float] = []
    query_avg_ms_max: list[float] = []
    cm_tps_min: list[float] = []
    hp_max_query_avg_ms: list[float] = []
    for row in (test_rows + nightly_rows):
        for src, bucket in (
            ("runtime_vacuum_efficiency_p50", vac_p50),
            ("runtime_conflict_rate_p95", conf_p95),
            ("runtime_txn_begin_lock_conflict_delta", begin_lock_delta),
            ("runtime_wal_compact_delta", wal_compact_delta),
            ("txn_normal_avg_ms", txn_normal_avg_ms),
            ("query_avg_ms_max", query_avg_ms_max),
            ("cm_tps_min", cm_tps_min),
            ("hp_max_query_avg_ms", hp_max_query_avg_ms),
        ):
            val = _to_float(row.get(src))
            if val is not None:
                bucket.append(val)

    nightly_total = len(nightly_rows)
    nightly_passed = sum(1 for r in nightly_rows if r.get("status") == "passed")
    nightly_failed = sum(1 for r in nightly_rows if r.get("status") == "failed")
    pass_rate = (float(nightly_passed) / float(nightly_total)) if nightly_total > 0 else None
    dashboard_gate_failed_count = sum(
        1 for r in nightly_rows if str(r.get("dashboard_quality_gate_status", "")).strip().lower() == "failed"
    )
    dashboard_gate_passed_count = sum(
        1 for r in nightly_rows if str(r.get("dashboard_quality_gate_status", "")).strip().lower() == "passed"
    )
    latest_nightly_dt = _parse_ts(nightly_rows[-1].get("timestamp")) if nightly_rows else None
    nightly_age_hours = None
    if latest_nightly_dt is not None:
        nightly_age_hours = (datetime.now(timezone.utc) - latest_nightly_dt).total_seconds() / 3600.0

    latest_query_avg_ms = _latest_metric(test_rows + nightly_rows, "query_avg_ms_max")
    latest_cm_tps_min = _latest_metric(test_rows + nightly_rows, "cm_tps_min")
    latest_hp_max_query_avg_ms = _latest_metric(test_rows + nightly_rows, "hp_max_query_avg_ms")
    latest_txn_normal_avg_ms = _latest_metric(test_rows + nightly_rows, "txn_normal_avg_ms")

    warning_reasons: list[str] = []
    critical_reasons: list[str] = []
    if pass_rate is not None:
        if pass_rate < args.critical_min_nightly_pass_rate:
            critical_reasons.append(
                f"nightly_pass_rate={pass_rate:.4f} < critical_min={args.critical_min_nightly_pass_rate:.4f}"
            )
        elif pass_rate < args.warn_min_nightly_pass_rate:
            warning_reasons.append(f"nightly_pass_rate={pass_rate:.4f} < warn_min={args.warn_min_nightly_pass_rate:.4f}")
    if latest_query_avg_ms is not None:
        if latest_query_avg_ms > args.critical_max_query_avg_ms:
            critical_reasons.append(
                f"latest_query_avg_ms={latest_query_avg_ms:.3f} > critical_max={args.critical_max_query_avg_ms:.3f}"
            )
        elif latest_query_avg_ms > args.warn_max_query_avg_ms:
            warning_reasons.append(f"latest_query_avg_ms={latest_query_avg_ms:.3f} > warn_max={args.warn_max_query_avg_ms:.3f}")
    if latest_cm_tps_min is not None:
        if latest_cm_tps_min < args.critical_min_cm_tps:
            critical_reasons.append(f"latest_cm_tps_min={latest_cm_tps_min:.3f} < critical_min={args.critical_min_cm_tps:.3f}")
        elif latest_cm_tps_min < args.warn_min_cm_tps:
            warning_reasons.append(f"latest_cm_tps_min={latest_cm_tps_min:.3f} < warn_min={args.warn_min_cm_tps:.3f}")
    if nightly_age_hours is not None and args.max_latest_nightly_age_hours >= 0.0 and nightly_age_hours > args.max_latest_nightly_age_hours:
        warning_reasons.append(
            f"latest_nightly_age_hours={nightly_age_hours:.3f} > max={args.max_latest_nightly_age_hours:.3f}"
        )

    health_tier = _health_tier_from_reasons(warning_reasons, critical_reasons)
    health_reasons = critical_reasons if critical_reasons else warning_reasons

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
        "secondary_metrics": {
            "dashboard_quality_gate_passed_count": dashboard_gate_passed_count,
            "dashboard_quality_gate_failed_count": dashboard_gate_failed_count,
        },
        "runtime_metrics": {
            "vacuum_efficiency_p50": _series(vac_p50),
            "conflict_rate_p95": _series(conf_p95),
            "txn_begin_lock_conflict_delta": _series(begin_lock_delta),
            "wal_compact_delta": _series(wal_compact_delta),
        },
        "perf_metrics": {
            "txn_normal_avg_ms": _series(txn_normal_avg_ms),
            "query_avg_ms_max": _series(query_avg_ms_max),
            "cm_tps_min": _series(cm_tps_min),
            "hp_max_query_avg_ms": _series(hp_max_query_avg_ms),
        },
        "health": {
            "tier": health_tier,
            "reasons": health_reasons,
            "latest_query_avg_ms": latest_query_avg_ms,
            "latest_cm_tps_min": latest_cm_tps_min,
            "latest_hp_max_query_avg_ms": latest_hp_max_query_avg_ms,
            "latest_txn_normal_avg_ms": latest_txn_normal_avg_ms,
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
    if _HEALTH_ORDER[health_tier] > _HEALTH_ORDER[args.max_health_tier]:
        print(
            "RUNTIME_TREND_QUALITY_GATE_FAILED: "
            f"health_tier={health_tier} exceeds allowed={args.max_health_tier}; "
            f"reasons={' | '.join(health_reasons) if health_reasons else 'none'}"
        )
        return 7
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

