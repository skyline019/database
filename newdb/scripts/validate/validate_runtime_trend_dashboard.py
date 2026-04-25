#!/usr/bin/env python3
"""Validate runtime trend dashboard JSON contract."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def fail(msg: str) -> int:
    print(f"RUNTIME_TREND_DASHBOARD_INVALID: {msg}", file=sys.stderr)
    return 2


def _is_num_or_none(v: object) -> bool:
    return v is None or isinstance(v, (int, float))


def _is_str_or_none(v: object) -> bool:
    return v is None or isinstance(v, str)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("dashboard_json_path")
    args = p.parse_args()

    path = Path(args.dashboard_json_path)
    if not path.is_file():
        return fail(f"file not found: {path}")

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return fail(f"invalid json: {exc}")

    if not isinstance(data, dict):
        return fail("top level must be object")
    if data.get("schema_version") != "newdb.runtime_trend_dashboard.v1":
        return fail("schema_version must be newdb.runtime_trend_dashboard.v1")
    if not isinstance(data.get("generated_at"), str) or not data["generated_at"]:
        return fail("generated_at must be non-empty string")

    for key in ("sources", "overview", "nightly_status", "runtime_metrics"):
        if not isinstance(data.get(key), dict):
            return fail(f"{key} must be object")
    if not isinstance(data.get("recent_runs"), list):
        return fail("recent_runs must be array")

    ns = data["nightly_status"]
    for k in ("total", "passed", "failed"):
        if not isinstance(ns.get(k), int) or ns[k] < 0:
            return fail(f"nightly_status.{k} must be non-negative int")
    if not _is_num_or_none(ns.get("pass_rate")):
        return fail("nightly_status.pass_rate must be number or null")

    rm = data["runtime_metrics"]
    for series_name in (
        "vacuum_efficiency_p50",
        "conflict_rate_p95",
        "txn_begin_lock_conflict_delta",
        "wal_compact_delta",
    ):
        s = rm.get(series_name)
        if not isinstance(s, dict):
            return fail(f"runtime_metrics.{series_name} must be object")
        if not isinstance(s.get("count"), int) or s["count"] < 0:
            return fail(f"runtime_metrics.{series_name}.count must be non-negative int")
        for k in ("min", "max", "avg"):
            if not _is_num_or_none(s.get(k)):
                return fail(f"runtime_metrics.{series_name}.{k} must be number or null")

    for idx, row in enumerate(data["recent_runs"], start=1):
        if not isinstance(row, dict):
            return fail(f"recent_runs[{idx}] must be object")
        source = row.get("source")
        if source not in ("test_loop", "nightly"):
            return fail(f"recent_runs[{idx}].source must be test_loop|nightly")
        if not _is_str_or_none(row.get("timestamp")):
            return fail(f"recent_runs[{idx}].timestamp must be string or null")
        if not _is_str_or_none(row.get("runtime_run_id")):
            return fail(f"recent_runs[{idx}].runtime_run_id must be string or null")
        if not _is_str_or_none(row.get("status")):
            return fail(f"recent_runs[{idx}].status must be string or null")
        for field in (
            "runtime_vacuum_efficiency_p50",
            "runtime_conflict_rate_p95",
            "runtime_txn_begin_lock_conflict_delta",
            "runtime_wal_compact_delta",
        ):
            if not _is_num_or_none(row.get(field)):
                return fail(f"recent_runs[{idx}].{field} must be number or null")

    print(f"RUNTIME_TREND_DASHBOARD_VALID: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

