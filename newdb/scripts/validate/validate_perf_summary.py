#!/usr/bin/env python3
"""Validate newdb perf summary JSON for observability pipelines."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REQUIRED_TOP_LEVEL = ("timestamp", "data_dir", "table", "schema_version", "perf")
REQUIRED_PERF_KEYS = (
    "txn_normal_avg_ms",
    "query_avg_ms_max",
    "where_policy_rejects",
    "where_policy_fallbacks",
)


def _fail(msg: str) -> int:
    print(f"PERF_SUMMARY_INVALID: {msg}", file=sys.stderr)
    return 2


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("summary_path", help="Path to test_loop summary JSON")
    args = p.parse_args()

    path = Path(args.summary_path)
    if not path.is_file():
        return _fail(f"summary file not found: {path}")

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # pragma: no cover
        return _fail(f"invalid JSON: {exc}")

    for key in REQUIRED_TOP_LEVEL:
        if key not in data:
            return _fail(f"missing top-level key: {key}")

    if data.get("schema_version") != "newdb.perf_summary.v1":
        return _fail("schema_version must be newdb.perf_summary.v1")

    perf = data.get("perf")
    if not isinstance(perf, dict):
        return _fail("perf must be an object")

    for key in REQUIRED_PERF_KEYS:
        if key not in perf:
            return _fail(f"missing perf key: {key}")

    rejects = perf.get("where_policy_rejects")
    fallbacks = perf.get("where_policy_fallbacks")
    if not isinstance(rejects, int) or rejects < 0:
        return _fail("where_policy_rejects must be non-negative int")
    if not isinstance(fallbacks, int) or fallbacks < 0:
        return _fail("where_policy_fallbacks must be non-negative int")

    print(f"PERF_SUMMARY_VALID: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

