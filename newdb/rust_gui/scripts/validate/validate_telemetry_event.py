#!/usr/bin/env python3
"""Validate telemetry event JSONL contract for newdb perf pipelines."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REQUIRED_KEYS = (
    "schema_version",
    "timestamp",
    "run_id",
    "source",
    "env",
    "build",
    "profile",
    "phase",
    "metrics",
)
ALLOWED_SOURCES = {"test_loop", "nightly_soak_runner"}
ALLOWED_PHASES = {
    "prep",
    "txn_bench",
    "query_bench",
    "eq_cache_bench",
    "concurrent_pressure",
    "high_pressure",
    "concurrent_million",
    "soak",
    "summary",
}


def fail(msg: str) -> int:
    print(f"TELEMETRY_INVALID: {msg}", file=sys.stderr)
    return 2


def validate_event(obj: dict, idx: int) -> int:
    for key in REQUIRED_KEYS:
        if key not in obj:
            return fail(f"line {idx}: missing key `{key}`")
    if obj["schema_version"] != "newdb.telemetry.v1":
        return fail(f"line {idx}: unsupported schema_version `{obj['schema_version']}`")
    if obj["source"] not in ALLOWED_SOURCES:
        return fail(f"line {idx}: unsupported source `{obj['source']}`")
    for dim_key in ("env", "build", "profile"):
        val = obj.get(dim_key)
        if not isinstance(val, str) or not val.strip():
            return fail(f"line {idx}: `{dim_key}` must be non-empty string")
    if obj["phase"] not in ALLOWED_PHASES:
        return fail(f"line {idx}: unsupported phase `{obj['phase']}`")
    if not isinstance(obj["metrics"], dict):
        return fail(f"line {idx}: metrics must be an object")
    return 0


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("jsonl_path")
    args = p.parse_args()
    path = Path(args.jsonl_path)
    if not path.is_file():
        return fail(f"file not found: {path}")

    count = 0
    for idx, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except Exception as exc:
            return fail(f"line {idx}: invalid json: {exc}")
        if not isinstance(obj, dict):
            return fail(f"line {idx}: event must be JSON object")
        rc = validate_event(obj, idx)
        if rc != 0:
            return rc
        count += 1
    if count == 0:
        return fail("no telemetry events found")
    print(f"TELEMETRY_VALID: {path} events={count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

