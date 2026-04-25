#!/usr/bin/env python3
"""Validate runtime stats JSONL contract for newdb runtime gates."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REQUIRED_TOP = ("schema_version", "ts_ms", "label", "stats")
REQUIRED_STATS = (
    "walsync",
    "normal_interval_ms",
    "autovacuum",
    "vacuum_ops_threshold",
    "vacuum_min_interval_sec",
    "vacuum_trigger_count",
    "vacuum_execute_count",
    "vacuum_cooldown_skip_count",
    "write_conflicts",
    "txn_begin_lock_conflicts",
    "wal_compact_count",
)
ALLOWED_WALSYNC = {"off", "normal", "full"}


def fail(msg: str) -> int:
    print(f"RUNTIME_STATS_INVALID: {msg}", file=sys.stderr)
    return 2


def _is_non_negative_int(val: object) -> bool:
    return isinstance(val, int) and val >= 0


def validate_row(obj: dict, idx: int) -> int:
    for key in REQUIRED_TOP:
        if key not in obj:
            return fail(f"line {idx}: missing key `{key}`")

    if obj["schema_version"] != "newdb.runtime_stats.v1":
        return fail(f"line {idx}: unsupported schema_version `{obj['schema_version']}`")
    if not _is_non_negative_int(obj["ts_ms"]):
        return fail(f"line {idx}: ts_ms must be non-negative int")
    if not isinstance(obj["label"], str) or not obj["label"]:
        return fail(f"line {idx}: label must be non-empty string")

    stats = obj["stats"]
    if not isinstance(stats, dict):
        return fail(f"line {idx}: stats must be object")
    for key in REQUIRED_STATS:
        if key not in stats:
            return fail(f"line {idx}: missing stats key `{key}`")

    if stats["walsync"] not in ALLOWED_WALSYNC:
        return fail(f"line {idx}: invalid walsync `{stats['walsync']}`")
    if not _is_non_negative_int(stats["normal_interval_ms"]):
        return fail(f"line {idx}: normal_interval_ms must be non-negative int")
    if not isinstance(stats["autovacuum"], bool):
        return fail(f"line {idx}: autovacuum must be bool")

    for key in (
        "vacuum_ops_threshold",
        "vacuum_min_interval_sec",
        "vacuum_trigger_count",
        "vacuum_execute_count",
        "vacuum_cooldown_skip_count",
        "write_conflicts",
        "txn_begin_lock_conflicts",
        "wal_compact_count",
    ):
        if not _is_non_negative_int(stats[key]):
            return fail(f"line {idx}: `{key}` must be non-negative int")

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
            return fail(f"line {idx}: row must be JSON object")
        rc = validate_row(obj, idx)
        if rc != 0:
            return rc
        count += 1

    if count == 0:
        return fail("no runtime rows found")
    print(f"RUNTIME_STATS_VALID: {path} rows={count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

