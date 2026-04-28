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
    "vacuum_compact_success_count",
    "vacuum_compact_failure_count",
    "vacuum_compact_bytes_reclaimed",
    "vacuum_compact_last_elapsed_ms",
    "vacuum_queue_depth",
    "vacuum_queue_depth_peak",
    "maintenance_checkpoint_trigger_count",
    "maintenance_checkpoint_vacuum_enqueue_count",
    "write_conflicts",
    "lock_wait_ms_total",
    "lock_wait_max_ms",
    "lock_deadlock_detect_count",
    "lock_deadlock_victim_count",
    "txn_begin_lock_conflicts",
    "wal_compact_count",
    "wal_recovery_runs",
    "wal_recovery_undo_ops",
    "wal_recovery_last_elapsed_ms",
    "wal_recovery_analyze_ms",
    "wal_recovery_undo_ms",
    "wal_recovery_finalize_ms",
    "wal_recovery_records_scanned",
    "wal_recovery_dangling_txns",
    "wal_group_commit_count",
    "wal_group_commit_batch_commits",
    "wal_group_commit_pending_commits",
    "txn_commit_count",
    "txn_commit_p95_ms",
    "txn_commit_max_ms",
    "wal_bytes_since_start",
    "wal_bytes_per_commit_avg",
    "lock_wait_p95_ms",
    "scheduler_throttle_count",
    "hot_index_enabled",
    "segment_target_bytes",
    "lsm_memtable_flush_count",
    "lsm_compaction_count",
    "lsm_segment_count",
    "lsm_memtable_bytes",
    "lsm_read_segments_scanned",
    "lsm_read_segments_scanned_p95",
    "lsm_compaction_bytes_in",
    "lsm_compaction_bytes_out",
    "lsm_compaction_queue_pending",
    "lsm_compaction_queue_inflight",
    "lsm_compaction_enqueue_skipped_backpressure",
    "lsm_segment_cache_hits",
    "lsm_segment_cache_misses",
    "rollback_savepoint_count",
    "rollback_partial_ops",
    "pitr_runs",
    "pitr_target_lsn",
    "pitr_elapsed_ms",
    "undo_chain_fallback_count",
    "where_query_cache_lookups",
    "where_query_cache_hits",
    "where_policy_checks",
    "where_policy_rejects",
    "where_fallback_scans",
    "where_plan_eq_sidecar_count",
    "where_plan_id_pk_count",
    "where_plan_fallback_count",
    "wal_adaptive_enabled",
    "group_commit_window_ms",
    "group_commit_max_batch_commits",
)
ALLOWED_WALSYNC = {"off", "normal", "full"}
LEGACY_DEFAULT_STATS = {
    "walsync": "normal",
    "normal_interval_ms": 0,
    "autovacuum": True,
    "maintenance_checkpoint_trigger_count": 0,
    "maintenance_checkpoint_vacuum_enqueue_count": 0,
    "txn_commit_max_ms": 0,
    "where_query_cache_lookups": 0,
    "where_query_cache_hits": 0,
    "where_policy_checks": 0,
    "where_policy_rejects": 0,
    "where_fallback_scans": 0,
    "where_plan_eq_sidecar_count": 0,
    "where_plan_id_pk_count": 0,
    "where_plan_fallback_count": 0,
    "rollback_savepoint_count": 0,
    "rollback_partial_ops": 0,
    "pitr_runs": 0,
    "pitr_target_lsn": 0,
    "pitr_elapsed_ms": 0,
    "undo_chain_fallback_count": 0,
}


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
    normalized_stats = dict(LEGACY_DEFAULT_STATS)
    normalized_stats.update(stats)
    for key in REQUIRED_STATS:
        if key not in normalized_stats:
            return fail(f"line {idx}: missing stats key `{key}`")

    if normalized_stats["walsync"] not in ALLOWED_WALSYNC:
        return fail(f"line {idx}: invalid walsync `{normalized_stats['walsync']}`")
    if not _is_non_negative_int(normalized_stats["normal_interval_ms"]):
        return fail(f"line {idx}: normal_interval_ms must be non-negative int")
    if not isinstance(normalized_stats["autovacuum"], bool):
        return fail(f"line {idx}: autovacuum must be bool")
    if not isinstance(normalized_stats["hot_index_enabled"], bool):
        return fail(f"line {idx}: hot_index_enabled must be bool")
    if not isinstance(normalized_stats["wal_adaptive_enabled"], bool):
        return fail(f"line {idx}: wal_adaptive_enabled must be bool")

    for key in (
        "vacuum_ops_threshold",
        "vacuum_min_interval_sec",
        "vacuum_trigger_count",
        "vacuum_execute_count",
        "vacuum_cooldown_skip_count",
        "vacuum_compact_success_count",
        "vacuum_compact_failure_count",
        "vacuum_compact_bytes_reclaimed",
        "vacuum_compact_last_elapsed_ms",
        "vacuum_queue_depth",
        "vacuum_queue_depth_peak",
        "maintenance_checkpoint_trigger_count",
        "maintenance_checkpoint_vacuum_enqueue_count",
        "write_conflicts",
        "lock_wait_ms_total",
        "lock_wait_max_ms",
        "lock_deadlock_detect_count",
        "lock_deadlock_victim_count",
        "txn_begin_lock_conflicts",
        "wal_compact_count",
        "wal_recovery_runs",
        "wal_recovery_undo_ops",
        "wal_recovery_last_elapsed_ms",
        "wal_recovery_analyze_ms",
        "wal_recovery_undo_ms",
        "wal_recovery_finalize_ms",
        "wal_recovery_records_scanned",
        "wal_recovery_dangling_txns",
        "wal_group_commit_count",
        "wal_group_commit_batch_commits",
        "wal_group_commit_pending_commits",
        "txn_commit_count",
        "txn_commit_p95_ms",
        "txn_commit_max_ms",
        "wal_bytes_since_start",
        "wal_bytes_per_commit_avg",
        "lock_wait_p95_ms",
        "scheduler_throttle_count",
        "segment_target_bytes",
        "lsm_memtable_flush_count",
        "lsm_compaction_count",
        "lsm_segment_count",
        "lsm_memtable_bytes",
        "lsm_read_segments_scanned",
        "lsm_read_segments_scanned_p95",
        "lsm_compaction_bytes_in",
        "lsm_compaction_bytes_out",
        "lsm_compaction_queue_pending",
        "lsm_compaction_queue_inflight",
        "lsm_compaction_enqueue_skipped_backpressure",
        "lsm_segment_cache_hits",
        "lsm_segment_cache_misses",
        "rollback_savepoint_count",
        "rollback_partial_ops",
        "pitr_runs",
        "pitr_target_lsn",
        "pitr_elapsed_ms",
        "undo_chain_fallback_count",
        "where_query_cache_lookups",
        "where_query_cache_hits",
        "where_policy_checks",
        "where_policy_rejects",
        "where_fallback_scans",
        "where_plan_eq_sidecar_count",
        "where_plan_id_pk_count",
        "where_plan_fallback_count",
        "group_commit_window_ms",
        "group_commit_max_batch_commits",
    ):
        if not _is_non_negative_int(normalized_stats[key]):
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

