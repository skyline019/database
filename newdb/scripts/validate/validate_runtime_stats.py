#!/usr/bin/env python3
"""Validate runtime stats JSONL contract for newdb runtime gates."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_CONTRACT_PATH = Path(__file__).resolve().parent / "contract" / "runtime_stats.v1.required.json"


def _load_runtime_stats_contract() -> tuple[tuple[str, ...], tuple[str, ...], str, tuple[str, ...], tuple[str, ...]]:
    try:
        with _CONTRACT_PATH.open(encoding="utf-8") as f:
            doc = json.load(f)
    except OSError as e:
        raise RuntimeError(f"runtime stats contract missing: {_CONTRACT_PATH}") from e
    top = doc.get("required_top_level_keys")
    stats = doc.get("required_stats_keys")
    ver = doc.get("expected_schema_version")
    embed = doc.get("stats_keys_cli_embed_layer")
    if not isinstance(top, list) or not all(isinstance(x, str) for x in top):
        raise RuntimeError(f"contract invalid required_top_level_keys: {_CONTRACT_PATH}")
    if not isinstance(stats, list) or not all(isinstance(x, str) for x in stats):
        raise RuntimeError(f"contract invalid required_stats_keys: {_CONTRACT_PATH}")
    if not isinstance(ver, str) or not ver:
        raise RuntimeError(f"contract invalid expected_schema_version: {_CONTRACT_PATH}")
    if not isinstance(embed, list) or not all(isinstance(x, str) for x in embed):
        raise RuntimeError(f"contract invalid stats_keys_cli_embed_layer: {_CONTRACT_PATH}")
    stats_t = tuple(stats)
    embed_t = tuple(embed)
    full_set = set(stats_t)
    embed_set = set(embed_t)
    if not embed_set.issubset(full_set):
        raise RuntimeError("stats_keys_cli_embed_layer must be a subset of required_stats_keys")
    engine_t = tuple(k for k in stats_t if k not in embed_set)
    return (tuple(top), stats_t, ver, embed_t, engine_t)


REQUIRED_TOP, REQUIRED_STATS, EXPECTED_SCHEMA_VERSION, STATS_KEYS_CLI_EMBED, STATS_KEYS_ENGINE = (
    _load_runtime_stats_contract()
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
    "where_heap_scan_budget_binding_events": 0,
    "where_fallback_scans": 0,
    "where_plan_eq_sidecar_count": 0,
    "where_plan_id_pk_count": 0,
    "where_plan_fallback_count": 0,
    "where_query_count": 0,
    "where_query_rows_scanned_total": 0,
    "where_query_rows_returned_total": 0,
    "rollback_savepoint_count": 0,
    "rollback_partial_ops": 0,
    "pitr_runs": 0,
    "pitr_target_lsn": 0,
    "pitr_elapsed_ms": 0,
    "undo_chain_fallback_count": 0,
    "lazy_materialize_count": 0,
    "lazy_materialize_rows_total": 0,
    "lazy_materialize_max_rows": 0,
    "lazy_materialize_elapsed_ms": 0,
    "heap_decode_slot_calls": 0,
    "heap_decode_slot_hits": 0,
    "heap_decode_slot_misses": 0,
    "vacuum_priority_score": 0,
    "vacuum_health_bonus_last": 0,
    "vacuum_score_file_bytes_term": 0,
    "vacuum_score_health_bonus_term": 0,
    "vacuum_score_wal_since_term": 0,
    "compact_debt_bytes": 0,
    "compact_debt_rows": 0,
    "compact_debt_ratio": 0.0,
    "compact_debt_priority": 0,
    "page_cache_hits": 0,
    "page_cache_misses": 0,
    "page_cache_evictions": 0,
    "page_cache_bytes_in_cache": 0,
    "memory_budget_max_bytes": 0,
    "memory_budget_used_bytes": 0,
    "memory_budget_reject_count": 0,
    "memory_budget_bytes_evicted_total": 0,
    "memory_budget_sidecar_load_skipped_total": 0,
    "table_storage_health_logical_rows": 0,
    "table_storage_health_physical_rows": 0,
    "table_storage_health_tombstone_rows": 0,
    "table_storage_health_data_file_bytes": 0,
    "table_storage_health_live_bytes": 0,
    "table_storage_health_dead_bytes": 0,
    "table_storage_health_fragmentation_ratio": 0.0,
    "table_storage_health_last_vacuum_lsn": 0,
    "table_storage_health_last_vacuum_elapsed_ms": 0,
    "table_storage_health_tier": "good",
    "transaction_snapshot_lsn": 0,
    "statement_snapshot_lsn": 0,
    "wal_recovery_redo_ms": 0,
    "wal_recovery_checkpoint_begin_count": 0,
    "wal_recovery_checkpoint_end_count": 0,
    "wal_recovery_policy": "",
    "write_conflict_last_sample": "",
    "file_lock_acquire_fail_count": 0,
    "file_lock_same_process_reuse_count": 0,
    "file_lock_stale_marker_count": 0,
    "sidecar_invalidate_count": 0,
    "sidecar_invalidate_fail_count": 0,
    "where_eq_sidecar_disk_bytes_read_total": 0,
    "where_eq_sidecar_disk_loads": 0,
    "mem_page_cache_used_bytes": 0,
    "mem_page_cache_evictions": 0,
    "mem_page_cache_admit_rejects": 0,
    "mem_sidecar_used_bytes": 0,
    "mem_sidecar_evictions": 0,
    "mem_sidecar_admit_rejects": 0,
    "mem_query_temp_used_bytes": 0,
    "mem_query_temp_evictions": 0,
    "mem_query_temp_admit_rejects": 0,
    "mem_global_used_bytes": 0,
    "mem_global_admit_rejects": 0,
    # LSM queue / cache counters (required schema; often omitted in older JSONL)
    "lsm_compaction_queue_pending": 0,
    "lsm_compaction_queue_inflight": 0,
    "lsm_compaction_enqueue_skipped_backpressure": 0,
    "lsm_segment_cache_hits": 0,
    "lsm_segment_cache_misses": 0,
}

for _req_key in REQUIRED_STATS:
    if _req_key in LEGACY_DEFAULT_STATS:
        continue
    if _req_key == "walsync":
        LEGACY_DEFAULT_STATS[_req_key] = "normal"
    elif _req_key in ("wal_recovery_policy", "write_conflict_last_sample", "last_snapshot_source"):
        LEGACY_DEFAULT_STATS[_req_key] = ""
    elif _req_key == "table_storage_health_tier":
        LEGACY_DEFAULT_STATS[_req_key] = "good"
    elif _req_key in ("table_storage_health_fragmentation_ratio", "compact_debt_ratio"):
        LEGACY_DEFAULT_STATS[_req_key] = 0.0
    elif _req_key == "autovacuum":
        LEGACY_DEFAULT_STATS[_req_key] = True
    elif _req_key == "hot_index_enabled":
        LEGACY_DEFAULT_STATS[_req_key] = True
    elif _req_key == "wal_adaptive_enabled":
        LEGACY_DEFAULT_STATS[_req_key] = False
    else:
        LEGACY_DEFAULT_STATS[_req_key] = 0

_NON_NEGATIVE_INT_STATS_KEYS: tuple[str, ...] = (
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
    "wal_recovery_redo_ms",
    "wal_recovery_checkpoint_begin_count",
    "wal_recovery_checkpoint_end_count",
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
    "lazy_materialize_count",
    "lazy_materialize_rows_total",
    "lazy_materialize_max_rows",
    "lazy_materialize_elapsed_ms",
    "where_query_cache_lookups",
    "where_query_cache_hits",
    "where_policy_checks",
    "where_policy_rejects",
    "where_heap_scan_budget_binding_events",
    "where_fallback_scans",
    "where_eq_sidecar_disk_bytes_read_total",
    "where_eq_sidecar_disk_loads",
    "where_plan_eq_sidecar_count",
    "where_plan_id_pk_count",
    "where_plan_fallback_count",
    "where_query_count",
    "where_query_rows_scanned_total",
    "where_query_rows_returned_total",
    "heap_decode_slot_calls",
    "heap_decode_slot_hits",
    "heap_decode_slot_misses",
    "vacuum_priority_score",
    "vacuum_health_bonus_last",
    "vacuum_score_file_bytes_term",
    "vacuum_score_health_bonus_term",
    "vacuum_score_wal_since_term",
    "compact_debt_bytes",
    "compact_debt_rows",
    "compact_debt_priority",
    "page_cache_hits",
    "page_cache_misses",
    "page_cache_evictions",
    "page_cache_bytes_in_cache",
    "memory_budget_max_bytes",
    "memory_budget_used_bytes",
    "memory_budget_reject_count",
    "memory_budget_bytes_evicted_total",
    "memory_budget_sidecar_load_skipped_total",
    "table_storage_health_logical_rows",
    "table_storage_health_physical_rows",
    "table_storage_health_tombstone_rows",
    "table_storage_health_data_file_bytes",
    "table_storage_health_live_bytes",
    "table_storage_health_dead_bytes",
    "table_storage_health_last_vacuum_lsn",
    "table_storage_health_last_vacuum_elapsed_ms",
    "transaction_snapshot_lsn",
    "statement_snapshot_lsn",
    "txn_snapshot_refresh_count",
    "txn_snapshot_pinned_count",
    "txn_readpath_disabled_count",
    "lock_key_range_count",
    "lock_key_predicate_count",
    "group_commit_window_ms",
    "group_commit_max_batch_commits",
    "file_lock_acquire_fail_count",
    "file_lock_same_process_reuse_count",
    "file_lock_stale_marker_count",
    "sidecar_invalidate_count",
    "sidecar_invalidate_fail_count",
)


def fail(msg: str) -> int:
    print(f"RUNTIME_STATS_INVALID: {msg}", file=sys.stderr)
    return 2


def _is_non_negative_int(val: object) -> bool:
    return isinstance(val, int) and val >= 0


def validate_row(
    obj: dict,
    idx: int,
    *,
    active_required: tuple[str, ...] | None = None,
) -> int:
    req = active_required if active_required is not None else REQUIRED_STATS
    req_set = set(req)

    for key in REQUIRED_TOP:
        if key not in obj:
            return fail(f"line {idx}: missing key `{key}`")

    if obj["schema_version"] != EXPECTED_SCHEMA_VERSION:
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
    for key in req:
        if key not in normalized_stats:
            return fail(f"line {idx}: missing stats key `{key}`")

    if "walsync" in req_set:
        if normalized_stats["walsync"] not in ALLOWED_WALSYNC:
            return fail(f"line {idx}: invalid walsync `{normalized_stats['walsync']}`")
    if "normal_interval_ms" in req_set:
        if not _is_non_negative_int(normalized_stats["normal_interval_ms"]):
            return fail(f"line {idx}: normal_interval_ms must be non-negative int")
    if "autovacuum" in req_set:
        if not isinstance(normalized_stats["autovacuum"], bool):
            return fail(f"line {idx}: autovacuum must be bool")
    if "hot_index_enabled" in req_set:
        if not isinstance(normalized_stats["hot_index_enabled"], bool):
            return fail(f"line {idx}: hot_index_enabled must be bool")
    if "wal_adaptive_enabled" in req_set:
        if not isinstance(normalized_stats["wal_adaptive_enabled"], bool):
            return fail(f"line {idx}: wal_adaptive_enabled must be bool")

    for key in _NON_NEGATIVE_INT_STATS_KEYS:
        if key not in req_set:
            continue
        if not _is_non_negative_int(normalized_stats[key]):
            return fail(f"line {idx}: `{key}` must be non-negative int")

    for key in ("wal_recovery_policy", "write_conflict_last_sample"):
        if key not in req_set:
            continue
        if key in normalized_stats and not isinstance(normalized_stats[key], str):
            return fail(f"line {idx}: `{key}` must be string when present")

    if "table_storage_health_tier" in req_set:
        tier = normalized_stats.get("table_storage_health_tier", "good")
        if not isinstance(tier, str) or tier not in ("good", "watch", "degraded", "critical"):
            return fail(f"line {idx}: table_storage_health_tier must be good|watch|degraded|critical")

    if "table_storage_health_fragmentation_ratio" in req_set:
        frag = normalized_stats["table_storage_health_fragmentation_ratio"]
        if not isinstance(frag, (int, float)):
            return fail(f"line {idx}: `table_storage_health_fragmentation_ratio` must be number")
        if float(frag) < 0.0:
            return fail(f"line {idx}: `table_storage_health_fragmentation_ratio` must be >= 0")

    if "compact_debt_ratio" in req_set:
        cdr = normalized_stats["compact_debt_ratio"]
        if not isinstance(cdr, (int, float)):
            return fail(f"line {idx}: `compact_debt_ratio` must be number")
        if float(cdr) < 0.0:
            return fail(f"line {idx}: `compact_debt_ratio` must be >= 0")

    return 0


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--stats-keys-tier",
        choices=("engine", "cli_embed"),
        default=None,
        help=(
            "optional: require and type-check only required_stats_keys minus stats_keys_cli_embed_layer "
            "(engine), or only stats_keys_cli_embed_layer (cli_embed); default is full required_stats_keys"
        ),
    )
    p.add_argument("jsonl_path")
    args = p.parse_args()

    if args.stats_keys_tier == "engine":
        active_required: tuple[str, ...] | None = STATS_KEYS_ENGINE
        tier_label = "engine"
    elif args.stats_keys_tier == "cli_embed":
        active_required = STATS_KEYS_CLI_EMBED
        tier_label = "cli_embed"
    else:
        active_required = None
        tier_label = "full"

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
        rc = validate_row(obj, idx, active_required=active_required)
        if rc != 0:
            return rc
        count += 1

    if count == 0:
        return fail("no runtime rows found")
    print(f"RUNTIME_STATS_VALID: {path} rows={count} tier={tier_label}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

