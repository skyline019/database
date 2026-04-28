#!/usr/bin/env python3
"""Compatibility regression tests for validate_runtime_stats.py."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path


def _run_validator(jsonl_path: Path) -> subprocess.CompletedProcess[str]:
    script = Path(__file__).with_name("validate_runtime_stats.py")
    return subprocess.run(
        [sys.executable, str(script), str(jsonl_path)],
        text=True,
        capture_output=True,
        check=False,
    )


def _legacy_row() -> dict:
    return {
        "schema_version": "newdb.runtime_stats.v1",
        "ts_ms": 1,
        "label": "pressure_before",
        "run_id": "compat_test",
        "stats": {
            "vacuum_ops_threshold": 25,
            "vacuum_min_interval_sec": 0,
            "vacuum_trigger_count": 0,
            "vacuum_execute_count": 0,
            "vacuum_cooldown_skip_count": 0,
            "vacuum_compact_success_count": 0,
            "vacuum_compact_failure_count": 0,
            "vacuum_compact_bytes_reclaimed": 0,
            "vacuum_compact_last_elapsed_ms": 0,
            "vacuum_queue_depth": 0,
            "vacuum_queue_depth_peak": 0,
            "write_conflicts": 0,
            "lock_wait_ms_total": 0,
            "lock_wait_max_ms": 0,
            "lock_deadlock_detect_count": 0,
            "lock_deadlock_victim_count": 0,
            "txn_begin_lock_conflicts": 0,
            "wal_compact_count": 0,
            "wal_recovery_runs": 0,
            "wal_recovery_undo_ops": 0,
            "wal_recovery_last_elapsed_ms": 0,
            "wal_recovery_analyze_ms": 0,
            "wal_recovery_undo_ms": 0,
            "wal_recovery_finalize_ms": 0,
            "wal_recovery_records_scanned": 0,
            "wal_recovery_dangling_txns": 0,
            "wal_group_commit_count": 0,
            "wal_group_commit_batch_commits": 0,
            "wal_group_commit_pending_commits": 0,
            "txn_commit_count": 0,
            "txn_commit_p95_ms": 0,
            "wal_bytes_since_start": 0,
            "wal_bytes_per_commit_avg": 0,
            "lock_wait_p95_ms": 0,
            "scheduler_throttle_count": 0,
            "hot_index_enabled": True,
            "segment_target_bytes": 128,
            "lsm_memtable_flush_count": 0,
            "lsm_compaction_count": 0,
            "lsm_segment_count": 0,
            "lsm_memtable_bytes": 0,
            "lsm_read_segments_scanned": 0,
            "lsm_read_segments_scanned_p95": 0,
            "lsm_compaction_bytes_in": 0,
            "lsm_compaction_bytes_out": 0,
            "lsm_compaction_queue_pending": 0,
            "lsm_compaction_queue_inflight": 0,
            "lsm_compaction_enqueue_skipped_backpressure": 0,
            "lsm_segment_cache_hits": 0,
            "lsm_segment_cache_misses": 0,
            "wal_adaptive_enabled": False,
            "group_commit_window_ms": 0,
            "group_commit_max_batch_commits": 1,
        },
    }


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="newdb_validate_stats_compat_") as td:
        td_path = Path(td)

        ok_path = td_path / "legacy_ok.jsonl"
        ok_path.write_text(json.dumps(_legacy_row()) + "\n", encoding="utf-8")
        ok = _run_validator(ok_path)
        if ok.returncode != 0:
            raise SystemExit(f"expected compat row pass, rc={ok.returncode}\nstdout={ok.stdout}\nstderr={ok.stderr}")

        bad = _legacy_row()
        bad["stats"]["lsm_compaction_count"] = -1
        bad_path = td_path / "legacy_bad.jsonl"
        bad_path.write_text(json.dumps(bad) + "\n", encoding="utf-8")
        failed = _run_validator(bad_path)
        if failed.returncode == 0:
            raise SystemExit("expected invalid row to fail, but validator returned rc=0")

    print("VALIDATE_RUNTIME_STATS_COMPAT_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
