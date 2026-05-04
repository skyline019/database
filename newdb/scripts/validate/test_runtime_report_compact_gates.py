#!/usr/bin/env python3
"""Regression checks for newdb_runtime_report compact gate flags.

This script verifies:
1) success path with compact failure delta == 0 and reclaimed bytes > 0
2) failure path for --max-vacuum-compact-failure-delta (exit code 16)
3) failure path for --min-vacuum-compact-reclaimed-bytes-delta (exit code 17)
4) failure path for --max-vacuum-queue-depth-peak (exit code 18)
5) failure path for --max-wal-recovery-last-elapsed-ms (exit code 19)
6) failure path for --max-lock-deadlock-detect-delta (exit code 20)
7) failure path for --max-scheduler-throttle-delta (exit code 21)
8) failure path for --min-wal-group-commit-batch-commits-delta (exit code 22)
9) failure path for --max-lock-deadlock-victim-delta (exit code 23)
10) failure path for --max-lock-wait-max-ms-delta (exit code 24)
11) failure path for --max-lsm-read-segments-scanned-p95 (exit code 27)
12) failure path for --min-lsm-compaction-bytes-amp-efficiency (exit code 28)
"""

from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
from pathlib import Path


def _row(
    ts_ms: int,
    label: str,
    compact_fail: int,
    reclaimed: int,
    queue_depth_peak: int = 0,
    wal_recovery_last_ms: int = 0,
    deadlock_detect_count: int = 0,
    deadlock_victim_count: int = 0,
    lock_wait_max_ms: int = 0,
    scheduler_throttle_count: int = 0,
    wal_group_commit_batch_commits: int = 0,
    lsm_read_segments_scanned_p95: int = 0,
    lsm_compaction_bytes_in: int = 0,
    lsm_compaction_bytes_out: int = 0,
) -> dict:
    return {
        "schema_version": "newdb.runtime_stats.v1",
        "ts_ms": ts_ms,
        "run_id": "compact_gate_test",
        "label": label,
        "stats": {
            "walsync": "normal",
            "normal_interval_ms": 20,
            "autovacuum": True,
            "vacuum_ops_threshold": 25,
            "vacuum_min_interval_sec": 0,
            "vacuum_trigger_count": 1 if label != "pressure_before" else 0,
            "vacuum_execute_count": 1 if label != "pressure_before" else 0,
            "vacuum_cooldown_skip_count": 0,
            "vacuum_compact_success_count": 1 if label != "pressure_before" else 0,
            "vacuum_compact_failure_count": compact_fail,
            "vacuum_compact_bytes_reclaimed": reclaimed,
            "vacuum_compact_last_elapsed_ms": 5 if label != "pressure_before" else 0,
            "vacuum_queue_depth": 0,
            "vacuum_queue_depth_peak": queue_depth_peak,
            "write_conflicts": 0,
            "lock_wait_ms_total": 0,
            "lock_wait_max_ms": lock_wait_max_ms,
            "lock_deadlock_detect_count": deadlock_detect_count,
            "lock_deadlock_victim_count": deadlock_victim_count,
            "txn_begin_lock_conflicts": 0,
            "wal_compact_count": 0,
            "wal_recovery_runs": 1,
            "wal_recovery_undo_ops": 0,
            "wal_recovery_last_elapsed_ms": wal_recovery_last_ms,
            "wal_group_commit_count": 0,
            "wal_group_commit_batch_commits": wal_group_commit_batch_commits,
            "wal_group_commit_pending_commits": 0,
            "txn_commit_count": 0,
            "txn_commit_p95_ms": 0,
            "txn_commit_max_ms": 0,
            "wal_bytes_since_start": 0,
            "wal_bytes_per_commit_avg": 0,
            "lock_wait_p95_ms": 0,
            "scheduler_throttle_count": scheduler_throttle_count,
            "hot_index_enabled": True,
            "segment_target_bytes": 0,
            "lsm_memtable_flush_count": 0,
            "lsm_compaction_count": 0,
            "lsm_segment_count": 0,
            "lsm_memtable_bytes": 0,
            "lsm_read_segments_scanned": lsm_read_segments_scanned_p95,
            "lsm_read_segments_scanned_p95": lsm_read_segments_scanned_p95,
            "lsm_compaction_bytes_in": lsm_compaction_bytes_in,
            "lsm_compaction_bytes_out": lsm_compaction_bytes_out,
            "wal_adaptive_enabled": False,
            "group_commit_window_ms": 0,
            "group_commit_max_batch_commits": 1,
        },
    }


def _write_jsonl(path: Path, before: dict, after: dict) -> None:
    path.write_text(
        json.dumps(before, separators=(",", ":"))
        + "\n"
        + json.dumps(after, separators=(",", ":"))
        + "\n",
        encoding="utf-8",
    )


def _run(binary: Path, jsonl: Path, *args: str) -> subprocess.CompletedProcess[str]:
    cmd = [str(binary), "--input", str(jsonl), "--last-n", "2", *args]
    return subprocess.run(cmd, text=True, capture_output=True, check=False)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("runtime_report_binary", help="Path to newdb_runtime_report(.exe)")
    args = p.parse_args()

    binary = Path(args.runtime_report_binary)
    if not binary.is_file():
        raise SystemExit(f"runtime_report binary not found: {binary}")

    with tempfile.TemporaryDirectory(prefix="newdb_runtime_report_gate_") as td:
        td_path = Path(td)

        # Success case.
        success_jsonl = td_path / "success.jsonl"
        _write_jsonl(
            success_jsonl,
            _row(1, "pressure_before", compact_fail=0, reclaimed=0),
            _row(2, "pressure_after", compact_fail=0, reclaimed=4096),
        )
        ok = _run(
            binary,
            success_jsonl,
            "--max-vacuum-compact-failure-delta",
            "0",
            "--min-vacuum-compact-reclaimed-bytes-delta",
            "1",
        )
        if ok.returncode != 0:
            raise SystemExit(
                "expected success case to pass, got rc="
                + str(ok.returncode)
                + "\nstdout:\n"
                + ok.stdout
                + "\nstderr:\n"
                + ok.stderr
            )
        try:
            summary = json.loads(ok.stdout.strip().splitlines()[-1])
        except Exception as exc:
            raise SystemExit(f"expected success output to contain summary json: {exc}\nstdout:\n{ok.stdout}")
        for key in (
            "vacuum_queue_depth_peak_max",
            "wal_recovery_runs_delta",
            "wal_recovery_undo_ops_delta",
            "wal_recovery_last_elapsed_ms_max",
            "lsm_read_segments_scanned_p95_max",
            "lsm_compaction_bytes_amp_efficiency",
        ):
            if key not in summary:
                raise SystemExit(f"missing key in summary json: {key}\nsummary={summary}")

        ok_json = _run(
            binary,
            success_jsonl,
            "--json",
            "--max-vacuum-compact-failure-delta",
            "0",
            "--min-vacuum-compact-reclaimed-bytes-delta",
            "1",
        )
        if ok_json.returncode != 0:
            raise SystemExit(
                "expected --json success case to pass, got rc="
                + str(ok_json.returncode)
                + "\nstdout:\n"
                + ok_json.stdout
            )
        json_lines = [ln for ln in ok_json.stdout.splitlines() if ln.strip()]
        if len(json_lines) != 1:
            raise SystemExit(
                "expected --json stdout to be a single line, got "
                + str(len(json_lines))
                + " lines:\n"
                + ok_json.stdout
            )
        json.loads(json_lines[0])

        # Failure case: compact failure delta > threshold => exit code 16.
        fail_delta_jsonl = td_path / "fail_delta.jsonl"
        _write_jsonl(
            fail_delta_jsonl,
            _row(1, "pressure_before", compact_fail=0, reclaimed=0),
            _row(2, "pressure_after", compact_fail=1, reclaimed=4096),
        )
        rc16 = _run(binary, fail_delta_jsonl, "--max-vacuum-compact-failure-delta", "0")
        if rc16.returncode != 16:
            raise SystemExit(
                "expected compact failure gate rc=16, got rc="
                + str(rc16.returncode)
                + "\nstdout:\n"
                + rc16.stdout
                + "\nstderr:\n"
                + rc16.stderr
            )

        # Failure case: reclaimed bytes below threshold => exit code 17.
        fail_reclaim_jsonl = td_path / "fail_reclaim.jsonl"
        _write_jsonl(
            fail_reclaim_jsonl,
            _row(1, "pressure_before", compact_fail=0, reclaimed=0),
            _row(2, "pressure_after", compact_fail=0, reclaimed=0),
        )
        rc17 = _run(
            binary,
            fail_reclaim_jsonl,
            "--min-vacuum-compact-reclaimed-bytes-delta",
            "1",
        )
        if rc17.returncode != 17:
            raise SystemExit(
                "expected reclaimed bytes gate rc=17, got rc="
                + str(rc17.returncode)
                + "\nstdout:\n"
                + rc17.stdout
                + "\nstderr:\n"
                + rc17.stderr
            )

        # Failure case: queue depth peak above threshold => exit code 18.
        fail_queue_jsonl = td_path / "fail_queue_peak.jsonl"
        _write_jsonl(
            fail_queue_jsonl,
            _row(1, "pressure_before", compact_fail=0, reclaimed=0, queue_depth_peak=0),
            _row(2, "pressure_after", compact_fail=0, reclaimed=4096, queue_depth_peak=3),
        )
        rc18 = _run(binary, fail_queue_jsonl, "--max-vacuum-queue-depth-peak", "0")
        if rc18.returncode != 18:
            raise SystemExit(
                "expected queue depth peak gate rc=18, got rc="
                + str(rc18.returncode)
                + "\nstdout:\n"
                + rc18.stdout
                + "\nstderr:\n"
                + rc18.stderr
            )

        # Failure case: recovery elapsed above threshold => exit code 19.
        fail_recovery_jsonl = td_path / "fail_recovery_elapsed.jsonl"
        _write_jsonl(
            fail_recovery_jsonl,
            _row(1, "pressure_before", compact_fail=0, reclaimed=0, wal_recovery_last_ms=1),
            _row(2, "pressure_after", compact_fail=0, reclaimed=4096, wal_recovery_last_ms=100),
        )
        rc19 = _run(binary, fail_recovery_jsonl, "--max-wal-recovery-last-elapsed-ms", "10")
        if rc19.returncode != 19:
            raise SystemExit(
                "expected wal recovery elapsed gate rc=19, got rc="
                + str(rc19.returncode)
                + "\nstdout:\n"
                + rc19.stdout
                + "\nstderr:\n"
                + rc19.stderr
            )

        # Failure case: deadlock detect delta > threshold => exit code 20.
        fail_deadlock_jsonl = td_path / "fail_deadlock_delta.jsonl"
        _write_jsonl(
            fail_deadlock_jsonl,
            _row(1, "pressure_before", compact_fail=0, reclaimed=0, deadlock_detect_count=0),
            _row(2, "pressure_after", compact_fail=0, reclaimed=4096, deadlock_detect_count=2),
        )
        rc20 = _run(binary, fail_deadlock_jsonl, "--max-lock-deadlock-detect-delta", "0")
        if rc20.returncode != 20:
            raise SystemExit(
                "expected deadlock detect gate rc=20, got rc="
                + str(rc20.returncode)
                + "\nstdout:\n"
                + rc20.stdout
                + "\nstderr:\n"
                + rc20.stderr
            )

        # Failure case: scheduler throttle delta > threshold => exit code 21.
        fail_throttle_jsonl = td_path / "fail_scheduler_throttle_delta.jsonl"
        _write_jsonl(
            fail_throttle_jsonl,
            _row(1, "pressure_before", compact_fail=0, reclaimed=0, scheduler_throttle_count=0),
            _row(2, "pressure_after", compact_fail=0, reclaimed=4096, scheduler_throttle_count=3),
        )
        rc21 = _run(binary, fail_throttle_jsonl, "--max-scheduler-throttle-delta", "0")
        if rc21.returncode != 21:
            raise SystemExit(
                "expected scheduler throttle gate rc=21, got rc="
                + str(rc21.returncode)
                + "\nstdout:\n"
                + rc21.stdout
                + "\nstderr:\n"
                + rc21.stderr
            )

        # Failure case: group-commit batch commits delta < threshold => exit code 22.
        fail_gc_batch_jsonl = td_path / "fail_group_commit_batch_delta.jsonl"
        _write_jsonl(
            fail_gc_batch_jsonl,
            _row(1, "pressure_before", compact_fail=0, reclaimed=0, wal_group_commit_batch_commits=0),
            _row(2, "pressure_after", compact_fail=0, reclaimed=4096, wal_group_commit_batch_commits=1),
        )
        rc22 = _run(
            binary,
            fail_gc_batch_jsonl,
            "--min-wal-group-commit-batch-commits-delta",
            "2",
        )
        if rc22.returncode != 22:
            raise SystemExit(
                "expected group commit batch delta gate rc=22, got rc="
                + str(rc22.returncode)
                + "\nstdout:\n"
                + rc22.stdout
                + "\nstderr:\n"
                + rc22.stderr
            )

        # Failure case: deadlock victim delta > threshold => exit code 23.
        fail_deadlock_victim_jsonl = td_path / "fail_deadlock_victim_delta.jsonl"
        _write_jsonl(
            fail_deadlock_victim_jsonl,
            _row(1, "pressure_before", compact_fail=0, reclaimed=0, deadlock_victim_count=0),
            _row(2, "pressure_after", compact_fail=0, reclaimed=4096, deadlock_victim_count=1),
        )
        rc23 = _run(binary, fail_deadlock_victim_jsonl, "--max-lock-deadlock-victim-delta", "0")
        if rc23.returncode != 23:
            raise SystemExit(
                "expected deadlock victim gate rc=23, got rc="
                + str(rc23.returncode)
                + "\nstdout:\n"
                + rc23.stdout
                + "\nstderr:\n"
                + rc23.stderr
            )

        # Failure case: lock wait max delta > threshold => exit code 24.
        fail_lock_wait_max_jsonl = td_path / "fail_lock_wait_max_delta.jsonl"
        _write_jsonl(
            fail_lock_wait_max_jsonl,
            _row(1, "pressure_before", compact_fail=0, reclaimed=0, lock_wait_max_ms=0),
            _row(2, "pressure_after", compact_fail=0, reclaimed=4096, lock_wait_max_ms=11),
        )
        rc24 = _run(binary, fail_lock_wait_max_jsonl, "--max-lock-wait-max-ms-delta", "10")
        if rc24.returncode != 24:
            raise SystemExit(
                "expected lock wait max gate rc=24, got rc="
                + str(rc24.returncode)
                + "\nstdout:\n"
                + rc24.stdout
                + "\nstderr:\n"
                + rc24.stderr
            )

        # Failure case: LSM scanned p95 > threshold => exit code 27.
        fail_lsm_scan_p95_jsonl = td_path / "fail_lsm_scan_p95.jsonl"
        _write_jsonl(
            fail_lsm_scan_p95_jsonl,
            _row(1, "pressure_before", compact_fail=0, reclaimed=0, lsm_read_segments_scanned_p95=0),
            _row(2, "pressure_after", compact_fail=0, reclaimed=4096, lsm_read_segments_scanned_p95=9),
        )
        rc27 = _run(binary, fail_lsm_scan_p95_jsonl, "--max-lsm-read-segments-scanned-p95", "6")
        if rc27.returncode != 27:
            raise SystemExit(
                "expected lsm scan p95 gate rc=27, got rc="
                + str(rc27.returncode)
                + "\nstdout:\n"
                + rc27.stdout
                + "\nstderr:\n"
                + rc27.stderr
            )

        # Failure case: compaction bytes amp efficiency < threshold => exit code 28.
        fail_lsm_amp_jsonl = td_path / "fail_lsm_amp_efficiency.jsonl"
        _write_jsonl(
            fail_lsm_amp_jsonl,
            _row(1, "pressure_before", compact_fail=0, reclaimed=0, lsm_compaction_bytes_in=0, lsm_compaction_bytes_out=0),
            _row(2, "pressure_after", compact_fail=0, reclaimed=4096, lsm_compaction_bytes_in=1000, lsm_compaction_bytes_out=50),
        )
        rc28 = _run(
            binary,
            fail_lsm_amp_jsonl,
            "--min-lsm-compaction-bytes-amp-efficiency",
            "0.2",
        )
        if rc28.returncode != 28:
            raise SystemExit(
                "expected lsm amp efficiency gate rc=28, got rc="
                + str(rc28.returncode)
                + "\nstdout:\n"
                + rc28.stdout
                + "\nstderr:\n"
                + rc28.stderr
            )

    print("RUNTIME_REPORT_COMPACT_GATES_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
