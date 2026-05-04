#!/usr/bin/env python3
"""Nightly / soak checklist (documentation runner).

Execute the listed ctest filters or workflows in your CI job; this script only
prints the recommended matrix so pipelines stay explicit.
"""

from __future__ import annotations

import argparse


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--json", action="store_true", help="Emit one JSON line for log parsers")
    args = p.parse_args()
    matrix = [
        "WalRecoveryIndexed / WalRecovery*",
        "TxnIsolationVisibility / TxnWriteConflict / LockKey*",
        "large WAL recover + checkpoint begin/end fault injections (custom harness)",
        "concurrent_pressure + runtime jsonl + ci_bench_gate.py --runtime-jsonl ... "
        "(--max-table-storage-health-* / --max-compact-debt-bytes-peak optional)",
        "storage: repeated DML + VACUUM + NEWDB_VACUUM_QUEUE_USE_HEALTH=1 + ctest StorageSoakLight + fragmentation gates",
        "optional heavy: NEWDB_ENABLE_HEAVY_SOAK=1 + NEWDB_SOAK_HINT_JSONL=<path> (StorageSoakHeavy appends one JSON line; "
        "merge into runtime_stats.jsonl or pass alongside: ci_bench_gate.py --runtime-jsonl only validates full "
        "`validate_runtime_stats.py` rows — either append compatible JSONL lines or run gate on a merged file / "
        "second pass for soak markers)",
        "optional PR/Nightly: verify_clean_reconfigure.ps1 -BenchGateStorage -BenchGateWalRecovery "
        "(runtime JSONL fixture + ci_bench_gate storage/WAL recovery ceilings; see scripts/ci/fixtures/README.md)",
        "sidecar rebuild interrupted (crash simulation) + NEWDB_INDEX_CATALOG_ENFORCE=1",
    ]
    if args.json:
        import json

        # Recommended starting bands for Nightly gates (host-dependent; tune with fixtures).
        threshold_hints = {
            "max_wal_recovery_last_elapsed_ms_start": 2000,
            "max_table_storage_health_fragmentation_ratio_verify_script": "0.95 (verify_clean_reconfigure -BenchGateStorage)",
            "max_table_storage_health_dead_bytes_verify_script": "1e9 placeholder in verify script",
            "max_compact_debt_bytes_peak_verify_script": "1e11 placeholder in verify script",
            "max_vacuum_health_bonus_last_verify_script": "1e11 placeholder in verify script",
            "notes": "See STORAGE_GOVERNANCE_AND_RECOVERY_BUDGETS.md section 4 and PERF_AND_CI_BUDGETS.md sections 3-4.",
        }
        print(
            json.dumps(
                {
                    "tool": "nightly_soak_hints",
                    "status": "ok",
                    "matrix": matrix,
                    "threshold_hints": threshold_hints,
                }
            )
        )
        return 0
    for i, item in enumerate(matrix, start=1):
        print(f"{i}. {item}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
