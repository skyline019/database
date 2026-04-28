#!/usr/bin/env python3
"""CI gate: smoke binary + microbench gtest (through ctest).

Note: `newdb_smoke` requires a subcommand and returns non-zero on usage output,
so the gate runs a minimal create/append/load flow on a temp `.bin`.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from typing import Optional


def _last_json_line(text: str) -> Optional[dict]:
    parsed = None
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
    return parsed


def _parse_prefixed_json_line(text: str, prefix: str) -> Optional[dict]:
    parsed = None
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith(prefix):
            continue
        payload = line[len(prefix):].strip()
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            continue
    return parsed


def _resolve_build_config(build_dir: str, requested: str) -> Optional[str]:
    if requested:
        return requested
    for cfg in ("RelWithDebInfo", "Release", "Debug", "MinSizeRel"):
        if os.path.isdir(os.path.join(build_dir, cfg)):
            return cfg
    return None


def _resolve_smoke_binary(build_dir: str, build_config: Optional[str]) -> str:
    binary_name = "newdb_smoke.exe" if os.name == "nt" else "newdb_smoke"
    candidates = [os.path.join(build_dir, binary_name)]
    if build_config:
        candidates.insert(0, os.path.join(build_dir, build_config, binary_name))
    for path in candidates:
        if os.path.isfile(path):
            return path
    return candidates[0]


def _resolve_runtime_report_binary(build_dir: str, build_config: Optional[str]) -> str:
    binary_name = "newdb_runtime_report.exe" if os.name == "nt" else "newdb_runtime_report"
    candidates = [os.path.join(build_dir, binary_name)]
    if build_config:
        candidates.insert(0, os.path.join(build_dir, build_config, binary_name))
    for path in candidates:
        if os.path.isfile(path):
            return path
    return candidates[0]


def _resolve_perf_binary(build_dir: str, build_config: Optional[str]) -> str:
    binary_name = "newdb_perf.exe" if os.name == "nt" else "newdb_perf"
    candidates = [os.path.join(build_dir, binary_name)]
    if build_config:
        candidates.insert(0, os.path.join(build_dir, build_config, binary_name))
    for path in candidates:
        if os.path.isfile(path):
            return path
    return candidates[0]


def _resolve_demo_binary(build_dir: str, build_config: Optional[str]) -> str:
    binary_name = "newdb_demo.exe" if os.name == "nt" else "newdb_demo"
    candidates = [os.path.join(build_dir, binary_name)]
    if build_config:
        candidates.insert(0, os.path.join(build_dir, build_config, binary_name))
    for path in candidates:
        if os.path.isfile(path):
            return path
    return candidates[0]


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument(
        "build_dir",
        nargs="?",
        default="build",
        help="CMake build directory (relative to repo root when not absolute)",
    )
    p.add_argument(
        "--build-config",
        default="",
        help="CMake multi-config profile (e.g. RelWithDebInfo). Auto-detected when omitted.",
    )
    p.add_argument(
        "--runtime-jsonl",
        default="",
        help="Path to runtime snapshot jsonl for extra gate checks (optional).",
    )
    p.add_argument(
        "--runtime-last-n",
        type=int,
        default=0,
        help="Use only last N snapshots for gate delta (disabled when 0, must be >=2 when enabled).",
    )
    p.add_argument(
        "--runtime-label-prefix",
        default="",
        help="Optional label prefix filter before computing runtime deltas.",
    )
    p.add_argument(
        "--runtime-run-id",
        default="",
        help="Optional run_id filter (use 'latest' to select latest run in file).",
    )
    p.add_argument(
        "--min-vacuum-efficiency",
        type=float,
        default=-1.0,
        help="Fail gate when vacuum_efficiency is below this threshold (disabled when <0).",
    )
    p.add_argument(
        "--max-conflict-rate",
        type=float,
        default=-1.0,
        help="Fail gate when conflict_rate is above this threshold (disabled when <0).",
    )
    p.add_argument(
        "--min-vacuum-efficiency-p50",
        type=float,
        default=-1.0,
        help="Fail gate when vacuum_efficiency_p50 is below this threshold (disabled when <0).",
    )
    p.add_argument(
        "--max-conflict-rate-p95",
        type=float,
        default=-1.0,
        help="Fail gate when conflict_rate_p95 is above this threshold (disabled when <0).",
    )
    p.add_argument(
        "--max-txn-begin-lock-conflict-delta",
        type=float,
        default=-1.0,
        help="Fail gate when txn_begin_lock_conflict_delta is above this threshold (disabled when <0).",
    )
    p.add_argument(
        "--max-wal-compact-delta",
        type=float,
        default=-1.0,
        help="Fail gate when wal_compact_delta is above this threshold (disabled when <0).",
    )
    p.add_argument(
        "--max-vacuum-compact-failure-delta",
        type=float,
        default=-1.0,
        help="Fail gate when vacuum_compact_failure_delta is above this threshold (disabled when <0, recommended: 0).",
    )
    p.add_argument(
        "--min-vacuum-compact-reclaimed-bytes-delta",
        type=float,
        default=-1.0,
        help="Fail gate when vacuum_compact_reclaimed_bytes_delta is below this threshold (disabled when <0, use in soak/nightly).",
    )
    p.add_argument(
        "--max-vacuum-queue-depth-peak",
        type=float,
        default=-1.0,
        help="Fail gate when vacuum_queue_depth_peak_max is above this threshold (disabled when <0).",
    )
    p.add_argument(
        "--max-wal-recovery-last-elapsed-ms",
        type=float,
        default=-1.0,
        help="Fail gate when wal_recovery_last_elapsed_ms_max is above this threshold (disabled when <0).",
    )
    p.add_argument(
        "--max-lock-deadlock-detect-delta",
        type=float,
        default=-1.0,
        help="Fail gate when lock_deadlock_detect_delta is above this threshold (disabled when <0).",
    )
    p.add_argument(
        "--max-lock-deadlock-victim-delta",
        type=float,
        default=-1.0,
        help="Fail gate when lock_deadlock_victim_delta is above this threshold (disabled when <0).",
    )
    p.add_argument(
        "--max-lock-wait-max-ms-delta",
        type=float,
        default=-1.0,
        help="Fail gate when lock_wait_max_ms_delta is above this threshold (disabled when <0).",
    )
    p.add_argument(
        "--max-scheduler-throttle-delta",
        type=float,
        default=-1.0,
        help="Fail gate when scheduler_throttle_delta is above this threshold (disabled when <0).",
    )
    p.add_argument(
        "--min-wal-group-commit-batch-commits-delta",
        type=float,
        default=-1.0,
        help="Fail gate when wal_group_commit_batch_commits_delta is below this threshold (disabled when <0).",
    )
    p.add_argument(
        "--max-lsm-segment-count",
        type=float,
        default=-1.0,
        help="Fail gate when lsm_segment_count_max is above this threshold (disabled when <0).",
    )
    p.add_argument(
        "--min-lsm-memtable-flush-delta",
        type=float,
        default=-1.0,
        help="Fail gate when lsm_memtable_flush_delta is below this threshold (disabled when <0).",
    )
    p.add_argument(
        "--max-lsm-read-segments-scanned-p95",
        type=float,
        default=-1.0,
        help="Fail gate when lsm_read_segments_scanned_p95_max is above this threshold (disabled when <0).",
    )
    p.add_argument(
        "--min-lsm-compaction-bytes-amp-efficiency",
        type=float,
        default=-1.0,
        help="Fail gate when lsm_compaction_bytes_amp_efficiency is below this threshold (disabled when <0).",
    )
    p.add_argument(
        "--run-newdb-perf",
        action="store_true",
        help="Run newdb_perf --json as an additional CI gate check.",
    )
    p.add_argument(
        "--newdb-perf-sizes",
        default="1000",
        help="CSV row targets passed to newdb_perf --sizes (used with --run-newdb-perf).",
    )
    p.add_argument(
        "--newdb-perf-query-loops",
        type=int,
        default=1,
        help="Query loop count passed to newdb_perf (used with --run-newdb-perf).",
    )
    p.add_argument(
        "--newdb-perf-txn-per-mode",
        type=int,
        default=10,
        help="Txn per mode passed to newdb_perf (used with --run-newdb-perf).",
    )
    p.add_argument(
        "--newdb-perf-build-chunk-size",
        type=int,
        default=1000,
        help="Build chunk size passed to newdb_perf (used with --run-newdb-perf).",
    )
    p.add_argument(
        "--max-newdb-perf-elapsed-ms",
        type=float,
        default=-1.0,
        help="Fail gate when newdb_perf elapsed_ms is above this threshold (disabled when <0).",
    )
    p.add_argument(
        "--min-newdb-perf-txn-avg-tps",
        type=float,
        default=-1.0,
        help="Fail gate when NEWDB_PERF_SUMMARY txn_avg_tps is below this threshold (disabled when <0).",
    )
    p.add_argument(
        "--min-newdb-perf-build-avg-tps",
        type=float,
        default=-1.0,
        help="Fail gate when NEWDB_PERF_SUMMARY build_avg_tps is below this threshold (disabled when <0).",
    )
    p.add_argument(
        "--newdb-perf-demo-exe",
        default="",
        help="Path to demo binary passed to newdb_perf --demo-exe (auto-resolved when empty).",
    )
    p.add_argument(
        "--pressure-summary-json",
        default="",
        help="Optional concurrent_pressure_summary_*.json path for TPS/latency gate checks.",
    )
    p.add_argument(
        "--min-runtime-pressure-tps",
        type=float,
        default=-1.0,
        help="Fail gate when runtime_pressure_tps_est is below this threshold (disabled when <0).",
    )
    p.add_argument(
        "--max-runtime-pressure-batch-p95-ms",
        type=float,
        default=-1.0,
        help="Fail gate when runtime_pressure_batch_ms_p95 is above this threshold (disabled when <0).",
    )
    p.add_argument(
        "--max-where-fallback-scans-max",
        type=float,
        default=-1.0,
        help="Fail gate when where_fallback_scans_max in pressure summary is above this threshold (disabled when <0).",
    )
    p.add_argument(
        "--min-where-plan-eq-sidecar-count-delta",
        type=float,
        default=-1.0,
        help="Fail gate when where_plan_eq_sidecar_count_delta in pressure summary is below this threshold (disabled when <0).",
    )
    args = p.parse_args()
    here = os.path.dirname(os.path.abspath(__file__))
    # scripts/ci/ci_bench_gate.py -> repo root is two levels up.
    repo_root = os.path.dirname(os.path.dirname(here))
    b = args.build_dir if os.path.isabs(args.build_dir) else os.path.join(repo_root, args.build_dir)
    if not os.path.isdir(b):
        print(f"ERROR: build dir not found: {b}", file=sys.stderr)
        return 2

    build_config = _resolve_build_config(b, args.build_config)
    smoke = _resolve_smoke_binary(b, build_config)
    if os.path.isfile(smoke):
        with tempfile.TemporaryDirectory(prefix="newdb_smoke_") as td:
            bin_path = os.path.join(td, "smoke_ci.bin")
            for cmd in (
                [smoke, "--json", "create", bin_path],
                [smoke, "--json", "append", bin_path, "1"],
                [smoke, "--json", "load", bin_path],
            ):
                out = subprocess.check_output(cmd, cwd=b, text=True)
                parsed = None
                for line in out.splitlines():
                    line = line.strip()
                    if not line.startswith("{"):
                        continue
                    try:
                        parsed = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                if not parsed:
                    print(f"ERROR: smoke JSON output missing for cmd: {' '.join(cmd)}", file=sys.stderr)
                    return 9
                if parsed.get("tool") != "newdb_smoke" or parsed.get("status") != "ok":
                    print("ERROR: smoke JSON reported failure: " + json.dumps(parsed, sort_keys=True),
                          file=sys.stderr)
                    return 10
    else:
        print(f"NOTE: skip newdb_smoke (not built): {smoke}")

    if args.run_newdb_perf:
        perf = _resolve_perf_binary(b, build_config)
        if not os.path.isfile(perf):
            print(f"ERROR: newdb_perf not found: {perf}", file=sys.stderr)
            return 11
        demo_exe = args.newdb_perf_demo_exe
        if not demo_exe:
            demo_exe = _resolve_demo_binary(b, build_config)
        if not os.path.isabs(demo_exe):
            demo_exe = os.path.join(repo_root, demo_exe)
        if not os.path.isfile(demo_exe):
            print(f"ERROR: demo binary for newdb_perf not found: {demo_exe}", file=sys.stderr)
            return 15
        with tempfile.TemporaryDirectory(prefix="newdb_perf_") as td:
            cmd = [
                perf,
                "--json",
                "--demo-exe",
                demo_exe,
                "--data-dir",
                td,
                "--sizes",
                args.newdb_perf_sizes,
                "--query-loops",
                str(args.newdb_perf_query_loops),
                "--txn-per-mode",
                str(args.newdb_perf_txn_per_mode),
                "--build-chunk-size",
                str(args.newdb_perf_build_chunk_size),
            ]
            out = subprocess.check_output(cmd, cwd=b, text=True)
            parsed = _last_json_line(out)
            if not parsed:
                print("ERROR: newdb_perf JSON output missing", file=sys.stderr)
                return 12
            if parsed.get("tool") != "newdb_perf" or parsed.get("status") != "ok":
                print("ERROR: newdb_perf reported failure: " + json.dumps(parsed, sort_keys=True),
                      file=sys.stderr)
                return 13
            elapsed_ms = float(parsed.get("elapsed_ms", 0.0))
            if args.max_newdb_perf_elapsed_ms >= 0.0 and elapsed_ms > args.max_newdb_perf_elapsed_ms:
                print(
                    f"ERROR: newdb_perf elapsed_ms={elapsed_ms} exceeds max "
                    f"{args.max_newdb_perf_elapsed_ms}",
                    file=sys.stderr,
                )
                return 14
            print("NEWDB_PERF_GATE_SUMMARY " + json.dumps(parsed, sort_keys=True))
            script_summary = _parse_prefixed_json_line(out, "NEWDB_PERF_SUMMARY")
            if script_summary:
                if script_summary.get("tool") != "million_scale_bench" or script_summary.get("status") != "ok":
                    print(
                        "ERROR: NEWDB_PERF_SUMMARY reported failure: "
                        + json.dumps(script_summary, sort_keys=True),
                        file=sys.stderr,
                    )
                    return 16
                txn_avg_tps = float(script_summary.get("txn_avg_tps", 0.0))
                build_avg_tps = float(script_summary.get("build_avg_tps", 0.0))
                if (
                    args.min_newdb_perf_txn_avg_tps >= 0.0
                    and txn_avg_tps < args.min_newdb_perf_txn_avg_tps
                ):
                    print(
                        f"ERROR: txn_avg_tps={txn_avg_tps} below min "
                        f"{args.min_newdb_perf_txn_avg_tps}",
                        file=sys.stderr,
                    )
                    return 17
                if (
                    args.min_newdb_perf_build_avg_tps >= 0.0
                    and build_avg_tps < args.min_newdb_perf_build_avg_tps
                ):
                    print(
                        f"ERROR: build_avg_tps={build_avg_tps} below min "
                        f"{args.min_newdb_perf_build_avg_tps}",
                        file=sys.stderr,
                    )
                    return 18
                print("NEWDB_PERF_SCRIPT_SUMMARY " + json.dumps(script_summary, sort_keys=True))
            else:
                print("NOTE: NEWDB_PERF_SUMMARY not found in newdb_perf output")

    ctest_cmd = ["ctest", "-R", "CiMicrobench", "--output-on-failure"]
    if build_config:
        ctest_cmd.extend(["-C", build_config])
    subprocess.check_call(ctest_cmd, cwd=b)

    if args.pressure_summary_json:
        summary_path = args.pressure_summary_json
        if not os.path.isabs(summary_path):
            summary_path = os.path.join(repo_root, summary_path)
        if not os.path.isfile(summary_path):
            print(f"ERROR: pressure summary not found: {summary_path}", file=sys.stderr)
            return 19
        try:
            with open(summary_path, "r", encoding="utf-8") as f:
                summary = json.load(f)
        except Exception as exc:
            print(f"ERROR: failed to parse pressure summary: {exc}", file=sys.stderr)
            return 20
        tps = float(summary.get("runtime_pressure_tps_est", 0.0))
        p95 = float(summary.get("runtime_pressure_batch_ms_p95", 0.0))
        where_fallback_scans_max = float(summary.get("where_fallback_scans_max", 0.0))
        where_plan_eq_sidecar_delta = float(summary.get("where_plan_eq_sidecar_count_delta", 0.0))
        if args.min_runtime_pressure_tps >= 0.0 and tps < args.min_runtime_pressure_tps:
            print(
                f"ERROR: runtime_pressure_tps_est={tps} below min {args.min_runtime_pressure_tps}",
                file=sys.stderr,
            )
            return 21
        if (
            args.max_runtime_pressure_batch_p95_ms >= 0.0
            and p95 > args.max_runtime_pressure_batch_p95_ms
        ):
            print(
                f"ERROR: runtime_pressure_batch_ms_p95={p95} exceeds max "
                f"{args.max_runtime_pressure_batch_p95_ms}",
                file=sys.stderr,
            )
            return 22
        if (
            args.max_where_fallback_scans_max >= 0.0
            and where_fallback_scans_max > args.max_where_fallback_scans_max
        ):
            print(
                f"ERROR: where_fallback_scans_max={where_fallback_scans_max} exceeds max "
                f"{args.max_where_fallback_scans_max}",
                file=sys.stderr,
            )
            return 23
        if (
            args.min_where_plan_eq_sidecar_count_delta >= 0.0
            and where_plan_eq_sidecar_delta < args.min_where_plan_eq_sidecar_count_delta
        ):
            print(
                f"ERROR: where_plan_eq_sidecar_count_delta={where_plan_eq_sidecar_delta} below min "
                f"{args.min_where_plan_eq_sidecar_count_delta}",
                file=sys.stderr,
            )
            return 24
        print(
            "PRESSURE_GATE_SUMMARY "
            + json.dumps(
                {
                    "summary": summary_path,
                    "runtime_pressure_tps_est": tps,
                    "runtime_pressure_batch_ms_p95": p95,
                    "where_fallback_scans_max": where_fallback_scans_max,
                    "where_plan_eq_sidecar_count_delta": where_plan_eq_sidecar_delta,
                    "benchmark_profile": summary.get("benchmark_profile", ""),
                    "runtime_walsync_mode": summary.get("runtime_walsync_mode", ""),
                },
                sort_keys=True,
            )
        )

    runtime_jsonl = args.runtime_jsonl
    if runtime_jsonl:
        if args.runtime_last_n != 0 and args.runtime_last_n < 2:
            print("ERROR: --runtime-last-n must be >= 2", file=sys.stderr)
            return 8
        if not os.path.isabs(runtime_jsonl):
            runtime_jsonl = os.path.join(repo_root, runtime_jsonl)
        if not os.path.isfile(runtime_jsonl):
            print(f"ERROR: runtime jsonl not found: {runtime_jsonl}", file=sys.stderr)
            return 6

        reporter = _resolve_runtime_report_binary(b, build_config)
        if not os.path.isfile(reporter):
            print(f"ERROR: runtime report tool not found: {reporter}", file=sys.stderr)
            return 7

        cmd = [reporter, "--input", runtime_jsonl]
        if args.runtime_last_n > 0:
            cmd.extend(["--last-n", str(args.runtime_last_n)])
        if args.runtime_run_id:
            cmd.extend(["--run-id", args.runtime_run_id])
        if args.runtime_label_prefix:
            cmd.extend(["--label-prefix", args.runtime_label_prefix])
        if args.min_vacuum_efficiency >= 0.0:
            cmd.extend(["--min-vacuum-efficiency", str(args.min_vacuum_efficiency)])
        if args.max_conflict_rate >= 0.0:
            cmd.extend(["--max-conflict-rate", str(args.max_conflict_rate)])
        if args.min_vacuum_efficiency_p50 >= 0.0:
            cmd.extend(["--min-vacuum-efficiency-p50", str(args.min_vacuum_efficiency_p50)])
        if args.max_conflict_rate_p95 >= 0.0:
            cmd.extend(["--max-conflict-rate-p95", str(args.max_conflict_rate_p95)])
        if args.max_txn_begin_lock_conflict_delta >= 0.0:
            cmd.extend(["--max-txn-begin-lock-conflict-delta", str(args.max_txn_begin_lock_conflict_delta)])
        if args.max_wal_compact_delta >= 0.0:
            cmd.extend(["--max-wal-compact-delta", str(args.max_wal_compact_delta)])
        if args.max_vacuum_compact_failure_delta >= 0.0:
            cmd.extend(
                ["--max-vacuum-compact-failure-delta", str(args.max_vacuum_compact_failure_delta)]
            )
        if args.min_vacuum_compact_reclaimed_bytes_delta >= 0.0:
            cmd.extend(
                [
                    "--min-vacuum-compact-reclaimed-bytes-delta",
                    str(args.min_vacuum_compact_reclaimed_bytes_delta),
                ]
            )
        if args.max_vacuum_queue_depth_peak >= 0.0:
            cmd.extend(["--max-vacuum-queue-depth-peak", str(args.max_vacuum_queue_depth_peak)])
        if args.max_wal_recovery_last_elapsed_ms >= 0.0:
            cmd.extend(
                ["--max-wal-recovery-last-elapsed-ms", str(args.max_wal_recovery_last_elapsed_ms)]
            )
        if args.max_lock_deadlock_detect_delta >= 0.0:
            cmd.extend(
                ["--max-lock-deadlock-detect-delta", str(args.max_lock_deadlock_detect_delta)]
            )
        if args.max_lock_deadlock_victim_delta >= 0.0:
            cmd.extend(
                ["--max-lock-deadlock-victim-delta", str(args.max_lock_deadlock_victim_delta)]
            )
        if args.max_lock_wait_max_ms_delta >= 0.0:
            cmd.extend(
                ["--max-lock-wait-max-ms-delta", str(args.max_lock_wait_max_ms_delta)]
            )
        if args.max_scheduler_throttle_delta >= 0.0:
            cmd.extend(
                ["--max-scheduler-throttle-delta", str(args.max_scheduler_throttle_delta)]
            )
        if args.min_wal_group_commit_batch_commits_delta >= 0.0:
            cmd.extend(
                [
                    "--min-wal-group-commit-batch-commits-delta",
                    str(args.min_wal_group_commit_batch_commits_delta),
                ]
            )
        if args.max_lsm_segment_count >= 0.0:
            cmd.extend(["--max-lsm-segment-count", str(args.max_lsm_segment_count)])
        if args.min_lsm_memtable_flush_delta >= 0.0:
            cmd.extend(["--min-lsm-memtable-flush-delta", str(args.min_lsm_memtable_flush_delta)])
        if args.max_lsm_read_segments_scanned_p95 >= 0.0:
            cmd.extend(["--max-lsm-read-segments-scanned-p95", str(args.max_lsm_read_segments_scanned_p95)])
        if args.min_lsm_compaction_bytes_amp_efficiency >= 0.0:
            cmd.extend(
                ["--min-lsm-compaction-bytes-amp-efficiency", str(args.min_lsm_compaction_bytes_amp_efficiency)]
            )
        out = subprocess.check_output(cmd, cwd=b, text=True).strip()
        # Keep one machine-readable summary line in CI logs.
        try:
            parsed = json.loads(out)
            print("RUNTIME_GATE_SUMMARY " + json.dumps(parsed, sort_keys=True))
        except Exception:
            print("RUNTIME_GATE_SUMMARY_RAW " + out)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
