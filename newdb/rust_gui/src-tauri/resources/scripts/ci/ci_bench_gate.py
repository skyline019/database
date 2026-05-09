#!/usr/bin/env python3
"""CI gate: smoke binary + microbench gtest (through ctest).

Note: `newdb_smoke` requires a subcommand and returns non-zero on usage output,
so the gate runs a minimal create/append/load flow on a temp `.bin`.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys
import tempfile
from typing import Any, Optional


def _apply_recommended_thresholds_blob(args: argparse.Namespace, blob: Any) -> None:
    """Merge a `recommended_thresholds_<profile>` style dict into argparse args.

    Only fills numeric attributes still at sentinel `-1` (float) / negative (int) so that
    profile defaults and explicit CLI flags continue to win.
    """
    thr: Optional[dict] = None
    if isinstance(blob, dict):
        prof = getattr(args, "_profile_effective", getattr(args, "profile", "local"))
        prof_key = f"recommended_thresholds_{prof}"
        if isinstance(blob.get(prof_key), dict):
            thr = blob[prof_key]
        elif isinstance(blob.get("recommended_thresholds_pr"), dict):
            thr = blob["recommended_thresholds_pr"]
        elif "profile" in blob:
            thr = blob
    if not isinstance(thr, dict):
        return
    for key, val in thr.items():
        if not hasattr(args, key):
            continue
        cur = getattr(args, key)
        if isinstance(cur, bool):
            continue
        if isinstance(cur, float) and cur >= 0.0:
            continue
        if isinstance(cur, int) and cur >= 0:
            continue
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            setattr(args, key, float(val))


def _apply_recommended_thresholds_path(args: argparse.Namespace, path: str) -> None:
    """Load a recommended-thresholds JSON file and merge keys into args (sentinel only)."""
    if not path or not os.path.isfile(path):
        return
    try:
        with open(path, encoding="utf-8") as f:
            blob = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"NOTE: could not apply recommended thresholds from {path}: {exc}", file=sys.stderr)
        return
    _apply_recommended_thresholds_blob(args, blob)


def _resolve_workspace_root(here: str) -> str:
    """Checkout root for monorepo CMake (``cmake -S newdb -B build`` → ``<root>/build``).

    ``here`` is the directory containing this script (``.../<root>/newdb/scripts/ci``).

    When ``<root>/.git`` exists and ``here`` resolves to ``<root>/newdb/scripts/ci``,
    return ``<root>``. Otherwise return the ``newdb`` project directory (standalone layout).
    """
    newdb_root = os.path.dirname(os.path.dirname(here))
    parent = os.path.dirname(newdb_root)
    if parent and (
        (os.path.isdir(os.path.join(parent, ".git")) or os.path.isfile(os.path.join(parent, ".git")))
        and os.path.normpath(newdb_root) == os.path.normpath(os.path.join(parent, "newdb"))
    ):
        return parent
    return newdb_root


def _detect_local_host_slug() -> str:
    """Best-effort local host_slug shaped like capture_baseline.py output (without compiler tail)."""
    try:
        cpu_count = str(os.cpu_count() or 0)
    except Exception:
        cpu_count = "0"
    bits = [
        platform.system().lower().replace(" ", "_"),
        platform.machine().lower().replace(" ", "_"),
        cpu_count,
    ]
    return "__".join(b for b in bits if b)


def _select_baseline_host(hosts: list, prefer_slug: str, local_slug: str) -> Optional[dict]:
    """Pick the closest host entry from `hosts[]` per plan precedence (slug → 3-seg → 2-seg → newest)."""
    if not isinstance(hosts, list) or not hosts:
        return None

    def first_match(predicate) -> Optional[dict]:
        for h in hosts:
            if isinstance(h, dict) and predicate(h):
                return h
        return None

    target = prefer_slug or local_slug
    if target:
        same = first_match(lambda h: h.get("host_slug") == target)
        if same is not None:
            return same
        target_bits = target.split("__")
        if len(target_bits) >= 3:
            three = first_match(
                lambda h: isinstance(h.get("host_slug"), str)
                and h["host_slug"].split("__")[:3] == target_bits[:3]
            )
            if three is not None:
                return three
        if len(target_bits) >= 2:
            two = first_match(
                lambda h: isinstance(h.get("host_slug"), str)
                and h["host_slug"].split("__")[:2] == target_bits[:2]
            )
            if two is not None:
                return two

    def _ts(h: dict) -> str:
        return str(h.get("generated_at_utc") or "")

    return max((h for h in hosts if isinstance(h, dict)), key=_ts, default=None)


def _apply_baseline_host_index(args: argparse.Namespace) -> None:
    """Walk the cross-host `host_index.json`, pick a manifest, and merge recommended thresholds.

    Always best-effort: missing file / parse errors only print a NOTE (matches plan §C.4).
    The selected slug is stashed at `args._baseline_host_slug_used` for gate-fail diagnostics.
    """
    idx_path = args.baseline_host_index
    if not idx_path:
        return
    if not os.path.isabs(idx_path):
        here_dir = os.path.dirname(os.path.abspath(__file__))
        repo_root = _resolve_workspace_root(here_dir)
        idx_path = os.path.join(repo_root, idx_path.replace("/", os.sep))
    if not os.path.isfile(idx_path):
        print(f"NOTE: --baseline-host-index file not found: {idx_path}", file=sys.stderr)
        return
    try:
        with open(idx_path, encoding="utf-8") as f:
            blob = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"NOTE: could not parse --baseline-host-index: {exc}", file=sys.stderr)
        return
    hosts = blob.get("hosts") if isinstance(blob, dict) else None
    if not isinstance(hosts, list) or not hosts:
        print("NOTE: --baseline-host-index has no hosts[] entries", file=sys.stderr)
        return
    chosen = _select_baseline_host(
        hosts,
        prefer_slug=getattr(args, "baseline_prefer_host_slug", "") or "",
        local_slug=_detect_local_host_slug(),
    )
    if chosen is None:
        print("NOTE: --baseline-host-index could not pick a host entry", file=sys.stderr)
        return
    setattr(args, "_baseline_host_slug_used", str(chosen.get("host_slug") or ""))
    manifest_path = chosen.get("manifest")
    if not isinstance(manifest_path, str) or not manifest_path:
        return
    if not os.path.isabs(manifest_path):
        manifest_path = os.path.join(os.path.dirname(idx_path), manifest_path)
    if not os.path.isfile(manifest_path):
        print(f"NOTE: baseline manifest missing: {manifest_path}", file=sys.stderr)
        return
    try:
        with open(manifest_path, encoding="utf-8") as f:
            manifest = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"NOTE: could not parse baseline manifest {manifest_path}: {exc}", file=sys.stderr)
        return
    _apply_recommended_thresholds_blob(args, manifest)


def _apply_profile_defaults(args: argparse.Namespace) -> None:
    """Set default gate thresholds by profile; explicit CLI values win if already set.

    Convention: argparse defaults for numeric gates are -1 (disabled). Profiles only
    tighten/enable when still at the sentinel default by comparing attribute identity
    is not possible; we use a post-parse pass storing original argv presence.

    Here we only apply when --profile was passed (detected via attribute set below).
    """
    prof = getattr(args, "_profile_effective", "local")
    if prof == "local":
        return
    if prof == "pr":
        if args.max_wal_recovery_last_elapsed_ms < 0.0:
            args.max_wal_recovery_last_elapsed_ms = 2000.0
        if args.max_vacuum_compact_failure_delta < 0.0:
            args.max_vacuum_compact_failure_delta = 0.0
        if args.max_compact_debt_bytes_peak < 0.0:
            args.max_compact_debt_bytes_peak = 1.0e11
        # Storage-health soft gates (enabled by default on PR; permissive ceilings — tighten per host baseline).
        if args.max_table_storage_health_fragmentation_ratio < 0.0:
            args.max_table_storage_health_fragmentation_ratio = 1.0
        if args.max_vacuum_health_bonus_last < 0.0:
            args.max_vacuum_health_bonus_last = 1.0e15
    elif prof == "nightly":
        if args.max_wal_recovery_last_elapsed_ms < 0.0:
            args.max_wal_recovery_last_elapsed_ms = 5000.0
        if args.min_page_cache_hit_ratio < 0.0:
            args.min_page_cache_hit_ratio = 0.0
        if args.max_where_scan_amplification < 0.0:
            args.max_where_scan_amplification = 1.0e9
        if args.max_memory_budget_reject_delta < 0.0:
            args.max_memory_budget_reject_delta = 1.0e9
    elif prof == "release":
        if args.max_wal_recovery_last_elapsed_ms < 0.0:
            args.max_wal_recovery_last_elapsed_ms = 2000.0
        if args.max_vacuum_compact_failure_delta < 0.0:
            args.max_vacuum_compact_failure_delta = 0.0
        if args.max_lazy_materialize_count_delta < 0.0:
            args.max_lazy_materialize_count_delta = 0.0
        if args.max_lazy_materialize_rows_total_delta < 0.0:
            args.max_lazy_materialize_rows_total_delta = 0.0
        if args.max_compact_debt_bytes_peak < 0.0:
            args.max_compact_debt_bytes_peak = 1.0e11
        if args.min_page_cache_hit_ratio < 0.0:
            args.min_page_cache_hit_ratio = 0.0
        if args.max_memory_budget_reject_delta < 0.0:
            args.max_memory_budget_reject_delta = 1.0e9
        if args.max_table_storage_health_fragmentation_ratio < 0.0:
            args.max_table_storage_health_fragmentation_ratio = 1.0


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


def _emit_gate_fail_json(
    args: argparse.Namespace,
    repo_root: str,
    resolved_build_dir: str,
    build_config: Optional[str],
    stage: str,
    exit_code: int,
    extra: Optional[dict[str, Any]] = None,
) -> None:
    """Best-effort machine-readable failure JSON for CI artifacts (any stage)."""
    outp = getattr(args, "gate_fail_json_out", "") or ""
    if not outp:
        return
    fail: dict[str, Any] = {
        "status": "fail",
        "tool": "ci_bench_gate",
        "stage": stage,
        "exit_code": exit_code,
        "profile": getattr(args, "_profile_effective", getattr(args, "profile", "local")),
        "build_dir_arg": args.build_dir,
        "resolved_build_dir": resolved_build_dir,
        "build_config": build_config or "",
        "baseline_host_slug_used": getattr(args, "_baseline_host_slug_used", ""),
    }
    if extra:
        fail.update(extra)
    if not os.path.isabs(outp):
        outp = os.path.join(repo_root, outp)
    try:
        parent = os.path.dirname(outp)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(outp, "w", encoding="utf-8") as f:
            json.dump(fail, f, indent=2)
    except Exception as exc:
        print(f"NOTE: failed to write gate-fail json: {exc}", file=sys.stderr)
    print(json.dumps(fail, indent=2), file=sys.stderr)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--profile",
        choices=("local", "pr", "nightly", "release"),
        default="local",
        help="Preset gate defaults for storage/WAL/query groups (explicit flags still override).",
    )
    p.add_argument(
        "--gate-fail-json-out",
        default="",
        help="When set, write machine-readable failure payload on non-zero exit (best-effort).",
    )
    p.add_argument(
        "--min-page-cache-hit-ratio",
        type=float,
        default=-1.0,
        help="Forwarded to newdb_runtime_report when --runtime-jsonl is set.",
    )
    p.add_argument(
        "--max-where-scan-amplification",
        type=float,
        default=-1.0,
        help="Forwarded to newdb_runtime_report when --runtime-jsonl is set.",
    )
    p.add_argument(
        "--max-memory-budget-reject-delta",
        type=float,
        default=-1.0,
        help="Forwarded to newdb_runtime_report when --runtime-jsonl is set.",
    )
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
        help="Path to runtime snapshot jsonl for extra gate checks (optional). Lines must satisfy "
        "scripts/validate/validate_runtime_stats.py; soak markers from NEWDB_SOAK_HINT_JSONL are not valid "
        "unless merged with compatible runtime rows or validated separately.",
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
    p.add_argument(
        "--max-lazy-materialize-count-delta",
        type=float,
        default=-1.0,
        help="Fail gate when lazy_materialize_count_delta exceeds this (disabled when <0; optional soak gate).",
    )
    p.add_argument(
        "--max-lazy-materialize-rows-total-delta",
        type=float,
        default=-1.0,
        help="Fail gate when lazy_materialize_rows_total_delta exceeds this (disabled when <0).",
    )
    p.add_argument(
        "--max-table-storage-health-fragmentation-ratio",
        type=float,
        default=-1.0,
        help="Fail gate when any runtime row with a storage-health sample exceeds this fragmentation "
        "ratio (disabled when <0; for soak with NEWDB_VACUUM_QUEUE_USE_HEALTH=1).",
    )
    p.add_argument(
        "--max-table-storage-health-dead-bytes",
        type=float,
        default=-1.0,
        help="Fail gate when dead_bytes peak from storage-health samples exceeds this (disabled when <0; "
        "proxy for compact_debt / §5.3 style debt ceilings in nightly).",
    )
    p.add_argument(
        "--max-vacuum-health-bonus-last",
        type=float,
        default=-1.0,
        help="Fail gate when max vacuum_health_bonus_last across snapshots exceeds this (disabled when <0).",
    )
    p.add_argument(
        "--max-compact-debt-bytes-peak",
        type=float,
        default=-1.0,
        help="Fail gate when compact_debt_bytes_peak from runtime report exceeds this (disabled when <0).",
    )
    p.add_argument(
        "--release-grade",
        action="store_true",
        help="When set, tighten unset lazy-materialize runtime gates to 0 (Release-style hardening).",
    )
    p.add_argument(
        "--recommended-thresholds-json",
        default="",
        help="Optional JSON file (e.g. nightly_soak_hints recommended_thresholds_pr) "
        "to fill gate thresholds still at sentinel -1 after profile defaults.",
    )
    p.add_argument(
        "--baseline-host-index",
        default="",
        help="Optional path to a cross-host `host_index.json` produced by capture_baseline.py "
        "with --cross-host-baseline-dir; thresholds are merged via nearest-host-slug match.",
    )
    p.add_argument(
        "--baseline-prefer-host-slug",
        default="",
        help="Optional override host_slug for --baseline-host-index lookup (skips auto detection).",
    )
    args = p.parse_args()
    setattr(args, "_profile_effective", args.profile)
    _apply_profile_defaults(args)
    if args.recommended_thresholds_json:
        rp = args.recommended_thresholds_json
        if not os.path.isabs(rp):
            here_dir = os.path.dirname(os.path.abspath(__file__))
            repo_root = _resolve_workspace_root(here_dir)
            rp = os.path.join(repo_root, rp.replace("/", os.sep))
        _apply_recommended_thresholds_path(args, rp)
    setattr(args, "_baseline_host_slug_used", "")
    if args.baseline_host_index:
        _apply_baseline_host_index(args)
    if args.release_grade:
        if args.max_lazy_materialize_count_delta < 0.0:
            args.max_lazy_materialize_count_delta = 0.0
        if args.max_lazy_materialize_rows_total_delta < 0.0:
            args.max_lazy_materialize_rows_total_delta = 0.0
        if args.max_lock_deadlock_victim_delta < 0.0:
            args.max_lock_deadlock_victim_delta = 0.0
    here = os.path.dirname(os.path.abspath(__file__))
    repo_root = _resolve_workspace_root(here)
    newdb_proj_root = os.path.dirname(os.path.dirname(here))
    if os.path.isabs(args.build_dir):
        b = args.build_dir
    else:
        b = os.path.join(repo_root, args.build_dir)
        # Monorepo: verify_clean_reconfigure uses -B under newdb/ (e.g. newdb/build_ci_windows), not workspace root.
        if not os.path.isdir(b):
            alt = os.path.join(newdb_proj_root, args.build_dir)
            if os.path.isdir(alt):
                b = alt
    if not os.path.isdir(b):
        print(f"ERROR: build dir not found: {b}", file=sys.stderr)
        _emit_gate_fail_json(
            args,
            repo_root,
            b,
            None,
            "build_dir_missing",
            2,
            {"reason": "build directory does not exist or is not a directory"},
        )
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
                try:
                    out = subprocess.check_output(cmd, cwd=b, text=True)
                except subprocess.CalledProcessError as exc:
                    err_txt = (exc.stderr or "").strip() if exc.stderr else ""
                    _emit_gate_fail_json(
                        args,
                        repo_root,
                        b,
                        build_config,
                        "newdb_smoke_exe",
                        exc.returncode,
                        {"cmd": " ".join(cmd), "stderr": err_txt},
                    )
                    return 26
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
                    _emit_gate_fail_json(
                        args,
                        repo_root,
                        b,
                        build_config,
                        "newdb_smoke_parse",
                        9,
                        {"cmd": " ".join(cmd)},
                    )
                    return 9
                if parsed.get("tool") != "newdb_smoke" or parsed.get("status") != "ok":
                    print("ERROR: smoke JSON reported failure: " + json.dumps(parsed, sort_keys=True),
                          file=sys.stderr)
                    _emit_gate_fail_json(
                        args,
                        repo_root,
                        b,
                        build_config,
                        "newdb_smoke",
                        10,
                        {"cmd": " ".join(cmd), "smoke": parsed},
                    )
                    return 10
    else:
        print(f"NOTE: skip newdb_smoke (not built): {smoke}")

    if args.run_newdb_perf:
        perf = _resolve_perf_binary(b, build_config)
        if not os.path.isfile(perf):
            print(f"ERROR: newdb_perf not found: {perf}", file=sys.stderr)
            _emit_gate_fail_json(
                args, repo_root, b, build_config, "newdb_perf_missing", 11, {"perf": perf}
            )
            return 11
        demo_exe = args.newdb_perf_demo_exe
        if not demo_exe:
            demo_exe = _resolve_demo_binary(b, build_config)
        if not os.path.isabs(demo_exe):
            demo_exe = os.path.join(repo_root, demo_exe)
        if not os.path.isfile(demo_exe):
            print(f"ERROR: demo binary for newdb_perf not found: {demo_exe}", file=sys.stderr)
            _emit_gate_fail_json(
                args, repo_root, b, build_config, "newdb_perf_demo_missing", 15, {"demo_exe": demo_exe}
            )
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
                _emit_gate_fail_json(args, repo_root, b, build_config, "newdb_perf_parse", 12, {})
                return 12
            if parsed.get("tool") != "newdb_perf" or parsed.get("status") != "ok":
                print("ERROR: newdb_perf reported failure: " + json.dumps(parsed, sort_keys=True),
                      file=sys.stderr)
                _emit_gate_fail_json(
                    args, repo_root, b, build_config, "newdb_perf", 13, {"parsed": parsed}
                )
                return 13
            elapsed_ms = float(parsed.get("elapsed_ms", 0.0))
            if args.max_newdb_perf_elapsed_ms >= 0.0 and elapsed_ms > args.max_newdb_perf_elapsed_ms:
                print(
                    f"ERROR: newdb_perf elapsed_ms={elapsed_ms} exceeds max "
                    f"{args.max_newdb_perf_elapsed_ms}",
                    file=sys.stderr,
                )
                _emit_gate_fail_json(
                    args,
                    repo_root,
                    b,
                    build_config,
                    "newdb_perf_elapsed",
                    14,
                    {"elapsed_ms": elapsed_ms, "max_ms": args.max_newdb_perf_elapsed_ms},
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
                    _emit_gate_fail_json(
                        args,
                        repo_root,
                        b,
                        build_config,
                        "newdb_perf_script_summary",
                        16,
                        {"script_summary": script_summary},
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
                    _emit_gate_fail_json(
                        args,
                        repo_root,
                        b,
                        build_config,
                        "newdb_perf_txn_tps",
                        17,
                        {"txn_avg_tps": txn_avg_tps, "min": args.min_newdb_perf_txn_avg_tps},
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
                    _emit_gate_fail_json(
                        args,
                        repo_root,
                        b,
                        build_config,
                        "newdb_perf_build_tps",
                        18,
                        {"build_avg_tps": build_avg_tps, "min": args.min_newdb_perf_build_avg_tps},
                    )
                    return 18
                print("NEWDB_PERF_SCRIPT_SUMMARY " + json.dumps(script_summary, sort_keys=True))
            else:
                print("NOTE: NEWDB_PERF_SUMMARY not found in newdb_perf output")

    # Microbench budget + dispatch routing regression (phase-2 verb fast path).
    ctest_cmd = ["ctest", "-R", "CiMicrobench|DispatchRouting", "--output-on-failure"]
    if build_config:
        ctest_cmd.extend(["-C", build_config])
    try:
        subprocess.check_call(ctest_cmd, cwd=b)
    except subprocess.CalledProcessError as exc:
        _emit_gate_fail_json(
            args,
            repo_root,
            b,
            build_config,
            "ctest",
            exc.returncode,
            {"cmd": " ".join(ctest_cmd)},
        )
        return 25

    if args.pressure_summary_json:
        summary_path = args.pressure_summary_json
        if not os.path.isabs(summary_path):
            summary_path = os.path.join(repo_root, summary_path)
        if not os.path.isfile(summary_path):
            print(f"ERROR: pressure summary not found: {summary_path}", file=sys.stderr)
            _emit_gate_fail_json(
                args,
                repo_root,
                b,
                build_config,
                "pressure_summary_missing",
                19,
                {"summary_path": summary_path},
            )
            return 19
        try:
            with open(summary_path, "r", encoding="utf-8") as f:
                summary = json.load(f)
        except Exception as exc:
            print(f"ERROR: failed to parse pressure summary: {exc}", file=sys.stderr)
            _emit_gate_fail_json(
                args,
                repo_root,
                b,
                build_config,
                "pressure_summary_parse",
                20,
                {"error": str(exc)},
            )
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
            _emit_gate_fail_json(
                args,
                repo_root,
                b,
                build_config,
                "pressure_tps",
                21,
                {"tps": tps, "min": args.min_runtime_pressure_tps},
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
            _emit_gate_fail_json(
                args,
                repo_root,
                b,
                build_config,
                "pressure_p95",
                22,
                {"p95": p95, "max": args.max_runtime_pressure_batch_p95_ms},
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
            _emit_gate_fail_json(
                args,
                repo_root,
                b,
                build_config,
                "pressure_where_fallback",
                23,
                {"where_fallback_scans_max": where_fallback_scans_max, "max": args.max_where_fallback_scans_max},
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
            _emit_gate_fail_json(
                args,
                repo_root,
                b,
                build_config,
                "pressure_eq_sidecar_delta",
                24,
                {
                    "where_plan_eq_sidecar_count_delta": where_plan_eq_sidecar_delta,
                    "min": args.min_where_plan_eq_sidecar_count_delta,
                },
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
            _emit_gate_fail_json(
                args,
                repo_root,
                b,
                build_config,
                "runtime_args",
                8,
                {"reason": "--runtime-last-n must be >= 2 when non-zero"},
            )
            return 8
        if not os.path.isabs(runtime_jsonl):
            runtime_jsonl = os.path.join(repo_root, runtime_jsonl)
        if not os.path.isfile(runtime_jsonl):
            print(f"ERROR: runtime jsonl not found: {runtime_jsonl}", file=sys.stderr)
            _emit_gate_fail_json(
                args,
                repo_root,
                b,
                build_config,
                "runtime_jsonl_missing",
                6,
                {"runtime_jsonl": runtime_jsonl},
            )
            return 6

        reporter = _resolve_runtime_report_binary(b, build_config)
        if not os.path.isfile(reporter):
            print(f"ERROR: runtime report tool not found: {reporter}", file=sys.stderr)
            _emit_gate_fail_json(
                args,
                repo_root,
                b,
                build_config,
                "newdb_runtime_report_missing",
                7,
                {"reporter": reporter},
            )
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
        if args.max_lazy_materialize_count_delta >= 0.0:
            cmd.extend(
                ["--max-lazy-materialize-count-delta", str(args.max_lazy_materialize_count_delta)]
            )
        if args.max_lazy_materialize_rows_total_delta >= 0.0:
            cmd.extend(
                [
                    "--max-lazy-materialize-rows-total-delta",
                    str(args.max_lazy_materialize_rows_total_delta),
                ]
            )
        if args.max_table_storage_health_fragmentation_ratio >= 0.0:
            cmd.extend(
                [
                    "--max-table-storage-health-fragmentation-ratio",
                    str(args.max_table_storage_health_fragmentation_ratio),
                ]
            )
        if args.max_table_storage_health_dead_bytes >= 0.0:
            cmd.extend(
                [
                    "--max-table-storage-health-dead-bytes",
                    str(args.max_table_storage_health_dead_bytes),
                ]
            )
        if args.max_vacuum_health_bonus_last >= 0.0:
            cmd.extend(
                [
                    "--max-vacuum-health-bonus-last",
                    str(args.max_vacuum_health_bonus_last),
                ]
            )
        if args.max_compact_debt_bytes_peak >= 0.0:
            cmd.extend(
                [
                    "--max-compact-debt-bytes-peak",
                    str(args.max_compact_debt_bytes_peak),
                ]
            )
        if args.min_page_cache_hit_ratio >= 0.0:
            cmd.extend(["--min-page-cache-hit-ratio", str(args.min_page_cache_hit_ratio)])
        if args.max_where_scan_amplification >= 0.0:
            cmd.extend(["--max-where-scan-amplification", str(args.max_where_scan_amplification)])
        if args.max_memory_budget_reject_delta >= 0.0:
            cmd.extend(["--max-memory-budget-reject-delta", str(args.max_memory_budget_reject_delta)])
        proc = subprocess.run(cmd, cwd=b, text=True, capture_output=True)
        out = (proc.stdout or "").strip()
        if proc.returncode != 0:
            fail: dict[str, Any] = {
                "status": "fail",
                "tool": "ci_bench_gate",
                "stage": "newdb_runtime_report",
                "exit_code": proc.returncode,
                "profile": getattr(args, "_profile_effective", args.profile),
                "build_dir_arg": args.build_dir,
                "resolved_build_dir": b,
                "build_config": build_config or "",
                "stderr": proc.stderr or "",
                "stdout": out,
            }
            if args.gate_fail_json_out:
                outp = args.gate_fail_json_out
                if not os.path.isabs(outp):
                    outp = os.path.join(repo_root, outp)
                try:
                    parent = os.path.dirname(outp)
                    if parent:
                        os.makedirs(parent, exist_ok=True)
                    with open(outp, "w", encoding="utf-8") as f:
                        json.dump(fail, f, indent=2)
                except Exception as exc:
                    print(f"NOTE: failed to write gate-fail json: {exc}", file=sys.stderr)
            print(json.dumps(fail, indent=2), file=sys.stderr)
            return proc.returncode
        # Keep one machine-readable summary line in CI logs.
        try:
            parsed = json.loads(out)
            print("RUNTIME_GATE_SUMMARY " + json.dumps(parsed, sort_keys=True))
        except Exception:
            print("RUNTIME_GATE_SUMMARY_RAW " + out)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
