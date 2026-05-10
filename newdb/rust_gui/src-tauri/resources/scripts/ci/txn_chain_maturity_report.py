#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""
Transaction-chain maturity report: runs ctest slices per dimension, parses pass rates,
prints weighted score + JSON. Clears common NEWDB_* in the child environment for repeatability.

Usage:
  python3 newdb/scripts/ci/txn_chain_maturity_report.py --build-dir newdb/build
  python3 newdb/scripts/ci/txn_chain_maturity_report.py --build-dir newdb/build --config Debug
  python3 newdb/scripts/ci/txn_chain_maturity_report.py --build-dir newdb/build --profile full
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import subprocess
import sys
from dataclasses import dataclass, asdict
from typing import Any

# Parent shells exporting these can skew isolation/readpath tests; drop for ctest children.
_ENV_CLEAR = (
    "NEWDB_TXN_ISOLATION_READPATH",
    "NEWDB_TXN_STMT_SAVEPOINT",
    "NEWDB_TXN_TRACE",
    "NEWDB_VISCHK",
    "NEWDB_WHERE_RESERVE_PREDICATE",
    "NEWDB_WHERE_RESERVE_RANGE",
    "NEWDB_LAZY_HEAP",
    "NEWDB_INDEX_CATALOG_ENFORCE",
)


@dataclass
class DimensionResult:
    id: str
    title: str
    weight: float
    regex: str
    tests_run: int
    tests_passed: int
    score_percent: float | None
    ctest_rc: int | None
    note: str
    exclude_regex: str | None = None


def _clean_env() -> dict[str, str]:
    env = dict(os.environ)
    for k in _ENV_CLEAR:
        env.pop(k, None)
    return env


def _ctest_cmd(build_dir: str, config: str | None, regex: str, exclude_regex: str | None) -> list[str]:
    out = ["ctest", "--test-dir", build_dir]
    if config:
        out += ["-C", config]
    out += ["-R", regex, "--output-on-failure"]
    if exclude_regex:
        out += ["-E", exclude_regex]
    return out


def _parse_ctest_summary(text: str) -> tuple[int | None, int | None, int | None]:
    """
    Returns (passed, total, rc_hint) from ctest stdout.
    Handles:
      '100% tests passed, 3 tests out of 3 total.'
      '100% tests passed, 0 tests failed out of 7'  (MSVC / newer CTest)
    """
    m = re.search(
        r"(\d+)% tests passed,\s*(\d+)\s*tests out of\s*(\d+)\s*total",
        text,
        re.IGNORECASE,
    )
    if m:
        passed = int(m.group(2))
        total = int(m.group(3))
        return passed, total, 0 if passed == total else 1

    m2 = re.search(
        r"(\d+)% tests passed,\s*(\d+)\s*tests failed out of\s*(\d+)",
        text,
        re.IGNORECASE,
    )
    if m2:
        failed = int(m2.group(2))
        total = int(m2.group(3))
        passed = total - failed
        return passed, total, 0 if failed == 0 else 1

    return None, None, None


def _run_dimension(
    build_dir: str,
    config: str | None,
    dim_id: str,
    title: str,
    weight: float,
    regex: str,
    exclude_regex: str | None,
) -> DimensionResult:
    cmd = _ctest_cmd(build_dir, config, regex, exclude_regex)
    proc = subprocess.run(
        cmd,
        cwd=build_dir,
        env=_clean_env(),
        text=True,
        capture_output=True,
        timeout=3600,
    )
    out = (proc.stdout or "") + "\n" + (proc.stderr or "")
    passed, total, _ = _parse_ctest_summary(out)

    note = ""
    if "No tests were found" in out or "No Tests Found" in out:
        return DimensionResult(
            id=dim_id,
            title=title,
            weight=weight,
            regex=regex,
            tests_run=0,
            tests_passed=0,
            score_percent=None,
            ctest_rc=proc.returncode,
            note="no_matching_tests",
            exclude_regex=exclude_regex,
        )

    if total is None or total == 0:
        return DimensionResult(
            id=dim_id,
            title=title,
            weight=weight,
            regex=regex,
            tests_run=0,
            tests_passed=0,
            score_percent=None,
            ctest_rc=proc.returncode,
            note="unparsed_output",
            exclude_regex=exclude_regex,
        )

    score = 100.0 * float(passed or 0) / float(total)
    if proc.returncode != 0 and passed == total:
        note = "ctest_nonzero_despite_summary"
    return DimensionResult(
        id=dim_id,
        title=title,
        weight=weight,
        regex=regex,
        tests_run=total,
        tests_passed=passed or 0,
        score_percent=round(score, 2),
        ctest_rc=proc.returncode,
        note=note,
        exclude_regex=exclude_regex,
    )


@dataclass
class DimensionSpec:
    id: str
    title: str
    weight: float
    regex: str
    exclude_regex: str | None = None


def _dimensions(profile: str) -> list[DimensionSpec]:
    # Weights sum to 100. Regex matches GTest names in newdb_tests.
    base: list[DimensionSpec] = [
        DimensionSpec("api_strict", "协调器 API 边界（无活跃事务/嵌套 BEGIN）", 10.0, "TxnChainStrict"),
        DimensionSpec("write_conflict", "写意图与等待策略", 18.0, "TxnWriteConflict"),
        DimensionSpec("file_lock", "文件锁与 runtime 计数", 12.0, "TxnFileLock"),
        DimensionSpec(
            "isolation",
            "隔离配置与 MVCC 可见性基线",
            22.0,
            "TxnIsolationConfig|TxnIsolationVisibility",
        ),
        DimensionSpec(
            "cli_shell_txn",
            "Shell 读路径与批量 DML/回滚",
            18.0,
            "TxnShellMultiEntrySnapshot|DemoWhereBatchDml",
        ),
        DimensionSpec("embedded", "嵌入契约与 undo 计数器", 10.0, "TxnEmbeddedContract|TxnUndoMetrics"),
        DimensionSpec(
            "wal_txn",
            "WAL/事务恢复与 vacuum（默认排除 Hybrid 驻留用例以控制耗时）",
            10.0,
            "DemoTxnWal|TxnAutoVacuum|RecoveryUndoChain",
            exclude_regex="Hybrid",
        ),
    ]
    if profile == "full":
        base = [d for d in base if d.id != "wal_txn"]
        base.append(
            DimensionSpec(
                "wal_txn",
                "WAL/事务恢复与 vacuum（含 Hybrid + WalRecoveryIndexed）",
                10.0,
                "DemoTxnWal|TxnAutoVacuum|RecoveryUndoChain|WalRecoveryIndexed",
                exclude_regex=None,
            )
        )
    return base


def _tier(score: float) -> str:
    if score >= 95.0:
        return "A"
    if score >= 80.0:
        return "B"
    if score >= 60.0:
        return "C"
    return "D"


def main() -> int:
    if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except OSError:
            pass

    ap = argparse.ArgumentParser(description="Transaction-chain maturity (weighted ctest slices).")
    ap.add_argument("--build-dir", required=True, help="CMake build directory containing CTestTestfile.cmake")
    ap.add_argument(
        "--config",
        default=os.environ.get("CMAKE_CONFIG", ""),
        help="Multi-config generator build type (e.g. Debug, Release). Empty to omit -C.",
    )
    ap.add_argument(
        "--profile",
        choices=("default", "full"),
        default="default",
        help="full: include WalRecoveryIndexed in wal_txn slice (slower).",
    )
    ap.add_argument("--json-out", default="", help="Write full report JSON to this path.")
    args = ap.parse_args()

    build_dir = os.path.abspath(args.build_dir)
    config = args.config.strip() or None

    dims = _dimensions(args.profile)
    results: list[DimensionResult] = []
    for spec in dims:
        results.append(
            _run_dimension(
                build_dir,
                config,
                spec.id,
                spec.title,
                spec.weight,
                spec.regex,
                spec.exclude_regex,
            )
        )

    active_weight = sum(r.weight for r in results if r.score_percent is not None)
    if active_weight <= 0:
        print("ERROR: no dimension produced a parseable test count.", file=sys.stderr)
        return 2

    weighted = sum(
        (r.weight / active_weight) * (r.score_percent or 0.0)
        for r in results
        if r.score_percent is not None
    )
    # Skipped dimensions (no tests): excluded from active_weight — re-normalize above already uses active_weight

    any_fail = any((r.ctest_rc or 0) != 0 for r in results if r.tests_run > 0)
    any_unparsed = any(r.note == "unparsed_output" for r in results)

    report: dict[str, Any] = {
        "overall_score_percent": round(weighted, 2),
        "maturity_tier": _tier(weighted),
        "platform": platform.system(),
        "build_dir": build_dir,
        "ctest_config": config or "",
        "profile": args.profile,
        "dimensions": [asdict(r) for r in results],
        "env_cleared_keys": list(_ENV_CLEAR),
    }

    # Human-readable
    def p(*args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("flush", True)
        print(*args, **kwargs)

    p("=== 事务链成熟度报告 (Transaction chain maturity) ===")
    p(f"平台: {report['platform']}  构建目录: {build_dir}  配置: {config or '(default)'}  profile={args.profile}")
    p(f"加权得分: {report['overall_score_percent']}%  等级: {report['maturity_tier']} (A>=95 B>=80 C>=60 D<60)")
    p()
    for r in results:
        sp = f"{r.score_percent}%" if r.score_percent is not None else "n/a"
        rc = "" if r.ctest_rc is None else f" rc={r.ctest_rc}"
        ex = f" -E {r.exclude_regex}" if r.exclude_regex else ""
        p(f"  [{r.id}] weight={r.weight}  {sp}  ({r.tests_passed}/{r.tests_run}){rc}{ex}  {r.title}")
        if r.note:
            p(f"         note={r.note}")
    p()
    p("说明: 子进程已清除常见 NEWDB_* 环境变量，见 newdb/docs/testing/ENVIRONMENT_BASELINE.md")

    if args.json_out:
        json_path = os.path.abspath(args.json_out)
        json_dir = os.path.dirname(json_path)
        if json_dir:
            os.makedirs(json_dir, exist_ok=True)
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

    if any_unparsed:
        return 3
    if any_fail:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
