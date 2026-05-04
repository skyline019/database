#!/usr/bin/env python3
"""Phase-0 baseline helper: build, test, optional bench gate, runtime stats validation.

Run from repo root (or set NEWDB_REPO_ROOT). Produces no artifacts by default except
console guidance; use --print-jsonl-hints for workload reminders.

With --emit-baseline-dir, writes a unified baseline layout:
  baseline/runtime_stats.jsonl   (optional copy from --runtime-jsonl-input)
  baseline/runtime_report.json (from newdb_runtime_report when JSONL present)
  baseline/ctest.log           (ctest transcript when ctest ran)
  baseline/manifest.json       (environment + git + workload profile + bench_gate_profile +
                                recommended_ci_bench_gate_cli for contract JSONL gate)
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _repo_root(here: Path) -> Path:
    newdb_root = here.parent.parent if here.name == "ci" else here.parent
    return Path(os.environ.get("NEWDB_REPO_ROOT", newdb_root))


def _try_git_commit(repo: Path) -> str:
    try:
        r = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(repo),
            capture_output=True,
            text=True,
            check=False,
        )
        if r.returncode == 0:
            return r.stdout.strip()
    except OSError:
        pass
    return ""


def _try_compiler_id() -> str:
    for cc in ("CXX", "CC"):
        v = os.environ.get(cc, "")
        if v:
            return f"{cc}={v}"
    return ""


def _cpu_count() -> int:
    try:
        return os.cpu_count() or 0
    except Exception:
        return 0


def _relevant_env_keys() -> dict[str, str]:
    keys = [
        "CMAKE_BUILD_TYPE",
        "CXX",
        "CC",
        "NEWDB_REPO_ROOT",
        "NEWDB_INDEX_CATALOG_ENFORCE",
        "NEWDB_TXN_ISOLATION_READPATH",
        "NEWDB_PAGE_CACHE_MAX_BYTES",
        "NEWDB_MEMORY_BUDGET_MAX_BYTES",
        "NEWDB_VACUUM_QUEUE_USE_HEALTH",
    ]
    out: dict[str, str] = {}
    for k in keys:
        if k in os.environ:
            out[k] = os.environ[k]
    return out


def _workload_profile_hints(profile: str) -> dict[str, Any]:
    """Label / workload hints keyed by profile (documentation + manifest)."""
    base = {
        "small": {
            "description": "Local quick validation",
            "suggested_labels": ["post_smoke"],
            "recommended_ctest_args": ["--output-on-failure", "-L", "newdb"],
            "recommended_ctest_cli": "ctest --test-dir <BUILD_DIR> --output-on-failure -L newdb",
        },
        "storage": {
            "description": "insert/update/delete/vacuum coverage",
            "suggested_labels": ["post_insert", "post_update", "post_delete", "post_vacuum"],
            "recommended_ctest_args": ["--output-on-failure", "-L", "newdb", "-R", "StorageSoak"],
            "recommended_ctest_cli": "ctest --test-dir <BUILD_DIR> --output-on-failure -L newdb -R StorageSoak",
        },
        "query": {
            "description": "PAGE/WHERE/COUNT/SUM/AVG style workload",
            "suggested_labels": ["post_where", "post_page", "post_count"],
            "recommended_ctest_args": [
                "--output-on-failure",
                "-L",
                "newdb",
                "-R",
                "DemoWhere|ShowPlan|QueryTableStats|DemoMdb|TxnShellMultiEntrySnapshot",
            ],
            "recommended_ctest_cli": "ctest --test-dir <BUILD_DIR> --output-on-failure -L newdb "
            "-R DemoWhere|ShowPlan|QueryTableStats|DemoMdb|TxnShellMultiEntrySnapshot",
        },
        "recovery": {
            "description": "WAL checkpoint + recover",
            "suggested_labels": ["post_wal_recover", "post_checkpoint"],
            "recommended_ctest_args": [
                "--output-on-failure",
                "-L",
                "newdb",
                "-R",
                "WalRecoveryIndexed|WalSegmentScanner|WalCodec|WalTruncate|WalConcurrency|DemoTxnWal",
            ],
            "recommended_ctest_cli": "ctest --test-dir <BUILD_DIR> --output-on-failure -L newdb "
            "-R WalRecoveryIndexed|WalSegmentScanner|WalCodec|WalTruncate|WalConcurrency|DemoTxnWal",
        },
        "all": {
            "description": "Nightly/Release full matrix",
            "suggested_labels": [
                "post_insert",
                "post_where",
                "post_vacuum",
                "post_wal_recover",
            ],
            "recommended_ctest_args": ["--output-on-failure", "-L", "newdb"],
            "recommended_ctest_cli": "ctest --test-dir <BUILD_DIR> --output-on-failure -L newdb",
        },
    }
    return base.get(profile, base["all"])


def main() -> int:
    p = argparse.ArgumentParser(description="newdb closed-loop baseline capture helper")
    p.add_argument(
        "--build-dir",
        default="build",
        help="CMake build directory (relative to newdb/ when not absolute)",
    )
    p.add_argument(
        "--ctest-config",
        default="",
        help="Multi-config generator build type (e.g. RelWithDebInfo). Passed as ctest -C on Windows.",
    )
    p.add_argument(
        "--ctest-regex",
        default="",
        help="Optional -R regex passed to ctest (recorded in manifest).",
    )
    p.add_argument(
        "--workload-profile",
        choices=("small", "storage", "query", "recovery", "all"),
        default="small",
        help="Workload profile for manifest hints and recommended JSONL labels.",
    )
    p.add_argument(
        "--bench-gate-profile",
        choices=("local", "pr", "nightly", "release"),
        default="local",
        help="Forwarded as --profile when running ci_bench_gate.py (recorded in emit-baseline manifest).",
    )
    p.add_argument("--skip-ctest", action="store_true", help="Skip ctest after configure/build")
    p.add_argument("--skip-bench-gate", action="store_true", help="Skip scripts/ci/ci_bench_gate.py")
    p.add_argument(
        "--print-jsonl-hints",
        action="store_true",
        help="Print recommended runtime_stats.jsonl labels (insert/where/vacuum/recover)",
    )
    p.add_argument(
        "--validate-runtime-fixture",
        action="store_true",
        help="Run validate_runtime_stats.py on scripts/ci/fixtures/runtime_stats_bench_gate_minimal.jsonl",
    )
    p.add_argument(
        "--write-archive-manifest",
        metavar="PATH",
        help="Write a JSON manifest (v2 archive contract) to PATH for CI/Nightly artifacts",
    )
    p.add_argument(
        "--emit-archive-contract",
        action="store_true",
        help="Print v2 archive contract (JSON) to stdout and exit",
    )
    p.add_argument(
        "--emit-baseline-dir",
        metavar="DIR",
        help="Write baseline/{runtime_stats.jsonl,runtime_report.json,ctest.log,manifest.json} under DIR",
    )
    p.add_argument(
        "--runtime-jsonl-input",
        metavar="PATH",
        help="When used with --emit-baseline-dir, copy this file to baseline/runtime_stats.jsonl",
    )
    args = p.parse_args()

    here = Path(__file__).resolve().parent
    repo = _repo_root(here)
    build = Path(args.build_dir)
    if not build.is_absolute():
        build = repo / build

    contract = {
        "contract_version": 2,
        "validate_runtime_stats": str(repo / "scripts" / "validate" / "validate_runtime_stats.py"),
        "fixture_minimal_jsonl": str(repo / "scripts" / "ci" / "fixtures" / "runtime_stats_bench_gate_minimal.jsonl"),
        "fixtures_readme": str(repo / "scripts" / "ci" / "fixtures" / "README.md"),
        "nightly_matrix_doc": str(repo / "docs" / "ci" / "PERF_AND_CI_BUDGETS.md"),
        "suggested_artifact_name": "runtime_stats_baseline.jsonl",
        "suggested_manifest_artifact_name": "newdb-runtime-archive-manifest",
        "suggested_manifest_relative_path": "scripts/results/ci_baseline_manifest.json",
        "jsonl_label_hints": [
            "post_insert",
            "post_where",
            "post_vacuum",
            "post_wal_recover",
        ],
        "workload_profile": args.workload_profile,
        "bench_gate_profile": args.bench_gate_profile,
        "workload_profile_hints": _workload_profile_hints(args.workload_profile),
    }
    if args.emit_archive_contract:
        print(json.dumps(contract, indent=2))
        return 0

    if args.write_archive_manifest:
        out = Path(args.write_archive_manifest)
        git_commit = ""
        git_root = ""
        for root in (repo.parent, repo):
            if (root / ".git").exists():
                git_commit = _try_git_commit(root)
                git_root = str(root)
                break
        manifest = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "repo_root": str(repo),
            "build_dir": str(build),
            "ctest_config": args.ctest_config or ("RelWithDebInfo" if os.name == "nt" else ""),
            "ctest_regex": args.ctest_regex,
            "workload_profile": args.workload_profile,
            "bench_gate_profile": args.bench_gate_profile,
            "os": platform.system(),
            "os_version": platform.version(),
            "python": sys.version.split()[0],
            "cpu_count": _cpu_count(),
            "compiler_hint": _try_compiler_id(),
            "git_commit": git_commit,
            "git_root": git_root,
            "relevant_env": _relevant_env_keys(),
            "contract": contract,
        }
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        print(f"[capture_baseline] wrote manifest -> {out}")
        return 0

    print(f"[capture_baseline] repo={repo}")
    print(f"[capture_baseline] build_dir={build}")
    print(f"[capture_baseline] workload_profile={args.workload_profile}")

    if args.print_jsonl_hints:
        hints = _workload_profile_hints(args.workload_profile)
        print(
            "[capture_baseline] Collect JSONL lines with labels covering: "
            + hints["description"]
        )
        print(f"[capture_baseline] suggested_labels={hints['suggested_labels']}")
        fix = repo / "scripts" / "ci" / "fixtures" / "runtime_stats_bench_gate_minimal.jsonl"
        print(f"[capture_baseline] Contract fixture (for bench gates): {fix}")

    if args.validate_runtime_fixture:
        fixture = repo / "scripts" / "ci" / "fixtures" / "runtime_stats_bench_gate_minimal.jsonl"
        vscript = repo / "scripts" / "validate" / "validate_runtime_stats.py"
        if not fixture.is_file():
            print(f"ERROR: missing fixture {fixture}", file=sys.stderr)
            return 2
        cmd = [sys.executable, str(vscript), str(fixture)]
        print("[capture_baseline] running:", " ".join(cmd))
        return subprocess.run(cmd, cwd=str(repo)).returncode

    if not build.is_dir():
        print(f"ERROR: build directory does not exist: {build}", file=sys.stderr)
        print("Configure and build first, e.g. cmake -S newdb -B build && cmake --build build", file=sys.stderr)
        return 2

    ctest_log_lines: list[str] = []
    if not args.skip_ctest:
        cmd = ["ctest", "--test-dir", str(build), "--output-on-failure"]
        cfg = args.ctest_config.strip()
        if not cfg and os.name == "nt":
            cfg = "RelWithDebInfo"
        if cfg:
            cmd[1:1] = ["-C", cfg]
        if args.ctest_regex.strip():
            cmd.extend(["-R", args.ctest_regex.strip()])
        print("[capture_baseline] running:", " ".join(cmd))
        r = subprocess.run(cmd, cwd=str(repo), capture_output=True, text=True)
        ctest_log_lines.append("$ " + " ".join(cmd) + "\n")
        ctest_log_lines.append(r.stdout or "")
        ctest_log_lines.append(r.stderr or "")
        ctest_log_lines.append(f"\n[exit code] {r.returncode}\n")
        print(r.stdout, end="")
        print(r.stderr, end="", file=sys.stderr)
        if r.returncode != 0:
            return r.returncode

    if not args.skip_bench_gate:
        gate = here / "ci_bench_gate.py"
        if gate.is_file():
            cmd = [sys.executable, str(gate), str(build), "--profile", args.bench_gate_profile]
            print("[capture_baseline] running:", " ".join(cmd))
            r = subprocess.run(cmd, cwd=str(repo))
            if r.returncode != 0:
                return r.returncode
        else:
            print(f"[capture_baseline] NOTE: missing {gate}, skip bench gate")

    if args.emit_baseline_dir:
        base = Path(args.emit_baseline_dir)
        base.mkdir(parents=True, exist_ok=True)
        bl = base / "baseline"
        bl.mkdir(parents=True, exist_ok=True)
        ctest_path = bl / "ctest.log"
        if ctest_log_lines:
            ctest_path.write_text("".join(ctest_log_lines), encoding="utf-8")
        else:
            ctest_path.write_text(
                "[capture_baseline] ctest skipped or no output captured\n", encoding="utf-8"
            )

        runtime_jsonl = bl / "runtime_stats.jsonl"
        if args.runtime_jsonl_input:
            src = Path(args.runtime_jsonl_input)
            if not src.is_file():
                print(f"ERROR: --runtime-jsonl-input not found: {src}", file=sys.stderr)
                return 3
            shutil.copyfile(src, runtime_jsonl)
        else:
            runtime_jsonl.write_text(
                "# Empty placeholder: re-run with --runtime-jsonl-input PATH "
                "or append JSONL from a soak/demo session.\n",
                encoding="utf-8",
            )

        report_path = bl / "runtime_report.json"
        reporter_name = "newdb_runtime_report.exe" if os.name == "nt" else "newdb_runtime_report"
        cfg = args.ctest_config.strip() or ("RelWithDebInfo" if os.name == "nt" else "")
        reporter = build / reporter_name
        for sub in filter(None, (cfg, "Release", "RelWithDebInfo", "Debug", "MinSizeRel")):
            cand = build / sub / reporter_name
            if cand.is_file():
                reporter = cand
                break
        else:
            cand = build / reporter_name
            if cand.is_file():
                reporter = cand
        if runtime_jsonl.is_file() and not runtime_jsonl.read_text(encoding="utf-8").lstrip().startswith("#"):
            if reporter.is_file():
                try:
                    out = subprocess.check_output(
                        [
                            str(reporter),
                            "--input",
                            str(runtime_jsonl),
                            "--last-n",
                            "2",
                            "--output",
                            str(report_path),
                        ],
                        cwd=str(repo),
                        text=True,
                    )
                    # --json writes summary to stdout; --output also writes file
                    _ = out
                except subprocess.CalledProcessError:
                    report_path.write_text(
                        json.dumps({"status": "error", "hint": "newdb_runtime_report failed on input"}),
                        encoding="utf-8",
                    )
            else:
                report_path.write_text(
                    json.dumps({"status": "skipped", "reason": f"missing {reporter}"}),
                    encoding="utf-8",
                )
        else:
            report_path.write_text(
                json.dumps({"status": "skipped", "reason": "no valid runtime_stats.jsonl"}),
                encoding="utf-8",
            )

        fixture_rel = "scripts/ci/fixtures/runtime_stats_bench_gate_minimal.jsonl"
        rec_gate_cli = (
            f"python3 scripts/ci/ci_bench_gate.py {str(build)} "
            f"--profile {args.bench_gate_profile} "
            f"--runtime-jsonl {fixture_rel} --runtime-last-n 2 "
            f"--gate-fail-json-out scripts/results/runtime_gate_fail.json"
        )
        manifest = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "repo_root": str(repo),
            "build_dir": str(build),
            "build_config": cfg,
            "ctest_config": args.ctest_config,
            "ctest_regex": args.ctest_regex,
            "workload_profile": args.workload_profile,
            "bench_gate_profile": args.bench_gate_profile,
            "recommended_ci_bench_gate_cli": rec_gate_cli,
            "workload_profile_hints": _workload_profile_hints(args.workload_profile),
            "os": platform.system(),
            "os_version": platform.version(),
            "python": sys.version.split()[0],
            "cpu_count": _cpu_count(),
            "compiler_hint": _try_compiler_id(),
            "relevant_env": _relevant_env_keys(),
            "git_commit": "",
            "git_root": "",
            "baseline_layout": {
                "runtime_stats_jsonl": str(runtime_jsonl),
                "runtime_report_json": str(report_path),
                "ctest_log": str(ctest_path),
                "manifest_json": str(bl / "manifest.json"),
            },
            "contract": contract,
        }
        for root in (repo, repo.parent):
            gc = _try_git_commit(root)
            if gc:
                manifest["git_commit"] = gc
                manifest["git_root"] = str(root)
                break
        (bl / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        print(f"[capture_baseline] wrote baseline bundle -> {bl}")

    print("[capture_baseline] done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
