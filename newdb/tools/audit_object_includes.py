#!/usr/bin/env python3
"""Audit direct #include lines in CMake-listed shell/query sources (OBJECT bricks + newdb_query).

Counts unique ``cli/...`` includes that fall *outside* each brick's home directory prefixes.
Keep in sync with newdb/CMakeLists.txt source lists for shell OBJECT libraries and ``newdb_query``.

Run from repo root or ``newdb/``::

  python newdb/tools/audit_object_includes.py
  python newdb/tools/audit_object_includes.py --fail-if-dispatch-cross-above 24
"""
from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict
from pathlib import Path

INCLUDE_RE = re.compile(
    r'^\s*#\s*include\s+([<"])([^>"]+)([>"])\s*(?://.*)?$',
    re.MULTILINE,
)

# Keep aligned with newdb/CMakeLists.txt
OBJECT_SOURCES: dict[str, list[str]] = {
    "newdb_shell_state": [
        "cli/shell/state/shell_state_ops.cc",
        "cli/shell/state/shell_state.cc",
        "cli/shell/state/shell_state_facade.cc",
        "cli/shell/c_api/cli_dispatch_command_line.cc",
        "cli/shell/c_api/c_api_cli_bridge.cc",
        "cli/shell/c_api/runtime_stats_json_builder.cc",
        "cli/shell/c_api/show_storage_log.cc",
    ],
    "newdb_shell_bootstrap_capi": [
        "cli/shell/bootstrap/demo_mdb_script.cc",
        "cli/shell/bootstrap/demo_runner.cc",
        "cli/shell/diag/demo_diag.cc",
        "cli/modules/import_export/demo_export.cc",
        "cli/shell/bootstrap/demo_cli.cpp",
    ],
    "newdb_shell_bootstrap_repl": ["cli/shell/repl/demo_shell.cc"],
    "newdb_shell_dispatch": [
        "cli/shell/dispatch/router/dispatch.cc",
        "cli/shell/dispatch/router/dispatch_routing.cc",
        "cli/shell/dispatch/handlers/session/session_handler.cc",
        "cli/shell/dispatch/handlers/txn/txn_handler.cc",
        "cli/shell/dispatch/handlers/ddl/ddl_handler.cc",
        "cli/shell/dispatch/handlers/dml/dml_handler.cc",
        "cli/shell/dispatch/handlers/query/query_handler.cc",
        "cli/shell/dispatch/handlers/io/io_handler.cc",
        "cli/shell/dispatch/handlers/workspace/workspace_handler.cc",
        "cli/shell/dispatch/support/args/args_impl.cc",
        "cli/shell/dispatch/support/index/fast_index_impl.cc",
        "cli/shell/dispatch/services/sidecar/sidecar_invalidate_service.cc",
        "cli/shell/dispatch/services/lsm/lsm_lite_service.cc",
    ],
    "newdb_shell_common": [
        "cli/modules/common/view/table_view.cc",
        "cli/modules/common/util/utils.cc",
        "cli/modules/common/logging/logging.cc",
        "cli/modules/import_export/import.cc",
    ],
    "newdb_shell_catalog": ["cli/modules/catalog/schema_catalog.cc"],
    "newdb_shell_txn": [
        "cli/modules/txn/coordinator/txn_manager.cc",
        "cli/modules/txn/coordinator/core/core_impl.cc",
        "cli/modules/txn/coordinator/lock/lock_service.cc",
        "cli/modules/txn/coordinator/wal/wal_service.cc",
        "cli/modules/txn/coordinator/recovery/recovery_analyze.cc",
        "cli/modules/txn/coordinator/recovery/recovery_redo.cc",
        "cli/modules/txn/coordinator/recovery/recovery_undo.cc",
        "cli/modules/txn/coordinator/recovery/heap_undo_apply.cc",
        "cli/modules/txn/coordinator/recovery/recovery_finalize.cc",
        "cli/modules/txn/coordinator/recovery/recovery_service.cc",
        "cli/modules/txn/coordinator/write_conflict/write_conflict_service.cc",
        "cli/modules/txn/coordinator/vacuum/vacuum_service.cc",
        "cli/modules/txn/coordinator/stats/stats_impl.cc",
    ],
    "newdb_shell_sidecar": [
        "cli/modules/sidecar/common/index_catalog.cc",
        "cli/modules/storage/table_storage_health.cc",
        "cli/modules/sidecar/common/bptree_index.cc",
        "cli/modules/sidecar/page/page_index_sidecar.cc",
        "cli/modules/sidecar/eq/equality_index_sidecar.cc",
        "cli/modules/sidecar/eq/eq_bloom.cc",
        "cli/modules/sidecar/covering/covering_index_sidecar.cc",
        "cli/modules/sidecar/visibility/visibility_checkpoint_sidecar.cc",
        "cli/modules/sidecar/common/sidecar_wal_lsn.cpp",
    ],
}

NEWDB_QUERY_SOURCES: list[str] = [
    "cli/modules/where/parser/condition.cc",
    "cli/modules/where/executor/match/match_impl.cc",
    "cli/modules/where/executor/cache/cache_impl.cc",
    "cli/modules/where/executor/policy/policy_service.cc",
    "cli/modules/where/executor/plan/plan_impl.cc",
    "cli/modules/where/executor/plan/plan_impl_support.cc",
    "cli/modules/where/executor/plan/plan_query_index.cc",
    "cli/modules/where/executor/plan/plan_scan_estimate.cc",
    "cli/modules/where/executor/plan/where_plan_catalog.cc",
    "cli/modules/where/executor/stats/table_stats.cc",
]


def brick_local_prefixes(name: str) -> list[str]:
    roots: dict[str, list[str]] = {
        "newdb_shell_state": ["cli/shell/state/", "cli/shell/c_api/"],
        "newdb_shell_bootstrap_capi": [
            "cli/shell/bootstrap/",
            "cli/shell/diag/",
            "cli/modules/import_export/",
        ],
        "newdb_shell_bootstrap_repl": ["cli/shell/repl/"],
        "newdb_shell_dispatch": ["cli/shell/dispatch/"],
        "newdb_shell_common": ["cli/modules/common/", "cli/modules/import_export/"],
        "newdb_shell_catalog": ["cli/modules/catalog/"],
        "newdb_shell_txn": ["cli/modules/txn/"],
        "newdb_shell_sidecar": ["cli/modules/sidecar/", "cli/modules/storage/"],
        "newdb_query": ["cli/modules/where/"],
    }
    return roots[name]


def is_cross_cli(brick: str, inc_path: str) -> bool:
    if not inc_path.startswith("cli/"):
        return False
    for pre in brick_local_prefixes(brick):
        if inc_path.startswith(pre):
            return False
    return True


def collect_cross_cli_for_brick(newdb_root: Path, brick: str, rels: list[str]) -> set[str]:
    found: set[str] = set()
    for rel in rels:
        fp = newdb_root / rel
        if not fp.is_file():
            continue
        text = fp.read_text(encoding="utf-8", errors="replace")
        for m in INCLUDE_RE.finditer(text):
            path = m.group(2).replace("\\", "/")
            if m.group(1) != '"':
                continue
            if is_cross_cli(brick, path):
                found.add(path)
    return found


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fail-if-dispatch-cross-above",
        type=int,
        metavar="N",
        default=None,
        help="Exit 1 if newdb_shell_dispatch cross-cli unique include count > N.",
    )
    parser.add_argument(
        "--fail-if-state-cross-above",
        type=int,
        metavar="N",
        default=None,
        help="Exit 1 if newdb_shell_state cross-cli unique include count > N.",
    )
    parser.add_argument(
        "--fail-if-max-cross-cli-above",
        type=int,
        metavar="N",
        default=None,
        help="Exit 1 if any brick's cross-cli unique include count > N.",
    )
    args = parser.parse_args()

    here = Path(__file__).resolve().parent
    newdb_root = here.parent

    bricks: list[tuple[str, list[str]]] = list(OBJECT_SOURCES.items())
    bricks.append(("newdb_query", NEWDB_QUERY_SOURCES))

    print("OBJECT / newdb_query brick cross-cli #include counts (quoted cli/... only)\n")
    max_count = 0
    err = False
    for brick, rels in bricks:
        cross = collect_cross_cli_for_brick(newdb_root, brick, rels)
        n = len(cross)
        max_count = max(max_count, n)
        print(f"{brick}: {n} unique cross-brick cli includes")
        for p in sorted(cross)[:12]:
            print(f"  {p}")
        if len(cross) > 12:
            print(f"  ... +{len(cross) - 12} more")
        print()

        if args.fail_if_dispatch_cross_above is not None and brick == "newdb_shell_dispatch":
            if n > args.fail_if_dispatch_cross_above:
                print(
                    f"error: dispatch cross-cli {n} > {args.fail_if_dispatch_cross_above}",
                    file=sys.stderr,
                )
                err = True
        if args.fail_if_state_cross_above is not None and brick == "newdb_shell_state":
            if n > args.fail_if_state_cross_above:
                print(
                    f"error: shell_state cross-cli {n} > {args.fail_if_state_cross_above}",
                    file=sys.stderr,
                )
                err = True
        if args.fail_if_max_cross_cli_above is not None and n > args.fail_if_max_cross_cli_above:
            print(
                f"error: {brick} cross-cli {n} > {args.fail_if_max_cross_cli_above}",
                file=sys.stderr,
            )
            err = True

    print(f"max cross-cli count across bricks: {max_count}")
    return 1 if err else 0


if __name__ == "__main__":
    sys.exit(main())
