#!/usr/bin/env python3
"""Merge latest redo/undo crash matrix JSON into runtime trend dashboard nightly section."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _load_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(data, dict):
        raise ValueError(f"json root must be object: {path}")
    return data


def _resolve_matrix_path(explicit: str, search_dir: str) -> Path:
    if explicit:
        p = Path(explicit)
        if not p.is_file():
            raise FileNotFoundError(f"crash matrix not found: {p}")
        return p
    d = Path(search_dir)
    if not d.is_dir():
        raise FileNotFoundError(f"search dir not found: {d}")
    files = sorted(d.glob("redo_undo_crash_matrix_*.json"), key=lambda p: p.stat().st_mtime)
    if not files:
        raise FileNotFoundError(f"no redo_undo_crash_matrix_*.json under: {d}")
    return files[-1]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dashboard", required=True, help="runtime_trend_dashboard.json path")
    ap.add_argument("--crash-matrix", default="", help="explicit crash matrix json path (optional)")
    ap.add_argument("--search-dir", default="", help="search dir for latest crash matrix (optional)")
    args = ap.parse_args()

    dashboard_path = Path(args.dashboard)
    if not dashboard_path.is_file():
        raise FileNotFoundError(f"dashboard not found: {dashboard_path}")

    search_dir = args.search_dir or str(dashboard_path.parent)
    matrix_path = _resolve_matrix_path(args.crash_matrix, search_dir)

    dashboard = _load_json(dashboard_path)
    matrix = _load_json(matrix_path)

    points = matrix.get("points")
    if not isinstance(points, list):
        raise ValueError("crash matrix `points` must be array")

    passed = 0
    failed = 0
    normalized_points: list[dict[str, Any]] = []
    for item in points:
        if not isinstance(item, dict):
            continue
        point = str(item.get("point", ""))
        ok = bool(item.get("pass", False))
        elapsed_ms = item.get("elapsed_ms")
        if not isinstance(elapsed_ms, (int, float)):
            elapsed_ms = 0
        normalized_points.append(
            {
                "point": point,
                "pass": ok,
                "elapsed_ms": int(elapsed_ms),
            }
        )
        if ok:
            passed += 1
        else:
            failed += 1

    nightly = dashboard.get("nightly")
    if not isinstance(nightly, dict):
        nightly = {}
        dashboard["nightly"] = nightly

    nightly["crash_matrix"] = {
        "schema_version": str(matrix.get("schema_version", "")),
        "ts_ms": matrix.get("ts_ms"),
        "source_json": str(matrix_path),
        "merged_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "total": len(normalized_points),
            "passed": passed,
            "failed": failed,
        },
        "points": normalized_points,
    }

    dashboard_path.write_text(json.dumps(dashboard, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"RUNTIME_TREND_DASHBOARD_CRASH_MATRIX_MERGED: {dashboard_path}")
    print(f"RUNTIME_TREND_DASHBOARD_CRASH_MATRIX_SOURCE: {matrix_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

