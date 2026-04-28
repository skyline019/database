#!/usr/bin/env python3
"""Rank tail-dominant write stages from runtime stats JSONL."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


STAGES: List[Tuple[str, str]] = [
    ("heap_io", "write_heap_append"),
    ("sidecar_invalidate", "write_sidecar_invalidate"),
    ("wal_append", "write_wal_append"),
    ("lsm_track", "write_lsm_track"),
    ("lsm_flush", "write_lsm_flush"),
    ("lsm_compaction", "write_lsm_compaction"),
    # Backward-compatible combined stage (if split fields are unavailable).
    ("lsm_rotate_compact", "write_lsm_rotate_compact"),
]


def _fail(msg: str) -> int:
    print(f"TAIL_STAGE_RANK_INVALID: {msg}", file=sys.stderr)
    return 2


def _load_rows(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for idx, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except Exception as exc:
            raise ValueError(f"line {idx}: invalid json: {exc}") from exc
        if not isinstance(obj, dict):
            raise ValueError(f"line {idx}: row must be JSON object")
        rows.append(obj)
    return rows


def _pick_target_row(rows: List[Dict[str, Any]], run_id: Optional[str]) -> Dict[str, Any]:
    selected = [r for r in rows if isinstance(r.get("stats"), dict)]
    if run_id:
        selected = [r for r in selected if r.get("run_id") == run_id]
    if not selected:
        raise ValueError("no matching rows with stats found")

    # Prefer pressure_after for direct tail attribution.
    after = [r for r in selected if r.get("label") == "pressure_after"]
    if after:
        return after[-1]
    return selected[-1]


def _nn_int(obj: Dict[str, Any], key: str) -> int:
    v = obj.get(key, 0)
    return int(v) if isinstance(v, int) and v >= 0 else 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", required=True, help="Path to runtime_stats_*.jsonl")
    ap.add_argument("--run-id", default="", help="Optional run_id filter")
    ap.add_argument("--json", action="store_true", help="Emit JSON output")
    args = ap.parse_args()

    path = Path(args.input)
    if not path.is_file():
        return _fail(f"file not found: {path}")

    try:
        rows = _load_rows(path)
        row = _pick_target_row(rows, args.run_id or None)
    except Exception as exc:
        return _fail(str(exc))

    stats = row["stats"]
    ranking: List[Dict[str, Any]] = []
    for name, key_base in STAGES:
        p95_key = f"{key_base}_p95_ms"
        max_key = f"{key_base}_max_ms"
        p95 = _nn_int(stats, p95_key)
        mx = _nn_int(stats, max_key)
        ranking.append(
            {
                "stage": name,
                "p95_ms": p95,
                "max_ms": mx,
                "score": p95 * 1000 + mx,  # prioritize p95, then max
            }
        )

    ranking.sort(key=lambda x: x["score"], reverse=True)
    non_zero = [r for r in ranking if r["p95_ms"] > 0 or r["max_ms"] > 0]

    result = {
        "input": str(path),
        "run_id": row.get("run_id", ""),
        "label": row.get("label", ""),
        "schema_version": row.get("schema_version", ""),
        "top_stage": non_zero[0]["stage"] if non_zero else "none",
        "ranking": non_zero if non_zero else ranking,
    }

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    print(
        "TAIL_STAGE_RANK "
        f"run_id={result['run_id']} label={result['label']} "
        f"top_stage={result['top_stage']}"
    )
    print("rank stage p95_ms max_ms")
    for idx, item in enumerate(result["ranking"], start=1):
        print(f"{idx:>4} {item['stage']:<22} {item['p95_ms']:>6} {item['max_ms']:>6}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

