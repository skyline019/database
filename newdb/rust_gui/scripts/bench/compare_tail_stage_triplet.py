#!/usr/bin/env python3
"""One-shot compare tail-stage attribution across 128/256/512 profiles."""

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
    ("lsm_rotate_compact", "write_lsm_rotate_compact"),
]


def load_rows(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for idx, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except Exception as exc:
            raise ValueError(f"{path}: line {idx}: invalid json: {exc}") from exc
        if isinstance(obj, dict):
            rows.append(obj)
    if not rows:
        raise ValueError(f"{path}: no json rows")
    return rows


def pick_after(rows: List[Dict[str, Any]], run_id: Optional[str]) -> Dict[str, Any]:
    selected = [r for r in rows if isinstance(r.get("stats"), dict)]
    if run_id:
        selected = [r for r in selected if r.get("run_id") == run_id]
    if not selected:
        raise ValueError("no rows match run_id")
    after = [r for r in selected if r.get("label") == "pressure_after"]
    return after[-1] if after else selected[-1]


def nn_int(d: Dict[str, Any], k: str) -> int:
    v = d.get(k, 0)
    return int(v) if isinstance(v, int) and v >= 0 else 0


def rank_from_stats(stats: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for name, base in STAGES:
        p95 = nn_int(stats, f"{base}_p95_ms")
        mx = nn_int(stats, f"{base}_max_ms")
        out.append({"stage": name, "p95_ms": p95, "max_ms": mx, "score": p95 * 1000 + mx})
    out.sort(key=lambda x: x["score"], reverse=True)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--jsonl-128", required=True)
    ap.add_argument("--jsonl-256", required=True)
    ap.add_argument("--jsonl-512", required=True)
    ap.add_argument("--run-id-128", default="")
    ap.add_argument("--run-id-256", default="")
    ap.add_argument("--run-id-512", default="")
    ap.add_argument("--top-k", type=int, default=4)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    try:
        p128 = Path(args.jsonl_128)
        p256 = Path(args.jsonl_256)
        p512 = Path(args.jsonl_512)
        r128 = pick_after(load_rows(p128), args.run_id_128 or None)
        r256 = pick_after(load_rows(p256), args.run_id_256 or None)
        r512 = pick_after(load_rows(p512), args.run_id_512 or None)
    except Exception as exc:
        print(f"TAIL_STAGE_TRIPLET_INVALID: {exc}", file=sys.stderr)
        return 2

    d128 = rank_from_stats(r128["stats"])
    d256 = rank_from_stats(r256["stats"])
    d512 = rank_from_stats(r512["stats"])
    top_k = max(1, args.top_k)

    result = {
        "profiles": {
            "128": {"path": str(p128), "run_id": r128.get("run_id", ""), "top": d128[0]["stage"], "ranking": d128},
            "256": {"path": str(p256), "run_id": r256.get("run_id", ""), "top": d256[0]["stage"], "ranking": d256},
            "512": {"path": str(p512), "run_id": r512.get("run_id", ""), "top": d512[0]["stage"], "ranking": d512},
        }
    }

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    print("TAIL_STAGE_TRIPLET_COMPARE")
    print(f"top_stage 128={result['profiles']['128']['top']} 256={result['profiles']['256']['top']} 512={result['profiles']['512']['top']}")
    print("rank stage                  128(p95/max)    256(p95/max)    512(p95/max)")

    # union of top-k stages from three profiles
    stage_order: List[str] = []
    for ranked in (d128[:top_k], d256[:top_k], d512[:top_k]):
        for item in ranked:
            if item["stage"] not in stage_order:
                stage_order.append(item["stage"])

    idx_map = {
        "128": {x["stage"]: x for x in d128},
        "256": {x["stage"]: x for x in d256},
        "512": {x["stage"]: x for x in d512},
    }
    for i, st in enumerate(stage_order, start=1):
        a = idx_map["128"].get(st, {"p95_ms": 0, "max_ms": 0})
        b = idx_map["256"].get(st, {"p95_ms": 0, "max_ms": 0})
        c = idx_map["512"].get(st, {"p95_ms": 0, "max_ms": 0})
        print(
            f"{i:>4} {st:<22} "
            f"{a['p95_ms']:>4}/{a['max_ms']:<4}      "
            f"{b['p95_ms']:>4}/{b['max_ms']:<4}      "
            f"{c['p95_ms']:>4}/{c['max_ms']:<4}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

