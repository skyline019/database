#!/usr/bin/env python3
"""Unit tests for `ci_bench_gate.py` cross-host baseline merge (Phase 6 closure).

Covers:
- Exact `host_slug` match feeds matching manifest thresholds.
- 2-segment fallback when full slug differs.
- Newest-by-`generated_at_utc` fallback when no slug overlap.
- Missing index file is best-effort (NOTE only, no exception).
- Explicit CLI value (>= 0) wins over manifest threshold (sentinel-only fill).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(ROOT / "newdb" / "scripts" / "ci"))

import ci_bench_gate  # noqa: E402  pylint: disable=wrong-import-position


def _make_args(**overrides) -> argparse.Namespace:
    args = argparse.Namespace(
        profile="pr",
        _profile_effective="pr",
        baseline_host_index="",
        baseline_prefer_host_slug="",
        _baseline_host_slug_used="",
        max_compact_debt_bytes_peak=-1.0,
        max_wal_recovery_last_elapsed_ms=-1.0,
        recommended_thresholds_json="",
    )
    for key, val in overrides.items():
        setattr(args, key, val)
    return args


def _write_manifest(dirp: Path, host_slug: str, peak: float, wal_ms: float) -> Path:
    host_dir = dirp / host_slug
    host_dir.mkdir(parents=True, exist_ok=True)
    manifest = host_dir / "manifest.json"
    blob = {
        "host_slug": host_slug,
        "recommended_thresholds_pr": {
            "max_compact_debt_bytes_peak": peak,
            "max_wal_recovery_last_elapsed_ms": wal_ms,
        },
    }
    manifest.write_text(json.dumps(blob), encoding="utf-8")
    return manifest


def _write_index(dirp: Path, hosts: list) -> Path:
    idx = dirp / "host_index.json"
    idx.write_text(json.dumps({"hosts": hosts}), encoding="utf-8")
    return idx


class CrossHostBaselineTests(unittest.TestCase):
    def test_exact_slug_match_fills_thresholds(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            slug_a = "linux__x86_64__8"
            man_a = _write_manifest(base, slug_a, 1.23e9, 333.0)
            slug_b = "linux__x86_64__16"
            _write_manifest(base, slug_b, 9.87e9, 999.0)
            idx = _write_index(
                base,
                [
                    {
                        "host_slug": slug_a,
                        "manifest": str(man_a),
                        "generated_at_utc": "2025-01-01T00:00:00Z",
                    },
                    {
                        "host_slug": slug_b,
                        "manifest": str(base / slug_b / "manifest.json"),
                        "generated_at_utc": "2025-12-01T00:00:00Z",
                    },
                ],
            )
            args = _make_args(baseline_host_index=str(idx), baseline_prefer_host_slug=slug_a)
            ci_bench_gate._apply_baseline_host_index(args)
            self.assertEqual(args._baseline_host_slug_used, slug_a)
            self.assertAlmostEqual(args.max_compact_debt_bytes_peak, 1.23e9)
            self.assertAlmostEqual(args.max_wal_recovery_last_elapsed_ms, 333.0)

    def test_two_segment_fallback_when_no_full_slug_match(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            slug_far = "linux__x86_64__4"
            man_far = _write_manifest(base, slug_far, 5.0e9, 555.0)
            idx = _write_index(
                base,
                [{
                    "host_slug": slug_far,
                    "manifest": str(man_far),
                    "generated_at_utc": "2025-06-01T00:00:00Z",
                }],
            )
            args = _make_args(
                baseline_host_index=str(idx),
                baseline_prefer_host_slug="linux__x86_64__99__rust1.83",
            )
            ci_bench_gate._apply_baseline_host_index(args)
            self.assertEqual(args._baseline_host_slug_used, slug_far)
            self.assertAlmostEqual(args.max_compact_debt_bytes_peak, 5.0e9)

    def test_newest_when_no_segment_overlap(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            man_old = _write_manifest(base, "darwin__arm64__8", 1.0, 1.0)
            man_new = _write_manifest(base, "darwin__arm64__16", 2.0, 2.0)
            idx = _write_index(
                base,
                [
                    {
                        "host_slug": "darwin__arm64__8",
                        "manifest": str(man_old),
                        "generated_at_utc": "2024-01-01T00:00:00Z",
                    },
                    {
                        "host_slug": "darwin__arm64__16",
                        "manifest": str(man_new),
                        "generated_at_utc": "2025-12-01T00:00:00Z",
                    },
                ],
            )
            args = _make_args(
                baseline_host_index=str(idx),
                baseline_prefer_host_slug="windows__amd64__1",
            )
            ci_bench_gate._apply_baseline_host_index(args)
            self.assertEqual(args._baseline_host_slug_used, "darwin__arm64__16")
            self.assertAlmostEqual(args.max_compact_debt_bytes_peak, 2.0)

    def test_missing_index_file_is_best_effort(self):
        args = _make_args(baseline_host_index=str(Path(tempfile.gettempdir()) / "no_such_idx.json"))
        ci_bench_gate._apply_baseline_host_index(args)
        self.assertEqual(args._baseline_host_slug_used, "")
        self.assertEqual(args.max_compact_debt_bytes_peak, -1.0)

    def test_explicit_threshold_wins_over_manifest(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            slug = "linux__x86_64__8"
            man = _write_manifest(base, slug, 7.0e9, 700.0)
            idx = _write_index(
                base,
                [{
                    "host_slug": slug,
                    "manifest": str(man),
                    "generated_at_utc": "2025-01-01T00:00:00Z",
                }],
            )
            args = _make_args(
                baseline_host_index=str(idx),
                baseline_prefer_host_slug=slug,
                max_compact_debt_bytes_peak=42.0,  # caller already set non-sentinel
            )
            ci_bench_gate._apply_baseline_host_index(args)
            self.assertEqual(args.max_compact_debt_bytes_peak, 42.0)
            self.assertAlmostEqual(args.max_wal_recovery_last_elapsed_ms, 700.0)


if __name__ == "__main__":
    unittest.main()
