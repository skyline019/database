#!/usr/bin/env python3
"""Ensure rust_gui commandPolicy RUNTIME_TUNING_DIAGNOSTIC_GROUPS keys are a subset of required_stats_keys."""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import sys


def extract_ts_keys(ts_path: pathlib.Path) -> set[str]:
    text = ts_path.read_text(encoding="utf-8")
    anchor = "export const RUNTIME_TUNING_DIAGNOSTIC_GROUPS"
    pos = text.find(anchor)
    if pos < 0:
        raise SystemExit(f"could not find RUNTIME_TUNING_DIAGNOSTIC_GROUPS in {ts_path}")
    sub = text[pos:]
    m_eq = re.search(r"=\s*\[", sub)
    if not m_eq:
        raise SystemExit(f"could not find '= [' for RUNTIME_TUNING_DIAGNOSTIC_GROUPS in {ts_path}")
    open_b = m_eq.end() - 1
    depth = 0
    end_idx = -1
    for i, ch in enumerate(sub[open_b:], start=open_b):
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                end_idx = i + 1
                break
    if end_idx < 0:
        raise SystemExit(f"could not parse RUNTIME_TUNING_DIAGNOSTIC_GROUPS array in {ts_path}")
    blob = sub[open_b:end_idx]
    keys: set[str] = set()
    for inner in re.findall(r"\bkeys\s*:\s*\[(.*?)\]", blob, re.DOTALL):
        keys.update(re.findall(r'"([^"]+)"', inner))
    return keys


def _markdown_stats_keys(md_text: str) -> set[str]:
    anchor = "## `stats` object fields"
    start = md_text.find(anchor)
    if start < 0:
        raise ValueError("markdown missing stats section heading")
    rest = md_text[start:]
    m = re.search(r"^## ", rest[len(anchor) :], flags=re.MULTILINE)
    chunk = rest[: len(anchor) + m.start()] if m else rest
    return set(re.findall(r"`([a-zA-Z][a-zA-Z0-9_]*)`", chunk))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--contract",
        type=pathlib.Path,
        default=pathlib.Path(__file__).resolve().parent / "contract" / "runtime_stats.v1.required.json",
    )
    ap.add_argument(
        "--command-policy",
        type=pathlib.Path,
        default=pathlib.Path(__file__).resolve().parents[2] / "rust_gui" / "src" / "commandPolicy.ts",
    )
    ap.add_argument(
        "--schema-md",
        type=pathlib.Path,
        default=pathlib.Path(__file__).resolve().parent / "RUNTIME_STATS_SCHEMA.md",
        help="If set, assert each GUI key appears in the stats section (backtick identifiers).",
    )
    args = ap.parse_args()

    contract = json.loads(args.contract.read_text(encoding="utf-8"))
    required = set(contract["required_stats_keys"])
    gui_keys = extract_ts_keys(args.command_policy)

    missing = sorted(gui_keys - required)
    if missing:
        print("GUI runtime tuning keys not present in contract required_stats_keys:", file=sys.stderr)
        for k in missing:
            print(f"  {k}", file=sys.stderr)
        return 1

    if args.schema_md is not None:
        try:
            md_keys = _markdown_stats_keys(args.schema_md.read_text(encoding="utf-8"))
        except (OSError, ValueError) as e:
            print(f"GUI_SCHEMA_DOC_ERROR: {e}", file=sys.stderr)
            return 2
        missing_doc = sorted(gui_keys - md_keys)
        if missing_doc:
            print("GUI keys missing from RUNTIME_STATS_SCHEMA.md stats section:", file=sys.stderr)
            for k in missing_doc:
                print(f"  {k}", file=sys.stderr)
            return 1

    print(f"OK: {len(gui_keys)} GUI keys are subset of contract ({len(required)} required keys).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
