#!/usr/bin/env python3
"""Ensure RUNTIME_STATS_SCHEMA.md documents every required_stats key from contract JSON."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path


def _load_contract_stats_and_cli_embed(contract_path: Path) -> tuple[set[str], set[str]]:
    with contract_path.open(encoding="utf-8") as f:
        doc = json.load(f)
    keys = doc.get("required_stats_keys")
    if not isinstance(keys, list) or not all(isinstance(x, str) for x in keys):
        raise ValueError("contract missing required_stats_keys list")
    req_set = set(keys)
    cli_raw = doc.get("stats_keys_cli_embed_layer")
    if cli_raw is None:
        return req_set, set()
    if not isinstance(cli_raw, list) or not all(isinstance(x, str) for x in cli_raw):
        raise ValueError("contract stats_keys_cli_embed_layer must be a list of strings")
    cli_set = set(cli_raw)
    if not cli_set <= req_set:
        bad = sorted(cli_set - req_set)
        raise ValueError(
            "stats_keys_cli_embed_layer entries must be subset of required_stats_keys; "
            f"extra: {bad}"
        )
    eng_set = req_set - cli_set
    overlap = cli_set & eng_set
    if overlap:
        raise ValueError(f"internal error: tier overlap {overlap}")
    return req_set, cli_set


def _markdown_stats_keys(md_text: str) -> set[str]:
    anchor = "## `stats` object fields"
    start = md_text.find(anchor)
    if start < 0:
        raise ValueError("markdown missing stats section heading")
    rest = md_text[start:]
    # Next top-level ## section after the stats heading block
    m = re.search(r"^## ", rest[len(anchor) :], flags=re.MULTILINE)
    chunk = rest[: len(anchor) + m.start()] if m else rest
    # Backtick identifiers (handles bullets listing multiple keys)
    return set(re.findall(r"`([a-zA-Z][a-zA-Z0-9_]*)`", chunk))


REQUIRED_LAYER_HEADINGS = (
    "### Engine layer (`newdb_engine_runtime_stats_json`)",
    "### CLI embed layer (`newdb_cli_runtime_stats_json`)",
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--contract",
        type=Path,
        default=Path(__file__).resolve().parent / "contract" / "runtime_stats.v1.required.json",
        help="Path to runtime_stats.v1.required.json",
    )
    parser.add_argument(
        "--markdown",
        type=Path,
        default=Path(__file__).resolve().parent / "RUNTIME_STATS_SCHEMA.md",
        help="Path to RUNTIME_STATS_SCHEMA.md",
    )
    args = parser.parse_args()

    try:
        contract_keys, cli_embed_keys = _load_contract_stats_and_cli_embed(args.contract)
        md_text = args.markdown.read_text(encoding="utf-8")
        md_keys = _markdown_stats_keys(md_text)
    except (OSError, ValueError) as e:
        print(f"RUNTIME_STATS_CONTRACT_PARITY_ERROR: {e}", file=sys.stderr)
        return 2

    missing_layers = [h for h in REQUIRED_LAYER_HEADINGS if h not in md_text]
    if missing_layers:
        print(
            "RUNTIME_STATS_CONTRACT_PARITY_FAIL: RUNTIME_STATS_SCHEMA.md missing layer headings:",
            file=sys.stderr,
        )
        for h in missing_layers:
            print(f"  - {h}", file=sys.stderr)
        return 1

    if cli_embed_keys:
        missing_cli = sorted(cli_embed_keys - md_keys)
        if missing_cli:
            print(
                "RUNTIME_STATS_CONTRACT_PARITY_FAIL: CLI embed tier keys missing from RUNTIME_STATS_SCHEMA.md:",
                file=sys.stderr,
            )
            for k in missing_cli:
                print(f"  - {k}", file=sys.stderr)
            return 1
        engine_only_keys = contract_keys - cli_embed_keys
        missing_eng = sorted(engine_only_keys - md_keys)
        if missing_eng:
            print(
                "RUNTIME_STATS_CONTRACT_PARITY_FAIL: engine-layer tier keys missing from RUNTIME_STATS_SCHEMA.md:",
                file=sys.stderr,
            )
            for k in missing_eng:
                print(f"  - {k}", file=sys.stderr)
            return 1

    missing_in_md = sorted(contract_keys - md_keys)
    if missing_in_md:
        print(
            "RUNTIME_STATS_CONTRACT_PARITY_FAIL: contract keys missing from RUNTIME_STATS_SCHEMA.md stats section:",
            file=sys.stderr,
        )
        for k in missing_in_md:
            print(f"  - {k}", file=sys.stderr)
        return 1

    tier_note = ""
    if cli_embed_keys:
        tier_note = (
            f" cli_embed={len(cli_embed_keys)} engine_derived={len(contract_keys - cli_embed_keys)}"
        )
    print(
        f"RUNTIME_STATS_CONTRACT_PARITY_OK contract_keys={len(contract_keys)} "
        f"markdown_stats_keys_found={len(md_keys)}{tier_note}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
