#!/usr/bin/env python3
"""Check C API ABI version and exported symbol surface."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


def fail(msg: str) -> int:
    print(f"C_API_ABI_INVALID: {msg}", file=sys.stderr)
    return 2


def parse_header_abi(header_text: str) -> int:
    m = re.search(r"#define\s+NEWDB_C_API_ABI_VERSION\s+(\d+)", header_text)
    if not m:
        raise ValueError("NEWDB_C_API_ABI_VERSION not found")
    return int(m.group(1))


def parse_header_symbols(header_text: str) -> list[str]:
    pat = re.compile(r"^\s*NEWDB_API\s+[^\(\n;]+\s+([a-zA-Z_]\w*)\s*\(", re.MULTILINE)
    return sorted(set(pat.findall(header_text)))


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--header", default="include/newdb/c_api.h")
    p.add_argument("--expected-symbols", default="scripts/validate/c_api_expected_symbols.txt")
    p.add_argument("--expected-abi", type=int, default=1)
    args = p.parse_args()

    header_path = Path(args.header)
    expected_path = Path(args.expected_symbols)
    if not header_path.is_file():
        return fail(f"header not found: {header_path}")
    if not expected_path.is_file():
        return fail(f"expected symbol list not found: {expected_path}")

    header_text = header_path.read_text(encoding="utf-8")
    try:
        abi = parse_header_abi(header_text)
    except ValueError as exc:
        return fail(str(exc))
    if abi != args.expected_abi:
        return fail(f"abi version mismatch: header={abi} expected={args.expected_abi}")

    symbols = parse_header_symbols(header_text)
    expected = [
        ln.strip()
        for ln in expected_path.read_text(encoding="utf-8").splitlines()
        if ln.strip() and not ln.strip().startswith("#")
    ]
    if symbols != sorted(expected):
        missing = sorted(set(expected) - set(symbols))
        extra = sorted(set(symbols) - set(expected))
        return fail(f"symbol surface changed: missing={missing} extra={extra}")

    print(f"C_API_ABI_VALID: abi={abi} symbols={len(symbols)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

