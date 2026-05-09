#!/usr/bin/env python3
"""Count TUs under newdb/ that include the C API bridge or dispatch router (coupling baseline).

Run from repo root or newdb/:
  python newdb/tools/count_bridge_dispatch_includes.py
  python newdb/tools/count_bridge_dispatch_includes.py --fail-if-bridge-above N --fail-if-dispatch-above M
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

BRIDGE_RE = re.compile(
    r'^\s*#\s*include\s+[<"](?:newdb/c_api_cli_bridge\.h|cli/shell/c_api/c_api_cli_bridge\.h|cli\\\\shell\\\\c_api\\\\c_api_cli_bridge\.h)[>"]',
    re.MULTILINE,
)
DISPATCH_RE = re.compile(
    r'^\s*#\s*include\s+[<"](?:cli/shell/dispatch/router/dispatch\.h|cli\\\\shell\\\\dispatch\\\\router\\\\dispatch\.h)[>"]',
    re.MULTILINE,
)


def collect(newdb_root: Path) -> tuple[list[Path], list[Path]]:
    bridge_hits: list[Path] = []
    dispatch_hits: list[Path] = []
    for ext in (".cc", ".cpp"):
        for p in newdb_root.rglob(f"*{ext}"):
            if "build" in p.parts or "_deps" in p.parts:
                continue
            try:
                text = p.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            try:
                rel = p.relative_to(newdb_root.parent)
            except ValueError:
                rel = p
            if BRIDGE_RE.search(text):
                bridge_hits.append(rel)
            if DISPATCH_RE.search(text):
                dispatch_hits.append(rel)
    bridge_hits.sort()
    dispatch_hits.sort()
    return bridge_hits, dispatch_hits


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fail-if-bridge-above", type=int, metavar="N", default=None)
    parser.add_argument("--fail-if-dispatch-above", type=int, metavar="N", default=None)
    args = parser.parse_args()

    here = Path(__file__).resolve().parent
    newdb_root = here.parent
    bridge, dispatch = collect(newdb_root)
    print(f"TU count with #include c_api_cli_bridge.h: {len(bridge)}")
    for p in bridge:
        print(f"  {p}")
    print(f"TU count with #include dispatch.h (router): {len(dispatch)}")
    for p in dispatch:
        print(f"  {p}")
    err = False
    if args.fail_if_bridge_above is not None and len(bridge) > args.fail_if_bridge_above:
        print(f"error: bridge count {len(bridge)} > {args.fail_if_bridge_above}", file=sys.stderr)
        err = True
    if args.fail_if_dispatch_above is not None and len(dispatch) > args.fail_if_dispatch_above:
        print(f"error: dispatch count {len(dispatch)} > {args.fail_if_dispatch_above}", file=sys.stderr)
        err = True
    return 1 if err else 0


if __name__ == "__main__":
    sys.exit(main())
