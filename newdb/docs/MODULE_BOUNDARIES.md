# newdb Module Boundaries

This document defines the second-stage module boundaries for `newdb`.

## Dependency Direction

- `cli/app` -> `cli/shell/*` -> `cli/modules/*` -> `engine/include/newdb/*`
- `engine/src/*` can depend on `engine/include/newdb/*` and other engine implementation units.
- `cli/*` must not include engine private implementation headers.

## CLI Submodules

- `cli/shell/bootstrap`: process startup, argv parsing, workspace bootstrap.
- `cli/shell/repl`: interactive loop and line execution surface.
- `cli/shell/dispatch`: command routing and command handler orchestration.
- `cli/shell/state`: long-lived shell/session state and state helpers.
- `cli/shell/diag`: diagnostic and verbose output.

- `cli/modules/where/parser`: where/agg parser and parse helpers.
- `cli/modules/where/executor`: where evaluation and aggregation execution.
- `cli/modules/txn/coordinator`: transaction coordinator and state machine.
- `cli/modules/sidecar/eq|covering|page|visibility|common`: sidecar implementations by access pattern.

## Engine Submodules

- `engine/src/session/api`: public session-facing behavior.
- `engine/src/session/table_access`: table load/cache/materialization behavior.
- `engine/src/wal/writer|codec|checkpoint|recovery`: WAL concerns split by responsibility.
- `engine/src/mvcc/snapshot|txn_index|gc`: visibility, txn bookkeeping, cleanup.
- `engine/src/io/page`: page IO concerns.

## Rules

- Prefer `#include "cli/.../file.h"` for CLI internal includes.
- Prefer `#include <newdb/...>` for engine public interfaces.
- Avoid adding cross-submodule includes when function parameters can carry dependencies.
