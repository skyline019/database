# newdb Module Boundaries

This document defines the second-stage module boundaries for `newdb`.

## Dependency Direction

- `cli/app` -> `cli/shell/*` -> `cli/modules/*` -> `engine/include/newdb/*`
- `engine/src/*` can depend on `engine/include/newdb/*` and other engine implementation units.
- `cli/*` must not include engine private implementation headers.

## CLI Submodules

- `cli/shell/bootstrap`: process startup, argv parsing, workspace bootstrap.
- `cli/shell/repl`: interactive loop and line execution surface.
- `cli/shell/dispatch`: command routing and handler orchestration, split as:
  - `cli/shell/dispatch/router`: `process_command_line` entrypoint and `dispatch_routing` (phase-2 verb fast path).
  - `cli/shell/dispatch/registry`: demo command catalog / umbrella headers and snapshot sources.
  - `cli/shell/dispatch/handlers`: domain command handlers (ddl, dml, query, txn, session, io, workspace).
  - `cli/shell/dispatch/support`: parsing, validation, and hot-index glue used by handlers.
  - `cli/shell/dispatch/services`: background or cross-cutting dispatch services (lsm-lite, sidecar invalidation).
  - `cli/shell/dispatch/shared`: shared declarations used across dispatch units (e.g. `dispatch_internal.h`).
- `cli/shell/state`: long-lived shell/session state and state helpers.
- `cli/shell/diag`: diagnostic and verbose output.

- `cli/modules/common/logging|util|view`: cross-cutting logging, small utilities, and table preview output.
- `cli/modules/where/parser`: where/agg parser and parse helpers.
- `cli/modules/where/executor`: where evaluation and aggregation execution.
- `cli/modules/txn/coordinator`: transaction coordinator and state machine.
- `cli/modules/sidecar/eq|covering|page|visibility|common`: sidecar implementations by access pattern.

## Engine Submodules

- `engine/src/session/api`: public session-facing behavior.
- `engine/src/session/table_access`: table load/cache/materialization behavior.
- `engine/src/api/c`: stable C ABI implementation (`c_api.cpp`) linked against public `newdb/c_api.h`.
- `engine/src/wal/writer|codec|checkpoint|recovery`: WAL concerns split by responsibility.
- `engine/src/mvcc/snapshot|txn_index|gc`: visibility, txn bookkeeping, cleanup.
- `engine/src/io/page`: page IO concerns.

## Rules

- Prefer `#include "cli/.../file.h"` for CLI internal includes.
- Prefer `#include <newdb/...>` for engine public interfaces.
- Avoid adding cross-submodule includes when function parameters can carry dependencies.

## Build

CMake 选项、MSVC `/MT`、MinGW 静态运行时与 CTest 用法见 [BUILD.md](../dev/BUILD.md)。性能 CI 预算见 [PERF_AND_CI_BUDGETS.md](../ci/PERF_AND_CI_BUDGETS.md)；对标 LevelDB/InnoDB 索引见 [COMPARE_LEVELDB_INNODB_ROADMAP.md](../roadmap/COMPARE_LEVELDB_INNODB_ROADMAP.md)。
