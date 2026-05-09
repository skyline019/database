# Engine-owned session handle (optional next boundary)

**Status:** **`newdb_engine_session_create` / `destroy` own the canonical `newdb::Session` for the full (non-slim) C API** and for **[`ShellStateOwner`](../../cli/shell/state/shell_state_owner.h)** used by `newdb_demo`: the owner holds an engine handle and constructs [`ShellState(ShellStateEngineBorrowedTag, handle)`](../../cli/shell/state/shell_state.h) (fallback to owned `ShellState()` only if handle creation fails). [`NewdbCApiCliSession`](../../engine/include/newdb/c_api_cli_bridge.h) uses the same borrow pattern. Borrow access: [`engine_session_access.h`](../../engine/include/newdb/engine_session_access.h). Slim [`newdb_session_create`](../../engine/src/api/c/c_api_slim.cpp) uses the engine handle without `newdb_shell`. Direct `ShellState()` construction (e.g. tests) still uses an **owned** `Session` when no engine host is passed.

**Scaffolding:** C typedef [`newdb_engine_session_t`](../../engine/include/newdb/engine_session_opaque.h); narrow C API in [`engine_session_handle.h`](../../engine/include/newdb/engine_session_handle.h).

This document is the **ADR-style sketch** for moving session lifetime behind an engine API if product requires it.

**CI:** See [`CI_SLIM_FULL_MATRIX.md`](CI_SLIM_FULL_MATRIX.md) for a dual `NEWDB_SHARED_SLIM` / full build recipe.

## Problem

- `newdb::Session` is a heavy C++ type (heap, MVCC, mutex). CLI and full C API still compile against it indirectly wherever `session.h` is included.
- Slim shared builds ([`NEWDB_SHARED_SLIM`](../../CMakeLists.txt)) intentionally avoid linking `newdb_shell`; a **stable opaque handle** would let the engine expose a minimal ABI without duplicating ownership rules in [`c_api_cli_bridge.cc`](../../cli/shell/c_api/c_api_cli_bridge.cc).

## Proposed API shape (illustrative, not committed)

**C ABI (stable symbols)**

- `newdb_engine_session*` — opaque pointer; `nullptr` on failure.
- `newdb_engine_session_create(const char* data_dir, const char* default_table, const char* log_path, uint32_t flags)` — returns handle; `flags` reserved (e.g. encrypt log).
- `newdb_engine_session_destroy(newdb_engine_session*)` — idempotent; releases heap locks and WAL bindings in a defined order.
- Narrow mutators: `set_table`, `execute_normalized_line`, `runtime_stats_json`, `where_plan_json` — mirror current C API surface where needed.

**C++ (`newdb_core`)**

- `class EngineSession` in an internal namespace; handle is `EngineSession*` or `shared_ptr` held only inside the engine DLL.
- No `Session` in public headers consumed by embedders; only POD views / JSON strings for stats.

## Lifecycle and teardown

1. **Single-threaded default**: handle methods are not re-entrant unless documented; concurrent use requires explicit “session per thread” or a future threaded API.
2. **Destroy order**: drop `HeapAccess` / heap guards → flush or detach log → tear down txn coordinator → destroy `Session` storage. Must match today’s [`shell_invalidate_session_table`](../../cli/shell/state/shell_state_ops.h) / `reset_session_heap_guard` semantics.
3. **Multi-run demos**: recreating a handle must not leave stale vacuum callbacks or FD mirrors; engine registers callbacks with **weak ownership** or explicit unregister on destroy.

## Threading contract

- Reuse the checklist in [`WAVE9_SESSION_DECOUPLING.md`](WAVE9_SESSION_DECOUPLING.md): any background vacuum / LSM work that captured `ShellState&` today must instead capture **handle id + engine dispatch queue**, or remain single-threaded.
- `lock_heap` equivalent becomes an engine call returning an **opaque guard token** or a narrow “mutate table” closure; document mutex nesting vs `invalidate()`.

## Slim vs full (`newdb_shared`)

| Build | Behavior |
|-------|----------|
| `NEWDB_SHARED_SLIM` | Stub or minimal `newdb_engine_session_*` returning `NOT_IMPLEMENTED` for CLI-only features; no link to `newdb_shell`. |
| Full shared | Implementation delegates to current CLI/session stack **or** a refactored core-only path; **one** ownership story. |

CI should build both and run `ctest -C Release -L newdb` on full; slim runs existing slim tests only.

## Migration (high level)

1. Introduce handle + implementation in `newdb_core` behind `#ifdef` or feature flag; keep `ShellState` as a thin adapter over the handle for one release.
2. Move [`NewdbCApiCliSession`](../../cli/shell/c_api/c_api_cli_bridge.cc) to store handle instead of `ShellState` when flag is on.
3. Delete adapter once all call sites use engine API; slim C API aligns with handle-only symbols.

## Relation

Until the above is approved, **`ShellState` + facade + `ShellSessionView` alias** remain the supported CLI layering ([`shell_session_view.h`](../../cli/shell/state/shell_session_view.h)).
