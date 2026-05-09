# Wave 9+ — Session memory model and `shell_state.h` decoupling

This document records the **deferred** work to stop embedding `newdb::Session` by value inside `ShellState`, so that `shell_state.h` no longer forces every including TU to compile the session, heap, and MVCC dependency graph.

## Goal

- Reduce compile-time coupling: handlers and support code that only need paths, logging, or txn facades should not transitively compile full `Session` internals via `shell_state.h`.
- Preserve (or explicitly redefine) **runtime** semantics: heap locking, teardown, and any async or service threads that observe `ShellState`.

## Candidate directions

1. **`std::unique_ptr<newdb::Session>`** on `ShellState`, constructed after `ShellState` is partially valid; default construction and reset paths must stay correct for demos and tests.
2. **Engine-owned session handle** (opaque id + API on `newdb_core`) with the CLI holding only the handle; largest ABI and ownership boundary change.

## Risks (must be reviewed before implementation)

- **`lock_heap` / `session_heap_guard`**: Today callers assume a stable `Session` object address for the lifetime of `ShellState`. Pointer indirection is usually fine, but any code storing raw `Session*` across rebind or reset must be audited.
- **Teardown and re-open**: Multi-run demos, script runners, and tests that reset or replace session state must not double-free or leave dangling references in txn/LSM/sidecar bundles.
- **Concurrency**: Any background work or callbacks that capture `ShellState&` or `Session&` need a clear invalidation story if the session is destroyed or moved under them.

## Review checklist (gate for coding) — **completed (2026-05)**

Sign-off for the CLI-side pointer milestone below.

1. **`Session&` inventory**: Handlers and services obtain `Session` via `ShellState::session()` / `ShellStateFacade::session()`; long-lived captures are the existing LSM/demo hooks that already bound `ShellState&` / `Session&` — behavior unchanged relative to a stable `Session` address for the lifetime of `ShellState` (session storage is now `unique_ptr`, heap address stable until `ShellState` destruction).
2. **Default construct / move**: `ShellState` is still **non-copyable**; default ctor builds an empty `Session` via `make_unique`. No move assignment added.
3. **Tests**: `ctest -C Release -L newdb` passed after the prototype on Windows MSVC Release (full `newdb_shared`, not `NEWDB_SHARED_SLIM`).
4. **Slim vs full**: Full shared library build exercised; slim remains the separate [`CORE_SHELL_INCLUDE_BOUNDARY.md`](CORE_SHELL_INCLUDE_BOUNDARY.md) path (engine-only C API without `newdb_shell`).

## Relation to Wave 9 (completed) refactors

Wave 9 focused on **handler include slimming** (`ShellStateFacade`, path helpers, removing `schema_io.h` from `shell_state.h`). Session indirection is **not** part of that wave; implement it only after this review is signed off.

## Wave 10 (completed) — compile coupling only

Wave 10 continued **TU-level slimming** (`demo_verbose` → `ShellStateFacade`, consolidating `logging_bind_shell` into [`shell_state_facade.cc`](../../cli/shell/state/shell_state_facade.cc), dropping the standalone logging bridge TU). It does **not** change `Session` storage or ABI. The **`Session` memory-model milestone below remains gated** on this document’s review checklist.

## Wave 11 (completed) — benchmark types only

[`ShellBenchmarkProfile`](../../cli/shell/state/shell_state_benchmark.h) / [`ShellRuntimePolicy`](../../cli/shell/state/shell_state_benchmark.h) were moved out of [`shell_state.h`](../../cli/shell/state/shell_state.h) into a slim header so [`lsm_lite_service.cc`](../../cli/shell/dispatch/services/lsm/lsm_lite_service.cc) no longer includes the full aggregate header; [`ShellStateFacade::runtime_policy()`](../../cli/shell/state/shell_state_facade.h) exposes the same fields for services. **`newdb::Session` is still embedded by value in `ShellState`**; no change to lock/teardown semantics.

## Wave 12 — baseline audit

Baseline recount and demo/C API bridge audit are recorded in [`SHELL_STATE_INCLUDE_AUDIT.md`](SHELL_STATE_INCLUDE_AUDIT.md) (Wave 12 section).

## Wave 13 (completed) — `unique_ptr<Session>` + slim `shell_state.h`

- `ShellState` owns `std::unique_ptr<newdb::Session>` and an internal heap-guard holder so [`shell_state.h`](../../cli/shell/state/shell_state.h) **does not** `#include` [`session.h`](../../engine/include/newdb/session.h). Path helpers that touched `Session` were moved to [`shell_state.cc`](../../cli/shell/state/shell_state.cc).
- Accessors: `ShellState::session()` / `session() const`; call sites use `st.session()` instead of `st.session`.
- **MSVC**: Forward-declare `newdb::Session` as **`struct Session`** to match [`session.h`](../../engine/include/newdb/session.h). A `class` forward declaration can produce a different mangling for `const Session&` member functions than TUs that include the real **struct** definition, causing unresolved external symbols at link time.

Optional next step — opaque engine-owned session handle — is tracked in [`ENGINE_SESSION_HANDLE.md`](ENGINE_SESSION_HANDLE.md).

## Wave 14 (completed) — `ShellState` pimpl + narrower dispatch includes

- Public [`shell_state.h`](../../cli/shell/state/shell_state.h) is a **class** with `unique_ptr<Impl>`; layout is declared in [`shell_state_impl.h`](../../cli/shell/state/shell_state_impl.h) (included from [`shell_state.cc`](../../cli/shell/state/shell_state.cc) and [`shell_state_ops.cc`](../../cli/shell/state/shell_state_ops.cc)). **Wave 17:** `shell_state_impl.h` no longer includes `<newdb/session.h>`; [`shell_state_heap_guard_internal.h`](../../cli/shell/state/shell_state_heap_guard_internal.h) defines [`HeapGuardBox`](../../cli/shell/state/shell_state_heap_guard_internal.h). Former public fields (`data_dir`, `log_file_path`, flags, `runtime_policy`) are **accessors**.
- [`txn_handler.cc`](../../cli/shell/dispatch/handlers/txn/txn_handler.cc) no longer includes `session.h` for JSON stats; [`ShellStateFacade`](../../cli/shell/state/shell_state_facade.h) exposes heap decode counters. [`fast_index_impl.cc`](../../cli/shell/dispatch/support/index/fast_index_impl.cc) includes [`heap_table.h`](../../engine/include/newdb/heap_table.h) only. [`shell_state_lsm.h`](../../cli/shell/state/shell_state_lsm.h) includes [`row.h`](../../engine/include/newdb/row.h) instead of `session.h`.
- Handler alias: [`shell_session_view.h`](../../cli/shell/state/shell_session_view.h) (`ShellSessionView` = `ShellStateFacade`).

## Wave 15 (completed) — facade session ops + owner entrypoint + slim CI matrix

- [`ShellStateFacade`](../../cli/shell/state/shell_state_facade.h): `ensure_loaded()`, `invalidate_session()`, `emit_where_plan_json(...)` (Wave 15: body in facade `.cc`; **Wave 17:** `emit_where_plan_json` / session field touches move to [`shell_state_ops.cc`](../../cli/shell/state/shell_state_ops.cc) / [`ShellState`](../../cli/shell/state/shell_state.h) so [`shell_state_facade.cc`](../../cli/shell/state/shell_state_facade.cc) needs **no** `<newdb/session.h>`). [`demo_runner.cc`](../../cli/shell/bootstrap/demo_runner.cc) uses the facade for table/path/`ensure_loaded` without extra `session()` chains where possible. [`demo_shell.cc`](../../cli/shell/repl/demo_shell.cc) drops unused `<newdb/session.h>`.
- [`ShellStateOwner`](../../cli/shell/state/shell_state_owner.h): [`demo_main.cc`](../../cli/app/demo_main.cc) owns shell state without `#include "cli/shell/state/shell_state.h"` (implementation in [`shell_state.cc`](../../cli/shell/state/shell_state.cc); Wave 16 removed standalone `shell_state_owner.cc`).
- [`HeapGuardBox`](../../cli/shell/state/shell_state_heap_guard_internal.h): see Wave 17 (was documented beside `shell_state_impl.h` before the internal header split).
- Engine opaque typedef: [`engine_session_opaque.h`](../../engine/include/newdb/engine_session_opaque.h). CI: [`CI_SLIM_FULL_MATRIX.md`](CI_SLIM_FULL_MATRIX.md) + job `linux-release-slim-shared` in [`.github/workflows/newdb-ci-reusable.yml`](../../../.github/workflows/newdb-ci-reusable.yml).

## Wave 16 (completed) — finish CLI decoupling metrics

- [`ShellState`](../../cli/shell/state/shell_state.h): `heap_decode_slot_{calls,hits,misses}()` so [`_generated_runtime_json.inc`](../../cli/shell/c_api/_generated_runtime_json.inc) and the C API bridge need **no** `<newdb/session.h>` for those fields.
- [`demo_runner_cli_batch.h`](../../cli/shell/bootstrap/demo_runner_cli_batch.h) + implementation in [`shell_state_ops.cc`](../../cli/shell/state/shell_state_ops.cc): batch `Session` + page JSON; **no** `#include "cli/shell/state/shell_state.h"` at call sites (resolved path from [`demo_runner.cc`](../../cli/shell/bootstrap/demo_runner.cc)).
- [`demo_runner.cc`](../../cli/shell/bootstrap/demo_runner.cc): **no** `<newdb/session.h>`.
- [`ShellStateOwner`](../../cli/shell/state/shell_state_owner.h) implementation merged into [`shell_state.cc`](../../cli/shell/state/shell_state.cc) (removed standalone `shell_state_owner.cc`).
- **`count_shell_state_includes.py` → 9** TUs including `shell_state.h`.

## Wave 17 (completed) — `session.h` quarantine + engine session handle stub

- [`shell_state_impl.h`](../../cli/shell/state/shell_state_impl.h): forward-declares `Session` / `HeapGuardBox`; **no** `<newdb/session.h>`.
- [`shell_state_heap_guard_internal.h`](../../cli/shell/state/shell_state_heap_guard_internal.h): `HeapGuardBox` + `#include <newdb/session.h>` (include only from [`shell_state.cc`](../../cli/shell/state/shell_state.cc) and [`shell_state_ops.cc`](../../cli/shell/state/shell_state_ops.cc)).
- [`ShellState`](../../cli/shell/state/shell_state.h): `session_table_name`, `session_data_path`, `session_schema`, `session_heap_table`, `session_ensure_loaded`, `session_invalidate`, `emit_where_plan_json` so [`shell_state_facade.cc`](../../cli/shell/state/shell_state_facade.cc) stays free of `session.h`.
- [`newdb_engine_session_create`](../../engine/include/newdb/engine_session_handle.h) / [`destroy`](../../engine/include/newdb/engine_session_handle.h) stub in [`engine_session_handle.cpp`](../../engine/src/session/api/engine_session_handle.cpp) (`newdb_core`); ADR [`ENGINE_SESSION_HANDLE.md`](ENGINE_SESSION_HANDLE.md).

## Wave 18 (completed) — batch `Session` folded into `shell_state_ops.cc`

- Removed [`demo_runner_cli_batch.cc`](../../cli/shell/bootstrap/demo_runner_cli_batch.cc); [`demo_run_cli_batch_*`](../../cli/shell/bootstrap/demo_runner_cli_batch.h) defined beside other shell ops. **No** `cli/**/*.cc` file `#include <newdb/session.h>` except transitively via [`shell_state_heap_guard_internal.h`](../../cli/shell/state/shell_state_heap_guard_internal.h).
