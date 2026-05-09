# ShellState layering (Wave 7+ review)

## Logical layers (current code)

1. **Session / heap**: `ShellState` is a **pimpl** (`ShellState::Impl` in [`shell_state_impl.h`](../../cli/shell/state/shell_state_impl.h)); the public header only forward-declares `Session` and exposes `session()`, path accessors, and `log_file_path()` / `data_dir()` / flags via methods (Wave 14). [`shell_state.h`](../../cli/shell/state/shell_state.h) **does not** include [`session.h`](../../engine/include/newdb/session.h). Path helpers and ctor live in [`shell_state.cc`](../../cli/shell/state/shell_state.cc). [`shell_state_ops.cc`](../../cli/shell/state/shell_state_ops.cc) includes [`shell_state_heap_guard_internal.h`](../../cli/shell/state/shell_state_heap_guard_internal.h) for `HeapGuardBox` (`Session::HeapAccess`); [`shell_state_impl.h`](../../cli/shell/state/shell_state_impl.h) has **no** `<newdb/session.h>` (Wave 17). [`shell_state_facade.cc`](../../cli/shell/state/shell_state_facade.cc) also avoids `session.h` by delegating field access to [`ShellState`](../../cli/shell/state/shell_state.h) (`session_table_name`, `session_schema`, `emit_where_plan_json`, …). Optional entry ownership: [`ShellStateOwner`](../../cli/shell/state/shell_state_owner.h). Any TU that dereferences `Session` members must `#include <newdb/session.h>` (or a header that pulls it). Translation units that need [`schema_io.h`](../../engine/include/newdb/schema_io.h) include it directly in their `.cc` files.
2. **Coordinator + WHERE**: held inside [`ShellTxnWhereRuntime`](../../cli/shell/state/shell_state_txn_where_runtime_impl.h), owned by `std::unique_ptr` on `ShellState`; use `txn()` / `where_ctx()` accessors ([`shell_state.cc`](../../cli/shell/state/shell_state.cc)). `shell_state.h` does **not** include `txn_manager.h` / `where.h`.
3. **LSM cache + sidecar tuning**: held inside [`ShellLsmSidecarRuntime`](../../cli/shell/state/shell_state_lsm_sidecar_runtime_impl.h), owned by `std::unique_ptr`; use `lsm()` / `sidecar()` accessors. `shell_state.h` does **not** include [`shell_state_lsm.h`](../../cli/shell/state/shell_state_lsm.h) / [`shell_state_sidecar.h`](../../cli/shell/state/shell_state_sidecar.h) directly.

## Include compile baseline (maintain when refactoring)

Regenerate counts with ripgrep from the repo root (dedupe paths if your tool double-counts Windows/Unix separators):

```bash
rg -l '#include "cli/shell/state/shell_state.h"' newdb --glob '*.cc' --glob '*.cpp' | sort -u | wc -l
```

Or run the maintained script (also wired as CMake target `newdb_count_shell_state_includes` when Python 3 is found):

```bash
python newdb/tools/count_shell_state_includes.py
```

**Snapshot (Wave 18):** [`dispatch.cc`](../../cli/shell/dispatch/router/dispatch.cc) does **not** include `shell_state.h` (facade + ops only). **`count_shell_state_includes.py` reports 9** `newdb/**/*.cc|cpp` translation units that include `shell_state.h`. CLI batch lives in [`shell_state_ops.cc`](../../cli/shell/state/shell_state_ops.cc) ([`demo_runner_cli_batch.h`](../../cli/shell/bootstrap/demo_runner_cli_batch.h)); `<newdb/session.h>` reaches `cli/` only through [`shell_state_heap_guard_internal.h`](../../cli/shell/state/shell_state_heap_guard_internal.h). See [`SHELL_STATE_INCLUDE_AUDIT.md`](SHELL_STATE_INCLUDE_AUDIT.md).

## When to `#include` `txn_manager.h` in a `.cc`

`shell_state.h` forward-declares `TxnCoordinator`. Any translation unit that **calls member functions** on `TxnCoordinator` (via `ShellState::txn()`, `ShellStateFacade::txn()`, or a local `TxnCoordinator&`) must include [`cli/modules/txn/coordinator/txn_manager.h`](../../cli/modules/txn/coordinator/txn_manager.h) (or another header that already brings the full class). Handlers that only use `ShellStateFacade` for session/schema paths may omit it until they touch txn APIs.

## Options for stronger compile-time decoupling (historical table)

| Option | Effect | Risk |
|--------|--------|------|
| A Nested structs | Group members; still one aggregate include unless accessors split headers | Medium churn |
| B `unique_ptr` session + pimpl aggregate | **`Session` and runtime bundles live in `ShellState::Impl`** (Wave 14); public header omits `session.h`. | Engine-owned handle remains optional next step ([`ENGINE_SESSION_HANDLE.md`](ENGINE_SESSION_HANDLE.md)) |
| C View-only headers | Thin read-only helpers; fewest layout changes | Low benefit |

## Current decision

`ShellState` remains the **single session aggregate** for the CLI (no move semantics: `Session` holds a mutex). **`newdb::Session`** is owned via `unique_ptr`; **txn/WHERE** and **LSM/sidecar** still live behind `unique_ptr` bundles with accessors. **`ShellStateFacade`** is the preferred handler surface; [`dispatch.cc`](../../cli/shell/dispatch/router/dispatch.cc) routes logging, paths, and heap-guard reset through the facade so that TU can avoid `shell_state.h`. Optional stronger boundary: engine-owned session handle — [`ENGINE_SESSION_HANDLE.md`](ENGINE_SESSION_HANDLE.md).

## Formal review record (Wave 7+ follow-up)

| Track | Decision | Rationale |
|-------|----------|-----------|
| **A — Nested member grouping** | **Partially done** | `ShellTxnWhereRuntime::TxnWhereMembers` and `ShellLsmSidecarRuntime` group related fields without changing external call style (`txn()`, `lsm()`, …). |
| **B — Opaque txn/WHERE runtime (`unique_ptr`)** | **Implemented** | See logical layer 2 above. |
| **B2 — LSM + sidecar opaque runtime** | **Implemented** | [`shell_state_lsm_sidecar_runtime_impl.h`](../../cli/shell/state/shell_state_lsm_sidecar_runtime_impl.h); accessors on `ShellState`. |
| **C — Views / thin headers** | Adopted incrementally | [`shell_state_fwd.h`](../../cli/shell/state/shell_state_fwd.h), [`shell_state_paths.h`](../../cli/shell/state/shell_state_paths.h), narrow [`shell_state_facade.h`](../../cli/shell/state/shell_state_facade.h). |

### Checklist (txn/WHERE opaque runtime — completed)

1. Inventory callbacks that capture `ShellState&` or `TxnCoordinator&` (see LSM service patterns).
2. Confirm default construction / reset paths for session teardown and multi-session demos.
3. Run full `ctest -C Release -L newdb` plus any GUI smoke tests in CI.

## Wave 9+ — `Session` decoupled from `shell_state.h` (Wave 13)

[`shell_state.h`](../../cli/shell/state/shell_state.h) no longer includes [`session.h`](../../engine/include/newdb/session.h); `ShellState` holds `unique_ptr<newdb::Session>`. **`schema_io.h` was removed from `shell_state.h`** (Wave 9); callers that need schema I/O APIs include that header in their own TUs.

History, risks, and review sign-off: [`WAVE9_SESSION_DECOUPLING.md`](WAVE9_SESSION_DECOUPLING.md). Future optional step (opaque engine handle + ABI): [`ENGINE_SESSION_HANDLE.md`](ENGINE_SESSION_HANDLE.md).
