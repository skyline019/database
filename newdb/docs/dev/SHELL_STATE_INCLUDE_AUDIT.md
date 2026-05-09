# Audit: translation units that `#include "cli/shell/state/shell_state.h"`

Regenerate the list with [`tools/count_shell_state_includes.py`](../../tools/count_shell_state_includes.py). **Wave 13:** `shell_state.h` no longer includes `<newdb/session.h>`; `Session` is owned via `unique_ptr` (access `st.session()` / `ShellStateFacade::session()`). **Wave 14:** `ShellState` uses a **pimpl** (`ShellState::Impl` in [`shell_state_impl.h`](../../cli/shell/state/shell_state_impl.h)); fields such as `data_dir` / `log_file_path` / flags are **accessors** (`st.data_dir()`, `st.log_file_path()`, …), shrinking the public header’s coupling to layout.

**Current TU count:** run `python tools/count_shell_state_includes.py` from `newdb/` — **6** as of the post–Wave-C test harness split (**5** production CLI TUs below + **`tests/shell_state_test_support.cc`** for `std::unique_ptr<ShellState>` deleter / factories). Earlier docs cited **9** when four integration tests still included `shell_state.h` directly; those tests now use [`ShellStateFacade`](../../cli/shell/state/shell_state_facade.h) + [`ShellStateOwner::shell()`](../../cli/shell/state/shell_state_owner.h) without pulling the aggregate header. Runtime stats builder uses [`shell_state_facade.h`](../../cli/shell/state/shell_state_facade.h) only (no direct `shell_state.h`). [`demo_main.cc`](../../cli/app/demo_main.cc) uses [`ShellStateOwner`](../../cli/shell/state/shell_state_owner.h) (implemented in `shell_state.cc`) and **does not** include `shell_state.h`. [`demo_shell.cc`](../../cli/shell/repl/demo_shell.cc) does not include `<newdb/session.h>`.

**Wave 17–18 (`<newdb/session.h>` in `cli/`):** No translation unit under `cli/` includes it **directly** except via [`shell_state_heap_guard_internal.h`](../../cli/shell/state/shell_state_heap_guard_internal.h) (pulled by [`shell_state.cc`](../../cli/shell/state/shell_state.cc) + [`shell_state_ops.cc`](../../cli/shell/state/shell_state_ops.cc)). [`shell_state_impl.h`](../../cli/shell/state/shell_state_impl.h) and [`shell_state_facade.cc`](../../cli/shell/state/shell_state_facade.cc) **do not** include `session.h`. **Wave 18:** CLI batch entrypoints ([`demo_runner_cli_batch.h`](../../cli/shell/bootstrap/demo_runner_cli_batch.h)) are implemented in `shell_state_ops.cc`; standalone [`demo_runner_cli_batch.cc`](../../cli/shell/bootstrap/demo_runner_cli_batch.cc) removed.

| TU | Needs `shell_state.h` because |
|----|------------------------------|
| [`cli/shell/bootstrap/demo_runner.cc`](../../cli/shell/bootstrap/demo_runner.cc) | Batch/import/exec paths, `ShellState&` orchestration, `resolve_table_file`, callbacks. |
| [`cli/shell/c_api/c_api_cli_bridge.cc`](../../cli/shell/c_api/c_api_cli_bridge.cc) | `NewdbCApiCliSession::Impl` embeds `ShellState` by value; bridge methods. |
| [`cli/shell/state/shell_state.cc`](../../cli/shell/state/shell_state.cc) | Defines `ShellState`, `ShellStateOwner`, path helpers. |
| [`cli/shell/state/shell_state_facade.cc`](../../cli/shell/state/shell_state_facade.cc) | Facade; `logging_bind_shell`; delegates session fields / `emit_where_plan_json` to `ShellState` (**no** `<newdb/session.h>`). |
| [`cli/shell/state/shell_state_ops.cc`](../../cli/shell/state/shell_state_ops.cc) | Heap guard, schema reload, `get_cached_table` (includes internal impl header). |
| [`tests/shell_state_test_support.cc`](../../tests/shell_state_test_support.cc) | Test-only `ShellState` heap owner / deleter; keeps `shell_state.h` out of other test TUs. |

**Already slimmed (no longer include `shell_state.h`):** integration tests [`test_demo_lsm_lite.cpp`](../../tests/test_demo_lsm_lite.cpp), [`test_demo_mdb_stop.cpp`](../../tests/test_demo_mdb_stop.cpp), [`test_show_plan_table_stats_stale_shell.cpp`](../../tests/test_show_plan_table_stats_stale_shell.cpp), [`test_txn_shell_multi_entry_snapshot.cpp`](../../tests/test_txn_shell_multi_entry_snapshot.cpp) (Facade + `ShellStateOwner`); [`cli/app/demo_main.cc`](../../cli/app/demo_main.cc) ([`ShellStateOwner`](../../cli/shell/state/shell_state_owner.h)); [`demo_diag.cc`](../../cli/shell/diag/demo_diag.cc) (`ShellStateFacade` only); [`lsm_lite_service.cc`](../../cli/shell/dispatch/services/lsm/lsm_lite_service.cc) uses [`shell_state_benchmark.h`](../../cli/shell/state/shell_state_benchmark.h) + facade + fwd header. Batch APIs: header [`demo_runner_cli_batch.h`](../../cli/shell/bootstrap/demo_runner_cli_batch.h) only; body in [`shell_state_ops.cc`](../../cli/shell/state/shell_state_ops.cc).

### Integration tests (optional further slimming)

Further reductions would target remaining production TUs (e.g. dispatch) via facade-only APIs ([`WAVE9_SESSION_DECOUPLING.md`](WAVE9_SESSION_DECOUPLING.md)); unify CLI `ShellState` ownership with [`newdb_engine_session_create`](../../engine/include/newdb/engine_session_handle.h) when product wants a single lifecycle ([`ENGINE_SESSION_HANDLE.md`](ENGINE_SESSION_HANDLE.md)).

**CI:** `python3 newdb/tools/count_shell_state_includes.py --fail-if-count-above 10` runs on the Linux GCC job (ceiling **10**; current baseline **6**). Bridge/dispatch coupling: [`tools/count_bridge_dispatch_includes.py`](../../tools/count_bridge_dispatch_includes.py) with ceilings on `newdb/c_api_cli_bridge.h` and `dispatch.h` includes.

### Demo / C API bridge (isolation audit)

| TU | Finding |
|----|---------|
| [`demo_main.cc`](../../cli/app/demo_main.cc) | [`ShellStateOwner`](../../cli/shell/state/shell_state_owner.h); no aggregate header include. |
| [`demo_runner.cc`](../../cli/shell/bootstrap/demo_runner.cc) | Heavy `ShellState&` use: `resolve_table_file`, `txn()`, `reload_schema_from_data_path`, callbacks; **no** `<newdb/session.h>`. |
| [`c_api_cli_bridge.cc`](../../cli/shell/c_api/c_api_cli_bridge.cc) | `Impl` holds `ShellState shell`; touches `session()`, schema reload, `process_command_line`. |

**Recommendation:** prefer [`ShellStateFacade`](../../cli/shell/state/shell_state_facade.h) + [`shell_state_paths.h`](../../cli/shell/state/shell_state_paths.h) in new handler code; use `heap_table()` / path accessors instead of pulling `<newdb/session.h>` when only table/schema paths are needed.
