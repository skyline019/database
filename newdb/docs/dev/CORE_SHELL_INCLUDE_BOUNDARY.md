# newdb_core vs CLI / shell include boundary

## Intent

- **`newdb_core`** ([`newdb/CMakeLists.txt`](../../CMakeLists.txt) `add_library(newdb_core STATIC ...)`) is the **engine** static library: schema, heap, WAL, session API, MVCC, etc.
- It must **not** `#include` headers under `cli/` (shell, dispatch, txn coordinator at CLI layer, WHERE executor, etc.). Those belong to **`newdb_capi_adapter`** / **`newdb_shell`** (see [`CMakeLists.txt`](../../CMakeLists.txt)) and test aggregations.

## Known exceptions

- There are **no** intentional `cli/` includes from files listed in `newdb_core` today. If you add engine code under `engine/`, keep this rule.
- **C API (full)**: [`engine/src/api/c/c_api.cpp`](../../engine/src/api/c/c_api.cpp) is **not** part of `newdb_core`. With `NEWDB_BUILD_SHARED` and not `NEWDB_SHARED_SLIM`, `newdb_shared` links **`newdb_capi_adapter`** (dispatch / bridge stack; **no** interactive [`demo_shell.cc`](../../cli/shell/repl/demo_shell.cc)). [`NewdbCApiCliSession`](../../engine/include/newdb/c_api_cli_bridge.h) uses [`newdb_engine_session_t`](../../engine/include/newdb/engine_session_handle.h); [`ShellState`](../../cli/shell/state/shell_state.h) borrows `Session` via [`ShellStateEngineBorrowedTag`](../../cli/shell/state/shell_state.h). `c_api.cpp` still avoids direct `cli/` includes.
- **Slim shared library**: `NEWDB_SHARED_SLIM=ON` builds [`c_api_slim.cpp`](../../engine/src/api/c/c_api_slim.cpp) only against `newdb_core` (stub session API).
- **WHERE planner / `newdb_session_where_plan_json`**: implementation sources remain under `cli/modules/where` but are built as the **`newdb_query`** static library and linked through **`newdb_capi_adapter`** ([`NEWDB_QUERY_LAYER.md`](../architecture/NEWDB_QUERY_LAYER.md)). There is still no engine-only substitute without re-homing the planner into `newdb_core`. Capability tier: [`C_API_CAPABILITY_TIERS.md`](C_API_CAPABILITY_TIERS.md).

## Non-goals (short-term decoupling)

- **`ShellState()` default construction in tests**: prefer [`make_shell_state_for_test`](../../tests/shell_state_test_support.h); marking the default constructor `[[deprecated]]` is reserved for a **major** release after auditing warning fallout.

## Compile coupling baselines (audit)

Optional spot-check (same scripts as CI budgets):

- [`tools/count_shell_state_includes.py`](../../tools/count_shell_state_includes.py) — e.g. `--fail-if-count-above 10` in [`.github/workflows/newdb-ci-reusable.yml`](../../../.github/workflows/newdb-ci-reusable.yml).
- [`tools/count_bridge_dispatch_includes.py`](../../tools/count_bridge_dispatch_includes.py) — e.g. `--fail-if-bridge-above 5 --fail-if-dispatch-above 10`.

**ShellState test hygiene (spot audit):** tests that need a real shell should use [`make_shell_state_for_test`](../../tests/shell_state_test_support.h). A quick scan of `newdb/tests/*.cpp` shows no direct `ShellState{}` / default construction in favor of that helper; re-check when adding new shell integration tests. Example include counts (run locally, should stay under CI caps): `shell_state.h` **9** TUs, `c_api_cli_bridge.h` **3**, `dispatch.h` **8** (re-run [`count_shell_state_includes.py`](../../tools/count_shell_state_includes.py) / [`count_bridge_dispatch_includes.py`](../../tools/count_bridge_dispatch_includes.py)).

## Enforcement

At CMake configure time, `newdb_core` sources are scanned for lines matching `#include ... cli/`. A match fails configuration with `message(FATAL_ERROR ...)`.

## Release verification (shared library matrix)

After changing engine/shell boundaries or C API wiring, validate both linkage modes:

1. **Full C API (shared links `newdb_capi_adapter`)** — default when building tests:
   - Configure with `-DNEWDB_BUILD_SHARED=ON -DNEWDB_SHARED_SLIM=OFF -DNEWDB_BUILD_TESTS=ON`.
   - Build at least: `newdb_capi_adapter`, `newdb_shell`, `newdb_tests`, `newdb_shared`, `newdb_capi_integration_tests`.

2. **Slim shared (`NEWDB_C_API_SLIM`)** — stub session, no shell:
   - Configure with `-DNEWDB_BUILD_SHARED=ON -DNEWDB_SHARED_SLIM=ON -DNEWDB_BUILD_TESTS=ON` (use a **fresh build directory** or reconfigure so the option is not stale).
   - Build: `newdb_shared`, `newdb_capi_slim_tests`.

Suggested test filter after builds: `ctest -C Release -L newdb`.

For **CLI-side** compile coupling metrics (`shell_state.h` fan-out, facade baseline), see [`SHELL_STATE_LAYERING.md`](SHELL_STATE_LAYERING.md) and [`SHELL_STATE_INCLUDE_AUDIT.md`](SHELL_STATE_INCLUDE_AUDIT.md) — not duplicate counts here.
