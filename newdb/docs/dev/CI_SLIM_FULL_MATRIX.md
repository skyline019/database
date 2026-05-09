# Slim vs full shared library — local / CI matrix

`NEWDB_SHARED_SLIM` (see [`newdb/CMakeLists.txt`](../../CMakeLists.txt)) builds `newdb_shared` without CLI closure. Full builds link [`newdb_capi_adapter`](../../CMakeLists.txt) (dispatch / bridge / txn / WHERE / sidecar; **no** [`demo_shell.cc`](../../cli/shell/repl/demo_shell.cc)). Interactive CLI remains [`newdb_shell`](../../CMakeLists.txt) = adapter + REPL TU.

Symbol tiers: [`C_API_CAPABILITY_TIERS.md`](C_API_CAPABILITY_TIERS.md).

## Linkage snapshot

| Build | `newdb_shared` links | Typical role |
|-------|----------------------|--------------|
| **Full** (`NEWDB_SHARED_SLIM=OFF`) | `newdb_core` + `newdb_capi_adapter` | Complete C API: session execute, runtime stats JSON, WHERE plan via dispatch stack. Larger binary; pulls CLI modules (txn coordinator, WHERE, sidecar, dispatch). |
| **Slim** (`NEWDB_SHARED_SLIM=ON`) | `newdb_core` only | Narrow ABI: schema check, version, slim session handle ([`c_api_slim.cpp`](../../engine/src/api/c/c_api_slim.cpp)); `newdb_session_execute` and planner paths return stub / error strings. Smaller surface for embedders that only need engine-adjacent checks. |

**Session ownership note:** Full C API and `newdb_demo` ([`ShellStateOwner`](../../cli/shell/state/shell_state_owner.h)) use [`newdb_engine_session_create`](../../engine/include/newdb/engine_session_handle.h) as the canonical owner of [`Session`](../../engine/include/newdb/session.h) and borrow into [`ShellState`](../../cli/shell/state/shell_state.h) ([`ENGINE_SESSION_HANDLE.md`](ENGINE_SESSION_HANDLE.md)). Tests may still construct [`ShellState`](../../cli/shell/state/shell_state.h) directly with an owned `Session`.

## C API surface by build ([`c_api.h`](../../engine/include/newdb/c_api.h))

| API | Slim | Full |
|-----|------|------|
| `newdb_version_string`, `newdb_api_version_*`, `newdb_abi_version`, `newdb_negotiate_abi` | Yes | Yes |
| `newdb_error_code_string` | Yes | Yes |
| `newdb_sum` | Yes | Yes |
| `newdb_check_schema_file` | Yes (engine loader) | Yes |
| `newdb_session_create` / `destroy` | Yes (engine-backed handle; no shell) | Yes (engine + borrowed `ShellState` / bridge) |
| `newdb_session_set_table` | Yes (updates logical table name only) | Yes (schema reload via shell path) |
| `newdb_session_last_error` | Yes | Yes |
| `newdb_session_execute` | Stub / `NEWDB_SHARED_SLIM` error text | Bridge [`try_engine_execute_fastpath`](../../cli/shell/c_api/c_api_cli_bridge.cc) (`COUNT`, `SHOW TUNING JSON`, `SHOW STATUS JSON`, `SHOW STORAGE`) then [`process_command_line`](../../cli/shell/dispatch/router/dispatch.cc) fallback |
| `newdb_session_runtime_stats` | Stub JSON (`slim` marker) | Full JSON from coordinator / shell metrics |
| `newdb_session_where_plan_json` | Stub JSON | Planner + `ShellState` |
| `newdb_session_append_runtime_snapshot` | Appends stub line | Appends full snapshot line |

## Recommended local check (two build trees)

From the repository root:

```bash
# Full (default)
cmake -S newdb -B build-full -DNEWDB_BUILD_SHARED=ON -DNEWDB_SHARED_SLIM=OFF
cmake --build build-full --config Release
ctest --test-dir build-full -C Release -L newdb

# Slim
cmake -S newdb -B build-slim -DNEWDB_BUILD_SHARED=ON -DNEWDB_SHARED_SLIM=ON
cmake --build build-slim --config Release
ctest --test-dir build-slim -C Release -L newdb
```

On Windows MSVC, use `--config Release` with both `cmake --build` and `ctest -C Release`.

## CI

Workflow [`.github/workflows/newdb-ci-reusable.yml`](../../../.github/workflows/newdb-ci-reusable.yml): **`linux-release-slim-shared`** configures `-DNEWDB_SHARED_SLIM=ON` and runs `newdb`-labeled tests on the slim target; **`linux-c-api-plugin-backend-smoke`** and **`windows-c-api-plugin-backend-smoke`** configure `-DNEWDB_C_API_PLUGIN_BACKEND=ON -DNEWDB_BUILD_CLI_BACKEND_PLUGIN=ON` and run `CliBackendPluginSmoke` (see [`C_API_PLUGIN_BACKEND.md`](C_API_PLUGIN_BACKEND.md)). Full Linux GCC / release gates remain the primary matrix.

Cross-reference: [`ENGINE_SESSION_HANDLE.md`](ENGINE_SESSION_HANDLE.md), [`CORE_SHELL_INCLUDE_BOUNDARY.md`](CORE_SHELL_INCLUDE_BOUNDARY.md).
