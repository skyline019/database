# C API capability tiers (Engine vs CLI)

This document maps [`newdb/engine/include/newdb/c_api.h`](../../engine/include/newdb/c_api.h) symbols to **which link layer** satisfies them. Use it when choosing [`NEWDB_SHARED_SLIM`](../../CMakeLists.txt) vs full shared or [`newdb_capi_adapter`](../../CMakeLists.txt) vs [`newdb_shell`](../../CMakeLists.txt).

## Tiers

| Tier | Meaning | Typical CMake targets |
|------|---------|------------------------|
| **EngineTier** | Implemented with [`newdb_core`](../../CMakeLists.txt) only (`Session`, WAL, heap, schema I/O, helpers). | `newdb_core` |
| **CliEmbedTier** | Needs REPL/dispatch stack but **not** interactive-only TU (no [`demo_shell.cc`](../../cli/shell/repl/demo_shell.cc)). | `newdb_capi_adapter` |
| **CliFullTier** | Full CLI including REPL / [`interactive_shell`](../../cli/shell/repl/demo_shell.cc). | `newdb_shell` |

## Symbol matrix

| C API | EngineTier | CliEmbedTier | CliFullTier | Notes |
|-------|------------|--------------|-------------|-------|
| Version / ABI / `newdb_sum` / `newdb_error_code_string` | yes | yes | yes | |
| `newdb_check_schema_file` | yes | yes | yes | |
| `newdb_session_create` / `destroy` | yes (engine handle) | yes | yes | Slim omits shell; full uses bridge |
| `newdb_session_set_table` | partial | yes | yes | Slim: logical name only; full/embed: schema reload via shell session ops |
| `newdb_session_execute` | stub (slim) | yes | yes | **Full/embed:** [`newdb_session_execute`](../../engine/src/api/c/c_api.cpp) first calls [`try_engine_execute_fastpath`](../../cli/shell/c_api/c_api_cli_bridge.cc): bare **`COUNT`**, **`SHOW TUNING JSON`**, **`SHOW STATUS JSON`**, **`SHOW STORAGE`** ([`emit_show_storage_log_lines`](../../cli/shell/c_api/show_storage_log.h)) skip [`process_command_line`](../../cli/shell/dispatch/router/dispatch.cc); other commands use dispatch. **Dispatch** 覆盖与 REPL 相同的动词集合（含批量 WHERE 写 **`UPDATEWHERE`** / **`DELETEWHERE`**、`SETATTRMULTI` 等），无单独 C 符号；嵌入集成只需传入与普通 CLI 一致的 UTF-8 命令行（经由 [`cli_dispatch_execute_normalized_line`](../../cli/shell/c_api/cli_dispatch_command_line.cc) / [`process_command_line_normalized`](../../cli/shell/c_api/c_api_cli_bridge.cc)）。[`txn_handler.cc`](../../cli/shell/dispatch/handlers/txn/txn_handler.cc) 等对 tuning/status JSON 与 `SHOW STORAGE` 的共享路径同上。**Interactive CLI:** bare `COUNT` may still early-return in [`query_handler.cc`](../../cli/shell/dispatch/handlers/query/query_handler.cc). |
| `newdb_session_runtime_stats` | stub (slim) | yes | yes | Same JSON object as `SHOW TUNING JSON`; POD fields emitted via [`runtime_stats_snapshot_json_write`](../../engine/src/json/runtime_stats_snapshot_json_write.cpp), coordinator/shell fields in [`_generated_runtime_json.inc`](../../cli/shell/c_api/_generated_runtime_json.inc) (included from [`runtime_stats_json_builder.cc`](../../cli/shell/c_api/runtime_stats_json_builder.cc)); see [`RUNTIME_STATS_JSON_LAYERING.md`](RUNTIME_STATS_JSON_LAYERING.md) |
| `newdb_session_where_plan_json` | stub (slim) | yes | yes | **CliEmbedTier / non-goal for pure core:** planner and parse stack are built as [`newdb_query`](../../CMakeLists.txt) (sources under `cli/modules/where`) and linked via [`newdb_capi_adapter`](../../CMakeLists.txt); promoting symbols into `newdb_core` is Track Q ([`NEWDB_QUERY_LAYER.md`](../architecture/NEWDB_QUERY_LAYER.md), [`CORE_SHELL_INCLUDE_BOUNDARY.md`](CORE_SHELL_INCLUDE_BOUNDARY.md)). |
| `newdb_session_append_runtime_snapshot` | stub / append | yes | yes | |

## Optional: dynamic CLI backend (CMake `NEWDB_C_API_PLUGIN_BACKEND`)

Runtime-loaded backend module and env `NEWDB_CLI_BACKEND_PATH`: [`C_API_PLUGIN_BACKEND.md`](C_API_PLUGIN_BACKEND.md). Legacy vtable sketch: [`c_api_backend_plugin.h`](../../engine/include/newdb/c_api_backend_plugin.h).

**Capability parity:** After a successful load, the **symbol matrix above still applies** (CliEmbedTier / CliFullTier behavior lives inside `newdb_cli_backend`, which links [`newdb_capi_adapter`](../../CMakeLists.txt) + [`newdb_query`](../../CMakeLists.txt)). The **host** `newdb_shared` is only EngineTier for link purposes; integration tests for plugin use `CliBackendPluginSmoke` and [`plugin_backend_packaging.md`](../../scripts/ci/plugin_backend_packaging.md). **ABI revision** used in CI: run `python3 newdb/scripts/validate/check_c_api_abi.py --expected-abi 1` (see [newdb-ci-reusable.yml](../../../.github/workflows/newdb-ci-reusable.yml) `c-api-abi-gate`).

## Slim builds and missing-backend semantics

- **`NEWDB_SHARED_SLIM`**: [`newdb_shared`](../../CMakeLists.txt) exposes only [`c_api_slim.cpp`](../../engine/src/api/c/c_api_slim.cpp). CliEmbedTier entry points (`newdb_session_execute`, `newdb_session_runtime_stats`, `newdb_session_where_plan_json`, …) return **stub JSON / documented error strings** instead of full coordinator/WHERE behavior.
- **`NEWDB_C_API_PLUGIN_BACKEND`**: if `NEWDB_CLI_BACKEND_PATH` does not load or symbol resolution fails, [`newdb_session_create`](../../engine/include/newdb/c_api.h) returns **`NULL`** with **no session handle** — document as backend unavailable (see `NEWDB_ERR_BACKEND_UNAVAILABLE` in [`c_api.h`](../../engine/include/newdb/c_api.h)).

## Related

- Dual configure matrix: [`CI_SLIM_FULL_MATRIX.md`](CI_SLIM_FULL_MATRIX.md)
- Session ownership: [`ENGINE_SESSION_HANDLE.md`](ENGINE_SESSION_HANDLE.md)
- Include boundaries: [`CORE_SHELL_INCLUDE_BOUNDARY.md`](CORE_SHELL_INCLUDE_BOUNDARY.md)
- Decoupling roadmap / presets / `newdb_query`: [`../architecture/DECOUPLING_ROADMAP.md`](../architecture/DECOUPLING_ROADMAP.md), [`../architecture/NEWDB_QUERY_LAYER.md`](../architecture/NEWDB_QUERY_LAYER.md)
