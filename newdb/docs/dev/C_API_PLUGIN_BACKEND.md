# C API dynamic CLI backend (`NEWDB_C_API_PLUGIN_BACKEND`)

Optional linkage mode for [`newdb_shared`](../../CMakeLists.txt): the shared library links **only** [`newdb_core`](../../CMakeLists.txt). Session APIs that normally live in [`newdb_capi_adapter`](../../CMakeLists.txt) are resolved at runtime by loading a separate module.

## Building

- **`NEWDB_BUILD_CLI_BACKEND_PLUGIN=ON`**: builds `newdb_cli_backend` (shared), exporting the C symbols consumed by `newdb_shared` in plugin mode.
- **`NEWDB_C_API_PLUGIN_BACKEND=ON`**: builds `newdb_shared` **without** statically linking `newdb_capi_adapter`. Mutually exclusive with `NEWDB_SHARED_SLIM` (configure will fail if both are set).

Full shared **without** plugin mode remains the default (static `newdb_capi_adapter`).

## Runtime

Set **`NEWDB_CLI_BACKEND_PATH`** to the absolute path of `newdb_cli_backend.dll` (Windows) or `libnewdb_cli_backend.so` (Unix) **before** calling [`newdb_session_create`](../../engine/include/newdb/c_api.h). If the variable is unset or the library fails to load/resolve symbols, [`newdb_session_create`](../../engine/include/newdb/c_api.h) returns `NULL`.

**Loader layout:** prefer an absolute path in `NEWDB_CLI_BACKEND_PATH` (CMake sets this for `newdb_cli_backend_plugin_smoke`). On Linux, if you rely on a bare soname instead, ensure the containing directory is on the dynamic linker search path (`LD_LIBRARY_PATH`, or an `RPATH`/`RUNPATH` on the host binary). On Windows, keep `newdb_cli_backend.dll` on `PATH` or beside the executable that loads `newdb.dll`.

Exported entry points are implemented in [`cli/shell/c_api/cli_backend_exports.cc`](../../cli/shell/c_api/cli_backend_exports.cc) (prefix `newdb_cli_backend_*`).

## CI

Linux and Windows jobs build plugin mode and run `CliBackendPluginSmoke` — see **`linux-c-api-plugin-backend-smoke`** / **`windows-c-api-plugin-backend-smoke`** in [`.github/workflows/newdb-ci-reusable.yml`](../../../.github/workflows/newdb-ci-reusable.yml).

## Productization / deployment stance

When the goal is **no static dependency** on [`newdb_capi_adapter`](../../CMakeLists.txt) inside the main `newdb` shared library, treat **plugin mode** as the shipping configuration:

1. Build artifacts: `newdb.dll`/`libnewdb.so` (engine + C API loader) **and** `newdb_cli_backend.dll`/`libnewdb_cli_backend.so` from the same tree (`NEWDB_BUILD_CLI_BACKEND_PLUGIN=ON`).
2. Ship both next to the host process (GUI, bench harness, language bindings). Set **`NEWDB_CLI_BACKEND_PATH`** to the **absolute** path of the backend module before `newdb_session_create`.
3. Document failure mode: if the backend is missing or symbols fail to resolve, `newdb_session_create` returns `NULL` — surface [`NEWDB_ERR_BACKEND_UNAVAILABLE`](../../engine/include/newdb/c_api.h) at higher layers where applicable.

Full embed mode (static `newdb_capi_adapter`) remains the **single-DLL convenience** default for demos and tools that do not require DLL separation.

**Install / zip layout copy-paste:** [plugin_backend_packaging.md](../../scripts/ci/plugin_backend_packaging.md) (artifact names, `NEWDB_CLI_BACKEND_PATH`, `PATH` / `LD_LIBRARY_PATH`).

## Related

- Capability tiers: [`C_API_CAPABILITY_TIERS.md`](C_API_CAPABILITY_TIERS.md)
- Sketch vtable (historical): [`c_api_backend_plugin.h`](../../engine/include/newdb/c_api_backend_plugin.h)
