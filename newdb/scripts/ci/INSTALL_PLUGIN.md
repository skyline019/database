# newdb plugin release (host + backend)

Ship **two** libraries from the **same** CMake build:

| Role | Linux (Ninja/Make) | Windows MSVC | Windows MinGW |
|------|-------------------|--------------|-----------------|
| Host | `libnewdb.so` | `newdb.dll` (import lib `newdb.lib`) | `libnewdb.dll` |
| Backend | `libnewdb_cli_backend.so` | `newdb_cli_backend.dll` | `libnewdb_cli_backend.dll` |

CI artifacts use **`shared_bundle/Release`** (Ninja single-config = Release; VS multi-config = `-C Release`). Paths in workflows: `build-plugin/shared_bundle/Release` (Linux), `build-plugin-win/shared_bundle/Release` (Windows).

- **Host**: engine + C API loader only when `NEWDB_C_API_PLUGIN_BACKEND=ON`.
- **Backend**: full CLI / dispatch stack.

## Runtime

1. Place both files in one directory (or ensure the loader can find the host library via `PATH` / `LD_LIBRARY_PATH` / rpath).
2. Before `newdb_session_create`, set **`NEWDB_CLI_BACKEND_PATH`** to the **absolute** path of the backend module.

See [C_API_PLUGIN_BACKEND.md](../../docs/dev/C_API_PLUGIN_BACKEND.md) and [plugin_backend_packaging.md](plugin_backend_packaging.md).

## CMake install

Configure with `-DNEWDB_INSTALL_PLUGIN_RELEASE=ON` (and plugin options). Installs both targets under `lib/newdb/` (or `CMAKE_INSTALL_LIBDIR/newdb`) and copies this file next to them.

## Bundle

`cmake --build <dir> --target shared_bundle` copies the plugin pair into `shared_bundle/<Config>/` when the backend target is enabled. See [BUILD.md](../../docs/dev/BUILD.md).
