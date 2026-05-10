# Plugin backend packaging notes (Track P)

**Official product layout:** ship the **host + backend pair** from one configure/build (not full static embed). CI uploads a **shared_bundle** artifact on plugin matrix jobs; locally run `cmake --build <build-dir> --target shared_bundle` to populate `shared_bundle/<Config>/` with both libraries, `README_PLUGIN.txt`, `INSTALL_PLUGIN.md`, and (Windows) `set_newdb_plugin_env.cmd`.

When shipping **`NEWDB_C_API_PLUGIN_BACKEND=ON`** + **`NEWDB_BUILD_CLI_BACKEND_PLUGIN=ON`**, the host process needs **two** shared libraries from the same build tree:

| Role | Linux | Windows MSVC | Windows MinGW |
|------|-------|--------------|----------------|
| Engine + C API loader | `libnewdb.so` | `newdb.dll` | `libnewdb.dll` |
| CLI / dispatch backend | `libnewdb_cli_backend.so` | `newdb_cli_backend.dll` | `libnewdb_cli_backend.dll` |

`README_PLUGIN.txt` / `set_newdb_plugin_env.cmd` inside **`shared_bundle/<Config>/`** use **`$<TARGET_FILE_NAME:...>`**, so the generated hints always match the current generator/toolchain.

Set **`NEWDB_CLI_BACKEND_PATH`** to the **absolute** path of the backend module **before** [`newdb_session_create`](../../engine/include/newdb/c_api.h). See [C_API_PLUGIN_BACKEND.md](../../docs/dev/C_API_PLUGIN_BACKEND.md).

## Version / build alignment (required)

Ship the **host** shared library (`libnewdb` / `newdb.dll`) and **`newdb_cli_backend`** from the **same** CMake configure tree and compatible options:

| Requirement | Why |
|-------------|-----|
| Same `NEWDB_C_API_PLUGIN_BACKEND=ON` + `NEWDB_BUILD_CLI_BACKEND_PLUGIN=ON` build | Export surface and `dlsym`/`GetProcAddress` names must match. |
| Same compiler family / C++ ABI as the build that produced both libs | Avoid MSVC vs MinGW / libc++ mismatches. |
| On Windows, same **CRT** linkage policy as the app (`/MD` vs `/MT`) | Mixed CRT is a common load-time failure mode. |

C ABI level is gated by [`check_c_api_abi.py`](../validate/check_c_api_abi.py) in CI; **plugin pairs** should be rebuilt together whenever that ABI revision bumps.

**Smoke:** configure with preset **`plugin-shared`** or **`plugin-shared-release`** ([CMakePresets.json](../../CMakePresets.json)), build `newdb_shared` + `newdb_cli_backend`, set `NEWDB_CLI_BACKEND_PATH`, run `ctest -R CliBackendPluginSmoke` (see [.github/workflows/newdb-ci-reusable.yml](../../../.github/workflows/newdb-ci-reusable.yml)).

## Suggested install layout

Place both libraries in one directory (e.g. `lib/newdb/` next to your app) and point the env var at the backend file only; keep the main library discoverable via `PATH` / `LD_LIBRARY_PATH` / rpath as you already do for `newdb`.

## Copy-paste examples

**PowerShell (session-local, for tests):**

```powershell
$bin = "C:\path\to\cmake-build\RelWithDebInfo"
$env:NEWDB_CLI_BACKEND_PATH = Join-Path $bin "newdb_cli_backend.dll"
# Ensure newdb.dll directory is on PATH or cwd when loading the main DLL
```

**Bash:**

```bash
export NEWDB_CLI_BACKEND_PATH=/abs/path/to/build/libnewdb_cli_backend.so
export LD_LIBRARY_PATH=/abs/path/to/build:${LD_LIBRARY_PATH}
```

## Rust GUI（`rust_gui`）

- **`scripts/sync_runtime_binaries.ps1`**：若 CMake 产物目录中存在 **`newdb_cli_backend.dll`**（或 MinGW 的 **`libnewdb_cli_backend.dll`**），会复制到 **`src-tauri/bin/newdb_cli_backend.dll`**。GUI 在加载 **`libnewdb.dll`** 前若 **`NEWDB_CLI_BACKEND_PATH`** 未设置，会**自动**将其设为该文件的绝对路径（`runtime_artifact_info` / `load_dll_api`）。
- **Tauri `bundle.resources`**：`rust_gui/src-tauri/tauri.conf.json` 已列入 **`bin/newdb_cli_backend.dll`**。发行与 **`newdb/.github/workflows/newdb-gui.yml`** 采用顺序：**plugin CMake 构建 → `sync_runtime_binaries.ps1` → `npm run tauri:build`**。本地一键：**`npm run tauri:build:plugin`**（[`rust_gui/scripts/build_tauri_plugin_bundle.ps1`](../../rust_gui/scripts/build_tauri_plugin_bundle.ps1)）。

## CI reference

- Presets **`plugin-shared`** / **`plugin-shared-release`** in [CMakePresets.json](../../CMakePresets.json); smoke tests **`CliBackendPluginSmoke`** in [.github/workflows/newdb-ci-reusable.yml](../../../.github/workflows/newdb-ci-reusable.yml).
- **Artifacts:** `linux-c-api-plugin-backend-smoke` / `windows-c-api-plugin-backend-smoke` upload **`shared_bundle/Release`** as `newdb-plugin-shared-bundle-linux` / `newdb-plugin-shared-bundle-windows` (host + backend + `README_PLUGIN.txt` + `INSTALL_PLUGIN.md` + Windows `set_newdb_plugin_env.cmd`).
- **Manual / tag release bundle:** workflow [`.github/workflows/newdb-plugin-release.yml`](../../../.github/workflows/newdb-plugin-release.yml) (`workflow_dispatch` or tag `newdb-plugin-*`) runs ABI gate + Linux `shared_bundle` and uploads `newdb-plugin-shared-bundle-linux-<ref>-<run_id>`. Bump `check_c_api_abi.py --expected-abi` in that workflow when the ABI revision changes.
- GUI 打包工作流：[`.github/workflows/newdb-gui.yml`](../../../.github/workflows/newdb-gui.yml)（Windows 上 MSVC + 上述顺序）。
