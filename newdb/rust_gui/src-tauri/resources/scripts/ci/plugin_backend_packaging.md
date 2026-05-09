# Plugin backend packaging notes (Track P)

When shipping **`NEWDB_C_API_PLUGIN_BACKEND=ON`** + **`NEWDB_BUILD_CLI_BACKEND_PLUGIN=ON`**, the host process needs **two** shared libraries from the same build tree:

| Role | Windows (typ.) | Linux (typ.) |
|------|----------------|--------------|
| Engine + C API loader | `newdb.dll` / `libnewdb.dll` | `libnewdb.so` |
| CLI / dispatch backend | `newdb_cli_backend.dll` | `libnewdb_cli_backend.so` |

Set **`NEWDB_CLI_BACKEND_PATH`** to the **absolute** path of the backend module **before** [`newdb_session_create`](../../engine/include/newdb/c_api.h). See [C_API_PLUGIN_BACKEND.md](../../docs/dev/C_API_PLUGIN_BACKEND.md).

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

- Preset **`plugin-shared`** in [CMakePresets.json](../../CMakePresets.json); smoke tests **`CliBackendPluginSmoke`** in [.github/workflows/newdb-ci-reusable.yml](../../../.github/workflows/newdb-ci-reusable.yml).
- GUI 打包工作流：[`.github/workflows/newdb-gui.yml`](../../.github/workflows/newdb-gui.yml)（Windows 上 MSVC + 上述顺序）。
