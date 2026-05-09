# newdb Rust + Vue GUI

This folder now uses a Tauri (Rust) + Vue desktop architecture.

## Design

- Keep the original Qt GUI (`newdb_gui`) unchanged.
- Rust backend provides desktop commands (`src-tauri/src/lib.rs`).
- Vue frontend provides modern multi-panel UI (`src/App.vue`).
- Command execution delegates to existing `newdb_demo` so legacy command capability is preserved.
- Dedicated benchmark executable `newdb_perf` is used for million-scale performance test runs.

## Features migrated

- Table list panel and table switching.
- Data page query (page / page size / order / desc).
- Command console with immediate execution.
- MDB script tab (multi-line command execution).
- Transaction buttons: `BEGIN`, `COMMIT`, `ROLLBACK`.
- Write-path tuning commands: `WALSYNC`, `AUTOVACUUM`, `SHOW TUNING`.
- Tools menu quick actions: `SHOW TUNING`, `WALSYNC normal 20`, `AUTOVACUUM on 300/off`.
- Tools menu performance entry: `百万级性能压测(可执行)...` (invokes `newdb_perf`).
- Million-scale benchmark recommendation: run `100k` tier first, then extend to `500k/1000k` after confirming elapsed time.
- High-throughput ingestion commands:
  - `BULKINSERT(start_id,count[,dept])` (safe default batch mode)
  - `BULKINSERTFAST(start_id,count[,dept])` (faster when IDs are guaranteed fresh/non-duplicate)
- Logs panel.
- Runtime dashboard panel (reads `scripts/results/runtime_trend_dashboard.json` and shows health tier / key perf metrics / reasons).

## Run

```bash
cd newdb/rust_gui
npm install
npm run tauri:dev
```

## Build

**推荐（plugin C API，与 `tauri.conf.json` 的 `bundle.resources` 一致）**：先编出 `newdb_cli_backend` 与 `libnewdb`，再同步，再打包。

```bash
cd newdb/rust_gui
npm run tauri:build:plugin
```

上述脚本在 Windows 上默认使用 Visual Studio 2022 生成器，二进制目录为 `../build_tauri_plugin_vs/RelWithDebInfo`。若你已在本地用 Ninja 完成 plugin 构建，可只同步并打包：

```powershell
powershell -ExecutionPolicy Bypass -File ./scripts/build_tauri_plugin_bundle.ps1 -SkipCMake -SyncBuildDir ../build/plugin-shared
```

**手动顺序**（任意满足 plugin 产物的 CMake 目录均可作为 `-BuildDir`）：

```bash
cd newdb/rust_gui
pwsh ./scripts/sync_runtime_binaries.ps1 -BuildDir ../build-mingw   # 或 MSVC 的 .../RelWithDebInfo
npm run tauri:build
```

注意：`bundle.resources` 要求存在 `bin/newdb_cli_backend.dll`；若构建树中未编 plugin backend，`tauri build` 会失败，请改用 `cmake --preset plugin-shared-rel` 或上面的 `tauri:build:plugin`。

## Backend command bridge

Rust backend calls `newdb_demo` with:

- `--exec-line <CMD>` for single command execution
- `--page <NO> <SIZE> --order <KEY> [--desc]` for data page fetch

Rust backend calls `newdb_perf` with:

- `--demo-exe <path>`
- `--data-dir <path>`
- `--sizes <csv>` / `--query-loops <n>` / `--txn-per-mode <n>` / `--build-chunk-size <n>`

Make sure `newdb_demo` is available under one of:

- `../build_shared/newdb_demo(.exe)`
- `../build/newdb_demo(.exe)`
- `../build_all/newdb_demo(.exe)`

And `newdb_perf` is available under one of:

- `../build_shared/newdb_perf(.exe)`
- `../build/newdb_perf(.exe)`
- `../build_all/newdb_perf(.exe)`

For bundle resources, sync runtime binaries into `src-tauri/bin` first:

```bash
pwsh ./scripts/sync_runtime_binaries.ps1 -BuildDir ../build-mingw
```

This syncs:

- `newdb_demo.exe`
- `newdb_perf.exe`
- `newdb_runtime_report.exe`
- `libnewdb.dll`（若构建产出为 `newdb.dll` 则复制并改名为 `libnewdb.dll`）
- `newdb_cli_backend.dll`（在启用 `NEWDB_BUILD_CLI_BACKEND_PLUGIN` 的构建中存在；Tauri 打包必需）
- `libgtest_capi.dll`（MSVC 下可能为 `gtest_capi.dll`，脚本会统一为 `libgtest_capi.dll`）；若 GoogleTest 为共享库，会顺带复制同目录下的 `gtest`/`gmock` 等同伴 DLL
- 产物查找顺序：构建根目录、`bin/`、`Release/`、`RelWithDebInfo/`、`Debug/`、`MinSizeRel/`（兼容 MinGW 单配置与 VS 多配置）
- GUI nightly/bench 所需脚本到 `src-tauri/resources/scripts/`（`soak/bench/validate/ci` 子集）
- 结果目录与种子文件到 `src-tauri/resources/scripts/results/`：
  - `runtime_trend_dashboard.json`
  - `test_loop_trend.jsonl`
  - `nightly_soak_trend.jsonl`
