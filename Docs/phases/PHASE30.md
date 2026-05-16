# 第三十期：`rust_gui` 绑定 StructDB C API（`structdb_capi_shared`）

## 目标

将 [`gui/rust_gui/`](../gui/rust_gui/) 从 **newdb** 动态库与会话模型（`libnewdb`、`newdb_demo` / `newdb_perf`、`newdb_cli_backend`、MinGW 运行时包等）迁移为 **StructDB**：

- 进程内 **`structdb_capi_shared`** + **`structdb_mdb_execute_line_ex`**（单 engine、单 embed、MDB 会话；见 [`PHASE28.md`](PHASE28.md) 与 [`c_api/include/structdb_capi.h`](../src/c_api/include/structdb_capi.h)）。
- 可选子进程：**`structdb_app`**（`--data-dir` / `--session-dir` / `--repl` / `--run-mdb`）、**`structdb_bench`**（压测入口，参数与历史 `newdb_perf` 不等价）。
- **不再** 依赖：`newdb_session_where_plan_json*`、`newdb_session_runtime_stats`、`newdb_runtime_report`、`newdb_cli_backend` 插件模型。

## 能力矩阵（相对旧 GUI）

| 能力 | StructDB 路径 / 说明 |
|------|----------------------|
| 命令 / 脚本执行 | Tauri → `execute_via_cap_session` → C API |
| CLI 独立窗口 | `structdb_app --repl` |
| 压测菜单 | `structdb_bench`（与仓库 [`benchmarks/`](../benchmarks/) 对齐；非 1:1 替代 `newdb_perf`） |
| WHERE JSON / runtime_stats JSON | **无** C API；`c_api_runtime_stats` 等 Tauri 命令返回明确错误；前端 `commandPolicy` 中 newdb 专用分组已清空 |
| 执行计划 | 以 **MDB 文本**（`SHOW PLAN` / `EXPLAIN WHERE`，见 [`PHASE25.md`](PHASE25.md)）为准，不假定 newdb JSON |
| 打包资源 | `tauri.conf.json` 最小 **`resources/scripts`**；**`structdb_capi_shared.dll`** 等由 [`gui/rust_gui/scripts/sync_runtime_binaries.ps1`](../gui/rust_gui/scripts/sync_runtime_binaries.ps1) 同步到 `src-tauri/bin/`（开发/发布前执行） |

## 默认路径与本地状态

- 引擎数据目录默认约定见 **`PHASE28` / `PHASE29`** 与根 [`README.md`](../README.md)（**`_data`**、`embed_session` 等）；与 `structdb_app` 一致。
- 撤销/重做栈持久化路径由 **`structdb.gui.undo_redo_stack.v4`** 与目录 **`.structdb_gui`** 下文件承载（旧 `.newdb_gui` 已弃用；新写入使用新路径）。

## 构建提要

1. 仓库根 CMake：**`-DSTRUCTDB_BUILD_CAPI_SHARED=ON`**，编出 **`structdb_capi_shared`**、**`structdb_app`**、**`structdb_bench`**。
2. `powershell -File gui/rust_gui/scripts/sync_runtime_binaries.ps1 -BuildDir <CMake 构建根，如 E:\db\StructDB\build> -Configuration RelWithDebInfo`。
3. `cd gui/rust_gui`（Windows：`cd gui\rust_gui`）后 `npm install && npm run tauri:dev` 或 `npm run build` / `npx tauri build`（完整安装器流水线可用 `npm run tauri:build:plugin` 调用 `build_tauri_plugin_bundle.ps1`）。**勿**使用仓库根下不存在的 `rust_gui/` 目录名。

## 验收建议

- `cargo check`（`gui/rust_gui/src-tauri`）与 `npm test`（Vitest）通过。
- 在选定 `data_dir` 下执行典型 MDB：`USE` / `COUNT` / `BEGIN`–`COMMIT`。
- 环境变量 **`STRUCTDB_GUI_EXPECT_DLL=1`** 时 `--self-check` 应能加载到 `structdb_capi_shared`（用于 CI）。

## 风险

- **CRT / 依赖**：仅使用 MSVC 产出的 `structdb_capi_shared`；勿再捆绑 MinGW 的 `libgcc_s` / `libstdc++`。
- **并发**：遵守头文件 **非线程安全** 约定；多窗口调用需自行串行化。
