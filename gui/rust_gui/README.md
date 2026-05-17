# StructDB Rust + Vue GUI（Tauri 2）

本目录为 **StructDB** 桌面客户端：**Vue 3** 前端 + **Tauri 2** Rust 后端。Rust 侧通过 **`libloading`** 加载 **`structdb_capi_shared`**（**`structdb_engine_open_ex`**（`flags==0` 时等价历史 `structdb_engine_open`）→ `structdb_embed_open` → `structdb_mdb_session_*` → **`structdb_mdb_execute_line_ex`**），不再使用 historical **newdb** DLL / `newdb_demo` / `newdb_cli_backend`。

## 与 CLI / 子进程的互斥（同一工作区）

- **同一 `data_dir` 只能有一个进程持有 WAL**：主窗口已通过 C API 打开 embed 时，内置终端会 **复用进程内引擎**（见 `src-tauri/src/lib.rs` 中 `cap_holds_embed_for_workspace`），避免再起 `structdb_app` 导致 **WAL 二次打开失败**。
- 若需 **独立** `structdb_app` 子进程，请使用 **不同数据目录**，或在宿主侧使用 **`structdb_engine_open_ex(..., STRUCTDB_ENGINE_OPEN_FLAG_EXCLUSIVE_DIR_LOCK)`** 等策略保证互斥；详见仓库 [Docs/phases/PHASE35.md](../../Docs/phases/PHASE35.md)。
- **三十六期（可选）**：设置环境变量 **`STRUCTDB_GUI_EXCLUSIVE_DIR_LOCK=1`**（或 **`true`**，大小写不敏感）时，GUI 以 **`STRUCTDB_ENGINE_OPEN_FLAG_EXCLUSIVE_DIR_LOCK`** 打开引擎（`data_dir/.structdb_exclusive.lock` 建议锁），第二进程指向同一目录时会 **打开失败**。**默认不设置**（关闭），便于本机多窗口调试。
- **推荐开启独占锁的场景**：生产环境 GUI 与 `structdb_app`/第二 GUI 可能共用同一 `data_dir`；**冷备份前**须停写，见 **[`Docs/BACKUP_RESTORE_RUNBOOK.md`](../../Docs/BACKUP_RESTORE_RUNBOOK.md)**。

## 先决条件

- **Node.js** + **npm**
- **Rust** stable（Windows 目标 **`x86_64-pc-windows-msvc`**）
- 已用 CMake 编出共享库与应用（示例）：

```powershell
cd E:\db\StructDB
cmake -B build -DSTRUCTDB_BUILD_CAPI_SHARED=ON
cmake --build build --config RelWithDebInfo --target structdb_capi_shared structdb_app
```

若当前 shell 在 **仓库根**（例如 `E:\db\StructDB`），路径必须是 **`gui\rust_gui`**（不是 `rust_gui`）。可按下述顺序执行（与根目录 `package.json` 无关，**npm 必须在 `gui/rust_gui` 下运行**）：

```powershell
cd E:\db\StructDB
powershell -ExecutionPolicy Bypass -File .\gui\rust_gui\scripts\sync_runtime_binaries.ps1 -BuildDir .\build -Configuration RelWithDebInfo
cd .\gui\rust_gui
npm install
npm run build
npx tauri build
```

## 同步运行时二进制

将 CMake 树中的 DLL/EXE 复制到 `src-tauri/bin/`（Tauri 与 `resolve_*` 启发式会从此目录解析）：

```powershell
cd E:\db\StructDB\gui\rust_gui
powershell -ExecutionPolicy Bypass -File ./scripts/sync_runtime_binaries.ps1 -BuildDir E:\db\StructDB\build -Configuration RelWithDebInfo
```

（Visual Studio 多配置时，产物在 `build\src\c_api\RelWithDebInfo\` 等子目录；`-BuildDir` 传 **CMake 构建根** `build` 即可，脚本会按 `-Configuration` 优先挑选同名配置目录下的文件。）

若使用 Ninja 单配置目录，把 `-BuildDir` 指到你的 `build` 根即可（脚本会递归查找文件名）。

## 开发

```powershell
cd E:\db\StructDB\gui\rust_gui
npm install
npm run tauri:dev
```

## Tauri `invoke` 接口与前端约定（与 `src-tauri/src/lib.rs` 同步）

以下为 **Rust 侧注册命令**（`tauri::generate_handler!`）的**功能分组**；完整列表以源码为准。

| 分组 | 命令名 | 说明 |
|------|--------|------|
| 会话 / 表 | `get_state`、`set_workspace`、`set_current_table`、`list_tables` | 工作区、`current_table`、`page_size` 等 |
| 设置 | `get_settings`、`set_settings` | UI 主题与布局持久化 |
| MDB / 命令 | `execute_command`、`execute_command_ex`、`infer_inverse_command` | 单行 MDB；`execute_command_ex` 带扩展错误信息 |
| 事务栈 | `txn_begin`、`txn_commit`、`txn_rollback`、`txn_savepoint`、`txn_rollback_to`、`txn_release_savepoint` | 与前端事务按钮一致 |
| 撤销栈持久化 | `save_stack_units`、`load_stack_units`、`stack_undo_unit`、`stack_redo_unit` | 栈单元序列化 |
| **MDB 脚本** | **`run_script`**、**`run_script_ex`**、**`cancel_mdb_script`** | 多行脚本逐行执行；**`run_script` / `run_script_ex` 首参为 `AppHandle`**（Tauri 注入）；非空行之间 **`emit("mdb-script-progress", …)`**；`cancel_mdb_script` 置位取消标志；输出可含 **`[SCRIPT] cancelled`**；**`run_script_ex`** 返回 JSON（camelCase）：`ok`、`output`、`errorCode`、`stopLine`、**`cancelled`** |
| 分页 | `query_page` | `page_size` **1..500** 钳制 |
| 诊断 / 导出 | `export_bundle`、`backup_storage_bundle`、`dll_info`、`runtime_artifact_info` | 诊断 zip 与 C API 1.9 冷备（`structdb_backup_bundle`） |
| Wave 4 / PITR | `recover_to_checkpoint_seq`、`get_mdb_durability`、`set_mdb_durability` | checkpoint 冷恢复与 MDB 耐久档位 |
| 内置终端 | `cli_terminal_start`、`cli_terminal_write_line`、`cli_terminal_stop` | 子进程终端（与工作区互斥见上文） |

**前端事件（脚本进度）**

- 事件名：**`mdb-script-progress`**
- Payload（camelCase）：**`lineDone`**、**`totalLines`**
- 实现：`App.vue` 中 `listen` + **「停止脚本」**（`invoke("cancel_mdb_script")`）+ 进度文案。

**MDB `SCAN` 游标（REPL / 脚本与解析器一致）**

- **`SCAN`**：自起点最多 5000 行并重置游标。
- **`SCAN MORE`** / **`SCAN MORE(n)`**：续打（默认每批 500 行，`n`∈1..5000）。
- **`SCAN RESET`**：清零游标。
- **`USE(...)`** 成功后游标归零。

**Wave 4（C API 1.9 / MDB PHASE43–45）**

- **耐久**：`SET DURABILITY 0|1|2` 或菜单「工具 → MDB 耐久档位」；Rust 侧 `set_mdb_durability` 同步到 `structdb_mdb_session_set_durability`。
- **冷备**：`backup_storage_bundle` → `structdb_backup_bundle`（关闭 embed 后复制 `data_dir`，含 `backup_manifest.json`）。
- **checkpoint 恢复**：`SHOW CHECKPOINTS` / `RECOVER TO CHECKPOINT_SEQ n` 或 `recover_to_checkpoint_seq`（`structdb_recover_data_dir_to_checkpoint_seq`）。
- **索引 / 分块导入**：菜单「表 → 索引」与「文件 → IMPORT SEGMENT」；帮助面板已收录 `CREATE/DROP INDEX`、`GROUP BY`、`PAGE_JSON` 等条目。
- 版本号：`npm run sync-version-from-capi` 从 `structdb_capi.h` 同步到 `package.json`、`tauri.conf.json`（含窗口标题 `StructDB x.y.z`）、`src-tauri/Cargo.toml`（当前 **1.9.0**）。关于对话框显示 **客户端版本** 与 **C API DLL** 版本。

## 测试

前端单元测试（Vitest）与 Rust crate 测试：

```powershell
cd E:\db\StructDB\gui\rust_gui
npm test
cd src-tauri
cargo test
```

**引擎与 MDB 集成测试**在仓库根 CMake 目标中（与 GUI 解耦），例如：

- **`structdb_tests`**：`tests/CMakeLists.txt`，`ctest --test-dir build -C Release -R structdb`（按需调整配置名）。
- **`structdb_bench`**：`benchmarks/CMakeLists.txt`；可选 **`STRUCTDB_ENABLE_PERF_GATE`** 注册 `structdb_perf_gate`（见根 `CMakeLists.txt` 与 `benchmarks/README.md`）。

GUI 进程仍支持 **`--self-check`**（无窗口）用于打包后快速校验 DLL 与最小 MDB 路径（见 `src-tauri/src/main.rs`）。

## 打包

`tauri.conf.json` 的 **`bundle.resources`** 当前仅包含 **`resources/scripts`**（避免缺失的占位路径导致 `tauri build` 失败）。**发布安装包前**请先执行上面的 **sync**，确保目标机器能通过 `PATH` / 可执行文件旁路找到 **`structdb_capi_shared.dll`** 及其 MSVC 依赖。

一键脚本（可选 CMake + sync + `npm run build` + `tauri build`）：

```powershell
npm run tauri:build:plugin
# 或仅同步已有构建树：
pwsh ./scripts/build_tauri_plugin_bundle.ps1 -SkipCMake -SyncBuildDir E:\db\StructDB\build\RelWithDebInfo
```

## 文档

- [`Docs/ARCHITECTURE.md`](../Docs/ARCHITECTURE.md)：全库数据流与代码版图（含 **§2.1 GUI** 与 Tauri 命令摘要）。
- [`Docs/OPTIMIZATION_PLAN.md`](../Docs/OPTIMIZATION_PLAN.md)：性能路线图与已落地项（含 GUI 脚本进度、存储读写锁等）。
- [`Docs/OS_IO_ISOLATION.md`](../Docs/OS_IO_ISOLATION.md)：OS 级 compaction 与 WAL I/O 隔离运维清单。
- [`Docs/phases/PHASE30.md`](../Docs/phases/PHASE30.md)：迁移说明、能力矩阵、验收与风险。
- [`Docs/phases/PHASE28.md`](../Docs/phases/PHASE28.md)：`structdb_capi_shared` 与 FFI 会话语义。
