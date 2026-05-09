# database / newdb 正式发布描述

**代码库**：[skyline019/database](https://github.com/skyline019/database)  
**工程根目录**：`newdb/`（`CMakeLists.txt` 中 `project(newdb)`）  
**桌面 GUI 包版本**（`newdb/rust_gui/src-tauri/Cargo.toml`）：**0.1.0**  
**引擎与 CLI**：未单独钉 SemVer；对外以 **Git 标签**（如 `v0.1.0`）标识源码快照。

---

## 1. 发布单元（本仓库交付的软件边界）

| 路径 | 内容 |
|------|------|
| `waterfall/` | 页式存储与通用基础库；由 `newdb_core` 链接 |
| `newdb/engine/` | C++ 存储引擎：堆表、WAL、MVCC、C ABI、页缓存、LSM-lite 协作等 |
| `newdb/cli/` | C++ 交互式命令层：`newdb_demo` 入口、shell、dispatch、事务/WHERE/sidecar 等模块 |
| `newdb/tools/` | `newdb_perf`、`newdb_smoke`、`newdb_runtime_report` 等工具目标 |
| `newdb/tests/` | GoogleTest 与 C API 桥接测试源码 |
| `newdb/rust_gui/` | Tauri 2 + Vue 桌面应用；经 `libnewdb`（`newdb_shared`）与/或 `newdb_demo` 子进程驱动引擎 |
| `newdb/scripts/` | CI、bench、validate、soak（Python / PowerShell） |
| `newdb/docs/` | 设计、CI、存储、事务、架构（含 `architecture/PROJECT_DATAFLOW_WHOLE.md`） |
| `newdb/intro/` | LaTeX 介绍工程，产出 PDF（路径见该目录说明） |
| `gtest_capi/` | 可选 gtest C API 示例子树 |

根目录 `readme.md` / `README.en.md`：仓库拓扑与**整体数据流**（Mermaid）；细节以 `newdb/docs/architecture/PROJECT_DATAFLOW_WHOLE.md` 为准。

---

## 2. 构建产物名称（由 CMake 生成，平台后缀略）

| 目标类型 | Windows 典型文件名 | 说明 |
|----------|-------------------|------|
| 交互演示/CLI | `newdb_demo.exe` | 主命令行入口 |
| 共享库（C ABI） | `newdb.dll`（安装/同步时常改名为 `libnewdb.dll` 供 Tauri 引用） | `OUTPUT_NAME` 为 `newdb` |
| 测试可执行文件 | `newdb_tests.exe` | GoogleTest 全集 |
| 工具 | `newdb_perf.exe`、`newdb_smoke.exe`、`newdb_runtime_report.exe` | 压测、冒烟、运行时统计报告 |
| GoogleTest C API（若启用） | `gtest_capi.dll` 等 | 与 `libgtest_capi.dll` 命名对齐见 `sync_runtime_binaries.ps1` |

**MSVC 多配置**：可执行文件与 DLL 位于 `<build-dir>/<Configuration>/`（如 `RelWithDebInfo/newdb_demo.exe`），而非仅 `<build-dir>/`。

**GUI 安装包**：由 `newdb/rust_gui` 执行 `npm run tauri:build` 生成；捆绑资源见 `rust_gui/src-tauri/tauri.conf.json` 的 `bundle.resources`（含 `bin/newdb_demo.exe`、`bin/libnewdb.dll`、`bin/newdb_cli_backend.dll`、`resources/scripts` 等）。发布前需以 **plugin C API** 配置构建引擎（`NEWDB_C_API_PLUGIN_BACKEND=ON`、`NEWDB_BUILD_CLI_BACKEND_PLUGIN=ON`），将产物同步至 `src-tauri/bin`（`rust_gui/scripts/sync_runtime_binaries.ps1`），再打包；推荐顺序见 **`npm run tauri:build:plugin`**（`rust_gui/scripts/build_tauri_plugin_bundle.ps1`）与 **`newdb/.github/workflows/newdb-gui.yml`**。

---

## 3. 工作区与用户数据文件（运行时落盘）

典型工作区目录中可出现：

| 模式 | 含义 |
|------|------|
| `*.bin` | 堆表数据文件 |
| `*.attr` 等 | 属性/辅助文件 |
| `demodb.wal` | WAL 日志 |
| `demodb.wal_lsn`、`demodb.walsync.conf`（或 `walsync.conf`） | LSN / 同步相关元数据 |
| `*.tablestats` | 表统计（可选） |
| `*.eqbloom`、`*.eqidx` 等 | 等值索引等 sidecar |

读路径经 `HeapTable`、可选 `page_cache`、MVCC 与 WHERE/sidecar；写路径经事务协调与 `WalManager` 持久化。恢复语义与 WAL 版本细节见 `newdb/docs` 下存储与事务文档。

---

## 4. 运行与构建环境（事实约束）

- **Windows**：Visual Studio 2022（`windows-clean-room` CI 使用 `Visual Studio 17 2022` + `RelWithDebInfo`）；或按 `newdb/docs/dev/BUILD.md` 使用 MinGW / 静态 CRT 等选项。
- **Linux**：GCC/Clang + Ninja（或工作流中的矩阵）；依赖与命令见 `.github/workflows/newdb-ci-reusable.yml` 与 `BUILD.md`。
- **Rust GUI**：Rust stable、Node 20（与 `newdb-gui` 等工作流一致）；具体见 `newdb/rust_gui/README.md`。

---

## 5. 自动化验证覆盖（当前 CI 含义）

- **Windows**：`newdb/scripts/ci/verify_clean_reconfigure.ps1`（干净目录 CMake 配置、构建、全量 `ctest`、语义 GTest 子集、Rust GUI 门、`ci_bench_gate.py`）；**不包含** GitHub 上的 `hybrid_e2e_gate.ps1`（pressure / runtime summary 类步骤已移除）。
- **Linux**：多 job 构建与测试、bench / runtime 契约等（见同一 workflow 文件）。

---

## 6. 与上一对外基线相比的变更摘要（滚动同步 CHANGELOG）

以下内容与仓库根 `CHANGELOG.md` 中「未发布」及近期已合并条目一致，用于发布说明正文：

- **文档**：根 README 改为架构数据流总览的截断版；`PROJECT_DATAFLOW_WHOLE` 等持续扩充。
- **CI（Windows）**：`verify_clean_reconfigure` 在 MSVC 多配置下正确解析 `newdb_tests.exe`；GUI 门前同步 `src-tauri/bin`；`ci_bench_gate.py` 在 monorepo 下解析 `newdb/build_*`；`hybrid_e2e_gate.ps1` 内测试可执行路径兼容配置子目录；GitHub `windows-clean-room` 不再调用 `hybrid_e2e_gate`。
- **引擎 / CLI / GUI**：WAL 恢复与统计、存储健康、调度与 GUI（视图菜单、主题、undo/查询等）按当前 `main` 提交为准。
- **测试**：`DemoTxnWal` 等与 hybrid dwell 相关用例在 Windows CI 上稳定化；校验脚本路径与 runtime_stats 默认键与引擎对齐。

（发布某一 **Git 标签** 时，将本节替换为该标签至上一标签之间的 `CHANGELOG.md` 小节全文即可。）

---

## 7. 获取源码快照

```text
git clone https://github.com/skyline019/database.git
cd database
git checkout <标签名>
```

CMake 配置与测试命令：`newdb/docs/dev/BUILD.md`。  
版本间差异列表：`CHANGELOG.md`。
