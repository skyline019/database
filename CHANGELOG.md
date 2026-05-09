# Changelog

本仓库变更记录；发布时请复制对应版本小节到 GitHub **Release** 描述。

**正式发布描述（产品边界、制品、工作区文件、环境、CI 含义、变更摘要）**见 [`newdb/docs/dev/RELEASE_PUBLISHING.md`](newdb/docs/dev/RELEASE_PUBLISHING.md)。

格式参考 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)（若项目未钉死版本，可暂用日期或 `0.x`）。

---

## [未发布] - Unreleased

### 相对 [GitHub `origin/main`](https://github.com/skyline019/database)（本批工作区）

> 统计：`git diff origin/main --shortstat` 约 **98 files / +4722 −4467**（以实际推送为准）。下列为按模块归纳，便于与远端树 diff 对照。

- **引擎 / C API**：`reorder_heap_ids_dense` 可选输出 **旧 id → 新 id** 映射；`CONFIRM_REORDER` 成功重写时由 CLI 打印 **`[REORDER_MAP_JSON]`** 供 GUI 摄入；会话句柄、C API 插件后端头文件、JSON 转义、runtime stats 快照等增量（见 `newdb/engine/`、`c_api*.cpp`）。
- **CLI / Shell**：`workspace_handler` 与重排日志衔接；事务协调器、WAL/恢复、WHERE **plan_impl** 拆分为多编译单元与头文件（`plan_impl_support`、`plan_query_index`、`plan_scan_estimate`、`where_plan_catalog` 等）；**shell_state** 分层与 facade/ops 拆分；**crc32c** 从 `engine` 迁至 **waterfall**（`crc32c_compat`），引擎内删除重复实现。
- **Rust GUI（Tauri）**：**撤销/重做** — 重做栈在新编辑下**保留**（不再因分叉清空）；持久化栈 **v4**（去掉 suspended/fork 字段；加载旧 v3 时 **legacy 暂存并入 redo**）；**id 重映射链** 在 undo/redo 执行前改写命令行；**整数 id 规范化**（`03`/`3`）、`DELETEPK`/`FINDPK`、缺表名时回退 **当前 USE**、多 `tables_touched` 回退尝试；事务内可逆操作**独立入栈**。**UI**：撤销/重做 **同区标签 + 淡出切换**；**USE 状态条**；表标签点击 **同步 USE**（含再次点击当前标签刷新）。`sync_runtime_binaries` / `build_tauri_plugin_bundle`、校验脚本镜像等。
- **前端（Vue）**：`App.vue` / `styles.css` / `commandPolicy` 等与上述行为一致；网格 id 与逆向推断 **数值等价** 匹配。
- **构建 / CI / 脚本**：根与 `newdb` 的 **GitHub Actions**、**CMakePresets**（如 `plugin-shared`）、`ci_bench_gate` / `sync_validate_scripts`、runtime stats **契约与 GUI 键** 校验脚本及文档补充。
- **测试**：`test_page_io`（重排映射）、`gtest` / C API / shell 相关用例更新；新增/调整若干 **shell_state**、**c_api_slim**、插件冒烟等测试与支撑代码。
- **文档**：`PROJECT_DATAFLOW_WHOLE`、`MODULE_BOUNDARIES`、`BUILD`、`RELEASE_PUBLISHING`、roadmap 与 **dev/** 下多篇新稿（C API 分层、shell 分层、CI 矩阵等）。

### 文档

- 根目录 `readme.md` / `README.en.md` 改为 [`newdb/docs/architecture/PROJECT_DATAFLOW_WHOLE.md`](newdb/docs/architecture/PROJECT_DATAFLOW_WHOLE.md) 的截断版：仅保留仓库顶层结构与整体数据流（Mermaid），细节仍见原文。
- `PROJECT_DATAFLOW_WHOLE` 等架构文档持续扩充（CLI 模块流、字段/形参手册等）。

### CI / 构建

- **Windows `verify_clean_reconfigure`**：语义门在 Visual Studio 多配置下正确解析 `newdb_tests.exe` 路径（`$BuildConfig` 子目录）。
- **GUI 门**：在 `cargo test` 前调用 `sync_runtime_binaries.ps1`，将 CMake 产物同步到 `rust_gui/src-tauri/bin`，满足 Tauri `bundle.resources` 对 `bin/newdb_demo.exe` 等文件的校验。
- **`ci_bench_gate.py`**：在 monorepo 布局下，相对 `build_dir` 优先解析为 `newdb/<build_dir>`；`verify_clean_reconfigure` 同时传入 **绝对** `buildPath`，避免误连到仓库根下的空目录。
- **`hybrid_e2e_gate.ps1`**：解析 `newdb_tests.exe` 时兼容 MSVC 配置子目录（与 verify 语义门一致）。
- **GitHub Actions**：`windows-clean-room` 不再执行 `hybrid_e2e_gate`（去掉 CI 上的 pressure / runtime summary 类步骤）；Ubuntu 等修复包括可复用 workflow 引用、`libcrc32c-dev` 与 workspace 根路径等。

### 引擎 / CLI / 工具

- WAL 恢复管线（redo planner/applier 等）、table stats、storage health、调度与事务/写冲突等持续迭代（详见近期提交与 `PROJECT_DATAFLOW`）。

### GUI（Rust + Vue / Tauri）

- 视图菜单、主题预设子菜单、品牌与面板布局等 UX 调整；后端查询分页与 undo 链等相关修复。

### 测试

- `DemoTxnWal` 等与 hybrid dwell 相关的用例在 Windows CI 上稳定化（时钟与环境清理）。
- 校验脚本：`check_c_api_abi` 等从 `newdb` 根解析头路径；`validate_runtime_stats` 补齐 LSM 相关默认键等。

---

<!-- 发布新版本时：将上方 [未发布] 改为具体版本号与日期，并新增一节 [未发布] 占位。示例：

## [0.2.0] - 2026-05-05

### 文档
- …

-->

## 模板：GitHub Release 描述（可直接粘贴）

将 `## [未发布]` 下各小节按需删减后粘贴：

```markdown
## 摘要
- 文档：根 README 对齐架构数据流总览（截断版）。
- CI：Windows 验证脚本与 bench gate、Tauri 资源同步、移除 GitHub 上 hybrid pressure 步骤。
- 引擎/GUI：WAL/统计/界面与测试稳定性等（见提交历史）。

## 文档
- 根目录 README：整体目录结构 + 编译/交互/持久化数据流（Mermaid），完整内容见 `newdb/docs/architecture/PROJECT_DATAFLOW_WHOLE.md`。

## CI
- `verify_clean_reconfigure`：MSVC 下 `newdb_tests` 路径；GUI 门前同步 `src-tauri/bin`；bench gate 使用绝对 build 路径 + monorepo 下 `newdb/build_*` 回退解析。
- `hybrid_e2e_gate`：MSVC 下测试可执行路径解析。
- GitHub：`windows-clean-room` 不再跑 `hybrid_e2e_gate`。

## 兼容性说明
- 本地若依赖已删除的 CI 步骤，可仍使用 `newdb/scripts/ci/hybrid_e2e_gate.ps1`（含可选 pressure）自行验收。

## 完整变更
- 见提交历史：`git log --oneline <上一标签>..HEAD`
```
