# Changelog

本仓库变更记录；发布时请复制对应版本小节到 GitHub **Release** 描述。

**正式发布描述（产品边界、制品、工作区文件、环境、CI 含义、变更摘要）**见 [`newdb/docs/dev/RELEASE_PUBLISHING.md`](newdb/docs/dev/RELEASE_PUBLISHING.md)。

格式参考 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)（若项目未钉死版本，可暂用日期或 `0.x`）。

---

## [未发布] - Unreleased

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
