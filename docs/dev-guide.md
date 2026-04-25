# database 开发者手册

> 本文档由根目录 README（工程版）迁移而来，面向参与开发与维护的同学。

## 项目组成

本仓库是一个数据库实验工程，包含两层核心内容：

- `waterfall/`：底层存储与通用基础组件
- `newdb/`：基于 heap-page/WAL/MVCC 的数据库引擎、CLI、测试体系与 Rust GUI

源码解析与设计复评文档见：`newdb/intro/out/newdb-intro.pdf`。

## 仓库结构

顶层主要目录如下：

- `.github/workflows/`：CI 工作流
- `docs/`：项目文档与设计说明
- `newdb/`：主工程（源码、测试、工具、GUI）
- `resources/`：资源文件
- `rules/`：工程规则
- `waterfall/`：底层模块

`newdb/` 关键子目录：

- `src/`：核心引擎（heap/page/io/WAL/MVCC/session）
- `include/`：公共头文件与契约接口
- `demo/`：CLI 命令层、中层业务编排与 sidecar 索引逻辑
- `tests/`：单元测试与集成测试
- `tools/`：perf/smoke/runtime report 工具
- `rust_gui/`：Tauri + Vue 图形界面
- `intro/`：LaTeX 技术解析文档工程

## 构建与运行

### 1) newdb（CMake）

建议使用 out-of-source build：

```bash
cd newdb
cmake -S . -B build_mingw
cmake --build build_mingw -j
```

常用目标：

- `newdb_demo`：CLI 主程序
- `newdb_perf`：性能工具
- `newdb_runtime_report`：运行时报告工具
- `newdb_tests`：测试程序

运行示例：

```bash
cd newdb/build_mingw
./newdb_demo
```

### 2) Rust GUI（Tauri + Vue）

前端构建：

```bash
cd newdb/rust_gui
npm install
npm run build
```

同步后端二进制与脚本资源到 GUI：

```powershell
cd newdb/rust_gui
powershell -ExecutionPolicy Bypass -File .\scripts\sync_runtime_binaries.ps1 -BuildDir ..\build_mingw
```

## 测试与质量护栏

运行回归测试：

```bash
cd newdb
ctest --test-dir build_mingw --output-on-failure
```

针对分页排序修复，可单测：

```bash
ctest --test-dir build_mingw -R "PageIndexSidecar.SortByIdAscNumericOrder" --output-on-failure
```

## 文档体系

`newdb/intro/main.tex` 汇总了完整源码解析章节，建议阅读顺序：

1. `01-overview`：整体架构与数据流
2. `02-06`：heap/page/io/table/mvcc/wal 核心路径
3. `07`：session 与 C API
4. `09-13`：demo/include/tests/tools/rust_gui 中层复评
5. `08`（附录）：与 LevelDB / InnoDB 的对比

在 WSL (Ubuntu) 编译 PDF：

```bash
cd /mnt/e/db/DB/newdb/intro
./build_wsl.sh
```

生成文件：

- `newdb/intro/out/newdb-intro.pdf`

## 开发约定

- 不在源码目录直接编译，统一使用独立 build 目录
- 新增功能优先补最小回归测试（尤其排序/并发/恢复路径）
- GUI 改动涉及运行时资源时，需同步更新 `sync_runtime_binaries.ps1`
- 文档更新优先落到 `newdb/intro/`，保持代码与文档同演进

## 仓库地址

- GitHub: [skyline019/database](https://github.com/skyline019/database)
