# database

[English](README.en.md) | 中文

教学与工程实践为导向的数据库实验仓库。下文是 [`newdb/docs/architecture/PROJECT_DATAFLOW_WHOLE.md`](newdb/docs/architecture/PROJECT_DATAFLOW_WHOLE.md) 的**截断版**：只保留**整体项目结构**与**整体数据流**；模块子表、字段释义、CI 观测细节等见原文。**CMake**：CLI 拆为 **`newdb_shell_*` OBJECT**，由 **`newdb_capi_adapter`** 聚合并链入默认 **`newdb_shared`**；**`newdb_demo`** / 集成测试经 **`newdb_shell`** 带上 REPL 闭包；装配图与 **plugin / slim** 见 [`MODULE_BOUNDARIES.md`](newdb/docs/architecture/MODULE_BOUNDARIES.md) 与 [`BUILD.md`](newdb/docs/dev/BUILD.md)。

## 仓库顶层结构

```
database/                    # 仓库根
├── waterfall/               # 页式存储与通用基础库（被 newdb_core 链接）
├── newdb/                   # 主工程：引擎 + CLI + 工具 + 测试 + Rust GUI + 脚本 + 文档
│   ├── engine/              # 存储引擎（C++）：堆表、WAL、MVCC、C ABI、缓存
│   ├── cli/                 # 交互式命令层（C++）：shell、dispatch、业务模块
│   ├── tools/               # 可执行工具：perf、smoke、runtime_report
│   ├── tests/               # GoogleTest 回归与 gtest_capi 桥接源码
│   ├── rust_gui/            # Tauri + Vue 桌面 GUI
│   ├── scripts/             # CI、压测、校验、soak（Python/PowerShell）
│   ├── docs/                # 设计与运维文档
│   ├── intro/               # LaTeX 源码解析工程 → PDF
│   └── CMakeLists.txt       # 构建编排
├── gtest_capi/              # 可选：gtest C API 示例/打包
├── docs/                    # 仓库级讲义（与 newdb/docs 互补）
├── rules/                   # Makefile 片段（非 CMake 路径）
├── Makefile                 # 顶层 make 入口
└── README.md / README.en.md
```

**编译依赖（宏观）**：`waterfall` ← `newdb/engine` ← newdb 可执行与库；`cli` 仅通过 `engine/include/newdb/*` 公共头访问引擎。

## 整体数据流：编译与链接

```mermaid
flowchart TB
  subgraph repo["仓库根 database"]
    WF[waterfall]
  end

  subgraph newdb["newdb/"]
    ENG[newdb_core engine]
    CAP[newdb_capi_adapter]
    SH[newdb_shell]
    DEM[newdb_demo]
    DLL[newdb_shared libnewdb]
    TOOLS[newdb_perf / smoke / runtime_report]
    TESTS[newdb_tests]
    GCAPI[gtest_capi DLL]
  end

  subgraph gui["rust_gui"]
    TAURI[Tauri + Vue]
    BIN[src-tauri/bin 同步产物]
  end

  WF --> ENG
  ENG --> CAP
  CAP --> DLL
  ENG --> SH
  SH --> DEM
  ENG --> TOOLS
  SH --> TESTS
  ENG --> GCAPI
  DLL --> TAURI
  BIN --> TAURI
  DEM -.子进程/可选.-> TAURI
```

（默认 **full embed**：`DLL` = `newdb_core` + `newdb_capi_adapter`；**plugin** 时主 DLL 仅引擎核，另需 `newdb_cli_backend` 与 **`NEWDB_CLI_BACKEND_PATH`**，见上文 Track P 与 [`C_API_PLUGIN_BACKEND.md`](newdb/docs/dev/C_API_PLUGIN_BACKEND.md)。）

### `libnewdb` 三种形态（对照）

与 [`PROJECT_DATAFLOW_WHOLE.md`](newdb/docs/architecture/PROJECT_DATAFLOW_WHOLE.md) **§3.1** 一致（截断版速查）。

```mermaid
flowchart TB
  subgraph defEmbed [默认_full_embed]
    E0[newdb_core]
    C0[newdb_capi_adapter]
    Sh0[newdb_shared]
    E0 --> C0
    C0 --> Sh0
  end
  subgraph slimMode [NEWDB_SHARED_SLIM]
    E1[newdb_core]
    Slim[c_api_slim]
    Sh1[newdb_shared]
    E1 --> Slim
    Slim --> Sh1
  end
  subgraph plugMode [Plugin_backend]
    E2[newdb_core]
    Sh2[newdb_shared]
    BE[newdb_cli_backend]
    E2 --> Sh2
    Sh2 -->|runtime_dlopen| BE
  end
```

## 整体数据流：交互式命令（GUI / demo / C API）

同一条逻辑命令可走 **进程内 DLL**（full_embed 或 **plugin**）或 **子进程 demo**；形态均为「命令文本 → 输出缓冲 / 日志」。

```mermaid
sequenceDiagram
  participant U as 用户
  participant G as rust_gui
  participant D as newdb_demo.exe
  participant L as libnewdb.dll C API
  participant S as engine Session API
  participant H as HeapTable + Page IO
  participant W as WalManager

  U->>G: 输入命令 / 菜单
  alt 进程内_full_embed
    G->>L: session_execute(line, buf)
    L->>S: 静态链入 newdb_capi_adapter 再入 Session
  else 进程内_plugin
    G->>L: session_execute(line, buf)
    L->>S: dlopen newdb_cli_backend 后同 Session 边界
  else 子进程_demo
    G->>D: --exec-line / stdin 管道
    D->>S: newdb_shell dispatch 到引擎
  end
  S->>H: 读改堆页 / 迭代
  S->>W: 写路径追加 WAL、同步策略
  H-->>S: 行集 / 状态
  W-->>S: LSN / 恢复点
  S-->>L: 结果文本 / 错误码
  L-->>G: 缓冲输出
  D-->>G: stdout/stderr
  G-->>U: 表格 / 日志面板
```

## 整体数据流：持久化与恢复（磁盘）

**说明（默认）**：CLI 写路径随 **`newdb_capi_adapter`** 与主 DLL 静态链接；**plugin** 下同一逻辑在 **`newdb_cli_backend`** 中，与引擎 WAL/堆关系不变。

```mermaid
flowchart LR
  subgraph workspace["工作区目录 workspace"]
    BIN["*.bin 堆表数据"]
    ATTR["*.attr 等"]
    WAL["demodb.wal"]
    LSN["demodb.wal_lsn / walsync.conf"]
    STATS["*.tablestats 可选统计"]
    EQ["*.eqbloom 等 sidecar"]
  end

  CLI[CLI handlers txn/wal] --> WAL
  CLI --> BIN
  ENG[engine WalManager codec] --> WAL
  ENG --> BIN
  REC[recovery wal_manager_recover_support] --> WAL
  REC --> BIN
  WHERE[where executor + table_stats] -.可选写入.-> STATS
  SIDE[sidecar writers] --> EQ
```

**读路径（宏观）**：打开表 → `HeapTable` + 可选 page_cache → MVCC 快照过滤 → WHERE/sidecar 加速。

**写路径（宏观）**：命令经事务协调器 → `WalManager` 落盘 → 堆与 sidecar 更新。

## C API plugin 装配（Track P）

在 **plugin** 预设下，进程内典型为 **两份二进制**：主 **`newdb` / `libnewdb`（`newdb_shared`）** 仅链引擎核，会话侧能力由单独的 **`newdb_cli_backend`** 共享库在运行时加载。运行或打包测试前请设置环境变量 **`NEWDB_CLI_BACKEND_PATH`** 指向该 backend DLL/so 的路径（与 `CMakePresets.json` 中的 **`plugin-shared`** 一致）。说明与故障排查见 [`newdb/docs/dev/C_API_PLUGIN_BACKEND.md`](newdb/docs/dev/C_API_PLUGIN_BACKEND.md) 与 [`newdb/docs/dev/BUILD.md`](newdb/docs/dev/BUILD.md)。

**Track Q（WHERE / 查询下沉）**：仍为独立里程碑，不阻塞日常 shell 重构；路线图分叉见 [`newdb/docs/architecture/MODULE_BOUNDARIES.md`](newdb/docs/architecture/MODULE_BOUNDARIES.md) 中 **WHERE planner** 小节。

## 快速入口与文档

- 源码：`newdb/` · 图形界面：`newdb/rust_gui/`
- 深度文档（PDF）：`newdb/intro/out/newdb-intro.pdf`
- 开发者手册：`docs/dev-guide.md`
- 模块边界：`newdb/docs/architecture/MODULE_BOUNDARIES.md`
- 构建与测试：`newdb/docs/dev/BUILD.md`

## 仓库地址

- GitHub: [skyline019/database](https://github.com/skyline019/database)
