# 《全仓库模块与子模块数据流总览》逐项讲解手册

本文在 [`PROJECT_DATAFLOW_WHOLE.md`](./PROJECT_DATAFLOW_WHOLE.md) 的基础上，按**仓库根 → newdb 各目录 → 关键源码文件 → 数据结构（类型/字段语义摘要）**展开，便于从「地图」钻到「文件与类型」。**每个字段的逐项释义**仍以原文 [**§12.12**](./PROJECT_DATAFLOW_WHOLE.md#1212-字段与形参释义手册) 为权威手册（字段极多，避免双处维护漂移）。

---

## 0. 阅读顺序与分工

| 材料 | 内容 |
|------|------|
| **本文** | 模块/子模块目录职责、主要 `.cc/.h` 入口、类型清单与数据载体、handler 责任链顺序、磁盘产物；**§11 关键方法实现**（与源码逐步对照） |
| **原文** | Mermaid 图、§11 细节、§12.11 源码行号节选、**§12.12 字段/形参全字典**、§13 维护清单 |
| **CMake 积木** | `newdb_shell_*` OBJECT → `newdb_capi_adapter` → 默认 `newdb_shared`；`newdb_shell` / plugin 形态 | 见 **原文 §2.2.1**、**§3.1**（三种 `newdb_shared` 形态对照图）、[MODULE_BOUNDARIES.md](./MODULE_BOUNDARIES.md)、[BUILD.md](../dev/BUILD.md) |

路径默认以**仓库根 `database/`** 为起点，写作 `newdb/...`、`waterfall/...`。

---

## 1. 仓库根顶层模块

| 路径 | 模块职责 | 典型产物/载体 |
|------|----------|----------------|
| `waterfall/` | 页式存储与通用基础库 | 静态/动态库，被 `newdb_core` 链接 |
| `newdb/` | 主工程：引擎、CLI、工具、测试、GUI、脚本、文档、CMake 编排 | `newdb_demo`、`libnewdb`、`newdb_tests`、各 `tools/*` |
| `gtest_capi/`（根下可选子树） | 独立 CMake 的 GTest C API 示例工程 | 与 `newdb` 内 `gtest_capi` **思路同源**，**不替代**主构建目标 |
| `docs/` | 仓库级讲义/handout | 人读 Markdown/LaTeX 等 |
| `rules/` | Makefile 片段 | 非 CMake 入口的补充规则 |
| `resources/` | 仓库级资源 | 依项目而定 |
| `Makefile` | 顶层 make 入口 | 调用子目录目标 |
| `README.md` / `README.en.md` | 项目说明 | — |

**编译依赖（牢记）**：`waterfall` ← `newdb/engine`（`newdb_core`）← 可执行体与 DLL；**`newdb/cli` 只能** `#include` **`newdb/engine/include/newdb/*.h`**，不得依赖 `engine/src/**`（见 [`MODULE_BOUNDARIES.md`](./MODULE_BOUNDARIES.md)）。

---

## 2. `newdb/engine`：存储引擎（子路径 → 职责 → 载体）

与原文 §2.1 一一对应，下表每行即一个**子模块**（目录级）。

| 子路径 | 子模块职责 | 典型数据载体 |
|--------|------------|----------------|
| `newdb/engine/include/newdb/` | **对外 API 边界**：C/C++ 声明 | 类型、句柄、枚举、函数声明 |
| `newdb/engine/src/api/c/` | **C ABI**：`session_create` / `destroy` / `set_table` / `execute` 等 | 命令 UTF-8 文本 → 结果缓冲、错误码 |
| `newdb/engine/src/session/api/` | 会话级 API 实现 | `Session` 状态、打开表、错误传播 |
| `newdb/engine/src/session/table_access/` | 表加载、物化、历史 | `HeapTable`、快照与路径解析 |
| `newdb/engine/src/heap/` | 堆文件逻辑：页内行、索引手指、`rebuild_indexes` 等 | 行字节、`index_by_id`、`sorted_indices` |
| `newdb/engine/src/io/page/` | 页级读写、`merge_one_page_into_fingers` 等 | 文件偏移、页缓冲 |
| `newdb/engine/src/schema/`、`src/catalog/` | 表模式与目录元数据 | `TableSchema`、`catalog` 相关结构 |
| `newdb/engine/src/codec/` | 元组编解码 | 列值 ↔ 字节流 |
| `newdb/engine/src/wal/writer/`、`codec/`、`recovery/` | WAL 追加、记录编解码、恢复与段扫描 | `demodb.wal`、`WalDecodedRecord`、LSN |
| `newdb/engine/src/mvcc/snapshot/` | 可见性规则与快照 | `snapshot_lsn`、读路径过滤 |
| `newdb/engine/src/lsm/` | LSM-lite 引擎侧协作 | 与 CLI LSM 壳层配合的层/段提示 |
| `newdb/engine/src/cache/` | 页缓存、与内存预算/registry 协同 | `page_cache_*` 统计 |
| `newdb/engine/src/util/` | CRC 等杂项 | 校验和 |

### 2.1 公共头文件清单（CLI 合法可见的「引擎面」）

下列文件位于 `newdb/engine/include/newdb/`（及子目录 `wal/`），是 **CLI / 工具 / GUI（经 C API）** 与引擎的主要编译耦合面：

| 头文件 | 主要类型/API 角色 |
|--------|-------------------|
| `c_api.h` | DLL 导出：会话与执行 |
| `session.h` | `Session`、`HeapAccess`、`lock_heap` |
| `heap_table.h` | `HeapTable`、行访问、逻辑行数 |
| `heap_page.h` / `heap_storage.h` / `heap_file_read_view.h` | 页与存储抽象 |
| `page_io.h` | 页级 IO |
| `row.h` | `Row` |
| `schema.h` / `catalog.h` / `schema_io.h` | 模式与目录 IO |
| `tuple_codec.h` | 元组编解码 |
| `wal_manager.h` | `WalManager`、`WalOp`、`WalRecordHeader`、`WalDecodedRecord`、`WalSyncMode` |
| `wal_codec.h` | WAL 负载编解码 |
| `wal/wal_redo_planner.h` | Redo 计划构建 |
| `wal/wal_redo_applier.h` | Redo 应用到堆 |
| `wal/wal_recovery_pipeline.h` | `WalRecordReader` 等恢复管道只读边界 |
| `wal/wal_segment_scanner.h` | 段扫描 |
| `mvcc.h` | MVCC 可见性 |
| `page_cache.h` | `PageCacheGlobalStats`、`page_cache_try_get` / `put` |
| `memory_budget.h` | `MemoryBudgetSnapshot`、`memory_budget_snapshot()` |
| `memory_registry.h` | `MemoryRegistry`、`MemoryKind`（`PageCache` / `EqSidecar` / `QueryTemp`） |
| `lsm_lite.h` | LSM-lite 对外形状 |
| `error.h` / `error_format.h` | 错误码与格式化 |

---

## 3. `newdb/cli`：命令与编排层（子路径 → 职责 → 载体）

与原文 §2.2 对应。

| 子路径 | 子模块职责 | 典型数据载体 |
|--------|------------|----------------|
| `newdb/cli/app/` | `newdb_demo` 入口、`main`、参数解析 | `argv`、`--data-dir`、workspace |
| `newdb/cli/shell/bootstrap/` | 进程启动、工作区引导 | 环境变量、路径规范化 |
| `newdb/cli/shell/repl/` | 交互行读取、调用 dispatch | 单行文本 |
| `newdb/cli/shell/dispatch/router/` | **`process_command_line`**、phase-1/2 责任链 | `ShellState&`、动词路由 |
| `newdb/cli/shell/dispatch/registry/` | 命令表、注册源 | 命令名 → 分支 |
| `newdb/cli/shell/dispatch/handlers/*` | 按域拆分：**txn / ddl / dml / query / io / workspace / session** | 解析后参数、日志、表引用 |
| `newdb/cli/shell/dispatch/support/` | 参数解析、热索引等辅助 | 中间结构 |
| `newdb/cli/shell/dispatch/services/` | **跨 handler 服务**：如 `sidecar_invalidate_service`、LSM 相关 | 失效事件、队列 |
| `newdb/cli/shell/dispatch/shared/` | 共享内部声明 | `dispatch_internal.h` |
| `newdb/cli/shell/state/` | **`ShellState`**、`get_cached_table`、`HeapReadViewGuard` | 会话、堆 guard、WHERE 上下文 |
| `newdb/cli/shell/diag/` | 诊断输出 | 文本 |
| `newdb/cli/modules/common/` | 日志、表格渲染、杂项工具 | 格式化行 |
| `newdb/cli/modules/catalog/` | 模式目录 on disk | schema 文件树 |
| `newdb/cli/modules/import_export/` | `IMPORTDIR`、EXPORT CSV/JSON | 路径、流 |
| `newdb/cli/modules/where/parser/` | WHERE 条件解析 | `WhereCond`、`CondOp` |
| `newdb/cli/modules/where/executor/` | **执行核**：`where.h`、`plan_impl`、`policy_service`、`cache_impl`、**`table_stats`** | 行 slot 向量、计划 JSON、`*.tablestats` |
| `newdb/cli/modules/txn/coordinator/` | **事务协调**：锁、WAL、恢复、vacuum、写冲突、**`stats_impl`** | `TxnRuntimeStats`、`TxnRecord` |
| `newdb/cli/modules/sidecar/*` | 各类 sidecar + **`index_catalog`** | `*.eqbloom` 等、`IndexDescriptor` |
| `newdb/cli/modules/storage/` | **`TableStorageHealth`** 只读度量 | 碎片率、与 vacuum 打分衔接 |

### 3.1 关键源码入口（文件级）

| 文件 | 代码职责 |
|------|----------|
| `newdb/cli/shell/dispatch/router/dispatch.cc` | **`process_command_line`**：phase-1 数组、phase-2 数组、`get_cached_table` |
| `newdb/cli/shell/dispatch/router/dispatch_routing.cc` | **`shell_line_targets_phase2_only`**：phase-2 前缀表 |
| `newdb/cli/shell/state/shell_state.h` | **`ShellState`**、`get_cached_table`、`shell_invalidate_session_table`、`HeapReadViewGuard` |
| `newdb/cli/shell/dispatch/handlers/txn/txn_handler.cc` | `handle_txn_commands` |
| `newdb/cli/shell/dispatch/handlers/workspace/workspace_handler.cc` | 工作区、`VACUUM`、`CONFIRM_REORDER`、`RESET`、`SCAN`、`SHOWLOG` 等 |
| `newdb/cli/shell/dispatch/handlers/io/io_handler.cc` | 导入等 IO 类 |
| `newdb/cli/shell/dispatch/handlers/ddl/ddl_handler.cc` | DDL / catalog / USE / CREATE / ALTER / RENAME / SHOW SCHEMA |
| `newdb/cli/shell/dispatch/handlers/dml/dml_handler.cc` | INSERT、UPDATE、DELETE、属性列 |
| `newdb/cli/shell/dispatch/handlers/query/query_handler.cc` | WHERE、COUNT、FIND、聚合、PAGE、EXPORT 等查询侧 |
| `newdb/cli/shell/dispatch/handlers/session/session_handler.cc` | 会话子通道（若与 txn 分离实现则在此域） |

### 3.2 Phase-1 / Phase-2 责任链（函数名级）

实现顺序以 `dispatch.cc` 为准（原文 §11.2–11.3、§12.11 节选）。

**Phase-1**（`shell_line_targets_phase2_only` 为假时才整链执行；命中则 `return true` 结束）：

| 顺序 | Handler 函数（概念入口） | 主要 `cli/modules` / 服务 |
|------|--------------------------|---------------------------|
| 1 | `handle_txn_commands` | `txn/coordinator/*` |
| 2 | `handle_workspace_admin_commands` | `common`、workspace、对 `*.bin` 的维护命令 |
| 3 | `handle_import_defattr_commands` | `import_export`、attr |
| 4 | `handle_schema_catalog_commands` | `catalog` |
| 5 | `handle_ddl_create_use_commands` | `catalog`、引擎建表 |
| 6 | `handle_ddl_alter_rename_commands` | DDL 变更、路径刷新 |
| 7 | `handle_schema_show_commands` | 元数据展示 |
| 8 | `handle_dml_insert_command` | `txn` + 堆写入、WAL |

**Phase-2**（需 `HeapTable&`；`get_cached_table` 失败则整段 no-op）：

| 顺序 | Handler 函数 | 主要向下调用 |
|------|----------------|----------------|
| 1 | `handle_schema_key_command` | 目录 / 主键、`HeapTable` |
| 2 | `handle_query_where_count_commands` | **`where/*`**、sidecar、`table_stats` |
| 3 | `handle_dml_update_delete_commands` | `txn`、行写、WAL、写冲突 |
| 4 | `handle_dml_attr_commands` | 属性列、堆更新 |
| 5 | `handle_query_find_commands` | `where` 或扫描 |
| 6 | `handle_query_sum_avg_commands` | 聚合 + WHERE |
| 7 | `handle_query_min_max_commands` | MIN/MAX |
| 8 | `handle_query_page_command` | 页级扫描、`page_index_sidecar` 等 |
| 9 | `handle_export_command` | `import_export` |

**Phase-2 前缀**（匹配则跳过整个 phase-1）：`PAGE`、`WHEREP`、`WHERE`、`COUNT`、`MIN`、`MAX`、`SUM`、`AVG`、`FIND(`、`FINDPK`、`QBAL`、`UPDATE`、`DELETE(`、`DELETEPK`、`EXPORT`、`SETATTR`、`RENATTR`、`DELATTR`、`SET PRIMARY KEY`（见 `dispatch_routing.cc`）。

### 3.3 `newdb/cli/modules/txn/coordinator` 内部文件（实现子模块）

| 文件 | 职责 |
|------|------|
| `txn_manager.h` / `txn_manager.cc` | **`TxnCoordinator`** 对外 API 与状态机外壳 |
| `core/core_impl.cc` | 事务核心状态与操作记录 |
| `lock/lock_service.cc` | 锁与死锁相关 |
| `wal/wal_service.cc` | 与 `WalManager` 协同的追加、flush、模式 |
| `recovery/{recovery_analyze,redo,undo,heap_undo_apply,finalize,recovery_service}.cc` | **`recoverFromWAL`** 编排；analyze / redo / undo 链与 fallback / finalize；堆 undo 与运行时回滚共用 |
| `vacuum/vacuum_service.cc` | vacuum 队列、compact、与 `measure_table_storage_health` 衔接 |
| `write_conflict/write_conflict_service.cc` | 写冲突检测与等待 |
| `write_conflict/lock_key.h` | **`LockKey`** / **`LockKeyKind`** |
| `stats/stats_impl.cc` | **`TxnRuntimeStats`** 采样与镜像（page_cache、memory_budget、MemoryRegistry） |
| `internal/txn_internal.h` | 协调器内部共享声明 |

### 3.4 `newdb/cli/modules/sidecar` 子模块（文件级）

| 路径 | 职责 |
|------|------|
| `sidecar/common/index_catalog.{h,cc}` | **`IndexKind`**、**`IndexDescriptor`**、目录名解析、与运行时 LSN/schema 版本匹配 |
| `sidecar/common/bptree_index.{h,cc}` | B+ 树侧车共用逻辑 |
| `sidecar/common/sidecar_wal_lsn.{h,cpp}` | 侧车与 WAL LSN 对齐辅助 |
| `sidecar/eq/equality_index_sidecar.{h,cc}` | 等值索引侧车 |
| `sidecar/eq/eq_bloom.{h,cc}` | 等值 Bloom 过滤 |
| `sidecar/covering/covering_index_sidecar.{h,cc}` | 覆盖投影侧车 |
| `sidecar/page/page_index_sidecar.{h,cc}` | 页索引侧车 |
| `sidecar/visibility/visibility_checkpoint_sidecar.{h,cc}` | 可见性 checkpoint 侧车 |

写路径经 **`newdb/cli/shell/dispatch/services/sidecar/sidecar_invalidate_service.cc`**（`sidecar_invalidate_all_indexes_for_data_file` 等，原文 §11.6）向上述侧车文件发**失效**信号，避免各 handler 手写删文件。

### 3.5 `newdb/cli/modules/where` 建议阅读顺序与文件清单

| 文件 | 职责 |
|------|------|
| `where/parser/condition.{h,cc}` | `CondOp`、`parse_cond_op`、条件解析 |
| `where/executor/where.{h,cc}` | **`WhereCond`**、**`WhereQueryContext`**、**`PlanCandidate`**、**`query_with_index`** 等对外 API 与主实现入口 |
| `where/executor/plan/plan_impl.cc` | 计划：统计过期、sidecar 可用性、回落扫描 |
| `where/executor/cache/cache_impl.cc` | 查询结果缓存键与 LRU |
| `where/executor/policy/policy_service.cc` | 策略门、`WherePolicyState` |
| `where/executor/match/match_impl.cc` | 行匹配热路径 |
| `where/executor/cost/cost_model.h` | 代价/行数启发式 |
| `where/executor/internal/query_internal.h` | executor 内部共享 |
| `where/executor/stats/table_stats.{h,cc}` | **`TableStats`** / **`ColumnStats`**、落盘与指纹 |

阅读顺序：parser → **`where.h`** → `plan_impl` / `cache_impl` / `policy_service` → `match_impl` → `table_stats`。

---

## 4. `newdb/tools`、`tests`、`rust_gui`、`scripts`

| 组件 | 职责 | 数据流要点 |
|------|------|------------|
| `newdb/tools/*` | `perf`、`concurrent_perf`、`smoke`、**`runtime_report`** | 链引擎或读统计 → 控制台/JSON |
| `newdb/tests/*.cpp` | 单测、集成、`gtest_c_api` 桥 | 临时目录、内存堆、断言 |
| `newdb/rust_gui/` | Tauri + Vue | **`libnewdb.dll`** C API **或** **`newdb_demo` 子进程**；命令字符串、分页、日志；**`CONFIRM_REORDER`** 经 UI 确认下发；`sync_runtime_binaries.ps1` 同步 `src-tauri/bin` 与脚本资源 |
| `newdb/scripts/ci`、`validate`、`soak`、`bench` | 门禁与统计 rollup | `*.jsonl`、`runtime_trend_dashboard.json` |

**GUI 与统计契约**：`newdb/rust_gui/src/commandPolicy.ts` 中 **`RUNTIME_TUNING_DIAGNOSTIC_GROUPS`** 的 `keys[]` 必须与 **`SHOW TUNING JSON` / `TxnRuntimeStats` 序列化键**一致；完整 schema 见 `RUNTIME_STATS_SCHEMA.md`（原文 §7、§12.11 节选）。

---

## 5. `newdb/docs` 与 `newdb/intro`

| 路径 | 数据流角色 |
|------|------------|
| `newdb/docs/**` | 人读：架构、存储治理、事务隔离、CI 预算等 |
| `newdb/intro/**` | LaTeX → PDF，与源码交叉引用 |

---

## 6. 磁盘产物与模块对应（数据结构在磁盘上的形状）

| 产物模式 | 写入方（典型） | 读取方（典型） |
|----------|----------------|----------------|
| `*.bin` / `*.attr` | 引擎堆 + CLI 事务写路径 | `HeapTable`、WHERE 扫描 |
| `demodb.wal`、`demodb.wal_lsn`、`walsync.conf` | `WalManager`、CLI WAL 配置 | 恢复、`WalRedoPlanner/Applier`、协调器 `recoverFromWAL` |
| `*.tablestats` | `save_table_stats_file`（executor 侧） | `load_table_stats_file`、`plan_impl`（stale 标记） |
| `*.eqbloom` 等 sidecar | sidecar writers | `plan_impl` + 各 `*_sidecar`；失效由 `sidecar_invalidate_service` |

---

## 7. 数据结构全书（类型级；字段级见原文 §12.12）

下列类型均在原文 **§12.2–§12.9** 有展开；此处给出**定义位置 + 子字段/枚举清单 + 一句语义**，便于检索。

### 7.1 CLI 聚合：`ShellState`（`newdb/cli/shell/state/shell_state.h`）

| 成员 / 嵌套类型 | 语义摘要 |
|-----------------|----------|
| `BenchmarkProfile` | `NewdbDefault` / `LeveldbLike` / `InnodbLike` / `HybridBalanced` |
| `RuntimePolicy` | `profile`、`initialized` |
| `LsmShellCache` | `hot_index_recent`、`lsm_memtable`、`lsm_immutable`、`lsm_segments`、`lsm_memtable_bytes`、`lsm_seq`、`lsm_table_name` |
| `LsmEntry` | `row`、`deleted`、`seq` |
| `LsmSegmentMeta` | `path`、`level`、`min_key`/`max_key`、`entry_count`、`max_seq` |
| `session` | `newdb::Session` |
| `session_heap_guard` | `optional<Session::HeapAccess>`，phase-2 堆指针生命周期 |
| `txn` | `TxnCoordinator` |
| `log_file_path` | 会话日志/锁路径基准 |
| `data_dir` | 数据根，参与 `resolve_table_file` |
| `mirror_output_fd` | 输出镜像 FD |
| `encrypt_log` / `verbose` | 日志加密与冗长诊断 |
| `where_ctx` | `WhereQueryContext` |
| `lsm` | `LsmShellCache` |
| `sidecar` | `SidecarShellTuning`：`sidecar_pending_writes`、`sidecar_invalidate_every_n`、`sidecar_invalidate_mode` |

**辅助函数**：`get_cached_table(ShellState&)`、`shell_invalidate_session_table(ShellState&)`。  
**RAII**：`HeapReadViewGuard` — 构造调用 `txn.syncHeapReadSnapshotForQuery(tbl)`，析构 `tbl.clear_snapshot()`。

### 7.2 事务：`TxnCoordinator` 周边（`newdb/cli/modules/txn/coordinator/txn_manager.h`）

| 类型 | 子字段 / 枚举值 | 语义角色 |
|------|-----------------|----------|
| `TxnState` | `None`、`Active`、`Committed`、`RolledBack` | 事务生命周期 |
| `TxnRecord` | `txn_id`、`state`、`table_name`、`operation`、`key`、`old_value`、`new_value`、`timestamp`、`op_seq`、`wal_lsn` | 逻辑回滚链与 WAL 对齐 |
| `TxnRuntimeStats` | **大量** `uint64`/`double`/`string`/`bool` 字段 | 扁平可观测：vacuum、WAL 恢复、写冲突、锁、LSM、page_cache、memory_budget、**`mem_*`**、storage_health、快照 LSN、**`write_*_p95_ms`** 等 |
| `WriteConflictPolicy` | `Reject`、`Wait` | 写冲突策略 |
| `TxnIsolationLevel` | `ReadCommitted`、`Snapshot` | 与读视图刷新语义对应 |
| `WriteTimingStage` | `HeapAppend` … `LsmCompaction` | 写路径分段计时标签 |
| `LockKey` / `LockKeyKind` | range / predicate / secondary 等扩展写意图 | 与 `tryReserveWriteLockKey` 共用 reservation map |

**`TxnCoordinator` 主要 API（类别）**：`begin`/`commit`/`rollback`/`savepoint`/`recoverToLsn`/`recoverToTime`；`acquireLock`/`releaseLock`/`isLocked`；`writeWAL`/`flushWAL`/`recoverFromWAL`/`setWalSyncMode`；`recordOperation`；`tryReserveWriteKey`/`tryReserveWriteKeysBatchSorted`/`tryReserveWriteLockKey`/`releaseWriteIntentStorageKeysForCurrentTxn`；写冲突与隔离级别 setter/getter（完整列表见头文件）。

**扩展写意图与 PK 行锁（heap + sidecar 实情）**

- **主锁仍是行 PK**：生产 DML 以 `LockKey::row_pk_write_intent`（`table#<id>`）为主；正确性以最终写到的行为准。
- **`index_eq_write_intent`**：在 `INSERT`/`UPDATE`/`SETATTR`/`SETATTRMULTI` 事务路径上，对发生变更的列按「旧值 / 新值」额外预留 `v2|idx|…` 键（`index` 字段使用**列名**作为逻辑索引名，与 eq sidecar 按列失效约定对齐）。冲突判定仍是 **`to_storage_key()` 字符串全等**，不做唯一性证明或桶级重叠推理。
- **`range_write_intent` / `predicate_write_intent`**：可选粗粒度互斥。`range` 仅在 **AND 链、单列、`>=`/`<=`、整数 RHS** 时可由 `where_try_derive_closed_int_range` 推导闭区间；**不**比较区间端点是否重叠——两把 range 锁是否冲突仍看 **storage key 是否完全相同**。`predicate` 使用 `where_predicate_fingerprint_for_write_intent`，与 `SHOW PLAN` / `EXPLAIN WHERE` 输出的 `predicate_fingerprint` 同源；等价性同样仅为 **字符串相等**。
- **事务内读路径接线**：在 `WHERE` / `SHOW PLAN` / `EXPLAIN WHERE` 中，若设置 `NEWDB_WHERE_RESERVE_PREDICATE=1` 或 `NEWDB_WHERE_RESERVE_RANGE=1` 且当前在事务中，会尝试对应 `tryReserveWriteLockKey`（便于观测 `lock_key_*_count` 与集成测试）。
- **批量多行写**：`SETATTRMULTI(key, value, id…)` 在事务内对目标 id **排序去重** 后调用 `tryReserveWriteKeysBatchSorted`；`UPDATEWHERE(set_key, set_value, WHERE, …)` / `DELETEWHERE(attr, op, value, …)` 先用 `query_with_index` 得到匹配行，再对涉及的 **row id** 做同样的有序批量 PK 预留（并与 `UPDATEWHERE` 单列变更的 **index_eq** 叠加）。任一 PK 预留失败会 **仅回滚本批已成功部分** 的预留。事务内上述三条命令在默认配置下使用 **内部 SAVEPOINT** 保证语句级堆/undo 一致，并在失败时 **invalidate** 会话堆；**`NEWDB_TXN_STMT_SAVEPOINT=0`** 时仍退回旧语义（IO 中途失败可 `releaseWriteIntentStorageKeysForCurrentTxn` 且不撤销已 `append_row` 的行）。详见 [`TXN_ISOLATION_AND_LOCKING.md`](../txn/TXN_ISOLATION_AND_LOCKING.md) §4.2。无 DEFATTR 的老表布局下 `UPDATEWHERE` 仅允许改 `name` / `balance`。
- **`UPDATEWHERE` 能力边界（产品评估结论）**：每条命令只更新 **单列** `set_key`。要对匹配到的多列一次性赋值，请对同一 WHERE 条件多次调用 `UPDATEWHERE`，或使用 `SETATTRMULTI` / 单行 `UPDATE(id, …)`（全列重写）。若未来需要「单次语法多列」，应另行定义不与逗号解析冲突的封装格式，并在计划中单独评审。
- **二级唯一索引（未来）**：catalog 若增加 **unique secondary**，应在校验唯一性之前对 `(table, index_name, encoded_key)` 调用 `index_eq_write_intent`，并与 PK 预留约定统一加锁顺序（例如先索引键后 PK），在此文档与 `write_conflict_service.cc` 注释中保持一致。
- **事务链已知边界**（无嵌套事务、`BEGIN`/`USE` 对齐与可选严格匹配、批量 DML 语句级 savepoint、Windows 同路径并发 `BEGIN`、SAVEPOINT 与直连协调器 footgun）：见 [`TXN_ISOLATION_AND_LOCKING.md`](../txn/TXN_ISOLATION_AND_LOCKING.md) §4。

### 7.3 WHERE：`newdb/cli/modules/where/executor/where.h` 等

| 类型 | 关键成员 | 语义 |
|------|----------|------|
| `PlanCost` | `estimated_rows` | 轻量代价包 |
| `PlanCandidate` | `id`、`estimated_cost`、`cost`、`rationale` | `SHOW PLAN` / C API `where_plan_json` 候选 |
| `WhereCond` | `attr`、`op`、`value`、`logic_with_prev` | 单谓词 |
| `WherePolicyState` | `blocked`、`message`、`window_sec`、`window_count` | 策略拒绝与滑动窗口 |
| `WhereQueryContext` | `query_cache*` LRU（`kMaxQueryCacheEntries=128`）、大量 `atomic` 计数器、`policy`、`query_stats_hint`、`last_plan_id`、`query_temp_reserved_bytes`、`mu` 等 | **每 shell** 查询态与观测 |

**自由函数**：`row_match_condition`、`parse_where_args_to_conds`、`row_match_multi_conditions`、`parse_agg_args_with_optional_where`、`where_estimate_scan_rows`、`where_build_plan_candidates`、**`query_with_index`**。

### 7.4 表统计：`newdb/cli/modules/where/executor/stats/table_stats.h`

| 类型 | 关键成员 | 语义 |
|------|----------|------|
| `ColumnStats` | `non_null_count`、`distinct_count`、`min_value`/`max_value`、`top_k` | 列级 ANALYZE 风格统计 |
| `TableStats` | `row_count`、`stats_built_ts_ms`、`stats_schema_fp`、`columns` | 表级 + 指纹防 DDL 陈旧 |

**API**：`table_stats_file_path_for_data_file`、`table_stats_schema_fingerprint`、`load_table_stats_file`、`save_table_stats_file`、`build_table_stats_from_heap`、`invalidate_table_stats_for_data_file`、`table_stats_matches_schema`、选择性函数 `eq_selectivity_from_stats` / `range_selectivity_from_stats`（签名见头文件）。

### 7.5 Sidecar 目录：`newdb/cli/modules/sidecar/common/index_catalog.h`

| 类型 | 取值 / 成员 | 语义 |
|------|-------------|------|
| `IndexKind` | `Eq`、`Range`、`Covering`、`PageOrder`、`Visibility` | 侧车类别枚举 |
| `IndexDescriptor` | `table_name`、`index_name`、`kind`、`data_lsn`、`schema_version`、`built_at_ms`、`valid` | 单索引元数据 |
| （解析辅助） | `IndexCatalogParsedTail`、`IndexCatalogPlaintextNames` | 侧车文件头尾明文解析 |

**API**：`index_descriptor_matches_runtime`、`index_catalog_fnv1a64`。

### 7.6 存储健康：`newdb/cli/modules/storage/table_storage_health.h`

| 类型 | 关键字段 | 语义 |
|------|----------|------|
| `TableStorageHealth` | `logical_rows`、`physical_rows`、`tombstone_*`、`data_file_bytes`、`live_bytes`、`dead_bytes`、`fragmentation_ratio`、`last_vacuum_*` | 只读快照，供 vacuum 与 `TxnRuntimeStats` 镜像 |

**函数**：`measure_table_storage_health(const HeapTable&)`。

### 7.7 引擎 WAL 与记录（`newdb/engine/include/newdb/wal_manager.h`）

| 类型 | 关键取值 / 成员 | 语义 |
|------|-----------------|------|
| `WalSyncMode` | `Full`、`Normal`、`Off` | fsync 策略 |
| `WalOp` | `INSERT`…`PITR_MARK` 等 | 记录类型；redo 计划仅消费 DML 子集（原文 §5） |
| `WalRecordHeader` | `magic`、`lsn`、`txn_id`、`payload_len`、`checksum`、`type`、`flags` | 磁盘记录头 |
| `WalDecodedRecord` | `lsn`、`txn_id`、`op`、`table`、`row`/`has_row`、`before_row`/`after_row`、`op_seq_in_txn`、`undo_prev_lsn` 等 | 解码后内存载体 |

### 7.8 页缓存与内存（`page_cache.h`、`memory_budget.h`、`memory_registry.h`）

| 类型/API | 语义 |
|----------|------|
| `PageCacheGlobalStats` | `hits`、`misses`、`evictions`、`bytes_in_cache`、`reject_oversized_page`、`bytes_evicted_total` |
| `page_cache_try_get` / `page_cache_put` | 进程级 LRU（按堆路径 + 页号） |
| `MemoryBudgetSnapshot` | 旧门面：`max_bytes`、`used_bytes`、`reject_count`、`eviction_events`、`bytes_evicted_total` |
| `MemoryRegistry` / `MemoryKind` | 统一 cap：`PageCache`、`EqSidecar`、`QueryTemp`；`try_admit`/`release`、evictor 回调 |

### 7.9 引擎会话与堆（`session.h`、`heap_table.h`）

| 类型 | 语义 |
|------|------|
| `Session` | 打开表、`lock_heap`、与 `HeapTable` 生命周期 |
| `HeapTable` | 堆上读改、MVCC 快照、`logical_row_count`、`rebuild_indexes` 行为（无快照时墓碑 last-writer-wins，原文 §5） |

### 7.10 物化与惰性（原文 §12.12.11）

| API | 行为摘要 |
|-----|----------|
| `newdb_materialize_heap_if_lazy` | 惰性 mmap 堆时强制物化；可接 stats_sink 更新 lazy 物化计数 |

---

## 8. 子模块 → 子模块矩阵（运行期载体）

与原文 §11.9 一致，便于反向查「谁调谁」。

| 源 | 经载体 | 典型目标 | 语义 |
|----|--------|----------|------|
| `repl` | 原始行文本 | `dispatch` | 一行一调度 |
| `dispatch` | `ShellState` | `handlers/*` | 两阶段、责任链 |
| `query_handler` | WHERE 字符串 | `where/parser` | 语法树 |
| `where/plan_impl` | 计划上下文 | `policy` / `cache` / `sidecar` | 剪枝与限流 |
| `where/executor` | 行 id / 列缓冲 | `HeapTable` | 取列、判条件 |
| `dml_handler` | 键与列值 | `txn` → `wal_service` | 持久化顺序 |
| `txn` | LSN、锁集合 | 引擎 WAL + heap | 一致性与隔离 |
| `ddl` | schema diff | `catalog` + `sidecar_invalidate` | 元数据与索引一致 |
| `stats_impl` | 计数器 | `SHOW TUNING` / `runtime_report` | 可观测性出口 |

---

## 9. 引擎 ↔ CLI 合法边界（再强调）

| 方向 | 载体 |
|------|------|
| CLI → Engine | `newdb::HeapTable*`、`Session`、C API（均经 `include/newdb/*.h`） |
| Engine → CLI | 行缓冲、错误码、LSN 可见性 |
| Engine ↔ 磁盘 | WAL codec、segment scanner | 对 CLI 透明 |

---

## 10. 维护：改一处要同步哪里

与原文 §13 对齐的**最短检查单**：

1. 新增 CLI 子模块：在 **handlers** 注册；不拉 `engine/src`。  
2. 新增磁盘产物或 **可序列化统计字段**：更新 `PROJECT_DATAFLOW_WHOLE.md` §5、§10、§11、§12；存储预算见 `STORAGE_GOVERNANCE_AND_RECOVERY_BUDGETS.md`；JSON 契约同步 **`scripts/validate`** 与 **`RUNTIME_STATS_SCHEMA.md`**；GUI 分组同步 **`commandPolicy.ts`** 与原文 §12.11 节选。  
3. **`TxnRuntimeStats` / `WhereQueryContext` 重命名**：必须同步 **§12.12** 全表。

---

## 11. 关键方法实现详解（与源码对照）

以下按**真实实现文件**说明控制流与副作用。各小节文字说明后附有**对应源码摘录**（围栏首行为 `起始行:结束行:newdb/相对路径`，便于在 IDE 中跳转对照）；行号随提交可能变化，请以符号名为准用 `rg` 校对。

### 11.1 `process_command_line`（`newdb/cli/shell/dispatch/router/dispatch.cc`）

**签名**：`bool process_command_line(ShellState& st, const char* input_line)`；返回 `false` 仅当会话子通道要求**退出 REPL**（`handle_session_commands` 返回 `keep_going == false`）。

**逐步行为**：



1. **规范化输入**：把 `input_line` 拷入 `std::string line`，循环去掉尾部 `\r`/`\n`；若为空直接 `return true`（空行视为已消费）。
2. **绑定日志**：`logging_bind_shell(&st)`；`eff_data = effective_data_path(st)`（即 `resolve_table_file(st, st.session.data_path)` 的规范绝对路径）。
3. **会话日志**：`append_session_log_line` 写入当前行（可选 XOR 帧由 `st.encrypt_log` 控制）。
4. **RAII `ShellHeapGuardClear`**：在函数退出时（无论哪条 return）执行 `st->session_heap_guard.reset()`，保证**本条命令结束后释放** `lock_heap`，下一条命令重新 `emplace`。这是 phase-2 与「命令间表缓存」行为的基础。
5. **会话子通道**：`handle_session_commands`；若 `session_handled` 为真则 `return true`；若 `keep_going` 为假则 `return false`（退出）。
6. **Phase-1**：当且仅当 `!shell_line_targets_phase2_only(line_cstr)` 时，顺序调用 8 个 lambda；**任一返回 `true` 则立即 `return true`**，不再进入 phase-2。
7. **Phase-2**：`tbl_ptr = get_cached_table(st)`；若为 `nullptr` 则 **`return true` 且无错误输出**（历史 shell：无表则静默忽略）。
8. 顺序调用 9 个 phase-2 lambda；若全部返回 `false`，打印 `[ERR] unknown command` 仍 `return true`（不把未知命令当成 REPL 退出条件）。



**对应源码**（`newdb/cli/shell/dispatch/router/dispatch.cc` 13–97 行）

```13:97:newdb/cli/shell/dispatch/router/dispatch.cc
bool process_command_line(ShellState& st, const char* input_line) {
    std::string& current_table = st.session.table_name;
    std::string& current_file = st.session.data_path;
    const char* log_file = st.log_file_path.c_str();
    std::string line;
    if (input_line != nullptr) {
        line = input_line;
    }
    while (!line.empty() && (line.back() == '\n' || line.back() == '\r')) {
        line.pop_back();
    }
    if (line.empty()) {
        return true;
    }
    const char* line_cstr = line.c_str();

    logging_bind_shell(&st);
    const std::string eff_data = effective_data_path(st);
    append_session_log_line(log_file, line_cstr, st.encrypt_log);

    struct ShellHeapGuardClear {
        ShellState* st;
        ~ShellHeapGuardClear() {
            if (st != nullptr) {
                st->session_heap_guard.reset();
            }
        }
    } shell_heap_clear{&st};

    bool session_handled = false;
    const bool keep_going = handle_session_commands(line_cstr, log_file, session_handled);
    if (!keep_going) {
        return false;
    }
    if (session_handled) {
        return true;
    }

    // Phase-1: commands that do not require a loaded HeapTable (txn, DDL, catalog, insert, ...).
    if (!shell_line_targets_phase2_only(line_cstr)) {
        const std::array<std::function<bool()>, 8> phase1_handlers = {
            [&]() { return handle_txn_commands(st, line_cstr, log_file, current_table); },
            [&]() { return handle_workspace_admin_commands(st, line_cstr, log_file, eff_data, current_table, current_file); },
            [&]() { return handle_import_defattr_commands(st, line_cstr, log_file, eff_data, current_file); },
            [&]() { return handle_schema_catalog_commands(st, line_cstr, log_file); },
            [&]() { return handle_ddl_create_use_commands(st, line_cstr, log_file, eff_data, current_table, current_file); },
            [&]() { return handle_ddl_alter_rename_commands(st, line_cstr, log_file, eff_data, current_table, current_file); },
            [&]() { return handle_schema_show_commands(st, line_cstr, log_file, current_table, current_file); },
            [&]() { return handle_dml_insert_command(st, line_cstr, log_file, eff_data, current_table, current_file); },
        };
        for (const auto& h : phase1_handlers) {
            if (h()) {
                return true;
            }
        }
    }

    // Phase-2: need a loaded heap table (Session::lock_heap via ShellState guard).
    newdb::HeapTable* tbl_ptr = get_cached_table(st);
    if (!tbl_ptr) {
        return true;
    }
    newdb::HeapTable& tbl = *tbl_ptr;

    // Phase-2 dispatch chain: commands requiring loaded table cache.
    const std::array<std::function<bool()>, 9> phase2_handlers = {
        [&]() { return handle_schema_key_command(st, line_cstr, log_file, eff_data, tbl); },
        [&]() { return handle_query_where_count_commands(st, line_cstr, log_file, eff_data, current_table, tbl); },
        [&]() { return handle_dml_update_delete_commands(st, line_cstr, log_file, eff_data, current_table, tbl); },
        [&]() { return handle_dml_attr_commands(st, line_cstr, log_file, eff_data, current_table, tbl); },
        [&]() { return handle_query_find_commands(st, line_cstr, log_file, eff_data, tbl); },
        [&]() { return handle_query_sum_avg_commands(st, line_cstr, log_file, eff_data, tbl); },
        [&]() { return handle_query_min_max_commands(st, line_cstr, log_file, tbl); },
        [&]() { return handle_query_page_command(st, line_cstr, log_file, eff_data, tbl); },
        [&]() { return handle_export_command(st, line_cstr, log_file, current_table, current_file, tbl); },
    };
    for (const auto& h : phase2_handlers) {
        if (h()) {
            return true;
        }
    }

    log_and_print(log_file, "[ERR] unknown command. Type HELP.\n");
    return true;
}
```

### 11.2 `shell_line_targets_phase2_only`（`newdb/cli/shell/dispatch/router/dispatch_routing.cc`）

**目的**：行首（跳过空白后）若以 phase-2 动词开头，则**跳过整个 phase-1**，避免在无表场景下跑事务/DDL 链。

**辅助函数**：

- `skip_ws`：跳过空格与 `\t`。
- `command_has_prefix_token(s, pfx, len)`：用 **`strncasecmp_ascii`** 比较前缀 `len` 字节；若前缀末字符为 `(`（如 `FIND(`）则匹配长度即算命中；否则要求 `s[len]` 为 `\0`、空白或 `(`，避免 `WHEREEVER` 误匹配 `WHERE`。



**主逻辑**：对静态表 `kPhase2Prefixes` 逐项调用 `command_has_prefix_token`；任一命中返回 `true`。



**对应源码**（`newdb/cli/shell/dispatch/router/dispatch_routing.cc` 11–70 行）

```11:70:newdb/cli/shell/dispatch/router/dispatch_routing.cc
const char* skip_ws(const char* p) {
    while (*p == ' ' || *p == '\t') {
        ++p;
    }
    return p;
}

bool command_has_prefix_token(const char* s, const char* pfx, std::size_t len) {
    if (strncasecmp_ascii(s, pfx, len) != 0) {
        return false;
    }
    if (len > 0 && pfx[len - 1] == '(') {
        return true;
    }
    const char c = s[len];
    return c == '\0' || c == ' ' || c == '\t' || c == '(';
}

} // namespace

bool shell_line_targets_phase2_only(const char* line) {
    if (line == nullptr) {
        return false;
    }
    const char* s = skip_ws(line);
    if (*s == '\0') {
        return false;
    }
    // Prefixes aligned with handlers in query/dml/io/ddl (phase-2 chain order is irrelevant here).
    static const struct {
        const char* pfx;
        std::size_t len;
    } kPhase2Prefixes[] = {
        {"PAGE", 4},
        {"WHEREP", 6},
        {"WHERE", 5},
        {"COUNT", 5},
        {"MIN", 3},
        {"MAX", 3},
        {"SUM", 3},
        {"AVG", 3},
        {"FIND(", 5},
        {"FINDPK", 6},
        {"QBAL", 4},
        {"UPDATE", 6},
        {"DELETE(", 7},
        {"DELETEPK", 8},
        {"EXPORT", 6},
        {"SETATTR", 7},
        {"RENATTR", 7},
        {"DELATTR", 7},
        {"SET PRIMARY KEY", 15},
    };
    for (const auto& e : kPhase2Prefixes) {
        if (command_has_prefix_token(s, e.pfx, e.len)) {
            return true;
        }
    }
    return false;
}
```
### 11.3 `get_cached_table` / `shell_invalidate_session_table` / `newdb_materialize_heap_if_lazy`（`shell_state.h` 内联）

**`get_cached_table(st)`**：

- 若 `session_heap_guard` 未设置或布尔值为假，则 `emplace(session.lock_heap(log_file_path.c_str()))`，在**当前命令**内持有 `Session::mut_`，使 `HeapTable*` 稳定。
- 返回 `acc ? &st.session.table : nullptr`：未成功打开堆表时指针为空。

**`shell_invalidate_session_table`**：先 **`session_heap_guard.reset()`** 再 **`session.invalidate()`**，注释说明避免在仍持有 `HeapAccess` 时 invalidate 导致**死锁**。

**`newdb_materialize_heap_if_lazy`**：非 mmap 堆直接 `Ok`；否则读 `NEWDB_LAZY_MATERIALIZE_WARN_ROWS`（默认 10000），超阈值 `fprintf` 警告；`steady_clock` 计时后调用 `t.materialize_all_rows(sch)`；若 `stats_sink` 非空则 `txn.noteLazyMaterialize` 更新 lazy 物化统计。



**`HeapReadViewGuard`**：构造时 `st.txn.syncHeapReadSnapshotForQuery(tbl)`，析构 `tbl.clear_snapshot()`，保证查询期间快照安装与清理成对。



**对应源码**（`newdb/cli/shell/state/shell_state.h` 121–194 行）

```121:194:newdb/cli/shell/state/shell_state.h
inline newdb::HeapTable* get_cached_table(ShellState& st) {
    if (!st.session_heap_guard.has_value() || !st.session_heap_guard.value()) {
        st.session_heap_guard.emplace(st.session.lock_heap(st.log_file_path.c_str()));
    }
    newdb::Session::HeapAccess& acc = st.session_heap_guard.value();
    return acc ? &st.session.table : nullptr;
}

// Call before `Session::invalidate()` so the session mutex is not held by `HeapAccess` (deadlock).
inline void shell_invalidate_session_table(ShellState& st) {
    st.session_heap_guard.reset();
    st.session.invalidate();
}

// Expands mmap-backed heap into `HeapTable::rows` (required before mutating `rows` in memory).
// Avoid on large tables except where mutation or APIs explicitly require a full vector: it forces
// a full read and can dominate memory. Prefer lazy heap paths (PAGE, indexed WHERE) when possible;
// tune `NEWDB_LAZY_MATERIALIZE_WARN_ROWS` (env) to surface accidental full materialization earlier.
// When `stats_sink` is non-null, successful materialization updates `TxnRuntimeStats` lazy counters.
inline newdb::Status newdb_materialize_heap_if_lazy(newdb::HeapTable& t,
                                                    const newdb::TableSchema& sch,
                                                    ShellState* stats_sink = nullptr) {
    if (!t.is_heap_storage_backed()) {
        return newdb::Status::Ok();
    }
    const std::size_t logical_rows = t.logical_row_count();
    std::size_t warn_rows = 10000;
    if (const char* env = std::getenv("NEWDB_LAZY_MATERIALIZE_WARN_ROWS")) {
        try {
            const std::size_t v = static_cast<std::size_t>(std::stoull(env));
            if (v > 0) {
                warn_rows = v;
            }
        } catch (...) {
        }
    }
    if (logical_rows >= warn_rows) {
        std::fprintf(stderr,
                     "[LAZY_MATERIALIZE] forcing full materialization rows=%zu (warn_rows=%zu). "
                     "Prefer PAGE/indexed WHERE/streaming reads for large tables.\n",
                     logical_rows,
                     warn_rows);
    }
    const auto t0 = std::chrono::steady_clock::now();
    const newdb::Status st = t.materialize_all_rows(sch);
    if (st.ok && stats_sink != nullptr) {
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
        stats_sink->txn.noteLazyMaterialize(
            static_cast<std::uint64_t>(logical_rows),
            static_cast<std::uint64_t>(ms < 0 ? 0 : ms));
    }
    return st;
}

inline void reload_schema_from_data_path(ShellState& st, const std::string& data_path) {
    st.session_heap_guard.reset();
    st.session.data_path = resolve_table_file(st, data_path);
    (void)newdb::load_schema_text(newdb::schema_sidecar_path_for_data_file(st.session.data_path),
                                   st.session.schema);
    st.session.invalidate();
}

inline const newdb::AttrMeta* find_attr_meta(const newdb::TableSchema& sch, const std::string& name) {
    return sch.find_attr(name);
}

struct HeapReadViewGuard {
    ShellState& st;
    newdb::HeapTable& tbl;
    HeapReadViewGuard(ShellState& s, newdb::HeapTable& t) : st(s), tbl(t) { st.txn.syncHeapReadSnapshotForQuery(tbl); }
    ~HeapReadViewGuard() { tbl.clear_snapshot(); }
};
```
### 11.4 `query_with_index`（`newdb/cli/modules/where/executor/plan/plan_impl.cc`）

**签名**：返回 `std::vector<std::size_t>` 为**逻辑行 slot 下标**（非主键值）。`ctx_ptr` 可空；空时使用进程内 `default_where_context()`。

**主流程摘要**（按源码顺序）：

1. **上下文与计数**：`where_policy_set` 清阻塞；`ctx.query_count++`。
2. **缓存**：`build_query_cache_key` → `query_cache_get`。命中时：若**非**「单条件且 `is_single_cond_index_friendly`」策略旁路，则走 `where_policy_gate("cache_hit", ...)`，策略拒绝则返回空向量；否则直接返回缓存结果并记 `trace.mode = "cache_hit"`。
3. **`conds.empty()`**：估计 `QueryTemp` 字节，`memory_registry_try_admit(QueryTemp)` 失败则策略置 `query_temp_memory_cap` 并返回空；成功则用 `QueryTempBytesGuard` RAII、`visible_slots_for_query` 得到**全表可见 slot**、写入缓存、`trace.mode = "full_scan_all"`。
4. **非空条件**：对主路径同样 `try_admit(QueryTemp)`；成功则 `maybe_prewarm_eq_sidecars`。
5. **单条件快路径**：`conds.size()==1` 时 `collect_single_condition_candidates`；若成功则按 `single_mode`（如 `eq_sidecar`、`id_lookup`） bump 计划计数、`query_cache_put`、返回。
6. **多条件 AND 优化**：若 `all_and_chain` 且两条件皆可索引友好，则分别收集候选、`sanitize_sort_slots`、`intersect_sorted_slots`，再对交集中每 slot `row_at_logical_slot` + `row_match_multi_conditions_prepared` 过滤；`trace.mode = "and_intersect_2"`。
7. **更一般 AND**：选择 **seed 条件**（`plan_metric_for_cond` 最小代价且能收集候选）；在 seed 候选上过滤其余条件（后续代码在 `and_fast_path_fallback` 之后继续，含回落全扫等；完整分支见同文件 `goto`/循环体）。



**内存**：`QueryTempBytesGuard` 在作用域结束释放 `MemoryRegistry` 中本次预留；与原文 §5 P5 闭环一致。

摘录在双条件 AND 快路径返回处结束；其后尚有 `and_fast_path_fallback`、seed 选择与全表扫描等分支，见 `plan_impl.cc` 同函数后续行。

**对应源码**（`newdb/cli/modules/where/executor/plan/plan_impl.cc` 445–572 行）

```445:572:newdb/cli/modules/where/executor/plan/plan_impl.cc
std::vector<std::size_t> query_with_index(const newdb::HeapTable& tbl,
                                          const newdb::TableSchema& schema,
                                          const std::vector<WhereCond>& conds,
                                          WhereQueryContext* ctx_ptr) {
    std::vector<std::size_t> result;
    WhereQueryContext& ctx = (ctx_ptr != nullptr) ? *ctx_ptr : default_where_context();
    where_policy_set(ctx, false, "");
    ctx.query_count.fetch_add(1, std::memory_order_relaxed);
    const auto bump_query_io = [&](const std::uint64_t rows_scanned, const std::size_t rows_returned) {
        ctx.query_rows_scanned_total.fetch_add(rows_scanned, std::memory_order_relaxed);
        ctx.query_rows_returned_total.fetch_add(static_cast<std::uint64_t>(rows_returned),
                                                std::memory_order_relaxed);
    };

    const std::size_t n = tbl.logical_row_count();
    const bool has_or = std::any_of(conds.begin(), conds.end(), [](const WhereCond& c) {
        return c.logic_with_prev == "OR";
    });
    const auto estimate_scan_rows = [&](const std::vector<WhereCond>& in) -> std::size_t {
        return where_estimate_scan_rows(tbl, schema, in, ctx_ptr);
    };
    QueryTraceGuard trace(n, conds.size(), &ctx);
    const std::string cache_key = build_query_cache_key(tbl, schema, conds);
    if (query_cache_get(ctx, cache_key, result)) {
        const bool policy_bypass = (conds.size() == 1 && is_single_cond_index_friendly(conds[0], schema));
        if (!policy_bypass &&
            !where_policy_gate("cache_hit", n, conds.size(), estimate_scan_rows(conds), has_or, ctx)) {
            trace.mode = "policy_reject";
            trace.rows = 0;
            trace.plan_candidates = 1;
            bump_query_io(0, 0);
            return {};
        }
        trace.mode = "cache_hit";
        trace.rows = result.size();
        trace.plan_candidates = 1;
        bump_query_io(0, result.size());
        return result;
    }

    if (conds.empty()) {
        const std::uint64_t qt_est = where_query_temp_est_bytes(n, 1);
        if (!newdb::memory_registry_try_admit(newdb::MemoryKind::QueryTemp, qt_est)) {
            where_policy_set(ctx, true, "query_temp_memory_cap");
            trace.mode = "query_temp_memory_cap";
            trace.rows = 0;
            trace.plan_candidates = 1;
            bump_query_io(0, 0);
            return {};
        }
        QueryTempBytesGuard qt_guard(qt_est, &ctx);
        result = visible_slots_for_query(tbl, schema, n);
        query_cache_put(ctx, cache_key, result);
        trace.mode = "full_scan_all";
        trace.rows = result.size();
        trace.plan_candidates = 1;
        bump_query_io(static_cast<std::uint64_t>(result.size()), result.size());
        return result;
    }
    const std::uint64_t qt_est_main = where_query_temp_est_bytes(n, conds.size());
    if (!newdb::memory_registry_try_admit(newdb::MemoryKind::QueryTemp, qt_est_main)) {
        where_policy_set(ctx, true, "query_temp_memory_cap");
        trace.mode = "query_temp_memory_cap";
        trace.rows = 0;
        trace.plan_candidates = 1;
        bump_query_io(0, 0);
        return {};
    }
    QueryTempBytesGuard qt_guard_main(qt_est_main, &ctx);
    maybe_prewarm_eq_sidecars(tbl, schema, conds, n, ctx);

    if (conds.size() == 1) {
        const WhereCond& c = conds[0];
        const char* single_mode = "single_scan";
        if (collect_single_condition_candidates(tbl, schema, c, n, result, single_mode, &ctx)) {
            if (std::strcmp(single_mode, "eq_sidecar") == 0) {
                ctx.plan_eq_sidecar_count.fetch_add(1, std::memory_order_relaxed);
            } else if (std::strcmp(single_mode, "id_lookup") == 0 || std::strcmp(single_mode, "pk_lookup") == 0) {
                ctx.plan_id_pk_count.fetch_add(1, std::memory_order_relaxed);
            }
            query_cache_put(ctx, cache_key, result);
            trace.mode = single_mode;
            trace.rows = result.size();
            trace.plan_candidates = 1;
            bump_query_io(static_cast<std::uint64_t>(result.size()), result.size());
            return result;
        }
    }
    const std::vector<PreparedCond> prepared_conds = prepare_conditions(schema, conds);

    // Batch optimization for multi-condition AND:
    // pick the most selective seed condition and filter remaining conditions on seed candidates.
    if (conds.size() > 1 && all_and_chain(conds)) {
        // Two-condition fast path: intersect indexed candidates directly.
        if (conds.size() == 2 &&
            is_single_cond_index_friendly(conds[0], schema) &&
            is_single_cond_index_friendly(conds[1], schema)) {
            std::vector<std::size_t> a_raw;
            std::vector<std::size_t> b_raw;
            const char* mode_a = "";
            const char* mode_b = "";
            const bool a_ok =
                collect_single_condition_candidates(tbl, schema, conds[0], n, a_raw, mode_a, &ctx);
            const bool b_ok =
                collect_single_condition_candidates(tbl, schema, conds[1], n, b_raw, mode_b, &ctx);
            if (!a_ok || !b_ok) {
                goto and_fast_path_fallback;
            }
            const std::vector<std::size_t> a = sanitize_sort_slots(a_raw, n);
            const std::vector<std::size_t> b = sanitize_sort_slots(b_raw, n);
            const std::vector<std::size_t> inter = intersect_sorted_slots(a, b);
            result.reserve(inter.size());
            newdb::Row r;
            for (const std::size_t slot : inter) {
                if (!row_at_logical_slot(tbl, slot, r)) {
                    continue;
                }
                if (row_match_multi_conditions_prepared(r, schema, prepared_conds)) {
                    result.push_back(slot);
                }
            }
            query_cache_put(ctx, cache_key, result);
            ctx.plan_eq_sidecar_count.fetch_add(1, std::memory_order_relaxed);
            trace.mode = "and_intersect_2";
            trace.rows = result.size();
            trace.plan_candidates = 3;
            bump_query_io(static_cast<std::uint64_t>(inter.size()), result.size());
            return result;
```
### 11.5 `TxnCoordinator::begin`（`newdb/cli/modules/txn/coordinator/core/core_impl.cc`）

在 **`m_txn_mutex`** 下：



- 若已是 `TxnState::Active` 则返回 Err。
- 分配单调 **`m_txn_id`**（全局 `g_txn_id_seed`），状态置 `Active`，清空 `m_txn_records`、savepoint 结构、写意图、`g_txn_wait_for_owner` 中本事务等待项。
- **`m_active_table = table_name`**，`resolveDataFilePath` → **`acquireLock(bin_file)`**；失败则计数 `txn_begin_lock_conflict_count`、状态回滚为 `None`、返回锁错误。
- **`writeWAL("TXN_BEGIN", ...)`** 后 **`flushWAL()`**。
- 若隔离级别为 **`Snapshot`** 且 `wal_` 存在，将 **`m_txn_read_view_lsn`** 设为 **`wal_->current_lsn()`**（用于固定读视图）；否则置 0。



**对应源码**（`newdb/cli/modules/txn/coordinator/core/core_impl.cc` 117–164 行）

```117:164:newdb/cli/modules/txn/coordinator/core/core_impl.cc
Result<bool> TxnCoordinator::begin(const std::string& table_name) {
    std::lock_guard<std::mutex> lk(m_txn_mutex);
    
    if (m_state.load() == TxnState::Active) {
        return Result<bool>::Err("??????");
    }
    
    // ?????????ID????????????begin ??????
    const std::int64_t next_txn_id = g_txn_id_seed.fetch_add(1, std::memory_order_relaxed) + 1;
    m_txn_id.store(next_txn_id, std::memory_order_relaxed);
    
    m_state.store(TxnState::Active);
    m_txn_op_seq.store(0, std::memory_order_relaxed);
    m_txn_records.clear();
    m_savepoints.clear();
    m_savepoints_lsn.clear();
    m_last_undo_lsn = 0;
    m_reserved_write_keys.clear();
    {
        std::lock_guard<std::mutex> wait_lk(g_write_intent_mu);
        g_txn_wait_for_owner.erase(static_cast<std::uint64_t>(next_txn_id));
    }
    
    m_active_table = table_name;
    const std::string bin_file = resolveDataFilePath(table_name);
    auto lockResult = acquireLock(bin_file);
    if (lockResult.isErr()) {
        m_txn_begin_lock_conflict_count.fetch_add(1, std::memory_order_relaxed);
        m_state.store(TxnState::None);
        return lockResult;
    }
    writeWAL("TXN_BEGIN", table_name, "", "", "");
    flushWAL();
    {
        std::lock_guard<std::mutex> wlk(m_wal_mutex);
        if (wal_) {
            if (txnIsolationLevel() == TxnIsolationLevel::Snapshot) {
                m_txn_read_view_lsn.store(wal_->current_lsn(), std::memory_order_relaxed);
            } else {
                m_txn_read_view_lsn.store(0, std::memory_order_relaxed);
            }
        } else {
            m_txn_read_view_lsn.store(0, std::memory_order_relaxed);
        }
    }

    return Result<bool>::Ok(true);
}
```
### 11.6 `TxnCoordinator::commit`（同文件）

在 **`m_txn_mutex`** 下：



- 非 `Active` 则 Err。
- **`writeWAL("TXN_COMMIT", ...)`**，`m_wal_group_commit_pending_commits++`，**`flushWAL()`**（与组提交窗口配合，见下节）。
- 提交计数、**释放本协调器持有的所有文件锁** `releaseLock`。
- 状态置 `Committed`，`clearWriteIntents`，清空 records/locks/active_table，**`m_txn_read_view_lsn = 0`**。
- 若 vacuum 线程在跑且写操作计数超阈值，则在锁外 **`triggerVacuum(committed_table)`**。
- 锁外 **`maybeCompactWalAfterCommit`**；并用滑动窗口样本更新 **`m_txn_commit_p95_ms` / max**（向 `m_commit_latency_ms_samples` 推入耗时）。



**对应源码**（`newdb/cli/modules/txn/coordinator/core/core_impl.cc` 167–232 行）

```167:232:newdb/cli/modules/txn/coordinator/core/core_impl.cc
Result<bool> TxnCoordinator::commit() {
    const auto commit_start = std::chrono::steady_clock::now();
    std::string committed_table;
    bool should_trigger_vacuum = false;
    {
        std::lock_guard<std::mutex> lk(m_txn_mutex);
        
        if (m_state.load() != TxnState::Active) {
            return Result<bool>::Err("??????");
        }
        
        writeWAL("TXN_COMMIT", m_active_table, "", "", "");
        m_wal_group_commit_pending_commits.fetch_add(1, std::memory_order_relaxed);
        flushWAL();
        m_txn_commit_count.fetch_add(1, std::memory_order_relaxed);
        
        std::vector<std::string> locked_copy;
        {
            std::lock_guard<std::mutex> lk2(m_lock_mutex);
            locked_copy = m_locked_files;
        }
        for (const auto& f : locked_copy) {
            (void)releaseLock(f);
        }
        
        committed_table = m_active_table;
        m_state.store(TxnState::Committed);
        m_txn_op_seq.store(0, std::memory_order_relaxed);
        clearWriteIntents();
        m_txn_records.clear();
        m_locked_files.clear();
        m_active_table.clear();
        m_txn_read_view_lsn.store(0, std::memory_order_relaxed);

        if (m_vacuum_running.load()) {
            const std::size_t threshold = m_vacuum_ops_threshold.load();
            const std::size_t count = m_vacuum_op_counter.load();
            if (threshold > 0 && count >= threshold && !committed_table.empty()) {
                m_vacuum_op_counter.store(0);
                should_trigger_vacuum = true;
            }
        }
    }
    maybeCompactWalAfterCommit(committed_table);
    if (should_trigger_vacuum) {
        triggerVacuum(committed_table);
    }
    const auto elapsed_ms = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - commit_start)
            .count());
    {
        std::lock_guard<std::mutex> lk(m_samples_mu);
        m_commit_latency_ms_samples.push_back(elapsed_ms);
        if (m_commit_latency_ms_samples.size() > 256) {
            m_commit_latency_ms_samples.erase(m_commit_latency_ms_samples.begin(),
                                              m_commit_latency_ms_samples.begin() + 64);
        }
    }
    std::uint64_t old_max = m_txn_commit_max_ms.load(std::memory_order_relaxed);
    while (elapsed_ms > old_max &&
           !m_txn_commit_max_ms.compare_exchange_weak(
               old_max, elapsed_ms, std::memory_order_relaxed, std::memory_order_relaxed)) {
    }
    return Result<bool>::Ok(true);
}
```
### 11.7 `TxnCoordinator::writeWAL` / `ensureWal` / `flushWAL`（`wal_service.cc`）

**`ensureWal()`**：

- `wal_` 为空时在 workspace 根创建 **`WalManager("demodb", ws)`**，尝试 **`loadWalSyncConfig`**；失败则默认 **`WalSyncMode::Normal`** + 20ms 间隔并持久化配置。
- 环境变量 **`NEWDB_WAL_SYNC`** 可覆盖为 `off`/`normal`/其他→`Full`；最后 **`wal_->open()`**。

**`writeWAL`**（持 **`m_wal_mutex`**）：

- 特殊操作字符串分支映射到引擎 API：  
  - **`TXN_BEGIN`**：`wm->begin_transaction(txn)`，并额外 **`append_record(SESSION_SNAPSHOT, ...)`** 以推进 LSN，使 Snapshot 下 `BEGIN` 能钉在非零 LSN（注释写明引擎 `begin_transaction` 本身可能不推进 LSN）。  
  - **`TXN_COMMIT` / `TXN_ROLLBACK`**：`commit_transaction` / `rollback_transaction`。  
  - **`SAVEPOINT_*` / `TXN_ABORT_PARTIAL` / `PITR_MARK` / `RECOVERY_DONE`**：各自 `append_record` 与 WalOp 对应。  
- **普通 DML**：根据 `operation` 选 `WalOp::INSERT/UPDATE/DELETE`；把 `old_val`/`new_val` 解析为 `before`/`after` 的 `Row.attrs`（跳过 `__deleted`），并把 legacy 字符串塞进 `row.attrs["__wal_old"]` 等供恢复兼容；`append_record_with_lsn` 写 undo 链 `m_last_undo_lsn`；记录 **`onWriteTiming(WalAppend, ms)`**。

**`flushWAL`**：



**`writeWAL`**


**`flushWAL`**


- 更新 **`m_wal_bytes_since_start`**（用 `wal_file_size_bytes` 差分）。
- **组提交**：若 `pending < max_batch` 且距上次 flush 不足 `window_ms`，则 **提前 return**（不调用 `wm->flush()`），实现批量提交合并刷盘。
- **Hybrid 自适应**：若开启，根据 vacuum 队列深度、恢复耗时 tail、锁等待 tail 等切换 `m_hybrid_mode`，在「耐久模式」下设 **`WalSyncMode::Full`** 并收紧组提交；否则 `Normal` + 20ms。
- 最后 **`wm->flush()`**、`persistWalsnHighWaterUnlocked`，并清零/累计组提交计数器。



**对应源码**（`newdb/cli/modules/txn/coordinator/wal/wal_service.cc` 144–165 行）

```144:165:newdb/cli/modules/txn/coordinator/wal/wal_service.cc
newdb::WalManager* TxnCoordinator::ensureWal() {
    if (!wal_) {
        const std::string ws = m_workspace_root.empty() ? "." : m_workspace_root;
        wal_ = std::make_unique<newdb::WalManager>("demodb", ws);
        const bool loaded = loadWalSyncConfig(*wal_);
        if (!loaded) {
            // Default startup profile for balanced throughput/durability.
            wal_->set_sync_mode(newdb::WalSyncMode::Normal);
            wal_->set_normal_sync_interval_ms(20);
            persistWalSyncConfig(*wal_);
        }
        if (const char* mode = std::getenv("NEWDB_WAL_SYNC")) {
            std::string m = mode;
            for (auto& ch : m) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
            if (m == "off") wal_->set_sync_mode(newdb::WalSyncMode::Off);
            else if (m == "normal") wal_->set_sync_mode(newdb::WalSyncMode::Normal);
            else wal_->set_sync_mode(newdb::WalSyncMode::Full);
        }
        (void)wal_->open();
    }
    return wal_.get();
}
```


**`writeWAL`**


**对应源码**（`newdb/cli/modules/txn/coordinator/wal/wal_service.cc` 168–318 行）

```168:318:newdb/cli/modules/txn/coordinator/wal/wal_service.cc
void TxnCoordinator::writeWAL(const std::string& operation, const std::string& table,
                          const std::string& key, const std::string& old_val, const std::string& new_val) {
    std::lock_guard<std::mutex> lk(m_wal_mutex);
    newdb::WalManager* wm = ensureWal();
    if (!wm) {
        return;
    }
    const uint64_t txn = static_cast<uint64_t>(m_txn_id.load());
    const auto wal_begin_t = std::chrono::steady_clock::now();
    const std::uint64_t db_object_id = static_cast<std::uint64_t>(std::hash<std::string>{}(table));

    if (operation == "TXN_BEGIN") {
        (void)wm->begin_transaction(txn);
        // Advance `current_lsn()` so Snapshot `BEGIN` can pin a non-zero read view on an otherwise
        // empty WAL (engine `begin_transaction` is a no-op for LSN).
        (void)wm->append_record(txn, newdb::WalOp::SESSION_SNAPSHOT, table, nullptr);
        const auto ms = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - wal_begin_t)
                .count());
        onWriteTiming(WriteTimingStage::WalAppend, ms);
        return;
    }
    if (operation == "TXN_COMMIT") {
        (void)wm->commit_transaction(txn);
        const auto ms = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - wal_begin_t)
                .count());
        onWriteTiming(WriteTimingStage::WalAppend, ms);
        return;
    }
    if (operation == "TXN_ROLLBACK") {
        (void)wm->rollback_transaction(txn);
        const auto ms = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - wal_begin_t)
                .count());
        onWriteTiming(WriteTimingStage::WalAppend, ms);
        return;
    }
    if (operation == "RECOVERY_DONE") {
        (void)wm->append_record(txn, newdb::WalOp::SESSION_SNAPSHOT, table, nullptr);
        const auto ms = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - wal_begin_t)
                .count());
        onWriteTiming(WriteTimingStage::WalAppend, ms);
        return;
    }
    if (operation == "SAVEPOINT_SET") {
        // savepoint_id: stable hash of savepoint name
        (void)wm->append_record(txn, newdb::WalOp::SAVEPOINT_SET, table, nullptr, nullptr, nullptr, nullptr,
                                /*db_object_id=*/db_object_id,
                                /*savepoint_id=*/static_cast<std::uint64_t>(std::hash<std::string>{}(key)),
                                /*undo_prev_lsn=*/m_last_undo_lsn,
                                /*pitr_target_lsn=*/0, /*pitr_target_ts_ms=*/0,
                                /*op_seq_in_txn=*/0);
        return;
    }
    if (operation == "SAVEPOINT_ROLLBACK") {
        (void)wm->append_record(txn, newdb::WalOp::SAVEPOINT_ROLLBACK, table, nullptr, nullptr, nullptr, nullptr,
                                db_object_id, static_cast<std::uint64_t>(std::hash<std::string>{}(key)), 0, 0, 0, 0);
        return;
    }
    if (operation == "TXN_ABORT_PARTIAL") {
        std::uint64_t cutoff = 0;
        try { cutoff = static_cast<std::uint64_t>(std::stoull(key)); } catch (...) {}
        (void)wm->append_record(txn, newdb::WalOp::TXN_ABORT_PARTIAL, table, nullptr, nullptr, nullptr, nullptr,
                                db_object_id, 0, 0, cutoff, 0, 0);
        return;
    }
    if (operation == "PITR_MARK") {
        std::uint64_t lsn_target = 0;
        std::uint64_t ts_target = 0;
        try { lsn_target = static_cast<std::uint64_t>(std::stoull(key)); } catch (...) {}
        try { ts_target = static_cast<std::uint64_t>(std::stoull(new_val)); } catch (...) {}
        (void)wm->append_record(txn, newdb::WalOp::PITR_MARK, table, nullptr, nullptr, nullptr, nullptr,
                                db_object_id, 0, 0, lsn_target, ts_target, 0);
        return;
    }

    newdb::WalOp op = newdb::WalOp::UPDATE;
    if (operation == "INSERT") {
        op = newdb::WalOp::INSERT;
    } else if (operation == "DELETE") {
        op = newdb::WalOp::DELETE;
    }

    newdb::Row row;
    try {
        row.id = std::stoi(key);
    } catch (...) {
        row.id = 0;
    }
    auto parse_attrs = [](const std::string& packed) {
        std::map<std::string, std::string> attrs;
        std::size_t i = 0;
        while (i < packed.size()) {
            const std::size_t sep = packed.find('=', i);
            if (sep == std::string::npos) break;
            const std::size_t end = packed.find(';', sep + 1);
            const std::string k = packed.substr(i, sep - i);
            const std::string v = (end == std::string::npos)
                                      ? packed.substr(sep + 1)
                                      : packed.substr(sep + 1, end - (sep + 1));
            if (!k.empty()) attrs[k] = v;
            if (end == std::string::npos) break;
            i = end + 1;
        }
        return attrs;
    };

    newdb::Row before;
    newdb::Row after;
    before.id = row.id;
    after.id = row.id;
    for (const auto& kv : parse_attrs(old_val)) {
        if (kv.first != "__deleted") before.attrs[kv.first] = kv.second;
    }
    for (const auto& kv : parse_attrs(new_val)) {
        if (kv.first != "__deleted") after.attrs[kv.first] = kv.second;
    }
    row.attrs["__wal_old"] = old_val; // legacy recovery compatibility
    row.attrs["__wal_new"] = new_val; // legacy recovery compatibility
    row.attrs["__wal_op"] = operation; // legacy recovery compatibility
    const std::uint64_t op_seq = m_txn_op_seq.load(std::memory_order_relaxed);
    const newdb::Row* before_ptr = nullptr;
    const newdb::Row* after_ptr = nullptr;
    if (op == newdb::WalOp::INSERT) {
        after_ptr = &after;
    } else if (op == newdb::WalOp::DELETE) {
        before_ptr = &before;
    } else {
        before_ptr = &before;
        after_ptr = &after;
    }
    std::uint64_t wal_lsn = 0;
    const std::uint64_t undo_prev = m_last_undo_lsn;
    (void)wm->append_record_with_lsn(txn, op, table, &row, nullptr, before_ptr, after_ptr,
                                    /*db_object_id=*/db_object_id, /*savepoint_id=*/0,
                                    /*undo_prev_lsn=*/undo_prev,
                                    /*pitr_target_lsn=*/0, /*pitr_target_ts_ms=*/0,
                                    op_seq, &wal_lsn);
    if (wal_lsn != 0) {
        m_last_undo_lsn = wal_lsn;
        // Best-effort: also annotate the last in-memory record with its WAL LSN.
        if (!m_txn_records.empty()) {
            m_txn_records.back().wal_lsn = wal_lsn;
        }
    }
    const auto ms = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - wal_begin_t).count());
    onWriteTiming(WriteTimingStage::WalAppend, ms);
}
```


**`flushWAL`**


**对应源码**（`newdb/cli/modules/txn/coordinator/wal/wal_service.cc` 357–433 行）

```357:433:newdb/cli/modules/txn/coordinator/wal/wal_service.cc
void TxnCoordinator::flushWAL() {
    std::lock_guard<std::mutex> lk(m_wal_mutex);
    newdb::WalManager* wm = ensureWal();
    if (!wm) {
        return;
    }
    // Track WAL growth as a coarse throughput proxy.
    const std::uint64_t wal_bytes = wm->wal_file_size_bytes();
    const std::uint64_t last = m_last_wal_bytes.exchange(wal_bytes, std::memory_order_relaxed);
    if (wal_bytes >= last) {
        m_wal_bytes_since_start.fetch_add(wal_bytes - last, std::memory_order_relaxed);
    }
    const auto pending = m_wal_group_commit_pending_commits.load(std::memory_order_relaxed);
    const auto window_ms = m_group_commit_window_ms.load(std::memory_order_relaxed);
    const auto max_batch = std::max<std::uint64_t>(1, m_group_commit_max_batch_commits.load(std::memory_order_relaxed));
    const auto now_ms = now_ms_steady();
    if (pending > 0 && window_ms > 0 && pending < max_batch) {
        const auto last_flush_ms = m_last_wal_flush_ms.load(std::memory_order_relaxed);
        if (last_flush_ms > 0 && (now_ms - last_flush_ms) < window_ms) {
            return;
        }
    }
    if (m_hybrid_adaptive_enabled.load(std::memory_order_relaxed)) {
        std::uint64_t queue_depth = m_vacuum_queue_depth.load(std::memory_order_relaxed);
        std::uint64_t recovery_tail = m_wal_recovery_last_elapsed_ms.load(std::memory_order_relaxed);
        std::uint64_t lock_tail = m_lock_wait_max_ms.load(std::memory_order_relaxed);
        (void)read_u64_env("NEWDB_HYBRID_TEST_QUEUE_DEPTH", &queue_depth);
        (void)read_u64_env("NEWDB_HYBRID_TEST_RECOVERY_TAIL_MS", &recovery_tail);
        (void)read_u64_env("NEWDB_HYBRID_TEST_LOCK_TAIL_MS", &lock_tail);
        const auto now_ms = now_ms_steady();
        const auto last_switch = m_hybrid_last_switch_ms.load(std::memory_order_relaxed);
        std::uint64_t min_dwell_ms = 5000;
        (void)read_u64_env("NEWDB_HYBRID_MIN_DWELL_MS", &min_dwell_ms);
        const bool can_switch = (last_switch == 0) || (now_ms >= last_switch && (now_ms - last_switch) >= min_dwell_ms);
        const bool need_durability = (recovery_tail > 600 || lock_tail > 100);
        const bool need_throughput = (queue_depth > 4 && !need_durability);
        const std::uint8_t cur_mode = m_hybrid_mode.load(std::memory_order_relaxed);
        std::uint8_t next_mode = cur_mode;
        std::string reason;
        if (need_durability) {
            next_mode = 1;
            reason = "recovery_or_lock_tail";
        } else if (need_throughput) {
            next_mode = 0;
            reason = "queue_backpressure";
        }
        if (can_switch && next_mode != cur_mode) {
            m_hybrid_mode.store(next_mode, std::memory_order_relaxed);
            m_hybrid_mode_switch_count.fetch_add(1, std::memory_order_relaxed);
            m_hybrid_last_switch_ms.store(now_ms, std::memory_order_relaxed);
            {
                std::lock_guard<std::mutex> lk(m_hybrid_mu);
                m_hybrid_last_switch_reason = reason;
            }
        }
        if (m_hybrid_mode.load(std::memory_order_relaxed) == 1) {
            wm->set_sync_mode(newdb::WalSyncMode::Full);
            m_group_commit_window_ms.store(0, std::memory_order_relaxed);
            m_group_commit_max_batch_commits.store(1, std::memory_order_relaxed);
        } else {
            wm->set_sync_mode(newdb::WalSyncMode::Normal);
            wm->set_normal_sync_interval_ms(20);
        }
    } else if (m_wal_adaptive_enabled.load(std::memory_order_relaxed) &&
               m_vacuum_queue_depth.load(std::memory_order_relaxed) > 4) {
        wm->set_sync_mode(newdb::WalSyncMode::Normal);
        wm->set_normal_sync_interval_ms(20);
    }
    (void)wm->flush();
    persistWalsnHighWaterUnlocked(wm);
    if (pending > 0) {
        m_wal_group_commit_count.fetch_add(1, std::memory_order_relaxed);
        m_wal_group_commit_batch_commits.fetch_add(pending, std::memory_order_relaxed);
        m_wal_group_commit_pending_commits.store(0, std::memory_order_relaxed);
    }
    m_last_wal_flush_ms.store(now_ms, std::memory_order_relaxed);
}
```
### 11.8 `TxnCoordinator::recoverFromWAL`（协调器堆恢复）

**输入环境**：`NEWDB_RECOVER_TARGET_LSN`、`NEWDB_RECOVER_TARGET_TS_MS` 可截断重放窗口；后者通过扫描带 `record_ts_ms` 的记录换算为 LSN cutoff。

**编排入口**：[`recovery_service.cc`](e:/db/DB/newdb/cli/modules/txn/coordinator/recovery/recovery_service.cc) 调用 `WalManager::read_all_records`、环境变量解析，再依次调用各子模块。

**模块拆分**（`newdb/cli/modules/txn/coordinator/recovery/`）：

- **`recovery_analyze`**：对解码后的 `WalDecodedRecord` 做纯分类，输出 `committed_by_txn` / `dangling_by_txn`、checkpoint 计数；每条 DML 转为 `TxnWalOp`（含 `undo_prev_lsn` / `before_row` / `after_row`）。
- **`recovery_redo`**：已提交事务按表聚合，表内按 **`record_lsn` 升序、`op_seq` 升序** redo；`NEWDB_REDO_IDEMPOTENT_MODE=strict` 或 InnoDB 风格 profile 时用 per-object 最大 LSN 幂等，否则 `redo_guard`。
- **`recovery_undo`**：悬挂事务 **per-txn** undo。从该事务 **最大 `record_lsn`（同 LSN 则取更大 `op_seq`）** 起沿 **`undo_prev_lsn`** 回溯；断链、环或无法覆盖全部操作时 **fallback** 为 **`record_lsn` 降序、`op_seq` 降序**（与旧版单事务内全局逆序一致）。若同一 `record_lsn` 出现多条记录（异常/遗留），解析 `undo_prev_lsn` 时在相同 LSN 的候选中取 **`op_seq` 严格小于当前记录** 的最大者作为前驱；若无则取该 LSN 下 **`op_seq` 最大** 的一条（兜底）。
- **`heap_undo_apply`**：与运行时 `rollback` / savepoint 共用的单行堆撤销语义。
- **`recovery_finalize`**：对每个悬挂 `txn_id` 设置 `m_txn_id` 后写 `TXN_ROLLBACK`，再 `RECOVERY_DONE` 与 `flushWAL`（回调注入）。

**多悬挂事务**：各事务按 **该事务内最大 LSN** 降序依次处理；与历史上「按表合并后全局 LSN 序」在交叉写多事务时可能不同（未提供 legacy 环境开关时以当前行为为准）。

**指标**：`wal_recovery_undo_chain_fallback_txns` 为本次恢复中 **至少触发过一次 undo 链 fallback 的悬挂事务个数**。

### 11.9 `TxnCoordinator::tryReserveWriteLockKey`（`write_conflict_service.cc`）

**前置**：非 `Active` 事务或空表名直接 `return true`（不预留）。

**循环**：

- 在全局互斥 **`g_write_intent_mu`** 下查 `g_write_intent_owner[key]`：若无人占用或占用者就是当前 `txn`，则登记 owner、本事务 `m_reserved_write_keys` 插入 key（首次 range/predicate 成功则 bump 对应计数），并累计等待时间到 **`lock_wait_*`**，**`return true`**。
- 否则登记 **`g_txn_wait_for_owner[waiter]=holder`**，并调用 **`detect_wait_cycle`**：若成环则选当前事务为 victim，bump 死锁与写冲突计数、写 `reason`、`return false`。
- **`Reject` 策略**：立即失败。  
- **`Wait` 策略**：在超时前 sleep/backoff；超时则 `write_conflict_wait_timeout_count++` 并失败。



`tryReserveWriteKey` 即 **`tryReserveWriteLockKey(LockKey::row_pk_write_intent(...))`** 的薄封装。



**对应源码**（`newdb/cli/modules/txn/coordinator/write_conflict/write_conflict_service.cc` 48–182 行）

```48:182:newdb/cli/modules/txn/coordinator/write_conflict/write_conflict_service.cc
bool TxnCoordinator::tryReserveWriteKey(const std::string& table_name, const int id, std::string* reason) {
    if (table_name.empty()) {
        return true;
    }
    return tryReserveWriteLockKey(LockKey::row_pk_write_intent(table_name, id), reason);
}

bool TxnCoordinator::tryReserveWriteLockKey(const LockKey& lk, std::string* reason) {
    if (m_state.load() != TxnState::Active) {
        return true;
    }
    if (lk.table.empty()) {
        return true;
    }
    const std::uint64_t txn = static_cast<std::uint64_t>(m_txn_id.load());
    const std::string key = lk.to_storage_key();
    const WriteConflictPolicy policy = m_write_conflict_policy.load(std::memory_order_relaxed);
    const std::uint64_t wait_timeout_ms = m_write_conflict_wait_timeout_ms.load(std::memory_order_relaxed);
    const auto wait_start = std::chrono::steady_clock::now();
    bool deadlock_reported = false;
    unsigned wait_backoff_round = 0;

    for (;;) {
        {
            std::lock_guard<std::mutex> lk_mu(g_write_intent_mu);
            const auto it = g_write_intent_owner.find(key);
            if (it == g_write_intent_owner.end() || it->second == txn) {
                g_write_intent_owner[key] = txn;
                g_txn_wait_for_owner.erase(txn);
                const auto ins = m_reserved_write_keys.insert(key);
                if (ins.second) {
                    if (lk.kind == LockKeyKind::RangeWriteIntent) {
                        m_lock_key_range_count.fetch_add(1, std::memory_order_relaxed);
                    } else if (lk.kind == LockKeyKind::PredicateWriteIntent) {
                        m_lock_key_predicate_count.fetch_add(1, std::memory_order_relaxed);
                    }
                }
                const auto waited_ms = static_cast<std::uint64_t>(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - wait_start)
                        .count());
                if (waited_ms > 0) {
                    m_lock_wait_ms_total.fetch_add(waited_ms, std::memory_order_relaxed);
                    {
                        std::lock_guard<std::mutex> lk_samples(m_samples_mu);
                        m_lock_wait_ms_samples.push_back(waited_ms);
                        if (m_lock_wait_ms_samples.size() > 256) {
                            m_lock_wait_ms_samples.erase(m_lock_wait_ms_samples.begin(),
                                                         m_lock_wait_ms_samples.begin() + 64);
                        }
                    }
                    std::uint64_t old_max = m_lock_wait_max_ms.load(std::memory_order_relaxed);
                    while (waited_ms > old_max &&
                           !m_lock_wait_max_ms.compare_exchange_weak(
                               old_max, waited_ms, std::memory_order_relaxed, std::memory_order_relaxed)) {
                    }
                }
                return true;
            }
            g_txn_wait_for_owner[txn] = it->second;
            if (!deadlock_reported) {
                std::uint64_t cycle_owner = 0;
                if (detect_wait_cycle(txn, cycle_owner)) {
                    m_lock_deadlock_detect_count.fetch_add(1, std::memory_order_relaxed);
                    m_lock_deadlock_victim_count.fetch_add(1, std::memory_order_relaxed);
                    deadlock_reported = true;
                    m_write_conflict_count.fetch_add(1, std::memory_order_relaxed);
                    recordWriteConflictSampleLockKey(lk, cycle_owner, "deadlock_victim");
                    g_txn_wait_for_owner.erase(txn);
                    if (reason != nullptr) {
                        *reason = "deadlock detected on " + key + ", current txn chosen as victim";
                    }
                    return false;
                }
            }
        }
        if (policy != WriteConflictPolicy::Wait || wait_timeout_ms == 0) {
            m_write_conflict_count.fetch_add(1, std::memory_order_relaxed);
            std::uint64_t holder = 0;
            {
                std::lock_guard<std::mutex> lk_mu(g_write_intent_mu);
                const auto hi = g_write_intent_owner.find(key);
                if (hi != g_write_intent_owner.end()) {
                    holder = hi->second;
                }
                g_txn_wait_for_owner.erase(txn);
            }
            recordWriteConflictSampleLockKey(lk, holder, "reject");
            if (reason != nullptr) {
                *reason = "write conflict on " + key + " (held by another active transaction)";
            }
            return false;
        }
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - wait_start);
        if (elapsed.count() >= static_cast<long long>(wait_timeout_ms)) {
            m_write_conflict_count.fetch_add(1, std::memory_order_relaxed);
            m_write_conflict_wait_timeout_count.fetch_add(1, std::memory_order_relaxed);
            m_lock_wait_ms_total.fetch_add(static_cast<std::uint64_t>(elapsed.count()), std::memory_order_relaxed);
            std::uint64_t wait_ms = static_cast<std::uint64_t>(elapsed.count());
            {
                std::lock_guard<std::mutex> lk_samples(m_samples_mu);
                m_lock_wait_ms_samples.push_back(wait_ms);
                if (m_lock_wait_ms_samples.size() > 256) {
                    m_lock_wait_ms_samples.erase(m_lock_wait_ms_samples.begin(),
                                                 m_lock_wait_ms_samples.begin() + 64);
                }
            }
            std::uint64_t old_max = m_lock_wait_max_ms.load(std::memory_order_relaxed);
            while (wait_ms > old_max &&
                   !m_lock_wait_max_ms.compare_exchange_weak(
                       old_max, wait_ms, std::memory_order_relaxed, std::memory_order_relaxed)) {
            }
            std::uint64_t holder = 0;
            {
                std::lock_guard<std::mutex> lk_mu(g_write_intent_mu);
                const auto hi = g_write_intent_owner.find(key);
                if (hi != g_write_intent_owner.end()) {
                    holder = hi->second;
                }
                g_txn_wait_for_owner.erase(txn);
            }
            recordWriteConflictSampleLockKey(lk, holder, "wait_timeout");
            if (reason != nullptr) {
                *reason = "write conflict wait timeout on " + key;
            }
            return false;
        }
        m_write_conflict_wait_count.fetch_add(1, std::memory_order_relaxed);
        const unsigned ms =
            (std::min)(128u, 1u << (std::min)(wait_backoff_round, static_cast<unsigned>(7)));
        wait_backoff_round = (std::min)(wait_backoff_round + 1u, 24u);
        std::this_thread::sleep_for(std::chrono::milliseconds(ms == 0u ? 1u : ms));
    }
}
```
### 11.10 `TxnCoordinator::syncHeapReadSnapshotForQuery`（`wal_service.cc`）



1. 环境变量 **`NEWDB_TXN_ISOLATION_READPATH`** 若为关闭语义，则 **`table.clear_snapshot()`**、`txn_readpath_disabled_count++`，发布 LSN 0 并返回。
2. 持 **`m_wal_mutex`** 取 `wal_`；无 WAL 则清快照返回。
3. 默认 **`lsn = wm->current_lsn()`**；**`Snapshot` 隔离**且事务 `Active` 且 **`m_txn_read_view_lsn != 0`**：使用固定 **`lsn = m_txn_read_view_lsn`**（事务钉扎），`txn_snapshot_pinned_count++`；否则视为语句级刷新，`txn_snapshot_refresh_count++`，`stmt_lsn = lsn`。
4. **`ReadCommitted`**：总是语句级刷新分支。
5. 构造 **`MVCCSnapshot{ snapshot_lsn = lsn, min_visible_lsn = 0 }`**，**`table.set_snapshot(snap)`**；可选 **`NEWDB_TXN_TRACE`** 打 stderr。



**对应源码**（`newdb/cli/modules/txn/coordinator/wal/wal_service.cc` 556–625 行）

```556:625:newdb/cli/modules/txn/coordinator/wal/wal_service.cc
void TxnCoordinator::syncHeapReadSnapshotForQuery(newdb::HeapTable& table) {
    auto set_source = [this](const std::uint8_t code) {
        m_last_snapshot_source_code.store(code, std::memory_order_relaxed);
    };
    auto publish_snapshot_lsns = [this](std::uint64_t txn_lsn, std::uint64_t stmt_lsn) {
        m_last_transaction_snapshot_lsn.store(txn_lsn, std::memory_order_relaxed);
        m_last_statement_snapshot_lsn.store(stmt_lsn, std::memory_order_relaxed);
    };
    if (const char* opt = std::getenv("NEWDB_TXN_ISOLATION_READPATH")) {
        std::string v = opt;
        for (auto& ch : v) {
            ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        }
        if (v == "0" || v == "off" || v == "false" || v == "no") {
            table.clear_snapshot();
            m_txn_readpath_disabled_count.fetch_add(1, std::memory_order_relaxed);
            set_source(3);
            publish_snapshot_lsns(0, 0);
            return;
        }
    }
    std::lock_guard<std::mutex> lk(m_wal_mutex);
    newdb::WalManager* wm = wal_.get();
    if (wm == nullptr) {
        table.clear_snapshot();
        set_source(0);
        publish_snapshot_lsns(0, 0);
        return;
    }
    std::uint64_t lsn = wm->current_lsn();
    std::uint64_t txn_lsn = 0;
    std::uint64_t stmt_lsn = 0;
    if (txnIsolationLevel() == TxnIsolationLevel::Snapshot) {
        const std::uint64_t fixed = m_txn_read_view_lsn.load(std::memory_order_relaxed);
        if (getState() == TxnState::Active && fixed != 0) {
            lsn = fixed;
            m_txn_snapshot_pinned_count.fetch_add(1, std::memory_order_relaxed);
            set_source(1);
            txn_lsn = lsn;
        } else {
            m_txn_snapshot_refresh_count.fetch_add(1, std::memory_order_relaxed);
            set_source(2);
            stmt_lsn = lsn;
        }
    } else {
        m_txn_snapshot_refresh_count.fetch_add(1, std::memory_order_relaxed);
        set_source(2);
        stmt_lsn = lsn;
    }
    publish_snapshot_lsns(txn_lsn, stmt_lsn);
    newdb::MVCCSnapshot snap;
    snap.snapshot_lsn = lsn;
    snap.min_visible_lsn = 0;
    table.set_snapshot(snap);
    if (const char* tr = std::getenv("NEWDB_TXN_TRACE")) {
        std::string tv = tr;
        for (auto& ch : tv) {
            ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        }
        if (tv == "1" || tv == "on" || tv == "true" || tv == "yes") {
            const char* iso =
                txnIsolationLevel() == TxnIsolationLevel::ReadCommitted ? "read_committed" : "snapshot";
            std::fprintf(stderr,
                         "[TXN_TRACE] sync_read_view iso=%s snapshot_lsn=%llu in_txn=%d\n",
                         iso,
                         static_cast<unsigned long long>(lsn),
                         getState() == TxnState::Active ? 1 : 0);
        }
    }
}
```
### 11.11 Sidecar 失效入口（`sidecar_invalidate_service.cc`）

**`invalidate_eq_sidecars_after_write(eff_data)`**：调用 **`sidecar_invalidate_all_indexes_for_data_file(eff_data)`**（定义在 `index_catalog` / sidecar 公共逻辑侧）。



**重载**：带 `std::set<std::string>& attrs` 时把变更属性集传入，以便做**更细粒度**失效（若底层实现支持）。



**对应源码**（`newdb/cli/shell/dispatch/services/sidecar/sidecar_invalidate_service.cc` 1–16 行）

```1:16:newdb/cli/shell/dispatch/services/sidecar/sidecar_invalidate_service.cc
#include <waterfall/config.h>

#include <set>
#include <string>

#include "cli/modules/sidecar/common/index_catalog.h"

void invalidate_eq_sidecars_after_write(const std::string& eff_data) {
    sidecar_invalidate_all_indexes_for_data_file(eff_data);
}

void invalidate_eq_sidecars_after_write(const std::string& eff_data,
                                        const std::set<std::string>& attrs) {
    sidecar_invalidate_all_indexes_for_data_file(eff_data, attrs);
}
```
### 11.12 `TxnCoordinator::rollback`（`core_impl.cc`）

在 **`m_txn_mutex`** 下，非 `Active` 直接 Err。实现顺序（见下附源码）：

1. 定义 **`parse_attrs`** / **`append_undo_record`**：`INSERT` 撤销为 **`append_row` 墓碑**（`__deleted=1`）；`UPDATE`/`DELETE` 将 **`old_value`** 解包为属性映射再 **`append_row`**。
2. **`m_txn_records` 逆序**逐个 `append_undo_record`（先在堆上抵消未提交变更）。
3. **`writeWAL("TXN_ROLLBACK", ...)`** 与 **`flushWAL()`**。
4. **释放文件锁**、`clearWriteIntents`、清空 records / active_table，**`m_txn_read_view_lsn = 0`**，状态 **`RolledBack`**。

与 **`recoverFromWAL`** 对悬挂事务的 **undo** 一样，靠向堆文件**追加新物理行**表达撤销，而非原地改写 WAL。

**对应源码**（`newdb/cli/modules/txn/coordinator/core/core_impl.cc` 235–319 行）

```235:319:newdb/cli/modules/txn/coordinator/core/core_impl.cc
Result<bool> TxnCoordinator::rollback() {
    std::lock_guard<std::mutex> lk(m_txn_mutex);
    
    if (m_state.load() != TxnState::Active) {
        return Result<bool>::Err("??????");
    }
    
    auto parse_attrs = [](const std::string& packed) {
        std::map<std::string, std::string> attrs;
        std::size_t i = 0;
        while (i < packed.size()) {
            const std::size_t sep = packed.find('=', i);
            if (sep == std::string::npos) {
                break;
            }
            const std::size_t end = packed.find(';', sep + 1);
            const std::string key = packed.substr(i, sep - i);
            const std::string val =
                (end == std::string::npos) ? packed.substr(sep + 1) : packed.substr(sep + 1, end - (sep + 1));
            if (!key.empty()) {
                attrs[key] = val;
            }
            if (end == std::string::npos) {
                break;
            }
            i = end + 1;
        }
        return attrs;
    };

    auto append_undo_record = [&](const TxnRecord& rec) {
        const std::string data_file = resolveDataFilePath(rec.table_name);
        try {
            const int id = std::stoi(rec.key);
            if (rec.operation == "INSERT") {
                newdb::Row tomb;
                tomb.id = id;
                tomb.attrs["__deleted"] = "1";
                (void)newdb::io::append_row(data_file.c_str(), tomb);
                return;
            }
            newdb::Row row;
            row.id = id;
            const auto attrs = parse_attrs(rec.old_value);
            for (const auto& kv : attrs) {
                if (kv.first != "__deleted") {
                    row.attrs[kv.first] = kv.second;
                }
            }
            (void)newdb::io::append_row(data_file.c_str(), row);
        } catch (...) {
            // ignore malformed undo records
        }
    };

    // ??????????????
    for (auto it = m_txn_records.rbegin(); it != m_txn_records.rend(); ++it) {
        append_undo_record(*it);
    }
    
    writeWAL("TXN_ROLLBACK", m_active_table, "", "", "");
    flushWAL();
    
    std::vector<std::string> locked_copy;
    {
        std::lock_guard<std::mutex> lk2(m_lock_mutex);
        locked_copy = m_locked_files;
    }
    for (const auto& f : locked_copy) {
        (void)releaseLock(f);
    }
    
    m_state.store(TxnState::RolledBack);
    m_txn_op_seq.store(0, std::memory_order_relaxed);
    m_savepoints.clear();
    m_savepoints_lsn.clear();
    m_last_undo_lsn = 0;
    clearWriteIntents();
    m_txn_records.clear();
    m_locked_files.clear();
    m_active_table.clear();
    m_txn_read_view_lsn.store(0, std::memory_order_relaxed);

    return Result<bool>::Ok(true);
}
```

## 12. 与原文章节索引

| 主题 | 原文 |
|------|------|
| 顶层树与 Mermaid | §1–§4 |
| 持久化、WAL 恢复、墓碑、MemoryRegistry | §5 |
| WHERE 逻辑图 | §6 |
| 观测 CI | §7 |
| 测试 | §8–§9 |
| 主数据平面 | §10 |
| handler 微观顺序与写/WHERE 子图 | §11 |
| 类型耦合全文 + 源码行号节选 | §12、§12.11 |
| **字段/形参字典** | **§12.12** |
| 维护说明 | §13 |

---

*本手册随 `newdb/` 目录树演进；若移动 handler 或重命名统计字段，请同步更新 §3.1–§3.2 与 §7.2 中的标识符；**关键方法实现**见上文 **§11**，并以原文 §12.11/§12.12 为准校对。*
