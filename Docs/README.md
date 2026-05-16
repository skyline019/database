# StructDB 文档

本目录收录 **工程方针**、**架构总览**、**版本变更记录**与各期专文；与仓库根 `CMakeLists.txt` 中的 `project(StructDB VERSION …)` 对应。**里程碑专文** 位于子目录 **[phases/](phases/)**（路径形如 `phases/PHASE23.md`）。

| 文档 | 说明 |
|------|------|
| **[../intro/README.md](../intro/README.md)** | **LaTeX 源码级手册**（`Engine`/`StorageEngine`/MDB/参数表/测试矩阵）；**WSL 仅编译 PDF**；工程构建见根 `README`；细节仍以 `POLICY` 与各 `PHASE*.md` 为准 |
| **[ARCHITECTURE.md](ARCHITECTURE.md)** | **总览**：端到端数据流（Mermaid）、代码目录与依赖、关键类型与 API 摘录（附源码行引用）；与 `POLICY` / 各 PHASE 互补 |
| **[OPTIMIZATION_PLAN.md](OPTIMIZATION_PLAN.md)** | **性能路线图**：里程碑快照、存储/MDB/GUI 已落地项与 backlog（与代码同步维护） |
| **[ENGINE_RUNTIME_CONFIG.md](ENGINE_RUNTIME_CONFIG.md)** | **配置清单**：运行时 / 启动期 / 实验开关与 `EngineConfigSnapshot` 锚点；与调度器压力、`Orchestrator` 前置 hook 交叉引用 |
| **[NEXT_REFACTOR_RECOMMENDATIONS.md](NEXT_REFACTOR_RECOMMENDATIONS.md)** | **重构台账**：`StorageEngine` 拆分与 §13 落实进度 |
| **[STORAGE_EVOLUTION_AND_OBSERVABILITY.md](STORAGE_EVOLUTION_AND_OBSERVABILITY.md)** | **存储演进收口**：MemTable 默认与远期 arena、`CompactionResult`、恢复策略与 Embed 编排索引、`stdb.storage.*` / bench 命名与压力 JSON 策略 |
| **[OS_IO_ISOLATION.md](OS_IO_ISOLATION.md)** | **OS 级 I/O**：compaction 与 WAL 分卷/cgroup/ionice 等运维清单；与 `WalPipeline` / 专用 `CompactionIoExecutor` 的关系 |
| [POLICY.md](POLICY.md) | 架构约束、构建与测试策略、平台与第三方依赖、贡献约定；**§4.0 文件系统保底**、**§3.3.1（九/十一/十二期 compaction）**、**§3.5（十期 undo 前缀 / v2 槽）**、**§4.4–4.5（七期 InnoDB 映射与耐久类比）**、**§4.5 后耐久矩阵（二十四期）**、**[`PHASE31.md`](phases/PHASE31.md)**、**[`PHASE34.md`](phases/PHASE34.md)**（多 TU 锚点与 `*Phase31*` filter） |
| [ONBOARDING.md](ONBOARDING.md) | 新人阅读顺序（`POLICY` §4 → `TXN_INNODB_MAP` §2 → `WAL_REPLAY` → `TESTING_TXN_CHAIN` → **`ARCHITECTURE`**） |
| [CHANGELOG.md](CHANGELOG.md) | 面向使用者的版本变更（建议每次发版或合并里程碑时更新） |
| [COMPACTION.md](COMPACTION.md) | 九期：`L0` 双文件合并、`MANIFEST`/checkpoint 顺序、`compaction_merge_count`；十期：写 checkpoint 时刷新 undo 水位（§4）；与 [phases/PHASE11.md](phases/PHASE11.md)、[phases/PHASE12.md](phases/PHASE12.md)、[phases/PHASE22.md](phases/PHASE22.md)、[phases/PHASE23.md](phases/PHASE23.md) 交叉引用 |
| [phases/WAL_REPLAY.md](phases/WAL_REPLAY.md) | **二十四期**：`wal.log` 重放判别（`STDBBW1` / 文本行）、与 checkpoint 权威、混排（链入 `POLICY` §3.1） |
| [phases/TESTING_TXN_CHAIN.md](phases/TESTING_TXN_CHAIN.md) | 六期～**三十八期**：事务链读路径审计与 GTest 切片（与 `POLICY` §4.1、§4.5、`UNDO_LOG_4C`、`PHASE31`、**`*Phase36*` / `*Phase37*` / `*Phase38*`** 对照） |
| [phases/TXN_INNODB_MAP.md](phases/TXN_INNODB_MAP.md) | 七期：InnoDB ↔ StructDB **非等价**概念表、MTR 类比、耐久 Level 0/1/2 映射 |
| [phases/TXN_BEGIN_PERSIST_DESIGN.md](phases/TXN_BEGIN_PERSIST_DESIGN.md) | 七期 7C（条件）：`BEGIN` 内 `persist_table` / 6D 设计草案与特性门闩 |
| [phases/UNDO_LOG_4C.md](phases/UNDO_LOG_4C.md) | 八期 / 四期 4C：`undo.log` 截断安全条件、WAL trim 与 `RebuildUndoStackFromLog` 矩阵、API 与配置说明 |
| [phases/PHASE10.md](phases/PHASE10.md) | **十期**：checkpoint **v2**、`undo_log_safe_prefix_bytes` 语义与 `undo_try_truncate_recyclable_prefix` |
| [phases/CHECKPOINT_UNDO_PREFIX.md](phases/CHECKPOINT_UNDO_PREFIX.md) | 九期 9C → **十期**：`undo.log` 与 checkpoint **v2 槽**联合前缀回收 |
| [phases/PHASE11.md](phases/PHASE11.md) | **十一期**：`l0_compact_trigger_threshold` / `flush_memtable` 后同步多轮 L0 合并 |
| [phases/PHASE12.md](phases/PHASE12.md) | **十二期**：`MANIFEST` `FORMAT2`、读路径 L0/L1 顺序、`l1_compact_output_from_l0_merge` |
| [phases/PHASE17.md](phases/PHASE17.md) | **十七期**：`BEGIN` 内 `persist_table`（**`mdb_persist_in_begin`**，默认开）与 `ROLLBACK` 边界 |
| [phases/PHASE19.md](phases/PHASE19.md) | **十九期**：GraphExecutor 注册 `drain_l0_compaction`、defer 时默认计划 `noop→drain`、`Engine::rerun_default_pipeline` |
| [phases/PHASE20.md](phases/PHASE20.md) | **二十期**：多段 WAL（`wal.segments` v2）、可选 compaction worker、IOCP / 可选 io_uring |
| [phases/PHASE21.md](phases/PHASE21.md) | **二十一期**：WAL 封存 GC、io_uring WalWriter、compaction 背压深化（`PHASE13_PLUS_PLAN` §11） |
| [phases/PHASE22.md](phases/PHASE22.md) | **二十二期**：L3 compaction、GraphExecutor 多资源背压探测、`undo` 物理分段（`undo.segments` v2） |
| [phases/PHASE23.md](phases/PHASE23.md) | **二十三期**：flush/compaction 收口、L4+、可选跨层 `ROLLBACK` 与 `undo_stack_` 对齐、I/O 文档矩阵 |
| [phases/PHASE24.md](phases/PHASE24.md) | **二十四期**：单写者 / 绕过 MDB、嵌入式耐久矩阵、`WAL_REPLAY`、`ONBOARDING`、可选 24A 观测/硬闸 |
| [phases/PHASE25.md](phases/PHASE25.md) | **二十五期**：MDB 命令库对标 newdb（语义映射）、`SCAN` 上限与 **`SCAN MORE`/`SCAN RESET`** 游标、`[NOT_SUPPORTED]` 等 |
| [phases/PHASE26.md](phases/PHASE26.md) | **二十六期**：MDB 子模块拆分；`MdbEnginePorts`；`mdb_dispatch.cpp` / `mdb_runner_dispatch.inc` / `mdb_ops_*.cpp`（见 PHASE32） |
| [phases/PHASE27.md](phases/PHASE27.md) | **二十七期**：C 绑定版本/错误码、`structdb_mdb_run_options` 与 `structdb_run_mdb_file_ex` |
| [phases/PHASE28.md](phases/PHASE28.md) | **二十八期**：可选 `structdb_capi_shared`、`structdb_mdb_execute_line_ex`（FFI）；`STRUCTDB_BUILD_CAPI_SHARED` |
| [phases/PHASE29.md](phases/PHASE29.md) | **二十九期**：perf 门禁、`session_log.txt` 与运维索引、MIT |
| [phases/PHASE30.md](phases/PHASE30.md) | **三十期**：`rust_gui` + `structdb_capi_shared`、打包与前端策略 |
| [phases/PHASE31.md](phases/PHASE31.md) | **三十一期**：事务链与存储边界不变式（矩阵、恢复链、GTest 名含 `Phase31` 的子集，推荐 `--gtest_filter=*Phase31*` 或显式前缀行，`TESTING_TXN_CHAIN` §13） |
| [phases/PHASE32.md](phases/PHASE32.md) | **三十二期**：MDB `mdb_ops_*` 多 TU；`storage_engine_detail` + compact/checkpoint 粗拆分（见 **三十三期** 再切） |
| [phases/PHASE33.md](phases/PHASE33.md) | **三十三期**：`storage_engine_open_wal` / `put_undo` / `read` + `compaction_lsm` / `segments_worker_checkpoint` 细 TU；语义不变 |
| [phases/PHASE34.md](phases/PHASE34.md) | **三十四期**：拆分后文档锚点与 GTest 文案固化；权威 `StorageEngine` 多 TU 表；可选 MDB `mdb_ops_*` 再切 |
| [phases/PHASE35.md](phases/PHASE35.md) | **三十五期**：L0 compaction 锁外 I/O、多线程 `submit` 互斥、可选目录建议锁、`structdb_engine_open_ex` |
| [phases/PHASE36.md](phases/PHASE36.md) | **三十六期**：L1+ 与 L0 对齐的两阶段 compaction、读锁 `shared_mutex` 延期结论、可选 Facade `kv_put` 队列与 GUI 独占锁环境变量 |
| [phases/PHASE37.md](phases/PHASE37.md) | **三十七期**：文档与 CHANGELOG 收口、`_tmp_tier_compact_*` 与专文对齐、`*Phase37*` 对称并发回归、三十八期候选划界 |
| [phases/PHASE38.md](phases/PHASE38.md) | **三十八期**：`CONFIRM_REORDER` / `[REORDER_MAP_JSON]`、GUI `id_remap_chain` 多行摄取与撤销栈一致性 |
| [phases/PHASE13_PLUS_PLAN.md](phases/PHASE13_PLUS_PLAN.md) | **计划草案**：十三～三十一期路线划界（含 `p31`→…→`p38`） |

上层设计意图见仓库内 Cursor 计划（文件名含 `structdb_layered_engine`）；**计划文件本身不作为本目录的同步副本**，以本目录与代码为准。
