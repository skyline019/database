# StructDB 工程方针（Engineering Policy）

本文档定义 StructDB 仓库在 **架构、依赖、构建、测试、平台与演进** 上的硬性约定，供贡献者与自动化流程遵循。若与临时讨论冲突，**以本文档与已合并的代码为准**；重大偏离须先修订本文档再改代码。

---

## 1. 目标与范围

StructDB 是一个 **分层运行时 + 混合存储（LSM 思路与页式/redo/undo 子集）+ 嵌入式可持久会话客户端** 的 C++17 工程，以 `Base/waterfall`（`wf::*`）为底座，在 Windows（MSVC）与 MSYS2 MinGW 上可构建为优先目标。

**不在本文档范围内**：`ThirdParty/` 各上游项目的内部政策；其许可证与行为以各自仓库为准。

---

## 2. 分层架构与依赖方向

### 2.1 概念栈（自上而下）

应用与工具 → **Facade**（对外 API、配置、服务容器）→ **Orchestrator**（策略、replan 触发）→ **Planner**（DAG + `plan_epoch`）→ **Scheduler**（拓扑就绪、背压、credits）→ **GraphExecutor**（`IOperator` 管线）→ **Storage / Infra** → **Base (`wf_waterfall`)**。

**背压索引（十四期 / 二十一期 / 二十二期）**：`Engine::storage_pressure_snapshot` / `sync_scheduler_budget_from_storage_pressure` 将存储只读压力映射到 `ResourceBudget`（**WAL 队列**、**CompactionSlots**）；**`GraphExecutor::execute(..., use_budget_probe=true)`** 在节点执行前依次探测 **WalQueueDepth、CompactionSlots、MemTableBytes**，触发 `ExecutionScheduler::callbacks_.on_backpressure`（与 `Orchestrator::set_on_backpressure` 衔接）。配置项见 `EngineConfigSnapshot`（`storage_pressure_*`）。详 [`PHASE21.md`](phases/PHASE21.md) §21C、[`PHASE22.md`](phases/PHASE22.md)。**WAL I/O 后端矩阵**见 **§2.4**。

嵌入式客户端（`client/embed`）**只允许**依赖 Facade 暴露的 API，**禁止**应用或客户端代码直接链接或调用 `engine/storage` 实现细节。

### 2.2 依赖规则（必须遵守）

1. **仅向下依赖**：高层模块可依赖低层；低层不得依赖高层。
2. **禁止** `engine/storage` → `engine/orchestrator` / `engine/planner` / `engine/facade` 等反向依赖。Replan 与策略上行只能通过 **Scheduler 事件** 与 **Facade 注入的策略/回调**，不得从 Storage 直接拉取 Orchestrator。
3. **同层解耦**：同层模块之间通过窄接口、事件或只读快照通信，避免头文件级循环依赖。
4. **Base 边界**：`wf::io` 中与 Linux 强绑定的路径（如 `io_uring`）**不得**作为 StructDB 存储路径的必选依赖；Windows 首版以 **阻塞文件 I/O + 线程池** 打通。可选异步 WAL 后端见 `engine/infra/include/structdb/infra/io_backend.hpp`、**§2.4** 与 **`STRUCTDB_WITH_IOCP` / `STRUCTDB_WITH_IO_URING`**（Linux + liburing，`WalWriter` 顺序写；见 [`PHASE21.md`](phases/PHASE21.md) §21B、[`PHASE20.md`](phases/PHASE20.md) §4）。

### 2.3 公开 API 与实现细节

- 各 `engine/*` 与 `client/embed` 库对外以 **`include/structdb/...` 头文件** 为契约；计划中的 `include/public` 命名若与当前目录不一致，**以 CMake 中 `target_include_directories` 实际导出的路径为准**。
- 实现细节放在 `src/`、`detail` 命名空间或匿名命名空间；不将内部类型暴露到稳定 ABI 头中（除非明确标记为实验性）。

### 2.4 WAL `WalWriter` I/O 后端支持矩阵（二十期 20C / 二十一 21B / 二十三 23D）

| 平台 / 构建 | `IoBackendKind::Blocking`（默认） | `IocpAsync`（`STRUCTDB_WITH_IOCP`） | `IoUringAsync`（`STRUCTDB_WITH_IO_URING` + liburing） |
|-------------|-----------------------------------|-------------------------------------|--------------------------------------------------------|
| **Windows MSVC** | 支持 | **可选 ON**（CMake 默认倾向开启 IOCP 目标时）；与 `WalWriter` 顺序写语义对齐 `PHASE20` 文档 | **不支持**（不链接 liburing；配置须保持 `Blocking` 或 `IocpAsync`） |
| **Windows MinGW** | 支持 | 以 CMake / 编译定义为准；无 IOCP 时回退阻塞 | 不支持 |
| **Linux** | 支持 | 不适用 | **可选 OFF（默认）**；`ON` 时须 `pkg-config liburing` 与 [`PHASE21.md`](phases/PHASE21.md) §21B 条件编译测 |

**非目标**：改变 **`wal.log` 崩溃恢复权威**、帧边界解析规则或 **默认** fsync 策略（异步后端仍须满足 `WalPipeline` / `WalWriter` 文档化的顺序与耐久档位映射；见 [`PHASE20.md`](phases/PHASE20.md)、[`CHANGELOG.md`](CHANGELOG.md)）。

---

## 3. 存储子系统（MVP 边界）

### 3.1 双轨职责

- **LSM 线（LevelDB 思路）**：WAL、MemTable、immutable、manifest / version 视图、L0… 占位与后续 compaction 挂钩；面向高吞吐用户 KV 或行存。
- **页式线（InnoDB 思路）**：与 `wf::storage` 中的 page/segment 等类型对齐；Buffer pool、Redo、Undo、checkpoint 协议；首版可只覆盖 **写路径子集**（如 metadata 页、seal 相关）。
- **崩溃恢复（当前实现）**：面向 embed/引擎重启的 **权威数据源为 `wal.log`**（自 checkpoint 的 `wal_offset` 起重放；不完整尾帧忽略）。`redo.log` 不参与 `StorageEngine::open` 重放；默认亦不再为与 WAL 重复的 put 追加 redo 镜像（避免双写；见 `CHANGELOG`）。**保底文件布局与双根目录**见 **§4.0**。
- **回滚帧与正向批次**：`rollback_one_undo_frame` 在持写锁下向 **`wal.log`** 追加的 **恢复写**（与正向 **`commit_embed_batch` 的 `STDBBW1` 二进制批次**）按 **同一字节时间序** 混排；`open` 重放仍遵循各帧类型的解析规则（见 `CHANGELOG` WAL 帧说明与 **[`WAL_REPLAY.md`](phases/WAL_REPLAY.md)**）。**不得**假设 WAL 仅含单一帧形态而忽略尾帧截断语义。
- **`undo.log` 与进程内栈**：版本化键写路径将上一物理值追加到 `undo.log` 并压入 `undo_stack_`；**默认** `open` **不**从磁盘重建栈（跨重启后栈为空，直至产生新的版本化写）。可选标志位 **`StorageEngine::kOpenFlagRebuildUndoStackFromLog`**：在 WAL 重放完成后 **只读扫描** `undo.log` 按帧顺序重建 `undo_stack_`；**仅当**日志与 WAL 已重放到的状态一致时语义成立，否则可能导致错误回滚；**默认关闭**，供高级场景与测试。
- **MDB `ROLLBACK` 与存储**：当 **`EngineConfigSnapshot::mdb_persist_in_begin`** 为 **true**（默认）且 per-run 开关允许时，`BEGIN` 内成功变更会经 **`persist_table`** 写入存储。若 **`mdb_chain_rollback_on_mdb_rollback`** 为 **false**（默认），**`ROLLBACK` 仍仅**恢复会话侧逻辑表（`current` / `session.txn`），**不**按 MDB 语义链式撤销已落盘写。若 **`mdb_chain_rollback_on_mdb_rollback=true`** 且 **`mdb_persist_in_begin=true`**，MDB `ROLLBACK` 在恢复会话表 **之前** 将 **`undo_stack_` 弹回 `BEGIN` 成功时刻记录的深度**（`Engine::embed_undo_stack_depth()` 水位；受限模型见 **§4.3**）。将 **`mdb_persist_in_begin=false`**（和/或 **`allow_persist_while_txn_active_experimental=false`**）可关闭事务中落盘（链式回滚亦随之不适用）。详见 **§4.3** 与 [`TXN_BEGIN_PERSIST_DESIGN.md`](phases/TXN_BEGIN_PERSIST_DESIGN.md)。**事务链分层与读视图**见 **§4.1**；读路径审计见 [`phases/TESTING_TXN_CHAIN.md`](TESTING_TXN_CHAIN.md)。

### 3.2 粘合策略

首版通过 **「用户数据偏 LSM、系统元数据偏页式」** 控制耦合面；扩展时保持子目录/类职责分离，避免单一大类承担全栈。

### 3.3 空间回收与 WAL 前缀裁剪（四期）

- **前置条件**：`checkpoint` 中的 `wal_offset` 表示 **下一次崩溃重放的起始字节**；此前缀内的记录必须已由 **`flush_memtable`** 落盘到 SST（或等价地已不再需要重放）。**禁止**在仍有未刷盘 MemTable 数据依赖旧 WAL 前缀时裁剪。
- **实现**：`StorageEngine::wal_try_trim_prefix_through_checkpoint` 将 `wal.log` 重写为 `[wal_offset, EOF)` 并把持久化 `wal_offset` 置 **0**；默认不执行。`EngineConfigSnapshot::wal_auto_trim_prefix_after_flush` 为 true 时，在每次成功 **`flush_memtable`** 写 checkpoint 之后自动尝试裁剪。`EngineConfigSnapshot::undo_auto_truncate_after_flush`（默认 false）在成功 **`flush_memtable`** 后截断 **`undo.log`**，与 WAL 裁剪独立；语义见 [`UNDO_LOG_4C.md`](phases/UNDO_LOG_4C.md)。
- **多段 WAL（二十期 / `wal.segments` v2）**：当 `data_dir/wal.segments` **首行格式版本为 2** 且列出已封存段路径时，崩溃恢复权威仍为 **有序 WAL 链**（封存段按元数据顺序 **自字节 0 全量** 重放，再对当前尾文件 **`wal.log`** 自 `checkpoint.wal_offset` 起重放）。`checkpoint.wal_offset` **仅**指向 **`wal.log`**，不指向历史封存文件。`wal_try_trim_prefix_through_checkpoint` **仅**重写当前 **`wal.log`** 尾部。默认 **不**删除 `wal/archive/` 下封存段；**二十一期**起可选 **`EngineConfigSnapshot::wal_archive_gc_after_flush`**（默认 false，且 **必须**与 `wal_auto_trim_prefix_after_flush` 同开）：在成功 `flush_memtable` 且完成 WAL 前缀 trim 后，更新 `wal.segments` v2 并删除已列出的封存文件（不变式见 [`PHASE21.md`](phases/PHASE21.md)）。未启用多段或 v1 元数据时，行为与单文件 **`wal.log`** 一致。
- **undo.log**：**八期子集**：`StorageEngine::undo_try_truncate_when_stack_empty` 与 `EngineConfigSnapshot::undo_auto_truncate_after_flush`（见 [`UNDO_LOG_4C.md`](phases/UNDO_LOG_4C.md)）；**十期**：`undo_try_truncate_recyclable_prefix` 与 checkpoint **v2** 水位（逻辑字节前缀）；**二十二 22C**：可选 **`undo.segments` v2 + `undo/archive/*`** 物理分段（`undo_segment_roll_max_bytes`，须在 `open` 前配置）。**生命周期与 WAL trim 的约束**见 **§3.5**。

### 3.3.1 L0 SST compaction（九期 / 十一期 / 十二期）

- **能力**：`StorageEngine::compact_merge_two_oldest_l0` 将 `MANIFEST` 中 **最旧两个 L0** SST 合并为一个新 SST，更新 `MANIFEST`、删除旧文件、写 **checkpoint**（与 `flush_memtable` 相同的「先 MANIFEST、再 checkpoint」思想）。**十二期**起 `MANIFEST` 可为 **`FORMAT2`**（每条 SST 带 **level**）；合并输出默认仍为 **L0**，可选 **L1**（见 [`PHASE12.md`](phases/PHASE12.md)）。详稿见 [`COMPACTION.md`](COMPACTION.md)。
- **调度**：**九期**起支持 **显式** 调用。**十一期**起支持 **`flush_memtable` 后可选自动多轮合并**：当 `EngineConfigSnapshot::l0_compact_trigger_threshold > 0` 且 **L0 段长度** **大于** 该阈值时，在单次 flush 内至多执行 `l0_compact_max_rounds_per_flush` 次合并（默认 **0=关闭**）。**二十三 23A**：当 **`l0_compact_max_inline_rounds_per_flush > 0`** 且 **`l0_compact_defer_after_flush` 为 false** 时，上述轮数上限进一步收紧为 **`min(l0_compact_max_rounds_per_flush, l0_compact_max_inline_rounds_per_flush)`**，以便与 **二十期 20B** worker / **`drain_pending_l0_compactions`** 的延后排空路径互补（见 [`PHASE23.md`](phases/PHASE23.md)、[`COMPACTION.md`](COMPACTION.md)）。仍 **无** 第二「随意写」线程路径违反 §4.2；**二十二 22B** 起 **`GraphExecutor` 预算探测** 与 Facade 压力注入共同构成背压可观测路径（见 [`PHASE22.md`](phases/PHASE22.md)）。**背压与 L2+、L3→L4、undo/WAL 分段、7C/6D、异步 I/O** 的路线划界见 [`PHASE13_PLUS_PLAN.md`](phases/PHASE13_PLUS_PLAN.md)（**十四～十八期**及 **§12–§13**，计划草案）。
- **观测**：`compaction_merge_count()` 仅作进程内观测。

### 3.4 checkpoint 链与崩溃模型（五期）

- **目标**：在 **不改变「WAL 为崩溃恢复权威」** 的前提下，提高 `checkpoint` 元数据在掉电/崩溃下的 **耐久与可恢复性**（借鉴 InnoDB checkpoint 区 **双槽交替** 思路），并引入单调 **`checkpoint_seq`** 供观测与上层（如 embed session）关联。
- **磁盘布局**（`data_dir` 下）：
  - **`checkpoint.a` / `checkpoint.b`**：固定长度二进制槽（魔数 `STCK`、格式版本、CRC32C 覆盖头部与载荷字段），载荷含 `wal_offset`、`redo_offset`、`manifest_version`、`mdb_catalog_epoch`、`checkpoint_seq`、写入时间戳等；**十期起** v2 槽（68 字节）另含 **`undo_log_safe_prefix_bytes`**（见 [`PHASE10.md`](phases/PHASE10.md) §1）。
  - **`checkpoint.active`**：单行 `a` 或 `b`，指示 **当前生效槽**；写入顺序为 **先完整写非活跃槽并 `fsync`，再更新 `checkpoint.active` 并 `fsync`**，避免单文件半写导致无法解析。
  - **遗留 `checkpoint` 文本行**：仍保留 **双写**，便于旧工具与回滚；**读路径**优先解析二进制槽（按 `checkpoint.active` 与 CRC 校验，失败则回退另一槽或遗留文件）。
- **与 MANIFEST 的先后关系**：与 `flush_memtable` 一致 — **先持久化 MANIFEST，再写 checkpoint**（槽 + 遗留行），以便 `manifest_version` 与 SST 列表一致。`StorageEngine::open` 在加载 MANIFEST 后读取 checkpoint：若 **`checkpoint.manifest_version` 大于已加载 MANIFEST 版本**，视为不一致，**应拒绝打开**（可能表示 checkpoint 超前或磁盘损坏）；若 checkpoint 版本 **低于或等于** MANIFEST，则属正常，差额由 **WAL 自 `wal_offset` 起重放** 补齐。
- **演进**：后续可追加环形 `checkpoint.log`（多历史记录）而不改变上述双槽最小集。

### 3.5 `undo.log` 生命周期与外部约束（七期论证 / 四期 4C / 十期前缀）

- **详稿**：[`phases/UNDO_LOG_4C.md`](phases/UNDO_LOG_4C.md)（八期：显式截断 API、可选 `flush` 后自动截断、互斥矩阵）；[`phases/CHECKPOINT_UNDO_PREFIX.md`](phases/CHECKPOINT_UNDO_PREFIX.md)（九期 9C → **十期**：checkpoint 与 `undo` 前缀联合回收 **v2 槽已落地**）；[`phases/PHASE10.md`](phases/PHASE10.md)（十期对外说明与 API）。
- **十期（已合入）**：`CheckpointWriter::write_rotating` 写入 **v2 二进制槽** 并在每次旋转写时 **重算** `CheckpointState::undo_log_safe_prefix_bytes`（与 `flush_memtable`、`compact_merge_two_oldest_l0`、`checkpoint()`、`wal_try_trim_prefix_through_checkpoint` 联动）；`StorageEngine::undo_try_truncate_recyclable_prefix` 在调用方确认安全策略后 **物理移除** `undo.log` 可回收前缀；**`open` 不**根据 checkpoint 中该水位 **自动** 截断 `undo.log`（与 **`kOpenFlagRebuildUndoStackFromLog`** 的风险论证仍以文档为准）。
- **`undo.log` 追加与截断**：除八期 **栈空整文件截断**、十期 **保守前缀截断** 外仍无轮转/分段 API；与 **WAL 前缀裁剪**（§3.3）及 **`kOpenFlagRebuildUndoStackFromLog`** 的交互须在 **4C / PHASE10** 中一并论证：截断 `undo.log` 前须保证 **无** 依赖该前缀 undo 帧的 **`undo_stack_` 回滚需求**，且与 WAL 已重放到的 **`commit_seq` 水位**一致，否则 **`open` 后 `rollback_one_undo_frame`** 语义失真。**二十三 23C**：对 **`mdb$` 键的首次可见写**，引擎在 **`undo_stack_` / `undo.log`** 中记录 **上一物理态为墓碑**（`mdb:tomb`），以便链式 `ROLLBACK` 能撤销 **INSERT** 类首次落盘写；与 [`UNDO_LOG_4C.md`](phases/UNDO_LOG_4C.md) 栈空截断语义一致（栈上多一帧时须 **多步** `rollback_one_undo_frame` 方可达栈空）。
- **与 WAL trim**：`wal_try_trim_prefix_through_checkpoint` 仅缩短 **`wal.log`**；**不得**暗示自动缩短 `undo.log`；若未来联合回收，须在 `CHANGELOG` 中单列 **安全窗口**（例如在 `flush_memtable` 后、无未决版本化写时）。
- **与七期 7C**：默认 **`mdb_persist_in_begin`** 下 **`BEGIN` 内 `persist_table`** 已走 `EmbedClient::submit`；进阶场景下 undo 帧与 WAL 帧的 **配对顺序** 仍须满足 [`phases/TXN_BEGIN_PERSIST_DESIGN.md`](phases/TXN_BEGIN_PERSIST_DESIGN.md) 草案。
- **与二十三 23C（链式 `ROLLBACK`）**：`rollback_one_undo_frame` / `undo_stack_` 与 `undo.log` 的 **配对**、前缀回收与 **`kOpenFlagRebuildUndoStackFromLog`** 风险仍须与 [`UNDO_LOG_4C.md`](phases/UNDO_LOG_4C.md) 矩阵一致；受限模型与 **`BEGIN` 水位** 见 **§4.3**、[`PHASE23.md`](phases/PHASE23.md)。

---

## 4. 嵌入式客户端（`client/embed`）

- **CommandBatch** 须携带可恢复字段（如 `client_session_id`、`seq`、`term`）与幂等 token；**Journal** 为可追加文本或等价格式，**session checkpoint**（`session.ckpt`）须能关联引擎 **`Manifest::version()`** 与（五期起）**`checkpoint_seq`**（第三行可选；缺省为 0，与旧会话文件兼容）。
- **恢复语义**：启动时重放 `seq > last_ack` 的批次；与引擎侧幂等配合，重复应用不得破坏状态。
- **持久化策略**（如 fsync 频率）必须可配置或文档化默认值；测试使用临时目录，不得在测试外写固定绝对路径。
- **三十一期（PHASE31）**：`undo_stack_` / `read_max_seq` / `session.txn` / checkpoint / WAL / compaction 的 **组合语义矩阵**、恢复链推荐序、flush·compact 不变式与「改存储写路径 checklist」见 **[`phases/PHASE31.md`](phases/PHASE31.md)**；可复制 **`--gtest_filter`** 见 [`phases/TESTING_TXN_CHAIN.md`](phases/TESTING_TXN_CHAIN.md) §13（推荐 **`*Phase31*`** 或该节显式前缀一行；**勿**单独使用裸 `Phase31*`，易匹配 0 条）。
- **三十二期（PHASE32）**：`client/mdb` 的 **`mdb_ops_*` 多编译单元** 与 `engine/storage` 的 **`storage_engine_detail.*`** 及 compact/checkpoint 粗拆分索引见 **[`phases/PHASE32.md`](phases/PHASE32.md)**；**不改变** §3–§4 语义，纯物理模块化。
- **三十三期（PHASE33）**：`storage_engine` 主路径与 compact/checkpoint 轨道的 **再切 TU**（`open_wal` / `put_undo` / `read`、`compaction_lsm`、`segments_worker_checkpoint`）见 **[`phases/PHASE33.md`](phases/PHASE33.md)**；**不改变** §3–§4 语义。
- **三十四期（PHASE34）**：拆分后 **文档锚点、索引与 GTest 可复制文案** 固化（含 **`*Phase31*`** 与权威 TU 表）；可选 MDB `mdb_ops_*` 再切；见 **[`phases/PHASE34.md`](phases/PHASE34.md)**。

### 4.0 文件系统保底机制（持久化与恢复地板）

本节把当前实现中 **实际落盘的路径** 定为 StructDB MVP 的 **保底（floor）**：上层与运维应 **仅依赖此处列出的载体** 做备份、恢复与故障分析；**不得**假设还存在另一套未文档化的「权威表文件」或并行日志。

#### 4.0.1 双根目录

| 根 | 典型配置 | 职责 |
|----|-----------|------|
| **引擎 `data_dir`** | `EngineConfigSnapshot::data_dir`（默认相对路径名 **`_data`**；与 `structdb_app` 未传 `--data-dir` 时一致） | **`StorageEngine`**：`wal.log`、checkpoint 链（§3.4）、MANIFEST、SST、版本化写相关的 **`undo.log`** 等；**逻辑表数据与 schema** 以 **`mdb$v2$*`**（及兼容的 v1 快照键）KV 形式存在于此树下，**无**「每表单独一个业务数据文件」的第二套权威存储。 |
| **会话 `session_dir`** | `EmbedClient::open(session_dir)`（`structdb_app` 默认 **`{data_dir}/embed_session`**） | **`session.journal`**（embed 批次 journal）、**`session.ckpt`**（会话 ack / 与引擎 checkpoint 关联）；MDB/REPL 另 **`session.txn`**（未提交会话事务，见 `CHANGELOG`）。 |

**约定**：仓库与工具链以 **`_data`** 作为引擎持久化根目录的**规范名**（便于 `.gitignore`、文档与运维统一）；部署可通过 **`--data-dir`** 指向任意绝对或相对路径。

运维 **冷备份/恢复** 步骤见 **[`BACKUP_RESTORE_RUNBOOK.md`](BACKUP_RESTORE_RUNBOOK.md)**（`scripts/backup_bundle.ps1` / `structdb_app --backup-bundle`）。

#### 4.0.2 保底文件清单（须随实现演进同步修订）

- **引擎侧（`data_dir`）**：`wal.log`（**崩溃恢复权威**的当前尾段；自 checkpoint 的 `wal_offset` 起重放；不完整尾帧规则见 `CHANGELOG`）；**二十期起**可选 `wal.segments`（v1 占位计数或 **v2 多段目录项**）与 **`wal/archive/*`** 封存段（与 v2 联用，详见 [`PHASE20.md`](phases/PHASE20.md)）；`checkpoint` / `checkpoint.a` / `checkpoint.b` / `checkpoint.active`（五期）；MANIFEST 与 SST；**`undo.log`**（版本化覆盖写；可选 `kOpenFlagRebuildUndoStackFromLog`）；**二十二 22C 起**可选 **`undo.segments`（v2）** 与 **`undo/archive/*`** 封存段（与 `EngineConfigSnapshot::undo_segment_roll_max_bytes` 联用，详见 [`PHASE22.md`](phases/PHASE22.md)、[`UNDO_LOG_4C.md`](phases/UNDO_LOG_4C.md)）；`redo.log` 存在但 **默认不作为 `open` 重放权威**（§3.1）。
- **会话侧（`session_dir`）**：`session.journal`、`session.ckpt`；可选 **`session.txn`**（仅当使用 MDB `BEGIN` 等）；**`session_log.txt`**（embed **活动日志**：`SESSION_OPEN` / `SESSION_CLOSE`、轮转规则见 `CHANGELOG`；**不参与**崩溃恢复权威，权威顺序见 **§4.0.3**）。

#### 4.0.3 恢复与权威顺序（概念）

1. **`StorageEngine::open`**：按 §3.1 / §3.4 加载 MANIFEST、checkpoint、**重放 WAL 链**（单文件时为 **`wal.log`**；多段 v2 时为先封存段再尾段，见 §3.3），得到 `mdb$` 键空间当前状态。  
2. **`EmbedClient::open` / `recover`**：在 `wal.log` 策略与 `CHANGELOG` 描述的前提下，重放 **`session.journal`** 中 `seq > last_ack` 的批次（与幂等 token 配合）。  
3. **MDB `session.txn`**：在 REPL/会话路径上恢复 **未提交** 逻辑表状态；**不**替代引擎 WAL 对 **已提交** `mdb$` 数据的权威。
4. **与 `session.txn` 的先后（部署推荐序）**：**先**完成 **`StorageEngine::open` 的 WAL 重放**（与 §3.1 一致），**再**在 `EmbedClient::open` 后由 MDB/REPL 首条命令路径 **`txn_log_try_recover_repl_session`** 解释 **`session.txn`**；后者 **仅**增量恢复未提交逻辑态，**不得**当作已提交物理态的替代来源。矩阵与代码锚点见 **[`phases/PHASE31.md`](phases/PHASE31.md)** §31A / §31B。

#### 4.0.4 耐久边界（保底之上仍有损风险）

- 未 **`fsync`** 的尾部（WAL 帧、journal 行、`session.txn` 的 v2 OP 等）在 **进程崩溃** 或掉电下可能丢失；具体开关见 `CHANGELOG`（如 `fsync_journal`、`MdbRunOptions::fsync_each_session_txn_op` 等）。**嵌入式耐久矩阵**（「可能丢什么 / 哪些开关 / 默认」）见 **§4.5** 表后一节。
- **同进程单写者**（§4.2）：多进程并发写同一 `data_dir` **不在**保底范围内。
- **二十四期（可选观测）**：`EngineConfigSnapshot::observe_embed_bypass_during_mdb_chain_txn` / `strict_reject_direct_kv_put_during_mdb_chain_txn`（均默认 **false**）与 `Engine::set_mdb_chain_txn_active_hint` 见 **[`PHASE24.md`](phases/PHASE24.md)**、**§4.2–§4.3**。

### 4.1 事务链分层（六期）

- **三层职责**（与 InnoDB「会话 / 引擎 / 恢复」类比，但非等价实现）：
  1. **MDB/REPL 会话事务**：`BEGIN`/`COMMIT`/`ROLLBACK`/`SAVEPOINT` 作用于 **内存逻辑表** `current` / `txn_base` 与 `session.txn`（含 `TXNV2` 增量）。默认 **`ROLLBACK` 不**隐式回滚已持久化到 `StorageEngine` 的 `mdb$` 数据；若 **`mdb_chain_rollback_on_mdb_rollback=true`** 且 **`mdb_persist_in_begin=true`**，则在恢复会话表 **之前** 按 **§4.3** 将 **`undo_stack_` 弹回 `BEGIN` 时刻深度**（受限模型）。
  2. **Embed 批次链**：`EmbedClient::submit` 将 dels/puts 打成 **单条 WAL 批次**；`session.journal` + `session.ckpt`（含五期起可选 **`checkpoint_seq`**）用于幂等与跨重启会话恢复；与引擎 `Manifest` / checkpoint 的关联见 §4 上文。
  3. **存储引擎**：`StorageEngine` 以 **WAL + `commit_seq`** 做 `mdb$` 版本可见性；`get`/`visit_prefix` 的 **`read_max_seq`** 为读视图上限；`undo.log` + `undo_stack_` 仅服务 **版本化覆盖写** 的进程内/可选重建回滚，**不等价**于 MDB 会话事务。
- **读视图与 `read_max_seq` 的绑定**（对齐 newdb 读路径「统一入口」思想，见 [`phases/TESTING_TXN_CHAIN.md`](phases/TESTING_TXN_CHAIN.md) 审计表）：
  - **`TXNISOLATION snapshot` 且 `BEGIN` 后**：脚本/REPL 通过 **`mdb_storage_read_seq_for_script`**（[`src/client/mdb/include/structdb/client/mdb_runner.hpp`](../src/client/mdb/include/structdb/client/mdb_runner.hpp)）得到 **`storage_read_seq`**，应传入所有需 MVCC 裁剪的 **`Engine::kv_get` / `kv_visit_prefix`**（`mdb_runner_dispatch.inc` 中访问 catalog/schema/行/快照的路径已统一）。
  - **`TXNISOLATION read_committed` 且事务激活**：`storage_read_seq` 为 **`engine.latest_commit_seq()`**（语句级可见最新已提交存储写）。
  - **非事务或纯逻辑命令**：使用 **`EmbedClient::read_snapshot_seq()`**（与 embed 水位对齐）。
- **`TXNISOLATION`**：**仅**在未处于 `BEGIN` 时允许切换。事务激活时执行 `TXNISOLATION` 将 **失败**（错误信息见实现），避免与已写入 `session.txn` 的快照语义纠缠；须先 **`COMMIT`/`ROLLBACK`**。
- **`SHOW SNAPSHOT`**：除 embed 与引擎水位外，在 **事务激活** 时额外输出 **`txn_storage_read_seq`**（与当行命令使用的 `storage_read_seq` 一致），便于对照 `SHOW TXN` 中的 `snap_seq`。

### 4.2 写并发与冲突策略（六期）

- **默认（MVP）**：**同进程单写者模型** — `StorageEngine` 写路径由 **互斥** 序列化；**不提供** InnoDB 式行锁等待或 newdb `WriteConflictPolicy::Wait` 的等价能力。
- **二十期：单逻辑写者 + 可选 compaction worker**：允许 **单独一条后台线程**（或固定 **1** 槽线程池）消费 **有界** `CompactionCommand` 队列，仅执行已由编排层授权的 compaction/drain 类工作。**禁止**该线程在未持 **`StorageEngine::mu_`**（或与文档化执行器等价的全局写锁）的情况下直接变异 MemTable / MANIFEST / WAL。**锁序**（建议）：**不得**在持 `mu_` 时阻塞等待 worker 完成同一引擎上的另一项需 `mu_` 的任务（避免自死锁）；worker 仅在弹出任务后 **短暂** 持 `mu_` 调用 `drain_pending_l0_compactions` 等 API。**三十五期**：L0 合并的 **重 I/O**（读双 SST、写临时 SST）在 **`mu_` 外** 完成，提交 MANIFEST/checkpoint 时仍 **持 `mu_`** 并校验计划未过期（见 [`PHASE35.md`](phases/PHASE35.md)、[`COMPACTION.md`](../COMPACTION.md)）。**三十六期**：**L1→L2、L2→L3、L3→L4** 采用与 L0 相同的 **两阶段 + 提交校验**（临时文件 `_tmp_tier_compact_*`；冲突可重试），公开 API 以 **`std::unique_lock(mu_)`** 进入 `*_with_relock_`。**读路径**：`get` / `visit_prefix` 仍与写路径共用 **`mu_`**；**`shared_mutex`** 读扩展 **未在三十六期启用**（预研与风险见 [`PHASE36.md`](phases/PHASE36.md)）。后台 compaction **不**构成第二「随意写」路径；与 §2.2 一致，**禁止** `engine/storage` → orchestrator/planner/facade **反向依赖**；背压与完成通知仍经 Facade 注入的 **窄接口 / 队列** 上行。**二十一期 21C**：`Engine::sync_scheduler_budget_from_storage_pressure` 可将 **worker 队列水位** 与 **`pending_deferred_l0_compact`** 映射到 `ResourceBudget::CompactionSlots`（见 [`PHASE21.md`](phases/PHASE21.md)）。
- **多会话 / 多客户端**：若同一 `data_dir` 上并发写，须由 **上层**（单进程编排、或进程外协调）保证互斥；违反则持久化损坏风险由部署模型承担。**三十五期（可选）**：`EngineConfigSnapshot::exclusive_data_dir_lock` / `structdb_engine_open_ex(..., STRUCTDB_ENGINE_OPEN_FLAG_EXCLUSIVE_DIR_LOCK)` 在 `data_dir/.structdb_exclusive.lock` 上持有 **建议锁**（POSIX `flock` / Windows `LockFile`），第二进程 **打开失败** 而非静默损坏；进程崩溃后锁随句柄释放。
- **同进程多 `EmbedClient` / 多线程**：`StorageEngine` 写路径仍由 **互斥** 序列化，但 **MDB 会话状态**（`current`、`session.txn`、脚本/REPL 的 `txn_active`）为 **每客户端 / 每会话** 资源；**不得**假设多个客户端交错执行 MDB 命令时仍等价于「单一脚本顺序」。与 **`persist_table` / `EmbedClient::submit`** 交错时，**读视图与 `undo_stack_` 水位** 的联合语义以 **§4.1 / §4.3** 与 [`PHASE24.md`](phases/PHASE24.md) 为准；超出文档组合 **不在**保底内。**三十五期**：`EmbedClient::submit` 对 **存储提交 + journal + 序号** 使用 **`submit_mu_`** 与 **`idem_mu_`** 的固定嵌套锁序，避免多线程宿主交错 `submit` 破坏 journal 单调性。
- **乐观冲突检测**：非默认；若未来在 `commit_embed_batch` 等路径引入 **显式版本检查**，须在 `CHANGELOG` 中单列行为与开关。

### 4.3 存储链式回滚与「事务中 `persist_table`」（六期 / 十七期 / 二十三 23C）

- **默认（`EngineConfigSnapshot::mdb_persist_in_begin` 为 true）**：`mdb_runner` 在 `BEGIN` 激活时仍对每次成功变更调用 **`persist_table`**（与 `EmbedClient::submit` 版本化写路径一致；与 **`MdbRunOptions::allow_persist_while_txn_active_experimental`** 为 AND）。`persist_table` 内 **`kv_visit_prefix` 收集旧索引键** 使用 **默认「最新可见」读视图**（`read_max_seq = max`），与上条语义一致。
- **`ROLLBACK` 与存储（默认）**：当 **`mdb_chain_rollback_on_mdb_rollback` 为 false**（默认）时，`ROLLBACK` **仅**恢复 MDB 会话内存中的 **`current`**（及删除 `session.txn`）；**不**按 MDB 事务链回退已提交的 embed 批次，也 **不**多步调用 `rollback_one_undo_frame`。因此 **`BEGIN` 内已落盘的写** 在 `ROLLBACK` 后仍可能以 **`latest_commit_seq`** 可见；会话内 `COUNT` 等仍反映回滚后的逻辑表。若需关闭事务内落盘，将 **`mdb_persist_in_begin=false`**（并可将 **`allow_persist_while_txn_active_experimental=false`** 作为每脚本/REPL 次覆盖）。
- **链式 `ROLLBACK`（二十三 23C，默认关）**：当 **`mdb_chain_rollback_on_mdb_rollback=true`** 且 **`mdb_persist_in_begin=true`** 且 per-run 开关允许 `persist_table` 时，MDB `ROLLBACK` 在恢复 **`current` / `session.txn` 之前** 调用 **`Engine::rollback_embed_undo_until(d)`**，其中 **`d`** 为 **`BEGIN` 成功写入 `txn_log` 后** 记录的 **`Engine::embed_undo_stack_depth()`**（`mdb_runner` 会话态）；内部循环 **`StorageEngine::rollback_one_undo_frame`** 直至栈深 **≤ `d`**。**`COMMIT`** 清除该水位，避免误滚后续非事务写。**禁止**在水位未定义时无限 pop（实现以深度为准，见 [`PHASE23.md`](phases/PHASE23.md)）。
- **受限语义（须在文档与测试中明示）**：`undo_stack_` 为 **进程内、每引擎** LIFO；首版水位为 **`undo_stack_.size()` 在 `BEGIN` 的快照**。若 **`BEGIN` 激活期间** 存在 **其他客户端或路径** 对同一引擎的版本化写 **绕过 MDB** 且改变栈深，则相对该水位的链式回滚语义 **未定义**（部署上应保持 **单写者** 或避免并发版本化写）。多会话长期方案见 [`PHASE13_PLUS_PLAN.md`](phases/PHASE13_PLUS_PLAN.md) 与 **`UNDO_LOG_4C.md`** 索引。
- **链式门闩开启时的部署清单（`mdb_chain_rollback_on_mdb_rollback=true` 且 `mdb_persist_in_begin=true`）**：
  - **禁止 / 高风险**：第二写者或后台任务对同一 `data_dir` 做 **版本化 `mdb$` 写**（含 **`Engine::kv_put`/`kv_remove` 直写 `mdb$*`** 与未经 MDB 编排的 `EmbedClient::submit`）与活跃 MDB 事务交错。
  - **未定义**：上述交错若发生，**不**保证 `ROLLBACK` 后存储与 MDB 逻辑表一致；见 [`PHASE24.md`](phases/PHASE24.md) **24A**。
  - **可选硬闸 / 观测**：`EngineConfigSnapshot::strict_reject_direct_kv_put_during_mdb_chain_txn` / `observe_embed_bypass_during_mdb_chain_txn`（默认 **关**）；`mdb_runner` 在 `BEGIN`/`COMMIT`/`ROLLBACK` 维护 **`Engine::set_mdb_chain_txn_active_hint`**（仅提示，非锁）。
- **论证与顺序**：与 WAL / `undo.log` 帧顺序、§3.1 混排重放规则交叉见 [`phases/TXN_BEGIN_PERSIST_DESIGN.md`](phases/TXN_BEGIN_PERSIST_DESIGN.md)。

### 4.4 InnoDB 概念映射（七期，非等价）

StructDB 以 **LSM + `wal.log` 权威 + `commit_seq` 读裁剪** 为主，与 InnoDB **页式 redo / 行级 trx_id / purge** 不等价。完整对照表、**MTR 类比**、**明确不包含的 InnoDB 能力** 见 [`phases/TXN_INNODB_MAP.md`](TXN_INNODB_MAP.md)。本节为索引；实现细节以代码与 `CHANGELOG` 为准。

### 4.5 耐久等级（InnoDB `innodb_flush_log_at_trx_commit` 类比，七期）

- **目的**：为运维与贡献者提供 **可沟通** 的耐久档位命名，映射到 **现有** `EmbedClient::submit(..., fsync_journal)`、`MdbRunOptions::{fsync_each_batch,fsync_each_session_txn_op}`、`EngineConfigSnapshot::fsync_every_write` 等；**不改变默认值**。
- **Level 0 / 1 / 2 语义表**：见 [`phases/TXN_INNODB_MAP.md`](TXN_INNODB_MAP.md) §2；**非** MySQL 服务器参数的二进制兼容实现。

#### 嵌入式耐久矩阵（二十四期 24B）

下列 **不按「Level 数字」排序**，而按 **「若未 fsync / 崩溃，可能丢什么」** 归纳；**默认**列与当前仓库默认一致（见 `EngineConfigSnapshot`、`MdbRunOptions` 默认值）。

| 风险面 | 可能丢什么 | 主要涉及路径 / 开关 | 默认 |
|--------|------------|---------------------|------|
| **WAL 尾批次** | 最后一次 `commit_embed_batch` / 直写 `put` 尚未 `fsync` 的 **`wal.log` 记录** | `EmbedClient::submit(..., fsync_journal)`；`MdbRunOptions::fsync_each_batch`；`EngineConfigSnapshot::fsync_every_write` | 多数默认 **弱**（见 `TXN_INNODB_MAP` §2 Level 2 类比） |
| **`session.journal` / `session.ckpt`** | 未 ack 的 embed 批次行、ckpt 第三行等 | `fsync_journal` 同上；与 WAL 权威关系见 §4.0.3 | 与 embed 批次策略一致 |
| **`session.txn`（MDB v2 OP）** | 未 `fsync` 的尾部 **v2 增量行** | `MdbRunOptions::fsync_each_session_txn_op` / REPL `fsync_session_txn_op` | 默认 **false**（较弱） |
| **checkpoint 槽** | 掉电时 **非活跃槽** 或 `checkpoint.active` 半写（读路径有 CRC/双槽回退） | `flush_memtable` / `checkpoint()` 写序；§3.4 | 五期起双槽；仍 **以 WAL 为准** |
| **链式 `ROLLBACK` 与 `undo_stack_`** | 不适用「丢 fsync」；若 **部署违反单写者**（§4.3），语义 **未定义** | `mdb_chain_rollback_on_mdb_rollback` + `mdb_persist_in_begin`；可选 **24A** 观测/硬闸 | 门闩默认 **false** |

---

## 5. 构建系统（CMake）

### 5.1 根工程职责

根 `CMakeLists.txt` 负责：C++17、工具链提示、`Base`、`ThirdParty/gtest_capi`、`engine/*`、`client/embed`、`app`、可选 `tests` / `benchmarks`。

### 5.2 关键选项（须保持文档同步）

| 选项 | 含义 |
|------|------|
| `STRUCTDB_STATIC_MSVC_RUNTIME` | MSVC 下与 `gtest_capi` 对齐的 `/MT`；默认 ON |
| `STRUCTDB_BUILD_TESTS` | 是否构建 `structdb_tests` |
| `STRUCTDB_BUILD_BENCHMARKS` | 是否构建 `structdb_bench` 及（默认路径下）Google Benchmark |
| `STRUCTDB_ENABLE_PERF_GATE` | 默认 **OFF**；**ON** 时注册 **`ctest` `structdb_perf_gate`**（**Python 3.8+**，将 `structdb_bench` 的 JSON 输出与 `benchmarks/baselines/structdb_bench_baseline.json` 对比；**MSVC 多配置**下运行 `ctest` 须带 **`-C Release`**） |
| `STRUCTDB_FETCH_FMT_SPDLOG_BENCHMARK` | ON：对 fmt / spdlog / benchmark 使用 **FetchContent**；OFF：使用 `ThirdParty/` 本地源码 |

**CRT / ODR**：与 GoogleTest、gtest_capi 等混链时，须保持运行时库选择一致；新增 DLL 目标时需在测试中处理 DLL 搜索路径（参见 `tests/CMakeLists.txt` 中的 Windows POST_BUILD 复制约定）。

### 5.3 第三方来源

- **fmt、spdlog、benchmark**：默认 **vendor**；可选 **FetchContent**（需网络；标签版本以 `cmake/StructDBThirdParty.cmake` 为准）。
- **crc32c、CRoaring、gtest_capi**：当前以仓库内 `ThirdParty/` 为准，不在 `StructDBThirdParty.cmake` 的 FetchContent 分支内重复声明，除非另有工程级变更。

### 5.4 MinGW

在 MinGW / `GNU` 于 Windows 上编译时，`STRUCTDB_STATIC_MSVC_RUNTIME` 仅对 MSVC 有意义；不得假设全平台存在 MSVC 专用 pragma；泄漏检测等须用 `#ifdef _MSC_VER` 隔离。

---

## 6. 测试与基准

### 6.1 单元与集成测试

- 使用 **GoogleTest** 编写断言；进程入口须调用 **`gtest_capi_init_from_argv`** 与 **`gtest_capi_run_all`**，以保持与 C API 宿主兼容（见 `ThirdParty/gtest_capi`）。
- 新功能须带测试或明确标注「未覆盖」的后续工单；背压、epoch、取消、embed 恢复等关键路径须有回归用例。
- **事务链 / 存储边界相关 PR**（触及 `POLICY` §3–§4、`client/embed`、`client/mdb`、`engine/storage` 写路径或 checkpoint/WAL/MANIFEST 顺序）：描述中须附 **至少一行** 可复制的 **`structdb_tests` / `mdb_tests` `--gtest_filter=...`**（与 [`phases/TESTING_TXN_CHAIN.md`](phases/TESTING_TXN_CHAIN.md)、[`phases/PHASE31.md`](phases/PHASE31.md) 索引一致即可）。

### 6.2 基准（Benchmark）与 perf 门禁

- `benchmarks/` 中对 MemTable、WAL、Compaction 挑选等提供 **可重复** 的微基准；磁盘-heavy 场景可先 mock 或可选参数，避免在默认 CI 中强依赖慢盘。
- **基线文件**：[`benchmarks/baselines/structdb_bench_baseline.json`](../benchmarks/baselines/structdb_bench_baseline.json) 为 Google Benchmark **`--benchmark_out_format=json`** 导出（以 **`real_time`（ns/次）** 作为门禁指标）。更新基线须在 **Release**、**代表性硬件** 上重跑 `structdb_bench` 并替换该文件，且在 [`CHANGELOG.md`](CHANGELOG.md) 中说明原因（性能回退接受、或基准场景变更等）。
- **对比脚本**：[`benchmarks/scripts/compare_bench.py`](../benchmarks/scripts/compare_bench.py)（**Python 3.8+**，仅标准库）。典型用法见 [`benchmarks/README.md`](../benchmarks/README.md)。默认 **`--max-ratio 1.5`**（当前运行相对基线允许至多 **1.5×** 的 `real_time` 劣化）。其他机器上若噪声导致偶发失败，可临时放宽比例，或在本仓库认可的 runner 上刷新基线。
- **可选 `ctest`**：`-DSTRUCTDB_ENABLE_PERF_GATE=ON` 时注册 **`structdb_perf_gate`**（`LABELS` 含 **`perf`**）。与常规单测分离；MSVC 下务必 **`ctest -C Release -L perf`**。默认 **OFF**，避免未装 Python 或机器差异阻塞默认构建。
- **读者索引**：[`PHASE29.md`](phases/PHASE29.md) §1 归纳本节命令与 CMake 门闩在运维流程中的位置。

---

## 7. 可观测性与调试

- **Logging**：统一走 `structdb::infra` 日志门面（默认 spdlog 实现可替换）。
- **Tracer / Metrics**：计划中的 span 与计数器接口以「可测试、低开销默认实现」为优先；生产级导出非 MVP 必需。
- **存储九期（9E）**：`StorageEngine::compaction_merge_count()` 统计本进程成功完成的 **L0 双文件合并** 次数（见 [`COMPACTION.md`](COMPACTION.md)）；**非**持久化指标，与 embed/MDB 耐久档位正交。
- **LeakDetector**：Debug 下 MSVC CRT 报告等仅作开发辅助；Release 行为不得依赖其副作用。
- **运维索引（二十九期）**：会话活动日志 **`session_log.txt`**、关键 **水位**、会话/引擎文件与背压只读信号的 **一页式索引** 见 [`PHASE29.md`](phases/PHASE29.md)（**非** Prometheus/OpenTelemetry 等完整监控栈）。

---

## 8. 版本与变更记录

- **语义化版本**（SemVer）与 `CMakeLists.txt` 中 `project(... VERSION)` 对齐。
- 任何影响构建、依赖、公开头文件或持久化格式的变更，**必须在 `Docs/CHANGELOG.md` 中** 在 `[Unreleased]` 或新标签小节下写明（参见该文件顶部说明）。

---

## 9. 贡献与代码审查检查清单

提交 PR 前建议自检：

1. 依赖方向是否违反 §2。
2. 是否新增仅 Linux 可用的必需路径而未在 Windows 提供等价或降级。
3. CMake 选项默认值是否仍适合离线构建。
4. 测试与（若适用）基准是否更新。
5. `Docs/CHANGELOG.md` 是否已记录用户可见变更。

---

## 10. 文档维护

- 修改构建选项、目录布局或持久化格式时，**同步更新** `POLICY.md` 与 `CHANGELOG.md`。
- 本文件使用 Markdown；链接使用仓库内相对路径，便于在 Git 托管界面浏览。
