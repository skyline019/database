# StructDB 下一阶段重构建议

本文结合当前代码实现，对 StructDB 下一阶段的重构方向做一个**可落地**的建议清单。目标不是推倒重来，而是在保持现有 WAL / checkpoint / embed / MDB 语义稳定的前提下，把系统从“功能齐全、工程复杂”推进到“结构清晰、性能可持续、回归更容易”的阶段。

---

## 1. 当前结构的判断

从代码看，StructDB 已经具备较完整的 LSM 引擎、嵌入式事务层、MDB 逻辑层与 GUI 交互层：

- `StorageEngine` 负责 WAL、MemTable、SST、manifest、checkpoint、compaction。
- `EmbedClient` 负责 session journal、恢复与批提交。
- `mdb_runner` 负责逻辑表、分页、持久化与脚本执行。
- `GraphExecutor` 负责计划执行、cancel 与 backpressure。

这意味着系统已经进入一个很典型的阶段：**不是缺功能，而是缺“结构化治理”**。

其中几个已经显现的架构特征很明确：

- `StorageEngine` 仍是 **对外门面**，但 **open 恢复（`RecoveryCoordinator` 编排 + `WalReplayApplier` 解码）**、WAL 段、compaction 物化、checkpoint/undo、独占目录锁等已迁入内部协调类（**`RecoveryCoordinator`**、**`WalReplayApplier`**、`WalCoordinator`、`CompactionCoordinator`、`CheckpointUndoCoordinator`），头文件中的「巨型 private 方法墙」已明显收敛。
- `MemTable` 默认后端为 **`MemTableSkipList`**（`EngineConfigSnapshot` / `StorageEngine` / `MemTableManager` 一致）；仍可选 **`MemTable`（`std::map`）**；arena / 分片见 [`STORAGE_EVOLUTION_AND_OBSERVABILITY.md`](STORAGE_EVOLUTION_AND_OBSERVABILITY.md)。
- compaction 已拆出 **`CompactionCoordinator`**（专用 I/O executor、分块节流、worker 队列仍由引擎持有队列状态，物化与 L0 节流在协调器内）。
- GUI 已经开始承载脚本进度、取消、分页钳制，前后端长任务体验开始成为系统特性之一。

---

## 2. 重构总目标

建议下一阶段重构围绕四个目标展开：

1. **拆职责**：让 `StorageEngine` 从“全能类”回到“协调器 + 少量关键状态”的角色。
2. **减耦合**：降低 WAL / checkpoint / compaction / undo / journal 之间的交叉依赖。
3. **提性能上限**：优先解决 MemTable 和 compaction 的结构性瓶颈。
4. **提可维护性**：让测试、观测、配置、回滚开关更模块化。

---

## 3. 第一优先级重构：MemTable 结构升级

### 3.1 为什么这是第一优先级

当前 `MemTable` 仍然是 `std::map<std::string, std::string, std::less<>>`：

```8:34:e:\db\StructDB\src\engine\storage\include\structdb\storage\memtable.hpp
class MemTable {
 public:
  void put(std::string key, std::string value);
  /// True if `key` is present (including tombstone); use with `get` to distinguish miss vs tomb.
  bool has_key(const std::string& key) const { return has_key(std::string_view(key)); }
  bool has_key(std::string_view key) const;
  /// Returns stored bytes including tombstone; false if absent.
  bool get_raw(const std::string& key, std::string* stored_out) const { return get_raw(std::string_view(key), stored_out); }
  bool get_raw(std::string_view key, std::string* stored_out) const;
  bool get(const std::string& key, std::string* value_out) const { return get(std::string_view(key), value_out); }
  bool get(std::string_view key, std::string* value_out) const;
  std::size_t bytes_approx() const;
```

这对正确性和有序遍历很好，但作为热路径内存结构，它的瓶颈非常清楚：

- 插入和查找都是 `O(log n)`
- 节点分配多，缓存局部性一般
- 大批量写入时，内存层本身就会成为热点
- 未来要做 shard / arena / 冻结层分离时，`std::map` 的迁移成本会越来越高

### 3.2 建议的重构方向

建议分两步走：

#### 第一步，抽象 `MemTable` 接口

把“有序 KV 容器”与“具体实现”分离：

- `MemTable` 保留行为接口
- 引入 `IMemTable` 或内部策略层
- 当前 `std::map` 实现作为默认后端

这样后续可以替换为：

- `std::map` + arena 优化版
- 分段跳表 / ordered vector + append buffer
- active table + immutable snapshot 的双层结构

#### 第二步，活跃层 / 冻结层（已产品化）

`flush_memtable` 将活跃表迁入 **`MemTableManager`** 管理的 **frozen** 快照，在**不持有** `mu_` 时写临时 SST，再持锁 `rename` 入 manifest；读路径合并 active 与 frozen；失败与 `close` 时用 `merge_missing_from` 回收冻结键。详见 `storage_engine.hpp` 中 `flush_memtable` 注释与 `memtable_manager.*`。

下一步仍可演进为 arena / 分片 / 双缓冲写入等，但 **双层模型与可插拔 `IMemTable` 已落地**。

### 3.3 预期收益

- 写入放大下降
- flush 更容易并行化
- `put` 与 flush 的锁竞争进一步下降
- 为未来 arena / shard / lock striping 铺路

---

## 4. 第二优先级重构：拆分 `StorageEngine` 的职责边界

### 4.1 为什么要拆（进展）

历史上 `StorageEngine` 曾集中 WAL、checkpoint、compaction、undo、恢复与压力快照；**当前** 已将大块逻辑迁入协调类（含 **`StorageTelemetry`**、**`WalReplayApplier`**），头文件中仍保留 **对外 API、配置 setter、worker 队列、原子计数器** 等与调度强绑定的成员。

### 4.2 建议拆成 5 个子模块（与代码映射）

| 建议名 | 当前主要承载 | 说明 |
|--------|----------------|------|
| `WalManager` / **`WalCoordinator`** | `wal_coordinator.*` | 段目录、roll、trim、GC、flush 时 persist、append/fsync 观测 |
| **MemTableManager** | `memtable_manager.*` | active / frozen、`reset_to_backend`、overlay 读；**默认后端 `SkipList`**（`Map` 可配） |
| `SstCompactor` / **`CompactionCoordinator`** | `compaction_coordinator.*` | L0/tiered 物化与提交、L0 节流、专用 compaction I/O executor 启停 |
| `RecoveryManager` / **`RecoveryCoordinator`** | `recovery_coordinator.*` | `open` 路径编排：目录准备、独占锁、段目录、打开 WAL/redo/undo、manifest、`COMMIT_SEQ`、**`replay_checkpoint_and_wal`**、段观测刷新（重放帧委托 **`WalReplayApplier`**） |
| **`WalReplayApplier`** | `wal_replay_applier.*` | WAL **行 / `STDBBW1` 批次**解码，友元写回 `mem_mgr_.active()`、`observe_stored_commit_seq_` |
| `StorageTelemetry` | `storage_telemetry.*`（`read_storage_pressure_snapshot`；`StorageEngine` 对外 API 委托） | 与 compaction 物化解耦 |

### 4.3 预期收益

- `StorageEngine` 头文件继续缩小（private helper 已迁出多批）
- 重构和测试可以按模块拆开
- 以后更容易做单元测试和故障注入
- 状态机边界更清晰，减少交叉 bug

---

## 5. 第三优先级重构：Compaction 任务与 I/O 的进一步解耦

### 5.1 当前状态

你们已经做了不少 compaction 工程优化：

- 专用 I/O executor
- 分块 I/O
- 并行 SST 读
- 字节桶限速
- worker 队列
- 低优先级后台线程

相关接口已经很明确：

```1:29:e:\db\StructDB\src\engine\storage\include\structdb\storage\compaction_io_executor.hpp
/// Thread-pool for compaction **materialize** disk work (merge SST read/write loops) so foreground / WAL paths
/// do not execute large I/O directly when enabled via `EngineConfigSnapshot::compaction_dedicated_io_executor`.
/// WAL is never opened on this pool — only SST / temp files under `data_dir` (see `WalWriter` vs merge materialize).
class CompactionIoExecutor {
 public:
  CompactionIoExecutor();
  ~CompactionIoExecutor();
```

以及 `StorageEngine` 中 dedicated executor 相关配置（仍由引擎持有 **队列与线程**，**物化与 L0 节流**在 `CompactionCoordinator`）：

```130:147:e:\db\StructDB\src\engine\storage\include\structdb\storage\storage_engine.hpp
  void set_compaction_dedicated_io_executor(bool enable) { compaction_dedicated_io_executor_ = enable; }
  void set_compaction_io_chunk_bytes(std::uint32_t bytes) { compaction_io_chunk_bytes_ = bytes; }
  /// When true (default), merge materialize may load the two input SSTs concurrently (two input files only).
  void set_compaction_parallel_sst_reads(bool enable) { compaction_parallel_sst_reads_ = enable; }
```

这说明 compaction 已经开始从“函数”走向“系统子组件”。下一步应该继续把它隔离得更彻底。

### 5.2 建议重构点

#### 1）把 compaction 任务描述对象化

现在 compaction 已有 snapshot struct，**物化与 manifest 提交**在 **`CompactionCoordinator`**；任务编排仍部分留在 `StorageEngine`（worker 队列、`drain_pending_l0_compactions` 等）。建议统一成：

- `CompactionJob`
- `CompactionSnapshot`（已有 L0 / tiered 结构）
- `CompactionResult`

这样 worker、I/O executor、commit 阶段可以共享同一个任务描述。

#### 2）把 materialize 逻辑从 commit 逻辑中分离

目标是：

- materialize 只负责“读旧 SST + 写新 SST”
- commit 只负责“校验 manifest + 原子替换 + checkpoint 逻辑”

当前这两个阶段虽然已经部分分开，但还不够强。

#### 3）把限速器独立为可复用组件

现在字节桶限速是挂在 `StorageEngine` 内部。建议把它独立成：

- `TokenBucket`
- `RateLimiter`
- 或 `I/O Budget` 模块

这样 WAL、compaction、导出等长任务可以共享统一的节流模型。

### 5.3 预期收益

- compaction 更容易扩展到多 worker / 多阶段
- 限速与调度逻辑统一
- 更容易给后台任务做优先级和预算控制

---

## 6. 第四优先级重构：统一恢复与 checkpoint 语义

### 6.1 现有问题

现在恢复相关逻辑集中在 **各协调器**；WAL 行/批重放解码在 **`WalReplayApplier`**（`wal_replay_applier.cpp`），经友元写回 `mem_mgr_.active()` 与 `observe_stored_commit_seq_`，由 **`RecoveryCoordinator::replay_checkpoint_and_wal`** 在持锁路径中调用。

- `RecoveryCoordinator`：`open` 顺序（目录、锁、段目录、打开日志、manifest、`COMMIT_SEQ`、checkpoint+WAL **重放调度**、观测刷新）
- **`WalReplayApplier`**：WAL 重放 **帧解码**（`apply_line_unlocked` / `apply_batch_unlocked`）
- `WalCoordinator` / `CheckpointUndoCoordinator`：段元数据与 undo 栈重建等

这本身没错，但如果没有统一的恢复状态机，未来很容易出现“某个边界刚好不一致”的问题。

### 6.2 建议建立恢复状态机

建议引入一个明确的恢复阶段枚举，例如：

- `Bootstrapping`
- `ReplayWal`
- `LoadCheckpoint`
- `RebuildInMemoryState`
- `ValidateManifest`
- `Ready`

然后让 `StorageEngine::open()` 只做调度 — **当前** 已委托给 **`RecoveryCoordinator`** 分步方法；`StorageRecoveryPhase` trace 仍按阶段命名。**WAL 帧解码** 已独立为 **`WalReplayApplier`**；若需再演进，可做 §6 的恢复策略对象化（非必须）。

### 6.3 将“恢复策略”与“存储动作”解耦

例如：

- 什么时候优先 WAL
- 什么时候优先 journal
- 什么时候允许 trim
- 什么时候 checkpoint 生效

这些规则应该集中到一个恢复策略层，而不是散落在多个分支里。

### 6.4 预期收益

- 崩溃恢复更容易推理
- 回归用例更容易覆盖不同状态
- 后续做 segment 化 WAL / undo 时不容易乱

---

## 7. 第五优先级重构：MDB 持久化和查询路径模块化

### 7.1 现状

MDB 已经做了不少优化，例如增量 persist、分页钳制、`SCAN MORE` 游标等。但从长远看，MDB 仍然容易成为“逻辑、IO、格式、UI”混杂的层。

### 7.2 建议拆出三层

#### 1）`LogicalTable`

只管理内存中的表语义：

- rows
- schema
- dirty 标记
- rollback 栈

#### 2）`MdbPersistence`

只管如何持久化到 KV：

- key 编码
- batch 组装
- 增量 / 全量 persist 策略

#### 3）`MdbQueryPaging`

只管分页、排序、partial sort、游标、`PAGE_JSON` 限制等。

### 7.3 为什么这样拆

因为这三者的演进节奏不同：

- 查询分页会持续优化
- 持久化策略会跟随恢复语义变化
- 逻辑表结构可能会再改

拆开后，每一块都能独立迭代。

---

## 8. 第六优先级重构：GraphExecutor 与背压的语义对齐

### 8.1 当前实现

`GraphExecutor::execute` 目前做了拓扑执行、cancel、以及可选预算探测：

```1:120:e:\db\StructDB\src\engine\runtime\src\graph_executor.cpp
void GraphExecutor::request_cancel() {
  std::lock_guard<std::mutex> lock(cancel_mu_);
  if (active_cancel_) active_cancel_->store(true, std::memory_order_relaxed);
}

bool GraphExecutor::execute(planner::ExecutionPlan plan, scheduler::ExecutionScheduler& sched,
                            bool use_budget_probe, std::string* error_out) {
  if (!sched.set_active_plan(std::move(plan), error_out)) return false;
  const auto* p = sched.active_plan();
```

这已经足够支持基本执行，但它仍然更像“执行器”，不是“统一调度层”。

### 8.2 建议

- 让 budget probe 与实际资源使用更一致
- 给 operator 生命周期明确的 prepare / execute / rollback 约束
- 对 cancel 统一返回原因，而不是只返回字符串错误
- 后续可以考虑把 storage backpressure 直接纳入执行计划的静态分析

### 8.3 预期收益

- 计划执行更可预测
- 大任务取消更稳定
- 存储压力与执行调度更容易联动

---

## 9. 第七优先级重构：统一配置、观测、回归入口

### 9.1 问题

现在很多优化能力已经存在，但它们的入口分散在：

- `StorageEngine` setter
- `EngineConfigSnapshot`
- benchmark
- tests
- GUI 开关
- 文档

这会导致“功能存在，但没人知道怎么组合使用”。

### 9.2 建议

#### 1）把配置分为三类

- **运行时安全开关**：可热切换，如节流、trace、进度
- **启动期结构开关**：如 segment roll、dedicated I/O、目录锁
- **实验性开关**：仅用于回归或 bench

#### 2）建立统一配置清单页

**已维护清单**：[`ENGINE_RUNTIME_CONFIG.md`](ENGINE_RUNTIME_CONFIG.md)（与 `EngineConfigSnapshot` 对齐；含 `Orchestrator::set_before_graph_execute` 与调度器压力交叉引用）。

建议以后新增功能时，必须同步写：

- 配置名
- 默认值
- 生命周期（启动/运行时）
- 回滚方式
- 对应测试

#### 3）统一观测指标前缀

像 `StoragePressureSnapshot`、trace span、bench 指标，建议按模块统一命名，减少日后查找成本。**存储引擎 trace** 已采用 **`stdb.storage.*`** 前缀（见 `storage_trace.hpp` 与 [`STORAGE_EVOLUTION_AND_OBSERVABILITY.md`](STORAGE_EVOLUTION_AND_OBSERVABILITY.md) §4）；**`StoragePressureSnapshot` 字段名**因 MDB JSON 兼容暂不批量重命名。

---

## 10. 推荐的重构顺序

如果按风险和收益排序，我建议这样做：

### 第 1 阶段：低风险、高收益

1. ~~把 `MemTable` 接口抽象出来~~ **（已落实：`IMemTable` + `MemTable`）**
2. ~~建立 `StorageEngine` 内部子模块边界~~ **（已落实：`MemTableManager`；`WalCoordinator`、`CompactionCoordinator`、`CheckpointUndoCoordinator`、`RecoveryCoordinator`；**`WalReplayApplier`**；**`StorageTelemetry`**；merge 字节桶、`compaction_snapshot`；`storage_engine_open_wal.cpp` 仅构造/`open`/`close`）**
3. ~~统一 compaction job / snapshot / result 对象~~ **（部分落实：L0 / tiered 快照结构外提至 `compaction_snapshot.hpp`；**`CompactionResult`** 类型已引入 `compaction_result.hpp`，bool 路径渐进迁移；`CompactionWorkerJob` 仍内嵌引擎）**
4. ~~统一配置分类与文档~~ **（已落实：[`ENGINE_RUNTIME_CONFIG.md`](ENGINE_RUNTIME_CONFIG.md) 三类清单 + 代码锚点）**

### 第 2 阶段：中风险、中高收益

5. ~~把恢复逻辑抽成状态机~~ **（已落实：命名阶段枚举 + `RecoveryCoordinator` 分步 + trace；策略对象化可续作）**
6. ~~把 MDB 持久化 / 查询分页拆开~~ **（已落实：`PAGE`/`PAGE_JSON` 迁入 `mdb_query_paging.cpp`；持久化在 `mdb_persistence.cpp`；`mdb_ops_pages_journal_import.cpp` 保留 journal/import 与 CSV 解析辅助）**
7. ~~把 token bucket / 节流器独立为可复用组件~~ **（已落实：compaction merge 用 `SteadyClockByteTokenBucket`；WAL 侧仍属 `WalWriter`）**

### 第 3 阶段：高收益、结构性升级

8. ~~重构 MemTable 实现为更适合 LSM 的结构~~ **（部分落实：`MemTableSkipList` + `EngineConfigSnapshot::memtable_backend`；arena/分片仍可选）**
9. ~~进一步拆分 WAL / checkpoint / undo 管理器~~ **（已落实：`WalCoordinator`、`CheckpointUndoCoordinator`；WAL 重放解码 **`WalReplayApplier`**；`RecoveryCoordinator` 负责 `open` 编排与重放调度）**
10. ~~完成 GraphExecutor 与 storage backpressure 的统一~~ **（已落实：`Orchestrator` 每次 `run_default`/`replan_and_run` 前可选 `before_graph_execute`；Facade 绑定 `sync_scheduler_budget_from_storage_pressure`；`GraphExecutor::execute` 对该路径 `use_budget_probe=true`）**

---

## 13. 落实进度（本波收尾，与代码同步）

本节为 **§1–§12 建议** 的执行台账；更细的优化里程碑仍写在 [`OPTIMIZATION_PLAN.md`](OPTIMIZATION_PLAN.md) §0。**本波** 目标为：`StorageEngine` 瘦身、恢复/压力/重放职责落位、文档锚点一致。

### 13.1 本波已合主题

| 主题 | 说明 | 主要路径 |
|------|------|-----------|
| MemTable 抽象 / flush / **可插拔后端** | `IMemTable`、`MemTable`（`std::map`）、**`MemTableSkipList`**、`MemTableBackend`、`MemTableManager::reset_to_backend`（`open` 重放前）、`swap_with` / `merge_missing_from` / `for_each_sorted_prefix_overlay` | `imemtable.hpp`、`memtable.*`、`memtable_skiplist.*`、`memtable_backend.hpp`、`memtable_manager.*` |
| Compaction 快照外提 | `CompactionL0MergeSnapshot`、`CompactionTieredPairSnapshot` | `compaction_snapshot.hpp` |
| Merge 字节桶 | `SteadyClockByteTokenBucket` | `byte_token_bucket.*` |
| `open` 恢复分步 + **`RecoveryCoordinator`** | `StorageRecoveryPhase`、`prepare` / `load` / `open_log` / **`replay_checkpoint_and_wal`** / `refresh`；`open()` 委托 | `recovery_phase.*`、`recovery_coordinator.*`、`storage_engine_open_wal.cpp`（仅构造/`open`/`close`） |
| **`WalReplayApplier`** | `apply_line_unlocked`、`apply_batch_unlocked`；`replay_checkpoint_and_wal` 内 `replay_one` 委托 | `wal_replay_applier.*` |
| MDB 三层头文件 / 持久化 / 分页 TU | `LogicalTable`、`MdbQueryPagingState`、`handle_page*`、`persist_table` 等 | `mdb_logical_table.hpp`、`mdb_persistence.*`、`mdb_query_paging.*` |
| GraphExecutor 背压与诊断 | `estimated_cost` → MemTable 探测量、`GraphExecuteDiagnostics` | `graph_executor.*`、`scheduler.hpp` |
| Orchestrator ↔ 存储压力 | 每次图执行前 `sync_scheduler_budget_from_storage_pressure`；`execute(..., use_budget_probe=true)` | `orchestrator.*`、`engine.cpp` |
| **`StorageTelemetry`** | `read_storage_pressure_snapshot`（持 `mu_` 共享锁填 `StoragePressureSnapshot`）；`StorageEngine` 一行委托 | `storage_telemetry.*`（`storage_engine_compaction_lsm.cpp` 仅委托） |

### 13.2 本波规划内项（已全部收敛）

以下条目在 **§3–§10** 中曾列为优先或可选；**当前代码与上表已覆盖**，不再作为阻塞项跟踪：

1. ~~MemTable 热路径（**部分**：`MemTableSkipList`）~~  
2. ~~`StorageEngine` 协调器 / 友元子模块（Wal / Compaction / Checkpoint-Undo / **Recovery** / **`WalReplayApplier`** / **Telemetry**）~~  
3. ~~`PAGE` / `PAGE_JSON` 分页 TU~~  
4. ~~配置清单（`ENGINE_RUNTIME_CONFIG.md`）~~  
5. ~~执行图 ↔ 存储压力（`before_graph_execute` + `use_budget_probe`）~~  

### 13.3 远期主线（已「一次收口」为专文 + 部分代码锚点）

原列为 MemTable / Compaction / 恢复+Embed / 观测 四条；**本轮**已将**可落地部分**合入代码与常量约定，其余明确标为**下一阶段**（避免无边界继续拆引擎外壳）：

- **专文**（权威）：[`STORAGE_EVOLUTION_AND_OBSERVABILITY.md`](STORAGE_EVOLUTION_AND_OBSERVABILITY.md)（默认 MemTable=`SkipList`、`CompactionResult`、`RecoveryOpenPolicy`、`stdb.storage.*` trace、`BM_StdbStorage*` bench、**不改** `StoragePressureSnapshot` JSON 字段名等）。
- **代码锚点**：`compaction_result.hpp`、`recovery_open_policy.hpp`、`storage_trace.hpp`；`EngineConfigSnapshot` / `MemTableManager` / `StorageEngine` **默认 SkipList**；存储层 trace / `engine_bench` 命名已按专文前缀调整。


## 11. 一句话版本

StructDB **本波存储侧大范围重构** 已收敛：`StorageEngine` 保留门面与锁内状态，**WAL 段 / compaction / checkpoint-undo / open 恢复 / WAL 帧解码 / 压力快照** 分别落在协调类与 **`WalReplayApplier`**、**`StorageTelemetry`** 上（见 §13）。**§13.3 四条远期主线**已收口为专文 [`STORAGE_EVOLUTION_AND_OBSERVABILITY.md`](STORAGE_EVOLUTION_AND_OBSERVABILITY.md)（含默认 **SkipList**、观测前缀、`CompactionResult` 等）；后续以该文为闸门推进 arena / job 外提等 **增强项**。

- **MemTable**：默认 **`SkipList`**；`Map` 仍可选；arena 等仅按专文扩展。  
- **存储引擎**：协调器 + **`WalReplayApplier`** + **`StorageTelemetry`** 已落位；**`storage_engine_open_wal.cpp`** 保持极薄。  
- **恢复 / compaction / 观测**：轻量策略与 trace/bench 前缀已对齐专文；与 Embed 交叉规则以 **`POLICY` + 专文链接** 为准。

---

## 12. 参考代码位置

- `src/engine/storage/include/structdb/storage/storage_engine.hpp`
- `src/engine/storage/include/structdb/storage/storage_telemetry.hpp`
- `src/engine/storage/include/structdb/storage/wal_replay_applier.hpp`
- `src/engine/storage/src/storage_engine_open_wal.cpp`（构造、`open`/`close`）
- `src/engine/storage/src/wal_replay_applier.cpp`、`src/engine/storage/src/recovery_coordinator.cpp`
- `src/engine/storage/include/structdb/storage/wal_coordinator.hpp`、`checkpoint_undo_coordinator.hpp`、`compaction_coordinator.hpp`、`recovery_coordinator.hpp`（及对应 `src/*.cpp`）
- `src/engine/storage/include/structdb/storage/memtable.hpp`
- `src/engine/storage/include/structdb/storage/imemtable.hpp`
- `src/engine/storage/include/structdb/storage/memtable_manager.hpp`
- `src/engine/storage/include/structdb/storage/recovery_phase.hpp`
- `src/engine/storage/include/structdb/storage/byte_token_bucket.hpp`
- `src/engine/storage/include/structdb/storage/compaction_snapshot.hpp`
- `src/engine/storage/include/structdb/storage/compaction_io_executor.hpp`
- `src/engine/runtime/include/structdb/runtime/graph_executor.hpp`
- `src/client/mdb/include/structdb/client/mdb_logical_table.hpp`
- `src/client/mdb/include/structdb/client/mdb_persistence.hpp`
- `src/client/mdb/include/structdb/client/mdb_query_paging.hpp`
- `src/client/mdb/src/mdb_query_paging.cpp`
- `src/client/embed/src/embed_client.cpp`
- [`Docs/STORAGE_EVOLUTION_AND_OBSERVABILITY.md`](STORAGE_EVOLUTION_AND_OBSERVABILITY.md)（§13.3 四条主线收口）
- `src/engine/storage/include/structdb/storage/compaction_result.hpp`
- `src/engine/storage/include/structdb/storage/recovery_open_policy.hpp`
- `src/engine/storage/include/structdb/storage/storage_trace.hpp`
- [`Docs/ENGINE_RUNTIME_CONFIG.md`](ENGINE_RUNTIME_CONFIG.md)
- `Docs/OPTIMIZATION_PLAN.md`
- `Docs/ARCHITECTURE.md`
