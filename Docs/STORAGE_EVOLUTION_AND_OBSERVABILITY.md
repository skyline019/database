# 存储演进与观测命名（与 `NEXT_REFACTOR_RECOMMENDATIONS.md` §13.3 对齐）

本文将 **MemTable / Compaction / 恢复与 Embed / 观测** 四条远期主线收口为**可执行的工程约定**：哪些已在代码落地、哪些保留为下一阶段、哪些仅文档化以避免无约束扩散。

---

## 1. MemTable：arena / 分片、默认后端、更激进结构

| 主题 | 状态 | 说明与锚点 |
|------|------|------------|
| **可插拔后端** | **已落实** | `MemTableBackend`（`Map` / `SkipList`）、`MemTableManager::reset_to_backend`（`open` 重放前）、`EngineConfigSnapshot::memtable_backend`。 |
| **默认后端** | **已切换为 `SkipList`** | `EngineConfigSnapshot`、`StorageEngine::memtable_backend_`、`MemTableManager` 默认构造与内部 `backend_` 默认均为 **`SkipList`**；仍可通过配置或 `set_memtable_backend(MemTableBackend::Map)` 显式使用 `std::map` 路径。 |
| **arena / 分片** | **远期** | 不改变当前 `put`/`get` 契约；若引入 arena，应落在 **`MemTable` / `MemTableSkipList` 私有分配器** 或独立 `MemTableArena`，并配套 `structdb_tests` 与 flush 物化路径回归。 |
| **更激进结构** | **远期** | 与调度器预算、`visit_prefix` 有序性语义一并评审后再定。 |

---

## 2. Compaction：`CompactionResult` 与 job 产品化

| 主题 | 状态 | 说明与锚点 |
|------|------|------------|
| **快照结构** | **已落实** | `compaction_snapshot.hpp`（L0 / tiered pair）。 |
| **`CompactionResult` 统一体** | **类型已引入** | `compaction_result.hpp`：`ok` + `error` + `copy_error_to`；**逐步**将 `CompactionCoordinator` / worker 路径上的 `bool` + `error_out` 收敛到该类型（避免单次巨型 diff）。 |
| **job 对象产品化** | **部分** | `StorageEngine::CompactionWorkerJob`（`packaged_task`）仍为引擎内嵌结构；若外提，建议与 `CompactionResult` 同一 TU 族，便于 worker 与内联 drain 共用。 |

---

## 3. 恢复：策略对象化与 Embed journal 编排

| 主题 | 状态 | 说明与锚点 |
|------|------|------------|
| **阶段枚举 + 编排** | **已落实** | `StorageRecoveryPhase`、`RecoveryCoordinator` 分步 + `SpanGuard`（前缀见 §4）。 |
| **轻量策略对象** | **已落实** | `recovery_open_policy.hpp`：`RecoveryOpenPolicy`、`kRebuildUndoStackFromLog` 与 `StorageEngine::kOpenFlagRebuildUndoStackFromLog` **数值同源**；`replay_checkpoint_and_wal` 使用 `recovery_policy.rebuild_undo_stack_from_log()`。 |
| **WAL 帧解码** | **已落实** | `WalReplayApplier`（`wal_replay_applier.*`）。 |
| **与 Embed journal 统一编排** | **文档权威** | 存储侧：`POLICY.md` §3、`WAL_REPLAY.md`、`recovery_coordinator.cpp`；Embed 侧：`embed_client.*`、`CHANGELOG` 中 journal 条目。跨子系统「谁先 replay、谁 trim」**以 `POLICY` + 专文为准**，不在此重复长表格；若未来合并为单一 `RecoveryOrchestrator` 文档，应在此文添加链接替换本节。 |

---

## 4. 观测：trace / benchmark / `StoragePressureSnapshot` 前缀

| 类别 | 约定 | 说明 |
|------|------|------|
| **Trace（`STRUCTDB_TRACE=1`）** | **`stdb.storage.*`** | 存储引擎内 `SpanGuard` 已使用 `structdb::storage::trace` 常量（`stdb.storage.open`、`stdb.storage.flush_memtable`、`stdb.storage.compact_merge_*` 等）；**`mdb.*` / `embed.*` 前缀不变**，避免与客户端 span 混淆。 |
| **Benchmark** | **`BM_StdbStorage*`** | `benchmarks/engine_bench.cpp` 中引擎向 benchmark 已统一 **`BM_StdbStorage`** 前缀；其它 bench（`BM_MemTablePut` 等）可按域逐步迁移。 |
| **`StoragePressureSnapshot` 字段** | **保持现有 JSON 名** | MDB `SHOW STORAGE` / `SHOW TUNING JSON` 已依赖字段名；**不在此轮做重命名**；若将来收敛，应走 **版本化 JSON** 或并列别名字段，并更新客户端。 |

---

## 5. 回归

- 全量：`ctest -C Release --output-on-failure`（含 `structdb_tests`、`mdb_tests`）。
- MemTable 默认：`Engine` 单测可断言 `EngineConfigSnapshot::memtable_backend == SkipList`（见 `structdb_tests`）。
