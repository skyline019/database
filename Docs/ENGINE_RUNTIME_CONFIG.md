# 引擎运行时 / 启动期 / 实验配置清单

本文档对应 [`NEXT_REFACTOR_RECOMMENDATIONS.md`](NEXT_REFACTOR_RECOMMENDATIONS.md) §9.2：把分散在 `EngineConfigSnapshot`、`StorageEngine::set_*`、`MDB SHOW TUNING` 等入口的开关按**生命周期**归类，便于组合与回归。

**权威字段定义**：[`src/engine/facade/include/structdb/facade/config.hpp`](../src/engine/facade/include/structdb/facade/config.hpp)（`EngineConfigSnapshot`）。**存储侧仅 `open` 前生效**的项以 `StorageEngine::set_*` 注释为准（见各 `storage_engine*.cpp` / `storage_engine.hpp`）。

---

## 1. 运行时安全 / 热路径可调（`Engine::startup` 之后仍可能生效）

| 能力 | 配置字段 / API | 默认 | 说明 |
|------|----------------|------|------|
| WAL fsync 最小间隔 | `wal_fsync_min_interval_ms` → `StorageEngine::set_wal_fsync_min_interval_ms` | `0`（关） | 成功 fsync 之间的墙钟间隔下限。 |
| L0 merge 最小间隔 | `compaction_merge_min_interval_ms` | `0` | 连续成功 L0 merge 之间的间隔。 |
| Merge 字节桶 | `compaction_merge_max_bytes_per_second` / `compaction_merge_burst_bytes` | `0` | materialize I/O 节流。 |
| WAL 追加字节桶 | `wal_append_max_bytes_per_second` / `wal_append_burst_bytes` | `0` | `WalWriter` 侧。 |
| 顺序读 SST / 低优先级线程 / 专用 I/O 池 | `compaction_sequential_sst_read` 等 | 见头文件 | merge 行为与线程调度。 |
| 调度器 ← 存储压力 | `storage_pressure_*` 系列 | 多为 `0`/false | `Engine::sync_scheduler_budget_from_storage_pressure` 读 `StoragePressureSnapshot` 后收紧 `WalQueueDepth` / `CompactionSlots` 有效上限。 |
| 执行图预算探测 | （固定）`Orchestrator` 在每次 `run_default` / `replan_and_run` 前调用可选 `before_graph_execute`；`GraphExecutor::execute(..., use_budget_probe=true)` | — | Facade 将 `before_graph_execute` 设为 `sync_scheduler_budget_from_storage_pressure`，使 **GraphExecutor 的 Wal/MemTable/Compaction 探测**与压力快照对齐。 |

---

## 2. 启动期结构开关（须在 `Engine::startup` / `StorageEngine::open` 前配置）

| 能力 | 配置字段 | 默认 | 说明 |
|------|----------|------|------|
| 数据目录 / 打开标志 | `data_dir`、`storage_open_flags` | `_data` / `0` | `storage_open_flags` 见 `StorageEngine::kOpenFlag*`。 |
| 独占目录建议锁 | `exclusive_data_dir_lock` | `false` | Phase 35。 |
| WAL / undo 段滚动上限 | `wal_segment_roll_max_bytes`、`undo_segment_roll_max_bytes` | `0` | `0` 表示不滚动。 |
| WAL I/O 后端 | `io_backend` | 默认构造 | `set_wal_io_backend`，须在 `open` 前。 |
| **MemTable 内存实现** | `memtable_backend`（`Map` / `SkipList`） | **`SkipList`** | 在 `Engine::startup` 调用 `StorageEngine::open` **之前**由 Facade 传入；`open` 重放 WAL 前 `MemTableManager::reset_to_backend` 生效。 |
| LSM 层与 flush 行为 | `l1_compact_output_from_l0_merge`、…、`l0_compact_defer_after_flush` 等 | 多为 `false` | 改变 manifest / flush 后是否 drain。 |
| Compaction worker | `enable_compaction_worker`、`compaction_worker_queue_depth` | `false` / `64` | `start_compaction_worker`。 |
| Facade 异步 `kv_put` 队列 | `kv_put_async_queue_depth` | `0` | `0` = 同步 `storage_->put`。 |

---

## 3. 实验 / 语义敏感（文档与单测约束更强）

| 能力 | 配置字段 | 默认 | 说明 |
|------|----------|------|------|
| MDB `BEGIN` 内 persist | `mdb_persist_in_begin`、`mdb_chain_rollback_on_mdb_rollback` | `true` / `false` | 见 `POLICY.md`、`TXN_BEGIN_PERSIST_DESIGN.md`。 |
| MDB 链 txn 观测 / 严格拒绝 | `observe_embed_bypass_*`、`strict_reject_direct_kv_put_*` | `false` | Phase 24A。 |
| 每次写入 fsync | `fsync_every_write` | `false` | 更强耐久，性能代价高。 |
| WAL trim / archive GC / undo truncate | `wal_auto_trim_*`、`wal_archive_gc_after_flush`、`undo_auto_truncate_after_flush` | 多为 `false` | 与 checkpoint 顺序相关。 |

---

## 4. 回归入口

- `ctest -C Release --output-on-failure -R "structdb_tests|mdb_tests"`
- 调度与图：`structdb_tests` 中 `GraphExecutor.*`、`Orchestrator.*`、`Engine.*Phase19*` 等。

---

## 5. 相关代码路径

- `Engine::sync_scheduler_budget_from_storage_pressure`、`Engine::storage_pressure_snapshot`：`engine.cpp`
- `Orchestrator::set_before_graph_execute`：`orchestrator.{hpp,cpp}`
- `GraphExecutor::execute(..., use_budget_probe)`：`graph_executor.cpp`
- `StorageEngine::read_storage_pressure_snapshot`：实现 **`storage_telemetry.cpp`**（声明 `storage_telemetry.hpp`）；`storage_engine_compaction_lsm.cpp` 中为对外 API 委托
