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
| MDB 增量 persist | `mdb_incremental_persist` | `true` | 小批量 dirty 仅写脏行 + row_index；见 [`PHASE39_PERSIST_PERF.md`](phases/PHASE39_PERSIST_PERF.md)。 |
| MDB 线格式 | `mdb_wire_encoding`（`Hex` / `Wire2`） | `Hex` | `Wire2` 为 `mdbwire2:` 长度前缀；persist 批次在 Wire2 下跳过文本 journal（WAL 权威）。 |
| MDB 合并落盘 | `mdb_persist_coalesce`、`mdb_persist_coalesce_max_dirty_rows` | `false` / `0` | 脚本 / `MdbRunOptions::persist_coalesce`；`FLUSH PERSIST` 或脚本结束刷盘；DDL（空 schema / schema_dirty）仍立即 persist。 |
| MDB 批量导入 | `mdb_bulk_import_mode` | `false` | `IMPORT MODE ON` 或 `MdbRunOptions::bulk_import_mode`；跳过写入期 `kSecIdx`，结束 `REBUILD INDEX`。 |
| Embed journal（导入） | `embed_journal_skip_until_commit` | `false` | 导入模式可延迟 session.journal；WAL 仍为恢复权威。 |
| Embed journal 二进制 | `embed_journal_binary` | `false` | 预留；默认仍为 tab 分隔文本。 |
| 存储批量 undo | `storage_batch_undo_lookup` | `true` | `commit_embed_batch` 单遍 undo 查找。 |
| MDB 脚本 bulk 摊销 | `mdb_script_amortize_bulk_dml` | `true` | 脚本 `BULKINSERT*` 推迟 persist；REPL/事务内语义见 PHASE40。 |
| MDB plain 行落盘 | `mdb_bulk_persist_plain_rows` | `true` | bulk 行免 `mdbhex1:`；大导入配合 `write_journal=false`。 |
| MDB 分块 persist | `mdb_persist_chunk_max_puts`、`mdb_persist_chunk_max_frame_bytes` | `32768` / `8MiB` | 大表 EOF 多帧 WAL；元数据仅末帧；多表须分表 `FLUSH PERSIST`。 |
| 导入 raw 落盘 | `storage_import_store_raw_logical` | `false` | `mdb_bulk_import_mode` 时 persist 自动开启。 |
| 导入 skip undo | `storage_import_batch_skip_undo` | `true` | 与 raw/plain 导入批配合；非导入勿默认依赖。 |
| Embed 批分帧 | `storage_embed_batch_max_frame_bytes` | `0` | 存储层巨型 `commit_embed_batch` 兜底拆分。 |
| MemTable 批量 put | `memtable_bulk_put_enabled` | `false` | 导入批内排序 + `reserve_capacity`（opt-in）。 |
| MDB 链 txn 观测 / 严格拒绝 | `observe_embed_bypass_*`、`strict_reject_direct_kv_put_*` | `false` | Phase 24A。 |
| 每次写入 fsync | `fsync_every_write` | `false` | 更强耐久，性能代价高。 |
| WAL trim / archive GC / undo truncate | `wal_auto_trim_*`、`wal_archive_gc_after_flush`、`undo_auto_truncate_after_flush` | 多为 `false` | 与 checkpoint 顺序相关。 |

---

## 4. 部署预设（Wave 1：OLTP / LSM / bulk）

在 **`Engine::startup` 之前** 通过 `EngineConfigSnapshot` 合并下列组合（字段见 [`config.hpp`](../src/engine/facade/include/structdb/facade/config.hpp)）。

### 4.1 OLTP（单行 DML / 交互 REPL）

| 字段 | 建议值 |
|------|--------|
| `mdb_incremental_persist` | `true`（默认） |
| `mdb_bulk_import_mode` | `false` |
| `mdb_script_amortize_bulk_dml` | `false`（仅当 REPL 不用 bulk 脚本时可关；默认 `true` 不影响单行 DML） |
| `mdb_bulk_persist_plain_rows` | 默认即可（bulk 专用） |
| `l0_compact_defer_after_flush` | `false` 或 **桌面** 档 |

基线：`scripts/bench/oltp_persist_micro.ps1`；门禁 P99 ≤ **1.2×** [`oltp_persist_baseline.json`](../benchmarks/baselines/oltp_persist_baseline.json)。

### 4.2 LSM 尾延迟（三档）

| 档位 | `l0_compact_defer_after_flush` | `l0_compact_max_inline_rounds_per_flush` | `enable_compaction_worker` | 说明 |
|------|------------------------------|------------------------------------------|----------------------------|------|
| **开发** | `true` | `0` 或 `1` | `false` | flush 后延后 drain，便于调试写路径 |
| **压测** | `true` | 默认 | `true` | worker + `drain_pending_l0_compactions`；观察 `SHOW STORAGE` **`pending_deferred_l0`** |
| **桌面** | `false` | `2`～默认 | 可选 `true` | 平衡 flush P99；见 [`COMPACTION.md`](COMPACTION.md) |

`SHOW STORAGE` / JSON 中 **`pending_deferred_l0_compact`** 与 [`COMPACTION.md`](COMPACTION.md)、[`PHASE21.md`](phases/PHASE21.md) §21C 压力联动；**无** `last_merge_throttle_ns` 字段（节流见 `compaction_merge_*_bytes_per_second`）。

### 4.3 Bulk 导入（四十期）

| 字段 | 默认脚本 | `--mdb-bulk-import` |
|------|----------|---------------------|
| `mdb_script_amortize_bulk_dml` | `true` | `true` |
| `mdb_bulk_import_mode` | 脚本 `IMPORT MODE` 或 CLI | `true` |
| `mdb_bulk_persist_plain_rows` | `true` | `true` |
| `mdb_persist_chunk_max_puts` / `mdb_persist_chunk_max_frame_bytes` | 32K / 8MiB | 同左 |
| `storage_import_batch_skip_undo` | `true`（导入批） | `true` |

门禁：`scripts/weekly_bench.ps1` 或 `mega_data_mdb_stress.ps1` 1M 行，TPS ≥ 矩阵 §7.1 **90%**（~214K 默认脚本）。可选 **`-SampleWorkingSet`** 将进程峰值 `WorkingSet64` 写入 summary JSON（`peak_working_set_bytes`，PHASE44 内存验收）。

**命名索引（PHASE41）**：导入期仍跳过隐式 `kSecIdx`；`FLUSH PERSIST` 后 **`REBUILD INDEX`** 重建行索引、隐式 string 侧车与 **`CREATE INDEX` 命名 postings**。大表可在导入结束再 `CREATE INDEX`（全表扫描建 postings）。

### 4.5 MDB 事务 profile（Wave 3，不改默认）

与 [`ONBOARDING.md`](ONBOARDING.md) 三档一致，启动前在 `EngineConfigSnapshot` 选定：

| Profile | `mdb_persist_in_begin` | `mdb_chain_rollback_on_mdb_rollback` | 场景 |
|---------|------------------------|--------------------------------------|------|
| `session_only`（默认） | `true` | `false` | 桌面 OLTP |
| `chain_rollback` | `true` | `true` | 需要 MDB ROLLBACK 弹 undo |
| `bulk_import` | `true` | `false` | + `mdb_bulk_import_mode` / `IMPORT MODE` |

### 4.6 读放大基准（Wave 4 注记）

`structdb_bench` 的 **`BM_StdbStorageVisitPrefix`** 与 [`benchmarks/baselines/structdb_bench_baseline.json`](../benchmarks/baselines/structdb_bench_baseline.json) 对比，用于 LSM 读放大趋势观察（非合入门禁）。Compaction 组合仍见 [`COMPACTION.md`](COMPACTION.md) 与 §4.1–4.3。

### 4.7 PITR 链校验

| 字段 | 默认 | 说明 |
|------|------|------|
| `checkpoint_chain_strict` | `false` | `true` 时 `Engine::startup` 在 chain 与 `read_latest` 不一致时失败 |

### 4.8 `commit_embed_batch` 小批分帧（审计摘要）

- **`storage_embed_batch_max_frame_bytes`** 默认 **`0`**（不拆分）；巨型批次才在存储层按帧上限拆分 WAL。
- 典型 OLTP 单帧键数 **≪** `mdb_persist_chunk_max_puts`（32K）；小批主要成本在 **WAL fsync / `persist_table` / undo**，而非分帧。
- 调大 `storage_embed_batch_max_frame_bytes` 仅用于 **超大 embed 批** 兜底，见 PHASE40 与 `StorageEngine.CommitEmbedBatchAutoSplitByFrameBytes` 测试。

---

## 5. 回归入口

- `ctest -C Release --output-on-failure -R "structdb_tests|mdb_tests"`
- MDB 全量：`structdb_tests --gtest_filter=Mdb.*`（**151** 项，含 **Phase40** 24、**Phase41** 9、**Phase42** 3、**Phase43** 1、**Phase44–45** 与 Wave0–1 smoke）
- OLTP 基线：`scripts/bench/oltp_persist_micro.ps1`；bulk 周门禁：`scripts/weekly_bench.ps1`
- PHASE40 切片：`--gtest_filter=Mdb.Phase40*`；存储 WAL：`StorageEngine.WalReplayImportRaw*:StorageEngine.WalReplay*Chunked*`
- 导入压测：`scripts/bench/mega_data_mdb_stress.ps1`（`-ImportMode`、`-EngineBulkImport`、`-MemtableBulkPut` 等）
- 调度与图：`structdb_tests` 中 `GraphExecutor.*`、`Orchestrator.*`、`Engine.*Phase19*` 等。

---

## 6. 相关代码路径

- `Engine::sync_scheduler_budget_from_storage_pressure`、`Engine::storage_pressure_snapshot`：`engine.cpp`
- `Orchestrator::set_before_graph_execute`：`orchestrator.{hpp,cpp}`
- `GraphExecutor::execute(..., use_budget_probe)`：`graph_executor.cpp`
- `StorageEngine::read_storage_pressure_snapshot`：实现 **`storage_telemetry.cpp`**（声明 `storage_telemetry.hpp`）；`storage_engine_compaction_lsm.cpp` 中为对外 API 委托
