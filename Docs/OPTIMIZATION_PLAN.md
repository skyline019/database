# StructDB 性能优化路线图

本文档给出 StructDB 的**全维度性能优化路线图**，并按 **1 周 / 1 月 / 1 季度** 拆解落地顺序。目标不是单点提速，而是同时改善：

- 写入吞吐
- 读延迟与 P99 稳定性
- 空间放大与写放大
- 恢复时间与崩溃恢复可靠性
- 后台 compaction 对前台的干扰
- GUI / C API / Embed 的交互体验
- 代码可维护性与可观测性

本方案与 [`ARCHITECTURE.md`](ARCHITECTURE.md) 配套，实施时以 [`POLICY.md`](POLICY.md) 和各 `phases/PHASE*.md` 为约束优先级。

---

## 0. 实施进度（与代码同步维护）

**约定**：每次合入与本路线图相关的优化或观测改动，应更新 **日期（YYYY-MM-DD）**、**域**、**简述**，并尽量补充 **回归入口**（`structdb_tests` / `mdb_tests` / bench 命令），便于对照 benchmark 与 CI。

### 0.1 里程碑快照（2026-05-14）

以下为一轮「**观测 + 存储读路径 + MDB 分页 + Embed 耐久 + 回归加固**」的集中交付，可作为后续优化的基线锚点。

| 主题 | 交付要点 | 主要代码位置 |
|------|-----------|----------------|
| 写路径节流 | `COMMIT_SEQ` 不在每次 `put`/embed batch 后刷盘；在 `open` 收尾、`close`、flush、checkpoint、compaction 等边界刷新 | `storage_engine_put_undo.cpp` 等 |
| SST 读放大 | **v2**：min/max footer；**v3**：`STDBSST3` + 64B Bloom；**flush/compaction** 统一 `write_sst_sorted_entries`；legacy / v2 仍可读 | `storage_engine_detail.cpp`、`storage_engine_compaction_lsm.cpp` |
| MDB 大表分页 | 主键 `id`：`partial_sort` 当前页前缀；**其它排序列**：对前 `min(start+page_size, n)` 行 `partial_sort`（与全表 `sort` 后切片等价），避免非 id 排序时整表 O(n log n) | `mdb_query_paging.cpp` |
| Embed 耐久 | `session.journal`：`FileWriter` 会话内追加；`append_journal_line(..., fsync)` → `write_all` + 可选 `sync()` | `embed_client.cpp`、`embed_client.hpp` |
| 可观测 | `STRUCTDB_TRACE=1` → `EnvTracer`；`SpanGuard` 覆盖 flush、`mdb.PAGE`/`PAGE_JSON`、`embed.submit`、**L0/L1/L2/L3 合并**、**`drain_l0_compactions`**、**compaction worker 单任务** | `tracer.cpp`、`storage_engine_compaction_lsm.cpp`、`storage_engine_segments_worker_checkpoint.cpp` |
| Compaction materialize | **可选** `CompactionIoExecutor`：**多 worker 共享队列**（线程名 `structdb_cio{0..N-1}`，默认可配 `compaction_io_pool_threads`，`0` 表示默认 **2**）；merge 读两 SST + 写临时 SST 的大循环在专用池上执行；可选**并行读两路输入 SST**（`compaction_parallel_sst_reads`）；**分块 I/O** + `compaction_merge_max_bytes_per_second>0` 时**按块**扣字节桶（`read_all_chunked` / `write_all_chunked`，节流与并行读路径由 **`SteadyClockByteTokenBucket`**（`byte_token_bucket.*`）在 `StorageEngine` 侧按块扣桶；**merge materialize 不经过 `WalWriter`**（与 WAL fd/追加路径分离，见 `wal.hpp` `WalPipeline` 注释）；`close`：`stop_compaction_worker` → `shutdown_compaction_io_executor`；**Linux** 上 worker（`structdb_cmpw`）与 CIO 在 `compaction_worker_low_priority_thread` 为真时可 `nice`+尽力 `SCHED_IDLE` | `compaction_io_executor.{hpp,cpp}`、`thread_compaction_sched.{hpp,cpp}`、`file_handle.{hpp,cpp}`、`storage_engine_detail.cpp`、`storage_engine_compaction_lsm.cpp`、`storage_engine_open_wal.cpp`、`storage_engine_segments_worker_checkpoint.cpp`、`wal.hpp` |
| 回归 | 严格嵌套 txn + `PAGE`/`PAGE_JSON` + 冷启动（recover / commit 两路径）；**Embed**：WAL 清空后 **journal 重放**、损坏 `session.journal` / `session.ckpt` 应失败、`wal_append_max_bps` 节流计数、同 batch **del+put**、空 `commit_embed_batch`；**Compaction**：专用 I/O / 分块 / Engine 配置 / defer+worker / L1→L2 等见 §0.2 末行；`ctest` 注册 `mdb_tests` | `tests/mdb_tests.cpp`、`tests/structdb_tests.cpp`、`tests/CMakeLists.txt` |
| 压力快照 | **compaction 成功合并累计**、**worker 入队/完成任务累计**（与 `compaction_merge_count()` 对齐）；`SHOW TUNING JSON` / `SHOW STORAGE JSON` 透出 | `storage_pressure.hpp`、`StorageEngine::read_storage_pressure_snapshot`（→ `StorageTelemetry` / `storage_telemetry.*`）、`mdb_runner_dispatch.inc` |

### 0.2 已落地清单（按域）

#### 存储引擎（`StorageEngine` / SST / WAL）

| 日期 | 项 | 说明 |
|------|-----|------|
| 2026-05-15 | **MemTable 抽象 + flush 协调类** | `IMemTable`、`MemTable`（`std::map`）、`MemTableManager`（active/frozen、`begin_flush`/`merge_frozen_into_active_and_clear`）；`StorageEngine` 成员由裸 `mem_`+`mem_flush_snapshot_` 迁入 manager。 |
| 2026-05-15 | **MemTable：`SkipList` 后端 + 配置** | `MemTableSkipList`；`MemTableBackend`；`MemTableManager::reset_to_backend`（WAL 重放前）；`EngineConfigSnapshot::memtable_backend`；`structdb_tests`：`MemTableSkipList.*`、`Engine.StartupWithMemTableSkipListBackend`。 |
| 2026-05-15 | **Compaction 快照头 + merge 字节桶** | `CompactionL0MergeSnapshot` / `CompactionTieredPairSnapshot`（`compaction_snapshot.hpp`）；compaction materialize 节流用 `SteadyClockByteTokenBucket`（`byte_token_bucket.*`），替代引擎内联 token 状态机。 |
| 2026-05-15 | **`open` 恢复阶段化 + `RecoveryCoordinator`** | `StorageRecoveryPhase` + `storage_recovery_phase_cstr`（`recovery_phase.*`）；**`RecoveryCoordinator`**：`prepare_directories_tmp` / `load_segment_catalogs` / `open_log_files_manifest_commit_seq` / `replay_checkpoint_and_wal` / `refresh_segment_observability` + 独占目录锁；**WAL 帧解码**委托 **`WalReplayApplier`**（`wal_replay_applier.*`）；`storage_engine_open_wal.cpp` 仅构造/`open`/`close`；各阶段 `SpanGuard`。 |
| 2026-05-15 | **`WalReplayApplier`（WAL 重放解码）** | `apply_line_unlocked` / `apply_batch_unlocked`；`StorageEngine` `friend` + 成员（位于 `RecoveryCoordinator` 之前）；`structdb_storage` CMake 增加 `wal_replay_applier.cpp`。 |
| 2026-05-15 | **`RecoveryOpenPolicy` + `CompactionResult` 类型锚点** | `recovery_open_policy.hpp`（与 `kOpenFlagRebuildUndoStackFromLog` 同源）；`compaction_result.hpp`（合并路径统一返回体，渐进迁移）。 |
| 2026-05-15 | **MemTable 默认 `SkipList` + 演进专文** | `EngineConfigSnapshot` / `StorageEngine` / `MemTableManager` 默认 **`MemTableBackend::SkipList`**；远期 arena/job 等见 [`STORAGE_EVOLUTION_AND_OBSERVABILITY.md`](STORAGE_EVOLUTION_AND_OBSERVABILITY.md)。 |
| 2026-05-15 | **观测命名：`stdb.storage.*` trace + `BM_StdbStorage*`** | `storage_trace.hpp`；`recovery_coordinator` / `compaction_coordinator` / `storage_engine_*` 内 `SpanGuard`；`benchmarks/engine_bench.cpp`。 |
| 2026-05-15 | **WAL / Compaction / Checkpoint-Undo 协调器** | **`WalCoordinator`**（`wal_coordinator.*`）；**`CompactionCoordinator`**（`compaction_coordinator.*`，物化与 manifest 提交）；**`CheckpointUndoCoordinator`**（`checkpoint_undo_coordinator.*`）；`StorageEngine` `friend` + 末字段委托。 |
| 2026-05-15 | **`StorageTelemetry`（压力快照）** | `read_storage_pressure_snapshot` 实现迁至 **`storage_telemetry.{hpp,cpp}`**；`StorageEngine::read_storage_pressure_snapshot` 委托；`friend`；`structdb_storage` CMake 增加 `storage_telemetry.cpp`。 |
| 2026-05-15 | **Orchestrator 图执行与存储压力闭环** | `Orchestrator::set_before_graph_execute`；`Engine::startup` 绑定为 `sync_scheduler_budget_from_storage_pressure`；`run_default` / `replan_and_run` 在 `GraphExecutor::execute` 前调用；**`use_budget_probe=true`** 使 Wal/MemTable/Compaction 预算探测与压力收紧后的 ceiling 一致。 |
| 2026-05-15 | **MDB `PAGE` / `PAGE_JSON` 独立编译单元** | 实现迁至 **`mdb_query_paging.cpp`**（声明在 `mdb_query_paging.hpp`）；`mdb_ops_pages_journal_import.cpp` 保留 `IMPORTDIR`/`SHOWLOG`、CSV 括号解析等；`compare_ids` 仅分页 TU 内使用。 |
| 2026-05-15 | **MDB 逻辑 / 持久化 / 分页头文件** | `mdb_logical_table.hpp`、`mdb_persistence.{hpp,cpp}`（原 `mdb_ops_persist_load`）、`mdb_query_paging.hpp`（`MdbQueryPagingState`）；`mdb_runner.hpp` 聚合导出；`MdbDispatchEnv::paging`。 |
| 2026-05-15 | **GraphExecutor 背压与诊断** | 预算探测 **MemTableBytes ∝ `OperatorNode::estimated_cost`**（有上下限）；`scheduler::backpressure_reason_cstr`；失败 `error_out` 形如 `backpressure:MemTableFull` 前缀段；可选 **`GraphExecuteDiagnostics`**（`GraphExecuteOutcome` + `BackpressureReason` + `failed_node_id`）。`structdb_tests` 背压用例断言诊断字段。 |
| 2026-05-14 | **COMMIT_SEQ 落盘节流** | 热路径不再每次 `put`/rollback/embed batch 后写 `COMMIT_SEQ`；边界刷新见 `open`/`close`/flush/checkpoint/compaction。 |
| 2026-05-14 | **SST v2 + min/max** | 魔数 `STDBSST2`、body、footer；`get`/`visit_prefix` 可跳过无关 SST 扫描；兼容旧格式。 |
| 2026-05-14 | **SST v3 + Bloom** | 新写入 `STDBSST3`，footer 含 64B Bloom；`sst_get_key` 点查可走 Bloom；`visit_prefix` **不用** Bloom（避免假阴性）；**L0 flush** 与 compaction 均写 v3（不再 flush 裸 legacy 流）。 |
| 2026-05-14 | **存储压力快照扩展** | `StoragePressureSnapshot`：WAL 追加/fsync、flush/checkpoint；**`compaction_merge_success_total`**（与 `compaction_merge_count()` 一致）、**`compaction_worker_tasks_{submitted,completed}_total`**；`read_storage_pressure_snapshot`；MDB `SHOW STORAGE` / `SHOW TUNING JSON` / `SHOW STORAGE JSON` 扩展；`structdb_tests`：`StoragePressureWalAndFlushCounters`、`StoragePressureCompactionMergeMatchesCounter`、`StoragePressureWorkerSubmitCompleteCounters`。 |
| 2026-05-14 | **L0 compaction 最小间隔（尾延迟平滑）** | `EngineConfigSnapshot::compaction_merge_min_interval_ms` → `StorageEngine`：连续成功 L0 merge 之间 `sleep_until`；**`compaction_merge_throttle_sleep_ns_total`** 进压力快照；MDB `SHOW TUNING` / `SHOW STORAGE*` 透出；单测 `CompactionMergeMinIntervalAddsThrottleSleepToSnapshot`。 |
| 2026-05-14 | **Compaction I/O 隔离 + 队列 + 字节限速** | **顺序读路径**：`sst_load_all_entries(..., sequential_scan_hint)` + `FileReader::open(..., sequential)`（Win `FILE_FLAG_SEQUENTIAL_SCAN`；Linux `posix_fadvise`）；**worker 线程**：Win `THREAD_MODE_BACKGROUND_BEGIN/END`（`compaction_worker_low_priority_thread`）；Linux 同开关下 `nice`+尽力 `SCHED_IDLE`（见 `thread_compaction_sched`）；**worker 队列**：按 `drain_priority` 高优先、`enqueue_seq` FIFO 挑选任务；**字节桶**：`compaction_merge_max_bytes_per_second` / `burst`，累计 **`compaction_merge_byte_throttle_sleep_ns_total`**（初版按整文件估算；**现** merge materialize 已改为**按分块读/写**扣桶，并与下行「专用 I/O」组合）；`Engine::drain_l0_compaction_queue(..., drain_priority)`（MDB `VACUUM` 使用高优先级）；单测 `CompactionMergeByteThrottleAddsSleepToSnapshot`。 |
| 2026-05-14 | **专用 compaction I/O 线程 + 分块节流** | `compaction_dedicated_io_executor` → `CompactionIoExecutor` 上执行 merge materialize；`sst_load_all_entries` / `write_sst_sorted_entries*` 支持 `read_chunk_bytes` / `write_chunk_bytes` 与按块回调，在 `compaction_merge_max_bytes_per_second>0` 时按块调用字节桶；`compaction_io_chunk_bytes`（`0` 表示在专用 I/O 或字节限速开启时使用默认 256KiB）；`EngineConfigSnapshot` / MDB `SHOW TUNING*` 透出；`structdb_tests`：`CompactionDedicatedIoExecutorAndChunkedByteThrottle`、`CompactionDedicatedIoExecutorNoByteThrottleL0Merge`、`CompactionExplicitIoChunkByteThrottleWithoutDedicatedIo`、`Engine.DedicatedIoAndChunkFromConfigRunsL0Compact`、`CompactionDeferredWorkerDrainUsesDedicatedIoMaterialize`、`CompactionTieredL1ToL2DedicatedIoByteThrottle`。 |
| 2026-05-14 | **Compaction worker / CIO：Linux 调度** | `structdb::infra::apply_compaction_background_thread_scheduling`（`thread_compaction_sched.cpp`）：`pthread_setname_np`、`nice(10)`、`pthread_setschedparam(SCHED_IDLE)`（失败忽略）；compaction worker 线程名 `structdb_cmpw`；`CompactionIoExecutor::start(num_workers, low_priority)` 与 `compaction_worker_low_priority_thread_` 对齐；CIO 线程名 `structdb_cio{i}`。 |
| 2026-05-14 | **Compaction 多线程 I/O 池 + 并行 SST 读 + merge 节流互斥** | `EngineConfigSnapshot::compaction_io_pool_threads` / `compaction_parallel_sst_reads`；`StorageEngine::ensure_compaction_io_executor_` 默认 2 worker（上限 32）；L0 / tiered merge 第二路可选 `std::async`；字节桶由 `SteadyClockByteTokenBucket` 承载（2026-05-15 从引擎内联字段抽出）；`SHOW TUNING*` 透出。 |
| 2026-05-14 | **MDB 增量 `persist_table` + 开关** | `LogicalTable` dirty / prev_cells / schema_dirty；小批量（≤8192 行）且表已存在、无 schema 脏时写增量 batch；`mdb_incremental_persist`（默认 true）；`SHOW TUNING*`；dispatch / txn v2 重放与 `logical_persist_*` 对齐（含 `SETATTRMULTI`）。 |
| 2026-05-14 | **GUI 分页上限** | Tauri `query_page` / `query_page_via_cap_session`：`page_size` 钳制 **1..500**，避免单次过大 `PAGE_JSON`。 |
| 2026-05-14 | **GUI：MDB 脚本进度与停止** | `invoke("cancel_mdb_script")` 置位 `script_cancel`；**`run_script` / `run_script_ex`** 行间检测并在输出中写入 **`[SCRIPT] cancelled`**；**`run_script_ex`** 返回体含 **`cancelled`**（camelCase）；执行过程中 **`emit("mdb-script-progress", { lineDone, totalLines })`**（Tauri 2 要求 payload `Clone`）；前端监听该事件并展示 **「停止脚本」** 与进度 | `gui/rust_gui/src-tauri/src/lib.rs`、`gui/rust_gui/src/App.vue` |
| 2026-05-14 | **MemTable：前缀有序遍历 + 字节计数** | `MemTable::for_each_sorted_prefix`（`lower_bound` 起直至键前缀失配）；`bytes_total_` 在 `put`/`clear` 维护，`bytes_approx()` O(1)；`StorageEngine::visit_prefix` 对 mem 层走前缀路径，缩窄前缀扫描（如 `mdb$*`）成本 | `memtable.{hpp,cpp}`、`storage_engine_read.cpp` |
| 2026-05-14 | **MemTable：flush 双缓冲 + SST 物化锁外** | 语义不变：**2026-05-15** 起由 **`MemTableManager`** 持有 frozen 快照（原 `mem_flush_snapshot_`）；`flush_memtable` 在 `mu_` 内 `swap` 冻结表、记 `wal_at_flush`、清 `undo_stack_`；**锁外**写临时 SST；再持锁 `rename` / `MANIFEST` / `lsm_` / checkpoint；读路径 **active→frozen** overlay；`open`/`close` 丢弃或 merge 冻结层。 |
| 2026-05-14 | **`StorageEngine::mu_` 读写锁拆分** | `std::shared_mutex`：`get`/`visit_prefix`/`read_storage_pressure_snapshot`/`wal_log_bytes_on_disk`/`undo_log_bytes_on_disk`/`read_checkpoint_state`/`undo_stack_depth` 使用 **`std::shared_lock`**；写路径与 compaction 使用 **`std::unique_lock`/`std::lock_guard`（独占）**；`compact_merge_*_with_relock_` 形参改为 `std::unique_lock<std::shared_mutex>&` | `storage_engine.hpp`、`storage_engine_read.cpp`、`storage_engine_put_undo.cpp`、`storage_engine_compaction_lsm.cpp`、`storage_engine_open_wal.cpp`、`storage_engine.cpp`、`storage_engine_segments_worker_checkpoint.cpp`、`wal.hpp`（注释：WAL 节流须在独占锁下调用） |
| 2026-05-14 | **OS 级 I/O 隔离（文档 + Linux WAL hint）** | 运维清单见 **`Docs/OS_IO_ISOLATION.md`**（cgroup/分卷/ionice 等）；Linux 下 **`wal.log` 追加 `FileWriter`** 在 `O_APPEND` 打开成功后 **`posix_fadvise(..., POSIX_FADV_SEQUENTIAL)`**（与 SST 顺序读提示互补，无语义变更） | `Docs/OS_IO_ISOLATION.md`、`file_handle.cpp` |

#### MDB / Embed / 客户端

| 日期 | 项 | 说明 |
|------|-----|------|
| 2026-05-14 | **PAGE_JSON / PAGE 按 id 分页** | 主键 `id`：`partial_sort` 当前页前缀；其它排序列全量排序。 |
| 2026-05-14 | **Embed journal 句柄 + fsync** | `FileWriter journal_w_` 会话复用；`fsync==true` 时 `sync()`。 |
| 2026-05-14 | **MDB 严格嵌套回归** | `TxnStrictNestedSavepointsPageJsonRecoverRestart`（SAVEPOINT 栈 + 40 行 + `PAGE`/`PAGE_JSON` + txn v2 `[RECOVER]` + `ROLLBACK`）；`TxnStrictNestedSavepointsPageJsonCommitRestart`（`COMMIT` 后冷启动 `USE`+`COUNT`）。 |
| 2026-05-14 | **MDB `SCAN` 游标分页** | **`SCAN MORE`** / **`SCAN MORE(n)`**（`n`∈1..5000，默认 500）按 `current.rows` 迭代序续打；**`SCAN RESET`** 清零游标；**`USE(...)`** 成功后游标归零；脚本与 REPL 各自维护 `scan_cursor_ordinal`（`MdbDispatchEnv`）；单测 **`Mdb.ScanMoreCursorPaging`** | `mdb_command_parser.cpp`、`mdb_runner_dispatch.inc`、`mdb_dispatch_env.hpp`、`mdb_runner_internal.hpp`、`mdb_runner.cpp`、`mdb_dispatch.cpp`、`tests/mdb_tests.cpp` |
| 2026-05-14 | **Embed 崩溃 / 损坏 / 嵌套回归** | `JournalReplayWhenWalEmptyAfterFlush`；`OpenFailsOnJournalBadSeqField` / `OpenFailsOnJournalInvalidPutFieldCount` / `OpenFailsOnTruncatedJournalSecondLine`；`OpenFailsWhenSessionCkptSecondLineNotU64` / `ThirdLineNotU64`；`NestedDelThenPutBatchSurvivesJournalReplayAfterWalTrim`；`SubmitRejectsTabInIdempotencyToken`；`RecoverWhenNotOpenReturnsFalse`；`Engine.NestedWalThrottleLowBpsMultiPutMonotonicCounters`；`StorageEngine.CommitEmbedBatchEmptyIsNoop`。 |

#### 基准与可观测

| 日期 | 项 | 说明 |
|------|-----|------|
| 2026-05-14 | **Benchmark：引擎路径** | `benchmarks/engine_bench.cpp` → `structdb_bench`（put/get/前缀、flush、open 重放、**`BM_StdbStoragePressureSnapshot`** 等）。 |
| 2026-05-14 | **tracing 运行时开关** | `STRUCTDB_TRACE=1` → `trace_install_from_env_once()` / `EnvTracer`；`Engine::startup` 安装。 |

### 0.3 回归与本地命令

| 目标 | 命令 |
|------|------|
| 全量单测（含 MDB + C API + Storage） | `ctest -C Release --output-on-failure`（或 Debug 配置） |
| 仅 MDB 套件（`Mdb.*`） | `ctest -C Release -R "^mdb_tests$"` → 等价 `structdb_tests --gtest_filter=Mdb.*` |
| Compaction 专用 I/O / 分块节流相关用例 | `structdb_tests.exe --gtest_filter=StorageEngine.CompactionDedicatedIo*:StorageEngine.CompactionExplicitIo*:StorageEngine.CompactionDeferredWorker*:StorageEngine.CompactionTieredL1ToL2*:Engine.DedicatedIo*`（Windows 下可执行文件在 `build/tests/Release/`） |
| 运行时 trace | 环境变量 `STRUCTDB_TRACE=1` 后跑业务或单测，日志前缀 `[structdb trace]` |

**Embed `session.journal` 恢复与单测约定**（避免回归用例误报）：

- `EmbedClient::parse_journal_for_recovery` 在 **`wal_log_bytes_on_disk() > 0`** 时认为 WAL 侧已（或将）承载权威变更：仍解析每行（如 idempotency token），但**不调用** `apply_fields_after_ack`。因此**磁盘上仍有 WAL 时，手工写入的损坏 journal 可能被跳过，`open` 仍成功**——断言「非法 journal 必须失败」的用例须先保证 **WAL 字节为 0**（例如 `startup` 后、无未 flush 写，或先走到 flush/trim 语义）。
- 每行若 **`seq <= last_ack_`**（`last_ack_` 来自 `session.ckpt` 首行，无 ckpt 时为 **0**），会 **continue** 且不进入 `apply_fields_after_ack`。覆盖「损坏 put 字段数」等错误时，**`seq` 必须大于当前 `last_ack_`**，否则整行被跳过、**`open` 会误通过**。
- 本地改测后请 **`cmake --build ... --target structdb_tests`** 再跑 `structdb_tests.exe`，避免命中未重编的旧二进制。

**CTest 全量回归（2026-05-14）**：`ctest -C Release --output-on-failure` 当前注册的 **3** 项（`structdb_tests`、`mdb_tests`、`structdb_capi_shared_smoke`）均已通过。

### 0.4 下一批（原 §8 优先级，滚动维护）

- [ ] **MemTable 结构**：arena / 更细锁（§6.1，中长期）；**已做**：**flush 双缓冲**（现由 **`MemTableManager`** 持有 frozen；锁外 SST 物化）、`IMemTable`/`MemTable`（`std::map`）、前缀/overlay、`bytes_approx`；**map 后端替换**仍待。
- [ ] **GUI**：表格侧仍可按产品需要收紧「禁止全表一次拉取」（§4.3）；**已做**：模块级 `MDB_PAGE_JSON_MAX_PAGE_SIZE`，`query_page` / cap 会话 / `find_row_map_by_id_via_query_page` 统一钳制 `page_size`；**MDB 多行脚本**：**进度事件** `mdb-script-progress` + **`cancel_mdb_script`** + `ScriptExecResult.cancelled`（`App.vue` 停止按钮）；其它长任务（导出/压测等）仍待统一进度模型。
- [ ] **MDB**：`PAGE_JSON` **大范围**游标化或缓存评估；**已做**：非 `id` 排序列分页 `partial_sort` 窗口；**`SCAN MORE`/`SCAN RESET`** 会话游标（大表日志输出分页）；增量 persist 默认路径，超大 dirty / schema 变更仍全量。
- [ ] **Compaction**：**已具备**：… **仍待**：与 WAL 的 **更强 OS 级**隔离（独立设备队列 / cgroup 权重等，见 **`Docs/OS_IO_ISOLATION.md`**）、按 merge 阶段更细默认调参。
- [ ] **WAL**：分段重放与 checkpoint 协同的进一步收紧（§5.1 已部分具备段元数据，续作见 `COMPACTION.md` / PHASE 文档）。
- [ ] **GraphExecutor / 存储压力联动**：**已做**：MemTable 探测按 **`estimated_cost` 缩放**、`backpressure_reason_cstr`、可选 **`GraphExecuteDiagnostics`**；**仍待**：与 `sync_scheduler_budget_from_storage_pressure` / `StoragePressureSnapshot` 的闭环与更细节点预算元数据。

### 0.5 验收对照（阶段 ↔ 文档）

| 阶段 | 文档章节 | 状态（2026-05-14 起） |
|------|-----------|------------------------|
| 1 周 | §4.1–§4.2 | **部分完成**：bench、压力快照（含 **compaction/worker 计数**）、trace（含 **merge/drain/worker**）、写路径节流已具备；§4.3 GUI、§4.4 全量 profiling 仍待续 |
| 1 月 | §5.1–§5.3 | **部分完成**：… **WAL 与 compaction 的 OS 级完全隔离** 仍续作（**清单与 Linux WAL `posix_fadvise` 已落地**，见 `Docs/OS_IO_ISOLATION.md`） |
| 1 季度 | §6.x | **部分启动**：MDB 增量 `persist_table` + dirty 跟踪；**MemTable** 仍为 `std::map` 主路径，已加 **flush 双缓冲**、**`std::shared_mutex` 读写路径分离（多读 / 独占写）**、前缀/overlay 扫描与 **`bytes_approx`**；缓存与调度联动仍待 |

---

## 1. 现状判断

从当前架构看，StructDB 的性能关键路径主要集中在以下四层：

1. **`StorageEngine`** — WAL、`commit_seq`、`MemTable`、SST/manifest、flush/compaction。
2. **`EmbedClient`** — journal、checkpoint、session 事务与耐久档位。
3. **`mdb_runner`** — 逻辑表、`persist_table`、`PAGE_JSON` / `SCAN` / `CONFIRM_REORDER`。
4. **`GraphExecutor`** — 计划执行、背压、flush/drain/compaction 调度。

当前最可能的瓶颈通常不是单一函数，而是：

- WAL **fsync** 频率与批次粒度
- **`std::map` + `std::shared_mutex`（`mu_`）** 型 MemTable：读路径 **`shared_lock`**（多 `get`/`visit_prefix` 可并行），写路径与 WAL 元数据变更 **`unique_lock`/`lock_guard`（独占）**；**`visit_prefix` 对 mem 层**已前缀 `lower_bound` + **overlay**；**`flush_memtable` 的 SST 排序写出在持独占锁段外**；前台 `put` 仍须独占锁
- **compaction** 对前台尾延迟与锁持有时间
- MDB 层 **全量** persist、非 id 列分页已用 **`partial_sort` 窗口**（不再整表 `std::sort`）、大结果集 `SCAN`
- **指标、trace、回归** 不足导致热点难定位（本轮已部分缓解）

---

## 2. 优化目标

### 2.1 性能目标
- 提高写入 QPS
- 降低普通 `get` 和 prefix scan 的延迟
- 降低 LSM 层数膨胀带来的读放大
- 减少 flush / compaction 对前台请求的尾延迟影响

### 2.2 稳定性目标
- 保持单写者与 WAL 权威恢复语义不变
- 恢复过程可验证、可重放、可回滚到安全边界
- 后台任务不破坏前台一致性

### 2.3 可维护性目标
- 将存储、嵌入式提交、MDB 逻辑、调度执行的热点边界显式化
- 形成持续可用的 **benchmark / tracing / 回归** 体系（本轮已搭骨架）

### 2.4 用户体验目标
- GUI 大表分页与查询不卡顿
- 长操作可感知、可中断、可恢复
- 执行结果更稳定，减少全量拉取和前端阻塞

---

## 3. 路线图总览

| 时间窗口 | 目标 | 主要动作 | 预期收益 | 进度备注 |
|---|---|---|---|---|
| 1 周 | 先定位、先止血 | 建指标、跑 profiling、收紧写路径、减少无谓 fsync | 快速找到真实热点，降低抖动 | bench、压力快照、trace、COMMIT_SEQ 节流、部分 MDB 分页已做 |
| 1 月 | 改善核心路径 | 批量写、WAL/Checkpoint、SST 过滤、compaction 受控 | 写更快、读更稳、恢复更快 | SST v3 Bloom、Embed journal、flush 写 v3 已做；**compaction worker、merge 时间/字节限速、顺序读、专用 materialize I/O 线程（可选）、分块节流** 已做；I/O 池与 OS 级隔离仍待 |
| 1 季度 | 结构性升级 | MemTable、MDB 增量、索引与缓存、调度背压 | 吞吐、P99、可扩展性 | **部分启动**：增量 persist、MemTable **flush 双缓冲 + 读写锁 + 前缀/overlay + `bytes_approx`**；**MDB `SCAN MORE`**；**GUI 脚本进度/停止**；arena / PAGE_JSON 大结果集游标等仍待 |

---

## 4. 1 周计划：先定位，再做低风险高收益优化

### 4.1 建立性能基线

**进度**：`structdb_bench` 已覆盖引擎侧 put/get/前缀/open 重放；`StoragePressureSnapshot` 已暴露 WAL 追加/fsync 与 flush/checkpoint 累计；**`STRUCTDB_TRACE=1`** 可打关键 span。

仍建议补齐（按需排期）：

- 单次 `put/get/remove/visit_prefix` 的 **P50/P99**（可用 bench + 外部统计）
- WAL fsync **次数与耗时分布**
- flush / compaction **触发次数与 wall time**
- `PAGE_JSON` / `SCAN` **按表行数** 的时延分布
- 恢复启动与 WAL 重放耗时
- GUI `invoke` 往返耗时

**产物**：bench 入口、trace 开关、压力快照字段；与优化前对比同一组命令与数据集。

### 4.2 收紧写路径

**进度**：`COMMIT_SEQ` 已从热路径移除；embed batch 仍共享 `reserve_commit_seq` + `commit_embed_batch` 单条 WAL 记录（语义不变）。

后续仍可考虑：

- 合并短时间内的多次写入
- 同一批逻辑写共享 `commit_seq`
- 减少重复 WAL 头部/元数据开销
- 审查是否仍存在不必要的同步刷盘

### 4.3 收紧 GUI / Embed 的全量操作

- GUI 列表页强制分页
- 避免前端一次性拉全表
- 对 `PAGE_JSON` 增加缓存或游标化入口的设计评估
- 对长操作提供进度与取消提示（**已做**：MDB 多行脚本 **`run_script_ex`** 的 **`mdb-script-progress`** 事件 + **`cancel_mdb_script`** + 结果字段 **`cancelled`**；其它 `invoke` 长任务仍待对齐）

### 4.4 1 周验收标准

- 能稳定复现当前热点（**bench + trace + 压力快照** 已具备基础）
- 有一份可重复 benchmark 结果（**`structdb_bench`**）
- 写入明显抖动下降（**COMMIT_SEQ 节流 + journal 句柄** 等已贡献一部分）
- 没有引入一致性回归（**`structdb_tests` + `mdb_tests`** 持续跑）

---

## 5. 1 月计划：优化核心读写与后台任务

### 5.1 StorageEngine 写入链优化

重点关注：WAL 追加与 fsync、`commit_seq` 批处理、`MemTable` 锁竞争、flush 前后状态切换。

**推荐动作**

1. **批量提交接口标准化** — MDB / Embed 尽量走批量写。
2. **WAL 分段与更快重放** — 与 checkpoint 协同跳过已确认前缀（段元数据已演进，续作见各 PHASE 文档）。
3. **SST 元数据过滤** — min/max（v2）+ **Bloom（v3 新写）**；`visit_prefix` 对 Bloom 保守；读路径仍兼容 v2/legacy。

### 5.2 Compaction 调度优化

目标：compaction 成为**受控后台任务**，而非前台干扰源。

- 后台 worker 或任务队列（**worker 已存在**；`StoragePressureSnapshot` 可观测队列深度与 **submitted/completed** 任务累计）
- **L0 合并最小间隔**：`EngineConfigSnapshot::compaction_merge_min_interval_ms`（与 WAL `wal_fsync_min_interval_ms` 同类）在多次连续 L0 merge 之间插入 wall-clock 间距，累计睡眠见 **`compaction_merge_throttle_sleep_ns_total`**
- **Merge 字节令牌桶**：`compaction_merge_max_bytes_per_second` / `compaction_merge_burst_bytes`，累计 **`compaction_merge_byte_throttle_sleep_ns_total`**；materialize 上在 **`compaction_merge_max_bytes_per_second>0`** 时按 **分块读/写** 回调扣桶（不再按整文件估算一次性扣款）
- **可选专用 materialize I/O 线程**：`EngineConfigSnapshot::compaction_dedicated_io_executor` → `CompactionIoExecutor::run_sync` 包裹 L0 / tiered pair 的 **impl**；`Engine::startup` 下发；`StorageEngine::close` 在 **停止 compaction worker 之后** `shutdown_compaction_io_executor`
- **分块大小**：`compaction_io_chunk_bytes`（`0` = 在「专用 I/O 或 merge BPS 开启」时用默认 **256KiB**，否则整段 `read_all`/`write_all` 快路径）
- **Compaction 读路径**：`compaction_sequential_sst_read` → merge materialize 的 `sst_load_all_entries(..., sequential_scan_hint, read_chunk_bytes, on_read_progress)`；点查 `sst_get_key` / `visit_prefix` 仍走默认打开
- 限速与预算（与 `ResourceBudget` / GraphExecutor 探测联动）
- L0 紧张优先；I/O 与前台隔离
- **`STRUCTDB_TRACE=1`** 下 span（存储引擎）：`stdb.storage.compact_merge_l0`、`stdb.storage.compact_merge_l1_to_l2`、`stdb.storage.compact_merge_l2_to_l3`、`stdb.storage.compact_merge_l3_to_l4`、`stdb.storage.drain_l0_compactions`、`stdb.storage.compaction_worker_task`（常量见 `storage_trace.hpp`）

### 5.3 EmbedClient 耐久路径优化

**进度**：`session.journal` 已用 **`FileWriter` 会话内追加**；`append_journal_line(..., fsync=true)` → `write_all` + `sync()`。

后续：

- checkpoint 频率与成本权衡
- session 批量与底层 batch 对齐
- 各档位语义对照 `phases/TXN_INNODB_MAP.md`（类比）

### 5.4 1 月验收标准

- 常见写入延迟更稳定
- 恢复时间缩短（与 WAL 段、checkpoint 续作相关）
- compaction 对前台影响降低（**worker、merge 时间/字节限速、可选专用 materialize I/O、分块节流** 已落地；与调度/budget 深度联动及多 I/O 线程仍待验证）
- 读放大减少（**SST v3 点查 Bloom + min/max** 已贡献；热点缓存仍属 P2）

---

## 6. 1 季度计划：结构性升级

### 6.1 MemTable 结构升级

当前 `std::map + std::shared_mutex`（`mu_`）：读路径 **`shared_lock`**、写路径 **`unique_lock`**；长期可替换为更适合 LSM 的内存组织、arena/pool。**已做（2026-05-14–15）**：`flush_memtable` 将活跃表迁入 **frozen**（现由 `MemTableManager` 管理），在**不持有** `mu_` 时写临时 SST 再持锁 `rename` 入 manifest；读路径合并 active 与冻结层；失败与 `close` 时用 `merge_missing_from` 回收冻结键。

### 6.2 MDB 增量持久化

目标：脏行/脏页级持久化，减少整表重写，降低 `PAGE_JSON`/`SCAN`/重排成本。

### 6.3 读取路径升级

热点缓存、prefix/range 导航、更强 SST 目录、大表**游标式**分页（与 §4.3 联动）。

### 6.4 调度与背压体系升级

`GraphExecutor` 预算与存储背压联动；flush/compaction/drain 明确优先级。

### 6.5 1 季度验收标准

大表尾延迟显著改善；恢复与 compaction 更稳定；热点可观测、回归可定位。

---

## 7. 代码级热点排查清单

### 7.1 `StorageEngine`

检查点：`put/remove/get` 锁竞争；WAL 小写；`fsync_wal` 策略；**`flush_memtable` SST 物化与 `put` 并行（仍单 writer 锁；禁止重叠 flush）**；compact 持锁；`visit_prefix` 无效 SST 扫描；`open` 恢复读放大。

**已做相关项**：SST min/max + v3 Bloom；flush 输出 v3；压力计数（**含 compaction_merge、worker tasks**）；flush / **L0–L3 merge** / **drain** / **worker task** 上 `SpanGuard`；merge materialize 可选 **`CompactionIoExecutor`** + **`read_all_chunked`/`write_all_chunked`** 与按块 merge 字节节流；**`mu_` 读侧 `shared_lock`**（`get`/`visit_prefix`/压力快照等并行读）。

### 7.2 `EmbedClient`

检查点：`submit` 批量化；journal fsync 频率；checkpoint 疏密；`commit_seq` 对齐；idempotency 成本；重试重复写。

**已做相关项**：journal `FileWriter` 复用 + 可选 `sync()`；**journal 与 WAL 重放优先级**（`wal_log_bytes_on_disk()>0` 时不落 journal 的 `apply_fields_after_ack`）及损坏行 / ckpt 校验回归见 `structdb_tests` 中 `EmbedClient.OpenFails*` 等用例；单测前置条件见 **§0.3** 说明。

### 7.3 `mdb_runner`

检查点：`persist_table` 全量；`PAGE_JSON` **id** 路径已 `partial_sort`，其它列仍全排；**`SCAN` 大结果集**（**`SCAN MORE`/`SCAN RESET`** 游标续打，仍按 `std::map` 行序）；`CONFIRM_REORDER` 成本；`LogicalTable` 内存；`str_idx` 命中率。

**已做相关项**：嵌套 SAVEPOINT + 大表 `PAGE`/`PAGE_JSON` + recover/commit 双路径回归（`mdb_tests.cpp`）。

### 7.4 `GraphExecutor`

检查点：节点切换开销；`request_cancel()`；budget 与真实压力对齐；计划拓扑 noop/drain；背压到存储侧。

---

## 8. 优先级建议（滚动）

### P0：必须先做

- benchmark / profiling / tracing（**bench + 压力快照 + `STRUCTDB_TRACE=1` 已具备**；持续补 span 与采样策略）
- WAL 与 batch 观测（**WAL 追加/fsync 计数已接压力快照**）
- GUI 避免全量拉取（分页 `page_size` 已钳制；**MDB 脚本**已具备 **进度事件 + 停止**；其它长 `invoke` 仍待产品化进度/取消）
- compaction 影响评估（**metrics 已具备**；… **更强 OS 级与 WAL 隔离**仍待（**见 `Docs/OS_IO_ISOLATION.md`**））

### P1：高收益

- 批量写路径整合
- SST 元数据过滤（**min/max + v3 Bloom 已落地**；前缀路径保守）
- checkpoint / WAL 分段策略深化
- 后台 compaction worker（**已具备**；续作：与专用 I/O 池联动、队列深度调参）

### P2：中长期结构优化

- MemTable 升级（当前仍为 map；已：**flush 双缓冲**、**`shared_mutex` 读写分离**、**前缀 `for_each_sorted_prefix`/`overlay`**、**O(1) `bytes_approx`**；续作 arena / `PAGE_JSON` 大结果集游标等）
- MDB 增量持久化（已具备默认路径；续作游标/大表策略）
- 热点缓存与更强索引
- 调度与背压联动完善

---

## 9. 风险与回滚策略

### 9.1 主要风险
- 批处理后错误传播更复杂
- compaction worker 引入并发问题
- MemTable 或索引改造引入一致性回归
- 恢复路径与 checkpoint 耦合更强

### 9.2 回滚原则
- 保留旧路径开关（SST **仍可读** v2/legacy）
- 每一步可独立关闭（如 trace 仅环境变量）
- 对恢复、事务、WAL、MDB txn 变更保留回归用例
- 正确性优先于极致性能

---

## 10. 建议的实施顺序（已发生 vs 建议后续）

**已在 2026-05-14 附近完成的前置步骤**（可视为「第 0 波」）：

1. 指标与 benchmark（压力快照、`structdb_bench`，含 **`BM_StdbStoragePressureSnapshot`**）
2. 写路径节流（`COMMIT_SEQ`、journal 句柄）
3. 读路径 SST 过滤（v2 min/max → v3 Bloom；flush 与 compaction 输出对齐）
4. **Compaction 可观测**（压力快照字段 + trace span：`compact_merge_*`、`drain_l0_compactions`、`compaction_worker_task`）
5. **Compaction materialize**：merge **字节桶按分块 I/O** 节流；可选 **`CompactionIoExecutor`**；`EngineConfigSnapshot` / MDB `SHOW TUNING*`；`structdb_tests` 中 `CompactionDedicatedIo*`、`CompactionExplicitIo*`、`CompactionDeferredWorkerDrain*`、`CompactionTieredL1ToL2*`、`Engine.DedicatedIoAndChunkFromConfig*` 等
6. MDB 大表 id 分页 + 严格嵌套回归 + `mdb_tests` ctest 别名 + **增量 `persist_table`（默认）** + GUI `page_size` 钳制
7. Embed **journal / WAL / ckpt** 损坏与嵌套 batch 回归（`structdb_tests` 中 `EmbedClient.*` / `Engine.NestedWalThrottle*` 等）

**建议后续顺序**（与风险匹配）：

1. compaction **OS 级 I/O 隔离**与尾延迟对照实验（在已有 **多 worker CIO** + 并行 SST 读 + 分块节流 + **Linux worker/CIO `nice`+`SCHED_IDLE`** 基础上）
2. WAL / checkpoint **策略**在真实负载下调参（与 §5.1、PHASE 文档对齐）
3. **GUI** 与 MDB **游标/缓存**（减少非 id 排序与全表扫描压力）
4. MemTable 结构改造（季度级）

---

## 11. 结语

StructDB 的优化原则是：**先把真实热点测出来，再做最少但有效的改动**。当前已在 **存储读路径（SST v3）、写路径节流、Embed journal、MDB id 分页、`SCAN MORE` 游标、trace、压力快照、merge 限速与专用 I/O、MDB 增量 persist、GUI 分页与 MDB 脚本进度/停止、`StorageEngine` 读写锁、`Docs/OS_IO_ISOLATION.md`** 等上建立可重复锚点；下一步应把 **更强 OS 级 I/O 隔离、GUI 其它长任务统一进度模型、`PAGE_JSON` 大范围游标/缓存、MemTable arena** 作为主线持续推进。

可选拆分（按需另起文档，避免本文件无限膨胀）：

- 《1 周执行清单》
- 《1 月里程碑任务表》
- 《性能基准脚本与数据集说明》

---

## 附录 A：开关与格式速查

| 项目 | 值 / 行为 |
|------|-----------|
| 运行时 trace | `STRUCTDB_TRACE=1`，见 `tracer.cpp`、`Engine::startup` |
| SST 新写魔数 | `STDBSST3`，footer：min/max + `u32` + 64B Bloom |
| SST 兼容读 | `STDBSST2`（无 Bloom）、无前缀 legacy（整文件 body） |
| MDB 单测别名 | `ctest -R ^mdb_tests$` → `Mdb.*` |
| 嵌套 txn 回归 | `TxnStrictNestedSavepointsPageJsonRecoverRestart`、`TxnStrictNestedSavepointsPageJsonCommitRestart` |
| Embed journal 与 WAL | `wal_log_bytes_on_disk()>0` 时 `parse_journal_for_recovery` **跳过** `apply_fields_after_ack`（仍以 journal 行为 idempotency 等元数据）；见 `embed_client.cpp` |
| 损坏 journal / ckpt 单测 | 期望走 `apply_fields` 失败路径时：**WAL 字节为 0** 且 **`seq > last_ack_`**；典型用例 `EmbedClient.OpenFailsOnJournalInvalidPutFieldCount` 等 |
| CTest 全量（当前） | `structdb_tests`、`mdb_tests`、`structdb_capi_shared_smoke` |
| 压力快照字段 | `wal_append_record_calls_total`、`wal_fsync_calls_total`、`flush_memtable_success_total`、`checkpoint_success_total`、`compaction_merge_success_total`、`compaction_merge_throttle_sleep_ns_total`、`compaction_merge_byte_throttle_sleep_ns_total`、`compaction_worker_tasks_{submitted,completed}_total`（见 `storage_pressure.hpp`） |
| Compaction 配置（`EngineConfigSnapshot` / `StorageEngine` setter） | `compaction_dedicated_io_executor`（默认 false）：merge materialize 是否在 **`CompactionIoExecutor`** 上执行；`compaction_io_pool_threads`（默认 **0**→运行时 **2**，上限 32）：CIO worker 数；`compaction_parallel_sst_reads`（默认 false）：merge 时第二路 SST 异步读；`compaction_io_chunk_bytes`（默认 0）：显式分块字节数，**0** 时在「专用 I/O 或 `compaction_merge_max_bytes_per_second>0`」下 materialize 使用 **256KiB** 默认块；与 `compaction_merge_max_bytes_per_second`、`compaction_merge_burst_bytes`、`compaction_sequential_sst_read` 等并列 |
| MDB 逻辑表持久化 | `mdb_incremental_persist`（默认 true）：小批量 dirty 走增量 embed batch；schema 脏或过大则全量 |
| Merge materialize I/O 分块 | `storage_engine_detail::sst_load_all_entries(..., read_chunk_bytes, on_read_progress)`；`write_sst_sorted_entries*` 的 `write_chunk_bytes` / `on_write_progress`；底层 `structdb::infra::FileReader::read_all_chunked`、`FileWriter::write_all_chunked` |
| trace span（节选） | **`stdb.storage.*`**：`stdb.storage.flush_memtable`、`stdb.storage.compact_merge_l0`、`stdb.storage.drain_l0_compactions`、`stdb.storage.compaction_worker_task`；客户端仍用 `mdb.PAGE`、`mdb.PAGE_JSON`、`embed.submit` 等 |
