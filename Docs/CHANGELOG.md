# Changelog

本文档记录 **StructDB 本仓库**（不含 `ThirdParty/` 上游各自变更）的用户可见与工程可见变更。

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### 竞品完善（Wave 4 / PHASE43 续 + PHASE44–45 + I-CAPI 1.9）

- **PITR 加固（PHASE43 续）**：`checkpoint_chain_validate`（`EngineConfigSnapshot::checkpoint_chain_strict`）；`structdb_app --recover-to-checkpoint-seq`；`scripts/recover_to_checkpoint.ps1` 调用 app；`backup_manifest.json` 含 `last_checkpoint_seq`。
- **索引（PHASE45）**：`DROP INDEX name ON table`；`CREATE UNIQUE INDEX`；[`PHASE45.md`](phases/PHASE45.md)。
- **Bulk（PHASE44）**：`IMPORT SEGMENT (token)` → persist idem `idem:import:<table>:seg:<token>`；段切换时自动 persist；样例 [`scripts/bench/import_segment_two_segments.mdb`](../scripts/bench/import_segment_two_segments.mdb)。
- **C API 1.9.0**：`structdb_recover_data_dir_to_checkpoint_seq`、`structdb_engine_recover_to_checkpoint_seq`、`structdb_backup_bundle`、`structdb_mdb_session_set_durability`。
- **文档/远期**：[`PHASE46_NAMESPACE.md`](phases/PHASE46_NAMESPACE.md)、[`PHASE46_SQL_MAPPING.md`](phases/PHASE46_SQL_MAPPING.md)；可选 [`.github/workflows/structdb-linux-smoke.yml`](../.github/workflows/structdb-linux-smoke.yml)。
- **回归**：`Mdb.Phase45*`（2）、`Mdb.Phase44*`（1）、`Mdb.Phase42OrderByPageJson`（1）、`StorageEngine.Phase43ChainValidateMismatchStrict`；`Capi.Phase45*`（2）；`Mdb.*` **151** 项全绿。

### 竞品完善（Wave 3 / PHASE42–43）

- **查询（I-QRY / PHASE42）**：`GROUP BY (col) COUNT`、`GROUP BY (col) SUM(col)`；`SCAN INDEX(name)`（命名索引键序；无 postings 时按列值回退）。
- **PITR（I-PITR / PHASE43）**：`checkpoint.chain` 侧车；`SHOW CHECKPOINTS`；`RECOVER TO CHECKPOINT_SEQ n`（`Engine::recover_to_checkpoint_seq`，须先 shutdown）；`RECOVER TO TIME/LSN` 仍定向 PHASE43。
- **存储**：[`checkpoint_chain.cpp`](../src/engine/storage/src/checkpoint_chain.cpp)；[`PHASE43.md`](phases/PHASE43.md)、[`PHASE42.md`](phases/PHASE42.md)。
- **Wave 2 补强**：`Mdb.Phase41DurabilityColdRestart`、`Phase41IndexColdRestart`、`Phase41DropRenameCrashOldTableGone`。
- **PHASE44 原型**：`IMPORT SEGMENT (token)` 日志提示；[`PHASE44_PERSIST_STREAM.md`](phases/PHASE44_PERSIST_STREAM.md) 续写 IMPORT_SEGMENT。
- **运维**：[`scripts/recover_to_checkpoint.ps1`](../scripts/recover_to_checkpoint.ps1)；[`BACKUP_RESTORE_RUNBOOK.md`](BACKUP_RESTORE_RUNBOOK.md) §7 checkpoint_seq。
- **文档**：`ONBOARDING` / `ENGINE_RUNTIME_CONFIG` §4.5 事务 profile；`COMPETITIVE_MATRIX` §6.4 GROUP BY ◐。
- **回归**：`Mdb.Phase42*`（2）、`Mdb.Phase43*`（1）、`StorageEngine.Phase43*`（2）；`Mdb.*` **147** 项全绿。

### 竞品完善（Wave 2 / PHASE41）

- **耐久（I-DUR）**：`SET DURABILITY 0|1|2`（会话级，映射 [`TXN_INNODB_MAP.md`](phases/TXN_INNODB_MAP.md)）；`SHOW TUNING` 含 `session_durability_level`；`WALSYNC`/`GROUPCOMMIT` → 提示 `SET DURABILITY`；`HOTINDEX` → 提示 `CREATE INDEX`。
- **DDL（41A/41B）**：`ALTER TABLE ADD COLUMN (name:type)`、`ALTER TABLE RENAME COLUMN (old,new)`；`BEGIN` 内拒绝；其余 `ALTER` 仍 `[NOT_SUPPORTED]` 并指向 PHASE41 子集。
- **运维（I-OPS）**：`RENAME TABLE` 新表 persist + 旧表键删除合并为 **单次** `submit_persist_command_batch`。
- **索引（I-IDX）**：`mdb$v2$nidxdef$` / `mdb$v2$nidx$`；`CREATE INDEX idx ON table(col)`；`EXPLAIN WHERE` → `named_index=<name>`；`REBUILD INDEX(name)`。
- **Bulk 首段（PHASE44）**：`ordered_row_ids_for_persist` 流式全量行遍历；设计稿 [`PHASE44_PERSIST_STREAM.md`](phases/PHASE44_PERSIST_STREAM.md)。
- **文档**：[`PHASE41.md`](phases/PHASE41.md)；矩阵 [`COMPETITIVE_MATRIX.md`](COMPETITIVE_MATRIX.md) §6；[`PHASE25.md`](phases/PHASE25.md) §25G 修订。
- **回归**：`Mdb.Phase41*`（6 项）；`Mdb.*` **141** 项全绿。
- **脚本**：`mega_data_mdb_stress.ps1` 可选 `-SampleWorkingSet`（峰值 WorkingSet 写入 summary JSON）。

### 竞品完善（Wave 0–1）

- **基线**：`structdb_app --oltp-persist-micro`；`scripts/bench/oltp_persist_micro.ps1` → `benchmarks/baselines/oltp_persist_baseline.json`；`run_persist_baseline.ps1` 串联 OLTP；`scripts/results/README.md`、`compare_mega_summary.py`。
- **运维**：[`BACKUP_RESTORE_RUNBOOK.md`](BACKUP_RESTORE_RUNBOOK.md)；`scripts/backup_bundle.ps1`、`structdb_app --backup-bundle`。
- **事务可预期**：[`ONBOARDING.md`](ONBOARDING.md) 三档配置；`SHOW TXN` / `SHOW SNAPSHOT` 增加 `storage_rollback_policy`。
- **配置预设**：[`ENGINE_RUNTIME_CONFIG.md`](ENGINE_RUNTIME_CONFIG.md) §4（OLTP / LSM / bulk）；`scripts/weekly_bench.ps1` bulk 门禁。
- **文档**：[`STRUCTDB_EVALUATION_SUMMARY.md`](STRUCTDB_EVALUATION_SUMMARY.md) MemTable 分场景表述；[`COMPETITIVE_MATRIX.md`](COMPETITIVE_MATRIX.md) §7.2 OLTP 基线链接。
- **回归**：`Mdb.ShowTxnStorageRollbackPolicy*`、`Mdb.BackupRestoreRunbookSmoke`。

### 工程可见（三十九期）

- **MDB persist 性能（默认语义不变）**：`LogicalTable::row_ids_ordered` 增量维护；`persist_table` 拆为 `build_persist_command_batch` / `submit_persist_command_batch`，脏行 >8192 分块 submit；`mdbwire2:` 线格式（`mdb_wire_encoding`，默认仍为 `Hex`）；`IMPORT MODE` / `FLUSH PERSIST` / `REBUILD INDEX`；`BULKINSERTFAST` 经 `persist_now()`（导入模式延迟落盘）。
- **Opt-in**：`mdb_persist_coalesce` + `MdbRunOptions::persist_coalesce`；`mdb_bulk_import_mode` + `IMPORT MODE` / `mdb_bulk_import_mode`；`embed_journal_skip_until_commit`；`storage_batch_undo_lookup`（默认 true）。
- **存储 / MemTable**：`commit_embed_batch` 批量 undo 查找；`MemTableArena` + SkipList 节点 arena 分配。
- **脚本 / 基线**：`scripts/run_persist_baseline.ps1`；`mega_data_mdb_stress.ps1 -ImportMode`；`benchmarks/baselines/structdb_bench_baseline.json` 含 `BM_StdbStorage*`。
- **回归**：`Mdb.Phase39*`（row_index 冷启动 COUNT、Wire2 parity、coalesce + FLUSH PERSIST、脚本默认 bulk 摊销）；专文 [`PHASE39_PERSIST_PERF.md`](Docs/phases/PHASE39_PERSIST_PERF.md)。
- **数量级续**：默认 `mdb_script_amortize_bulk_dml`（脚本 `BULKINSERT*` EOF 落盘）；脏行 >8192 单次全量 persist；`storage_batch_undo_mem_only` / `storage_import_batch_skip_undo`；`structdb_app` 增加 `--mdb-bulk-import` 等 CLI。

### 工程可见（四十期）

- **大表 bulk 行索引**：全表导入时延迟 `row_ids_ordered`（避免 O(n²) 有序插入）；单调/数字主键友好追加；persist 前一次性重建。
- **大表 plain 行落盘**：`mdb_bulk_persist_plain_rows`（默认 true）跳过 `mdbhex1:`；导入批 WAL/MemTable 零拷贝；plain 批次跳过 text journal。
- **MDB 分块 persist**：`mdb_persist_chunk_max_puts` / `mdb_persist_chunk_max_frame_bytes`；大表全量 snapshot 多帧 `submit_ex`，catalog/schema/`row_index` 仅末帧；`idempotency_token` `{base}:chunk:{i}`。
- **导入 raw 逻辑值**：`storage_import_store_raw_logical`；`mdb_bulk_import_mode` 时 persist 自动 raw（无 `ver$` 包装）；`memtable_bulk_put_enabled`（opt-in 排序批量 put）。
- **存储兜底分帧**：`storage_embed_batch_max_frame_bytes`（默认 `0`）在 `commit_embed_batch` 内拆分 WAL 帧。
- **性能（本机 1M 行，`RowsPerLine=1000`）**：默认脚本摊销 + 分块 + plain 约 **~238K TPS**；`--mdb-bulk-import` 约 **~328K TPS**（相对 PHASE39 ~7.5K 约 **32–44×**）。门禁脚本 `scripts/bench/mega_data_mdb_stress.ps1`。
- **回归**：**`Mdb.Phase40*`**（24 项：严格/失败/恢复/联动）、**`Mdb.*`**（127 项全绿）；`StorageEngine.WalReplayImportRaw*`、`WalReplay*ChunkedMdb*`、`CommitEmbedBatchAutoSplitByFrameBytes`；专文 [`PHASE40_PERSIST_PERF.md`](phases/PHASE40_PERSIST_PERF.md)。

### 工程 / 重构（持续）

- **MDB**：`PAGE` / `PAGE_JSON` 实现迁至 **`mdb_query_paging.{hpp,cpp}`**；`mdb_ops_pages_journal_import.cpp` 保留 `IMPORTDIR`、`SHOWLOG` 与 CSV 括号解析等（台账见 [`NEXT_REFACTOR_RECOMMENDATIONS.md`](Docs/NEXT_REFACTOR_RECOMMENDATIONS.md) §13）。
- **存储 / 观测 / MemTable**：**默认 `MemTableBackend::SkipList`**（`EngineConfigSnapshot` / `StorageEngine` / `MemTableManager`）；**`stdb.storage.*`** trace 常量（`storage_trace.hpp`）；**`BM_StdbStorage*`** 引擎 bench 名；**`CompactionResult`**、**`RecoveryOpenPolicy`** 类型锚点；演进说明见 [`STORAGE_EVOLUTION_AND_OBSERVABILITY.md`](Docs/STORAGE_EVOLUTION_AND_OBSERVABILITY.md)。
- **引擎**：`Orchestrator::set_before_graph_execute` + `GraphExecutor::execute(..., use_budget_probe=true)` 与 `sync_scheduler_budget_from_storage_pressure` 联动；配置清单见 [`ENGINE_RUNTIME_CONFIG.md`](Docs/ENGINE_RUNTIME_CONFIG.md)。

### 工程可见（三十八期）

- **MDB**：`CONFIRM_REORDER({…})` 解析为独立动词；在 **非事务** 上下文中迁移当前 `USE` 表的行锚并 **`persist_table`**；成功时输出 **`[REORDER_MAP_JSON]`**（与 GUI 字段一致）。实现见 [`PHASE38.md`](Docs/phases/PHASE38.md)、[`mdb_ops_reorder.cpp`](../src/client/mdb/src/mdb_ops_reorder.cpp)。**25G** 拒绝矩阵不再包含 `CONFIRM_REORDER`（见 [`PHASE25.md`](Docs/phases/PHASE25.md)）。
- **GUI**：`ingest_reorder_map_from_engine_output` 对输出中 **每一行** `[REORDER_MAP_JSON]` 追加一层 `id_remap_chain`（去掉早先的「首行即 break」）；切换工作区时仍 **`id_remap_chain.clear()`**（与 `PHASE38` 专文一致）。
- **文档**：新增 [`PHASE38.md`](Docs/phases/PHASE38.md)；更新 [README](Docs/README.md)、[TESTING_TXN_CHAIN §15](Docs/phases/TESTING_TXN_CHAIN.md)、[PHASE13_PLUS_PLAN](Docs/phases/PHASE13_PLUS_PLAN.md)（`p37`→`p38`）。
- **回归**：`mdb_tests` **`Mdb.Phase38*`**（happy path、两表两行 JSON、互换 `pairs`、解析矩阵与 `BEGIN` 内拒绝）；`rust_gui` **`phase38_remap_ingest_tests`**。

### 工程可见（三十七期）

- **文档与索引**：新增 [`PHASE37.md`](Docs/phases/PHASE37.md)；更新 [README](Docs/README.md)、[TESTING_TXN_CHAIN §14](Docs/phases/TESTING_TXN_CHAIN.md)、[PHASE13_PLUS_PLAN](Docs/phases/PHASE13_PLUS_PLAN.md)（`p35`→`p36`→`p37`）、[COMPACTION §3](Docs/COMPACTION.md) 验收 filter。
- **CHANGELOG 对齐**：三十六期回归条目补全第三项用例名（与 `structdb_tests` 一致）。
- **回归**：`structdb_tests` **`StorageEngine.Phase37ConcurrentPutWhileL1ToL2Compact`**、**`StorageEngine.Phase37ConcurrentPutWhileL3ToL4Compact`**（与 `*Phase36*` 对称的 L1→L2 / L3→L4 并发 compaction）；**`StorageEngine.CompactionConcurrencySemanticMatrix`**（[`PHASE31.md`](Docs/phases/PHASE31.md) 矩阵 F 多行顺序断言）；**`StorageEngine.ConcurrentNestedL0DrainAndL1MergeWhilePuts`**（延后 L0 drain 与 L1→L2 嵌套并发）；C API 独占锁仍以 **`capi_test` `Capi.ExclusiveDirLockSecondOpenFails`** 为准。

### 工程可见（三十六期）

- **L1+ compaction**：`compact_merge_two_oldest_l1_to_l2` / `l2_to_l3` / `l3_to_l4` 与 **三十五期 L0** 对齐为 **快照 → `mu_` 外读 SST 与写 `_tmp_tier_compact_*` → 锁内校验与 `rename` / MANIFEST / checkpoint**；冲突返回可重试错误串。详见 [`PHASE36.md`](Docs/phases/PHASE36.md)、[`COMPACTION.md`](Docs/COMPACTION.md)、[`POLICY.md`](Docs/POLICY.md) §4.2。
- **读并发**：维持 **`StorageEngine::mu_`**；**`shared_mutex`** 拆分 `get`/`visit_prefix` **本期不实施**，预研结论见 [`PHASE36.md`](Docs/phases/PHASE36.md)。
- **Facade（可选）**：`EngineConfigSnapshot::kv_put_async_queue_depth`（默认 **0**）；**`>0`** 时 **`Engine::kv_put`** 经 **单 worker 有界队列**调用 `storage_->put`，满则 **`kv_put` 返回 `false`**。`StoragePressureSnapshot` 增加 **`facade_kv_put_queue_depth` / `facade_kv_put_queue_cap`**（由 `Engine::storage_pressure_snapshot` 填充）。
- **GUI**：环境变量 **`STRUCTDB_GUI_EXCLUSIVE_DIR_LOCK`** 为 **`1`/`true`** 时以 **`structdb_engine_open_ex(..., STRUCTDB_ENGINE_OPEN_FLAG_EXCLUSIVE_DIR_LOCK)`** 打开引擎（默认关）；见 [`gui/rust_gui/README.md`](../gui/rust_gui/README.md)。
- **回归**：`structdb_tests` **`StorageEngine.Phase36ConcurrentPutWhileL2ToL3Compact`**、**`Engine.Phase36FacadeKvPutQueueCapObservedInPressure`**、**`Engine.Phase36FacadeKvPutAsyncQueuedPathWiring`**。

### 工程可见（三十五期）

- **L0 compaction**：`compact_merge_two_oldest_l0` / `drain_pending_l0_compactions` / 阈值内联 compact 在 **快照 manifest 头** 后于 **`mu_` 外**读 SST、合并、写 **`_tmp_l0_compact_*.sst`**，再持锁校验、`rename`、写 MANIFEST/checkpoint（冲突可重试）。详见 [`PHASE35.md`](Docs/phases/PHASE35.md)、[`COMPACTION.md`](Docs/COMPACTION.md)、[`POLICY.md`](Docs/POLICY.md) §4.2。
- **`EmbedClient::submit`**：增加 **`submit_mu_`**，与 **`idem_mu_`** 嵌套，序列化多线程 `submit` 的存储 + journal + 序号路径。
- **跨进程建议锁**：`EngineConfigSnapshot::exclusive_data_dir_lock`、`StorageEngine::set_exclusive_directory_lock`、`structdb_engine_open_ex` + **`STRUCTDB_ENGINE_OPEN_FLAG_EXCLUSIVE_DIR_LOCK`**（`data_dir/.structdb_exclusive.lock`；POSIX `flock` / Windows `LockFile`）。C API 版本号 **1.7.3**。
- **回归**：`structdb_tests` **`StorageEngine.Phase35ConcurrentPutWithDeferredL0Drain`**；`capi_test` **`Capi.ExclusiveDirLockSecondOpenFails`**。

### 工程可见（三十四期）

- **文档固化**：新增 **[`PHASE34.md`](Docs/phases/PHASE34.md)**（`StorageEngine` 多 TU 权威映射、`--gtest_filter` 与三十一期子集说明）；修正 [PHASE20](Docs/phases/PHASE20.md) / [PHASE21](Docs/phases/PHASE21.md) 等仍指向旧单文件的锚点；索引 [README](Docs/README.md)、[POLICY](Docs/POLICY.md)、[PHASE13_PLUS_PLAN](Docs/phases/PHASE13_PLUS_PLAN.md)、[ONBOARDING](Docs/ONBOARDING.md) 与 **`*Phase31*`** 可复制 filter 文案对齐。
- **MDB**：`structdb_client_mdb` 将原 **`mdb_ops_storage_and_tools.cpp`** 再拆为 **`mdb_ops_persist_load.cpp`**（表加载/持久化/存在性、`gather_drop_table_keys`、`rename_table_storage`）与 **`mdb_ops_pages_journal_import.cpp`**（`PAGE`/`PAGE_JSON` 辅助、`IMPORTDIR`、`SHOWLOG` 尾行）；声明仍在 `mdb_runner_internal.hpp`。

### 工程可见（三十三期）

- **`StorageEngine` 再细分 TU**：`structdb_storage` 新增 **`storage_engine_open_wal.cpp`**（ctor、段目录、`open`/`close`、WAL 重放）、**`storage_engine_put_undo.cpp`**（写路径、undo、WAL trim/GC）、**`storage_engine_read.cpp`**（`decode_get_visible_`、`get`、`visit_prefix`）；**`storage_engine.cpp`** 仅保留 **`COMMIT_SEQ`** 与 `observe` / `reserve` 等薄核；原 **`storage_engine_compact_checkpoint.cpp`** 拆为 **`storage_engine_compaction_lsm.cpp`**（flush、各层 compact、背压快照）与 **`storage_engine_segments_worker_checkpoint.cpp`**（段元数据持久化、undo/WAL roll、compaction worker、`checkpoint`）。语义与磁盘顺序不变；详见 [`PHASE33.md`](Docs/phases/PHASE33.md)。

### 工程可见（三十二期）

- **MDB 多编译单元**：`structdb_client_mdb` 中原 `mdb_runner_ops.cpp` 已拆为 **`mdb_ops_string_wire.cpp`**、**`mdb_ops_logical_index.cpp`**、**`mdb_ops_predicate.cpp`**、**`mdb_ops_txn_log.cpp`**、**`mdb_ops_persist_load.cpp`**、**`mdb_ops_pages_journal_import.cpp`**（三十二期五文件；**三十四期** 将原 `mdb_ops_storage_and_tools.cpp` 再分为后二者），共享 **`mdb_ops_detail.hpp`**（`ascii_strncasecmp`）。详见 [`PHASE32.md`](Docs/phases/PHASE32.md)、[`PHASE34.md`](Docs/phases/PHASE34.md)；[`PHASE26.md`](Docs/phases/PHASE26.md) 模块图已更新。
- **`StorageEngine` 跨 TU（粗拆分）**：`structdb_storage` 新增 **`storage_engine_detail.cpp`**（SST/段元数据/manifest 读序辅助）并将 flush/compact/checkpoint 从主文件迁出（**三十二期** 为单 compact TU；**三十三期** 再分为 `compaction_lsm` 与 `segments_worker_checkpoint`）。语义不变；回归见 `structdb_tests` **`StorageEngine.*` / `Engine.*`**（三十一期子集推荐 **`--gtest_filter=*Phase31*`** 或 [PHASE31](Docs/phases/PHASE31.md) 显式前缀一行）。

### 工程可见（三十一期）

- **文档**：[`Docs/phases/PHASE31.md`](Docs/phases/PHASE31.md)（31A–31E 组合矩阵、恢复权威顺序、PR checklist）；[`Docs/POLICY.md`](Docs/POLICY.md) §4 / §4.0.3 / §6.1 链到 PHASE31 与事务链 PR 的 **`--gtest_filter`** 约定；[`Docs/phases/TESTING_TXN_CHAIN.md`](Docs/phases/TESTING_TXN_CHAIN.md) §13；[`Docs/phases/PHASE13_PLUS_PLAN.md`](Docs/phases/PHASE13_PLUS_PLAN.md)（`p31` 节点与索引）、[`Docs/README.md`](Docs/README.md)。
- **回归**：`structdb_tests` 新增 **`StorageEngine.Phase31*`**（flush/compact 后 checkpoint **`manifest_version`** 与 **`manifest().version()`** 一致）、**`Engine.Phase31WalCommittedKvSurvivesColdRestart`**、**`EmbedClient.Phase31JournalReplayAfterCrashChainsAfterEngineWal`**、**`Engine.Phase31CorruptSessionTxnDropsTxnLogOnRepl`**（损坏 `session.txn` 后 `SHOW TXN` 显式非活跃且不崩溃）、**`Engine.Phase31ObserveBypassWithSecondEmbedClientAlsoOpen`**（双 `EmbedClient` 已 open 时 24A 观测语义不变）。

### 用户可见

- **MDB / GUI 分页**：新增 **`PAGE_JSON(page,size,sort_col,asc|desc)`**，在已 `USE` 的逻辑表上**全量排序后分页**，输出一行 **`[PAGE_JSON]{...}`**（`headers` / `columns` / `rows`），不受 **`SCAN` 5000 行**限制；**`structdb_app`** 支持 **`--page-json --page <n> <sz> --order <col> [--desc] --table <name>`**（与 GUI 回退路径一致）。
- **GUI CLI 终端**：当主窗口已通过 C API 占用与 CLI 相同的 **`data_dir`**（WAL 独占）时，**自动使用进程内 MDB 转发**，避免 `startup failed: wal open`。
- **文档**：新增 **[`Docs/ARCHITECTURE.md`](Docs/ARCHITECTURE.md)**（数据流与代码版图 Mermaid、关键类型源码摘录）；**[`Docs/README.md`](Docs/README.md)**、**[`Docs/ONBOARDING.md`](Docs/ONBOARDING.md)**、**[`Docs/POLICY.md`](Docs/POLICY.md)** 与根 **[`README.md`](../README.md)** 的索引与交叉链接已对齐 **`Docs/phases/`** 下的 PHASE 专文路径。

### 工程可见（三十期）

- **GUI（`gui/rust_gui/`）**：Tauri 后端改为加载 **`structdb_capi_shared`**，通过 **`structdb_mdb_execute_line_ex`** 执行 MDB；子进程 **`structdb_app`** / **`structdb_bench`** 替代 `newdb_demo` / `newdb_perf`；移除 **`newdb_cli_backend`**、MinGW 运行时包与 **`newdb_runtime_report`** 依赖链。
- **打包**：`tauri.conf.json` 资源最小集为 **`resources/scripts`**；**`gui/rust_gui/scripts/sync_runtime_binaries.ps1`** 与 **`build_tauri_plugin_bundle.ps1`** 同步 **`structdb_capi_shared.dll`**、`structdb_app.exe`、`structdb_bench.exe` 至 `src-tauri/bin/`。
- **品牌与本地状态**：产品名 / 包名 / Tauri **`identifier`** 对齐 **StructDB**；撤销栈目录 **`.structdb_gui`**；栈 JSON **`structdb.gui.undo_redo_stack.v4`**；诊断包 manifest **`structdb.gui.bundle_manifest.v1`**。
- **前端策略**：`commandPolicy` 删除 newdb **`where_plan_json`** 细节码与 **`RUNTIME_TUNING_DIAGNOSTIC_GROUPS`** 键表（C API 无等价 JSON）；帮助文案标明执行计划 / 调优 JSON 以 **StructDB MDB** 为准。
- **文档**：[`Docs/phases/PHASE30.md`](Docs/phases/PHASE30.md)；[`gui/rust_gui/README.md`](../gui/rust_gui/README.md) 重写为 StructDB 构建与同步步骤。

### 工程可见（二十九期）

- **许可**：仓库根新增 **`LICENSE`（MIT）**；StructDB 自有代码 SPDX **`MIT`**（`ThirdParty/*` 仍各循其许可）；见根 **`README.md`**。
- **Perf 门禁**：[`benchmarks/baselines/structdb_bench_baseline.json`](benchmarks/baselines/structdb_bench_baseline.json)（Google Benchmark JSON）；[`benchmarks/scripts/compare_bench.py`](benchmarks/scripts/compare_bench.py)（**Python 3.8+**，仅标准库；默认按 **`real_time`**、`--max-ratio 1.5` 对比）；[`benchmarks/README.md`](benchmarks/README.md)。CMake **`STRUCTDB_ENABLE_PERF_GATE`**（默认 OFF）注册 **`ctest` `structdb_perf_gate`**（`LABEL` **`perf`**；MSVC 须 **`ctest -C Release -L perf`**）。
- **文档**：[`Docs/phases/PHASE29.md`](Docs/phases/PHASE29.md)（会话 **`session_log.txt`**、水位、会话/引擎文件运维索引；单键裁剪 / 受限语义 / `ROLLBACK` 与存储的 **`POLICY` 读者路线图**）；[`Docs/POLICY.md`](Docs/POLICY.md) **§4.0.2** 增补 **`session_log.txt`**；**§6.2**/**§7** 与 PHASE29 交叉引用。

### C API 契约 1.7.0（会话活动日志与详细错误）

- **`session_log.txt`**：每次 **`EmbedClient::open`** 成功追加 **`SESSION_OPEN`**（UTC、pid、水位），**`close`** 追加 **`SESSION_CLOSE`**；超 **2MiB** 轮转 **`session_log.arch.*`**，至多保留 **12** 份归档。
- **`structdb_embed_get_session_log_path_utf8`**。
- **`EmbedClient::open` / journal / ckpt** 等错误字符串带路径与片段，便于排障。

### C API 契约 1.6.0（崩溃加固 / 确认水位）

- **`STRUCTDB_CAPI_ERR_STORAGE_IO`**：**`structdb_engine_wal_sync`**、**`flush_memtable`**、**`checkpoint`**、**`structdb_embed_save_checkpoint`**。
- **查询**：**`structdb_engine_latest_commit_seq`**、**`structdb_embed_last_ack_seq`** / **`next_seq`** / **`read_snapshot_seq`**。
- 头文件 **「崩溃加固、响应与确认」** 小节：MDB **`fsync_*`** 与上述 API 的组合说明。

### C API 契约 1.5.0（持久化对话 / 事务说明与查询）

- 头文件 **「持久化对话与持久化事务」** 约定：**`structdb_mdb_execute_line*`** + 固定 **`session_dir`** 用于 **`session.txn`** 恢复；**`structdb_run_mdb_file*`** 每次运行删除 **`session.txn`**。
- **`structdb_embed_get_session_dir_utf8`**：返回 **`structdb_embed_open`** 实际使用的会话目录（UTF-8），便于宿主持久化。

### C API 契约 1.4.0（收尾）

- **`structdb_mdb_execute_line`**：等价于 **`structdb_mdb_execute_line_ex(..., opts=NULL)`**（与 **`structdb_run_mdb_file`** / **`_ex`** 对称）。
- **`cmake --install`**：始终安装静态 **`structdb_capi`** 与 **`structdb_capi.h`**；开启 **`STRUCTDB_BUILD_CAPI_SHARED`** 时另安装 **`structdb_capi_shared`**。

### 工程可见（二十八期）

- **C 绑定共享库与 FFI 会话**：CMake **`STRUCTDB_BUILD_CAPI_SHARED`**（默认 OFF）生成 **`structdb_capi_shared`**；**`STRUCTDB_CAPI_EXPORT`** / `STRUCTDB_CAPI_SHARED` 导入；**`structdb_engine_open` / `shutdown`**、**`structdb_embed_open` / `structdb_embed_get_session_dir_utf8` / `close`**、**`structdb_mdb_session_*`**、**`structdb_mdb_execute_line`** / **`structdb_mdb_execute_line_ex`**（`mdb_repl_execute_line` 路径；日志文件为 **追加**）。**`structdb_capi_version()`** 打包版本整数；**单 engine 单 embed**；**`shutdown` 自动关闭**仍打开的 embed。头文件语义版本 **1.7.0**（在 **1.6.0** 能力上另含 **`session_log.txt`** 活动日志与 **`structdb_embed_get_session_log_path_utf8`**；**`EmbedClient`** 详细错误串；见 **1.7.0** / **1.6.0** 小节）：**`NULL`/`""` 的 `data_dir`/`session_dir`** 自动解析为 **`cwd/_data`** 与 **`{data_dir}/embed_session`**；**`structdb_capi_get_default_paths`**、**`structdb_engine_get_data_dir_utf8`** 供 FFI 固定持久化路径；**`STRUCTDB_MDB_RUN_OPTIONS_SIZE_V1` / `SIZE_V2`** 与 **`repl_exit_requested_out`**；Windows 上引擎 **`data_dir`** 经 **`std::filesystem::u8path`** 打开（UTF-8 契约）；**`structdb_capi_version_string()`** 与宏由实现同源生成；**`run_mdb_impl` 异常路径关闭 embed**；**日志回调/写文件不向外抛异常**；**`structdb_mdb_execute_line_ex` 校验 `active_embed`**；**Clang `default` 导出**；非 MSVC 下静态 **`structdb_capi`** 与共享目标 **PIC**；**`cmake --install`** 安装静态库与头文件（共享目标可选）。详见 [`PHASE28.md`](Docs/phases/PHASE28.md)；`Capi.*` 与可选 **`structdb_capi_shared_smoke`**。

### 工程可见（二十七期）

- **C 绑定 `structdb_capi`**：版本宏/`structdb_capi_version_string()`、稳定返回码 `structdb_capi_rc`、`structdb_mdb_run_options`（`struct_size` 前缀）与 `structdb_run_mdb_file_ex`；可选脚本行日志（截断写文件 UTF-8 路径、和/或每行 NUL 回调，先回调后文件）；`structdb_run_mdb_file` 仍为 `_ex(..., NULL)` 薄包装。详见 [`PHASE27.md`](Docs/phases/PHASE27.md)；单测 `Capi.*`。

### 工程可见（二十六期）

- **MDB 客户端模块化**：`structdb_client_mdb` 新增 `mdb_engine_ports.cpp`（`MdbEnginePorts`）、`mdb_runner_ops.cpp`（原 `mdb_runner.cpp` 内大块实现；**三十二期起** 已再拆为 `mdb_ops_*.cpp`，见 [`PHASE32.md`](Docs/phases/PHASE32.md)）、`mdb_dispatch.cpp`（**唯一** `#include "mdb_runner_dispatch.inc"`）；`persist_table` / `load_table_from_storage` / `IMPORTDIR` 等经 `MdbEnginePorts` 访问引擎。详见 [`PHASE26.md`](Docs/phases/PHASE26.md)。

### 破坏性 / 行为变更（二十期）

- **`structdb::infra::IoBackendKind`**：`IocpAsyncPlaceholder` 重命名为 **`IocpAsync`**，`IoUringAsyncPlaceholder` 重命名为 **`IoUringAsync`**；`io_backend_kind_is_async_placeholder` 仍对二者为 true（直至实现完成可再收紧语义）。依赖旧枚举名的调用方须更新。
- **`wal.segments`**：首行为 **`2`** 时表示 **多段 WAL 目录**（见 [`PHASE20.md`](PHASE20.md)）；旧引擎 **拒绝打开** 该库（须升级到含二十期的版本或使用迁移工具）。首行 **`1`** 或未存在该文件时行为与此前一致。
- **CMake**：新增 **`STRUCTDB_WITH_IOCP`**（Windows MSVC 默认 ON）、**`STRUCTDB_WITH_IO_URING`**（默认 OFF，仅 Linux 可选）。

### 说明（MVCC / newdb）

- **隔离与未提交写**：未提交变更主要在 MDB 会话内存；**`COMMIT`** 经 embed 批次落盘。当 **`EngineConfigSnapshot::mdb_persist_in_begin`**（默认 **true**）与 per-run 开关允许时，**`BEGIN`** 内成功变更也会经 **`persist_table`** 写入引擎（见 `POLICY` §4.3）。**默认** **`ROLLBACK`** 不撤销这些存储写；若 **`mdb_chain_rollback_on_mdb_rollback=true`**（**二十三 23C**），则 **`ROLLBACK`** 在恢复会话表前将 **`undo_stack_` 弹回 `BEGIN` 水位**（受限模型，见 [`PHASE23.md`](Docs/phases/PHASE23.md)）。`TXNISOLATION snapshot` 下 `BEGIN` 时固定存储读序号（`txn_snap_seq`）；从 `session.txn` **恢复后不重算、不回拨** `txn_snap_seq`（仍为文件中 `SNAP`），故重启后可能 **低于** `latest_commit_seq`；`read_committed` 下事务内存储读随 `latest_commit_seq`。引擎侧未 flush 数据依赖 **`wal.log` 重放**；REPL 未提交事务依赖 **`session_dir/session.txn`**：`BEGIN` 时写入基线快照，并在 **`TXNV2` 段** 追加 **v2 增量行**（`V2OP\tKIND\t<hex(payload)>`），崩溃后由首条 REPL 命令路径 **`txn_log_try_recover_repl_session`** 重放到 `current`（损坏或无法解析的 `V2OP` 行会导致整份 `session.txn` 被丢弃并结束事务）。**`[RECOVER]` 日志**：成功路径 `ok mode=baseline_only|v2_replay` 与 `v2_ops`/`snap_seq`；失败路径 `drop reason=...`（含 `detail=`）。**崩溃模型**：未 `fsync` 的尾部 v2 行可能在进程崩溃后丢失；`MdbRunOptions::fsync_each_session_txn_op` 与 `mdb_repl_execute_line(..., fsync_session_txn_op)` 为 true 时每次追加后 `fsync`（与 `fsync_each_batch` 独立）。
- **与 newdb**：`BULKINSERT` 语法见「新增」中 BULKINSERT 条目（`|` 列表为 StructDB 扩展）；非完整 PostgreSQL 式 MVCC，**单键单版本 + `read_max_seq` 裁剪**。
- **崩溃恢复模型**：单条 **`STDBBW1\n` 二进制 WAL 批次帧**（`commit_embed_batch` / `EmbedClient::submit`）在「该帧长度已完整落盘」的前提下重放为 **整批 dels/puts 一次提交到 MemTable**；与旧版「每键一行 `k=v\\n`」帧并存。帧内尾部长度不足或结构损坏会导致 `open` 失败；**外层 WAL 帧被截断**（文件尾不完整 4+n 字节）时该帧被忽略（与既有尾帧规则一致）。
- **六期（事务链严格语义）**：`BEGIN` 激活时 **`TXNISOLATION` 被拒绝**（须先 `COMMIT`/`ROLLBACK`）；`mdb_storage_read_seq_for_script` 在 **snapshot** 下将 `txn_snap_seq` **防御性上限裁剪**为 `engine.latest_commit_seq()`（正常会话不应触发）。
- **MDB 解析**：单独输入 **`SAVEPOINT`**（无保存点名称）现在返回明确错误 **`SAVEPOINT: need name`**（此前解析失败时 `last_error` 可能为空）。
- **`POLICY` §4.0（文件系统保底）**：`data_dir` / `session_dir` 落盘集合与恢复权威顺序；**默认引擎目录名**为 **`_data`**（与 `EngineConfigSnapshot` / `structdb_app` 一致，见下条）。
- **破坏性（默认路径）**：`EngineConfigSnapshot::data_dir` 与 `structdb_app` **`--data-dir` 默认值**由 **`.structdb` 改为 `_data`**（相对当前工作目录）；未指定 **`--session-dir`** 时仍为 **`{data_dir}/embed_session`**。升级已有库可将目录重命名为 **`_data`**，或继续使用 **`--data-dir .structdb`**。单测内引擎目录由 **`…/data`** 统一为 **`…/_data`**，与规范名一致。
- **七期（事务链 InnoDB）**：[`Docs/TXN_INNODB_MAP.md`](Docs/phases/TXN_INNODB_MAP.md)（概念映射 + 耐久 Level 0/1/2）；[`Docs/TXN_BEGIN_PERSIST_DESIGN.md`](Docs/phases/TXN_BEGIN_PERSIST_DESIGN.md)（7C/6D 条件草案）；`POLICY` **§3.5**、**§4.4–§4.5**。
- **八期（`undo.log` 4C 子集）**：[`Docs/phases/UNDO_LOG_4C.md`](Docs/phases/UNDO_LOG_4C.md)；`StorageEngine::undo_try_truncate_when_stack_empty`；`EngineConfigSnapshot::undo_auto_truncate_after_flush`（默认 false，与 `flush_memtable` 联动）。**8D / 6D–7C（历史）**：`BEGIN` 内 `persist_table` 曾以设计占位为主；**十七期**起默认经 **`EngineConfigSnapshot::mdb_persist_in_begin`** 开启（见 [`PHASE17.md`](Docs/phases/PHASE17.md)、[`TXN_BEGIN_PERSIST_DESIGN.md`](Docs/phases/TXN_BEGIN_PERSIST_DESIGN.md)）。
- **九期（Compaction 与 I/O）**：[`Docs/COMPACTION.md`](Docs/COMPACTION.md)（`compact_merge_two_oldest_l0`、`compaction_merge_count`）；[`src/engine/infra/include/structdb/infra/io_backend.hpp`](src/engine/infra/include/structdb/infra/io_backend.hpp)（`IoBackendKind` / 默认阻塞）；checkpoint 与 `undo.log` 前缀水位的 **v1 槽占位** 已由 **十期** 演进为 **v2 持久化**（见下条与 [`Docs/CHECKPOINT_UNDO_PREFIX.md`](Docs/phases/CHECKPOINT_UNDO_PREFIX.md)）。**9D / 6D–7C（历史）**：与「说明」八期条一致；**十七期**已默认开启 `BEGIN` 内 `persist_table`（可关）。**9E**：`compaction_merge_count` 为进程内观测；`EngineConfigSnapshot` / 耐久默认组合不变。
- **十期（checkpoint v2 与 `undo.log` 前缀回收）**：[`Docs/phases/PHASE10.md`](Docs/phases/PHASE10.md)；二进制槽 **v2（68 字节 `STCK`）** 持久化 `CheckpointState::undo_log_safe_prefix_bytes`；`StorageEngine::undo_try_truncate_recyclable_prefix`；`flush_memtable` / `compact_merge_two_oldest_l0` / `checkpoint()` / `wal_try_trim_prefix_through_checkpoint` 写旋转 checkpoint 时 **重算** 该水位；`open` **不**按水位自动截断 `undo.log`。
- **十一期（L0 compaction 阈值自动调度）**：[`Docs/phases/PHASE11.md`](Docs/phases/PHASE11.md)；`EngineConfigSnapshot::l0_compact_trigger_threshold`（**0**=关）、`l0_compact_max_rounds_per_flush`；`flush_memtable` 在 WAL/undo 可选钩子之后可 **同步多轮** 调用 **`compact_merge_two_oldest_l0`**（内部 `*_unlocked_` 实现，避免与 `flush` 重入死锁）；阈值对比 **MANIFEST 中 L0 段长度**（见 [`PHASE12.md`](Docs/phases/PHASE12.md)）。**不**含 L2+、并发 compaction、Scheduler 背压全链路。
- **十二期（MANIFEST L0/L1 MVP）**：[`Docs/phases/PHASE12.md`](Docs/phases/PHASE12.md)；`MANIFEST` **`FORMAT2`** 与 `ManifestSst::level`（0/1）；`Manifest::push_l0_sst` / `set_sst_entries` / `l0_prefix_length`；读路径 **L0 新→旧、再 L1 新→旧**；`EngineConfigSnapshot::l1_compact_output_from_l0_merge`（默认 false）与 `StorageEngine::set_l1_compact_output_from_l0_merge`。

### 新增

- **二十五期（MDB 命令库 / newdb 语义映射）**：[`PHASE25.md`](Docs/phases/PHASE25.md)；扩展 `mdb_parse_command_line` / `mdb_runner_dispatch.inc`：`DROP TABLE`、`RENAME TABLE`、`RESET`、`SHOW ATTR`/`DESCRIBE`、`SHOW KEY`、`SET PRIMARY KEY`（`PKCOL` 与 `FINDPK` 逻辑列）、`SHOW TUNING`/`SHOW STATUS`（及 JSON）、`SHOW STORAGE`（及 JSON）、`VACUUM`（flush+drain）、`SCAN`、`BULKINSERTFAST`、`IMPORTDIR`（**修复** `IMPORTDIR(path)` 无空格时误吞整行导致 `expected (path)`）、`EXPORT` 裸路径、`EXIT`/`QUIT`（`MdbRunResult::repl_exit_requested`）、`SHOWLOG`（`EmbedClient::embed_journal_path`）、`RELEASE SAVEPOINT`、`QBAL`、`SHOW PLAN`/`EXPLAIN WHERE`（文本）、`[NOT_SUPPORTED]` 矩阵；**`DEFATTR` 列类型**：`int`/`string`/`varchar`/`text`/`char`/`float`/`double`/`datetime`/`timestamp`（`type_matches` 严格校验，`WHERE` 双端可解析为浮点时用数值比较）；`structdb_tests` **`Mdb.Phase25*`**、**`Mdb.Phase25Strict*`**、**`Mdb.Strict*`**、**`Mdb.IntegrateTxnRecoverRollbackRestartChain`**；`Docs/README.md`、`PHASE13_PLUS_PLAN.md`（**`p25`**）索引。
- **二十四期（部分落地）**：[`PHASE24.md`](Docs/phases/PHASE24.md)；[`WAL_REPLAY.md`](Docs/phases/WAL_REPLAY.md)（`STDBBW1` / 文本 WAL 行、`open` 重放顺序；`POLICY` §3.1 链入）；[`ONBOARDING.md`](Docs/ONBOARDING.md)；`POLICY` §4.2 / §4.3 / §4.0.4 / **§4.5 后「嵌入式耐久矩阵」**；[`TXN_INNODB_MAP.md`](Docs/phases/TXN_INNODB_MAP.md) §2 与矩阵交叉引用；**24A** — `EngineConfigSnapshot::{observe_embed_bypass_during_mdb_chain_txn,strict_reject_direct_kv_put_during_mdb_chain_txn}`（默认 **false**）、`Engine::set_mdb_chain_txn_active_hint` / `embed_bypass_kv_put_during_mdb_chain_observed`；`mdb_runner_dispatch.inc` 在 **`BEGIN`/`COMMIT`/`ROLLBACK`** 维护 hint；`structdb_tests` **`Engine.Phase24*`**。根 **`README.md`** MVCC / `ROLLBACK` 与 **23C** 对齐；`Docs/README.md`、`PHASE13_PLUS_PLAN.md`（mermaid **`p24`**、索引）。
- **二十三期（部分落地）**：[`PHASE23.md`](Docs/phases/PHASE23.md)；**23A** — **`EngineConfigSnapshot::l0_compact_max_inline_rounds_per_flush`**（默认 **0**）：在非 defer 的 Phase 11 自动 L0 路径上，将单次 `flush_memtable` 内合并轮数上限收紧为 **`min(l0_compact_max_rounds_per_flush, …)`**；`structdb_tests` **`StorageEngine.Phase23L0InlineCapLimitsRoundsPerFlush`**。**23B** — **`l4_compact_output_from_l3_merge`**（默认 false）、**`StorageEngine::compact_merge_two_oldest_l3_to_l4`**、`FORMAT2` **level 4**；`structdb_tests` **`Manifest.Phase23Format2LoadsLevel4`**、**`StorageEngine.Phase23L3ToL4CompactAndRestart`**。**23C** — **`mdb_chain_rollback_on_mdb_rollback`**（默认 false）：为 true 且 **`mdb_persist_in_begin`** 时 MDB **`ROLLBACK`** 先 **`Engine::rollback_embed_undo_until`** 再恢复会话表；**`Engine::embed_undo_stack_depth`**；`mdb_tests` **`Mdb.TxnBeginPersistChainRollbackPopsStorageWhenEnabled`**；**`append_versioned_undo_if_needed_unlocked_`** 对 **`mdb$` 键首次可见写** 在查找无旧值时仍压入 **`mdb:tomb`** 作为 undo 上一态，使 **INSERT** 可被 `rollback_one_undo_frame` 撤销；`structdb_tests` **`StorageEngine.UndoTruncateSucceedsWhenStackEmptyAfterRollback`** 需 **两次** pop 方栈空。**23D** — `POLICY` **§2.4** WAL `WalWriter` I/O 后端支持矩阵（Blocking / IOCP / io_uring）。`COMPACTION.md`、`PHASE11.md`、`PHASE13_PLUS_PLAN` §7 / mermaid `p23`、`POLICY` §3.1 / §3.3.1 / §3.5 / §4.1 / §4.3、`TXN_BEGIN_PERSIST_DESIGN.md`、`PHASE22.md` §2/§5、`TESTING_TXN_CHAIN` §12、`README` 索引已同步。
- **十七期（`BEGIN` 内 `persist_table`）**：[`PHASE17.md`](Docs/phases/PHASE17.md)；**`EngineConfigSnapshot::mdb_persist_in_begin`**（默认 **true**）与 **`MdbRunOptions::allow_persist_while_txn_active_experimental`**（默认 **true**）**AND** 后，`run_mdb_script` / `mdb_repl_execute_line` 在 **`BEGIN`** 激活时对每次成功变更调用 **`persist_table`**。**默认** **`ROLLBACK`** 仍仅恢复会话逻辑表与 `session.txn`，**不**撤销已落盘存储（`POLICY` §4.3）；链式存储回滚见 **二十三期** 门闩。**`Mdb.TxnBeginPersist*`**；`TESTING_TXN_CHAIN` §3 / §10 / §12。

- **二十一期（部分落地）**：[`PHASE21.md`](Docs/phases/PHASE21.md)；**21A** — 可选 **`EngineConfigSnapshot::wal_archive_gc_after_flush`**（默认 false；须与 **`wal_auto_trim_prefix_after_flush`** 同开）：`flush_memtable` 在 WAL 前缀 trim 成功后更新 **`wal.segments` v2**（封存路径列表清空）并删除对应 **`wal/archive/`** 文件；`structdb_tests` 增补 **多段尾 `wal.log` 截断** 与封存 GC 用例。`POLICY` §3.3、`PHASE20` §4、`README`、`PHASE13_PLUS_PLAN` §11 已同步。**21B** — Linux CMake **`STRUCTDB_WITH_IO_URING=ON`** + **`pkg-config liburing`** 时 **`IouringSequentialFileWriter`** 与 **`WalWriter`** `IoUringAsync` 真路径；**`WalPipeline`** 文档锚点（`wal.hpp`）；条件编译单测 **`StorageEngine.WalIoUringBackendRoundTrip`**。**21C** — **`StoragePressureSnapshot`** 扩展 compaction worker 队列字段与 **`pending_deferred_l0_compact`**；**`ResourceBudget::set_compaction_slots_pressure_delta`**；**`EngineConfigSnapshot::{storage_pressure_compaction_queue_soft_pct,storage_pressure_deferred_l0_slot_tighten}`**；**`Engine::sync_scheduler_budget_from_storage_pressure`** 写入 **`CompactionSlots`** 压力增量；**`enqueue_drain_l0_compaction_and_wait(..., wait_ms)`** / **`drain_l0_compaction_queue(..., worker_wait_ms)`**；`structdb_tests` **`ResourceBudget.CompactionSlotsPressureDeltaFromPhase21`**、**`Engine.Phase21DeferredL0TightensCompactionSlotsBudget`**；`COMPACTION.md`、`POLICY` §2.1 / §4.2 索引。
- **二十二期（部分落地）**：[`PHASE22.md`](Docs/phases/PHASE22.md)；**22A** — **`EngineConfigSnapshot::l3_compact_output_from_l2_merge`**（默认 false）、**`StorageEngine::compact_merge_two_oldest_l2_to_l3`**、`FORMAT2` **level 3** 与既有层块读序；`structdb_tests` **`StorageEngine.Phase22L2ToL3CompactAndRestart`**、**`Manifest.Phase22Format2LoadsLevel3`**。**22B** — **`GraphExecutor::execute`** 在 **`use_budget_probe`** 下依次 **`acquire_for_node`** **WalQueueDepth、CompactionSlots、MemTableBytes**（各 1 单位），使 **`Orchestrator::set_on_backpressure`** 可观测 **`WalBacklogged` / `CompactionBusy` / `MemTableFull`**；`structdb_tests` **`Orchestrator.Phase22CompactionProbeBackpressureFires`**。**22C** — **`EngineConfigSnapshot::undo_segment_roll_max_bytes`**（默认 0）、**`undo.segments` v2**、**`undo/archive/{seq}.log`**、`UndoLog::sync` / **`truncate_prefix_at_path`**、逻辑字节串上的 **`undo_try_truncate_recyclable_prefix`** 与 **`kOpenFlagRebuildUndoStackFromLog`** 多段拼接；`structdb_tests` **`StorageEngine.Phase22UndoSegmentsRoll`**。**22D** — 原 **`PHASE17.md`** 门闩已由 **十七期** `mdb_persist_in_begin` 与 `Mdb.TxnBeginPersist*` 闭合（见 [`PHASE17.md`](Docs/phases/PHASE17.md)）。`COMPACTION.md`、`UNDO_LOG_4C.md`、`POLICY` §2.1 / §3.3 / §4.0、`PHASE13_PLUS_PLAN` §12–15、`TESTING_TXN_CHAIN` §10–11、`README` 索引已同步。
- **二十期（部分落地）**：[`PHASE20.md`](Docs/phases/PHASE20.md)；**20A** — `wal.segments` **v2**、`wal/archive/` 封存、`StorageEngine::set_wal_segment_roll_max_bytes` 与 `open` 链式重放、**`wal_try_trim_prefix_through_checkpoint`** 仍仅折叠 **`wal.log`**；**20B** — `EngineConfigSnapshot::enable_compaction_worker` / **`compaction_worker_queue_depth`**，`Engine::startup` 启动单 worker，`Engine::drain_l0_compaction_queue` 在启用时 **入队并等待** `drain_pending_l0_compactions`；**20C** — MSVC 下可选 **IOCP 顺序 WAL 写**（`WalWriter` + `STRUCTDB_WITH_IOCP`），Linux 下 **`STRUCTDB_WITH_IO_URING=ON` + liburing** 时 **io_uring 顺序 WAL 写**（二十一期 21B 合入真路径；默认 OFF 时仍为阻塞 `FileWriter`）。`POLICY` §3.3 / §4.0 / §4.2 已同步。
- **`Engine::kv_get` / `kv_visit_prefix` / `kv_put` / `kv_remove` / `latest_commit_seq`**：`mdb$` 键值带 `mdbver1:` 版本包装与 `mdb:tomb` 墓碑；`kv_get` 可选 `read_max_seq`（`UINT64_MAX` 表示最新可见）；`client/mdb` 经 Facade 访问，避免直接依赖 `storage_engine.hpp`。**`EngineConfigSnapshot::storage_open_flags`**（默认 0）传入 **`StorageEngine::open`**（例如 `StorageEngine::kOpenFlagRebuildUndoStackFromLog`，见 `CHANGELOG`/`POLICY` 风险说明）。**`EngineConfigSnapshot::wal_auto_trim_prefix_after_flush`**（默认 false）：为 true 时每次成功 **`flush_memtable`** 写 checkpoint 后自动 **`wal_try_trim_prefix_through_checkpoint`**（四期 4B，见 `POLICY` §3.3）。**`EngineConfigSnapshot::wal_archive_gc_after_flush`**（默认 false）：为 true 时（且须 **`wal_auto_trim_prefix_after_flush`**）在 trim 成功后执行 **`wal/archive/`** 封存 GC（二十一期 21A，见 [`PHASE21.md`](Docs/phases/PHASE21.md)）。**`EngineConfigSnapshot::undo_auto_truncate_after_flush`**（默认 false）：为 true 时在成功 **`flush_memtable`** 后截断 **`undo.log`**（八期 / 4C 子集，见 `Docs/phases/UNDO_LOG_4C.md`）。**`EngineConfigSnapshot::l0_compact_trigger_threshold` / `l0_compact_max_rounds_per_flush`**（十一期，默认 **0** 关闭自动 L0 合并）：见 [`Docs/phases/PHASE11.md`](Docs/phases/PHASE11.md)。**`EngineConfigSnapshot::l1_compact_output_from_l0_merge`**（十二期，默认 **false**）：见 [`Docs/phases/PHASE12.md`](Docs/phases/PHASE12.md)。**十三～十九期（部分落地，见 [`PHASE13_PLUS_PLAN.md`](Docs/phases/PHASE13_PLUS_PLAN.md)、[`PHASE19.md`](Docs/phases/PHASE19.md)）**：**`l0_compact_defer_after_flush`** + **`StorageEngine::drain_pending_l0_compactions`**；**`storage_pressure_l0_soft_start`** + **`Engine::sync_scheduler_budget_from_storage_pressure`** / **`storage_pressure_snapshot`** + **`ResourceBudget::set_wal_queue_depth_pressure_delta`**；**`l2_compact_output_from_l1_merge`** + **`compact_merge_two_oldest_l1_to_l2`**，`MANIFEST` `FORMAT2` 行级 **`level` 多字节解析** 与 **L2+ 读序**；**`wal.segments`** v1 元数据（当前仍为单物理 `wal.log`）；**`EngineConfigSnapshot::mdb_persist_in_begin`**（默认 **true**）与 **`MdbRunOptions::allow_persist_while_txn_active_experimental`** / **`MdbInteractiveSession::set_allow_persist_while_txn_active_experimental`**（**AND**；`ROLLBACK` 不撤销已落盘写，见 `POLICY` §4.3）；**`IoBackendConfig::kind`** 扩展 **IOCP/io_uring 占位枚举**（默认阻塞 I/O 不变）；**十九** **`GraphExecutor`** 注册 **`drain_l0_compaction`**，`l0_compact_defer_after_flush` 时 **`Orchestrator` 默认 / replan** 线性计划 **`noop`→`drain`**，**`Engine::rerun_default_pipeline`**。
- **`StorageEngine`**：`mdb$` 前缀键写入单调 `commit_seq`（`COMMIT_SEQ` 持久化）；**九期**：`compact_merge_two_oldest_l0`、`Manifest` **`FORMAT2`** / `set_sst_entries` / `push_l0_sst`、`compaction_merge_count()`；**十一期**：`compact_merge_two_oldest_l0_unlocked_`、`try_compact_l0_if_over_threshold_unlocked_`（`flush_memtable` 末尾，受 `l0_compact_trigger_threshold` 与 **L0 段长度** 约束）；**十二期**：`set_l1_compact_output_from_l0_merge`、分层读路径；**十期**：`CheckpointState::undo_log_safe_prefix_bytes` 经 **`CheckpointWriter::write_rotating`** 写入 **v2** 槽并在上述写 checkpoint 路径 **重算**；`undo_try_truncate_recyclable_prefix`；`undo_stack_` 条目记录 **undo 帧文件内起始偏移** 以支持保守前缀回收。详见 [`Docs/phases/PHASE10.md`](Docs/phases/PHASE10.md)、[`Docs/phases/PHASE11.md`](Docs/phases/PHASE11.md)、[`Docs/phases/PHASE12.md`](Docs/phases/PHASE12.md)。**同一 `EmbedClient::submit`/journal 回放行内** 所有需版本化的 put 通过 `reserve_commit_seq()` + `put(..., batch_seq)` **共享同一** `commit_seq`，保证读快照下整批原子可见；单键仍为覆盖写而非完整多版本链。`remove`/`visit_prefix`/`get` 尊重墓碑与读快照；MemTable 对墓碑视为缺失；写路径互斥。**启动时**自 `checkpoint` 中记录的 `wal_offset` 起重放 `wal.log` 中未 flush 的帧（不完整尾帧忽略）；`flush_memtable` 成功后更新 checkpoint 的 `wal_offset`/`redo_offset`。**版本化键**覆盖写前将 **MemTable 若无条目则从 SST（新到旧，跳过墓碑）** 取到的上一物理值写入 `undo.log`（`STRDBUV1` 帧）并维护进程内 LIFO 栈；`rollback_one_undo_frame()` 恢复上一版本（**默认**栈不跨重启；可选 `open(..., kOpenFlagRebuildUndoStackFromLog)` 在 WAL 重放后 **只读扫描 `undo.log`** 按帧顺序重建 `undo_stack_`，**须与 WAL 已重放状态一致**，否则语义不成立；见测试负例）。**`commit_embed_batch`**：`EmbedClient::submit` 使用单条 **`STDBBW1\n`** WAL 二进制帧承载整批 dels/puts（与旧 `key=value\\n` 文本帧并存），在「该帧完整 fsync 后崩溃」模型下重放为整批原子；`wal_sync()` 供批次末尾 fsync。**Redo**：`open` 恢复 **不以 `redo.log` 为准**；默认 **`kAppendRedoMirrorWal=false`** 下不向 redo 追加与 WAL 重复的 put 镜像（可按需改回编译期开关）。**四期**：`undo_log_bytes_on_disk`、`read_checkpoint_state`、`wal_try_trim_prefix_through_checkpoint`（WAL 前缀裁剪，须在 `flush_memtable` 后使用；见 `POLICY` §3.3）。**五期**：`checkpoint.a`/`checkpoint.b`+`checkpoint.active`（CRC32C 定长槽；**十期起**新写槽为 **v2**，仍可读 **v1**）与遗留 `checkpoint` 文本双写；`CheckpointState::checkpoint_seq`；`CheckpointWriter::read_latest`/`write_rotating`；`open` 若 checkpoint 中 `manifest_version` 大于已加载 MANIFEST 则失败（见 `POLICY` §3.4）。
- **`client/embed`**：`CommandBatch::dels`；journal 在 puts 前追加 `D` 段（`D\tndel\tk1\tk2...`），旧 journal 无 `D` 仍可读；`submit` 通过 **`StorageEngine::commit_embed_batch`** 先删后写、**单 WAL 记录**；**有 puts 时**在引擎内保留一次 `commit_seq` 分配（不再对每键单独 `reserve_commit_seq`）。成功后刷新 `read_snapshot_seq`。**`wal.log` 非空时** `open` 恢复仅重建 idempotency 与 seq 水位，**不再**向引擎二次 apply journal（避免与 WAL 重放双写）；`wal.log` 为空时仍按旧逻辑回放未 ack 行。`fsync_journal=true` 时由 **`commit_embed_batch(..., fsync_wal=true)`** 在单帧上 fsync（与 InnoDB group commit 类比：单批次末尾一次 WAL fsync）。`session_directory()` 供会话文件路径查询。**`session.ckpt`**：第三行可选记录引擎 **`checkpoint_seq`**（与 `POLICY` §4 / §3.4）；兼容仅两行的旧文件。
- **`EmbedClient::read_snapshot_seq` / `refresh_read_snapshot`**；`mdb` 脚本 `BEGIN` 固定 `txn_snap_seq`，`TXNISOLATION read_committed|snapshot` 控制存储读。
- **六期（事务链）**：`POLICY` **§4.0（文件系统保底）**、§4.1–§4.3；[`Docs/phases/TESTING_TXN_CHAIN.md`](Docs/phases/TESTING_TXN_CHAIN.md)；公开 **`mdb_storage_read_seq_for_script`**；`SHOW SNAPSHOT` 在事务激活时输出 **`txn_storage_read_seq`**（与当行存储读序号一致）。
- **`.mdb` / REPL**：`BULKINSERT`：**StructDB 扩展** — 括号为 `id,col,...|id,col,...`（`|` 分隔多行），与 newdb 若不同以本仓库为准；`RENATTR(old,new)`；`EXPORT JSON <path>` 写出 UTF-8 JSON 数组；`mdb_runner_dispatch.inc` 与 `run_mdb_script` / `mdb_repl_execute_line` 共用；**`MdbInteractiveSession`**（`unique_ptr<ReplSessionState>`）+ `mdb_repl_reset`；**`structdb_app --repl`** 真循环。`BEGIN` 时写入 `session_dir/session.txn`（`BEGIN` 时刻的 `txn_base` 快照正文为 `serialize_table`：`v1` + **`NAM\t<table>`** 行 + schema/行，便于恢复后 `current.name` 正确；`TXNV2` 段）；事务内每次成功变更追加一行 **v2 OP**；`COMMIT`/`ROLLBACK` 删除该文件；**REPL 首条命令前**若存在未闭合 `session.txn` 则恢复 `txn_active`、`txn_base` 与 **`current`（v2 重放）**；恢复时先将文件读入内存并关闭句柄后再删文件，避免 Windows 上删除仍被打开的文件失败。`SHOW TXN`（事务激活时含 `engine_latest_commit_seq`）/ `SHOW SNAPSHOT`（事务激活时另含 **`txn_storage_read_seq=`**，与当行 `storage_read_seq` 一致；见 `POLICY` §4.1）。`run_mdb_script` 开头清除孤儿 `session.txn` 以免脚本与 REPL 状态串线；**`MdbRunOptions::fail_if_unclosed_txn`** 为 true 时，脚本 EOF 若仍处 `BEGIN` 则返回失败（默认仍为隐式内存回滚）。**`ROLLBACK`**：恢复 MDB 会话逻辑表（`current` / `txn_base`）；**不**调用引擎 `rollback_one_undo_frame` 以撤销 `BEGIN` 内已 **`persist_table`** 的存储写（见 **`mdb_persist_in_begin`** 与 `POLICY` §4.3）。**`MdbRunOptions::fsync_each_session_txn_op`**：见上文崩溃模型。
- **`persist_table`**：`persist` 前按 `mdb$v2$idx$<table>$` 前缀收集旧二级索引键并加入 `dels`；**已存在 v2 schema 时不再写入** `mdb$v1$table$` 整表 hex 快照，并在同批 `dels` 中移除可能由 `CREATE TABLE` 留下的遗留 v1 键（仍以 `mdb$v2$*` + catalog 为权威；`load_table_from_storage` 保留对仅 v1 旧库的兼容）。
- **`mdb$v2$` 二级索引键（MVP）**：`persist_table` 对 string 列写入 `mdb$v2$idx$...`；内存侧 `str_idx` 加速 `=` 谓词；`checkpoint()` 触碰 `BufferPool`（占位）。
- **`mdb_command_parser`**：与 newdb `dispatch.cc` phase-2 前缀表对齐的动词分类（`UPDATEWHERE`/`DELETEWHERE`/事务等）。
- **`.mdb` 扩展**：`WHERE`/`COUNT(col,op,val)`/`UPDATEWHERE`/`DELETEWHERE`/`SETATTR`/`SETATTRMULTI`/`DELETE`/`DELETEPK`/`FIND`/`FINDPK`/`WHEREP`/`SUM`/`AVG`/`MIN`/`MAX`/`QBAL`(stub)/`LIST TABLES`/`SHOW TXN`/`SHOW SNAPSHOT`/`BEGIN`/`COMMIT`/`ROLLBACK`/`SAVEPOINT`/`ROLLBACK TO SAVEPOINT`/`BULKINSERT`/`RENATTR`/`EXPORT JSON`/`TXNISOLATION`/`HELP`。
- **`structdb_capi`（静态库）**：`structdb_run_mdb_file` / `structdb_run_mdb_file_ex` 仅依赖 Facade + Embed + MDB 栈（二十七期扩展选项与日志，见 [`PHASE27.md`](Docs/phases/PHASE27.md)）。
- **测试**：`tests/mdb_tests.cpp` 与 `capi_test.cpp`；含 `Engine.StartupPassesStorageOpenFlags`、`Engine.WalAutoTrimAfterFlushFromConfig`、`Engine.L0AutoCompactAfterFlushFromConfig`、`Engine.Phase24EmbedBypassCounterIncrementsWhenObserved`、`Engine.Phase24StrictRejectsDirectMdbKvPutWhenHinted`、`Engine.Phase24MdbBeginMaintainsHintForObservePath`、`StorageEngine.Phase4aObservabilityAfterVersionedPutAndFlush`、`StorageEngine.WalTrimPrefixThroughCheckpointSurvivesRestart`、`StorageEngine.WalTrimRejectsCheckpointPastEof`、`StorageEngine.CheckpointOpenRejectsCheckpointAheadOfManifest`、`CheckpointWriter.ReadLatestFallsBackWhenActiveSlotCorrupt`、`StorageEngine.LegacyCheckpointOnlyStillOpens`、`StorageEngine.GetSeesValueAfterFlush`、`Engine.WalReplaySurvivesRestartWithoutFlush`、`StorageEngine.VersionedUndoRollbackOne`、`StorageEngine.VersionedUndoUsesSstWhenMemEmpty`、`StorageEngine.EmbedWalBatchSingleRecordReplay`、`StorageEngine.WalTruncatedTailIgnoresPartialLastRecord`、`StorageEngine.WalReplayRejectsMalformedCompleteRecord`、`StorageEngine.WalBatchThenMalformedLineRecordRejectsOpen`、`StorageEngine.RebuildUndoStackFromLogAfterClose`、`StorageEngine.RebuildUndoStackFromLogRejectsBadMagic`、`StorageEngine.UndoTruncateRejectsWhenStackNonEmpty`、`StorageEngine.UndoTruncateSucceedsWhenStackEmptyAfterRollback`、`StorageEngine.UndoTruncateSucceedsAfterFlushMemtableClearsStack`、`StorageEngine.UndoTruncateThenOpenWithRebuildEmptyStack`、`Engine.UndoAutoTruncateAfterFlushFromConfig`、`Infra.IoBackendDefaultIsBlocking`、`CheckpointState.UndoPrefixBytesDefaultZero`、`StorageEngine.Phase10*`、`StorageEngine.Phase11*`、`StorageEngine.Phase12*`、`Manifest.Phase12*`、`Engine.L1CompactOutputFromConfig`、`StorageEngine.CompactionRejectsLessThanTwoSsts`、`StorageEngine.CompactionMergeTwoOldestSurvivesRestart`、`StorageEngine.CompactionNewerL0WinsDuplicateKey`、`EmbedClient.MultiKeyBatchFsyncSurvivesRestart`、`EmbedClient.SessionCkptPersistsCheckpointSeq`、`Mdb.WhereUpdateWhereTxn`、`Mdb.ReplSessionPersistsTxnAcrossLines`、`Mdb.ReplRestoresTxnFromSessionTxn`、`Mdb.ReplRestoresTxnV2MultiInsertAfterRestart`、`Mdb.ReplSessionTxnV2CorruptLineDropsLog`、`Mdb.TwoReplSessionsAfterEngineRestart`、`Mdb.BulkInsertRenattrExportJson`、`Mdb.EngineKvReadSeqVisibility`、`Mdb.PersistClearsStaleSecondaryIndex`、`Mdb.TwoEmbedSessionsStaleReadSnapshot`、`Mdb.TwoEmbedSessionsAfterEngineRestart`、`Mdb.ScriptFailsOnUnclosedTxnWhenOptSet`、`Mdb.TxnChainStorageReadSeqHelperAndShowSnapshot`、`Mdb.TxnChainTxnIsolationRejectedDuringOpenTxn`、`Mdb.TxnChainReadCommittedShowSnapshotTracksLatest`、`Mdb.TxnChainScriptSnapshotShowSnapshotConsistent`、`Mdb.TxnChainShowSnapshotNoTxnOmitsTxnStorage`、`Mdb.TxnChainAfterCommitShowSnapshotOmitsTxnStorage`、`Mdb.TxnChainHelperNonTxnMatchesEmbedReadSnapshot`、`Mdb.TxnChainScriptTxnIsolationDuringTxnFails`、`Capi.RunMdbFile`。
- **`client/embed`**：`submit` 拒绝在 `idempotency_token`、`client_session_id`、任意 put 键值或 **del 键** 中含 `\t`/`\n`/`\r` 的批次（与 tab 分隔的 journal 行不兼容）；journal 与 checkpoint 中的无符号整数改用 `std::from_chars` 解析，避免损坏行触发 `stoull` 异常；读 journal/checkpoint 行时去除行尾 `\r`。
- **Windows / MSVC + `/MT`（默认）**：`gtest_capi` 与 GoogleTest 改为 **静态** 链入 `structdb_tests`，避免 **`gtest.dll` 与测试 EXE 各用一套静态 CRT 堆** 触发的 Debug 断言（`debug_heap.cpp` / `__acrt_first_block == header`）。若将 `STRUCTDB_STATIC_MSVC_RUNTIME` 设为 `OFF`（`/MD`），仍可按原方式使用共享 `gtest` / `gtest_capi`。
- 仓库根 **[README.md](../README.md)**：项目简介、指向 `Docs/` 的链接、MSVC 快速构建与常用 CMake 选项。
- **七期文档**：[`Docs/TXN_INNODB_MAP.md`](Docs/phases/TXN_INNODB_MAP.md)、[`Docs/TXN_BEGIN_PERSIST_DESIGN.md`](Docs/phases/TXN_BEGIN_PERSIST_DESIGN.md)；`POLICY` §3.5、§4.4–§4.5；`Docs/phases/TESTING_TXN_CHAIN.md` 七期维度；`Docs/README.md` 索引更新。
- **八期 / 四期 4C（子集）**：[`Docs/phases/UNDO_LOG_4C.md`](Docs/phases/UNDO_LOG_4C.md)；`undo.log` 在 **`undo_stack_` 为空**时的整文件截断 API；可选 **`undo_auto_truncate_after_flush`**（`flush_memtable` 成功后截断，默认关闭）。
- **九期文档**：[`Docs/COMPACTION.md`](Docs/COMPACTION.md)、[`Docs/CHECKPOINT_UNDO_PREFIX.md`](Docs/phases/CHECKPOINT_UNDO_PREFIX.md)；`structdb::infra::IoBackendKind` / `IoBackendConfig`（[`src/engine/infra/include/structdb/infra/io_backend.hpp`](src/engine/infra/include/structdb/infra/io_backend.hpp)）。**十期文档**：[`Docs/phases/PHASE10.md`](Docs/phases/PHASE10.md)。**十一期文档**：[`Docs/phases/PHASE11.md`](Docs/phases/PHASE11.md)。**十二期文档**：[`Docs/phases/PHASE12.md`](Docs/phases/PHASE12.md)。

### 计划

- 按 `Docs/POLICY.md` 与分层计划迭代 IOCP/io_uring 后端、compaction 与更强持久化保证。
- **四期（分阶段）**：
  - **4A**：`wal.log` / `undo.log` 字节与 `checkpoint` 可观测（`wal_log_bytes_on_disk`、`undo_log_bytes_on_disk`、`read_checkpoint_state`）；文档见 `POLICY` §3.3 与 `README` 适用边界。
  - **4B**：`wal_try_trim_prefix_through_checkpoint` 与可选 `EngineConfigSnapshot::wal_auto_trim_prefix_after_flush`（`flush_memtable` 成功后裁剪 WAL 前缀）。
  - **4C（部分落地）**：`undo.log` **整文件**截断（栈空 + 可选 `flush` 后自动），见 [`UNDO_LOG_4C.md`](Docs/phases/UNDO_LOG_4C.md)；**与 checkpoint 联合的「安全前缀」水位与 v2 槽** 见 [`CHECKPOINT_UNDO_PREFIX.md`](Docs/phases/CHECKPOINT_UNDO_PREFIX.md)、[`PHASE10.md`](Docs/phases/PHASE10.md)（**十期**）；**`undo.log` 物理分段（`undo.segments` v2）** 见 **二十二 22C**（[`PHASE22.md`](Docs/phases/PHASE22.md)）。**WAL 多段 v2**（二十期）与 **可选 `wal/archive/` 封存 GC**（二十一期 21A，默认关闭）见 [`PHASE20.md`](Docs/phases/PHASE20.md)、[`PHASE21.md`](Docs/phases/PHASE21.md)。
- **五期（checkpoint 链，Unreleased 已合入）**：
  - **5A**：`POLICY` §3.4 与下述实现同步。
  - **5B–5C**：双槽 + `read_latest` / `write_rotating`；`StorageEngine` 全路径接入；`open` 校验 checkpoint 不超前于 MANIFEST。
  - **5D**：`session.ckpt` 第三行 `checkpoint_seq`；`EmbedClient::last_engine_checkpoint_seq`。
- **六期（事务链，Unreleased 推进中）**：
  - **6A**：`POLICY` §4.0、§4.1–§4.3、`Docs/phases/TESTING_TXN_CHAIN.md`；`mdb_storage_read_seq_for_script` 公开；`CHANGELOG` 计划条目。
  - **6B**：读路径审计（`mdb_runner_dispatch` / `persist_table`）；`SHOW SNAPSHOT` 输出 `txn_storage_read_seq`；回归 `Mdb.TxnChain*`。
  - **6C**：`POLICY` §4.2 单写者 MVP 与乐观冲突（非默认）说明。
  - **6D**：`persist_table` 在 **`BEGIN`** 内默认执行（**`mdb_persist_in_begin`**）；默认 **不**将 MDB `ROLLBACK` 与 `undo_stack_` 链式对齐；**二十三 23C** 以 **`mdb_chain_rollback_on_mdb_rollback`**（默认 false）可选开启。
  - **6E**：本文档维度 + GTest filter 建议；可选 [`scripts/txn_chain_gtest_slice.ps1`](../scripts/txn_chain_gtest_slice.ps1)（无 CI 百分比门禁）。
- **七期（事务链 InnoDB，Unreleased 推进中）**：
  - **7A**：[`Docs/TXN_INNODB_MAP.md`](Docs/phases/TXN_INNODB_MAP.md)；`POLICY` §4.4–4.5、`Docs/README.md` 索引；[`Docs/TXN_BEGIN_PERSIST_DESIGN.md`](Docs/phases/TXN_BEGIN_PERSIST_DESIGN.md) 链入 §4.3。
  - **7B**：耐久 Level 0/1/2 映射（`TXN_INNODB_MAP` §2 + `EngineConfigSnapshot` / `MdbRunOptions` 注释）；默认行为不变。
  - **7C**：`BEGIN` 内 `persist_table` — 默认开启（**`mdb_persist_in_begin`**）；跨层 `ROLLBACK` 对齐 `undo_stack_` 以 **二十三 23C** 门闩 **`mdb_chain_rollback_on_mdb_rollback`** 交付（默认 **关**）。
  - **7D**：`POLICY` §3.5 `undo.log` 生命周期与 4C / WAL trim / 重建栈论证。
  - **7E**：`TESTING_TXN_CHAIN` InnoDB/耐久维度；`txn_chain_gtest_slice.ps1` 注释扩展。
- **八期（`undo.log` 回收 / 4C 落地，Unreleased）**：
  - **8A**：[`Docs/phases/UNDO_LOG_4C.md`](Docs/phases/UNDO_LOG_4C.md)；`POLICY` §3.5 详稿链；`Docs/README.md` 索引；本「计划」条目。
  - **8B**：`StorageEngine::undo_try_truncate_when_stack_empty`、`UndoLog::truncate_to_empty`；`EngineConfigSnapshot::undo_auto_truncate_after_flush`（默认 false）；`Engine::startup` 注入 setter。
  - **8C**：`structdb_tests` 非法拒绝 / 合法截断 / Facade 配置联动；`TESTING_TXN_CHAIN` 八期 filter 建议。
  - **8D（历史）**：六期 **6D** / 七期 **7C** 曾以设计占位为主；**十七期**已默认合入 `BEGIN` 内 `persist_table`（可关）。
- **九期（Compaction 与 I/O，Unreleased）**：
  - **9A**：[`Docs/COMPACTION.md`](Docs/COMPACTION.md)；`StorageEngine::compact_merge_two_oldest_l0`、`compaction_merge_count()`；`POLICY` §3.3.1；`structdb_tests` `StorageEngine.Compaction*`。
  - **9B**：[`io_backend.hpp`](src/engine/infra/include/structdb/infra/io_backend.hpp) 可切换种类骨架（默认 `Blocking`，与现有 `FileWriter` 行为等价）。
  - **9C**：[`CHECKPOINT_UNDO_PREFIX.md`](Docs/phases/CHECKPOINT_UNDO_PREFIX.md) 与 [`PHASE10.md`](Docs/phases/PHASE10.md) — **`undo_log_safe_prefix_bytes` v2 槽持久化**、**`undo_try_truncate_recyclable_prefix`**（由 **十期** 合入；`structdb_tests` `StorageEngine.Phase10*`）。
  - **9D（历史）**：曾记 **6D / 7C** 未启动；**十七期**已默认开启（可关）。
  - **9E**：`compaction_merge_count` 与 `POLICY` §7 可观测性说明；`EngineConfigSnapshot` 默认组合不变。
- **十一期（L0 阈值自动 compaction，Unreleased 已合入）**：
  - **11A**：[`PHASE11.md`](Docs/phases/PHASE11.md)；`EngineConfigSnapshot::l0_compact_*`；`flush_memtable` 内 `try_compact_l0_if_over_threshold_unlocked_`；`POLICY` §3.3.1、`COMPACTION`、`TESTING_TXN_CHAIN` §8；`structdb_tests` `StorageEngine.Phase11*`、`Engine.L0AutoCompactAfterFlushFromConfig`。
- **十二期（MANIFEST L0/L1 MVP，Unreleased 已合入）**：
  - **12A**：[`PHASE12.md`](Docs/phases/PHASE12.md)；`Manifest` `FORMAT2` / `ManifestSst`；`EngineConfigSnapshot::l1_compact_output_from_l0_merge`；读路径 L0/L1；`POLICY` §3.3.1、`COMPACTION`、`TESTING_TXN_CHAIN` §9；`structdb_tests` `StorageEngine.Phase12*`、`Manifest.Phase12*`、`Engine.L1CompactOutputFromConfig`。
- **十三期及后续（路线图草案，仅文档）**：[`PHASE13_PLUS_PLAN.md`](Docs/phases/PHASE13_PLUS_PLAN.md) — 十三～**二十四**期边界与推荐顺序（非已实现承诺）。**十四～十八期**：与 `POLICY` §2.2 / §3.3–3.5 / §4.2–4.3 及现有 `ExecutionScheduler` / `io_backend.hpp` 锚点对齐的 **目标 / 非目标 / 验收** 扩写见该文档 §4–§8；**十九期**见 **§9** 与 [`PHASE19.md`](Docs/phases/PHASE19.md)；**二十期**见 **§10** 与 [`PHASE20.md`](Docs/phases/PHASE20.md)；**二十一期**见 **§11** 与 [`PHASE21.md`](Docs/phases/PHASE21.md)；**二十二期**见 **§12** 与 [`PHASE22.md`](Docs/phases/PHASE22.md)。**代码（Unreleased 部分落地）**：十三 `l0_compact_defer_after_flush` + `drain_pending_l0_compactions`；十四 压力快照 + `wal_queue_depth_pressure_delta`；十五 L2 manifest/读序 + `compact_merge_two_oldest_l1_to_l2`；十六 `wal.segments` v1；十七 MDB 实验门闩；十八 `IoBackendKind`；**十九** `GraphExecutor` `drain_l0_compaction` 与 `Engine::rerun_default_pipeline`；**二十** 多段 WAL v2、compaction worker、IOCP/io_uring 构建开关与 WAL 写路径（见 [`PHASE20.md`](Docs/phases/PHASE20.md)）；**二十一** 可选封存 GC、io_uring WalWriter、compaction 背压深化（见 [`PHASE21.md`](Docs/phases/PHASE21.md)）；**二十二** L3 compaction、GraphExecutor 多资源背压探测、`undo` 物理分段（见 [`PHASE22.md`](Docs/phases/PHASE22.md)）。
- **二十四期（文档与可选观测）**：[`PHASE24.md`](Docs/phases/PHASE24.md) — 单写者/绕过 MDB、耐久矩阵、WAL 重放、ONBOARDING；与 **`PHASE13_PLUS_PLAN`** mermaid **`p24`**、`CHANGELOG`「新增」交叉引用。

---

## [0.1.0] - 2026-05-12

### 新增

- **根 CMake 超级工程**：`project(StructDB 0.1.0)`，统一 `Base`、`engine/*`、`client/embed`、`app`、`tests`、`benchmarks` 与 `ThirdParty/gtest_capi`。
- **第三方依赖策略**：默认使用 `ThirdParty/` 中的 **fmt、spdlog、Google Benchmark**；可选 `STRUCTDB_FETCH_FMT_SPDLOG_BENCHMARK=ON` 通过 **FetchContent** 拉取固定标签版本（见 `cmake/StructDBThirdParty.cmake`）。
- **MSVC 运行时**：`STRUCTDB_STATIC_MSVC_RUNTIME` 与 `gtest_capi` 的静态 CRT 选项对齐。
- **MinGW 提示**：在检测到 MinGW / Windows 上 GNU 工具链时输出说明性 `message`。
- **分层运行时模块**：`engine/infra`（日志、Tracer、Metrics、RAII 文件句柄、线程池、泄漏检测钩子、`io_backend.hpp` 路线图、`lockfree_queue` 互斥后备）、`engine/planner`（DAG、`plan_epoch`、逻辑 buffer 元数据）、`engine/scheduler`（预算与背压）、`engine/runtime`（`GraphExecutor`、`IOperator`、协作式取消）、`engine/orchestrator`、`engine/facade`（`Engine`、`ServiceContainer`、配置快照）。
- **存储 MVP**：WAL、MemTable、Manifest、`LsmState`、Buffer pool、Redo/Undo、checkpoint 协议等与 `wf::storage` 类型的对接与扩展占位。
- **嵌入式客户端**：`client/embed` 会话 journal、checkpoint、幂等 token、崩溃恢复路径；对外薄头 `structdb/client/structdb_embed.hpp`。
- **应用入口**：`app/main.cpp`，支持 `--data-dir` / `--session-dir` / `--repl` / `--run-mdb`；默认 **`--data-dir` 为 `_data`**，默认会话为 **`_data/embed_session`**。
- **测试**：`structdb_tests`（GoogleTest + `gtest_capi_init_from_argv` / `gtest_capi_run_all`）；Windows 下将所需 `gtest` / `gtest_capi` DLL 复制到可执行文件旁以便 `ctest`。
- **基准**：`structdb_bench`（MemTable、WAL、Compaction 挑选类 baseline）。

### 变更

- 无更早基线；本版本为首次整合发布型快照。

### 修复

- **Windows / MSVC 与 Base**：在 `wf` 相关源码中处理无 `endian.h`、`likely`/`unlikely` 等可移植性差异，避免在非 GCC 环境下编译失败。
- **UTF-8**：`Base` 目标在 MSVC 下启用 `/utf-8` 以稳定处理含中文或 UTF-8 源文件的场景。

### 工程说明

- **IO 策略**：存储与 infra 首版以阻塞 I/O + 线程池为主；**不把 `io_uring` 作为 Windows 必选路径**；后续替换点集中在 `io_backend.hpp` 文档与 `FileWriter`/`FileReader`/`ThreadPool` 调用边界。
