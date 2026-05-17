# PHASE40：分帧 WAL 与导入快路径

在 PHASE39（脚本摊销、单次全量 persist、skip undo）基础上，通过 **MDB 分块 submit**、**导入 raw 逻辑值**、**plain 行落盘**、延迟 **`row_ids_ordered`**、可选 **MemTable 批量 put** 与存储层 **embed 批自动分帧** 降低单帧百万键的长尾与 per-key 固定成本。

## 开关（`EngineConfigSnapshot`）

| 字段 | 默认 | 说明 |
|------|------|------|
| `mdb_persist_chunk_max_puts` | 32768 | 每帧最多行 put；catalog/schema/`row_index` 仅末帧 |
| `mdb_persist_chunk_max_frame_bytes` | 8 MiB | 估算 WAL 帧字节超限时也分块；`0` 仅按行数 |
| `mdb_bulk_persist_plain_rows` | true | 大表/bulk 行值为 tab 分隔 plain blob，免 `mdbhex1:`；plain 批次可 `write_journal=false` |
| `storage_import_store_raw_logical` | false | 显式开启 raw；`mdb_bulk_import_mode` 时 persist 自动 raw |
| `storage_embed_batch_max_frame_bytes` | 0 | 存储层 `commit_embed_batch` 内部分帧；`0` 关闭 |
| `memtable_bulk_put_enabled` | false | 大批 put 前按键排序 + `reserve_capacity`（opt-in） |
| `mdb_script_amortize_bulk_dml` | true | 脚本内 `BULKINSERT*` 推迟到 EOF / `FLUSH PERSIST`（REPL/事务内行为不同，见下） |

CLI / 脚本：`structdb_app --mdb-bulk-import`、`--memtable-bulk-put`、`--mdb-persist-chunk-max-puts`、`--storage-embed-batch-max-frame-bytes`；压测见 `scripts/bench/mega_data_mdb_stress.ps1`（见 `ENGINE_RUNTIME_CONFIG.md`）。

## MDB 分块 persist

- 大表全量 snapshot（脏行 >8192 或全表）且超过 chunk 阈值时，`persist_table` 调用 `persist_table_chunked`。
- 首块：`snapshot` del + 可选全表 `sec_idx` del；中间块：仅行 put；末块：行 put + catalog/schema/`row_index`/sec_idx。
- 每块独立 `idempotency_token`：`{base}:chunk:{i}`；仅末块 `fsync`（与 `fsync_each_batch` 对齐）。
- 小表增量路径（≤8192 脏行）不变。
- **多表脚本**：每张表须各自 `FLUSH PERSIST`（或 `USE` 该表后 EOF 刷盘）；不能假定脚本结束只 persist 当前 `USE` 表。

## 行索引与 plain 行

- 全表 bulk（行数 >8192 且整表脏）时 **延迟** 维护有序 `row_ids_ordered`，persist 前 `logical_row_index_rebuild_from_rows`，避免 O(n²) 有序插入。
- `mdb_bulk_persist_plain_rows`：大行用 `\t` 分隔 logical blob；含 `\t` 的字符串列仍可用 hex 路径（parity 见 `Mdb.Phase40StrictPlainVsHexStringParity`）。

## 导入 raw 逻辑值

- 条件：`import_batch_skip_undo` 且 `import_store_raw_logical`（bulk 导入由 `apply_storage_persist_hints` 在 `mdb_bulk_import_mode` 时开启）。
- `commit_embed_batch` 内对 `mdb$*` 键不写 `ver$` 包装；读路径 `unwrap_visible` 兼容非版本前缀。
- raw 导入批可直接用 `puts` 写 WAL/MemTable，避免 `stored_puts` 二次拷贝。
- **仅 insert-only 导入**；非导入路径不得默认开启。

## REPL / 事务与脚本差异

| 上下文 | `BULKINSERTFAST` persist 时机 |
|--------|------------------------------|
| **脚本** + `mdb_script_amortize_bulk_dml=true` | EOF / `FLUSH PERSIST` 摊销（推荐压测路径） |
| **REPL** | 每批可立即 `persist_now()` |
| **事务内 REPL** | `script_amortize && !txn_active` 为 false → 事务内 bulk **立即 persist**；`ROLLBACK` 仅恢复 session 除非 `mdb_chain_rollback_on_mdb_rollback=true` |

## 耐久与回滚

- 每 WAL 帧原子；分块中途崩溃可能已持久化部分行（与 PHASE39 导入语义一致）。
- 导入批无 undo 帧 → `rollback_one_undo_frame` / MDB chain rollback **不保证**恢复该批。
- WAL 尾残缺字节：重放忽略未帧化尾部（`WalReplayIgnoresUnframedTrailing*` 类用例）；非法长度前缀帧 → `open` 失败。

## 观测

- `STRUCTDB_TRACE=1`：`embed.submit.chunk`、`stdb.storage.commit_embed_batch`（payload 数量为 puts 数）。
- `SHOW TUNING` / `SHOW TUNING JSON` 暴露上述字段。

## 性能门禁（本机参考）

条件：`STRUCTDB_MEGA_ROWS=1000000`、`RowsPerLine=1000`（`mega_data_mdb_stress.ps1`），Release 构建。结果归档 `scripts/results/mega_data_summary_*.json`。

| 场景 | 约 TPS（相对 PHASE39 ~7.5K） |
|------|------------------------------|
| 默认（脚本摊销 + 分块 + plain） | **~238K**（~32×） |
| `--mdb-bulk-import`（+ import raw / skip undo） | **~328K**（~44×） |
| `memtable_bulk_put` + bulk import | 可再抬一档（视磁盘/fsync） |

向 **100×**（相对 PHASE39 基线 ~750K+）或相对当前 ~300K 再 ×100 的路线图见 [`OPTIMIZATION_PLAN.md`](../OPTIMIZATION_PLAN.md) §0.4（IMPORT_SEGMENT、流式 chunk build、group commit 等）。

## 回归

### GTest 切片

```text
structdb_tests --gtest_filter=Mdb.Phase40*
structdb_tests --gtest_filter=Mdb.*
structdb_tests --gtest_filter=StorageEngine.WalReplayImportRaw*:StorageEngine.WalReplay*Chunked*:StorageEngine.CommitEmbedBatchAutoSplit*
```

### `Mdb.Phase40*` 矩阵（24 项）

| 类别 | 用例 | 要点 |
|------|------|------|
| 基础 | `Phase40ChunkedPersistCountAfterRestart` | 小 chunk、冷启动 COUNT |
| 基础 | `Phase40ChunkedMetadataOnlyOnLastChunk` | 中间帧仅行 put，末帧含 `row_index` |
| 基础 | `Phase40ImportRawParityAfterRestart` | raw vs hex 重启一致 |
| 基础 | `Phase40PlainRowMonotonicIndexCountAfterRestart` | plain + 单调索引 |
| 严格 | `Phase40StrictTenKDeferredIndexRestart` | 10K、PAGE/WHERE、多 WAL 帧 |
| 严格 | `Phase40StrictLexicographicStringIdsRestart` | 字符串 PK 字典序 |
| 严格 | `Phase40StrictBulkThenMutateRestart` | bulk 后 UPDATE/DELETE + FLUSH |
| 严格 | `Phase40StrictNestedTxnBulkCommitRestart` | SAVEPOINT / ROLLBACK TO |
| 严格 | `Phase40StrictCrossTableBulkIsolation` | 分表 FLUSH、冷启动隔离 |
| 严格 | `Phase40StrictPlainVsHexStringParity` | 特殊字符列 parity |
| 严格 | `Phase40StrictImportModeSecIdxRestart` | IMPORT + REBUILD INDEX |
| 严格 | `Phase40StrictTinyChunkPersistRestart` | 极小 chunk 边界 |
| 失败 | `Phase40FailureBulkFastArityAndDuplicate` | arity / 重复 id（非原子批） |
| 失败 | `Phase40FailureWalCorruptAfterChunkedPersist` | 损坏 WAL 帧拒绝 open |
| 恢复 | `Phase40RecoveryWalTailAfterChunkedBulk` | 尾字节忽略 |
| 恢复 | `Phase40RecoveryWalReplayWithoutMemtableFlush` | 仅 WAL 重放 |
| 恢复 | `Phase40RecoverySstAfterWalTrim` | flush + WAL trim → SST |
| 事务 | `Phase40StrictTxnRollbackDiscardsBulk` | chain rollback + session COUNT |
| 联动 | `Phase40IntegrationIncrementalPersistBulkParity` | 增量 persist 开/关 |
| 联动 | `Phase40IntegrationCoalesceBulkRestart` | coalesce + bulk |
| 联动 | `Phase40IntegrationEmbedBatchSplitBulkRestart` | 存储层分帧 + MDB 分块 |
| 联动 | `Phase40IntegrationMemtableBulkPutRestart` | memtable bulk vs 默认 |
| 联动 | `Phase40IntegrationCheckpointAfterBulkPersist` | `save_checkpoint` 后会话 |
| 联动 | `Phase40IntegrationImportModeTxnRollbackToRestart` | IMPORT + SAVEPOINT |

### StorageEngine

- `WalReplayImportRawBatch`、`WalReplayImportRawBatchAfterAutoSplit`
- `WalReplayIgnoresTailByteAfterChunkedMdbEmbedBatch`、`WalReplayRejectsCorruptAfterChunkedMdbEmbedBatch`
- `CommitEmbedBatchAutoSplitByFrameBytes`

**更广 MDB 回归**：`Mdb.*` 共 **127** 项（含 Phase40），与事务链 / SETATTRMULTI / Phase25–39 无冲突。
