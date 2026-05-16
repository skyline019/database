# 事务链：读路径审计与测试切片（六期 / 七期 / 八期 / 九期 / 十期 / 十一期 / 十二期）

本文档与 [`POLICY.md`](POLICY.md) **§4.0（文件系统保底）**、**§4.1–§4.5** 对齐，说明 StructDB **MDB/REPL + Embed + `StorageEngine`** 的读视图如何落到 `read_max_seq`，并给出 **GoogleTest 过滤** 切片（借鉴 newdb `TXN_CHAIN_MATURITY` 文档的维度思想；本地可参考 `E:/db/DB/newdb/docs/testing/TXN_CHAIN_MATURITY.md`）。**InnoDB 术语与非等价映射**见 [`TXN_INNODB_MAP.md`](TXN_INNODB_MAP.md)。

## 1. 读路径审计表（引擎 KV）

| 入口 | 是否传入事务内 `storage_read_seq` | 说明 |
|------|-------------------------------------|------|
| `TXNISOLATION` | N/A（不读 `mdb$`） | **事务外**方可切换；`BEGIN` 内拒绝（见 `POLICY` §4.1） |
| `load_table_from_storage` / `table_exists_in_storage` | 是 | 由调用方传入 `read_max_seq`（脚本/REPL 为 `mdb_storage_read_seq_for_script` 结果） |
| `mdb_runner_dispatch.inc` 中 `LIST TABLES` 的 `kv_visit_prefix(kCatalog, …, storage_read_seq)` | 是 | 与脚本/REPL 当前行 `storage_read_seq` 一致 |
| `persist_table` 内 `kv_visit_prefix`（删旧二级索引前缀） | 否（`read_max_seq` 默认最新） | 在 **`persist_now`** 为真时调用（含 **`BEGIN` 内** 当 `mdb_persist_in_begin` 与 per-run 开关允许时）；与 §4.3 一致 |
| `Engine::kv_get` 无三参封装 | 否 | 表示「最新可见」；不得用于事务内需钉快照的 `mdb$` 诊断 |

## 2. 与 newdb 的对照（非实现依赖）

| newdb 概念 | StructDB 对应 |
|------------|----------------|
| `TxnCoordinator` + 读路径 `HeapQueryReadScope` | `mdb_storage_read_seq_for_script` + `Engine::kv_get(..., read_max_seq)` |
| `Snapshot` / `ReadCommitted` | `TXNISOLATION snapshot` 钉 `txn_snap_seq` / `read_committed` 用 `latest_commit_seq()` |
| `WriteConflictPolicy` | 见 **POLICY §4.2**（默认单写者 MVP） |

## 3. 建议的 `structdb_tests` / `mdb_tests` 过滤维度

`tests/mdb_tests.cpp` 中 **`Mdb.TxnChain*`**：REPL 路径统一经 **`TxnChainStrictRepl::strict_new_table_from_create`** — **CREATE TABLE → USE → DEFATTR → LIST TABLES（含 `[TABLE] <name>`）→ COUNT（rows=0）** 后才进入事务/快照断言；`.mdb` 脚本用例在 `BEGIN` 前显式包含 **`LIST TABLES` + `COUNT`**，并由 **`assert_mdb_script_log_has_strict_ddl_prefix`** 校验日志。用例矩阵含 **正确用法**（`TXNISOLATION read committed` 含空格、未知 tail 回落 snapshot、二次 `BEGIN` 不改变 `snap_seq`、`SAVEPOINT` + `ROLLBACK TO SAVEPOINT` 正向路径、无事务 `COMMIT`/`ROLLBACK` 仅打日志、脚本无 `BEGIN` 的 `COMMIT`）与 **错误用法**（事务中 `TXNISOLATION`、无事务 `SAVEPOINT`/`ROLLBACK TO SAVEPOINT`、未知 savepoint、重复 `DEFATTR`、已存在表再 `CREATE TABLE`、裸 `SAVEPOINT`、脚本无 `BEGIN` 的 `SAVEPOINT`、仅 `ROLLBACK TO SAVEPOINT` 无尾名时 **parse 为 unknown verb** 等）。

本地跑事务链相关用例（示例，PowerShell）：

```powershell
.\build\tests\Release\structdb_tests.exe --gtest_filter="Mdb.TxnChain*:Mdb.Repl*:Mdb.WhereUpdateWhereTxn:Mdb.EngineKvReadSeqVisibility:Mdb.TwoEmbedSessions*"
```

或使用仓库内薄脚本（默认 `../build`，可通过参数覆盖）：

```powershell
.\scripts\txn_chain_gtest_slice.ps1 -BuildDir E:\db\StructDB\build
```

| 维度 ID | 内容 | 建议 filter 子串 |
|---------|------|------------------|
| `txn_session` | REPL `session.txn` 恢复、`BEGIN`/`SHOW TXN` | `Mdb.Repl*` |
| `txn_read_view` | `kv_get` 与 `read_max_seq`、`SHOW SNAPSHOT` | `Mdb.EngineKvReadSeqVisibility`、`Mdb.TxnChain*` |
| `txn_embed` | 双会话、读快照 | `Mdb.TwoEmbedSessions*` |
| `txn_script` | 脚本内 `BEGIN`/`UPDATEWHERE` | `Mdb.WhereUpdateWhereTxn` |
| `txn_phase17` | `BEGIN` 内 `persist_table`、`mdb_persist_in_begin` 关断 | `Mdb.TxnBeginPersist*` |

**说明**：StructDB 当前 **不** 引入 newdb 的 `txn_chain_maturity_report.py`；若后续需要 CI 百分比门禁，可再增加薄包装脚本。

## 4. 七期：InnoDB 映射与耐久 / 存储（补充 filter）

| 维度 ID | 内容 | 建议 filter 子串 |
|---------|------|------------------|
| `txn_innodb_map` | 概念与文档一致性（本表为索引） | （无专用用例前缀；回归仍跑 §3 事务链集） |
| `txn_durability` | embed WAL fsync、WAL trim、`MultiKeyBatchFsync` | `EmbedClient.MultiKeyBatchFsyncSurvivesRestart`、`Engine.WalAutoTrimAfterFlushFromConfig`、`StorageEngine.WalTrim*` |
| `txn_undo_wal` | `undo.log`、版本化写、checkpoint、**八期截断** | `StorageEngine.VersionedUndo*`、`StorageEngine.EmbedWalBatch*`、`StorageEngine.RebuildUndoStack*`、`StorageEngine.UndoTruncate*`、`Engine.UndoAutoTruncateAfterFlushFromConfig`、`StorageEngine.CheckpointOpen*` |
| `txn_semantic_matrix` | MemTable Map/SkipList ×（flush 落盘 / 仅 WAL）矩阵；WAL 第二帧非法行失败；未带长度前缀的尾部字节按崩溃尾忽略；三层嵌套 embed 冷启 | `StorageEngine.MemTableBackendFlushVsWalOnly*`、`StorageEngine.WalReplayRejectsMissingEquals*`、`StorageEngine.WalReplayIgnoresUnframedTrailing*`、`EmbedClient.TripleNestedBatch*` |

可选第二条命令（与 [`scripts/txn_chain_gtest_slice.ps1`](../scripts/txn_chain_gtest_slice.ps1) 内注释一致）：

```powershell
.\build\tests\Release\structdb_tests.exe --gtest_filter="EmbedClient.MultiKeyBatchFsyncSurvivesRestart:Engine.WalAutoTrimAfterFlushFromConfig:Engine.UndoAutoTruncateAfterFlushFromConfig:StorageEngine.VersionedUndo*:StorageEngine.EmbedWalBatch*:StorageEngine.WalTrim*:StorageEngine.RebuildUndoStack*:StorageEngine.UndoTruncate*"
```

## 5. 八期 / 四期 4C：`undo.log` 截断

与 [`UNDO_LOG_4C.md`](UNDO_LOG_4C.md)、`POLICY` §3.3–3.5 对齐。建议 filter：

| 维度 ID | 内容 | 建议 filter 子串 |
|---------|------|------------------|
| `txn_undo_4c` | 栈空截断 API、`flush` 后自动截断 | `StorageEngine.UndoTruncate*`、`Engine.UndoAutoTruncateAfterFlushFromConfig` |

第三条命令（仅 4C 子集，便于快速跑）：

```powershell
.\build\tests\Release\structdb_tests.exe --gtest_filter="StorageEngine.UndoTruncate*:Engine.UndoAutoTruncateAfterFlushFromConfig"
```

## 6. 九期：L0 compaction 与 I/O 骨架

与 [`COMPACTION.md`](COMPACTION.md)、`POLICY` §3.3.1、`structdb::infra::io_backend.hpp` 对齐。

| 维度 ID | 内容 | 建议 filter 子串 |
|---------|------|------------------|
| `txn_compaction` | 最旧两 L0 SST 合并、重启可读 | `StorageEngine.Compaction*` |
| `txn_io_backend` | `IoBackendKind` 默认阻塞 | `Infra.IoBackend*` |
| `txn_checkpoint_undo` | checkpoint v2 与 `undo_log_safe_prefix_bytes` 水位、前缀截断 | `CheckpointState.UndoPrefix*`、`StorageEngine.Phase10*` |

第四条命令（compaction + 小测）：

```powershell
.\build\tests\Release\structdb_tests.exe --gtest_filter="StorageEngine.Compaction*:Infra.IoBackend*:CheckpointState.UndoPrefix*:StorageEngine.Phase10*:StorageEngine.Phase11*:StorageEngine.Phase12*:Manifest.Phase12*:Engine.L0AutoCompactAfterFlushFromConfig:Engine.L1CompactOutputFromConfig:StorageEngine.Phase23*:Manifest.Phase23*"
```

## 7. 十期：checkpoint v2 与 `undo.log` 前缀

与 [`PHASE10.md`](PHASE10.md)、[`CHECKPOINT_UNDO_PREFIX.md`](CHECKPOINT_UNDO_PREFIX.md)、`POLICY` §3.5 对齐。

| 维度 ID | 内容 | 建议 filter 子串 |
|---------|------|------------------|
| `txn_phase10` | v2 槽、`undo_log_safe_prefix_bytes` 持久化、`undo_try_truncate_recyclable_prefix` | `StorageEngine.Phase10*`、`CheckpointState.UndoPrefix*` |

第五条命令（十期子集）：

```powershell
.\build\tests\Release\structdb_tests.exe --gtest_filter="StorageEngine.Phase10*:CheckpointState.UndoPrefix*"
```

## 8. 十一期：L0 阈值自动 compaction

与 [`PHASE11.md`](PHASE11.md)、[`COMPACTION.md`](COMPACTION.md)、`POLICY` §3.3.1 对齐。

| 维度 ID | 内容 | 建议 filter 子串 |
|---------|------|------------------|
| `txn_phase11` | `flush_memtable` 后可选 L0 自动合并、`EngineConfigSnapshot` 阈值 | `StorageEngine.Phase11*`、`Engine.L0AutoCompactAfterFlushFromConfig` |

第六条命令（十一期子集）：

```powershell
.\build\tests\Release\structdb_tests.exe --gtest_filter="StorageEngine.Phase11*:Engine.L0AutoCompactAfterFlushFromConfig"
```

## 9. 十二期：`MANIFEST` L0/L1 与可选 L1 合并输出

与 [`PHASE12.md`](PHASE12.md)、[`COMPACTION.md`](COMPACTION.md) 对齐。

| 维度 ID | 内容 | 建议 filter 子串 |
|---------|------|------------------|
| `txn_phase12` | `FORMAT2`、读路径 L0/L1、`l1_compact_output_from_l0_merge` | `StorageEngine.Phase12*`、`Manifest.Phase12*`、`Engine.L1CompactOutputFromConfig` |

第七条命令（十二期子集）：

```powershell
.\build\tests\Release\structdb_tests.exe --gtest_filter="StorageEngine.Phase12*:Manifest.Phase12*:Engine.L1CompactOutputFromConfig"
```

## 10. 十七期：`BEGIN` 内 `persist_table`

与 [`PHASE17.md`](PHASE17.md)、`POLICY` §4.3、[`TXN_BEGIN_PERSIST_DESIGN.md`](TXN_BEGIN_PERSIST_DESIGN.md) 对齐。

| 维度 ID | 内容 | 建议 filter 子串 |
|---------|------|------------------|
| `txn_phase17` | 默认事务内 `persist_table`；引擎 / 脚本关闭路径 | `Mdb.TxnBeginPersist*` |

第八条命令（十七期子集）：

```powershell
.\build\tests\Release\structdb_tests.exe --gtest_filter="Mdb.TxnBeginPersist*"
```

## 11. 二十二期：L3 compaction、`undo` 分段与 GraphExecutor 背压探测

与 [`PHASE22.md`](PHASE22.md)、[`COMPACTION.md`](COMPACTION.md)、[`UNDO_LOG_4C.md`](UNDO_LOG_4C.md) 对齐。

| 维度 ID | 内容 | 建议 filter 子串 |
|---------|------|------------------|
| `txn_phase22` | L2→L3 manifest、undo `undo.segments` v2、Orchestrator 背压 | `StorageEngine.Phase22*`、`Manifest.Phase22*`、`Orchestrator.Phase22*` |

```powershell
.\build\tests\Release\structdb_tests.exe --gtest_filter="StorageEngine.Phase22*:Manifest.Phase22*:Orchestrator.Phase22*"
```

## 12. 二十三期：L0 内联上限、L3→L4、可选链式 `ROLLBACK`

与 [`PHASE23.md`](PHASE23.md)、[`COMPACTION.md`](COMPACTION.md)、`POLICY` §3.1 / §3.3.1 / §4.3、[`TXN_BEGIN_PERSIST_DESIGN.md`](TXN_BEGIN_PERSIST_DESIGN.md) 对齐。

| 维度 ID | 内容 | 建议 filter 子串 |
|---------|------|------------------|
| `txn_phase23` | `l0_compact_max_inline_rounds_per_flush`、`compact_merge_two_oldest_l3_to_l4`、manifest L4；MDB **`mdb_chain_rollback_on_mdb_rollback`** | `StorageEngine.Phase23*`、`Manifest.Phase23*`、`Mdb.TxnBeginPersistChainRollback*` |

```powershell
.\build\tests\Release\structdb_tests.exe --gtest_filter="StorageEngine.Phase23*:Manifest.Phase23*:Mdb.TxnBeginPersistChainRollback*"
```

## 13. 三十一期：事务链与存储边界矩阵（PHASE31）

与 [`PHASE31.md`](PHASE31.md)、[`POLICY.md`](../POLICY.md) §4 / §4.0.3、§6.1 对齐：恢复链顺序、flush/compact 与 `manifest_version`、24A 绕写观测、**损坏 `session.txn` 丢弃事务**（不静默崩溃）。

| 维度 ID | 内容 | 建议 filter 子串 |
|---------|------|------------------|
| `txn_phase31_matrix` | 31A–31E 索引与组合语义 | （以文档为主；无单独前缀） |
| `txn_phase31_recovery` | 引擎冷启 WAL、`EmbedClient` journal 崩溃重放、`session.txn` 损坏 | `Engine.Phase31Wal*`、`EmbedClient.Phase31*`、`Engine.Phase31CorruptSessionTxnDropsTxnLogOnRepl` |
| `txn_phase31_order` | flush/compact 后 checkpoint 与 MANIFEST 版本一致 | `StorageEngine.Phase31Flush*`、`StorageEngine.Phase31Compact*` |
| `txn_phase31_bypass` | 24A + 双 `EmbedClient` 已 open | `Engine.Phase31ObserveBypass*`、`Engine.Phase24*` |

第九条命令（**PHASE31 子集**，可与 §3 主命令拼接）：

```powershell
.\build\tests\Release\structdb_tests.exe --gtest_filter="StorageEngine.Phase31*:Engine.Phase31*:EmbedClient.Phase31*:Engine.Phase24*"
```

**`session.txn` 损坏路径**（`mdb_tests`，与 `Mdb.ReplSessionTxnV2CorruptLineDropsLog` 互补）：

```powershell
.\build\tests\Release\mdb_tests.exe --gtest_filter="Mdb.ReplSessionTxnV2CorruptLineDropsLog"
```

## 14. 三十六～三十七期：L1+ 两阶段 compaction 与 Facade 写队列（PHASE36 / PHASE37）

与 [`PHASE36.md`](PHASE36.md)、[`PHASE37.md`](PHASE37.md)、[`COMPACTION.md`](../COMPACTION.md) 对齐：并发 **`put`/`flush`** 与 **`compact_merge_two_oldest_l1_to_l2`** / **`l3_to_l4`** 等路径无死锁、`get` 与基线键一致；Facade **`kv_put_async_queue_depth`** 与 **`storage_pressure_snapshot`** 可观测字段。另含 **语义矩阵**（[`PHASE31.md`](PHASE31.md) 矩阵 F：checkpoint `manifest_version` 与 `manifest().version()`）与 **嵌套并发**（延后 L0 drain + L1→L2 双后台 + 主线程写）。

| 维度 ID | 内容 | 建议 filter 子串 |
|---------|------|------------------|
| `compact_phase36_l2_l3` | L2→L3 两阶段 + 并发写 | `StorageEngine.Phase36ConcurrentPutWhileL2ToL3Compact` |
| `compact_phase37_l1_l2` | L1→L2 两阶段 + 并发写 | `StorageEngine.Phase37ConcurrentPutWhileL1ToL2Compact` |
| `compact_phase37_l3_l4` | L3→L4 两阶段 + 并发写 | `StorageEngine.Phase37ConcurrentPutWhileL3ToL4Compact` |
| `facade_kv_put_queue` | 有界异步 `kv_put` 与压力快照 | `Engine.Phase36FacadeKvPut*` |
| `compact_semantic_matrix_f` | 矩阵 F：flush / L0 / L1→L2 / L2→L3 后 checkpoint 与 MANIFEST 版本一致 | `StorageEngine.CompactionConcurrencySemanticMatrix` |
| `compact_nested_l0_l1` | 嵌套并发：延后 L0 drain + L1→L2 + 交错写 | `StorageEngine.ConcurrentNestedL0DrainAndL1MergeWhilePuts` |

第十条命令（可与 §3 主命令或 §13 子集拼接）：

```powershell
.\build\tests\Release\structdb_tests.exe --gtest_filter=*Phase36*:*Phase37*:StorageEngine.CompactionConcurrencySemanticMatrix:StorageEngine.ConcurrentNestedL0DrainAndL1MergeWhilePuts:StorageEngine.*:Engine.*
```

**跨进程建议锁（C API，与 GUI `STRUCTDB_GUI_EXCLUSIVE_DIR_LOCK` 正交）**：

```powershell
.\build\tests\Release\capi_test.exe --gtest_filter=Capi.ExclusiveDirLockSecondOpenFails
```

## 15. 三十八期：`CONFIRM_REORDER` 与 `[REORDER_MAP_JSON]`（PHASE38）

与 [`PHASE38.md`](PHASE38.md) 对齐：MDB **`CONFIRM_REORDER({…})`** 成功后输出 **`[REORDER_MAP_JSON]`**；GUI 对 **多行** 映射逐层 push `id_remap_chain`。**`BEGIN` 内拒绝** 与 **两表脚本两行 JSON** 见 `mdb_tests` 用例名 **`Mdb.Phase38*`**。

```powershell
.\build\tests\Release\mdb_tests.exe --gtest_filter=*Phase38*
```

Rust GUI（`rust_gui` crate）对 **`ingest_reorder_map_from_engine_output`** 的多行行为见 **`phase38_remap_ingest_tests`** 单元测试（`cargo test -p` 随工程包名而定）。
