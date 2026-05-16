# 三十四期（PHASE34）：拆分固化收尾

## 目标与非目标

**目标**：在 **[PHASE33.md](PHASE33.md)** 已落地的 `StorageEngine` 多 TU 拆分之上，完成**文档与回归说明固化**（权威源文件映射、历史期文锚点修正、`--gtest_filter` 可复制文案统一）；并将 MDB **`mdb_ops_storage_and_tools.cpp`** 再拆为 **`mdb_ops_persist_load.cpp`** 与 **`mdb_ops_pages_journal_import.cpp`**（与三十二期声明面不变）。

**非目标**：不重排 WAL / checkpoint / MANIFEST 写序，不改 `commit_seq` / undo / 读路径语义；不移动 [`storage_engine.hpp`](../../src/engine/storage/include/structdb/storage/storage_engine.hpp) 中的类定义。

**依赖纪律**（与 [`POLICY.md`](../POLICY.md) §2.2 一致）：`engine/storage` 不反向依赖 orchestrator。

---

## 权威：`structdb_storage` 中 `StorageEngine` 实现文件映射

以下路径均相对于 `src/engine/storage/src/`。类定义仍在 `include/structdb/storage/storage_engine.hpp`。

| 源文件 | 职责摘要 |
|--------|----------|
| `storage_engine_detail.cpp`（+ `storage_engine_detail.hpp`） | SST/段元数据、`manifest_sst_paths_lookup_order` 等 `namespace storage_engine_detail` 辅助 |
| `storage_engine.cpp` | `versioned_read_seq_latest`、`COMMIT_SEQ` 读写、`observe_stored_commit_seq_`、`reserve_commit_seq` |
| `storage_engine_open_wal.cpp` | 构造函数、`open` / `close`；WAL 重放解码在 **`wal_replay_applier.cpp`**，调度在 **`recovery_coordinator.cpp`** |
| `storage_engine_put_undo.cpp` | 写路径、`commit_embed_batch*`、`put`、`undo` 栈与前缀辅助、`wal_try_trim*`、`wal_gc_sealed_archives_unlocked_`、`rollback_*`、`remove` |
| `storage_engine_read.cpp` | `decode_get_visible_`、`get`、`visit_prefix` |
| `storage_engine_compaction_lsm.cpp` | `flush_memtable`、各层 compact、`try_compact_l0_if_over_threshold_unlocked_`、`drain_pending_l0_compactions`、`read_storage_pressure_snapshot` |
| `storage_engine_segments_worker_checkpoint.cpp` | 段元数据 flush 持久化、undo/WAL roll、`compaction_worker`、`enqueue_drain_l0_compaction_and_wait`、`checkpoint` |

历史名 **`storage_engine_compact_checkpoint.cpp`**（三十二期单文件）已由 **三十三期** 的 `compaction_lsm` + `segments_worker_checkpoint` 取代。

---

## MDB：`structdb_client_mdb`（三十四期再切）

| 源文件 | 职责摘要 |
|--------|----------|
| `mdb_ops_persist_load.cpp` | `load_table_from_storage`、`persist_table`、`table_exists_in_storage`、`gather_drop_table_keys`、`rename_table_storage` |
| `mdb_ops_pages_journal_import.cpp` | `log_line`、`split_csv_paren_content`、`extract_paren_block`、`parse_defattrs`、`compare_ids`、`handle_page`、`handle_page_json`、`import_mdb_directory`、`append_embed_journal_tail` |

---

## GTest：`Phase31` 子集（可复制）

GoogleTest 全名形如 `StorageEngine.Phase31FlushCheckpointManifestVersionMatchesManifest`，**不要**单独使用 `Phase31*`（常匹配 0 条）。请使用其一：

- **通配**：`--gtest_filter=*Phase31*`
- **显式**（与 [TESTING_TXN_CHAIN.md](TESTING_TXN_CHAIN.md) §13、[PHASE31.md](PHASE31.md) 推荐行一致）：  
  `StorageEngine.Phase31*:Engine.Phase31*:EmbedClient.Phase31*:Engine.Phase24*`

---

## 验收命令

```powershell
cmake --build <build_dir> --target structdb_tests
.\build\tests\Release\structdb_tests.exe --gtest_filter="StorageEngine.*:Engine.*"
.\build\tests\Release\structdb_tests.exe --gtest_filter="*Phase31*"
```

---

## 修订记录

| 日期 | 摘要 |
|------|------|
| 2026-05-14 | 初稿：TU 权威表、GTest 说明、与 PHASE33 交叉引用 |
| 2026-05-14 | MDB：`mdb_ops_persist_load` / `mdb_ops_pages_journal_import`（原 `mdb_ops_storage_and_tools`） |
