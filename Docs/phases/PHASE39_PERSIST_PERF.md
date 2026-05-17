# 三十九期：Persist 性能（PHASE39）

## 开关矩阵（`EngineConfigSnapshot`，默认均为 off / 兼容）

| 字段 | 默认 | 作用 |
|------|------|------|
| `mdb_wire_encoding` | `Hex` | `Wire2` = `mdbwire2:` 长度前缀二进制 |
| `mdb_persist_coalesce` | false | 延迟 `persist_table` 至 `FLUSH PERSIST` / `COMMIT` / 脚本结束 |
| `mdb_persist_coalesce_max_dirty_rows` | 0 | coalesce 时脏行达阈值自动刷盘 |
| `mdb_bulk_import_mode` | false | `BULKINSERTFAST` 不逐行落盘；跳过写入期 `kSecIdx` |
| `embed_journal_skip_until_commit` | false | 落盘写 WAL 但不写 `session.journal` 直至允许 |
| `storage_batch_undo_lookup` | true | `commit_embed_batch` 每键一次 undo 查找 |

MDB 表面：`IMPORT MODE ON|OFF`、`FLUSH PERSIST`、`REBUILD INDEX`。

## 耐久（opt-in）

- **coalesce / import**：未 `FLUSH PERSIST` / `COMMIT` 前崩溃可能丢失已执行 DML（与未提交事务类似）；**WAL 仍为引擎权威**。
- **journal 跳过**：会话恢复依赖 WAL + `session.journal` 中已写行；跳过期间仅 WAL 承载已提交批次。

## 回归

`ctest -R "Mdb.Phase39"`；性能：`scripts/run_persist_baseline.ps1`、`-ImportMode` 的 `mega_data_mdb_stress.ps1`。

## 数量级优化（2026-05-16 续）

| 问题 | 对策 |
|------|------|
| 脚本内每条 `BULKINSERTFAST` 触发一次 `persist_table`（~1000 次/百万行） | 默认 `mdb_script_amortize_bulk_dml`：脚本内仅 mark，**EOF 一次落盘** |
| 脏行 >8192 时增量 persist 分块重复写 `row_index` | 改为 **单次全量 snapshot batch** |
| 百万键 `commit_embed_batch` 对 SST 做 undo 查找 | `storage_batch_undo_mem_only`；大批量/导入 **`storage_import_batch_skip_undo`** |

**本机 1M 行实测（`RowsPerLine=1000`）**：默认脚本 **~7.5K TPS**（改造前 ~4.7K）；`--mdb-bulk-import` **~12K TPS**（改造前 IMPORT 路径 ~6.6K）。关闭脚本摊销：`MdbRunOptions::amortize_bulk_dml_in_script=false` + `mdb_script_amortize_bulk_dml=false`。

**四十期续作**（分块 WAL、plain/raw、延迟行索引）：同场景约 **~238K–328K TPS**，见 [`PHASE40_PERSIST_PERF.md`](PHASE40_PERSIST_PERF.md)。
