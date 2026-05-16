# 三十三期（PHASE33）：`StorageEngine` 编译单元再细分

## 目标与非目标

**目标**：在 **[PHASE32.md](PHASE32.md)** 已抽出 **`storage_engine_detail.*`** 与粗粒度 compact/checkpoint TU 的基础上，将仍偏大的 **`storage_engine.cpp`** 与 **`storage_engine_compact_checkpoint.cpp`** 再按职责切成多个 `.cpp`，缩短单文件改动面、改善增量编译；**不改变** WAL / MANIFEST / checkpoint 写序、`commit_seq`、undo 栈与读路径语义。

**非目标**：磁盘格式与默认行为变更；新建独立 CMake 子库（仍仅向 `src/engine/storage/CMakeLists.txt` 追加源文件）；移动 [`storage_engine.hpp`](../../src/engine/storage/include/structdb/storage/storage_engine.hpp) 中的类定义。

**依赖纪律**（与 [`POLICY.md`](../POLICY.md) §2.2 一致）：`engine/storage` 不反向依赖 orchestrator。

---

## 子条 33A–33E（`structdb_storage` 源文件映射）

| 子条 | 文件 | 内容摘要 |
|------|------|----------|
| **33A** | `storage_engine_open_wal.cpp` | `StorageEngine` 构造函数；`open` / `close`（**WAL 重放解码** 在 **`wal_replay_applier.cpp`** / **`WalReplayApplier`**，由 **`recovery_coordinator.cpp`** 调度） |
| **33B** | `storage_engine_put_undo.cpp` | 版本化 undo 查找与追加、`logical_to_stored_for_put_unlocked_`、`rebuild_undo_stack_from_undo_log_unlocked_`、`commit_embed_batch*`、`put_impl` / `put`、`wal_sync`、WAL/undo 字节统计、undo 前缀与 checkpoint 辅助、`wal_try_trim*`、`wal_gc_sealed_archives_unlocked_`、`rollback_*`、`remove`；写路径专用 `kAppendRedoMirrorWal`（匿名命名空间） |
| **33C** | `storage_engine_read.cpp` | `decode_get_visible_`、`get`、`visit_prefix` |
| **33D** | `storage_engine_compaction_lsm.cpp` | `flush_memtable`；L0/L1/L2/L3 合并 `*_unlocked_` 与公开包装、`try_compact_l0_if_over_threshold_unlocked_`、`drain_pending_l0_compactions`、`read_storage_pressure_snapshot` |
| **33E** | `storage_engine_segments_worker_checkpoint.cpp` | `persist_wal_segments_for_flush_unlocked_` / `persist_undo_segments_for_flush_unlocked_`、`undo_logical_stream_total_bytes_unlocked_`、`undo_consume_logical_prefix_unlocked_`、undo/WAL 分段 roll、`start_compaction_worker` / `stop_compaction_worker` / `compaction_worker_loop_` / `enqueue_drain_l0_compaction_and_wait`、`checkpoint` |

**保留在** **`storage_engine.cpp`**：`versioned_read_seq_latest`、`load_commit_seq_hw_` / `persist_commit_seq_hw_`、`observe_stored_commit_seq_`、`reserve_commit_seq`。

**三十二期** 的 **`storage_engine_compact_checkpoint.cpp`** 在本期 **删除**，由 **33D** + **33E** 取代。

---

## 与三十一期不变式

触及 flush、compact、checkpoint、WAL trim、undo 前缀或分段时，除常规 `StorageEngine.*` / `Engine.*` 外，须额外跑 **[PHASE31.md](PHASE31.md)** 推荐的 **`--gtest_filter=*Phase31*`**（或该文 / `TESTING_TXN_CHAIN` §13 中的显式前缀一行；勿单独使用裸 `Phase31*`，易匹配 0 条）。

---

## 验收命令

```powershell
cmake --build <build_dir> --target structdb_tests
.\build\tests\Release\structdb_tests.exe --gtest_filter="StorageEngine.*:Engine.*"
.\build\tests\Release\structdb_tests.exe --gtest_filter="*Phase31*"
```

（非 MSVC 生成树请将 `structdb_tests` 可执行路径替换为本地 `cmake --build` 输出目录。）

---

## 修订记录

| 日期 | 摘要 |
|------|------|
| 2026-05-14 | 初稿：33A–33E 映射、验收与 PHASE31 交叉引用 |
| 2026-05-14 | GTest 文案与 **[PHASE34.md](PHASE34.md)**：`*Phase31*`；后续维护以 PHASE34 权威 TU 表为准 |
