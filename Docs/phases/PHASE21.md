# 二十一期：缺陷偿还与演进（相对二十期）

本文档描述 **二十一期** 相对 [`PHASE20.md`](PHASE20.md) 的增量：文档与 WAL/undo 叙述对齐、**可选** `wal/archive/` 封存回收、Linux **io_uring** WAL 路径与 **WalPipeline** 显式顺序、compaction 压力与 **ResourceBudget** 的深化联动。

## 1. 目标

| 子阶段 | 摘要 |
|--------|------|
| **21A** | `wal.segments` v2 下可选 **`wal/archive/` GC**（默认关闭）：仅在 **`flush_memtable`** 成功写 checkpoint 且 **`wal_auto_trim_prefix_after_flush`** 已执行 **`wal_try_trim_prefix_through_checkpoint`** 之后，清空封存列表并删除对应文件；验收含 **多段 + 尾 `wal.log` 截断** 重放。 |
| **21B** | CMake **`STRUCTDB_WITH_IO_URING=ON`**（Linux）+ **`pkg-config liburing`**：**`IouringSequentialFileWriter`**、**`WalWriter`** `IoUringAsync`；**`WalPipeline`** 文档（`wal.hpp` / `io_backend.hpp`）；**`StorageEngine.WalIoUringBackendRoundTrip`**（`STRUCTDB_HAVE_IO_URING` 时）。 |
| **21C** | **`pending_deferred_l0_compact`** / compaction worker 队列深度 → `StoragePressureSnapshot`；**`ResourceBudget::set_compaction_slots_pressure_delta`**；`Engine::sync_scheduler_budget_from_storage_pressure`；可选 **`drain_l0_compaction_queue(..., worker_wait_ms)`** / **`enqueue_drain_l0_compaction_and_wait(..., wait_ms)`**；`COMPACTION.md` / `POLICY` §4.2 索引。 |

## 2. 非目标

- **十七期**（`BEGIN` 内 `persist_table`）：默认路径见 [`PHASE17.md`](PHASE17.md)；本期待办不重复实现。
- **`undo.log` 物理分段（16B）**：与 checkpoint v2 水位强相关，独立里程碑；本期的 GC **不**触及 `undo.log` 分段格式。

## 3. WAL 封存 GC（21A）安全不变式

1. **默认关闭**：`EngineConfigSnapshot::wal_archive_gc_after_flush` 与 `StorageEngine::set_wal_archive_gc_after_flush` 默认为 **false**；未开启时行为与二十期一致。
2. **依赖 WAL 前缀裁剪**：`wal_archive_gc_after_flush == true` 时 **必须**同时启用 **`wal_auto_trim_prefix_after_flush`**；否则 **`flush_memtable` 在持锁工作开始前即失败**（避免在已写 MANIFEST/checkpoint 后才报错的不一致窗口）。
3. **调用顺序**（单次 `flush_memtable`）：先 MANIFEST → mem clear → checkpoint（含 `wal_offset =` 当前尾 `wal.log` 大小）→ 持久化 `wal.segments` → **`wal_try_trim`**（折叠尾段前缀，`wal_offset` 置 0）→ **再** `wal_gc_sealed_archives_unlocked_`：先 **重写 `wal.segments` v2**（封存路径列表为空，`next_roll_seq` 不变），**再** `std::filesystem::remove` 各封存文件；任一步失败则尽力恢复内存态与目录元数据一致性。
4. **语义**：GC 仅在 **已成功 flush** 且 **trim 已证明** 尾段前缀可由 SST + 新 checkpoint 覆盖的前提下，删除 **仅由 v2 目录枚举的** 封存文件；与二十期 [`PHASE20.md`](PHASE20.md) §4「MVP 不自动删 archive」相比，本期为 **显式 opt-in** 的运维回收路径。

## 4. 相关锚点

- `src/engine/storage/src/storage_engine_compaction_lsm.cpp` — `flush_memtable`、`read_storage_pressure_snapshot`。
- `src/engine/storage/src/storage_engine_put_undo.cpp` — `wal_gc_sealed_archives_unlocked_`（与 flush 内 `wal_try_trim` 调用链配合）。
- `src/engine/storage/src/storage_engine_segments_worker_checkpoint.cpp` — `enqueue_drain_l0_compaction_and_wait`、compaction worker 循环。
- `src/engine/facade/include/structdb/facade/config.hpp` — `wal_archive_gc_after_flush`。
- `src/engine/storage/src/wal.cpp` — `WalWriter`、io_uring 路径（21B）。
- `src/engine/facade/src/engine.cpp` — `sync_scheduler_budget_from_storage_pressure`（21C）。
- `src/engine/storage/src/wal.cpp` — `WalWriter`、io_uring 路径（21B）。
- `src/engine/facade/src/engine.cpp` — `sync_scheduler_budget_from_storage_pressure`（21C）。
