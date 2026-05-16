# Checkpoint 与 `undo.log` 前缀回收（九期 9C → 十期落地）

本文与 [`UNDO_LOG_4C.md`](UNDO_LOG_4C.md) §3.3、`POLICY` §3.5、[`PHASE10.md`](PHASE10.md) 配套，描述 **`CheckpointState::undo_log_safe_prefix_bytes`** 的语义与磁盘布局。

## 1. 意图

- 在 **不**改变 **WAL 为崩溃恢复权威** 的前提下，为「**已知安全** 的 `undo.log` 前缀字节数」提供 **可持久化** 水位，使运维与代码可在单点对齐 **WAL `wal_offset`、MANIFEST 版本、undo 帧水位**。
- **十期起**：`CheckpointWriter::write_rotating` 将水位写入 **二进制槽格式 v2**（68 字节 `STCK`，见 [`PHASE10.md`](PHASE10.md) §1）。`read_latest` 可读 **v1（60 字节）** 与 **v2**；读 v1 槽时该字段 **视为 0**。遗留文本 `checkpoint` 行仍不含该字段（读入为 0）。
- 与 **八期** 整文件截断（`undo_try_truncate_when_stack_empty`）及 **十期** 前缀截断（`undo_try_truncate_recyclable_prefix`）**独立**；调用方须遵守 `POLICY` §3.5 安全窗口。

## 2. 持久化约束（已实现路径）

- 写入前将水位 **夹紧** 到 **≤ 当前 `undo.log` 文件大小**；与 **`kOpenFlagRebuildUndoStackFromLog`** 的交互仍以 **`open` 不自动按水位截断** 为原则（见 `PHASE10` §3）。
- **`compact_merge_two_oldest_l0`** 与 **`flush_memtable`**、**`checkpoint()`**、**`wal_try_trim_prefix_through_checkpoint`** 在写旋转 checkpoint 时 **重算并写入** `undo_log_safe_prefix_bytes`（与 [`COMPACTION.md`](COMPACTION.md) §4 一致）。

## 3. 与九期 compaction 的关系

- `compact_merge_two_oldest_l0` 完成后写 checkpoint 时 **刷新** undo 水位（与 `flush_memtable` 使用同一 `fill_checkpoint_undo_safe_prefix` 规则）。**分段 `undo.log` 轮转** 仍为 4C 后续，不在十期默认范围。
