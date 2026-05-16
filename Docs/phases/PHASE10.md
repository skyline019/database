# 十期：Checkpoint v2 与 `undo.log` 前缀回收（9C 落地）

本文描述 **十期** 已实现内容：在 **不改变「WAL 为崩溃恢复权威」** 的前提下，将九期 9C 草案字段 **`CheckpointState::undo_log_safe_prefix_bytes`** 持久化到 **二进制 checkpoint 槽格式 v2**，并提供 **`undo.log` 物理前缀截断** API 与保守水位计算。

## 1. Checkpoint 二进制槽 v2

| 项目 | 说明 |
|------|------|
| 魔数 | 与 v1 相同：`STCK`（4 字节） |
| 格式版本 | `2`（`u32` LE，偏移 4） |
| 载荷 | `checkpoint_seq`、`written_unix_ns`、`wal_offset`、`redo_offset`、`manifest_version`、`mdb_catalog_epoch`（各 `u64` LE，布局与 v1 一致至偏移 55） |
| **新增** | **`undo_log_safe_prefix_bytes`**（`u64` LE，偏移 56–63） |
| CRC32C | 覆盖字节 **`[0, 64)`**，存于偏移 **64–67**；槽总长 **68** 字节 |
| 兼容读 | `read_latest` 仍识别 **v1（60 字节）** 槽；v1 解码时 **`undo_log_safe_prefix_bytes` 置 0** |
| 新写 | `write_rotating` 始终写入 **v2** 槽 |

遗留文本 `checkpoint` 行仍为四字段，不含 undo 水位；读入时 **`undo_log_safe_prefix_bytes = 0`**。

## 2. `undo_log_safe_prefix_bytes` 语义（运行时）

在 **持有 `StorageEngine` 写互斥** 的上下文中：

- **`undo_stack_` 为空**：水位 = 当前 `undo.log` 文件大小（整文件对进程内回滚而言均为可回收前缀；与八期「栈空整文件截断」语义一致，但十期通过前缀 API 或水位持久化分步完成）。
- **`undo_stack_` 非空**：水位 = **`undo_stack_.front().frame_start_byte`**（仍挂栈的 **最旧** 一帧在文件中的起始字节偏移；该偏移之前的字节不再被任何栈条目引用，可物理截断）。

每次 **`flush_memtable`**、**`compact_merge_two_oldest_l0`**、**`checkpoint()`** 以及 **`wal_try_trim_prefix_through_checkpoint`** 在写旋转 checkpoint 前，用上述规则 **重算** 并写入 `CheckpointState::undo_log_safe_prefix_bytes`（并 **夹紧** 到不超过当前 `undo.log` 文件大小）。

## 3. 公开 API

| API | 行为 |
|-----|------|
| `StorageEngine::undo_try_truncate_recyclable_prefix` | 按当前保守规则计算 `P`，若 `P>0` 则物理删除 `undo.log` 的 `[0, P)`，并将栈内条目的 **`frame_start_byte` 减 `P`**。`P==0` 为 no-op。 |
| `StorageEngine::undo_try_truncate_when_stack_empty`（八期） | 不变：仅栈空时整文件截断；当栈空且 `P` 等于文件大小时，与十期前缀截断效果等价。 |

**与 `kOpenFlagRebuildUndoStackFromLog`：** 若依赖从 `undo.log` 重建栈，调用方须保证 **截断前缀与 WAL 重放后状态一致**（与 `POLICY` §3.5、`UNDO_LOG_4C` 一致）；十期 **不在 `open` 中** 根据 checkpoint 水位自动截断 `undo.log`。

## 4. 单测入口

- `StorageEngine.Phase10*`（见 `tests/structdb_tests.cpp`）
- 事务链文档：[TESTING_TXN_CHAIN.md](TESTING_TXN_CHAIN.md) §「十期」

## 5. 相关文档

- [CHECKPOINT_UNDO_PREFIX.md](CHECKPOINT_UNDO_PREFIX.md)（已更新为 v2 已落地）
- [POLICY.md](POLICY.md) §3.5
- [COMPACTION.md](COMPACTION.md)（compaction 写 checkpoint 时刷新 undo 水位）
- [CHANGELOG.md](CHANGELOG.md)
