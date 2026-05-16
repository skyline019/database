# 二十三期：机制收口与跨层回滚

在 **二十二期**（[`PHASE22.md`](PHASE22.md)）主干已合入的前提下，本期收口路线图未闭合项：**十三期 flush 尾延迟**、**L4+ compaction**、**MDB `ROLLBACK` 与 `undo_stack_` 链式对齐（可选）**、**I/O 文档矩阵** 与路线图一致性。

## 1. 子阶段与交付

| 子阶段 | 内容 | 默认 / 门闩 |
|--------|------|-------------|
| **23A** | `EngineConfigSnapshot::l0_compact_max_inline_rounds_per_flush`：在 **非 defer** 的 Phase 11 自动 L0 合并路径上，将单次 `flush_memtable` 内同步合并轮数 **上限** 为 `min(l0_compact_max_rounds_per_flush, …)`；`0` 表示不额外收紧。与既有 **`l0_compact_defer_after_flush`** + worker（十三 / 二十期）**互补**，见 [`COMPACTION.md`](COMPACTION.md)。 | 默认 **0** |
| **23B** | **`l4_compact_output_from_l3_merge`** + `compact_merge_two_oldest_l3_to_l4`：`FORMAT2` **level 4** SST、读序随 `manifest_sst_paths_lookup_order` 延伸。 | 默认 **false** |
| **23C** | **`mdb_chain_rollback_on_mdb_rollback`**（默认 **false**）：为 **true** 且 **`mdb_persist_in_begin`** 为 **true** 时，MDB `ROLLBACK` 在恢复会话表 **之前** 调用 `Engine::rollback_embed_undo_until`，将 `undo_stack_` 弹回 **`BEGIN` 成功时刻** 记录的深度（`Engine::embed_undo_stack_depth()`）。 | 默认 **false** |
| **23D** | WAL `WalWriter` 与 **IOCP / io_uring** 的 **构建矩阵与默认** 在 `POLICY` / `CHANGELOG` / [`io_backend.hpp`](../src/engine/infra/include/structdb/infra/io_backend.hpp) 注释对齐；代码路径以既有 **二十期 20C / 二十一期 21B** 为准。 | 文档为主 |
| **23E** | `README`、`PHASE13_PLUS_PLAN`、`TESTING_TXN_CHAIN` §、`CHANGELOG` 索引；修正 **二十二期** 文档漂移。 | — |

## 2. 非目标

- **多线程并发写** 同一 `StorageEngine`（仍须 `POLICY` §4.2 单写者）。
- **size-tiered** 多路挑选（仍列为后续；本期选 **L4+** 作为 23B 主干）。
- **崩溃恢复后会话 `session.txn` 重放** 路径上 **自动** 恢复 `BEGIN` 时的 undo 水位：`txn_undo_stack_depth_at_begin` **不**从磁盘恢复，重放后 **重置为未设置**，链式存储回滚在恢复会话内 **不生效**（须新 `BEGIN` 再采集水位）。

## 3. 23C 受限语义（须 `POLICY` §4.3 一致）

1. **水位**：`undo_stack_.size()` 在 **`BEGIN` 提交 `session.txn` 成功后** 读取；每次 `persist_table` / 版本化写按现有规则 push。**首次对某 `mdb$` 键的可见写** 在栈/日志中记录 **上一态为墓碑**（与 `StorageEngine::append_versioned_undo_if_needed_unlocked_` 一致），否则单步回滚无法撤销「纯 INSERT」。
2. **`ROLLBACK`**：先 **`rollback_embed_undo_until(*depth)`**（循环 `rollback_one_undo_frame`），再恢复 `current` / 删 `session.txn`。
3. **并发**：`BEGIN` 激活期间若存在 **绕过 MDB** 的同一引擎版本化写，水位与栈 **可能不一致** — **未定义**（与 [`TXN_BEGIN_PERSIST_DESIGN.md`](TXN_BEGIN_PERSIST_DESIGN.md) 草案一致）。
4. **`ROLLBACK TO SAVEPOINT`**：**不**调整存储 undo 链（与 Phase 7 savepoint 语义一致；仅会话层）。

## 4. 验收

- `structdb_tests`：**`StorageEngine.Phase23L0InlineCapLimitsRoundsPerFlush`**、**`StorageEngine.Phase23L3ToL4CompactAndRestart`**、**`Manifest.Phase23Format2LoadsLevel4`**。
- `mdb_tests`：**`Mdb.TxnBeginPersistChainRollbackPopsStorageWhenEnabled`**（门闩开：链式 `ROLLBACK` 后 `kv_get` 无残留行；默认关路径见 **`Mdb.TxnBeginPersistStorageSurvivesRollback`**）。
- `POLICY` §2.4 / §3.1 / §3.5 / §4.3、`TXN_BEGIN_PERSIST_DESIGN.md`、`CHANGELOG` 计划条目。

## 5. 修订记录

| 日期 | 摘要 |
|------|------|
| 2026-05-13 | 初稿：23A–23E 边界与 23C 受限模型 |
