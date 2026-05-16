# 十一期：L0 compaction 阈值自动调度

本文描述 **十一期** 已实现内容：在 **不改变九期** `compact_merge_two_oldest_l0` 合并语义与 **WAL 崩溃恢复权威** 的前提下，为 **`flush_memtable` 成功路径** 增加 **可选** 的 L0 SST 数量阈值触发 —— 在单次 flush 末尾同步执行至多 **`l0_compact_max_rounds_per_flush`** 轮「最旧两文件合并」，直到 `MANIFEST` 中 SST 数量 **≤ 阈值** 或达到轮数上限。

## 1. 配置（`EngineConfigSnapshot`）

| 字段 | 类型 | 默认 | 说明 |
|------|------|------|------|
| `l0_compact_trigger_threshold` | `uint32_t` | **0** | **`0` = 关闭** 自动 L0 compaction。若 **> 0**，当 **MANIFEST 中 L0 段长度**（leading `level==0` 条目数，见 [`PHASE12.md`](PHASE12.md)）**>** 该阈值时，在 `flush_memtable` 成功写 checkpoint（及可选 WAL/undo 钩子）之后尝试合并。 |
| `l0_compact_max_rounds_per_flush` | `uint32_t` | **4** | 单次 `flush_memtable` 内最多调用 `compact_merge_two_oldest_l0` 的次数；传入 **0** 时在引擎内按 **4** 处理。用于限制 flush 延迟。 |
| `l0_compact_max_inline_rounds_per_flush` | `uint32_t` | **0** | **二十三 23A**：当 **> 0** 且 **`l0_compact_defer_after_flush` 为 false** 时，与上条取 **最小值** 作为 flush 内同步合并轮数上限；**0** 表示不额外收紧。见 [`PHASE23.md`](PHASE23.md)。 |

`Engine::startup` 将上述字段注入 `StorageEngine::set_l0_compact_trigger_threshold` / `set_l0_compact_max_rounds_per_flush` / **`set_l0_compact_max_inline_rounds_per_flush`**（与 `wal_auto_trim_prefix_after_flush` 注入方式一致）。

## 2. 行为与顺序

1. `flush_memtable`：MemTable → SST → `MANIFEST` → `mem_.clear` / `undo_stack_.clear` → **checkpoint**（含十期 undo 水位）→ 可选 **WAL trim** → 可选 **`undo.log` 整文件截断** → **十一期 try_compact**（受 **`l0_compact_max_inline_rounds_per_flush`** 与阈值约束，见 [`PHASE23.md`](PHASE23.md)）→ `persist_commit_seq_hw_`。
2. **try_compact** 在 **同一把 `StorageEngine` 写互斥** 下调用内部 **`compact_merge_two_oldest_l0_unlocked_`**，避免与公开 `compact_merge_two_oldest_l0` 二次加锁死锁。
3. **显式** `compact_merge_two_oldest_l0()` 仍对外可用；与自动路径共享同一合并实现。

## 3. 非目标（十二期或独立项）

- **L1+**、size-tiered、多路并发 compaction、与 **Scheduler/Orchestrator 背压** 全链路联动 —— 见 [`COMPACTION.md`](COMPACTION.md) 未实现列表。**十二期**已落地 **MANIFEST 中 L0/L1 与可选 L1 合并输出**（[`PHASE12.md`](PHASE12.md)）；**L2+** 等仍不在此列。
- **`checkpoint()` 单独路径** 十一期 **不** 自动触发 try_compact（仅 `flush_memtable`，与实现一致、减少重复抖动）。

## 4. 单测

- `StorageEngine.Phase11*`、`StorageEngine.Phase23L0InlineCapLimitsRoundsPerFlush`、`Engine.L0AutoCompactAfterFlushFromConfig`（见 `tests/structdb_tests.cpp`）。
- 事务链文档：[TESTING_TXN_CHAIN.md](TESTING_TXN_CHAIN.md) §8。

## 5. 相关文档

- [`COMPACTION.md`](COMPACTION.md)、[`POLICY.md`](POLICY.md) §3.3.1、[`CHANGELOG.md`](CHANGELOG.md)。
