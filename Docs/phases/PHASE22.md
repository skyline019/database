# 二十二期：独立大项（L2+ 主干 + 背压全链路 + undo 分段 + 十七期 persist）

本文档将二十二期定位为相对二十/二十一的**独立大版本**许可范围；实现约束仍须遵守 [`POLICY.md`](POLICY.md) §2.2、§3.3、WAL 崩溃恢复权威、§4.2 单写者与 compaction 锁序。

## 1. 目标

| 子阶段 | 内容 | 默认交付 |
|--------|------|----------|
| **22A** | 十五期主干 **L2+**（FORMAT2 行级 `level`、读序、L2→L3 compaction 路径） | **是**（本仓库默认选 **22A1 L2+**；**size-tiered** 为显式非目标，顺延 22.x 或产品重开） |
| **22B** | 十四期背压 **Orchestrator → Scheduler → GraphExecutor** 全链路可观测触发 | **是** |
| **22C** | 十六期 **16B**：`undo.log` 物理分段 + `undo.segments` v2 与 `UNDO_LOG_4C` / `POLICY` 矩阵 | **是**（与 22A1 不同 PR/合并窗口时仍建议分开发布，见 [`PHASE13_PLUS_PLAN.md`](PHASE13_PLUS_PLAN.md) §2） |
| **22D** | 十七期：`BEGIN` 内 `persist_table` | **是**（默认 **`mdb_persist_in_begin`**；见 [`PHASE17.md`](PHASE17.md)） |

## 2. 非目标

- **size-tiered** 多路挑选作为二十二期的**默认**主干（与 22A1 二选一中的另一支；若启用须另开 `PHASE22` 修订与 `CHANGELOG`）。
- 与 **FORMAT2 / 层级读序大改** 同 merge 窗口硬绑 **大规模 undo 格式迁移**（人力并行时分支或双 release）。
- **`BEGIN` 内 `persist_table` 的跨层 `ROLLBACK`（与 `undo_stack_` 链式对齐）** 默认 **非**二十二期的默认可交付语义；**二十三 23C** 以 **`mdb_chain_rollback_on_mdb_rollback`**（默认 **false**）提供受限实现（见 [`PHASE23.md`](PHASE23.md)、`POLICY` §4.3）。

## 3. 验收

- `structdb_tests`：**22A** L3 manifest 与 `compact_merge_two_oldest_l2_to_l3`、读序；**L3 compaction 崩溃恢复 smoke**（合并后重启可读）。
- **22B**：`GraphExecutor` 预算探测覆盖 **WAL 队列 / CompactionSlots / MemTable**；`Orchestrator` 背压回调在 **WalBacklogged** 与 **CompactionBusy** 场景可触发。
- **22C**：`undo_segment_roll_max_bytes` 下多段 `undo/archive/*.log` + `undo.segments` v2；前缀回收与 `frame_start` 逻辑字节一致。
- **22D**：**`mdb_persist_in_begin`** 默认实现与 `Mdb.TxnBeginPersist*`、`PHASE17.md` / `POLICY` §4.3 同步。

## 4. 配置摘要（代码锚点）

| 配置 / API | 说明 |
|------------|------|
| `EngineConfigSnapshot::l3_compact_output_from_l2_merge` | 允许 `compact_merge_two_oldest_l2_to_l3`（须已启用 L2→L1 链上的 L2 能力)。 |
| `EngineConfigSnapshot::undo_segment_roll_max_bytes` | `>0` 且于 `open` 前生效：`undo.log` 超阈滚入 `undo/archive/{seq}.log`，写 `undo.segments` v2。 |

## 5. 与二十～二十一期关系

- **二十一期**：WAL 封存 GC、io_uring、`ResourceBudget` / `CompactionSlots` delta 已合入。
- **二十二期**：补齐路线图曾标为「未实现」的 **L3 compaction**、**GraphExecutor 多资源背压探测**、**undo 物理分段**；**十七期** `mdb_persist_in_begin` 与 `Mdb.TxnBeginPersist*` 已默认合入。**二十三 23C** 起跨层 `ROLLBACK` 以独立门闩交付（见 [`PHASE23.md`](PHASE23.md)）。

## 6. 修订记录

| 日期 | 摘要 |
|------|------|
| 2026-05-13 | 初稿：22A 默认 L2+、22B–22D 边界与验收 |
