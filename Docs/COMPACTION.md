# LSM Compaction（九期 MVP）

本文描述 StructDB **九期** 已实现的最小 **L0 SST 合并** 路径，与 [`POLICY.md`](POLICY.md) §3、`MANIFEST` 格式及 [`StorageEngine::flush_memtable`](e:/db/StructDB/src/engine/storage/src/storage_engine_compaction_lsm.cpp) 顺序对齐。

## 1. 当前能力

- **`StorageEngine::compact_merge_two_oldest_l0`**：当 `MANIFEST` 中 **至少有两个 L0**（**前两项** `level==0`）时，将 **最旧两个 L0** 合并为 **一个** 新 SST；键相同时 **后一文件（较新）** 的值覆盖前一文件。**默认**输出 **`L0-{manifest_version+1}.sst`** 并置于列表前端（九期语义）；**十二期**可选输出 **`L1-{gen}.sst`**（见 §1 下条与 [`PHASE12.md`](phases/PHASE12.md)）。
- **顺序**：写入新 SST → 更新并 **`MANIFEST` 持久化**（失败则删除新 SST 并恢复内存中的 manifest 列表）→ 删除两个旧 SST 文件 → **`CheckpointWriter::write_rotating`**（更新 `manifest_version` 与当前 `wal_offset`/`redo_offset`，并写入十期 **`undo_log_safe_prefix_bytes`** 水位，见 [`CHECKPOINT_UNDO_PREFIX.md`](phases/CHECKPOINT_UNDO_PREFIX.md) / [`PHASE10.md`](phases/PHASE10.md)）。
- **十二期（MVP）**：`MANIFEST` 支持 **`FORMAT2`** 与每条 SST 的 **level**；`get`/`visit_prefix` 等按 **层块**扫描：每层内 **新→旧**，层间 **L0 → L1 → L2 → …**（升序 level，见 `manifest_sst_paths_lookup_order`）。
- **十五 / 二十二期（22A）**：**L1→L2** — `EngineConfigSnapshot::l2_compact_output_from_l1_merge` + `compact_merge_two_oldest_l1_to_l2`；**L2→L3** — `l3_compact_output_from_l2_merge` + `compact_merge_two_oldest_l2_to_l3`（须 MANIFEST 中 **最前连续 L2 块** 内至少两个 `level==2` 项）。详 [`PHASE22.md`](phases/PHASE22.md)。
- **二十三 23B**：**L3→L4** — `l4_compact_output_from_l3_merge` + `compact_merge_two_oldest_l3_to_l4`（输出 `L4-{gen}.sst`，读序随 `FORMAT2` 层块延伸）。详 [`PHASE23.md`](phases/PHASE23.md)。
- **三十五期（PHASE35）**：**L0** `compact_merge_two_oldest_l0` / `drain_pending_l0_compactions` 在 **持 `mu_` 快照计划** 后于锁外读 SST、合并、写 **`_tmp_l0_compact_*.sst`**，再 **短持锁** 校验 manifest 头、**rename** 为最终 `L0-`/`L1-` 名并写 MANIFEST / checkpoint（与上条顺序一致；冲突时丢弃临时文件，可重试）。详见 [`PHASE35.md`](phases/PHASE35.md)。
- **三十六期（PHASE36）**：**L1→L2 / L2→L3 / L3→L4** 的 `compact_merge_two_oldest_*` 与 L0 对齐为 **快照 → 锁外物化 `_tmp_tier_compact_*` → 锁内校验与提交**（详见 [`PHASE36.md`](phases/PHASE36.md)）。
- **仍未实现**：**size-tiered** 多路挑选、**并发** compaction（后台 worker 仍须遵守 `POLICY` §4.2 单写者与锁序）。
- **背压（十四 / 二十一 / 二十二期）**：**十四期**起 L0 深度等 → Facade 映射 WAL 队列预算；**21C** 起 worker 队列 / 延后 L0 → **`CompactionSlots`**（[`PHASE21.md`](phases/PHASE21.md)、`Engine::sync_scheduler_budget_from_storage_pressure`）；**二十二 22B** 起 `GraphExecutor` 在 `use_budget_probe` 下对 **WalQueueDepth、CompactionSlots、MemTableBytes** 依次 `acquire_for_node`，使 `Orchestrator::on_backpressure` 可观测 **`WalBacklogged` / `CompactionBusy` / `MemTableFull`**（见 [`PHASE22.md`](phases/PHASE22.md)）。

## 2. 不变式与限制

- **WAL 权威不变**：compaction **不**替代 `wal.log` 重放；仅整理已 flush 到 SST 的键值布局。
- **调用方**：**九期**须 **显式** 调用 `compact_merge_two_oldest_l0`。**十一期**起可在 `EngineConfigSnapshot::l0_compact_trigger_threshold > 0` 时由 **`flush_memtable`** 在成功写 checkpoint 后 **自动** 多轮合并 L0（单写者、同步；阈值对比 **L0 段长度**；见 [`PHASE11.md`](phases/PHASE11.md)、[`PHASE12.md`](phases/PHASE12.md)）。**二十三 23A**：`l0_compact_max_inline_rounds_per_flush > 0` 且 **`l0_compact_defer_after_flush=false`** 时，将单次 flush 内自动合并轮数 **上限** 收紧为 `min(l0_compact_max_rounds_per_flush, …)`，与 **`l0_compact_defer_after_flush=true`** 时 **`drain_pending_l0_compactions` / 二十期 worker 队列**（[`PHASE19.md`](phases/PHASE19.md)、[`PHASE20.md`](phases/PHASE20.md)）互补，避免长事务 flush 尾部独占。**`checkpoint()`** 路径不触发该自动逻辑。
- **观测**：`StorageEngine::compaction_merge_count()` 统计本进程内 **成功完成** 的合并次数（进程内计数，重启归零）。

## 3. 验收参考

- 测试：`StorageEngine.Compaction*`、`StorageEngine.Phase11*`、`Engine.L0AutoCompactAfterFlushFromConfig`、`StorageEngine.Phase12*`、`Manifest.Phase12*`、`Engine.L1CompactOutputFromConfig`、**`StorageEngine.Phase22*`** / **`Manifest.Phase22*`**、**`Orchestrator.Phase22*`**、**`StorageEngine.Phase23*`** / **`Manifest.Phase23*`**、**`StorageEngine.Phase35*`**、**`*Phase36*`**、**`*Phase37*`**、**`StorageEngine.CompactionConcurrencySemanticMatrix`**、**`StorageEngine.ConcurrentNestedL0DrainAndL1MergeWhilePuts`**（见 [`Docs/TESTING_TXN_CHAIN.md`](TESTING_TXN_CHAIN.md) §6、§8、§9、§10、§12、**§14**）。

## 4. 与 `undo.log` 前缀回收（十期）

Compaction 写 checkpoint 时与 `flush_memtable` 一致，会 **重算** `CheckpointState::undo_log_safe_prefix_bytes`（不单独引入 compaction 调度下的 undo 语义；若未来在后台 compaction 与活跃回滚并发，须在 `POLICY` §3.5 扩展论证）。对向说明见 [`CHECKPOINT_UNDO_PREFIX.md`](phases/CHECKPOINT_UNDO_PREFIX.md) §3。
