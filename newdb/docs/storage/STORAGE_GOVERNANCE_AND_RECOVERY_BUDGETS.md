# 存储治理与恢复验收（阶段 A）

本文定义 **heap/WAL/sidecar** 长期运行下的治理策略与可观测验收思路，对标计划中「LevelDB 式后台合并治理」方向；实现分布在事务协调器 vacuum 线程、默认 heap compact 与环境变量。

## 1. VACUUM / compact 主线

- **后台线程**：[`vacuum_service.cc`](../../cli/modules/txn/coordinator/vacuum/vacuum_service.cc) 中 `startVacuumThread`：工作线程最长 **60s** 等待队列信号或超时唤醒，处理 **`m_vacuum_queue`**（项为 `(table, debt_score)`，**`debt_score` 默认取 heap `.bin` 文件字节数**，每次出队取 **debt 最大** 的表；仍保留冷却、去重与队列上限）。
  - **可选 health 加权**：`NEWDB_VACUUM_QUEUE_USE_HEALTH=1` 时，入队前对目标表做一次 **lazy** `load_heap_file` + [`measure_table_storage_health`](../../cli/modules/storage/table_storage_health.h)，将 **`tombstone_slots * NEWDB_VACUUM_HEALTH_SLOT_WEIGHT`（默认 65536）+ `floor(tombstone_ratio * 1e6)`** 累加到 `debt_score`（饱和到 `uint64` 上限）；失败或开关关闭时行为与「仅文件字节」一致。运行态导出 **`vacuum_health_bonus_last`**（最后一次入队的 bonus）。
- **入队**：`triggerVacuum(table_name)`（同文件）：
  - 按表维护 **冷却**：两次触发间隔不少于 `m_vacuum_min_interval_sec`（可通过 `setVacuumMinIntervalSec` / 持久化配置调整）；冷却内触发计入 `vacuum_cooldown_skip_count`。
  - **Score 与 `wal_since_vacuum` / cooldown（与评估文档对齐）**：**成功 compact 后**（`vacuum_service` 默认路径），以当前 **`WalManager::current_lsn()`** 与耗时刷新 **`TxnRuntimeStats` 的 `table_storage_health_last_vacuum_lsn` / `table_storage_health_last_vacuum_elapsed_ms`**（若 health 重采样失败则仅合并 `last_vacuum_*` 两列）。**默认**仍 **不** 将 `wal_since` 叠入入队 score（v1 选项 B）；若需实验性「WAL 距上次 vacuum」项，可设 **`NEWDB_VACUUM_SCORE_WAL_SINCE=1`**，在 `last_vacuum_lsn` 与当前 LSN 均可得时对 `queue_score` 加 **有上界** 的 gap 项。**冷却**仅通过 **`vacuum_cooldown_skip_count`** 与最小间隔配置观测，与入队 **debt_score** 正交（冷却本身不入 score）。
  - **去重**：同一表在 `m_vacuum_pending` 中未处理完则不重复入队。
  - **队列上限**：默认 **256**，可通过环境变量 **`NEWDB_MAINTENANCE_QUEUE_MAX`** 覆盖；超限计入 scheduler throttle。
  - **执行**：若设置了 `m_vacuum_callback` 则调用回调；否则 **`compact_table_file_default`** 对 `.bin` 做默认堆紧缩。
- **并发保护**：队列深度超过 **16** 时线程短暂 `sleep(2ms)`，避免纯自旋占满 CPU。

**运维含义**：newdb 不等同 LevelDB 的全自动分层 compaction，而是以「**提交后触发 + 后台队列 + 按表冷却**」控制回收节奏；长期 soak 应观察 `TxnRuntimeStats` / JSON 统计中的 `vacuum_*` 与 `scheduler_throttle_count`。

## 2. 大表与懒加载物化

- **策略**：读路径优先 **PAGE / 索引化 WHERE / sidecar**，避免对大堆表做全量 `rows` 物化。
- **写路径**：DML 在需改内存行向量前调用 [`newdb_materialize_heap_if_lazy`](../../cli/shell/state/shell_state.h)：`HeapTable` 由堆文件支持且逻辑行数超过阈值时，仍会向 **stderr** 打印 `[LAZY_MATERIALIZE]` 告警；同时 **`TxnRuntimeStats`** 累加 `lazy_materialize_*`（见 [`RUNTIME_STATS_SCHEMA.md`](../../scripts/validate/RUNTIME_STATS_SCHEMA.md)），供 `ci_bench_gate.py` / `newdb_runtime_report` 软门使用。
- **环境变量**：**`NEWDB_LAZY_MATERIALIZE_WARN_ROWS`**（默认 `10000`）：超过即告警；用于 CI/集成环境及早发现「意外全表物化」。
- **堆解码观测**：运行态 JSON 中的 **`heap_decode_slot_*`** 反映当前会话表的惰性解码计数，可与 WHERE `where_query_rows_scanned_total` 对照。
- **存储健康度（雏形）**：[`table_storage_health.h`](../../cli/modules/storage/table_storage_health.h) 提供 `measure_table_storage_health`（逻辑/物理行、墓碑、按文件字节的活/死估算、`fragmentation_ratio` 等，与路线图 §5.2 对齐；`last_vacuum_*` 字段预留为 0）。在 **`NEWDB_VACUUM_QUEUE_USE_HEALTH=1`** 且 lazy 加载成功时，入队路径会 **`recordLastStorageHealthSnapshot`**，运行态 JSON 导出 `table_storage_health_*` 字段；可选门禁：`ci_bench_gate.py` 透传至 `newdb_runtime_report` 的 **`--max-table-storage-health-fragmentation-ratio`**、**`--max-table-storage-health-dead-bytes`**、**`--max-vacuum-health-bonus-last`**（健康采样与 bonus 语义见第 4 节）。
- **查询统计落盘（可选）**：`NEWDB_QUERY_USE_TABLE_STATS=1` 且 **`NEWDB_QUERY_PERSIST_TABLE_STATS=1`** 时，WHERE 规划用统计可写入 **`<data>.bin.tablestats`**；随索引失效与 heap compact 清除（见 `invalidate_table_stats_for_data_file`）。

## 3. 恢复与 WAL 验收指标（建议预算）

**文档交叉（2026-05-04）**：不导出独立 **recover 子阶段 C/CLI API** 的产品边界见 [`NEWDB_FILE_LEVEL_MODIFICATION_PLAN.md`](../roadmap/NEWDB_FILE_LEVEL_MODIFICATION_PLAN.md) **§3.3**；观测仍以 **`WalRecoveryStats`** + runtime **`wal_recovery_*`** 与 **`test_wal_recovery_indexed` / `test_wal_segment_scanner`** 为主。

引擎侧 **`WalRecoveryStats`** 字段语义见 [intro 附录](../../intro/01-overview/section.tex) 与 [`wal_manager.h`](../../engine/include/newdb/wal_manager.h)。恢复流程已使用共享的 **`list_wal_segment_paths`**（[`wal_segment_scanner.h`](../../engine/include/newdb/wal/wal_segment_scanner.h)）做 WAL 目录清单统计（`wal_dir_inventory_dot_wal_files`）并记录 **`checkpoint_scan_ms`**（目录清单 + 段索引第一遍扫描耗时；默认恢复语义不变）。建议在 **夜间/soak** 或带 `runtime_report` 流水线的 CI 中，通过 [`scripts/ci/ci_bench_gate.py`](../../scripts/ci/ci_bench_gate.py) 的 **`--max-wal-recovery-last-elapsed-ms`** 等对 **`wal_recovery_last_elapsed_ms`** 设上限（需配合 `newdb_runtime_report` + JSONL 输入；详见 [PERF_AND_CI_BUDGETS.md](../ci/PERF_AND_CI_BUDGETS.md)）。**收口约定**：与 `verify_clean_reconfigure.ps1 -BenchGateWalRecovery` 对齐时，可从 **`2000` ms** 起按机器调参（仅约束 JSONL 窗口内峰值，**非** PR 默认阻断）。

**可选恢复环境变量（默认不改变既有恢复结果）**：

- **`NEWDB_RECOVER_MIN_LSN`**：数值下界，与 **`replay_start_lsn`** 下限对齐（未设置则为 0）。
- **`NEWDB_RECOVER_ENABLE_OFFSET_SEEK=1`**：在段级索引支持下跳过明显早于重放起点的段/偏移（需与 MIN_LSN 或下方 checkpoint 选项配合才有收益）。
- **`NEWDB_RECOVER_USE_CHECKPOINT_LSN=1`**：将有效重放起点抬升为 **`max(NEWDB_RECOVER_MIN_LSN, last_complete_checkpoint_lsn)`**，其中 **`last_complete_checkpoint_lsn`** 来自完整 checkpoint 边界（含 v1 payload 中的 **`checkpoint_snapshot_lsn`**，旧 WAL 无该字段时退化为记录头 LSN）。

**定性目标**：

| 维度 | 目标 |
|------|------|
| 堆文件增长 | 同等业务下 vacuum 成功次数与回收字节可解释；失败次数异常时有告警 |
| 恢复时间 | 典型库上 recovery 耗时相对基线无数量级回退 |
| 校验失败 | `checksum_failures` / `decode_failures` 在健康负载下为 0 |

## 4. CI 闭环（runtime JSONL → 校验 → 门禁）

长期 soak 或带运行态快照的流水线建议按顺序闭环，避免「字段已导出但无人读」：

1. **采集**：会话或压测进程将 `TxnRuntimeStats` 以 JSON 行写入 `runtime_stats.jsonl`（字段语义见 [`RUNTIME_STATS_SCHEMA.md`](../../scripts/validate/RUNTIME_STATS_SCHEMA.md)）。
2. **契约**：[`scripts/validate/validate_runtime_stats.py`](../../scripts/validate/validate_runtime_stats.py) 校验每行类型与必选键（含 `table_storage_health_*`、`vacuum_health_bonus_last` 等）。
3. **门禁**：[`scripts/ci/ci_bench_gate.py`](../../scripts/ci/ci_bench_gate.py) 在传入 **`--runtime-jsonl`** 时调用 **`newdb_runtime_report`**，可选阈值包括：
   - **`--max-table-storage-health-fragmentation-ratio`**：健康采样窗口内碎片率峰值上限；
   - **`--max-table-storage-health-dead-bytes`**：`table_storage_health_dead_bytes` 峰值上限（与队列侧 heap 字节 debt 互补的「死空间」代理，便于对标路线图中的 compact debt 预算思路）；
   - **`--max-vacuum-health-bonus-last`**：`vacuum_health_bonus_last` 在窗口内的最大值上限（`NEWDB_VACUUM_QUEUE_USE_HEALTH=1` 时墓碑/ratio 加权 bonus）。
4. **汇总行**：门禁成功时日志中的 **`RUNTIME_GATE_SUMMARY`** JSON 会附带 `table_storage_health_fragmentation_peak`、`table_storage_health_dead_bytes_peak`、`vacuum_health_bonus_last_max` 等峰值字段，便于与 `validate_runtime_stats` 结果对照归档。

**Nightly 阈值提示**：`python scripts/ci/nightly_soak_hints.py --json` 输出中的 **`threshold_hints`** 给出与 **`verify_clean_reconfigure.ps1 -BenchGateStorage`** 对齐的**起点**（如 **`max_wal_recovery_last_elapsed_ms_start`: 2000**）；与 fixture 解耦的机器相关区间仍以本文第 3 节与 [`PERF_AND_CI_BUDGETS.md`](../ci/PERF_AND_CI_BUDGETS.md) 第 3 节为准调参。

**`verify_clean_reconfigure.ps1 -ReleaseGrade`** 会对 `ci_bench_gate.py` 附加 **`--release-grade`**（收紧 lazy 物化 delta 等）；若要在 Release 级流水线对 health/debt 做硬门，需在同一脚本调用中显式传入上述 **`--max-table-storage-health-*` / `--max-vacuum-health-bonus-last`** 与 **`--runtime-jsonl`**（默认干净构建不采集 jsonl，故不设阈值）。

## 5. 相关源码索引

- Vacuum：`cli/modules/txn/coordinator/vacuum/vacuum_service.cc`
- 物化辅助：`cli/shell/state/shell_state.h`（`newdb_materialize_heap_if_lazy`）
- WAL 恢复统计：`engine/include/newdb/wal_manager.h`（`WalRecoveryStats`）
