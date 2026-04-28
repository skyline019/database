# newdb Runtime Stats Schema

Schema name: `newdb.runtime_stats.v1`  
Format: JSON Lines (`.jsonl`), one snapshot per line.

## Top-level fields

- `schema_version` (string, required): must be `newdb.runtime_stats.v1`
- `ts_ms` (int, required): unix timestamp in milliseconds
- `label` (string, required): snapshot label, e.g. `pressure_before`, `pressure_sample_1`, `pressure_after`
- `run_id` (string, optional): run correlation id for windowed analysis
- `stats` (object, required): runtime counters/status payload

## `stats` object fields

- `walsync` (string): one of `off|normal|full`
- `normal_interval_ms` (int >= 0)
- `autovacuum` (bool)
- `vacuum_ops_threshold` (int >= 0)
- `vacuum_min_interval_sec` (int >= 0)
- `vacuum_trigger_count` (int >= 0)
- `vacuum_execute_count` (int >= 0)
- `vacuum_cooldown_skip_count` (int >= 0)
- `vacuum_compact_success_count` (int >= 0)
- `vacuum_compact_failure_count` (int >= 0)
- `vacuum_compact_bytes_reclaimed` (int >= 0)
- `vacuum_compact_last_elapsed_ms` (int >= 0)
- `vacuum_queue_depth` (int >= 0): 当前 autovacuum 待处理队列深度
- `vacuum_queue_depth_peak` (int >= 0): 运行期观测到的队列深度峰值
- `maintenance_checkpoint_trigger_count` (int >= 0): WAL checkpoint/compact 触发次数
- `maintenance_checkpoint_vacuum_enqueue_count` (int >= 0): checkpoint 后联动入队 vacuum 次数
- `write_conflicts` (int >= 0)
- `lock_wait_ms_total` (int >= 0): 写冲突等待累计耗时
- `lock_wait_max_ms` (int >= 0): 单次写冲突等待最大耗时
- `lock_deadlock_detect_count` (int >= 0): 最小死锁检测命中次数
- `lock_deadlock_victim_count` (int >= 0): 死锁中被选为 victim 的事务次数
- `txn_begin_lock_conflicts` (int >= 0): `BEGIN(table)` 阶段文件锁冲突次数
- `wal_compact_count` (int >= 0): WAL checkpoint+truncate 成功执行次数
- `wal_recovery_runs` (int >= 0): WAL 恢复流程调用次数（含空恢复）
- `wal_recovery_undo_ops` (int >= 0): WAL 恢复累计补偿操作条数
- `wal_recovery_last_elapsed_ms` (int >= 0): 最近一次 WAL 恢复耗时
- `wal_recovery_analyze_ms` (int >= 0): 最近一次 WAL 恢复 analyze 阶段耗时（读/解析/归档）
- `wal_recovery_undo_ms` (int >= 0): 最近一次 WAL 恢复 undo 阶段耗时（补偿落盘）
- `wal_recovery_finalize_ms` (int >= 0): 最近一次 WAL 恢复 finalize 阶段耗时（补写终态/刷盘）
- `wal_recovery_records_scanned` (int >= 0): 最近一次恢复扫描的 WAL 记录数
- `wal_recovery_dangling_txns` (int >= 0): 最近一次恢复识别到的 dangling 事务数
- `wal_group_commit_count` (int >= 0): group commit flush 次数
- `wal_group_commit_batch_commits` (int >= 0): group commit 累计批次提交事务数
- `wal_group_commit_pending_commits` (int >= 0): 尚未 flush 的待批提交事务数
- `txn_commit_count` (int >= 0): 累计提交次数
- `txn_commit_p95_ms` (int >= 0): 提交耗时 P95（最近窗口）
- `txn_commit_max_ms` (int >= 0): 提交耗时最大值（累计）
- `wal_bytes_since_start` (int >= 0): 运行期 WAL 文件增长字节累计（近似）
- `wal_bytes_per_commit_avg` (int >= 0): 平均每次提交 WAL 增量（近似）
- `lock_wait_p95_ms` (int >= 0): 写冲突等待耗时 P95（最近窗口）
- `scheduler_throttle_count` (int >= 0): 后台调度节流触发次数
- `hot_index_enabled` (bool): 热索引开关状态
- `segment_target_bytes` (int >= 0): 逻辑分段目标大小（字节）
- `lsm_memtable_flush_count` (int >= 0): memtable flush 到 segment 的累计次数
- `lsm_compaction_count` (int >= 0): LSM-lite 轻量 compaction 执行次数
- `lsm_segment_count` (int >= 0): 当前存活 segment 文件数
- `lsm_memtable_bytes` (int >= 0): 当前 memtable 估算字节数
- `lsm_read_segments_scanned` (int >= 0): FIND 读路径累计扫描 segment 数
- `lsm_read_segments_scanned_p95` (int >= 0): FIND 单次扫描 segment 数的 p95
- `lsm_compaction_bytes_in` (int >= 0): compaction 输入字节累计值
- `lsm_compaction_bytes_out` (int >= 0): compaction 输出字节累计值
- `lsm_compaction_queue_pending` (int >= 0): 异步 compaction 队列中等待任务数
- `lsm_compaction_queue_inflight` (int >= 0): 异步 compaction 处理中任务数
- `lsm_compaction_enqueue_skipped_backpressure` (int >= 0): 因背压跳过 enqueue 的累计次数
- `lsm_segment_cache_hits` (int >= 0): LSM segment cache 命中次数
- `lsm_segment_cache_misses` (int >= 0): LSM segment cache 未命中次数
- `lsm_compaction_bytes_amp_efficiency_min_window` (number >= 0): 观测窗口内 compaction bytes_out/bytes_in 比值
- `lsm_read_segments_scanned_p95_window` (int >= 0): 观测窗口内 scan p95
- `hybrid_mode` (string): `throughput_mode|durability_mode`
- `hybrid_mode_switch_count` (int >= 0): hybrid 模式切换次数
- `hybrid_last_switch_reason` (string): 最近一次切换原因
- `rollback_savepoint_count` (int >= 0): savepoint 级回退次数
- `rollback_partial_ops` (int >= 0): savepoint 部分回退涉及操作数
- `pitr_runs` (int >= 0): PITR 回退执行次数
- `pitr_target_lsn` (int >= 0): 最近一次 PITR 目标 LSN
- `pitr_elapsed_ms` (int >= 0): 最近一次 PITR 耗时
- `undo_chain_fallback_count` (int >= 0): undo 链缺失时 fallback 次数
- `where_query_cache_lookups` (int >= 0): WHERE 查询缓存查找次数
- `where_query_cache_hits` (int >= 0): WHERE 查询缓存命中次数
- `where_policy_checks` (int >= 0): WHERE policy 检查次数
- `where_policy_rejects` (int >= 0): WHERE policy 拒绝次数
- `where_fallback_scans` (int >= 0): WHERE fallback 全/大范围扫描次数
- `where_plan_eq_sidecar_count` (int >= 0): 使用 eq sidecar 的查询计划计数
- `where_plan_id_pk_count` (int >= 0): 使用 id/pk 快路径的查询计划计数
- `where_plan_fallback_count` (int >= 0): 使用 fallback 扫描计划计数
- `wal_adaptive_enabled` (bool): adaptive WAL 开关状态
- `group_commit_window_ms` (int >= 0): group commit 窗口毫秒
- `group_commit_max_batch_commits` (int >= 0): group commit 最大批提交数

## Notes

- Counters are cumulative within one runtime/session and should be compared via deltas.
- For CI/runtime gate isolation, prefer filtering by `run_id`, then applying optional `--last-n` windows.
- Validation script: `scripts/validate/validate_runtime_stats.py`.
- Schema validation input must be the raw runtime snapshot JSONL (for example, `runtime_stats_raw_jsonl` in pressure summary).
- `concurrent_pressure_summary_*.json` and `runtime_gate_summary` are gate outputs and are not valid inputs for `validate_runtime_stats.py`.
- For backward compatibility, validator accepts older pressure rows without `walsync/where_*` fields and fills safe defaults during validation.

## LSM-lite knobs (environment variables)

These knobs are used to control the LSM-lite demo write path. They are intentionally environment-driven so
bench/soak scripts can tune without rebuilding.

- `NEWDB_LSM_L0_COMPACT_TRIGGER` (int >= 1, default: 4): minimum number of L0 segments before a compaction attempt.
- `NEWDB_LSM_L0_COMPACT_BATCH` (int >= 1, default: 4): max number of segments compacted per batch.
- `NEWDB_LSM_COMPACTION_ASYNC` (bool, default: off): enable async compaction mode.
- `NEWDB_LSM_COMPACTION_WORKERS` (int >= 1, default: 2): async compaction worker count.
- `NEWDB_LSM_COMPACTION_MAX_PENDING` (int >= 0, default: 0/disabled): max pending compaction tasks before backpressure.
- `NEWDB_LSM_COMPACTION_REAP_BUDGET` (int >= 1, default: 4): max compaction batches executed per loop.
- `NEWDB_LSM_LEVELED_L1_SOFT_SEGMENTS` (int >= 1, default: 24): leveled_lite 软上限，低于该值可提前停止压缩。
- `NEWDB_LSM_LEVELED_L1_HARD_SEGMENTS` (int >= 1, default: 48): leveled_lite 硬上限，超过后强制追加 compaction 批次。

## LSM-lite on-disk layout

- Directory: `<data>.lsm/` alongside `<data>.bin`
- Segment file names:
  - `L0_<id>.log`: flush outputs
  - `L1_<id>.log`: compaction outputs

