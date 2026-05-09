# newdb Runtime Stats Schema

Schema name: `newdb.runtime_stats.v1`  
Format: JSON Lines (`.jsonl`), one snapshot per line.

**CI gate key list (single source for `validate_runtime_stats.py`):** [`contract/runtime_stats.v1.required.json`](contract/runtime_stats.v1.required.json). Optional tier tags: `stats_keys_cli_embed_layer` lists keys sourced from the CLI bridge sandwich (remainder are treated as engine-layer keys for parity tooling). Add a key there and in this document when promoting a counter to a required gate field. Optional JSON Schema for one line shape: [`contract/runtime_stats.v1.line.schema.json`](contract/runtime_stats.v1.line.schema.json).

**`validate_runtime_stats.py` CLI（默认不变）**：无额外参数时仍校验完整 `required_stats_keys`。可选 `--stats-keys-tier engine` 仅要求并做类型检查的是 `required_stats_keys` 去掉 `stats_keys_cli_embed_layer` 后的集合；`--stats-keys-tier cli_embed` 仅针对 `stats_keys_cli_embed_layer` 子集。GUI / 夜间流水线可选用分层门禁；仓库 CI 仍以完整契约为主。

## Top-level fields

- `schema_version` (string, required): must be `newdb.runtime_stats.v1`
- `ts_ms` (int, required): unix timestamp in milliseconds
- `label` (string, required): snapshot label, e.g. `pressure_before`, `pressure_sample_1`, `pressure_after`
- `run_id` (string, optional): run correlation id for windowed analysis
- `stats` (object, required): runtime counters/status payload

### Engine layer (`newdb_engine_runtime_stats_json`)

POD / coordinator counters emitted only through [`runtime_stats_snapshot_json_write.cpp`](../../engine/src/json/runtime_stats_snapshot_json_write.cpp) (the `append_runtime_stats_snapshot_json_members_*` pipeline). See [RUNTIME_STATS_JSON_LAYERING.md](../../docs/dev/RUNTIME_STATS_JSON_LAYERING.md).

### CLI embed layer (`newdb_cli_runtime_stats_json`)

Keys whose values are sourced from the CLI bridge fragment [`_generated_runtime_json.inc`](../../cli/shell/c_api/_generated_runtime_json.inc) (isolation, WHERE metrics, heap decode slot counters, coordinator facade fields, etc.). See [RUNTIME_STATS_JSON_LAYERING.md](../../docs/dev/RUNTIME_STATS_JSON_LAYERING.md).

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
- `wal_recovery_redo_ms` (int >= 0, optional / legacy default 0): CLI `recoverFromWAL` 路径上 redo（已提交重放）阶段墙钟毫秒
- `wal_recovery_checkpoint_begin_count` / `wal_recovery_checkpoint_end_count` (int >= 0, optional): 扫描 WAL 时观测到的 checkpoint begin/end 记录计数
- `wal_recovery_records_after_checkpoint` (int >= 0, optional): 最近协助恢复路径下，LSN 大于最后完整 checkpoint 边界的记录条数（CLI `capture_recovery_scan_stats` + 扫描向量）
- `wal_recovery_segments_after_checkpoint` (int >= 0, optional): 参与索引的 WAL 段数（`WalRecoveryStats::indexed_segments` 镜像）
- `wal_recovery_redo_plan_pending_txn_count` (int >= 0, optional): 重放计划阶段观测到的未提交事务数（与 CLI reconcile 中 `dangling_by_txn` 规模一致）
- `wal_recovery_apply_conflict_count` (int >= 0, optional): redo 应用阶段因幂等/去重跳过的操作次数
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
- `where_heap_scan_budget_binding_events` (int >= 0): `NEWDB_WHERE_HEAP_SCAN_BUDGET_ROWS` 使有效扫描上限低于比例 `scan_cap` 的次数（观测）
- `where_fallback_scans` (int >= 0): WHERE fallback 全/大范围扫描次数
- `where_plan_eq_sidecar_count` (int >= 0): 使用 eq sidecar 的查询计划计数
- `where_plan_id_pk_count` (int >= 0): 使用 id/pk 快路径的查询计划计数
- `where_plan_fallback_count` (int >= 0): 使用 fallback 扫描计划计数
- `where_query_count` (int >= 0): 完成的 `query_with_index` 调用次数
- `where_query_rows_scanned_total` (int >= 0): 查询路径累计解码/扫描行次（与 `estimated_scan_rows_*` 不同，为实际执行量）
- `where_query_rows_returned_total` (int >= 0): 查询路径累计返回匹配 slot 数
- `where_eq_sidecar_disk_bytes_read_total` (int >= 0, optional): equality `.eqidx` 从磁盘加载的字节累计（不含内存缓存命中）
- `where_eq_sidecar_disk_loads` (int >= 0, optional): equality sidecar 磁盘加载次数
- `lazy_materialize_count` (int >= 0): 惰性堆物化触发次数
- `lazy_materialize_rows_total` (int >= 0): 惰性物化累计展开行数
- `lazy_materialize_max_rows` (int >= 0): 单次物化观测到的最大行数
- `lazy_materialize_elapsed_ms` (int >= 0): 惰性物化累计耗时（毫秒）
- `heap_decode_slot_calls` (int >= 0): 堆行解码调用次数（当前会话表）
- `heap_decode_slot_hits` (int >= 0): 堆行解码命中次数
- `heap_decode_slot_misses` (int >= 0): 堆行解码未命中次数
- `vacuum_priority_score` (int >= 0): 维护队列压力启发值（触发 vacuum 入队时由深度与 pending 估算）
- `vacuum_health_bonus_last` (int >= 0): 最近一次 vacuum 入队时由 `measure_table_storage_health` 推导的墓碑/比率 bonus（仅 `NEWDB_VACUUM_QUEUE_USE_HEALTH=1` 时非零；否则为 0）
- `table_storage_health_logical_rows` (int >= 0): 最近一次成功采样的逻辑行数（健康 env 下 lazy 加载成功；否则为 0）
- `table_storage_health_physical_rows` (int >= 0): 同上采样的物理/元数据 slot 数
- `table_storage_health_tombstone_rows` (int >= 0): 墓碑行计数（与内部 `tombstone_slots` 对齐）
- `table_storage_health_data_file_bytes` (int >= 0): `.bin` 字节数（mmap 视图或 `name.bin` 探测）
- `table_storage_health_live_bytes` / `table_storage_health_dead_bytes` (int >= 0): 按墓碑比率估算的活/死字节
- `table_storage_health_fragmentation_ratio` (number >= 0): `dead_bytes / data_file_bytes` 或仅墓碑比率（无文件大小时）
- `table_storage_health_last_vacuum_lsn` / `table_storage_health_last_vacuum_elapsed_ms` (int >= 0)
- `table_storage_health_tier` (`good` | `watch` | `degraded` | `critical`)：由最近一次存储健康快照的碎片率与 `dead_bytes` 阈值推导
- `transaction_snapshot_lsn` / `statement_snapshot_lsn` (int >= 0)：读路径 MVCC 锚点 LSN（分别对应 Snapshot `BEGIN` 钉扎与语句级刷新）
- `txn_snapshot_refresh_count` (int >= 0)：ReadCommitted / 语句级 snapshot 刷新计数
- `txn_snapshot_pinned_count` (int >= 0)：Snapshot 跨语句钉扎计数（诊断）
- `txn_readpath_disabled_count` (int >= 0)：`NEWDB_TXN_ISOLATION_READPATH=0` 跳过读路径 snapshot 的次数
- `last_snapshot_source` (string)：`none|txn|statement|disabled`
- `lock_key_range_count` (int >= 0)：首次成功的 `LockKeyKind::RangeWriteIntent` 预留次数
- `lock_key_predicate_count` (int >= 0)：首次成功的 `LockKeyKind::PredicateWriteIntent` 预留次数
- `wal_adaptive_enabled` (bool): adaptive WAL 开关状态
- `group_commit_window_ms` (int >= 0): group commit 窗口毫秒
- `group_commit_max_batch_commits` (int >= 0): group commit 最大批提交数
- `compact_debt_bytes` / `compact_debt_rows` / `compact_debt_priority` (int >= 0); `compact_debt_ratio` (number >= 0): vacuum 入队与 runtime 观测同源 debt 代理
- `page_cache_hits` / `page_cache_misses` / `page_cache_evictions` (int >= 0): 进程级惰性堆页缓存
- `page_cache_bytes_in_cache` (int >= 0): 当前缓存占用字节
- `memory_budget_max_bytes` (int >= 0): `NEWDB_PAGE_CACHE_MAX_BYTES` 配置上限（0 表示未启用 cap）
- `memory_budget_used_bytes` (int >= 0): 当前计入预算的字节（与 `page_cache_bytes_in_cache` 一致）
- `memory_budget_reject_count` (int >= 0): 单页大于 cap 时拒绝 `page_cache_put` 的累计次数
- `memory_budget_bytes_evicted_total` (int >= 0): PageCache LRU 淘汰累计释放字节（`PageCacheGlobalStats::bytes_evicted_total`）
- `memory_budget_sidecar_load_skipped_total` (int >= 0): 当 `memory_budget_used_bytes + .eqidx` 文件大小超过 cap 时跳过磁盘加载的次数（`equality_index_sidecar`）
- `NEWDB_MEMORY_BUDGET_MAX_BYTES`（可选）：若设置且 >0，则 `memory_budget_max_bytes` 优先取该值，否则回退 `NEWDB_PAGE_CACHE_MAX_BYTES`（见 `memory_budget.h`）

**Note**：eq sidecar 与 PageCache 共用 cap 的软拒绝已落地；更细的「sidecar 常驻内存记账」仍可按路线图扩展。

## Optional v2 fields (`newdb.runtime_stats.v2` draft, backward compatible)

The validator `validate_runtime_stats.py` continues to accept `schema_version: newdb.runtime_stats.v1`.
The following keys are **optional** on `stats` and may appear in newer binaries / soak exports without
breaking v1 validation:

| Field | Type | Meaning |
|-------|------|---------|
| `txn_snapshot_refresh_count` | int | ReadCommitted / statement snapshot refresh count |
| `txn_snapshot_pinned_count` | int | Snapshot pinned across statements (diagnostic) |
| `txn_readpath_disabled_count` | int | `NEWDB_TXN_ISOLATION_READPATH=0` skip count |
| `last_snapshot_source` | string | `none\|txn\|statement\|disabled` |
| `wal_recovery_policy` | string | Recovery entry policy label (CLI reconcile + optional heap recover tags) |
| `write_conflict_last_sample` | string | Last write-conflict diagnostic line (`table=...;row=...;holder=...;tag=...`) |
| `sidecar_invalidate_count` | int | `sidecar_invalidate_all_indexes_for_data_file` requests (non-empty path) |
| `sidecar_invalidate_fail_count` | int | Equality sidecar on-disk remove attempts that returned a non-`no_such_file` error |
| `file_lock_acquire_fail_count` | int | OS file lock acquire failures |
| `file_lock_same_process_reuse_count` | int | `acquireLock` early-return when already held in-process |
| `file_lock_stale_marker_count` | int | Empty `.lock` marker removed under `NEWDB_FILE_LOCK_STRICT=1` before retry |
| `vacuum_score_file_bytes_term` | int | Last vacuum enqueue score: raw `.bin` size term |
| `vacuum_score_health_bonus_term` | int | Last vacuum enqueue score: health-derived bonus term |
| `vacuum_score_wal_since_term` | int | Last vacuum enqueue score: optional WAL-since term (`NEWDB_VACUUM_SCORE_WAL_SINCE=1`) |
| `lock_key_range_count` | int | Successful first-time `LockKeyKind::RangeWriteIntent` reservations (`tryReserveWriteLockKey`) |
| `lock_key_predicate_count` | int | Successful first-time `LockKeyKind::PredicateWriteIntent` reservations |
| `mem_page_cache_used_bytes` | int | Phase-5 MemoryRegistry per-kind PageCache used bytes (`memory_registry_totals().page_cache_used_bytes`) |
| `mem_page_cache_evictions` | int | Phase-5 MemoryRegistry PageCache eviction counter (record_eviction calls) |
| `mem_page_cache_admit_rejects` | int | Phase-5 MemoryRegistry PageCache `try_admit` rejects (cap exhausted or oversized page) |
| `mem_sidecar_used_bytes` | int | Phase-5 MemoryRegistry equality sidecar used bytes (LRU resident) |
| `mem_sidecar_evictions` | int | Phase-5 MemoryRegistry equality sidecar evictor invocations |
| `mem_sidecar_admit_rejects` | int | Phase-5 MemoryRegistry equality sidecar `try_admit` rejects (entry too large or cap exhausted) |
| `mem_query_temp_used_bytes` | int | Phase-5 MemoryRegistry WHERE query temp reservation (rough estimate) |
| `mem_query_temp_evictions` | int | Phase-5 MemoryRegistry WHERE query temp evictions (placeholder; reserved) |
| `mem_query_temp_admit_rejects` | int | Phase-5 MemoryRegistry WHERE query temp `try_admit` rejects (caller fell back) |
| `mem_global_used_bytes` | int | Phase-5 MemoryRegistry aggregate per-kind used bytes |
| `mem_global_admit_rejects` | int | Phase-5 MemoryRegistry aggregate per-kind admit reject counter |

`newdb_runtime_report` may emit derived summaries (not in JSONL rows): `page_cache_hit_ratio`,
`where_scan_amplification`, `wal_recovery_redo_ratio` in its `--output` / `--json` report.

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

