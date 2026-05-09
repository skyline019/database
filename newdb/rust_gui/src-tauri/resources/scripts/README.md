# newdb scripts 目录说明

本目录存放构建验证、压测、稳定性跑批、ABI/数据校验等脚本。

## 目录结构

- `scripts/ci/`：CI 与发布门禁脚本
- `scripts/bench/`：性能/压测脚本
- `scripts/soak/`：稳定性循环与夜间跑批
- `scripts/validate/`：ABI、遥测、汇总校验
- `scripts/results/`：运行产物

## 1) 验证与质量门禁（scripts/ci）

- `ci/plugin_backend_packaging.md`  
  Track P：plugin 双产物（`newdb` + `newdb_cli_backend`）与 **`NEWDB_CLI_BACKEND_PATH`** 的发行目录与 shell 示例；详见 [C_API_PLUGIN_BACKEND.md](../docs/dev/C_API_PLUGIN_BACKEND.md)。

- `ci/sync_validate_scripts.py`  
  校验（或 `--apply` 同步）`scripts/validate/` 到 `rust_gui/scripts/validate/` 与
  `rust_gui/src-tauri/resources/scripts/validate/`，避免 Tauri bundle 与主干契约漂移。

- `ci/capture_baseline.py`  
  可选 `--emit-baseline-dir DIR` 生成 `baseline/`（runtime JSONL、manifest、ctest 日志）；产物默认不入库（见根 `.gitignore` / `newdb/.gitignore` 的 `/baseline/`）。可选 **`--cross-host-baseline-dir DIR`**（需与 `--emit-baseline-dir` 同用）复制 `manifest.json` 到 **`DIR/<host_slug>/`** 并维护 **`DIR/host_index.json`**，供跨机基线与 `ci_bench_gate.py --recommended-thresholds-json` 配套。

- `ci/verify_clean_reconfigure.ps1`  
  一体化验证入口（CMake clean configure/build/test + Rust/Vue gates + release-grade 规则）。

- `ci/check_coverage_threshold.py`  
  覆盖率阈值检查。

- `ci/ci_bench_gate.py`  
  CI 轻量压测门禁；支持 **`--recommended-thresholds-json`**（合并 `nightly_soak_hints` 导出的 PR 建议阈值到仍为 sentinel 的门禁参数）；支持可选 `--runtime-jsonl` + 阈值参数
  （`--min-vacuum-efficiency` / `--max-conflict-rate`）调用 `newdb_runtime_report`
  做运行时统计趋势门禁。额外支持 `--runtime-last-n` 与
  `--runtime-label-prefix`，用于单次窗口隔离与标签过滤。P11 增补
  `--max-txn-begin-lock-conflict-delta` 与 `--max-wal-compact-delta`。
  P1 进一步增补 compact 质量门禁：
  `--max-vacuum-compact-failure-delta` 与
  `--min-vacuum-compact-reclaimed-bytes-delta`。

## 2) 性能与压测脚本（scripts/bench）

- `bench/million_scale_bench.ps1`
- `bench/concurrent_million_scale_bench.ps1`
- `bench/concurrent_pressure_bench.ps1`
- `bench/query_bench.ps1`
- `bench/txn_write_bench.ps1`
- `bench/eq_sidecar_cache_bench.ps1`
- `bench/eq_sidecar_invalidation_bench.ps1`

其中 `bench/concurrent_pressure_bench.ps1` 已支持：
- 每次运行默认独立 `runtime_stats_concurrent_pressure_<timestamp>.jsonl`
- `-AppendRuntimeJsonl`（显式开启后才追加）
- `-RunRuntimeGate` + 阈值参数（支持均值与趋势分位阈值）
- 新增 `-MaxTxnBeginLockConflictDelta` / `-MaxWalCompactDelta`（锁冲突与 WAL 压缩 delta 门禁）
- 新增 `-MaxVacuumCompactFailureDelta` /
  `-MinVacuumCompactReclaimedBytesDelta`（autovacuum compact 质量门禁）
- 新增 `-MaxVacuumQueueDepthPeak` /
  `-MaxWalRecoveryLastElapsedMs`（autovacuum 积压与 WAL 恢复时延门禁）
- 新增 `-MaxLockDeadlockDetectDelta` / `-MaxLockDeadlockVictimDelta` /
  `-MaxLockWaitMaxMsDelta` / `-MaxSchedulerThrottleDelta` /
  `-MinWalGroupCommitBatchCommitsDelta`（并发锁冲突与调度质量门禁）
- `-RuntimePressureBatches` / `-RuntimePressureBatchSize` / `-RuntimeSampleEveryBatches`（控制同生命周期多点采样强度）
- 采样会写入 `run_id`，可配合 `newdb_runtime_report --run-id` 做单次运行趋势分析（含 p50/p95）

相关输入模板：
- `query_bench.mdb`
- `txn_wal_vacuum_test.mdb`

## 3) 稳定性跑批（scripts/soak）

- `soak/test_loop.ps1`
- `soak/nightly_soak_runner.ps1`

`test_loop.ps1` 与 `nightly_soak_runner.ps1` 当前会输出趋势 JSONL，包含
`runtime_samples`、`runtime_vacuum_efficiency_p50`、`runtime_conflict_rate_p95`、
`runtime_txn_begin_lock_conflict_delta`、`runtime_wal_compact_delta`、
`runtime_vacuum_compact_failure_delta`、`runtime_vacuum_compact_reclaimed_bytes_delta`、
`runtime_vacuum_compact_success_delta`、
`runtime_vacuum_queue_depth_peak_max`、`runtime_wal_recovery_runs_delta`、
`runtime_wal_recovery_undo_ops_delta`、`runtime_wal_recovery_last_elapsed_ms_max`
等运行时门禁指标，便于长期回归追踪。

**汇总与归档路径（相对 `newdb/` 仓库根）**：

- `nightly_soak_runner.ps1`：`scripts/results/nightly_soak_trend.jsonl`、同目录下 `test_loop_trend.jsonl`；单次运行的 perf summary 由脚本解析 `test_loop` 输出路径后调用 `validate_perf_summary.py`（见 `nightly_soak_runner.ps1` 内 `Parse-SummaryPath`）。
- 与 **JSONL 归档契约**、**`ci_bench_gate.py --runtime-jsonl`** 对齐的说明见 [`docs/ci/PERF_AND_CI_BUDGETS.md`](../docs/ci/PERF_AND_CI_BUDGETS.md) §3–§4 与 [`docs/storage/STORAGE_GOVERNANCE_AND_RECOVERY_BUDGETS.md`](../docs/storage/STORAGE_GOVERNANCE_AND_RECOVERY_BUDGETS.md) §4；Nightly 阈值推荐区间可由 `python scripts/ci/nightly_soak_hints.py --json` 的 `threshold_hints` 字段查阅。

## 4) C API / 遥测 / 汇总校验（scripts/validate）

- `validate/check_c_api_abi.py`
- `validate/c_api_expected_symbols.txt`
- `validate/validate_telemetry_event.py`
- `validate/validate_perf_summary.py`
- `validate/validate_runtime_stats.py`
- `validate/validate_runtime_trend_dashboard.py`
- `validate/test_runtime_report_compact_gates.py`
- `PERF_METRICS_SCHEMA.md`
- `RUNTIME_STATS_SCHEMA.md`

`newdb.runtime_stats.v1` 当前除 vacuum/conflict 指标外，已补充
`txn_begin_lock_conflicts`、`wal_compact_count`，并新增 compact 观测字段
`vacuum_compact_success_count` / `vacuum_compact_failure_count` /
`vacuum_compact_bytes_reclaimed` / `vacuum_compact_last_elapsed_ms`，
用于观测事务启动锁争用、WAL 压缩强度与 autovacuum compact 质量。

## 5) 文档与运维说明

- `WRITE_PATH_TUNING_RUNBOOK.md`
- `PROD_REMEDIATION_PLAN.md`

## 6) 结果目录

- `results/` 为脚本生成产物目录（json/csv/jsonl），详见 `results/README.md`。
- `soak/runtime_trend_rollup.py` 会把 `test_loop_trend.jsonl` 与 `nightly_soak_trend.jsonl`
  聚合为 `runtime_trend_dashboard.json`（schema: `newdb.runtime_trend_dashboard.v1`），并输出
  `recent_runs`（默认最近 30 条，可用 `--recent-limit` 调整）方便前端直接绘制趋势线。
  P12 起 `recent_runs` 先按时间归并排序后再截断，并补充 `data_quality`（nightly 覆盖度）。
  P13 起补充 `secondary_metrics`（dashboard 质量门禁通过/失败计数）。
  P15 起补充 `perf_metrics`（`txn_normal_avg_ms/query_avg_ms_max/cm_tps_min/hp_max_query_avg_ms`）与
  `health` 分级对象（`healthy/warning/critical` + reasons）。
  支持 `--require-nightly-samples` 与 `--max-latest-nightly-age-hours` 质量门禁参数。
  支持 `--max-health-tier` 与 warn/critical 阈值参数（query/cm_tps/nightly_pass_rate）做分级门禁。
  `nightly_soak_runner.ps1` 会把该门禁结果写回 trend（`dashboard_quality_gate_status/reason`）。
  P16 起 `nightly_soak_runner.ps1` 还会回写 `dashboard_health_tier/dashboard_health_reasons`。
  `nightly_soak_runner.ps1 -LiteProfile` 可用于稳定 nightly 产样（避免高压分支造成抖动）。
  P14 起新增首轮 bootstrap 旁路：当 `nightly_soak_trend.jsonl` 尚无样本时，runner 首轮不强制
  `--require-nightly-samples`，先落第一条样本后恢复严格门禁，避免 Runs=1 场景首轮必失败。
  同时 nightly workflow 失败场景也会上传 artifact，并在门禁失败时自动创建 issue 告警。

---

## 使用建议

- 默认先跑：`scripts/ci/verify_clean_reconfigure.ps1`
- 需要趋势/耐久验证时再跑：`scripts/soak/nightly_soak_runner.ps1` / `scripts/soak/test_loop.ps1`
- profile 对比建议覆盖：`leveldb-like` / `innodb-like` / `hybrid-balanced`
- 压测结果与趋势文件统一放在 `results/`，避免污染根目录

