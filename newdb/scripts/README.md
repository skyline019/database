# newdb scripts 目录说明

本目录存放构建验证、压测、稳定性跑批、ABI/数据校验等脚本。

## 目录结构

- `scripts/ci/`：CI 与发布门禁脚本
- `scripts/bench/`：性能/压测脚本
- `scripts/soak/`：稳定性循环与夜间跑批
- `scripts/validate/`：ABI、遥测、汇总校验
- `scripts/results/`：运行产物

## 1) 验证与质量门禁（scripts/ci）

- `ci/verify_clean_reconfigure.ps1`  
  一体化验证入口（CMake clean configure/build/test + Rust/Vue gates + release-grade 规则）。

- `ci/check_coverage_threshold.py`  
  覆盖率阈值检查。

- `ci/ci_bench_gate.py`  
  CI 轻量压测门禁；支持可选 `--runtime-jsonl` + 阈值参数
  （`--min-vacuum-efficiency` / `--max-conflict-rate`）调用 `newdb_runtime_report`
  做运行时统计趋势门禁。额外支持 `--runtime-last-n` 与
  `--runtime-label-prefix`，用于单次窗口隔离与标签过滤。P11 增补
  `--max-txn-begin-lock-conflict-delta` 与 `--max-wal-compact-delta`。

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
`runtime_txn_begin_lock_conflict_delta`、`runtime_wal_compact_delta`
等运行时门禁指标，便于长期回归追踪。

## 4) C API / 遥测 / 汇总校验（scripts/validate）

- `validate/check_c_api_abi.py`
- `validate/c_api_expected_symbols.txt`
- `validate/validate_telemetry_event.py`
- `validate/validate_perf_summary.py`
- `validate/validate_runtime_stats.py`
- `validate/validate_runtime_trend_dashboard.py`
- `PERF_METRICS_SCHEMA.md`
- `RUNTIME_STATS_SCHEMA.md`

`newdb.runtime_stats.v1` 当前除 vacuum/conflict 指标外，已补充
`txn_begin_lock_conflicts` 与 `wal_compact_count`，用于观测事务启动锁争用与
WAL 压缩触发强度。

## 5) 文档与运维说明

- `WRITE_PATH_TUNING_RUNBOOK.md`
- `PROD_REMEDIATION_PLAN.md`

## 6) 结果目录

- `results/` 为脚本生成产物目录（json/csv/jsonl），详见 `results/README.md`。
- `soak/runtime_trend_rollup.py` 会把 `test_loop_trend.jsonl` 与 `nightly_soak_trend.jsonl`
  聚合为 `runtime_trend_dashboard.json`（schema: `newdb.runtime_trend_dashboard.v1`），并输出
  `recent_runs`（默认最近 30 条，可用 `--recent-limit` 调整）方便前端直接绘制趋势线。
  P12 起 `recent_runs` 先按时间归并排序后再截断，并补充 `data_quality`（nightly 覆盖度）。
  支持 `--require-nightly-samples` 与 `--max-latest-nightly-age-hours` 质量门禁参数。
  `nightly_soak_runner.ps1` 会把该门禁结果写回 trend（`dashboard_quality_gate_status/reason`）。

---

## 使用建议

- 默认先跑：`scripts/ci/verify_clean_reconfigure.ps1`
- 需要趋势/耐久验证时再跑：`scripts/soak/nightly_soak_runner.ps1` / `scripts/soak/test_loop.ps1`
- 压测结果与趋势文件统一放在 `results/`，避免污染根目录

