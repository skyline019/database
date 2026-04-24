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
  CI 轻量压测门禁。

## 2) 性能与压测脚本（scripts/bench）

- `bench/million_scale_bench.ps1`
- `bench/concurrent_million_scale_bench.ps1`
- `bench/concurrent_pressure_bench.ps1`
- `bench/query_bench.ps1`
- `bench/txn_write_bench.ps1`
- `bench/eq_sidecar_cache_bench.ps1`
- `bench/eq_sidecar_invalidation_bench.ps1`

相关输入模板：
- `query_bench.mdb`
- `txn_wal_vacuum_test.mdb`

## 3) 稳定性跑批（scripts/soak）

- `soak/test_loop.ps1`
- `soak/nightly_soak_runner.ps1`

## 4) C API / 遥测 / 汇总校验（scripts/validate）

- `validate/check_c_api_abi.py`
- `validate/c_api_expected_symbols.txt`
- `validate/validate_telemetry_event.py`
- `validate/validate_perf_summary.py`
- `PERF_METRICS_SCHEMA.md`

## 5) 文档与运维说明

- `WRITE_PATH_TUNING_RUNBOOK.md`
- `PROD_REMEDIATION_PLAN.md`

## 6) 结果目录

- `results/` 为脚本生成产物目录（json/csv/jsonl），详见 `results/README.md`。

---

## 使用建议

- 默认先跑：`scripts/ci/verify_clean_reconfigure.ps1`
- 需要趋势/耐久验证时再跑：`scripts/soak/nightly_soak_runner.ps1` / `scripts/soak/test_loop.ps1`
- 压测结果与趋势文件统一放在 `results/`，避免污染根目录

