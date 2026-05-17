# Benchmark 结果归档（`scripts/results/`）

本目录存放 **可重复对比** 的压测 JSON 摘要；权威基线副本在 [`benchmarks/baselines/`](../../benchmarks/baselines/)。

## 命名约定

| 前缀 | 来源脚本 | 说明 |
|------|----------|------|
| `mega_data_summary_*.json` | [`scripts/bench/mega_data_mdb_stress.ps1`](../bench/mega_data_mdb_stress.ps1) | 大表 `BULKINSERTFAST` 吞吐 |
| `oltp_persist_summary_*.json` | [`scripts/bench/oltp_persist_micro.ps1`](../bench/oltp_persist_micro.ps1) | 单行 INSERT/UPDATE 延迟 |
| `persist_baseline_*.json` | [`scripts/run_persist_baseline.ps1`](../run_persist_baseline.ps1) | 汇总指针（bench + OLTP 路径） |
| `mdb_query_summary_*.json` | [`mdb_query_bench.ps1`](../bench/mdb_query_bench.ps1) | 标准查询压测 |
| `mdb_query_complex_summary_*.json` | [`mdb_query_complex_stress.ps1`](../bench/mdb_query_complex_stress.ps1) | 高压 + 复杂查询（`mdb_query_complex_v1`） |

时间戳格式：`yyyyMMdd_HHmmss` 或 `yyyyMMdd_HHmmss_fff`（UTC）。

## 标准 JSON 字段（mega_data）

每次运行应包含（便于 [`benchmarks/scripts/compare_mega_summary.py`](../../benchmarks/scripts/compare_mega_summary.py)）：

- `timestamp`（ISO-8601 UTC）
- `benchmark_profile`（如 `mega_data_mdb_bulk_v1`）
- `runtime_pressure_tps_est`
- `total_rows`、`rows_per_bulk_line`、`wall_ms`
- `import_mode`、`engine_bulk_import`
- `build_dir`、`build_config`（可选）
- `git_sha`（可选，仓库内运行时填充）

## 复现与稳定性（同一机器两次 mega_data 差异 &lt;5%）

```powershell
$BuildDir = "e:\db\StructDB\build"
& scripts\bench\mega_data_mdb_stress.ps1 -BuildDir $BuildDir -Rows 100000 -RowsPerLine 1000
& scripts\bench\mega_data_mdb_stress.ps1 -BuildDir $BuildDir -Rows 100000 -RowsPerLine 1000
python benchmarks\scripts\compare_mega_summary.py `
  --baseline (Get-ChildItem scripts\results\mega_data_summary_*.json | Sort-Object LastWriteTime -Descending | Select-Object -Skip 1 -First 1).FullName `
  --current  (Get-ChildItem scripts\results\mega_data_summary_*.json | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName `
  --max-tps-ratio-delta 0.05
```

Release 构建、关闭其它磁盘重压；对比 **同一** `-Rows` / `-RowsPerLine` / `-ImportMode` 组合。

## 复杂查询压测（mdb_query_complex）

- 规范/占位：[`benchmarks/baselines/mdb_query_complex_baseline.json`](../../benchmarks/baselines/mdb_query_complex_baseline.json)
- 峰值说明：[`benchmarks/MDB_E2E_PEAK_PERFORMANCE.md`](../../benchmarks/MDB_E2E_PEAK_PERFORMANCE.md)
- 插入峰值参考：[`benchmarks/baselines/mdb_bulk_insert_peak.json`](../../benchmarks/baselines/mdb_bulk_insert_peak.json)
- 生成：`scripts\bench\mdb_query_complex_stress.ps1`
- 对比：`python benchmarks\scripts\compare_mdb_query_summary.py --baseline ... --current ... --ignore-queries scan_index_ik_stats_full`
- 门禁建议：各查询 `ms_p95` 较基线 **≤1.25×**（`--max-p95-ratio 1.25`）；`scan_index_ik_stats_full` 为 soak，默认不参与比值

## OLTP 基线

- 规范文件：[`benchmarks/baselines/oltp_persist_baseline.json`](../../benchmarks/baselines/oltp_persist_baseline.json)
- 生成：`scripts\bench\oltp_persist_micro.ps1` 或 `scripts\run_persist_baseline.ps1`
- 门禁建议：合入后 `insert_p99_ms` / `update_p99_ms` 较基线 **≤1.2×**（同配置）

## 相关文档

- [`Docs/COMPETITIVE_MATRIX.md`](../../Docs/COMPETITIVE_MATRIX.md) §7
- [`Docs/COMPETITIVE_IMPROVEMENT_PLAN.md`](../../Docs/COMPETITIVE_IMPROVEMENT_PLAN.md)
