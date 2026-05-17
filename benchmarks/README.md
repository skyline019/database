# StructDB 微基准（`structdb_bench`）

源码：`memtable_bench.cpp`、`wal_bench.cpp`、`compaction_bench.cpp`。构建目标 **`structdb_bench`**（见根 `CMakeLists.txt` 的 `STRUCTDB_BUILD_BENCHMARKS`）。

## Perf 门禁（JSON 基线 + 对比脚本）

1. **Release** 构建 `structdb_bench`（MSVC 示例：`build\benchmarks\Release\structdb_bench.exe`）。
2. 可选：重跑并覆盖仓库内基线（须在 **代表性机器** 上执行，并在 `Docs/CHANGELOG.md` 说明）：

   ```text
   structdb_bench --benchmark_format=json --benchmark_out=benchmarks/baselines/structdb_bench_baseline.json --benchmark_out_format=json
   ```

3. 与基线对比（**`real_time`（ns/次）**；默认允许相对劣化 **1.5×**）：

   ```text
   python benchmarks/scripts/compare_bench.py --bench-exe <path/to/structdb_bench> --baseline benchmarks/baselines/structdb_bench_baseline.json --max-ratio 1.5
   ```

   或先导出当前 JSON 再对比：`--current run.json`。

4. **ctest**（需 `-DSTRUCTDB_ENABLE_PERF_GATE=ON`；MSVC 须带 **`-C Release`**）：

   ```text
   ctest --test-dir build -C Release -L perf
   ```

其他 CPU/负载环境下若偶发超阈，可临时放宽 `--max-ratio`，或在同一类机器上更新基线；详见 [`Docs/POLICY.md`](../Docs/POLICY.md) §6.2。

**脚本依赖**：Python **3.8+**（仅标准库）。

## 端到端高压（MDB / 多进程）

与上述微基准不同，仓库另有 **`structdb_app --run-mdb`** 脚本压测（超大批量、多进程并行），见 [`../scripts/bench/README.md`](../scripts/bench/README.md)。

**MDB 查询 / 插入峰值与基线**（100 万行、QBAL / SCAN INDEX 等）：[`MDB_E2E_PEAK_PERFORMANCE.md`](MDB_E2E_PEAK_PERFORMANCE.md)。

| 基线文件 | 用途 |
|----------|------|
| [`baselines/mdb_query_complex_baseline.json`](baselines/mdb_query_complex_baseline.json) | 复杂查询 warm **P95** 门禁（`compare_mdb_query_summary.py`） |
| [`baselines/mdb_bulk_insert_peak.json`](baselines/mdb_bulk_insert_peak.json) | 插入峰值参考（一体加载 / `EngineBulkImport`） |
| [`baselines/mega_data_baseline.json`](baselines/mega_data_baseline.json) | `mega_data` TPS 下限参考 |
