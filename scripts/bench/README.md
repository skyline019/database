# StructDB 脚本级高压测试

与 Google Benchmark（`structdb_bench`）互补：本目录脚本通过 **`structdb_app --run-mdb`** 驱动真实 MDB 管线（`BULKINSERTFAST`、多进程并行等），适合 **超大数据量** 与 **多进程吞吐** 场景。

## 前置

- 已 CMake 构建 `structdb_app`（例如 `build\src\app\Release\structdb_app.exe`）。
- Windows PowerShell 5.1+。

## `concurrent_pressure_bench.ps1`

与先前 GUI 传入脚本的参数命名一致（`-BuildDir`、`-Jobs`、`-RuntimePressureBatches`、`-RuntimePressureBatchSize` 等）；请在仓库根用 PowerShell 直接调用，不再由 Tauri 封装。

- **`Jobs == 1`**：单库、连续多批 `BULKINSERTFAST`，统计批耗时 **P95**，`runtime_pressure_tps_est` ≈ 总行数 / 各批毫秒之和。
- **`Jobs > 1`**：每 Job **独立 `_data` 目录** 并行跑一份完整加载脚本（避免多进程写同一 KV），`runtime_pressure_tps_est` ≈ 总行数 / **最慢 Job** 墙钟秒；P95 为各 Job 耗时的分位数。

示例（仓库根目录，Release）：

```powershell
powershell -ExecutionPolicy Bypass -File scripts/bench/concurrent_pressure_bench.ps1 `
  -BuildDir build -Jobs 8 -RuntimePressureBatches 48 -RuntimePressureBatchSize 600 -RuntimeEchoProgress
```

结果 JSON：`scripts/results/concurrent_pressure_summary_*.json`。`-RepeatUntilFail N`：失败时最多尝试 N 次（每次新建临时目录），用于偶发 IO/锁抖动下的 soak。

**百万级（约 100 万行，多进程）**：总行数 ≈ `RuntimePressureBatches * RuntimePressureBatchSize`（并行时按 Job 均分）。示例：

```powershell
powershell -ExecutionPolicy Bypass -File scripts/bench/concurrent_pressure_bench.ps1 `
  -BuildDir build -Jobs 4 -RuntimePressureBatches 500 -RuntimePressureBatchSize 2000
```

## `mega_data_mdb_stress.ps1`

单进程、单表、**超大** `BULKINSERTFAST` 分段插入。

- **`-Rows`**：总行数；省略则读环境变量 **`STRUCTDB_MEGA_ROWS`**，默认 **120000**。
- **`-RowsPerLine`**：每条 `BULKINSERTFAST` 内行数（默认 800）。

示例（**100 万行**，单进程、注意时间与磁盘）：

```powershell
powershell -ExecutionPolicy Bypass -File scripts/bench/mega_data_mdb_stress.ps1 -BuildDir build -Rows 1000000 -RowsPerLine 1000
```

示例（50 万行，环境变量方式）：

```powershell
$env:STRUCTDB_MEGA_ROWS = "500000"
powershell -ExecutionPolicy Bypass -File scripts/bench/mega_data_mdb_stress.ps1 -BuildDir build -RowsPerLine 1000
```

结果：`scripts/results/mega_data_summary_*.json`。

## 与 `structdb_bench` 的关系

- **引擎微基准**（put/get/visit_prefix 等）：见仓库 `benchmarks/README.md` 与目标 `structdb_bench`。
- **本目录**：偏 **端到端 MDB + 存储** 与 **多进程** 吞吐，适合夜间或发布前手工/CI 调度（勿默认绑在轻量 `ctest` 上）。
