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

## `mdb_query_bench.ps1`

在已加载（或脚本内先加载）的表上测量 **COUNT / EXPLAIN WHERE / WHERE / PAGE_JSON**（含 `AFTER` 游标与 `COLS` 列裁剪），单会话计时（`structdb_app --query-bench`）。

- `PAGE_JSON(page_no,page_size,sort_col,asc|desc)` — 页号分页（`id` 排序走有序键切片）
- `PAGE_JSON(AFTER,cursor,page_size,sort_col,asc|desc)` — **键集游标**，返回 `next_after` / `has_more`（深分页推荐）
- `PAGE_JSON(...,COLS,id,a)` — **列裁剪** + `compact` JSON（省略 `columns` 元数据）
- `PAGE_JSON(...,STREAM)` — **流式**：`[PAGE_JSON_META]` + `[PAGE_JSON_ROW]`×N + `[PAGE_JSON_END]`（会话 `row_ptr` 缓存）
- `PAGE_JSON(...,IDS_ONLY)` — 仅 `ids` 数组（无 headers/rows）；可组合 `AFTER` / `STREAM`
- 会话内 **稠密序数缓存**（`row_ids_ordered` 对齐时 O(1) 取行，无 `map::find`）

**宽表对比**（`-VariedSchema`：每行唯一 `tag`/`payload` + `REBUILD INDEX`）：

```powershell
powershell -ExecutionPolicy Bypass -File scripts/bench/mdb_query_bench.ps1 -BuildDir build -LoadRows 50000 -VariedSchema -EngineBulkImport
```

加载压测：`mega_data_varied_mdb_stress.ps1` → `scripts/results/mega_data_varied_summary_*.json`。

```powershell
# 10 万行：加载 + 查询（默认 5 次 bench + 1 次 warmup）
powershell -ExecutionPolicy Bypass -File scripts/bench/mdb_query_bench.ps1 -BuildDir build -LoadRows 100000

# 复用已有 data_dir（跳过加载）
powershell -ExecutionPolicy Bypass -File scripts/bench/mdb_query_bench.ps1 -BuildDir build -SkipLoad `
  -DataDir "E:\path\to\_data" -LoadRows 1000000

# 冷启动后再测一轮查询（新进程、重载表）
powershell -ExecutionPolicy Bypass -File scripts/bench/mdb_query_bench.ps1 -BuildDir build -LoadRows 100000 -ColdRestart
```

结果：`scripts/results/mdb_query_summary_*.json`（含各查询 `ms_avg` / `ms_p50` / `ms_p95`）。

`-BenchProfile all` 或 `analytics` 时追加 **GROUP BY / 非 id 排序 PAGE_JSON / SCAN INDEX / SUM / QBAL** 等（需 analytics 表结构，见下节）。

## `mdb_query_complex_stress.ps1`（高压 + 复杂查询）

**一站式**：大表加载 + 命名索引 + **标准 + 分析** 查询压测（`--bench-profile all`）。

| 阶段 | 内容 |
|------|------|
| 加载 | `DEFATTR(id:int,dept:int,val:int,k:string)`，`dept=id%100`，`k=t{id%50}`，`BULKINSERTFAST` |
| 索引 | `CREATE INDEX ik ON <table>(k)` |
| 查询 | `COUNT`、`WHERE`/`EXPLAIN`（id/k/dept）、`PAGE_JSON`（id/val/dept 排序、AFTER/STREAM）、`GROUP BY (dept) COUNT/SUM(val)`、`SCAN INDEX(ik,5000,STATS)`（门禁）、`SCAN INDEX(ik,STATS)`（soak）、`SUM(val)`、`QBAL(val,0)` |

```powershell
# 默认 20 万行（环境变量 STRUCTDB_QUERY_COMPLEX_ROWS 可覆盖）
powershell -ExecutionPolicy Bypass -File scripts/bench/mdb_query_complex_stress.ps1 -BuildDir build

# 百万行 + 冷启动后再测查询（耗时长）
$env:STRUCTDB_QUERY_COMPLEX_ROWS = "1000000"
powershell -ExecutionPolicy Bypass -File scripts/bench/mdb_query_complex_stress.ps1 `
  -BuildDir build -Rows 1000000 -ColdRestart -EngineBulkImport
```

结果：`scripts/results/mdb_query_complex_summary_*.json`。

**基线归档**（首次或换机器后执行一次）：

```powershell
python benchmarks/scripts/promote_mdb_query_baseline.py `
  --from (Get-ChildItem scripts/results/mdb_query_complex_summary_*.json | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName
```

**门禁对比**（相对已归档基线，默认 P95 ≤ **1.25×**；全表 `SCAN INDEX(ik,STATS)` 为 soak，默认忽略）：

```powershell
python benchmarks/scripts/compare_mdb_query_summary.py `
  --baseline benchmarks/baselines/mdb_query_complex_baseline.json `
  --current (Get-ChildItem scripts/results/mdb_query_complex_summary_*.json | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName `
  --max-p95-ratio 1.25 `
  --ignore-queries scan_index_ik_stats_full
```

峰值性能与插入路径说明：[`Docs/PEAK_PERFORMANCE.md`](../../Docs/PEAK_PERFORMANCE.md)。

`structdb_app` 参数：`--bench-profile standard|analytics|all`（与 `--query-bench` 联用）。

## 与 `structdb_bench` 的关系

- **引擎微基准**（put/get/visit_prefix 等）：见仓库 `benchmarks/README.md` 与目标 `structdb_bench`。
- **本目录**：偏 **端到端 MDB + 存储** 与 **多进程** 吞吐，适合夜间或发布前手工/CI 调度（勿默认绑在轻量 `ctest` 上）。
