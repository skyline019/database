# StructDB 最高性能记录（独立文档）

本文档 **仅** 汇总 StructDB 在代表性环境下的 **插入** 与 **查询** 峰值数据、复现命令与基线路径。竞品对比见 [`COMPETITIVE_MATRIX.md`](COMPETITIVE_MATRIX.md) §7.3；性能路线图见 [`OPTIMIZATION_PLAN.md`](OPTIMIZATION_PLAN.md)。

**数据截至**：2026-05-17 · **C API / GUI**：1.9.0 · **平台**：Windows 10+，`RelWithDebInfo` / `Release`

---

## 1. 一页摘要

| 类别 | 场景 | 峰值指标 | 脚本 / 入口 |
|------|------|----------|-------------|
| **插入** | 纯 bulk（引擎批量） | **~328 000 行/秒**，~3.05 s / 100 万行 | `mega_data_mdb_stress.ps1` + `-EngineBulkImport` |
| **插入** | 一体加载（IMPORT + 索引 + 后续查询） | **~3848 TPS** 峰值；典型 **3370–3410** | `mdb_query_complex_stress.ps1` |
| **插入** | 多进程并行（参考下限） | **~238 000 TPS** | `concurrent_pressure_bench.ps1` |
| **插入** | 单行 OLTP（persist 默认） | insert **~3000 TPS**，P99 **~0.63 ms** | `oltp_persist_micro.ps1` |
| **查询** | 主键 / 索引点查（warm P95） | **0.003–0.004 ms** | `mdb_query_complex` / `--query-bench` |
| **查询** | `SUM` / `QBAL`（聚合缓存） | **0.002–0.003 ms** | 同上 |
| **查询** | `SCAN INDEX(ik,5000,STATS)`（门禁） | **3.174 ms** | 同上 |
| **查询** | `SCAN INDEX(ik,STATS)` 全量（soak） | **~1144 ms** | 同上，不作 PR 门禁 |

**勿混比**：上表各行 **协议、是否建索引、是否 warm、是否 `EngineBulkImport`** 不同，不可直接排名。

---

## 2. 测量约定

| 项 | 约定 |
|----|------|
| 构建 | `cmake --build build --config RelWithDebInfo --target structdb_app`（或 `Release`） |
| 数据规模 | **1 000 000** 行（除非 OLTP micro 注明 1000 行） |
| 分析表 | `qcx`：`DEFATTR(id:int,dept:int,val:int,k:string)`，`dept=id%100`，`k=t{id%50}`，`CREATE INDEX ik ON qcx(k)` |
| 查询指标 | 单会话 **warm**，`--bench-profile all`，`bench_warmup=1`，`bench_iters=5`，报告 **`ms_p95`** |
| 归档 | `scripts/results/*_summary_*.json`；门禁基线副本在 `benchmarks/baselines/` |

---

## 3. 插入最高性能

### 3.1 路径 A — 纯 bulk（全局最高 TPS）

| 项 | 值 |
|----|-----|
| **TPS** | **327 868.9**（`runtime_pressure_tps_est`） |
| **墙钟** | **3050 ms** / 1M 行 |
| **配置** | `-Rows 1000000 -RowsPerLine 1000` **`-EngineBulkImport`** |
| **摘要** | `scripts/results/mega_data_summary_20260516_103927_414.json` |

```powershell
.\scripts\bench\mega_data_mdb_stress.ps1 -BuildDir build -Rows 1000000 -RowsPerLine 1000 -EngineBulkImport
```

**说明**：走 PHASE39/40 专线路径（plain/raw、分块 persist、导入 skip undo 等）；**无** SQL、**无** 与复杂查询脚本相同的索引维护节奏。

### 3.2 路径 B — 一体加载（真实「导入 + 索引 + 压测」）

| 项 | 值 |
|----|-----|
| **峰值 TPS** | **3848**（`load_wall_ms` ≈ 259 878） |
| **典型 TPS** | **3370–3410**（多次 `mdb_query_complex` 运行） |
| **协议** | `IMPORT MODE ON` → `BULKINSERTFAST`（**800 行/条**）→ `FLUSH` |
| **`EngineBulkImport`** | **关**（默认） |
| **峰值摘要** | `mdb_query_complex_summary_20260516_173946_672.json` |
| **查询最优轮加载** | **2848 TPS**（同脚本含完整查询阶段，`024113_810`） |

```powershell
.\scripts\bench\mdb_query_complex_stress.ps1 -BuildDir build -Rows 1000000
```

百万行自动进度输出；可选 `-EchoProgress`、加载时 `structdb_app --mdb-stream-log`。

### 3.3 路径 C — 多进程并行

| 项 | 值 |
|----|-----|
| **参考 TPS** | **~238 000**（默认脚本参数下的门禁下限） |
| **口径** | 每 Job 独立 `_data`；TPS ≈ 总行数 / **最慢 Job** 墙钟 |

```powershell
.\scripts\bench\concurrent_pressure_bench.ps1 -BuildDir build -Jobs 4 -RuntimePressureBatches 500 -RuntimePressureBatchSize 2000
```

### 3.4 单行 OLTP（非 bulk）

| 指标 | 值 |
|------|-----|
| insert TPS | **3001.5** |
| insert P99 | **0.634 ms** |
| update TPS | **2746.9** |
| update P99 | **0.653 ms** |
| 行数 | 1000（`oltp_persist_micro_v1`） |

```powershell
.\scripts\bench\oltp_persist_micro.ps1 -BuildDir build
```

基线：[`benchmarks/baselines/oltp_persist_baseline.json`](../benchmarks/baselines/oltp_persist_baseline.json)。

---

## 4. 查询最高性能（warm P95，100 万行）

**门禁基线**：[`benchmarks/baselines/mdb_query_complex_baseline.json`](../benchmarks/baselines/mdb_query_complex_baseline.json)  
**来源摘要**：`scripts/results/mdb_query_complex_summary_20260517_024113_810.json`

### 4.1 全用例表（25 项）

| 用例名 | MDB（`structdb_app --query-bench`） | ms_p95 |
|--------|-------------------------------------|--------|
| `count` | `COUNT(*)` | **0.002** |
| `explain_where_hit` | `EXPLAIN WHERE`（id 命中） | **0.004** |
| `where_hit` | `WHERE id=…` 命中 | **0.003** |
| `where_miss` | `WHERE id=…` 未命中 | **0.003** |
| `page_json_first` | `PAGE_JSON` 首页 | **0.023** |
| `page_json_mid` | `PAGE_JSON` 中间页 | **0.022** |
| `page_json_last` | `PAGE_JSON` 末页 | **0.024** |
| `page_json_after_mid` | `PAGE_JSON(AFTER,…)` | **0.024** |
| `page_json_cols_id` | `PAGE_JSON(…,COLS,id)` | **0.014** |
| `page_json_after_stream` | `PAGE_JSON(…,AFTER,…,STREAM)` | **0.037** |
| `page_json_ids_only` | `PAGE_JSON(…,IDS_ONLY)` | **0.005** |
| `group_by_dept_count` | `GROUP BY (dept) COUNT` | **0.148** |
| `group_by_dept_sum` | `GROUP BY (dept) SUM(val)` | **0.151** |
| `sum_val` | `SUM(val)` | **0.002** |
| `qbal_val` | `QBAL(val,0)` | **0.003** |
| `explain_dept_hit` | `EXPLAIN WHERE`（dept） | **0.817** |
| `where_dept_hit` | `WHERE dept=…` | **0.636** |
| `explain_k_hit` | `EXPLAIN WHERE`（k / 索引） | **0.005** |
| `where_k_hit` | `WHERE k=…` | **0.004** |
| `page_json_sort_val_desc` | `PAGE_JSON(…,val,desc)` | **0.078** |
| `page_json_sort_dept_asc` | `PAGE_JSON(…,dept,asc)` | **0.056** |
| `page_json_mid_val_desc` | 中间页 + val 降序 | **0.024** |
| `scan_index_ik` | `SCAN INDEX(ik,5000,STATS)` | **3.174** |
| `scan_index_ik_ids` | `SCAN INDEX(ik,5000,IDS)` | **7.168** |
| `scan_index_ik_stats_full` | `SCAN INDEX(ik,STATS)` 全量 | **1144.33** |

### 4.2 分级说明

| 级别 | 用例 | 用途 |
|------|------|------|
| **亚毫秒** | 点查、`SUM`/`QBAL`、`PAGE_JSON` 多数变体 | PR 门禁（`--max-p95-ratio 1.25`） |
| **毫秒** | `GROUP BY`、`SCAN INDEX` capped | PR 门禁 |
| **秒级** | `scan_index_ik_stats_full`、非索引 `WHERE dept` | soak / 能力展示，默认 `--ignore-queries` |

### 4.3 相对 2026-05-16 的里程碑

| 用例 | 优化前 P95（约） | 当前 P95 | 手段 |
|------|------------------|----------|------|
| `qbal_val` | **75–85 ms** | **0.003 ms** | QBAL 整数 `>=` + `agg_cache`（`logical_agg_try_qbal_int_ge`） |
| `scan_index_ik`（门禁语义） | **~1100 ms**（全表 STATS） | **3.174 ms** | 改为 `SCAN INDEX(ik,5000,STATS)`；全量单独用例 |

### 4.4 实现要点（查询）

1. **聚合缓存**：列 `min` / `valid_rows`；`SUM`/`QBAL` 在可判定条件下 O(1)。
2. **SCAN INDEX**：`STATS` / `IDS` / 行发射分离；门禁 **禁止** 用全表 STATS 作回归对比。
3. **PAGE_JSON**：有序 `row_ids_ordered` 切片、`AFTER` 键集分页、`IDS_ONLY` / `STREAM`。

---

## 5. 复现与基线维护

### 5.1 一键压测（查询 + 加载）

```powershell
cmake --build build --config RelWithDebInfo --target structdb_app
.\scripts\bench\mdb_query_complex_stress.ps1 -BuildDir build -Rows 1000000
```

结果：`scripts/results/mdb_query_complex_summary_<时间戳>.json`

### 5.2 门禁对比

```powershell
python benchmarks/scripts/compare_mdb_query_summary.py `
  --baseline benchmarks/baselines/mdb_query_complex_baseline.json `
  --current scripts/results/mdb_query_complex_summary_<最新>.json `
  --max-p95-ratio 1.25 `
  --ignore-queries scan_index_ik_stats_full
```

### 5.3 提升查询基线

```powershell
python benchmarks/scripts/promote_mdb_query_baseline.py `
  --from scripts/results/mdb_query_complex_summary_<确认为最优>.json
```

### 5.4 基线与峰值 JSON

| 文件 | 用途 |
|------|------|
| [`benchmarks/baselines/mdb_query_complex_baseline.json`](../benchmarks/baselines/mdb_query_complex_baseline.json) | 复杂查询 warm **P95** 门禁（25 用例） |
| [`benchmarks/baselines/mdb_bulk_insert_peak.json`](../benchmarks/baselines/mdb_bulk_insert_peak.json) | 插入峰值 **参考**（非自动门禁） |
| [`benchmarks/baselines/mega_data_baseline.json`](../benchmarks/baselines/mega_data_baseline.json) | `mega_data` TPS **下限** |
| [`benchmarks/baselines/oltp_persist_baseline.json`](../benchmarks/baselines/oltp_persist_baseline.json) | 单行 DML P99 门禁 |

---

## 6. 宣称边界（必读）

1. **bulk 峰值**须标明：`BULKINSERTFAST` + 脚本摊销 +（可选）`-EngineBulkImport` / `--mdb-bulk-import`；导入批 **无 undo**，分块崩溃可能部分落盘（见 [`phases/PHASE40_PERSIST_PERF.md`](phases/PHASE40_PERSIST_PERF.md)）。
2. **查询峰值**须在 **warm 单会话**、**100 万行 qcx**、**`bench_profile=all`** 下复现；换机器允许 ±25% P95 波动，应更新基线而非直接对比绝对毫秒。
3. **不等于** PostgreSQL/MySQL 同机对标；见 [`COMPETITIVE_MATRIX.md`](COMPETITIVE_MATRIX.md)。

---

## 7. 相关文档

| 文档 | 内容 |
|------|------|
| [`scripts/bench/README.md`](../scripts/bench/README.md) | 压测脚本参数 |
| [`scripts/results/README.md`](../scripts/results/README.md) | 结果 JSON 命名与对比 |
| [`benchmarks/README.md`](../benchmarks/README.md) | 引擎微基准 `structdb_bench` |
| [`COMPETITIVE_MATRIX.md`](COMPETITIVE_MATRIX.md) | 竞品全维度 + §7.3 量级对照 |

---

## 修订记录

| 日期 | 摘要 |
|------|------|
| 2026-05-17 | 初版：独立最高性能文档；插入三路径 + OLTP；查询 25 用例全表；QBAL/SCAN 里程碑 |
