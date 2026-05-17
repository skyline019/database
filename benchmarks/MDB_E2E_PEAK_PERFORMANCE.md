# MDB 端到端峰值性能记录

本文档记录 **StructDB `structdb_app` + MDB** 在代表性环境下的 **查询** 与 **插入** 最高性能（截至 2026-05-17）。数值来自 `scripts/results/` 归档 JSON；**查询门禁基线**见 [`baselines/mdb_query_complex_baseline.json`](baselines/mdb_query_complex_baseline.json)。

## 环境与构建

| 项 | 说明 |
|----|------|
| 构建 | `RelWithDebInfo` 或 `Release`，`structdb_app` |
| 平台 | Windows 10+，PowerShell 5.1+ |
| 数据规模 | **100 万行**，表 `qcx`：`id,dept,val,k` + `INDEX ik(k)` |
| 查询压测 | 单会话 **warm**，`--bench-profile all`，`bench_warmup=1`，`bench_iters=5`，指标取 **`ms_p95`** |

复现（查询 + 一体加载）：

```powershell
cmake --build build --config RelWithDebInfo --target structdb_app
.\scripts\bench\mdb_query_complex_stress.ps1 -BuildDir build -Rows 1000000
```

百万行时脚本自动 `-EchoProgress`；加载阶段可加 `--mdb-stream-log` 查看进度。

---

## 查询最高性能（warm，`profile=all`）

**权威基线来源**：`scripts/results/mdb_query_complex_summary_20260517_024113_810.json`（已提升为仓库基线）。

### 亚毫秒 / 毫秒级（PR 门禁关注）

| 用例 | MDB 语句（摘要） | ms_p95 | 说明 |
|------|------------------|--------|------|
| `count` | `COUNT(*)` | **0.002** | 元数据 O(1) |
| `where_hit` / `where_miss` | `WHERE id=…` | **0.003** | 主键点查 |
| `where_k_hit` | `WHERE k=…` | **0.004** | 索引点查 |
| `sum_val` | `SUM(val)` | **0.002** | 聚合缓存 |
| `qbal_val` | `QBAL(val,0)` | **0.003** | QBAL 整数 `>=` 快路径（`agg_cache`） |
| `page_json_*` | `PAGE_JSON` 各变体 | **0.005–0.037** | 含 AFTER / STREAM / COLS / IDS_ONLY |
| `group_by_dept_*` | `GROUP BY dept` | **0.15** 量级 | 百档分组 |
| `scan_index_ik` | `SCAN INDEX(ik,5000,STATS)` | **3.174** | **门禁用例**： capped 统计，勿用全表 STATS |
| `scan_index_ik_ids` | `SCAN INDEX(ik,5000,IDS)` | **7.168** | capped 仅 id |

### 秒级（soak / 全量，不作 PR 门禁）

| 用例 | MDB 语句 | ms_p95 | 说明 |
|------|----------|--------|------|
| `scan_index_ik_stats_full` | `SCAN INDEX(ik,STATS)` | **~1144** | 全索引遍历；波动大，对比时 `--ignore-queries` 跳过 |
| `where_dept_hit` | `WHERE dept=…` | **~0.64** | 非索引列过滤 |
| `explain_dept_hit` | `EXPLAIN … dept` | **~0.82** | 计划开销 |

### 关键优化（实现要点）

1. **QBAL / SUM**：`logical_agg_try_qbal_int_ge` + 列 `min` / `valid_rows` 元数据，满足 `threshold <= col_min` 时 O(1)。
2. **SCAN INDEX**：`STATS` / `IDS` / `FullRow` 发射模式；门禁使用 `5000,STATS`，避免旧版全列 5000 行或全表 STATS（~1.1 s）误判回归。
3. **PAGE_JSON**：有序键切片、游标 `AFTER`、列裁剪与 `IDS_ONLY`。

门禁对比：

```powershell
python benchmarks/scripts/compare_mdb_query_summary.py `
  --baseline benchmarks/baselines/mdb_query_complex_baseline.json `
  --current scripts/results/mdb_query_complex_summary_<最新>.json `
  --max-p95-ratio 1.25 `
  --ignore-queries scan_index_ik_stats_full
```

提升基线（换机器或确认新一轮为最优后）：

```powershell
python benchmarks/scripts/promote_mdb_query_baseline.py `
  --from scripts/results/mdb_query_complex_summary_20260517_024113_810.json
```

---

## 插入最高性能

端到端脚本压测中有 **三条常用路径**，峰值不同，勿混比。

### 路径 A：复杂查询脚本一体加载（推荐对照「真实导入」）

脚本：[`scripts/bench/mdb_query_complex_stress.ps1`](../scripts/bench/mdb_query_complex_stress.ps1)

| 项 | 数值 / 配置 |
|----|-------------|
| **峰值 TPS** | **~3848**（`load_tps_est`，`load_wall_ms≈260s`） |
| **典型良好** | **3370–3410**（同配置多次运行） |
| 协议 | `IMPORT MODE ON` → 循环 `BULKINSERTFAST(...)`（**800 行/条**）→ `FLUSH` |
| `EngineBulkImport` | **关闭**（默认）；与查询压测同库、同 schema |
| 来源摘要 | `mdb_query_complex_summary_20260516_173946_672.json`（加载最快一轮） |

```powershell
.\scripts\bench\mdb_query_complex_stress.ps1 -BuildDir build -Rows 1000000
# 可选对比引擎批量：-EngineBulkImport（会改变加载路径，TPS 口径不同）
```

一体加载占端到端时间绝大部分；查询阶段在 warm 会话下通常为 **数十秒** 量级。

### 路径 B：`mega_data` + 引擎批量（纯插入峰值）

脚本：[`scripts/bench/mega_data_mdb_stress.ps1`](../scripts/bench/mega_data_mdb_stress.ps1)

| 项 | 数值 / 配置 |
|----|-------------|
| **峰值 TPS** | **~327 869**（`runtime_pressure_tps_est`） |
| 墙钟 | **~3.05 s** / 100 万行 |
| 配置 | `-Rows 1000000 -RowsPerLine 1000` **+ `-EngineBulkImport`** |
| 来源摘要 | `mega_data_summary_20260516_103927_414.json` |

```powershell
.\scripts\bench\mega_data_mdb_stress.ps1 -BuildDir build -Rows 1000000 -RowsPerLine 1000 -EngineBulkImport
```

参考快照：[`baselines/mdb_bulk_insert_peak.json`](baselines/mdb_bulk_insert_peak.json)。

### 路径 C：多进程 `concurrent_pressure`（并行吞吐）

脚本：[`scripts/bench/concurrent_pressure_bench.ps1`](../scripts/bench/concurrent_pressure_bench.ps1)

| 项 | 说明 |
|----|------|
| 参考下限 | **~238 000** TPS（[`mega_data_baseline.json`](baselines/mega_data_baseline.json)，默认脚本参数） |
| 口径 | 多 Job 独立 `_data`；`runtime_pressure_tps_est` ≈ 总行数 / **最慢 Job** 墙钟 |

---

## 基线文件一览

| 文件 | 用途 |
|------|------|
| [`mdb_query_complex_baseline.json`](baselines/mdb_query_complex_baseline.json) | 复杂查询 **warm P95** 门禁（25 用例，`mdb_query_complex_v1`） |
| [`mdb_bulk_insert_peak.json`](baselines/mdb_bulk_insert_peak.json) | 插入峰值 **参考**（非自动门禁） |
| [`mega_data_baseline.json`](baselines/mega_data_baseline.json) | `mega_data` / 多进程 **TPS 下限** 参考 |

## 相关文档

- **全量竞品对比（上市库 / SQLite / MongoDB 等）**：[`Docs/COMPETITIVE_MATRIX.md`](../Docs/COMPETITIVE_MATRIX.md) §2.1、§7.3
- 脚本说明：[`scripts/bench/README.md`](../scripts/bench/README.md)
- 结果归档：[`scripts/results/README.md`](../scripts/results/README.md)
- 微基准（引擎 put/get）：[`benchmarks/README.md`](README.md)
