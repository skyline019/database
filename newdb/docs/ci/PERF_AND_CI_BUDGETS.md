# 性能预算与 CI 门禁（阶段 C）

## 1. 一体化验证脚本

- **[`scripts/ci/verify_clean_reconfigure.ps1`](../../scripts/ci/verify_clean_reconfigure.ps1)**：`cmake` 干净目录配置 → 构建 → **全量 `ctest`** → 可选语义门（GTest filter）→ GUI 门 → **Bench 门**。
- **Bench 门**：调用 **[`scripts/ci/ci_bench_gate.py`](../../scripts/ci/ci_bench_gate.py)**（除非 `-SkipBenchGate`）。

## 2. `ci_bench_gate.py` 默认步骤

1. **`newdb_smoke`**：`create` / `append` / `load` 最小 JSON 流水线（若已构建）。
2. **可选 `newdb_perf`**：`--run-newdb-perf` 时跑吞吐 JSON，可用 `--max-newdb-perf-elapsed-ms`、`--min-newdb-perf-txn-avg-tps` 等设预算。
3. **CTest 性能类子集**：`-R "CiMicrobench|DispatchRouting"`  
   - **`CiMicrobench.SortByNonIdColumnUnderBudget`**：4000 行内存表排序 **&lt; 2000 ms**（[`test_ci_microbench.cpp`](../../tests/test_ci_microbench.cpp)）。  
   - **`DispatchRouting.Phase2PrefixesSkipPhase1`**：phase-2 前缀路由回归（[`test_dispatch_routing.cpp`](../../tests/test_dispatch_routing.cpp)）。
4. **可选**：`--runtime-jsonl` + `newdb_runtime_report` 对 vacuum/WAL recovery/锁等统计做阈值门禁（参数见脚本 `--help`）。其中 **`where_query_count` / `where_query_rows_scanned_total` / `where_query_rows_returned_total`**、`lazy_materialize_*`、`heap_decode_slot_*`、`vacuum_priority_score`、**`vacuum_health_bonus_last`**（见 [`RUNTIME_STATS_SCHEMA.md`](../../scripts/validate/RUNTIME_STATS_SCHEMA.md)）用于观测 WHERE 扫描/返回、惰性物化、解码路径与 vacuum health bonus；可与 `where_plan_*`、`where_fallback_scans` 对照做查询退化分析（`OPTIMIZATION_PLAN_2026` 阶段 3–4）。**阶段 9 扩展**：同一路径下可选用 **`--max-table-storage-health-fragmentation-ratio`**、**`--max-table-storage-health-dead-bytes`**、**`--max-vacuum-health-bonus-last`**（由 `ci_bench_gate.py` 透传到 `newdb_runtime_report`；汇总 JSON 含 `table_storage_health_*_peak` 与 `vacuum_health_bonus_last_max`）。闭环说明见 [`STORAGE_GOVERNANCE_AND_RECOVERY_BUDGETS.md`](../storage/STORAGE_GOVERNANCE_AND_RECOVERY_BUDGETS.md) 第 4 节。**`NEWDB_QUERY_USE_TABLE_STATS=1`** 时 WHERE 规划可消费 `build_table_stats_from_heap` 的 NDV 提示（阶段 5）。**`NEWDB_WHERE_HEAP_SCAN_BUDGET_ROWS`** 见 [`BUILD.md`](BUILD.md)。
5. **`ci_bench_gate.py --release-grade`**：在未显式传入 lazy 物化阈值时，将 **`--max-lazy-materialize-count-delta`** 与 **`--max-lazy-materialize-rows-total-delta`** 收紧为 **0**（与 Release 硬门方向一致；仍可用显式参数覆盖）。**`verify_clean_reconfigure.ps1 -ReleaseGrade`** 会将 **`--release-grade`** 传给 bench 门；对 storage health / debt 的数值硬门仍需自行传入 **`--runtime-jsonl`** 与上述 **`--max-table-storage-health-*`** / **`--max-vacuum-health-bonus-last`**。
6. **可选**：`--pressure-summary-json` 对接并发压测摘要。

## 3. PR / Nightly / Release 门禁矩阵（收口约定）

| 门类型 | 建议内容 | 说明 |
|--------|----------|------|
| **PR** | 全量 **`ctest`**（或团队约定的子集）+ **`ci_bench_gate.py`** 默认步骤：`newdb_smoke`（若已构建）、**`CiMicrobench`**、**`DispatchRouting`** | 保持轻量；**不**默认附带需 JSONL 的 storage / WAL recovery 数值门（避免拖慢每台开发机）。 |
| **Nightly** | 在 PR 基础上：`powershell -File scripts/ci/verify_clean_reconfigure.ps1` **`-BenchGateStorage` `-BenchGateWalRecovery`**（或等价地对 `ci_bench_gate.py` 传入 **`--runtime-jsonl`** 指向契约样例，见 `scripts/ci/fixtures/README.md`） | Storage / WAL 门使用 **`newdb_runtime_report`** 汇总阈值；**`--max-wal-recovery-last-elapsed-ms`** 建议从 **`2000`**（毫秒）起按机器调参（与 `verify_clean_reconfigure` 中 WalRecovery 开关默认一致；仅约束 JSONL 窗口内 **`wal_recovery_last_elapsed_ms` 峰值**，非 PR 阻断）。**默认策略**：主干 PR **不**强制 **`BenchGateStorage`**；若要将 storage 数值门上升为 PR 阻断，应单独加 job（与 §3 **PR** 行「轻量」一致），而非修改全员默认 verify。 |
| **Release** | **`-ReleaseGrade`**：禁止 **`SkipBenchGate`**；可按需 **`--run-newdb-perf`** 与 TPS 下限；storage / health 硬门仍建议显式传 **`--runtime-jsonl`** + **`--max-table-storage-health-*`** / **`--max-compact-debt-bytes-peak`** | 与 [`BUILD.md`](BUILD.md) 中 verify 脚本说明一致。 |

**可选 Nightly 测试子集**：例如 **`StorageSoakLight.*`**（`ctest -R StorageSoakLight`，见 [`BUILD.md`](BUILD.md)「Nightly 子集」）；与长运行门禁脚本组合使用。

## 4. Baseline / Nightly JSONL 归档契约（v2）

用于阶段 0「基线可重复」：把采集到的 **`newdb.runtime_stats.v1`** JSONL、门禁阈值跑出的 **`newdb_runtime_report`** 日志与 **`ctest`** 配置绑定在同一清单里，便于流水线存档或人工对比。

1. **契约样板**：[`scripts/ci/fixtures/runtime_stats_bench_gate_minimal.jsonl`](../../scripts/ci/fixtures/runtime_stats_bench_gate_minimal.jsonl)（BenchGateStorage/WalRecovery 与字段校验）。
2. **校验**：`python scripts/validate/validate_runtime_stats.py <归档.jsonl>`。
3. **辅助脚本**：[capture_baseline.py](../../scripts/ci/capture_baseline.py)  
   - `--emit-archive-contract`：打印归档契约 JSON（路径提示、suggested 文件名）。  
   - `--write-archive-manifest PATH`：写入带 UTC 时间、`repo_root`、`build_dir`、`ctest_config` 的 manifest（CI artifact 可附带）。
4. **采集顺序建议**：全量/约定 **`ctest`** → 业务负载快照 JSONL（标签覆盖 insert/WHERE/vacuum/recover）→ `validate_runtime_stats.py` → （可选）`ci_bench_gate.py --runtime-jsonl …` + `newdb_runtime_report`。
5. **PR 侧 WAL 恢复耗时硬门**：默认 **不** 阻断 PR（与 §3 矩阵一致）；若团队要将 `--max-wal-recovery-last-elapsed-ms` 提前进 PR，建议先做 **软失败 / warning** 或缩小 **`ctest -R`** 子集，避免误杀不稳定 CI 主机。
6. **CI artifact（manifest）**：`capture_baseline.py --write-archive-manifest <path>` 推荐写入 **`newdb/scripts/results/ci_baseline_manifest.json`**（与契约中的 `suggested_manifest_relative_path` 一致）。GitHub Actions 上传 artifact 建议命名为 **`newdb-runtime-archive-manifest`**（见 **`newdb/.github/workflows/newdb-ci.yml`** 中 **`linux-bench-gate-runtime-contract`** job）；**保留天数**：仓库默认可用 **30–90** 天（按团队存储预算在 workflow `retention-days` 调整）。
7. **可选第二道门（PR 不默认）**：上述 workflow job 在干净构建后额外跑 **`ci_bench_gate.py`** + **`--runtime-jsonl scripts/ci/fixtures/runtime_stats_bench_gate_minimal.jsonl`** 与 **`verify_clean_reconfigure.ps1 -BenchGateStorage -BenchGateWalRecovery`** 等价的 storage / WAL recovery 宽松上限，与主干 **`linux-gcc`** 的轻量 bench 门互补。

## 5. 相关文档

- 存储与恢复预算：[STORAGE_GOVERNANCE_AND_RECOVERY_BUDGETS.md](STORAGE_GOVERNANCE_AND_RECOVERY_BUDGETS.md)
- 构建选项与 **MSVC 全量编译 / `ctest` / PDB 说明**：[BUILD.md](BUILD.md)（「MSVC / Visual Studio：全量编译与测试」）
- 运行时统计字段说明：[RUNTIME_STATS_SCHEMA.md](../../scripts/validate/RUNTIME_STATS_SCHEMA.md)
- 评估与阶段收口：[NEWDB_OPTIMIZATION_ASSESSMENT_AND_PLAN.md](NEWDB_OPTIMIZATION_ASSESSMENT_AND_PLAN.md) §6「阶段性收口定义（v1）」  
- **ReleaseGrade / fallback**：Release 矩阵跑 **`verify_clean_reconfigure.ps1 -ReleaseGrade`** 时，建议在 soak 或 bench 前按需收紧 **`NEWDB_WHERE_HEAP_SCAN_BUDGET_ROWS`**（见 [BUILD.md](BUILD.md)），与 **`ci_bench_gate --release-grade`** 收紧 lazy 物化、deltas 一致。
