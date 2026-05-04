# newdb 当前评估与修改优化计划

本文基于当前 `newdb` 项目结构、`CMakeLists.txt`、核心头文件和现有优化文档，对数据库当前状态、主要短板和后续修改优化路线进行整理。

**修订（2026-05-04 补充）**：`ci_bench_gate.py` 对 **smoke / ctest / newdb_perf / pressure / `newdb_runtime_report`** 等失败路径均可写 **`--gate-fail-json-out`**（含 `stage` / `profile` / build 解析路径）；`capture_baseline.py` manifest 含 **`bench_gate_profile`** 与 **`recommended_ci_bench_gate_cli`**（与 **`linux-bench-gate-runtime-contract`** 同构）。详见 [`NEWDB_FILE_LEVEL_MODIFICATION_PLAN.md`](NEWDB_FILE_LEVEL_MODIFICATION_PLAN.md) §1 / §11.1。

**修订（2026-05）**：下列闭环已有部分代码与门禁落地。**状态约定**：🟢 表示该阶段目标中已对齐主干的切片；🟡 表示部分完成或仍有关键缺口。细分说明见 §1.3 与各阶段「状态」小节。关键锚点：`engine/src/wal/writer/wal_manager.cpp`（checkpoint 默认剪枝）、`tests/test_wal_recovery_indexed.cpp`（checkpoint fault + **多 segment / 尾缀不完整 checkpoint** + **未提交 insert / 不完整 checkpoint** 变体）、`cli/modules/txn/coordinator/write_conflict/lock_key.h`、`docs/txn/TXN_ISOLATION_AND_LOCKING.md`（§1.2 读路径 snapshot 审计表；**§1.1 `;bld=`** building/ready 语义）、`tests/test_txn_isolation_visibility.cpp`（**RC 刷新 vs Snapshot 钉扎**、**多线程 barrier smoke**、**`MultithreadLocalTablesHighLoadStressBounded`**）、`tests/test_txn_write_conflict.cpp`（**`AlternatingCoordinatorsSameWorkspaceBoundedStress`**）、`cli/modules/txn/coordinator/vacuum/vacuum_service.cc`（`compute_compact_debt_enqueue_metrics` 与入队键同源；**真空成功后**回写 **`table_storage_health_last_vacuum_*`**；可选 **`NEWDB_VACUUM_SCORE_WAL_SINCE=1`**）、`cli/modules/txn/coordinator/recovery/recovery_service.cc`（runtime **`wal_recovery_redo_ms` / `wal_recovery_checkpoint_begin_count` / `wal_recovery_checkpoint_end_count`**）、`docs/STORAGE_GOVERNANCE_AND_RECOVERY_BUDGETS.md`（**选项 B 默认**不入 score 的 `wal_since` 项、cooldown 观测）、`TxnRuntimeStats` 的 `compact_debt_*`、**`page_cache_*`** 与 **`memory_budget_*`** / **`memory_budget_reject_count`**、`engine/src/cache/page_cache.cpp` + **`NEWDB_PAGE_CACHE_MAX_BYTES`**、`cli/modules/where/executor/plan/plan_impl.cc`（`NEWDB_QUERY_COST_MODEL`、**OR/范围/Ne/Contains** 估计；**有界 **`plan_candidates_considered`** 与 `query_with_index` 对齐**）、`query_handler.cc`（**`EXPLAIN WHERE`** + **`SHOW PLAN` JSON** + **`plan_id`**）、`table_stats.{h,cc}`、`cli/modules/sidecar/common/index_catalog.{h,cc}`（**`;bld=`**、**`tests/test_index_catalog.BuildStateBuildingVsReadyParsed`**）、**GitHub `linux-index-catalog-enforce`** 与 **`linux-bench-gate-runtime-contract`**（见 **`BUILD.md`** / **`.github/workflows/newdb-ci.yml`**）、`equality_index_sidecar.cc` / `eq_bloom.cc`（**tmp→rename**；**`where_eq_sidecar_disk_*`** 磁盘读观测）、`tests/test_storage_soak.cpp` / `tests/test_page_cache.cpp`、`tools/report/newdb_runtime_report.cpp`、`scripts/ci/ci_bench_gate.py`、`scripts/ci/verify_clean_reconfigure.ps1`、`scripts/ci/capture_baseline.py`（**`--validate-runtime-fixture`**、**`--emit-archive-contract` / `--write-archive-manifest`**）、`scripts/ci/nightly_soak_hints.py`（**`--json` → `threshold_hints`**）、[`PERF_AND_CI_BUDGETS.md`](../ci/PERF_AND_CI_BUDGETS.md) **§3 / §4**、**`scripts/validate/validate_runtime_stats.py`** / **`RUNTIME_STATS_SCHEMA.md`**（含 v2 方向说明）、`docs/BUILD.md`。

---

## 1. 当前数据库状态判断

### 1.1 已具备的能力

#### 存储层

- 页式 heap 存储。
- lazy heap decode。
- 可选全局 LRU **PageCache**（`NEWDB_PAGE_CACHE_MAX_BYTES`，与惰性堆 `read_page_copy` 路径配合）。
- heap read view。
- row metadata / MVCC visibility 入口。
- 基础 heap compact / vacuum。

#### WAL 与恢复

- `WalManager` 已支持多类 WAL op：`INSERT`、`UPDATE`、`DELETE`、`COMMIT`、`ROLLBACK`、`CHECKPOINT_BEGIN/END`、savepoint、partial abort、PITR marker 等。
- 恢复路径**默认**将 `replay_start_lsn` 抬升至最近**完整** checkpoint（`NEWDB_RECOVER_USE_CHECKPOINT_LSN=0` 可关闭）；`NEWDB_RECOVER_ENABLE_OFFSET_SEEK` 仍为可选加速。
- `WalRecoveryStats` 已有恢复可观测字段，如：
  - `records_read`
  - `checksum_failures`
  - `decode_failures`
  - `last_complete_checkpoint_lsn`
  - `replay_start_lsn`
  - `checkpoint_scan_ms`
  - `redo_plan_ms`
  - `redo_apply_ms`

#### 事务与隔离

- 有 `TxnCoordinator`。
- 有 `TxnIsolationLevel::ReadCommitted` / `Snapshot`。
- CLI 查询读路径已接 `active_snapshot`。
- 写冲突有进程内 write intent。
- 有等待、超时、死锁 victim 统计。

#### 查询与索引

- WHERE 执行器已有 sidecar、equality index、covering index、visibility sidecar、table stats 等雏形。
- 支持可选表统计持久化（含 schema **`fp=`** 与内存侧 **`stats_schema_fp` / `table_stats_matches_schema`**）。
- **`EXPLAIN WHERE`** 可输出 **`plan_id`**；**`SHOW PLAN`** 输出与 **`EXPLAIN WHERE`** 同语法的**单行 JSON**（`plan_id`、**`estimated_scan_rows`**、**`plan_candidates_considered`**（**有界**统计，与 `query_with_index` 各路径 `QueryTraceGuard` 对齐，**非**完整计划枚举）、`logical_rows`、`matched_rows` 等）；可选 **`NEWDB_QUERY_COST_MODEL`** 与表统计提示配合做轻量路径估计（含 AND 链种子及 **OR 估算链路上对 NDV 的复用**）。
- 有运行时统计字段用于观测 fallback scan、扫描行数、返回行数等。

#### 可观测性与 CI

- 已有 `newdb.runtime_stats.v1`。
- 有 `validate_runtime_stats.py`（契约含 **`compact_debt_*`**、**`page_cache_*`**、**`memory_budget_*`** 等字段）。
- 有 `ci_bench_gate.py`（可透传 **`--max-compact-debt-bytes-peak`**、**`--max-table-storage-health-*`**、**`--max-wal-recovery-last-elapsed-ms`** 等；**`--release-grade`** 时默认收紧 **lazy 物化 delta** 与 **lock_deadlock_victim delta**）。
- 有 `newdb_runtime_report` 对 JSONL 的汇总门禁（含 **`compact_debt_bytes_peak`**）。
- 有 `scripts/ci/capture_baseline.py`（基线：ctest + 可选 bench gate；**`--validate-runtime-fixture`**；**`--emit-archive-contract` / `--write-archive-manifest`** 与 [`PERF_AND_CI_BUDGETS.md`](../ci/PERF_AND_CI_BUDGETS.md) **§4** 归档契约）、`scripts/ci/verify_clean_reconfigure.ps1`（可选 **`-BenchGateStorage` / `-BenchGateWalRecovery`**）、`scripts/ci/nightly_soak_hints.py`（**`--json`** 输出 **`threshold_hints`** 等）、GitHub **`linux-bench-gate-runtime-contract`**（manifest artifact + 契约 JSONL 与 storage/WAL recovery 数值门，**不**改本地默认 verify）。
- 有 ReleaseGrade 方向的门禁。
- 文档体系较完整。

### 1.2 当前主要短板

| 领域 | 当前问题 | 风险 |
|---|---|---|
| 事务隔离 | 目前是有限 MVCC 快照语义，不是完整 InnoDB RR/RC；**已增** **交错提交**、**多线程 barrier smoke**、**`MultithreadLocalTablesHighLoadStressBounded`**、**`AlternatingCoordinatorsSameWorkspaceBoundedStress`**（单 workspace、有界步数、交替 disjoint 写键） | 生产级**真并发**多线程 `TxnCoordinator` + 同表 `begin` 矩阵（受 OS 文件锁语义影响）、GUI 直连路径与完整 RR/RC 语义下边界仍易误解 |
| 锁系统 | 已有结构化 **`LockKey`**（存储键仍为兼容的 `table#id`）；**仍缺** 范围锁、谓词锁、二级索引级冲突抽象 | 复杂并发模型下扩展性仍有限 |
| WAL 恢复 | 已默认从最近完整 checkpoint 起剪枝重放；**已增** `test_wal_recovery_indexed` 中孤立 END、双 BEGIN 无 END、`NEWDB_RECOVER_MIN_LSN`、**多 WAL segment + 尾缀不完整 checkpoint**、**未提交 insert + 不完整 checkpoint** 等 fault 变体；🟢 **Nightly** 恢复耗时门（**`--max-wal-recovery-last-elapsed-ms`**）与 [`PERF_AND_CI_BUDGETS.md`](../ci/PERF_AND_CI_BUDGETS.md) 对齐；🟢 **PR 第二条 opt-in 门**：**`linux-bench-gate-runtime-contract`** 对契约 JSONL 跑 **`--max-wal-recovery-last-elapsed-ms`**（**不**改全员默认 verify）；runtime 已增 **`wal_recovery_redo_ms` / `wal_recovery_checkpoint_begin_count` / `wal_recovery_checkpoint_end_count`**（CLI `recoverFromWAL` 路径）；**仍缺** planner/applier 类拆分、**PR 默认**全员恢复硬门 | 大 WAL 下恢复时间仍依赖测试覆盖与 offset seek 等组合 |
| 存储治理 | runtime **`compact_debt_*`** 与 **vacuum 入队键同源**；**真空成功后** runtime **`table_storage_health_last_vacuum_*`** 由 **`WalManager::current_lsn()`** 等与耗时刷新（见 **`vacuum_service`**）；**选项 B 默认**仍 **不** 将 `wal_since` 叠入 score，可选 **`NEWDB_VACUUM_SCORE_WAL_SINCE=1`**；**已有** `StorageSoakLight`、`nightly_soak_hints`；**cooldown** 以 **`vacuum_cooldown_skip_count`** 观测；Nightly 默认阈值仍靠环境调参 | 长期 soak 与 CI 全矩阵仍可加强 |
| 查询优化 | 已有 NDV/table stats；**已增** `NEWDB_QUERY_COST_MODEL=1` 下 AND 链、**OR 估算链路**及对 **范围 / `Ne` / `Contains`** 的统计启发；**`SHOW PLAN` JSON 含 `estimated_scan_rows`、`plan_candidates_considered`（有界真值）**（与 `query_with_index` / policy 估计对齐），与 **`plan_id`** 一致；[`OPTIMIZATION_PLAN_2026.md`](OPTIMIZATION_PLAN_2026.md) 阶段 5 已补 **histogram 最小切片**文档（NDV 级，非 equi-depth 全套装） | 多候选**完整**枚举、全路径统一 cost、列级 **equi-depth histogram**、**独立优化器级 cost 模型**仍待办 |
| 缓存与内存 | **已有**全局 LRU PageCache（`NEWDB_PAGE_CACHE_MAX_BYTES`）+ runtime **`page_cache_*`**；**已增** **`memory_budget_max_bytes` / `memory_budget_used_bytes` / `memory_budget_reject_count`**（cap、占用快照、单页超 cap 拒绝 put 计数）；**已增** equality 侧车 **`where_eq_sidecar_disk_bytes_read_total` / `where_eq_sidecar_disk_loads`**（冷路径磁盘读观测，见 **`RUNTIME_STATS_SCHEMA.md`**）；heap 仍有单页缓存；**仍缺** sidecar/query 与统一 **cap/淘汰** 策略合一 | 大表多查询 I/O 部分缓解；峰值内存治理仍待办 |
| 测试矩阵 | 单项 GTest 很多；**已增** verify 脚本可选 storage/WAL JSONL 门、fixture 契约样例；fault injection / 长 soak / release 级门禁全矩阵仍可加强 | 正确性和性能回归不一定第一时间暴露 |

### 1.3 实施进度一览（🟢 / 🟡）

与 §3 各阶段对应；细项仍以各节「状态」为准。

| 阶段 | 标记 | 说明 |
|------|------|------|
| 阶段 0 | 🟡 | 🟢 `capture_baseline.py`（含 **`--validate-runtime-fixture`**、**`--emit-archive-contract` / `--write-archive-manifest`**）、**`scripts/ci/fixtures/*.jsonl`** 契约样例、[`PERF_AND_CI_BUDGETS.md`](../ci/PERF_AND_CI_BUDGETS.md) **§4 归档契约**、`verify_clean_reconfigure.ps1` 可选 **BenchGateStorage/WalRecovery**、`nightly_soak_hints.py --json`（含 **`threshold_hints`**）、**`linux-bench-gate-runtime-contract`**（manifest artifact + JSONL bench 门）；🟡 业务负载 JSONL 与 baseline **数值**跨环境固化仍部分依赖机器调参 |
| 阶段 1 | 🟡 | 🟢 `LockKey` + 存储键兼容 `table#id`、**`TXN_ISOLATION_AND_LOCKING.md` §1.2 读路径审计表**（CLI/C API/GUI）、**`InterleavedCommitRcRefreshesSnapshotStaysPinned`**、**`MultithreadLocalTablesSnapshotVsRcMatchesBaseline`**、**`MultithreadLocalTablesHighLoadStressBounded`**、**`AlternatingCoordinatorsSameWorkspaceBoundedStress`**；🟡 GUI 非转发直连引擎路径、更大规模随机交错仍待扩充 |
| 阶段 2 | 🟡 | 🟢 默认 checkpoint 剪枝（`NEWDB_RECOVER_USE_CHECKPOINT_LSN=0` 关闭）、**扩展 fault 用例**（含 **多 segment**、**不完整 checkpoint + 未提交 insert**、孤立 `CHECKPOINT_END`、双 `BEGIN` 无 `END`、`NEWDB_RECOVER_MIN_LSN`）；🟢 **Nightly** 恢复耗时门文档化（**`--max-wal-recovery-last-elapsed-ms`** 建议起点见 [`PERF_AND_CI_BUDGETS.md`](../ci/PERF_AND_CI_BUDGETS.md) §3）；🟢 **PR 可选**：**`linux-bench-gate-runtime-contract`** 对契约 JSONL 跑同一 WAL recovery 峰值门（与阶段 0 manifest artifact 同 job）；runtime 已增 **`wal_recovery_redo_ms` / checkpoint 计数**（CLI 恢复路径）；🟡 恢复模块命名拆分、更严格未闭合回退、**全员 PR 默认**恢复硬门 |
| 阶段 3 | 🟡 | 🟢 runtime **`compact_debt_*`**、`compact_debt_bytes_peak` 门、**入队 debt 与 `compact_debt_priority` 同源公式**、`StorageSoakLight`（**Nightly 子集**：`ctest -R StorageSoakLight`）、**`nightly_soak_hints`**、**治理文档对 score 边界（cooldown）的表述**；**真空成功后**回写 **`table_storage_health_last_vacuum_*`**（LSN 来自 **`WalManager::current_lsn()`**）；**选项 B 默认**仍 **不** 将 `wal_since` 并入 score，可选 **`NEWDB_VACUUM_SCORE_WAL_SINCE=1`** 实验 gap 项；🟡 Nightly 默认阈值仍建议按环境调参、**verify** storage 门仍非 PR 默认（见 [`PERF_AND_CI_BUDGETS.md`](../ci/PERF_AND_CI_BUDGETS.md) §3） |
| 阶段 4 | 🟡 | 🟢 **`EXPLAIN WHERE`**、**`SHOW PLAN`（JSON，`estimated_scan_rows`、`plan_candidates_considered` 有界真值与 `query_with_index` 对齐）**、**`plan_id`**、**`built_ts_ms`**、**`NEWDB_QUERY_COST_MODEL`**（AND + **OR 链路**及 **范围 / `Ne` / `Contains`** 与 **`table_stats`** 对齐的启发）、**`table_stats_matches_schema` / `stats_schema_fp`**；🟡 完整候选枚举、列级 histogram、**独立优化器级 cost 模型** |
| 阶段 5 | 🟡 | 🟢 单页缓存与 **`heap_decode_slot_*`** 观测、**全局 PageCache** + **`page_cache_*`** + **`memory_budget_*`** 快照 + **`memory_budget_reject_count`**（单页超 cap 拒绝缓存）、**`where_eq_sidecar_disk_*`**（equality sidecar 磁盘读观测）；🟡 sidecar/query 纳入同一 cap 与统一淘汰策略 |
| 阶段 6 | 🟡 | 🟢 既有 IndexCatalog / **`NEWDB_INDEX_CATALOG_ENFORCE=1`** 路径、**`BUILD.md` CI 文档化**、GitHub **`linux-index-catalog-enforce`** job、**eq sidecar / bloom 原子写**、头行 **`;bld=`（写入 `ready`）**、**`tests/test_index_catalog.cpp`** 对 **`bld=1/2`** 解析；🟡 enforce 仍非默认全 PR、完整跨进程 **building→ready** 协调 |
| 阶段 7 | 🟡 | 🟢 `--release-grade` 收紧 deadlock victim、可选 **`compact_debt`** 峰值门、`nightly_soak_hints`、[`PERF_AND_CI_BUDGETS.md`](../ci/PERF_AND_CI_BUDGETS.md) **PR / Nightly / Release** 矩阵；🟡 自动化 soak 报告归档、硬门全矩阵 |

与 [`OPTIMIZATION_PLAN_2026.md`](OPTIMIZATION_PLAN_2026.md)「实施进度一览」对照时，**以本文 §1.3 与 §3 各阶段「状态」为准**同步 🟢/🟡，避免双文档长期漂移。

---

## 2. 推荐总体优化方向

建议把后续优化目标定为：

> 先把 `newdb` 做成“长期运行可解释、失败可恢复、性能可观测”的教学/原型型数据库内核，而不是立刻追求完整工业级 OLTP。

原因是当前系统已经有很多模块，但真正缺的是“闭环”：

- 事务语义闭环。
- WAL 恢复闭环。
- 存储治理闭环。
- 查询计划与统计闭环。
- CI/Release 门禁闭环。

---

# 3. 分阶段修改优化计划

## 阶段 0：建立当前基线，避免盲改

### 目标

先固定当前行为、性能和门禁结果，作为后续优化比较基线。

### 建议工作

1. 跑一次完整构建与测试：
   - CMake clean configure。
   - 全量 `ctest`。
   - bench gate。
   - 若环境允许，跑 ReleaseGrade。
2. 生成一份当前 runtime stats JSONL：
   - 覆盖 insert/update/delete。
   - 覆盖 page/where/count/sum/avg。
   - 覆盖 vacuum。
   - 覆盖 WAL recover。
3. 固化基线指标：
   - WAL recovery elapsed。
   - WHERE scanned/returned ratio。
   - lazy materialize 次数。
   - heap decode hit/miss。
   - vacuum queue depth / compact reclaimed bytes。
   - table storage health fragmentation。
   - write conflict / lock wait。

### 验收标准

- 有一份可重复生成的 baseline 报告。
- 后续优化必须能证明“不破坏正确性、不显著退化性能”。

### 状态

- 🟢 `scripts/ci/capture_baseline.py`（Windows 默认 `ctest -C RelWithDebInfo`；**`--validate-runtime-fixture`** 校验 **`scripts/ci/fixtures/runtime_stats_bench_gate_minimal.jsonl`**）、`scripts/ci/nightly_soak_hints.py`（`--json` 输出 **`threshold_hints`**）、**`verify_clean_reconfigure.ps1`** 可选 **`-BenchGateStorage` / `-BenchGateWalRecovery`**（与 **`ci_bench_gate.py`** + 上述 fixture 对齐 storage / WAL recovery 门）、**`linux-bench-gate-runtime-contract`** job（manifest artifact + 同 fixture 数值门）。
- 🟡 全量 clean configure、**业务负载** JSONL 归档与 baseline 数值固化仍依赖流水线/人工。

---

## 阶段 1：事务隔离与锁语义收敛

### 目标

把当前 `ReadCommitted` / `Snapshot` 的有限语义彻底说清楚、测清楚、跑通所有读路径。

### 修改点

#### 1. 统一读路径 snapshot 接入

检查所有查询入口是否都通过 `syncHeapReadSnapshotForQuery` / `HeapReadViewGuard`。

需要确认以下路径一致：

- `PAGE`
- `WHERE`
- `COUNT`
- `FIND`
- `SUM`
- `AVG`
- GUI 调用路径
- C API 调用路径

#### 2. 强化隔离测试

- `ReadCommitted`：每条语句刷新 snapshot。
- `Snapshot`：事务开始固定 snapshot。
- 未提交写不可见。
- 事务中途提交的数据在 RC 可见、Snapshot 不可见。
- rollback 后不可见。
- concurrent update/delete 场景。

#### 3. 整理 write intent 抽象

目前是 `table#id`。建议引入内部结构：

```cpp
struct LockKey {
    std::string table;
    std::string index;
    std::string key;
    LockKeyKind kind;
};
```

第一阶段不必实现范围锁，但先把接口从字符串拼接迁移到结构化 key，方便后续扩展。

### 状态

- 🟢 `LockKey` / `LockKeyKind`（`write_conflict/lock_key.h`），`to_storage_key()` 与历史 `table#id` 一致；GTest `LockKey.RowPkWriteIntentMatchesLegacyStorageKey`。
- 🟢 **读路径审计表**：[`TXN_ISOLATION_AND_LOCKING.md`](../txn/TXN_ISOLATION_AND_LOCKING.md) §1.2（CLI `PAGE`/`WHERE`/聚合与 C API 经 dispatch 的一致性；`NEWDB_TXN_ISOLATION_READPATH=0` 例外）。
- 🟢 **交错提交读视图**：`test_txn_isolation_visibility.InterleavedCommitRcRefreshesSnapshotStaysPinned`（先取 Snapshot 读视图再发生 peer commit：Snapshot 仍不可见新行；RC 刷新后可见）。
- 🟢 **多线程 smoke**：`MultithreadLocalTablesSnapshotVsRcMatchesBaseline`（各线程独立 `HeapTable` + 相同 `MVCCSnapshot`，barrier 同步多波次）。
- 🟢 **TxnCoordinator 有界 stress**：`AlternatingCoordinatorsSameWorkspaceBoundedStress`（同一 workspace、**单线程**交替 disjoint 写键；与 **`TXN_ISOLATION_AND_LOCKING.md`** 中「本用例断言事务/写意图在固定步数下可重复完成」的说明一致；**非** OS 文件锁下的真并发 `begin` 矩阵）。
- 🟡 GUI 直连自定义调用、**真并发**多线程 `TxnCoordinator` + 同表 `begin` 全矩阵仍受平台锁语义约束，待扩充或 `GTEST_SKIP` 慢机策略。

### 优先级

P0。

### 验收标准

- 文档、CLI 输出、实际行为一致。
- 新增事务隔离矩阵测试。
- runtime stats 能清楚反映冲突、等待、超时、deadlock。

---

## 阶段 2：WAL 恢复默认优化与结构拆分

### 目标

让 WAL 恢复从“能恢复”进一步升级为“大日志下恢复时间可控”。

### 修改点

#### 1. 默认启用安全 checkpoint 剪枝

🟢 **已默认**：未设置或空环境变量时，恢复路径用「最近完整 checkpoint」抬升 `replay_start_lsn`；**`NEWDB_RECOVER_USE_CHECKPOINT_LSN=0`** 回退全量语义；**`NEWDB_RECOVER_ENABLE_OFFSET_SEEK`** 仍为 opt-in。`test_wal_recovery_indexed` 在 `=0` 下采基线全量重放计数。

#### 2. 拆分恢复流程

建议把现在的恢复辅助逻辑继续拆成：

| 组件 | 职责 |
|---|---|
| `WalSegmentScanner` | 扫描 segment、建立 LSN 范围和 offset index |
| `WalRecordReader` | 读取、seek、校验 checksum、decode record |
| `WalRedoPlanner` | 判断 committed / rollback / partial abort / dangling txn |
| `WalRedoApplier` | 对 heap 应用 redo/undo |
| `WalRecoveryReporter` | 汇总 stats |

#### 3. 严格处理不完整 checkpoint

- `CHECKPOINT_BEGIN` 有但没有匹配 `CHECKPOINT_END`：必须回退到上一个完整 checkpoint。
- midpoint recovery 计数必须进入 runtime stats。
- 增加 crash fault matrix。

#### 4. 强化恢复统计

已有字段可以继续产品化：

- `last_complete_checkpoint_lsn`
- `replay_start_lsn`
- `records_after_checkpoint`
- `checkpoint_scan_ms`
- `redo_plan_ms`
- `redo_apply_ms`
- `index_rebuild_ms`

### 优先级

P0 / P1。

### 验收标准

- checkpoint 前大量 WAL 不再全量重放。
- 不完整 checkpoint 可安全回退。
- WAL recovery stats 能被 runtime report 和 CI gate 使用。
- 大 WAL 恢复时间有明确预算。

### 状态

- 🟢 默认 checkpoint 剪枝；不完整 BEGIN/END 的 midpoint 计数等既有行为保留。
- 🟢 **Fault 矩阵增量**：`test_wal_recovery_indexed` 覆盖孤立 `CHECKPOINT_END`（`last_complete` 前进、无 midpoint）、双 `CHECKPOINT_BEGIN` 无 `END`（midpoint≥2）、`NEWDB_RECOVER_MIN_LSN` 抬高 `replay_start_lsn`、**小 `segment_max_bytes` 下多段 WAL + 尾缀不完整 `CHECKPOINT_BEGIN`**（恢复后行数完整、midpoint≥1）、**不完整 checkpoint 内未提交 insert 不可见**等。
- 🟢 **Nightly 恢复耗时门**：[`PERF_AND_CI_BUDGETS.md`](../ci/PERF_AND_CI_BUDGETS.md) §3、[`STORAGE_GOVERNANCE_AND_RECOVERY_BUDGETS.md`](../storage/STORAGE_GOVERNANCE_AND_RECOVERY_BUDGETS.md) §3 写明 **`--max-wal-recovery-last-elapsed-ms`** 建议起点（与 `verify_clean_reconfigure -BenchGateWalRecovery` 对齐）；**§4** 写明 **PR 侧不默认**将恢复耗时门作阻断、若上调需软门/子集策略。
- 🟢 **PR 可选第二道门**：**`linux-bench-gate-runtime-contract`** 在 CI 中对契约 JSONL 跑 **`--max-wal-recovery-last-elapsed-ms`**（与 Nightly 文档一致，**不**改本地默认 verify）。
- 🟢 **CLI `recoverFromWAL` 可观测加深**：`TxnRuntimeStats` / JSON 导出 **`wal_recovery_redo_ms`**、**`wal_recovery_checkpoint_begin_count`**、**`wal_recovery_checkpoint_end_count`**（与引擎 `WalRecoveryStats` 全字段仍不对等；见 §1.2）。
- 🟡 `WalSegmentScanner` / `WalRedoPlanner` 等拆分、更严格未闭合回退语义、恢复耗时 **全员 PR 默认**常规硬门。

---

## 阶段 3：存储治理与 vacuum debt 闭环

### 目标

让 heap 文件长期运行后空间增长、碎片率和 vacuum 行为都可解释、可限制。

### 修改点

#### 1. 引入明确的 `compact_debt_bytes`

当前已有：

- `table_storage_health_dead_bytes`
- `vacuum_health_bonus_last`
- `vacuum_priority_score`

建议新增专名字段：

- `compact_debt_bytes`
- `compact_debt_rows`
- `compact_debt_ratio`
- `compact_debt_priority`

🟢 上述四项已在 **`TxnRuntimeStats` / runtime JSON** 导出（`triggerVacuum` 入队：`compact_debt_bytes`；health 采样成功时填 `compact_debt_rows` / `compact_debt_ratio`，否则 0；`compact_debt_priority` 同队列 score）。**`newdb_runtime_report`**：`compact_debt_bytes_peak`、`--max-compact-debt-bytes-peak`。

#### 2. 统一 vacuum queue debt 计算

当前 debt 默认与 heap 文件字节数相关，health 可选加权。建议统一为：

```text
compact_debt_score =
    dead_bytes_weight
  + tombstone_rows_weight
  + fragmentation_ratio_weight
  + wal_since_last_vacuum_weight
  - cooldown_penalty
```

🟢 **主干已落地（与上文理想式对照）**：`vacuum_service.cc` 中 **`compute_compact_debt_enqueue_metrics`**：在 `NEWDB_VACUUM_QUEUE_USE_HEALTH=1` 且 lazy health 采样成功时，`queue_score = file_bytes + dead_bytes + tombstone_slots×NEWDB_VACUUM_HEALTH_SLOT_WEIGHT + (tombstone_ratio×1e6 取整)`（饱和加）；**vacuum 队列第二键**与 **`compact_debt_bytes` / `compact_debt_priority` / `vacuum_priority_score`** 同源。**`wal_since` 距离项**：**默认不并入**入队 score（v1 选项 B）；若需实验，在 **`last_vacuum_lsn` 与当前 LSN 均可得**时设 **`NEWDB_VACUUM_SCORE_WAL_SINCE=1`** 启用在 score 上叠加 **有上界** 的 gap。**`cooldown_penalty`** 仍不入 score（冷却以 `vacuum_cooldown_skip_count` 等观测）。详见 [`STORAGE_GOVERNANCE_AND_RECOVERY_BUDGETS.md`](../storage/STORAGE_GOVERNANCE_AND_RECOVERY_BUDGETS.md) §1。

#### 3. 默认开启 health-aware vacuum

- 先在 Nightly 打开。
- 观察稳定后再在 ReleaseGrade 打开。
- 保留环境变量关闭。

#### 4. 增加 storage soak 测试

- 大量 insert/delete/update。
- 周期性 compact。
- 检查 dead bytes 不无限增长。
- 检查 fragmentation ratio 不超过阈值。
- 检查 compact reclaimed bytes 合理。

### 优先级

P1。

### 验收标准

- runtime stats 中有明确 compact debt。
- Release/Nightly 可设置碎片率和 dead bytes 阈值。
- vacuum 不会在写入高峰造成明显抖动。
- 长期 soak 后 heap 文件大小可控。

### 状态

- 🟢 `compact_debt_*`、report/gate、`validate_runtime_stats.py` 契约；**入队键与 runtime 专名同源**（见上「理想式对照」）；**`test_storage_soak.cpp`**（`StorageSoakLight`，**Nightly 子集**见 [`BUILD.md`](../dev/BUILD.md)）+ **`nightly_soak_hints.py`**；**治理文档**与 **`vacuum_service`**：**真空成功后**刷新 **`table_storage_health_last_vacuum_*`**；**默认**仍不把 `wal_since` 并入 score（可选 **`NEWDB_VACUUM_SCORE_WAL_SINCE`**）。
- **收口 v1（选项 B）**：**默认**不将 `wal_since` 并入 `compact_debt_priority` / 入队 score；**v2 增量**为真实 LSN 观测 + 可选实验项（见上）。**verify** 对 storage 数值门仍 **opt-in**（见 [`PERF_AND_CI_BUDGETS.md`](../ci/PERF_AND_CI_BUDGETS.md) §3，**不**将 `BenchGateStorage` 强塞为全员 PR 默认 verify）。
- 🟡 更长压测 soak、**硬门全矩阵**与报告归档仍可增强。

---

## 阶段 4：查询优化器与统计信息产品化

### 目标

把 WHERE 计划从“启发式 + sidecar 命中”升级为“轻量 cost model”。

### 修改点

#### 1. 完善 `TableStats` / `ColumnStats`

建议字段：

- row count
- null count
- NDV
- min/max
- histogram 简化版
- top-K frequent values
- last analyze time
- schema fingerprint

#### 2. 引入统一 cost model

对每个候选计划估算：

- heap scan cost
- equality sidecar cost
- covering sidecar cost
- page index cost
- materialization cost
- expected rows
- expected decoded rows

#### 3. 增加 explain/debug 输出

CLI 增加类似：

- `EXPLAIN WHERE ...`
- 或 `SHOW PLAN`

输出：

- chosen plan
- candidate plans
- estimated rows
- estimated cost
- fallback reason

🟢 **`EXPLAIN WHERE`**（同 `WHERE` 解析与 `query_with_index`），输出本次计数器增量（`fallback_scans`、`plan_eq_sidecar_count` 等）及 **`plan_id=`**（与 `query_with_index` 内 `QueryTraceGuard` 记录的 **`last_plan_id`** 一致，如 `id_lookup`、`fallback_scan` 等）。🟢 **`SHOW PLAN`**：同上谓词语法，输出**固定单行 JSON**（含 **`estimated_scan_rows`**、**`plan_candidates_considered`**（**有界**真值，与 **`query_with_index`** / **`QueryTraceGuard`** 一致），与 **`where_estimate_scan_rows`** / policy 门一致；以及 `plan_id`、`logical_rows`、`matched_rows`、`path`、`delta`）。🟢 轻量 cost：**`NEWDB_QUERY_COST_MODEL=1`** 且存在 `TableStats` 提示时，AND 链种子/求值顺序用 NDV 估计；**OR 估算链路**及对 **范围比较、`Ne`、`Contains`** 使用 **`eq_selectivity_from_stats` / `range_selectivity_from_stats`** 与 **`table_stats.h`** 一致。**🟡** 尚无多候选**完整**枚举、**独立优化器级 cost**、全路径统一 cost。

#### 4. 统计持久化正式化

当前 `.tablestats` 已有雏形。建议加入：

- version
- schema fingerprint
- data fingerprint / wal lsn
- built timestamp
- stats validity reason

🟢 **`built_ts_ms=`**（`fp=` 与 `row_count=` 之间）、`TableStats::stats_built_ts_ms`、旧文件兼容加载。🟢 **schema 指纹落地到内存结构**：`TableStats::stats_schema_fp`（load/build 后非 0）、**`table_stats_matches_schema(stats, schema)`** 供调用方判断是否与当前 DDL 一致。🟡 data fingerprint / wal lsn / validity reason 文本字段等仍待办。

#### 5. 防止 fallback scan 失控

- `NEWDB_WHERE_HEAP_SCAN_BUDGET_ROWS` 当前已有方向。
- 建议默认在 ReleaseGrade 中设置更严格阈值。
- fallback scan 超预算应进入 runtime stats 和 CI gate。

### 优先级

P1。

### 验收标准

- 高基数等值查询稳定走 sidecar / index。
- 低选择性查询不会盲目走代价更高路径。
- fallback scan 有预算。
- explain 能解释为什么选择某个计划。

### 状态

- 🟢 `EXPLAIN WHERE`、**`SHOW PLAN`（JSON，`estimated_scan_rows`、有界 `plan_candidates_considered`）**、**`plan_id`**、`.tablestats` 的 **`built_ts_ms`**、**`stats_schema_fp` / `table_stats_matches_schema`**、**`NEWDB_QUERY_COST_MODEL`**（与 **`NEWDB_QUERY_USE_TABLE_STATS`** 等配合；**OR / 范围 / `Ne` / `Contains`** 路径已接统计启发）。
- 🟡 列级扩展统计、**equi-depth histogram**、**全路径独立优化器级 cost**、data fingerprint / wal lsn / validity reason 文本（**`OPTIMIZATION_PLAN_2026`** 已写 **histogram 最小切片**文档：以 NDV 为主，非 §6.2 全套装）。

---

## 阶段 5：统一 page cache 与 memory budget

### 目标

控制大表、多查询、lazy decode 下的 I/O 和内存抖动。

### 当前问题

`HeapTable` 中已有单页缓存相关字段：

- `heap_cached_page_no_`
- `heap_cached_page_buf_`
- `decode_heap_slot_hits`
- `decode_heap_slot_misses`

这对单表单查询有效，但无法作为全局缓存策略。

### 修改点

#### 1. 新增 `PageCache` 模块

- key：`file_id + page_no`
- value：page buffer
- policy：LRU / CLOCK
- memory limit：可配置

#### 2. 引入全局 memory budget

覆盖：

- heap page cache
- sidecar cache
- table stats cache
- query temporary buffers
- lazy materialize guard

#### 3. runtime stats 增加

- `page_cache_hits`
- `page_cache_misses`
- `page_cache_evictions`
- `page_cache_bytes`（实现字段名为 **`page_cache_bytes_in_cache`**，与 JSON 一致）
- **`memory_budget_max_bytes`**（来自 **`NEWDB_PAGE_CACHE_MAX_BYTES`**，0 表示未设 cap）
- **`memory_budget_used_bytes`**（当前与 **`page_cache_bytes_in_cache`** 对齐；后续可并入 sidecar 等）
- **`memory_budget_reject_count`**（🟢 单页大小超过 cap 时拒绝 `page_cache_put` 的累计次数，见阶段 5「状态」）

#### 4. 限制意外物化

当前 lazy materialize 已有 stats。建议增加：

- 超大表写路径物化前确认策略。
- ReleaseGrade 默认禁止意外全表物化。
- 对必须物化的操作给出明确提示。

### 优先级

P1 / P2。

### 验收标准

- 大表 PAGE / WHERE 重复查询 cache hit 明显上升。
- 内存使用有上限。
- lazy materialize 不再无声造成大内存峰值。

### 状态

- 🟢 **全局 PageCache MVP**：`engine/include/newdb/page_cache.h`、`engine/src/cache/page_cache.cpp`，惰性堆 **`read_page_copy`** 路径上 **LRU + `NEWDB_PAGE_CACHE_MAX_BYTES`**；**`TxnRuntimeStats` / C API / `validate_runtime_stats.py`** 含 **`page_cache_*`** 与 **`memory_budget_max_bytes` / `memory_budget_used_bytes` / `memory_budget_reject_count`**（单页超 cap 拒绝 put 计数）；GTest **`test_page_cache.cpp`**。
- 🟢 **Equality sidecar 磁盘读观测**：runtime / JSONL 契约字段 **`where_eq_sidecar_disk_bytes_read_total` / `where_eq_sidecar_disk_loads`**（**`RUNTIME_STATS_SCHEMA.md`**、`validate_runtime_stats.py` **LEGACY** 默认、`rust_gui` 副本同步）。
- 🟡 **统一 memory budget**（sidecar/query 缓冲等合一、与 ReleaseGrade 默认绑定的超限策略）仍未落地；单表 **`heap_cached_page_*`** 与 **`heap_decode_slot_*`** 观测仍保留（§1.1）。

---

## 阶段 6：sidecar 与索引生命周期统一

### 目标

让 equality/page/covering/visibility sidecar 的创建、校验、失效、重建走统一目录与协议。

### 修改点

#### 1. `IndexCatalog` 版本化

当前已有 descriptor 和 header meta。建议进一步加入：

- catalog version
- sidecar format version
- index kind version
- schema fingerprint
- data fingerprint
- wal lsn
- build status

#### 2. 统一 sidecar build 状态

防止半构建文件被误用：

- building
- committed
- invalid
- dropped

#### 3. 原子重建

- 写临时文件。
- fsync。
- rename。
- 更新 catalog。
- 清理旧版本。

#### 4. enforce 默认化

当前 `NEWDB_INDEX_CATALOG_ENFORCE=1` 是 opt-in。建议：

- PR/Nightly 打开。
- 观察稳定后默认打开。
- 遇到不一致自动删除并重建，而不是静默使用。

🟡 **`NEWDB_INDEX_CATALOG_ENFORCE`** 仍为显式 **`=1`** 启用；未做 CI 环境默认。[`BUILD.md`](../dev/BUILD.md) 已说明 **CI 流水线可 opt-in 设为 `1`** 以硬失败 catalog 身份不一致。

### 优先级

P1。

### 验收标准

- 不再使用过期 sidecar。
- 崩溃中断 sidecar build 后可自动恢复。
- index catalog 校验默认开启。

### 状态

- 🟢 既有 IndexCatalog / descriptor / enforce 路径（见 [`OPTIMIZATION_PLAN_2026.md`](OPTIMIZATION_PLAN_2026.md) 阶段 6）。
- 🟢 **equality sidecar / eq bloom 原子写**：先写 **`<path>.tmp`**，关闭流后 **remove + rename** 到目标路径（降低半写文件被读概率）。
- 🟢 头行 catalog 尾缀 **`;bld=`**：新写入侧统一带 **`bld=2`（ready）**，`IndexCatalogParsedTail::catalog_build_state` 可解析；旧文件无该字段视为 legacy；GTest **`ParseBuildStateFromBldToken`**、**`BuildStateBuildingVsReadyParsed`** 覆盖 **`bld=1`（building）/ `bld=2`（ready）** 与 legacy；**[`TXN_ISOLATION_AND_LOCKING.md`](../txn/TXN_ISOLATION_AND_LOCKING.md)** §1.1 文档化 **building→ready** 运维语义（与 **`NEWDB_INDEX_CATALOG_ENFORCE`** 删除重建路径对照）。
- 🟡 版本化字段全集、**多态 build 状态机**（building / invalid / … 与跨进程运行时协调）、**enforce CI 默认**仍按上文「修改点」待办。

---

## 阶段 7：ReleaseGrade 与 Nightly 门禁升级

### 目标

让已有 runtime stats 真正阻止回归。

### 修改点

#### 1. ReleaseGrade 默认硬门

建议收紧：

- lazy materialize count delta = 0。
- checksum failures = 0。
- decode failures = 0。
- fallback scan delta 受限。
- WAL recovery elapsed 受限。
- lock deadlock victim 非预期为 0。
- table fragmentation ratio 受限。
- compact debt 受限。

🟢 **`--release-grade`**：默认收紧 lazy 物化 delta、**`max_lock_deadlock_victim_delta=0`**；可选 **`--max-compact-debt-bytes-peak`** → **`compact_debt_bytes`** 峰值。🟡 checksum/decode、fallback、WAL recovery elapsed、fragmentation 等仍依赖显式阈值与 JSONL。

#### 2. Nightly soak

新增：

- WAL 大日志恢复。
- 大表 insert/delete/update/vacuum。
- sidecar build/rebuild/invalidate。
- 并发读写。
- crash between checkpoint begin/end。
- crash during sidecar rebuild。

#### 3. 报告归档

每次 Nightly 产出：

- runtime stats summary。
- perf summary。
- recovery summary。
- storage health summary。
- query plan summary。

### 优先级

P0 / P1。

### 验收标准

- CI 不只是“能编译、测试过”，还能发现性能与长期运行退化。
- ReleaseGrade 不允许跳过关键门禁。

### 状态

- 🟢 ReleaseGrade 下 deadlock victim 默认门、可选 **`compact_debt`** 峰值门、`nightly_soak_hints.py`（**`--json` / `threshold_hints`**）、**`linux-bench-gate-runtime-contract`**（manifest + JSONL 门）。
- 🟡 自动化 soak **全矩阵**、报告归档与 Dashboard 级硬门仍可增强（**`BUILD.md` / `scripts/README.md`** 已写 **`scripts/results/`** 下趋势路径约定）。

---

# 4. 推荐优先级排序

如果只能选最重要的 5 件事，建议按这个顺序做：

1. **WAL checkpoint 默认剪枝 + fault matrix**
   - 🟢 默认剪枝；🟢 **部分 fault 用例**（`test_wal_recovery_indexed`）；🟢 **Nightly** **`--max-wal-recovery-last-elapsed-ms`** 与 [`PERF_AND_CI_BUDGETS.md`](../ci/PERF_AND_CI_BUDGETS.md) 对齐；🟢 **PR 可选** **`linux-bench-gate-runtime-contract`**；🟢 runtime **`wal_recovery_redo_ms` / checkpoint 计数**；🟡 恢复拆分、恢复耗时 **全员 PR 默认**硬门。
2. **事务隔离读路径全覆盖 + 测试矩阵**
   - 🟢 `LockKey`、**§1.2 读路径审计表**、**多线程 barrier smoke**、**`AlternatingCoordinatorsSameWorkspaceBoundedStress`**；🟡 **真并发**多线程 `TxnCoordinator` + 同表 `begin` 全矩阵。
3. **compact debt 专名指标 + storage soak 门禁**
   - 🟢 **`compact_debt_*`** 与 gate、**入队与 runtime 同源 score**、**`StorageSoakLight`**、**hints**、**治理文档**、**`last_vacuum_*` runtime 观测**；🟡 **默认 verify** 仍不附带 storage 硬门（见 **`PERF_AND_CI_BUDGETS.md`** §3）；可选 **`NEWDB_VACUUM_SCORE_WAL_SINCE`** 是否作为长期默认需再评估。
4. **查询 cost model + EXPLAIN**
   - 🟢 **`EXPLAIN WHERE`**、**`SHOW PLAN`（JSON，含 `estimated_scan_rows`、有界 `plan_candidates_considered`）**、**`plan_id`**、**`built_ts_ms`**、**`NEWDB_QUERY_COST_MODEL`**、**`stats_schema_fp`**；🟡 多候选**完整**枚举、**equi-depth histogram**、**全路径优化器级 cost**。
5. **page cache / memory budget 最小版本**
   - 🟢 **全局 PageCache MVP** + **`page_cache_*`** + **`memory_budget_max_bytes` / `memory_budget_used_bytes` / `memory_budget_reject_count`** + **`where_eq_sidecar_disk_*`**；🟡 **统一 budget**（sidecar/query 缓冲与淘汰、与 ReleaseGrade 的默认绑定策略）。

---

# 5. 建议的近期迭代计划

## 第 1 周：基线与事务/WAL 测试

- 🟡 跑通完整 baseline（🟢 **`capture_baseline.py`**；🟢 **`--write-archive-manifest`** + CI **`linux-bench-gate-runtime-contract`** artifact；🟡 业务负载 JSONL 数值仍依赖环境）。
- 🟡 补齐事务隔离测试（🟢 RC vs Snapshot 基线用例已存在；🟢 **交错提交 + 双读视图**；🟢 **引擎多线程 barrier smoke**；🟢 **`MultithreadLocalTablesHighLoadStressBounded`**；🟢 **`AlternatingCoordinatorsSameWorkspaceBoundedStress`**；🟡 生产级**真并发**多线程 `TxnCoordinator` stress 仍待办）。
- 🟢 ~~补 checkpoint begin/end fault 变体~~（`test_wal_recovery_indexed` 增量）。
- 🟢 ~~读路径 snapshot 行为文档化~~（`TXN_ISOLATION_AND_LOCKING.md` §1.2）。

## 第 2 周：WAL 恢复优化

- 🟢 ~~默认使用最近完整 checkpoint 作为 replay start~~（`NEWDB_RECOVER_USE_CHECKPOINT_LSN=0` 关闭）。
- 🟡 不完整 checkpoint 回退（强化语义；🟢 **多 segment / 尾缀不完整 cp** 测试已增）。
- 🟢 Nightly：**`--max-wal-recovery-last-elapsed-ms`** 建议阈值与 [`PERF_AND_CI_BUDGETS.md`](../ci/PERF_AND_CI_BUDGETS.md) / `newdb_runtime_report` 叙述一致；🟢 **部分更深导出**：runtime **`wal_recovery_redo_ms` / checkpoint 计数**（CLI `recoverFromWAL`）；🟢 **PR 可选** **`linux-bench-gate-runtime-contract`**；🟡 引擎 **`WalRecoveryStats` 全字段**镜像、恢复耗时 **全员 PR 默认**硬门。

## 第 3 周：存储治理

- 🟢 ~~`compact_debt_*` 专名~~（runtime / validate / report peak）。
- 🟢 ~~统一 vacuum 入队与 `compact_debt_*` 同源 score~~（`compute_compact_debt_enqueue_metrics`）。
- 🟢 ~~增加轻量 storage soak~~（`StorageSoakLight`）；🟡 长压测与 **`verify_clean_reconfigure`** 默认 storage 硬门。
- 🟡 Nightly 对 **`--max-table-storage-health-*` / `--max-compact-debt-bytes-peak`** 的默认阈值固化；🟢 **`verify_clean_reconfigure -BenchGateStorage -BenchGateWalRecovery`** + fixture JSONL 可作为显式矩阵项；🟢 **`nightly_soak_hints --json`**（**`threshold_hints`**）；🟢 **`BUILD.md` / `scripts/README.md`** 写 **`scripts/results/`** soak 路径。

## 第 4 周：查询优化

- 🟡 完善 table stats（🟢 已有 **`built_ts_ms`**、**`stats_schema_fp` / `table_stats_matches_schema`**；列级 / histogram 等仍待办）。
- 🟢 ~~explain 输出~~（**`EXPLAIN WHERE`** 计数器 + **`plan_id`**）；🟢 ~~轻量 cost~~（**`NEWDB_QUERY_COST_MODEL`**，含 **OR / 范围 / `Ne` / `Contains`**）；🟢 ~~**`SHOW PLAN` JSON**~~（**`estimated_scan_rows`**、**有界 `plan_candidates_considered`** 等；**histogram** 最小切片见 **`OPTIMIZATION_PLAN_2026`** 阶段 5 文档）。
- 🟡 cost 覆盖 equality sidecar / covering / heap scan **全路径**与**候选计划**级统一。
- 🟡 fallback scan 默认预算化。

## 第 5 周以后：缓存与索引生命周期

- 🟢 ~~PageCache 最小实现~~（`NEWDB_PAGE_CACHE_MAX_BYTES` + **`page_cache_*`** + **`memory_budget_reject_count`**）。
- 🟡 IndexCatalog 版本化（🟢 头行 **`;bld=`**、**`ParseBuildStateFromBldToken` / `BuildStateBuildingVsReadyParsed`**、**`linux-index-catalog-enforce`** CI job；🟢 **`TXN_ISOLATION_AND_LOCKING.md`** §1.1 **building→ready** 说明，见 **`BUILD.md`**）。
- 🟢 ~~equality sidecar / bloom 原子写~~（tmp→rename）。
- 🟡 ReleaseGrade / CI 默认 enforce（当前 🟡 仍为 opt-in，见阶段 6 / `BUILD.md`）。

---

# 6. 最终评价

当前 `newdb` 的基础已经比较扎实，尤其是：

- 文档和源码映射较完整。
- 测试数量较多。
- WAL / MVCC / sidecar / runtime stats 都已有雏形。
- 优化路线文档已经覆盖了大部分正确方向。

但现在最需要避免的是“功能点很多，但闭环不够硬”。下一阶段不建议继续横向加新功能，而应优先做：

1. **正确性闭环**：🟡 事务隔离**真并发**多线程 `TxnCoordinator` 全矩阵、WAL **RedoPlanner 级**拆分；🟢 默认剪枝、🟢 **扩展 fault 用例**、🟢 **MVCC 交错读视图**、🟢 **多线程 barrier smoke**、🟢 **高迭代有界 stress**、🟢 **`LockKey`**、🟢 **TxnCoordinator 有界交替 stress**、🟢 **CLI 恢复路径** runtime **`wal_recovery_redo_ms` / checkpoint 计数**。
2. **长期运行闭环**：🟡 长 soak / verify **默认** storage 门（🟢 **`compact_debt_*`** 同源 score、**`StorageSoakLight`**（`ctest -R StorageSoakLight`）、health、**opt-in verify BenchGate**、**`linux-bench-gate-runtime-contract`**、**`nightly_soak_hints --json`（`threshold_hints`）**、**治理文档 score 边界**、[`PERF_AND_CI_BUDGETS.md`](../ci/PERF_AND_CI_BUDGETS.md) **PR/Nightly 矩阵**）。
3. **性能解释闭环**：🟡 **优化器级全路径 cost**（🟢 **`EXPLAIN WHERE` + `SHOW PLAN` JSON**（**`estimated_scan_rows`**、**有界 `plan_candidates_considered`**）+ **`plan_id`**、**`NEWDB_QUERY_COST_MODEL`**、**`stats_schema_fp`**、runtime gate；🟢 **histogram 最小切片**文档见 **`OPTIMIZATION_PLAN_2026`** 阶段 5）。
4. **资源控制闭环**：🟡 **统一 memory budget 策略**（🟢 **全局 PageCache MVP** + **`page_cache_*`** + **`memory_budget_*`** 与 **`memory_budget_reject_count`**；🟢 **`where_eq_sidecar_disk_*`** 侧车磁盘读观测；🟢 lazy materialize 等部分 gate；**`RUNTIME_STATS_SCHEMA.md`** 已记 **v2** sidecar/query 合一方向）。

如果按上述计划推进，`newdb` 可以从“教学/原型数据库”进一步升级为“可长期运行、行为可解释、质量门禁明确的轻量数据库内核”。

### 6.1 阶段性收口定义（v1）

在**不宣称**工业级 InnoDB 语义的前提下，以下条件满足时可视为「闭环 v1」已收口（与 [`PERF_AND_CI_BUDGETS.md`](../ci/PERF_AND_CI_BUDGETS.md) §3、[`BUILD.md`](../dev/BUILD.md)「Nightly 子集」一致）：

| 闭环 | 可勾选标准 |
|------|------------|
| **正确性** | 默认 checkpoint 剪枝；**`test_wal_recovery_indexed`** 覆盖多类 fault（含多 segment、不完整 checkpoint、未提交 insert）；**`test_txn_isolation_visibility`** 含交错提交与**多线程本地表** smoke；**`test_txn_write_conflict.AlternatingCoordinatorsSameWorkspaceBoundedStress`**（有界 workspace 写意图）；runtime 含 **`wal_recovery_redo_ms` / `wal_recovery_checkpoint_*`**（CLI `recoverFromWAL`）。 |
| **长期运行 / CI** | **`compact_debt_*`** 与入队 score 同源；**`StorageSoakLight`**；**`verify … BenchGate*`** 与 fixture JSONL 文档化（[`PERF_AND_CI_BUDGETS.md`](../ci/PERF_AND_CI_BUDGETS.md) §3/§4）；**`linux-bench-gate-runtime-contract`**（manifest + 同 fixture 数值门）；**`nightly_soak_hints --json`**（**`threshold_hints`**）；阶段 3 **选项 B**（**默认**不入 score 的 `wal_since` 项）；**v2** 后 **`last_vacuum_*`** 可为运行观测真值，与公式默认策略独立。 |
| **性能解释** | **`EXPLAIN WHERE`** + **`SHOW PLAN` JSON**（**`estimated_scan_rows`**、**`plan_candidates_considered` 有界真值**（与 `query_with_index` 路径一致，与 policy 估计一致））；**`NEWDB_QUERY_COST_MODEL`** + **`table_stats`** 对 AND/OR/范围等路径有统计启发。 |
| **资源** | **`page_cache_*`** + **`memory_budget_*`** + **`memory_budget_reject_count`** + **`where_eq_sidecar_disk_*`** 进入 runtime JSON 与 **`validate_runtime_stats.py`** / **`RUNTIME_STATS_SCHEMA.md`**。 |

### 6.2 非目标（当前内核边界 / backlog）

以下**不**纳入「闭环 v1」验收，避免范围膨胀：

- `LockKey` **范围锁 / 谓词锁 / 二级索引写冲突**完整抽象。
- **WalRecordReader / WalRedoPlanner** 类级拆分；未闭合事务的 InnoDB 级严格语义。
- **候选计划枚举**、**histogram**、**`.tablestats` 内 data fingerprint / wal lsn / validity reason** 全套装。
- **GUI 非转发路径**与引擎读路径逐行对齐（可单独开 GUI 里程碑）。

### 6.3 闭环 v2 判据（可选收口语义）

在 v1 表之上，若需将「剩余闭环」工程包标为可勾选交付，可额外采用下列 **v2** 判据（与主干实现对照；**非**工业级 InnoDB 宣称）：

| 判据 | 说明 |
|------|------|
| **第二道 CI 门** | 存在 **`linux-bench-gate-runtime-contract`**（或等价脚本/流水线），对 **`runtime_stats_bench_gate_minimal.jsonl`** 跑 **`ci_bench_gate`** 的 storage / WAL recovery 峰值门，且不修改本地默认 **`verify`**。 |
| **`plan_candidates_considered`** | **`SHOW PLAN` JSON** 中该字段为 **有界真值**（与 **`query_with_index`** / **`QueryTraceGuard`** 一致），**非**恒为占位常量。 |
| **恢复可观测加深** | runtime / C API / shell JSON 含 **`wal_recovery_redo_ms`**、**`wal_recovery_checkpoint_begin_count`**、**`wal_recovery_checkpoint_end_count`**。 |
| **侧车读观测** | runtime 契约含 **`where_eq_sidecar_disk_bytes_read_total` / `where_eq_sidecar_disk_loads`**，并写入 **`RUNTIME_STATS_SCHEMA.md`** / **`validate_runtime_stats.py`**（含 **LEGACY** 默认）。 |
| **IndexCatalog 状态文档** | **`TXN_ISOLATION_AND_LOCKING.md`** 描述 **`;bld=`** building/ready；**`test_index_catalog.BuildStateBuildingVsReadyParsed`** 通过。 |
| **Nightly 阈值提示** | **`nightly_soak_hints.py --json`** 输出 **`threshold_hints`**；[`STORAGE_GOVERNANCE_AND_RECOVERY_BUDGETS.md`](../storage/STORAGE_GOVERNANCE_AND_RECOVERY_BUDGETS.md) 与 [`BUILD.md`](../dev/BUILD.md)、[`scripts/README.md`](../../scripts/README.md) 写明 soak 趋势与 summary 的约定路径。 |
