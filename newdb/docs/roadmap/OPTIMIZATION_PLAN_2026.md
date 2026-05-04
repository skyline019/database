# newdb 2026 优化路线图

本文把当前 `newdb` 从“教学/原型型单机数据库”继续推进到“可观测、语义清晰、可长期运行的数据库内核”的优化工作拆成可执行工程包。内容基于当前代码与文档状态：heap + WAL + MVCC 快照、事务协调器、sidecar 索引、GUI/C API、测试与 CI 门禁。

---

## 实施进度一览（与主干同步）

> 反映文档修订时仓库已实现范围；源码索引见 [`NEWDB_DESIGN_POINT_TO_FILE_MAP.md`](../architecture/NEWDB_DESIGN_POINT_TO_FILE_MAP.md)，构建与回归见 [`BUILD.md`](../dev/BUILD.md)。
>
> **与 [`NEWDB_OPTIMIZATION_ASSESSMENT_AND_PLAN.md`](NEWDB_OPTIMIZATION_ASSESSMENT_AND_PLAN.md) §1.3 对齐**：评估文档按「阶段 0–7」用 🟢/🟡 描述**同一主干上的边际缺口**；本表沿用路线图 **§11 阶段 0–10** 编号（与评估表**非一一对应**）。细粒度「仍缺什么」**以评估文档 §1.3 与各阶段「状态」为准**，避免双文档长期漂移。
>
> **CI / 基线小闭环（阶段 0 相关）**：[`PERF_AND_CI_BUDGETS.md`](../ci/PERF_AND_CI_BUDGETS.md) §3 定义 **PR / Nightly / Release** 矩阵、**§4** 写 **JSONL 归档契约**；`scripts/ci/verify_clean_reconfigure.ps1` 可选 **`-BenchGateStorage`** / **`-BenchGateWalRecovery`**；`scripts/ci/capture_baseline.py` 支持 **`--validate-runtime-fixture`**、**`--emit-archive-contract`**、**`--write-archive-manifest`**。

| §11 阶段 | 状态 | 说明 |
|---------|------|------|
| 阶段 0 | **主干完成 / 边际 🟡** | `BUILD.md` 等；`verify_clean_reconfigure` / `capture_baseline`（含 manifest/contract）/ `nightly_soak_hints --json`（`threshold_hints`）；**`linux-bench-gate-runtime-contract`**；全量 clean + 业务 JSONL 数值仍部分依赖机器（见评估 §1.3 阶段 0） |
| 阶段 1 | **主干完成 / 边际 🟡** | `tests/test_txn_isolation_visibility.cpp`（含多线程 barrier + **高迭代 stress**）；`TxnCoordinator` 隔离与读路径；生产级高负载并发仍见评估阶段 1 |
| 阶段 2 | **主干完成 / 边际 🟡** | `syncHeapReadSnapshotForQuery`、`HeapReadViewGuard`；[`TXN_ISOLATION_AND_LOCKING.md`](../txn/TXN_ISOLATION_AND_LOCKING.md) |
| 阶段 3 | **主干完成 / 边际 🟡** | `where_query_*`、`validate_runtime_stats.py` |
| 阶段 4 | **主干完成 / 边际 🟡** | `lazy_materialize_*`、bench gate；[`STORAGE_GOVERNANCE_AND_RECOVERY_BUDGETS.md`](../storage/STORAGE_GOVERNANCE_AND_RECOVERY_BUDGETS.md) |
| 阶段 5 | **主干完成 / 边际 🟡** | **`.tablestats`**、`test_query_table_stats`；列级 histogram 等仍见评估 |
| 阶段 6 | **主干完成 / 边际 🟡** | IndexCatalog 头行、**`NEWDB_INDEX_CATALOG_ENFORCE`**（**默认仍为 opt-in**；**`linux-index-catalog-enforce`** job，见 `BUILD.md`）；**`;bld=`** 构建态尾缀解析（写入侧为 ready；**`TXN_ISOLATION_AND_LOCKING.md`** §1.1 状态说明 + **`test_index_catalog.BuildStateBuildingVsReadyParsed`**） |
| 阶段 7 | **主干完成 / 边际 🟡** | `wal_manager_recover_support.cpp` 中 **`recover_build_segment_index`** / **`recover_replay_segments`** |
| 阶段 8 | **部分完成** | **`checkpoint_midpoint_recovery_count`**；**`test_wal_recovery_indexed`** 多 fault / 多 segment / **未提交 insert + 不完整 cp**；**Nightly** **`--max-wal-recovery-last-elapsed-ms`** 文档化（见 **`PERF_AND_CI_BUDGETS.md`** §3）；**待办**：planner 类进一步拆分、**PR 默认**恢复硬门 |
| 阶段 9 | **部分完成** | **`m_vacuum_queue`**、**`table_storage_health_*`**（真空成功后回写 **`last_vacuum_*`**）、**`ci_bench_gate`** 透传；**选项 B 默认**：score **不**含 `wal_since`；可选 **`NEWDB_VACUUM_SCORE_WAL_SINCE=1`**。**待办**：Nightly soak 默认阈值、verify **默认** storage 门策略（见 `PERF_AND_CI_BUDGETS.md` §3） |
| 阶段 10 | **部分完成** | **`heap_decode_slot_*`**、`--release-grade`、**`NEWDB_WHERE_HEAP_SCAN_BUDGET_ROWS`**；**`memory_budget_*`**（含 **`memory_budget_reject_count`**）与 **`page_cache_*`**；**`SHOW PLAN` `estimated_scan_rows`**。**待办**：sidecar 等纳入同一上限与淘汰策略、ReleaseGrade 全矩阵 |

**建议下一迭代**：仍以评估文档 §1.3 的 🟡 行为准——**阶段 10**（memory budget 策略扩展）、**阶段 8**（fault / 恢复耗时门）、**阶段 9**（debt 公式与 soak）择最小可测切片；WAL 可继续抽 **`WalRecordReader`** 等独立类（非强制）。

---

## 1. 当前定位与总体判断

### 1.1 当前优势

`newdb` 当前已经具备以下基础能力：

- 页式 heap 存储与 lazy decode。
- WAL、checkpoint、PITR marker、savepoint、partial abort 等恢复相关机制。
- MVCC snapshot 与 `HeapTable::is_row_visible` 可见性入口。
- 事务协调器、写意图冲突检测、文件锁、后台 vacuum 队列。
- WHERE 执行器中的 `id` / primary key / equality sidecar / 多条件交集优化。
- C API、Rust/Tauri、Vue GUI 的结构化错误契约。
- GTest、Rust test、Frontend test 与 CI/bench gate。

### 1.2 当前主要短板

下列短板中，部分已在 §「实施进度一览」对应阶段**部分缓解**（仍以完整 SQL/工业级语义为目标）。

| 领域 | 当前短板 | 风险 |
|------|----------|------|
| 事务隔离 | CLI 查询读路径已接 `active_snapshot`；**仍非**完整 InnoDB 式 RR/RC + 间隙锁 + undo 链 | 复杂并发下用户预期与引擎边界仍需文档与测试收敛 |
| 写冲突 | 基于进程内 `table#id` 写意图，缺少统一锁键抽象 | 难以扩展到范围锁、谓词冲突与更复杂并发语义 |
| WAL 恢复 | 已具备 **`list_wal_segment_paths`**、**`wal_manager_recover_support.cpp`**（`recover_build_segment_index` / `recover_replay_segments`）与 **opt-in** 的 checkpoint 重放起点（`NEWDB_RECOVER_USE_CHECKPOINT_LSN` 等）；**缺**默认开启的段剪枝、begin/end 崩溃的严格回退、并行恢复 | 恢复路径仍非「工业级默认优化」；PITR/并行恢复仍属后续 |
| 存储治理 | **`TableStorageHealth`**（§5.2 级字段，见 `table_storage_health.h`）与 runtime **`table_storage_health_*`**；可选 **`NEWDB_VACUUM_QUEUE_USE_HEALTH`** 并入队列 **debt**；**CI**：`ci_bench_gate.py` → **`newdb_runtime_report`** 可选 **`--max-table-storage-health-*`** / **`--max-vacuum-health-bonus-last`**（见 **`STORAGE_GOVERNANCE_AND_RECOVERY_BUDGETS.md`** 第 4 节） | **仍缺** runtime 专名 **`compact_debt_bytes`** 与 soak **默认**硬门调参 |
| 查询优化 | **`NEWDB_QUERY_USE_TABLE_STATS`** 下 NDV 驱动等值选择性；可选 **`NEWDB_QUERY_PERSIST_TABLE_STATS`** 落盘 **`.tablestats`**；**缺** 系统化 `estimated_cond_cost` 全路径覆盖与「持久 ANALYZE」级产品化 | 偏斜/大表下计划与运维预期仍可继续收敛 |
| sidecar 索引 | **`IndexDescriptor`**、**集中 invalidation**、**`NEWDB_INDEX_CATALOG_ENFORCE`**；头行 **`idx_*` + FNV** 与可选 **`tbl_n`/`inx_n`** 明文；enforce 路径 **kind + 指纹 + 身份** 对齐 | 后续可扩展：更多调用点统一 enforce、头格式版本化 |
| 缓存与内存 | 运行态已导出 **`heap_decode_slot_*`**；可选 **`NEWDB_WHERE_HEAP_SCAN_BUDGET_ROWS`** 收紧 WHERE 估算扫描预算；**缺** 统一 buffer/page cache 与 memory budget | 大表、多查询场景下抖动仍难从单一预算约束 |

### 1.3 推荐目标

短期不建议把 `newdb` 直接定位为高并发生产 OLTP。更合理的 2026 目标是：

1. **语义清晰**：事务隔离、写冲突、恢复边界都有明确契约和测试。
2. **性能可解释**：查询、写入、WAL、vacuum、sidecar 命中率都能量化。
3. **长期运行可控**：空间回收、恢复时间、lazy materialize、fallback scan 有预算和告警。
4. **结构可演进**：sidecar 与索引层逐步统一，WAL recovery 拆分，查询优化器引入统计信息。

---

## 2. 优先级总览

### P0：正确性与可观测性

1. 事务隔离语义真正落地。
2. 事务隔离与写冲突测试矩阵。
3. 查询、写路径、WAL、vacuum runtime stats 补齐。
4. lazy materialize、fallback scan、recovery elapsed 接入 CI gate。

### P1：性能结构优化

1. `TableStats` / `ColumnStats` 与统计驱动查询计划。
2. `IndexCatalog` 与统一 sidecar 生命周期。（🟢 **基线已落地**：descriptor、头行元数据、**`NEWDB_INDEX_CATALOG_ENFORCE`**、可选明文 **`tbl_n`/`inx_n`**；深化：range index、头格式版本化等）
3. WAL recovery：段扫描 + **`recover` 双阶段辅助函数**已落地（`wal_manager_recover_support.cpp`）；独立 **Reader/Planner/Applier 类**仍为可选深化。
4. storage health 与 vacuum debt 调度。（🟡 runtime + 可选 **CI** 门；**`compact_debt_bytes`** 专名与默认 soak 预算仍待收尾）

### P2：成熟内核能力

1. buffer/page cache 与全局 memory budget。
2. range-capable secondary index。
3. 更完整的 checkpoint/recovery 起点选择。
4. 更完整锁管理器、范围锁或 SSI 风格冲突检测。

---

## 3. 工程包 A：事务隔离与写冲突

### 3.1 目标

把 `TxnIsolationLevel` 从“配置项/展示字段”升级为真实影响读视图的语义开关。

### 3.2 建议语义

| 隔离级别 | 建议行为 |
|----------|----------|
| `ReadCommitted` | 每条读语句开始时构造新的 read snapshot，只能看到语句开始前已提交数据 |
| `Snapshot` | 事务开始时固定 read snapshot，事务内重复读稳定 |

### 3.3 主要改造点

- `cli/modules/txn/coordinator/txn_manager.h/.cc`
  - 明确事务 begin 时是否生成 snapshot。
  - 暴露当前事务 read snapshot 信息。
- `engine/src/mvcc/*`
  - 明确 snapshot 构造输入：committed txn set / lsn / timestamp。
- `engine/include/newdb/heap_table.h`
  - `active_snapshot` 当前已有入口，需确保所有读路径一致消费。
- `cli/modules/where/executor/*`
  - 查询开始时根据 isolation level 选择 statement snapshot 或 txn snapshot。
- `cli/shell/dispatch/handlers/txn/txn_handler.cc`
  - CLI 设置隔离级别后输出清晰说明。

### 3.4 测试矩阵

新增或强化：

- `TxnIsolation.NoDirtyRead`
- `TxnIsolation.ReadCommittedSeesCommittedBetweenStatements`
- `TxnIsolation.SnapshotDoesNotSeeCommittedAfterBegin`
- `TxnIsolation.SnapshotRepeatableReadStable`
- `TxnIsolation.WriteConflictReject`
- `TxnIsolation.WriteConflictWaitTimeout`
- `TxnIsolation.DeadlockVictimCountIncrements`

### 3.5 验收标准

- 文档、CLI 输出和实际读行为一致。
- `ReadCommitted` 与 `Snapshot` 在并发读写测试中表现不同。
- `TxnRuntimeStats` 能体现冲突、等待、超时、deadlock victim。

---

## 4. 工程包 B：WAL 与恢复结构化

### 4.1 目标

降低 `WalManager::recover` 的复杂度，为 checkpoint 优化、PITR、并行恢复和恢复预算打基础。

### 4.2 建议拆分

| 组件 | 职责 |
|------|------|
| `WalSegmentScanner` | 发现 WAL segment，计算 min/max LSN，建立 offset index |
| `WalRecordReader` | 读取 record，校验 checksum，处理 seek |
| `WalRedoPlanner` | 聚合 pending txn，识别 committed/rollback/partial abort |
| `WalRedoApplier` | 将 redo/undo 应用到 `HeapTable`，并输出 apply stats |

### 4.3 checkpoint 优化

当前 checkpoint begin/end 已存在。下一步建议：

- begin/end payload 中显式写入：
  - checkpoint id
  - snapshot lsn
  - table/schema version
  - sidecar catalog version
- recovery 识别最近完整 checkpoint。
- 不完整 checkpoint 自动回退到上一个完整 checkpoint。
- `checkpoint_begin` 不再忽略 `snapshot_lsn`。

### 4.4 新增恢复指标

建议扩展 `WalRecoveryStats`：

- `last_complete_checkpoint_lsn`
- `replay_start_lsn`
- `records_after_checkpoint`
- `segments_after_checkpoint`
- `checkpoint_scan_ms`
- `redo_plan_ms`
- `redo_apply_ms`
- `index_rebuild_ms`

### 4.5 验收标准

- WAL 相关 GTest 行为不变。
- recovery stats 字段可被 runtime report 导出。
- 带 checkpoint 的恢复能跳过无需重放的历史 segment。
- crash between checkpoint begin/end 能被识别并安全回退。

---

## 5. 工程包 C：存储治理与 vacuum debt

### 5.1 目标

从“提交后触发 + 冷却 + 后台队列”的 vacuum 模型，升级为“表级健康度 + debt 调度 + CI 预算”的长期治理模型。

### 5.2 表级健康元数据

**主干状态**：已实现 **`TableStorageHealth`** / **`measure_table_storage_health`**（[`cli/modules/storage/table_storage_health.h`](../../cli/modules/storage/table_storage_health.h)），字段与下列建议结构一致；**`last_vacuum_lsn` / `last_vacuum_elapsed_ms`** 仍为预留（导出常为 0）。下文保留为设计对照。

```cpp
struct TableStorageHealth {
    std::uint64_t logical_rows;
    std::uint64_t physical_rows;
    std::uint64_t tombstone_rows;
    std::uint64_t data_file_bytes;
    std::uint64_t live_bytes;
    std::uint64_t dead_bytes;
    double fragmentation_ratio;
    std::uint64_t last_vacuum_lsn;
    std::uint64_t last_vacuum_elapsed_ms;
};
```

### 5.3 vacuum debt 调度

建议计算：

- `compact_debt_bytes`
- `dead_row_ratio`
- `wal_since_last_vacuum`
- `read_amplification_estimate`
- `vacuum_priority_score`

调度策略：

| 场景 | 策略 |
|------|------|
| 小表且 dead ratio 高 | 快速 compact |
| 大表但 dead ratio 低 | 延迟 compact |
| 大表且 dead bytes 高 | 后台低优先级 compact |
| 写入高峰 | 抑制 compact |
| 读延迟上升 | 降低 vacuum 并发或延后 |

### 5.4 lazy materialize 指标化

把 `[LAZY_MATERIALIZE]` stderr 告警升级为 runtime stats：

- `lazy_materialize_count`
- `lazy_materialize_rows_total`
- `lazy_materialize_max_rows`
- `lazy_materialize_elapsed_ms`

### 5.5 验收标准

- runtime report 能看到表级 fragmentation/debt。
- soak 后 `compact_debt_bytes` 可解释且不会持续无界增长。
- CI 能阻止大表路径发生意外 lazy materialize。

---

## 6. 工程包 D：查询优化器与统计信息

### 6.1 目标

把 WHERE 计划从硬编码启发式升级为统计驱动的轻量 cost model。

### 6.2 新增统计结构

建议新增：

```cpp
struct ColumnStats {
    std::uint64_t non_null_count;
    std::uint64_t distinct_count;
    std::string min_value;
    std::string max_value;
    std::vector<std::pair<std::string, std::uint64_t>> top_values;
};

struct TableStats {
    std::uint64_t row_count;
    std::unordered_map<std::string, ColumnStats> columns;
    std::uint64_t stats_lsn;
    std::uint64_t schema_version;
};
```

### 6.3 替换硬编码选择率

当前类似 `Eq = 0.08`、`Contains = 0.35` 的估算应逐步替换为：

| 条件 | 估算方式 |
|------|----------|
| `id = x` | 0 或 1 |
| primary key eq | 0 或 1 |
| 普通列 eq | `row_count / distinct_count` |
| top-k 热点值 | 使用实际 top-k 频率 |
| range | 使用 min/max 或 histogram |
| unknown | fallback heuristic |

### 6.4 EXPLAIN / QUERY_PLAN

建议新增轻量 explain 输出：

```text
[QUERY_PLAN] mode=and_seed_filter seed=age op=Eq est=120 actual=98 index=eq_sidecar
```

后续可升级为正式命令：

```text
EXPLAIN WHERE users age = 18 AND city = beijing
```

### 6.5 深分页优化

建议引入 keyset pagination：

```text
PAGE users ORDER BY age,id AFTER age=30,id=10000 LIMIT 100
```

避免深 OFFSET 造成不稳定扫描成本。

### 6.6 验收标准

- skewed data 下 seed condition 选择优于硬编码策略。
- fallback scan ratio 可观测且有 CI gate。
- 查询 p95 与 rows scanned 有明确 runtime stats。

---

## 7. 工程包 E：IndexCatalog 与 sidecar 生命周期

### 7.1 目标

统一 eq、covering、page、visibility 等 sidecar 的元数据、失效、恢复与重建策略。

### 7.2 IndexDescriptor

**主干状态**：**`IndexKind` / `IndexDescriptor`**、头行尾缀 **`index_catalog_sidecar_meta_suffix`**（含可选 **`tbl_n`/`inx_n`**）与 **`NEWDB_INDEX_CATALOG_ENFORCE`** 已落地（见 [`cli/modules/sidecar/common/index_catalog.h`](../../cli/modules/sidecar/common/index_catalog.h)）。下列为设计对照。

```cpp
enum class IndexKind {
    Eq,
    Range,
    Covering,
    PageOrder,
    Visibility,
};

struct IndexDescriptor {
    std::string table_name;
    std::string index_name;
    std::vector<std::string> columns;
    IndexKind kind;
    std::uint64_t data_lsn;
    std::uint64_t schema_version;
    std::uint64_t built_at_ms;
    bool valid;
};
```

### 7.3 统一失效规则

以下操作必须集中触发 sidecar invalidation：

- `INSERT`
- `UPDATE`
- `DELETE`
- `DEFATTR` / schema change
- vacuum/compact
- WAL recovery
- table rename/drop
- lazy materialize rewrite

建议把失效入口集中到：

- `cli/shell/dispatch/services/sidecar_invalidation`
- 或 `cli/modules/sidecar/common`

### 7.4 range-capable index

中期把 equality sidecar 扩展到 range-capable index，支持：

- `=`
- `IN`
- `>` / `>=`
- `<` / `<=`

这样可以明显减少范围查询 fallback scan。

### 7.5 验收标准

- schema version 不匹配时 sidecar 不会被误用。
- data LSN 落后时 sidecar 会自动失效或重建。
- WAL recovery 后 sidecar 状态可解释。
- vacuum/compact 后 page/visibility sidecar 不 stale。

---

## 8. 工程包 F：runtime stats、CI 与可观测性

### 8.1 目标

把性能退化从“人工观察”变成“CI 自动阻断”。

### 8.2 建议新增 query/cache stats

建议加入 runtime report：

- `query_count`
- `query_p95_ms`
- `query_max_ms`
- `query_rows_scanned_total`
- `query_rows_returned_total`
- `query_fallback_scan_count`
- `query_eq_sidecar_hit_count`
- `query_id_pk_hit_count`
- `query_cache_hit_count`
- `query_cache_miss_count`
- `heap_decode_cache_hit_count`
- `heap_decode_cache_miss_count`

### 8.3 建议新增 CI gates

| Gate | 目的 |
|------|------|
| fallback scan ratio gate | 防止查询优化退化 |
| recovery elapsed gate | 防止 WAL 恢复变慢 |
| lazy materialize gate | 防止大表意外物化 |
| sidecar stale gate | 防止索引与数据版本不一致 |
| vacuum debt gate | 防止长期空间膨胀（🟡 **可选**：`--max-table-storage-health-dead-bytes` 等代理门 + **`compact_debt_bytes`** 专名仍待对齐） |
| p95 write stage gate | 防止写路径尾延迟回退 |

### 8.4 验收标准

- `validate_runtime_stats.py` 能校验新增字段。
- `ci_bench_gate.py` 能对关键指标设阈值。
- ReleaseGrade 不允许跳过关键语义与性能门。

---

## 9. 90 天执行计划

> **说明**：下列仍为「90 天路线图」节奏；与文首 **「实施进度一览」** 对照可知：第 1 阶段所列观测与契约项**已在主干大部落地**；WAL **recover** 已拆分；**TableStats** 可选落盘（**`.tablestats`**）；sidecar 头行含 **FNV + `idx_*`**，并可选 **`tbl_n`/`inx_n`** 明文与 **`NEWDB_INDEX_CATALOG_ENFORCE`**；**`TableStorageHealth`** 与 runtime **`table_storage_health_*`**、**`vacuum_health_bonus_last`**；vacuum 队列按 **heap 字节 + 可选 health bonus**；**`ci_bench_gate.py`** 可透传 **`newdb_runtime_report`** 的 **health/debt 代理**三门禁（见 **`STORAGE_GOVERNANCE_AND_RECOVERY_BUDGETS.md`** 第 4 节）；**`checkpoint_midpoint_recovery_count`**；**`test_wal_recovery_indexed.CheckpointBetweenBeginEndReplaysCommittedInsert`**；**`NEWDB_WHERE_HEAP_SCAN_BUDGET_ROWS`**。后续缺口：**page cache / memory budget / keyset**、**`compact_debt_bytes`** 专名与 soak 默认阈值、recovery **独立类**进一步拆分、checkpoint **默认剪枝**与未闭合 begin/end **严格**语义等。

### 第 1 阶段：第 1-2 周，契约与观测先行

交付：

1. 事务隔离测试矩阵。（✅）
2. `NEWDB_TXN_TRACE` 或等价事务可见性调试输出。（✅）
3. 查询 plan counters 和 runtime stats。（✅）
4. lazy materialize runtime stats。（✅）

验收：

- 当前语义被测试固定。
- 查询路径能看到 id/pk/sidecar/fallback/cache 的比例。
- 大表意外物化不再只是 stderr 告警。

### 第 2 阶段：第 3-6 周，查询与索引结构优化

交付：

1. `TableStats` / `ColumnStats` 最小版本。（✅ 含可选 **`.tablestats`** 持久化）
2. 统计驱动 `estimated_cond_cost`。（🟡 部分：`estimate_scan_rows` 等值分支）
3. `IndexCatalog` 最小版本。（✅ descriptor + 头行元数据 + **`NEWDB_INDEX_CATALOG_ENFORCE`** + 可选明文 **`tbl_n`/`inx_n`**）
4. sidecar schema version / data LSN 校验。（🟡 **`idx_sv`/`idx_dl`** 与失效逻辑；持续收敛边界用例）

验收：

- skewed distribution microbench 能选出更优 seed。
- stale sidecar 不会被误用。
- fallback scan ratio 有基线和门禁。

### 第 3 阶段：第 7-10 周，WAL recovery 与 storage health

交付：

1. `WalSegmentScanner` / `WalRecordReader` / `WalRedoPlanner` / `WalRedoApplier` 拆分。（🟡 仅 Scanner；余者待办）
2. checkpoint payload 消费 `snapshot_lsn`。（🟡 **`checkpoint_snapshot_lsn`** + opt-in 恢复起点；缺**默认**剪枝）
3. `TableStorageHealth`。（✅ §5.2 级字段 + runtime 导出；**`last_vacuum_*`** 仍为预留）
4. vacuum debt 调度。（🟡 队列 **heap 字节** + 可选 **health bonus**；**CI** 可选 **dead_bytes / bonus / fragmentation** 门；缺 **`compact_debt_bytes`** 专名）

验收：

- recovery 行为不变但结构更清晰。
- checkpoint 后恢复扫描量下降。
- soak 中 compact debt 可解释。

### 第 4 阶段：第 11-13 周，缓存与长期治理

交付：

1. 基础 page cache / decode cache 统一统计。（🟡 `heap_decode_slot_*` 运行态；缺统一 cache 层）
2. memory budget 雏形。（⏳）
3. deep pagination/keyset pagination 方案或最小实现。（⏳）
4. ReleaseGrade runtime gates。（🟡 **`verify_clean_reconfigure -ReleaseGrade`** 传 **`--release-grade`** + lazy 硬门；带 **`--runtime-jsonl`** 时可叠 health/debt 门；缺 recovery/cache **全矩阵**）

验收：

- 大表查询 p95 更稳定。
- cache hit/miss 可观测。
- ReleaseGrade 能覆盖语义、恢复、查询退化、存储治理。

---

## 10. 推荐提交顺序

### PR 1：事务隔离测试矩阵

- 新增隔离级别行为测试。
- 更新 `TXN_ISOLATION_AND_LOCKING.md`。
- 明确 `ReadCommitted` 与 `Snapshot` 的 expected behavior。

### PR 2：查询 trace/runtime stats 正式化

- 把现有 query trace 的关键信息纳入 runtime report。
- 增加 plan counters。
- 更新 runtime stats schema 与验证脚本。

### PR 3：lazy materialize 与 fallback scan gate

- 新增 lazy materialize counters。
- 新增 fallback scan ratio gate。
- 更新 `PERF_AND_CI_BUDGETS.md`。

### PR 4：TableStats / ColumnStats 最小实现

- 增加统计信息构建与持久化。
- 查询 seed 选择使用 NDV/top-k。
- 增加 skewed data microbench。

### PR 5：IndexCatalog 最小实现

- **`IndexDescriptor`** 与头行 **`idx_*` + FNV**，可选 **`tbl_n`/`inx_n`** 明文。
- sidecar 统一记录 schema / data 指纹与 **`wal_lsn`**（**`idx_sv`/`idx_dl`**）。
- 集中 invalidation；**`NEWDB_INDEX_CATALOG_ENFORCE`** 与 **`test_index_catalog`**。

### PR 6：WAL recovery 拆分

- 行为不变重构。
- 保持 WAL 测试全绿。
- 增加 recovery phase stats。

### PR 7：storage health 与 vacuum debt

- 表级健康度（**`TableStorageHealth`** + runtime 导出）。
- debt score（heap 字节 + 可选 **health bonus**）。
- vacuum 调度策略升级；**CI**：`newdb_runtime_report` health/debt 代理门（可选阈值）。

---

## 11. 分阶段代码修改计划

本节把上面的 PR 顺序进一步拆成可以逐步落地的代码修改阶段。每个阶段都遵循：**先测试/观测，后切换行为；先旁路接入，后默认启用；先保持外部接口稳定，后内部重构**。

### 阶段 0：建立修改保护线

**进度**：✅ 已完成（文首进度表）。

**目标**：在开始核心逻辑改造前，确认当前构建、测试和文档基线。

**建议修改**：

- 不改核心行为。
- 只补充阶段计划文档与必要 TODO 注释。
- 确认当前可用构建目录与 `ctest` 入口。

**涉及文件**：

- `docs/roadmap/OPTIMIZATION_PLAN_2026.md`
- `docs/dev/BUILD.md`
- `docs/ci/PERF_AND_CI_BUDGETS.md`

**验收**：

- 文档无诊断错误。
- 明确后续每阶段的测试命令和回滚点。

### 阶段 1：事务隔离测试矩阵先行

**进度**：✅ 已完成：`tests/test_txn_isolation_visibility.cpp`；无 WAL append 的用例需手动固定 `snapshot_lsn` 以贴合 `MVCCManager::create_snapshot` 与 WAL high-water 的关系。

**目标**：先固定预期语义，不急于一次性大改实现。

**代码修改**：

1. 新增隔离语义 GTest：
   - `tests/test_txn_isolation_visibility.cpp`
2. 在 `CMakeLists.txt` 中注册新测试目标。
3. 更新隔离文档，明确哪些测试当前应通过，哪些是目标行为。

**涉及文件**：

- `tests/test_txn_isolation_visibility.cpp`
- `CMakeLists.txt`
- `docs/txn/TXN_ISOLATION_AND_LOCKING.md`
- `cli/modules/txn/coordinator/txn_manager.h`

**建议测试用例**：

- `TxnIsolation.NoDirtyRead`
- `TxnIsolation.ReadCommittedSeesCommittedBetweenStatements`
- `TxnIsolation.SnapshotDoesNotSeeCommittedAfterBegin`
- `TxnIsolation.SnapshotRepeatableReadStable`
- `TxnIsolation.WriteConflictReject`
- `TxnIsolation.WriteConflictWaitTimeout`

**验收**：

- 当前已实现语义对应测试通过。
- 尚未实现的目标语义必须有清晰注释，不允许静默误判为已支持。
- 文档说明与测试名称一致。

**回滚点**：

- 如果新增测试影响主线 CI，可先把目标行为测试标记为 disabled 或放入扩展矩阵，但保留文档契约。

### 阶段 2：事务 read snapshot 接入

**进度**：✅ 已完成：`syncHeapReadSnapshotForQuery`（`wal_service.cc`）、`HeapReadViewGuard`（`shell_state.h`）、查询 handler 接入；**集成级**「RC 与 Snapshot 在真实 CLI 并发读写下差异」仍可追加小测。

**目标**：让 `TxnIsolationLevel` 真实影响读路径。

**代码修改**：

1. 在 `TxnCoordinator` 中保存事务级 read snapshot。
2. `Snapshot`：事务 begin 时创建并固定 snapshot。
3. `ReadCommitted`：每条查询语句开始时创建 statement snapshot。
4. 查询执行入口将 snapshot 设置到 `HeapTable::active_snapshot`，结束后按需清理。
5. 增加 `NEWDB_TXN_TRACE=1` 输出可见性调试信息。

**涉及文件**：

- `cli/modules/txn/coordinator/txn_manager.h`
- `cli/modules/txn/coordinator/*.cc`
- `engine/include/newdb/mvcc.h`
- `engine/src/mvcc/*`
- `engine/include/newdb/heap_table.h`
- `engine/src/heap_table*.cc`
- `cli/modules/where/executor/*`
- `cli/shell/dispatch/handlers/txn/txn_handler.cc`

**验收**：

- 阶段 1 中 `ReadCommitted` 与 `Snapshot` 行为测试通过。
- `NEWDB_TXN_TRACE=1` 能输出 isolation level、snapshot 标识和 visibility decision。
- 默认关闭 trace 时无额外 stderr 噪音。

**回滚点**：

- 保留旧路径开关，例如 `NEWDB_TXN_ISOLATION_READPATH=0`，便于定位回归。

### 阶段 3：查询 plan counters 与 runtime stats

**进度**：✅ 已完成；对外 JSON 字段前缀为 **`where_query_*`**（与早期草案中的 `query_*` 命名并存时以 schema 为准）。

**目标**：把现有查询 trace 从 stderr 调试升级为结构化统计。

**代码修改**：

1. 在查询上下文或 runtime stats 中增加 plan counters：
   - `query_count`
   - `query_fallback_scan_count`
   - `query_eq_sidecar_hit_count`
   - `query_id_pk_hit_count`
   - `query_cache_hit_count`
   - `query_cache_miss_count`
   - `query_rows_scanned_total`
   - `query_rows_returned_total`
2. 在 `query_with_index` 的各 plan 分支累加 counters。
3. runtime report JSON 输出新增字段。
4. 更新验证脚本和 schema 文档。

**涉及文件**：

- `cli/modules/where/executor/plan/plan_impl.cc`
- `cli/modules/where/executor/internal/query_internal.h`
- `cli/modules/txn/coordinator/txn_manager.h`
- `scripts/validate/validate_runtime_stats.py`
- `scripts/validate/RUNTIME_STATS_SCHEMA.md`
- `docs/ci/PERF_AND_CI_BUDGETS.md`

**验收**：

- 单条件 `id` 查询能增加 id/pk counter。
- 普通等值查询命中 sidecar 时增加 eq sidecar counter。
- fallback scan 有明确计数。
- runtime stats validation 通过。

**回滚点**：

- counters 只读不影响查询结果；如有问题可只禁用 JSON 输出，不回滚执行器逻辑。

### 阶段 4：lazy materialize 指标化与 CI 软门

**进度**：✅ 已完成；PowerShell **`verify_clean_reconfigure.ps1 -ReleaseGrade`** 与 Python **`ci_bench_gate.py --release-grade`** 分工不同（前者禁 Skip，后者收紧 lazy 运行时阈值）。

**目标**：把大表意外物化从 stderr 告警升级为可检测指标。

**代码修改**：

1. 在 runtime stats 中增加：
   - `lazy_materialize_count`
   - `lazy_materialize_rows_total`
   - `lazy_materialize_max_rows`
   - `lazy_materialize_elapsed_ms`
2. 在 `newdb_materialize_heap_if_lazy` 路径累加统计。
3. `ci_bench_gate.py` 增加可选阈值参数。
4. PR 阶段默认软门，ReleaseGrade 阶段硬门。

**涉及文件**：

- `cli/shell/state/shell_state.h`
- `cli/modules/txn/coordinator/txn_manager.h`
- `scripts/ci/ci_bench_gate.py`
- `scripts/validate/validate_runtime_stats.py`
- `scripts/validate/RUNTIME_STATS_SCHEMA.md`
- `docs/storage/STORAGE_GOVERNANCE_AND_RECOVERY_BUDGETS.md`

**验收**：

- 大表写路径触发 materialize 时 counters 增加。
- CI 可通过参数限制最大 materialize rows 或次数。
- 默认行为不改变。

### 阶段 5：TableStats / ColumnStats 最小版本

**进度**：🟢 **已完成（路线图当前范围）**：内存扫描构建、`plan_impl` 等值分支、`NEWDB_QUERY_USE_TABLE_STATS=0` 回退；**`NEWDB_QUERY_PERSIST_TABLE_STATS=1`** 时读写 **`<data>.bin.tablestats`**（schema fingerprint，见 `table_stats.{h,cc}`）；失效挂钩 **`sidecar_invalidate_all_indexes_for_data_file`** 与 **`compact_table_file_default`**；`test_query_table_stats` 持久化 roundtrip；`test_ci_microbench` 高基数选择性用例。

**与评估 §6.2 对齐的最小切片（histogram）**：完整 equi-depth **列直方图**仍列为 backlog；当前契约以 **列级 NDV**（及 min/max 等 `TableStats` 字段）支撑等值/范围启发。若后续引入直方图，建议以 **`.tablestats` 可选扩展字段** 渐进落地，并保持 `validate_runtime_stats` / 旧文件 **向后兼容**。

**目标**：为查询优化器提供轻量统计信息，替换部分硬编码选择率。

**代码修改**：

1. ~~新增统计结构与构建函数。~~（已有）
2. ~~支持对已加载 `HeapTable` 扫描生成 stats。~~（已有）
3. 在 `estimated_cond_cost` / `estimate_scan_rows` 中优先使用 stats。（**部分路径**已由 `plan_impl` 消费 `query_stats_hint`）
4. 没有 stats 时回退现有 heuristic。（**已有**）
5. ~~增加 skewed / 高基数 microbench。~~（**已有** `CiMicrobench.HighCardinalityColumnYieldsLowEqSelectivity`）

**建议新增文件**：

- `cli/modules/where/executor/stats/table_stats.h`
- `cli/modules/where/executor/stats/table_stats.cc`
- `tests/test_query_table_stats.cpp`

**涉及文件**：

- `cli/modules/where/executor/plan/plan_impl.cc`
- `cli/modules/where/executor/internal/query_internal.h`
- `CMakeLists.txt`
- `tests/test_ci_microbench.cpp`

**验收**：

- 无 stats 时行为与当前一致。
- 有 stats 时等值条件估算使用 `row_count / distinct_count`。
- skewed data 下能选择更小候选集作为 seed。

**回滚点**：

- 用环境变量 `NEWDB_QUERY_USE_TABLE_STATS=0` 保留旧 heuristic。

### 阶段 6：IndexCatalog 最小接入

**进度**：🟢 **已完成（路线图当前范围）**：`IndexDescriptor`、`index_descriptor_matches_runtime`、**集中 invalidation**；equality / page / covering / visibility 构建路径在 **`wal_lsn`** 后追加 **`index_catalog_sidecar_meta_suffix`**（**`idx_kind` / `built_ms` / `idx_sv` / `idx_dl` / `tbl_fnv` / `inx_fnv`**），可选 **`tbl_n`/`inx_n`**（百分号编码明文表名/索引名，与 FNV 并存）；**`NEWDB_INDEX_CATALOG_ENFORCE`** 下 **`index_catalog_header_identity_ok`** 校验 **kind、指纹、FNV、明文**（旧头无 **`tbl_n`/`inx_n`** 时仅 FNV）；解析端对 **`wal_lsn`** 数字前缀读取兼容旧文件；**`test_index_catalog`** 覆盖 roundtrip 与 enforce。

**目标**：统一 sidecar 元数据，先做 stale detection，不急于重写所有索引实现。

**代码修改**：

1. ~~新增 `IndexKind` / `IndexDescriptor`。~~（已有）
2. sidecar build 时写入 schema version 和 data LSN。（**已有**：同上 + 可选 **`tbl_n`/`inx_n`**）
3. sidecar lookup 时先校验 descriptor。（**已有**：enforce 路径与 **`index_catalog_header_identity_ok`**）
4. ~~schema change、DML、vacuum、recovery 后集中 invalidation。~~（已有）

**建议新增文件**：

- `cli/modules/sidecar/common/index_catalog.h`
- `cli/modules/sidecar/common/index_catalog.cc`

**涉及文件**：

- `cli/modules/sidecar/eq/*`
- `cli/modules/sidecar/covering/*`
- `cli/modules/sidecar/page/*`
- `cli/modules/sidecar/visibility/*`
- `cli/shell/dispatch/services/sidecar_invalidation*`
- `cli/shell/dispatch/handlers/ddl/*`
- `cli/shell/dispatch/handlers/dml/*`
- `tests/test_index_catalog.cpp`

**验收**：

- schema version 不匹配时 sidecar 不被使用。
- data LSN 落后时 sidecar 自动失效或重建。
- vacuum/compact 后 page/visibility sidecar 不 stale。

**回滚点**：

- `NEWDB_INDEX_CATALOG_ENFORCE=0` 时只记录 descriptor，不阻断旧 sidecar 使用。

### 阶段 7：WAL recovery 行为不变重构

**进度**：🟢 **已完成（本路线图意义上的「行为不变拆分」）**：`WalManager::recover` 仍对外保持原签名；实现上已拆为 **`recover_build_segment_index`**（第一遍建段索引 + `checkpoint_scan_ms` 边界内目录清单）与 **`recover_replay_segments`**（第二遍读校验与 `WalOp` 重做），代码位于 **`engine/src/wal/recovery/wal_manager_recover_support.cpp`**；与 Reader/Planner/Applier 的**职责映射**见该文件顶部注释。原有 **`WalSegmentScanner`**（`list_wal_segment_paths`）继续用于清单统计。

**目标**：拆分恢复内部结构，但保持外部接口和测试行为不变。

**代码修改**：

1. ~~提取 `WalSegmentScanner`。~~（已有）
2. 将第一遍 / 第二遍循环迁入独立编译单元（**已完成**：`wal_manager_recover_support.cpp`）。
3. 可选后续：`WalRecordReader` / `WalRedoPlanner` / `WalRedoApplier` **独立类**（非本轮强制）。
4. `WalManager::recover` 只保留流程编排（**已完成**）。

**建议新增 / 已有文件**：

- `engine/src/wal/recovery/wal_segment_scanner.h/.cpp`（已有）
- `engine/src/wal/recovery/wal_manager_recover_support.cpp`（**新增**，recover 主体 helper）
- （可选后续）`wal_record_reader` / `wal_redo_planner` / `wal_redo_applier` 独立头文件

**涉及文件**：

- `engine/src/wal/writer/wal_manager.cpp`
- `engine/src/wal/recovery/wal_manager_recover_support.cpp`
- `engine/include/newdb/wal_manager.h`
- `CMakeLists.txt`
- `tests/test_wal_*.cpp`

**验收**：

- WAL 全量测试通过。
- `WalManager::recover` 外部签名不变。
- recovery stats 不回退。

**回滚点**：

- 分组件提取必须小步提交；每次只移动一类逻辑，测试通过后再继续。

### 阶段 8：checkpoint payload 与恢复起点优化

**进度**：🟡 **部分完成**。**已落地**：CHECKPOINT_BEGIN/END 的 v1 payload 可选 **`checkpoint_snapshot_lsn`**；**`last_complete_checkpoint_lsn`**、**`NEWDB_RECOVER_USE_CHECKPOINT_LSN`** + **`NEWDB_RECOVER_ENABLE_OFFSET_SEEK`**（默认关）；**`redo_plan_ms` / `redo_apply_ms`**；**`checkpoint_midpoint_recovery_count`**（扫描尾 **`CHECKPOINT_BEGIN`** 未配对 END 时按 depth 递增，**不改变** **`last_complete_checkpoint_lsn`**）；**`test_wal_recovery_indexed.IncompleteCheckpointBeginWithoutEndCountsMidpoint`**；**`test_wal_recovery_indexed.CheckpointBetweenBeginEndReplaysCommittedInsert`**（**配对** BEGIN→INSERT→END 后 **recover** 可见数据）。测试见 `test_wal_recovery_indexed.cpp`、`test_wal_codec.cpp`。**仍未完成**：上述剪枝**默认开启**、**未闭合** begin/end（崩溃）的**严格**回退策略、更全 **`checkpoint_between_begin_end`** fault 矩阵、专用 **`test_wal_checkpoint*.cpp`**。

**目标**：让 checkpoint 真正成为 recovery replay 起点优化依据。

**代码修改**：

1. checkpoint begin/end payload 写入 snapshot（**已完成**：`checkpoint_snapshot_lsn` meta）。
2. recovery 扫描完整 checkpoint 边界并写入 stats（**已完成**：`last_complete_checkpoint_lsn`）。
3. **可选** env 下从完整 checkpoint 之后收紧重放起点并配合段级 seek（**已完成**，默认关闭）。
4. crash between begin/end 时回退到前一个完整 checkpoint。（**未做**）
5. recovery phase stats（**部分完成**：`redo_plan_ms` / `redo_apply_ms` 等）。

**涉及文件**：

- `engine/src/wal/writer/wal_manager.cpp`
- `engine/src/wal/codec/*`
- `engine/src/wal/recovery/*`
- `engine/include/newdb/wal_manager.h`
- `tests/test_wal_recovery_indexed.cpp`、`tests/test_wal_codec.cpp`（checkpoint 相关用例）；可选补充 `tests/test_wal_checkpoint*.cpp`

**验收**：

- `checkpoint_between_begin_end` fault injection 测试通过。
- recovery scanned records 在 checkpoint 后下降。
- `last_complete_checkpoint_lsn` / `replay_start_lsn` 输出正确。

### 阶段 9：storage health 与 vacuum debt

**进度**：🟡 **部分完成**：`measure_table_storage_health`（§5.2 级字段与 runtime 导出）、`vacuum_priority_score`；**`m_vacuum_queue`** 已存 **`(table_name, debt_score)`**，**`debt_score` 默认 heap `.bin` 字节数**，**`NEWDB_VACUUM_QUEUE_USE_HEALTH=1`** 时 lazy **`load_heap_file`** 后累加 **墓碑 slot × `NEWDB_VACUUM_HEALTH_SLOT_WEIGHT`（默认 65536）+ ratio×1e6**；**`vacuum_health_bonus_last`** 与 **`vacuum_priority_score_last`** 可观测。**CI**：**`ci_bench_gate.py`** 对 **`newdb_runtime_report`** 增加 **health/debt 代理** 可选门（**`--max-table-storage-health-dead-bytes`**、**`--max-vacuum-health-bonus-last`**，与既有碎片率门并用）；**`STORAGE_GOVERNANCE_AND_RECOVERY_BUDGETS.md`** 第 4 节描述 jsonl→校验→门禁闭环。**仍欠**：独立 **`compact_debt_bytes`** 指标名与 soak 默认硬门调参。

**目标**：把 vacuum 从触发式改造成可解释的 debt 调度。

**代码修改**：

1. ~~新增 `TableStorageHealth`。~~（已有雏形）
2. heap compact 后计算 reclaimed bytes、dead ratio、fragmentation。（**部分**由现有 compact 统计与 runtime 体现）
3. ~~vacuum queue 根据 `vacuum_priority_score` 排序或分级。~~（**部分完成**：按 **heap 文件字节** 优先；**`NEWDB_VACUUM_QUEUE_USE_HEALTH=1`** 时 debt 叠加 **墓碑/ratio** bonus）
4. runtime report 输出 compact debt。（**部分**）
5. CI 增加 soak/夜间可选 gate。（**部分**：`ci_bench_gate.py` 透传 **`--max-table-storage-health-fragmentation-ratio`** / **`--max-table-storage-health-dead-bytes`** / **`--max-vacuum-health-bonus-last`**；`verify_clean_reconfigure -ReleaseGrade` 传 **`--release-grade`**）

**涉及文件**：

- `cli/modules/txn/coordinator/vacuum/vacuum_service.cc`
- `cli/modules/txn/coordinator/txn_manager.h`
- `engine/include/newdb/heap_storage.h`
- `engine/src/io/page/*`
- `scripts/ci/ci_bench_gate.py`
- `tools/report/newdb_runtime_report.cpp`
- `docs/storage/STORAGE_GOVERNANCE_AND_RECOVERY_BUDGETS.md`

**验收**：

- vacuum priority 可解释。
- compact debt 不持续无界增长。
- 写入高峰时 vacuum 不明显放大 p95 延迟。

### 阶段 10：缓存与长期运行治理

**进度**：🟡 **部分完成**：运行态 **`heap_decode_slot_*`**、`vacuum_priority_score`、`ci_bench_gate.py --release-grade`；可选 **`NEWDB_WHERE_HEAP_SCAN_BUDGET_ROWS`**（**`where_policy_heap_scan_budget_rows`**）在 ratelimit 模式下收紧 **`where_policy_scan_cap_rows`** 的**有效上限**；**`Where.HeapScanBudgetTightensEffectiveScanCap`**（`test_demo_where.cpp`）。**未做**：统一 page cache / **引擎级** memory budget、keyset pagination、ReleaseGrade **全矩阵** gate。

**目标**：补齐大表和长期运行下的稳定性基础。

**代码修改**：

1. 统一 heap decode cache stats。
2. 引入最小 page cache / memory budget。
3. 支持 keyset pagination 的语法或内部 API。
4. ReleaseGrade gate 覆盖 query/recovery/vacuum/cache。

**涉及文件**：

- `engine/include/newdb/heap_table.h`
- `engine/src/io/page/*`
- `cli/modules/where/executor/*`
- `cli/shell/dispatch/handlers/query/*`
- `scripts/ci/verify_clean_reconfigure.ps1`
- `scripts/ci/ci_bench_gate.py`

**验收**：

- 大表 PAGE/WHERE p95 更稳定。
- cache hit/miss 可观测。
- ReleaseGrade 能阻断核心退化。

### 推荐实际落地顺序

如果按最小风险推进，建议顺序如下：

1. 阶段 1：事务隔离测试矩阵。
2. 阶段 3：查询 plan counters 与 runtime stats。
3. 阶段 4：lazy materialize 指标化。
4. 阶段 2：事务 read snapshot 接入。
5. 阶段 5：TableStats / ColumnStats。
6. 阶段 6：IndexCatalog。
7. 阶段 7：WAL recovery 拆分。
8. 阶段 8：checkpoint 恢复起点优化。
9. 阶段 9：storage health 与 vacuum debt。
10. 阶段 10：缓存与长期运行治理。

这个顺序的原则是：先增加观测和测试保护，再修改核心行为；先做旁路统计，再切换优化策略；先做行为不变重构，再做恢复语义增强。

---

## 12. 风险与缓解

| 风险 | 缓解 |
|------|------|
| 隔离语义改动影响现有测试 | 先写测试矩阵和文档，分阶段启用 expected behavior |
| 统计信息维护增加写路径成本 | 初期采用手动/后台 analyze，不在每次写入同步更新完整统计 |
| IndexCatalog 引入后 sidecar 行为变化 | **`NEWDB_INDEX_CATALOG_ENFORCE=0`** 保留宽松路径；enforce 分类型小步启用 |
| WAL recovery 拆分引入回归 | 行为不变重构，先保持 `recover` 外部接口不变 |
| vacuum debt 调度误伤写入延迟 | 加 runtime gate 与可配置开关，默认保守策略 |
| CI gate 过严导致开发受阻 | PR gate 使用软阈值，ReleaseGrade 使用硬阈值 |

---

## 13. 最终里程碑定义

### M1：语义可验证

- 事务隔离行为有测试。（🟡：引擎 GTest + CLI 读路径 snapshot；**非**完整 SQL 隔离）
- 写冲突、等待、deadlock victim 可观测。
- 文档与实际行为一致。（🟡：`TXN_ISOLATION_AND_LOCKING.md` 已同步读路径开关；细粒度仍随阶段 2 收尾增强）

### M2：性能可解释

- 查询计划、fallback scan、sidecar 命中、cache hit/miss 可观测。（🟡：`where_plan_*` / `where_fallback_scans` / `heap_decode_slot_*` 等已导出；可选 **`NEWDB_WHERE_HEAP_SCAN_BUDGET_ROWS`** 收紧 ratelimit **有效 scan cap**；统一 decode cache 层仍缺）
- WAL recovery phase stats 可观测。（🟡：`WalRecoveryStats` 含 `checkpoint_scan_ms`、`replay_start_lsn`、`redo_plan_ms`/`redo_apply_ms`、`last_complete_checkpoint_lsn` 等；与「独立 Reader 类 + 默认 checkpoint 剪枝」仍有差距）
- lazy materialize 和 vacuum debt 可观测。（🟡：`lazy_materialize_*`、`vacuum_priority_score`、**`vacuum_health_bonus_last`**、**`table_storage_health_*`**；队列 **heap 字节 + 可选 health**；**CI** 可选 **`newdb_runtime_report`** 三门禁；**仍欠** **`compact_debt_bytes`** 专名与默认 soak 阈值）

### M3：结构可演进

- 查询优化器开始使用统计信息。（🟡：可选 **`NEWDB_QUERY_USE_TABLE_STATS`** + 可选 **`NEWDB_QUERY_PERSIST_TABLE_STATS`** 落盘 **`.tablestats`**）
- sidecar 统一 catalog。（🟢：descriptor + 集中 invalidation + **`NEWDB_INDEX_CATALOG_ENFORCE`** + **`idx_*` + FNV** + 可选 **`tbl_n`/`inx_n`** 明文）
- WAL recovery 拆分。（🟡：`wal_segment_scanner` + **`wal_manager_recover_support` 双阶段**；独立四类组件类仍欠）
- vacuum 调度基于 health/debt。（🟡：**heap 字节** + 可选 **health bonus**；runtime **health** 字段与 **CI** 可选门已接；**仍欠** **`compact_debt_bytes`** 与默认 soak 预算）

### M4：长期运行可控

- soak 后空间膨胀、恢复耗时、查询 p95、写路径 p95 都有预算。（🟡：多项 runtime JSON + bench/report 可选阈值；**缺**全套 soak 预算闭环）
- ReleaseGrade gate 能阻断核心退化。（🟡：verify 脚本禁 Skip + bench `--release-grade` 收紧 lazy；**缺** recovery/cache 全覆盖）
- `newdb` 可稳定支撑可控负载下的长期单机运行。（⏳）
