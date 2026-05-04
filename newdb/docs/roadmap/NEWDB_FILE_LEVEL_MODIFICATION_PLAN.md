# newdb 文件级修改方案

本文直接面向 `newdb` 当前代码树，给出“具体到文件级别”的后续修改方案。目标不是一次性把系统改成完整工业级数据库，而是优先补齐长期运行、恢复、隔离、查询规划、内存治理和 CI 门禁的工程闭环。

## 0. 总体优先级

| 优先级 | 主题 | 目标 | 主要影响文件 |
|---|---|---|---|
| P0 | 基线与门禁闭环 | 先保证每次修改可度量、可回归 | `scripts/ci/*`、`tools/report/*`、`.github/workflows/newdb-ci.yml` |
| P1 | 事务隔离与锁语义收敛 | 避免读路径、写冲突、跨入口语义分叉 | `txn_manager.h`、`wal_service.cc`、`query_handler.cc`、`c_api.cpp` |
| P1 | WAL 恢复模块化与故障覆盖 | 降低大 WAL 恢复风险，明确 checkpoint 行为 | `wal_manager.cpp`、`wal_manager_recover_support.cpp`、`wal_segment_scanner.cpp` |
| P2 | 查询规划与统计升级 | 从启发式索引选择升级到可解释轻量优化器 | `plan_impl.cc`、`table_stats.*`、`index_catalog.*` |
| P2 | 存储治理与内存预算统一 | 将 PageCache、sidecar、query 临时结构纳入统一预算 | `page_cache.*`、`eq_sidecar.*`、`vacuum_service.cc` |
| P3 | GUI/C API 契约补强 | 保证 CLI/C API/GUI 错误与读路径一致 | `c_api.cpp`、`rust_gui/src-tauri/src/lib.rs`、`App.vue` |

### 0.1 完成度图例与总览（跟踪用）

| 标记 | 含义 |
|------|------|
| 🟢 | 已在当前代码树落地，达到本节「修改建议」的主要可验收子集 |
| 🟡 | 部分落地：关键子项已完成，其余仍按原文排期 |
| ⚪ | 尚未在本轮落地，仍以原文为准 |

**快照日期：2026-05-04**（与仓库实际不一致时请更新本表及各节说明。）

**本次修订**：在 Phase2 基线之上对齐 **Phase 3**——WAL **`NEWDB_WAL_CHECKPOINT_PRUNE` / `NEWDB_WAL_CHECKPOINT_STRICT`**、`WalCheckpointTruncateTrace`、`WalRecoveryStats` 增补 incomplete / offset-seek fallback / `recovery_uncommitted_records_ignored`；**`memory_budget.*`** + **`memory_budget_bytes_evicted_total`**；**`NEWDB_TABLESTATS_V2`**（min/max/top-k）与 **`where_build_plan_candidates`**；SHOW PLAN JSON **`plan_candidates`** / **`table_stats_stale`**；runtime **`transaction_snapshot_lsn` / `statement_snapshot_lsn`**、**`table_storage_health_tier`**；CI **`linux-release-gate`**；GUI **`pagePolicy.ts` / `commandPolicy.ts`**；校验脚本与 **`RUNTIME_STATS_SCHEMA.md`** 已含上述键。**闭环增量（Phase4 文档跟踪）**：**`TXN_BEGIN`** 追加 **`SESSION_SNAPSHOT`** 以保证 Snapshot `BEGIN` 钉扎非零 LSN；**`test_wal_segment_scanner` / `test_txn_isolation_visibility` / `test_query_table_stats` / `test_eq_sidecar_bucket`** 扩展；eq sidecar **`memory_budget_sidecar_load_skipped_total`**；**`PlanCost.estimated_rows`** + SHOW PLAN **`cost`** 对象；**`save_table_stats_file`** tmp→rename；**`nightly_soak_hints` / `StorageSoakHeavy` / `capture_baseline`** 与 GUI **`guiPackageKind`** 诊断。**Phase4 收口（本轮）**：`test_wal_recovery_indexed`（clean EOF `partial_tail`、坏尾魔数恢复）、`test_wal_segment_scanner`（字典序多文件）、`test_txn_shell_multi_entry_snapshot`（COUNT/PAGE/WHERE/FIND + `SHOW TUNING JSON` 钉扎 LSN）、`test_show_plan_table_stats_stale_shell`（`NEWDB_QUERY_USE/PERSIST_TABLE_STATS` + 坏 stats 文件 → `table_stats_stale:true`）、`where_heap_scan_budget_binding_events`（`policy_service` + SHOW TUNING JSON + `validate_runtime_stats` / `c_api`）、`capture_baseline.py` 各 profile 的 **`recommended_ctest_args` / `recommended_ctest_cli`**；`nightly_soak_hints` / `ci_bench_gate --runtime-jsonl` 与 **`NEWDB_SOAK_HINT_JSONL`** 契约说明。**Phase5（全体闭环对齐 §11）**：**`ci_bench_gate`** 全阶段 **`gate-fail-json-out`**；**`capture_baseline`** **`bench_gate_profile` / `recommended_ci_bench_gate_cli`**；PR workflow **`--profile pr`** + fixture **`validate_runtime_stats`**；**§3.3** 明确 **不**导出 recover 子阶段 API；**`rust_gui`** Export 弹窗 **`RUNTIME_TUNING_DIAGNOSTIC_GROUPS`** 与 schema 对齐清单。

| 章节 | 状态 | 摘要 |
|------|------|------|
| §1 基线 / CI / `newdb_runtime_report` | 🟡 | 同上；**已**在 PR 主线（根目录与 `newdb/.github`）bench gate 使用 **`--profile pr`**，可选 **`--gate-fail-json-out`** 覆盖 smoke/ctest/perf/pressure/runtime 各阶段；**`capture_baseline.py`** manifest 含 **`bench_gate_profile`** 与 **`recommended_ci_bench_gate_cli`**（契约 JSONL + `--runtime-last-n 2`）；**`linux-bench-gate-runtime-contract`** 与 **`validate_runtime_stats`** fixture 步保留；**未**全自动跨机 profile 数值基线矩阵 |
| §2 事务 / 锁 / 读路径 | 🟡 | 同上 + **`transaction_snapshot_lsn` / `statement_snapshot_lsn`**（`syncHeapReadSnapshotForQuery` 写入 runtime/C API）；**未**再拆更多隔离级别矩阵 |
| §3 WAL 恢复 | 🟡 | `WalRecoveryStats`：`recovery_policy`（含 `incomplete_checkpoint_tail` / `offset_seek_fallback`）、`incomplete_checkpoint_count`、`recovery_uncommitted_records_ignored`、`offset_seek_fseek_fallback_count`；`NEWDB_WAL_CHECKPOINT_PRUNE` / `NEWDB_WAL_CHECKPOINT_STRICT` + `WalCheckpointTruncateTrace` / `last_checkpoint_truncate_trace()`；segment 索引扫描阶段注释对齐 doc；**未**把 redo plan 拆成独立可导出 API |
| §4 Heap / PageCache | 🟡 | 同上 + eq **磁盘加载**在 cap 下软跳过（**`memory_budget_sidecar_load_skipped_total`**）；**已**增 **`where_heap_scan_budget_binding_events`**（`NEWDB_WHERE_HEAP_SCAN_BUDGET_ROWS` 收紧 ratio cap 时的观测计数）；**未**做 `heap_table` 诊断 API / query 临时结构统一硬 cap |
| §5 规划 / stats / sidecar | 🟡 | 同上 + **`PlanCost.estimated_rows`** 与 SHOW PLAN **`cost`**；**`save_table_stats_file`** tmp→rename；单测 **stats stale** / **eq 预算**；**已** Shell 级 **`test_show_plan_table_stats_stale_shell`**（`table_stats_stale:true` 端到端）；**未**OR/histogram 与 GUI 全量结构化 |
| §6 Vacuum / soak | 🟡 | 同上 + **`table_storage_health_tier`**（`good|watch|degraded|critical`）；**`StorageSoakHeavy`** 可选写 **`NEWDB_SOAK_HINT_JSONL`**；**已**在 `nightly_soak_hints.py` / `ci_bench_gate.py --runtime-jsonl` 说明与 **`validate_runtime_stats`** 行的合流约束；soak 仍 opt-in |
| §7 C API / GUI | 🟡 | 同上 + **`pagePolicy.ts`** / **`commandPolicy.ts`**；**`runtime_artifact_info`** 含 **`guiPackageKind`**（debug/release）；**未**全量 GUI 结构化消费 |
| §8 测试 / CMake | 🟡 | 统一 `LABELS newdb`；矩阵文档补充 **`ctest -L newdb`** 与 WAL/sidecar **`-R`** 组合示例；**已**增 **`test_txn_shell_multi_entry_snapshot` / `test_show_plan_table_stats_stale_shell` / `test_where_heap_scan_budget_binding`** 与 WAL/segment 扩展用例；**未**为单测文件自动附加多标签（仍推荐 `-R` 筛选） |
| §9 文档同步 | 🟡 | 校验脚本与 schema 已含 **`memory_budget_sidecar_load_skipped_total`**；其余同前；`NEWDB_OPTIMIZATION_*` / `STORAGE_GOVERNANCE_*` 未强制同步 |

---

## 1. 基线、运行态统计与 CI 门禁

**本节完成度：🟡**（见 §0.1）

### 1.1 `newdb/scripts/ci/capture_baseline.py`

**现状**：已支持 ctest、fixture 校验、归档 manifest；**已**提供 **`--workload-profile`**（`small|storage|query|recovery|all`）写入 manifest 与 workload hints；**`_workload_profile_hints`** 各 profile **已含** **`recommended_ctest_args`** 与可复制 **`recommended_ctest_cli`**（占位 `<BUILD_DIR>`）；**`--bench-gate-profile`** 控制内嵌 **`ci_bench_gate`** 的 **`--profile`** 并写入 **`emit-baseline-dir`** / **`--write-archive-manifest`** 的 **`bench_gate_profile`**；**`recommended_ci_bench_gate_cli`** 给出与 **`linux-bench-gate-runtime-contract`** 同构的契约 JSONL 门禁命令行；**`_relevant_env_keys`** 已含 **`NEWDB_MEMORY_BUDGET_MAX_BYTES`** 等；业务负载 JSONL 与跨环境数值基线仍可按 profile 固化。

**修改建议**：

1. ~~将 profile 与真实 ctest 筛选 / JSONL 采集命令更紧绑定（当前 hints 仍以文档级建议为主）。~~（**已**在 manifest / contract 的 profile hints 落地 CLI 片段；可按主机再调 `-R` 正则。）
2. 输出统一目录结构：
   - `baseline/runtime_stats.jsonl`
   - `baseline/runtime_report.json`
   - `baseline/ctest.log`
   - `baseline/manifest.json`
3. 在 manifest 中写入：编译器、构建类型、OS、CPU 核数、关键环境变量、Git commit、测试筛选表达式。

**验收**：同一命令在本机可生成完整 baseline 目录，且 `validate_runtime_stats.py` 能直接校验生成的 JSONL。

### 1.2 `newdb/scripts/ci/ci_bench_gate.py`

**现状**：**`--profile local|pr|nightly|release`** 与显式阈值覆盖并存；**`--gate-fail-json-out`** 在失败时写入统一 JSON（含 **`stage`**、`profile`、`resolved_build_dir`、`build_config` 及阶段相关字段），覆盖 **build 目录缺失、smoke、newdb_perf、ctest、pressure summary、runtime jsonl / `newdb_runtime_report`** 等路径；**`linux-bench-gate-runtime-contract`** 与根 PR workflow 已示例化输出路径。

**修改建议**（后续）：

1. 将 storage/WAL/query 三类阈值在文档 **`docs/ci/PERF_AND_CI_BUDGETS.md`** 中与 profile 矩阵进一步对齐。
2. 对以下指标按需增加默认软门或硬门：`wal_recovery_last_elapsed_ms`、`compact_debt_bytes_peak`、`page_cache_hit_ratio`、`where_scanned_returned_ratio`、`memory_budget_reject_count`。

**验收**：fixture JSONL、真实 runtime JSONL 均可通过同一脚本生成明确 gate 结果；失败时可选落盘 JSON 供 CI artifact 解析。

### 1.3 `newdb/tools/report/newdb_runtime_report.cpp`

**现状**：派生指标（`page_cache_hit_ratio`、`where_scan_amplification`、`wal_recovery_redo_ratio`、`memory_budget_reject_delta` 等）已在汇总路径落地；**`--json`** 与默认行为一致：**仅向 stdout 输出一行 summary JSON**（stderr 仍为 gate 失败信息）；`scripts/validate/test_runtime_report_compact_gates.py` 含 `--json` 成功路径与「单行 stdout」断言。

**修改建议**（仍待细化）：

1. 维持历史 JSONL 弱字段兼容。
2. 如需「human-readable 旁路输出」，另行约定 stderr 前缀或单独子命令，避免破坏「仅 JSON」契约。

### 1.4 `.github/workflows/newdb-ci.yml`

**现状**：仓库内两套入口——**`newdb/.github/workflows/newdb-ci.yml`**（完整矩阵：observability gates、**`linux-bench-gate-runtime-contract`**、**`linux-release-gate`**、Nightly soak 等）与**根目录** **`.github/workflows/newdb-ci.yml`**（`newdb/**` path 过滤的精简 gcc/clang）；PR 主线 bench gate **已**统一示例 **`--profile pr`** + **`--gate-fail-json-out`**，根 workflow **已**增加一步 **`validate_runtime_stats.py`** 跑 **`runtime_stats_bench_gate_minimal.jsonl`**。顶层 **`on`** 已增加 **`workflow_dispatch`** 与 **周 `schedule`**；**`linux-nightly-soak`**、**`linux-release-gate`**、**`linux-index-catalog-enforce`** 等保留。

**修改建议**（后续）：

1. 若需更严 **Release** 专属阈值或 artifact 命名与 contract job 完全同源，可在 **`linux-release-gate`** 上叠参数或拆第二个 Release-only gate。
2. 上传物维持：`runtime_stats` JSONL、runtime report、`baseline manifest`、`runtime_gate_fail.json` 等。

---

## 2. 事务隔离、锁与读路径一致性

**本节完成度：🟡**（见 §0.1）

### 2.1 `newdb/cli/modules/txn/coordinator/txn_manager.h`

**现状**：已有 `TxnIsolationLevel::ReadCommitted` / `Snapshot`、write intent、runtime stats；**已**在 **`TxnRuntimeStats` / C API / SHOW TUNING JSON** 中导出 **`transaction_snapshot_lsn`**（Snapshot 事务钉扎读视图时的 LSN）与 **`statement_snapshot_lsn`**（语句级刷新时的 LSN），并与 **`last_snapshot_source`**（`none|txn|statement|disabled`）同源逻辑（见 **`wal_service.cc`** **`syncHeapReadSnapshotForQuery`**）。语义仍是有限 MVCC 子集。

**修改建议**：

1. 继续细化文档与测试矩阵（CLI PAGE/WHERE/COUNT/FIND 一致性交叠）；**`test_txn_isolation_visibility`** 已覆盖 **active txn** 下 RC/Snapshot 与 **`syncHeapReadSnapshotForQuery`** 的 LSN 导出。
2. **已**在 **`TxnRuntimeStats` / SHOW TUNING / C API** 中导出 **`txn_snapshot_refresh_count`**、**`txn_snapshot_pinned_count`**、**`txn_readpath_disabled_count`**（与 **`wal_service.cc`** 读路径一致）。
3. 将 `LockKey` 的结构化字段继续向二级索引/范围锁预留：
   - `table`
   - `kind=row|index|range|predicate`
   - `key_begin`
   - `key_end`

**验收**：`SHOW TUNING` 与 C API JSON 能看到读视图来源和刷新次数。

### 2.2 `newdb/cli/modules/txn/coordinator/wal/wal_service.cc`

**现状**：**`syncHeapReadSnapshotForQuery`** 统一设置 **`HeapTable`** snapshot 与 **`last_snapshot_source`**；**`NEWDB_TXN_ISOLATION_READPATH=0`** 路径递增 **`txn_readpath_disabled_count`**（见单测）；**`TXN_BEGIN`** 在写 WAL 时追加 **`SESSION_SNAPSHOT`**，使空 WAL 上 Snapshot **`BEGIN`** 仍能钉扎非零 LSN（与 recover 对 **`SESSION_SNAPSHOT`** 的 no-op 重放一致）。

**修改建议**：

1. 审计各查询 handler 是否**仅**经 **`HeapReadViewGuard`** 间接调用上述逻辑，无自拼 snapshot。
2. Snapshot 非 active 时的 statement 退化与 **`NEWDB_TXN_TRACE=1`** 标注可再收紧文档化。
3. **验收补充**：**`test_txn_isolation_visibility`** 已含 coordinator 级 LSN 用例；**仍缺**全矩阵 PAGE/WHERE/COUNT/FIND 集成测试。

### 2.3 `newdb/cli/shell/state/shell_state.h`

**修改建议**：

1. 强化 `HeapReadViewGuard` 的 RAII 保证：构造失败不残留旧 snapshot，析构总是恢复进入前状态。
2. 增加 debug-only 断言：guard 离开后 `HeapTable::active_snapshot` 与进入前一致。
3. 对 lazy heap materialize 路径补充 snapshot 保护，避免物化时绕过可见性语义。

### 2.4 `newdb/cli/shell/dispatch/handlers/query/query_handler.cc`

**现状**：**`SHOW PLAN`** JSON **已**含 **`snapshot_lsn`**、**`snapshot_source`**（与 runtime **`last_snapshot_source`** 对齐）、**`readpath_enabled`**，以及 **`plan_candidates`**（含 **`cost.estimated_rows`**）、**`table_stats_stale`** 等。

**修改建议**：

1. 持续审计并统一 **`PAGE` / `WHERE` / `EXPLAIN WHERE` / `COUNT` / `FIND` / 聚合** 等入口的 **`HeapReadViewGuard`** 使用模式。
2. 将 **`EXPLAIN WHERE`** 文本路径与 **`SHOW PLAN`** JSON 字段语义对齐（若产品需要）。

### 2.5 `newdb/cli/modules/txn/coordinator/write_conflict/write_conflict_service.cc`

**现状**：`LockKey::row_pk_write_intent` → `to_storage_key()` 用于 intent map；**最近一条**冲突摘要写入 **`write_conflict_last_sample`**（`table=` / `row=` / `holder=` / `tag=`）；Wait 路径为 **有上限的指数退避 sleep**（ms 上限 128）。

**修改建议**（后续）：

1. 环形缓冲保留最近 **N** 条冲突（当前为单行覆盖）。
2. 可选自适应策略（负载感知）。

### 2.6 `newdb/cli/modules/txn/coordinator/lock/lock_service.cc`

**现状**：**`file_lock_same_process_reuse_count`**（同路径二次 `acquireLock`）；**`file_lock_acquire_fail_count`**（打开失败 / OS 独占锁失败）；**`file_lock_stale_marker_count`**（**`NEWDB_FILE_LOCK_STRICT=1`** 下，锁失败后尝试删除 **体积为 0** 的 `.lock` 并重试一次）。尚未做「按 mtime 判断陈旧」或跨平台错误码长篇格式化。

**修改建议**（后续）：陈旧判定策略、错误消息结构化、与 GUI 诊断联动。

---

## 3. WAL、checkpoint 与恢复

**本节完成度：🟡**（见 §0.1；已落地 `NEWDB_WAL_CHECKPOINT_PRUNE` / `NEWDB_WAL_CHECKPOINT_STRICT`、尾部括号深度扫描与 `WalCheckpointTruncateTrace`；recover 增补 `incomplete_checkpoint_count` / offset-seek fallback 计数与 policy 标签；§3.2「多 segment 可达性」等仍偏运维约定）

### 3.1 `newdb/engine/include/newdb/wal_manager.h`

**现状**：`WalRecoveryStats` 已含 **`recovery_policy`**（字符串标签，含 **`incomplete_checkpoint_tail`**、**`offset_seek_fallback`** 等与 recover/truncate 路径组合）、**`segment_index_partial_tail_stops`** / **`segment_index_bad_header_stops`**、**`uncommitted_txn_discarded_count`**、**`recovery_uncommitted_records_ignored`**（与丢弃未提交 redo 同步递增）、**`incomplete_checkpoint_count`**（EOF 未闭合 CHECKPOINT_BEGIN 括号深度）、**`offset_seek_fseek_fallback_count`**；沿用 **`checkpoint_midpoint_recovery_count`**；**`read_record`** 签名为 **`const`**，便于 recover 路径复用；原有 redo/checkpoint 计时与索引字段保留。

**修改建议**（结构化分组仍未做完）：

1. 将 `WalRecoveryStats` 拆分或分组：
   - scan：segment 数、record 数、checksum/decode fail；
   - checkpoint：begin/end 数、last complete checkpoint；
   - plan：replay_start_lsn、offset seek 命中；
   - apply：redo_apply_ms、applied/ignored record 数。
2. 为 PITR marker、partial abort、savepoint recovery 增加统计占位，避免后续扩展破坏 schema。

### 3.2 `newdb/engine/src/wal/writer/wal_manager.cpp`

**现状**：已实现 **`NEWDB_WAL_CHECKPOINT_PRUNE`**（默认截断；`0` 时仍写控制记录但不截断文件）、**`NEWDB_WAL_CHECKPOINT_STRICT`**（`1` 时 EOF 仍有未闭合 checkpoint 括号则拒绝截断）、**`WalCheckpointTruncateTrace`** 与 **`last_checkpoint_truncate_trace()`**；truncate 决策与 stats/policy 联动。

**修改建议**：

1. checkpoint 写入采用更明确的状态：`BEGIN -> flushed data -> END`。
2. checkpoint prune 前增强运维级检查文档化：
   - 是否存在完整 checkpoint；
   - 是否所有必要 segment 可达；
   - 尾部未闭合 checkpoint 与 STRICT 组合的运维 playbook。

### 3.3 `newdb/engine/src/wal/recovery/wal_manager_recover_support.cpp`

**现状**：segment 索引扫描路径已对 **`read_record`** 失败按 **`EOF` / `fread payload failed`** 与 **其它错误** 区分 partial tail vs bad header；**`recover_scan_checkpoint_bracket_depth_at_eof_nolock()`** 等辅助用于 EOF 括号深度与 STRICT 决策；replay 结束统计 **未提交 txn** 丢弃数；**`recovery_uncommitted_records_ignored`**、**`incomplete_checkpoint_count`**、**`offset_seek_fseek_fallback_count`** 与 **`recovery_policy`** 标签（含 **`incomplete_checkpoint_tail`**、**`offset_seek_fallback`**）已与 **`wal_manager.cpp`** 对齐。

**修改建议**（仍以函数级阶段拆分为目标）：

1. 将当前恢复逻辑拆成内部阶段函数：
   - `scan_segments()`（索引扫描循环 + 注释阶段标记）
   - `find_last_complete_checkpoint()`（仍由索引扫描内 `RecoverCheckpointTracker` 产出 `last_complete_checkpoint_lsn`）
   - `build_redo_plan()` / `apply_redo_plan()`（仍对应 `recover()` 中 replay 起点计算 + `recover_replay_segments`）
2. 对外可选导出「阶段边界」观测（当前行为已在单测与 stats 中覆盖）。

**产品边界（本轮收口）**：**不**新增对外 **C/CLI recover 子阶段计时 API**；可观测性以 **`WalRecoveryStats`** 与 CLI recover 路径已落地的 runtime **`wal_recovery_*` / `wal_recovery_redo_ms` / checkpoint 计数为准，并由 **`test_wal_recovery_indexed`**、**`test_wal_segment_scanner`** 覆盖主要故障与排序/inventory 组合；更深矩阵按 §3.5–§3.6 backlog 迭代。

### 3.4 `newdb/engine/src/wal/recovery/wal_segment_scanner.cpp`

**现状**：**`list_wal_segment_paths`** 按 **`std::filesystem::path`** 排序再转 string；新增 **`list_wal_segment_inventory`**（path + **`size_bytes`**）；单测覆盖排序与 inventory。

**修改建议**（可增强）：

1. 对 segment 文件排序规则做平台无关保证，避免 Windows/Linux 路径排序差异。
2. 对损坏尾部 record 增加“安全停止”策略：
   - checksum fail at tail 可作为 partial tail；
   - 中间 checksum fail 作为 corruption。
3. 增加扫描结果结构：包含 segment path、first_lsn、last_lsn、valid_record_count、tail_status。

### 3.5 `newdb/tests/test_wal_recovery_indexed.cpp`

**新增测试建议**：

1. 完整 checkpoint 后多 segment prune。
2. 尾部不完整 checkpoint 被忽略。
3. 未提交 insert/update/delete 在 recover 后不可见。
4. `NEWDB_RECOVER_MIN_LSN` 与 checkpoint 同时存在时优先级明确。
5. offset seek 文件损坏时 fallback 到 scan。

### 3.6 `newdb/tests/test_wal_segment_scanner.cpp`

**新增测试建议**：

1. segment 排序跨平台一致。
2. partial tail 安全停止。
3. 中间损坏 record 计入 corruption。
4. 空 segment、缺号 segment、重复 segment 的行为固定。

---

## 4. Heap 存储、MVCC 可见性与 PageCache

**本节完成度：🟡**（见 §0.1：**§4.4 PageCache / MemoryBudget** 已落地；**§4.1–§4.3** Heap/MVCC 诊断与 mmap 统一等仍以原文为主）

### 4.1 `newdb/engine/include/newdb/heap_table.h`

**修改建议**：

1. 为 `HeapTable` 增加 snapshot 诊断字段或只读方法：
   - `has_active_snapshot()`
   - `active_snapshot_lsn()`
2. 将单页 `heap_cached_page_*` 的容量纳入统一 memory budget 统计。
3. `logical_row_count()` 增加可见行计数辅助方法：`visible_row_count()`，用于 SHOW PLAN/EXPLAIN 估算。

### 4.2 `newdb/engine/src/heap/heap_table.cpp`

**修改建议**：

1. `materialize_all_rows()` 当前会 decode 全量并 `push_back`，建议增加参数控制：
   - `MaterializeMode::AllRows`
   - `MaterializeMode::VisibleRowsOnly`
2. `rebuild_indexes()` 已按 `is_row_visible()` 过滤索引，建议把过滤原因计数到 decode stats：
   - tombstone skipped；
   - snapshot invisible skipped；
   - decode failed skipped。
3. `decode_heap_slot()` 中 PageCache miss 后读盘路径增加耗时与 bytes 统计。
4. 对 `strip_mvcc_internal_attrs()` 增加测试，确保内部字段不会泄露到 CLI/GUI。

### 4.3 `newdb/engine/src/heap/heap_file_read_view.cpp`

**修改建议**：

1. 统一 mmap 与 fread 路径的错误码。
2. 对 page read fail 增加 path/page_no/errno 上下文。
3. 将 page_size、num_pages、file_size 暴露给 runtime report。

### 4.4 `newdb/engine/include/newdb/page_cache.h` 与 `newdb/engine/src/cache/page_cache.cpp`

**现状**：`PageCacheGlobalStats` 已含 **`bytes_evicted_total`**（LRU 释放字节累计）；`memory_budget.h` / `memory_budget.cpp` 提供 **`MemoryBudgetSnapshot`** 与 **`memory_budget_max_bytes_env()`**（`NEWDB_MEMORY_BUDGET_MAX_BYTES` 优先于 `NEWDB_PAGE_CACHE_MAX_BYTES`）；`NEWDB_PAGE_CACHE_MAX_BYTES` **每次**查询 `getenv`（便于单测切换）；`TxnRuntimeStats` / C API / `validate_runtime_stats.py` 已导出 **`memory_budget_bytes_evicted_total`**。

**修改建议**（后续）：

1. 抽象出更完整的 `MemoryBudget`：
   - sidecar 加载使用；
   - query 临时候选集合使用。
2. 新增 API：
   - `page_cache_resize(max_bytes)`
   - `page_cache_clear()`
3. largest page rejected 等更细粒度观测。

### 4.5 `newdb/tests/test_page_cache.cpp`

**新增测试建议**：

1. cap 内命中。
2. 超 cap 淘汰。
3. 单页大于 cap 拒绝缓存。
4. resize 后不超过新预算。
5. 多线程 get/put smoke。

---

## 5. 查询规划、表统计与 Sidecar 索引

**本节完成度：🟡**（见 §0.1；sidecar/catalog 与 **`PlanCandidate` / `PlanCost.estimated_rows` / `where_build_plan_candidates`**、**`NEWDB_TABLESTATS_V2`**、SHOW PLAN **`plan_candidates` / `table_stats_stale` / `cost`**、**`save_table_stats_file`** tmp→rename 已落地；**OR 去重 / histogram / `PlanChoiceReason`** 等仍为路线图）

### 5.1 `newdb/cli/modules/where/executor/plan/plan_impl.cc`

**现状**：已有 `NEWDB_QUERY_COST_MODEL`、AND/OR/范围/Ne/Contains 启发，`SHOW PLAN` 可输出估算与 `plan_id`；**已**提供 **`PlanCandidate`**（含 **`PlanCost.estimated_rows`**）与 **`where_build_plan_candidates()`**（heap_scan / id_lookup / pk_lookup / eq_sidecar 等轻量代价排序），供 SHOW PLAN JSON **`plan_candidates`** / **`cost`** 使用。

**修改建议**：

1. 将规划阶段拆出更完整结构：
   - `PlanChoiceReason`
2. 不再只记录有界 `plan_candidates_considered`，而是输出候选摘要：
   - id lookup；
   - pk lookup；
   - eq sidecar；
   - visibility scan；
   - full scan。
3. 对 OR 条件增加去重成本估算。
4. 对范围条件引入最小 histogram 接口，先不做完整 equi-depth，也要能表达 min/max/ndv。
5. 将 `QueryTraceGuard` 中 runtime 写回逻辑移动到独立 helper，减少析构副作用。

### 5.2 `newdb/cli/modules/where/executor/stats/table_stats.h`

**现状**：**`ColumnStats`** 已含 **`min_value` / `max_value` / `top_k`**（至多 3 个高频值）；磁盘格式 **`NEWDB_TABLESTATS_V2`**（每列一行六字段：`name;nn;d;min;max;top`）；仍可读 **`NEWDB_TABLESTATS_V1`**。

**修改建议**：

1. 扩展列统计至完整 ANALYZE 语义（若需要）：
   - `row_count`
   - `null_count`
   - 与 **`ndv`** 命名对齐（当前以 **`distinct_count`** 表达）
2. schema fingerprint 不匹配时提供明确状态：
   - `missing`
   - `matched`
   - `stale_schema`
   - `stale_data_lsn`。
3. 为 histogram 预留版本号，避免旧 stats 文件无法兼容。

### 5.3 `newdb/cli/modules/where/executor/stats/table_stats.cc`

**现状**：**`build_table_stats_from_heap`** 会为字符串列更新 **min/max**（字典序）与 **top-k**；**`save_table_stats_file`** 默认写 **V2**，并采用 **`.tmp` → `rename`** 落盘；持久化 stats 与 schema fp 不匹配时 load 失败——**`query_handler.cc`** 在 **`NEWDB_QUERY_PERSIST_TABLE_STATS=1`** 下将 **`table_stats_stale`** 写入 SHOW PLAN JSON。**注意**：**`DELATTR`/多数写路径**经 **`invalidate_eq_sidecars_after_write`** → **`sidecar_invalidate_all_indexes_for_data_file`** 会 **`invalidate_table_stats_for_data_file`**，磁盘上 **旧 `.tablestats` 被删**，因此 **不会**再出现「schema 已变而旧文件仍在」的 `table_stats_stale:true`；该类断言的 CLI 单测以 **篡改/损坏 fp 的 `.tablestats`**（`test_show_plan_table_stats_stale_shell`）为准。**`build_query_cache_key`** 已含 **`table_stats_schema_fingerprint(schema)`**，避免 schema 变更后误用 WHERE 结果缓存。

**修改建议**：

1. ANALYZE/采样时限制最大扫描行数，避免大表阻塞。
2. 对 int/date/datetime 类型生成 min/max（当前路径以 string 存储为主）。
3. 失败原子性：rename 前磁盘满等边界仍可按需加固。

### 5.4 `newdb/cli/modules/sidecar/eq/equality_index_sidecar.cc`

**现状**：**`NEWDB_INDEX_CATALOG_ENFORCE=1`** 时，header **`catalog_build_state == 1`（building）** 在缓存命中与磁盘加载路径均 **删除 sidecar/bloom 并拒绝使用**；descriptor 与 runtime 不一致时在 stderr 附带 **`explain_index_descriptor_mismatch`** 摘要；**`eq_sidecar_invalidate_remove_fail_count`** 统计非「文件不存在」的删除错误；**已**在 **磁盘加载 `.eqidx`** 前做 **memory budget 软 cap**（**`memory_budget_used_bytes + file_size`** 与 **`NEWDB_MEMORY_BUDGET_MAX_BYTES` / `NEWDB_PAGE_CACHE_MAX_BYTES`** 比较），超限则跳过加载并累计 **`memory_budget_sidecar_load_skipped_total`**（见 **`test_eq_sidecar_bucket`**）。

**修改建议**（仍适用）：

1. 读取 sidecar 时必须检查：
   - schema fingerprint；
   - data WAL LSN；
   - build state `bld=ready`；
   - index kind 与 attr name。
2. `NEWDB_INDEX_CATALOG_ENFORCE=1` 下，如果 build state 为 building，直接删除并重建。
3. query 路径临时结构与 sidecar **常驻内存**记账仍可按统一预算扩展（当前为「加载前按文件大小 + PageCache 已用」的软拒绝）。
4. 冷路径磁盘读已经有 `where_eq_sidecar_disk_*`，建议增加 build 耗时与写入 bytes。

### 5.5 `newdb/cli/modules/sidecar/common/index_catalog.h` 与 `index_catalog.cc`

**现状**：**`explain_index_descriptor_mismatch`**；**`index_catalog_sidecar_invalidate_request_count`**（central invalidate 入口计数）；与 **`equality_index_sidecar`** 配合的 enforce 语义见 §5.4。

**修改建议**（磁盘写入顺序仍为路线图）：

1. 将 `bld=building|ready` 从解析兼容升级为强语义：
   - 创建时先写 building；
   - 完成后 rename 为 ready；
   - 查询只消费 ready。
2. catalog descriptor 增加：
   - table path canonical form；
   - schema fp；
   - data fp 或 WAL LSN；
   - build start/end timestamp。
3. 提供 `explain_index_descriptor_mismatch()`，供 stderr/SHOW PLAN 输出可读原因。

### 5.6 `cli/shell/dispatch/services/sidecar/sidecar_invalidate_service.cc`

**现状**：仍为本路径转发 **`sidecar_invalidate_all_indexes_for_data_file`**；runtime 侧 **`sidecar_invalidate_count`** 取自 **`index_catalog_sidecar_invalidate_request_count`**，**`sidecar_invalidate_fail_count`** 取自 **`eq_sidecar_invalidate_remove_fail_count`**（校验脚本与 `RUNTIME_STATS_SCHEMA.md` 已列）。

**修改建议**（后续）：结构化失效原因、按表聚合、与 DML trace 对齐。

### 5.7 `newdb/tests/test_query_table_stats.cpp`

**现状**：已覆盖 **V2 持久化头**、**min/max/top-k**、**`where_build_plan_candidates`**（id lookup）、**schema fp 不匹配时 load 失败**（与 **`table_stats_stale`** 判定语义对齐的单元场景 **`StaleStatsWhenSchemaMismatchMatchesQueryHandlerSemantics`**）等。

**新增测试建议**：

1. ~~CLI 集成：**`NEWDB_QUERY_USE_TABLE_STATS=1`** + **`NEWDB_QUERY_PERSIST_TABLE_STATS=1`** 下 **SHOW PLAN** 的 **`table_stats_stale:true`** 端到端断言。~~（**已** `test_show_plan_table_stats_stale_shell`：损坏 fp 的 `.tablestats`；**非** `DELATTR`——写路径会删 stats 文件，见 §5.7 现状段。）
2. min/max 影响范围 selectivity（stats 更深接入代价模型后）。

### 5.8 `newdb/tests/test_index_catalog.cpp`

**现状**：已增加 **`ExplainDescriptorMismatch`**、**`InvalidateIncrementsRequestCounter`** 等用例。

**新增测试建议**：

1. building sidecar 不被查询消费。
2. ready sidecar 可消费。
3. descriptor mismatch 给出具体 mismatch reason。
4. tmp -> rename 期间中断后不会读到半成品。

---

## 6. Vacuum、compact debt 与存储健康度

**本节完成度：🟡**（见 §0.1）

### 6.1 `newdb/cli/modules/txn/coordinator/vacuum/vacuum_service.cc`

**现状**：入队评分由 **`compute_vacuum_score_breakdown`**（内部结构：file / health_bonus / wal_since / total）统一计算；最后一次入队的分项写入 **`TxnRuntimeStats`**：**`vacuum_score_file_bytes_term`**、**`vacuum_score_health_bonus_term`**、**`vacuum_score_wal_since_term`**（经 **`stats_impl`** → **C API** / **SHOW TUNING JSON**）。

**修改建议**（后续）：

1. 将 score 计算拆成纯函数并完整单测：
   - fragmentation；
   - tombstone ratio；
   - compact debt；
   - wal_since 可选项；
   - cooldown penalty。
2. `NEWDB_VACUUM_SCORE_WAL_SINCE=1` 当前为实验项，建议在 runtime 中明确标记是否启用。
3. 真空成功后已回写 `table_storage_health_last_vacuum_*`，建议补充：
   - reclaimed bytes；
   - rewritten rows；
   - elapsed ms；
   - cooldown until。
4. 入队与 priority score 使用同一个结构化 `VacuumScoreBreakdown`，避免公式漂移。

### 6.2 `newdb/cli/modules/storage/table_storage_health.h` 与 `table_storage_health.cc`

**现状**：**`TxnRuntimeStats`** / C API / **`validate_runtime_stats.py`** 已导出 **`table_storage_health_tier`**（**`good|watch|degraded|critical`**），由 **`stats_impl.cc`** 根据最近一次 **`measure_table_storage_health`** 快照的 **`fragmentation_ratio`** 与 **`dead_bytes`** 阈值推导（非改引擎侧 `table_storage_health` 核心结构）。

**修改建议**：

1. 将等级计算下沉或与 **`table_storage_health`** 模块合一，输出各因子贡献摘要。
2. 增加从 runtime JSONL 聚合多表健康度的 helper。

### 6.3 `newdb/tests/test_table_storage_health.cpp`

**新增测试建议**：

1. fragmentation 到 score 的单调性。
2. vacuum 后 health 恢复。
3. cooldown 期间不会重复入队。
4. wal_since 开关对 score 的影响。

### 6.4 `newdb/tests/test_storage_soak.cpp`

**现状**：**`StorageSoakLight`** 供 PR/Nightly；**`StorageSoakHeavy`** 在 **`NEWDB_ENABLE_HEAVY_SOAK=1`** 时运行，且若设置 **`NEWDB_SOAK_HINT_JSONL=<path>`** 会向该文件 **append 一行 JSON**（供 baseline / gate 解析）；**`nightly_soak_hints.py`** 矩阵已提及该环境变量。

**修改建议**：

1. 将 heavy 输出与 **`ci_bench_gate.py` / capture_baseline** 阈值建议流水线正式对接。
2. 扩展 heavy 内真实负载（当前 hook 以占位成功为主时可逐步加厚）。

---

## 7. C API 与 GUI 路径一致性

**本节完成度：🟡**（见 §0.1）

### 7.1 `newdb/engine/src/api/c/c_api.cpp`

**现状**：**`build_runtime_stats_json()`** **已**含事务读路径、sidecar、WAL recovery、file-lock、vacuum 分项、**`memory_budget_sidecar_load_skipped_total`** 等（见 **`test_c_api`** 与 **`RUNTIME_STATS_SCHEMA.md`**）。

**修改建议**：

1. `newdb_session_execute` 业务错误路径上结构化 error code 与 Rust 消费进一步对齐。
2. `SHOW PLAN` / `EXPLAIN WHERE` 若需避免 GUI 二次解析，可评估 C API 直出结构化计划 JSON（当前仍以 CLI 文本合约为准）。
3. 增加 C API 与 CLI **同查询 snapshot** 的对照单测（可选）。

### 7.2 `newdb/rust_gui/src-tauri/src/lib.rs`

**现状**：**`runtime_artifact_info`** **已**含 demo/perf/runtime_report/DLL 路径与时间戳、**`runtime_stats_schema_version`**、**`backend_git_commit`**（**`NEWDB_GIT_COMMIT`**）、**`build_profile`**（**`NEWDB_BUILD_PROFILE`**）、**`gui_package_kind`**（debug/release，来自 **`cfg!(debug_assertions)`**）。

**修改建议**：

1. `execute_command_ex` 进一步以结构化 error code 为主、字符串为辅。
2. `query_page` / 调试路径对 SHOW PLAN JSON 的保留与展示（与 **App.vue** 面板联动）。
3. 对绕开 C API 的引擎直连路径做审计与一致性标注。

### 7.3 `newdb/rust_gui/src/App.vue`

**现状**：Export 弹窗 **已**含 **「计划 / 诊断」** 区（**`lastShowPlanRaw`**、**`errorCodeNumeric`** 等）；DLL/artifacts 弹窗 **已**展示 **`guiPackageKind`** 与 **`runtime_stats_schema_version`** 等。

**修改建议**：

1. 错误展示按类别（parse / schema / storage / lock）做更细视觉分区（可选）。
2. 对可疑空 PAGE 结果保留最后一次有效数据 + **warning banner**（若尚未全覆盖）。

### 7.4 `newdb/rust_gui/src/pagePolicy.ts` 与 `commandPolicy.ts`

**修改建议**：

1. 将策略输入从文本标记升级为结构化字段。
2. 增加锁冲突、WAL 恢复失败、sidecar stale 三类错误策略。
3. 对“空表但成功”和“后端错误导致空结果”保持严格区分。

---

## 8. 测试矩阵与 CMake 集成

**本节完成度：🟡**（见 §0.1）

### 8.1 `newdb/CMakeLists.txt`

**修改建议**：

1. 给测试打 label：
   - `unit`
   - `storage`
   - `wal`
   - `txn`
   - `query`
   - `soak`
   - `release`
2. 将 heavy soak 默认排除，仅在 `NEWDB_ENABLE_HEAVY_TESTS=1` 或 CTest label 选择时运行。
3. 为 Windows/MSVC、MinGW、Linux 三类构建保留一致测试入口。

### 8.2 `newdb/docs/testing/TESTS_FAULT_INJECTION_AND_TXN_MATRIX.md`

**修改建议**：

1. 增加“每个风险点对应哪些测试”的矩阵。
2. 将新增测试按 P0/P1/P2 标记。
3. 写明本地快速命令、PR 命令、Nightly 命令、Release 命令。

### 8.3 新增或扩展测试文件清单

| 文件 | 建议 |
|---|---|
| `tests/test_txn_isolation_visibility.cpp` | **已** coordinator 级 RC/Snapshot LSN；**仍缺** CLI 多入口全矩阵 |
| `tests/test_txn_write_conflict.cpp` | 增加结构化 LockKey、wait 退避、冲突采样测试 |
| `tests/test_wal_recovery_indexed.cpp` | 增加 checkpoint prune、未闭合 checkpoint、未提交记录忽略测试 |
| `tests/test_wal_segment_scanner.cpp` | **已**空目录 / 非 `.wal` 忽略 / inventory 与 path 序一致；**仍缺** partial tail、损坏中段等 |
| `tests/test_page_cache.cpp` | 增加 resize、eviction、多线程 smoke |
| `tests/test_query_table_stats.cpp` | **已** stale/schema 不匹配语义单测；**仍缺** min/max 对范围代价的集成 |
| `tests/test_index_catalog.cpp` | 增加 building/ready 强语义与 mismatch reason 测试 |
| `tests/test_table_storage_health.cpp` | 增加 score breakdown 与 cooldown 测试 |
| `tests/test_c_api.cpp` | 增加 runtime stats 字段与 snapshot 一致性测试 |

---

## 9. 文档同步方案

**本节完成度：🟡**（见 §0.1）

### 9.1 `newdb/docs/roadmap/NEWDB_OPTIMIZATION_ASSESSMENT_AND_PLAN.md`

**修改建议**：

1. 本文件作为总体路线图继续保留。
2. 每完成一个文件级任务，在对应阶段更新状态为 🟢。
3. 避免与本文长期重复：总体状态写在该文，具体文件清单写在本文。

### 9.2 `newdb/docs/txn/TXN_ISOLATION_AND_LOCKING.md`

**修改建议**：

1. 补充 `snapshot_source`、刷新计数、禁用读路径计数。
2. 明确 Snapshot/ReadCommitted 仍非完整 SQL RR/RC。
3. 增加 GUI 直连路径审计结论。

### 9.3 `newdb/docs/storage/STORAGE_GOVERNANCE_AND_RECOVERY_BUDGETS.md`

**修改建议**：

1. 补充 `VacuumScoreBreakdown` 字段说明。
2. 补充 PageCache 与统一 memory budget 的预算表。
3. 补充 Nightly/Release soak 的推荐阈值。

### 9.4 `newdb/scripts/validate/RUNTIME_STATS_SCHEMA.md`

**现状**：文档与脚本已包含 **`memory_budget_bytes_evicted_total`**、**`memory_budget_sidecar_load_skipped_total`**、**`where_heap_scan_budget_binding_events`**、**`transaction_snapshot_lsn` / `statement_snapshot_lsn`**、**`table_storage_health_tier`**（枚举校验）、**`txn_snapshot_*`** / **`txn_readpath_disabled_count`** 等；并与 **`wal_recovery_policy`**、**`write_conflict_last_sample`**、**`sidecar_invalidate_*`**、**`file_lock_*`**、**`vacuum_score_*_term`** 等并存；**`rust_gui`** 下 **`scripts/validate`** 与 **`src-tauri/resources/scripts/validate`** 建议与主干脚本保持镜像同步。

**修改建议**（后续）：

1. 若引入真正的 **`vacuum_score_breakdown` object**，需与三项 scalar 并存策略及校验脚本一并约定。
2. 持续保持 **`schema_version: newdb.runtime_stats.v1`** 向后兼容。

---

## 10. 建议实施顺序

各批前 **状态** 对应当前仓库（见 §0.1）。

### 第一批：P0，可快速落地

1. 🟡 `capture_baseline.py`（**已** workload profile + manifest + **`bench_gate_profile`** + **`recommended_ci_bench_gate_cli`**；**未**全自动跨机 profile 数值基线）。
2. 🟢 `ci_bench_gate.py` profile 默认阈值 + **全阶段** **`--gate-fail-json-out`** + **`newdb_runtime_report`** 转发参数。
3. 🟢 `query_handler.cc` / `wal_service.cc` 输出 snapshot 相关字段到 `SHOW PLAN`（及读路径统计进 runtime JSON）。
4. 🟡 `test_txn_isolation_visibility.cpp` + **`test_txn_shell_multi_entry_snapshot.cpp`**（**已** Snapshot 事务下 COUNT/PAGE/WHERE/FIND 与 **`SHOW TUNING JSON`** 的 **`transaction_snapshot_lsn`/`statement_snapshot_lsn`** 一致性；**未**全覆盖 GUI/多会话组合矩阵）。
5. 🟢 `RUNTIME_STATS_SCHEMA.md` 增加 optional / v2 草案字段说明。

### 第二批：P1，正确性优先

1. 🟡 `wal_manager_recover_support.cpp` 拆分恢复阶段（**已**索引扫描阶段注释、`incomplete_checkpoint_count` / offset-seek fallback 统计、`recovery_uncommitted_records_ignored`；**未**拆成对外可见的独立阶段 API）。
2. 🟡 `wal_segment_scanner.cpp`（**已** inventory + 排序 + **空目录 / 非 .wal 忽略 / 顺序一致** 单测 + **字典序 vs 数值序** 多文件确定性；**已** `test_wal_recovery_indexed` 增补 **clean EOF `partial_tail`** 与 **坏尾追加后恢复**；**未**全平台 tail corruption 组合矩阵）。
3. 🟡 `index_catalog.*` 强化 building/ready 强语义（**已** `explain_index_descriptor_mismatch` + invalidate 计数；**未**改磁盘写入顺序文档级原子性）。
4. 🟡 `equality_index_sidecar.cc` 严格消费 ready sidecar（**已** enforce 下拒绝 `bld=1` building；**未**全路径 fp/LSN 强校验扩展）。
5. 🟡 `write_conflict_service.cc` 统一 LockKey serializer（**已**沿用 `LockKey::to_storage_key` + 冲突采样行 + 退避；**未**单独序列化格式版本号）。

### 第三批：P2，性能与长期运行

1. 🟡 `plan_impl.cc`（**已** `PlanCandidate` + **`PlanCost.estimated_rows`** + SHOW PLAN **`cost`**；**未**OR 去重 / histogram）。
2. 🟡 `table_stats.*`（**已** V2 + min/max/top-k + SHOW PLAN **`table_stats_stale`** + **tmp→rename** 写入 + **`test_show_plan_table_stats_stale_shell`**；**未**typed min/max 全类型）。
3. 🟡 `page_cache.*` + `memory_budget.*`（**已**淘汰字节 + **`memory_budget_sidecar_load_skipped_total`**（eq 磁盘 cap）+ **`where_heap_scan_budget_binding_events`**（WHERE policy heap cap 收紧观测）；**未**query 临时结构统一硬 cap）。
4. 🟡 `vacuum_service.cc` 引入 `VacuumScoreBreakdown`（**已**导出 `vacuum_score_*_term` 三字段）。
5. 🟡 `test_storage_soak.cpp`（**已** `StorageSoakHeavy` + **`NEWDB_SOAK_HINT_JSONL`** 可选一行 JSON；**已**在 `nightly_soak_hints` / `ci_bench_gate --runtime-jsonl` help 说明与 runtime 契约行的衔接方式；**未**接完整阈值闭环）。

### 第四批：P3，体验与发布工程

1. 🟡 `c_api.cpp` 增加结构化 error code 与 runtime stats v2 字段（**已**含 `wal_recovery_policy`、file-lock、sidecar invalidate、vacuum score 分项等）。
2. 🟡 `rust_gui/src-tauri/src/lib.rs` 消费结构化错误（**已有** `error_code_numeric` 解析 `numeric=`）。
3. 🟡 `App.vue` 增加计划/诊断面板（**已有** Export 弹窗内「计划/诊断」折叠内容区）。
4. 🟡 GitHub Actions 拆分 PR/Nightly/Release gates（**已**加 `linux-nightly-soak`、**`linux-release-gate`**）。

---

## 11. 完成判定

当以下条件满足时，可认为本轮文件级修改方案完成闭环：

1. 每个 P0/P1 修改点都有对应测试或 CI gate。
2. runtime stats 可以解释 WAL 恢复、查询计划、PageCache、Vacuum、锁冲突的主要行为。
3. `SHOW PLAN` 能说明为什么选择某个计划，以及是否受 snapshot/sidecar/stats 影响。
4. WAL 恢复面对不完整 checkpoint、partial tail、未提交事务时行为稳定且可观测。
5. CLI、C API、GUI 在错误语义和查询读路径上不再分叉。

### 11.1 当前进度对照（相对上文 5 条）

| 判定项 | 状态 | 说明 |
|--------|------|------|
| 1. P0/P1 测试或 CI gate | 🟡 | 同上 + WAL segment / recovery 尾缀 / **`test_txn_shell_multi_entry_snapshot`** / eq 预算 / **`test_where_heap_scan_budget_binding`** / stats stale Shell 用例；**recover 子阶段对外 API** 明确 **不**做（见 §3.3 产品边界），观测留在 **`WalRecoveryStats`** + runtime **`wal_recovery_*`** |
| 2. runtime stats 可解释主要行为 | 🟢 | 同上字段集 + **`ci_bench_gate`** 全阶段失败 JSON、**`capture_baseline`** manifest 内 **`recommended_ci_bench_gate_cli`**；与 **`linux-bench-gate-runtime-contract`** / **`validate_runtime_stats`** fixture 路径对齐 |
| 3. `SHOW PLAN` 可解释计划与 snapshot/sidecar/stats | 🟡 | 同上 + **`plan_candidates` / `table_stats_stale` / `cost.estimated_rows`**；**`test_show_plan_table_stats_stale_shell`**；**产品决策**：**`DELATTR`** 经 sidecar **invalidate** 会删 **`.tablestats`**，与「仅坏 fp 文件」stale 展示 **二选一**（§5.7）；WHERE 缓存键 **已**含 **schema fp**；OR 链 **batch** 路径见 **`plan_impl`**（histogram 仍为 backlog） |
| 4. WAL 恢复可观测边界场景 | 🟡 | 同上 + §3.3 边界声明；**`test_wal_recovery_indexed` / `test_wal_segment_scanner`** 覆盖排序、inventory、尾缀与多 segment 子集；更深故障组合 §3.5–§3.6 backlog |
| 5. CLI / C API / GUI 语义一致 | 🟡 | 同上 + **`guiPackageKind`**；**`rust_gui`** **`commandPolicy.ts`** 导出 **`RUNTIME_TUNING_DIAGNOSTIC_GROUPS`**，**Export** 弹窗展示与 **`RUNTIME_STATS_SCHEMA.md`** 对齐的键清单；C API JSON 与校验脚本仍为主干契约 |
