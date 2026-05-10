# 事务、锁与 WAL 回归 / 故障矩阵（阶段 B）

本文档把 **`newdb/tests`** 中与事务、锁、恢复相关的用例编成矩阵，便于扩展「故障注入」与并发场景时对照缺口。

## 1. 写冲突与等待策略

| 场景 | GTest 文件 / 用例 | 覆盖点 |
|------|-------------------|--------|
| 同表同行并发事务拒绝 | `test_txn_write_conflict.cpp` — `SameTableSameIdRejectedAcrossActiveTransactions` | 第二事务 BEGIN 后写冲突 |
| BEGIN 与进程级锁语义 | `SameTableConcurrentBeginRespectsProcessScopedLockSemantics` | 与文件锁/协调器交互 |
| 异表同行允许 | `DifferentTableSameIdAllowed` | key = table#id |
| 回滚释放 intent | `RollbackReleasesWriteIntent` | `clearWriteIntents` |
| Wait 策略恢复 | `WaitPolicyCanAcquireAfterPeerCommit`、`WaitPolicyTimeoutReturnsConflict` | 等待与超时计数 |

## 2. 文件锁（跨协调器）

| 场景 | GTest 文件 / 用例 | 覆盖点 |
|------|-------------------|--------|
| 第二协调器无法锁同表 | `test_txn_file_lock.cpp` — `SecondCoordinatorCannotAcquireSameTableLock` | `acquireLock` / OS 锁；**失败时**断言 `file_lock_acquire_fail_count >= 1`（协调器 **B**） |
| 同进程重复 `acquireLock` | `SameProcessSecondAcquireCountsReuse` | `file_lock_same_process_reuse_count` |

### 2.2 Windows CI vs Linux CI（期望差异）

| 用例 | Windows（典型） | Linux（单进程 GTest） |
|------|-----------------|----------------------|
| `SecondCoordinatorCannotAcquireSameTableLock` | `l2.isErr()` **必真**；**B** 的 `file_lock_acquire_fail_count >= 1` | `l2` 可能 Ok 或 Err；仅当 `l2.isErr()` 时断言失败计数 |
| `SameTableConcurrentBeginRespectsProcessScopedLockSemantics` | `b.begin` **Err**；**B** 的 `txn_begin_lock_conflict_count >= 1` 且 `file_lock_acquire_fail_count >= 1` | `b.begin` 可 Ok；若 Err 则同上计数断言 |

GitHub Actions：`windows-clean-room`（[`newdb-ci-reusable.yml`](../../../.github/workflows/newdb-ci-reusable.yml)）跑 Windows 路径；`linux-gcc` / `linux-clang` 跑 POSIX 路径。

## 2.1 隔离配置与 `isLocked` 基线

| 场景 | GTest 文件 / 用例 | 覆盖点 |
|------|-------------------|--------|
| 默认 Snapshot | `test_txn_isolation_config.cpp` — `DefaultIsolationIsSnapshot` | `txnIsolationLevel()` |
| RC / Snapshot 切换 | `SetIsolationRoundTrip` | `setTxnIsolationLevel` |
| 未持锁路径 | `IsLockedFalseWhenNotHeld` | `isLocked` 进程内语义 |
| Snapshot 多查询入口 LSN 一致 | `test_txn_shell_multi_entry_snapshot.cpp` | `TXNISOLATION snapshot` + `BEGIN` + COUNT/PAGE/WHERE/FIND + **`SHOW TUNING JSON`**（`transaction_snapshot_lsn` / `statement_snapshot_lsn`） |

## 3. VACUUM / WAL / sidecar

| 场景 | GTest 文件 / 用例 | 覆盖点 |
|------|-------------------|--------|
| 提交后触发 vacuum、非事务中途 | `test_txn_autovacuum.cpp` | 触发时机与 cooldown |
| WAL epoch / sidecar | `test_sidecar_wal_lsn_stress.cpp` | LSN 与 sidecar 一致性 |
| WAL 并发追加可读 | `test_wal_concurrency.cpp` | writer 串行与读者 |
| 恢复索引统计 / 尾缀观测 | `test_wal_recovery_indexed.cpp` | segment 索引路径、**`segment_index_partial_tail_stops`**（clean EOF）、**坏尾追加**仍可恢复已提交行 |
| WAL 目录枚举排序 | `test_wal_segment_scanner.cpp` | **字典序**多 `.wal` 与 inventory 与 path 顺序一致 |

**说明**：recover **阶段化对外 API** 不在本轮交付；矩阵与统计锚点见 [`NEWDB_FILE_LEVEL_MODIFICATION_PLAN.md`](../roadmap/NEWDB_FILE_LEVEL_MODIFICATION_PLAN.md) **§3.3**。

## 4. 故障注入与扩展建议

当前测试以 **进程内多线程 / 多协调器** 为主。若要逼近「崩溃恢复」：

- 在隔离目录运行 demo + **kill -9** 后重放 WAL（可脚本化，尚未统一收纳为一个 GTest）。
- 对 **`WalRecoveryStats`** 断言：`checksum_failures`、`elapsed_ms` 预算（见 [STORAGE_GOVERNANCE_AND_RECOVERY_BUDGETS.md](../storage/STORAGE_GOVERNANCE_AND_RECOVERY_BUDGETS.md)）。

### 4.1 `rollbackToSavepoint` / 堆 undo 失败（可量化）

| 场景 | GTest | 预期观测（`TxnRuntimeStats` / `SHOW TUNING JSON`） | 运维动作 |
|------|-------|---------------------------------------------------|----------|
| undo 链上单条 `append_undo_row_to_heap` 失败（如非法 `key` 触发 `stoi` 异常） | `test_txn_undo_metrics.cpp` — `TxnUndoMetrics.InvalidRecordKeyIncrementsUndoChainFallbackCount` | `undo_chain_fallback_count >= 1`，`rollback_savepoint_count == 1`；返回值仍为 **Ok**（与 [`TXN_ISOLATION_AND_LOCKING.md`](../txn/TXN_ISOLATION_AND_LOCKING.md) §4.2 一致） | 整事务 **`ROLLBACK`**，排查堆 / 权限 / 记录损坏 |
| WAL 恢复阶段 undo 链退化 | `test_demo_txn_wal.cpp` 等恢复矩阵 | `wal_recovery_undo_chain_fallback_txns`（恢复后 stats） | 同上，并查 WAL / 恢复日志 |

**建议新增用例方向**（按需排期）：

1. `TxnIsolationLevel` 切换后，同一会话两次读 **快照边界** 可区分 RC vs Snapshot（需引擎配合）。
2. 文件锁残留 + 新进程启动：验证不误判 `isLocked`（仅进程内语义）。

## 5. CMake / CTest 命名

事务相关测试均注册在目标 **`newdb_tests`**；可用：

```bash
ctest -L newdb --output-on-failure
ctest -R 'TxnWriteConflict|TxnFileLock|TxnAutoVacuum|WalRecoveryIndexed|WalSegmentScanner|TxnShellMultiEntrySnapshot|ShowPlanTableStatsStaleShell|WhereHeapScanBudgetBinding|IndexCatalog|SidecarWalLsn' --output-on-failure
# 与文件级方案 §8.2 对齐的 stats / sidecar 子集（可按需增减）：
ctest -R 'WalSegmentScanner|WalRecoveryIndexed|TxnIsolationVisibility|EqSidecar|QueryTableStats|ShowPlanTableStatsStaleShell' --output-on-failure
# 运维量化子集（嵌入契约、undo 计数器、与 §7 对齐）：
ctest -R 'TxnEmbeddedContract|TxnUndoMetrics' --output-on-failure
# 读路径 / 隔离基线（建议在干净环境下跑，见 ENVIRONMENT_BASELINE.md）：
ctest -R 'TxnIsolationVisibility|TxnShellMultiEntrySnapshot|DemoWhereBatchDml' --output-on-failure
# 协调器严格 API 边界：
ctest -R 'TxnChainStrict' --output-on-failure
# 加权成熟度得分（JSON 可选）：见 TXN_CHAIN_MATURITY.md
python3 newdb/scripts/ci/txn_chain_maturity_report.py --build-dir build --json-out newdb/scripts/results/txn_maturity.json
```

## 6. 优先级与 CI 命令速查（P0–P2）

| 优先级 | 风险主题 | 代表性测试 / 门禁 | 本地快速 | PR / Nightly |
|--------|----------|-------------------|----------|--------------|
| P0 | runtime JSONL 契约 + gate | `validate_runtime_stats.py`、`ci_bench_gate.py`、`newdb_runtime_report` | `python3 newdb/scripts/validate/validate_runtime_stats.py <fixture.jsonl>` | `.github/workflows/newdb-ci.yml` 中 `linux-bench-gate-runtime-contract` |
| P1 | 读视图 / snapshot 统计 | `test_txn_isolation_visibility.cpp`（含 readpath off 计数）、`SHOW PLAN` JSON 字段 | `ctest -R TxnIsolationVisibility --output-on-failure` | 同上 + `linux-index-catalog-enforce` |
| P2 | 存储 soak / 健康度 | `test_storage_soak.cpp`（`StorageSoakLight`）、`StorageSoakHeavy`（`NEWDB_ENABLE_HEAVY_SOAK=1`） | `ctest -R StorageSoakLight` | Nightly：`NEWDB_ENABLE_HEAVY_SOAK=1 ctest -R StorageSoakHeavy` |
| P2 | 嵌入 API / undo 量化 | `test_txn_embedded_contract.cpp`、`test_txn_undo_metrics.cpp` | `ctest -R 'TxnEmbeddedContract|TxnUndoMetrics'` | 随 `linux-gcc` / `windows-clean-room` 全量 `ctest` |
| P2 | 事务链成熟度（加权得分） | [`txn_chain_maturity_report.py`](../../scripts/ci/txn_chain_maturity_report.py)、[`TXN_CHAIN_MATURITY.md`](TXN_CHAIN_MATURITY.md) | `python3 newdb/scripts/ci/txn_chain_maturity_report.py --build-dir build` | 可选归档 `txn_maturity.json` |
| P2 | 协调器严格 API | `test_txn_chain_strict.cpp` — `TxnChainStrict` | `ctest -R TxnChainStrict` | 同上 |

## 7. 嵌入 API 契约（不经 `process_command_line`）

[`txn_manager.h`](../../cli/modules/txn/coordinator/txn_manager.h)：`rollbackToSavepoint` 注释要求 **绕过 shell dispatch 的调用方** 在成功后自行 **`Session::invalidate` / 重载** 会话堆，否则内存与磁盘可能不一致。

| 操作 | 推荐：失效/重载会话堆 | 推荐观测 |
|------|----------------------|----------|
| `rollbackToSavepoint` | 是 | `rollback_savepoint_count`；与磁盘对比用 `load_heap_file` |
| 用户显式 `ROLLBACK TO SAVEPOINT`（CLI） | 否（handler 已 `shell_invalidate_session_table`） | 同上 |
| `rollback()` / `commit()` | CLI 路径已处理；嵌入直连需自审 | `TxnState`、WAL |
| `recoverFromWAL()` | 是（所有持有该表 `HeapTable` 缓存的会话） | `wal_recovery_*` 字段 |

| GTest | 断言意图 |
|-------|----------|
| `TxnEmbeddedContract.RollbackToSavepointWithoutSessionInvalidateLeavesStaleHeap` | 仅协调器 undo 后 **不** `invalidate` → 会话堆与 `load_heap_file` **不一致**（文档化风险） |
| `TxnEmbeddedContract.InvalidateAndReloadAlignsHeapWithDiskAfterSavepointRollback` | `invalidate` + `ensure_loaded` 后一致 |

## 8. 环境变量基线与可重复性

大量 `NEWDB_*` 由 `std::getenv` 读取；**父 shell 若 export 了读路径关闭等变量，子进程中的 GTest 会继承**，导致假阳性/假阴性（例如 `TxnIsolationVisibility` 与 `last_snapshot_source`）。

- **变量清单与默认值语义**：见同目录 [`ENVIRONMENT_BASELINE.md`](ENVIRONMENT_BASELINE.md)。
- **推荐本地跑法**：[`scripts/run_newdb_tests_cleanenv.sh`](../scripts/run_newdb_tests_cleanenv.sh)、[`scripts/run_newdb_tests_cleanenv.ps1`](../scripts/run_newdb_tests_cleanenv.ps1)（清除常见 `NEWDB_TXN_*` / `NEWDB_VISCHK` 后调用 `ctest` 或可执行文件）。
- **CI**：工作流默认不设置这些变量；显式设置的 job（如 `NEWDB_INDEX_CATALOG_ENFORCE=1`）以 `env:` 块为准。详见 [`.github/workflows/newdb-ci-reusable.yml`](../../../.github/workflows/newdb-ci-reusable.yml)。
