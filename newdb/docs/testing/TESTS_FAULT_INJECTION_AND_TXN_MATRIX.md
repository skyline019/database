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
| 第二协调器无法锁同表 | `test_txn_file_lock.cpp` — `SecondCoordinatorCannotAcquireSameTableLock` | `acquireLock` / OS 锁 |

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
```

## 6. 优先级与 CI 命令速查（P0–P2）

| 优先级 | 风险主题 | 代表性测试 / 门禁 | 本地快速 | PR / Nightly |
|--------|----------|-------------------|----------|--------------|
| P0 | runtime JSONL 契约 + gate | `validate_runtime_stats.py`、`ci_bench_gate.py`、`newdb_runtime_report` | `python3 newdb/scripts/validate/validate_runtime_stats.py <fixture.jsonl>` | `.github/workflows/newdb-ci.yml` 中 `linux-bench-gate-runtime-contract` |
| P1 | 读视图 / snapshot 统计 | `test_txn_isolation_visibility.cpp`（含 readpath off 计数）、`SHOW PLAN` JSON 字段 | `ctest -R TxnIsolationVisibility --output-on-failure` | 同上 + `linux-index-catalog-enforce` |
| P2 | 存储 soak / 健康度 | `test_storage_soak.cpp`（`StorageSoakLight`）、`StorageSoakHeavy`（`NEWDB_ENABLE_HEAVY_SOAK=1`） | `ctest -R StorageSoakLight` | Nightly：`NEWDB_ENABLE_HEAVY_SOAK=1 ctest -R StorageSoakHeavy` |
