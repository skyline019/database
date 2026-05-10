# 测试与集成环境变量基线

本文档列出 **易影响事务链 / 读路径 / 侧车行为** 的 `NEWDB_*` 变量。未列出或未设置时，行为以代码默认为准；**在交互 shell 中 `export` 后，同一终端启动的 `ctest` / `newdb_tests` 会继承**，可能导致与 CI 不一致。

## 事务与读路径

| 变量 | 未设置时（典型） | 设为 `0` / `off` / `false` / `no` 时 |
|------|------------------|--------------------------------------|
| `NEWDB_TXN_ISOLATION_READPATH` | 开启读路径：`syncHeapReadSnapshotForQuery` 按隔离级别安装/刷新堆快照 | 跳过读快照安装；`last_snapshot_source` 为 `disabled` |
| `NEWDB_TXN_STMT_SAVEPOINT` | 事务内批量语句使用内部 savepoint（`DELETEWHERE` / `BULKINSERT` 等） | 关闭语句级 savepoint |
| `NEWDB_TXN_TRACE` | 无 stderr 跟踪 | `1`/`on`/`true`/`yes` 打印读视图跟踪 |
| `NEWDB_TXN_ENFORCE_BEGIN_USE_MATCH` | 仅警告 `BEGIN` 表名与 `USE` 不一致 | `1`/`on` 等拒绝 `BEGIN` |
| `NEWDB_TXN_SNAPSHOT_BACKUP` | 不在 `BEGIN` 时备份数据文件 | 开启 `.txn.bak` 快照（写放大） |

## 侧车与可见性

| 变量 | 说明 |
|------|------|
| `NEWDB_VISCHK` | 未设置或开启：使用 visibility checkpoint 侧车；`0`/`off` 等关闭 |
| `NEWDB_INDEX_CATALOG_ENFORCE` | `1` 时侧车描述符不匹配则删除重建（CI 子集 job 会显式设置） |
| `NEWDB_SIDECAR_INVALIDATE_VERBOSE` | 控制侧车失效日志是否打印 |

## 文件锁

| 变量 | 说明 |
|------|------|
| `NEWDB_FILE_LOCK_STRICT` | `1` 时对空锁标记文件尝试剔除后重试 |

## 推荐实践

1. **本地跑事务/隔离相关 GTest**：使用 [`scripts/run_newdb_tests_cleanenv.sh`](../../scripts/run_newdb_tests_cleanenv.sh) 或 [`scripts/run_newdb_tests_cleanenv.ps1`](../../scripts/run_newdb_tests_cleanenv.ps1)，或手动在子进程中清除上表变量。
2. **对照 CI**：以 [`.github/workflows/newdb-ci-reusable.yml`](../../../.github/workflows/newdb-ci-reusable.yml) 中各 job 的 `env:` 为准；未声明即不设置。
3. **扩展清单**：全局检索 `std::getenv("NEWDB_` 可发现其它调试开关；欢迎在本文件追加一行说明。

## 相关文档

- [TESTS_FAULT_INJECTION_AND_TXN_MATRIX.md](TESTS_FAULT_INJECTION_AND_TXN_MATRIX.md) §8、`ctest` 命令
- [TXN_CHAIN_MATURITY.md](TXN_CHAIN_MATURITY.md) — 事务链分维度加权得分与 `txn_chain_maturity_report.py`
- [TXN_ISOLATION_AND_LOCKING.md](../txn/TXN_ISOLATION_AND_LOCKING.md)
