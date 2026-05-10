# 事务链测试成熟度（量化）

本文档定义 **事务链**（`TxnCoordinator` + CLI 事务路径 + WAL/锁/读视图）的成熟度度量：通过分维度 `ctest` 切片与加权得分，得到可重复的 **百分比分数** 与 **等级 A–D**。

## 运行方式

在已配置并编译的构建目录下执行（多配置生成器需带 `-C`）：

```bash
# Linux / macOS（Ninja，单配置）
python3 newdb/scripts/ci/txn_chain_maturity_report.py --build-dir newdb/build

# Visual Studio 多配置
python3 newdb/scripts/ci/txn_chain_maturity_report.py --build-dir newdb/build --config Debug

# 含 WalRecoveryIndexed 的慢速全量切片
python3 newdb/scripts/ci/txn_chain_maturity_report.py --build-dir newdb/build --profile full
```

写出 JSON（供 CI 归档或门禁）：

```bash
python3 newdb/scripts/ci/txn_chain_maturity_report.py --build-dir newdb/build --json-out newdb/scripts/results/txn_maturity.json
```

脚本会在子进程中 **清除** 若干 `NEWDB_*` 环境变量（与 [`ENVIRONMENT_BASELINE.md`](ENVIRONMENT_BASELINE.md) 一致），降低本地 shell 污染导致的误判。

## 维度与权重（默认 `profile=default`）

| 维度 ID | 内容 | 权重 | ctest 正则（`newdb_tests`） |
|---------|------|------|---------------------------|
| `api_strict` | 协调器非法状态（无事务 commit/rollback、嵌套 BEGIN、savepoint 前提） | 10 | `TxnChainStrict` |
| `write_conflict` | 写冲突与等待 | 18 | `TxnWriteConflict` |
| `file_lock` | 跨协调器锁与失败计数 | 12 | `TxnFileLock` |
| `isolation` | 隔离配置与 MVCC 可见性 | 22 | `TxnIsolationConfig|TxnIsolationVisibility` |
| `cli_shell_txn` | Shell 多入口快照与批量 DML/回滚 | 18 | `TxnShellMultiEntrySnapshot|DemoWhereBatchDml` |
| `embedded` | 嵌入 `rollbackToSavepoint` 契约与 `undo_chain_fallback_count` | 10 | `TxnEmbeddedContract|TxnUndoMetrics` |
| `wal_txn` | WAL/恢复/vacuum/undo 链（默认 **ctest `-E Hybrid`**，跳过 Hybrid 驻留类用例以控制总耗时；不含 `WalRecoveryIndexed`） | 10 | `DemoTxnWal|TxnAutoVacuum|RecoveryUndoChain` + `-E Hybrid` |

`profile=full` 时：`wal_txn` 正则追加 `|WalRecoveryIndexed`，且 **不再** 传 `-E Hybrid`（覆盖 `DemoTxnWal.Hybrid*` 与大型 recovery，明显更慢）。

**若某维度在本地构建中未注册任何匹配用例**（例如裁剪了测试目标），该维度从分母中剔除并 **按剩余权重重归一化**。

## 得分与等级

- **维度得分**：该维度内 `ctest` 摘要行中 `passed/total * 100%`。
- **总分**：`sum(weight_i / W * score_i)`，其中 `W` 为所有可解析维度的权重之和。
- **等级**：
  - **A**：总分 ≥ 95%
  - **B**：≥ 80%
  - **C**：≥ 60%
  - **D**：&lt; 60%

任一维度内 **ctest 返回非零**（有失败用例）时，脚本 **退出码 1**（便于 CI `&&` 门禁）；输出无法解析时为 **3**。

## 与矩阵文档的关系

用例清单与扩展方向见 [`TESTS_FAULT_INJECTION_AND_TXN_MATRIX.md`](TESTS_FAULT_INJECTION_AND_TXN_MATRIX.md)。新增事务链相关 GTest 时：

1. 套件名前缀应落入上表某一维度的正则，或  
2. 扩展 `txn_chain_maturity_report.py` 中 `_dimensions()` 列表。

## 严格 API 单测

[`test_txn_chain_strict.cpp`](../../tests/test_txn_chain_strict.cpp)（`TxnChainStrict.*`）覆盖不经 CLI 的协调器边界，与 [`txn_manager.h`](../../cli/modules/txn/coordinator/txn_manager.h) 状态机一致。
