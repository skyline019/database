# StructDB 文档阅读顺序（新人入口）

目标：约 **10～20 分钟** 建立 **持久化边界、写并发、事务链、WAL 重放、代码版图** 的心智模型，再按需深入各 PHASE 专文。

## 建议顺序

0. **[ARCHITECTURE.md](ARCHITECTURE.md)** — **总览**：Mermaid 数据流与代码组织、关键类型源码摘录（可选但强烈推荐）。  
1. **[POLICY.md](POLICY.md) §4.0** — `data_dir` / `session_dir` 保底文件清单与恢复顺序。  
2. **[POLICY.md](POLICY.md) §4.2** — 同进程单写者；多 `EmbedClient`/多线程与「不在保底内」的说明。  
3. **[POLICY.md](POLICY.md) §4.3** — `BEGIN` 内 `persist_table`、默认 `ROLLBACK` 与 **`mdb_chain_rollback_on_mdb_rollback`**（二十三期）链式语义及 **受限模型**。  
4. **[phases/TXN_INNODB_MAP.md](phases/TXN_INNODB_MAP.md) §2** — 耐久 Level 0/1/2 **类比** 与 `fsync_journal` / `fsync_each_batch` / `fsync_each_session_txn_op` / `fsync_every_write` 对应关系。  
5. **[POLICY.md](POLICY.md) §4.5** 与 **§4.5 后「嵌入式耐久矩阵」表**（二十四期）— 未 `fsync` 尾部 **可能丢什么**。  
6. **[phases/WAL_REPLAY.md](phases/WAL_REPLAY.md)** — `STDBBW1` 与文本行、`rollback` 混排、checkpoint `wal_offset`。  
7. **[phases/TESTING_TXN_CHAIN.md](phases/TESTING_TXN_CHAIN.md)** — 读路径审计与 GTest filter（含 `txn_phase23` 等）。

## MDB 事务与存储回滚（三档配置）

`SHOW TXN` / `SHOW SNAPSHOT` 输出 **`storage_rollback_policy=`**（当前门闩，非运行时切换）。部署时在 **`EngineConfigSnapshot`** 启动前选定一档：

| 档位 | `mdb_persist_in_begin` | `mdb_chain_rollback_on_mdb_rollback` | `storage_rollback_policy` | 行为摘要 |
|------|------------------------|--------------------------------------|---------------------------|----------|
| **A（默认）** | `true` | `false` | `session_only` | `BEGIN` 内可落盘；**`ROLLBACK` 仅恢复会话表**，不链式弹 `undo_stack_` |
| **B（链式 undo）** | `true` | `true` | `chain_undo` | MDB **`ROLLBACK` 将 `undo_stack_` 弹回 `BEGIN` 水位**（受限模型，见 [`PHASE23.md`](phases/PHASE23.md)） |
| **C（事务内不落盘）** | `false` | （任意） | `no_persist_in_txn` | 事务内不写 `mdb$`；`ROLLBACK` 仅会话 |

耐久类比见 **[`phases/TXN_INNODB_MAP.md`](phases/TXN_INNODB_MAP.md) §2**（非 InnoDB 等价）。组合矩阵回归：**[`phases/PHASE31.md`](phases/PHASE31.md)**、`Mdb.IntegrateTxnRecoverRollbackRestartChain`。

## OLTP 写路径（非 bulk）

- 默认 **`mdb_incremental_persist=true`**（小批量脏行增量落盘）。
- **勿** 对单行 OLTP 开启 `mdb_bulk_import_mode` / 脚本 bulk 摊销；压测见 **`scripts/bench/oltp_persist_micro.ps1`** → [`benchmarks/baselines/oltp_persist_baseline.json`](../benchmarks/baselines/oltp_persist_baseline.json)。
- REPL 下 **`mdb_script_amortize_bulk_dml`** 仅影响脚本 **`BULKINSERT*`** EOF 落盘，**不**改变单行 `INSERT`/`UPDATE` 的立即 persist（默认）。

配置预设表：**[`ENGINE_RUNTIME_CONFIG.md`](ENGINE_RUNTIME_CONFIG.md) §4.5**（事务 profile）、§6（LSM/压测）。

## PITR 与 checkpoint 链（Wave 3）

- 每次 `flush_memtable` 成功可在 `data_dir/checkpoint.chain` 追加序号记录。
- **`SHOW CHECKPOINTS`** / **`RECOVER TO CHECKPOINT_SEQ n`**：破坏性截断 WAL；须 **`Engine::shutdown()`** 后恢复并重启（见 [`phases/PHASE43.md`](phases/PHASE43.md)）。
- 脚本：**`scripts/recover_to_checkpoint.ps1`**；冷备仍见 **[`BACKUP_RESTORE_RUNBOOK.md`](BACKUP_RESTORE_RUNBOOK.md)**。

## 路线图索引

- 计划草案：**[phases/PHASE13_PLUS_PLAN.md](phases/PHASE13_PLUS_PLAN.md)**  
- 最近里程碑：**[phases/PHASE23.md](phases/PHASE23.md)**（链式 `ROLLBACK`、L4、内联 L0 上限）、**[phases/PHASE24.md](phases/PHASE24.md)**（边界与入口）；**[phases/PHASE32.md](phases/PHASE32.md)** → **[phases/PHASE33.md](phases/PHASE33.md)** → **[phases/PHASE34.md](phases/PHASE34.md)**（MDB / `StorageEngine` 多 TU 与文档固化）

## 根 README

仓库根 **[README.md](../README.md)** 提供构建与目录速览；若与 `POLICY` / `PHASE23` 冲突，**以 `POLICY` 与 `Docs/phases/PHASE*.md` 为准**；总览图见 **[ARCHITECTURE.md](ARCHITECTURE.md)**。
