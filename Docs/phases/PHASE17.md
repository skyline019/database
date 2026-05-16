# 十七期：`BEGIN` 内 `persist_table`（7C / 6D）

## 1. 当前状态

- **实现**：**默认开启** — `EngineConfigSnapshot::mdb_persist_in_begin`（默认 **true**）与 `MdbRunOptions::allow_persist_while_txn_active_experimental`（默认 **true**）同时为真时，`run_mdb_script` / `mdb_repl_execute_line` 在 `BEGIN` 激活时对每次成功变更调用 **`persist_table`**（与 `COMMIT` 相同的 embed 批次路径）。关闭引擎级开关或脚本/REPL 的 per-run 开关可恢复「事务内不落盘」行为。
- **设计入口**：[`TXN_BEGIN_PERSIST_DESIGN.md`](TXN_BEGIN_PERSIST_DESIGN.md)（顺序约束、`ROLLBACK` 与存储关系）。
- **与二十二期的关系**：原 **22D** 门闩已由本实现与 `POLICY` §4.3、`Mdb.TxnBeginPersist*` 单测闭合。

## 2. 验收（已交付）

1. `persist_table` 在 `BEGIN` 内走现有 `EmbedClient::submit` / 版本化写路径；**不**声称 MDB `ROLLBACK` 链式撤销存储（见 `POLICY` §4.3）。
2. `structdb_tests`：`Mdb.TxnBeginPersistStorageSurvivesRollback`、`Mdb.TxnBeginPersistDisabledNoStorageDuringTxn`。
3. `TESTING_TXN_CHAIN.md`：`txn_phase17` 行（若已索引）。

## 3. 修订记录

| 日期 | 摘要 |
|------|------|
| 2026-05-13 | 初稿：二十二期 22D 文档门闩占位，指向 `TXN_BEGIN_PERSIST_DESIGN` |
| 2026-05-13 | 默认开启 `mdb_persist_in_begin`；更新 `POLICY` §4.3 与单测 |
