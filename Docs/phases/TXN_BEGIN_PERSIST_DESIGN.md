# 设计草案：`BEGIN` 内 `persist_table`（六期 6D / 七期 7C / 二十三 23C）

**状态**：**默认已实现**（`EngineConfigSnapshot::mdb_persist_in_begin`，默认 **true**；可与 `MdbRunOptions::allow_persist_while_txn_active_experimental` 组合关闭）。**跨层 `ROLLBACK`**：默认 **`mdb_chain_rollback_on_mdb_rollback=false`** 时，`ROLLBACK` **仍仅**恢复会话逻辑表与 `session.txn`；为 **`true`** 且 **`mdb_persist_in_begin=true`** 时，**二十三 23C** 在恢复会话表 **之前** 将 **`undo_stack_` 弹回 `BEGIN` 成功时刻的深度**（受限模型，见 **`POLICY` §4.3**、[`PHASE23.md`](PHASE23.md)）。本文档保留 **顺序与开关边界** 说明，避免与 §3.1 WAL 权威、§3.3 WAL 前缀裁剪、§3.4 checkpoint、**回滚帧与正向批次混排**（`POLICY` §3.1）及 **`kOpenFlagRebuildUndoStackFromLog`** 冲突。

## 1. 动机（InnoDB 类比）

- InnoDB 中，事务修改的数据页通过 **MTR** 记入 redo，提交与回滚与 **undo** 链协作。  
- StructDB：**`BEGIN` 内 `persist_table`** 已默认走版本化写 + WAL 批次；**默认** **`ROLLBACK`** 只恢复 **`session.txn` + 内存逻辑表**，**不**撤销已落盘 `mdb$` 行。  
- **链式 `ROLLBACK`（23C）**：显式门闩下循环 **`rollback_one_undo_frame`**，依赖 **`undo_stack_` 深度水位**；须在 **单写者** 或等价部署下使用；与 WAL 混排重放见 `POLICY` §3.1。

## 2. 特性门闩（已实现）

- **引擎**：**`EngineConfigSnapshot::mdb_persist_in_begin`**（默认 **`true`**）。为 **`false`** 时，`persist_now` 在 `txn_active` 时 **跳过** `persist_table`。
- **每脚本 / REPL 行**：**`MdbRunOptions::allow_persist_while_txn_active_experimental`**（默认 **`true`**）及 REPL 形参 / `MdbInteractiveSession::set_allow_persist_while_txn_active_experimental` 与引擎开关 **AND**；任一为 **`false`** 则事务内不落盘。
- **二十三 23C**：**`EngineConfigSnapshot::mdb_chain_rollback_on_mdb_rollback`**（默认 **`false`**）。为 **`true`** 且 **`mdb_persist_in_begin=true`** 时，MDB **`ROLLBACK`** 调用 **`Engine::rollback_embed_undo_until`** 到 `BEGIN` 记录的 **`embed_undo_stack_depth()`**；**`COMMIT`** 丢弃该水位。

## 3. 顺序约束（实现与论证）

1. **版本化键**：对将覆盖的 `mdb$` 键，先写 **`undo.log`** 帧并更新 **`undo_stack_`**（与现有版本化写路径一致）。  
2. **WAL**：将本批 dels/puts 写入 **单条 `STDBBW1` 帧**（`commit_embed_batch`），必要时 **`wal_sync`**。  
3. **MemTable / SST**：保持现有 `put`/`remove` 与 `commit_seq` 分配规则。  
4. **`flush_memtable` / checkpoint**：仍须满足 §3.4（MANIFEST 先于 checkpoint）；**WAL 前缀裁剪**（§3.3）仅在对齐 `wal_offset` 之后。  
5. **MDB `ROLLBACK`（链式开）**：在恢复 **`current` / `session.txn` 之前** 执行 **`rollback_embed_undo_until`**，使存储可见性与随后会话回滚一致；**SAVEPOINT / ROLLBACK TO SAVEPOINT** 不改变本文件对存储 `undo_stack_` 的约定（仍以会话侧语义为主）。

## 4. 与 `session.journal` / `session.txn` 的关系

- **`session.txn`**：继续记录逻辑表 v2 OP。  
- **`session.journal`**：embed 批次与引擎 WAL 的关系不变；**禁止**在未定义顺序下双写导致与 WAL 重放重复 apply（参见 `CHANGELOG` embed 恢复说明）。

## 5. 验收门槛（草案）

- `POLICY` / `CHANGELOG` / `PHASE17.md` / `PHASE23.md` / `TESTING_TXN_CHAIN`（**`txn_phase17`**、**`txn_phase23`**）与 **`Mdb.TxnBeginPersist*`**。  
- 崩溃模型：至少覆盖「帧完整 / 帧截断 / `session.txn` 尾部丢失」三类之一在设计评审中备案（进阶 hardening）。
