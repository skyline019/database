# 三十一期（PHASE31）：事务链与存储边界不变式

## 目标与非目标

**目标**：把 `undo_stack_`、`commit_seq`、`read_max_seq`、`session.txn`、checkpoint、WAL、compaction 之间的**组合语义**收敛为 **显式不变式 + 自动化回归 + 一页矩阵**，治理边界 bug、单写者误用、恢复链顺序与文档漂移。

**非目标**：新 WAL/undo 磁盘格式；多进程共享存储；完整 InnoDB 级 MVCC。默认引擎与 embed 行为**不因本期而变**；任何新门闩须 **默认 off**（与 **二十四期 24A** 一致）。

**权威交叉引用**：[`POLICY.md`](../POLICY.md) §3–§4、[`PHASE23.md`](PHASE23.md)（23C）、[`PHASE24.md`](PHASE24.md)（24A）、[`TESTING_TXN_CHAIN.md`](TESTING_TXN_CHAIN.md)、[`UNDO_LOG_4C.md`](UNDO_LOG_4C.md)、[`COMPACTION.md`](../COMPACTION.md)。

---

## 子条 31A–31E（PR 切分建议）

| 子条 | 内容 | 主要交付 |
|------|------|----------|
| **31A** | 组合语义矩阵（本文 §31A） | 本文件 + `POLICY` §4 索引句 |
| **31B** | 恢复链与顺序 | `TESTING_TXN_CHAIN` §13、`Engine::startup` → `EmbedClient::open` → MDB 恢复 的代码锚点；GTest：`Phase31Wal*`、`Phase31*Journal*`、`Engine.Phase31CorruptSessionTxnDropsTxnLogOnRepl` |
| **31C** | flush/compact 与 checkpoint/MANIFEST | `StorageEngine.Phase31Flush*`、`Phase31Compact*`（checkpoint 中 `manifest_version` 与 `manifest().version()` 一致） |
| **31D** | 绕写 / 多 embed / 24A | `Engine.Phase31*Bypass*`、`Phase24*` 补强；未定义组合在矩阵中显式标注 |
| **31E** | 漂移治理 | 本文 §3 checklist；`POLICY` §6.1 轻量规则（PR 附可复制 `--gtest_filter`） |

---

## 31A：组合语义矩阵（单一真源索引）

下列单元格为 **期望行为** 或 **显式未定义**；实现以已合并代码为准，矛盾时先修实现或修文档再关单。

### 维度 A：`mdb_persist_in_begin` × `mdb_chain_rollback_on_mdb_rollback` × `ROLLBACK` / `COMMIT`

| `persist_in_begin` | `chain_rollback` | 操作 | 期望（摘要） | 测试 / 锚点 |
|--------------------|------------------|------|--------------|-------------|
| on | off | `ROLLBACK` | 仅恢复 MDB 逻辑表 + `session.txn`；**不**弹 `undo_stack_`；已落盘 `mdb$` 仍可见 | `Mdb.TxnBeginPersist*`、`POLICY` §4.3 |
| on | on | `ROLLBACK` | 先弹 `undo_stack_` 至 `BEGIN` 深度，再恢复会话态（受限单写者模型） | `Mdb.TxnBeginPersistChainRollback*` |
| on | on | `COMMIT` | 清除 `BEGIN` 水位；后续写不按事务回滚 | `PHASE23.md` 23C |
| off | on | `ROLLBACK` | 链式回滚**不适用**（无事务中落盘或 per-run 关断） | `Mdb.TxnBeginPersist*` |

### 维度 B：`TXNISOLATION snapshot` 与 `txn_snap_seq`

| 场景 | 期望 | 锚点 |
|------|------|------|
| `BEGIN` 后钉 `txn_snap_seq` | `read_max_seq` / 存储读序号 ≤ 快照语义；与 `latest_commit_seq` 偏序见 `POLICY` §4.1 | `Mdb.TxnChain*`、`Mdb.EngineKvReadSeqVisibility` |
| 从 `session.txn` 恢复后 | **不重算** 文件中 `SNAP`；可低于重启后 `latest_commit_seq` | `CHANGELOG`「说明（MVCC）」 |

### 维度 C：`persist_table` 内 `kv_visit_prefix` 与 `read_max_seq`

| 场景 | 期望 | 锚点 |
|------|------|------|
| 删旧索引键前缀 | 使用 **默认最新可见**（`read_max_seq = max`） | `POLICY` §4.3、`TESTING_TXN_CHAIN` §1 表 |

### 维度 D：`kOpenFlagRebuildUndoStackFromLog`

| 前提 | 期望 |
|------|------|
| WAL 已重放状态与 `undo.log` 扫描重建栈一致 | 可选；`rollback_one_undo_frame` 与文档一致 |
| 与 WAL 水位 / `undo.log` 前缀截断 **不一致** | **高风险 / 语义失真**；见 [`UNDO_LOG_4C.md`](UNDO_LOG_4C.md)、`POLICY` §3.5 |

### 维度 E：多 `EmbedClient` × 链式门闩（24A）

| 场景 | 期望 |
|------|------|
| 单客户端 `BEGIN` 期间 **直接** `Engine::kv_put(mdb$*)` | 可选观测计数 +1；可选 strict 拒绝 |
| 两客户端均已 `open`、会话目录不同；其一处于 `BEGIN` | 直接 `kv_put(mdb$*)` 行为与单客户端一致（**不**因第二客户端存在而改变计数语义）；部署上仍须遵守 §4.2 单写者 |
| 第二客户端 **交错** `submit` 版本化写与活跃 MDB 事务 | **未定义**（`POLICY` §4.2–4.3）；本期以文档 + 可选观测为主 |

### 维度 F：flush / compact 与 MANIFEST → checkpoint

| 路径 | 期望 |
|------|------|
| `flush_memtable` | **先**持久化 MANIFEST，**再**写 checkpoint（与 `POLICY` §3.4 / §3.3.1 一致） |
| `compact_merge_two_oldest_l0` | 同上 |
| 回归锚点 | checkpoint 中 **`manifest_version`** 与当前 **`manifest().version()`** 一致（`Phase31Flush*` / `Phase31Compact*`） |

### 维度 G：恢复权威顺序

| 顺序 | 职责 |
|------|------|
| 1 | `StorageEngine::open`：MANIFEST + checkpoint + **WAL 重放** → 已提交 `mdb$` 物理态 |
| 2 | `EmbedClient::open`：`session.journal` 幂等重放 |
| 3 | MDB 首条路径：`session.txn` → **未提交**逻辑态 |

代码锚点：`Engine::startup` → `StorageEngine::open`（`src/engine/facade`）；`src/client/embed/src/embed_client.cpp` **`EmbedClient::open`**；`src/client/mdb` **`txn_log_try_recover_repl_session`**（[`mdb_ops_txn_log.cpp`](../src/client/mdb/src/mdb_ops_txn_log.cpp)）。

---

## 31E：改存储写路径 checklist（防漂移）

1. 若改变 WAL / checkpoint / MANIFEST 写序：更新 **`POLICY` §3.3–3.4**、[`COMPACTION.md`](../COMPACTION.md)、本文 §2 矩阵 F。  
2. 若改变事务链 / 读视图：更新 **`POLICY` §4.1–4.3**、[`TESTING_TXN_CHAIN.md`](TESTING_TXN_CHAIN.md)。  
3. 新增 GTest：在 PR 描述或评论中粘贴 **`--gtest_filter`** 一行（见 `POLICY` §6.1）。

---

## 验收命令（本期）

```powershell
cmake --build <build_dir> --target structdb_tests mdb_tests
ctest --test-dir <build_dir> -R "structdb_tests" --output-on-failure
```

**PHASE31 推荐 filter（`structdb_tests`）**：

```powershell
.\build\tests\Release\structdb_tests.exe --gtest_filter="StorageEngine.Phase31*:Engine.Phase31*:EmbedClient.Phase31*:Engine.Phase24*"
```

**与 `session.txn` 损坏路径（`mdb_tests`）**：

```powershell
.\build\tests\Release\mdb_tests.exe --gtest_filter="Mdb.ReplSessionTxnV2CorruptLineDropsLog"
```

---

## 修订记录

| 日期 | 摘要 |
|------|------|
| 2026-05-13 | 初稿：31A–31E、矩阵维度、验收 filter |
