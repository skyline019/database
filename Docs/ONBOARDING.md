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

## 路线图索引

- 计划草案：**[phases/PHASE13_PLUS_PLAN.md](phases/PHASE13_PLUS_PLAN.md)**  
- 最近里程碑：**[phases/PHASE23.md](phases/PHASE23.md)**（链式 `ROLLBACK`、L4、内联 L0 上限）、**[phases/PHASE24.md](phases/PHASE24.md)**（边界与入口）；**[phases/PHASE32.md](phases/PHASE32.md)** → **[phases/PHASE33.md](phases/PHASE33.md)** → **[phases/PHASE34.md](phases/PHASE34.md)**（MDB / `StorageEngine` 多 TU 与文档固化）

## 根 README

仓库根 **[README.md](../README.md)** 提供构建与目录速览；若与 `POLICY` / `PHASE23` 冲突，**以 `POLICY` 与 `Docs/phases/PHASE*.md` 为准**；总览图见 **[ARCHITECTURE.md](ARCHITECTURE.md)**。
