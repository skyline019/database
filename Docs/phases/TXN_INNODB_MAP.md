# InnoDB 概念与 StructDB 事务链对照（七期，非等价）

本文档与 [`POLICY.md`](POLICY.md) **§4.0–§4.5** 对齐，仅作 **语义类比**；StructDB **不**实现 InnoDB 的二进制兼容或同等锁/多版本链。崩溃恢复权威仍以 **`wal.log`** 为准（§3.1）。

## 1. 核心对象映射

| InnoDB | StructDB 对应 | 说明 |
|--------|-----------------|------|
| **redo log**（物理页重放） | **`wal.log`** + `commit_embed_batch` 的 **`STDBBW1`** 帧 | 类比「组提交边界」：单批次末尾可选 `wal_sync`（`fsync_journal`）；**非**页式 redo，`open` **不重放** `redo.log` 作为权威。 |
| **log buffer** | MemTable / 编码缓冲区 + WAL 编码路径 | 无独立 InnoDB 式 log buffer 文件；批次写入 WAL 前在进程内缓冲。 |
| **undo log / rollback segment** | **`undo.log`** + **`undo_stack_`** | 服务 **版本化 `mdb$` 覆盖写** 的上一值记录与进程内回滚；**不等价**于 MDB `ROLLBACK`（§4.1）。可选 `kOpenFlagRebuildUndoStackFromLog` 跨重启重建栈，约束见 `POLICY` §3.1。 |
| **trx_id / row trx_id** | **`commit_seq`**（单调）+ `mdbver1:` 包装 | 无 per-row InnoDB trx_id；读可见性由 **`read_max_seq`** 裁剪。 |
| **Read View** | **`txn_snap_seq`**（`BEGIN` 固定）+ **`mdb_storage_read_seq_for_script`** | `TXNISOLATION snapshot` 钉快照；`read_committed` 用 `latest_commit_seq()`；见 [`TESTING_TXN_CHAIN.md`](TESTING_TXN_CHAIN.md)。 |
| **mini-transaction (MTR)** | **单条 `STDBBW1` WAL 批次** + 同批次共享 **`commit_seq`** | 一批 dels/puts **原子**提交到 MemTable（帧完整落盘前提下可重放）；checkpoint 顺序仍遵 §3.4。 |
| **doublewrite buffer** | *无* | 未实现；不纳入七期。 |
| **purge / history list** | *无完整等价* | 无旧版本行链清理；单键单版本 + 覆盖写。 |
| **record / gap / next-key lock** | **单 mutex 写路径**（§4.2） | 无 InnoDB 式锁等待与队列。 |

## 2. 耐久等级（类比 `innodb_flush_log_at_trx_commit`，非二进制兼容）

以下 **Level 0/1/2** 仅为 **部署说明用类比**；实际行为由下列 **现有开关组合** 决定，**默认值与当前代码一致**（见 [`EngineConfigSnapshot`](e:/db/StructDB/src/engine/facade/include/structdb/facade/config.hpp)、`EmbedClient::submit`、`MdbRunOptions`）。

| 类比 Level | 意图（InnoDB 读者理解） | StructDB 开关组合（示例） |
|------------|-------------------------|-----------------------------|
| **2**（每秒刷盘、较弱） | 减少 fsync 次数 | `fsync_journal=false`、`MdbRunOptions::fsync_each_batch=false`、`fsync_each_session_txn_op=false`；`EngineConfigSnapshot::fsync_every_write=false`（默认）。 |
| **1**（常见折中） | 批次边界 fsync | `EmbedClient::submit(..., fsync_journal=true)`（即 MDB `fsync_each_batch` 为 true 时 `persist_table` 传入的 fsync）；仍可能丢失 **未 fsync 的** `session.txn` 尾部。 |
| **0**（每次提交刷盘） | 强会话 + WAL | 在 Level 1 基础上，`MdbRunOptions::fsync_each_session_txn_op=true`（每条 `session.txn` v2 OP 后 fsync）；引擎侧若需更强可讨论 `fsync_every_write`（影响面大，须单独评估）。 |

**说明**：本节 **Level 0/1/2** 与 **[`POLICY.md`](POLICY.md) §4.5** 后 **「嵌入式耐久矩阵」**（按风险面归纳）互补；部署时两表一并阅读。

## 3. MTR 与 checkpoint（概念）

- **MTR 类比**：一次 `commit_embed_batch` 写入的 **单 WAL 二进制帧** 对应「一组对 btree 页的修改原子提交」的 **缩小版** — 对象为 **MemTable 中的 `mdb$` 键**而非 InnoDB 页。
- **checkpoint**：五期双槽 + 遗留文本（§3.4）；`flush_memtable` 后更新 `wal_offset`；可选 **WAL 前缀裁剪**（§3.3）须在数据已刷盘后进行。

## 4. 明确不在 StructDB MVP 内对齐的 InnoDB 能力

- 页式 **redo 应用** 替代当前 WAL 权威。  
- **全局事务 id**、**多版本行链**、**purge 线程**。  
- **间隙锁 / 锁升级 / 死锁检测**。

详见 [`POLICY.md`](POLICY.md) §4.2、§4.5 与「计划」七期条目。

## 5. 相关文档

- [`TESTING_TXN_CHAIN.md`](TESTING_TXN_CHAIN.md) — 读路径审计与 GTest 维度（含七期扩展）。  
- [`TXN_BEGIN_PERSIST_DESIGN.md`](TXN_BEGIN_PERSIST_DESIGN.md) — `BEGIN` 内 `persist_table`（6D/7C）**条件**设计草案。  
- [`POLICY.md`](POLICY.md) §3.3、§3.5 — `undo.log` 生命周期与 4C 联动论证。
