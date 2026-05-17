# PHASE41：受限 DDL、耐久档位与命名二级索引（Wave 2）

在 Wave 0–1（OLTP 基线、备份 Runbook、事务三档、`SHOW TXN`/`SHOW SNAPSHOT` 预设）基础上，落地竞品完善路线 **Wave 2**：PHASE41 受限 DDL、会话级耐久、`CREATE INDEX` 命名侧车、DROP/RENAME 单 WAL 批次原子性，以及 persist 流式遍历首段（与 PHASE44 设计衔接）。

权威路线：[`COMPETITIVE_IMPROVEMENT_PLAN.md`](../COMPETITIVE_IMPROVEMENT_PLAN.md) §4–§5；矩阵：[`COMPETITIVE_MATRIX.md`](../COMPETITIVE_MATRIX.md) §6。

## 1. 子阶段与范围

| 子阶段 | 内容 |
|--------|------|
| **41A** | `ALTER TABLE ADD COLUMN (name:type[,default])` → 复用 `mdb_apply_addattr_inner`；`BEGIN` 内拒绝；`persist_table` + schema 脏标记。 |
| **41B** | `ALTER TABLE RENAME COLUMN (old,new)` → 复用 RENATTR 路径 + `persist_now`。 |
| **41C** | 其余 `ALTER TABLE …` 仍 `[NOT_SUPPORTED]`，文案指向 ADD/RENAME 子集（见 `mdb_command_parser.cpp`）。 |
| **I-DUR** | `SET DURABILITY n`（`n∈{0,1,2}`）写入 REPL 会话；映射见 [`TXN_INNODB_MAP.md`](TXN_INNODB_MAP.md) §2（`fsync_each_batch` / `fsync_each_session_txn_op`）；**不**改 `EngineConfigSnapshot` 默认。`WALSYNC`/`GROUPCOMMIT` → 定向提示 `SET DURABILITY` + `SHOW TUNING`。 |
| **I-IDX** | 命名索引键空间 `mdb$v2$nidxdef$` / `mdb$v2$nidx$`；`CREATE INDEX name ON table(col)`；`EXPLAIN WHERE` 输出 `named_index=<name>`；`REBUILD INDEX` / `REBUILD INDEX(name)`。与隐式 `mdb$v2$idx$`（`kSecIdx`）并存。 |
| **I-OPS** | `RENAME TABLE`：`build_persist_command_batch`（新表）+ `gather_drop_table_keys`（旧表）→ **单次** `submit_persist_command_batch`（不再分两批 submit）。 |

**非目标（不变）**：无 JOIN/SQL 全栈；无 `RECOVER TO` 墙钟时间；无堆式 `AUTOVACUUM`；复合索引 / 在线 DDL 多阶段（`DROP INDEX` / `CREATE UNIQUE INDEX` 见 [`PHASE45.md`](PHASE45.md)）。

## 2. 键空间（命名索引）

| 键 | 用途 |
|----|------|
| `mdb$v2$nidxdef$<table>$<indexName>` | 定义：列名（plain text） |
| `mdb$v2$nidx$<table>$<indexName>$<col>$<token>$<pk>` | postings（string 列 token 为 hex；数值列为 trim 文本） |

内存：`LogicalTable::named_indexes`；`load_named_index_defs_from_storage` / `create_named_index_storage` / `gather_named_index_keys`（DROP/RENAME 时一并删除）。

## 3. 耐久档位（会话）

| Level | 行为（REPL 覆盖 `MdbRunOptions`） |
|-------|-----------------------------------|
| 0 | `fsync_each_batch=false`，`fsync_each_session_txn_op=false` |
| 1 | `fsync_each_batch=true`（默认档，与多数 OLTP 预设一致） |
| 2 | `fsync_each_batch=true`，`fsync_each_session_txn_op=true` |

- `SHOW TUNING` / `SHOW TUNING JSON` 含 `session_durability_level`。
- **冷启动后会话档位重置**为默认（未持久化到 session 文件）；引擎全局 `fsync_every_write` 等仍见 `EngineConfigSnapshot`。
- **POLICY**：无新 WAL 权威语义；仅客户端 submit/fsync 策略。

## 4. 崩溃与原子性

| 操作 | 语义 |
|------|------|
| `RENAME TABLE` | 单 `CommandBatch` + 单一 `idempotency_token` 前缀（`idem:rn:*`）；与 [`PHASE31.md`](PHASE31.md) 矩阵交叉：不完整批次 + 冷启动应不出现双表名共存。 |
| `DROP TABLE` | 既有 gather + 单批 submit；命名索引键由 `gather_named_index_keys` 纳入。 |
| `ALTER ADD/RENAME` | `BEGIN` 内拒绝；崩溃窗口同 PHASE31「schema 已 persist、行未齐」类用例。 |

## 5. 与 PHASE40 / PHASE44 关系

- **PHASE40**：大表 **分块 submit**（`mdb_persist_chunk_*`）仍用于帧过大场景；RENAME 单批若超 `storage_embed_batch_max_frame_bytes` 由存储层分帧，语义仍为一次逻辑批次。
- **PHASE44**：`build_persist_command_batch` 全量路径改为 `ordered_row_ids_for_persist` 顺序遍历，降低 `snapshot map` 峰值；`IMPORT_SEGMENT` 仅设计稿见 [`PHASE44_PERSIST_STREAM.md`](PHASE44_PERSIST_STREAM.md)。

## 6. 回归

```text
structdb_tests --gtest_filter=Mdb.Phase41*
structdb_tests --gtest_filter=Mdb.*
```

| 测试 | 覆盖 |
|------|------|
| `Mdb.Phase41DurabilitySetAndShowTuning` | `SET DURABILITY 1` + `SHOW TUNING` |
| `Mdb.Phase41DurabilityNotSupportedHint` | `WALSYNC` → `SET DURABILITY` 提示 |
| `Mdb.Phase41AlterTableAddColumn` | ADD COLUMN + INSERT |
| `Mdb.Phase41AlterTableRenameColumn` | RENAME COLUMN + `SHOW ATTR` |
| `Mdb.Phase41DropRenameAtomicSmoke` | RENAME + 冷启动 COUNT |
| `Mdb.Phase41CreateIndexExplainWhere` | CREATE INDEX + EXPLAIN/WHERE |

夜间 CI 建议：`--gtest_filter=Mdb.Phase41*:Mdb.ShowTxn*:Mdb.BackupRestore*`

## 7. 修订记录

| 日期 | 摘要 |
|------|------|
| 2026-05-16 | 初稿：41A–41C、I-DUR、I-IDX、I-OPS、GTest 与矩阵同步 |
