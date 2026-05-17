# PHASE45：DROP INDEX 与 UNIQUE 索引（Wave 4）

在 PHASE41 命名索引基础上，支持 **删除单索引** 与 **单列唯一索引**（INSERT/CREATE 冲突检测）。

## 1. 语法

| 命令 | 说明 |
|------|------|
| `DROP INDEX name ON table` | 删除 `nidxdef$` / `nidx$` 键；`BEGIN` 内拒绝 |
| `CREATE UNIQUE INDEX name ON table(col)` | 定义值 `u:<col>`；重建 postings 时查重 |
| `CREATE INDEX …` | 非唯一（与 PHASE41 相同） |

**非目标**：复合索引 `(a,b)`；在线 DDL；`DROP INDEX` 墙钟恢复。

## 2. 存储

- 定义键值：`u:column` 表示唯一；否则为 plain `column`（向后兼容旧定义）。
- `gather_named_index_keys_for_index`：仅删指定索引前缀。

## 3. 回归

```text
structdb_tests --gtest_filter=Mdb.Phase45*
```

| 测试 | 覆盖 |
|------|------|
| `Mdb.Phase45DropIndex` | CREATE / EXPLAIN / DROP / EXPLAIN |
| `Mdb.Phase45UniqueIndexInsertConflict` | 重复键 CREATE UNIQUE 失败 |

## 4. 修订记录

| 日期 | 摘要 |
|------|------|
| 2026-05-16 | 初稿：DROP INDEX、CREATE UNIQUE INDEX、GTest |
