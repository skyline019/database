# PHASE46（远期）：只读 SQL → MDB 映射调研（I-SQL）

**状态**：调研稿；**不**改动存储引擎（Wave 4 远期轨 F）。

## 1. 目标子集

| SQL | MDB 等价（草案） |
|-----|------------------|
| `SELECT col FROM t WHERE c = v LIMIT n` | `USE(t)` + `WHERE(c,=,v)` + `PAGE(1,n,col,asc)` |
| `SELECT COUNT(*) FROM t` | `COUNT` |
| `ORDER BY c DESC` | `PAGE_JSON(1,n,c,desc)` |

## 2. 明确不支持

JOIN、子查询、聚合函数全集、`RECOVER`、DDL（指向 PHASE41/45 子集）。

## 3. 建议实现形态

独立只读工具进程（非 REPL 内嵌），解析 → 生成 `.mdb` 脚本或 `structdb_mdb_execute_line` 序列。

## 4. 修订记录

| 日期 | 摘要 |
|------|------|
| 2026-05-16 | Wave 4 占位：映射表与边界 |
