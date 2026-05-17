# PHASE42：轻量 GROUP BY 与索引序 SCAN（Wave 3）

在 PHASE41 命名索引基础上，提供单表 **内存聚合** 与 **按命名索引键序 SCAN**，不引入 JOIN/SQL。

## 1. 语法

| 命令 | 说明 |
|------|------|
| `GROUP BY (col) COUNT` | 按 `col` 分组计数 |
| `GROUP BY (col) SUM(other)` | 按 `col` 分组，对 `other`（int/float/double）求和 |
| `SCAN INDEX(idx)` | 按命名索引 KV 键序输出行（≤5000）；无 postings 时按索引列值排序回退 |

**约束**：`BEGIN` 内拒绝；分组数 ≤5000；无 `HAVING`/JOIN。

## 2. 与既有聚合区别

| 命令 | 范围 |
|------|------|
| `SUM(col)` / `AVG` / `MIN` / `MAX` | 全表单列 |
| `QBAL(col,min)` | 过滤后计数+求和 |
| `GROUP BY …` | 分组聚合 |

## 3. 索引序 SCAN

- 优先 `kv_visit_prefix` 扫描 `mdb$v2$nidx$<table>$<idx>$<col>$` postings，主键取键尾 `$` 后段。
- 若存储侧无 postings（仅内存表），按索引列字典序遍历 `current.rows`。

## 4. 与 PAGE_JSON / ORDER BY

多列排序与分页由既有 **`PAGE_JSON(page,pageSize,col,asc|desc)`** 提供（`partial_sort` 路径）；与 `GROUP BY` 正交。

示例：`PAGE_JSON(1,10,v,desc)` 按列 `v` 降序第一页。

**验收**：`Mdb.Phase42OrderByPageJson`。

## 5. 回归

```text
structdb_tests --gtest_filter=Mdb.Phase42*
```

| 测试 | 覆盖 |
|------|------|
| `Mdb.Phase42GroupByCountAndSum` | COUNT + SUM |
| `Mdb.Phase42ScanIndexOrder` | SCAN INDEX 三行 |

## 6. 修订记录

| 日期 | 摘要 |
|------|------|
| 2026-05-16 | 初稿：GROUP BY、SCAN INDEX、GTest |
| 2026-05-16 | Wave 4：§4 PAGE_JSON + `Mdb.Phase42OrderByPageJson` |
