# PHASE46（远期）：逻辑 namespace `mdb$ns$`（I-SCH）

**状态**：设计占位；无默认实现（Wave 4 远期轨 F）。

## 1. 动机

多租户场景下在单 `data_dir` 内用 **逻辑前缀** 隔离 catalog，替代 SQL `CREATE SCHEMA`。

## 2. 草案键空间

| 键 | 用途 |
|----|------|
| `mdb$ns$<tenant>$v2$table$…` | 表/行/索引键加租户段 |
| `mdb$ns$meta$<tenant>` | 租户 epoch / 配额 |

## 3. 触发条件

- 产品确认多租户共库需求；
- POLICY §4.0 单写者与独占锁策略评审通过。

## 4. 与 Wave 4 关系

竞品路线 **I-SCH** 列入 [`COMPETITIVE_IMPROVEMENT_PLAN.md`](../COMPETITIVE_IMPROVEMENT_PLAN.md) Wave 4 可选轨；实现推迟至 PHASE46。
