# 三十七期（PHASE37）：缺陷完善与收尾

## 与三十六期的关系

[PHASE36.md](PHASE36.md) 已交付 L1+ 两阶段 compaction、可选 Facade `kv_put` 队列、GUI 独占锁环境变量与 `*Phase36*` 回归。PHASE37 **不**扩展存储语义，只做 **文档/索引/CHANGELOG 对齐**、**临时文件前缀与文档一致**、**对称并发回归**（L1→L2、L3→L4）及 **三十八期候选** 划界。

## 收尾清单

| 项 | 说明 |
|----|------|
| 变更记录 | [CHANGELOG.md](../CHANGELOG.md) Unreleased：三十六期回归三项与三十七期条目与代码一致 |
| 事务链测试索引 | [TESTING_TXN_CHAIN.md](TESTING_TXN_CHAIN.md) §14：`--gtest_filter` 含 `*Phase36*` / `*Phase37*`、`CompactionConcurrencySemanticMatrix`、`ConcurrentNestedL0DrainAndL1MergeWhilePuts` |
| 路线图 | [PHASE13_PLUS_PLAN.md](PHASE13_PLUS_PLAN.md) mermaid **`p35`→`p36`→`p37`**；§14 文档索引 |
| L1+ 临时 SST | 实现前缀 **`_tmp_tier_compact_{src_level}_{ver}_{rand}.sst`**（与 [COMPACTION.md](../COMPACTION.md)、PHASE36 描述一致） |
| Facade 队列满 | **`kv_put` 返回 `false`**；调用方重试或降载（未改 C API） |

## 三十八期候选（本期不实施）

- **`shared_mutex`**：仅 **`get`** 上 `shared_lock` 或全量读写拆分须单独不变式评审与 [POLICY.md](../POLICY.md) §4.2 更新（见 PHASE36「读并发」）。
- **Scheduler 背压**：`Engine::sync_scheduler_budget_from_storage_pressure` 根据 **`facade_kv_put_queue_depth`** 收紧预算须新配置阈值与产品语义。

## 验收

```text
.\build\tests\Release\structdb_tests.exe --gtest_filter=*Phase36*:*Phase37*:StorageEngine.CompactionConcurrencySemanticMatrix:StorageEngine.ConcurrentNestedL0DrainAndL1MergeWhilePuts:StorageEngine.*:Engine.*
```

全量：`structdb_tests`、`capi_test`（独占锁见 **`Capi.ExclusiveDirLockSecondOpenFails`**）。
