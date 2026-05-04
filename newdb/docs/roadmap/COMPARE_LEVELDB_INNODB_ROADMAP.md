# newdb 与 LevelDB / InnoDB 对标与路线图（阶段 D）

本文是 **一页索引**：浓缩对比结论与分阶段优化路线；**深度对比**（六维结构、优劣小节、M1/M2 里程碑）以 LaTeX 附录为准。

## 1. 深度文档位置

- **权威长文**：[intro/08-compare-leveldb-innodb/section.tex](../../intro/08-compare-leveldb-innodb/section.tex)（编译 PDF：`intro/out/newdb-intro.pdf`，参见 `intro/README.md`）。
- **模块依赖**：[MODULE_BOUNDARIES.md](MODULE_BOUNDARIES.md)。
- **设计点映射**：[NEWDB_DESIGN_POINT_TO_FILE_MAP.md](NEWDB_DESIGN_POINT_TO_FILE_MAP.md)（含本文档与执行计划文档链接）。

## 2. 三者一句话

| 系统 | 形态 | 强项 |
|------|------|------|
| **newdb** | 页式 heap + WAL + MVCC 快照 + CLI 事务协调 | 可读、可改、教学/原型；恢复链路相对短 |
| **LevelDB** | LSM + WAL | 高写吞吐、顺序写、嵌入式 KV 成熟 |
| **InnoDB** | B+Tree 聚簇 + redo/undo + 锁 | 通用 OLTP、事务与运维成熟 |

## 3. newdb 优劣摘要

- **优势**：工程边界清晰；WAL + 恢复快照/savepoint/PITR；写路径相对轻；配套测试与 CI 门。
- **相对 LevelDB**：缺少系统性分层 compaction；长期空间与尾延迟治理需自建 vacuum/监控。
- **相对 InnoDB**：锁与隔离级别深度不足；无完整 undo 版本链与 buffer pool 级页生命周期。

## 4. 路线图（执行顺序）

1. **阶段 A** — 存储治理与恢复预算：[STORAGE_GOVERNANCE_AND_RECOVERY_BUDGETS.md](STORAGE_GOVERNANCE_AND_RECOVERY_BUDGETS.md)。
2. **阶段 B** — 事务语义与测试矩阵：[TXN_ISOLATION_AND_LOCKING.md](TXN_ISOLATION_AND_LOCKING.md)、[TESTS_FAULT_INJECTION_AND_TXN_MATRIX.md](TESTS_FAULT_INJECTION_AND_TXN_MATRIX.md)。
3. **阶段 C** — 性能与 CI：[PERF_AND_CI_BUDGETS.md](PERF_AND_CI_BUDGETS.md)。
4. **边界**：不追求短期替代生产核心 InnoDB 或极限嵌入式 LSM；定位教学/原型/可控负载。
