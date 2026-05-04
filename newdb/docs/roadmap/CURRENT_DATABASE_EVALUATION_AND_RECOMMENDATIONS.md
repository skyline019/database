# newdb 当前数据库评估与修改建议

> 生成日期：2026-05-04  
> 范围：基于当前 `newdb` 文档、工程结构、已实现模块与现有优化计划，对数据库能力、短板、风险和后续修改建议进行整理。

---

## 1. 总体结论

当前 `newdb` 已经从简单教学型数据库，演进为具备较完整实验内核特征的单机数据库原型。它已经覆盖了 heap 存储、WAL、MVCC 快照、事务协调、sidecar 索引、查询执行、C API、GUI、运行时统计与 CI 门禁等多个方向。

但从工程成熟度看，当前系统更适合定位为：

> **长期运行可解释、失败可恢复、性能可观测的教学/原型型数据库内核。**

不建议短期直接定位为完整生产级 OLTP 数据库，主要原因是：事务隔离、锁系统、恢复路径、查询优化器、统一缓存/内存治理和 release 级测试矩阵仍有关键缺口。

后续优化建议优先围绕以下目标推进：

1. **正确性闭环**：事务隔离、写冲突、WAL 恢复边界必须清晰且有测试覆盖。
2. **可观测性闭环**：查询、WAL、vacuum、缓存、内存、sidecar 都要能输出稳定 runtime stats。
3. **长期运行治理**：表膨胀、compact debt、vacuum 调度、恢复耗时、fallback scan 都应有预算和门禁。
4. **结构可演进**：WAL recovery、查询优化、索引生命周期、缓存预算应逐步模块化。

---

## 2. 当前已具备能力

### 2.1 存储层

当前存储层已经具备较好的基础：

- 页式 heap 存储。
- lazy heap decode。
- heap read view。
- row metadata 与 MVCC visibility 入口。
- 基础 compact / vacuum。
- 可选全局 LRU PageCache，受 `NEWDB_PAGE_CACHE_MAX_BYTES` 控制。
- `page_cache_*`、`heap_decode_slot_*`、`memory_budget_*` 等运行时观测字段。

评价：

- 对教学型数据库已经比较完整。
- 对长期运行场景，仍需要把 sidecar、query 执行和 page cache 纳入统一内存预算与淘汰策略。

### 2.2 WAL 与恢复

当前 WAL 能力较丰富，已经支持：

- `INSERT`、`UPDATE`、`DELETE`。
- `COMMIT`、`ROLLBACK`。
- `CHECKPOINT_BEGIN` / `CHECKPOINT_END`。
- savepoint。
- partial abort。
- PITR marker。
- 多 WAL segment 扫描。
- 从最近完整 checkpoint 起恢复的默认剪枝策略。
- `WalRecoveryStats` 中已有 `records_read`、`checksum_failures`、`decode_failures`、`last_complete_checkpoint_lsn`、`replay_start_lsn`、`checkpoint_scan_ms`、`redo_plan_ms`、`redo_apply_ms` 等字段。

评价：

- WAL 机制已经明显超过最小教学实现。
- 当前重点不应继续堆功能，而应拆分恢复路径，降低 `recover` 复杂度，并把恢复耗时门禁逐步从 opt-in 推向默认 CI。

### 2.3 事务与隔离

当前已经具备：

- `TxnCoordinator`。
- `TxnIsolationLevel::ReadCommitted` / `Snapshot`。
- CLI 查询读路径接入 `active_snapshot`。
- 进程内 write intent 写冲突检测。
- lock wait、timeout、deadlock victim 等统计。
- 相关并发与隔离测试。

评价：

- 当前是有限 MVCC 快照语义，而不是完整 InnoDB 风格 RC/RR。
- 需要继续统一所有读路径，避免 CLI、C API、GUI 或内部辅助路径行为不一致。

### 2.4 查询与索引

当前查询侧已有：

- WHERE 执行器。
- equality sidecar。
- covering index sidecar。
- visibility sidecar。
- table stats。
- `EXPLAIN WHERE`。
- `SHOW PLAN` JSON。
- `plan_id`、`estimated_scan_rows`、`plan_candidates_considered` 等可观测字段。
- 可选 `NEWDB_QUERY_COST_MODEL`。
- NDV 驱动的轻量选择性估计。

评价：

- 当前已经具备轻量优化器雏形。
- 短板是还没有完整候选计划枚举、统一 cost model、列级 histogram 和成熟统计信息维护机制。

### 2.5 可观测性与 CI

当前已经具备：

- `newdb.runtime_stats.v1`。
- `validate_runtime_stats.py`。
- `ci_bench_gate.py`。
- `capture_baseline.py`。
- `verify_clean_reconfigure.ps1`。
- `nightly_soak_hints.py`。
- `newdb_runtime_report`。
- GitHub CI 中的 runtime contract / index catalog enforce 等 job。

评价：

- 可观测性方向是当前项目优势。
- 后续关键是把 opt-in 的门禁逐步沉淀为 PR / Nightly / Release 分层默认策略。

---

## 3. 当前主要短板与风险

| 领域 | 当前短板 | 主要风险 |
|---|---|---|
| 事务隔离 | 有限 MVCC 快照语义，不是完整 InnoDB RC/RR；GUI/C API 等路径仍需持续审计 | 并发场景下用户预期与真实语义不一致 |
| 锁系统 | 以 write intent 为主，缺少范围锁、谓词锁、二级索引级冲突抽象 | 后续支持范围查询、复杂更新、二级索引一致性时扩展困难 |
| WAL 恢复 | 已有 checkpoint 剪枝，但 recovery 结构仍可继续拆分；恢复耗时门禁不是全员默认 | 大 WAL 或复杂 fault 下恢复路径难维护、性能回归不易第一时间暴露 |
| 存储治理 | compact debt、table storage health 已有雏形，但 soak 阈值和 release 矩阵仍不完整 | 长期运行后表膨胀、碎片、vacuum 滞后可能累积 |
| 查询优化 | 轻量 cost model 已有，但完整候选枚举、histogram、统一优化器仍缺 | 大表、偏斜数据、多条件查询下计划不稳定 |
| 缓存与内存 | PageCache 与 memory budget 已有，但 sidecar/query 尚未统一纳入 cap 与淘汰策略 | 大数据集或多查询并发下内存峰值不可控 |
| 测试矩阵 | 单项测试较多，但 fault injection、长 soak、release-grade 全矩阵仍不足 | 复杂组合回归不易提前发现 |
| 工程结构 | 模块多、迁移痕迹多，部分旧路径删除与新路径并存 | 维护成本升高，CMake 与 include 路径容易漂移 |

---

## 4. 修改建议优先级

### P0：先做基线与正确性收敛

这是当前最优先级，不建议跳过。

建议修改：

1. 固化当前 baseline：
   - 全量 configure。
   - 全量 `ctest`。
   - bench gate。
   - runtime stats JSONL。
   - WAL recovery elapsed。
   - compact debt peak。
   - page cache hit/miss。
   - memory budget reject。
   - fallback scan ratio。

2. 统一事务读路径：
   - 审计 `PAGE`、`WHERE`、`COUNT`、`FIND`、`SUM`、`AVG`。
   - 审计 CLI、C API、GUI 是否都通过一致 snapshot 入口。
   - 明确 `ReadCommitted` 是语句级 snapshot。
   - 明确 `Snapshot` 是事务级 pinned snapshot。

3. 强化写冲突测试：
   - 同表同 key 冲突。
   - 同表不同 key 不冲突。
   - 多 coordinator 同 workspace。
   - timeout。
   - deadlock victim 统计。

4. 把事务语义写入文档和 CLI 输出：
   - 避免用户误以为当前已经是完整 InnoDB RR/RC。

验收标准：

- 文档、CLI 输出、测试行为一致。
- 所有读入口在隔离语义上可解释。
- CI 至少能覆盖基本 RC/Snapshot 差异。

---

### P1：拆分 WAL recovery，强化恢复门禁

建议修改：

1. 继续拆分 recovery：
   - `WalSegmentScanner`：发现 segment、建立 LSN 范围和 offset index。
   - `WalRecordReader`：读取 record、校验 checksum、处理 seek。
   - `WalRedoPlanner`：聚合 pending txn、识别 commit/rollback/partial abort。
   - `WalRedoApplier`：应用 redo/undo，并输出 apply stats。

2. 强化 checkpoint 语义：
   - 明确 checkpoint id。
   - 明确 snapshot lsn。
   - 记录 table/schema version。
   - 记录 sidecar catalog version。
   - 不完整 checkpoint 必须回退到上一个完整 checkpoint。

3. 增加恢复指标：
   - `records_after_checkpoint`。
   - `segments_after_checkpoint`。
   - `checkpoint_scan_ms`。
   - `redo_plan_ms`。
   - `redo_apply_ms`。
   - `index_rebuild_ms`。

4. 把 WAL recovery elapsed gate 分层：
   - PR：轻量 fixture。
   - Nightly：真实业务 JSONL。
   - Release：release-grade 上限。

验收标准：

- recovery 模块职责清晰。
- checkpoint fault 用例稳定。
- 大 WAL 下恢复时间可被 JSONL 和 CI 观测。

---

### P2：完善存储治理与 vacuum debt

建议修改：

1. 将 `TableStorageHealth` 作为长期治理核心：
   - logical rows。
   - physical rows。
   - tombstone rows。
   - live bytes。
   - dead bytes。
   - fragmentation ratio。
   - last vacuum lsn。
   - last vacuum elapsed。

2. 稳定 compact debt 公式：
   - 以 dead bytes 为核心。
   - fragmentation ratio 作为加权项。
   - `wal_since_last_vacuum` 先保持可选实验项。

3. 强化 vacuum 队列：
   - 入队优先级与 runtime `compact_debt_priority` 同源。
   - cooldown skip 明确计数。
   - vacuum 成功后必须刷新 `last_vacuum_*`。

4. 建立 soak 门禁：
   - `compact_debt_bytes_peak`。
   - `fragmentation_ratio`。
   - `vacuum_cooldown_skip_count`。
   - `table_storage_health_last_vacuum_elapsed_ms`。

验收标准：

- 长期写删负载下 compact debt 不无限增长。
- vacuum 行为可解释。
- Nightly soak 能给出阈值建议。

---

### P3：推进查询优化器和统计信息

建议修改：

1. 完善 `TableStats`：
   - logical rows。
   - NDV。
   - null count。
   - min/max。
   - schema fingerprint。

2. 引入最小 histogram：
   - 先做轻量 bucket。
   - 不必一次实现完整 equi-depth histogram。
   - 优先覆盖高频等值和范围查询。

3. 统一 cost model：
   - full scan cost。
   - equality sidecar cost。
   - covering sidecar cost。
   - intersection cost。
   - visibility filter cost。

4. 强化 `SHOW PLAN`：
   - 输出候选计划。
   - 输出选择原因。
   - 输出估算 vs 实际 rows。
   - 输出是否使用 stale stats。

验收标准：

- 常见 WHERE 查询能解释为什么选某条路径。
- 偏斜数据下计划不明显退化。
- stats stale 时有可观测字段。

---

### P4：统一缓存与内存治理

建议修改：

1. 将以下对象纳入统一 memory budget：
   - heap page cache。
   - equality sidecar cache。
   - bloom sidecar cache。
   - visibility sidecar cache。
   - query 临时结构。
   - table stats / histogram。

2. 明确淘汰策略：
   - LRU。
   - pin count。
   - dirty / clean 区分。
   - 大对象 admission control。

3. 扩展 runtime stats：
   - per-cache used bytes。
   - eviction count。
   - admission reject count。
   - pinned bytes。
   - spill count。

4. 建议保留环境变量开关：
   - `NEWDB_MEMORY_BUDGET_MAX_BYTES`。
   - `NEWDB_PAGE_CACHE_MAX_BYTES`。
   - `NEWDB_SIDECAR_CACHE_MAX_BYTES`。

验收标准：

- 大表查询时内存峰值受控。
- sidecar 冷/热路径可观测。
- 单页超 cap、总量超 cap、淘汰行为都能在 stats 中体现。

---

### P5：清理工程结构与构建边界

建议修改：

1. 清理旧目录迁移痕迹：
   - `cli/modules/logging` 与 `cli/modules/common/logging`。
   - `cli/modules/util` 与 `cli/modules/common/util`。
   - `cli/modules/view` 与 `cli/modules/common/view`。
   - `shell/dispatch` 旧路径与新 `registry/router/support/shared` 路径。

2. 收敛 CMake：
   - 明确 core、demo_lib、tests、shared library 的源文件归属。
   - 避免同一源文件在旧路径和新路径重复出现。
   - 对生成目录、build 目录、target 目录加强 `.gitignore`。

3. 建议忽略以下本地产物：
   - `newdb/build/`。
   - `newdb/build_verify/`。
   - `newdb/build_local_gtest/`。
   - `newdb/rust_gui/src-tauri/target/`。
   - `*.obj`、`*.pdb`、`*.lib`、`*.dll`、`*.exe`。
   - `*.wal`、`*.wal_lsn`、`*.walsync.conf`。
   - sidecar 临时或运行产物。

验收标准：

- `git status` 不再被大量构建产物淹没。
- CMake 源文件列表与真实模块边界一致。
- 新开发者能快速判断哪些文件是源码，哪些是产物。

---

## 5. 推荐实施路线

### 第一阶段：1 到 2 天

目标：建立基线，减少盲改。

建议完成：

1. 清理 `.gitignore`，过滤 build、target、WAL、sidecar 产物。
2. 跑一次 clean configure + ctest。
3. 生成 runtime stats fixture。
4. 记录当前 WAL recovery、compact debt、page cache、memory budget 指标。
5. 把 baseline manifest 保存为 CI artifact。

### 第二阶段：3 到 5 天

目标：事务隔离和 WAL recovery 正确性收敛。

建议完成：

1. 审计所有读路径 snapshot 接入。
2. 强化 RC vs Snapshot 测试。
3. 拆分 WAL recovery 的 reader/planner/applier 最小版本。
4. 增加 checkpoint fault 用例。
5. 把 WAL recovery elapsed gate 接入 PR 可选门。

### 第三阶段：1 到 2 周

目标：长期运行治理。

建议完成：

1. 稳定 compact debt 公式。
2. 完善 TableStorageHealth 输出。
3. 增加 storage soak。
4. 将 vacuum 成功后的治理指标写入 runtime stats。
5. 将 storage 门禁纳入 Nightly 默认。

### 第四阶段：2 到 4 周

目标：性能可解释。

建议完成：

1. 完善 TableStats。
2. 引入轻量 histogram。
3. 统一 query cost model。
4. 扩展 `SHOW PLAN`。
5. 对 sidecar/query/cache 做统一内存预算。

---

## 6. 不建议立即投入的方向

短期不建议优先做以下事项：

1. 完整 SQL parser。
2. 完整 InnoDB 级 RR/RC/Next-Key Lock。
3. 分布式事务。
4. 复杂 B+Tree 二级索引全套实现。
5. MVCC undo log 全量工业化。
6. 多租户资源隔离。

原因：当前更核心的问题是已有模块的语义闭环、恢复闭环、可观测闭环和 CI 闭环。过早扩展功能会放大维护成本。

---

## 7. 当前最推荐的下一步

如果只选一个最值得马上做的方向，建议选择：

> **先清理工程产物与建立 baseline，再推进事务读路径审计和 WAL recovery 门禁。**

具体顺序：

1. 更新 `.gitignore`，让 `git status` 只显示真实源码和文档修改。
2. 跑 clean configure + ctest。
3. 生成 runtime stats baseline。
4. 审计所有读路径是否一致消费 snapshot。
5. 把 WAL recovery elapsed gate 固化到 PR opt-in / Nightly default。

这样收益最高：

- 能降低仓库噪音。
- 能减少后续误改风险。
- 能快速发现正确性回归。
- 能让后续性能优化有可比较基线。

---

## 8. 总结

当前 `newdb` 的最大优势是：模块覆盖面广、文档较完整、可观测性方向已经启动、测试数量较多。

当前最大问题不是“缺少功能”，而是：

- 语义边界还需要进一步收敛。
- 恢复路径还需要进一步结构化。
- 长期运行治理还需要更强默认门禁。
- 查询优化和缓存预算还需要统一模型。
- 工程产物和源码边界需要清理。

推荐后续按照以下优先级推进：

1. **基线与仓库清理**。
2. **事务隔离与读路径一致性**。
3. **WAL recovery 拆分与恢复门禁**。
4. **storage health / compact debt / vacuum soak**。
5. **query stats / cost model / SHOW PLAN**。
6. **统一 memory budget 与 cache eviction**。

完成这些后，`newdb` 会更接近一个可长期演进、可解释、可测试、可恢复的数据库内核。