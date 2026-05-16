# 二十四期：部署边界、耐久叙事与文档入口

在 **二十三期**（[`PHASE23.md`](PHASE23.md)）收口链式 `ROLLBACK` 与 I/O 矩阵的前提下，本期把 **单写者 / 绕过 MDB、崩溃尾部与 fsync、WAL 混排重放、文档入口与根 README 漂移** 收口为可运维说明与 **可选观测/硬闸**。

## 1. 子阶段与交付

| 子阶段 | 内容 | 默认 |
|--------|------|------|
| **24A** | [`POLICY.md`](POLICY.md) §4.2 / §4.3：同进程多 `EmbedClient`、多线程与保底模型；链式门闩开启时的部署清单。**可选**：`EngineConfigSnapshot::observe_embed_bypass_during_mdb_chain_txn`（计数 `Engine::embed_bypass_kv_put_during_mdb_chain_observed()`）；**可选**：`strict_reject_direct_kv_put_during_mdb_chain_txn`（`Engine::kv_put` 对 `mdb$*` 在提示活跃事务时 **拒绝**）。`mdb_runner` 在 `BEGIN`/`COMMIT`/`ROLLBACK` 维护 `Engine::set_mdb_chain_txn_active_hint`。 | 观测/严格均 **默认关** |
| **24B** | [`POLICY.md`](POLICY.md) §4.5 后 **嵌入式耐久矩阵**（与 [`TXN_INNODB_MAP.md`](TXN_INNODB_MAP.md) §2 交叉引用）：未 fsync 尾部可能丢什么、涉及哪些开关。 | 文档 |
| **24C** | [`WAL_REPLAY.md`](WAL_REPLAY.md)：帧类型、`STDBBW1` 与文本行判别、尾帧截断、`open` 重放顺序；`POLICY` §3.1 链入。 | 文档 |
| **24D** | [`ONBOARDING.md`](ONBOARDING.md) 新人阅读顺序；[`README.md`](README.md)（Docs 索引）、[`PHASE13_PLUS_PLAN.md`](PHASE13_PLUS_PLAN.md) `p24`；**根** [`README.md`](../README.md) MVCC / `ROLLBACK` 与 23C 对齐。 | — |
| **24E** | [`CHANGELOG.md`](CHANGELOG.md) 计划条目；`structdb_tests` **`Engine.Phase24*`**。 | — |

## 2. 非目标

- **不**默认引入多写者并发正确性（行锁、2PC）。
- **不**改变 WAL 崩溃恢复权威、不默认每条写 `fsync`。
- **不**将 `rollback_one_undo_frame` 的 WAL 行默认改为 `STDBBW1` 帧（除非单独开期）。

## 3. 验收

- 根 `README` 与 `POLICY` §4.3 对链式 `ROLLBACK` **无矛盾**。
- 可从 `PHASE24` / `ONBOARDING` 直达 **单写者 / 耐久矩阵 / WAL_REPLAY**。
- `structdb_tests` 全绿。

## 4. 修订记录

| 日期 | 摘要 |
|------|------|
| 2026-05-13 | 初稿：24A–24E 边界与文档入口 |
