# 十九期：GraphExecutor 托管排空与默认管线扩展

本文档描述 **十九期** 已实现/计划中的行为：在 **不改变** `POLICY` §3.3.1（先 MANIFEST、再 checkpoint、单写者）的前提下，把 **十三期** 的 **`drain_pending_l0_compactions`** 接入 **`GraphExecutor` 已注册算子** 与 **`Orchestrator` 默认线性计划**，使应用在 **`l0_compact_defer_after_flush`** 打开时可通过 **`Engine::rerun_default_pipeline`**（内部 `replan_and_run`）**重复执行** `noop → drain_l0_compaction`，而无需直接依赖 `Orchestrator` 指针。

**说明**：十九期 **不** 引入后台 compaction 线程；**不** 改变 WAL 重放权威；**不** 扩展 `IoBackendKind` 的实际异步读写（仍属 **十八期** 后续）。

---

## 1. 背景

- **十三期** 已实现：`flush_memtable` 在阈值 + defer 模式下可 **推迟** L0 合并，由 **`StorageEngine::drain_pending_l0_compactions`** / **`Engine::drain_l0_compaction_queue`** 显式排空。  
- **缺口**：默认 **`run_default`** 仅执行 `noop`，**未** 在引擎生命周期内自动再走排空边车；调用方须记得手动 `drain_*`。

---

## 2. 目标

1. 在 **`Engine::startup`** 内向 **`GraphExecutor`** 注册算子 **`drain_l0_compaction`**（名称与注册表一致）。  
2. 当 **`EngineConfigSnapshot::l0_compact_defer_after_flush == true`** 时，**默认计划**（及每次 **`replan_and_run`**）为线性 **`noop` → `drain_l0_compaction`**；否则仍为单节点 **`noop`**（与历史行为兼容）。  
3. 提供 **`Engine::rerun_default_pipeline`**：对未持有 **`Orchestrator*`** 的嵌入场景，封装 **`replan_and_run`**（递增 **`plan_epoch`**）。

---

## 3. 非目标

- 不将 **`drain`** 强行插入 **`flush_memtable` 算子内部**（保持 flush 与 compact 解耦）。  
- 不在此期实现 **size-tiered**、**L3+**、**多段 WAL 重放**。

---

## 4. 验收

- `structdb_tests`：`Engine.Phase19DeferPlanIncludesDrainAndReplanReducesL0`（或等价名）：`l0_compact_defer_after_flush` + 阈值 + 多次 `flush_memtable` 后 **`rerun_default_pipeline`** 降低 SST 数量。  
- `CHANGELOG` / [`PHASE13_PLUS_PLAN.md`](PHASE13_PLUS_PLAN.md) 十九期条目与 mermaid 边 **`p18 --> p19`**（十九期为 **编排面收口**，可与十八期并行文档维护）。

---

## 5. 相关代码

| 区域 | 说明 |
|------|------|
| [`src/engine/facade/src/engine.cpp`](../src/engine/facade/src/engine.cpp) | `DrainL0CompactionOperator`、`PlanBuilder` 分支、`rerun_default_pipeline` |
| [`src/engine/facade/include/structdb/facade/engine.hpp`](../src/engine/facade/include/structdb/facade/engine.hpp) | `rerun_default_pipeline` 声明 |
| [`src/engine/orchestrator/.../orchestrator.hpp`](../src/engine/orchestrator/include/structdb/orchestrator/orchestrator.hpp) | 已有 `replan_and_run` |

---

## 6. 修订记录

| 日期 | 摘要 |
|------|------|
| 2026-05-13 | 初稿：十九期 GraphExecutor 排空算子与默认管线 |
