# StructDB 现状评估与下一步计划

本文基于当前 `OPTIMIZATION_PLAN.md` 与 `NEXT_REFACTOR_RECOMMENDATIONS.md`，对 StructDB 做一次面向执行与汇报的综合整理，覆盖：

- 按模块打分
- **计划演进快照（§0）**与执行原则
- 当前架构风险 TOP 10
- 未来 1 个月优先级执行计划
- 面向汇报的一页版总结
- 按「性能 / 稳定性 / 架构 / 体验」四象限重新归纳

**计划原则（演进）**：与长任务、I/O 节流、可观测性、C API / GUI 强相关的改动，**允许在合理边界内做大范围相关重构**（例如统一进度/取消贯穿 infra → storage → client → capi → Tauri），前提是配套单测与版本化 ABI，避免「半套接口」长期并存。

---

## 0. 计划演进快照（相对初版评估）

以下反映 **2026-05** 前后相对本文初版清单的落地情况，用于滚动更新「第 3 周」类目标，而非推翻全盘时间表。

| 主题 | 状态 | 说明 |
|------|------|------|
| 性能基线脚本 | **已落地** | `scripts/run_perf_baseline.ps1`：可重复跑关键 bench，便于前后对比。 |
| WAL / compaction 字节节流统一 | **已落地** | `SteadyClockByteTokenBucket` 复用（含 WAL），减少多套节流逻辑分叉。 |
| 长任务核心模型（C++ infra） | **已落地** | `long_task_progress.hpp`：`LongTaskKind` / `LongTaskReporter` / `LongTaskCancelToken` 等。 |
| MDB 脚本长任务 | **已落地** | `MdbRunOptions::long_task`：行级进度 + 协作取消；有回归用例。 |
| Compaction 长任务 | **部分落地** | merge materialize 多阶段 + 字节进度；worker 队列级进度与 drain 路径已接 reporter；job 内细粒度跳过/取消仍可加强。 |
| C API 长任务 | **已落地（1.8.0）** | `structdb_long_task_control*`、`STRUCTDB_CAPI_ERR_CANCELLED`、批处理 `begin/end_mdb_script_batch`、`engine_bind_long_task`；GUI 经动态库走同一控制面。 |
| GUI / Rust | **已落地** | `long-task-progress` 事件、脚本走 C API 批处理与取消；`AppState` 用 `Send` 包装持有 C 指针；export / bench 侧进度事件已接；UI 文案与更多任务类型展示可按需迭代。 |

**下一波（仍属「大范围相关重构」许可内）**：MemTable 预研与热路径压实、compaction job 级取消语义、导出/压测与 `script_cancel` 的完全对齐、文档与 `OPTIMIZATION_PLAN` 交叉引用更新。

---

## 1. 按模块打分表

> 评分采用 10 分制，偏向「当前成熟度 + 下一阶段可持续性」。

| 模块 | 分数 | 评价 |
|---|---:|---|
| `StorageEngine` | 8.5 | 核心链路完整，恢复、WAL、SST、compaction、pressure、trace 已成体系，但整体复杂度仍高。 |
| `MemTable` | 6.5 | 已有抽象、SkipList、读写拆分、frozen 层，但仍是未来最关键的性能升级点。 |
| `WAL / Recovery` | 8.0 | 恢复分阶段、重放解码独立、回归较完整；语义复杂但整体可靠。 |
| `Compaction` | 8.0 | worker、专用 I/O、分块节流、并行读、低优先级线程均已落地，重点在尾延迟与并发复杂度控制。 |
| `StorageTelemetry / Observability` | 8.5 | 压力快照、trace、bench、回归入口较完整，定位热点的基础较强。 |
| `EmbedClient` | 7.5 | journal、checkpoint、回归都不错，但仍需持续关注恢复边界与批提交成本。 |
| `mdb_runner` | 8.0 | 分页、增量 persist、`SCAN MORE` 依旧；脚本侧已接统一 **`LongTaskProgress` / 取消 token**，与 C API 对齐。 |
| `GraphExecutor` | 7.0 | 背压与预算探测已有基础，但与存储压力闭环仍可更紧。 |
| GUI | 7.5 | 分页与脚本控制仍在；**长任务**已走统一事件（含 C API 取消），export/bench 有进度事件；大结果集与全链路 UI  polish 仍可加强。 |
| `C API` | 8.0 | **1.8.0** 起长任务控制与批处理语义明确；需保持头文件/绑定与引擎行为同步演进。 |
| 测试体系 | 8.0 | 回归覆盖面较强，尤其恢复、Embed、compaction、MDB；**已增加长任务 / C API 相关用例**；性能基线需制度化对比。 |
| 配置与文档 | 8.5 | 文档体系非常完整，说明系统已经进入治理阶段。 |

### 总体判断

- **最强项**：可观测性、恢复链路、compaction 工程化、文档体系
- **最弱项**：MemTable 结构、compaction 尾延迟与 job 内取消细化、GUI 大结果集体验
- **最重要主线**：性能上限（MemTable / compaction）+ 架构边界 + **长任务主路径已通，需在覆盖范围与取消语义上收口**

---

## 2. 当前架构风险清单 TOP 10

### 1）MemTable 结构仍可能成为写入上限瓶颈
现有 SkipList / `std::map` / frozen 层已经缓解压力，但未来性能上限仍高度依赖 MemTable 结构演进。

### 2）恢复语义与 checkpoint / journal / WAL 的边界复杂
恢复链路已能工作，但边界条件多、路径多，最容易出现时序型问题。

### 3）Compaction 并发与尾延迟风险
worker、专用 I/O、分块节流、并行读会提高复杂度，也会增加 P99 抖动风险。

### 4）`StorageEngine` 仍偏“总控中心”
虽然协调器已开始拆分，但门面类仍承载较多关键状态，未来有重新膨胀风险。

### 5）MDB 仍存在「逻辑 + IO + 分页 + 持久化」耦合风险
当前已经改善，但长远看仍需要继续拆分职责边界。

### 6）长任务体验不统一（已显著缓解，仍有余量）
跨层 **`LongTaskProgress` + C API `long_task_control`** 已形成主路径；剩余风险主要在 **非脚本任务**（导出/子进程 bench）与引擎后台 job 的取消粒度、以及前端展示一致性。

### 7）配置开关多，组合矩阵会越来越大
后端、I/O、节流、分页等模式增多后，回归组合会指数上升。

### 8）可观测性已经有了，但缺统一性能基线治理
有 trace、快照、bench；**已增加基线脚本入口**，仍需把 **结果归档、对比门槛、CI 或定期任务** 固化成惯例。

### 9）大结果集 / 大分页 / 大脚本仍可能制造用户侧卡顿
即便已有部分优化，系统级游标化和缓存化仍未完全结束。

### 10）测试覆盖需要继续强化边界与性能回归
正确性测试不错，但长时间稳定性、并发边界、性能回退仍需更体系化覆盖。

---

## 3. 未来 1 个月优先级执行计划

> **滚动说明**：原「第 1 周 / 第 3 周」中与基线脚本、长任务统一相关的条目 **已部分完成**（见 §0）。下面按 **当前优先级** 重排，仍保持四周节奏；允许为同一主题跨周迭代。

### 第 1 周：基线数据化 + 热点排序（加强）

**目标**：从「能跑基线」到「基线可对比、可解释」。

**任务**
- 用 `scripts/run_perf_baseline.ps1`（及现有 bench）固定 **场景矩阵**，记录 P50 / P95 / P99（至少关键项）
- trace / pressure snapshot 与基线跑次 **同一配置** 对照
- 列出仍缺统计的长任务路径（compaction job 内、纯导出子路径等）

**产出**
- 一次可归档的基线输出 + 简短解读
- 「下一优化」排序清单（与 `OPTIMIZATION_PLAN.md` 对齐）

---

### 第 2 周：MemTable 结构预研（不变，仍是上限主线）

**目标**：确认下一代内存结构路线。

**任务**
- 评估 SkipList / Map / frozen 层的真实瓶颈
- 对比 shard、arena、lock striping、append-friendly 结构
- 明确必须保留的能力：有序遍历、前缀扫描、overlay 读、快速 flush

**产出**
- MemTable 下一阶段方案草案
- 风险评估
- 迁移成本清单

---

### 第 3 周：长任务治理「收口」与后台任务（演进）

**目标**：在 **已有统一模型** 上收口语义与覆盖范围。

**任务**
- compaction：**队列/merge 进度已有**，继续评估 **单 job 内** 取消与跳过策略（避免「已请求取消仍长时间占用 worker」）
- GUI：export / bench / 脚本 三类任务的 **进度与取消** 行为对齐（含 `script_cancel` 与 C API 的叠加语义）
- 文档：`ARCHITECTURE.md` / `OPTIMIZATION_PLAN.md` 补充长任务数据流（可选，建议与代码变更同 PR）

**产出**
- 行为一致的长任务清单（引擎 / C API / GUI）
- 新增或加强的回归用例（取消、错误码、事件 payload）

---

### 第 4 周：回归加固与发布准备

**目标**：把本迭代（含长任务与节流）稳住。

**任务**
- 全量单测 + 关键 C API / MDB / compaction 用例
- 压测与长时间运行抽样（视资源）
- 汇总优化前后对比（引用第 1 周基线）

**产出**
- 回归结果表
- 性能对比摘要
- 对外汇报材料

---

## 4. 面向老板/汇报用的一页版总结

### StructDB 当前状态

StructDB 已经具备较完整的数据库系统能力，核心存储、恢复、嵌入式事务、MDB 查询、GUI 交互和观测体系都已成型。系统当前不是“缺功能”，而是进入了“工程治理和结构优化”阶段。

### 已取得的主要进展

- 存储引擎恢复链路阶段化，WAL 重放解码和 compaction 协调已拆分
- SST 读路径优化、Bloom/min-max、compaction 专用 I/O 与节流已落地
- MemTable 已具备抽象、SkipList、读写拆分和 frozen 层
- MDB 已支持分页优化、增量 persist、`SCAN MORE`；脚本路径已接 **`LongTaskProgress` + 协作取消**，并与 **C API 1.8.0**（`long_task_control`、批处理、取消错误码）对齐
- **字节节流**：WAL 与 compaction 侧统一复用 token bucket 抽象，减少重复实现
- **长任务**：C++ infra 模型 + storage compaction merge / worker 队列进度上报 + GUI `long-task-progress` + Rust 侧安全持有 C 控制指针
- 观测体系较完整，支持 trace、压力快照和 benchmark；**性能基线脚本**已提供可重复入口

### 当前主要问题

- MemTable 仍是未来性能上限的关键短板
- compaction 的并发与尾延迟仍需持续治理；**单 job 取消粒度**仍可加强
- 长任务主路径已统一，**导出 / bench / 后台 job** 的体验与语义仍需收口
- 配置组合和边界场景的测试矩阵越来越大

### 接下来 1 个月重点

1. **基线数据化**（脚本 + 归档 + 对比解读），锁定真实热点
2. 评估 MemTable 下一代结构
3. 长任务 **收口**：compaction job 内、GUI 多任务类型、取消语义一致
4. 补齐回归和性能对比，确保改动稳定

### 一句话结论

**StructDB 已经从“功能建设期”进入“结构治理期”；长任务与字节节流已形成跨层主路径。下一阶段重点是基线数据化、MemTable / compaction 结构上限、长任务在非脚本与单 job 场景的语义收口，并保持恢复与回归稳定。**

---

## 5. 按“性能 / 稳定性 / 架构 / 体验”四象限重新整理

### 5.1 性能

**已具备**
- SST v2 / v3、min/max、Bloom
- compaction worker、专用 I/O、分块节流；**与 WAL 共享字节 bucket 抽象**
- `COMMIT_SEQ` 热路径节流
- `PAGE_JSON` / `SCAN` 优化
- `visit_prefix` 与 MemTable 前缀扫描优化

**仍待加强**
- MemTable 结构升级
- 更系统的 P50 / P95 / P99 基线（脚本已有，需 **制度化对比**）
- 大结果集游标化 / 缓存化
- 更低的尾延迟抖动

---

### 5.2 稳定性

**已具备**
- 分阶段恢复
- WAL 重放解码独立
- Embed journal / ckpt 回归
- 兼容旧 SST 格式
- 压力快照与回归体系

**仍待加强**
- 恢复与 checkpoint 的边界一致性
- compaction 并发竞态治理
- 极端时序与长时间稳定性测试

---

### 5.3 架构

**已具备**
- 门面类 + 协调器的拆分方向
- `RecoveryCoordinator`、`WalReplayApplier`、`CompactionCoordinator`、`StorageTelemetry`
- MDB 逻辑 / 持久化 / 分页拆分趋势
- 配置与文档体系较完整

**仍待加强**
- `StorageEngine` 继续瘦身
- MDB 更彻底的职责隔离
- 恢复策略与存储动作进一步解耦
- 配置入口进一步收敛

---

### 5.4 体验

**已具备**
- GUI 分页钳制
- 脚本进度与停止；**C API 长任务控制 + 统一 `long-task-progress` 事件**
- MDB 大结果集部分优化
- 导出 / bench 进度事件（与统一 payload 对齐方向）

**仍待加强**
- 大表 / 大脚本体验一致性（前端展示与后端粒度）
- 导出 / bench / compaction 与 **纯脚本取消位** 的语义完全对齐
- 一次性全量拉取入口排查（若仍有）

---

## 6. 结论

StructDB 当前已经具备较完整的数据库系统能力，技术方向正确，性能治理和结构拆分也都在持续推进。下一阶段最重要的工作不是再堆功能，而是：

1. **继续拆清边界**
2. **压实热路径**
3. **长任务治理收口**（在已统一模型上扩展覆盖、细化取消）
4. **提升 MemTable 和 compaction 的结构上限**
5. **让性能基线与回归体系更标准化**（基线脚本 → 定期对比）

如果需要，这份文档还可以继续拆成：

- `1` 页汇报版
- `1` 个月执行清单版
- 风险清单版
- 模块评分版
