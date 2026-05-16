# 二十期：存储子系统大改版（20A / 20B / 20C）

本文档描述 **二十期** 的目标、磁盘格式、锁序与验收矩阵；与 [`POLICY.md`](POLICY.md) §3.3、§4.0、§4.2 及 [`PHASE13_PLUS_PLAN.md`](PHASE13_PLUS_PLAN.md) 路线图一致。

## 1. 定位

- **20A — 多段 WAL**：在保留 **WAL 链为崩溃恢复权威** 的前提下，支持 **`wal.segments` v2** 与 **`wal/archive/`** 下已封存段；`open` 按 **封存段序（全文件重放）→ 当前 `wal.log`（自 `checkpoint.wal_offset`）** 重放。
- **20B — Compaction worker**：**有界队列 + 单后台线程**；仅执行经 Facade/编排授权的 **drain L0** 类任务；**所有** `StorageEngine` 状态变异仍在持 **`mu_`** 的 API 内完成（worker **不**构成第二随意写者）。
- **20C — 异步 I/O**：Windows 上可选 **IOCP 顺序 WAL 追加**（`STRUCTDB_WITH_IOCP`）；Linux 上可选 **io_uring**（`STRUCTDB_WITH_IO_URING`，默认 **OFF**），与 `POLICY` §2.4 一致。

## 2. 非目标

- **十七期**（`BEGIN` 内 `persist_table`）：见 [`PHASE17.md`](PHASE17.md)；二十期不扩展其语义。
- **不**引入 `engine/storage` → orchestrator/planner/facade **反向依赖**（`POLICY` §2.2）。
- **不**改变 **先 MANIFEST、再 checkpoint**（§3.4 / §3.3.1）。

## 3. `wal.segments` v2 磁盘格式

文本文件，UTF-8，换行 `\n`：

1. 首行：`2`（格式版本）。
2. 第二行：十进制 `next_roll_seq`（下一封存文件序号，单调递增）。
3. 第三行：十进制 `N`（封存段数量）。
4. 接下来 `N` 行：每行为相对 `data_dir` 的 POSIX 风格相对路径（禁止 `..`），**最旧 → 最新**。
5. **当前尾段**固定为根目录下的 **`wal.log`**（不出现在列表中）。

**v1（十六期占位）**：首行为 `1`、第二行为段计数；实现仍可为单物理文件。打开时若首行为 `1`，按既有单文件语义。

**拒绝打开**：v2 解析失败、路径非法、或列出的封存文件缺失时，`StorageEngine::open` 失败并返回错误信息。

## 4. WAL 前缀裁剪与封存段

- `wal_try_trim_prefix_through_checkpoint` **仅**重写 **`wal.log`**，语义与单文件期一致：`checkpoint.wal_offset` 之前的尾段前缀被折叠，持久化 `wal_offset` 置 **0**。
- **不**在二十期 MVP 中默认删除 `wal/archive/` 下文件。**二十一期 21A** 起提供 **显式 opt-in** 封存回收（`wal_archive_gc_after_flush` + `wal_auto_trim_prefix_after_flush`），规格见 [`PHASE21.md`](PHASE21.md) §3；未开启时行为与本节原述一致。

## 5. 锁序（20B + 单逻辑写者）

1. **主写路径**（`put` / `flush_memtable` / `commit_embed_batch` 等）与 **worker** 均通过 **`StorageEngine::mu_`** 序列化变异。
2. **禁止**：在已持 `mu_` 的代码路径中 **阻塞等待** worker 执行另一项同样需要 `mu_` 的任务（避免自死锁）。当前实现：**入队 / 等待** 在 Facade 调用方完成，worker 仅在执行任务时获取 `mu_`。
3. **GraphExecutor**：`drain_l0_compaction` 算子行为不变；当启用 **compaction worker** 时，`Engine::drain_l0_compaction_queue` 走 **入队并等待完成**（与「同步直接 `drain_pending_l0_compactions`」二选一由配置决定，默认关闭 worker = 同步路径）。

## 6. CMake 与平台

| 选项 | 默认 | 说明 |
|------|------|------|
| `STRUCTDB_WITH_IOCP` | Windows MSVC：**ON**；其他：**OFF** | 构建 IOCP 顺序写实现；关闭时 `IoBackendKind::IocpAsync` 在运行时可拒绝或回退（见 `CHANGELOG`）。 |
| `STRUCTDB_WITH_IO_URING` | **OFF** | 仅 Linux；链接 `liburing` 时方可启用真路径；未启用时保留编译桩或禁用枚举路径。 |

## 7. 验收矩阵（摘要）

| 子阶段 | 验收 |
|--------|------|
| 20A | 多段轮换 + `open` 全量重放；尾帧截断规则不变；v2 损坏/缺文件 **拒绝打开**。 |
| 20B | 队列有界（满则失败）；TSan 无新增竞态；`drain_l0_compaction_queue` 在 worker 模式下功能等价于同步 drain。 |
| 20C | IOCP 下 WAL 帧顺序与 fsync 语义与阻塞模式一致；CI 文档标明 io_uring 为可选。 |

## 8. 相关代码锚点

- `src/engine/storage/src/storage_engine_open_wal.cpp` — 构造、`open`/`close`。
- `src/engine/storage/src/recovery_coordinator.cpp` — **`replay_checkpoint_and_wal`**（封存段 + 尾 `wal.log` 重放调度）。
- `src/engine/storage/src/wal_replay_applier.cpp` — **`apply_line_unlocked`** / **`apply_batch_unlocked`**（重放帧解码）。
- `src/engine/storage/src/storage_engine_put_undo.cpp` — `wal_try_trim_prefix_through_checkpoint*`（trim 折叠尾段前缀）。
- `src/engine/storage/src/storage_engine_segments_worker_checkpoint.cpp` — `wal_roll_to_new_segment_unlocked_`、`wal_maybe_roll_after_append_unlocked_`、`start_compaction_worker` / `stop_compaction_worker` / `enqueue_drain_l0_compaction_and_wait`。
- `src/engine/storage/src/wal.cpp` — `WalWriter`、可选 IOCP 写路径。
- `src/engine/facade/src/engine.cpp` — 配置注入、`drain_l0_compaction_queue` 分流。
- `src/engine/infra/include/structdb/infra/io_backend.hpp` — `IoBackendKind`。
