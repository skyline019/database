# `undo.log` 生命周期与四期 4C（八期）

本文档是 [`POLICY.md`](POLICY.md) **§3.5** 的展开稿，描述 **`undo.log` 截断 / 回收** 的安全条件、与 **WAL 前缀裁剪**、**`kOpenFlagRebuildUndoStackFromLog`** 的关系，以及当前代码提供的 **显式 API** 与 **可选自动行为**（默认关闭）。

## 1. 背景与不变式

- **`undo.log`**：版本化 `mdb$` 覆盖写前，将「上一物理存储值」以 **`STRDBUV1`** 帧追加到磁盘；用于 **`rollback_one_undo_frame`** 与进程内 **`undo_stack_`（LIFO）**。
- **`undo_stack_`**：默认仅在当前进程内由版本化 `put` 填充；`open(..., kOpenFlagRebuildUndoStackFromLog)` 时从 **逻辑 `undo` 串**（**二十二 22C**：按 `undo.segments` v2 封存顺序拼接 **`undo/archive/*` + 尾 `undo.log`**；未启用分段时等同单 **`undo.log` 全文**）顺序扫描重建栈（实验性，见 `CHANGELOG`）。
- **WAL 前缀裁剪**（`wal_try_trim_prefix_through_checkpoint` / 4B）：只缩短 **`wal.log`**；**不得**据此暗示已安全截断 **`undo.log`**（`POLICY` §3.5）。

**核心不变式（截断前）**：若仍存在依赖 **`undo.log` 前缀帧** 的 **`undo_stack_` 回滚需求**（栈非空），或无法保证与已重放 WAL 的 **`commit_seq` 水位**一致，则 **禁止**截断 `undo.log`，否则 `open` 后 **`rollback_one_undo_frame`** 或与 **`RebuildUndoStackFromLog`** 组合的语义可能失真。

## 2. 与 WAL trim、`RebuildUndoStackFromLog` 的互斥矩阵（概念）

| 场景 | 截断 `undo.log` | 说明 |
|------|-----------------|------|
| `undo_stack_` 非空（进程内仍有可 pop 帧） | **禁止** | 显式 API 返回失败 |
| 仅缩短 `wal.log`（4B） | **独立** | 不自动截 `undo.log` |
| `flush_memtable` 后（实现会 **清空** `undo_stack_`） | **可选** | 栈已空；旧磁盘帧对「仅默认 open」会话本不可再通过栈回滚；若需依赖 **`RebuildUndoStackFromLog`** 恢复跨重启回滚链，**勿**在未理解语义前开启自动截断 |
| `kOpenFlagRebuildUndoStackFromLog` | **慎用 + 截断** | 截断后重建栈为空；须与运维预期一致 |
| **多段 `undo`（二十二 22C，`undo.segments` v2）** | **按 §3.5 + `PHASE22`** | 重建栈时按封存顺序拼接 **`undo/archive/*` + 尾 `undo.log`**；前缀回收在 **逻辑字节串** 上调整 `undo_stack_` 帧偏移；未启用分段时与单文件语义一致 |

## 3. 当前实现（八期 / 4C 子集）

### 3.1 `StorageEngine::undo_try_truncate_when_stack_empty`

- **语义**：在引擎已 `open` 且 **`undo_stack_` 为空** 时，将 **`undo.log` 截断为 0 字节**并重新打开追加写句柄。
- **失败**：栈非空、未打开、或底层 `resize_file`/重打开失败（错误串见实现）。

### 3.2 `EngineConfigSnapshot::undo_auto_truncate_after_flush`（默认 `false`）

- **语义**：当为 `true` 时，在 **`flush_memtable` 成功写完 checkpoint**（且可选 WAL trim 成功）之后，**无条件**对 `undo.log` 做与 3.1 相同的截断（此时 **`flush_memtable` 已清空 `undo_stack_`**，与「栈空」一致）。
- **默认**：`false`，与既有行为一致。

### 3.3 物理分段（二十二 22C / 十六期 16B 子集）

- **配置**：`EngineConfigSnapshot::undo_segment_roll_max_bytes`（默认 **0**=关闭），须在 **`StorageEngine::open` 之前** 经 `StorageEngine::set_undo_segment_roll_max_bytes` 生效（与 WAL 分段配置形状一致）。
- **元数据**：`data_dir/undo.segments` **v2**（与 `wal.segments` v2 同形：格式版本行、`next_roll_seq`、封存相对路径列表）；封存文件位于 **`undo/archive/{seq}.log`**（`seq` 单调递增）。
- **前缀回收**：`undo_try_truncate_recyclable_prefix` 在 **逻辑 undo 字节串**（封存段顺序 + 当前 `undo.log`）上消费前缀；`undo_stack_` 中帧偏移为该串上的 **全局** 起始字节。
- **运维**：启用分段后备份须包含 **`undo.segments`** 与 **`undo/archive/`** 及尾 **`undo.log`**（见 [`POLICY.md`](POLICY.md) §4.0.2）。

### 3.4 未纳入（仍为后续）

- 与 **checkpoint 槽** 内其它「已回收 undo 前缀偏移」类扩展字段（若有）的联合持久化。
- **七期 7C / 六期 6D**（`BEGIN` 内 `persist_table`）：见 [`TXN_BEGIN_PERSIST_DESIGN.md`](TXN_BEGIN_PERSIST_DESIGN.md) 与 [`PHASE17.md`](PHASE17.md)；**跨层 `ROLLBACK`** 与 `undo_stack_` 链式对齐 **未**实现。

**十期已合入（与分段对照）**：**按字节前缀** 的物理回收通过 **`undo_try_truncate_recyclable_prefix`** 与 checkpoint **v2** 水位字段实现；见 [`PHASE10.md`](PHASE10.md)、[`CHECKPOINT_UNDO_PREFIX.md`](CHECKPOINT_UNDO_PREFIX.md)。**不**改变上表「栈非空禁止整文件清空」的边界；前缀截断仍要求调用方遵守 **`POLICY` §3.5**。

## 4. 运维建议

- 需要 **磁盘回收** 且接受 **`RebuildUndoStackFromLog` 不再能从历史 `undo.log` 恢复回滚链** 时，再考虑在维护窗口调用显式截断，或开启 **`undo_auto_truncate_after_flush`** 并充分回归。
- **备份**：截断前备份 `data_dir/undo.log`（若存在），与 WAL / checkpoint 备份策略一致；启用 **22C 分段** 时另须备份 **`undo.segments`** 与 **`undo/archive/`**。
