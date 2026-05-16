# WAL 重放与帧形态（权威：`wal.log` + checkpoint）

本文与 [`POLICY.md`](POLICY.md) **§3.1**、**§3.4** 及 [`CHANGELOG.md`](CHANGELOG.md) 中 WAL / 批次说明一致；实现锚点为 **`RecoveryCoordinator::replay_checkpoint_and_wal`** 内 **`replay_one`** 与 `wal_replay_from_offset`（[`recovery_coordinator.cpp`](../src/engine/storage/src/recovery_coordinator.cpp)）；帧解码在 **`WalReplayApplier`**（[`wal_replay_applier.cpp`](../src/engine/storage/src/wal_replay_applier.cpp)）。

## 1. 崩溃恢复读哪里

1. 加载 `MANIFEST`、读 **checkpoint**（含 `wal_offset`）。若 checkpoint 中 `manifest_version` **大于** 已加载 MANIFEST 版本 → **拒绝打开**。
2. 若启用多段 WAL（`wal.segments` v2），按元数据顺序 **自字节 0** 重放各 **封存段**，再对当前尾文件 **`wal.log`** 自 **`checkpoint.wal_offset`** 起重放。`wal_offset` **仅**指向尾 `wal.log`（见 `POLICY` §3.3）。

## 2. 单条记录的两种形态（顺序扫描）

对尾 `wal.log`（及封存段）从 `wal_offset` 起，按 **长度前缀或行边界** 切出的每条 **`rec`**，重放逻辑为：

1. 若 `rec` 前缀为 **`STDBBW1\n`** → 按 **二进制批次** 解析（`ndels` / `nputs`、键长值长…），整帧 **原子** `apply` 到 MemTable（见 **`WalReplayApplier::apply_batch_unlocked`**）。帧内结构损坏 → `open` **失败**。
2. 否则 → 按 **单行文本** `key=value\n` 解析（须 **以 `\n` 结尾**），`apply` 到 MemTable（**`WalReplayApplier::apply_line_unlocked`**）。**不完整尾行**（缺 `\n`）→ 该条 **拒绝重放** 并报错路径由实现与 `CHANGELOG` 约定（与「尾帧截断忽略」规则区分：见下条）。

**混排**：`rollback_one_undo_frame` 等路径追加的 **文本恢复行** 与 `commit_embed_batch` 的 **`STDBBW1`** 帧在同一字节流中 **按时间序** 排列；`open` 时 **逐条** 判别前缀，**不得**假设 WAL 仅含一种形态。

## 3. 与 `CHANGELOG` 尾帧规则的关系

- **外层 WAL 记录**若按实现被 **截断**（例如长度不足以读完整帧），可能表现为 **忽略不完整最后一帧** 或 **打开失败** — 以 `CHANGELOG` 与具体版本为准。
- **运维理解**：权威仍是 **WAL + checkpoint**；不要仅凭「只有二进制批次」假设工具链或旧库内容。

## 4. 与 redo / undo 的关系

- **`redo.log`**：**不**作为 `open` 重放权威（`POLICY` §3.1）。
- **`undo.log`**：服务版本化写与 **`undo_stack_`**；与 WAL 混排 **无**「单条 redo 替代 WAL」语义。

## 5. 相关链接

- [`POLICY.md`](POLICY.md) §3.1、§3.3、§3.4  
- [`TXN_BEGIN_PERSIST_DESIGN.md`](TXN_BEGIN_PERSIST_DESIGN.md)  
- [`PHASE20.md`](PHASE20.md)（多段 WAL）  
- [`PHASE24.md`](PHASE24.md)
