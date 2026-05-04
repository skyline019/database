# 事务隔离、写冲突与文件锁语义（阶段 B）

本文档描述 **`TxnCoordinator`** 中与并发相关的**已实现行为**与**已知边界**，便于与 InnoDB 等级别的隔离/锁体系对照；不代表 SQL 标准完整语义。

## 1. 枚举与配置入口

定义见 [`txn_manager.h`](../../cli/modules/txn/coordinator/txn_manager.h)：

- **`TxnIsolationLevel`**：`ReadCommitted`、`Snapshot`（默认 **`Snapshot`**）。
- **`WriteConflictPolicy`**：`Reject`、`Wait`；可配置等待超时 `writeConflictWaitTimeoutMs`。

CLI 可通过事务相关命令切换隔离标签（见 [`txn_handler.cc`](../../cli/shell/dispatch/handlers/txn/txn_handler.cc)）；C API 运行态 JSON 也会透出当前隔离字符串。

### 1.1 实现边界（重要）

`TxnIsolationLevel` 仍是 **有限子集**（非完整 InnoDB RR/RC + 间隙锁 + undo 链）。读可见性仍以引擎 **`MVCCSnapshot`** / `HeapTable::is_row_visible` 为准。

**已接入 CLI 查询读路径（阶段 2）**：在 `PAGE` / `WHERE` / `COUNT` / `FIND` / `SUM` / `AVG` 等入口，`TxnCoordinator::syncHeapReadSnapshotForQuery` 与 `HeapReadViewGuard` 会在当前 `HeapTable` 上设置/清理 `active_snapshot`：

- **`Snapshot`**：事务 `BEGIN` 时固定 `m_txn_read_view_lsn`，读路径使用该 LSN 构造 `MVCCSnapshot`。
- **`ReadCommitted`**：每条查询在 `syncHeapReadSnapshotForQuery` 内使用 WAL 当前 LSN 刷新读视图（语句级快照语义）。
- **`NEWDB_TXN_ISOLATION_READPATH=0|off|false|no`**：跳过上述设置并 `clear_snapshot()`，保留旧行为以便对照。
- **Runtime stats（`SHOW TUNING JSON` / C API JSONL）**：`txn_snapshot_refresh_count`（RC 或 Snapshot 退化语句视图每次刷新）、`txn_snapshot_pinned_count`（活跃 Snapshot 事务使用固定 `m_txn_read_view_lsn`）、`txn_readpath_disabled_count`（读路径被 env 关闭次数）、`last_snapshot_source`（`none|txn|statement|disabled` 之一，反映最近一次 `syncHeapReadSnapshotForQuery` 的归类）。
- **`NEWDB_TXN_TRACE=1|on|true|yes`**：向 stderr 打印隔离级别与 `snapshot_lsn`（默认无输出）。
- **`NEWDB_INDEX_CATALOG_ENFORCE=1`**：平等索引 sidecar 命中路径上，若 **`index_descriptor_matches_runtime`** 判定描述符与当前 `attr_sig` / workspace WAL LSN 不一致，则 **删除 sidecar 并走重建**；未设置或非 `1` 时仅 **stderr 提示**（不阻断查询）。
- **Sidecar 头行 catalog 尾部**：新写入的 equality / page / covering / visibility sidecar 第一行在 **`wal_lsn`** 之后由 **`index_catalog_sidecar_meta_suffix`** 追加 **`idx_kind` / `built_ms` / `idx_sv` / `idx_dl` / `tbl_fnv` / `inx_fnv`**（与 **`IndexDescriptor`** 的 schema/data 指纹及表路径、索引键的 FNV 身份对齐）；旧文件无尾缀或仅有 **`idx_kind`/`built_ms`** 时解析仍兼容。
- **Index catalog 构建态（`;bld=`）**：头行尾缀 **`bld=1`** 表示 **building**（侧车仍在构建或未保证与 runtime 身份一致）；**`bld=2`**（或缺省/旧文件语义上的 ready）表示 **ready**。写入侧当前对新建 sidecar 使用 **ready** 尾缀；**`NEWDB_INDEX_CATALOG_ENFORCE=1`** 下若 **`index_descriptor_matches_runtime`** 失败会 **删除 sidecar 并重建**，与 building/ready 的运维含义见 **`tests/test_index_catalog.cpp`**（`BuildStateBuildingVsReadyParsed`）及评估文档阶段 6。

实现见 [`wal_service.cc`](../../cli/modules/txn/coordinator/wal/wal_service.cc) 与 [`shell_state.h`](../../cli/shell/state/shell_state.h) 中 `HeapReadViewGuard`。引擎层契约仍以 `tests/test_txn_isolation_visibility.cpp` 等为基线。

### 1.2 读路径 snapshot 审计（CLI / C API）

| 入口 | 经 `HeapReadViewGuard` / `syncHeapReadSnapshotForQuery` | 备注 |
|------|--------------------------------------------------------|------|
| CLI `PAGE` / `WHERE` / `EXPLAIN WHERE` / `SHOW PLAN` / `COUNT`（含条件）/ `SUM` / `AVG` / `MIN` / `MAX` | 是 | 见 [`query_handler.cc`](../../cli/shell/dispatch/handlers/query/query_handler.cc)；`SHOW PLAN` JSON 含 `snapshot_lsn` / `snapshot_source` / `readpath_enabled` |
| CLI `FIND` / `FINDPK` | 是 | 同文件 `handle_query_find_commands` |
| C API `newdb_session_execute` | 是（间接） | 走 [`process_command_line`](../../cli/shell/dispatch/router/dispatch.h) → 与 CLI 相同 handler，查询子命令具备 guard |
| GUI（Rust/Tauri） | 是（间接） | 若仅通过 C API/CLI 命令转发，与上一行一致；**直接绑引擎的自定义调用**需单独审计 |
| `NEWDB_TXN_ISOLATION_READPATH=0` 等 | 否（故意跳过） | 对照/调试路径 |

## 2. 进程内写意图（Write intent）

实现见 [`write_conflict_service.cc`](../../cli/modules/txn/coordinator/write_conflict/write_conflict_service.cc)：

- **`tryReserveWriteKey(table_name, id)`**：在事务 **Active** 时，以 `table#id` 为键登记全局写意图（`g_write_intent_owner`）。
- **冲突**：若键已被其他活跃事务占用：
  - **`Reject`** 或超时为 0：立即失败并返回原因字符串。
  - **`Wait`**：短间隔轮询等待；超时计入 `write_conflict_wait_timeout_count`。
- **死锁**：基于等待图检测环；选中当前事务为 victim，递增 `lock_deadlock_detect_count` / `lock_deadlock_victim_count`。

释放：`rollback` / `commit` 等路径调用 **`clearWriteIntents`** 清理本事务保留键。

## 3. 文件锁（表路径）

- **`acquireLock` / `releaseLock`**：对数据文件路径加 OS 级锁（实现见 [`lock_service.cc`](../../cli/modules/txn/coordinator/lock/lock_service.cc)）。
- **`isLocked(path)`**：**真** 当且仅当 **本 `TxnCoordinator` 实例** 已通过 `acquireLock` 持有该路径的锁句柄（见头文件注释）。**不**表示「仅 `.lock` 文件存在」或跨进程探测。

跨进程互斥依赖同一套文件锁 API；进程内查询请使用上述语义，避免与「锁文件残留」混淆。

## 4. 与 InnoDB / LevelDB 对照（摘要）

| 能力 | newdb 现状 | InnoDB | LevelDB |
|------|------------|--------|---------|
| 行级写冲突协调 | 进程内 write intent + 可选等待 | 行锁 + 事务队列 | 非核心 |
| 隔离级别 | CLI 查询读路径已消费配置；非 SQL 完整语义 | RC/RR 等完整 | N/A |
| MVCC | 快照可见性 + WAL 恢复链路 | Redo/Undo + 版本链 | N/A |

## 5. 相关测试

见 [TESTS_FAULT_INJECTION_AND_TXN_MATRIX.md](../testing/TESTS_FAULT_INJECTION_AND_TXN_MATRIX.md)（`test_txn_write_conflict`、`test_txn_file_lock`、`test_sidecar_wal_lsn_stress` 等）。

新增隔离可见性基线：

- `tests/test_txn_isolation_config.cpp`：覆盖 `TxnIsolationLevel` 默认值与配置 round-trip。
- `tests/test_txn_isolation_visibility.cpp`：固定 `MVCCSnapshot` 可见性基础契约，包括：
  - snapshot LSN 之后创建的版本不可见；
  - snapshot 前已删除的版本不可见；
  - active creator transaction 产生的版本不可见，防止 dirty read；
  - 固定 snapshot 能保持 repeatable-read-like 视图；
  - read-committed-like 语义需要每条语句刷新 statement snapshot（`ReadCommittedUsesFreshStatementSnapshot`：首条语句快照下不可见 txn2 行，刷新后第二条语句快照下可见）。

这些测试先固定引擎层 `MVCCSnapshot` 与隔离配置的预期行为；CLI 读路径已接上 `syncHeapReadSnapshotForQuery`（见 §1.1）。
