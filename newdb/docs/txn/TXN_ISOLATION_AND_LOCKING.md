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
- **非活跃事务（未 `BEGIN` 或已 `COMMIT`/`ROLLBACK`）**：`syncHeapReadSnapshotForQuery` **不**在堆上安装 `MVCCSnapshot`（等价于该次查询不做 MVCC 过滤），避免补偿/undo 行 `created_lsn` 与 WAL 边界对齐误差导致 **`COUNT`/`WHERE` 在 `ROLLBACK` 后误返回 0**；活跃事务内仍按上两行语义钉扎或刷新读视图。
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

**`ROLLBACK` 或会话堆失效后的顺序**：若表为懒堆承载，[`query_handler.cc`](../../cli/shell/dispatch/handlers/query/query_handler.cc) 中 `WHERE` / 裸 `COUNT` / `COUNT(条件)` 等在挂 **`HeapReadViewGuard`** 与 **`syncHeapReadSnapshotForQuery`** 之前会先 **`newdb_materialize_heap_if_lazy`**；否则读快照可能仍锚在旧堆视图，出现 **`COUNT`/`WHERE` 与磁盘 `load_heap_file` 短暂不一致**（易误判为 undo 失败）。

## 2. 进程内写意图（Write intent）

实现见 [`write_conflict_service.cc`](../../cli/modules/txn/coordinator/write_conflict/write_conflict_service.cc)：

- **`tryReserveWriteKey(table_name, id)`**：在事务 **Active** 时，以 `table#id` 为键登记全局写意图（`g_write_intent_owner`）。等价于 **`tryReserveWriteLockKey(LockKey::row_pk_write_intent(...))`**。
- **`LockKey` / `tryReserveWriteLockKey`**（[`lock_key.h`](../../cli/modules/txn/coordinator/write_conflict/lock_key.h)）：结构化键，除主键行意图外预留 **索引等值 / 范围 / 谓词** 序列化（`to_storage_key()` 使用 `v2|idx|…`、`v2|range|…`、`v2|pred|…` 前缀，不与遗留 `table#id` 冲突）。**事务内 DML** 在 `INSERT` / `UPDATE` / `SETATTR` / `SETATTRMULTI` / **`UPDATEWHERE`** 上对变更列叠加 **`index_eq_write_intent`**（列名作逻辑索引名）；批量路径使用 **`tryReserveWriteKeysBatchSorted`**（含 `SETATTRMULTI`、`DELETEWHERE`、上述 WHERE 批量写）。语句中途失败可 **`releaseWriteIntentStorageKeysForCurrentTxn`** 仅撤销本语句已登记键（不改变已成功 append 的 WAL 行）。可选：事务内 **`WHERE` / `SHOW PLAN` / `EXPLAIN WHERE`** 在设置 **`NEWDB_WHERE_RESERVE_PREDICATE`** / **`NEWDB_WHERE_RESERVE_RANGE`** 时尝试谓词 / 闭区间 range 预留（观测 `lock_key_*_count`）。语义边界与 unique secondary 接线约定见 [`PROJECT_DATAFLOW_WHOLE_详解.md`](../architecture/PROJECT_DATAFLOW_WHOLE_详解.md)「扩展写意图与 PK 行锁」。**Runtime**：`lock_key_range_count` / `lock_key_predicate_count` 统计范围 / 谓词键的**首次**成功预约（与 `m_reserved_write_keys` 插入一致，重复预约同一存储键不重复计数）。
- **冲突**：若键已被其他活跃事务占用：
  - **`Reject`** 或超时为 0：立即失败并返回原因字符串。
  - **`Wait`**：短间隔轮询等待；超时计入 `write_conflict_wait_timeout_count`。
- **死锁**：基于等待图检测环；选中当前事务为 victim，递增 `lock_deadlock_detect_count` / `lock_deadlock_victim_count`。

释放：`rollback` / `commit` 等路径调用 **`clearWriteIntents`** 清理本事务保留键。

## 3. 文件锁（表路径）

- **`acquireLock` / `releaseLock`**：对数据文件路径加 OS 级锁（实现见 [`lock_service.cc`](../../cli/modules/txn/coordinator/lock/lock_service.cc)）。
- **`isLocked(path)`**：**真** 当且仅当 **本 `TxnCoordinator` 实例** 已通过 `acquireLock` 持有该路径的锁句柄（见头文件注释）。**不**表示「仅 `.lock` 文件存在」或跨进程探测。

跨进程互斥依赖同一套文件锁 API；进程内查询请使用上述语义，避免与「锁文件残留」混淆。

## 4. 已知边界（事务链）

下列行为为**当前实现的既定语义**，而非遗漏实现；集成与脚本应主动规避。

### 4.1 嵌套事务

- **不支持**嵌套事务：已在 `TxnState::Active` 时再次 `BEGIN` 会失败（协调器返回错误，详见日志中的具体原因字符串）。
- 需要「事务块内的检查点」请使用显式 **`SAVEPOINT name`** / **`ROLLBACK TO SAVEPOINT name`**（见 [`core_impl.cc`](../../cli/modules/txn/coordinator/core/core_impl.cc)）。

### 4.2 批量 DML 与语句级原子性

- **`SETATTRMULTI`**、**`UPDATEWHERE`**、**`DELETEWHERE`**、事务内 **`BULKINSERT` / `BULKINSERTFAST`**（在 `NEWDB_TXN_STMT_SAVEPOINT` 未关闭时）在活跃事务内（默认）会在执行批量副作用前设置内部 **`SAVEPOINT`**（名称形如 **`__newdb_stmt_<n>`**），语句完整成功后再 **`RELEASE SAVEPOINT`**。中途失败（含 `append_row` / **`append_rows`** 失败等）时：协调器 **`rollbackToSavepoint`** 撤销自该语句起累计的 **`m_txn_records` / 堆** 变更，且 **dispatch 路径** 会 **`shell_invalidate_session_table`**，使会话 **`HeapTable`** 与磁盘一致。
- **`SETATTRMULTI`** 在事务内对每一行成功写入同步 **`recordOperation("UPDATE", ...)`**（与 **`UPDATEWHERE`** 一致），以便与上述 savepoint/undo 链对齐。
- **关闭语句级 savepoint**（恢复旧「半条语句」语义，便于对照）：环境变量 **`NEWDB_TXN_STMT_SAVEPOINT=0`** / **`off`** / **`false`** / **`no`**。
- **非事务**（未 `BEGIN`）或 **绕过 `process_command_line`/handler** 的嵌入调用：不经过上述包装；失败时仍可能出现堆/缓存与 WAL 的部分进度不一致，需整事务 **`ROLLBACK`** 或自行管理 savepoint 与会话缓存。
- **`rollbackToSavepoint` 内部 undo 失败**（`append_undo_row_to_heap` 失败等）时，语句级「全有或全无」**不**再保证，应 **`ROLLBACK`** 整事务并排查。
- CLI 会在 `BEGIN` 失败等路径打印协调器返回的**具体错误**（例如已在事务中、文件锁冲突），便于区分「写意图冲突」与「未能开启事务」。
- 成功 **`ROLLBACK`** 后，handler 会 **`shell_invalidate_session_table`**、**`lsm_lite_clear_txn_views`**（避免按 id 的 LSM 缓存与堆不一致），并对当前数据文件调用 **`invalidate_eq_sidecars_after_write`**（避免等值 sidecar 与回滚后的堆脱节）。此后 **`WHERE`/`COUNT` 等读路径** 仍须在固定读快照前 **物化懒堆**（见 §1.2 表下说明与 [`query_handler.cc`](../../cli/shell/dispatch/handlers/query/query_handler.cc)）。

### 4.3 Windows 与同表并发 `BEGIN`

- `BEGIN` 会对数据文件路径调用 **`acquireLock`**（[`lock_service.cc`](../../cli/modules/txn/coordinator/lock/lock_service.cc)）。在 **Windows** 上该锁常为**进程范围语义**，同一进程内第二个 `ShellState`/`TxnCoordinator` 对**同一路径**再 `BEGIN` 可能失败，易与「写意图 `Reject`」混淆；失败时返回串会附带 **`[Windows: ...]`** 提示以便与写意图原因区分。
- 相关单测：[`test_txn_write_conflict.cpp`](../../tests/test_txn_write_conflict.cpp)（`SameTableConcurrentBeginRespectsProcessScopedLockSemantics`）、[`test_demo_where_batch_dml.cpp`](../../tests/test_demo_where_batch_dml.cpp)（跨会话写冲突在非 Windows 下覆盖）。

### 4.4 `BEGIN` 表名与 `USE`

- **`BEGIN [table]`** 中的 `table` 决定 **`resolveDataFilePath`** 与 **文件锁** 目标；应与当前 **`USE(table)`** 选中的逻辑表一致。
- Shell 在「显式写出 `BEGIN` 表名且与当前 `USE` 表名不一致」时会打印 **`[TXN] warning`**（不改变锁语义，仅提示）。若设置 **`NEWDB_TXN_ENFORCE_BEGIN_USE_MATCH=1`**（或 **`on`/`true`/`yes`**），则 **拒绝** `BEGIN` 并打印 **`[TXN] BEGIN rejected`**。
- 更完整的写意图与批量路径说明见 [`PROJECT_DATAFLOW_WHOLE_详解.md`](../architecture/PROJECT_DATAFLOW_WHOLE_详解.md)「扩展写意图与 PK 行锁」。

### 4.5 语句级回滚与 SAVEPOINT（可行性结论）

- **`rollbackToSavepoint`** 依赖 `m_txn_records` 与 `op_seq`：对 savepoint **之后**的记录逆序执行 **`append_undo_row_to_heap`**，再截断 `m_txn_records` 并写 **`SAVEPOINT_ROLLBACK`** WAL（见 [`rollbackToSavepoint`](../../cli/modules/txn/coordinator/core/core_impl.cc)）。
- **CLI / C API（经 `process_command_line`）**：用户显式 **`ROLLBACK TO SAVEPOINT`** 成功时，[`txn_handler.cc`](../../cli/shell/dispatch/handlers/txn/txn_handler.cc) 会调用 **`shell_invalidate_session_table`**；事务内 **`UPDATEWHERE` / `DELETEWHERE` / `SETATTRMULTI` / `BULKINSERT`（及 FAST）** 使用的**内部 savepoint** 在语句失败时由 **handler** 同样失效会话堆（见 §4.2）。
- **直连 `TxnCoordinator::rollbackToSavepoint`**（单测或自定义嵌入、不经 dispatch）：**不会**自动刷新 `HeapTable`；调用方须在 undo 后自行 **`invalidate`/重载** 会话表，否则可能出现内存与磁盘短暂不一致（见 [`txn_manager.h`](../../cli/modules/txn/coordinator/txn_manager.h) 注释）。

## 5. 与 InnoDB / LevelDB 对照（摘要）

| 能力 | newdb 现状 | InnoDB | LevelDB |
|------|------------|--------|---------|
| 行级写冲突协调 | 进程内 write intent + 可选等待 | 行锁 + 事务队列 | 非核心 |
| 隔离级别 | CLI 查询读路径已消费配置；非 SQL 完整语义 | RC/RR 等完整 | N/A |
| MVCC | 快照可见性 + WAL 恢复链路 | Redo/Undo + 版本链 | N/A |

## 6. 相关测试

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
