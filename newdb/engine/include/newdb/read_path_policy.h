#pragma once

/// Read-path / MVCC snapshot policy for newdb.
///
/// User-visible reads that honor transaction isolation must refresh
/// `HeapTable::active_snapshot` through `TxnCoordinator::syncHeapReadSnapshotForQuery`
/// (typically via `HeapReadViewGuard` in `cli/shell/state/shell_state_heap_read_guard.h`).
///
/// The C API (`engine/src/api/c/c_api.cpp`) routes SQL-like commands through the same
/// CLI dispatch stack as `newdb_demo`, so interactive CLI, embedding tests, and the
/// shared library share one snapshot-selection implementation. See
/// `docs/txn/TXN_ISOLATION_AND_LOCKING.md` for semantics (subset of SQL RC / snapshot).
