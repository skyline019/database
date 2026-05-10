#pragma once

#include <string>
#include <atomic>
#include <mutex>
#include <thread>
#include <condition_variable>
#include <vector>
#include <unordered_set>
#include <functional>
#include <memory>
#include <cstddef>
#include <chrono>
#include <unordered_map>

#include <newdb/wal_sync_mode.h>

namespace newdb {
struct HeapTable;
class WalManager;
struct TableStorageHealth;
}

struct LockKey;

#include "cli/modules/common/util/result.h"
#include "cli/modules/txn/coordinator/txn_runtime_stats.h"
#include "cli/modules/txn/coordinator/txn_coordinator_types.h"

struct TxnCoordinatorState;

// Transaction coordinator (embedded in ShellState). Logic is split across
// `cli/modules/txn/coordinator/{core,wal,lock,recovery,vacuum,write_conflict,stats}/*.cc`;
// extend those translation units instead of growing this header when adding subsystem behavior.
//
// 事务管理器（嵌入 ShellState，无全局单例）
class TxnCoordinator {
public:
    TxnCoordinator();
    ~TxnCoordinator();
    TxnCoordinator(const TxnCoordinator&) = delete;
    TxnCoordinator& operator=(const TxnCoordinator&) = delete;

    // 事务控制
    Result<bool> begin(const std::string& table_name);
    Result<bool> commit();
    Result<bool> rollback();
    Result<bool> savepoint(const std::string& name);
    /// Rolls back heap/WAL records after `name` per in-memory `m_txn_records`. Callers that bypass the
    /// shell dispatch layer must refresh or invalidate any in-process `HeapTable` cache for the affected
    /// table (CLI handlers call `shell_invalidate_session_table` after a user `ROLLBACK TO SAVEPOINT`).
    Result<bool> rollbackToSavepoint(const std::string& name);
    Result<bool> releaseSavepoint(const std::string& name);
    Result<bool> recoverToLsn(std::uint64_t target_lsn);
    Result<bool> recoverToTime(std::uint64_t target_ts_ms);
    TxnState getState() const;
    int64_t getTxnId() const;
    bool inTransaction() const;
    
    // 文件锁（本进程内 TxnCoordinator 持有的 OS 锁句柄表；跨进程互斥不由此查询。）
    Result<bool> acquireLock(const std::string& file_path);
    Result<bool> releaseLock(const std::string& file_path);
    // True iff this coordinator currently holds `acquireLock(file_path)` for the same `file_path`.
    bool isLocked(const std::string& file_path) const;
    
    // WAL (Write-Ahead Log)
    void writeWAL(const std::string& operation, const std::string& table,
                   const std::string& key, const std::string& old_val, const std::string& new_val);
    void flushWAL();
    bool recoverFromWAL();
    void setWalSyncMode(newdb::WalSyncMode mode);
    newdb::WalSyncMode walSyncMode();
    void setWalNormalSyncIntervalMs(std::uint64_t ms);
    std::uint64_t walNormalSyncIntervalMs();
    
    // 记录事务操作 (用于回滚)
    void recordOperation(const std::string& operation, const std::string& table,
                         const std::string& key, const std::string& old_val, const std::string& new_val);
    bool tryReserveWriteKey(const std::string& table_name, int id, std::string* reason = nullptr);
    /// Sorts and de-duplicates `row_ids`, then reserves PK row intents in order. On first failure, releases
    /// only the keys successfully acquired in this batch (statement-level partial rollback of reservations).
    bool tryReserveWriteKeysBatchSorted(const std::string& table_name,
                                        std::vector<int> row_ids,
                                        std::string* reason = nullptr);
    /// Extended write-intent reservation (range / predicate / secondary index). Uses the same global map as row PK.
    bool tryReserveWriteLockKey(const LockKey& lk, std::string* reason = nullptr);
    /// Drop reservations for `storage_keys` when owned by this txn (global map + `m_reserved_write_keys`).
    void releaseWriteIntentStorageKeysForCurrentTxn(const std::vector<std::string>& storage_keys);
    void setWriteConflictPolicy(WriteConflictPolicy policy);
    WriteConflictPolicy writeConflictPolicy() const;
    void setWriteConflictWaitTimeoutMs(std::uint64_t ms);
    std::uint64_t writeConflictWaitTimeoutMs() const;
    void setTxnIsolationLevel(TxnIsolationLevel level);
    TxnIsolationLevel txnIsolationLevel() const;
    
    // 后台 VACUUM
    void startVacuumThread();
    void stopVacuumThread();
    void triggerVacuum(const std::string& table_name);
    void setVacuumCallback(std::function<void(const std::string&)> cb);
    void setVacuumOpsThreshold(std::size_t threshold);
    std::size_t vacuumOpsThreshold() const;
    void setVacuumMinIntervalSec(std::size_t sec);
    std::size_t vacuumMinIntervalSec() const;
    std::uint64_t vacuumTriggerCount() const;
    std::uint64_t vacuumExecuteCount() const;
    std::uint64_t vacuumCooldownSkipCount() const;
    std::uint64_t vacuumCompactSuccessCount() const;
    std::uint64_t vacuumCompactFailureCount() const;
    std::uint64_t vacuumCompactBytesReclaimed() const;
    std::uint64_t vacuumCompactLastElapsedMs() const;
    std::uint64_t vacuumQueueDepth() const;
    std::uint64_t vacuumQueueDepthPeak() const;
    std::uint64_t writeConflictCount() const;
    std::uint64_t writeConflictWaitCount() const;
    std::uint64_t writeConflictWaitTimeoutCount() const;
    std::uint64_t txnBeginLockConflictCount() const;
    std::uint64_t walCompactCount() const;
    std::uint64_t walRecoveryRuns() const;
    std::uint64_t walRecoveryUndoOps() const;
    std::uint64_t walRecoveryLastElapsedMs() const;
    std::uint64_t walRecoveryAnalyzeMs() const;
    std::uint64_t walRecoveryUndoMs() const;
    std::uint64_t walRecoveryFinalizeMs() const;
    std::uint64_t walRecoveryRecordsScanned() const;
    std::uint64_t walRecoveryDanglingTxns() const;
    std::uint64_t walRecoveryRedoMs() const;
    std::uint64_t walRecoveryCheckpointBeginCount() const;
    std::uint64_t walRecoveryCheckpointEndCount() const;
    std::uint64_t walGroupCommitCount() const;
    std::uint64_t walGroupCommitBatchCommits() const;
    std::uint64_t walGroupCommitPendingCommits() const;
    std::uint64_t txnCommitCount() const;
    std::uint64_t txnCommitMaxMs() const;
    std::uint64_t walBytesSinceStart() const;
    std::uint64_t lockDeadlockDetectCount() const;
    std::uint64_t lockDeadlockVictimCount() const;
    std::uint64_t lockWaitMsTotal() const;
    std::uint64_t lockWaitMaxMs() const;
    std::uint64_t schedulerThrottleCount() const;
    void setGroupCommitWindowMs(std::uint64_t ms);
    std::uint64_t groupCommitWindowMs() const;
    void setGroupCommitMaxBatchCommits(std::uint64_t n);
    std::uint64_t groupCommitMaxBatchCommits() const;
    void setWalAdaptiveEnabled(bool enabled);
    bool walAdaptiveEnabled() const;
    void setHotIndexEnabled(bool enabled);
    bool hotIndexEnabled() const;
    void setSegmentTargetBytes(std::uint64_t bytes);
    std::uint64_t segmentTargetBytes() const;
    void setLsmSegmentCount(std::uint64_t n);
    void setLsmMemtableBytes(std::uint64_t n);
    void onLsmMemtableFlush();
    void onLsmCompaction();
    void onLsmReadSegmentsScanned(std::uint64_t n);
    void onLsmCompactionBytes(std::uint64_t bytes_in, std::uint64_t bytes_out);
    void onLsmCompactionQueueDepth(std::uint64_t pending, std::uint64_t inflight);
    void onLsmCompactionEnqueueSkippedBackpressure();
    void onLsmSegmentCacheLookup(bool hit);
    void setHybridAdaptiveEnabled(bool enabled);
    bool hybridAdaptiveEnabled() const;
    void onSchedulerThrottled();
    void onWriteTiming(WriteTimingStage stage, std::uint64_t elapsed_ms);
    /// Full heap materialization from lazy decode path (DML / explicit materialize).
    void noteLazyMaterialize(std::uint64_t rows, std::uint64_t elapsed_ms);
    /// LSN-based read snapshot for query paths (`HeapTable::active_snapshot`). Respects
    /// `TxnIsolationLevel`, `NEWDB_TXN_ISOLATION_READPATH=0` to disable, `NEWDB_TXN_TRACE=1` for stderr trace.
    void syncHeapReadSnapshotForQuery(newdb::HeapTable& table);
    TxnRuntimeStats runtimeStats() const;
    bool vacuumRunning() const;
    /// Updated when vacuum enqueue runs `measure_table_storage_health` (health queue env on, load ok).
    void recordLastStorageHealthSnapshot(const newdb::TableStorageHealth& h);
    /// Updates only `last_vacuum_*` fields without resetting other health columns (vacuum thread).
    void mergeLastVacuumIntoStorageHealth(std::uint64_t last_vacuum_lsn, std::uint64_t last_vacuum_elapsed_ms);

    // Prefix for <table>.bin lock paths (same as ShellState::data_dir). Empty => cwd-relative names.
    void set_workspace_root(std::string path);
    const std::string& workspace_root() const;

private:
    std::unique_ptr<TxnCoordinatorState> st_;

    // WAL 文件路径
    std::string getWALPath(const std::string& table_name) const;
    std::string walSyncConfigPath() const;
    bool loadWalSyncConfig(newdb::WalManager& wm) const;
    void persistWalSyncConfig(const newdb::WalManager& wm) const;
    std::string vacuumConfigPath() const;
    void loadVacuumConfig();
    void persistVacuumConfig() const;
    std::string resolveDataFilePath(const std::string& table_name) const;
    newdb::WalManager* ensureWal();
    void maybeCompactWalAfterCommit(const std::string& committed_table);
    void persistWalsnHighWaterUnlocked(newdb::WalManager* wm);
    void clearWriteIntents();
    void recordWriteConflictSample(const std::string& table_name, int row_id, std::uint64_t holder_txn, const char* tag);
    void recordWriteConflictSampleLockKey(const LockKey& lk, std::uint64_t holder_txn, const char* tag);
};
