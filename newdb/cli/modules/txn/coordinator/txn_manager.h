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

#include <newdb/wal_manager.h>

namespace newdb {
class HeapTable;
}

#include "cli/modules/common/util/result.h"
#include "cli/modules/common/util/constants.h"
#include "cli/modules/storage/table_storage_health.h"
#include "cli/modules/txn/coordinator/write_conflict/lock_key.h"

// 事务状态
enum class TxnState {
    None,       // 无事务
    Active,     // 事务活跃中
    Committed,  // 已提交
    RolledBack  // 已回滚
};

// 事务记录
struct TxnRecord {
    int64_t txn_id;
    TxnState state;
    std::string table_name;
    std::string operation;  // INSERT/UPDATE/DELETE
    std::string key;       // 主键值
    std::string old_value; // 修改前的值 (用于回滚)
    std::string new_value; // 修改后的值
    int64_t timestamp;
    std::uint64_t op_seq{0};
    std::uint64_t wal_lsn{0};
};

struct TxnRuntimeStats {
    std::uint64_t vacuum_trigger_count{0};
    std::uint64_t vacuum_execute_count{0};
    std::uint64_t vacuum_cooldown_skip_count{0};
    std::uint64_t vacuum_compact_success_count{0};
    std::uint64_t vacuum_compact_failure_count{0};
    std::uint64_t vacuum_compact_bytes_reclaimed{0};
    std::uint64_t vacuum_compact_last_elapsed_ms{0};
    std::uint64_t vacuum_queue_depth{0};
    std::uint64_t vacuum_queue_depth_peak{0};
    std::uint64_t maintenance_checkpoint_trigger_count{0};
    std::uint64_t maintenance_checkpoint_vacuum_enqueue_count{0};
    std::uint64_t write_conflict_count{0};
    std::uint64_t write_conflict_wait_count{0};
    std::uint64_t write_conflict_wait_timeout_count{0};
    /// Last sampled write-conflict line (`table=...;row=...;holder=...;tag=...`) for observability.
    std::string write_conflict_last_sample;
    std::uint64_t file_lock_acquire_fail_count{0};
    std::uint64_t file_lock_same_process_reuse_count{0};
    /// Lock marker removed as stale (strict env + age); best-effort cross-process cleanup.
    std::uint64_t file_lock_stale_marker_count{0};
    std::uint64_t sidecar_invalidate_count{0};
    std::uint64_t sidecar_invalidate_fail_count{0};
    std::uint64_t txn_begin_lock_conflict_count{0};
    std::uint64_t wal_compact_count{0};
    std::uint64_t wal_recovery_runs{0};
    std::uint64_t wal_recovery_undo_ops{0};
    std::uint64_t wal_recovery_last_elapsed_ms{0};
    std::uint64_t wal_recovery_analyze_ms{0};
    std::uint64_t wal_recovery_undo_ms{0};
    std::uint64_t wal_recovery_finalize_ms{0};
    std::uint64_t wal_recovery_records_scanned{0};
    std::uint64_t wal_recovery_dangling_txns{0};
    /// WAL recovery redo phase (committed replay) wall time for the CLI coordinator path.
    std::uint64_t wal_recovery_redo_ms{0};
    /// Count of CHECKPOINT_BEGIN records observed during the last `recoverFromWAL` scan pass.
    std::uint64_t wal_recovery_checkpoint_begin_count{0};
    /// Count of CHECKPOINT_END records observed during the last `recoverFromWAL` scan pass.
    std::uint64_t wal_recovery_checkpoint_end_count{0};
    /// WAL records with LSN after last complete checkpoint (CLI reconcile + `capture_recovery_scan_stats`).
    std::uint64_t wal_recovery_records_after_checkpoint{0};
    /// Indexed WAL segments from segment scan (`WalRecoveryStats::indexed_segments`).
    std::uint64_t wal_recovery_segments_after_checkpoint{0};
    /// Uncommitted txn count observed during redo planning (`dangling_by_txn.size()` in reconcile path).
    std::uint64_t wal_recovery_redo_plan_pending_txn_count{0};
    /// Idempotent redo skips / conflicts (`strict` mode duplicates or guard collisions).
    std::uint64_t wal_recovery_apply_conflict_count{0};
    /// Last successful coordinator WAL reconcile policy tag (may include `|heap_policy=...` when engine stats exist).
    std::string wal_recovery_policy;
    std::uint64_t wal_group_commit_count{0};
    std::uint64_t wal_group_commit_batch_commits{0};
    std::uint64_t wal_group_commit_pending_commits{0};
    std::uint64_t txn_commit_count{0};
    std::uint64_t txn_commit_p95_ms{0};
    std::uint64_t txn_commit_max_ms{0};
    std::uint64_t wal_bytes_since_start{0};
    std::uint64_t wal_bytes_per_commit_avg{0};
    std::uint64_t lock_deadlock_detect_count{0};
    std::uint64_t lock_deadlock_victim_count{0};
    std::uint64_t lock_wait_ms_total{0};
    std::uint64_t lock_wait_max_ms{0};
    std::uint64_t lock_wait_p95_ms{0};
    std::uint64_t scheduler_throttle_count{0};
    bool hot_index_enabled{true};
    std::uint64_t segment_target_bytes{0};
    std::uint64_t lsm_memtable_flush_count{0};
    std::uint64_t lsm_compaction_count{0};
    std::uint64_t lsm_segment_count{0};
    std::uint64_t lsm_memtable_bytes{0};
    std::uint64_t lsm_read_segments_scanned{0};
    std::uint64_t lsm_read_segments_scanned_p95{0};
    std::uint64_t lsm_compaction_bytes_in{0};
    std::uint64_t lsm_compaction_bytes_out{0};
    std::uint64_t lsm_compaction_queue_pending{0};
    std::uint64_t lsm_compaction_queue_inflight{0};
    std::uint64_t lsm_compaction_enqueue_skipped_backpressure{0};
    std::uint64_t lsm_segment_cache_hits{0};
    std::uint64_t lsm_segment_cache_misses{0};
    double lsm_compaction_bytes_amp_efficiency_min_window{0.0};
    std::uint64_t lsm_read_segments_scanned_p95_window{0};
    std::string hybrid_mode{"throughput_mode"};
    std::uint64_t hybrid_mode_switch_count{0};
    std::string hybrid_last_switch_reason;
    std::uint64_t rollback_savepoint_count{0};
    std::uint64_t rollback_partial_ops{0};
    std::uint64_t pitr_runs{0};
    std::uint64_t pitr_target_lsn{0};
    std::uint64_t pitr_elapsed_ms{0};
    std::uint64_t undo_chain_fallback_count{0};
    std::uint64_t lazy_materialize_count{0};
    std::uint64_t lazy_materialize_rows_total{0};
    std::uint64_t lazy_materialize_max_rows{0};
    std::uint64_t lazy_materialize_elapsed_ms{0};
    /// Last observed vacuum queue pressure heuristic (depth-based; phase 9).
    std::uint64_t vacuum_priority_score{0};
    /// Last `measure_table_storage_health`-derived bonus added to file-size debt when
    /// `NEWDB_VACUUM_QUEUE_USE_HEALTH=1` (0 when disabled or load failed).
    std::uint64_t vacuum_health_bonus_last{0};
    /// Last vacuum enqueue score decomposition (see `compute_vacuum_score_breakdown` in vacuum_service.cc).
    std::uint64_t vacuum_score_file_bytes_term{0};
    std::uint64_t vacuum_score_health_bonus_term{0};
    std::uint64_t vacuum_score_wal_since_term{0};
    /// Last successful `measure_table_storage_health` snapshot (vacuum enqueue path when health env on).
    std::uint64_t table_storage_health_logical_rows{0};
    std::uint64_t table_storage_health_physical_rows{0};
    std::uint64_t table_storage_health_tombstone_rows{0};
    std::uint64_t table_storage_health_data_file_bytes{0};
    std::uint64_t table_storage_health_live_bytes{0};
    std::uint64_t table_storage_health_dead_bytes{0};
    double table_storage_health_fragmentation_ratio{0.0};
    std::uint64_t table_storage_health_last_vacuum_lsn{0};
    std::uint64_t table_storage_health_last_vacuum_elapsed_ms{0};
    /// `good` | `watch` | `degraded` | `critical` — derived from last storage health snapshot (see `stats_impl.cc`).
    std::string table_storage_health_tier{"good"};
    /// Last vacuum enqueue debt (file bytes + optional health bonus); see `triggerVacuum`.
    std::uint64_t compact_debt_bytes{0};
    std::uint64_t compact_debt_rows{0};
    double compact_debt_ratio{0.0};
    std::uint64_t compact_debt_priority{0};
    /// Process-wide heap page cache (see `NEWDB_PAGE_CACHE_MAX_BYTES`); counters since process start.
    std::uint64_t page_cache_hits{0};
    std::uint64_t page_cache_misses{0};
    std::uint64_t page_cache_evictions{0};
    std::uint64_t page_cache_bytes_in_cache{0};
    /// Configured process-wide page cache cap (`NEWDB_PAGE_CACHE_MAX_BYTES`, 0 = disabled / unlimited).
    std::uint64_t memory_budget_max_bytes{0};
    /// Current bytes attributed to the page cache LRU (phase-1 stand-in for unified engine memory budget).
    std::uint64_t memory_budget_used_bytes{0};
    /// Page cache refused `put` because one page exceeded `NEWDB_PAGE_CACHE_MAX_BYTES` (see `PageCacheGlobalStats`).
    std::uint64_t memory_budget_reject_count{0};
    /// LRU eviction volume (see `PageCacheGlobalStats::bytes_evicted_total`).
    std::uint64_t memory_budget_bytes_evicted_total{0};
    /// Eq sidecar loads skipped when on-disk index size + page-cache bytes would exceed memory budget cap.
    std::uint64_t memory_budget_sidecar_load_skipped_total{0};
    /// Per-kind page cache bytes admitted into MemoryRegistry (Phase 5 v2 closed loop).
    std::uint64_t mem_page_cache_used_bytes{0};
    /// Per-kind page cache LRU evictions reported back to the MemoryRegistry.
    std::uint64_t mem_page_cache_evictions{0};
    /// Per-kind page cache `try_admit` rejects (oversized page or cap exhausted).
    std::uint64_t mem_page_cache_admit_rejects{0};
    /// Per-kind equality sidecar bytes admitted (header + bucket slot estimate).
    std::uint64_t mem_sidecar_used_bytes{0};
    /// Per-kind equality sidecar LRU evictions returned by registered evictor.
    std::uint64_t mem_sidecar_evictions{0};
    /// Per-kind equality sidecar `try_admit` rejects (entry too large or cap exhausted).
    std::uint64_t mem_sidecar_admit_rejects{0};
    /// Per-kind WHERE query temp bytes admitted (rough `logical_rows*sizeof(size_t)+conds*256` estimate).
    std::uint64_t mem_query_temp_used_bytes{0};
    /// Per-kind WHERE query temp evictions (placeholder; reserved for future eviction hooks).
    std::uint64_t mem_query_temp_evictions{0};
    /// Per-kind WHERE query temp `try_admit` rejects (caller fell back to no-op result).
    std::uint64_t mem_query_temp_admit_rejects{0};
    /// Aggregate of per-kind used bytes (`page_cache + sidecar + query_temp`).
    std::uint64_t mem_global_used_bytes{0};
    /// Sum of per-kind admit rejects (cross-kind global view of memory pressure).
    std::uint64_t mem_global_admit_rejects{0};
    /// `BEGIN` snapshot / ReadCommitted statement refresh diagnostics (see `syncHeapReadSnapshotForQuery`).
    std::uint64_t txn_snapshot_refresh_count{0};
    std::uint64_t txn_snapshot_pinned_count{0};
    std::uint64_t txn_readpath_disabled_count{0};
    std::string last_snapshot_source{"none"};
    /// Last LSN pinned for Snapshot isolation `BEGIN` (0 when none / ReadCommitted).
    std::uint64_t transaction_snapshot_lsn{0};
    /// Last LSN used for statement-level read view refresh (ReadCommitted or Snapshot without txn pin).
    std::uint64_t statement_snapshot_lsn{0};
    /// Successful first-time reservations of `LockKeyKind::RangeWriteIntent` keys (see `tryReserveWriteLockKey`).
    std::uint64_t lock_key_range_count{0};
    /// Successful first-time reservations of `LockKeyKind::PredicateWriteIntent` keys.
    std::uint64_t lock_key_predicate_count{0};

    // Write-path staged timing (p95/max of sampled operations).
    std::uint64_t write_heap_append_p95_ms{0};
    std::uint64_t write_heap_append_max_ms{0};
    std::uint64_t write_hot_index_p95_ms{0};
    std::uint64_t write_hot_index_max_ms{0};
    std::uint64_t write_sidecar_invalidate_p95_ms{0};
    std::uint64_t write_sidecar_invalidate_max_ms{0};
    std::uint64_t write_wal_append_p95_ms{0};
    std::uint64_t write_wal_append_max_ms{0};
    std::uint64_t write_lsm_track_p95_ms{0};
    std::uint64_t write_lsm_track_max_ms{0};
    std::uint64_t write_lsm_flush_p95_ms{0};
    std::uint64_t write_lsm_flush_max_ms{0};
    std::uint64_t write_lsm_compaction_p95_ms{0};
    std::uint64_t write_lsm_compaction_max_ms{0};
    std::uint64_t write_lsm_rotate_compact_p95_ms{0};
    std::uint64_t write_lsm_rotate_compact_max_ms{0};
};

enum class WriteConflictPolicy {
    Reject,
    Wait,
};

// Semantics vs SQL/InnoDB and write-intent locking: docs/txn/TXN_ISOLATION_AND_LOCKING.md
enum class TxnIsolationLevel {
    ReadCommitted,
    Snapshot,
};

enum class WriteTimingStage : std::uint8_t {
    HeapAppend = 0,
    HotIndex = 1,
    SidecarInvalidate = 2,
    WalAppend = 3,
    LsmTrack = 4,
    LsmRotateCompact = 5,
    LsmFlush = 6,
    LsmCompaction = 7,
};

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
    Result<bool> rollbackToSavepoint(const std::string& name);
    Result<bool> releaseSavepoint(const std::string& name);
    Result<bool> recoverToLsn(std::uint64_t target_lsn);
    Result<bool> recoverToTime(std::uint64_t target_ts_ms);
    TxnState getState() const { return m_state.load(); }
    int64_t getTxnId() const { return m_txn_id.load(); }
    bool inTransaction() const { return m_state.load() == TxnState::Active; }
    
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
    /// Extended write-intent reservation (range / predicate / secondary index). Uses the same global map as row PK.
    bool tryReserveWriteLockKey(const LockKey& lk, std::string* reason = nullptr);
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
    std::size_t vacuumOpsThreshold() const { return m_vacuum_ops_threshold.load(); }
    void setVacuumMinIntervalSec(std::size_t sec);
    std::size_t vacuumMinIntervalSec() const { return m_vacuum_min_interval_sec.load(); }
    std::uint64_t vacuumTriggerCount() const { return m_vacuum_trigger_count.load(); }
    std::uint64_t vacuumExecuteCount() const { return m_vacuum_execute_count.load(); }
    std::uint64_t vacuumCooldownSkipCount() const { return m_vacuum_cooldown_skip_count.load(); }
    std::uint64_t vacuumCompactSuccessCount() const { return m_vacuum_compact_success_count.load(); }
    std::uint64_t vacuumCompactFailureCount() const { return m_vacuum_compact_failure_count.load(); }
    std::uint64_t vacuumCompactBytesReclaimed() const { return m_vacuum_compact_bytes_reclaimed.load(); }
    std::uint64_t vacuumCompactLastElapsedMs() const { return m_vacuum_compact_last_elapsed_ms.load(); }
    std::uint64_t vacuumQueueDepth() const { return m_vacuum_queue_depth.load(); }
    std::uint64_t vacuumQueueDepthPeak() const { return m_vacuum_queue_depth_peak.load(); }
    std::uint64_t writeConflictCount() const { return m_write_conflict_count.load(); }
    std::uint64_t writeConflictWaitCount() const { return m_write_conflict_wait_count.load(); }
    std::uint64_t writeConflictWaitTimeoutCount() const { return m_write_conflict_wait_timeout_count.load(); }
    std::uint64_t txnBeginLockConflictCount() const { return m_txn_begin_lock_conflict_count.load(); }
    std::uint64_t walCompactCount() const { return m_wal_compact_count.load(); }
    std::uint64_t walRecoveryRuns() const { return m_wal_recovery_runs.load(); }
    std::uint64_t walRecoveryUndoOps() const { return m_wal_recovery_undo_ops.load(); }
    std::uint64_t walRecoveryLastElapsedMs() const { return m_wal_recovery_last_elapsed_ms.load(); }
    std::uint64_t walRecoveryAnalyzeMs() const { return m_wal_recovery_analyze_ms.load(); }
    std::uint64_t walRecoveryUndoMs() const { return m_wal_recovery_undo_ms.load(); }
    std::uint64_t walRecoveryFinalizeMs() const { return m_wal_recovery_finalize_ms.load(); }
    std::uint64_t walRecoveryRecordsScanned() const { return m_wal_recovery_records_scanned.load(); }
    std::uint64_t walRecoveryDanglingTxns() const { return m_wal_recovery_dangling_txns.load(); }
    std::uint64_t walRecoveryRedoMs() const { return m_wal_recovery_redo_ms.load(); }
    std::uint64_t walRecoveryCheckpointBeginCount() const { return m_wal_recovery_checkpoint_begin_count.load(); }
    std::uint64_t walRecoveryCheckpointEndCount() const { return m_wal_recovery_checkpoint_end_count.load(); }
    std::uint64_t walGroupCommitCount() const { return m_wal_group_commit_count.load(); }
    std::uint64_t walGroupCommitBatchCommits() const { return m_wal_group_commit_batch_commits.load(); }
    std::uint64_t walGroupCommitPendingCommits() const { return m_wal_group_commit_pending_commits.load(); }
    std::uint64_t txnCommitCount() const { return m_txn_commit_count.load(); }
    std::uint64_t txnCommitMaxMs() const { return m_txn_commit_max_ms.load(); }
    std::uint64_t walBytesSinceStart() const { return m_wal_bytes_since_start.load(); }
    std::uint64_t lockDeadlockDetectCount() const { return m_lock_deadlock_detect_count.load(); }
    std::uint64_t lockDeadlockVictimCount() const { return m_lock_deadlock_victim_count.load(); }
    std::uint64_t lockWaitMsTotal() const { return m_lock_wait_ms_total.load(); }
    std::uint64_t lockWaitMaxMs() const { return m_lock_wait_max_ms.load(); }
    std::uint64_t schedulerThrottleCount() const { return m_scheduler_throttle_count.load(); }
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
    bool vacuumRunning() const { return m_vacuum_running.load(); }
    /// Updated when vacuum enqueue runs `measure_table_storage_health` (health queue env on, load ok).
    void recordLastStorageHealthSnapshot(const newdb::TableStorageHealth& h);
    /// Updates only `last_vacuum_*` fields without resetting other health columns (vacuum thread).
    void mergeLastVacuumIntoStorageHealth(std::uint64_t last_vacuum_lsn, std::uint64_t last_vacuum_elapsed_ms);

    // Prefix for <table>.bin lock paths (same as ShellState::data_dir). Empty => cwd-relative names.
    void set_workspace_root(std::string path);
    const std::string& workspace_root() const { return m_workspace_root; }

private:
    struct LockHandleState {
        std::string lock_file_path;
#if defined(_WIN32)
        void* handle{nullptr};
#else
        int fd{-1};
#endif
    };
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
    std::uint64_t wal_compact_max_bytes{4ull * 1024ull * 1024ull};
    
    // 事务状态
    std::atomic<TxnState> m_state{TxnState::None};
    std::atomic<int64_t> m_txn_id{0};
    std::mutex m_txn_mutex;
    
    // 事务记录缓冲 (用于回滚)
    std::vector<TxnRecord> m_txn_records;
    
    // 文件锁（查询是否持锁需在 const 方法中加锁，故 mutex 为 mutable）
    mutable std::mutex m_lock_mutex;
    std::vector<std::string> m_locked_files;
    std::unordered_map<std::string, LockHandleState> m_lock_handles;
    
    // WAL
    std::mutex m_wal_mutex;
    std::unique_ptr<newdb::WalManager> wal_;

    // Staged write timing samples (p95/max). We keep small rolling buffers per stage.
    std::atomic<std::uint64_t> m_write_timing_heap_append_max_ms{0};
    std::atomic<std::uint64_t> m_write_timing_hot_index_max_ms{0};
    std::atomic<std::uint64_t> m_write_timing_sidecar_invalidate_max_ms{0};
    std::atomic<std::uint64_t> m_write_timing_wal_append_max_ms{0};
    std::atomic<std::uint64_t> m_write_timing_lsm_track_max_ms{0};
    std::atomic<std::uint64_t> m_write_timing_lsm_flush_max_ms{0};
    std::atomic<std::uint64_t> m_write_timing_lsm_compaction_max_ms{0};
    std::atomic<std::uint64_t> m_write_timing_lsm_rotate_compact_max_ms{0};
    mutable std::vector<std::uint64_t> m_write_timing_heap_append_samples;
    mutable std::vector<std::uint64_t> m_write_timing_hot_index_samples;
    mutable std::vector<std::uint64_t> m_write_timing_sidecar_invalidate_samples;
    mutable std::vector<std::uint64_t> m_write_timing_wal_append_samples;
    mutable std::vector<std::uint64_t> m_write_timing_lsm_track_samples;
    mutable std::vector<std::uint64_t> m_write_timing_lsm_flush_samples;
    mutable std::vector<std::uint64_t> m_write_timing_lsm_compaction_samples;
    mutable std::vector<std::uint64_t> m_write_timing_lsm_rotate_compact_samples;
    std::atomic<std::uint64_t> m_write_timing_sample_counter{0};
    std::atomic<std::uint64_t> m_write_timing_sample_every_n{64};
    
    // VACUUM 线程
    std::thread m_vacuum_thread;
    std::atomic<bool> m_vacuum_running{false};
    std::mutex m_vacuum_mutex;
    std::condition_variable m_vacuum_cv;
    /// `(table_name, debt_score)` — higher `debt_score` (file bytes + optional health bonus) runs first.
    std::vector<std::pair<std::string, std::uint64_t>> m_vacuum_queue;
    std::unordered_set<std::string> m_vacuum_pending;
    std::function<void(const std::string&)> m_vacuum_callback;
    std::atomic<std::size_t> m_vacuum_ops_threshold{300};
    std::atomic<std::size_t> m_vacuum_min_interval_sec{30};
    std::atomic<std::size_t> m_vacuum_op_counter{0};
    std::atomic<std::uint64_t> m_vacuum_trigger_count{0};
    std::atomic<std::uint64_t> m_vacuum_execute_count{0};
    std::atomic<std::uint64_t> m_vacuum_cooldown_skip_count{0};
    std::atomic<std::uint64_t> m_vacuum_compact_success_count{0};
    std::atomic<std::uint64_t> m_vacuum_compact_failure_count{0};
    std::atomic<std::uint64_t> m_vacuum_compact_bytes_reclaimed{0};
    std::atomic<std::uint64_t> m_vacuum_compact_last_elapsed_ms{0};
    std::atomic<std::uint64_t> m_vacuum_queue_depth{0};
    std::atomic<std::uint64_t> m_vacuum_queue_depth_peak{0};
    std::atomic<std::uint64_t> m_maintenance_checkpoint_trigger_count{0};
    std::atomic<std::uint64_t> m_maintenance_checkpoint_vacuum_enqueue_count{0};
    std::atomic<WriteConflictPolicy> m_write_conflict_policy{WriteConflictPolicy::Reject};
    std::atomic<std::uint64_t> m_write_conflict_wait_timeout_ms{0};
    std::atomic<TxnIsolationLevel> m_txn_isolation_level{TxnIsolationLevel::Snapshot};
    std::atomic<std::uint64_t> m_write_conflict_count{0};
    std::atomic<std::uint64_t> m_write_conflict_wait_count{0};
    std::atomic<std::uint64_t> m_write_conflict_wait_timeout_count{0};
    mutable std::mutex m_write_conflict_sample_mu;
    std::string m_write_conflict_last_sample;
    std::atomic<std::uint64_t> m_file_lock_acquire_fail_count{0};
    std::atomic<std::uint64_t> m_file_lock_same_process_reuse_count{0};
    std::atomic<std::uint64_t> m_file_lock_stale_marker_count{0};
    std::atomic<std::uint64_t> m_txn_begin_lock_conflict_count{0};
    std::atomic<std::uint64_t> m_wal_compact_count{0};
    std::atomic<std::uint64_t> m_wal_recovery_runs{0};
    std::atomic<std::uint64_t> m_wal_recovery_undo_ops{0};
    std::atomic<std::uint64_t> m_wal_recovery_last_elapsed_ms{0};
    std::atomic<std::uint64_t> m_wal_recovery_analyze_ms{0};
    std::atomic<std::uint64_t> m_wal_recovery_undo_ms{0};
    std::atomic<std::uint64_t> m_wal_recovery_finalize_ms{0};
    std::atomic<std::uint64_t> m_wal_recovery_records_scanned{0};
    std::atomic<std::uint64_t> m_wal_recovery_dangling_txns{0};
    std::atomic<std::uint64_t> m_wal_recovery_redo_ms{0};
    std::atomic<std::uint64_t> m_wal_recovery_checkpoint_begin_count{0};
    std::atomic<std::uint64_t> m_wal_recovery_checkpoint_end_count{0};
    std::atomic<std::uint64_t> m_wal_recovery_records_after_checkpoint{0};
    std::atomic<std::uint64_t> m_wal_recovery_segments_after_checkpoint{0};
    std::atomic<std::uint64_t> m_wal_recovery_redo_plan_pending_txn_count{0};
    std::atomic<std::uint64_t> m_wal_recovery_apply_conflict_count{0};
    mutable std::mutex m_wal_recovery_policy_mu;
    std::string m_wal_recovery_policy;
    std::atomic<std::uint64_t> m_wal_group_commit_count{0};
    std::atomic<std::uint64_t> m_wal_group_commit_batch_commits{0};
    std::atomic<std::uint64_t> m_wal_group_commit_pending_commits{0};
    std::atomic<std::uint64_t> m_txn_commit_count{0};
    std::atomic<std::uint64_t> m_txn_commit_max_ms{0};
    std::atomic<std::uint64_t> m_wal_bytes_since_start{0};
    std::atomic<std::uint64_t> m_last_wal_bytes{0};
    std::atomic<std::uint64_t> m_lock_deadlock_detect_count{0};
    std::atomic<std::uint64_t> m_lock_deadlock_victim_count{0};
    std::atomic<std::uint64_t> m_lock_wait_ms_total{0};
    std::atomic<std::uint64_t> m_lock_wait_max_ms{0};
    std::atomic<std::uint64_t> m_scheduler_throttle_count{0};
    std::atomic<std::uint64_t> m_group_commit_window_ms{0};
    std::atomic<std::uint64_t> m_group_commit_max_batch_commits{1};
    std::atomic<bool> m_wal_adaptive_enabled{false};
    std::atomic<std::uint64_t> m_last_wal_flush_ms{0};
    std::atomic<bool> m_hot_index_enabled{true};
    std::atomic<std::uint64_t> m_segment_target_bytes{0};
    std::atomic<std::uint64_t> m_lsm_memtable_flush_count{0};
    std::atomic<std::uint64_t> m_lsm_compaction_count{0};
    std::atomic<std::uint64_t> m_lsm_segment_count{0};
    std::atomic<std::uint64_t> m_lsm_memtable_bytes{0};
    std::atomic<std::uint64_t> m_lsm_read_segments_scanned{0};
    std::atomic<std::uint64_t> m_lsm_compaction_bytes_in{0};
    std::atomic<std::uint64_t> m_lsm_compaction_bytes_out{0};
    std::atomic<std::uint64_t> m_lsm_compaction_queue_pending{0};
    std::atomic<std::uint64_t> m_lsm_compaction_queue_inflight{0};
    std::atomic<std::uint64_t> m_lsm_compaction_enqueue_skipped_backpressure{0};
    std::atomic<std::uint64_t> m_lsm_segment_cache_hits{0};
    std::atomic<std::uint64_t> m_lsm_segment_cache_misses{0};
    std::atomic<bool> m_hybrid_adaptive_enabled{false};
    std::atomic<std::uint64_t> m_hybrid_mode_switch_count{0};
    std::atomic<std::uint8_t> m_hybrid_mode{0}; // 0 throughput, 1 durability
    std::atomic<std::uint64_t> m_hybrid_last_switch_ms{0};
    std::string m_hybrid_last_switch_reason;
    mutable std::mutex m_hybrid_mu;
    mutable std::vector<std::uint64_t> m_lsm_read_segments_scanned_samples;
    std::unordered_map<std::string, std::chrono::steady_clock::time_point> m_vacuum_last_run;

    mutable std::mutex m_samples_mu;
    mutable std::vector<std::uint64_t> m_commit_latency_ms_samples;
    mutable std::vector<std::uint64_t> m_lock_wait_ms_samples;

    std::string m_workspace_root;
    std::string m_active_table;
    std::unordered_set<std::string> m_reserved_write_keys;
    std::atomic<std::uint64_t> m_txn_op_seq{0};
    std::map<std::string, std::uint64_t> m_savepoints;
    std::map<std::string, std::uint64_t> m_savepoints_lsn;
    std::uint64_t m_last_undo_lsn{0};
    std::atomic<std::uint64_t> m_rollback_savepoint_count{0};
    std::atomic<std::uint64_t> m_rollback_partial_ops{0};
    std::atomic<std::uint64_t> m_pitr_runs{0};
    std::atomic<std::uint64_t> m_pitr_target_lsn{0};
    std::atomic<std::uint64_t> m_pitr_elapsed_ms{0};
    std::atomic<std::uint64_t> m_undo_chain_fallback_count{0};
    std::atomic<std::uint64_t> m_lazy_materialize_count{0};
    std::atomic<std::uint64_t> m_lazy_materialize_rows_total{0};
    std::atomic<std::uint64_t> m_lazy_materialize_max_rows{0};
    std::atomic<std::uint64_t> m_lazy_materialize_elapsed_ms{0};
    std::atomic<std::uint64_t> m_vacuum_priority_score_last{0};
    std::atomic<std::uint64_t> m_vacuum_health_bonus_last{0};
    std::atomic<std::uint64_t> m_vacuum_score_file_term_last{0};
    std::atomic<std::uint64_t> m_vacuum_score_health_bonus_term_last{0};
    std::atomic<std::uint64_t> m_vacuum_score_wal_since_term_last{0};
    std::atomic<std::uint64_t> m_compact_debt_bytes_last{0};
    std::atomic<std::uint64_t> m_compact_debt_rows_last{0};
    std::atomic<std::uint64_t> m_compact_debt_ratio_micro_last{0};
    std::atomic<std::uint64_t> m_compact_debt_priority_last{0};
    mutable std::mutex m_last_storage_health_mu;
    newdb::TableStorageHealth m_last_storage_health{};
    /// Fixed `snapshot_lsn` for Snapshot isolation for the duration of an active txn (0 = not set).
    std::atomic<std::uint64_t> m_txn_read_view_lsn{0};
    std::atomic<std::uint64_t> m_txn_snapshot_refresh_count{0};
    std::atomic<std::uint64_t> m_txn_snapshot_pinned_count{0};
    std::atomic<std::uint64_t> m_txn_readpath_disabled_count{0};
    /// 0=none, 1=txn, 2=statement, 3=disabled — surfaced as `last_snapshot_source` string in runtime stats.
    std::atomic<std::uint8_t> m_last_snapshot_source_code{0};
    std::atomic<std::uint64_t> m_last_transaction_snapshot_lsn{0};
    std::atomic<std::uint64_t> m_last_statement_snapshot_lsn{0};
    std::atomic<std::uint64_t> m_lock_key_range_count{0};
    std::atomic<std::uint64_t> m_lock_key_predicate_count{0};
};
