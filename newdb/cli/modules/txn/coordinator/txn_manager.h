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

#include "cli/modules/util/result.h"
#include "cli/modules/util/constants.h"

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
    
    // 文件锁
    Result<bool> acquireLock(const std::string& file_path);
    Result<bool> releaseLock(const std::string& file_path);
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
    TxnRuntimeStats runtimeStats() const;
    bool vacuumRunning() const { return m_vacuum_running.load(); }

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
    std::uint64_t wal_compact_max_bytes{4ull * 1024ull * 1024ull};
    
    // 事务状态
    std::atomic<TxnState> m_state{TxnState::None};
    std::atomic<int64_t> m_txn_id{0};
    std::mutex m_txn_mutex;
    
    // 事务记录缓冲 (用于回滚)
    std::vector<TxnRecord> m_txn_records;
    
    // 文件锁
    std::mutex m_lock_mutex;
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
    std::vector<std::string> m_vacuum_queue;
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
};
