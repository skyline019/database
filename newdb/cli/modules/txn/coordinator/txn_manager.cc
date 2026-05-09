#include <waterfall/config.h>

#include <cstdlib>
#include <mutex>
#include <string>
#include <vector>

#include "cli/modules/txn/coordinator/txn_manager.h"
#include "cli/modules/txn/coordinator/txn_coordinator_state.h"

TxnCoordinator::TxnCoordinator() : st_(std::make_unique<TxnCoordinatorState>()) {
    std::uint64_t every_n = 8;
    if (const char* raw = std::getenv("NEWDB_WRITE_TIMING_EVERY_N")) {
        try {
            const std::uint64_t v = static_cast<std::uint64_t>(std::stoull(raw));
            if (v > 0) every_n = v;
        } catch (...) {
        }
    }
    st_->m_write_timing_sample_every_n.store(every_n, std::memory_order_relaxed);
}

TxnCoordinator::~TxnCoordinator() {
    stopVacuumThread();
    clearWriteIntents();
    std::vector<std::string> locked_copy;
    {
        std::lock_guard<std::mutex> lk(st_->m_lock_mutex);
        locked_copy = st_->m_locked_files;
    }
    for (const auto& f : locked_copy) {
        (void)releaseLock(f);
    }
}

TxnState TxnCoordinator::getState() const {
    return st_->m_state.load();
}

int64_t TxnCoordinator::getTxnId() const {
    return st_->m_txn_id.load();
}

bool TxnCoordinator::inTransaction() const {
    return st_->m_state.load() == TxnState::Active;
}

std::size_t TxnCoordinator::vacuumOpsThreshold() const {
    return st_->m_vacuum_ops_threshold.load();
}

std::size_t TxnCoordinator::vacuumMinIntervalSec() const {
    return st_->m_vacuum_min_interval_sec.load();
}

std::uint64_t TxnCoordinator::vacuumTriggerCount() const {
    return st_->m_vacuum_trigger_count.load();
}

std::uint64_t TxnCoordinator::vacuumExecuteCount() const {
    return st_->m_vacuum_execute_count.load();
}

std::uint64_t TxnCoordinator::vacuumCooldownSkipCount() const {
    return st_->m_vacuum_cooldown_skip_count.load();
}

std::uint64_t TxnCoordinator::vacuumCompactSuccessCount() const {
    return st_->m_vacuum_compact_success_count.load();
}

std::uint64_t TxnCoordinator::vacuumCompactFailureCount() const {
    return st_->m_vacuum_compact_failure_count.load();
}

std::uint64_t TxnCoordinator::vacuumCompactBytesReclaimed() const {
    return st_->m_vacuum_compact_bytes_reclaimed.load();
}

std::uint64_t TxnCoordinator::vacuumCompactLastElapsedMs() const {
    return st_->m_vacuum_compact_last_elapsed_ms.load();
}

std::uint64_t TxnCoordinator::vacuumQueueDepth() const {
    return st_->m_vacuum_queue_depth.load();
}

std::uint64_t TxnCoordinator::vacuumQueueDepthPeak() const {
    return st_->m_vacuum_queue_depth_peak.load();
}

std::uint64_t TxnCoordinator::writeConflictCount() const {
    return st_->m_write_conflict_count.load();
}

std::uint64_t TxnCoordinator::writeConflictWaitCount() const {
    return st_->m_write_conflict_wait_count.load();
}

std::uint64_t TxnCoordinator::writeConflictWaitTimeoutCount() const {
    return st_->m_write_conflict_wait_timeout_count.load();
}

std::uint64_t TxnCoordinator::txnBeginLockConflictCount() const {
    return st_->m_txn_begin_lock_conflict_count.load();
}

std::uint64_t TxnCoordinator::walCompactCount() const {
    return st_->m_wal_compact_count.load();
}

std::uint64_t TxnCoordinator::walRecoveryRuns() const {
    return st_->m_wal_recovery_runs.load();
}

std::uint64_t TxnCoordinator::walRecoveryUndoOps() const {
    return st_->m_wal_recovery_undo_ops.load();
}

std::uint64_t TxnCoordinator::walRecoveryLastElapsedMs() const {
    return st_->m_wal_recovery_last_elapsed_ms.load();
}

std::uint64_t TxnCoordinator::walRecoveryAnalyzeMs() const {
    return st_->m_wal_recovery_analyze_ms.load();
}

std::uint64_t TxnCoordinator::walRecoveryUndoMs() const {
    return st_->m_wal_recovery_undo_ms.load();
}

std::uint64_t TxnCoordinator::walRecoveryFinalizeMs() const {
    return st_->m_wal_recovery_finalize_ms.load();
}

std::uint64_t TxnCoordinator::walRecoveryRecordsScanned() const {
    return st_->m_wal_recovery_records_scanned.load();
}

std::uint64_t TxnCoordinator::walRecoveryDanglingTxns() const {
    return st_->m_wal_recovery_dangling_txns.load();
}

std::uint64_t TxnCoordinator::walRecoveryRedoMs() const {
    return st_->m_wal_recovery_redo_ms.load();
}

std::uint64_t TxnCoordinator::walRecoveryCheckpointBeginCount() const {
    return st_->m_wal_recovery_checkpoint_begin_count.load();
}

std::uint64_t TxnCoordinator::walRecoveryCheckpointEndCount() const {
    return st_->m_wal_recovery_checkpoint_end_count.load();
}

std::uint64_t TxnCoordinator::walGroupCommitCount() const {
    return st_->m_wal_group_commit_count.load();
}

std::uint64_t TxnCoordinator::walGroupCommitBatchCommits() const {
    return st_->m_wal_group_commit_batch_commits.load();
}

std::uint64_t TxnCoordinator::walGroupCommitPendingCommits() const {
    return st_->m_wal_group_commit_pending_commits.load();
}

std::uint64_t TxnCoordinator::txnCommitCount() const {
    return st_->m_txn_commit_count.load();
}

std::uint64_t TxnCoordinator::txnCommitMaxMs() const {
    return st_->m_txn_commit_max_ms.load();
}

std::uint64_t TxnCoordinator::walBytesSinceStart() const {
    return st_->m_wal_bytes_since_start.load();
}

std::uint64_t TxnCoordinator::lockDeadlockDetectCount() const {
    return st_->m_lock_deadlock_detect_count.load();
}

std::uint64_t TxnCoordinator::lockDeadlockVictimCount() const {
    return st_->m_lock_deadlock_victim_count.load();
}

std::uint64_t TxnCoordinator::lockWaitMsTotal() const {
    return st_->m_lock_wait_ms_total.load();
}

std::uint64_t TxnCoordinator::lockWaitMaxMs() const {
    return st_->m_lock_wait_max_ms.load();
}

std::uint64_t TxnCoordinator::schedulerThrottleCount() const {
    return st_->m_scheduler_throttle_count.load();
}

bool TxnCoordinator::vacuumRunning() const {
    return st_->m_vacuum_running.load();
}

const std::string& TxnCoordinator::workspace_root() const {
    return st_->m_workspace_root;
}
