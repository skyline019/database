#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <newdb/wal_manager.h>

#include "cli/modules/common/util/constants.h"
#include "cli/modules/storage/table_storage_health.h"
#include "cli/modules/txn/coordinator/txn_coordinator_types.h"

/// Implementation detail for `TxnCoordinator`; include only from coordinator `.cc` units.
struct TxnCoordinatorState {
    struct LockHandleState {
        std::string lock_file_path;
#if defined(_WIN32)
        void* handle{nullptr};
#else
        int fd{-1};
#endif
    };

    std::uint64_t wal_compact_max_bytes{4ull * 1024ull * 1024ull};

    std::atomic<TxnState> m_state{TxnState::None};
    std::atomic<int64_t> m_txn_id{0};
    std::mutex m_txn_mutex;

    std::vector<TxnRecord> m_txn_records;

    mutable std::mutex m_lock_mutex;
    std::vector<std::string> m_locked_files;
    std::unordered_map<std::string, LockHandleState> m_lock_handles;

    std::mutex m_wal_mutex;
    std::unique_ptr<newdb::WalManager> wal_;

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

    std::thread m_vacuum_thread;
    std::atomic<bool> m_vacuum_running{false};
    std::mutex m_vacuum_mutex;
    std::condition_variable m_vacuum_cv;
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
    std::atomic<std::uint8_t> m_hybrid_mode{0};
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
    std::atomic<std::uint64_t> m_txn_read_view_lsn{0};
    std::atomic<std::uint64_t> m_txn_snapshot_refresh_count{0};
    std::atomic<std::uint64_t> m_txn_snapshot_pinned_count{0};
    std::atomic<std::uint64_t> m_txn_readpath_disabled_count{0};
    std::atomic<std::uint8_t> m_last_snapshot_source_code{0};
    std::atomic<std::uint64_t> m_last_transaction_snapshot_lsn{0};
    std::atomic<std::uint64_t> m_last_statement_snapshot_lsn{0};
    std::atomic<std::uint64_t> m_lock_key_range_count{0};
    std::atomic<std::uint64_t> m_lock_key_predicate_count{0};
};
