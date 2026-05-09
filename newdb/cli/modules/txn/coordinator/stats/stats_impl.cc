#include <waterfall/config.h>

#include "cli/modules/txn/coordinator/txn_manager.h"
#include "cli/modules/txn/coordinator/txn_coordinator_state.h"
#include "cli/modules/txn/coordinator/internal/txn_internal.h"
#include "cli/modules/common/logging/logging.h"
#include "cli/modules/common/util/constants.h"
#include "cli/modules/sidecar/common/sidecar_wal_lsn.h"
#include "cli/modules/sidecar/common/index_catalog.h"
#include "cli/modules/sidecar/eq/equality_index_sidecar.h"

#include <newdb/heap_table.h>
#include <newdb/page_io.h>
#include <newdb/schema_io.h>
#include <newdb/memory_budget.h>
#include <newdb/memory_registry.h>
#include <newdb/page_cache.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

void TxnCoordinator::onLsmMemtableFlush() {
    st_->m_lsm_memtable_flush_count.fetch_add(1, std::memory_order_relaxed);
}


void TxnCoordinator::onLsmCompaction() {
    st_->m_lsm_compaction_count.fetch_add(1, std::memory_order_relaxed);
}


void TxnCoordinator::onLsmReadSegmentsScanned(const std::uint64_t n) {
    st_->m_lsm_read_segments_scanned.fetch_add(n, std::memory_order_relaxed);
    std::lock_guard<std::mutex> lk(st_->m_samples_mu);
    st_->m_lsm_read_segments_scanned_samples.push_back(n);
    if (st_->m_lsm_read_segments_scanned_samples.size() > 256) {
        st_->m_lsm_read_segments_scanned_samples.erase(st_->m_lsm_read_segments_scanned_samples.begin(),
                                                  st_->m_lsm_read_segments_scanned_samples.begin() + 64);
    }
}


void TxnCoordinator::onLsmCompactionBytes(const std::uint64_t bytes_in, const std::uint64_t bytes_out) {
    st_->m_lsm_compaction_bytes_in.fetch_add(bytes_in, std::memory_order_relaxed);
    st_->m_lsm_compaction_bytes_out.fetch_add(bytes_out, std::memory_order_relaxed);
}


void TxnCoordinator::onLsmCompactionQueueDepth(const std::uint64_t pending, const std::uint64_t inflight) {
    st_->m_lsm_compaction_queue_pending.store(pending, std::memory_order_relaxed);
    st_->m_lsm_compaction_queue_inflight.store(inflight, std::memory_order_relaxed);
}


void TxnCoordinator::onLsmCompactionEnqueueSkippedBackpressure() {
    st_->m_lsm_compaction_enqueue_skipped_backpressure.fetch_add(1, std::memory_order_relaxed);
}


void TxnCoordinator::onLsmSegmentCacheLookup(const bool hit) {
    if (hit) {
        st_->m_lsm_segment_cache_hits.fetch_add(1, std::memory_order_relaxed);
    } else {
        st_->m_lsm_segment_cache_misses.fetch_add(1, std::memory_order_relaxed);
    }
}


void TxnCoordinator::onSchedulerThrottled() {
    st_->m_scheduler_throttle_count.fetch_add(1, std::memory_order_relaxed);
}


void TxnCoordinator::noteLazyMaterialize(const std::uint64_t rows, const std::uint64_t elapsed_ms) {
    st_->m_lazy_materialize_count.fetch_add(1, std::memory_order_relaxed);
    st_->m_lazy_materialize_rows_total.fetch_add(rows, std::memory_order_relaxed);
    st_->m_lazy_materialize_elapsed_ms.fetch_add(elapsed_ms, std::memory_order_relaxed);
    std::uint64_t old_max = st_->m_lazy_materialize_max_rows.load(std::memory_order_relaxed);
    while (rows > old_max &&
           !st_->m_lazy_materialize_max_rows.compare_exchange_weak(old_max, rows, std::memory_order_relaxed,
                                                              std::memory_order_relaxed)) {
    }
}


void TxnCoordinator::onWriteTiming(const WriteTimingStage stage, const std::uint64_t elapsed_ms) {
    // Always track max, but only sample into p95 buffers every N operations to keep overhead small.
    auto bump_max = [&](std::atomic<std::uint64_t>& maxv) {
        std::uint64_t old = maxv.load(std::memory_order_relaxed);
        while (elapsed_ms > old &&
               !maxv.compare_exchange_weak(old, elapsed_ms, std::memory_order_relaxed, std::memory_order_relaxed)) {
        }
    };
    switch (stage) {
    case WriteTimingStage::HeapAppend: bump_max(st_->m_write_timing_heap_append_max_ms); break;
    case WriteTimingStage::HotIndex: bump_max(st_->m_write_timing_hot_index_max_ms); break;
    case WriteTimingStage::SidecarInvalidate: bump_max(st_->m_write_timing_sidecar_invalidate_max_ms); break;
    case WriteTimingStage::WalAppend: bump_max(st_->m_write_timing_wal_append_max_ms); break;
    case WriteTimingStage::LsmTrack: bump_max(st_->m_write_timing_lsm_track_max_ms); break;
    case WriteTimingStage::LsmFlush: bump_max(st_->m_write_timing_lsm_flush_max_ms); break;
    case WriteTimingStage::LsmCompaction: bump_max(st_->m_write_timing_lsm_compaction_max_ms); break;
    case WriteTimingStage::LsmRotateCompact: bump_max(st_->m_write_timing_lsm_rotate_compact_max_ms); break;
    default: break;
    }

    const std::uint64_t every_n = st_->m_write_timing_sample_every_n.load(std::memory_order_relaxed);
    const std::uint64_t k = st_->m_write_timing_sample_counter.fetch_add(1, std::memory_order_relaxed) + 1;
    if (every_n == 0 || (k % every_n) != 0) {
        return;
    }
    std::lock_guard<std::mutex> lk(st_->m_samples_mu);
    auto push_trim = [&](std::vector<std::uint64_t>& v) {
        v.push_back(elapsed_ms);
        if (v.size() > 256) {
            v.erase(v.begin(), v.begin() + 64);
        }
    };
    switch (stage) {
    case WriteTimingStage::HeapAppend: push_trim(st_->m_write_timing_heap_append_samples); break;
    case WriteTimingStage::HotIndex: push_trim(st_->m_write_timing_hot_index_samples); break;
    case WriteTimingStage::SidecarInvalidate: push_trim(st_->m_write_timing_sidecar_invalidate_samples); break;
    case WriteTimingStage::WalAppend: push_trim(st_->m_write_timing_wal_append_samples); break;
    case WriteTimingStage::LsmTrack: push_trim(st_->m_write_timing_lsm_track_samples); break;
    case WriteTimingStage::LsmFlush: push_trim(st_->m_write_timing_lsm_flush_samples); break;
    case WriteTimingStage::LsmCompaction: push_trim(st_->m_write_timing_lsm_compaction_samples); break;
    case WriteTimingStage::LsmRotateCompact: push_trim(st_->m_write_timing_lsm_rotate_compact_samples); break;
    default: break;
    }
}


TxnRuntimeStats TxnCoordinator::runtimeStats() const {
    TxnRuntimeStats s{};
    s.vacuum_trigger_count = st_->m_vacuum_trigger_count.load(std::memory_order_relaxed);
    s.vacuum_execute_count = st_->m_vacuum_execute_count.load(std::memory_order_relaxed);
    s.vacuum_cooldown_skip_count = st_->m_vacuum_cooldown_skip_count.load(std::memory_order_relaxed);
    s.vacuum_compact_success_count = st_->m_vacuum_compact_success_count.load(std::memory_order_relaxed);
    s.vacuum_compact_failure_count = st_->m_vacuum_compact_failure_count.load(std::memory_order_relaxed);
    s.vacuum_compact_bytes_reclaimed = st_->m_vacuum_compact_bytes_reclaimed.load(std::memory_order_relaxed);
    s.vacuum_compact_last_elapsed_ms = st_->m_vacuum_compact_last_elapsed_ms.load(std::memory_order_relaxed);
    s.vacuum_queue_depth = st_->m_vacuum_queue_depth.load(std::memory_order_relaxed);
    s.vacuum_queue_depth_peak = st_->m_vacuum_queue_depth_peak.load(std::memory_order_relaxed);
    s.maintenance_checkpoint_trigger_count =
        st_->m_maintenance_checkpoint_trigger_count.load(std::memory_order_relaxed);
    s.maintenance_checkpoint_vacuum_enqueue_count =
        st_->m_maintenance_checkpoint_vacuum_enqueue_count.load(std::memory_order_relaxed);
    s.write_conflict_count = st_->m_write_conflict_count.load(std::memory_order_relaxed);
    s.write_conflict_wait_count = st_->m_write_conflict_wait_count.load(std::memory_order_relaxed);
    s.write_conflict_wait_timeout_count = st_->m_write_conflict_wait_timeout_count.load(std::memory_order_relaxed);
    {
        std::lock_guard<std::mutex> lk(st_->m_write_conflict_sample_mu);
        s.write_conflict_last_sample = st_->m_write_conflict_last_sample;
    }
    s.file_lock_acquire_fail_count = st_->m_file_lock_acquire_fail_count.load(std::memory_order_relaxed);
    s.file_lock_same_process_reuse_count = st_->m_file_lock_same_process_reuse_count.load(std::memory_order_relaxed);
    s.file_lock_stale_marker_count = st_->m_file_lock_stale_marker_count.load(std::memory_order_relaxed);
    s.sidecar_invalidate_count = index_catalog_sidecar_invalidate_request_count();
    s.sidecar_invalidate_fail_count = eq_sidecar_invalidate_remove_fail_count();
    s.txn_begin_lock_conflict_count = st_->m_txn_begin_lock_conflict_count.load(std::memory_order_relaxed);
    s.wal_compact_count = st_->m_wal_compact_count.load(std::memory_order_relaxed);
    s.wal_recovery_runs = st_->m_wal_recovery_runs.load(std::memory_order_relaxed);
    s.wal_recovery_undo_ops = st_->m_wal_recovery_undo_ops.load(std::memory_order_relaxed);
    s.wal_recovery_last_elapsed_ms = st_->m_wal_recovery_last_elapsed_ms.load(std::memory_order_relaxed);
    s.wal_recovery_analyze_ms = st_->m_wal_recovery_analyze_ms.load(std::memory_order_relaxed);
    s.wal_recovery_undo_ms = st_->m_wal_recovery_undo_ms.load(std::memory_order_relaxed);
    s.wal_recovery_finalize_ms = st_->m_wal_recovery_finalize_ms.load(std::memory_order_relaxed);
    s.wal_recovery_records_scanned = st_->m_wal_recovery_records_scanned.load(std::memory_order_relaxed);
    s.wal_recovery_dangling_txns = st_->m_wal_recovery_dangling_txns.load(std::memory_order_relaxed);
    s.wal_recovery_redo_ms = st_->m_wal_recovery_redo_ms.load(std::memory_order_relaxed);
    s.wal_recovery_checkpoint_begin_count = st_->m_wal_recovery_checkpoint_begin_count.load(std::memory_order_relaxed);
    s.wal_recovery_checkpoint_end_count = st_->m_wal_recovery_checkpoint_end_count.load(std::memory_order_relaxed);
    s.wal_recovery_records_after_checkpoint = st_->m_wal_recovery_records_after_checkpoint.load(std::memory_order_relaxed);
    s.wal_recovery_segments_after_checkpoint = st_->m_wal_recovery_segments_after_checkpoint.load(std::memory_order_relaxed);
    s.wal_recovery_redo_plan_pending_txn_count =
        st_->m_wal_recovery_redo_plan_pending_txn_count.load(std::memory_order_relaxed);
    s.wal_recovery_apply_conflict_count = st_->m_wal_recovery_apply_conflict_count.load(std::memory_order_relaxed);
    {
        std::lock_guard<std::mutex> lk(st_->m_wal_recovery_policy_mu);
        s.wal_recovery_policy = st_->m_wal_recovery_policy;
    }
    s.wal_group_commit_count = st_->m_wal_group_commit_count.load(std::memory_order_relaxed);
    s.wal_group_commit_batch_commits = st_->m_wal_group_commit_batch_commits.load(std::memory_order_relaxed);
    s.wal_group_commit_pending_commits = st_->m_wal_group_commit_pending_commits.load(std::memory_order_relaxed);
    s.txn_commit_count = st_->m_txn_commit_count.load(std::memory_order_relaxed);
    s.txn_commit_max_ms = st_->m_txn_commit_max_ms.load(std::memory_order_relaxed);
    s.wal_bytes_since_start = st_->m_wal_bytes_since_start.load(std::memory_order_relaxed);
    if (s.txn_commit_count > 0) {
        s.wal_bytes_per_commit_avg = s.wal_bytes_since_start / s.txn_commit_count;
    }
    s.lock_deadlock_detect_count = st_->m_lock_deadlock_detect_count.load(std::memory_order_relaxed);
    s.lock_deadlock_victim_count = st_->m_lock_deadlock_victim_count.load(std::memory_order_relaxed);
    s.lock_wait_ms_total = st_->m_lock_wait_ms_total.load(std::memory_order_relaxed);
    s.lock_wait_max_ms = st_->m_lock_wait_max_ms.load(std::memory_order_relaxed);
    {
        std::vector<std::uint64_t> commits;
        std::vector<std::uint64_t> waits;
        std::vector<std::uint64_t> heap_app;
        std::vector<std::uint64_t> hot_idx;
        std::vector<std::uint64_t> sidecar;
        std::vector<std::uint64_t> wal_app;
        std::vector<std::uint64_t> lsm_track;
        std::vector<std::uint64_t> lsm_flush;
        std::vector<std::uint64_t> lsm_compaction;
        std::vector<std::uint64_t> lsm_rot;
        {
            std::lock_guard<std::mutex> lk(st_->m_samples_mu);
            commits = st_->m_commit_latency_ms_samples;
            waits = st_->m_lock_wait_ms_samples;
            heap_app = st_->m_write_timing_heap_append_samples;
            hot_idx = st_->m_write_timing_hot_index_samples;
            sidecar = st_->m_write_timing_sidecar_invalidate_samples;
            wal_app = st_->m_write_timing_wal_append_samples;
            lsm_track = st_->m_write_timing_lsm_track_samples;
            lsm_flush = st_->m_write_timing_lsm_flush_samples;
            lsm_compaction = st_->m_write_timing_lsm_compaction_samples;
            lsm_rot = st_->m_write_timing_lsm_rotate_compact_samples;
        }
        auto p95 = [](std::vector<std::uint64_t> v) -> std::uint64_t {
            if (v.empty()) return 0;
            std::sort(v.begin(), v.end());
            const std::size_t n = v.size();
            const std::size_t rank = static_cast<std::size_t>(std::ceil(0.95 * static_cast<double>(n)));
            const std::size_t idx = (rank == 0) ? 0 : (rank - 1);
            return v[std::min(idx, n - 1)];
        };
        s.txn_commit_p95_ms = p95(std::move(commits));
        s.lock_wait_p95_ms = p95(std::move(waits));
        s.write_heap_append_p95_ms = p95(std::move(heap_app));
        s.write_hot_index_p95_ms = p95(std::move(hot_idx));
        s.write_sidecar_invalidate_p95_ms = p95(std::move(sidecar));
        s.write_wal_append_p95_ms = p95(std::move(wal_app));
        s.write_lsm_track_p95_ms = p95(std::move(lsm_track));
        s.write_lsm_flush_p95_ms = p95(std::move(lsm_flush));
        s.write_lsm_compaction_p95_ms = p95(std::move(lsm_compaction));
        s.write_lsm_rotate_compact_p95_ms = p95(std::move(lsm_rot));
    }
    s.scheduler_throttle_count = st_->m_scheduler_throttle_count.load(std::memory_order_relaxed);
    s.hot_index_enabled = st_->m_hot_index_enabled.load(std::memory_order_relaxed);
    s.segment_target_bytes = st_->m_segment_target_bytes.load(std::memory_order_relaxed);
    s.lsm_memtable_flush_count = st_->m_lsm_memtable_flush_count.load(std::memory_order_relaxed);
    s.lsm_compaction_count = st_->m_lsm_compaction_count.load(std::memory_order_relaxed);
    s.lsm_segment_count = st_->m_lsm_segment_count.load(std::memory_order_relaxed);
    s.lsm_memtable_bytes = st_->m_lsm_memtable_bytes.load(std::memory_order_relaxed);
    s.lsm_read_segments_scanned = st_->m_lsm_read_segments_scanned.load(std::memory_order_relaxed);
    s.lsm_compaction_bytes_in = st_->m_lsm_compaction_bytes_in.load(std::memory_order_relaxed);
    s.lsm_compaction_bytes_out = st_->m_lsm_compaction_bytes_out.load(std::memory_order_relaxed);
    s.lsm_compaction_queue_pending = st_->m_lsm_compaction_queue_pending.load(std::memory_order_relaxed);
    s.lsm_compaction_queue_inflight = st_->m_lsm_compaction_queue_inflight.load(std::memory_order_relaxed);
    s.lsm_compaction_enqueue_skipped_backpressure =
        st_->m_lsm_compaction_enqueue_skipped_backpressure.load(std::memory_order_relaxed);
    s.lsm_segment_cache_hits = st_->m_lsm_segment_cache_hits.load(std::memory_order_relaxed);
    s.lsm_segment_cache_misses = st_->m_lsm_segment_cache_misses.load(std::memory_order_relaxed);
    const auto comp_in = static_cast<double>(s.lsm_compaction_bytes_in);
    const auto comp_out = static_cast<double>(s.lsm_compaction_bytes_out);
    s.lsm_compaction_bytes_amp_efficiency_min_window = (comp_in > 0.0) ? (comp_out / comp_in) : 0.0;
    s.write_heap_append_max_ms = st_->m_write_timing_heap_append_max_ms.load(std::memory_order_relaxed);
    s.write_hot_index_max_ms = st_->m_write_timing_hot_index_max_ms.load(std::memory_order_relaxed);
    s.write_sidecar_invalidate_max_ms = st_->m_write_timing_sidecar_invalidate_max_ms.load(std::memory_order_relaxed);
    s.write_wal_append_max_ms = st_->m_write_timing_wal_append_max_ms.load(std::memory_order_relaxed);
    s.write_lsm_track_max_ms = st_->m_write_timing_lsm_track_max_ms.load(std::memory_order_relaxed);
    s.write_lsm_flush_max_ms = st_->m_write_timing_lsm_flush_max_ms.load(std::memory_order_relaxed);
    s.write_lsm_compaction_max_ms = st_->m_write_timing_lsm_compaction_max_ms.load(std::memory_order_relaxed);
    s.write_lsm_rotate_compact_max_ms = st_->m_write_timing_lsm_rotate_compact_max_ms.load(std::memory_order_relaxed);
    {
        std::vector<std::uint64_t> scans;
        {
            std::lock_guard<std::mutex> lk(st_->m_samples_mu);
            scans = st_->m_lsm_read_segments_scanned_samples;
        }
        auto p95 = [](std::vector<std::uint64_t> v) -> std::uint64_t {
            if (v.empty()) return 0;
            std::sort(v.begin(), v.end());
            const std::size_t n = v.size();
            const std::size_t rank = static_cast<std::size_t>(std::ceil(0.95 * static_cast<double>(n)));
            const std::size_t idx = (rank == 0) ? 0 : (rank - 1);
            return v[std::min(idx, n - 1)];
        };
        s.lsm_read_segments_scanned_p95 = p95(std::move(scans));
        s.lsm_read_segments_scanned_p95_window = s.lsm_read_segments_scanned_p95;
    }
    s.hybrid_mode_switch_count = st_->m_hybrid_mode_switch_count.load(std::memory_order_relaxed);
    s.hybrid_mode = (st_->m_hybrid_mode.load(std::memory_order_relaxed) == 0) ? "throughput_mode" : "durability_mode";
    {
        std::lock_guard<std::mutex> lk(st_->m_hybrid_mu);
        s.hybrid_last_switch_reason = st_->m_hybrid_last_switch_reason;
    }
    s.rollback_savepoint_count = st_->m_rollback_savepoint_count.load(std::memory_order_relaxed);
    s.rollback_partial_ops = st_->m_rollback_partial_ops.load(std::memory_order_relaxed);
    s.pitr_runs = st_->m_pitr_runs.load(std::memory_order_relaxed);
    s.pitr_target_lsn = st_->m_pitr_target_lsn.load(std::memory_order_relaxed);
    s.pitr_elapsed_ms = st_->m_pitr_elapsed_ms.load(std::memory_order_relaxed);
    s.undo_chain_fallback_count = st_->m_undo_chain_fallback_count.load(std::memory_order_relaxed);
    s.lazy_materialize_count = st_->m_lazy_materialize_count.load(std::memory_order_relaxed);
    s.lazy_materialize_rows_total = st_->m_lazy_materialize_rows_total.load(std::memory_order_relaxed);
    s.lazy_materialize_max_rows = st_->m_lazy_materialize_max_rows.load(std::memory_order_relaxed);
    s.lazy_materialize_elapsed_ms = st_->m_lazy_materialize_elapsed_ms.load(std::memory_order_relaxed);
    s.vacuum_priority_score = st_->m_vacuum_priority_score_last.load(std::memory_order_relaxed);
    s.vacuum_health_bonus_last = st_->m_vacuum_health_bonus_last.load(std::memory_order_relaxed);
    s.vacuum_score_file_bytes_term = st_->m_vacuum_score_file_term_last.load(std::memory_order_relaxed);
    s.vacuum_score_health_bonus_term = st_->m_vacuum_score_health_bonus_term_last.load(std::memory_order_relaxed);
    s.vacuum_score_wal_since_term = st_->m_vacuum_score_wal_since_term_last.load(std::memory_order_relaxed);
    s.compact_debt_bytes = st_->m_compact_debt_bytes_last.load(std::memory_order_relaxed);
    s.compact_debt_rows = st_->m_compact_debt_rows_last.load(std::memory_order_relaxed);
    {
        const std::uint64_t rm = st_->m_compact_debt_ratio_micro_last.load(std::memory_order_relaxed);
        s.compact_debt_ratio = static_cast<double>(rm) / 1000000.0;
    }
    s.compact_debt_priority = st_->m_compact_debt_priority_last.load(std::memory_order_relaxed);
    {
        const newdb::PageCacheGlobalStats pc = newdb::page_cache_global_stats();
        const newdb::MemoryRegistryTotals regs = newdb::memory_registry_totals();
        s.page_cache_hits = pc.hits;
        s.page_cache_misses = pc.misses;
        s.page_cache_evictions = pc.evictions;
        s.page_cache_bytes_in_cache = pc.bytes_in_cache;
        s.memory_budget_max_bytes = newdb::memory_budget_max_bytes_env();
        s.memory_budget_used_bytes = regs.global_used_bytes != 0
                                         ? regs.global_used_bytes
                                         : pc.bytes_in_cache;
        s.memory_budget_reject_count = regs.global_admit_rejects + pc.reject_oversized_page;
        s.memory_budget_bytes_evicted_total = pc.bytes_evicted_total;
        s.memory_budget_sidecar_load_skipped_total = eq_sidecar_memory_budget_skip_count();
        s.mem_page_cache_used_bytes = regs.page_cache_used_bytes;
        s.mem_page_cache_evictions = regs.page_cache_evictions;
        s.mem_page_cache_admit_rejects = regs.page_cache_admit_rejects;
        s.mem_sidecar_used_bytes = regs.sidecar_used_bytes;
        s.mem_sidecar_evictions = regs.sidecar_evictions;
        s.mem_sidecar_admit_rejects = regs.sidecar_admit_rejects;
        s.mem_query_temp_used_bytes = regs.query_temp_used_bytes;
        s.mem_query_temp_evictions = regs.query_temp_evictions;
        s.mem_query_temp_admit_rejects = regs.query_temp_admit_rejects;
        s.mem_global_used_bytes = regs.global_used_bytes;
        s.mem_global_admit_rejects = regs.global_admit_rejects;
    }
    {
        std::lock_guard<std::mutex> lk(st_->m_last_storage_health_mu);
        const auto& z = st_->m_last_storage_health;
        s.table_storage_health_logical_rows = z.logical_rows;
        s.table_storage_health_physical_rows = z.physical_rows;
        s.table_storage_health_tombstone_rows = z.tombstone_rows;
        s.table_storage_health_data_file_bytes = z.data_file_bytes;
        s.table_storage_health_live_bytes = z.live_bytes;
        s.table_storage_health_dead_bytes = z.dead_bytes;
        s.table_storage_health_fragmentation_ratio = z.fragmentation_ratio;
        s.table_storage_health_last_vacuum_lsn = z.last_vacuum_lsn;
        s.table_storage_health_last_vacuum_elapsed_ms = z.last_vacuum_elapsed_ms;
        const double frag = z.fragmentation_ratio;
        const std::uint64_t dead = z.dead_bytes;
        if (frag >= 0.65 || dead > (16ull * 1024 * 1024)) {
            s.table_storage_health_tier = "critical";
        } else if (frag >= 0.45 || dead > (4ull * 1024 * 1024)) {
            s.table_storage_health_tier = "degraded";
        } else if (frag >= 0.25 || dead > (1024ull * 1024)) {
            s.table_storage_health_tier = "watch";
        } else {
            s.table_storage_health_tier = "good";
        }
    }
    s.txn_snapshot_refresh_count = st_->m_txn_snapshot_refresh_count.load(std::memory_order_relaxed);
    s.txn_snapshot_pinned_count = st_->m_txn_snapshot_pinned_count.load(std::memory_order_relaxed);
    s.txn_readpath_disabled_count = st_->m_txn_readpath_disabled_count.load(std::memory_order_relaxed);
    s.transaction_snapshot_lsn = st_->m_last_transaction_snapshot_lsn.load(std::memory_order_relaxed);
    s.statement_snapshot_lsn = st_->m_last_statement_snapshot_lsn.load(std::memory_order_relaxed);
    switch (st_->m_last_snapshot_source_code.load(std::memory_order_relaxed)) {
    case 1:
        s.last_snapshot_source = "txn";
        break;
    case 2:
        s.last_snapshot_source = "statement";
        break;
    case 3:
        s.last_snapshot_source = "disabled";
        break;
    default:
        s.last_snapshot_source = "none";
        break;
    }
    s.lock_key_range_count = st_->m_lock_key_range_count.load(std::memory_order_relaxed);
    s.lock_key_predicate_count = st_->m_lock_key_predicate_count.load(std::memory_order_relaxed);
    return s;
}

void TxnCoordinator::recordLastStorageHealthSnapshot(const newdb::TableStorageHealth& h) {
    std::lock_guard<std::mutex> lk(st_->m_last_storage_health_mu);
    st_->m_last_storage_health = h;
}

void TxnCoordinator::mergeLastVacuumIntoStorageHealth(std::uint64_t last_vacuum_lsn,
                                                    std::uint64_t last_vacuum_elapsed_ms) {
    std::lock_guard<std::mutex> lk(st_->m_last_storage_health_mu);
    if (last_vacuum_lsn != 0) {
        st_->m_last_storage_health.last_vacuum_lsn = last_vacuum_lsn;
    }
    st_->m_last_storage_health.last_vacuum_elapsed_ms = last_vacuum_elapsed_ms;
}

