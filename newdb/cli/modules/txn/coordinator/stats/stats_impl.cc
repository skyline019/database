#include <waterfall/config.h>

#include "cli/modules/txn/coordinator/txn_manager.h"
#include "cli/modules/txn/coordinator/internal/txn_internal.h"
#include "cli/modules/logging/logging.h"
#include "cli/modules/util/constants.h"
#include "cli/modules/sidecar/common/sidecar_wal_lsn.h"

#include <newdb/heap_table.h>
#include <newdb/page_io.h>
#include <newdb/schema_io.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
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
    m_lsm_memtable_flush_count.fetch_add(1, std::memory_order_relaxed);
}


void TxnCoordinator::onLsmCompaction() {
    m_lsm_compaction_count.fetch_add(1, std::memory_order_relaxed);
}


void TxnCoordinator::onLsmReadSegmentsScanned(const std::uint64_t n) {
    m_lsm_read_segments_scanned.fetch_add(n, std::memory_order_relaxed);
    std::lock_guard<std::mutex> lk(m_samples_mu);
    m_lsm_read_segments_scanned_samples.push_back(n);
    if (m_lsm_read_segments_scanned_samples.size() > 256) {
        m_lsm_read_segments_scanned_samples.erase(m_lsm_read_segments_scanned_samples.begin(),
                                                  m_lsm_read_segments_scanned_samples.begin() + 64);
    }
}


void TxnCoordinator::onLsmCompactionBytes(const std::uint64_t bytes_in, const std::uint64_t bytes_out) {
    m_lsm_compaction_bytes_in.fetch_add(bytes_in, std::memory_order_relaxed);
    m_lsm_compaction_bytes_out.fetch_add(bytes_out, std::memory_order_relaxed);
}


void TxnCoordinator::onLsmCompactionQueueDepth(const std::uint64_t pending, const std::uint64_t inflight) {
    m_lsm_compaction_queue_pending.store(pending, std::memory_order_relaxed);
    m_lsm_compaction_queue_inflight.store(inflight, std::memory_order_relaxed);
}


void TxnCoordinator::onLsmCompactionEnqueueSkippedBackpressure() {
    m_lsm_compaction_enqueue_skipped_backpressure.fetch_add(1, std::memory_order_relaxed);
}


void TxnCoordinator::onLsmSegmentCacheLookup(const bool hit) {
    if (hit) {
        m_lsm_segment_cache_hits.fetch_add(1, std::memory_order_relaxed);
    } else {
        m_lsm_segment_cache_misses.fetch_add(1, std::memory_order_relaxed);
    }
}


void TxnCoordinator::onSchedulerThrottled() {
    m_scheduler_throttle_count.fetch_add(1, std::memory_order_relaxed);
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
    case WriteTimingStage::HeapAppend: bump_max(m_write_timing_heap_append_max_ms); break;
    case WriteTimingStage::HotIndex: bump_max(m_write_timing_hot_index_max_ms); break;
    case WriteTimingStage::SidecarInvalidate: bump_max(m_write_timing_sidecar_invalidate_max_ms); break;
    case WriteTimingStage::WalAppend: bump_max(m_write_timing_wal_append_max_ms); break;
    case WriteTimingStage::LsmTrack: bump_max(m_write_timing_lsm_track_max_ms); break;
    case WriteTimingStage::LsmFlush: bump_max(m_write_timing_lsm_flush_max_ms); break;
    case WriteTimingStage::LsmCompaction: bump_max(m_write_timing_lsm_compaction_max_ms); break;
    case WriteTimingStage::LsmRotateCompact: bump_max(m_write_timing_lsm_rotate_compact_max_ms); break;
    default: break;
    }

    const std::uint64_t every_n = m_write_timing_sample_every_n.load(std::memory_order_relaxed);
    const std::uint64_t k = m_write_timing_sample_counter.fetch_add(1, std::memory_order_relaxed) + 1;
    if (every_n == 0 || (k % every_n) != 0) {
        return;
    }
    std::lock_guard<std::mutex> lk(m_samples_mu);
    auto push_trim = [&](std::vector<std::uint64_t>& v) {
        v.push_back(elapsed_ms);
        if (v.size() > 256) {
            v.erase(v.begin(), v.begin() + 64);
        }
    };
    switch (stage) {
    case WriteTimingStage::HeapAppend: push_trim(m_write_timing_heap_append_samples); break;
    case WriteTimingStage::HotIndex: push_trim(m_write_timing_hot_index_samples); break;
    case WriteTimingStage::SidecarInvalidate: push_trim(m_write_timing_sidecar_invalidate_samples); break;
    case WriteTimingStage::WalAppend: push_trim(m_write_timing_wal_append_samples); break;
    case WriteTimingStage::LsmTrack: push_trim(m_write_timing_lsm_track_samples); break;
    case WriteTimingStage::LsmFlush: push_trim(m_write_timing_lsm_flush_samples); break;
    case WriteTimingStage::LsmCompaction: push_trim(m_write_timing_lsm_compaction_samples); break;
    case WriteTimingStage::LsmRotateCompact: push_trim(m_write_timing_lsm_rotate_compact_samples); break;
    default: break;
    }
}


TxnRuntimeStats TxnCoordinator::runtimeStats() const {
    TxnRuntimeStats s{};
    s.vacuum_trigger_count = m_vacuum_trigger_count.load(std::memory_order_relaxed);
    s.vacuum_execute_count = m_vacuum_execute_count.load(std::memory_order_relaxed);
    s.vacuum_cooldown_skip_count = m_vacuum_cooldown_skip_count.load(std::memory_order_relaxed);
    s.vacuum_compact_success_count = m_vacuum_compact_success_count.load(std::memory_order_relaxed);
    s.vacuum_compact_failure_count = m_vacuum_compact_failure_count.load(std::memory_order_relaxed);
    s.vacuum_compact_bytes_reclaimed = m_vacuum_compact_bytes_reclaimed.load(std::memory_order_relaxed);
    s.vacuum_compact_last_elapsed_ms = m_vacuum_compact_last_elapsed_ms.load(std::memory_order_relaxed);
    s.vacuum_queue_depth = m_vacuum_queue_depth.load(std::memory_order_relaxed);
    s.vacuum_queue_depth_peak = m_vacuum_queue_depth_peak.load(std::memory_order_relaxed);
    s.maintenance_checkpoint_trigger_count =
        m_maintenance_checkpoint_trigger_count.load(std::memory_order_relaxed);
    s.maintenance_checkpoint_vacuum_enqueue_count =
        m_maintenance_checkpoint_vacuum_enqueue_count.load(std::memory_order_relaxed);
    s.write_conflict_count = m_write_conflict_count.load(std::memory_order_relaxed);
    s.write_conflict_wait_count = m_write_conflict_wait_count.load(std::memory_order_relaxed);
    s.write_conflict_wait_timeout_count = m_write_conflict_wait_timeout_count.load(std::memory_order_relaxed);
    s.txn_begin_lock_conflict_count = m_txn_begin_lock_conflict_count.load(std::memory_order_relaxed);
    s.wal_compact_count = m_wal_compact_count.load(std::memory_order_relaxed);
    s.wal_recovery_runs = m_wal_recovery_runs.load(std::memory_order_relaxed);
    s.wal_recovery_undo_ops = m_wal_recovery_undo_ops.load(std::memory_order_relaxed);
    s.wal_recovery_last_elapsed_ms = m_wal_recovery_last_elapsed_ms.load(std::memory_order_relaxed);
    s.wal_recovery_analyze_ms = m_wal_recovery_analyze_ms.load(std::memory_order_relaxed);
    s.wal_recovery_undo_ms = m_wal_recovery_undo_ms.load(std::memory_order_relaxed);
    s.wal_recovery_finalize_ms = m_wal_recovery_finalize_ms.load(std::memory_order_relaxed);
    s.wal_recovery_records_scanned = m_wal_recovery_records_scanned.load(std::memory_order_relaxed);
    s.wal_recovery_dangling_txns = m_wal_recovery_dangling_txns.load(std::memory_order_relaxed);
    s.wal_group_commit_count = m_wal_group_commit_count.load(std::memory_order_relaxed);
    s.wal_group_commit_batch_commits = m_wal_group_commit_batch_commits.load(std::memory_order_relaxed);
    s.wal_group_commit_pending_commits = m_wal_group_commit_pending_commits.load(std::memory_order_relaxed);
    s.txn_commit_count = m_txn_commit_count.load(std::memory_order_relaxed);
    s.txn_commit_max_ms = m_txn_commit_max_ms.load(std::memory_order_relaxed);
    s.wal_bytes_since_start = m_wal_bytes_since_start.load(std::memory_order_relaxed);
    if (s.txn_commit_count > 0) {
        s.wal_bytes_per_commit_avg = s.wal_bytes_since_start / s.txn_commit_count;
    }
    s.lock_deadlock_detect_count = m_lock_deadlock_detect_count.load(std::memory_order_relaxed);
    s.lock_deadlock_victim_count = m_lock_deadlock_victim_count.load(std::memory_order_relaxed);
    s.lock_wait_ms_total = m_lock_wait_ms_total.load(std::memory_order_relaxed);
    s.lock_wait_max_ms = m_lock_wait_max_ms.load(std::memory_order_relaxed);
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
            std::lock_guard<std::mutex> lk(m_samples_mu);
            commits = m_commit_latency_ms_samples;
            waits = m_lock_wait_ms_samples;
            heap_app = m_write_timing_heap_append_samples;
            hot_idx = m_write_timing_hot_index_samples;
            sidecar = m_write_timing_sidecar_invalidate_samples;
            wal_app = m_write_timing_wal_append_samples;
            lsm_track = m_write_timing_lsm_track_samples;
            lsm_flush = m_write_timing_lsm_flush_samples;
            lsm_compaction = m_write_timing_lsm_compaction_samples;
            lsm_rot = m_write_timing_lsm_rotate_compact_samples;
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
    s.scheduler_throttle_count = m_scheduler_throttle_count.load(std::memory_order_relaxed);
    s.hot_index_enabled = m_hot_index_enabled.load(std::memory_order_relaxed);
    s.segment_target_bytes = m_segment_target_bytes.load(std::memory_order_relaxed);
    s.lsm_memtable_flush_count = m_lsm_memtable_flush_count.load(std::memory_order_relaxed);
    s.lsm_compaction_count = m_lsm_compaction_count.load(std::memory_order_relaxed);
    s.lsm_segment_count = m_lsm_segment_count.load(std::memory_order_relaxed);
    s.lsm_memtable_bytes = m_lsm_memtable_bytes.load(std::memory_order_relaxed);
    s.lsm_read_segments_scanned = m_lsm_read_segments_scanned.load(std::memory_order_relaxed);
    s.lsm_compaction_bytes_in = m_lsm_compaction_bytes_in.load(std::memory_order_relaxed);
    s.lsm_compaction_bytes_out = m_lsm_compaction_bytes_out.load(std::memory_order_relaxed);
    s.lsm_compaction_queue_pending = m_lsm_compaction_queue_pending.load(std::memory_order_relaxed);
    s.lsm_compaction_queue_inflight = m_lsm_compaction_queue_inflight.load(std::memory_order_relaxed);
    s.lsm_compaction_enqueue_skipped_backpressure =
        m_lsm_compaction_enqueue_skipped_backpressure.load(std::memory_order_relaxed);
    s.lsm_segment_cache_hits = m_lsm_segment_cache_hits.load(std::memory_order_relaxed);
    s.lsm_segment_cache_misses = m_lsm_segment_cache_misses.load(std::memory_order_relaxed);
    const auto comp_in = static_cast<double>(s.lsm_compaction_bytes_in);
    const auto comp_out = static_cast<double>(s.lsm_compaction_bytes_out);
    s.lsm_compaction_bytes_amp_efficiency_min_window = (comp_in > 0.0) ? (comp_out / comp_in) : 0.0;
    s.write_heap_append_max_ms = m_write_timing_heap_append_max_ms.load(std::memory_order_relaxed);
    s.write_hot_index_max_ms = m_write_timing_hot_index_max_ms.load(std::memory_order_relaxed);
    s.write_sidecar_invalidate_max_ms = m_write_timing_sidecar_invalidate_max_ms.load(std::memory_order_relaxed);
    s.write_wal_append_max_ms = m_write_timing_wal_append_max_ms.load(std::memory_order_relaxed);
    s.write_lsm_track_max_ms = m_write_timing_lsm_track_max_ms.load(std::memory_order_relaxed);
    s.write_lsm_flush_max_ms = m_write_timing_lsm_flush_max_ms.load(std::memory_order_relaxed);
    s.write_lsm_compaction_max_ms = m_write_timing_lsm_compaction_max_ms.load(std::memory_order_relaxed);
    s.write_lsm_rotate_compact_max_ms = m_write_timing_lsm_rotate_compact_max_ms.load(std::memory_order_relaxed);
    {
        std::vector<std::uint64_t> scans;
        {
            std::lock_guard<std::mutex> lk(m_samples_mu);
            scans = m_lsm_read_segments_scanned_samples;
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
    s.hybrid_mode_switch_count = m_hybrid_mode_switch_count.load(std::memory_order_relaxed);
    s.hybrid_mode = (m_hybrid_mode.load(std::memory_order_relaxed) == 0) ? "throughput_mode" : "durability_mode";
    {
        std::lock_guard<std::mutex> lk(m_hybrid_mu);
        s.hybrid_last_switch_reason = m_hybrid_last_switch_reason;
    }
    s.rollback_savepoint_count = m_rollback_savepoint_count.load(std::memory_order_relaxed);
    s.rollback_partial_ops = m_rollback_partial_ops.load(std::memory_order_relaxed);
    s.pitr_runs = m_pitr_runs.load(std::memory_order_relaxed);
    s.pitr_target_lsn = m_pitr_target_lsn.load(std::memory_order_relaxed);
    s.pitr_elapsed_ms = m_pitr_elapsed_ms.load(std::memory_order_relaxed);
    s.undo_chain_fallback_count = m_undo_chain_fallback_count.load(std::memory_order_relaxed);
    return s;
}


