#include "structdb/storage/storage_telemetry.hpp"

#include "structdb/storage/storage_engine.hpp"

#include <cstdint>
#include <mutex>
#include <shared_mutex>

#include "storage_engine_detail.hpp"

namespace sed = structdb::storage::storage_engine_detail;

namespace structdb::storage {

void StorageTelemetry::read_storage_pressure_snapshot(StoragePressureSnapshot* out) const {
  if (!out) return;
  *out = StoragePressureSnapshot{};
  std::shared_lock<std::shared_mutex> lk(engine_.mu_);
  out->l0_files = engine_.manifest_.l0_prefix_length();
  std::uint64_t wsz = sed::file_size_u64_or_zero(engine_.wal_.path());
  for (const auto& rel : engine_.wal_sealed_relative_) wsz += sed::file_size_u64_or_zero(engine_.dir_ / rel);
  out->wal_bytes = wsz;
  std::uint64_t usz = sed::file_size_u64_or_zero(engine_.dir_ / "undo.log");
  for (const auto& rel : engine_.undo_sealed_relative_) usz += sed::file_size_u64_or_zero(engine_.dir_ / rel);
  out->undo_bytes = usz;
  out->manifest_version = engine_.manifest_.version();
  out->pending_deferred_l0_compact = engine_.pending_deferred_l0_compact_;
  out->wal_append_record_calls_total = engine_.wal_append_record_calls_total_.load(std::memory_order_relaxed);
  out->wal_fsync_calls_total = engine_.wal_fsync_calls_total_.load(std::memory_order_relaxed);
  out->flush_memtable_success_total = engine_.flush_memtable_success_total_.load(std::memory_order_relaxed);
  out->checkpoint_success_total = engine_.checkpoint_success_total_.load(std::memory_order_relaxed);
  out->compaction_merge_success_total = engine_.compaction_merge_count_.load(std::memory_order_relaxed);
  out->compaction_worker_tasks_submitted_total =
      engine_.compaction_worker_tasks_submitted_total_.load(std::memory_order_relaxed);
  out->compaction_worker_tasks_completed_total =
      engine_.compaction_worker_tasks_completed_total_.load(std::memory_order_relaxed);
  out->wal_append_frame_bytes_committed_total = engine_.wal_.append_frame_bytes_committed_total();
  out->wal_append_throttle_sleep_ns_total = engine_.wal_.append_throttle_sleep_ns_total();
  out->compaction_merge_throttle_sleep_ns_total =
      engine_.compaction_merge_throttle_sleep_ns_total_.load(std::memory_order_relaxed);
  out->compaction_merge_byte_throttle_sleep_ns_total =
      engine_.compaction_merge_byte_throttle_sleep_ns_total_.load(std::memory_order_relaxed);
  {
    std::lock_guard<std::mutex> wlk(engine_.compaction_worker_mu_);
    out->compaction_worker_queue_depth = static_cast<std::uint32_t>(engine_.compaction_tasks_.size());
    out->compaction_worker_queue_cap =
        engine_.compaction_worker_joinable_ ? static_cast<std::uint32_t>(engine_.compaction_queue_cap_) : 0u;
  }
}

}  // namespace structdb::storage
