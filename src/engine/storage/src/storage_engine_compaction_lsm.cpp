#include "storage_engine_detail.hpp"
#include "structdb/storage/storage_engine.hpp"

#include "structdb/infra/tracer.hpp"
#include "structdb/infra/long_task_progress.hpp"
#include "structdb/storage/storage_trace.hpp"
#include "structdb/storage/wal.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <future>
#include <iomanip>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <shared_mutex>
#include <sstream>
#include <string_view>
#include <thread>
#include <vector>

namespace {
constexpr const char kMemFlushTempSst[] = "_structdb_memflush_tmp.sst";
}  // namespace

namespace sed = structdb::storage::storage_engine_detail;

namespace structdb::storage {
bool StorageEngine::flush_memtable(std::string* error_out) {
  infra::SpanGuard trace_flush(trace::kFlushMemtable, 0);
  if (!opened_) {
    if (error_out) *error_out = "not open";
    return false;
  }
  if (wal_archive_gc_after_flush_ && !wal_auto_trim_prefix_after_flush_) {
    if (error_out) *error_out = "wal_archive_gc_after_flush requires wal_auto_trim_prefix_after_flush";
    return false;
  }
  const auto tmp_path = dir_ / kMemFlushTempSst;
  std::shared_ptr<IMemTable> snap;
  std::uint64_t wal_at_flush = 0;
  {
    std::unique_lock<std::shared_mutex> lk(mu_);
    if (!mem_mgr_.begin_flush_move_active_to_frozen(error_out)) return false;
    wal_at_flush = sed::file_size_u64_or_zero(wal_.path());
    undo_stack_.clear();
    snap = mem_mgr_.frozen_flush();
  }
  std::string sst_err;
  if (!sed::write_sst_sorted_entries_from_for_each(
          tmp_path,
          [&snap](const std::function<bool(const std::string&, const std::string&)>& visitor) {
            return snap->for_each_sorted(visitor);
          },
          &sst_err)) {
    std::unique_lock<std::shared_mutex> lk(mu_);
    mem_mgr_.merge_frozen_into_active_and_clear();
    lk.unlock();
    std::error_code rm_ec;
    std::filesystem::remove(tmp_path, rm_ec);
    if (error_out) *error_out = sst_err.empty() ? "sst write" : sst_err;
    return false;
  }
  std::unique_lock<std::shared_mutex> lk(mu_);
  const std::uint64_t gen = manifest_.version() + 1;
  const std::string sst_name = std::string("L0-") + std::to_string(gen) + ".sst";
  const auto sst_path = dir_ / sst_name;
  {
    std::error_code ec;
    std::filesystem::remove(sst_path, ec);
    std::filesystem::rename(tmp_path, sst_path, ec);
    if (ec) {
      mem_mgr_.merge_frozen_into_active_and_clear();
      lk.unlock();
      std::error_code rm_ec;
      std::filesystem::remove(tmp_path, rm_ec);
      if (error_out) *error_out = ec.message();
      return false;
    }
  }
  manifest_.set_version(gen);
  manifest_.push_l0_sst(sst_name);
  if (!manifest_.save(dir_ / "MANIFEST")) {
    std::error_code rm_ec;
    std::filesystem::remove(sst_path, rm_ec);
    mem_mgr_.merge_frozen_into_active_and_clear();
    if (error_out) *error_out = "manifest save";
    return false;
  }
  mem_mgr_.clear_frozen_flush_only();
  lsm_.sync_from_manifest(manifest_);
  CheckpointState ck{};
  (void)ckpt_.read_latest(dir_, &ck, nullptr);
  ck.wal_offset = wal_at_flush;
  ck.redo_offset = sed::file_size_u64_or_zero(dir_ / "redo.log");
  ck.manifest_version = manifest_.version();
  checkpoint_undo_coordinator_.fill_checkpoint_undo_safe_prefix_unlocked(&ck);
  if (!ckpt_.write_rotating(dir_, ck, error_out)) {
    if (error_out && error_out->empty()) *error_out = "checkpoint write (flush)";
    return false;
  }
  {
    std::string seg_err;
    if (!wal_coordinator_.persist_segments_for_flush_unlocked(&seg_err)) {
      if (error_out) *error_out = seg_err;
      return false;
    }
    if (!checkpoint_undo_coordinator_.persist_undo_segments_for_flush_unlocked(&seg_err)) {
      if (error_out) *error_out = seg_err;
      return false;
    }
    if (wal_catalog_v2_) {
      wal_segment_count_observed_ = static_cast<std::uint32_t>(wal_sealed_relative_.size() + 1u);
    } else {
      wal_segment_count_observed_ = 1u;
    }
    if (undo_catalog_v2_) {
      undo_segment_count_observed_ = static_cast<std::uint32_t>(undo_sealed_relative_.size() + 1u);
    } else {
      undo_segment_count_observed_ = 1u;
    }
  }
  if (wal_auto_trim_prefix_after_flush_) {
    std::string terr;
    if (!wal_coordinator_.try_trim_prefix_through_checkpoint_unlocked(&terr)) {
      if (error_out) *error_out = terr;
      return false;
    }
  }
  if (wal_archive_gc_after_flush_) {
    std::string gerr;
    if (!wal_coordinator_.gc_sealed_archives_unlocked(&gerr)) {
      if (error_out) *error_out = gerr.empty() ? "wal archive gc after flush" : gerr;
      return false;
    }
  }
  if (undo_auto_truncate_after_flush_) {
    std::string uerr;
    if (!undo_.truncate_to_empty(&uerr)) {
      if (error_out) *error_out = uerr.empty() ? "undo truncate after flush" : uerr;
      return false;
    }
  }
  if (l0_compact_trigger_threshold_ > 0) {
    if (l0_compact_defer_after_flush_) {
      if (manifest_.l0_prefix_length() > static_cast<std::size_t>(l0_compact_trigger_threshold_)) {
        pending_deferred_l0_compact_ = true;
      }
    } else {
      pending_deferred_l0_compact_ = false;
      std::string cerr;
      if (!compaction_coordinator_.try_compact_l0_if_over_threshold(lk, &cerr)) {
        if (error_out) *error_out = cerr.empty() ? "l0 auto compact after flush" : cerr;
        return false;
      }
    }
  } else {
    pending_deferred_l0_compact_ = false;
  }
  std::string err;
  (void)persist_commit_seq_hw_(&err);
  flush_memtable_success_total_.fetch_add(1, std::memory_order_relaxed);
  return true;
}

void StorageEngine::read_storage_pressure_snapshot(StoragePressureSnapshot* out) const {
  storage_telemetry_.read_storage_pressure_snapshot(out);
}

bool StorageEngine::compact_merge_two_oldest_l0(std::string* error_out) {
  if (!opened_) {
    if (error_out) *error_out = "not open";
    return false;
  }
  std::unique_lock<std::shared_mutex> lk(mu_);
  return compaction_coordinator_.compact_merge_two_oldest_l0_with_relock(lk, error_out);
}

bool StorageEngine::compact_merge_two_oldest_l1_to_l2(std::string* error_out) {
  if (!opened_) {
    if (error_out) *error_out = "not open";
    return false;
  }
  if (!l2_compact_output_from_l1_merge_) {
    if (error_out) *error_out = "compact L1->L2 disabled (set_l2_compact_output_from_l1_merge)";
    return false;
  }
  std::unique_lock<std::shared_mutex> lk(mu_);
  return compaction_coordinator_.compact_merge_two_oldest_l1_to_l2_with_relock(lk, error_out);
}

bool StorageEngine::compact_merge_two_oldest_l2_to_l3(std::string* error_out) {
  if (!opened_) {
    if (error_out) *error_out = "not open";
    return false;
  }
  if (!l3_compact_output_from_l2_merge_) {
    if (error_out) *error_out = "compact L2->L3 disabled (set_l3_compact_output_from_l2_merge)";
    return false;
  }
  std::unique_lock<std::shared_mutex> lk(mu_);
  return compaction_coordinator_.compact_merge_two_oldest_l2_to_l3_with_relock(lk, error_out);
}

bool StorageEngine::compact_merge_two_oldest_l3_to_l4(std::string* error_out) {
  if (!opened_) {
    if (error_out) *error_out = "not open";
    return false;
  }
  if (!l4_compact_output_from_l3_merge_) {
    if (error_out) *error_out = "compact L3->L4 disabled (set_l4_compact_output_from_l3_merge)";
    return false;
  }
  std::unique_lock<std::shared_mutex> lk(mu_);
  return compaction_coordinator_.compact_merge_two_oldest_l3_to_l4_with_relock(lk, error_out);
}

bool StorageEngine::drain_pending_l0_compactions(std::uint32_t max_rounds, std::string* error_out) {
  infra::SpanGuard trace_drain(trace::kDrainL0Compactions, 0);
  if (!opened_) {
    if (error_out) *error_out = "not open";
    return false;
  }
  std::unique_lock<std::shared_mutex> lk(mu_);
  if (!pending_deferred_l0_compact_) return true;
  const std::uint32_t cap = l0_compact_trigger_threshold_;
  if (cap == 0) {
    pending_deferred_l0_compact_ = false;
    return true;
  }
  std::uint32_t rounds = 0;
  while (rounds < max_rounds && manifest_.l0_prefix_length() > static_cast<std::size_t>(cap)) {
    if (manifest_.l0_prefix_length() < 2) break;
    report_active_long_task_(infra::LongTaskKind::CompactionFlush, infra::LongTaskStatus::Running, rounds, max_rounds,
                             "compaction_l0_drain round");
    if (active_long_task() && active_long_task()->poll_cancel_and_report_cancelling()) {
      if (error_out) *error_out = "compact: cancelled";
      return false;
    }
    if (!compaction_coordinator_.compact_merge_two_oldest_l0_with_relock(lk, error_out)) return false;
    ++rounds;
  }
  if (manifest_.l0_prefix_length() <= static_cast<std::size_t>(cap) || manifest_.l0_prefix_length() < 2) {
    pending_deferred_l0_compact_ = false;
  }
  report_active_long_task_(infra::LongTaskKind::CompactionFlush, infra::LongTaskStatus::Completed, rounds,
                           max_rounds > 0 ? max_rounds : 1, "compaction_l0_drain done");
  return true;
}


}  // namespace structdb::storage

