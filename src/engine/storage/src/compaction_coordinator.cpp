#include "structdb/storage/compaction_coordinator.hpp"

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

#include "storage_engine_detail.hpp"

namespace sed = structdb::storage::storage_engine_detail;

namespace {

using structdb::infra::LongTaskKind;
using structdb::infra::LongTaskReporter;
using structdb::infra::LongTaskStatus;

class CompactionMergeLongTask {
 public:
  explicit CompactionMergeLongTask(structdb::storage::StorageEngine& eng, std::string detail)
      : lt_(eng.active_long_task()) {
    if (!lt_) return;
    lt_->set_kind(LongTaskKind::CompactionMerge);
    lt_->set_detail(std::move(detail));
    lt_->report(LongTaskStatus::Running, 0, kPhaseTotal);
  }

  bool cancelled(std::string* error_out) {
    if (!lt_ || !lt_->poll_cancel_and_report_cancelling()) return false;
    if (error_out) *error_out = "compact: cancelled";
    lt_->report(LongTaskStatus::Cancelled, phase_done_, kPhaseTotal);
    return true;
  }

  void phase(std::uint64_t done) {
    phase_done_ = done;
    if (lt_) lt_->report(LongTaskStatus::Running, done, kPhaseTotal);
  }

  void note_io_bytes(std::size_t n) {
    if (!lt_) return;
    bytes_done_ += static_cast<std::uint64_t>(n);
    lt_->report_bytes(LongTaskStatus::Running, bytes_done_);
  }

  void complete() {
    if (lt_) lt_->report(LongTaskStatus::Completed, kPhaseTotal, kPhaseTotal);
  }

  void fail() {
    if (lt_) lt_->report(LongTaskStatus::Failed, phase_done_, kPhaseTotal);
  }

 private:
  static constexpr std::uint64_t kPhaseTotal = 4;
  LongTaskReporter* lt_{nullptr};
  std::uint64_t phase_done_{0};
  std::uint64_t bytes_done_{0};
};

}  // namespace

namespace structdb::storage {
bool CompactionCoordinator::capture_l0_merge_snapshot_unlocked(CompactionCoordinator::L0MergePrepareSnapshot* out, std::string* error_out) const {
  if (!out) {
    if (error_out) *error_out = "compact: internal (null plan)";
    return false;
  }
  const auto saved_entries = engine_.manifest_.sst_entries();
  if (saved_entries.size() < 2) {
    if (error_out) *error_out = "compact: need at least two L0 SSTs";
    return false;
  }
  if (saved_entries[0].level != 0 || saved_entries[1].level != 0) {
    if (error_out) *error_out = "compact: first two manifest SSTs must be L0";
    return false;
  }
  out->base_manifest_version = engine_.manifest_.version();
  out->s0_rel = saved_entries[0].relative_path;
  out->s1_rel = saved_entries[1].relative_path;
  out->l1_output = engine_.l1_compact_output_from_l0_merge_;
  const std::uint64_t gen = out->base_manifest_version + 1;
  out->out_basename =
      out->l1_output ? (std::string("L1-") + std::to_string(gen) + ".sst") : (std::string("L0-") + std::to_string(gen) + ".sst");
  return true;
}

bool CompactionCoordinator::materialize_l0_merge_to_temp_file(const CompactionCoordinator::L0MergePrepareSnapshot& snap, std::filesystem::path* temp_path_out,
                                                       std::string* error_out) {
  ensure_compaction_io_executor();
  if (engine_.compaction_dedicated_io_executor_ && engine_.compaction_io_executor_) {
    const bool ok = engine_.compaction_io_executor_->run_sync([this, &snap, temp_path_out, error_out]() {
      return materialize_l0_merge_to_temp_file_impl(snap, temp_path_out, error_out);
    });
    if (!ok && error_out && error_out->empty()) *error_out = "compact: dedicated compaction I/O executor unavailable";
    return ok;
  }
  return materialize_l0_merge_to_temp_file_impl(snap, temp_path_out, error_out);
}

bool CompactionCoordinator::materialize_l0_merge_to_temp_file_impl(const CompactionCoordinator::L0MergePrepareSnapshot& snap, std::filesystem::path* temp_path_out,
                                                            std::string* error_out) {
  if (!temp_path_out) {
    if (error_out) *error_out = "compact: internal (null temp path)";
    return false;
  }
  CompactionMergeLongTask task(engine_, "L0 merge " + snap.s0_rel + " + " + snap.s1_rel);
  if (task.cancelled(error_out)) return false;

  const auto p0 = engine_.dir_ / snap.s0_rel;
  const auto p1 = engine_.dir_ / snap.s1_rel;
  const bool seq = engine_.compaction_sequential_sst_read_;
  const std::size_t chunk = effective_io_chunk_bytes();
  CompactionMergeLongTask* task_ptr = &task;
  std::function<void(std::size_t)> on_read = [this, task_ptr](std::size_t n) {
    engine_.compaction_merge_byte_tb_.throttle(static_cast<std::uint64_t>(n),
                                               &engine_.compaction_merge_byte_throttle_sleep_ns_total_);
    task_ptr->note_io_bytes(n);
  };
  std::vector<std::pair<std::string, std::string>> e0, e1;
  std::string err;
  if (engine_.compaction_parallel_sst_reads_) {
    std::string err1;
    auto fut1 = std::async(std::launch::async, [this, p1, seq, chunk, on_read, &e1, &err1]() -> bool {
      std::string er;
      if (!sed::sst_load_all_entries(p1, &e1, &er, seq, chunk, on_read)) {
        err1 = std::move(er);
        return false;
      }
      return true;
    });
    if (!sed::sst_load_all_entries(p0, &e0, &err, seq, chunk, on_read)) {
      (void)fut1.get();
      if (error_out) *error_out = err;
      task.fail();
      return false;
    }
    task.phase(1);
    if (task.cancelled(error_out)) return false;
    if (!fut1.get()) {
      if (error_out) *error_out = err1.empty() ? "sst load secondary" : err1;
      task.fail();
      return false;
    }
  } else {
    if (!sed::sst_load_all_entries(p0, &e0, &err, seq, chunk, on_read)) {
      if (error_out) *error_out = err;
      task.fail();
      return false;
    }
    task.phase(1);
    if (task.cancelled(error_out)) return false;
    if (!sed::sst_load_all_entries(p1, &e1, &err, seq, chunk, on_read)) {
      if (error_out) *error_out = err;
      task.fail();
      return false;
    }
  }
  task.phase(2);
  if (task.cancelled(error_out)) return false;
  std::map<std::string, std::string> merged;
  for (auto& pr : e0) merged[std::move(pr.first)] = std::move(pr.second);
  for (auto& pr : e1) merged[std::move(pr.first)] = std::move(pr.second);

  task.phase(3);
  if (task.cancelled(error_out)) return false;
  std::function<void(std::size_t)> on_write = [this, task_ptr](std::size_t n) {
    engine_.compaction_merge_byte_tb_.throttle(static_cast<std::uint64_t>(n),
                                               &engine_.compaction_merge_byte_throttle_sleep_ns_total_);
    task_ptr->note_io_bytes(n);
  };

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<unsigned> dist(0, 0xFFFFFFFFu);
  std::filesystem::path tmp;
  for (int attempt = 0; attempt < 8; ++attempt) {
    tmp = engine_.dir_ / ("_tmp_l0_compact_" + std::to_string(snap.base_manifest_version) + "_" + std::to_string(dist(gen)) + ".sst");
    std::error_code ec;
    if (std::filesystem::exists(tmp, ec)) continue;
    if (sed::write_sst_sorted_entries(tmp, merged, &err, chunk, on_write)) {
      *temp_path_out = std::move(tmp);
      task.complete();
      return true;
    }
    if (error_out) *error_out = err;
    std::filesystem::remove(tmp, ec);
    task.fail();
    return false;
  }
  if (error_out) *error_out = "compact: could not allocate unique temp sst name";
  task.fail();
  return false;
}

bool CompactionCoordinator::commit_l0_merge_from_temp_unlocked(const CompactionCoordinator::L0MergePrepareSnapshot& snap, const std::filesystem::path& temp_path,
                                                        std::string* error_out) {
  const auto saved_entries = engine_.manifest_.sst_entries();
  if (engine_.manifest_.version() != snap.base_manifest_version) {
    if (error_out) *error_out = "compact: manifest changed during merge (retry)";
    return false;
  }
  if (saved_entries.size() < 2 || saved_entries[0].level != 0 || saved_entries[1].level != 0 ||
      saved_entries[0].relative_path != snap.s0_rel || saved_entries[1].relative_path != snap.s1_rel) {
    if (error_out) *error_out = "compact: L0 head changed during merge (retry)";
    return false;
  }
  const auto final_path = engine_.dir_ / snap.out_basename;
  std::error_code ec;
  std::filesystem::rename(temp_path, final_path, ec);
  if (ec) {
    if (error_out) *error_out = "compact: rename temp sst: " + ec.message();
    return false;
  }

  std::vector<ManifestSst> new_entries;
  new_entries.reserve(saved_entries.size() - 1);
  if (!snap.l1_output) {
    new_entries.push_back(ManifestSst{0, snap.out_basename});
    for (std::size_t i = 2; i < saved_entries.size(); ++i) new_entries.push_back(saved_entries[i]);
  } else {
    for (std::size_t i = 2; i < saved_entries.size(); ++i) new_entries.push_back(saved_entries[i]);
    new_entries.push_back(ManifestSst{1, snap.out_basename});
  }

  const std::uint64_t gen = snap.base_manifest_version + 1;
  const std::uint64_t prev_ver = engine_.manifest_.version();
  engine_.manifest_.set_version(gen);
  engine_.manifest_.set_sst_entries(std::move(new_entries));
  if (!engine_.manifest_.save(engine_.dir_ / "MANIFEST")) {
    engine_.manifest_.set_version(prev_ver);
    engine_.manifest_.set_sst_entries(std::vector<ManifestSst>(saved_entries));
    if (error_out) *error_out = "manifest save (compact)";
    std::filesystem::remove(final_path, ec);
    return false;
  }

  const auto p0 = engine_.dir_ / snap.s0_rel;
  const auto p1 = engine_.dir_ / snap.s1_rel;
  std::filesystem::remove(p0, ec);
  std::filesystem::remove(p1, ec);

  engine_.lsm_.sync_from_manifest(engine_.manifest_);
  CheckpointState ck{};
  (void)engine_.ckpt_.read_latest(engine_.dir_, &ck, nullptr);
  ck.wal_offset = sed::file_size_u64_or_zero(engine_.wal_.path());
  ck.redo_offset = sed::file_size_u64_or_zero(engine_.dir_ / "redo.log");
  ck.manifest_version = engine_.manifest_.version();
  engine_.checkpoint_undo_coordinator_.fill_checkpoint_undo_safe_prefix_unlocked(&ck);
  if (!engine_.ckpt_.write_rotating(engine_.dir_, ck, error_out)) {
    if (error_out && error_out->empty()) *error_out = "checkpoint write (compact)";
    return false;
  }
  {
    std::string seg_err;
    if (!engine_.wal_coordinator_.persist_segments_for_flush_unlocked(&seg_err)) {
      if (error_out) *error_out = seg_err;
      return false;
    }
    if (!engine_.checkpoint_undo_coordinator_.persist_undo_segments_for_flush_unlocked(&seg_err)) {
      if (error_out) *error_out = seg_err;
      return false;
    }
    if (engine_.wal_catalog_v2_) {
      engine_.wal_segment_count_observed_ = static_cast<std::uint32_t>(engine_.wal_sealed_relative_.size() + 1u);
    } else {
      engine_.wal_segment_count_observed_ = 1u;
    }
    if (engine_.undo_catalog_v2_) {
      engine_.undo_segment_count_observed_ = static_cast<std::uint32_t>(engine_.undo_sealed_relative_.size() + 1u);
    } else {
      engine_.undo_segment_count_observed_ = 1u;
    }
  }
  engine_.compaction_merge_count_.fetch_add(1, std::memory_order_relaxed);
  std::string perr;
  (void)engine_.persist_commit_seq_hw_(&perr);
  return true;
}

bool CompactionCoordinator::compact_merge_two_oldest_l0_with_relock(std::unique_lock<std::shared_mutex>& lk, std::string* error_out) {
  infra::SpanGuard trace_l0_merge(trace::kCompactMergeL0, 0);
  lk.unlock();
  throttle_before_l0_merge_if_configured();
  lk.lock();
  CompactionCoordinator::L0MergePrepareSnapshot snap;
  if (!capture_l0_merge_snapshot_unlocked(&snap, error_out)) return false;
  std::filesystem::path tmp;
  lk.unlock();
  const bool prep_ok = materialize_l0_merge_to_temp_file(snap, &tmp, error_out);
  lk.lock();
  if (!prep_ok) {
    std::error_code ec;
    if (!tmp.empty()) std::filesystem::remove(tmp, ec);
    return false;
  }
  if (!commit_l0_merge_from_temp_unlocked(snap, tmp, error_out)) {
    std::error_code ec;
    std::filesystem::remove(tmp, ec);
    return false;
  }
  mark_after_successful_l0_merge_for_throttle();
  return true;
}

bool CompactionCoordinator::try_compact_l0_if_over_threshold(std::unique_lock<std::shared_mutex>& lk, std::string* error_out) {
  const std::uint32_t cap = engine_.l0_compact_trigger_threshold_;
  if (cap == 0) return true;
  const std::uint32_t maxr = engine_.l0_compact_max_rounds_per_flush_ == 0 ? 4u : engine_.l0_compact_max_rounds_per_flush_;
  const std::uint32_t eff_max =
      engine_.l0_compact_max_inline_rounds_per_flush_ > 0 ? (std::min)(maxr, engine_.l0_compact_max_inline_rounds_per_flush_) : maxr;
  std::uint32_t rounds = 0;
  while (engine_.manifest_.l0_prefix_length() > static_cast<std::size_t>(cap) && rounds < eff_max) {
    if (engine_.manifest_.l0_prefix_length() < 2) break;
    if (!compact_merge_two_oldest_l0_with_relock(lk, error_out)) return false;
    ++rounds;
  }
  return true;
}

bool CompactionCoordinator::capture_l1_l2_merge_snapshot_unlocked(CompactionCoordinator::TieredPairMergeSnapshot* out, std::string* error_out) const {
  if (!out) {
    if (error_out) *error_out = "compact: internal (null tiered plan)";
    return false;
  }
  const auto saved_entries = engine_.manifest_.sst_entries();
  std::size_t first_l1 = 0;
  while (first_l1 < saved_entries.size() && saved_entries[first_l1].level == 0) ++first_l1;
  if (first_l1 + 2 > saved_entries.size()) {
    if (error_out) *error_out = "compact L1: need at least two L1 SSTs";
    return false;
  }
  if (saved_entries[first_l1].level != 1 || saved_entries[first_l1 + 1].level != 1) {
    if (error_out) *error_out = "compact L1: first two L1-block SSTs must be level 1";
    return false;
  }
  out->base_manifest_version = engine_.manifest_.version();
  out->first_idx = first_l1;
  out->s0_rel = saved_entries[first_l1].relative_path;
  out->s1_rel = saved_entries[first_l1 + 1].relative_path;
  out->src_level = 1;
  out->dst_level = 2;
  out->out_basename = std::string("L2-") + std::to_string(out->base_manifest_version + 1) + ".sst";
  return true;
}

bool CompactionCoordinator::capture_l2_l3_merge_snapshot_unlocked(CompactionCoordinator::TieredPairMergeSnapshot* out, std::string* error_out) const {
  if (!out) {
    if (error_out) *error_out = "compact: internal (null tiered plan)";
    return false;
  }
  const auto saved_entries = engine_.manifest_.sst_entries();
  std::size_t first_l2 = 0;
  while (first_l2 < saved_entries.size() && saved_entries[first_l2].level < 2) ++first_l2;
  if (first_l2 + 2 > saved_entries.size()) {
    if (error_out) *error_out = "compact L2: need at least two L2 SSTs";
    return false;
  }
  if (saved_entries[first_l2].level != 2 || saved_entries[first_l2 + 1].level != 2) {
    if (error_out) *error_out = "compact L2: first two L2-block SSTs must be level 2";
    return false;
  }
  out->base_manifest_version = engine_.manifest_.version();
  out->first_idx = first_l2;
  out->s0_rel = saved_entries[first_l2].relative_path;
  out->s1_rel = saved_entries[first_l2 + 1].relative_path;
  out->src_level = 2;
  out->dst_level = 3;
  out->out_basename = std::string("L3-") + std::to_string(out->base_manifest_version + 1) + ".sst";
  return true;
}

bool CompactionCoordinator::capture_l3_l4_merge_snapshot_unlocked(CompactionCoordinator::TieredPairMergeSnapshot* out, std::string* error_out) const {
  if (!out) {
    if (error_out) *error_out = "compact: internal (null tiered plan)";
    return false;
  }
  const auto saved_entries = engine_.manifest_.sst_entries();
  std::size_t first_l3 = 0;
  while (first_l3 < saved_entries.size() && saved_entries[first_l3].level < 3) ++first_l3;
  if (first_l3 + 2 > saved_entries.size()) {
    if (error_out) *error_out = "compact L3: need at least two L3 SSTs";
    return false;
  }
  if (saved_entries[first_l3].level != 3 || saved_entries[first_l3 + 1].level != 3) {
    if (error_out) *error_out = "compact L3: first two L3-block SSTs must be level 3";
    return false;
  }
  out->base_manifest_version = engine_.manifest_.version();
  out->first_idx = first_l3;
  out->s0_rel = saved_entries[first_l3].relative_path;
  out->s1_rel = saved_entries[first_l3 + 1].relative_path;
  out->src_level = 3;
  out->dst_level = 4;
  out->out_basename = std::string("L4-") + std::to_string(out->base_manifest_version + 1) + ".sst";
  return true;
}

bool CompactionCoordinator::materialize_tiered_pair_merge_to_temp(const CompactionCoordinator::TieredPairMergeSnapshot& snap, std::filesystem::path* temp_path_out,
                                                           std::string* error_out) {
  ensure_compaction_io_executor();
  if (engine_.compaction_dedicated_io_executor_ && engine_.compaction_io_executor_) {
    const bool ok = engine_.compaction_io_executor_->run_sync([this, &snap, temp_path_out, error_out]() {
      return materialize_tiered_pair_merge_to_temp_impl(snap, temp_path_out, error_out);
    });
    if (!ok && error_out && error_out->empty()) *error_out = "compact: dedicated compaction I/O executor unavailable";
    return ok;
  }
  return materialize_tiered_pair_merge_to_temp_impl(snap, temp_path_out, error_out);
}

bool CompactionCoordinator::materialize_tiered_pair_merge_to_temp_impl(const CompactionCoordinator::TieredPairMergeSnapshot& snap, std::filesystem::path* temp_path_out,
                                                                std::string* error_out) {
  if (!temp_path_out) {
    if (error_out) *error_out = "compact: internal (null temp path)";
    return false;
  }
  CompactionMergeLongTask task(engine_, "L" + std::to_string(snap.src_level) + " merge " + snap.s0_rel + " + " + snap.s1_rel);
  if (task.cancelled(error_out)) return false;

  const auto p0 = engine_.dir_ / snap.s0_rel;
  const auto p1 = engine_.dir_ / snap.s1_rel;
  const bool seq = engine_.compaction_sequential_sst_read_;
  const std::size_t chunk = effective_io_chunk_bytes();
  CompactionMergeLongTask* task_ptr = &task;
  std::function<void(std::size_t)> on_read = [this, task_ptr](std::size_t n) {
    engine_.compaction_merge_byte_tb_.throttle(static_cast<std::uint64_t>(n),
                                               &engine_.compaction_merge_byte_throttle_sleep_ns_total_);
    task_ptr->note_io_bytes(n);
  };
  std::vector<std::pair<std::string, std::string>> e0, e1;
  std::string err;
  if (engine_.compaction_parallel_sst_reads_) {
    std::string err1;
    auto fut1 = std::async(std::launch::async, [this, p1, seq, chunk, on_read, &e1, &err1]() -> bool {
      std::string er;
      if (!sed::sst_load_all_entries(p1, &e1, &er, seq, chunk, on_read)) {
        err1 = std::move(er);
        return false;
      }
      return true;
    });
    if (!sed::sst_load_all_entries(p0, &e0, &err, seq, chunk, on_read)) {
      (void)fut1.get();
      if (error_out) *error_out = err;
      task.fail();
      return false;
    }
    task.phase(1);
    if (task.cancelled(error_out)) return false;
    if (!fut1.get()) {
      if (error_out) *error_out = err1.empty() ? "sst load secondary" : err1;
      task.fail();
      return false;
    }
  } else {
    if (!sed::sst_load_all_entries(p0, &e0, &err, seq, chunk, on_read)) {
      if (error_out) *error_out = err;
      task.fail();
      return false;
    }
    task.phase(1);
    if (task.cancelled(error_out)) return false;
    if (!sed::sst_load_all_entries(p1, &e1, &err, seq, chunk, on_read)) {
      if (error_out) *error_out = err;
      task.fail();
      return false;
    }
  }
  task.phase(2);
  if (task.cancelled(error_out)) return false;
  std::map<std::string, std::string> merged;
  for (auto& pr : e0) merged[std::move(pr.first)] = std::move(pr.second);
  for (auto& pr : e1) merged[std::move(pr.first)] = std::move(pr.second);

  task.phase(3);
  if (task.cancelled(error_out)) return false;
  std::function<void(std::size_t)> on_write = [this, task_ptr](std::size_t n) {
    engine_.compaction_merge_byte_tb_.throttle(static_cast<std::uint64_t>(n),
                                               &engine_.compaction_merge_byte_throttle_sleep_ns_total_);
    task_ptr->note_io_bytes(n);
  };

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<unsigned> dist(0, 0xFFFFFFFFu);
  std::filesystem::path tmp;
  for (int attempt = 0; attempt < 8; ++attempt) {
    tmp = engine_.dir_ / ("_tmp_tier_compact_" + std::to_string(snap.src_level) + "_" + std::to_string(snap.base_manifest_version) +
                  "_" + std::to_string(dist(gen)) + ".sst");
    std::error_code ec;
    if (std::filesystem::exists(tmp, ec)) continue;
    if (sed::write_sst_sorted_entries(tmp, merged, &err, chunk, on_write)) {
      *temp_path_out = std::move(tmp);
      task.complete();
      return true;
    }
    if (error_out) *error_out = err;
    std::filesystem::remove(tmp, ec);
    task.fail();
    return false;
  }
  if (error_out) *error_out = "compact: could not allocate unique temp sst name (tiered)";
  task.fail();
  return false;
}

bool CompactionCoordinator::commit_tiered_pair_merge_from_temp_unlocked(const CompactionCoordinator::TieredPairMergeSnapshot& snap,
                                                                const std::filesystem::path& temp_path,
                                                                std::string* error_out) {
  const auto saved_entries = engine_.manifest_.sst_entries();
  if (engine_.manifest_.version() != snap.base_manifest_version) {
    if (error_out) *error_out = "compact: manifest changed during merge (retry)";
    return false;
  }
  if (snap.first_idx + 2 > saved_entries.size()) {
    if (error_out) *error_out = "compact: manifest shrunk during merge (retry)";
    return false;
  }
  if (saved_entries[snap.first_idx].level != snap.src_level || saved_entries[snap.first_idx + 1].level != snap.src_level ||
      saved_entries[snap.first_idx].relative_path != snap.s0_rel ||
      saved_entries[snap.first_idx + 1].relative_path != snap.s1_rel) {
    if (error_out) *error_out = "compact: tiered pair head changed during merge (retry)";
    return false;
  }

  const auto final_path = engine_.dir_ / snap.out_basename;
  std::error_code ec;
  std::filesystem::rename(temp_path, final_path, ec);
  if (ec) {
    if (error_out) *error_out = "compact: rename temp sst (tiered): " + ec.message();
    return false;
  }

  std::vector<ManifestSst> new_entries;
  new_entries.reserve(saved_entries.size() - 1);
  for (std::size_t i = 0; i < snap.first_idx; ++i) new_entries.push_back(saved_entries[i]);
  for (std::size_t i = snap.first_idx + 2; i < saved_entries.size(); ++i) new_entries.push_back(saved_entries[i]);
  new_entries.push_back(ManifestSst{static_cast<std::uint8_t>(snap.dst_level), snap.out_basename});

  const std::uint64_t prev_ver = engine_.manifest_.version();
  engine_.manifest_.set_version(snap.base_manifest_version + 1);
  engine_.manifest_.set_sst_entries(std::move(new_entries));
  const char* merr = "manifest save (compact tiered)";
  if (snap.dst_level == 2) merr = "manifest save (compact L1)";
  if (snap.dst_level == 3) merr = "manifest save (compact L2)";
  if (snap.dst_level == 4) merr = "manifest save (compact L3)";
  if (!engine_.manifest_.save(engine_.dir_ / "MANIFEST")) {
    engine_.manifest_.set_version(prev_ver);
    engine_.manifest_.set_sst_entries(std::vector<ManifestSst>(saved_entries));
    if (error_out) *error_out = merr;
    std::filesystem::remove(final_path, ec);
    return false;
  }

  const auto p0 = engine_.dir_ / snap.s0_rel;
  const auto p1 = engine_.dir_ / snap.s1_rel;
  std::filesystem::remove(p0, ec);
  std::filesystem::remove(p1, ec);

  engine_.lsm_.sync_from_manifest(engine_.manifest_);
  CheckpointState ck{};
  (void)engine_.ckpt_.read_latest(engine_.dir_, &ck, nullptr);
  ck.wal_offset = sed::file_size_u64_or_zero(engine_.wal_.path());
  ck.redo_offset = sed::file_size_u64_or_zero(engine_.dir_ / "redo.log");
  ck.manifest_version = engine_.manifest_.version();
  engine_.checkpoint_undo_coordinator_.fill_checkpoint_undo_safe_prefix_unlocked(&ck);
  const char* cerr = "checkpoint write (compact tiered)";
  if (snap.dst_level == 2) cerr = "checkpoint write (compact L1)";
  if (snap.dst_level == 3) cerr = "checkpoint write (compact L2->L3)";
  if (snap.dst_level == 4) cerr = "checkpoint write (compact L3->L4)";
  if (!engine_.ckpt_.write_rotating(engine_.dir_, ck, error_out)) {
    if (error_out && error_out->empty()) *error_out = cerr;
    return false;
  }
  {
    std::string seg_err;
    if (!engine_.wal_coordinator_.persist_segments_for_flush_unlocked(&seg_err)) {
      if (error_out) *error_out = seg_err;
      return false;
    }
    if (!engine_.checkpoint_undo_coordinator_.persist_undo_segments_for_flush_unlocked(&seg_err)) {
      if (error_out) *error_out = seg_err;
      return false;
    }
    if (engine_.wal_catalog_v2_) {
      engine_.wal_segment_count_observed_ = static_cast<std::uint32_t>(engine_.wal_sealed_relative_.size() + 1u);
    } else {
      engine_.wal_segment_count_observed_ = 1u;
    }
    if (engine_.undo_catalog_v2_) {
      engine_.undo_segment_count_observed_ = static_cast<std::uint32_t>(engine_.undo_sealed_relative_.size() + 1u);
    } else {
      engine_.undo_segment_count_observed_ = 1u;
    }
  }
  engine_.compaction_merge_count_.fetch_add(1, std::memory_order_relaxed);
  std::string perr;
  (void)engine_.persist_commit_seq_hw_(&perr);
  return true;
}

bool CompactionCoordinator::compact_merge_two_oldest_l1_to_l2_with_relock(std::unique_lock<std::shared_mutex>& lk, std::string* error_out) {
    infra::SpanGuard trace_tier(trace::kCompactMergeL1ToL2, 0);
  CompactionCoordinator::TieredPairMergeSnapshot snap;
  if (!capture_l1_l2_merge_snapshot_unlocked(&snap, error_out)) return false;
  std::filesystem::path tmp;
  lk.unlock();
  const bool prep_ok = materialize_tiered_pair_merge_to_temp(snap, &tmp, error_out);
  lk.lock();
  if (!prep_ok) {
    std::error_code ec;
    if (!tmp.empty()) std::filesystem::remove(tmp, ec);
    return false;
  }
  if (!commit_tiered_pair_merge_from_temp_unlocked(snap, tmp, error_out)) {
    std::error_code ec;
    std::filesystem::remove(tmp, ec);
    return false;
  }
  return true;
}

bool CompactionCoordinator::compact_merge_two_oldest_l2_to_l3_with_relock(std::unique_lock<std::shared_mutex>& lk, std::string* error_out) {
    infra::SpanGuard trace_tier(trace::kCompactMergeL2ToL3, 0);
  CompactionCoordinator::TieredPairMergeSnapshot snap;
  if (!capture_l2_l3_merge_snapshot_unlocked(&snap, error_out)) return false;
  std::filesystem::path tmp;
  lk.unlock();
  const bool prep_ok = materialize_tiered_pair_merge_to_temp(snap, &tmp, error_out);
  lk.lock();
  if (!prep_ok) {
    std::error_code ec;
    if (!tmp.empty()) std::filesystem::remove(tmp, ec);
    return false;
  }
  if (!commit_tiered_pair_merge_from_temp_unlocked(snap, tmp, error_out)) {
    std::error_code ec;
    std::filesystem::remove(tmp, ec);
    return false;
  }
  return true;
}

bool CompactionCoordinator::compact_merge_two_oldest_l3_to_l4_with_relock(std::unique_lock<std::shared_mutex>& lk, std::string* error_out) {
    infra::SpanGuard trace_tier(trace::kCompactMergeL3ToL4, 0);
  CompactionCoordinator::TieredPairMergeSnapshot snap;
  if (!capture_l3_l4_merge_snapshot_unlocked(&snap, error_out)) return false;
  std::filesystem::path tmp;
  lk.unlock();
  const bool prep_ok = materialize_tiered_pair_merge_to_temp(snap, &tmp, error_out);
  lk.lock();
  if (!prep_ok) {
    std::error_code ec;
    if (!tmp.empty()) std::filesystem::remove(tmp, ec);
    return false;
  }
  if (!commit_tiered_pair_merge_from_temp_unlocked(snap, tmp, error_out)) {
    std::error_code ec;
    std::filesystem::remove(tmp, ec);
    return false;
  }
  return true;
}
void CompactionCoordinator::ensure_compaction_io_executor() {
  if (!engine_.compaction_dedicated_io_executor_ || engine_.compaction_io_executor_) return;
  engine_.compaction_io_executor_ = std::make_unique<CompactionIoExecutor>();
  std::size_t nw = static_cast<std::size_t>(engine_.compaction_io_pool_threads_);
  if (nw == 0) nw = 2;
  if (nw > 32) nw = 32;
  engine_.compaction_io_executor_->start(nw, engine_.compaction_worker_low_priority_thread_);
}

void CompactionCoordinator::shutdown_compaction_io_executor() {
  if (!engine_.compaction_io_executor_) return;
  engine_.compaction_io_executor_->stop();
  engine_.compaction_io_executor_.reset();
}

std::size_t CompactionCoordinator::effective_io_chunk_bytes() const {
  constexpr std::size_t kDefaultChunk = 256u * 1024u;
  if (engine_.compaction_io_chunk_bytes_ != 0u) return static_cast<std::size_t>(engine_.compaction_io_chunk_bytes_);
  if (engine_.compaction_dedicated_io_executor_ || engine_.compaction_merge_byte_tb_.max_bytes_per_second() > 0) return kDefaultChunk;
  return 0u;
}

void CompactionCoordinator::throttle_before_l0_merge_if_configured() {
  if (engine_.compaction_merge_min_interval_ms_ == 0) return;
  if (!engine_.has_last_l0_merge_wall_) return;
  const auto gap = std::chrono::milliseconds(static_cast<int>(engine_.compaction_merge_min_interval_ms_));
  const auto now = std::chrono::steady_clock::now();
  const auto next_ok = engine_.last_l0_merge_wall_ + gap;
  if (now < next_ok) {
    const auto t0 = std::chrono::steady_clock::now();
    std::this_thread::sleep_until(next_ok);
    const auto t1 = std::chrono::steady_clock::now();
    const auto slept = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
    if (slept > 0) {
      engine_.compaction_merge_throttle_sleep_ns_total_.fetch_add(static_cast<std::uint64_t>(slept),
                                                                    std::memory_order_relaxed);
    }
  }
}

void CompactionCoordinator::mark_after_successful_l0_merge_for_throttle() {
  if (engine_.compaction_merge_min_interval_ms_ == 0) return;
  engine_.last_l0_merge_wall_ = std::chrono::steady_clock::now();
  engine_.has_last_l0_merge_wall_ = true;
}

}  // namespace structdb::storage
