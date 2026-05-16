#pragma once

#include "structdb/storage/compaction_snapshot.hpp"
#include "structdb/storage/compaction_result.hpp"

#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>

namespace structdb::storage {

class StorageEngine;

/// L0 / tiered SST merge materialization, checkpoint tail after merge, compaction I/O executor + L0 throttle.
/// 合并路径返回值逐步收敛到 `CompactionResult`（`compaction_result.hpp`）；当前对外仍以 `bool` + `error_out` 为主。
class CompactionCoordinator {
 public:
  explicit CompactionCoordinator(StorageEngine& engine) noexcept : engine_(engine) {}

  using L0MergePrepareSnapshot = CompactionL0MergeSnapshot;
  using TieredPairMergeSnapshot = CompactionTieredPairSnapshot;

  bool capture_l0_merge_snapshot_unlocked(L0MergePrepareSnapshot* out, std::string* error_out) const;
  bool materialize_l0_merge_to_temp_file(const L0MergePrepareSnapshot& snap, std::filesystem::path* temp_path_out,
                                         std::string* error_out);
  bool materialize_l0_merge_to_temp_file_impl(const L0MergePrepareSnapshot& snap, std::filesystem::path* temp_path_out,
                                               std::string* error_out);
  bool commit_l0_merge_from_temp_unlocked(const L0MergePrepareSnapshot& snap, const std::filesystem::path& temp_path,
                                          std::string* error_out);
  bool compact_merge_two_oldest_l0_with_relock(std::unique_lock<std::shared_mutex>& lk, std::string* error_out);
  bool try_compact_l0_if_over_threshold(std::unique_lock<std::shared_mutex>& lk, std::string* error_out);

  bool capture_l1_l2_merge_snapshot_unlocked(TieredPairMergeSnapshot* out, std::string* error_out) const;
  bool capture_l2_l3_merge_snapshot_unlocked(TieredPairMergeSnapshot* out, std::string* error_out) const;
  bool capture_l3_l4_merge_snapshot_unlocked(TieredPairMergeSnapshot* out, std::string* error_out) const;
  bool materialize_tiered_pair_merge_to_temp(const TieredPairMergeSnapshot& snap, std::filesystem::path* temp_path_out,
                                              std::string* error_out);
  bool materialize_tiered_pair_merge_to_temp_impl(const TieredPairMergeSnapshot& snap, std::filesystem::path* temp_path_out,
                                                   std::string* error_out);
  bool commit_tiered_pair_merge_from_temp_unlocked(const TieredPairMergeSnapshot& snap,
                                                    const std::filesystem::path& temp_path, std::string* error_out);
  bool compact_merge_two_oldest_l1_to_l2_with_relock(std::unique_lock<std::shared_mutex>& lk, std::string* error_out);
  bool compact_merge_two_oldest_l2_to_l3_with_relock(std::unique_lock<std::shared_mutex>& lk, std::string* error_out);
  bool compact_merge_two_oldest_l3_to_l4_with_relock(std::unique_lock<std::shared_mutex>& lk, std::string* error_out);

  void ensure_compaction_io_executor();
  void shutdown_compaction_io_executor();
  std::size_t effective_io_chunk_bytes() const;
  void throttle_before_l0_merge_if_configured();
  void mark_after_successful_l0_merge_for_throttle();

 private:
  StorageEngine& engine_;
};

}  // namespace structdb::storage
