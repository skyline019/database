#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace structdb::storage {

struct CheckpointState;
class StorageEngine;

/// Undo segment catalog, logical stream accounting, checkpoint `undo_log_safe_prefix_bytes`, rebuild/rollback.
class CheckpointUndoCoordinator {
 public:
  explicit CheckpointUndoCoordinator(StorageEngine& engine) noexcept : engine_(engine) {}

  bool persist_undo_segments_for_flush_unlocked(std::string* error_out);
  bool load_undo_segments_catalog_for_open(std::string* error_out);

  std::uint64_t undo_logical_stream_total_bytes_unlocked() const;
  bool undo_consume_logical_prefix_unlocked(std::uint64_t prefix_bytes, std::string* error_out);
  bool undo_roll_to_new_segment_unlocked(std::string* error_out);
  bool undo_maybe_roll_after_append_unlocked(std::string* error_out);

  void fill_checkpoint_undo_safe_prefix_unlocked(CheckpointState* ck) const;
  bool undo_truncate_recyclable_prefix_unlocked(std::string* error_out);
  std::uint64_t compute_undo_recyclable_prefix_bytes_unlocked() const;

  bool rebuild_undo_stack_from_undo_log_unlocked(std::string* error_out);
  bool rollback_one_undo_frame_unlocked(std::string* error_out);

 private:
  StorageEngine& engine_;
};

}  // namespace structdb::storage
