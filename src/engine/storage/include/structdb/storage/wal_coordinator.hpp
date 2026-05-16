#pragma once

#include <string>

namespace structdb::storage {

class StorageEngine;

/// Owns WAL segment catalog, roll/trim/GC, and append/fsync observability counters (caller holds `mu_` as before).
class WalCoordinator {
 public:
  explicit WalCoordinator(StorageEngine& engine) noexcept : engine_(engine) {}

  bool persist_segments_for_flush_unlocked(std::string* error_out);
  bool load_segments_catalog_for_open(std::string* error_out);

  bool try_trim_prefix_through_checkpoint_unlocked(std::string* error_out);
  bool gc_sealed_archives_unlocked(std::string* error_out);

  void observe_append_unlocked(bool record_fsync);
  void observe_fsync_unlocked();

  bool roll_to_new_segment_unlocked(std::string* error_out);
  bool maybe_roll_after_append_unlocked();

 private:
  StorageEngine& engine_;
};

}  // namespace structdb::storage
