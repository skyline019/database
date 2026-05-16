#pragma once

#include <string>

namespace structdb::storage {

class StorageEngine;

/// `open()` 路径：目录准备、独占锁、段目录加载、WAL/redo/undo/manifest/commit_seq、checkpoint+WAL 重放、段观测刷新。
/// WAL 行/批解码由 **`WalReplayApplier`**（`wal_replay_applier.*`，`friend` `StorageEngine`）在 `replay_checkpoint_and_wal` 中调用。
class RecoveryCoordinator {
 public:
  explicit RecoveryCoordinator(StorageEngine& engine) noexcept : engine_(engine) {}

  bool acquire_exclusive_directory_lock(std::string* error_out);
  void release_exclusive_directory_lock();

  bool prepare_directories_tmp(std::string* error_out);
  bool load_segment_catalogs(std::string* error_out);
  bool open_log_files_manifest_commit_seq(std::string* error_out);
  bool replay_checkpoint_and_wal(unsigned open_flags, std::string* error_out);
  void refresh_segment_observability();

 private:
  StorageEngine& engine_;
};

}  // namespace structdb::storage
