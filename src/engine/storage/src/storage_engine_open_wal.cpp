#include "structdb/storage/storage_engine.hpp"

#include "structdb/infra/tracer.hpp"
#include "structdb/storage/storage_trace.hpp"

#include <cstdint>
#include <filesystem>
#include <mutex>
#include <shared_mutex>

namespace structdb::storage {

StorageEngine::StorageEngine(std::filesystem::path data_dir)
    : dir_(std::move(data_dir)),
      pool_(std::make_unique<BufferPool>(16)),
      wal_coordinator_(*this),
      compaction_coordinator_(*this),
      checkpoint_undo_coordinator_(*this),
      wal_replay_applier_(*this),
      recovery_coordinator_(*this),
      storage_telemetry_(*this) {}

bool StorageEngine::open(std::string* error_out, unsigned open_flags) {
  struct ExclusiveOpenGuard {
    StorageEngine* self;
    bool lock_held{false};
    explicit ExclusiveOpenGuard(StorageEngine* s) : self(s) {}
    bool acquire(std::string* err) {
      if (!self->exclusive_directory_lock_) return true;
      if (!self->recovery_coordinator_.acquire_exclusive_directory_lock(err)) return false;
      lock_held = true;
      return true;
    }
    ~ExclusiveOpenGuard() {
      if (lock_held) self->recovery_coordinator_.release_exclusive_directory_lock();
    }
    void disarm() { lock_held = false; }
  } excl_guard(this);
  infra::SpanGuard trace_open(trace::kOpenRoot, 0);
  if (!recovery_coordinator_.prepare_directories_tmp(error_out)) return false;
  if (!excl_guard.acquire(error_out)) return false;
  undo_stack_.clear();
  if (!recovery_coordinator_.load_segment_catalogs(error_out)) return false;
  if (!recovery_coordinator_.open_log_files_manifest_commit_seq(error_out)) return false;
  if (!recovery_coordinator_.replay_checkpoint_and_wal(open_flags, error_out)) return false;
  recovery_coordinator_.refresh_segment_observability();
  excl_guard.disarm();
  opened_ = true;
  return true;
}

void StorageEngine::close() {
  stop_compaction_worker();
  compaction_coordinator_.shutdown_compaction_io_executor();
  if (opened_) {
    std::string err;
    (void)persist_commit_seq_hw_(&err);
  }
  {
    std::lock_guard<std::shared_mutex> lk(mu_);
    mem_mgr_.merge_frozen_into_active_and_clear();
  }
  undo_stack_.clear();
  wal_.close();
  redo_.close();
  undo_.close();
  opened_ = false;
  recovery_coordinator_.release_exclusive_directory_lock();
}

}  // namespace structdb::storage
