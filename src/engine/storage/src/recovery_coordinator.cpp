#include "structdb/storage/recovery_coordinator.hpp"

#include "structdb/storage/storage_engine.hpp"

#include "structdb/infra/tracer.hpp"
#include "structdb/storage/recovery_phase.hpp"
#include "structdb/storage/checkpoint.hpp"
#include "structdb/storage/storage_trace.hpp"
#include "structdb/storage/wal.hpp"

#include <cstdint>
#include <filesystem>
#include <mutex>
#include <shared_mutex>
#include <vector>

#if defined(_WIN32)
#  ifndef NOMINMAX
#    define NOMINMAX
#  endif
#  include <windows.h>
#else
#  include <fcntl.h>
#  include <sys/file.h>
#  include <unistd.h>
#endif

#include "storage_engine_detail.hpp"

namespace sed = structdb::storage::storage_engine_detail;

namespace structdb::storage {

bool RecoveryCoordinator::acquire_exclusive_directory_lock(std::string* error_out) {
  if (!engine_.exclusive_directory_lock_) return true;
  const auto lock_path = engine_.dir_ / ".structdb_exclusive.lock";
#if defined(_WIN32)
  const std::wstring wpath = lock_path.wstring();
  HANDLE h = CreateFileW(wpath.c_str(), GENERIC_READ | GENERIC_WRITE,
                          FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
  if (h == INVALID_HANDLE_VALUE) {
    if (error_out) *error_out = "exclusive lock: cannot open .structdb_exclusive.lock";
    return false;
  }
  if (!LockFile(h, 0, 0, 1, 0)) {
    CloseHandle(h);
    if (error_out) *error_out = "exclusive lock: data_dir is already locked by another process";
    return false;
  }
  engine_.exclusive_lock_handle_ = h;
#else
  const std::string p8 = lock_path.u8string();
  const int fd = ::open(p8.c_str(), O_RDWR | O_CREAT, 0644);
  if (fd < 0) {
    if (error_out) *error_out = "exclusive lock: cannot open .structdb_exclusive.lock";
    return false;
  }
  if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
    ::close(fd);
    if (error_out) *error_out = "exclusive lock: data_dir is already locked by another process";
    return false;
  }
  engine_.exclusive_lock_fd_ = fd;
#endif
  return true;
}

void RecoveryCoordinator::release_exclusive_directory_lock() {
#if defined(_WIN32)
  if (engine_.exclusive_lock_handle_) {
    UnlockFile(static_cast<HANDLE>(engine_.exclusive_lock_handle_), 0, 0, 1, 0);
    CloseHandle(static_cast<HANDLE>(engine_.exclusive_lock_handle_));
    engine_.exclusive_lock_handle_ = nullptr;
  }
#else
  if (engine_.exclusive_lock_fd_ >= 0) {
    ::flock(engine_.exclusive_lock_fd_, LOCK_UN);
    ::close(engine_.exclusive_lock_fd_);
    engine_.exclusive_lock_fd_ = -1;
  }
#endif
}

bool RecoveryCoordinator::prepare_directories_tmp(std::string* error_out) {
  (void)error_out;
  infra::SpanGuard trace_phase(
      std::string(trace::kOpenPhasePrefix) + storage_recovery_phase_cstr(StorageRecoveryPhase::PrepareDataDir), 0);
  std::filesystem::create_directories(engine_.dir_);
  std::error_code ec;
  std::filesystem::remove(engine_.dir_ / "_structdb_memflush_tmp.sst", ec);
  return true;
}

bool RecoveryCoordinator::load_segment_catalogs(std::string* error_out) {
  infra::SpanGuard trace_phase(
      std::string(trace::kOpenPhasePrefix) + storage_recovery_phase_cstr(StorageRecoveryPhase::LoadSegmentCatalogs), 0);
  if (!engine_.wal_coordinator_.load_segments_catalog_for_open(error_out)) return false;
  return engine_.checkpoint_undo_coordinator_.load_undo_segments_catalog_for_open(error_out);
}

bool RecoveryCoordinator::open_log_files_manifest_commit_seq(std::string* error_out) {
  {
    infra::SpanGuard trace_phase(
        std::string(trace::kOpenPhasePrefix) + storage_recovery_phase_cstr(StorageRecoveryPhase::OpenWalRedoUndo), 0);
    if (!engine_.wal_.open(engine_.dir_, engine_.wal_io_cfg_)) {
      if (error_out) *error_out = "wal open";
      return false;
    }
    if (!engine_.redo_.open(engine_.dir_)) {
      if (error_out) *error_out = "redo open";
      return false;
    }
    if (!engine_.undo_.open(engine_.dir_)) {
      if (error_out) *error_out = "undo open";
      return false;
    }
  }
  {
    infra::SpanGuard trace_phase(
        std::string(trace::kOpenPhasePrefix) + storage_recovery_phase_cstr(StorageRecoveryPhase::LoadManifestIfPresent), 0);
    const auto man_path = engine_.dir_ / "MANIFEST";
    if (std::filesystem::exists(man_path)) {
      if (!engine_.manifest_.load(man_path)) {
        if (error_out) *error_out = "manifest load";
        return false;
      }
      engine_.lsm_.sync_from_manifest(engine_.manifest_);
    }
  }
  {
    infra::SpanGuard trace_phase(
        std::string(trace::kOpenPhasePrefix) + storage_recovery_phase_cstr(StorageRecoveryPhase::LoadCommitSeqHighWater), 0);
    return engine_.load_commit_seq_hw_(error_out);
  }
}

bool RecoveryCoordinator::replay_checkpoint_and_wal(unsigned open_flags, std::string* error_out) {
  infra::SpanGuard trace_phase(
      std::string(trace::kOpenPhasePrefix) + storage_recovery_phase_cstr(StorageRecoveryPhase::ReadCheckpointAndReplayWal), 0);
  std::lock_guard<std::shared_mutex> lk(engine_.mu_);
  engine_.mem_mgr_.reset_to_backend(engine_.memtable_backend_);
  CheckpointState ck{};
  std::string ckr;
  if (!engine_.ckpt_.read_latest(engine_.dir_, &ck, &ckr)) {
    if (error_out) *error_out = ckr.empty() ? "checkpoint read" : ckr;
    return false;
  }
  if (ck.manifest_version > engine_.manifest_.version()) {
    if (error_out) *error_out = "checkpoint ahead of manifest";
    return false;
  }
  const std::uint64_t wal_off = ck.wal_offset;
  const auto replay_one = [this](std::string_view rec, std::string* err) {
    constexpr std::string_view kBatchHdr = "STDBBW1\n";
    if (rec.size() >= kBatchHdr.size() && rec.compare(0, kBatchHdr.size(), kBatchHdr) == 0) {
      return engine_.wal_replay_applier_.apply_batch_unlocked(rec, err);
    }
    return engine_.wal_replay_applier_.apply_line_unlocked(rec, err);
  };
  for (const auto& rel : engine_.wal_sealed_relative_) {
    if (!wal_replay_from_offset(engine_.dir_ / rel, 0, replay_one, error_out)) return false;
  }
  if (!wal_replay_from_offset(engine_.wal_.path(), wal_off, replay_one, error_out)) {
    return false;
  }
  const RecoveryOpenPolicy recovery_policy{open_flags};
  if (recovery_policy.rebuild_undo_stack_from_log()) {
    infra::SpanGuard trace_undo(
        std::string(trace::kOpenPhasePrefix) + storage_recovery_phase_cstr(StorageRecoveryPhase::OptionalRebuildUndoStack),
        0);
    if (!engine_.checkpoint_undo_coordinator_.rebuild_undo_stack_from_undo_log_unlocked(error_out)) return false;
  }
  {
    infra::SpanGuard trace_persist(
        std::string(trace::kOpenPhasePrefix) +
            storage_recovery_phase_cstr(StorageRecoveryPhase::PersistCommitSeqAfterReplay),
        0);
    std::string err2;
    (void)engine_.persist_commit_seq_hw_(&err2);
  }
  {
    infra::SpanGuard trace_mem(
        std::string(trace::kOpenPhasePrefix) +
            storage_recovery_phase_cstr(StorageRecoveryPhase::ClearStaleMemFlushSnapshot),
        0);
    engine_.mem_mgr_.discard_frozen_snapshot();
  }
  return true;
}

void RecoveryCoordinator::refresh_segment_observability() {
  infra::SpanGuard trace_phase(
      std::string(trace::kOpenPhasePrefix) + storage_recovery_phase_cstr(StorageRecoveryPhase::RefreshSegmentObservability), 0);
  if (engine_.wal_catalog_v2_) {
    engine_.wal_segment_count_observed_ = static_cast<std::uint32_t>(engine_.wal_sealed_relative_.size() + 1u);
  } else {
    engine_.wal_segment_count_observed_ = sed::read_wal_segments_metadata_or_default(engine_.dir_);
  }
  if (engine_.undo_catalog_v2_) {
    engine_.undo_segment_count_observed_ = static_cast<std::uint32_t>(engine_.undo_sealed_relative_.size() + 1u);
  } else {
    engine_.undo_segment_count_observed_ = 1u;
  }
}

}  // namespace structdb::storage
