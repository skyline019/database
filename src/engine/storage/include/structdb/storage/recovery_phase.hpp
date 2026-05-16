#pragma once

#include <cstdint>

namespace structdb::storage {

/// Named phases for `StorageEngine::open` crash-recovery / bootstrap sequencing (observability + docs alignment).
enum class StorageRecoveryPhase : std::uint8_t {
  PrepareDataDir = 0,
  AcquireExclusiveLock,
  LoadSegmentCatalogs,
  OpenWalRedoUndo,
  LoadManifestIfPresent,
  LoadCommitSeqHighWater,
  ReadCheckpointAndReplayWal,
  OptionalRebuildUndoStack,
  PersistCommitSeqAfterReplay,
  ClearStaleMemFlushSnapshot,
  RefreshSegmentObservability,
  MarkOpened,
};

const char* storage_recovery_phase_cstr(StorageRecoveryPhase p) noexcept;

}  // namespace structdb::storage
