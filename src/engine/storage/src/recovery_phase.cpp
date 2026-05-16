#include "structdb/storage/recovery_phase.hpp"

namespace structdb::storage {

const char* storage_recovery_phase_cstr(StorageRecoveryPhase p) noexcept {
  switch (p) {
    case StorageRecoveryPhase::PrepareDataDir:
      return "PrepareDataDir";
    case StorageRecoveryPhase::AcquireExclusiveLock:
      return "AcquireExclusiveLock";
    case StorageRecoveryPhase::LoadSegmentCatalogs:
      return "LoadSegmentCatalogs";
    case StorageRecoveryPhase::OpenWalRedoUndo:
      return "OpenWalRedoUndo";
    case StorageRecoveryPhase::LoadManifestIfPresent:
      return "LoadManifestIfPresent";
    case StorageRecoveryPhase::LoadCommitSeqHighWater:
      return "LoadCommitSeqHighWater";
    case StorageRecoveryPhase::ReadCheckpointAndReplayWal:
      return "ReadCheckpointAndReplayWal";
    case StorageRecoveryPhase::OptionalRebuildUndoStack:
      return "OptionalRebuildUndoStack";
    case StorageRecoveryPhase::PersistCommitSeqAfterReplay:
      return "PersistCommitSeqAfterReplay";
    case StorageRecoveryPhase::ClearStaleMemFlushSnapshot:
      return "ClearStaleMemFlushSnapshot";
    case StorageRecoveryPhase::RefreshSegmentObservability:
      return "RefreshSegmentObservability";
    case StorageRecoveryPhase::MarkOpened:
      return "MarkOpened";
  }
  return "StorageRecoveryPhase(?)";
}

}  // namespace structdb::storage
