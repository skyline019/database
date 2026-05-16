#pragma once

#include <cstdint>

namespace structdb::storage {

/// 与 `StorageEngine::kOpenFlagRebuildUndoStackFromLog` 数值一致；单一事实来源。
namespace recovery_open_policy {
inline constexpr unsigned kRebuildUndoStackFromLog = 1u;
}

/// `StorageEngine::open(..., open_flags)` 的策略投影（轻量对象化入口，便于后续扩展 embed/journal 交叉规则）。
struct RecoveryOpenPolicy {
  unsigned open_flags{0};
  bool rebuild_undo_stack_from_log() const noexcept {
    return (open_flags & recovery_open_policy::kRebuildUndoStackFromLog) != 0;
  }
};

}  // namespace structdb::storage
