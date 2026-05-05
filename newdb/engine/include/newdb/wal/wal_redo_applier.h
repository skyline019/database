#pragma once

#include <newdb/heap_table.h>
#include <newdb/wal/wal_redo_planner.h>

#include <cstdint>

namespace newdb {
namespace wal_recovery {

/// Per-replay accounting reported by `WalRedoApplier::apply` for runtime stats wiring.
struct ApplyStats {
    /// INSERT/UPDATE/DELETE applied (including DELETE→tombstone append).
    std::uint64_t records_applied{0};
    /// Idempotent collisions (e.g. duplicate INSERT against existing pk).
    std::uint64_t conflicts{0};
    /// Ops skipped (currently DELETE on missing target id).
    std::uint64_t skipped{0};
    /// Wall-time accumulated across the apply pass (best-effort `monotonic_ms` deltas).
    std::uint64_t apply_ms{0};
};

/// Phase-2 closed-loop split: stateless applier that consumes `RedoPlanSummary` produced by
/// `WalRedoPlanner` and writes into the target `HeapTable`.
///
/// Behavior is intentionally identical to the legacy `recover_replay_segments` apply branch:
/// - `INSERT`/`UPDATE`: replace the row at the existing `index_by_id` slot if present, otherwise append.
/// - `DELETE`: if `find_by_id` finds an active target, append a tombstone row (`__deleted=1`).
class WalRedoApplier {
public:
    explicit WalRedoApplier(HeapTable& target) : target_(&target) {}

    [[nodiscard]] ApplyStats apply(const RedoPlanSummary& plan) const;

private:
    HeapTable* target_{nullptr};
};

}  // namespace wal_recovery
}  // namespace newdb
