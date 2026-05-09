#pragma once

/// Optional persisted ANALYZE-style stats for WHERE planning (`*.tablestats`).
/// Plan code should treat this as an opaque pointer to `TableStats`; keep heavy
/// includes in `table_stats.cc` / `plan_impl.cc` rather than widening `where.h`.
struct TableStats;

struct WherePlanningStatsRef {
    const TableStats* table_stats{nullptr};
    [[nodiscard]] bool has_stats() const noexcept { return table_stats != nullptr; }
};
