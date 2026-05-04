#pragma once

#include <cstdint>

namespace newdb {

/// Soft memory budget facade (doc §4.4): currently driven by page cache env + `page_cache_global_stats()`.
struct MemoryBudgetSnapshot {
    std::uint64_t max_bytes{0};
    std::uint64_t used_bytes{0};
    std::uint64_t reject_count{0};
    std::uint64_t eviction_events{0};
    std::uint64_t bytes_evicted_total{0};
};

/// `NEWDB_MEMORY_BUDGET_MAX_BYTES` if set and >0, else `NEWDB_PAGE_CACHE_MAX_BYTES` (same semantics as page cache cap).
[[nodiscard]] std::uint64_t memory_budget_max_bytes_env();

[[nodiscard]] MemoryBudgetSnapshot memory_budget_snapshot();

} // namespace newdb
