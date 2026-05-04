#include <newdb/memory_budget.h>
#include <newdb/page_cache.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>

namespace newdb {
namespace {

bool parse_positive_u64(const char* raw, std::uint64_t& out) {
    if (raw == nullptr || raw[0] == '\0') {
        return false;
    }
    char* end = nullptr;
    const unsigned long long v = std::strtoull(raw, &end, 10);
    if (end == raw || (end != nullptr && *end != '\0')) {
        return false;
    }
    if (v == 0) {
        return false;
    }
    constexpr unsigned long long kMax = static_cast<unsigned long long>(~std::uint64_t{0});
    out = static_cast<std::uint64_t>(std::min(v, kMax));
    return true;
}

} // namespace

std::uint64_t memory_budget_max_bytes_env() {
    if (const char* e = std::getenv("NEWDB_MEMORY_BUDGET_MAX_BYTES")) {
        std::uint64_t v = 0;
        if (parse_positive_u64(e, v)) {
            return v;
        }
    }
    if (const char* e = std::getenv("NEWDB_PAGE_CACHE_MAX_BYTES")) {
        std::uint64_t v = 0;
        if (parse_positive_u64(e, v)) {
            return v;
        }
    }
    return 0;
}

MemoryBudgetSnapshot memory_budget_snapshot() {
    const PageCacheGlobalStats pc = page_cache_global_stats();
    MemoryBudgetSnapshot s{};
    s.max_bytes = memory_budget_max_bytes_env();
    s.used_bytes = pc.bytes_in_cache;
    s.reject_count = pc.reject_oversized_page;
    s.eviction_events = pc.evictions;
    s.bytes_evicted_total = pc.bytes_evicted_total;
    return s;
}

} // namespace newdb
