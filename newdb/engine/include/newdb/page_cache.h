#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace newdb {

struct PageCacheGlobalStats {
    std::uint64_t hits{0};
    std::uint64_t misses{0};
    std::uint64_t evictions{0};
    /// Sum of cached page payloads (each entry is one page_size bytes).
    std::uint64_t bytes_in_cache{0};
    /// `page_cache_put` skipped because a single page exceeds configured max bytes.
    std::uint64_t reject_oversized_page{0};
    /// Cumulative payload bytes removed by LRU eviction (since process start or last `page_cache_reset_stats_for_test`).
    std::uint64_t bytes_evicted_total{0};
};

/// Returns cumulative counters since process start (thread-safe).
[[nodiscard]] PageCacheGlobalStats page_cache_global_stats();

/// Test hook: clear all cached pages, release their registry bytes, and zero global stats counters.
void page_cache_reset_stats_for_test();

/// When `NEWDB_PAGE_CACHE_MAX_BYTES` > 0, try to copy a cached page into `buf` (length `page_size`).
[[nodiscard]] bool page_cache_try_get(const std::string& heap_file_path,
                                      std::size_t page_no,
                                      std::size_t page_size,
                                      unsigned char* buf);

/// Insert a full page copy into the cache (no-op when cache disabled).
void page_cache_put(const std::string& heap_file_path,
                    std::size_t page_no,
                    std::size_t page_size,
                    const unsigned char* data);

} // namespace newdb
