#include <newdb/page_cache.h>
#include <newdb/memory_registry.h>

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <list>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace newdb {
namespace {

std::mutex g_mu;
std::uint64_t g_hits{0};
std::uint64_t g_misses{0};
std::uint64_t g_evictions{0};

struct Entry {
    std::string key;
    std::string path;
    std::size_t page_no{0};
    std::vector<unsigned char> data;
};

std::list<Entry> g_lru;
std::unordered_map<std::string, std::list<Entry>::iterator> g_index;
std::uint64_t g_bytes{0};
std::uint64_t g_reject_oversized{0};
std::uint64_t g_bytes_evicted_total{0};
std::atomic<bool> g_evictor_registered{false};

std::size_t max_cache_bytes() {
    if (const char* e = std::getenv("NEWDB_PAGE_CACHE_MAX_BYTES")) {
        char* end = nullptr;
        const unsigned long long v = std::strtoull(e, &end, 10);
        if (end != e && (end == nullptr || *end == '\0') && v > 0) {
            return static_cast<std::size_t>(std::min<unsigned long long>(v, static_cast<unsigned long long>(-1)));
        }
    }
    return 0;
}

std::string make_key(const std::string& path, std::size_t page_no) {
    std::string k;
    k.reserve(path.size() + 24);
    k = path;
    k.push_back('\0');
    k += std::to_string(page_no);
    return k;
}

std::uint64_t evict_bytes_from_tail_unlocked(std::uint64_t target_free) {
    std::uint64_t freed = 0;
    while (freed < target_free && !g_lru.empty()) {
        auto it = std::prev(g_lru.end());
        const std::size_t sz = it->data.size();
        g_bytes -= sz;
        g_bytes_evicted_total += static_cast<std::uint64_t>(sz);
        g_index.erase(it->key);
        g_lru.erase(it);
        ++g_evictions;
        freed += sz;
    }
    return freed;
}

std::uint64_t page_cache_evictor_callback(std::uint64_t target_free) {
    std::uint64_t freed = 0;
    {
        std::lock_guard<std::mutex> lk(g_mu);
        freed = evict_bytes_from_tail_unlocked(target_free);
    }
    if (freed > 0) {
        memory_registry_release(MemoryKind::PageCache, freed);
        memory_registry_record_eviction(MemoryKind::PageCache, freed);
    }
    return freed;
}

void ensure_evictor_registered() {
    bool expected = false;
    if (g_evictor_registered.compare_exchange_strong(expected, true)) {
        memory_registry_register_evictor(MemoryKind::PageCache, &page_cache_evictor_callback);
    }
}

std::uint64_t evict_until_under_unlocked(const std::size_t cap, const std::size_t reserve_for_new) {
    std::uint64_t freed_total = 0;
    while (g_bytes + reserve_for_new > cap && !g_lru.empty()) {
        auto it = std::prev(g_lru.end());
        const std::size_t freed = it->data.size();
        g_bytes -= freed;
        g_bytes_evicted_total += static_cast<std::uint64_t>(freed);
        g_index.erase(it->key);
        g_lru.erase(it);
        ++g_evictions;
        freed_total += freed;
    }
    return freed_total;
}

} // namespace

PageCacheGlobalStats page_cache_global_stats() {
    std::lock_guard<std::mutex> lk(g_mu);
    PageCacheGlobalStats s{};
    s.hits = g_hits;
    s.misses = g_misses;
    s.evictions = g_evictions;
    s.bytes_in_cache = g_bytes;
    s.reject_oversized_page = g_reject_oversized;
    s.bytes_evicted_total = g_bytes_evicted_total;
    return s;
}

void page_cache_reset_stats_for_test() {
    std::uint64_t to_release = 0;
    {
        std::lock_guard<std::mutex> lk(g_mu);
        to_release = g_bytes;
        g_lru.clear();
        g_index.clear();
        g_bytes = 0;
        g_hits = 0;
        g_misses = 0;
        g_evictions = 0;
        g_reject_oversized = 0;
        g_bytes_evicted_total = 0;
        // Allow re-registration after `memory_registry_reset_for_test()` clears registry evictors.
        g_evictor_registered.store(false, std::memory_order_relaxed);
    }
    if (to_release > 0) {
        memory_registry_release(MemoryKind::PageCache, to_release);
    }
}

bool page_cache_try_get(const std::string& heap_file_path,
                        const std::size_t page_no,
                        const std::size_t page_size,
                        unsigned char* buf) {
    const std::size_t cap = max_cache_bytes();
    if (cap == 0 || buf == nullptr || page_size == 0) {
        return false;
    }
    const std::string key = make_key(heap_file_path, page_no);
    std::lock_guard<std::mutex> lk(g_mu);
    const auto it = g_index.find(key);
    if (it == g_index.end()) {
        ++g_misses;
        return false;
    }
    if (it->second->data.size() != page_size) {
        ++g_misses;
        return false;
    }
    std::memcpy(buf, it->second->data.data(), page_size);
    g_lru.splice(g_lru.begin(), g_lru, it->second);
    ++g_hits;
    return true;
}

void page_cache_put(const std::string& heap_file_path,
                    const std::size_t page_no,
                    const std::size_t page_size,
                    const unsigned char* data) {
    const std::size_t cap = max_cache_bytes();
    if (cap == 0 || data == nullptr || page_size == 0) {
        return;
    }
    if (page_size > cap) {
        std::lock_guard<std::mutex> lk(g_mu);
        ++g_reject_oversized;
        return;
    }
    ensure_evictor_registered();
    if (!memory_registry_try_admit(MemoryKind::PageCache, static_cast<std::uint64_t>(page_size))) {
        std::lock_guard<std::mutex> lk(g_mu);
        ++g_reject_oversized;
        return;
    }
    const std::string key = make_key(heap_file_path, page_no);
    std::uint64_t replaced_bytes = 0;
    std::uint64_t evicted_bytes = 0;
    {
        std::lock_guard<std::mutex> lk(g_mu);
        const auto existing = g_index.find(key);
        if (existing != g_index.end()) {
            const std::size_t old_sz = existing->second->data.size();
            g_bytes -= old_sz;
            g_lru.erase(existing->second);
            g_index.erase(existing);
            replaced_bytes = static_cast<std::uint64_t>(old_sz);
        }
        evicted_bytes = evict_until_under_unlocked(cap, page_size);
        Entry ent;
        ent.key = key;
        ent.path = heap_file_path;
        ent.page_no = page_no;
        ent.data.assign(data, data + page_size);
        g_lru.push_front(std::move(ent));
        g_index[key] = g_lru.begin();
        g_bytes += page_size;
    }
    if (replaced_bytes > 0) {
        memory_registry_release(MemoryKind::PageCache, replaced_bytes);
    }
    if (evicted_bytes > 0) {
        memory_registry_release(MemoryKind::PageCache, evicted_bytes);
        memory_registry_record_eviction(MemoryKind::PageCache, evicted_bytes);
    }
}

} // namespace newdb
