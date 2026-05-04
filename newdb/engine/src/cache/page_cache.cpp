#include <newdb/page_cache.h>

#include <algorithm>
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

void evict_until_under_unlocked(const std::size_t cap, const std::size_t reserve_for_new) {
    while (g_bytes + reserve_for_new > cap && !g_lru.empty()) {
        auto it = std::prev(g_lru.end());
        const std::size_t freed = it->data.size();
        g_bytes -= freed;
        g_bytes_evicted_total += static_cast<std::uint64_t>(freed);
        g_index.erase(it->key);
        g_lru.erase(it);
        ++g_evictions;
    }
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
    std::lock_guard<std::mutex> lk(g_mu);
    g_hits = 0;
    g_misses = 0;
    g_evictions = 0;
    g_reject_oversized = 0;
    g_bytes_evicted_total = 0;
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
    const std::string key = make_key(heap_file_path, page_no);
    std::lock_guard<std::mutex> lk(g_mu);
    const auto existing = g_index.find(key);
    if (existing != g_index.end()) {
        g_bytes -= existing->second->data.size();
        g_lru.erase(existing->second);
        g_index.erase(existing);
    }
    evict_until_under_unlocked(cap, page_size);
    Entry ent;
    ent.key = key;
    ent.path = heap_file_path;
    ent.page_no = page_no;
    ent.data.assign(data, data + page_size);
    g_lru.push_front(std::move(ent));
    g_index[key] = g_lru.begin();
    g_bytes += page_size;
}

} // namespace newdb
