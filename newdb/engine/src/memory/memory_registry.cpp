#include <newdb/memory_registry.h>

#include <array>
#include <cstdlib>
#include <mutex>

namespace newdb {

namespace {

struct KindState {
    std::uint64_t used_bytes{0};
    std::uint64_t evictions{0};
    std::uint64_t admit_rejects{0};
    std::uint64_t bytes_evicted_total{0};
    MemoryEvictor evictor{};
};

std::recursive_mutex g_mu;
std::array<KindState, kMemoryKindCount> g_kinds;
std::uint64_t g_global_admit_rejects{0};

bool parse_u64_env(const char* name, std::uint64_t& out) {
    if (name == nullptr) {
        return false;
    }
    const char* raw = std::getenv(name);
    if (raw == nullptr || raw[0] == '\0') {
        return false;
    }
    char* end = nullptr;
    const unsigned long long v = std::strtoull(raw, &end, 10);
    if (end == raw || (end != nullptr && *end != '\0')) {
        return false;
    }
    out = static_cast<std::uint64_t>(v);
    return true;
}

std::uint64_t kind_cap_lookup(MemoryKind kind) {
    std::uint64_t v = 0;
    switch (kind) {
    case MemoryKind::PageCache:
        if (parse_u64_env("NEWDB_PAGE_CACHE_MAX_BYTES", v)) {
            return v;
        }
        return 0;
    case MemoryKind::EqSidecar:
        if (parse_u64_env("NEWDB_SIDECAR_CACHE_MAX_BYTES", v)) {
            return v;
        }
        return 0;
    case MemoryKind::QueryTemp:
        if (parse_u64_env("NEWDB_QUERY_TEMP_MAX_BYTES", v)) {
            return v;
        }
        return 0;
    }
    return 0;
}

std::uint64_t global_cap_lookup() {
    std::uint64_t v = 0;
    if (parse_u64_env("NEWDB_MEMORY_BUDGET_MAX_BYTES", v)) {
        return v;
    }
    if (parse_u64_env("NEWDB_PAGE_CACHE_MAX_BYTES", v)) {
        return v;
    }
    return 0;
}

std::uint64_t sum_used_locked() {
    std::uint64_t s = 0;
    for (const auto& k : g_kinds) {
        s += k.used_bytes;
    }
    return s;
}

std::uint64_t need_free_locked(MemoryKind kind, std::uint64_t bytes,
                               std::uint64_t kind_cap, std::uint64_t global_cap) {
    std::uint64_t need = 0;
    const int idx = static_cast<int>(kind);
    if (kind_cap > 0 && g_kinds[idx].used_bytes + bytes > kind_cap) {
        need = (g_kinds[idx].used_bytes + bytes) - kind_cap;
    }
    if (global_cap > 0) {
        const std::uint64_t total = sum_used_locked();
        if (total + bytes > global_cap) {
            const std::uint64_t need_global = (total + bytes) - global_cap;
            if (need_global > need) {
                need = need_global;
            }
        }
    }
    return need;
}

std::uint64_t request_eviction_locked(MemoryKind primary_kind, std::uint64_t target_free) {
    if (target_free == 0) {
        return 0;
    }
    std::uint64_t freed_total = 0;
    auto run = [&](MemoryKind k) -> bool {
        const int idx = static_cast<int>(k);
        MemoryEvictor cb = g_kinds[idx].evictor;
        if (!cb) {
            return false;
        }
        const std::uint64_t need = (target_free > freed_total) ? (target_free - freed_total) : 0;
        if (need == 0) {
            return true;
        }
        std::uint64_t freed = 0;
        try {
            freed = cb(need);
        } catch (...) {
            freed = 0;
        }
        freed_total += freed;
        return freed_total >= target_free;
    };
    if (run(primary_kind)) {
        return freed_total;
    }
    for (int i = 0; i < kMemoryKindCount; ++i) {
        const MemoryKind k = static_cast<MemoryKind>(i);
        if (k == primary_kind) {
            continue;
        }
        if (run(k)) {
            return freed_total;
        }
    }
    return freed_total;
}

}  // namespace

std::uint64_t memory_registry_kind_cap(MemoryKind kind) {
    return kind_cap_lookup(kind);
}

std::uint64_t memory_registry_global_cap() {
    return global_cap_lookup();
}

bool memory_registry_try_admit(MemoryKind kind, std::uint64_t bytes) {
    if (bytes == 0) {
        return true;
    }
    std::lock_guard<std::recursive_mutex> lk(g_mu);
    const int idx = static_cast<int>(kind);
    const std::uint64_t kind_cap = kind_cap_lookup(kind);
    const std::uint64_t global_cap = global_cap_lookup();
    if (kind_cap > 0 && bytes > kind_cap) {
        ++g_kinds[idx].admit_rejects;
        ++g_global_admit_rejects;
        return false;
    }
    if (global_cap > 0 && bytes > global_cap) {
        ++g_kinds[idx].admit_rejects;
        ++g_global_admit_rejects;
        return false;
    }
    std::uint64_t need = need_free_locked(kind, bytes, kind_cap, global_cap);
    if (need > 0) {
        (void)request_eviction_locked(kind, need);
        need = need_free_locked(kind, bytes, kind_cap, global_cap);
    }
    if (need > 0) {
        ++g_kinds[idx].admit_rejects;
        ++g_global_admit_rejects;
        return false;
    }
    g_kinds[idx].used_bytes += bytes;
    return true;
}

void memory_registry_release(MemoryKind kind, std::uint64_t bytes) {
    if (bytes == 0) {
        return;
    }
    std::lock_guard<std::recursive_mutex> lk(g_mu);
    const int idx = static_cast<int>(kind);
    if (g_kinds[idx].used_bytes <= bytes) {
        g_kinds[idx].used_bytes = 0;
    } else {
        g_kinds[idx].used_bytes -= bytes;
    }
}

void memory_registry_record_eviction(MemoryKind kind, std::uint64_t bytes) {
    std::lock_guard<std::recursive_mutex> lk(g_mu);
    const int idx = static_cast<int>(kind);
    ++g_kinds[idx].evictions;
    g_kinds[idx].bytes_evicted_total += bytes;
}

void memory_registry_register_evictor(MemoryKind kind, MemoryEvictor evictor) {
    std::lock_guard<std::recursive_mutex> lk(g_mu);
    g_kinds[static_cast<int>(kind)].evictor = std::move(evictor);
}

MemoryRegistryTotals memory_registry_totals() {
    std::lock_guard<std::recursive_mutex> lk(g_mu);
    MemoryRegistryTotals t{};
    const auto& pc = g_kinds[static_cast<int>(MemoryKind::PageCache)];
    t.page_cache_used_bytes = pc.used_bytes;
    t.page_cache_evictions = pc.evictions;
    t.page_cache_admit_rejects = pc.admit_rejects;
    t.page_cache_bytes_evicted_total = pc.bytes_evicted_total;
    const auto& sc = g_kinds[static_cast<int>(MemoryKind::EqSidecar)];
    t.sidecar_used_bytes = sc.used_bytes;
    t.sidecar_evictions = sc.evictions;
    t.sidecar_admit_rejects = sc.admit_rejects;
    t.sidecar_bytes_evicted_total = sc.bytes_evicted_total;
    const auto& qt = g_kinds[static_cast<int>(MemoryKind::QueryTemp)];
    t.query_temp_used_bytes = qt.used_bytes;
    t.query_temp_evictions = qt.evictions;
    t.query_temp_admit_rejects = qt.admit_rejects;
    t.query_temp_bytes_evicted_total = qt.bytes_evicted_total;
    t.global_used_bytes = pc.used_bytes + sc.used_bytes + qt.used_bytes;
    t.global_admit_rejects = g_global_admit_rejects;
    return t;
}

void memory_registry_reset_for_test() {
    std::lock_guard<std::recursive_mutex> lk(g_mu);
    for (auto& k : g_kinds) {
        k.used_bytes = 0;
        k.evictions = 0;
        k.admit_rejects = 0;
        k.bytes_evicted_total = 0;
        k.evictor = MemoryEvictor{};
    }
    g_global_admit_rejects = 0;
}

}  // namespace newdb
