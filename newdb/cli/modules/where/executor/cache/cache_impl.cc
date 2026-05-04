#include <waterfall/config.h>

#include "cli/modules/where/executor/where.h"
#include "cli/modules/where/executor/stats/table_stats.h"
#include "cli/modules/where/executor/internal/query_internal.h"
#include "cli/modules/sidecar/eq/equality_index_sidecar.h"
#include "cli/modules/sidecar/visibility/visibility_checkpoint_sidecar.h"

#include <algorithm>
#include <atomic>
#include <charconv>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <limits>
#include <mutex>
#include <newdb/row.h>
#include <numeric>
#include <optional>
#include <sstream>
#include <unordered_set>
#include <unordered_map>
#include <list>

WhereQueryContext& default_where_context() {
    static WhereQueryContext ctx;
    return ctx;
}

struct QueryTraceGuard {
    const char* mode{"compute"};
    std::size_t rows{0};
    std::size_t logical_rows{0};
    std::size_t cond_count{0};
    std::chrono::steady_clock::time_point start{std::chrono::steady_clock::now()};
    bool enabled{false};
    std::size_t or_union_size{0};
    std::size_t or_verified_size{0};
    WhereQueryContext* context{nullptr};

    QueryTraceGuard(const std::size_t n, const std::size_t conds, WhereQueryContext* ctx)
        : logical_rows(n), cond_count(conds), context(ctx) {
        const char* v = std::getenv("NEWDB_QUERY_TRACE");
        enabled = (v != nullptr && std::string(v) == "1");
    }

    ~QueryTraceGuard() {
        if (enabled) {
            const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start).count();
            std::fprintf(stderr,
                         "[QUERY_TRACE] mode=%s conds=%zu rows=%zu logical=%zu or_union=%zu or_verified=%zu cache_hits=%llu cache_lookups=%llu elapsed_us=%lld\n",
                         mode,
                         cond_count,
                         rows,
                         logical_rows,
                         or_union_size,
                         or_verified_size,
                         static_cast<unsigned long long>(context ? context->cache_hits.load(std::memory_order_relaxed) : 0),
                         static_cast<unsigned long long>(context ? context->cache_lookups.load(std::memory_order_relaxed) : 0),
                         static_cast<long long>(elapsed));
        }
        const char* w = std::getenv("NEWDB_WHERE_WARN_HEAVY");
        if (w) {
            std::string s(w);
            for (auto& c : s) {
                c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
            }
            if (s == "1" || s == "on" || s == "true" || s == "yes") {
                std::size_t min_n = 10000;
                if (const char* mn = std::getenv("NEWDB_WHERE_WARN_HEAVY_MIN_ROWS")) {
                    std::size_t v = 0;
                    if (std::from_chars(mn, mn + std::strlen(mn), v).ec == std::errc{} && v > 0) {
                        min_n = v;
                    }
                }
                if (logical_rows >= min_n &&
                    (std::strcmp(mode, "and_ordered_scan") == 0 || std::strcmp(mode, "fallback_scan") == 0 ||
                     std::strcmp(mode, "full_scan_all") == 0)) {
                    std::fprintf(
                        stderr,
                        "[WHERE_WARN] plan=%s logical_rows=%zu: multi-condition or non-indexed path may full-scan the "
                        "heap. Prefer: narrow with indexed/equality (eq sidecar) conditions first, then apply the rest; "
                        "for stable sort use PAGE; split huge tables (time / key range). "
                        "See WRITE_PATH_TUNING_RUNBOOK.md \"Big data and WHERE\".\n",
                        mode,
                        logical_rows);
                }
            }
        }
    }
};


std::string build_conds_signature(const std::vector<WhereCond>& conds) {
    std::ostringstream oss;
    for (std::size_t i = 0; i < conds.size(); ++i) {
        const WhereCond& c = conds[i];
        if (i > 0) {
            oss << "|";
        }
        oss << c.logic_with_prev << "#"
            << c.attr << "#"
            << static_cast<int>(c.op) << "#"
            << c.value;
    }
    return oss.str();
}


std::string build_query_cache_key(const newdb::HeapTable& tbl,
                                  const newdb::TableSchema& schema,
                                  const std::vector<WhereCond>& conds) {
    std::uint64_t id_fp = static_cast<std::uint64_t>(tbl.index_by_id.size());
    std::size_t sample = 0;
    for (const auto& kv : tbl.index_by_id) {
        id_fp ^= (static_cast<std::uint64_t>(kv.first) << (sample % 13));
        id_fp ^= (static_cast<std::uint64_t>(kv.second) << ((sample + 3) % 17));
        if (++sample >= 8) {
            break;
        }
    }
    const std::uint64_t schema_fp = table_stats_schema_fingerprint(schema);
    std::ostringstream oss;
    oss << tbl.name << "@"
        << schema.primary_key << "@"
        << schema_fp << "@"
        << tbl.logical_row_count() << "@"
        << id_fp << "@"
        << build_conds_signature(conds);
    return oss.str();
}


bool query_cache_get(WhereQueryContext& ctx, const std::string& key, std::vector<std::size_t>& out) {
    ctx.cache_lookups.fetch_add(1, std::memory_order_relaxed);
    std::lock_guard<std::mutex> lk(ctx.mu);
    const auto it = ctx.query_cache.find(key);
    if (it == ctx.query_cache.end()) {
        return false;
    }
    ctx.cache_hits.fetch_add(1, std::memory_order_relaxed);
    const auto pos_it = ctx.query_cache_lru_pos.find(key);
    if (pos_it != ctx.query_cache_lru_pos.end()) {
        ctx.query_cache_lru.splice(ctx.query_cache_lru.end(), ctx.query_cache_lru, pos_it->second);
        pos_it->second = std::prev(ctx.query_cache_lru.end());
    }
    out = it->second;
    return true;
}


void query_cache_put(WhereQueryContext& ctx, const std::string& key, std::vector<std::size_t> slots) {
    std::lock_guard<std::mutex> lk(ctx.mu);
    const auto cache_it = ctx.query_cache.find(key);
    if (cache_it == ctx.query_cache.end()) {
        ctx.query_cache_lru.push_back(key);
        ctx.query_cache_lru_pos[key] = std::prev(ctx.query_cache_lru.end());
    } else {
        const auto pos_it = ctx.query_cache_lru_pos.find(key);
        if (pos_it != ctx.query_cache_lru_pos.end()) {
            ctx.query_cache_lru.splice(ctx.query_cache_lru.end(), ctx.query_cache_lru, pos_it->second);
            pos_it->second = std::prev(ctx.query_cache_lru.end());
        }
    }
    ctx.query_cache[key] = std::move(slots);
    while (ctx.query_cache_lru.size() > WhereQueryContext::kMaxQueryCacheEntries) {
        const std::string old = ctx.query_cache_lru.front();
        ctx.query_cache_lru.pop_front();
        ctx.query_cache.erase(old);
        ctx.query_cache_lru_pos.erase(old);
    }
}



