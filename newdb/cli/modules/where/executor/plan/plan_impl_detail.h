#pragma once

#include "cli/modules/where/executor/plan/plan_impl_support.h"
#include "cli/modules/where/executor/where.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <mutex>
#include <string>

namespace where_plan_detail {

inline std::uint64_t query_temp_est_bytes(const std::size_t n, const std::size_t cond_count) {
    const std::uint64_t rows = static_cast<std::uint64_t>(n);
    const std::uint64_t cc =
        static_cast<std::uint64_t>(std::max<std::size_t>(std::size_t{1}, cond_count));
    const std::uint64_t v =
        rows * static_cast<std::uint64_t>(sizeof(std::size_t)) + cc * static_cast<std::uint64_t>(256);
    constexpr std::uint64_t kCap = (std::uint64_t{1} << 40);
    return v > kCap ? kCap : v;
}

struct QueryTempBytesGuard {
    std::uint64_t bytes{0};
    WhereQueryContext* ctx{nullptr};
    explicit QueryTempBytesGuard(std::uint64_t b, WhereQueryContext* c) : bytes(b), ctx(c) {
        if (ctx != nullptr) {
            ctx->query_temp_reserved_bytes.store(bytes, std::memory_order_relaxed);
        }
    }
    ~QueryTempBytesGuard() {
        if (bytes > 0) {
            where_plan_impl_release_query_temp(bytes);
        }
        if (ctx != nullptr) {
            ctx->query_temp_reserved_bytes.store(0, std::memory_order_relaxed);
        }
    }
    QueryTempBytesGuard(const QueryTempBytesGuard&) = delete;
    QueryTempBytesGuard& operator=(const QueryTempBytesGuard&) = delete;
};

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
    std::uint32_t plan_candidates{1};

    QueryTraceGuard(const std::size_t n, const std::size_t conds, WhereQueryContext* ctx)
        : logical_rows(n), cond_count(conds), context(ctx) {
        const char* v = std::getenv("NEWDB_QUERY_TRACE");
        enabled = (v != nullptr && std::string(v) == "1");
    }

    ~QueryTraceGuard() {
        if (context != nullptr) {
            std::lock_guard<std::mutex> lk(context->mu);
            context->last_plan_id = mode;
            const std::uint32_t store_pc = plan_candidates == 0u ? 1u : plan_candidates;
            context->last_plan_candidates_considered.store(store_pc, std::memory_order_relaxed);
        }
        if (enabled) {
            const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start).count();
            std::fprintf(stderr,
                         "[QUERY_TRACE] mode=%s conds=%zu rows=%zu logical=%zu or_union=%zu or_verified=%zu cache_hits=%llu cache_lookups=%llu elapsed_us=%lld\n",
                         mode, cond_count, rows, logical_rows, or_union_size, or_verified_size,
                         static_cast<unsigned long long>(context ? context->cache_hits.load(std::memory_order_relaxed) : 0),
                         static_cast<unsigned long long>(context ? context->cache_lookups.load(std::memory_order_relaxed) : 0),
                         static_cast<long long>(elapsed));
        }
    }
};

}  // namespace where_plan_detail
