#include <waterfall/config.h>

#include "cli/modules/where/executor/where.h"
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

std::string where_policy_mode() {
    const char* env = std::getenv("NEWDB_WHERE_POLICY_MODE");
    if (env == nullptr) {
        return "ratelimit";
    }
    std::string v(env);
    for (char& c : v) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    if (v == "off" || v == "warn" || v == "ratelimit" || v == "reject") {
        return v;
    }
    return "ratelimit";
}


std::size_t where_policy_min_rows() {
    std::size_t min_rows = 5000;
    if (const char* env = std::getenv("NEWDB_WHERE_POLICY_MIN_ROWS")) {
        std::size_t v = 0;
        if (std::from_chars(env, env + std::strlen(env), v).ec == std::errc{} && v > 0) {
            min_rows = v;
        }
    }
    return min_rows;
}


std::size_t where_policy_qps_limit(const std::size_t logical_rows, const std::size_t cond_count) {
    std::size_t qps = logical_rows >= 1000000 ? 1 : 2;
    if (cond_count >= 4) {
        qps = std::min<std::size_t>(qps, 1);
    }
    if (const char* env = std::getenv("NEWDB_WHERE_RATE_LIMIT_QPS")) {
        std::size_t v = 0;
        if (std::from_chars(env, env + std::strlen(env), v).ec == std::errc{}) {
            qps = v;
        }
    }
    return qps;
}


std::size_t where_policy_heap_scan_budget_rows() {
    if (const char* env = std::getenv("NEWDB_WHERE_HEAP_SCAN_BUDGET_ROWS")) {
        std::size_t v = 0;
        if (std::from_chars(env, env + std::strlen(env), v).ec == std::errc{} && v > 0) {
            return v;
        }
    }
    return 0;
}

std::size_t where_policy_scan_cap_rows(const std::size_t logical_rows,
                                       const std::size_t cond_count,
                                       const bool has_or) {
    long double ratio = 0.85L;
    if (logical_rows >= 1000000) {
        ratio = has_or ? 0.45L : 0.60L;
    } else if (logical_rows >= 300000) {
        ratio = has_or ? 0.55L : 0.70L;
    } else if (logical_rows >= 100000) {
        ratio = has_or ? 0.65L : 0.78L;
    }
    if (cond_count >= 4) {
        ratio *= 0.9L;
    }
    if (const char* env = std::getenv("NEWDB_WHERE_SCAN_CAP_RATIO_PCT")) {
        std::size_t pct = 0;
        if (std::from_chars(env, env + std::strlen(env), pct).ec == std::errc{} && pct > 0 && pct <= 100) {
            ratio = static_cast<long double>(pct) / 100.0L;
        }
    }
    const auto cap = static_cast<std::size_t>(static_cast<long double>(logical_rows) * ratio);
    return std::max<std::size_t>(1, cap);
}


void where_policy_set(WhereQueryContext& ctx, const bool blocked, const std::string& msg) {
    std::lock_guard<std::mutex> lk(ctx.mu);
    ctx.policy.blocked = blocked;
    ctx.policy.message = msg;
}


bool where_policy_gate(const char* plan,
                       const std::size_t logical_rows,
                       const std::size_t cond_count,
                       const std::size_t estimated_scan_rows,
                       const bool has_or,
                       WhereQueryContext& ctx) {
    ctx.policy_checks.fetch_add(1, std::memory_order_relaxed);
    ctx.estimated_scan_rows_total.fetch_add(static_cast<std::uint64_t>(estimated_scan_rows), std::memory_order_relaxed);
    ctx.estimated_scan_rows_samples.fetch_add(1, std::memory_order_relaxed);
    const std::string mode = where_policy_mode();
    if (mode == "off" || logical_rows < where_policy_min_rows()) {
        where_policy_set(ctx, false, "");
        return true;
    }
    std::ostringstream oss;
    oss << "plan=" << plan << " logical_rows=" << logical_rows << " conds=" << cond_count
        << " heavy path detected; prefer index-friendly equality narrowing first";
    if (mode == "warn") {
        std::fprintf(stderr, "[WHERE_POLICY] warn %s\n", oss.str().c_str());
        where_policy_set(ctx, false, "");
        return true;
    }
    if (mode == "reject") {
        ctx.policy_rejects.fetch_add(1, std::memory_order_relaxed);
        where_policy_set(ctx, true, oss.str());
        return false;
    }
    const std::size_t scan_cap = where_policy_scan_cap_rows(logical_rows, cond_count, has_or);
    const std::size_t heap_budget = where_policy_heap_scan_budget_rows();
    const std::size_t effective_cap =
        (heap_budget > 0) ? (std::min)(scan_cap, heap_budget) : scan_cap;
    if (heap_budget > 0 && effective_cap < scan_cap) {
        ctx.where_heap_scan_budget_binding_events.fetch_add(1, std::memory_order_relaxed);
    }
    if (estimated_scan_rows >= effective_cap) {
        ctx.policy_rejects.fetch_add(1, std::memory_order_relaxed);
        std::fprintf(stderr,
                     "[WHERE_POLICY] reject plan=%s logical_rows=%zu conds=%zu has_or=%d est_scan_rows=%zu "
                     "scan_cap=%zu heap_budget=%zu effective_cap=%zu reason=estimated_scan_too_wide\n",
                     plan,
                     logical_rows,
                     cond_count,
                     has_or ? 1 : 0,
                     estimated_scan_rows,
                     scan_cap,
                     heap_budget,
                     effective_cap);
        where_policy_set(ctx, true, oss.str() + " (estimated scan too wide)");
        return false;
    }
    const std::size_t qps = where_policy_qps_limit(logical_rows, cond_count);
    if (qps == 0) {
        where_policy_set(ctx, false, "");
        return true;
    }
    const auto now_sec = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
    {
        std::lock_guard<std::mutex> lk(ctx.mu);
        if (now_sec != ctx.policy.window_sec) {
            ctx.policy.window_sec = now_sec;
            ctx.policy.window_count = 0;
        }
        ++ctx.policy.window_count;
        if (ctx.policy.window_count > qps) {
            ctx.policy_rejects.fetch_add(1, std::memory_order_relaxed);
            std::fprintf(stderr,
                         "[WHERE_POLICY] reject plan=%s logical_rows=%zu conds=%zu has_or=%d est_scan_rows=%zu qps=%zu reason=rate_limited\n",
                         plan,
                         logical_rows,
                         cond_count,
                         has_or ? 1 : 0,
                         estimated_scan_rows,
                         qps);
            ctx.policy.blocked = true;
            ctx.policy.message = oss.str() + " (rate-limited)";
            return false;
        }
        ctx.policy.blocked = false;
        ctx.policy.message.clear();
    }
    return true;
}


bool where_policy_last_blocked(const WhereQueryContext* ctx_ptr) {
    const WhereQueryContext& ctx = (ctx_ptr != nullptr) ? *ctx_ptr : default_where_context();
    std::lock_guard<std::mutex> lk(ctx.mu);
    return ctx.policy.blocked;
}


std::string where_policy_last_message(const WhereQueryContext* ctx_ptr) {
    const WhereQueryContext& ctx = (ctx_ptr != nullptr) ? *ctx_ptr : default_where_context();
    std::lock_guard<std::mutex> lk(ctx.mu);
    return ctx.policy.message;
}


