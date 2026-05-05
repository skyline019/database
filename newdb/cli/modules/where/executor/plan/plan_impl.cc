#include <waterfall/config.h>

#include "cli/modules/where/executor/where.h"
#include "cli/modules/where/executor/internal/query_internal.h"
#include "cli/modules/sidecar/eq/equality_index_sidecar.h"
#include "cli/modules/sidecar/visibility/visibility_checkpoint_sidecar.h"
#include "cli/modules/where/executor/cost/cost_model.h"

#include <algorithm>
#include <atomic>
#include <cmath>
#include <charconv>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <limits>
#include <mutex>
#include <newdb/memory_registry.h>
#include <newdb/row.h>
#include <numeric>
#include <optional>
#include <sstream>
#include <unordered_set>
#include <unordered_map>
#include <map>
#include <list>

namespace {

std::uint64_t where_query_temp_est_bytes(const std::size_t n, const std::size_t cond_count) {
    const std::uint64_t rows = static_cast<std::uint64_t>(n);
    const std::uint64_t cc = static_cast<std::uint64_t>(std::max<std::size_t>(std::size_t{1}, cond_count));
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
            newdb::memory_registry_release(newdb::MemoryKind::QueryTemp, bytes);
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
}

bool row_at_logical_slot(const newdb::HeapTable& tbl, const std::size_t slot, newdb::Row& out) {
    if (tbl.is_heap_storage_backed()) {
        return tbl.decode_heap_slot(slot, out);
    }
    out = tbl.rows[slot];
    return true;
}


std::vector<std::size_t> visible_slots_for_query(const newdb::HeapTable& tbl,
                                                 const newdb::TableSchema& schema,
                                                 const std::size_t n) {
    if (tbl.name.empty()) {
        std::vector<std::size_t> all(n);
        std::iota(all.begin(), all.end(), 0);
        return all;
    }
    std::vector<std::size_t> slots =
        load_or_build_visibility_checkpoint_sidecar(tbl.name + ".bin", schema, tbl);
    return slots;
}


bool collect_single_condition_candidates(const newdb::HeapTable& tbl,
                                         const newdb::TableSchema& schema,
                                         const WhereCond& c,
                                         const std::size_t n,
                                         std::vector<std::size_t>& out_slots,
                                         const char*& trace_mode,
                                         WhereQueryContext* where_obs) {
    if (c.attr == "id") {
        try {
            const int target_id = std::stoi(c.value);
            const auto it = tbl.index_by_id.find(target_id);
            if (it != tbl.index_by_id.end()) {
                if (c.op == CondOp::Eq) {
                    out_slots.push_back(it->second);
                } else {
                    newdb::Row r;
                    if (row_at_logical_slot(tbl, it->second, r) &&
                        row_match_condition(r, schema, c.attr, c.op, c.value)) {
                        out_slots.push_back(it->second);
                    }
                }
            }
            trace_mode = "id_lookup";
            return true;
        } catch (...) {
            return false;
        }
    }

    const std::string& pk = schema.primary_key;
    if (c.attr == pk && !tbl.index_by_pk_value.empty()) {
        const auto it = tbl.index_by_pk_value.find(c.value);
        if (it != tbl.index_by_pk_value.end()) {
            if (c.op == CondOp::Eq) {
                out_slots.push_back(it->second);
            } else {
                newdb::Row r;
                if (row_at_logical_slot(tbl, it->second, r) &&
                    row_match_condition(r, schema, c.attr, c.op, c.value)) {
                    out_slots.push_back(it->second);
                }
            }
        }
        trace_mode = "pk_lookup";
        return true;
    }

    if (c.op == CondOp::Eq && c.attr != "id" && c.attr != pk) {
        const EqLookupResult eq_lookup = lookup_or_build_eq_index_sidecar(
            EqIndexRequest{
                .data_file = tbl.name + ".bin",
                .attr_name = c.attr,
                .table_name = tbl.name,
            },
            schema,
            tbl,
            c.value,
            where_obs);
        if (eq_lookup.used_index) {
            out_slots.reserve(eq_lookup.slots.size());
            for (const std::size_t slot : eq_lookup.slots) {
                if (slot < n) {
                    out_slots.push_back(slot);
                }
            }
            trace_mode = "eq_sidecar";
            return true;
        }
    }
    return false;
}


void maybe_prewarm_eq_sidecars(const newdb::HeapTable& tbl,
                               const newdb::TableSchema& schema,
                               const std::vector<WhereCond>& conds,
                               const std::size_t logical_rows,
                               WhereQueryContext& ctx) {
    if (tbl.name.empty() || logical_rows < 100000) {
        return;
    }
    for (const WhereCond& c : conds) {
        if (c.op != CondOp::Eq || c.attr == "id" || c.attr == schema.primary_key) {
            continue;
        }
        const std::string key = tbl.name + "|" + c.attr;
        {
            std::lock_guard<std::mutex> lk(ctx.mu);
            if (ctx.eq_sidecar_prewarmed.find(key) != ctx.eq_sidecar_prewarmed.end()) {
                continue;
            }
        }
        (void)lookup_or_build_eq_index_sidecar(
            EqIndexRequest{
                .data_file = tbl.name + ".bin",
                .attr_name = c.attr,
                .table_name = tbl.name,
            },
            schema,
            tbl,
            c.value,
            &ctx);
        std::lock_guard<std::mutex> lk(ctx.mu);
        ctx.eq_sidecar_prewarmed.insert(std::move(key));
    }
}


bool is_single_cond_index_friendly(const WhereCond& c, const newdb::TableSchema& schema) {
    if (c.attr == "id") {
        return true;
    }
    if (c.attr == schema.primary_key) {
        return true;
    }
    return c.op == CondOp::Eq;
}


std::size_t estimated_cond_cost(const WhereCond& c, const newdb::TableSchema& schema) {
    // Lower means better seed candidate for AND chains.
    if (c.attr == "id" && c.op == CondOp::Eq) return 0;
    if (c.attr == schema.primary_key && c.op == CondOp::Eq) return 1;
    if (c.op == CondOp::Eq) return 2;
    if (c.op == CondOp::Ge || c.op == CondOp::Gt || c.op == CondOp::Le || c.op == CondOp::Lt) return 3;
    if (c.op == CondOp::Ne) return 4;
    return 5;
}

bool query_cost_model_env_enabled() {
    const char* e = std::getenv("NEWDB_QUERY_COST_MODEL");
    return e != nullptr && e[0] == '1' && e[1] == '\0';
}

std::size_t estimated_cond_output_rows(const WhereCond& c,
                                       const newdb::TableSchema& schema,
                                       const std::size_t n,
                                       const TableStats* stats) {
    if (c.attr == "id" && c.op == CondOp::Eq) {
        return 1;
    }
    if (c.attr == schema.primary_key && c.op == CondOp::Eq) {
        return 1;
    }
    if (c.op == CondOp::Eq && stats != nullptr && n > 0) {
        const double sel = eq_selectivity_from_stats(stats, c.attr, n);
        if (sel > 0.0) {
            const long double est =
                static_cast<long double>(n) * static_cast<long double>(sel);
            const std::size_t rows = static_cast<std::size_t>(est < 1.0L ? 1.0L : est);
            return std::min(rows, n);
        }
    }
    if (stats != nullptr && n > 0 &&
        (c.op == CondOp::Ge || c.op == CondOp::Gt || c.op == CondOp::Le || c.op == CondOp::Lt)) {
        const double span = range_selectivity_from_stats(stats, c.attr, n);
        if (span > 0.0) {
            const long double est =
                static_cast<long double>(n) * static_cast<long double>(span);
            const std::size_t rows = static_cast<std::size_t>(est < 1.0L ? 1.0L : est);
            return std::min(rows, n);
        }
    }
    if (stats != nullptr && n > 0 && c.op == CondOp::Ne) {
        const double eq = eq_selectivity_from_stats(stats, c.attr, n);
        if (eq > 0.0) {
            const double span = std::min(0.49, std::max(0.05, 1.0 - eq * 0.95));
            const long double est = static_cast<long double>(n) * static_cast<long double>(span);
            const std::size_t rows = static_cast<std::size_t>(est < 1.0L ? 1.0L : est);
            return std::min(rows, n);
        }
    }
    if (stats != nullptr && n > 0 && c.op == CondOp::Contains) {
        const double eq = eq_selectivity_from_stats(stats, c.attr, n);
        if (eq > 0.0) {
            const double span = std::min(0.45, std::max(0.08, std::sqrt(eq)));
            const long double est = static_cast<long double>(n) * static_cast<long double>(span);
            const std::size_t rows = static_cast<std::size_t>(est < 1.0L ? 1.0L : est);
            return std::min(rows, n);
        }
    }
    return n;
}

std::size_t plan_metric_for_cond(const WhereCond& c,
                                 const newdb::TableSchema& schema,
                                 const std::size_t n,
                                 const TableStats* stats) {
    if (query_cost_model_env_enabled() && stats != nullptr) {
        return estimated_cond_output_rows(c, schema, n, stats);
    }
    return estimated_cond_cost(c, schema);
}


bool collect_single_condition_candidates_sanitized(const newdb::HeapTable& tbl,
                                                   const newdb::TableSchema& schema,
                                                   const WhereCond& c,
                                                   const std::size_t n,
                                                   std::vector<std::size_t>& out_slots,
                                                   WhereQueryContext* where_obs) {
    std::vector<std::size_t> raw_slots;
    const char* mode = "";
    if (!collect_single_condition_candidates(tbl, schema, c, n, raw_slots, mode, where_obs)) {
        return false;
    }
    out_slots.clear();
    out_slots.reserve(raw_slots.size());
    std::unordered_set<std::size_t> seen;
    seen.reserve(raw_slots.size());
    for (const std::size_t slot : raw_slots) {
        if (slot < n && seen.insert(slot).second) {
            out_slots.push_back(slot);
        }
    }
    return true;
}


std::vector<std::size_t> sanitize_sort_slots(const std::vector<std::size_t>& in, const std::size_t n) {
    std::vector<std::size_t> out;
    out.reserve(in.size());
    for (const std::size_t slot : in) {
        if (slot < n) {
            out.push_back(slot);
        }
    }
    std::sort(out.begin(), out.end());
    out.erase(std::unique(out.begin(), out.end()), out.end());
    return out;
}


std::vector<std::size_t> intersect_sorted_slots(const std::vector<std::size_t>& a,
                                                const std::vector<std::size_t>& b) {
    std::vector<std::size_t> out;
    out.reserve(std::min(a.size(), b.size()));
    std::size_t i = 0;
    std::size_t j = 0;
    while (i < a.size() && j < b.size()) {
        if (a[i] == b[j]) {
            out.push_back(a[i]);
            ++i;
            ++j;
        } else if (a[i] < b[j]) {
            ++i;
        } else {
            ++j;
        }
    }
    return out;
}

std::size_t where_estimate_scan_rows(const newdb::HeapTable& tbl,
                                      const newdb::TableSchema& schema,
                                      const std::vector<WhereCond>& conds,
                                      WhereQueryContext* ctx_ptr) {
    (void)schema;
    const std::size_t n = tbl.logical_row_count();
    if (conds.empty()) {
        return n;
    }
    long double est = static_cast<long double>(n);
    for (const auto& c : conds) {
        long double factor = 1.0L;
        if (c.attr == "id" && c.op == CondOp::Eq) {
            factor = 0.0001L;
        } else if (c.op == CondOp::Eq) {
            factor = 0.08L;
            if (const char* raw = std::getenv("NEWDB_QUERY_USE_TABLE_STATS")) {
                if (raw[0] == '1' && raw[1] == '\0' && ctx_ptr != nullptr && ctx_ptr->query_stats_hint != nullptr) {
                    const double sel = eq_selectivity_from_stats(ctx_ptr->query_stats_hint, c.attr, n);
                    if (sel > 0.0) {
                        factor = static_cast<long double>(sel);
                    }
                }
            }
        } else if (c.op == CondOp::Contains) {
            factor = 0.35L;
            if (const char* raw = std::getenv("NEWDB_QUERY_USE_TABLE_STATS")) {
                if (raw[0] == '1' && raw[1] == '\0' && ctx_ptr != nullptr && ctx_ptr->query_stats_hint != nullptr) {
                    const double span = range_selectivity_from_stats(ctx_ptr->query_stats_hint, c.attr, n);
                    if (span > 0.0) {
                        factor = static_cast<long double>(span);
                    }
                }
            }
        } else if (c.op == CondOp::Ge || c.op == CondOp::Gt || c.op == CondOp::Le || c.op == CondOp::Lt) {
            factor = 0.45L;
            if (const char* raw = std::getenv("NEWDB_QUERY_USE_TABLE_STATS")) {
                if (raw[0] == '1' && raw[1] == '\0' && ctx_ptr != nullptr && ctx_ptr->query_stats_hint != nullptr) {
                    const double span = range_selectivity_from_stats(ctx_ptr->query_stats_hint, c.attr, n);
                    if (span > 0.0) {
                        factor = static_cast<long double>(span);
                    }
                }
            }
        } else if (c.op == CondOp::Ne) {
            factor = 0.45L;
            if (const char* raw = std::getenv("NEWDB_QUERY_USE_TABLE_STATS")) {
                if (raw[0] == '1' && raw[1] == '\0' && ctx_ptr != nullptr && ctx_ptr->query_stats_hint != nullptr) {
                    const double eq = eq_selectivity_from_stats(ctx_ptr->query_stats_hint, c.attr, n);
                    if (eq > 0.0) {
                        factor = static_cast<long double>(std::min(0.49, std::max(0.05, 1.0 - eq * 0.95)));
                    }
                }
            }
        } else {
            factor = 0.45L;
        }
        if (c.logic_with_prev == "OR") {
            est = std::min<long double>(static_cast<long double>(n), est + static_cast<long double>(n) * factor);
        } else {
            est *= factor;
        }
    }
    if (est < 1.0L) est = 1.0L;
    if (est > static_cast<long double>(n)) est = static_cast<long double>(n);
    return static_cast<std::size_t>(est);
}

std::vector<std::size_t> query_with_index(const newdb::HeapTable& tbl,
                                          const newdb::TableSchema& schema,
                                          const std::vector<WhereCond>& conds,
                                          WhereQueryContext* ctx_ptr) {
    std::vector<std::size_t> result;
    WhereQueryContext& ctx = (ctx_ptr != nullptr) ? *ctx_ptr : default_where_context();
    where_policy_set(ctx, false, "");
    ctx.query_count.fetch_add(1, std::memory_order_relaxed);
    const auto bump_query_io = [&](const std::uint64_t rows_scanned, const std::size_t rows_returned) {
        ctx.query_rows_scanned_total.fetch_add(rows_scanned, std::memory_order_relaxed);
        ctx.query_rows_returned_total.fetch_add(static_cast<std::uint64_t>(rows_returned),
                                                std::memory_order_relaxed);
    };

    const std::size_t n = tbl.logical_row_count();
    const bool has_or = std::any_of(conds.begin(), conds.end(), [](const WhereCond& c) {
        return c.logic_with_prev == "OR";
    });
    const auto estimate_scan_rows = [&](const std::vector<WhereCond>& in) -> std::size_t {
        return where_estimate_scan_rows(tbl, schema, in, ctx_ptr);
    };
    QueryTraceGuard trace(n, conds.size(), &ctx);
    const std::string cache_key = build_query_cache_key(tbl, schema, conds);
    if (query_cache_get(ctx, cache_key, result)) {
        const bool policy_bypass = (conds.size() == 1 && is_single_cond_index_friendly(conds[0], schema));
        if (!policy_bypass &&
            !where_policy_gate("cache_hit", n, conds.size(), estimate_scan_rows(conds), has_or, ctx)) {
            trace.mode = "policy_reject";
            trace.rows = 0;
            trace.plan_candidates = 1;
            bump_query_io(0, 0);
            return {};
        }
        trace.mode = "cache_hit";
        trace.rows = result.size();
        trace.plan_candidates = 1;
        bump_query_io(0, result.size());
        return result;
    }

    if (conds.empty()) {
        const std::uint64_t qt_est = where_query_temp_est_bytes(n, 1);
        if (!newdb::memory_registry_try_admit(newdb::MemoryKind::QueryTemp, qt_est)) {
            where_policy_set(ctx, true, "query_temp_memory_cap");
            trace.mode = "query_temp_memory_cap";
            trace.rows = 0;
            trace.plan_candidates = 1;
            bump_query_io(0, 0);
            return {};
        }
        QueryTempBytesGuard qt_guard(qt_est, &ctx);
        result = visible_slots_for_query(tbl, schema, n);
        query_cache_put(ctx, cache_key, result);
        trace.mode = "full_scan_all";
        trace.rows = result.size();
        trace.plan_candidates = 1;
        bump_query_io(static_cast<std::uint64_t>(result.size()), result.size());
        return result;
    }
    const std::uint64_t qt_est_main = where_query_temp_est_bytes(n, conds.size());
    if (!newdb::memory_registry_try_admit(newdb::MemoryKind::QueryTemp, qt_est_main)) {
        where_policy_set(ctx, true, "query_temp_memory_cap");
        trace.mode = "query_temp_memory_cap";
        trace.rows = 0;
        trace.plan_candidates = 1;
        bump_query_io(0, 0);
        return {};
    }
    QueryTempBytesGuard qt_guard_main(qt_est_main, &ctx);
    maybe_prewarm_eq_sidecars(tbl, schema, conds, n, ctx);

    if (conds.size() == 1) {
        const WhereCond& c = conds[0];
        const char* single_mode = "single_scan";
        if (collect_single_condition_candidates(tbl, schema, c, n, result, single_mode, &ctx)) {
            if (std::strcmp(single_mode, "eq_sidecar") == 0) {
                ctx.plan_eq_sidecar_count.fetch_add(1, std::memory_order_relaxed);
            } else if (std::strcmp(single_mode, "id_lookup") == 0 || std::strcmp(single_mode, "pk_lookup") == 0) {
                ctx.plan_id_pk_count.fetch_add(1, std::memory_order_relaxed);
            }
            query_cache_put(ctx, cache_key, result);
            trace.mode = single_mode;
            trace.rows = result.size();
            trace.plan_candidates = 1;
            bump_query_io(static_cast<std::uint64_t>(result.size()), result.size());
            return result;
        }
    }
    const std::vector<PreparedCond> prepared_conds = prepare_conditions(schema, conds);

    // Batch optimization for multi-condition AND:
    // pick the most selective seed condition and filter remaining conditions on seed candidates.
    if (conds.size() > 1 && all_and_chain(conds)) {
        // Two-condition fast path: intersect indexed candidates directly.
        if (conds.size() == 2 &&
            is_single_cond_index_friendly(conds[0], schema) &&
            is_single_cond_index_friendly(conds[1], schema)) {
            std::vector<std::size_t> a_raw;
            std::vector<std::size_t> b_raw;
            const char* mode_a = "";
            const char* mode_b = "";
            const bool a_ok =
                collect_single_condition_candidates(tbl, schema, conds[0], n, a_raw, mode_a, &ctx);
            const bool b_ok =
                collect_single_condition_candidates(tbl, schema, conds[1], n, b_raw, mode_b, &ctx);
            if (!a_ok || !b_ok) {
                goto and_fast_path_fallback;
            }
            const std::vector<std::size_t> a = sanitize_sort_slots(a_raw, n);
            const std::vector<std::size_t> b = sanitize_sort_slots(b_raw, n);
            const std::vector<std::size_t> inter = intersect_sorted_slots(a, b);
            result.reserve(inter.size());
            newdb::Row r;
            for (const std::size_t slot : inter) {
                if (!row_at_logical_slot(tbl, slot, r)) {
                    continue;
                }
                if (row_match_multi_conditions_prepared(r, schema, prepared_conds)) {
                    result.push_back(slot);
                }
            }
            query_cache_put(ctx, cache_key, result);
            ctx.plan_eq_sidecar_count.fetch_add(1, std::memory_order_relaxed);
            trace.mode = "and_intersect_2";
            trace.rows = result.size();
            trace.plan_candidates = 3;
            bump_query_io(static_cast<std::uint64_t>(inter.size()), result.size());
            return result;
        }
and_fast_path_fallback:

        std::size_t seed = 0;
        std::size_t best_cost = std::numeric_limits<std::size_t>::max();
        std::size_t best_card = std::numeric_limits<std::size_t>::max();
        std::vector<std::vector<std::size_t>> seed_candidates(conds.size());
        std::vector<bool> seed_candidates_ready(conds.size(), false);
        for (std::size_t i = 0; i < conds.size(); ++i) {
            if (!is_single_cond_index_friendly(conds[i], schema)) {
                continue;
            }
            const std::size_t cost =
                plan_metric_for_cond(conds[i], schema, n, ctx.query_stats_hint);
            std::vector<std::size_t> candidates;
            if (!collect_single_condition_candidates_sanitized(tbl, schema, conds[i], n, candidates, &ctx)) {
                continue;
            }
            const std::size_t card_value = candidates.size();
            seed_candidates[i] = std::move(candidates);
            seed_candidates_ready[i] = true;
            if (cost < best_cost || (cost == best_cost && card_value < best_card)) {
                best_cost = cost;
                best_card = card_value;
                seed = i;
            }
        }
        if (best_cost != std::numeric_limits<std::size_t>::max()) {
            std::vector<std::size_t> eval_order;
            eval_order.reserve(conds.size());
            for (std::size_t i = 0; i < conds.size(); ++i) {
                if (i != seed) {
                    eval_order.push_back(i);
                }
            }
            std::sort(eval_order.begin(), eval_order.end(), [&](const std::size_t a, const std::size_t b) {
                return plan_metric_for_cond(conds[a], schema, n, ctx.query_stats_hint) <
                       plan_metric_for_cond(conds[b], schema, n, ctx.query_stats_hint);
            });

            if (!seed_candidates_ready[seed]) {
                query_cache_put(ctx, cache_key, result);
                trace.mode = "and_seed_filter";
                trace.rows = result.size();
                trace.plan_candidates = 1;
                bump_query_io(0, result.size());
                return result;
            }
            const std::vector<std::size_t>& seed_slots = seed_candidates[seed];
            result.reserve(seed_slots.size());
            newdb::Row r;
            for (const std::size_t slot : seed_slots) {
                if (slot >= n) {
                    continue;
                }
                if (!row_at_logical_slot(tbl, slot, r)) {
                    continue;
                }
                if (row_match_all_conditions_ordered_prepared(r, schema, prepared_conds, eval_order, seed)) {
                    result.push_back(slot);
                }
            }
            query_cache_put(ctx, cache_key, result);
            ctx.plan_eq_sidecar_count.fetch_add(1, std::memory_order_relaxed);
            trace.mode = "and_seed_filter";
            trace.rows = result.size();
            {
                std::uint32_t c = 1;
                for (std::size_t i = 0; i < conds.size(); ++i) {
                    if (seed_candidates_ready[i]) {
                        ++c;
                    }
                }
                trace.plan_candidates = c;
            }
            bump_query_io(static_cast<std::uint64_t>(seed_slots.size()), result.size());
            return result;
        }

        // No index-friendly seed, still keep ordered short-circuit evaluation.
        std::vector<std::size_t> eval_order;
        eval_order.reserve(conds.size());
        for (std::size_t i = 0; i < conds.size(); ++i) {
            eval_order.push_back(i);
        }
        std::sort(eval_order.begin(), eval_order.end(), [&](const std::size_t a, const std::size_t b) {
            return plan_metric_for_cond(conds[a], schema, n, ctx.query_stats_hint) <
                   plan_metric_for_cond(conds[b], schema, n, ctx.query_stats_hint);
        });

        if (!where_policy_gate("and_ordered_scan", n, conds.size(), estimate_scan_rows(conds), has_or, ctx)) {
            trace.mode = "policy_block";
            trace.rows = 0;
            trace.plan_candidates = 1;
            bump_query_io(0, 0);
            return {};
        }
        trace.plan_candidates = 2;
        const std::vector<std::size_t> visible_slots = visible_slots_for_query(tbl, schema, n);
        result.reserve(visible_slots.size());
        newdb::Row r;
        for (const std::size_t i : visible_slots) {
            if (!row_at_logical_slot(tbl, i, r)) {
                continue;
            }
            if (row_match_all_conditions_ordered_prepared(r, schema, prepared_conds, eval_order, std::numeric_limits<std::size_t>::max())) {
                result.push_back(i);
            }
        }
        query_cache_put(ctx, cache_key, result);
        trace.mode = "and_ordered_scan";
        trace.rows = result.size();
        bump_query_io(static_cast<std::uint64_t>(visible_slots.size()), result.size());
        return result;
    }

    // Batch optimization for pure OR chain:
    // union indexed single-condition candidates first, then verify with full OR expression.
    if (conds.size() > 1 && all_or_chain(conds)) {
        bool all_indexable = true;
        for (const auto& c : conds) {
            if (!is_single_cond_index_friendly(c, schema)) {
                all_indexable = false;
                break;
            }
        }
        if (all_indexable) {
            std::unordered_set<std::size_t> union_slots;
            for (const auto& c : conds) {
                std::vector<WhereCond> one{c};
                std::vector<std::size_t> one_slots;
                const char* one_mode = "";
                if (!collect_single_condition_candidates(tbl, schema, c, n, one_slots, one_mode, &ctx)) {
                    all_indexable = false;
                    break;
                }
                union_slots.insert(one_slots.begin(), one_slots.end());
            }
            if (!all_indexable) {
                goto or_chain_fallback;
            }
            trace.or_union_size = union_slots.size();
            std::vector<std::size_t> ordered_slots;
            ordered_slots.reserve(union_slots.size());
            for (const std::size_t slot : union_slots) {
                if (slot < n) {
                    ordered_slots.push_back(slot);
                }
            }
            std::sort(ordered_slots.begin(), ordered_slots.end());

            result.reserve(ordered_slots.size());
            newdb::Row r;
            for (const std::size_t slot : ordered_slots) {
                if (!row_at_logical_slot(tbl, slot, r)) {
                    continue;
                }
                if (row_match_multi_conditions_prepared(r, schema, prepared_conds)) {
                    result.push_back(slot);
                }
            }
            trace.or_verified_size = result.size();
            query_cache_put(ctx, cache_key, result);
            trace.mode = "or_union_verify";
            trace.rows = result.size();
            {
                const std::uint32_t k = static_cast<std::uint32_t>(conds.size());
                trace.plan_candidates = k == 0u ? 1u : k;
            }
            bump_query_io(static_cast<std::uint64_t>(ordered_slots.size()), result.size());
            return result;
        }
    }
or_chain_fallback:

    if (!where_policy_gate("fallback_scan", n, conds.size(), estimate_scan_rows(conds), has_or, ctx)) {
        trace.mode = "policy_block";
        trace.rows = 0;
        trace.plan_candidates = 1;
        bump_query_io(0, 0);
        return {};
    }
    ctx.fallback_scans.fetch_add(1, std::memory_order_relaxed);
    ctx.plan_fallback_count.fetch_add(1, std::memory_order_relaxed);
    {
        const auto checks = ctx.policy_checks.load(std::memory_order_relaxed);
        const auto rejects = ctx.policy_rejects.load(std::memory_order_relaxed);
        const auto fallback = ctx.fallback_scans.load(std::memory_order_relaxed);
        const auto est_total = ctx.estimated_scan_rows_total.load(std::memory_order_relaxed);
        const auto est_samples = ctx.estimated_scan_rows_samples.load(std::memory_order_relaxed);
        const std::uint64_t est_avg = est_samples == 0 ? 0 : (est_total / est_samples);
        std::fprintf(stderr,
                     "[WHERE_POLICY] fallback plan=fallback_scan logical_rows=%zu checks=%llu rejects=%llu fallback_scans=%llu est_scan_rows_avg=%llu\n",
                     n,
                     static_cast<unsigned long long>(checks),
                     static_cast<unsigned long long>(rejects),
                     static_cast<unsigned long long>(fallback),
                     static_cast<unsigned long long>(est_avg));
    }
    const std::vector<std::size_t> visible_slots = visible_slots_for_query(tbl, schema, n);
    result.reserve(visible_slots.size());
    newdb::Row r;
    for (const std::size_t i : visible_slots) {
        if (!row_at_logical_slot(tbl, i, r)) {
            continue;
        }
        if (row_match_multi_conditions_prepared(r, schema, prepared_conds)) {
            result.push_back(i);
        }
    }
    query_cache_put(ctx, cache_key, result);
    trace.mode = "fallback_scan";
    trace.rows = result.size();
    trace.plan_candidates = 2;
    bump_query_io(static_cast<std::uint64_t>(visible_slots.size()), result.size());
    return result;
}


std::vector<std::size_t> build_candidate_slots(const newdb::HeapTable& tbl,
                                               const newdb::TableSchema& schema,
                                               const std::vector<WhereCond>& conds,
                                               WhereQueryContext* ctx) {
    return query_with_index(tbl, schema, conds, ctx);
}

std::vector<PlanCandidate> where_build_plan_candidates(const newdb::HeapTable& tbl,
                                                       const newdb::TableSchema& schema,
                                                       const std::vector<WhereCond>& conds,
                                                       const TableStats* stats_hint) {
    std::map<std::string, double> best;
    const std::size_t n = tbl.logical_row_count();
    const std::uint64_t qt_est_plan = where_query_temp_est_bytes(n, conds.empty() ? 1 : conds.size());
    if (!newdb::memory_registry_try_admit(newdb::MemoryKind::QueryTemp, qt_est_plan)) {
        std::vector<PlanCandidate> out;
        PlanCandidate pc;
        pc.id = "heap_scan";
        pc.estimated_cost = newdb::where_cost::heap_full_scan_cost(n);
        pc.cost.estimated_rows = static_cast<double>(n);
        pc.rationale = "query_temp_memory_cap_minimal_plan";
        out.push_back(std::move(pc));
        return out;
    }
    QueryTempBytesGuard qt_guard_plan(qt_est_plan, nullptr);
    best["heap_scan"] = newdb::where_cost::heap_full_scan_cost(n);
    if (conds.size() == 1) {
        const WhereCond& c = conds[0];
        if (c.attr == "id" && c.op == CondOp::Eq) {
            best["id_lookup"] = newdb::where_cost::pk_point_lookup_cost();
        }
        const std::string& pk = schema.primary_key;
        if (!pk.empty() && c.attr == pk && c.op == CondOp::Eq) {
            best["pk_lookup"] = newdb::where_cost::pk_point_lookup_cost();
        }
        if (c.op == CondOp::Eq && c.attr != "id" && c.attr != pk) {
            double sel = 0.08;
            if (stats_hint != nullptr) {
                const double s = eq_selectivity_from_stats(stats_hint, c.attr, n);
                if (s > 0.0) sel = s;
            }
            best["eq_sidecar"] = newdb::where_cost::eq_sidecar_probe_cost(n, sel);
        }
    }
    std::vector<PlanCandidate> out;
    out.reserve(best.size());
    for (const auto& kv : best) {
        PlanCandidate pc;
        pc.id = kv.first;
        pc.estimated_cost = kv.second;
        if (kv.first == "heap_scan") {
            pc.cost.estimated_rows = static_cast<double>(n);
            pc.rationale = "full_heap_visibility_scan";
        } else if (kv.first == "id_lookup" || kv.first == "pk_lookup") {
            pc.cost.estimated_rows = 1.0;
            pc.rationale = "unique_pk_point_lookup";
        } else if (kv.first == "eq_sidecar") {
            pc.cost.estimated_rows = std::max(1.0, kv.second);
            pc.rationale = "equality_sidecar_ndv_estimate";
        } else {
            pc.cost.estimated_rows = static_cast<double>(n);
            pc.rationale = "generic";
        }
        out.push_back(std::move(pc));
    }
    std::sort(out.begin(), out.end(), [](const PlanCandidate& a, const PlanCandidate& b) {
        if (a.estimated_cost != b.estimated_cost) {
            return a.estimated_cost < b.estimated_cost;
        }
        return a.id < b.id;
    });
    if (!out.empty()) {
        out.front().rationale += "|chosen_lowest_cost";
    }
    return out;
}



