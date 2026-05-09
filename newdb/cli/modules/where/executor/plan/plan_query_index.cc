#include <waterfall/config.h>

#include "cli/modules/where/executor/where.h"
#include "cli/modules/where/executor/internal/query_internal.h"
#include "cli/modules/where/executor/plan/plan_impl_detail.h"
#include "cli/modules/where/executor/plan/plan_impl_internals.h"
#include "cli/modules/where/executor/plan/plan_impl_support.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <limits>
#include <unordered_set>
#include <vector>

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
    where_plan_detail::QueryTraceGuard trace(n, conds.size(), &ctx);
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
        const std::uint64_t qt_est = where_plan_detail::query_temp_est_bytes(n, 1);
        if (!where_plan_impl_try_admit_query_temp(qt_est)) {
            where_policy_set(ctx, true, "query_temp_memory_cap");
            trace.mode = "query_temp_memory_cap";
            trace.rows = 0;
            trace.plan_candidates = 1;
            bump_query_io(0, 0);
            return {};
        }
        where_plan_detail::QueryTempBytesGuard qt_guard(qt_est, &ctx);
        result = visible_slots_for_query(tbl, schema, n);
        query_cache_put(ctx, cache_key, result);
        trace.mode = "full_scan_all";
        trace.rows = result.size();
        trace.plan_candidates = 1;
        bump_query_io(static_cast<std::uint64_t>(result.size()), result.size());
        return result;
    }
    const std::uint64_t qt_est_main = where_plan_detail::query_temp_est_bytes(n, conds.size());
    if (!where_plan_impl_try_admit_query_temp(qt_est_main)) {
        where_policy_set(ctx, true, "query_temp_memory_cap");
        trace.mode = "query_temp_memory_cap";
        trace.rows = 0;
        trace.plan_candidates = 1;
        bump_query_io(0, 0);
        return {};
    }
    where_plan_detail::QueryTempBytesGuard qt_guard_main(qt_est_main, &ctx);
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
                if (!where_plan_impl_row_at_logical_slot(tbl, slot, r)) {
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
                if (!where_plan_impl_row_at_logical_slot(tbl, slot, r)) {
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
            if (!where_plan_impl_row_at_logical_slot(tbl, i, r)) {
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
                if (!where_plan_impl_row_at_logical_slot(tbl, slot, r)) {
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
        if (!where_plan_impl_row_at_logical_slot(tbl, i, r)) {
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
