#include <waterfall/config.h>

#include "cli/modules/where/executor/plan/plan_impl_internals.h"
#include "cli/modules/where/executor/internal/query_internal.h"
#include "cli/modules/where/executor/plan/where_plan_catalog.h"
#include "cli/modules/where/executor/plan/plan_impl_support.h"
#include "cli/modules/where/executor/plan/plan_impl_detail.h"
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
#include <numeric>
#include <optional>
#include <sstream>
#include <unordered_set>
#include <unordered_map>
#include <map>
#include <list>

std::vector<std::size_t> visible_slots_for_query(const newdb::HeapTable& tbl,
                                                 const newdb::TableSchema& schema,
                                                 const std::size_t n) {
    return where_plan_catalog_visibility_slots(tbl, schema, n);
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
                    if (where_plan_impl_row_at_logical_slot(tbl, it->second, r) &&
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
                if (where_plan_impl_row_at_logical_slot(tbl, it->second, r) &&
                    row_match_condition(r, schema, c.attr, c.op, c.value)) {
                    out_slots.push_back(it->second);
                }
            }
        }
        trace_mode = "pk_lookup";
        return true;
    }

    if (c.op == CondOp::Eq && c.attr != "id" && c.attr != pk) {
        const WherePlanEqLookupResult eq_lookup =
            where_plan_catalog_eq_lookup(tbl, schema, c.attr, c.value, where_obs);
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
        where_plan_catalog_prewarm_eq_probe(tbl, schema, c.attr, c.value, ctx);
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

std::vector<PlanCandidate> where_build_plan_candidates(const newdb::HeapTable& tbl,
                                                       const newdb::TableSchema& schema,
                                                       const std::vector<WhereCond>& conds,
                                                       WherePlanningStatsRef stats) {
    const TableStats* stats_hint = stats.table_stats;
    std::map<std::string, double> best;
    const std::size_t n = tbl.logical_row_count();
    const std::uint64_t qt_est_plan =
        where_plan_detail::query_temp_est_bytes(n, conds.empty() ? 1 : conds.size());
    if (!where_plan_impl_try_admit_query_temp(qt_est_plan)) {
        std::vector<PlanCandidate> out;
        PlanCandidate pc;
        pc.id = "heap_scan";
        pc.estimated_cost = newdb::where_cost::heap_full_scan_cost(n);
        pc.cost.estimated_rows = static_cast<double>(n);
        pc.rationale = "query_temp_memory_cap_minimal_plan";
        out.push_back(std::move(pc));
        return out;
    }
    where_plan_detail::QueryTempBytesGuard qt_guard_plan(qt_est_plan, nullptr);
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



