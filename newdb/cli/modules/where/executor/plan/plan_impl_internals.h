#pragma once

/// Shared declarations for WHERE plan helpers split across `plan_impl.cc` and `plan_query_index.cc`.

#include "cli/modules/where/executor/plan/plan_scan_estimate.h"
#include "cli/modules/where/executor/stats/table_stats.h"
#include "cli/modules/where/executor/where.h"

#include <cstddef>
#include <vector>

std::vector<std::size_t> visible_slots_for_query(const newdb::HeapTable& tbl,
                                                 const newdb::TableSchema& schema,
                                                 const std::size_t n);
bool collect_single_condition_candidates(const newdb::HeapTable& tbl,
                                         const newdb::TableSchema& schema,
                                         const WhereCond& c,
                                         const std::size_t n,
                                         std::vector<std::size_t>& out_slots,
                                         const char*& trace_mode,
                                         WhereQueryContext* where_obs);
void maybe_prewarm_eq_sidecars(const newdb::HeapTable& tbl,
                               const newdb::TableSchema& schema,
                               const std::vector<WhereCond>& conds,
                               const std::size_t logical_rows,
                               WhereQueryContext& ctx);
bool is_single_cond_index_friendly(const WhereCond& c, const newdb::TableSchema& schema);
bool collect_single_condition_candidates_sanitized(const newdb::HeapTable& tbl,
                                                   const newdb::TableSchema& schema,
                                                   const WhereCond& c,
                                                   const std::size_t n,
                                                   std::vector<std::size_t>& out_slots,
                                                   WhereQueryContext* where_obs);
std::vector<std::size_t> sanitize_sort_slots(const std::vector<std::size_t>& in, const std::size_t n);
std::vector<std::size_t> intersect_sorted_slots(const std::vector<std::size_t>& a,
                                                  const std::vector<std::size_t>& b);
std::size_t plan_metric_for_cond(const WhereCond& c,
                                 const newdb::TableSchema& schema,
                                 const std::size_t n,
                                 const TableStats* stats);
