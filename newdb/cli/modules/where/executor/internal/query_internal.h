#pragma once

#include "cli/modules/where/executor/where.h"

#include <string>
#include <vector>

struct PreparedCond {
    WhereCond cond;
    newdb::AttrType attr_type{newdb::AttrType::String};
    bool rhs_int_ready{false};
    long long rhs_int{0};
    bool attr_idx_ready{false};
    std::size_t attr_idx{0};
};

WhereQueryContext& default_where_context();
std::string build_conds_signature(const std::vector<WhereCond>& conds);
std::string build_query_cache_key(const newdb::HeapTable& tbl,
                                  const newdb::TableSchema& schema,
                                  const std::vector<WhereCond>& conds);
bool query_cache_get(WhereQueryContext& ctx, const std::string& key, std::vector<std::size_t>& out);
void query_cache_put(WhereQueryContext& ctx, const std::string& key, std::vector<std::size_t> slots);
void where_policy_set(WhereQueryContext& ctx, bool blocked, const std::string& msg);
bool all_and_chain(const std::vector<WhereCond>& conds);
bool all_or_chain(const std::vector<WhereCond>& conds);
std::vector<PreparedCond> prepare_conditions(const newdb::TableSchema& schema,
                                             const std::vector<WhereCond>& conds);
bool row_match_all_conditions_ordered_prepared(const newdb::Row& r,
                                               const newdb::TableSchema& schema,
                                               const std::vector<PreparedCond>& conds,
                                               const std::vector<std::size_t>& order,
                                               std::size_t skip_idx);
bool row_match_multi_conditions_prepared(const newdb::Row& r,
                                         const newdb::TableSchema& schema,
                                         const std::vector<PreparedCond>& conds);

bool where_policy_gate(const char* plan,
                       const std::size_t logical_rows,
                       const std::size_t cond_count,
                       const std::size_t estimated_scan_rows,
                       const bool has_or,
                       WhereQueryContext& ctx);

/// Optional hard cap on estimated scan rows (`NEWDB_WHERE_HEAP_SCAN_BUDGET_ROWS`, default 0 = off).
std::size_t where_policy_heap_scan_budget_rows();
