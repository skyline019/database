#pragma once

#include "condition.h"

#include <newdb/heap_table.h>
#include <newdb/schema.h>

#include <atomic>
#include <cstdint>
#include <list>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

struct WhereCond {
    std::string attr;
    CondOp op{CondOp::Unknown};
    std::string value;
    std::string logic_with_prev;
};

struct WherePolicyState {
    bool blocked{false};
    std::string message;
    std::uint64_t window_sec{0};
    std::size_t window_count{0};
};

struct WhereQueryContext {
    static constexpr std::size_t kMaxQueryCacheEntries = 128;

    std::unordered_map<std::string, std::vector<std::size_t>> query_cache;
    std::list<std::string> query_cache_lru;
    std::unordered_map<std::string, std::list<std::string>::iterator> query_cache_lru_pos;
    std::unordered_set<std::string> eq_sidecar_prewarmed;
    std::atomic<std::uint64_t> cache_lookups{0};
    std::atomic<std::uint64_t> cache_hits{0};
    std::atomic<std::uint64_t> policy_checks{0};
    std::atomic<std::uint64_t> policy_rejects{0};
    std::atomic<std::uint64_t> fallback_scans{0};
    std::atomic<std::uint64_t> estimated_scan_rows_total{0};
    std::atomic<std::uint64_t> estimated_scan_rows_samples{0};
    WherePolicyState policy{};
    mutable std::mutex mu;
};

bool row_match_condition(const newdb::Row& r,
                         const newdb::TableSchema& schema,
                         const std::string& attr,
                         CondOp op,
                         const std::string& value);

bool parse_where_args_to_conds(const std::vector<std::string>& args,
                               std::vector<WhereCond>& conds,
                               std::string& err_msg);

bool row_match_multi_conditions(const newdb::Row& r,
                                const newdb::TableSchema& schema,
                                const std::vector<WhereCond>& conds);

bool parse_agg_args_with_optional_where(const std::vector<std::string>& args,
                                        std::string& target_attr,
                                        std::vector<WhereCond>& conds,
                                        std::string& err_msg);

std::vector<std::size_t> query_with_index(const newdb::HeapTable& tbl,
                                          const newdb::TableSchema& schema,
                                          const std::vector<WhereCond>& conds,
                                          WhereQueryContext* ctx = nullptr);

std::vector<std::size_t> build_candidate_slots(const newdb::HeapTable& tbl,
                                               const newdb::TableSchema& schema,
                                               const std::vector<WhereCond>& conds,
                                               WhereQueryContext* ctx = nullptr);

bool where_policy_last_blocked(const WhereQueryContext* ctx = nullptr);
std::string where_policy_last_message(const WhereQueryContext* ctx = nullptr);
