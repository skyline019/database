#pragma once

#include "cli/modules/where/parser/condition.h"
#include "cli/modules/where/executor/stats/table_stats.h"

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

/// Lightweight cost bundle for `PlanCandidate` (extensible toward full optimizer).
struct PlanCost {
    double estimated_rows{0.0};
};

struct PlanCandidate {
    std::string id;
    double estimated_cost{0.0};
    PlanCost cost;
};

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
    std::atomic<std::uint64_t> plan_eq_sidecar_count{0};
    std::atomic<std::uint64_t> plan_id_pk_count{0};
    std::atomic<std::uint64_t> plan_fallback_count{0};
    /// Completed WHERE index queries (each `query_with_index` call).
    std::atomic<std::uint64_t> query_count{0};
    /// Heap rows decoded / examined on the hot path (per-query aggregate).
    std::atomic<std::uint64_t> query_rows_scanned_total{0};
    /// Matching row slots returned (per-query aggregate).
    std::atomic<std::uint64_t> query_rows_returned_total{0};
    /// Optional ANALYZE-style stats for `NEWDB_QUERY_USE_TABLE_STATS=1` planning hints.
    const TableStats* query_stats_hint{nullptr};
    std::atomic<std::uint64_t> estimated_scan_rows_total{0};
    std::atomic<std::uint64_t> estimated_scan_rows_samples{0};
    /// Last completed `query_with_index`: bounded count of index/scan shapes evaluated (cap 8).
    std::atomic<std::uint32_t> last_plan_candidates_considered{1};
    /// Equality sidecar: bytes read from disk when loading `.eqidx` (not memory-cache hits).
    std::atomic<std::uint64_t> where_eq_sidecar_disk_bytes_read_total{0};
    /// Equality sidecar: count of on-disk sidecar loads (each `try_lookup` miss path).
    std::atomic<std::uint64_t> where_eq_sidecar_disk_loads{0};
    /// `NEWDB_WHERE_HEAP_SCAN_BUDGET_ROWS` tightened the scan cap below ratio-derived `scan_cap` (observability).
    std::atomic<std::uint64_t> where_heap_scan_budget_binding_events{0};
    WherePolicyState policy{};
    /// Last completed `query_with_index` plan label (e.g. `fallback_scan`, `id_lookup`).
    std::string last_plan_id;
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

/// Policy / SHOW PLAN: heuristic estimated rows scanned for `conds` (matches `query_with_index` gates).
std::size_t where_estimate_scan_rows(const newdb::HeapTable& tbl,
                                     const newdb::TableSchema& schema,
                                     const std::vector<WhereCond>& conds,
                                     WhereQueryContext* ctx = nullptr);

/// Lightweight alternative access paths for EXPLAIN / SHOW PLAN (sorted by ascending `estimated_cost`).
std::vector<PlanCandidate> where_build_plan_candidates(const newdb::HeapTable& tbl,
                                                       const newdb::TableSchema& schema,
                                                       const std::vector<WhereCond>& conds,
                                                       const TableStats* stats_hint);

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
