#pragma once

#include "structdb/client/mdb_logical_table.hpp"

#include <string>
#include <string_view>

namespace structdb::client::mdb {

/// Invalidate SUM / GROUP BY caches (row or schema changes).
void logical_agg_invalidate(LogicalTable* t);

/// One-pass rebuild of int column sums and int-group buckets (after bulk load).
void logical_agg_rebuild(LogicalTable* t);

/// Cached `SUM(int_col)`; returns false if cache miss or unsupported type.
bool logical_agg_try_int_sum(const LogicalTable& t, std::string_view col, long long* sum_out, int* count_out);

/// Cached `QBAL(int_col,min)` when `min <=` column minimum (all valid rows match).
bool logical_agg_try_qbal_int_ge(const LogicalTable& t, std::string_view col, long long minv, long long* sum_out,
                                  std::size_t* matched_out);

/// Cached `GROUP BY (group_col) COUNT`.
bool logical_agg_try_group_by_count(const LogicalTable& t, std::string_view group_col,
                                    std::vector<std::string>* log_lines, std::string* err);

/// Cached `GROUP BY (group_col) SUM(sum_col)`.
bool logical_agg_try_group_by_sum(const LogicalTable& t, std::string_view group_col, std::string_view sum_col,
                                  std::vector<std::string>* log_lines, std::string* err);

/// Build asc+desc `col_sort_cache` for all int-like columns (bulk / reopen).
void logical_col_sort_prewarm_int_columns(const LogicalTable& t);

/// Row-id order rebuild + agg + column-sort prewarm (post-bulk).
void logical_table_refresh_analytics_caches(LogicalTable* t);

}  // namespace structdb::client::mdb
