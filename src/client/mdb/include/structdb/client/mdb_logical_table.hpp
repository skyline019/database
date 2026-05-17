#pragma once

#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace structdb::client::mdb {

/// User-defined secondary index (PHASE41/45): one column; optional unique (PHASE45).
struct NamedIndexDef {
  std::string column;
  bool unique{false};
};

/// Cached row-id order for `PAGE_JSON` / `PAGE` on a non-id sort column.
struct ColumnSortOrderCache {
  std::vector<std::string> ids;
  bool built{false};
};

struct ColumnSortCacheEntry {
  ColumnSortOrderCache asc;
  ColumnSortOrderCache desc;
};

struct GroupBucketAgg {
  std::size_t count{0};
  long long sum{0};
};

/// Incremental analytics: int `SUM(col)` and `GROUP BY` on int columns (P2).
struct LogicalTableAggCache {
  bool valid{false};
  std::unordered_map<std::string, long long> int_col_sum;
  /// Per int column: minimum parsed cell value (for `QBAL(col,min)` fast path when min <= col_min).
  std::unordered_map<std::string, long long> int_col_min;
  /// Rows with a parseable int in that column (rebuild pass).
  std::unordered_map<std::string, std::size_t> int_col_valid_rows;
  std::unordered_map<std::string, std::unordered_map<std::string, GroupBucketAgg>> group_count;
  std::unordered_map<std::string, std::unordered_map<std::string, GroupBucketAgg>> group_sum;
};

/// In-memory logical table (schema + rows + secondary indexes + MDB persist dirty tracking).
struct LogicalTable {
  std::string name;
  std::vector<std::pair<std::string, std::string>> schema;
  std::map<std::string, std::vector<std::string>> rows;
  std::string pk_column;
  std::unordered_map<std::string, std::unordered_multimap<std::string, std::string>> str_idx;
  /// Equality lookup for int-like columns (`int`/`bigint`/…); value → row id.
  std::unordered_map<std::string, std::unordered_multimap<std::string, std::string>> int_idx;
  /// Row ids touched since last successful `persist_table` (for incremental embed batches when enabled).
  std::unordered_set<std::string> mdb_persist_dirty_rows;
  /// Previous cell vector before UPDATE/DELETE (same id as key); absent for pure INSERT dirties.
  std::unordered_map<std::string, std::vector<std::string>> mdb_persist_prev_cells;
  /// Schema/catalog layout changed — forces full persist.
  bool mdb_persist_schema_dirty{false};
  /// Primary keys in lexicographic order (mirrors `rows`); maintained incrementally for persist.
  std::vector<std::string> row_ids_ordered;
  /// Cached: `row_ids_ordered` matches `rows` and passes `logical_row_ids_is_numeric_sorted` for asc/desc.
  bool row_ids_sorted_asc{false};
  bool row_ids_sorted_desc{false};
  /// `CREATE INDEX` definitions persisted under `mdb$v2$nidxdef$`.
  std::unordered_map<std::string, NamedIndexDef> named_indexes;
  /// Lazy sort orders for non-id `PAGE_JSON` / `PAGE` (invalidated on row/schema changes).
  mutable std::unordered_map<std::string, ColumnSortCacheEntry> col_sort_cache;
  mutable LogicalTableAggCache agg_cache;

  void clear_data_keep_name() {
    schema.clear();
    rows.clear();
    str_idx.clear();
    int_idx.clear();
    pk_column.clear();
    mdb_persist_dirty_rows.clear();
    mdb_persist_prev_cells.clear();
    mdb_persist_schema_dirty = false;
    row_ids_ordered.clear();
    row_ids_sorted_asc = false;
    row_ids_sorted_desc = false;
    named_indexes.clear();
    col_sort_cache.clear();
    agg_cache = LogicalTableAggCache{};
  }
};

}  // namespace structdb::client::mdb
