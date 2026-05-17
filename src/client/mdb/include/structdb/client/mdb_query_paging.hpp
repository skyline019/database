#pragma once

#include "structdb/client/mdb_logical_table.hpp"

#include <cstddef>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace structdb::client::mdb {

/// Query / scan pagination state (e.g. `SCAN MORE` cursor over `LogicalTable::rows` key order).
struct MdbQueryPagingState {
  /// Ordinal skip when walking `current.rows` in sorted key order for `SCAN MORE`.
  std::size_t scan_cursor_ordinal{0};
  /// Session cache: row id → cell vector (O(1) `PAGE_JSON` row resolve).
  std::string row_ptr_cache_table;
  std::size_t row_ptr_cache_rows{0};
  std::unordered_map<std::string, const std::vector<std::string>*> row_ptr_by_id;
  /// Dense ordinal cache: `row_cells_dense[i]` is cells for `LogicalTable::row_ids_ordered[i]`.
  std::string row_dense_table;
  std::size_t row_dense_rows{0};
  std::size_t row_dense_ordered{0};
  std::vector<const std::vector<std::string>*> row_cells_dense;
  /// Reused emit buffer (avoids per-query heap churn).
  std::string json_emit_buf;
};

void mdb_paging_reset_table_caches(MdbQueryPagingState* paging);

bool handle_page(const LogicalTable& t, std::string_view inner, std::vector<std::string>* sink, std::string* err);
bool handle_page_json(const LogicalTable& t, std::string_view inner, std::vector<std::string>* sink, std::string* err,
                      MdbQueryPagingState* paging);

}  // namespace structdb::client::mdb
