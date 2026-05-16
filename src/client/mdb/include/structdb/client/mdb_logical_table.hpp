#pragma once

#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace structdb::client::mdb {

/// In-memory logical table (schema + rows + secondary indexes + MDB persist dirty tracking).
struct LogicalTable {
  std::string name;
  std::vector<std::pair<std::string, std::string>> schema;
  std::map<std::string, std::vector<std::string>> rows;
  std::string pk_column;
  std::unordered_map<std::string, std::unordered_multimap<std::string, std::string>> str_idx;
  /// Row ids touched since last successful `persist_table` (for incremental embed batches when enabled).
  std::unordered_set<std::string> mdb_persist_dirty_rows;
  /// Previous cell vector before UPDATE/DELETE (same id as key); absent for pure INSERT dirties.
  std::unordered_map<std::string, std::vector<std::string>> mdb_persist_prev_cells;
  /// Schema/catalog layout changed — forces full persist.
  bool mdb_persist_schema_dirty{false};

  void clear_data_keep_name() {
    schema.clear();
    rows.clear();
    str_idx.clear();
    pk_column.clear();
    mdb_persist_dirty_rows.clear();
    mdb_persist_prev_cells.clear();
    mdb_persist_schema_dirty = false;
  }
};

}  // namespace structdb::client::mdb
