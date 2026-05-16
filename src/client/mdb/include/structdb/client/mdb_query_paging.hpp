#pragma once

#include "structdb/client/mdb_logical_table.hpp"

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

namespace structdb::client::mdb {

/// Query / scan pagination state (e.g. `SCAN MORE` cursor over `LogicalTable::rows` key order).
struct MdbQueryPagingState {
  /// Ordinal skip when walking `current.rows` in sorted key order for `SCAN MORE`.
  std::size_t scan_cursor_ordinal{0};
};

bool handle_page(const LogicalTable& t, std::string_view inner, std::vector<std::string>* sink, std::string* err);
bool handle_page_json(const LogicalTable& t, std::string_view inner, std::vector<std::string>* sink,
                      std::string* err);

}  // namespace structdb::client::mdb
