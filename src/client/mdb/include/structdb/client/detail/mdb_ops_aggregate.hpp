#pragma once

#include <functional>
#include <string>
#include <string_view>
#include <vector>

namespace structdb::client::mdb {

struct LogicalTable;
struct MdbEnginePorts;

/// `SCAN INDEX` row emission: full columns, ids only, or summary (no per-row lines).
enum class MdbScanIndexEmit { FullRow, IdsOnly, StatsOnly };

struct MdbScanIndexSpec {
  std::string index_name;
  std::size_t max_rows{5000};
  MdbScanIndexEmit emit{MdbScanIndexEmit::FullRow};
};

/// `SCAN INDEX(idx)` / `(idx,STATS)` / `(idx,IDS)` / `(idx,limit,IDS)` …
bool parse_scan_index_spec(const std::vector<std::string>& args, MdbScanIndexSpec* spec, std::string* err);

/// `GROUP BY (group_col) COUNT` or `GROUP BY (group_col) SUM(value_col)`.
bool mdb_execute_group_by(LogicalTable& table, std::string_view spec_inner, std::vector<std::string>* log_lines,
                          std::string* err);

/// Collect primary keys in named-index key order (lexicographic by KV key).
bool mdb_collect_row_ids_by_named_index(const MdbEnginePorts& ports, const LogicalTable& table,
                                        std::string_view index_name, std::vector<std::string>* row_ids_out,
                                        std::string* err);

/// Walk named-index postings in KV key order; stops after `max_rows` rows (`StatsOnly`: count keys, no `on_row`).
bool mdb_scan_named_index_rows(const MdbEnginePorts& ports, const LogicalTable& table, std::string_view index_name,
                               std::size_t max_rows, MdbScanIndexEmit emit,
                               const std::function<bool(const std::string& row_id)>& on_row,
                               std::size_t* keys_scanned_out, std::size_t* rows_shown_out, std::string* err);

}  // namespace structdb::client::mdb
