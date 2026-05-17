#pragma once

#include "structdb/client/mdb_logical_table.hpp"
#include "structdb/client/detail/mdb_engine_ports.hpp"

#include <string>
#include <string_view>

namespace structdb::client::mdb {

/// Parse `idx ON table(col)` from CREATE INDEX tail. When `unique_out` non-null, accepts leading `UNIQUE`.
bool parse_create_index_spec(std::string_view tail, std::string* index_name, std::string* table_name,
                             std::string* column, std::string* err, bool* unique_out = nullptr);

/// Parse `idx ON table` from DROP INDEX tail.
bool parse_drop_index_spec(std::string_view tail, std::string* index_name, std::string* table_name, std::string* err);

void gather_named_index_keys_for_index(const MdbEnginePorts& ports, std::string_view table_name,
                                       std::string_view index_name, std::uint64_t read_max_seq,
                                       structdb::client::CommandBatch* b);

std::string encode_named_index_def_value(const NamedIndexDef& def);
bool decode_named_index_def_value(std::string_view stored, NamedIndexDef* def);

void gather_named_index_keys(const MdbEnginePorts& ports, std::string_view table_name, std::uint64_t read_max_seq,
                             structdb::client::CommandBatch* b);

void load_named_index_defs_from_storage(const MdbEnginePorts& ports, std::string_view table_name,
                                        LogicalTable* t, std::uint64_t read_max_seq);

std::string named_index_token_for_cell(std::string_view col_type, std::string_view cell);

bool rebuild_named_index_postings(const MdbEnginePorts& ports, LogicalTable& t, std::string_view index_name,
                                  structdb::client::CommandBatch* b);

bool find_named_index_for_column(const LogicalTable& t, std::string_view col, std::string* index_name_out);

bool create_named_index_storage(const MdbEnginePorts& ports, LogicalTable& t, std::string_view index_name,
                                std::string_view column, bool unique, std::string idem_token, bool fsync,
                                std::string* err);

bool drop_named_index_storage(const MdbEnginePorts& ports, LogicalTable& t, std::string_view index_name,
                              std::string idem_token, bool fsync, std::string* err);

/// Returns false if a unique index would violate uniqueness for `row_id` / `cells`.
bool named_index_check_row_unique(const LogicalTable& t, std::string_view row_id,
                                  const std::vector<std::string>& cells, std::string* err);

}  // namespace structdb::client::mdb
