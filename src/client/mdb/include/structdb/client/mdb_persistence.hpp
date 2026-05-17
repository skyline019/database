#pragma once

#include "structdb/facade/config.hpp"

#include <cstdint>
#include <string>
#include <string_view>

namespace structdb::client {
struct CommandBatch;
}

namespace structdb::client::mdb {

struct LogicalTable;
struct MdbEnginePorts;

/// Load a logical table from KV (`mdb$` keyspace, v2 layout, or legacy snapshot).
bool load_table_from_storage(const MdbEnginePorts& ports, std::string_view table_name, LogicalTable* t,
                             std::uint64_t read_max_seq, std::string* err);

struct PersistBuildOptions {
  bool skip_secondary_index{false};
  /// Row puts: plain `id\\tcol...` without `mdbhex1:` / `mdbwire2:` (bulk import; load via `wire_decode` fallback).
  bool plain_row_values{false};
  structdb::facade::MdbWireEncoding wire_encoding{structdb::facade::MdbWireEncoding::Hex};
};

/// Build embed batch for `t` without submitting (for tests and chunked persist).
bool build_persist_command_batch(const MdbEnginePorts& ports, LogicalTable& t, std::string idem_token,
                                 const PersistBuildOptions& opt, structdb::client::CommandBatch* out,
                                 std::string* err);

/// Submit one or more chunks when dirty set exceeds internal limits.
bool submit_persist_command_batch(const MdbEnginePorts& ports, structdb::client::CommandBatch batch, bool fsync,
                                  std::string* err, bool write_journal = true);

/// Persist logical table to storage (incremental when enabled, else full snapshot path).
bool persist_table(const MdbEnginePorts& ports, LogicalTable& t, std::string idem_token, bool fsync, std::string* err);

/// Rebuild all `kSecIdx` postings from in-memory rows (after bulk import).
bool rebuild_secondary_index_storage(const MdbEnginePorts& ports, LogicalTable& t, std::string idem_token, bool fsync,
                                     std::string* err);

bool table_exists_in_storage(const MdbEnginePorts& ports, std::string_view table_name, std::uint64_t read_max_seq);

void gather_drop_table_keys(const MdbEnginePorts& ports, std::string_view table_name, std::uint64_t read_max_seq,
                            structdb::client::CommandBatch* b);

bool rename_table_storage(const MdbEnginePorts& ports, std::string_view old_name, std::string_view new_name,
                          const std::string& idem_persist, const std::string& idem_drop_old, bool fsync,
                          std::string* err);

}  // namespace structdb::client::mdb
