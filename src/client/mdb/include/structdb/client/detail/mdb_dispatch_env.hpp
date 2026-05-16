#pragma once

#include "structdb/client/detail/mdb_runner_internal.hpp"

#include <functional>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace structdb::client::mdb {

/// Per-line bindings for `mdb_runner_dispatch.inc` (PHASE26: single include site).
struct MdbDispatchEnv {
  MdbParsedLine& pl;
  MdbRunResult& res;
  LogicalTable& current;
  structdb::facade::Engine& engine;
  structdb::client::EmbedClient& client;
  MdbEnginePorts ports;
  std::vector<std::string>& line_log;
  std::vector<std::string>* log_sink;
  std::function<void()> flush_line_log;
  std::function<bool()> persist_now;
  std::function<bool(std::string_view, std::string_view)> txn_v2_append;
  std::string& err;
  std::uint64_t storage_read_seq;
  const std::string& idem;
  bool fsync_batch;
  bool& txn_active;
  LogicalTable& txn_base;
  bool& txn_read_committed;
  std::uint64_t& txn_snap_seq;
  std::map<std::string, LogicalTable>& savepoints;
  std::optional<std::size_t>& txn_undo_stack_depth_at_begin;
  /// `SCAN MORE` / paging cursor state (script or REPL session).
  MdbQueryPagingState& paging;
};

MdbRunResult mdb_dispatch_execute_line(MdbDispatchEnv& env);

}  // namespace structdb::client::mdb
