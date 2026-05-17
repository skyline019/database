#include "structdb/client/detail/mdb_dispatch_env.hpp"
#include "structdb/client/detail/mdb_named_index.hpp"
#include "structdb/client/detail/mdb_ops_agg_cache.hpp"
#include "structdb/client/detail/mdb_ops_aggregate.hpp"
#include "structdb/client/detail/mdb_runner_internal.hpp"
#include "structdb/client/mdb_persistence.hpp"
#include "structdb/client/mdb_query_paging.hpp"
#include "structdb/client/embed_client.hpp"
#include "structdb/facade/engine.hpp"
#include "structdb/storage/mdb_keyspace.hpp"
#include "structdb/storage/storage_engine.hpp"
#include "structdb/storage/checkpoint_chain.hpp"
#include "structdb/storage/storage_pressure.hpp"

#include <cctype>
#include <filesystem>
#include <fstream>
#include <limits>
#include <set>
#include <sstream>
#include <string>

namespace structdb::client::mdb {

MdbRunResult mdb_dispatch_execute_line(MdbDispatchEnv& e) {
  MdbParsedLine& pl = e.pl;
  MdbRunResult& res = e.res;
  LogicalTable& current = e.current;
  facade::Engine& engine = e.engine;
  EmbedClient& client = e.client;
  MdbEnginePorts ports = e.ports;
  std::vector<std::string>& line_log = e.line_log;
  std::vector<std::string>* log_sink = e.log_sink;
  auto& flush_line_log = e.flush_line_log;
  auto& persist_now = e.persist_now;
  auto& txn_v2_append = e.txn_v2_append;
  std::string& err = e.err;
  const std::uint64_t storage_read_seq = e.storage_read_seq;
  const std::string& idem = e.idem;
  const bool fsync_batch = e.fsync_batch;
  bool& txn_active = e.txn_active;
  LogicalTable& txn_base = e.txn_base;
  bool& txn_read_committed = e.txn_read_committed;
  std::uint64_t& txn_snap_seq = e.txn_snap_seq;
  auto& savepoints = e.savepoints;
  auto& txn_undo_stack_depth_at_begin = e.txn_undo_stack_depth_at_begin;
  MdbQueryPagingState& paging = e.paging;
  std::size_t& scan_cursor_ordinal = paging.scan_cursor_ordinal;
  const bool persist_coalesce = e.persist_coalesce;
  const bool bulk_import_mode = e.bulk_import_mode;
  std::optional<bool>* bulk_import_session_override = e.bulk_import_session_override;
  const bool script_amortize_bulk_dml = e.script_amortize_bulk_dml;
  std::optional<int>* session_durability_level = e.session_durability_level;
  std::optional<std::string>* import_segment_token = e.import_segment_token;
  (void)persist_coalesce;
  (void)bulk_import_mode;

#include "mdb_runner_dispatch.inc"
  return res;
}

}  // namespace structdb::client::mdb
