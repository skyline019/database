#include "structdb/client/mdb_runner.hpp"

#include "structdb/client/detail/mdb_dispatch_env.hpp"
#include "structdb/client/detail/mdb_runner_internal.hpp"
#include "structdb/client/embed_client.hpp"
#include "structdb/client/mdb_command_parser.hpp"
#include "structdb/facade/engine.hpp"
#include "structdb/infra/long_task_progress.hpp"

#include <atomic>
#include <cstdint>
#include <fstream>
#include <istream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace structdb::client::mdb {

namespace {
std::atomic<std::uint64_t> g_mdb_repl_idem_seq{1};

std::size_t count_executable_script_lines(std::istream& in) {
  std::size_t n = 0;
  std::string line;
  while (std::getline(in, line)) {
    trim_inplace(line);
    if (!line.empty() && line[0] != '#') ++n;
  }
  return n;
}
}  // namespace

std::uint64_t mdb_storage_read_seq_for_script(facade::Engine& engine, EmbedClient& client, bool txn_active,
                                              bool txn_read_committed, std::uint64_t txn_snap_seq) {
  if (txn_active) {
    const std::uint64_t latest = engine.latest_commit_seq();
    if (txn_read_committed) return latest;
    return txn_snap_seq > latest ? latest : txn_snap_seq;
  }
  return client.read_snapshot_seq();
}

std::string mdb_table_snapshot_key(std::string_view table_name) { return snapshot_key_for(table_name); }

bool mdb_decode_stored_snapshot(std::string_view stored, std::string* decoded_out) {
  if (!decoded_out) return false;
  std::string err;
  return wire_decode_snapshot_blob(stored, decoded_out, &err);
}

MdbRunResult run_mdb_script(facade::Engine& engine, EmbedClient& client, const MdbRunOptions& opt) {
  MdbRunResult res;
  std::ifstream in(opt.script_path);
  if (!in) {
    res.ok = false;
    res.last_error = "open script file";
    return res;
  }

  txn_log_remove_if_exists(client);

  LogicalTable current;
  current.name.clear();
  bool txn_active = false;
  LogicalTable txn_base;
  bool txn_read_committed = false;
  std::uint64_t txn_snap_seq = 0;
  std::optional<std::size_t> txn_undo_stack_depth_at_begin;
  std::map<std::string, LogicalTable> savepoints;
  std::string script_str = opt.script_path.string();
  MdbQueryPagingState script_paging;

  infra::LongTaskReporter* const lt = opt.long_task;
  std::size_t executable_total = 0;
  std::size_t executable_done = 0;
  if (lt) {
    lt->set_kind(infra::LongTaskKind::MdbScript);
    lt->set_detail(script_str);
    {
      std::ifstream count_in(opt.script_path);
      if (count_in) executable_total = count_executable_script_lines(count_in);
    }
    lt->report(infra::LongTaskStatus::Running, 0, executable_total);
  }

  std::string line;
  std::size_t line_no = 0;
  while (std::getline(in, line)) {
    ++line_no;
    res.last_line_no = line_no;
    trim_inplace(line);
    if (line.empty() || line[0] == '#') continue;

    if (lt && lt->poll_cancel_and_report_cancelling()) {
      res.ok = false;
      res.cancelled = true;
      res.last_error = "cancelled";
      lt->report(infra::LongTaskStatus::Cancelled, executable_done, executable_total);
      txn_log_remove_if_exists(client);
      return res;
    }

    std::vector<std::string> line_log;
    std::vector<std::string>* log_sink = opt.log_sink;
    const bool fsync_batch = opt.fsync_each_batch;
    auto flush_line_log = [&]() {
      if (log_sink) {
        for (auto& s : line_log) log_sink->push_back(std::move(s));
      }
    };

    std::string err;
    const std::string idem = "mdb:" + script_str + ":" + std::to_string(line_no);

    MdbParsedLine pl;
    std::string perr;
    if (!mdb_parse_command_line(std::string_view(line), &pl, &perr)) {
      log_line(log_sink ? &line_log : nullptr, std::string("[ERR] ") + perr);
      flush_line_log();
      res.ok = false;
      res.last_error = perr;
      return res;
    }

    const std::uint64_t storage_read_seq =
        mdb_storage_read_seq_for_script(engine, client, txn_active, txn_read_committed, txn_snap_seq);

    const bool eng_mdb_persist_in_begin = engine.config().snapshot().mdb_persist_in_begin;
    auto persist_now = [&]() -> bool {
      if (txn_active) {
        if (!eng_mdb_persist_in_begin || !opt.allow_persist_while_txn_active_experimental) return true;
        return persist_table(MdbEnginePorts::from(engine, client), current, idem, fsync_batch, &err);
      }
      return persist_table(MdbEnginePorts::from(engine, client), current, idem, fsync_batch, &err);
    };

    const bool fsync_session_txn_op = opt.fsync_each_session_txn_op;
    auto txn_v2_append = [&](std::string_view kind, std::string_view payload) -> bool {
      if (!txn_active) return true;
      return txn_log_append_v2_op(client, kind, payload, fsync_session_txn_op, &err);
    };

    MdbDispatchEnv env{pl,
                       res,
                       current,
                       engine,
                       client,
                       MdbEnginePorts::from(engine, client),
                       line_log,
                       log_sink,
                       flush_line_log,
                       persist_now,
                       txn_v2_append,
                       err,
                       storage_read_seq,
                       idem,
                       fsync_batch,
                       txn_active,
                       txn_base,
                       txn_read_committed,
                       txn_snap_seq,
                       savepoints,
                       txn_undo_stack_depth_at_begin,
                       script_paging};
    res = mdb_dispatch_execute_line(env);
    if (!res.ok) {
      if (lt) lt->report(infra::LongTaskStatus::Failed, executable_done, executable_total);
      return res;
    }
    if (res.repl_exit_requested) {
      if (lt) lt->report(infra::LongTaskStatus::Completed, executable_done, executable_total);
      return res;
    }
    ++executable_done;
    if (lt) lt->report(infra::LongTaskStatus::Running, executable_done, executable_total);
  }

  if (txn_active) {
    if (opt.fail_if_unclosed_txn) {
      res.ok = false;
      res.last_error = "script ended with unclosed BEGIN (COMMIT or ROLLBACK required)";
      txn_log_remove_if_exists(client);
      if (lt) lt->report(infra::LongTaskStatus::Failed, executable_done, executable_total);
      return res;
    }
    current = clone_table(txn_base);
    logical_rebuild_str_index(&current);
    txn_active = false;
  }

  txn_log_remove_if_exists(client);

  if (lt) lt->report(infra::LongTaskStatus::Completed, executable_done, executable_total);

  return res;
}

MdbInteractiveSession::MdbInteractiveSession() : impl_(std::make_unique<ReplSessionState>()) {}

MdbInteractiveSession::~MdbInteractiveSession() = default;

MdbInteractiveSession::MdbInteractiveSession(MdbInteractiveSession&& o) noexcept = default;

MdbInteractiveSession& MdbInteractiveSession::operator=(MdbInteractiveSession&& o) noexcept = default;

void mdb_repl_reset(MdbInteractiveSession& session) {
  if (!session.impl_) return;
  *session.impl_ = ReplSessionState{};
}

MdbRunResult mdb_repl_execute_line(facade::Engine& engine, EmbedClient& client, MdbInteractiveSession& session,
                                   std::string_view line_sv, std::vector<std::string>* log_sink, bool fsync_each_batch,
                                   bool fsync_session_txn_op, std::string* err_out,
                                   bool allow_persist_while_txn_active_experimental) {
  MdbRunResult res;
  auto* st = session.impl_.get();
  if (!st) {
    res.ok = false;
    res.last_error = "repl: session";
    if (err_out) *err_out = res.last_error;
    return res;
  }

  std::string line(line_sv);
  trim_inplace(line);
  if (line.empty() || line[0] == '#') return res;

  res.last_line_no = ++st->repl_line_no;

  std::vector<std::string> line_log;
  const bool fsync_batch = fsync_each_batch;
  auto flush_line_log = [&]() {
    if (log_sink) {
      for (auto& s : line_log) log_sink->push_back(std::move(s));
    }
  };

  std::string err;
  const std::string idem =
      std::string("mdb:repl:") + std::to_string(g_mdb_repl_idem_seq.fetch_add(1, std::memory_order_relaxed));

  LogicalTable& current = st->current;
  bool& txn_active = st->txn_active;
  LogicalTable& txn_base = st->txn_base;
  bool& txn_read_committed = st->txn_read_committed;
  std::uint64_t& txn_snap_seq = st->txn_snap_seq;
  std::optional<std::size_t>& txn_undo_stack_depth_at_begin = st->txn_undo_stack_depth_at_begin;
  std::map<std::string, LogicalTable>& savepoints = st->savepoints;

  if (!st->attempted_txn_recovery) {
    st->attempted_txn_recovery = true;
    std::string rerr;
    if (!txn_log_try_recover_repl_session(MdbEnginePorts::from(engine, client), st, log_sink, &rerr)) {
      res.ok = false;
      res.last_error = rerr;
      if (err_out) *err_out = rerr;
      return res;
    }
  }

  MdbParsedLine pl;
  std::string perr;
  if (!mdb_parse_command_line(std::string_view(line), &pl, &perr)) {
    log_line(log_sink ? &line_log : nullptr, std::string("[ERR] ") + perr);
    flush_line_log();
    res.ok = false;
    res.last_error = perr;
    if (err_out) *err_out = perr;
    return res;
  }

  const std::uint64_t storage_read_seq =
      mdb_storage_read_seq_for_script(engine, client, txn_active, txn_read_committed, txn_snap_seq);

  const bool eng_mdb_persist_in_begin = engine.config().snapshot().mdb_persist_in_begin;
  const bool allow_txn_persist = eng_mdb_persist_in_begin &&
                                 (allow_persist_while_txn_active_experimental ||
                                  st->allow_persist_while_txn_active_experimental);

  auto persist_now = [&]() -> bool {
    if (txn_active) {
      if (!allow_txn_persist) return true;
      return persist_table(MdbEnginePorts::from(engine, client), current, idem, fsync_batch, &err);
    }
    return persist_table(MdbEnginePorts::from(engine, client), current, idem, fsync_batch, &err);
  };

  auto txn_v2_append = [&](std::string_view kind, std::string_view payload) -> bool {
    if (!txn_active) return true;
    return txn_log_append_v2_op(client, kind, payload, fsync_session_txn_op, &err);
  };

  MdbDispatchEnv env{pl,
                     res,
                     current,
                     engine,
                     client,
                     MdbEnginePorts::from(engine, client),
                     line_log,
                     log_sink,
                     flush_line_log,
                     persist_now,
                     txn_v2_append,
                     err,
                     storage_read_seq,
                     idem,
                     fsync_batch,
                     txn_active,
                     txn_base,
                     txn_read_committed,
                     txn_snap_seq,
                     savepoints,
                     txn_undo_stack_depth_at_begin,
                     st->query_paging};
  res = mdb_dispatch_execute_line(env);

  if (err_out && !res.ok && !res.last_error.empty()) *err_out = res.last_error;
  return res;
}

void MdbInteractiveSession::set_allow_persist_while_txn_active_experimental(bool enable) {
  if (impl_) impl_->allow_persist_while_txn_active_experimental = enable;
}

bool MdbInteractiveSession::allow_persist_while_txn_active_experimental() const {
  return impl_ && impl_->allow_persist_while_txn_active_experimental;
}

}  // namespace structdb::client::mdb
