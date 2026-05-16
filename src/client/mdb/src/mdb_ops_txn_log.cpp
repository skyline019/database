#include "structdb/client/detail/mdb_runner_internal.hpp"
#include "structdb/client/embed_client.hpp"
#include "structdb/facade/engine.hpp"
#include "structdb/infra/file_handle.hpp"
#include "structdb/storage/mdb_keyspace.hpp"
#include "structdb/client/mdb_runner.hpp"

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <limits>
#include <sstream>
#include <string>
#include <vector>

namespace structdb::client::mdb {

namespace {

constexpr const char kTxnLogFilename[] = "session.txn";

}  // namespace

std::filesystem::path txn_log_path_for(const structdb::client::EmbedClient& c) {
  const auto jp = c.embed_journal_path();
  if (!jp.empty()) return jp.parent_path() / kTxnLogFilename;
  return c.session_directory() / kTxnLogFilename;
}

void txn_log_remove_if_exists(const structdb::client::EmbedClient& c) {
  std::error_code ec;
  std::filesystem::remove(txn_log_path_for(c), ec);
  std::filesystem::remove(c.session_directory() / kTxnLogFilename, ec);
}

bool txn_log_write_begin(const structdb::client::EmbedClient& client, const LogicalTable& txn_base_snapshot,
                         bool txn_read_committed, std::uint64_t txn_snap_seq, std::string* err) {
  const auto path = txn_log_path_for(client);
  std::ofstream out(path, std::ios::trunc | std::ios::binary);
  if (!out) {
    if (err) *err = "session.txn: open";
    return false;
  }
  const std::string raw = serialize_table(txn_base_snapshot);
  out << "v1\nBEGIN\n";
  out << "RC " << (txn_read_committed ? 1 : 0) << "\n";
  out << "SNAP " << txn_snap_seq << "\n";
  out << "LEN " << raw.size() << "\n";
  out.write(raw.data(), static_cast<std::streamsize>(raw.size()));
  out << "\nTXNV2\n";
  if (!out) {
    if (err) *err = "session.txn: write";
    return false;
  }
  return true;
}

bool parse_u64_dec_sv(std::string_view s, std::uint64_t* out) {
  if (!out || s.empty()) return false;
  std::uint64_t v = 0;
  for (unsigned char c : s) {
    if (c < '0' || c > '9') return false;
    v = v * 10 + static_cast<std::uint64_t>(c - '0');
  }
  *out = v;
  return true;
}

bool txn_log_append_v2_op(const EmbedClient& client, std::string_view kind, std::string_view payload_utf8, bool fsync_wal,
                          std::string* err) {
  std::string line;
  line.assign("V2OP\t");
  line.append(kind);
  line.push_back('\t');
  line.append(hex_encode(payload_utf8));
  line.push_back('\n');
  infra::FileWriter w(txn_log_path_for(client), true);
  if (!w.is_open()) {
    if (err) *err = "session.txn v2: append open";
    return false;
  }
  if (!w.write_all(line.data(), line.size())) {
    if (err) *err = "session.txn v2: append write";
    return false;
  }
  if (fsync_wal && !w.sync()) {
    if (err) *err = "session.txn v2: append sync";
    return false;
  }
  return true;
}

bool split_v2op_line(std::string_view line, std::string_view* kind, std::string_view* hex_payload, std::string* err) {
  constexpr std::string_view kPfx = "V2OP\t";
  if (line.size() < kPfx.size() || line.compare(0, kPfx.size(), kPfx) != 0) {
    if (err) *err = "session.txn v2: bad prefix";
    return false;
  }
  line.remove_prefix(kPfx.size());
  const auto tab = line.find('\t');
  if (tab == std::string_view::npos) {
    if (err) *err = "session.txn v2: missing tab";
    return false;
  }
  *kind = line.substr(0, tab);
  *hex_payload = line.substr(tab + 1);
  return true;
}

bool txn_replay_one_v2_op(const MdbEnginePorts& ports, LogicalTable* current, std::uint64_t read_max_seq,
                          std::string_view line, std::string* err) {
  std::string_view kind;
  std::string_view hexpay;
  if (!split_v2op_line(line, &kind, &hexpay, err)) return false;
  std::string payload;
  if (!hex_decode(hexpay, &payload)) {
    if (err) *err = "session.txn v2: hex decode";
    return false;
  }
  if (kind == "CREATETABLE") {
    current->clear_data_keep_name();
    current->name = std::string(trim_copy(payload));
    return true;
  }
  if (kind == "USE") {
    const std::string tname = trim_copy(payload);
    return load_table_from_storage(ports, tname, current, read_max_seq, err);
  }
  if (kind == "DEFATTR") {
    std::vector<std::pair<std::string, std::string>> attrs;
    if (!parse_defattrs(payload, &attrs, err)) return false;
    current->schema = std::move(attrs);
    logical_rebuild_str_index(current);
    logical_persist_mark_schema_change(current);
    return true;
  }
  if (kind == "ADDATTR") {
    std::string add_inner;
    if (!mdb_addattr_txn_payload_to_apply_inner(payload, &add_inner, err)) return false;
    if (!mdb_apply_addattr_inner(current, add_inner, err)) return false;
    logical_rebuild_str_index(current);
    logical_persist_mark_schema_change(current);
    return true;
  }
  if (kind == "DELATTR") {
    const std::string col = trim_copy(payload);
    int idx = -1;
    for (std::size_t j = 0; j < current->schema.size(); ++j) {
      if (col_name_eq(current->schema[j].first, col)) {
        idx = static_cast<int>(j);
        break;
      }
    }
    if (idx < 0) {
      if (err) *err = "DELATTR replay: unknown column";
      return false;
    }
    current->schema.erase(current->schema.begin() + idx);
    for (auto& kv : current->rows) {
      if (static_cast<int>(kv.second.size()) > idx) kv.second.erase(kv.second.begin() + idx);
    }
    logical_rebuild_str_index(current);
    logical_persist_mark_schema_change(current);
    return true;
  }
  if (kind == "RENATTR") {
    const auto tab = payload.find('\t');
    if (tab == std::string::npos) {
      if (err) *err = "RENATTR replay";
      return false;
    }
    const std::string oldn = trim_copy(std::string_view(payload).substr(0, tab));
    const std::string newn = trim_copy(std::string_view(payload).substr(tab + 1));
    const int ci = schema_col_index(*current, oldn);
    if (ci < 0) {
      if (err) *err = "RENATTR replay: unknown column";
      return false;
    }
    current->schema[static_cast<std::size_t>(ci)].first = newn;
    logical_rebuild_str_index(current);
    logical_persist_mark_schema_change(current);
    return true;
  }
  if (kind == "INSERT") {
    const auto tab = payload.find('\t');
    if (tab == std::string::npos) {
      if (err) *err = "INSERT replay";
      return false;
    }
    const std::string id = payload.substr(0, tab);
    std::vector<std::string> cells;
    std::size_t pos = tab + 1;
    while (pos <= payload.size()) {
      const auto t2 = payload.find('\t', pos);
      const std::size_t end = (t2 == std::string::npos) ? payload.size() : t2;
      cells.emplace_back(payload.substr(pos, end - pos));
      if (t2 == std::string::npos) break;
      pos = t2 + 1;
    }
    if (cells.size() != current->schema.size()) {
      if (err) *err = "INSERT replay: arity";
      return false;
    }
    for (std::size_t c = 0; c < current->schema.size(); ++c) {
      if (!type_matches(current->schema[c].second, cells[c])) {
        if (err) *err = "INSERT replay: type";
        return false;
      }
    }
    current->rows[id] = std::move(cells);
    logical_str_index_add_row(current, id, current->rows[id]);
    logical_persist_mark_insert(current, id);
    return true;
  }
  if (kind == "UPDATE") {
    const auto tab = payload.find('\t');
    if (tab == std::string::npos) {
      if (err) *err = "UPDATE replay";
      return false;
    }
    const std::string id = payload.substr(0, tab);
    auto it = current->rows.find(id);
    if (it == current->rows.end()) {
      if (err) *err = "UPDATE replay: missing row";
      return false;
    }
    std::vector<std::string> cells;
    std::size_t pos = tab + 1;
    while (pos <= payload.size()) {
      const auto t2 = payload.find('\t', pos);
      const std::size_t end = (t2 == std::string::npos) ? payload.size() : t2;
      cells.emplace_back(payload.substr(pos, end - pos));
      if (t2 == std::string::npos) break;
      pos = t2 + 1;
    }
    if (cells.size() != current->schema.size()) {
      if (err) *err = "UPDATE replay: arity";
      return false;
    }
    for (std::size_t c = 0; c < current->schema.size(); ++c) {
      if (!type_matches(current->schema[c].second, cells[c])) {
        if (err) *err = "UPDATE replay: type";
        return false;
      }
    }
    std::vector<std::string> prev = it->second;
    logical_persist_mark_update(current, id, prev);
    logical_str_index_remove_row(current, id, it->second);
    it->second = std::move(cells);
    logical_str_index_add_row(current, id, it->second);
    return true;
  }
  if (kind == "DELETE") {
    const std::string id = trim_copy(payload);
    auto it = current->rows.find(id);
    if (it == current->rows.end()) return true;
    std::vector<std::string> prev = it->second;
    logical_persist_mark_delete_before_erase(current, id, prev);
    logical_str_index_remove_row(current, id, it->second);
    current->rows.erase(it);
    return true;
  }
  if (kind == "SETATTR") {
    const auto t1 = payload.find('\t');
    const auto t2 = (t1 == std::string::npos) ? std::string::npos : payload.find('\t', t1 + 1);
    if (t1 == std::string::npos || t2 == std::string::npos) {
      if (err) *err = "SETATTR replay";
      return false;
    }
    const std::string rid = payload.substr(0, t1);
    const std::string ucol = payload.substr(t1 + 1, t2 - t1 - 1);
    const std::string newval = payload.substr(t2 + 1);
    auto it = current->rows.find(rid);
    if (it == current->rows.end()) {
      if (err) *err = "SETATTR replay: missing row";
      return false;
    }
    const int uix = schema_col_index(*current, ucol);
    if (uix < 0) {
      if (err) *err = "SETATTR replay: bad col";
      return false;
    }
    if (!type_matches(current->schema[static_cast<std::size_t>(uix)].second, newval)) {
      if (err) *err = "SETATTR replay: type";
      return false;
    }
    std::vector<std::string> prev_row = it->second;
    logical_persist_mark_update(current, rid, prev_row);
    logical_str_index_remove_row(current, rid, it->second);
    it->second[static_cast<std::size_t>(uix)] = newval;
    logical_str_index_add_row(current, rid, it->second);
    return true;
  }
  if (kind == "UPDATEWHERE") {
    std::vector<std::string> pr;
    {
      std::size_t pos = 0;
      for (int k = 0; k < 5; ++k) {
        const auto t = payload.find('\t', pos);
        if (k < 4 && t == std::string::npos) {
          if (err) *err = "UPDATEWHERE replay";
          return false;
        }
        if (k < 4) {
          pr.emplace_back(payload.substr(pos, t - pos));
          pos = t + 1;
        } else {
          pr.emplace_back(payload.substr(pos));
        }
      }
    }
    const std::string& ucol = pr[0];
    const std::string& newval = pr[1];
    const std::string& wcol = pr[2];
    const std::string& wop = pr[3];
    const std::string& wval = pr[4];
    const int uix = schema_col_index(*current, ucol);
    if (uix < 0) {
      if (err) *err = "UPDATEWHERE replay: col";
      return false;
    }
    if (!type_matches(current->schema[static_cast<std::size_t>(uix)].second, newval)) {
      if (err) *err = "UPDATEWHERE replay: type";
      return false;
    }
    const auto ids = collect_matching_row_ids(*current, wcol, wop, wval);
    for (const std::string& rid : ids) {
      auto it = current->rows.find(rid);
      if (it == current->rows.end()) continue;
      std::vector<std::string> prev = it->second;
      logical_persist_mark_update(current, rid, prev);
      logical_str_index_remove_row(current, rid, it->second);
      it->second[static_cast<std::size_t>(uix)] = newval;
      logical_str_index_add_row(current, rid, it->second);
    }
    return true;
  }
  if (kind == "DELETEWHERE") {
    std::vector<std::string> pr;
    std::size_t pos = 0;
    for (int k = 0; k < 3; ++k) {
      const auto t = payload.find('\t', pos);
      if (k < 2 && t == std::string::npos) {
        if (err) *err = "DELETEWHERE replay";
        return false;
      }
      if (k < 2) {
        pr.emplace_back(payload.substr(pos, t - pos));
        pos = t + 1;
      } else {
        pr.emplace_back(payload.substr(pos));
      }
    }
    const auto ids = collect_matching_row_ids(*current, pr[0], pr[1], pr[2]);
    for (const std::string& rid : ids) {
      auto it = current->rows.find(rid);
      if (it == current->rows.end()) continue;
      std::vector<std::string> prev = it->second;
      logical_persist_mark_delete_before_erase(current, rid, prev);
      logical_str_index_remove_row(current, rid, it->second);
      current->rows.erase(it);
    }
    return true;
  }
  if (kind == "SETATTRMULTI") {
    const auto t1 = payload.find('\t');
    if (t1 == std::string::npos) {
      if (err) *err = "SETATTRMULTI replay";
      return false;
    }
    const auto t2 = payload.find('\t', t1 + 1);
    if (t2 == std::string::npos) {
      if (err) *err = "SETATTRMULTI replay";
      return false;
    }
    const std::string ucol = payload.substr(0, t1);
    const std::string newval = payload.substr(t1 + 1, t2 - t1 - 1);
    const int uix = schema_col_index(*current, ucol);
    if (uix < 0) {
      if (err) *err = "SETATTRMULTI replay: col";
      return false;
    }
    if (!type_matches(current->schema[static_cast<std::size_t>(uix)].second, newval)) {
      if (err) *err = "SETATTRMULTI replay: type";
      return false;
    }
    std::size_t pos = t2 + 1;
    while (pos <= payload.size()) {
      const auto tn = payload.find('\t', pos);
      const std::size_t end = (tn == std::string::npos) ? payload.size() : tn;
      if (pos < end) {
        const std::string rid = payload.substr(pos, end - pos);
        auto it = current->rows.find(rid);
        if (it != current->rows.end()) {
          std::vector<std::string> prev_row = it->second;
          logical_persist_mark_update(current, it->first, prev_row);
          logical_str_index_remove_row(current, it->first, it->second);
          it->second[static_cast<std::size_t>(uix)] = newval;
          logical_str_index_add_row(current, it->first, it->second);
        }
      }
      if (tn == std::string::npos) break;
      pos = tn + 1;
    }
    return true;
  }
  if (kind == "BULKINSERT") {
    const auto chunks = split_pipe_rows(payload);
    for (const std::string_view chunk : chunks) {
      const auto cells = split_csv_paren_content(chunk);
      if (cells.size() != 1 + current->schema.size()) {
        if (err) *err = "BULKINSERT replay: arity";
        return false;
      }
      const std::string id = std::string(cells[0]);
      std::vector<std::string> vals(cells.begin() + 1, cells.end());
      for (std::size_t c = 0; c < current->schema.size(); ++c) {
        if (!type_matches(current->schema[c].second, vals[c])) {
          if (err) *err = "BULKINSERT replay: type";
          return false;
        }
      }
      current->rows[id] = vals;
      logical_str_index_add_row(current, id, current->rows[id]);
      logical_persist_mark_insert(current, id);
    }
    return true;
  }
  if (kind == "DROPTABLE") {
    const std::string tn = trim_copy(payload);
    if (current->name == tn) {
      current->clear_data_keep_name();
      current->name = tn;
    }
    return true;
  }
  if (kind == "RENAMETABLE") {
    const auto tab = payload.find('\t');
    if (tab == std::string_view::npos) {
      if (err) *err = "RENAMETABLE replay";
      return false;
    }
    const std::string oldn = trim_copy(payload.substr(0, tab));
    const std::string newn = trim_copy(payload.substr(tab + 1));
    if (current->name == oldn) return load_table_from_storage(ports, newn, current, read_max_seq, err);
    return true;
  }
  if (kind == "SETPK") {
    current->pk_column = trim_copy(payload);
    return true;
  }
  if (err) *err = "session.txn v2: unknown op";
  return false;
}

bool txn_log_try_recover_repl_session(const MdbEnginePorts& ports, ReplSessionState* st,
                                      std::vector<std::string>* log_sink, std::string* err_out) {
  const auto path = txn_log_path_for(*ports.client);
  if (!std::filesystem::exists(path)) return true;
  std::ifstream fin(path, std::ios::binary | std::ios::ate);
  if (!fin) {
    if (err_out) *err_out = "session.txn: read";
    return false;
  }
  const auto sz = fin.tellg();
  if (sz < 0) {
    if (err_out) *err_out = "session.txn: size";
    return false;
  }
  fin.seekg(0, std::ios::beg);
  std::string whole(static_cast<std::size_t>(sz), '\0');
  fin.read(whole.data(), static_cast<std::streamsize>(whole.size()));
  if (!fin) {
    if (err_out) *err_out = "session.txn: short read";
    return false;
  }
  fin.close();
  std::istringstream in(std::move(whole));
  std::string line;
  if (!std::getline(in, line)) {
    txn_log_remove_if_exists(*ports.client);
    log_line(log_sink, std::string("[RECOVER] drop reason=empty_file"));
    return true;
  }
  if (!line.empty() && line.back() == '\r') line.pop_back();
  if (line != "v1") {
    txn_log_remove_if_exists(*ports.client);
    log_line(log_sink, std::string("[RECOVER] drop reason=bad_header"));
    return true;
  }
  if (!std::getline(in, line)) {
    txn_log_remove_if_exists(*ports.client);
    log_line(log_sink, std::string("[RECOVER] drop reason=no_begin_line"));
    return true;
  }
  if (!line.empty() && line.back() == '\r') line.pop_back();
  if (line != "BEGIN") {
    txn_log_remove_if_exists(*ports.client);
    log_line(log_sink, std::string("[RECOVER] drop reason=not_begin"));
    return true;
  }
  if (!std::getline(in, line)) {
    txn_log_remove_if_exists(*ports.client);
    log_line(log_sink, std::string("[RECOVER] drop reason=eof_after_begin"));
    return true;
  }
  if (!line.empty() && line.back() == '\r') line.pop_back();
  bool rc = false;
  if (line.rfind("RC ", 0) != 0) {
    txn_log_remove_if_exists(*ports.client);
    log_line(log_sink, std::string("[RECOVER] drop reason=no_rc_line"));
    return true;
  }
  {
    std::uint64_t v = 0;
    if (!parse_u64_dec_sv(std::string_view(line).substr(3), &v)) {
      txn_log_remove_if_exists(*ports.client);
      log_line(log_sink, std::string("[RECOVER] drop reason=bad_rc"));
      return true;
    }
    rc = v != 0;
  }
  if (!std::getline(in, line)) {
    txn_log_remove_if_exists(*ports.client);
    log_line(log_sink, std::string("[RECOVER] drop reason=eof_after_rc"));
    return true;
  }
  if (!line.empty() && line.back() == '\r') line.pop_back();
  std::uint64_t snap = 0;
  if (line.rfind("SNAP ", 0) != 0 || !parse_u64_dec_sv(std::string_view(line).substr(5), &snap)) {
    txn_log_remove_if_exists(*ports.client);
    log_line(log_sink, std::string("[RECOVER] drop reason=bad_snap"));
    return true;
  }
  if (!std::getline(in, line)) {
    txn_log_remove_if_exists(*ports.client);
    log_line(log_sink, std::string("[RECOVER] drop reason=eof_after_snap"));
    return true;
  }
  if (!line.empty() && line.back() == '\r') line.pop_back();
  std::uint64_t len = 0;
  if (line.rfind("LEN ", 0) != 0 || !parse_u64_dec_sv(std::string_view(line).substr(4), &len)) {
    txn_log_remove_if_exists(*ports.client);
    log_line(log_sink, std::string("[RECOVER] drop reason=bad_len"));
    return true;
  }
  std::string raw;
  raw.resize(static_cast<std::size_t>(len));
  in.read(raw.data(), static_cast<std::streamsize>(len));
  if (static_cast<std::size_t>(in.gcount()) != static_cast<std::size_t>(len)) {
    txn_log_remove_if_exists(*ports.client);
    log_line(log_sink, std::string("[RECOVER] drop reason=short_payload"));
    return true;
  }
  LogicalTable restored;
  std::string derr;
  if (!deserialize_table(raw, &restored, &derr)) {
    txn_log_remove_if_exists(*ports.client);
    log_line(log_sink, std::string("[RECOVER] drop reason=bad_snapshot detail=") + derr);
    return true;
  }
  st->txn_base = std::move(restored);
  logical_rebuild_str_index(&st->txn_base);
  st->txn_read_committed = rc;
  st->txn_snap_seq = snap;
  st->current = clone_table(st->txn_base);
  logical_rebuild_str_index(&st->current);
  logical_persist_invalidate_incremental(&st->current);
  st->txn_active = true;
  st->txn_undo_stack_depth_at_begin.reset();
  st->savepoints.clear();
  const std::uint64_t read_seq = rc ? ports.engine->latest_commit_seq() : snap;
  std::string maybe_txnv2;
  while (std::getline(in, maybe_txnv2)) {
    if (!maybe_txnv2.empty() && maybe_txnv2.back() == '\r') maybe_txnv2.pop_back();
    if (!maybe_txnv2.empty()) break;
  }
  if (!in && maybe_txnv2.empty()) {
    std::ostringstream o;
    o << "[RECOVER] ok mode=baseline_only v2_ops=0 snap_seq=" << snap << " rc=" << (rc ? 1 : 0);
    log_line(log_sink, o.str());
    return true;
  }
  if (maybe_txnv2 != "TXNV2") {
    txn_log_remove_if_exists(*ports.client);
    log_line(log_sink, std::string("[RECOVER] drop reason=expected_txnv2"));
    return true;
  }
  std::size_t nrep = 0;
  std::string op_line;
  while (std::getline(in, op_line)) {
    if (!op_line.empty() && op_line.back() == '\r') op_line.pop_back();
    if (op_line.empty()) continue;
    std::string replay_err;
    if (!txn_replay_one_v2_op(ports, &st->current, read_seq, op_line, &replay_err)) {
      txn_log_remove_if_exists(*ports.client);
      st->txn_active = false;
      if (log_sink) {
        log_line(log_sink, std::string("[RECOVER] drop reason=v2_replay detail=") + replay_err);
      }
      return true;
    }
    ++nrep;
  }
  logical_rebuild_str_index(&st->current);
  logical_persist_invalidate_incremental(&st->current);
  {
    std::ostringstream o;
    o << "[RECOVER] ok mode=v2_replay v2_ops=" << nrep << " snap_seq=" << snap << " rc=" << (rc ? 1 : 0);
    log_line(log_sink, o.str());
  }
  return true;
}

}  // namespace structdb::client::mdb
