#include "structdb/client/detail/mdb_named_index.hpp"

#include "structdb/client/detail/mdb_ops_detail.hpp"
#include "structdb/client/detail/mdb_runner_internal.hpp"
#include "structdb/client/embed_client.hpp"
#include "structdb/client/mdb_persistence.hpp"
#include "structdb/facade/engine.hpp"
#include "structdb/storage/mdb_keyspace.hpp"

#include <limits>
#include <unordered_set>

#include <cctype>
#include <sstream>

namespace structdb::client::mdb {

namespace {

std::string lower_copy(std::string_view s) {
  std::string o(s);
  for (auto& c : o) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  return o;
}

constexpr std::string_view kUniqueDefPrefix = "u:";

}  // namespace

std::string encode_named_index_def_value(const NamedIndexDef& def) {
  if (def.unique) return std::string(kUniqueDefPrefix) + def.column;
  return def.column;
}

bool decode_named_index_def_value(std::string_view stored, NamedIndexDef* def) {
  if (!def) return false;
  def->unique = false;
  def->column = std::string(stored);
  if (stored.size() > kUniqueDefPrefix.size() &&
      stored.compare(0, kUniqueDefPrefix.size(), kUniqueDefPrefix) == 0) {
    def->unique = true;
    def->column = std::string(stored.substr(kUniqueDefPrefix.size()));
  }
  return !def->column.empty();
}

bool parse_create_index_spec(std::string_view tail, std::string* index_name, std::string* table_name,
                             std::string* column, std::string* err, bool* unique_out) {
  std::string_view work = tail;
  if (unique_out) *unique_out = false;
  const std::string low = lower_copy(work);
  if (low.rfind("unique ", 0) == 0) {
    if (unique_out) *unique_out = true;
    work.remove_prefix(7);
    while (!work.empty() && (work.front() == ' ' || work.front() == '\t')) work.remove_prefix(1);
  }
  const std::string t = trim_copy(work);
  const std::size_t on_pos = lower_copy(t).find(" on ");
  if (on_pos == std::string::npos) {
    if (err) *err = "CREATE INDEX: expected name ON table(col)";
    return false;
  }
  *index_name = trim_copy(t.substr(0, on_pos));
  if (index_name->empty()) {
    if (err) *err = "CREATE INDEX: empty index name";
    return false;
  }
  std::string_view rest(t.data() + on_pos + 4, t.size() - on_pos - 4);
  while (!rest.empty() && (rest.front() == ' ' || rest.front() == '\t')) rest.remove_prefix(1);
  const std::size_t lp = rest.find('(');
  if (lp == std::string_view::npos) {
    if (err) *err = "CREATE INDEX: expected table(col)";
    return false;
  }
  *table_name = trim_copy(rest.substr(0, lp));
  if (table_name->empty()) {
    if (err) *err = "CREATE INDEX: empty table";
    return false;
  }
  const std::size_t rp = rest.rfind(')');
  if (rp == std::string_view::npos || rp <= lp + 1) {
    if (err) *err = "CREATE INDEX: bad (col)";
    return false;
  }
  *column = trim_copy(rest.substr(lp + 1, rp - lp - 1));
  if (column->empty()) {
    if (err) *err = "CREATE INDEX: empty column";
    return false;
  }
  return true;
}

bool parse_drop_index_spec(std::string_view tail, std::string* index_name, std::string* table_name, std::string* err) {
  const std::string t = trim_copy(tail);
  const std::size_t on_pos = lower_copy(t).find(" on ");
  if (on_pos == std::string::npos) {
    if (err) *err = "DROP INDEX: expected name ON table";
    return false;
  }
  *index_name = trim_copy(t.substr(0, on_pos));
  if (index_name->empty()) {
    if (err) *err = "DROP INDEX: empty index name";
    return false;
  }
  *table_name = trim_copy(t.substr(on_pos + 4));
  if (table_name->empty()) {
    if (err) *err = "DROP INDEX: empty table";
    return false;
  }
  return true;
}

void gather_named_index_keys_for_index(const MdbEnginePorts& ports, std::string_view table_name,
                                       std::string_view index_name, std::uint64_t read_max_seq,
                                       structdb::client::CommandBatch* b) {
  if (!b || !ports.engine) return;
  namespace mk = structdb::storage::mdb_keyspace;
  b->dels.push_back(mk::named_index_def_key(table_name, index_name));
  const std::string post_pfx =
      std::string(mk::kNamedIdx) + std::string(table_name) + '$' + std::string(index_name) + '$';
  ports.engine->kv_visit_prefix(post_pfx, [&](std::string_view k, std::string_view v) {
    (void)v;
    b->dels.push_back(std::string(k));
    return true;
  }, read_max_seq);
}

void gather_named_index_keys(const MdbEnginePorts& ports, std::string_view table_name, std::uint64_t read_max_seq,
                             structdb::client::CommandBatch* b) {
  if (!b || !ports.engine) return;
  namespace mk = structdb::storage::mdb_keyspace;
  const std::string def_pfx = std::string(mk::kNamedIdxDef) + std::string(table_name) + '$';
  ports.engine->kv_visit_prefix(def_pfx, [&](std::string_view k, std::string_view v) {
    (void)v;
    b->dels.push_back(std::string(k));
    return true;
  }, read_max_seq);
  const std::string post_pfx = std::string(mk::kNamedIdx) + std::string(table_name) + '$';
  ports.engine->kv_visit_prefix(post_pfx, [&](std::string_view k, std::string_view v) {
    (void)v;
    b->dels.push_back(std::string(k));
    return true;
  }, read_max_seq);
}

void load_named_index_defs_from_storage(const MdbEnginePorts& ports, std::string_view table_name, LogicalTable* t,
                                        std::uint64_t read_max_seq) {
  if (!t || !ports.engine) return;
  t->named_indexes.clear();
  namespace mk = structdb::storage::mdb_keyspace;
  const std::string def_pfx = std::string(mk::kNamedIdxDef) + std::string(table_name) + '$';
  ports.engine->kv_visit_prefix(def_pfx, [&](std::string_view k, std::string_view v) {
    const std::string key(k);
    if (key.size() <= def_pfx.size()) return true;
    const std::string idx_name = key.substr(def_pfx.size());
    if (idx_name.empty()) return true;
    NamedIndexDef def;
    if (!decode_named_index_def_value(v, &def)) return true;
    t->named_indexes[idx_name] = std::move(def);
    return true;
  }, read_max_seq);
}

std::string named_index_token_for_cell(std::string_view col_type, std::string_view cell) {
  if (is_string_type(col_type)) return hex_encode(cell);
  return trim_copy(cell);
}

bool rebuild_named_index_postings(const MdbEnginePorts& ports, LogicalTable& t, std::string_view index_name,
                                  structdb::client::CommandBatch* b) {
  if (!b || !ports.engine) return false;
  const auto it = t.named_indexes.find(std::string(index_name));
  if (it == t.named_indexes.end()) return true;
  namespace mk = structdb::storage::mdb_keyspace;
  const std::string col = it->second.column;
  const bool unique = it->second.unique;
  const int ci = schema_col_index(t, col);
  if (ci < 0) return true;
  const std::string post_pfx =
      std::string(mk::kNamedIdx) + t.name + '$' + std::string(index_name) + '$' + col + '$';
  const std::uint64_t read_max = (std::numeric_limits<std::uint64_t>::max)();
  ports.engine->kv_visit_prefix(post_pfx, [&](std::string_view k, std::string_view v) {
    (void)v;
    b->dels.push_back(std::string(k));
    return true;
  }, read_max);
  const std::string& typ = t.schema[static_cast<std::size_t>(ci)].second;
  std::unordered_set<std::string> seen_tokens;
  for (const auto& kv : t.rows) {
    if (static_cast<std::size_t>(ci) >= kv.second.size()) continue;
    const std::string tok = named_index_token_for_cell(typ, kv.second[static_cast<std::size_t>(ci)]);
    if (unique) {
      if (!seen_tokens.insert(tok).second) {
        return false;
      }
      for (const auto& other : t.rows) {
        if (other.first == kv.first) continue;
        if (static_cast<std::size_t>(ci) >= other.second.size()) continue;
        if (named_index_token_for_cell(typ, other.second[static_cast<std::size_t>(ci)]) == tok) {
          return false;
        }
      }
    }
    std::string ik = mk::named_index_posting_key(t.name, index_name, col, tok, kv.first);
    b->puts.push_back({std::move(ik), "1"});
  }
  return true;
}

bool find_named_index_for_column(const LogicalTable& t, std::string_view col, std::string* index_name_out) {
  for (const auto& kv : t.named_indexes) {
    if (col_name_eq(kv.second.column, col)) {
      if (index_name_out) *index_name_out = kv.first;
      return true;
    }
  }
  return false;
}

bool named_index_check_row_unique(const LogicalTable& t, std::string_view row_id,
                                  const std::vector<std::string>& cells, std::string* err) {
  for (const auto& nidx : t.named_indexes) {
    if (!nidx.second.unique) continue;
    const int ci = schema_col_index(t, nidx.second.column);
    if (ci < 0 || static_cast<std::size_t>(ci) >= cells.size()) continue;
    const std::string& typ = t.schema[static_cast<std::size_t>(ci)].second;
    const std::string tok = named_index_token_for_cell(typ, cells[static_cast<std::size_t>(ci)]);
    for (const auto& kv : t.rows) {
      if (kv.first == row_id) continue;
      if (static_cast<std::size_t>(ci) >= kv.second.size()) continue;
      if (named_index_token_for_cell(typ, kv.second[static_cast<std::size_t>(ci)]) == tok) {
        if (err) *err = "UNIQUE INDEX violation on " + nidx.first;
        return false;
      }
    }
  }
  return true;
}

bool create_named_index_storage(const MdbEnginePorts& ports, LogicalTable& t, std::string_view index_name,
                                std::string_view column, bool unique, std::string idem_token, bool fsync,
                                std::string* err) {
  if (t.name.empty()) {
    if (err) *err = "CREATE INDEX: no table";
    return false;
  }
  if (t.named_indexes.count(std::string(index_name))) {
    if (err) *err = "CREATE INDEX: duplicate name";
    return false;
  }
  const int ci = schema_col_index(t, column);
  if (ci < 0) {
    if (err) *err = "CREATE INDEX: unknown column";
    return false;
  }
  NamedIndexDef def;
  def.column = std::string(column);
  def.unique = unique;
  t.named_indexes[std::string(index_name)] = def;
  structdb::client::CommandBatch b;
  b.idempotency_token = std::move(idem_token);
  namespace mk = structdb::storage::mdb_keyspace;
  b.puts.push_back({mk::named_index_def_key(t.name, index_name), encode_named_index_def_value(def)});
  if (!rebuild_named_index_postings(ports, t, index_name, &b)) {
    t.named_indexes.erase(std::string(index_name));
    if (err) *err = unique ? "CREATE UNIQUE INDEX: duplicate key value" : "CREATE INDEX: rebuild postings failed";
    return false;
  }
  return submit_persist_command_batch(ports, std::move(b), fsync, err);
}

bool drop_named_index_storage(const MdbEnginePorts& ports, LogicalTable& t, std::string_view index_name,
                              std::string idem_token, bool fsync, std::string* err) {
  if (!t.named_indexes.count(std::string(index_name))) {
    if (err) *err = "DROP INDEX: not found";
    return false;
  }
  t.named_indexes.erase(std::string(index_name));
  structdb::client::CommandBatch b;
  b.idempotency_token = std::move(idem_token);
  const std::uint64_t read_max = (std::numeric_limits<std::uint64_t>::max)();
  gather_named_index_keys_for_index(ports, t.name, index_name, read_max, &b);
  return submit_persist_command_batch(ports, std::move(b), fsync, err);
}

}  // namespace structdb::client::mdb
