#include "structdb/client/detail/mdb_ops_aggregate.hpp"

#include "structdb/client/detail/mdb_ops_agg_cache.hpp"
#include "structdb/client/detail/mdb_named_index.hpp"
#include "structdb/client/detail/mdb_ops_detail.hpp"
#include "structdb/client/detail/mdb_runner_internal.hpp"
#include "structdb/client/detail/mdb_engine_ports.hpp"
#include "structdb/client/mdb_logical_table.hpp"
#include "structdb/facade/engine.hpp"
#include "structdb/storage/mdb_keyspace.hpp"

#include <algorithm>
#include <cctype>
#include <functional>
#include <limits>
#include <sstream>
#include <unordered_map>
#include <utility>

namespace structdb::client::mdb {

namespace {

bool token_is_scan_stats(std::string_view tok) {
  std::string u(tok);
  for (char& c : u) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  return u == "stats" || u == "stat";
}

bool token_is_scan_ids(std::string_view tok) {
  std::string u(tok);
  for (char& c : u) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  return u == "ids" || u == "ids_only" || u == "id";
}

}  // namespace

constexpr std::size_t kGroupByMaxGroups = 5000;

bool parse_scan_index_spec(const std::vector<std::string>& args, MdbScanIndexSpec* spec, std::string* err) {
  if (!spec) {
    if (err) *err = "SCAN INDEX: internal";
    return false;
  }
  if (args.empty()) {
    if (err) *err = "SCAN INDEX: need (name)";
    return false;
  }
  spec->index_name = trim_copy(args[0]);
  if (spec->index_name.empty()) {
    if (err) *err = "SCAN INDEX: need (name)";
    return false;
  }
  spec->max_rows = 5000;
  spec->emit = MdbScanIndexEmit::FullRow;
  bool have_limit = false;
  for (std::size_t i = 1; i < args.size(); ++i) {
    const std::string tok = trim_copy(args[i]);
    if (tok.empty()) continue;
    if (token_is_scan_stats(tok)) {
      spec->emit = MdbScanIndexEmit::StatsOnly;
      continue;
    }
    if (token_is_scan_ids(tok)) {
      if (spec->emit == MdbScanIndexEmit::StatsOnly) {
        if (err) *err = "SCAN INDEX: STATS and IDS are mutually exclusive";
        return false;
      }
      spec->emit = MdbScanIndexEmit::IdsOnly;
      continue;
    }
    try {
      const std::size_t lim = static_cast<std::size_t>(std::stoull(tok));
      if (lim == 0) {
        if (err) *err = "SCAN INDEX: limit must be > 0";
        return false;
      }
      spec->max_rows = lim;
      have_limit = true;
    } catch (...) {
      if (err) *err = "SCAN INDEX: bad option (expected limit, STATS, or IDS)";
      return false;
    }
  }
  if (spec->emit == MdbScanIndexEmit::StatsOnly && !have_limit) {
    spec->max_rows = std::numeric_limits<std::size_t>::max();
  }
  return true;
}

namespace {

struct GroupAgg {
  std::size_t count{0};
  long double sum{0};
};

bool parse_group_by_spec(std::string_view inner, std::string* group_col, bool* do_count, std::string* sum_col,
                         std::string* err) {
  const std::string t = trim_copy(inner);
  const std::size_t lp = t.find('(');
  if (lp == std::string::npos) {
    if (err) *err = "GROUP BY: expected (col) COUNT or (col) SUM(col)";
    return false;
  }
  const std::size_t rp = t.find(')', lp + 1);
  if (rp == std::string::npos) {
    if (err) *err = "GROUP BY: bad (col)";
    return false;
  }
  *group_col = trim_copy(t.substr(lp + 1, rp - lp - 1));
  std::string tail = trim_copy(t.substr(rp + 1));
  for (char& c : tail) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  if (tail == "count") {
    *do_count = true;
    sum_col->clear();
    return !group_col->empty();
  }
  if (tail.rfind("sum(", 0) == 0) {
    const std::size_t lp2 = tail.find('(');
    const std::size_t rp2 = tail.rfind(')');
    if (rp2 == std::string::npos || rp2 <= lp2 + 1) {
      if (err) *err = "GROUP BY: SUM needs (col)";
      return false;
    }
    *sum_col = trim_copy(tail.substr(lp2 + 1, rp2 - lp2 - 1));
    *do_count = false;
    return !group_col->empty() && !sum_col->empty();
  }
  if (err) *err = "GROUP BY: use COUNT or SUM(col)";
  return false;
}

}  // namespace

bool mdb_execute_group_by(LogicalTable& table, std::string_view spec_inner, std::vector<std::string>* log_lines,
                          std::string* err) {
  if (table.name.empty() || table.schema.empty()) {
    if (err) *err = "GROUP BY: no table/schema";
    return false;
  }
  std::string group_col;
  bool do_count = true;
  std::string sum_col;
  if (!parse_group_by_spec(spec_inner, &group_col, &do_count, &sum_col, err)) return false;

  if (do_count) {
    if (logical_agg_try_group_by_count(table, group_col, log_lines, err)) return true;
  } else if (logical_agg_try_group_by_sum(table, group_col, sum_col, log_lines, err)) {
    return true;
  }

  const int gi = schema_col_index(table, group_col);
  if (gi < 0) {
    if (err) *err = "GROUP BY: unknown group column";
    return false;
  }
  int si = -1;
  if (!do_count) {
    si = schema_col_index(table, sum_col);
    if (si < 0) {
      if (err) *err = "GROUP BY: unknown SUM column";
      return false;
    }
    const std::string& typ = table.schema[static_cast<std::size_t>(si)].second;
    if (!ascii_starts_with_ci(typ, "int") && !ascii_starts_with_ci(typ, "float") &&
        !ascii_starts_with_ci(typ, "double")) {
      if (err) *err = "GROUP BY SUM: column must be int/float/double";
      return false;
    }
  }

  std::unordered_map<std::string, GroupAgg> groups;
  for (const auto& kv : table.rows) {
    if (static_cast<std::size_t>(gi) >= kv.second.size()) continue;
    const std::string key = kv.second[static_cast<std::size_t>(gi)];
    GroupAgg& g = groups[key];
    ++g.count;
    if (!do_count && si >= 0) {
      const std::string& cell = kv.second[static_cast<std::size_t>(si)];
      try {
        g.sum += std::stold(cell);
      } catch (...) {
        continue;
      }
    }
    if (groups.size() > kGroupByMaxGroups) {
      if (err) *err = "GROUP BY: too many groups (max 5000)";
      return false;
    }
  }

  std::vector<std::string> keys;
  keys.reserve(groups.size());
  for (const auto& kv : groups) keys.push_back(kv.first);
  std::sort(keys.begin(), keys.end());

  if (log_lines) {
    std::ostringstream hdr;
    hdr << "[GROUP BY] groups=" << groups.size() << " col=" << group_col;
    log_lines->push_back(hdr.str());
    for (const auto& k : keys) {
      const GroupAgg& g = groups.at(k);
      std::ostringstream o;
      o << "[GROUP] key=" << k << " count=" << g.count;
      if (!do_count) o << " sum=" << g.sum;
      log_lines->push_back(o.str());
    }
  }
  return true;
}

bool mdb_scan_named_index_rows(const MdbEnginePorts& ports, const LogicalTable& table, std::string_view index_name,
                               std::size_t max_rows, MdbScanIndexEmit emit,
                               const std::function<bool(const std::string& row_id)>& on_row,
                               std::size_t* keys_scanned_out, std::size_t* rows_shown_out, std::string* err) {
  if (!ports.engine) {
    if (err) *err = "SCAN INDEX: internal";
    return false;
  }
  if (emit != MdbScanIndexEmit::StatsOnly && !on_row) {
    if (err) *err = "SCAN INDEX: internal";
    return false;
  }
  if (keys_scanned_out) *keys_scanned_out = 0;
  if (rows_shown_out) *rows_shown_out = 0;
  const auto it = table.named_indexes.find(std::string(index_name));
  if (it == table.named_indexes.end()) {
    if (err) *err = "SCAN INDEX: unknown index";
    return false;
  }
  namespace mk = structdb::storage::mdb_keyspace;
  const std::string col = it->second.column;
  const std::string pfx = std::string(mk::kNamedIdx) + table.name + '$' + std::string(index_name) + '$' + col + '$';
  std::size_t emitted = 0;
  std::size_t keys = 0;
  const auto accept_row = [&](const std::string& rid) -> bool {
    if (emit == MdbScanIndexEmit::StatsOnly) {
      ++emitted;
      return true;
    }
    const auto row_it = table.rows.find(rid);
    if (row_it == table.rows.end()) return true;
    if (!on_row(rid)) return false;
    ++emitted;
    return true;
  };
  ports.engine->kv_visit_prefix(pfx, [&](std::string_view k, std::string_view v) {
    (void)v;
    ++keys;
    if (k.size() <= pfx.size()) return true;
    const std::size_t dollar = k.rfind('$');
    if (dollar == std::string_view::npos || dollar + 1 >= k.size()) return true;
    if (emitted >= max_rows) return false;
    const std::string rid(k.substr(dollar + 1));
    return accept_row(rid);
  });
  if (keys > 0) {
    if (keys_scanned_out) *keys_scanned_out = keys;
    if (rows_shown_out) *rows_shown_out = emitted;
    return true;
  }
  const int ci = schema_col_index(table, col);
  if (ci < 0) {
    if (err) *err = "SCAN INDEX: column missing in schema";
    return false;
  }
  std::vector<std::pair<std::string, std::string>> keyed;
  keyed.reserve(table.rows.size());
  for (const auto& kv : table.rows) {
    if (static_cast<std::size_t>(ci) >= kv.second.size()) continue;
    keyed.push_back(std::make_pair(kv.second[static_cast<std::size_t>(ci)], kv.first));
  }
  std::sort(keyed.begin(), keyed.end());
  for (const auto& p : keyed) {
    ++keys;
    if (emitted >= max_rows) break;
    if (!accept_row(p.second)) break;
  }
  if (keys_scanned_out) *keys_scanned_out = keys;
  if (rows_shown_out) *rows_shown_out = emitted;
  return true;
}

bool mdb_collect_row_ids_by_named_index(const MdbEnginePorts& ports, const LogicalTable& table,
                                        std::string_view index_name, std::vector<std::string>* row_ids_out,
                                        std::string* err) {
  if (!row_ids_out || !ports.engine) {
    if (err) *err = "SCAN INDEX: internal";
    return false;
  }
  row_ids_out->clear();
  const auto it = table.named_indexes.find(std::string(index_name));
  if (it == table.named_indexes.end()) {
    if (err) *err = "SCAN INDEX: unknown index";
    return false;
  }
  namespace mk = structdb::storage::mdb_keyspace;
  const std::string col = it->second.column;
  const std::string pfx = std::string(mk::kNamedIdx) + table.name + '$' + std::string(index_name) + '$' + col + '$';
  std::vector<std::string> keys;
  ports.engine->kv_visit_prefix(pfx, [&](std::string_view k, std::string_view v) {
    (void)v;
    if (k.size() <= pfx.size()) return true;
    const std::size_t dollar = k.rfind('$');
    if (dollar == std::string_view::npos || dollar + 1 >= k.size()) return true;
    row_ids_out->push_back(std::string(k.substr(dollar + 1)));
    return true;
  });
  std::sort(row_ids_out->begin(), row_ids_out->end());
  row_ids_out->erase(std::unique(row_ids_out->begin(), row_ids_out->end()), row_ids_out->end());
  if (row_ids_out->empty()) {
    const int ci = schema_col_index(table, col);
    if (ci < 0) {
      if (err) *err = "SCAN INDEX: column missing in schema";
      return false;
    }
    std::vector<std::pair<std::string, std::string>> keyed;
    keyed.reserve(table.rows.size());
    for (const auto& kv : table.rows) {
      if (static_cast<std::size_t>(ci) >= kv.second.size()) continue;
      keyed.push_back(
          std::make_pair(kv.second[static_cast<std::size_t>(ci)], kv.first));
    }
    std::sort(keyed.begin(), keyed.end());
    for (const auto& p : keyed) row_ids_out->push_back(p.second);
  }
  return true;
}

}  // namespace structdb::client::mdb
