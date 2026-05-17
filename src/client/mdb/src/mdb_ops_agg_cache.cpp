#include "structdb/client/detail/mdb_ops_agg_cache.hpp"

#include "structdb/client/detail/mdb_ops_detail.hpp"
#include "structdb/client/detail/mdb_runner_internal.hpp"

#include <sstream>
#include <utility>

namespace structdb::client::mdb {

namespace {

constexpr char kGroupPairSep = '\x1f';

std::string group_pair_key(std::string_view group_col, std::string_view sum_col) {
  std::string k(group_col);
  k.push_back(kGroupPairSep);
  k.append(sum_col);
  return k;
}

bool parse_int_cell(std::string_view cell, long long* out) {
  if (!is_int_literal(cell)) return false;
  try {
    *out = std::stoll(std::string(cell));
    return true;
  } catch (...) {
    return false;
  }
}

}  // namespace

void logical_agg_invalidate(LogicalTable* t) {
  if (!t) return;
  t->agg_cache.valid = false;
  t->agg_cache.int_col_sum.clear();
  t->agg_cache.int_col_min.clear();
  t->agg_cache.int_col_valid_rows.clear();
  t->agg_cache.group_count.clear();
  t->agg_cache.group_sum.clear();
}

void logical_agg_rebuild(LogicalTable* t) {
  if (!t || t->rows.empty() || t->schema.empty()) {
    if (t) logical_agg_invalidate(t);
    return;
  }
  logical_agg_invalidate(t);
  std::vector<int> int_cols;
  int_cols.reserve(t->schema.size());
  for (std::size_t i = 0; i < t->schema.size(); ++i) {
    if (is_int_type(t->schema[i].second)) int_cols.push_back(static_cast<int>(i));
  }
  if (int_cols.empty()) {
    t->agg_cache.valid = true;
    return;
  }
  auto& cache = t->agg_cache;
  for (const auto& kv : t->rows) {
    const auto& cells = kv.second;
    for (const int ci : int_cols) {
      if (static_cast<std::size_t>(ci) >= cells.size()) continue;
      const std::string& col_name = t->schema[static_cast<std::size_t>(ci)].first;
      long long v = 0;
      if (!parse_int_cell(cells[static_cast<std::size_t>(ci)], &v)) continue;
      cache.int_col_sum[col_name] += v;
      ++cache.int_col_valid_rows[col_name];
      auto min_it = cache.int_col_min.find(col_name);
      if (min_it == cache.int_col_min.end()) {
        cache.int_col_min.emplace(col_name, v);
      } else if (v < min_it->second) {
        min_it->second = v;
      }
    }
    for (const int gi : int_cols) {
      if (static_cast<std::size_t>(gi) >= cells.size()) continue;
      const std::string& gname = t->schema[static_cast<std::size_t>(gi)].first;
      const std::string& gkey = cells[static_cast<std::size_t>(gi)];
      ++cache.group_count[gname][gkey].count;
      for (const int sj : int_cols) {
        if (static_cast<std::size_t>(sj) >= cells.size()) continue;
        long long sv = 0;
        if (!parse_int_cell(cells[static_cast<std::size_t>(sj)], &sv)) continue;
        const std::string pair = group_pair_key(gname, t->schema[static_cast<std::size_t>(sj)].first);
        auto& b = cache.group_sum[pair][gkey];
        ++b.count;
        b.sum += sv;
      }
    }
  }
  cache.valid = true;
}

bool logical_agg_try_int_sum(const LogicalTable& t, std::string_view col, long long* sum_out, int* count_out) {
  if (!sum_out || !count_out || !t.agg_cache.valid) return false;
  const int ci = schema_col_index(t, col);
  if (ci < 0 || !is_int_type(t.schema[static_cast<std::size_t>(ci)].second)) return false;
  const auto it = t.agg_cache.int_col_sum.find(std::string(col));
  if (it == t.agg_cache.int_col_sum.end()) return false;
  *sum_out = it->second;
  *count_out = static_cast<int>(t.rows.size());
  return true;
}

bool logical_agg_try_qbal_int_ge(const LogicalTable& t, std::string_view col, long long minv, long long* sum_out,
                                 std::size_t* matched_out) {
  if (!sum_out || !matched_out || !t.agg_cache.valid) return false;
  const int ci = schema_col_index(t, col);
  if (ci < 0 || !is_int_type(t.schema[static_cast<std::size_t>(ci)].second)) return false;
  const std::string col_key(col);
  const auto sum_it = t.agg_cache.int_col_sum.find(col_key);
  const auto min_it = t.agg_cache.int_col_min.find(col_key);
  const auto valid_it = t.agg_cache.int_col_valid_rows.find(col_key);
  if (sum_it == t.agg_cache.int_col_sum.end() || min_it == t.agg_cache.int_col_min.end() ||
      valid_it == t.agg_cache.int_col_valid_rows.end() || valid_it->second == 0) {
    return false;
  }
  if (minv > min_it->second) return false;
  *sum_out = sum_it->second;
  *matched_out = valid_it->second;
  return true;
}

bool logical_agg_try_group_by_count(const LogicalTable& t, std::string_view group_col,
                                    std::vector<std::string>* log_lines, std::string* err) {
  if (!t.agg_cache.valid) return false;
  const int gi = schema_col_index(t, group_col);
  if (gi < 0 || !is_int_type(t.schema[static_cast<std::size_t>(gi)].second)) return false;
  const auto it = t.agg_cache.group_count.find(std::string(group_col));
  if (it == t.agg_cache.group_count.end()) return false;
  if (log_lines) {
    std::ostringstream hdr;
    hdr << "[GROUP BY] groups=" << it->second.size() << " col=" << group_col;
    log_lines->push_back(hdr.str());
    std::vector<std::string> keys;
    keys.reserve(it->second.size());
    for (const auto& kv : it->second) keys.push_back(kv.first);
    std::sort(keys.begin(), keys.end());
    for (const auto& k : keys) {
      const auto& g = it->second.at(k);
      std::ostringstream o;
      o << "[GROUP] key=" << k << " count=" << g.count;
      log_lines->push_back(o.str());
    }
  }
  (void)err;
  return true;
}

bool logical_agg_try_group_by_sum(const LogicalTable& t, std::string_view group_col, std::string_view sum_col,
                                  std::vector<std::string>* log_lines, std::string* err) {
  if (!t.agg_cache.valid) return false;
  const int gi = schema_col_index(t, group_col);
  const int si = schema_col_index(t, sum_col);
  if (gi < 0 || si < 0) return false;
  if (!is_int_type(t.schema[static_cast<std::size_t>(gi)].second) ||
      !is_int_type(t.schema[static_cast<std::size_t>(si)].second)) {
    return false;
  }
  const auto it = t.agg_cache.group_sum.find(group_pair_key(group_col, sum_col));
  if (it == t.agg_cache.group_sum.end()) return false;
  if (log_lines) {
    std::ostringstream hdr;
    hdr << "[GROUP BY] groups=" << it->second.size() << " col=" << group_col;
    log_lines->push_back(hdr.str());
    std::vector<std::string> keys;
    keys.reserve(it->second.size());
    for (const auto& kv : it->second) keys.push_back(kv.first);
    std::sort(keys.begin(), keys.end());
    for (const auto& k : keys) {
      const auto& g = it->second.at(k);
      std::ostringstream o;
      o << "[GROUP] key=" << k << " count=" << g.count << " sum=" << g.sum;
      log_lines->push_back(o.str());
    }
  }
  (void)err;
  return true;
}

void logical_col_sort_prewarm_int_columns(const LogicalTable& t) {
  if (t.rows.empty()) return;
  for (const auto& sc : t.schema) {
    if (!is_int_type(sc.second)) continue;
    (void)logical_col_sort_order(t, sc.first, false);
    (void)logical_col_sort_order(t, sc.first, true);
  }
}

void logical_table_refresh_analytics_caches(LogicalTable* t) {
  if (!t) return;
  if (t->row_ids_ordered.size() != t->rows.size()) {
    logical_row_index_rebuild_from_rows(t);
  }
  logical_agg_rebuild(t);
  logical_col_sort_prewarm_int_columns(*t);
}

}  // namespace structdb::client::mdb
