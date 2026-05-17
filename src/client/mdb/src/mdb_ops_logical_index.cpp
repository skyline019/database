#include "structdb/client/detail/mdb_ops_agg_cache.hpp"
#include "structdb/client/detail/mdb_ops_detail.hpp"
#include "structdb/client/detail/mdb_runner_internal.hpp"

#include <algorithm>
#include <cstdio>
#include <sstream>

namespace structdb::client::mdb {

long long parse_int_literal_view(std::string_view s) {
  std::size_t i = 0;
  int sign = 1;
  if (!s.empty() && (s[0] == '+' || s[0] == '-')) {
    if (s[0] == '-') sign = -1;
    ++i;
  }
  long long v = 0;
  for (; i < s.size(); ++i) v = v * 10 + static_cast<long long>(s[i] - '0');
  return v * sign;
}

int compare_row_ids(std::string_view a, std::string_view b) {
  if (is_int_literal(a) && is_int_literal(b)) {
    const long long la = parse_int_literal_view(a);
    const long long lb = parse_int_literal_view(b);
    if (la < lb) return -1;
    if (la > lb) return 1;
    return 0;
  }
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}

bool logical_row_ids_all_int_literals(const std::vector<std::string>& ids) {
  for (const auto& id : ids) {
    if (!is_int_literal(id)) return false;
  }
  return true;
}

void logical_row_ids_sort_numeric(std::vector<std::string>& ids, bool descending) {
  const auto less = [&](const std::string& a, const std::string& b) {
    const int c = compare_row_ids(a, b);
    if (descending) return c > 0;
    return c < 0;
  };
  std::sort(ids.begin(), ids.end(), less);
}

bool logical_row_ids_is_numeric_sorted(const std::vector<std::string>& ids, bool descending) {
  if (ids.size() < 2) return true;
  if (!logical_row_ids_all_int_literals(ids)) return false;
  for (std::size_t i = 1; i < ids.size(); ++i) {
    const int c = compare_row_ids(ids[i - 1], ids[i]);
    if (descending) {
      if (c < 0) return false;
    } else if (c > 0) {
      return false;
    }
  }
  return true;
}

const std::vector<std::string>* logical_row_ids_for_id_page(const LogicalTable& t, bool descending,
                                                            std::vector<std::string>* scratch) {
  if (!scratch) return nullptr;
  const int n_total = static_cast<int>(t.rows.size());
  if (n_total <= 0) return nullptr;
  if (t.row_ids_ordered.size() == t.rows.size()) {
    if (!descending && t.row_ids_sorted_asc) return &t.row_ids_ordered;
    if (descending && t.row_ids_sorted_desc) return &t.row_ids_ordered;
  }
  scratch->clear();
  scratch->reserve(t.rows.size());
  for (const auto& kv : t.rows) scratch->push_back(kv.first);
  logical_row_ids_sort_numeric(*scratch, descending);
  return scratch;
}

bool col_name_eq(std::string_view a, std::string_view b) {
  return a.size() == b.size() && detail::ascii_strncasecmp(a.data(), b.data(), a.size()) == 0;
}

bool is_string_type(std::string_view typ) {
  return ascii_starts_with_ci(typ, "string") || ascii_starts_with_ci(typ, "varchar") ||
         ascii_starts_with_ci(typ, "text") || field_type_is_char(typ);
}

bool is_int_type(std::string_view typ) {
  return ascii_starts_with_ci(typ, "int") || ascii_starts_with_ci(typ, "bigint") ||
         ascii_starts_with_ci(typ, "smallint") || ascii_starts_with_ci(typ, "tinyint");
}

int schema_col_index(const LogicalTable& t, std::string_view col) {
  for (std::size_t i = 0; i < t.schema.size(); ++i) {
    if (col_name_eq(t.schema[i].first, col)) return static_cast<int>(i);
  }
  return -1;
}

void logical_col_sort_invalidate(LogicalTable* t) {
  if (!t) return;
  t->col_sort_cache.clear();
}

namespace {

void logical_maybe_invalidate_col_sort(LogicalTable* t) {
  if (t && !t->col_sort_cache.empty()) logical_col_sort_invalidate(t);
}

void logical_maybe_invalidate_agg(LogicalTable* t) {
  if (t && t->agg_cache.valid) logical_agg_invalidate(t);
}

}  // namespace

void logical_rebuild_str_index(LogicalTable* t) {
  if (!t) return;
  t->str_idx.clear();
  for (const auto& sc : t->schema) {
    if (is_string_type(sc.second)) (void)t->str_idx[sc.first];
  }
  for (const auto& kv : t->rows) {
    const std::string& id = kv.first;
    for (std::size_t c = 0; c < t->schema.size(); ++c) {
      if (!is_string_type(t->schema[c].second)) continue;
      t->str_idx[t->schema[c].first].emplace(kv.second[c], id);
    }
  }
  logical_rebuild_int_index(t);
  logical_col_sort_invalidate(t);
}

void logical_rebuild_int_index(LogicalTable* t) {
  if (!t) return;
  t->int_idx.clear();
  for (const auto& sc : t->schema) {
    if (is_int_type(sc.second)) (void)t->int_idx[sc.first];
  }
  for (const auto& kv : t->rows) {
    const std::string& id = kv.first;
    for (std::size_t c = 0; c < t->schema.size(); ++c) {
      if (!is_int_type(t->schema[c].second)) continue;
      if (static_cast<std::size_t>(c) >= kv.second.size()) continue;
      t->int_idx[t->schema[c].first].emplace(kv.second[c], id);
    }
  }
}

void logical_int_index_remove_row(LogicalTable* t, const std::string& id, const std::vector<std::string>& cells) {
  if (!t) return;
  for (std::size_t c = 0; c < t->schema.size(); ++c) {
    if (!is_int_type(t->schema[c].second)) continue;
    if (static_cast<std::size_t>(c) >= cells.size()) continue;
    auto it_map = t->int_idx.find(t->schema[c].first);
    if (it_map == t->int_idx.end()) continue;
    auto& mm = it_map->second;
    const auto er = mm.equal_range(cells[c]);
    for (auto it = er.first; it != er.second;) {
      if (it->second == id)
        it = mm.erase(it);
      else
        ++it;
    }
  }
}

void logical_int_index_add_row(LogicalTable* t, const std::string& id, const std::vector<std::string>& cells) {
  if (!t) return;
  for (std::size_t c = 0; c < t->schema.size(); ++c) {
    if (!is_int_type(t->schema[c].second)) continue;
    if (static_cast<std::size_t>(c) >= cells.size()) continue;
    t->int_idx[t->schema[c].first].emplace(cells[c], id);
  }
}

void logical_str_index_remove_row(LogicalTable* t, const std::string& id, const std::vector<std::string>& cells) {
  if (!t) return;
  for (std::size_t c = 0; c < t->schema.size(); ++c) {
    if (!is_string_type(t->schema[c].second)) continue;
    auto it_map = t->str_idx.find(t->schema[c].first);
    if (it_map == t->str_idx.end()) continue;
    auto& mm = it_map->second;
    const auto er = mm.equal_range(cells[c]);
    for (auto it = er.first; it != er.second;) {
      if (it->second == id)
        it = mm.erase(it);
      else
        ++it;
    }
  }
  logical_int_index_remove_row(t, id, cells);
  logical_maybe_invalidate_col_sort(t);
  logical_maybe_invalidate_agg(t);
}

void logical_str_index_add_row(LogicalTable* t, const std::string& id, const std::vector<std::string>& cells) {
  if (!t) return;
  for (std::size_t c = 0; c < t->schema.size(); ++c) {
    if (!is_string_type(t->schema[c].second)) continue;
    t->str_idx[t->schema[c].first].emplace(cells[c], id);
  }
  logical_int_index_add_row(t, id, cells);
  logical_maybe_invalidate_col_sort(t);
  logical_maybe_invalidate_agg(t);
}

namespace {

int compare_cells_for_sort(const std::vector<std::string>& a, const std::vector<std::string>& b, int col_idx,
                           bool desc) {
  const std::string& va = a[static_cast<std::size_t>(col_idx)];
  const std::string& vb = b[static_cast<std::size_t>(col_idx)];
  int c = 0;
  if (is_int_literal(va) && is_int_literal(vb)) {
    c = compare_row_ids(va, vb);
  } else if (va < vb) {
    c = -1;
  } else if (va > vb) {
    c = 1;
  }
  if (desc) return c > 0;
  return c < 0;
}

}  // namespace

const std::vector<std::string>* logical_col_sort_order(const LogicalTable& t, std::string_view sort_col, bool desc) {
  const int col_idx = schema_col_index(t, sort_col);
  if (col_idx < 0) return nullptr;
  auto& entry = t.col_sort_cache[std::string(sort_col)];
  ColumnSortOrderCache& slot = desc ? entry.desc : entry.asc;
  if (slot.built && slot.ids.size() == t.rows.size()) return &slot.ids;
  slot.ids.clear();
  slot.ids.reserve(t.rows.size());
  using RowPtr = const std::pair<const std::string, std::vector<std::string>>*;
  std::vector<RowPtr> ptrs;
  ptrs.reserve(t.rows.size());
  for (const auto& row : t.rows) ptrs.push_back(&row);
  const auto col_less = [&](RowPtr a, RowPtr b) {
    return compare_cells_for_sort(a->second, b->second, col_idx, desc);
  };
  std::sort(ptrs.begin(), ptrs.end(), col_less);
  for (const auto* p : ptrs) slot.ids.push_back(p->first);
  slot.built = true;
  return &slot.ids;
}

LogicalTable clone_table(const LogicalTable& s) {
  LogicalTable d;
  d.name = s.name;
  d.schema = s.schema;
  d.rows = s.rows;
  d.pk_column = s.pk_column;
  d.str_idx = s.str_idx;
  d.int_idx = s.int_idx;
  d.named_indexes = s.named_indexes;
  d.col_sort_cache = s.col_sort_cache;
  d.agg_cache = s.agg_cache;
  d.mdb_persist_dirty_rows = s.mdb_persist_dirty_rows;
  d.mdb_persist_prev_cells = s.mdb_persist_prev_cells;
  d.mdb_persist_schema_dirty = s.mdb_persist_schema_dirty;
  d.row_ids_ordered = s.row_ids_ordered;
  d.row_ids_sorted_asc = s.row_ids_sorted_asc;
  d.row_ids_sorted_desc = s.row_ids_sorted_desc;
  return d;
}

void logical_row_index_invalidate_sort_flags(LogicalTable* t) {
  if (!t) return;
  t->row_ids_sorted_asc = false;
  t->row_ids_sorted_desc = false;
  logical_col_sort_invalidate(t);
  logical_agg_invalidate(t);
}

void logical_row_index_rebuild_from_rows(LogicalTable* t) {
  if (!t) return;
  t->row_ids_ordered.clear();
  t->row_ids_ordered.reserve(t->rows.size());
  for (const auto& kv : t->rows) t->row_ids_ordered.push_back(kv.first);
  logical_row_index_invalidate_sort_flags(t);
  if (logical_row_ids_all_int_literals(t->row_ids_ordered)) {
    logical_row_ids_sort_numeric(t->row_ids_ordered, false);
    t->row_ids_sorted_asc = true;
  } else {
    std::sort(t->row_ids_ordered.begin(), t->row_ids_ordered.end());
  }
}

void logical_row_index_insert(LogicalTable* t, const std::string& id) {
  if (!t) return;
  logical_row_index_invalidate_sort_flags(t);
  auto& v = t->row_ids_ordered;
  if (!v.empty() && logical_row_ids_all_int_literals(v) && is_int_literal(id)) {
    const int c = compare_row_ids(v.back(), id);
    if (c < 0) {
      v.push_back(id);
      return;
    }
    if (c == 0) return;
  }
  if (!v.empty() && logical_row_ids_all_int_literals(v) && is_int_literal(id)) {
    const auto it =
        std::lower_bound(v.begin(), v.end(), id,
                         [](const std::string& a, const std::string& b) { return compare_row_ids(a, b) < 0; });
    if (it != v.end() && *it == id) return;
    v.insert(it, id);
    return;
  }
  const auto it = std::lower_bound(v.begin(), v.end(), id);
  if (it != v.end() && *it == id) return;
  v.insert(it, id);
}

void logical_row_index_remove(LogicalTable* t, const std::string& id) {
  if (!t) return;
  logical_row_index_invalidate_sort_flags(t);
  auto& v = t->row_ids_ordered;
  const auto it = std::lower_bound(v.begin(), v.end(), id);
  if (it != v.end() && *it == id) v.erase(it);
}

std::string logical_row_index_newline_blob(const LogicalTable& t) {
  if (t.row_ids_ordered.size() == t.rows.size()) {
    std::string out;
    for (std::size_t i = 0; i < t.row_ids_ordered.size(); ++i) {
      if (i) out.push_back('\n');
      out.append(t.row_ids_ordered[i]);
    }
    return out;
  }
  std::string out;
  bool first = true;
  for (const auto& kv : t.rows) {
    if (!first) out.push_back('\n');
    first = false;
    out.append(kv.first);
  }
  return out;
}

void logical_persist_clear_dirty(LogicalTable* t) {
  if (!t) return;
  t->mdb_persist_dirty_rows.clear();
  t->mdb_persist_prev_cells.clear();
  t->mdb_persist_schema_dirty = false;
}

void logical_persist_invalidate_incremental(LogicalTable* t) {
  if (!t) return;
  t->mdb_persist_schema_dirty = true;
}

void logical_persist_mark_insert(LogicalTable* t, const std::string& id) {
  if (!t) return;
  t->mdb_persist_dirty_rows.insert(id);
  t->mdb_persist_prev_cells.erase(id);
  // Full-table bulk load: defer sorted `row_ids_ordered` until `persist_table` (avoids O(n^2) inserts).
  if (t->rows.size() > 8192 && t->mdb_persist_dirty_rows.size() == t->rows.size()) return;
  auto& ordered = t->row_ids_ordered;
  if (!ordered.empty() && id > ordered.back()) {
    ordered.push_back(id);
    return;
  }
  logical_row_index_insert(t, id);
}

void logical_persist_mark_update(LogicalTable* t, const std::string& id, std::vector<std::string> prev_cells) {
  if (!t) return;
  t->mdb_persist_dirty_rows.insert(id);
  if (t->mdb_persist_prev_cells.find(id) == t->mdb_persist_prev_cells.end()) {
    t->mdb_persist_prev_cells.emplace(id, std::move(prev_cells));
  }
}

void logical_persist_mark_delete_before_erase(LogicalTable* t, const std::string& id,
                                              const std::vector<std::string>& prev_cells) {
  if (!t) return;
  t->mdb_persist_dirty_rows.insert(id);
  t->mdb_persist_prev_cells[id] = prev_cells;
  logical_row_index_remove(t, id);
}

void logical_persist_mark_schema_change(LogicalTable* t) {
  if (!t) return;
  t->mdb_persist_schema_dirty = true;
  logical_row_index_rebuild_from_rows(t);
}

void mdb_append_iso_datetime(std::ostringstream& o, int y, int mo, int d, int h, int mi, int s) {
  char buf[40];
  if (h == 0 && mi == 0 && s == 0) {
    (void)std::snprintf(buf, sizeof buf, "%04d-%02d-%02d", y, mo, d);
  } else {
    (void)std::snprintf(buf, sizeof buf, "%04d-%02d-%02d %02d:%02d:%02d", y, mo, d, h, mi, s);
  }
  o << buf;
}

}  // namespace structdb::client::mdb
