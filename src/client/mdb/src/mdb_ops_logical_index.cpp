#include "structdb/client/detail/mdb_ops_detail.hpp"
#include "structdb/client/detail/mdb_runner_internal.hpp"

#include <cstdio>
#include <sstream>

namespace structdb::client::mdb {

bool col_name_eq(std::string_view a, std::string_view b) {
  return a.size() == b.size() && detail::ascii_strncasecmp(a.data(), b.data(), a.size()) == 0;
}

bool is_string_type(std::string_view typ) {
  return ascii_starts_with_ci(typ, "string") || ascii_starts_with_ci(typ, "varchar") ||
         ascii_starts_with_ci(typ, "text") || field_type_is_char(typ);
}

int schema_col_index(const LogicalTable& t, std::string_view col) {
  for (std::size_t i = 0; i < t.schema.size(); ++i) {
    if (col_name_eq(t.schema[i].first, col)) return static_cast<int>(i);
  }
  return -1;
}

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
}

void logical_str_index_add_row(LogicalTable* t, const std::string& id, const std::vector<std::string>& cells) {
  if (!t) return;
  for (std::size_t c = 0; c < t->schema.size(); ++c) {
    if (!is_string_type(t->schema[c].second)) continue;
    t->str_idx[t->schema[c].first].emplace(cells[c], id);
  }
}

LogicalTable clone_table(const LogicalTable& s) {
  LogicalTable d;
  d.name = s.name;
  d.schema = s.schema;
  d.rows = s.rows;
  d.pk_column = s.pk_column;
  d.str_idx = s.str_idx;
  d.mdb_persist_dirty_rows = s.mdb_persist_dirty_rows;
  d.mdb_persist_prev_cells = s.mdb_persist_prev_cells;
  d.mdb_persist_schema_dirty = s.mdb_persist_schema_dirty;
  return d;
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
}

void logical_persist_mark_schema_change(LogicalTable* t) {
  if (!t) return;
  t->mdb_persist_schema_dirty = true;
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
