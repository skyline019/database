#include "structdb/client/mdb_query_paging.hpp"

#include "structdb/client/detail/mdb_ops_detail.hpp"
#include "structdb/client/detail/mdb_runner_internal.hpp"
#include "structdb/infra/tracer.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <string>
#include <unordered_map>
#include <vector>

namespace {

constexpr int kPagePartialSortMaxStart = 8192;

bool should_page_id_by_cursor(const structdb::client::mdb::LogicalTable& t, int start, int page_sz, int n_total,
                              bool desc) {
  if (n_total <= 0) return false;
  if (t.row_ids_ordered.size() == static_cast<std::size_t>(n_total) &&
      ((!desc && t.row_ids_sorted_asc) || (desc && t.row_ids_sorted_desc))) {
    return true;
  }
  if (start >= kPagePartialSortMaxStart) return true;
  if (start + page_sz >= n_total) return true;
  return false;
}

bool should_page_col_by_cursor(const structdb::client::mdb::LogicalTable& t, std::string_view sort_col, int start,
                               int page_sz, int n_total, bool desc) {
  if (n_total <= 0) return false;
  const std::vector<std::string>* order = structdb::client::mdb::logical_col_sort_order(t, sort_col, desc);
  if (!order || order->size() != static_cast<std::size_t>(n_total)) return false;
  (void)start;
  (void)page_sz;
  (void)desc;
  return true;
}

using RowPtr = const std::pair<const std::string, std::vector<std::string>>*;

void fill_page_row_ids_by_column(const structdb::client::mdb::LogicalTable& t, int col_idx, bool desc, int start,
                                 int page_sz, std::vector<std::string>* out) {
  out->clear();
  const int n_total = static_cast<int>(t.rows.size());
  if (start >= n_total || page_sz <= 0) return;
  const int need_end = std::min(start + page_sz, n_total);
  std::vector<RowPtr> ptrs;
  ptrs.reserve(static_cast<std::size_t>(n_total));
  for (const auto& row : t.rows) ptrs.push_back(&row);
  const auto col_less = [&](RowPtr a, RowPtr b) {
    int c = 0;
    const std::string& va = a->second[static_cast<std::size_t>(col_idx)];
    const std::string& vb = b->second[static_cast<std::size_t>(col_idx)];
    if (structdb::client::mdb::is_int_literal(va) && structdb::client::mdb::is_int_literal(vb)) {
      c = structdb::client::mdb::compare_row_ids(va, vb);
    } else if (va < vb) {
      c = -1;
    } else if (va > vb) {
      c = 1;
    }
    if (desc) return c > 0;
    return c < 0;
  };
  if (start > 0) {
    std::nth_element(ptrs.begin(), ptrs.begin() + static_cast<std::ptrdiff_t>(start), ptrs.end(), col_less);
  }
  if (need_end > start) {
    std::partial_sort(ptrs.begin() + static_cast<std::ptrdiff_t>(start),
                      ptrs.begin() + static_cast<std::ptrdiff_t>(need_end), ptrs.end(), col_less);
  }
  out->reserve(static_cast<std::size_t>(need_end - start));
  for (int i = start; i < need_end; ++i) out->push_back(ptrs[static_cast<std::size_t>(i)]->first);
}

void json_append_quoted(std::string& out, std::string_view s) {
  out.push_back('"');
  for (unsigned char uc : s) {
    const char c = static_cast<char>(uc);
    if (c == '"' || c == '\\') out.push_back('\\');
    if (c == '\n' || c == '\r' || c == '\t') {
      out.push_back(' ');
      continue;
    }
    out.push_back(c);
  }
  out.push_back('"');
}

void json_append_id_cell(std::string& out, std::string_view s) {
  if (!structdb::client::mdb::is_int_literal(s)) {
    json_append_quoted(out, s);
    return;
  }
  out.push_back('"');
  out.append(s);
  out.push_back('"');
}

void json_append_u32(std::string& out, std::uint32_t v) {
  char buf[16];
  const int n = std::snprintf(buf, sizeof buf, "%u", v);
  if (n > 0) out.append(buf, static_cast<std::size_t>(n));
}

struct PageJsonColSlot {
  bool is_row_id{false};
  int schema_idx{-1};
};

struct PageJsonColDef {
  std::string name;
  std::string ty;
  PageJsonColSlot slot{};
};

struct PageJsonRequest {
  bool after_mode{false};
  std::string after_cursor;
  int page_no{1};
  int page_sz{0};
  std::string sort_col;
  bool desc{false};
  bool stream_mode{false};
  bool ids_only{false};
  std::vector<std::string> col_filter;
};

bool parse_page_json_request(std::vector<std::string> args, PageJsonRequest* req, std::string* err) {
  if (!req) return false;
  if (args.size() < 4) {
    if (err) *err = "PAGE_JSON: need page_no,page_size,sort_col,asc|desc or AFTER,cursor,page_size,sort_col,asc|desc";
    return false;
  }
  std::size_t col_start = 0;
  if (structdb::client::mdb::ascii_starts_with_ci(args[0], "after")) {
    if (args.size() < 5) {
      if (err) *err = "PAGE_JSON AFTER: need AFTER,cursor,page_size,sort_col,asc|desc";
      return false;
    }
    req->after_mode = true;
    req->after_cursor = std::move(args[1]);
    req->page_sz = std::stoi(args[2]);
    req->sort_col = std::move(args[3]);
    req->desc = structdb::client::mdb::ascii_starts_with_ci(args[4], "desc");
    col_start = 5;
  } else {
    req->page_no = std::stoi(args[0]);
    req->page_sz = std::stoi(args[1]);
    req->sort_col = std::move(args[2]);
    req->desc = structdb::client::mdb::ascii_starts_with_ci(args[3], "desc");
    col_start = 4;
  }
  if (req->page_sz < 1 || (!req->after_mode && req->page_no < 1)) {
    if (err) *err = "PAGE_JSON: bad page/size";
    return false;
  }
  if (col_start < args.size()) {
    if (structdb::client::mdb::ascii_starts_with_ci(args[col_start], "cols")) {
      ++col_start;
    }
    for (std::size_t i = col_start; i < args.size(); ++i) {
      if (args[i].empty()) continue;
      if (structdb::client::mdb::ascii_starts_with_ci(args[i], "stream")) {
        req->stream_mode = true;
        continue;
      }
      if (structdb::client::mdb::ascii_starts_with_ci(args[i], "ids_only")) {
        req->ids_only = true;
        continue;
      }
      req->col_filter.push_back(std::move(args[i]));
    }
  }
  return true;
}

const std::unordered_map<std::string, const std::vector<std::string>*>* ensure_page_row_ptr_cache(
    const structdb::client::mdb::LogicalTable& t, structdb::client::mdb::MdbQueryPagingState* paging) {
  if (!paging) return nullptr;
  if (paging->row_ptr_cache_table != t.name || paging->row_ptr_cache_rows != t.rows.size()) {
    paging->row_ptr_cache_table = t.name;
    paging->row_ptr_cache_rows = t.rows.size();
    paging->row_ptr_by_id.clear();
    paging->row_ptr_by_id.reserve(t.rows.size());
    for (const auto& kv : t.rows) paging->row_ptr_by_id.emplace(kv.first, &kv.second);
  }
  return &paging->row_ptr_by_id;
}

void json_append_rank(std::string& out, int rank) {
  out.push_back('"');
  char buf[24];
  const int n = std::snprintf(buf, sizeof buf, "%d", rank);
  if (n > 0) out.append(buf, static_cast<std::size_t>(n));
  out.push_back('"');
}

const std::vector<std::string>* resolve_row_cells(
    const structdb::client::mdb::LogicalTable& t, const std::string& row_id,
    const std::unordered_map<std::string, const std::vector<std::string>*>* ptr_cache) {
  if (ptr_cache) {
    const auto it = ptr_cache->find(row_id);
    if (it == ptr_cache->end()) return nullptr;
    return it->second;
  }
  const auto it = t.rows.find(row_id);
  if (it == t.rows.end()) return nullptr;
  return &it->second;
}

const std::vector<std::string>* resolve_row_cells_at(
    const structdb::client::mdb::LogicalTable& t, const std::vector<std::string>& order, std::size_t index,
    const std::unordered_map<std::string, const std::vector<std::string>*>* ptr_cache) {
  if (index >= order.size()) return nullptr;
  return resolve_row_cells(t, order[index], ptr_cache);
}

void append_one_page_json_row(std::string& j, int rank, std::string_view row_id,
                              const std::vector<std::string>& cells, const std::vector<PageJsonColDef>& col_defs) {
  j.push_back('[');
  json_append_rank(j, rank);
  for (const auto& cd : col_defs) {
    j.push_back(',');
      if (cd.slot.is_row_id) {
        json_append_id_cell(j, row_id);
      } else if (cd.slot.schema_idx >= 0 && static_cast<std::size_t>(cd.slot.schema_idx) < cells.size()) {
        json_append_id_cell(j, cells[static_cast<std::size_t>(cd.slot.schema_idx)]);
    } else {
      j.append("\"\"");
    }
  }
  j.push_back(']');
}

bool build_page_json_columns(const structdb::client::mdb::LogicalTable& t, const PageJsonRequest& req,
                             std::vector<PageJsonColDef>* out, std::string* err) {
  if (!out) return false;
  out->clear();
  auto want_col = [&](std::string_view name) {
    if (req.col_filter.empty()) return true;
    for (const auto& c : req.col_filter) {
      if (structdb::client::mdb::col_name_eq(c, name)) return true;
    }
    return false;
  };
  if (want_col("id")) {
    PageJsonColDef cd;
    cd.name = "id";
    cd.ty = "int";
    cd.slot.is_row_id = true;
    out->push_back(std::move(cd));
  }
  for (std::size_t si = 0; si < t.schema.size(); ++si) {
    if (structdb::client::mdb::col_name_eq(t.schema[si].first, "id")) continue;
    if (!want_col(t.schema[si].first)) continue;
    PageJsonColDef cd;
    cd.name = t.schema[si].first;
    cd.ty = t.schema[si].second;
    cd.slot.schema_idx = static_cast<int>(si);
    out->push_back(cd);
  }
  if (out->empty()) {
    if (err) *err = "PAGE_JSON: no columns selected";
    return false;
  }
  return true;
}

std::size_t find_after_index(const std::vector<std::string>& order, std::string_view after_cursor, bool desc) {
  if (order.empty()) return 0;
  if (after_cursor.empty() || after_cursor == "0") return 0;
  if (!desc) {
    std::size_t lo = 0;
    std::size_t hi = order.size();
    while (lo < hi) {
      const std::size_t mid = lo + (hi - lo) / 2;
      if (structdb::client::mdb::compare_row_ids(order[mid], after_cursor) <= 0)
        lo = mid + 1;
      else
        hi = mid;
    }
    return lo;
  }
  std::size_t lo = 0;
  std::size_t hi = order.size();
  while (lo < hi) {
    const std::size_t mid = lo + (hi - lo) / 2;
    if (structdb::client::mdb::compare_row_ids(order[mid], after_cursor) < 0)
      hi = mid;
    else
      lo = mid + 1;
  }
  return lo;
}

void append_page_ids_array(std::string& j, const std::vector<std::string>& order, std::size_t begin,
                           std::size_t end) {
  bool first = true;
  j.append("\"ids\":[");
  for (std::size_t i = begin; i < end; ++i) {
    if (!first) j.push_back(',');
    first = false;
    json_append_id_cell(j, order[i]);
  }
  j.push_back(']');
}

void emit_page_json_ids_only_meta(std::string& j, const PageJsonRequest& req, std::size_t returned,
                                  const std::string* next_after_out, bool has_more) {
  j.append(req.stream_mode ? "[PAGE_JSON_META]{" : "[PAGE_JSON]{");
  j.append("\"page\":{\"mode\":\"");
  j.append(req.after_mode ? "after" : "page");
  if (req.after_mode) {
    j.append("\",\"after\":");
    json_append_id_cell(j, req.after_cursor);
  } else {
    j.append("\",\"page_no\":");
    json_append_u32(j, static_cast<std::uint32_t>(req.page_no));
  }
  j.append(",\"page_size\":");
  json_append_u32(j, static_cast<std::uint32_t>(req.page_sz));
  j.append(",\"returned\":");
  json_append_u32(j, static_cast<std::uint32_t>(returned));
  if (req.after_mode && next_after_out && !next_after_out->empty()) {
    j.append(",\"next_after\":");
    json_append_id_cell(j, *next_after_out);
  }
  if (has_more) {
    j.append(",\"has_more\":true");
  } else {
    j.append(",\"has_more\":false");
  }
  j.append(",\"ids_only\":true}");
}

void emit_page_json_page_meta(std::string& j, const PageJsonRequest& req, std::size_t returned, int page_no_for_meta,
                              const std::string* next_after_out, bool has_more, bool ids_only) {
  j.append(req.stream_mode ? "[PAGE_JSON_META]{" : "[PAGE_JSON]{");
  j.append("\"page\":{");
  if (req.after_mode) {
    j.append("\"mode\":\"after\",\"after\":");
    json_append_quoted(j, req.after_cursor);
    j.append(",\"page_size\":");
    j.append(std::to_string(req.page_sz));
    if (next_after_out && !next_after_out->empty()) {
      j.append(",\"next_after\":");
      json_append_quoted(j, *next_after_out);
    }
    j.append(",\"returned\":");
    j.append(std::to_string(returned));
    j.append(has_more ? ",\"has_more\":true" : ",\"has_more\":false");
  } else {
    j.append("\"mode\":\"page\",\"page_no\":");
    j.append(std::to_string(page_no_for_meta));
    j.append(",\"page_size\":");
    j.append(std::to_string(req.page_sz));
    j.append(",\"returned\":");
    j.append(std::to_string(returned));
  }
  if (ids_only) j.append(",\"ids_only\":true");
  j.append("}");
}

void emit_page_json_ids_body(const structdb::client::mdb::LogicalTable& /*t*/, const PageJsonRequest& req,
                             const std::vector<std::string>& order, std::size_t begin, std::size_t end,
                             const std::string* next_after_out, bool has_more,
                             structdb::client::mdb::MdbQueryPagingState* paging, std::vector<std::string>* sink) {
  const std::size_t returned = end > begin ? end - begin : 0;
  std::string local_buf;
  std::string& j = paging ? paging->json_emit_buf : local_buf;
  j.clear();
  j.reserve(returned * 10 + 96);
  emit_page_json_ids_only_meta(j, req, returned, next_after_out, has_more);
  if (!req.stream_mode) {
    j.push_back(',');
    append_page_ids_array(j, order, begin, end);
    j.append("]}");
    structdb::client::mdb::log_line(sink, j);
    return;
  }
  j.push_back('}');
  structdb::client::mdb::log_line(sink, j);
  std::string& ids_line = paging ? paging->json_emit_buf : local_buf;
  ids_line.clear();
  ids_line.reserve(returned * 10 + 24);
  ids_line.append("[PAGE_JSON_IDS]");
  append_page_ids_array(ids_line, order, begin, end);
  structdb::client::mdb::log_line(sink, ids_line);
  structdb::client::mdb::log_line(sink, "[PAGE_JSON_END]");
}

void emit_page_json_meta(std::string& j, const PageJsonRequest& req, const std::vector<PageJsonColDef>& col_defs,
                         std::size_t returned, int page_no_for_meta, const std::string* next_after_out, bool has_more,
                         bool include_rows_key) {
  const bool compact = !req.col_filter.empty();
  j.reserve(j.size() + returned * (32 + col_defs.size() * 12) + 256);
  emit_page_json_page_meta(j, req, returned, page_no_for_meta, next_after_out, has_more, false);
  if (compact) j.append(",\"compact\":true");
  j.append(",\"headers\":[");
  json_append_quoted(j, "#");
  for (const auto& cd : col_defs) {
    j.push_back(',');
    json_append_quoted(j, cd.name);
  }
  j.push_back(']');
  if (!compact) {
    j.append(",\"columns\":[");
    for (std::size_t ci = 0; ci < col_defs.size(); ++ci) {
      if (ci) j.push_back(',');
      j.append("{\"name\":");
      json_append_quoted(j, col_defs[ci].name);
      j.append(",\"ty\":");
      json_append_quoted(j, col_defs[ci].ty);
      j.push_back('}');
    }
    j.push_back(']');
    j.push_back(',');
  } else {
    j.push_back(',');
  }
  if (include_rows_key) {
    j.append("\"rows\":[");
  } else {
    j.append("}");
  }
}

void emit_page_json_body(const structdb::client::mdb::LogicalTable& t, const PageJsonRequest& req,
                         const std::vector<PageJsonColDef>& col_defs, const std::vector<std::string>& order,
                         std::size_t begin, std::size_t end, int page_no_for_meta, int rank_start,
                         const std::string* next_after_out, bool has_more,
                         const std::unordered_map<std::string, const std::vector<std::string>*>* ptr_cache,
                         structdb::client::mdb::MdbQueryPagingState* paging, std::vector<std::string>* sink) {
  const std::size_t returned = end > begin ? end - begin : 0;
  std::string local_buf;
  std::string& j = paging ? paging->json_emit_buf : local_buf;
  j.clear();
  j.reserve(returned * (24 + col_defs.size() * 14) + 192);
  emit_page_json_meta(j, req, col_defs, returned, page_no_for_meta, next_after_out, has_more, !req.stream_mode);
  if (!req.stream_mode) {
    bool first_row = true;
    for (std::size_t i = begin; i < end; ++i) {
      const std::vector<std::string>* cells = resolve_row_cells_at(t, order, i, ptr_cache);
      if (!cells) continue;
      if (!first_row) j.push_back(',');
      first_row = false;
      append_one_page_json_row(j, rank_start + static_cast<int>(i - begin) + 1, order[i], *cells, col_defs);
    }
    j.append("]}");
    structdb::client::mdb::log_line(sink, j);
    return;
  }
  structdb::client::mdb::log_line(sink, j);
  std::string& row_buf = paging ? paging->json_emit_buf : local_buf;
  for (std::size_t i = begin; i < end; ++i) {
    const std::vector<std::string>* cells = resolve_row_cells_at(t, order, i, ptr_cache);
    if (!cells) continue;
    row_buf.clear();
    row_buf.reserve(40 + col_defs.size() * 16);
    row_buf.append("[PAGE_JSON_ROW]");
    append_one_page_json_row(row_buf, rank_start + static_cast<int>(i - begin) + 1, order[i], *cells, col_defs);
    structdb::client::mdb::log_line(sink, row_buf);
  }
  structdb::client::mdb::log_line(sink, "[PAGE_JSON_END]");
}

}  // namespace

namespace structdb::client::mdb {

void mdb_paging_reset_table_caches(MdbQueryPagingState* paging) {
  if (!paging) return;
  paging->row_ptr_cache_table.clear();
  paging->row_ptr_cache_rows = 0;
  paging->row_ptr_by_id.clear();
  paging->row_dense_table.clear();
  paging->row_dense_rows = 0;
  paging->row_dense_ordered = 0;
  paging->row_cells_dense.clear();
  paging->json_emit_buf.clear();
}

bool handle_page(const LogicalTable& t, std::string_view inner, std::vector<std::string>* sink, std::string* err) {
  infra::SpanGuard trace_page("mdb.PAGE", 0);
  auto args = split_csv_paren_content(inner);
  if (args.size() != 4) {
    if (err) *err = "PAGE: need 4 args";
    return false;
  }
  const int page_no = std::stoi(args[0]);
  const int page_sz = std::stoi(args[1]);
  const std::string sort_col = args[2];
  const bool desc = ascii_starts_with_ci(args[3], "desc");
  if (page_no < 1 || page_sz < 1) {
    if (err) *err = "PAGE: bad page/size";
    return false;
  }
  const bool sort_by_row_id =
      (sort_col.size() == 2 && detail::ascii_strncasecmp(sort_col.data(), "id", 2) == 0);
  int col_idx = -1;
  if (!sort_by_row_id) {
    for (std::size_t i = 0; i < t.schema.size(); ++i) {
      if (t.schema[i].first.size() == sort_col.size() &&
          detail::ascii_strncasecmp(t.schema[i].first.data(), sort_col.data(), sort_col.size()) == 0) {
        col_idx = static_cast<int>(i);
        break;
      }
    }
    if (col_idx < 0) {
      if (err) *err = "PAGE: unknown sort column";
      return false;
    }
  }
  const int start = (page_no - 1) * page_sz;
  const int n_total = static_cast<int>(t.rows.size());
  std::ostringstream o;
  o << "[PAGE] table=" << t.name << " page=" << page_no << " size=" << page_sz;
  log_line(sink, o.str());
  if (start >= n_total) return true;

  if (sort_by_row_id && should_page_id_by_cursor(t, start, page_sz, n_total, desc)) {
    std::vector<std::string> scratch;
    const std::vector<std::string>* order = logical_row_ids_for_id_page(t, desc, &scratch);
    if (order) {
      for (int i = start; i < start + page_sz && i < n_total; ++i) {
        std::ostringstream row;
        row << "  id=" << (*order)[static_cast<std::size_t>(i)];
        log_line(sink, row.str());
      }
      return true;
    }
  }

  if (!sort_by_row_id && should_page_col_by_cursor(t, sort_col, start, page_sz, n_total, desc)) {
    const std::vector<std::string>* order = logical_col_sort_order(t, sort_col, desc);
    if (order) {
      for (int i = start; i < start + page_sz && i < n_total; ++i) {
        std::ostringstream row;
        row << "  id=" << (*order)[static_cast<std::size_t>(i)];
        log_line(sink, row.str());
      }
      return true;
    }
  }

  if (sort_by_row_id) {
    std::vector<RowPtr> ptrs;
    ptrs.reserve(static_cast<std::size_t>(n_total));
    for (const auto& row : t.rows) ptrs.push_back(&row);
    const int need = std::min(start + page_sz, n_total);
    const auto row_less = [&](RowPtr a, RowPtr b) {
      const int c = compare_row_ids(a->first, b->first);
      if (desc) return c > 0;
      return c < 0;
    };
    if (need > 0) {
      std::partial_sort(ptrs.begin(), ptrs.begin() + static_cast<std::ptrdiff_t>(need), ptrs.end(), row_less);
    }
    for (int i = start; i < start + page_sz && i < n_total; ++i) {
      std::ostringstream row;
      row << "  id=" << ptrs[static_cast<std::size_t>(i)]->first;
      log_line(sink, row.str());
    }
    return true;
  }

  std::vector<std::string> page_ids;
  fill_page_row_ids_by_column(t, col_idx, desc, start, page_sz, &page_ids);
  for (const std::string& rid : page_ids) {
    std::ostringstream row;
    row << "  id=" << rid;
    log_line(sink, row.str());
  }
  return true;
}

bool handle_page_json(const LogicalTable& t, std::string_view inner, std::vector<std::string>* sink, std::string* err,
                      MdbQueryPagingState* paging) {
  infra::SpanGuard trace_pg("mdb.PAGE_JSON", 0);
  if (t.name.empty()) {
    if (err) *err = "PAGE_JSON: no table";
    return false;
  }
  auto args = split_csv_paren_content(inner);
  PageJsonRequest req;
  if (!parse_page_json_request(std::move(args), &req, err)) return false;

  const bool sort_by_row_id =
      (req.sort_col.size() == 2 && detail::ascii_strncasecmp(req.sort_col.data(), "id", 2) == 0);
  int col_idx = -1;
  if (!sort_by_row_id) {
    for (std::size_t i = 0; i < t.schema.size(); ++i) {
      if (t.schema[i].first.size() == req.sort_col.size() &&
          detail::ascii_strncasecmp(t.schema[i].first.data(), req.sort_col.data(), req.sort_col.size()) == 0) {
        col_idx = static_cast<int>(i);
        break;
      }
    }
    if (col_idx < 0) {
      if (err) *err = "PAGE_JSON: unknown sort column";
      return false;
    }
  }

  if (req.ids_only && !sort_by_row_id) {
    if (err) *err = "PAGE_JSON IDS_ONLY: sort column must be id";
    return false;
  }

  std::vector<PageJsonColDef> col_defs;
  if (!req.ids_only && !build_page_json_columns(t, req, &col_defs, err)) return false;

  const int n_total = static_cast<int>(t.rows.size());
  std::vector<std::string> scratch_order;
  const std::vector<std::string>* order_ptr = nullptr;

  const auto emit_slice = [&](const std::vector<std::string>& order, std::size_t begin, std::size_t end,
                              int page_no_for_meta, int rank_start, const std::string* next_after, bool has_more) {
    if (req.ids_only) {
      emit_page_json_ids_body(t, req, order, begin, end, next_after, has_more, paging, sink);
      return;
    }
    const auto* ptr_cache = ensure_page_row_ptr_cache(t, paging);
    emit_page_json_body(t, req, col_defs, order, begin, end, page_no_for_meta, rank_start, next_after, has_more,
                        ptr_cache, paging, sink);
  };

  const auto ensure_order = [&]() -> const std::vector<std::string>* {
    if (order_ptr) return order_ptr;
    if (sort_by_row_id) {
      order_ptr = logical_row_ids_for_id_page(t, req.desc, &scratch_order);
      return order_ptr;
    }
    scratch_order.clear();
    scratch_order.reserve(t.rows.size());
    for (const auto& kv : t.rows) scratch_order.push_back(kv.first);
    logical_row_ids_sort_numeric(scratch_order, req.desc);
    order_ptr = &scratch_order;
    return order_ptr;
  };

  if (req.after_mode) {
    if (n_total <= 0) {
      const std::vector<std::string> empty;
      emit_slice(empty, 0, 0, req.page_no, 0, nullptr, false);
      return true;
    }
    if (!sort_by_row_id) {
      if (err) *err = "PAGE_JSON AFTER: sort column must be id for cursor paging";
      return false;
    }
    const std::vector<std::string>* order = ensure_order();
    if (!order) {
      if (err) *err = "PAGE_JSON AFTER: no row order";
      return false;
    }
    const std::size_t begin = find_after_index(*order, req.after_cursor, req.desc);
    const std::size_t end = std::min(begin + static_cast<std::size_t>(req.page_sz), order->size());
    std::string next_after;
    const bool has_more = end < order->size();
    if (has_more && end > begin) next_after = (*order)[end - 1];
    emit_slice(*order, begin, end, req.page_no, 0, has_more ? &next_after : nullptr, has_more);
    return true;
  }

  const int start = (req.page_no - 1) * req.page_sz;
  if (start >= n_total) {
    const std::vector<std::string> empty;
    emit_slice(empty, 0, 0, req.page_no, start, nullptr, false);
    return true;
  }

  if (sort_by_row_id && should_page_id_by_cursor(t, start, req.page_sz, n_total, req.desc)) {
    const std::vector<std::string>* order = ensure_order();
    if (order) {
      const std::size_t begin = static_cast<std::size_t>(start);
      const std::size_t end = std::min(begin + static_cast<std::size_t>(req.page_sz), order->size());
      emit_slice(*order, begin, end, req.page_no, start, nullptr, end < order->size());
      return true;
    }
  }

  if (!sort_by_row_id && should_page_col_by_cursor(t, req.sort_col, start, req.page_sz, n_total, req.desc)) {
    const std::vector<std::string>* order = logical_col_sort_order(t, req.sort_col, req.desc);
    if (order) {
      const std::size_t begin = static_cast<std::size_t>(start);
      const std::size_t end = std::min(begin + static_cast<std::size_t>(req.page_sz), order->size());
      emit_slice(*order, begin, end, req.page_no, start, nullptr, end < order->size());
      return true;
    }
  }

  if (sort_by_row_id) {
    std::vector<RowPtr> ptrs;
    ptrs.reserve(static_cast<std::size_t>(n_total));
    for (const auto& row : t.rows) ptrs.push_back(&row);
    const int need = std::min(start + req.page_sz, n_total);
    const auto row_less = [&](RowPtr a, RowPtr b) {
      const int c = compare_row_ids(a->first, b->first);
      if (req.desc) return c > 0;
      return c < 0;
    };
    if (need > 0) {
      std::partial_sort(ptrs.begin(), ptrs.begin() + static_cast<std::ptrdiff_t>(need), ptrs.end(), row_less);
    }
    scratch_order.clear();
    scratch_order.reserve(static_cast<std::size_t>(need - start));
    for (int i = start; i < start + req.page_sz && i < n_total; ++i) {
      scratch_order.push_back(ptrs[static_cast<std::size_t>(i)]->first);
    }
    emit_slice(scratch_order, 0, scratch_order.size(), req.page_no, start, nullptr, start + req.page_sz < n_total);
    return true;
  }

  fill_page_row_ids_by_column(t, col_idx, req.desc, start, req.page_sz, &scratch_order);
  emit_slice(scratch_order, 0, scratch_order.size(), req.page_no, start, nullptr, start + req.page_sz < n_total);
  return true;
}

}  // namespace structdb::client::mdb
