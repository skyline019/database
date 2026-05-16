#include "structdb/client/mdb_query_paging.hpp"

#include "structdb/client/detail/mdb_ops_detail.hpp"
#include "structdb/client/detail/mdb_runner_internal.hpp"
#include "structdb/infra/tracer.hpp"

#include <algorithm>
#include <sstream>
#include <string>

namespace {

int compare_ids(std::string_view a, std::string_view b) {
  if (structdb::client::mdb::is_int_literal(a) && structdb::client::mdb::is_int_literal(b)) {
    const long long la = std::stoll(std::string(a));
    const long long lb = std::stoll(std::string(b));
    if (la < lb) return -1;
    if (la > lb) return 1;
    return 0;
  }
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}

}  // namespace

namespace structdb::client::mdb {

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
  if (sort_by_row_id) {
    if (start >= n_total) return true;
    using RowPtr = const std::pair<const std::string, std::vector<std::string>>*;
    std::vector<RowPtr> ptrs;
    ptrs.reserve(static_cast<std::size_t>(n_total));
    for (const auto& row : t.rows) ptrs.push_back(&row);
    const int need = std::min(start + page_sz, n_total);
    const auto row_less = [&](RowPtr a, RowPtr b) {
      const int c = compare_ids(a->first, b->first);
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
  if (start >= n_total) return true;
  using RowPtr = const std::pair<const std::string, std::vector<std::string>>*;
  std::vector<RowPtr> ptrs;
  ptrs.reserve(static_cast<std::size_t>(n_total));
  for (const auto& row : t.rows) ptrs.push_back(&row);
  const int need = std::min(start + page_sz, n_total);
  const auto col_less = [&](RowPtr a, RowPtr b) {
    int c = 0;
    const std::string& va = a->second[static_cast<std::size_t>(col_idx)];
    const std::string& vb = b->second[static_cast<std::size_t>(col_idx)];
    if (is_int_literal(va) && is_int_literal(vb)) {
      c = compare_ids(va, vb);
    } else if (va < vb)
      c = -1;
    else if (va > vb)
      c = 1;
    if (desc) return c > 0;
    return c < 0;
  };
  if (need > 0) {
    std::partial_sort(ptrs.begin(), ptrs.begin() + static_cast<std::ptrdiff_t>(need), ptrs.end(), col_less);
  }
  for (int i = start; i < start + page_sz && i < n_total; ++i) {
    std::ostringstream row;
    row << "  id=" << ptrs[static_cast<std::size_t>(i)]->first;
    log_line(sink, row.str());
  }
  return true;
}

bool handle_page_json(const LogicalTable& t, std::string_view inner, std::vector<std::string>* sink,
                      std::string* err) {
  infra::SpanGuard trace_pg("mdb.PAGE_JSON", 0);
  if (t.name.empty()) {
    if (err) *err = "PAGE_JSON: no table";
    return false;
  }
  auto args = split_csv_paren_content(inner);
  if (args.size() != 4) {
    if (err) *err = "PAGE_JSON: need 4 args (page_no,page_size,sort_col,asc|desc)";
    return false;
  }
  const int page_no = std::stoi(args[0]);
  const int page_sz = std::stoi(args[1]);
  const std::string sort_col = args[2];
  const bool desc = ascii_starts_with_ci(args[3], "desc");
  if (page_no < 1 || page_sz < 1) {
    if (err) *err = "PAGE_JSON: bad page/size";
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
      if (err) *err = "PAGE_JSON: unknown sort column";
      return false;
    }
  }
  const int start = (page_no - 1) * page_sz;
  const int n_total = static_cast<int>(t.rows.size());

  std::vector<std::pair<std::string, std::string>> col_defs;
  col_defs.emplace_back("id", "int");
  for (const auto& sc : t.schema) {
    if (col_name_eq(sc.first, "id")) continue;
    col_defs.push_back(sc);
  }

  std::ostringstream j;
  j << "[PAGE_JSON]{";
  j << "\"headers\":[";
  j << json_quote_cell("#");
  for (const auto& cd : col_defs) {
    j << ',' << json_quote_cell(cd.first);
  }
  j << "],\"columns\":[";
  for (std::size_t ci = 0; ci < col_defs.size(); ++ci) {
    if (ci) j << ',';
    j << "{\"name\":" << json_quote_cell(col_defs[ci].first) << ",\"ty\":" << json_quote_cell(col_defs[ci].second)
      << "}";
  }
  j << "],\"rows\":[";
  bool first_row = true;
  const auto append_row = [&](int rank_one_based, const std::string& row_id,
                              const std::vector<std::string>& cells) {
    if (!first_row) j << ',';
    first_row = false;
    j << '[' << json_quote_cell(std::to_string(rank_one_based));
    for (const auto& cd : col_defs) {
      j << ',';
      if (col_name_eq(cd.first, "id")) {
        j << json_quote_cell(row_id);
      } else {
        int idx = -1;
        for (std::size_t k = 0; k < t.schema.size(); ++k) {
          if (col_name_eq(t.schema[k].first, cd.first)) {
            idx = static_cast<int>(k);
            break;
          }
        }
        if (idx >= 0 && static_cast<std::size_t>(idx) < cells.size()) {
          j << json_quote_cell(cells[static_cast<std::size_t>(idx)]);
        } else {
          j << "\"\"";
        }
      }
    }
    j << ']';
  };

  if (sort_by_row_id) {
    if (start < n_total) {
      using RowPtr = const std::pair<const std::string, std::vector<std::string>>*;
      std::vector<RowPtr> ptrs;
      ptrs.reserve(static_cast<std::size_t>(n_total));
      for (const auto& row : t.rows) ptrs.push_back(&row);
      const int need = std::min(start + page_sz, n_total);
      const auto row_less = [&](RowPtr a, RowPtr b) {
        const int c = compare_ids(a->first, b->first);
        if (desc) return c > 0;
        return c < 0;
      };
      if (need > 0) {
        std::partial_sort(ptrs.begin(), ptrs.begin() + static_cast<std::ptrdiff_t>(need), ptrs.end(), row_less);
      }
      for (int i = start; i < start + page_sz && i < n_total; ++i) {
        const auto* ent = ptrs[static_cast<std::size_t>(i)];
        append_row(i + 1, ent->first, ent->second);
      }
    }
  } else if (start < n_total) {
    using RowPtr = const std::pair<const std::string, std::vector<std::string>>*;
    std::vector<RowPtr> ptrs;
    ptrs.reserve(static_cast<std::size_t>(n_total));
    for (const auto& row : t.rows) ptrs.push_back(&row);
    const int need = std::min(start + page_sz, n_total);
    const auto col_less = [&](RowPtr a, RowPtr b) {
      int c = 0;
      const std::string& va = a->second[static_cast<std::size_t>(col_idx)];
      const std::string& vb = b->second[static_cast<std::size_t>(col_idx)];
      if (is_int_literal(va) && is_int_literal(vb)) {
        c = compare_ids(va, vb);
      } else if (va < vb)
        c = -1;
      else if (va > vb)
        c = 1;
      if (desc) return c > 0;
      return c < 0;
    };
    if (need > 0) {
      std::partial_sort(ptrs.begin(), ptrs.begin() + static_cast<std::ptrdiff_t>(need), ptrs.end(), col_less);
    }
    for (int i = start; i < start + page_sz && i < n_total; ++i) {
      const auto* ent = ptrs[static_cast<std::size_t>(i)];
      append_row(i + 1, ent->first, ent->second);
    }
  }
  j << "]}";
  log_line(sink, j.str());
  return true;
}

}  // namespace structdb::client::mdb
