#include "structdb/client/detail/mdb_runner_internal.hpp"

#include <cctype>
#include <sstream>
#include <unordered_set>
#include <utility>
#include <vector>

namespace structdb::client::mdb {

namespace {

void jskip_ws(std::string_view& in) {
  while (!in.empty() && static_cast<unsigned char>(in.front()) <= 32) in.remove_prefix(1);
}

bool jparse_string(std::string_view& in, std::string* out) {
  jskip_ws(in);
  if (in.empty() || in[0] != '"') return false;
  in.remove_prefix(1);
  out->clear();
  while (!in.empty()) {
    const unsigned char c = static_cast<unsigned char>(in[0]);
    if (c == '"') {
      in.remove_prefix(1);
      return true;
    }
    if (c == '\\') {
      in.remove_prefix(1);
      if (in.empty()) return false;
      const char e = in[0];
      in.remove_prefix(1);
      if (e == 'n')
        out->push_back('\n');
      else if (e == 'r')
        out->push_back('\r');
      else if (e == 't')
        out->push_back('\t');
      else
        out->push_back(e);
      continue;
    }
    out->push_back(static_cast<char>(c));
    in.remove_prefix(1);
  }
  return false;
}

bool jparse_number_token(std::string_view& in, std::string* out) {
  jskip_ws(in);
  if (in.empty()) return false;
  std::size_t i = 0;
  if (in[0] == '-') i = 1;
  if (i >= in.size() || !std::isdigit(static_cast<unsigned char>(in[i]))) return false;
  std::size_t j = i;
  while (j < in.size() && std::isdigit(static_cast<unsigned char>(in[j]))) ++j;
  if (j < in.size() && in[j] == '.') {
    ++j;
    while (j < in.size() && std::isdigit(static_cast<unsigned char>(in[j]))) ++j;
  }
  *out = std::string(in.substr(0, j));
  in.remove_prefix(j);
  return true;
}

bool jparse_cell(std::string_view& in, std::string* out) {
  jskip_ws(in);
  if (in.empty()) return false;
  if (in[0] == '"') return jparse_string(in, out);
  return jparse_number_token(in, out);
}

bool jexpect(std::string_view& in, char ch) {
  jskip_ws(in);
  if (in.empty() || in[0] != ch) return false;
  in.remove_prefix(1);
  return true;
}

bool skip_json_value(std::string_view& in, std::string* err) {
  jskip_ws(in);
  if (in.empty()) return false;
  const char c = in[0];
  if (c == '"') {
    std::string tmp;
    return jparse_string(in, &tmp);
  }
  if (c == '{') {
    in.remove_prefix(1);
    for (;;) {
      jskip_ws(in);
      if (!in.empty() && in[0] == '}') {
        in.remove_prefix(1);
        return true;
      }
      std::string key;
      if (!jparse_string(in, &key)) {
        if (err) *err = "CONFIRM_REORDER: skip object key";
        return false;
      }
      if (!jexpect(in, ':')) {
        if (err) *err = "CONFIRM_REORDER: skip object ':'";
        return false;
      }
      if (!skip_json_value(in, err)) return false;
      jskip_ws(in);
      if (!in.empty() && in[0] == ',') {
        in.remove_prefix(1);
        continue;
      }
      if (!in.empty() && in[0] == '}') {
        in.remove_prefix(1);
        return true;
      }
      if (err) *err = "CONFIRM_REORDER: skip object ',' or '}'";
      return false;
    }
  }
  if (c == '[') {
    in.remove_prefix(1);
    for (;;) {
      jskip_ws(in);
      if (!in.empty() && in[0] == ']') {
        in.remove_prefix(1);
        return true;
      }
      if (!skip_json_value(in, err)) return false;
      jskip_ws(in);
      if (!in.empty() && in[0] == ',') {
        in.remove_prefix(1);
        continue;
      }
      if (!in.empty() && in[0] == ']') {
        in.remove_prefix(1);
        return true;
      }
      if (err) *err = "CONFIRM_REORDER: skip array ',' or ']'";
      return false;
    }
  }
  if (in.size() >= 4 && in.substr(0, 4) == "true") {
    in.remove_prefix(4);
    return true;
  }
  if (in.size() >= 5 && in.substr(0, 5) == "false") {
    in.remove_prefix(5);
    return true;
  }
  if (in.size() >= 4 && in.substr(0, 4) == "null") {
    in.remove_prefix(4);
    return true;
  }
  std::string tmp;
  if (!jparse_number_token(in, &tmp)) {
    if (err) *err = "CONFIRM_REORDER: skip unknown token";
    return false;
  }
  return true;
}

bool parse_pairs_array(std::string_view& in, std::vector<std::pair<std::string, std::string>>* pairs,
                       std::string* err) {
  pairs->clear();
  if (!jexpect(in, '[')) {
    if (err) *err = "CONFIRM_REORDER: pairs '['";
    return false;
  }
  jskip_ws(in);
  if (!in.empty() && in[0] == ']') {
    in.remove_prefix(1);
    return true;
  }
  for (;;) {
    if (!jexpect(in, '[')) {
      if (err) *err = "CONFIRM_REORDER: pair '['";
      return false;
    }
    std::string a;
    std::string b;
    if (!jparse_cell(in, &a) || !jexpect(in, ',')) {
      if (err) *err = "CONFIRM_REORDER: pair cell";
      return false;
    }
    if (!jparse_cell(in, &b) || !jexpect(in, ']')) {
      if (err) *err = "CONFIRM_REORDER: pair end";
      return false;
    }
    pairs->emplace_back(std::move(a), std::move(b));
    jskip_ws(in);
    if (!in.empty() && in[0] == ',') {
      in.remove_prefix(1);
      continue;
    }
    if (!jexpect(in, ']')) {
      if (err) *err = "CONFIRM_REORDER: pairs ']'";
      return false;
    }
    return true;
  }
}

bool parse_reorder_blob(std::string_view inner_trimmed, std::string* table_out,
                        std::vector<std::pair<std::string, std::string>>* pairs_out, std::string* err) {
  table_out->clear();
  pairs_out->clear();
  std::string_view in = inner_trimmed;
  jskip_ws(in);
  if (!jexpect(in, '{')) {
    if (err) *err = "CONFIRM_REORDER: expected '{'";
    return false;
  }
  bool have_table = false;
  bool have_pairs = false;
  std::string table_v;
  std::vector<std::pair<std::string, std::string>> pairs_v;
  for (;;) {
    jskip_ws(in);
    if (!in.empty() && in[0] == '}') {
      in.remove_prefix(1);
      break;
    }
    std::string key;
    if (!jparse_string(in, &key)) {
      if (err) *err = "CONFIRM_REORDER: key";
      return false;
    }
    if (!jexpect(in, ':')) {
      if (err) *err = "CONFIRM_REORDER: ':'";
      return false;
    }
    if (key == "table") {
      if (!jparse_string(in, &table_v)) {
        if (err) *err = "CONFIRM_REORDER: table value";
        return false;
      }
      have_table = true;
    } else if (key == "pairs") {
      if (!parse_pairs_array(in, &pairs_v, err)) return false;
      have_pairs = true;
    } else {
      if (!skip_json_value(in, err)) return false;
    }
    jskip_ws(in);
    if (!in.empty() && in[0] == ',') {
      in.remove_prefix(1);
      continue;
    }
    jskip_ws(in);
    if (!in.empty() && in[0] == '}') {
      in.remove_prefix(1);
      break;
    }
    if (err) *err = "CONFIRM_REORDER: expected ',' or '}'";
    return false;
  }
  jskip_ws(in);
  if (!in.empty()) {
    if (err) *err = "CONFIRM_REORDER: trailing json";
    return false;
  }
  if (!have_table || !have_pairs) {
    if (err) *err = "CONFIRM_REORDER: need table and pairs";
    return false;
  }
  *table_out = std::move(table_v);
  *pairs_out = std::move(pairs_v);
  return true;
}

static constexpr const char* kTmpPfx = "__structdb_reorder_tmp__";

bool apply_reorder_pairs_in_memory(LogicalTable* t, const std::vector<std::pair<std::string, std::string>>& pairs_in,
                                   std::string* err) {
  std::vector<std::pair<std::string, std::string>> work;
  work.reserve(pairs_in.size());
  std::unordered_set<std::string> seen_old;
  for (const auto& pr : pairs_in) {
    if (pr.first == pr.second) continue;
    if (!seen_old.insert(pr.first).second) {
      if (err) *err = "CONFIRM_REORDER: duplicate old id in pairs";
      return false;
    }
    work.push_back(pr);
  }
  if (work.empty()) {
    if (err) *err = "CONFIRM_REORDER: empty pairs after filtering";
    return false;
  }
  const std::string pkcol = std::string(effective_pk_column_name(*t));
  const int pki = pkcol.empty() ? -1 : schema_col_index(*t, pkcol);
  if (!pkcol.empty() && pki < 0) {
    if (err) *err = "CONFIRM_REORDER: pk column not in schema";
    return false;
  }

  for (std::size_t i = 0; i < work.size(); ++i) {
    const std::string tmp = std::string(kTmpPfx) + std::to_string(i);
    if (t->rows.count(tmp)) {
      if (err) *err = "CONFIRM_REORDER: temp id collision";
      return false;
    }
  }

  LogicalTable backup = clone_table(*t);

  auto rollback_mem = [&]() { *t = std::move(backup); };

  for (std::size_t i = 0; i < work.size(); ++i) {
    const std::string& old_id = work[i].first;
    const std::string tmp = std::string(kTmpPfx) + std::to_string(i);
    auto it = t->rows.find(old_id);
    if (it == t->rows.end()) {
      rollback_mem();
      if (err) *err = "CONFIRM_REORDER: missing row " + old_id;
      return false;
    }
    std::vector<std::string> cells = std::move(it->second);
    t->rows.erase(it);
    if (pki >= 0 && static_cast<std::size_t>(pki) < cells.size()) {
      cells[static_cast<std::size_t>(pki)] = tmp;
    }
    t->rows.emplace(tmp, std::move(cells));
  }

  for (std::size_t i = 0; i < work.size(); ++i) {
    const std::string tmp = std::string(kTmpPfx) + std::to_string(i);
    const std::string& new_id = work[i].second;
    if (t->rows.count(new_id)) {
      rollback_mem();
      if (err) *err = "CONFIRM_REORDER: target id already exists: " + new_id;
      return false;
    }
    auto it = t->rows.find(tmp);
    if (it == t->rows.end()) {
      rollback_mem();
      if (err) *err = "CONFIRM_REORDER: internal lost temp row";
      return false;
    }
    std::vector<std::string> cells = std::move(it->second);
    t->rows.erase(it);
    if (pki >= 0 && static_cast<std::size_t>(pki) < cells.size()) {
      cells[static_cast<std::size_t>(pki)] = new_id;
    }
    t->rows.emplace(new_id, std::move(cells));
  }

  logical_rebuild_str_index(t);
  return true;
}

std::string build_reorder_map_log_line(std::string_view table,
                                       const std::vector<std::pair<std::string, std::string>>& pairs_emit) {
  std::ostringstream o;
  o << "[REORDER_MAP_JSON]{\"table\":" << json_quote_cell(table) << ",\"pairs\":[";
  for (std::size_t i = 0; i < pairs_emit.size(); ++i) {
    if (i) o << ',';
    o << '[' << json_quote_cell(pairs_emit[i].first) << ',' << json_quote_cell(pairs_emit[i].second) << ']';
  }
  o << "]}";
  return o.str();
}

}  // namespace

bool mdb_confirm_reorder_execute(LogicalTable* current, std::string_view paren_inner, std::string* log_line_out,
                                 std::string* err) {
  log_line_out->clear();
  if (!current || current->name.empty()) {
    if (err) *err = "CONFIRM_REORDER: no current table";
    return false;
  }
  if (current->schema.empty()) {
    if (err) *err = "CONFIRM_REORDER: empty schema";
    return false;
  }
  std::string table_json;
  std::vector<std::pair<std::string, std::string>> pairs;
  const std::string inner = trim_copy(paren_inner);
  if (!parse_reorder_blob(inner, &table_json, &pairs, err)) return false;
  if (table_json != current->name) {
    if (err) *err = "CONFIRM_REORDER: json table does not match USE (" + current->name + " vs " + table_json + ")";
    return false;
  }
  std::vector<std::pair<std::string, std::string>> emit_pairs;
  for (const auto& pr : pairs) {
    if (pr.first == pr.second) continue;
    emit_pairs.push_back(pr);
  }
  if (emit_pairs.empty()) {
    if (err) *err = "CONFIRM_REORDER: no non-identity pairs";
    return false;
  }
  if (!apply_reorder_pairs_in_memory(current, pairs, err)) return false;
  *log_line_out = build_reorder_map_log_line(current->name, emit_pairs);
  return true;
}

}  // namespace structdb::client::mdb
