#include "structdb/client/detail/mdb_ops_detail.hpp"
#include "structdb/client/detail/mdb_runner_internal.hpp"
#include "structdb/client/embed_client.hpp"
#include "structdb/facade/engine.hpp"
#include "structdb/storage/mdb_keyspace.hpp"
#include "structdb/client/mdb_runner.hpp"

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <limits>
#include <map>
#include <sstream>
#include <string>
#include <vector>

namespace structdb::client::mdb {

namespace {

bool addattr_csv_token_all_digits(std::string_view s) {
  if (s.empty()) return false;
  for (unsigned char c : s) {
    if (c < '0' || c > '9') return false;
  }
  return true;
}

}  // namespace

void log_line(std::vector<std::string>* sink, const std::string& s) {
  if (sink) sink->push_back(s);
}

std::vector<std::string> split_csv_paren_content(std::string_view inner) {
  std::vector<std::string> out;
  std::size_t i = 0;
  while (i < inner.size()) {
    while (i < inner.size() && (inner[i] == ' ' || inner[i] == '\t')) ++i;
    if (i >= inner.size()) break;
    std::size_t j = i;
    while (j < inner.size() && inner[j] != ',') ++j;
    out.emplace_back(trim_copy(inner.substr(i, j - i)));
    i = j;
    if (i < inner.size() && inner[i] == ',') ++i;
  }
  return out;
}

bool extract_paren_block(std::string_view line, std::size_t open_pos, std::string_view* inner, std::string* err) {
  if (open_pos >= line.size() || line[open_pos] != '(') {
    if (err) *err = "expected '('";
    return false;
  }
  const auto close = line.find(')', open_pos + 1);
  if (close == std::string::npos) {
    if (err) *err = "expected ')'";
    return false;
  }
  *inner = line.substr(open_pos + 1, close - open_pos - 1);
  return true;
}

bool parse_defattrs(std::string_view inner, std::vector<std::pair<std::string, std::string>>* attrs,
                    std::string* err) {
  attrs->clear();
  auto parts = split_csv_paren_content(inner);
  for (const auto& p : parts) {
    const auto colon = p.find(':');
    if (colon == std::string::npos) {
      if (err) *err = "DEFATTR: bad pair";
      return false;
    }
    std::string col = trim_copy(std::string_view(p).substr(0, colon));
    std::string typ = trim_copy(std::string_view(p).substr(colon + 1));
    trim_inplace(col);
    trim_inplace(typ);
    if (!known_logical_type(typ)) {
      if (err) *err = "DEFATTR: unknown type '" + typ + "'";
      return false;
    }
    for (const auto& e : *attrs) {
      if (col.size() == e.first.size() && detail::ascii_strncasecmp(col.data(), e.first.data(), col.size()) == 0) {
        if (err) *err = "DEFATTR: duplicate column";
        return false;
      }
    }
    attrs->emplace_back(std::move(col), std::move(typ));
  }
  return true;
}

bool mdb_addattr_paren_inner_to_txn_payload(std::string_view inner, std::string* out, std::string* err) {
  if (!out) {
    if (err) *err = "ADDATTR: internal";
    return false;
  }
  const auto parts = split_csv_paren_content(inner);
  if (parts.empty()) {
    if (err) *err = "ADDATTR: empty";
    return false;
  }
  if (parts.size() > 2) {
    if (err) *err = "ADDATTR: expected name:type or index,name:type";
    return false;
  }
  if (parts.size() == 2 && addattr_csv_token_all_digits(parts[0]) && parts[1].find(':') != std::string::npos) {
    *out = parts[0];
    out->push_back('\t');
    out->append(parts[1]);
    return true;
  }
  if (parts.size() == 1) {
    *out = parts[0];
    return true;
  }
  if (err) *err = "ADDATTR: expected name:type or index,name:type";
  return false;
}

bool mdb_addattr_txn_payload_to_apply_inner(std::string_view payload, std::string* inner_out, std::string* err) {
  if (!inner_out) {
    if (err) *err = "ADDATTR: internal";
    return false;
  }
  const std::string p = trim_copy(payload);
  const auto tab = p.find('\t');
  if (tab == std::string::npos) {
    *inner_out = p;
    return true;
  }
  const std::string left = trim_copy(std::string_view(p).substr(0, tab));
  const std::string right = trim_copy(std::string_view(p).substr(tab + 1));
  if (!addattr_csv_token_all_digits(left) || right.find(':') == std::string::npos) {
    if (err) *err = "ADDATTR txn: bad payload";
    return false;
  }
  *inner_out = left + "," + right;
  return true;
}

bool mdb_apply_addattr_inner(LogicalTable* current, std::string_view inner, std::string* err) {
  if (!current) {
    if (err) *err = "ADDATTR: internal";
    return false;
  }
  if (current->name.empty()) {
    if (err) *err = "ADDATTR: no table";
    return false;
  }
  if (current->schema.empty()) {
    if (err) *err = "ADDATTR: schema empty (DEFATTR first)";
    return false;
  }
  const auto parts = split_csv_paren_content(inner);
  if (parts.empty()) {
    if (err) *err = "ADDATTR: empty";
    return false;
  }
  if (parts.size() > 2) {
    if (err) *err = "ADDATTR: expected name:type or index,name:type";
    return false;
  }
  bool has_insert_index = false;
  std::size_t insert_index = 0;
  std::string pair_inner;
  if (parts.size() == 2 && addattr_csv_token_all_digits(parts[0]) && parts[1].find(':') != std::string::npos) {
    has_insert_index = true;
    insert_index = static_cast<std::size_t>(std::stoull(parts[0]));
    pair_inner = parts[1];
  } else if (parts.size() == 1) {
    pair_inner = parts[0];
  } else if (parts.size() == 2) {
    pair_inner = trim_copy(inner);
  } else {
    if (err) *err = "ADDATTR: expected name:type or index,name:type";
    return false;
  }
  std::vector<std::pair<std::string, std::string>> attrs;
  if (!parse_defattrs(pair_inner, &attrs, err)) return false;
  if (attrs.size() != 1) {
    if (err) *err = "ADDATTR: expected exactly one name:type";
    return false;
  }
  const std::string& col = attrs[0].first;
  const std::string& typ = attrs[0].second;
  for (const auto& s : current->schema) {
    if (col_name_eq(s.first, col)) {
      if (err) *err = "ADDATTR: duplicate column";
      return false;
    }
  }
  const std::string defv = default_cell_for_new_column(typ);
  if (!type_matches(typ, defv)) {
    if (err) *err = "ADDATTR: internal default cell";
    return false;
  }
  std::size_t insert_pos = current->schema.size();
  if (has_insert_index) {
    if (insert_index > current->schema.size()) {
      if (err) *err = "ADDATTR: index out of range";
      return false;
    }
    insert_pos = insert_index;
  }
  current->schema.insert(current->schema.begin() + static_cast<std::ptrdiff_t>(insert_pos), {col, typ});
  for (auto& kv : current->rows) {
    kv.second.insert(kv.second.begin() + static_cast<std::ptrdiff_t>(insert_pos), defv);
  }
  return true;
}

bool import_mdb_directory(const MdbEnginePorts& ports, std::string_view dir_path_sv,
                          std::vector<std::string>* log_accum, std::string* err) {
  namespace fs = std::filesystem;
  const fs::path dir(trim_copy(dir_path_sv));
  std::error_code ec;
  if (!fs::exists(dir, ec) || !fs::is_directory(dir, ec)) {
    if (err) *err = "IMPORTDIR: not a directory";
    return false;
  }
  for (const auto& ent : fs::directory_iterator(dir, ec)) {
    if (ec) break;
    if (!ent.is_regular_file()) continue;
    if (ent.path().extension() != ".mdb") continue;
    MdbRunOptions opt;
    opt.script_path = ent.path();
    opt.log_sink = log_accum;
    const MdbRunResult r = run_mdb_script(*ports.engine, *ports.client, opt);
    if (!r.ok) {
      if (err) *err = r.last_error;
      return false;
    }
  }
  return true;
}

void append_embed_journal_tail(const EmbedClient& client, std::size_t max_lines, std::vector<std::string>* sink) {
  const auto path = client.embed_journal_path();
  if (path.empty()) {
    log_line(sink, "[SHOWLOG] no journal path (session not opened?)");
    return;
  }
  std::ifstream in(path, std::ios::binary | std::ios::ate);
  if (!in) {
    log_line(sink, std::string("[SHOWLOG] cannot open ") + path.string());
    return;
  }
  const auto sz = in.tellg();
  if (sz <= 0) {
    log_line(sink, "[SHOWLOG] (empty)");
    return;
  }
  std::streamoff start = 0;
  constexpr std::streamoff kTail = 65536;
  if (sz > kTail) start = static_cast<std::streamoff>(sz) - kTail;
  in.seekg(start, std::ios::beg);
  std::string chunk(static_cast<std::size_t>(static_cast<std::streamoff>(sz) - start), '\0');
  in.read(chunk.data(), static_cast<std::streamsize>(chunk.size()));
  std::vector<std::string> lines;
  std::string cur;
  for (char c : chunk) {
    if (c == '\n') {
      lines.push_back(std::move(cur));
      cur.clear();
    } else if (c != '\r')
      cur.push_back(c);
  }
  if (!cur.empty()) lines.push_back(std::move(cur));
  const std::size_t n = lines.size();
  const std::size_t from = n > max_lines ? n - max_lines : 0;
  log_line(sink, std::string("[SHOWLOG] ") + path.string() + " (last " + std::to_string(n - from) + " lines)");
  for (std::size_t i = from; i < n; ++i) log_line(sink, "  " + lines[i]);
}

}  // namespace structdb::client::mdb
