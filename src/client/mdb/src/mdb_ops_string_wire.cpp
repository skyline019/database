#include "structdb/client/detail/mdb_ops_detail.hpp"
#include "structdb/client/detail/mdb_runner_internal.hpp"
#include "structdb/client/mdb_runner.hpp"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <limits>
#include <map>
#include <sstream>
#include <vector>

namespace structdb::client::mdb {

std::string snapshot_key_for(std::string_view table) {
  return std::string(kSnapPrefix) + std::string(table);
}

bool ascii_starts_with_ci(std::string_view s, std::string_view pfx) {
  if (s.size() < pfx.size()) return false;
  return detail::ascii_strncasecmp(s.data(), pfx.data(), pfx.size()) == 0;
}

void trim_inplace(std::string& s) {
  while (!s.empty() && (static_cast<unsigned char>(s.front()) <= ' ')) s.erase(s.begin());
  while (!s.empty() && (static_cast<unsigned char>(s.back()) <= ' ')) s.pop_back();
}

std::string trim_copy(std::string_view sv) {
  std::size_t i = 0;
  while (i < sv.size() && (static_cast<unsigned char>(sv[i]) <= ' ')) ++i;
  std::size_t j = sv.size();
  while (j > i && (static_cast<unsigned char>(sv[j - 1]) <= ' ')) --j;
  return std::string(sv.substr(i, j - i));
}

bool is_int_literal(std::string_view s) {
  if (s.empty()) return false;
  std::size_t i = 0;
  if (s[0] == '+' || s[0] == '-') {
    if (s.size() == 1) return false;
    ++i;
  }
  for (; i < s.size(); ++i) {
    if (s[i] < '0' || s[i] > '9') return false;
  }
  return true;
}

bool field_type_is_char(std::string_view typ) {
  if (!ascii_starts_with_ci(typ, "char")) return false;
  if (typ.size() == 4) return true;
  return typ.size() > 4 && typ[4] == '(';
}

bool known_logical_type(std::string_view typ) {
  if (typ.empty()) return false;
  if (ascii_starts_with_ci(typ, "int")) return true;
  if (ascii_starts_with_ci(typ, "string")) return true;
  if (field_type_is_char(typ)) return true;
  if (ascii_starts_with_ci(typ, "varchar")) return true;
  if (ascii_starts_with_ci(typ, "text")) return true;
  if (ascii_starts_with_ci(typ, "float")) return true;
  if (ascii_starts_with_ci(typ, "double")) return true;
  if (ascii_starts_with_ci(typ, "datetime")) return true;
  if (ascii_starts_with_ci(typ, "timestamp")) return true;
  return false;
}

bool parse_full_double_str(const std::string& t, double* out) {
  char* end = nullptr;
  const double v = std::strtod(t.c_str(), &end);
  if (end == t.c_str()) return false;
  while (*end == ' ' || *end == '\t') ++end;
  if (*end != '\0') return false;
  if (!std::isfinite(v)) return false;
  *out = v;
  return true;
}

bool is_double_literal(std::string_view sv) {
  std::string t(sv);
  double d{};
  return parse_full_double_str(t, &d);
}

bool is_float_literal(std::string_view sv) {
  std::string t(sv);
  if (!t.empty() && (t.back() == 'f' || t.back() == 'F')) t.pop_back();
  trim_inplace(t);
  char* end = nullptr;
  const float v = std::strtof(t.c_str(), &end);
  if (end == t.c_str()) return false;
  while (*end == ' ' || *end == '\t') ++end;
  if (*end != '\0') return false;
  return std::isfinite(v);
}

static bool is_leap_year_int(int y) { return (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0); }

static int days_in_month_int(int y, int m) {
  static const int kDays[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  if (m < 1 || m > 12) return 0;
  if (m == 2) return is_leap_year_int(y) ? 29 : 28;
  return kDays[m - 1];
}

bool parse_datetime_calendar(std::string_view s, int* y, int* mo, int* d, int* hh, int* mi, int* ss, bool* has_time) {
  auto dig = [](char c) { return c >= '0' && c <= '9'; };
  if (s.size() < 10) return false;
  for (std::size_t i = 0; i < 10; ++i) {
    if (i == 4 || i == 7) {
      if (s[i] != '-') return false;
    } else if (!dig(s[i]))
      return false;
  }
  const int yy = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
  const int mm = (s[5] - '0') * 10 + (s[6] - '0');
  const int dd = (s[8] - '0') * 10 + (s[9] - '0');
  if (mm < 1 || mm > 12) return false;
  const int dim = days_in_month_int(yy, mm);
  if (dim == 0 || dd < 1 || dd > dim) return false;
  int h = 0;
  int m = 0;
  int sec = 0;
  bool ht = false;
  if (s.size() == 10) {
    *y = yy;
    *mo = mm;
    *d = dd;
    *hh = h;
    *mi = m;
    *ss = sec;
    *has_time = ht;
    return true;
  }
  if (s.size() != 19) return false;
  if (s[10] != ' ' && s[10] != 'T') return false;
  for (std::size_t i = 11; i < 19; ++i) {
    if (i == 13 || i == 16) {
      if (s[i] != ':') return false;
    } else if (!dig(s[i]))
      return false;
  }
  h = (s[11] - '0') * 10 + (s[12] - '0');
  m = (s[14] - '0') * 10 + (s[15] - '0');
  sec = (s[17] - '0') * 10 + (s[18] - '0');
  if (h > 23 || m > 59 || sec > 59) return false;
  ht = true;
  *y = yy;
  *mo = mm;
  *d = dd;
  *hh = h;
  *mi = m;
  *ss = sec;
  *has_time = ht;
  return true;
}

bool is_datetime_literal(std::string_view s) {
  int y = 0;
  int mo = 0;
  int d = 0;
  int hh = 0;
  int mi = 0;
  int ss = 0;
  bool ht = false;
  return parse_datetime_calendar(s, &y, &mo, &d, &hh, &mi, &ss, &ht);
}

bool type_matches(std::string_view type, std::string_view value) {
  if (ascii_starts_with_ci(type, "int")) return is_int_literal(value);
  if (ascii_starts_with_ci(type, "string") || ascii_starts_with_ci(type, "varchar") ||
      ascii_starts_with_ci(type, "text"))
    return true;
  if (field_type_is_char(type)) return value.empty() || value.size() == 1;
  if (ascii_starts_with_ci(type, "float")) return is_float_literal(value);
  if (ascii_starts_with_ci(type, "double")) return is_double_literal(value);
  if (ascii_starts_with_ci(type, "datetime")) return is_datetime_literal(value);
  if (ascii_starts_with_ci(type, "timestamp")) return is_int_literal(value) || is_datetime_literal(value);
  return false;
}

std::string default_cell_for_new_column(std::string_view typ) {
  if (ascii_starts_with_ci(typ, "string") || ascii_starts_with_ci(typ, "varchar") || ascii_starts_with_ci(typ, "text"))
    return "0";
  if (field_type_is_char(typ)) return "";
  if (ascii_starts_with_ci(typ, "int")) return "0";
  if (ascii_starts_with_ci(typ, "float")) return "0";
  if (ascii_starts_with_ci(typ, "double")) return "0";
  if (ascii_starts_with_ci(typ, "datetime")) return "2000-01-01";
  if (ascii_starts_with_ci(typ, "timestamp")) return "0";
  return "0";
}

std::string type_mismatch_msg(std::string_view col, std::string_view type, std::string_view got) {
  std::ostringstream o;
  o << "[UPDATE] attribute '" << col << "' expects " << type << ", got '" << got << "'";
  return o.str();
}

std::string hex_encode(std::string_view in) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string out;
  out.reserve(in.size() * 2);
  for (unsigned char c : in) {
    out.push_back(kHex[c >> 4]);
    out.push_back(kHex[c & 15]);
  }
  return out;
}

static int from_hex(char c) {
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'a' && c <= 'f') return c - 'a' + 10;
  if (c >= 'A' && c <= 'F') return c - 'A' + 10;
  return -1;
}

bool hex_decode(std::string_view in, std::string* out) {
  if (!out) return false;
  out->clear();
  if (in.size() % 2 != 0) return false;
  out->reserve(in.size() / 2);
  for (std::size_t i = 0; i < in.size(); i += 2) {
    const int hi = from_hex(in[i]);
    const int lo = from_hex(in[i + 1]);
    if (hi < 0 || lo < 0) return false;
    out->push_back(static_cast<char>((hi << 4) | lo));
  }
  return true;
}

std::string wire_encode_snapshot_blob(std::string_view raw) {
  constexpr char kPfx[] = "mdbhex1:";
  return std::string(kPfx) + hex_encode(raw);
}

bool wire_decode_snapshot_blob(std::string_view stored, std::string* raw_out, std::string* err) {
  if (!raw_out) return false;
  raw_out->clear();
  if (stored.empty()) {
    if (err) *err = "empty snapshot value";
    return false;
  }
  constexpr char kPfx[] = "mdbhex1:";
  constexpr std::size_t kPfxLen = sizeof(kPfx) - 1;
  if (stored.size() > kPfxLen &&
      detail::ascii_strncasecmp(stored.data(), kPfx, static_cast<int>(kPfxLen)) == 0) {
    if (!hex_decode(stored.substr(kPfxLen), raw_out)) {
      if (err) *err = "hex decode failed";
      return false;
    }
    return true;
  }
  *raw_out = std::string(stored);
  return true;
}

std::vector<std::string_view> split_pipe_rows(std::string_view inner) {
  std::vector<std::string_view> out;
  std::size_t i = 0;
  while (i < inner.size()) {
    while (i < inner.size() && (inner[i] == ' ' || inner[i] == '\t')) ++i;
    if (i >= inner.size()) break;
    const std::size_t j = inner.find('|', i);
    if (j == std::string_view::npos) {
      std::string_view chunk = inner.substr(i);
      while (!chunk.empty() && (chunk.back() == ' ' || chunk.back() == '\t')) chunk.remove_suffix(1);
      if (!chunk.empty()) out.push_back(chunk);
      break;
    }
    std::string_view chunk = inner.substr(i, j - i);
    while (!chunk.empty() && (chunk.back() == ' ' || chunk.back() == '\t')) chunk.remove_suffix(1);
    if (!chunk.empty()) out.push_back(chunk);
    i = j + 1;
  }
  return out;
}

std::string json_quote_cell(std::string_view s) {
  std::string o;
  o.push_back('"');
  for (unsigned char uc : s) {
    const char c = static_cast<char>(uc);
    if (c == '"' || c == '\\') o.push_back('\\');
    if (c == '\n' || c == '\r' || c == '\t') {
      o.push_back(' ');
      continue;
    }
    o.push_back(c);
  }
  o.push_back('"');
  return o;
}

bool parse_export_json_path(const std::string& tail, std::filesystem::path* path_out, std::string* err) {
  if (!path_out) return false;
  std::size_t i = 0;
  while (i < tail.size() && std::isspace(static_cast<unsigned char>(tail[i]))) ++i;
  if (i >= tail.size()) {
    if (err) *err = "EXPORT: missing path";
    return false;
  }
  if (ascii_starts_with_ci(std::string_view(tail).substr(i), "json")) {
    i += 4;
    while (i < tail.size() && std::isspace(static_cast<unsigned char>(tail[i]))) ++i;
    if (i >= tail.size()) {
      if (err) *err = "EXPORT: missing path after JSON";
      return false;
    }
    *path_out = std::filesystem::path(trim_copy(std::string_view(tail).substr(i)));
    return true;
  }
  *path_out = std::filesystem::path(trim_copy(std::string_view(tail).substr(i)));
  return true;
}

std::string_view effective_pk_column_name(const LogicalTable& t) {
  if (!t.pk_column.empty()) return t.pk_column;
  if (!t.schema.empty()) return t.schema[0].first;
  return {};
}

std::string serialize_table(const LogicalTable& t) {
  std::ostringstream o;
  o << "v1\n";
  o << "NAM\t" << t.name << "\n";
  for (std::size_t i = 0; i < t.schema.size(); ++i) {
    if (i) o << ',';
    o << t.schema[i].first << ':' << t.schema[i].second;
  }
  o << '\n';
  if (!t.pk_column.empty()) {
    o << "PKCOL\t" << t.pk_column << "\n";
  }
  for (const auto& kv : t.rows) {
    o << kv.first;
    for (const auto& cell : kv.second) {
      o << '\t' << cell;
    }
    o << '\n';
  }
  return o.str();
}

bool deserialize_table(std::string_view blob, LogicalTable* out, std::string* err) {
  out->clear_data_keep_name();
  std::vector<std::string> lines;
  std::string cur;
  for (char c : blob) {
    if (c == '\n') {
      lines.push_back(std::move(cur));
      cur.clear();
    } else if (c != '\r') {
      cur.push_back(c);
    }
  }
  if (!cur.empty()) lines.push_back(std::move(cur));
  if (lines.empty()) {
    if (err) *err = "snapshot: empty";
    return false;
  }
  if (lines[0] != "v1") {
    if (err) *err = "snapshot: bad header";
    return false;
  }
  if (lines.size() < 2) {
    if (err) *err = "snapshot: short";
    return false;
  }
  std::size_t schema_line_index = 1;
  if (lines[1].rfind("NAM\t", 0) == 0) {
    out->name = trim_copy(std::string_view(lines[1]).substr(4));
    schema_line_index = 2;
  }
  if (lines.size() <= schema_line_index) {
    if (err) *err = "snapshot: short";
    return false;
  }
  const std::string& schema_line = lines[schema_line_index];
  if (!schema_line.empty()) {
    std::size_t start = 0;
    while (start <= schema_line.size()) {
      const auto comma = schema_line.find(',', start);
      const std::size_t end = (comma == std::string::npos) ? schema_line.size() : comma;
      std::string tok = schema_line.substr(start, end - start);
      trim_inplace(tok);
      const auto colon = tok.find(':');
      if (colon == std::string::npos) {
        if (err) *err = "snapshot: bad schema token";
        return false;
      }
      std::string col = tok.substr(0, colon);
      std::string typ = tok.substr(colon + 1);
      trim_inplace(col);
      trim_inplace(typ);
      out->schema.emplace_back(std::move(col), std::move(typ));
      if (comma == std::string::npos) break;
      start = comma + 1;
    }
  }
  std::size_t li = schema_line_index + 1;
  out->pk_column.clear();
  while (li < lines.size()) {
    std::string meta = lines[li];
    trim_inplace(meta);
    if (meta.empty()) {
      ++li;
      continue;
    }
    if (meta.rfind("PKCOL\t", 0) == 0) {
      out->pk_column = trim_copy(std::string_view(meta).substr(6));
      ++li;
      continue;
    }
    break;
  }
  for (; li < lines.size(); ++li) {
    std::string ln = lines[li];
    trim_inplace(ln);
    if (ln.empty()) continue;
    std::vector<std::string> cells;
    std::size_t pos = 0;
    while (pos <= ln.size()) {
      const auto tab = ln.find('\t', pos);
      const std::size_t end = (tab == std::string::npos) ? ln.size() : tab;
      cells.emplace_back(ln.substr(pos, end - pos));
      if (tab == std::string::npos) break;
      pos = tab + 1;
    }
    if (cells.empty()) continue;
    const std::string id = cells[0];
    std::vector<std::string> vals(cells.begin() + 1, cells.end());
    if (vals.size() != out->schema.size()) {
      if (err) *err = "snapshot: row width";
      return false;
    }
    out->rows[id] = std::move(vals);
  }
  return true;
}

}  // namespace structdb::client::mdb
