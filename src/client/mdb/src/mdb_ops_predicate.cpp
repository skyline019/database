#include "structdb/client/detail/mdb_ops_detail.hpp"
#include "structdb/client/detail/mdb_runner_internal.hpp"

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <cstdint>
#include <sstream>
#include <string>
#include <vector>

namespace structdb::client::mdb {

namespace {

bool sql_like_percent_under(std::string_view text, std::string_view pat) {
  const int n = static_cast<int>(text.size());
  const int m = static_cast<int>(pat.size());
  std::vector<std::vector<char>> dp(static_cast<std::size_t>(n) + 1,
                                     std::vector<char>(static_cast<std::size_t>(m) + 1, 0));
  dp[0][0] = 1;
  for (int j = 1; j <= m; ++j) {
    if (pat[static_cast<std::size_t>(j - 1)] == '%') dp[0][j] = dp[0][j - 1];
  }
  for (int i = 1; i <= n; ++i) {
    for (int j = 1; j <= m; ++j) {
      const char pc = pat[static_cast<std::size_t>(j - 1)];
      if (pc == '%') {
        dp[static_cast<std::size_t>(i)][static_cast<std::size_t>(j)] =
            static_cast<char>(dp[static_cast<std::size_t>(i)][static_cast<std::size_t>(j - 1)] ||
                              (dp[static_cast<std::size_t>(i - 1)][static_cast<std::size_t>(j)] != 0));
      } else if (pc == '_') {
        dp[static_cast<std::size_t>(i)][static_cast<std::size_t>(j)] =
            dp[static_cast<std::size_t>(i - 1)][static_cast<std::size_t>(j - 1)];
      } else {
        dp[static_cast<std::size_t>(i)][static_cast<std::size_t>(j)] = static_cast<char>(
            (dp[static_cast<std::size_t>(i - 1)][static_cast<std::size_t>(j - 1)] != 0) &&
            text[static_cast<std::size_t>(i - 1)] == pc);
      }
    }
  }
  return dp[static_cast<std::size_t>(n)][static_cast<std::size_t>(m)] != 0;
}

}  // namespace

bool op_is_like(std::string_view op_trimmed) {
  std::string t(trim_copy(op_trimmed));
  for (char& c : t) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  return t == "like";
}

int cmp_datetime_parts(int y1, int mo1, int d1, int h1, int mi1, int s1, int y2, int mo2, int d2, int h2, int mi2,
                       int s2) {
  if (y1 != y2) return y1 < y2 ? -1 : 1;
  if (mo1 != mo2) return mo1 < mo2 ? -1 : 1;
  if (d1 != d2) return d1 < d2 ? -1 : 1;
  if (h1 != h2) return h1 < h2 ? -1 : 1;
  if (mi1 != mi2) return mi1 < mi2 ? -1 : 1;
  if (s1 != s2) return s1 < s2 ? -1 : 1;
  return 0;
}

static bool apply_ordering_cmp(int cmp, std::string_view op) {
  const bool eq = (op == "=" || (op.size() == 1 && op[0] == '='));
  const bool ne = (op == "!=" || ascii_starts_with_ci(op, "<>"));
  const bool lt = (op == "<");
  const bool gt = (op == ">");
  const bool le = (op == "<=");
  const bool ge = (op == ">=");
  if (eq) return cmp == 0;
  if (ne) return cmp != 0;
  if (lt) return cmp < 0;
  if (gt) return cmp > 0;
  if (le) return cmp <= 0;
  if (ge) return cmp >= 0;
  return false;
}

static bool compare_doubles_ordered(double da, double db, std::string_view op) {
  const double mag = (std::max)(std::abs(da), std::abs(db));
  const double eps = 1e-9 * (1.0 + mag);
  int cmp = 0;
  if (da < db - eps) cmp = -1;
  else if (da > db + eps) cmp = 1;
  return apply_ordering_cmp(cmp, op);
}

bool compare_scalar(std::string_view cell, std::string_view op, std::string_view rhs) {
  const bool eq = (op == "=" || (op.size() == 1 && op[0] == '='));
  const bool ne = (op == "!=" || ascii_starts_with_ci(op, "<>"));
  const bool lt = (op == "<");
  const bool gt = (op == ">");
  const bool le = (op == "<=");
  const bool ge = (op == ">=");
  {
    std::string sc(cell);
    std::string sr(rhs);
    double da = 0.0;
    double db = 0.0;
    if (parse_full_double_str(sc, &da) && parse_full_double_str(sr, &db)) {
      const double mag = (std::max)(std::abs(da), std::abs(db));
      const double eps = 1e-9 * (1.0 + mag);
      const auto same = [&](double x, double y) { return std::abs(x - y) <= eps; };
      if (eq) return same(da, db);
      if (ne) return !same(da, db);
      if (lt) return da < db - eps;
      if (gt) return da > db + eps;
      if (le) return da < db + eps;
      if (ge) return da > db - eps;
      return false;
    }
  }
  if (is_int_literal(cell) && is_int_literal(rhs)) {
    const long long a = std::stoll(std::string(cell));
    const long long b = std::stoll(std::string(rhs));
    if (eq) return a == b;
    if (ne) return a != b;
    if (lt) return a < b;
    if (gt) return a > b;
    if (le) return a <= b;
    if (ge) return a >= b;
    return false;
  }
  if (eq) return cell.size() == rhs.size() && detail::ascii_strncasecmp(cell.data(), rhs.data(), cell.size()) == 0;
  if (ne) return !(cell.size() == rhs.size() && detail::ascii_strncasecmp(cell.data(), rhs.data(), cell.size()) == 0);
  if (lt) return cell < rhs;
  if (gt) return cell > rhs;
  if (le) return cell < rhs || (cell.size() == rhs.size() && detail::ascii_strncasecmp(cell.data(), rhs.data(), cell.size()) == 0);
  if (ge) return cell > rhs || (cell.size() == rhs.size() && detail::ascii_strncasecmp(cell.data(), rhs.data(), cell.size()) == 0);
  return false;
}

bool compare_typed_cell(std::string_view cell, std::string_view col_type, std::string_view pred_op,
                        std::string_view pred_val) {
  const std::string op_storage = trim_copy(pred_op);
  const std::string_view op = op_storage;
  if (op_is_like(op_storage)) {
    if (!is_string_type(col_type)) return false;
    return sql_like_percent_under(cell, pred_val);
  }
  if (ascii_starts_with_ci(col_type, "datetime")) {
    int y1 = 0;
    int mo1 = 0;
    int d1 = 0;
    int h1 = 0;
    int mi1 = 0;
    int s1 = 0;
    bool ht1 = false;
    int y2 = 0;
    int mo2 = 0;
    int d2 = 0;
    int h2 = 0;
    int mi2 = 0;
    int s2 = 0;
    bool ht2 = false;
    if (!parse_datetime_calendar(cell, &y1, &mo1, &d1, &h1, &mi1, &s1, &ht1)) return false;
    if (!parse_datetime_calendar(pred_val, &y2, &mo2, &d2, &h2, &mi2, &s2, &ht2)) return false;
    const int cmp = cmp_datetime_parts(y1, mo1, d1, h1, mi1, s1, y2, mo2, d2, h2, mi2, s2);
    return apply_ordering_cmp(cmp, op);
  }
  if (ascii_starts_with_ci(col_type, "timestamp")) {
    if (is_int_literal(cell) && is_int_literal(pred_val)) {
      const long long a = std::stoll(std::string(cell));
      const long long b = std::stoll(std::string(pred_val));
      const int cmp = a < b ? -1 : (a > b ? 1 : 0);
      return apply_ordering_cmp(cmp, op);
    }
    int y1 = 0;
    int mo1 = 0;
    int d1 = 0;
    int h1 = 0;
    int mi1 = 0;
    int s1 = 0;
    bool ht1 = false;
    int y2 = 0;
    int mo2 = 0;
    int d2 = 0;
    int h2 = 0;
    int mi2 = 0;
    int s2 = 0;
    bool ht2 = false;
    if (parse_datetime_calendar(cell, &y1, &mo1, &d1, &h1, &mi1, &s1, &ht1) &&
        parse_datetime_calendar(pred_val, &y2, &mo2, &d2, &h2, &mi2, &s2, &ht2)) {
      const int cmp = cmp_datetime_parts(y1, mo1, d1, h1, mi1, s1, y2, mo2, d2, h2, mi2, s2);
      return apply_ordering_cmp(cmp, op);
    }
    return false;
  }
  if (ascii_starts_with_ci(col_type, "int")) {
    if (!is_int_literal(cell) || !is_int_literal(pred_val)) return false;
    const long long a = std::stoll(std::string(cell));
    const long long b = std::stoll(std::string(pred_val));
    const int cmp = a < b ? -1 : (a > b ? 1 : 0);
    return apply_ordering_cmp(cmp, op);
  }
  if (ascii_starts_with_ci(col_type, "float") || ascii_starts_with_ci(col_type, "double")) {
    std::string sc(cell);
    std::string sr(pred_val);
    double da = 0.0;
    double db = 0.0;
    if (!parse_full_double_str(sc, &da) || !parse_full_double_str(sr, &db)) return false;
    return compare_doubles_ordered(da, db, op);
  }
  return compare_scalar(cell, op, pred_val);
}

bool row_matches_predicate(const LogicalTable& t, const std::string& row_id, const std::vector<std::string>& cells,
                           std::string_view pred_col, std::string_view pred_op, std::string_view pred_val) {
  if (col_name_eq(pred_col, "id")) {
    const std::string opb = trim_copy(pred_op);
    if (op_is_like(opb)) return false;
    if (is_int_literal(row_id) && is_int_literal(pred_val)) {
      const long long a = std::stoll(std::string(row_id));
      const long long b = std::stoll(std::string(pred_val));
      const int cmp = a < b ? -1 : (a > b ? 1 : 0);
      return apply_ordering_cmp(cmp, opb);
    }
    return compare_scalar(row_id, opb, pred_val);
  }
  const int ci = schema_col_index(t, pred_col);
  if (ci < 0) return false;
  return compare_typed_cell(cells[static_cast<std::size_t>(ci)], t.schema[static_cast<std::size_t>(ci)].second,
                            pred_op, pred_val);
}

std::vector<std::string> collect_matching_row_ids(const LogicalTable& t, std::string_view pred_col,
                                                   std::string_view pred_op, std::string_view pred_val) {
  std::vector<std::string> out;
  const int idx = schema_col_index(t, pred_col);
  const std::string op_trim = trim_copy(pred_op);
  if (op_trim == "=" && idx >= 0 && is_string_type(t.schema[static_cast<std::size_t>(idx)].second)) {
    auto it = t.str_idx.find(std::string(pred_col));
    if (it != t.str_idx.end()) {
      const auto er = it->second.equal_range(std::string(pred_val));
      for (auto j = er.first; j != er.second; ++j) {
        const auto r = t.rows.find(j->second);
        if (r != t.rows.end()) out.push_back(r->first);
      }
      return out;
    }
  }
  for (const auto& kv : t.rows) {
    if (row_matches_predicate(t, kv.first, kv.second, pred_col, pred_op, pred_val)) out.push_back(kv.first);
  }
  return out;
}

}  // namespace structdb::client::mdb
