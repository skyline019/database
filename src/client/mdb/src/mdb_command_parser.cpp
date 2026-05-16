#include "structdb/client/mdb_command_parser.hpp"

#include <cctype>
#include <string>

#if defined(_MSC_VER)
#  include <cstring>
#else
#  include <strings.h>
#endif

namespace structdb::client::mdb {

namespace {

#if defined(_MSC_VER)
int ascii_strncasecmp(const char* a, const char* b, std::size_t n) {
  return _strnicmp(a, b, static_cast<int>(n));
}
#else
int ascii_strncasecmp(const char* a, const char* b, std::size_t n) {
  if (n == 0) return 0;
  return ::strncasecmp(a, b, n);
}
#endif

std::string trim_copy_sv(std::string_view sv) {
  std::size_t i = 0;
  while (i < sv.size() && (static_cast<unsigned char>(sv[i]) <= ' ')) ++i;
  std::size_t j = sv.size();
  while (j > i && (static_cast<unsigned char>(sv[j - 1]) <= ' ')) --j;
  return std::string(sv.substr(i, j - i));
}

const char* skip_ws_cstr(const char* p) {
  while (*p == ' ' || *p == '\t') ++p;
  return p;
}

bool command_has_prefix_token(const char* s, const char* pfx, std::size_t len) {
  if (ascii_strncasecmp(s, pfx, len) != 0) return false;
  if (len > 0 && pfx[len - 1] == '(') return true;
  const char c = s[len];
  return c == '\0' || c == ' ' || c == '\t' || c == '(';
}

bool extract_open_paren(std::string_view line, std::size_t after, MdbParsedLine* out) {
  std::size_t i = after;
  while (i < line.size() && (line[i] == ' ' || line[i] == '\t')) ++i;
  if (i >= line.size() || line[i] != '(') return false;
  const auto close = line.find(')', i + 1);
  if (close == std::string_view::npos) return false;
  out->has_paren = true;
  out->open_paren = i;
  out->paren_inner = line.substr(i + 1, close - i - 1);
  return true;
}

}  // namespace

bool mdb_parse_command_line(std::string_view line_trimmed, MdbParsedLine* out, std::string* err) {
  if (!out) {
    if (err) *err = "null out";
    return false;
  }
  *out = MdbParsedLine{};
  std::string low;
  low.reserve(line_trimmed.size());
  for (unsigned char c : line_trimmed) low.push_back(static_cast<char>(std::tolower(c)));

  if (low.rfind("rollback to savepoint ", 0) == 0) {
    out->verb = MdbVerb::RollbackToSavepoint;
    out->tail = trim_copy_sv(line_trimmed.substr(22));
    return true;
  }
  if (low.rfind("release savepoint ", 0) == 0) {
    out->verb = MdbVerb::ReleaseSavepoint;
    out->tail = trim_copy_sv(line_trimmed.substr(18));
    if (out->tail.empty()) {
      if (err) *err = "RELEASE SAVEPOINT: need name";
      return false;
    }
    return true;
  }
  if (low == "exit" || low == "quit") {
    out->verb = MdbVerb::Exit;
    return true;
  }
  if (low == "showlog") {
    out->verb = MdbVerb::ShowLog;
    return true;
  }
  if (low == "show attr" || low == "describe") {
    out->verb = MdbVerb::ShowAttr;
    return true;
  }
  if (low == "show key" || low == "show primary key") {
    out->verb = MdbVerb::ShowKey;
    return true;
  }
  if (low == "vacuum") {
    out->verb = MdbVerb::Vacuum;
    return true;
  }
  if (low == "scan" || low.rfind("scan ", 0) == 0) {
    out->verb = MdbVerb::Scan;
    if (low == "scan") return true;
    if (low == "scan reset") {
      out->tail = "RESET";
      return true;
    }
    if (low.rfind("scan more", 0) != 0) {
      if (err) *err = "SCAN: use SCAN, SCAN RESET, or SCAN MORE [ (batch) ]";
      return false;
    }
    out->tail = "MORE";
    const std::size_t pos = line_trimmed.find('(');
    if (pos != std::string_view::npos) {
      if (!extract_open_paren(line_trimmed, pos, out)) {
        if (err) *err = "SCAN MORE: bad (...)";
        return false;
      }
    }
    return true;
  }
  if (low.rfind("show tuning json", 0) == 0 || low.rfind("show status json", 0) == 0) {
    out->verb = MdbVerb::ShowTuningJson;
    return true;
  }
  if (low == "show tuning" || low == "show status") {
    out->verb = MdbVerb::ShowTuning;
    return true;
  }
  if (low.rfind("show storage json", 0) == 0) {
    out->verb = MdbVerb::ShowStorageJson;
    return true;
  }
  if (low == "show storage") {
    out->verb = MdbVerb::ShowStorage;
    return true;
  }
  if (low.rfind("list tables", 0) == 0) {
    out->verb = MdbVerb::ListTables;
    return true;
  }
  if (low.rfind("show tables", 0) == 0) {
    out->verb = MdbVerb::ShowTables;
    return true;
  }
  if (low == "show txn" || low == "show transaction") {
    out->verb = MdbVerb::ShowTxn;
    return true;
  }
  if (low == "show snapshot") {
    out->verb = MdbVerb::ShowSnapshot;
    return true;
  }
  if (low.rfind("txnisolation", 0) == 0) {
    out->verb = MdbVerb::TxnIsolation;
    out->tail = trim_copy_sv(line_trimmed.substr(12));
    return true;
  }
  if (low.rfind("export", 0) == 0) {
    out->verb = MdbVerb::Export;
    out->tail = trim_copy_sv(line_trimmed.substr(6));
    return true;
  }
  if (low.rfind("importdir", 0) == 0) {
    out->verb = MdbVerb::ImportDir;
    constexpr std::size_t kImportDirLen = 9;
    if (line_trimmed.size() < kImportDirLen) {
      if (err) *err = "IMPORTDIR: malformed";
      return false;
    }
    std::size_t i = kImportDirLen;
    while (i < line_trimmed.size() && std::isspace(static_cast<unsigned char>(line_trimmed[i]))) ++i;
    if (!extract_open_paren(line_trimmed, i, out)) {
      if (err) *err = "IMPORTDIR: expected (path)";
      return false;
    }
    return true;
  }
  if (low == "help" || low == "?") {
    out->verb = MdbVerb::Help;
    return true;
  }
  if (low.rfind("create schema", 0) == 0) {
    out->verb = MdbVerb::NotPortable;
    out->tail = trim_copy_sv(line_trimmed);
    return true;
  }
  if (low.rfind("create", 0) == 0) {
    std::size_t i = 0;
    while (i < line_trimmed.size() && !std::isspace(static_cast<unsigned char>(line_trimmed[i]))) ++i;
    while (i < line_trimmed.size() && std::isspace(static_cast<unsigned char>(line_trimmed[i]))) ++i;
    if (line_trimmed.size() - i < 5 || ascii_strncasecmp(line_trimmed.data() + i, "TABLE", 5) != 0) {
      if (err) *err = "CREATE: expected TABLE";
      return false;
    }
    const char c = (line_trimmed.size() - i > 5) ? static_cast<char>(line_trimmed[i + 5]) : '\0';
    if (!(c == '\0' || c == ' ' || c == '\t' || c == '(')) {
      if (err) *err = "CREATE: bad token after TABLE";
      return false;
    }
    i += 5;
    while (i < line_trimmed.size() && std::isspace(static_cast<unsigned char>(line_trimmed[i]))) ++i;
    if (!extract_open_paren(line_trimmed, i, out)) {
      if (err) *err = "CREATE: expected (name)";
      return false;
    }
    out->verb = MdbVerb::CreateTable;
    return true;
  }

  if (low.rfind("drop schema", 0) == 0) {
    out->verb = MdbVerb::NotPortable;
    out->tail = trim_copy_sv(line_trimmed);
    return true;
  }
  if (low.rfind("drop table", 0) == 0) {
    std::size_t i = 0;
    while (i < line_trimmed.size() && !std::isspace(static_cast<unsigned char>(line_trimmed[i]))) ++i;
    while (i < line_trimmed.size() && std::isspace(static_cast<unsigned char>(line_trimmed[i]))) ++i;
    if (line_trimmed.size() - i < 5 || ascii_strncasecmp(line_trimmed.data() + i, "TABLE", 5) != 0) {
      if (err) *err = "DROP: expected TABLE";
      return false;
    }
    i += 5;
    while (i < line_trimmed.size() && std::isspace(static_cast<unsigned char>(line_trimmed[i]))) ++i;
    if (!extract_open_paren(line_trimmed, i, out)) {
      if (err) *err = "DROP: expected (name)";
      return false;
    }
    out->verb = MdbVerb::DropTable;
    return true;
  }

  if (low.rfind("rename table", 0) == 0) {
    std::size_t i = 0;
    while (i < line_trimmed.size() && !std::isspace(static_cast<unsigned char>(line_trimmed[i]))) ++i;
    while (i < line_trimmed.size() && std::isspace(static_cast<unsigned char>(line_trimmed[i]))) ++i;
    if (line_trimmed.size() - i < 5 || ascii_strncasecmp(line_trimmed.data() + i, "TABLE", 5) != 0) {
      if (err) *err = "RENAME: expected TABLE";
      return false;
    }
    i += 5;
    while (i < line_trimmed.size() && std::isspace(static_cast<unsigned char>(line_trimmed[i]))) ++i;
    if (!extract_open_paren(line_trimmed, i, out)) {
      if (err) *err = "RENAME: expected (new_name)";
      return false;
    }
    out->verb = MdbVerb::RenameTable;
    return true;
  }

  if (low == "reset") {
    out->verb = MdbVerb::ResetTable;
    return true;
  }

  if (low.rfind("set primary key", 0) == 0) {
    std::size_t i = 0;
    while (i < line_trimmed.size() && !std::isspace(static_cast<unsigned char>(line_trimmed[i]))) ++i;
    while (i < line_trimmed.size() && std::isspace(static_cast<unsigned char>(line_trimmed[i]))) ++i;
    if (line_trimmed.size() - i < 7 || ascii_strncasecmp(line_trimmed.data() + i, "PRIMARY", 7) != 0) {
      if (err) *err = "SET PRIMARY KEY: expected PRIMARY";
      return false;
    }
    i += 7;
    while (i < line_trimmed.size() && std::isspace(static_cast<unsigned char>(line_trimmed[i]))) ++i;
    if (line_trimmed.size() - i < 3 || ascii_strncasecmp(line_trimmed.data() + i, "KEY", 3) != 0) {
      if (err) *err = "SET PRIMARY KEY: expected KEY";
      return false;
    }
    i += 3;
    while (i < line_trimmed.size() && std::isspace(static_cast<unsigned char>(line_trimmed[i]))) ++i;
    if (!extract_open_paren(line_trimmed, i, out)) {
      if (err) *err = "SET PRIMARY KEY: expected (col)";
      return false;
    }
    out->verb = MdbVerb::SetPrimaryKey;
    return true;
  }

  const char* s = skip_ws_cstr(line_trimmed.data());

  if (command_has_prefix_token(s, "CONFIRM_REORDER(", sizeof("CONFIRM_REORDER(") - 1)) {
    const auto o = line_trimmed.find('(');
    const auto c = line_trimmed.rfind(')');
    if (o == std::string_view::npos || c == std::string_view::npos || c <= o) {
      if (err) *err = "CONFIRM_REORDER: expected (...)";
      return false;
    }
    out->verb = MdbVerb::ConfirmReorder;
    out->has_paren = true;
    out->open_paren = o;
    out->paren_inner = line_trimmed.substr(o + 1, c - o - 1);
    return true;
  }

  struct Entry {
    const char* pfx;
    std::size_t len;
    MdbVerb verb;
    bool needs_paren;
  };
  static const Entry kEntries[] = {
      {"BULKINSERTFAST(", 14, MdbVerb::BulkInsertFast, true},
      {"UPDATEWHERE(", 12, MdbVerb::UpdateWhere, true},
      {"DELETEWHERE(", 12, MdbVerb::DeleteWhere, true},
      {"SETATTRMULTI(", 12, MdbVerb::SetAttrMulti, true},
      {"DELETEPK(", 9, MdbVerb::DeletePk, true},
      {"SETATTR(", 7, MdbVerb::SetAttr, true},
      {"DELETE(", 7, MdbVerb::DeleteRow, true},
      {"RENATTR(", 7, MdbVerb::RenAttr, true},
      {"FINDPK(", 6, MdbVerb::FindPk, true},
      {"FIND(", 5, MdbVerb::Find, true},
      {"ADDATTR(", 8, MdbVerb::AddAttr, true},
      {"DEFATTR(", 7, MdbVerb::DefAttr, true},
      {"DELATTR(", 7, MdbVerb::DelAttr, true},
      {"WHEREP(", 7, MdbVerb::WhereP, true},
      {"WHERE(", 6, MdbVerb::Where, true},
      {"COUNT(", 6, MdbVerb::CountPred, true},
      {"UPDATE(", 7, MdbVerb::Update, true},
      {"INSERT(", 7, MdbVerb::Insert, true},
      // StructDB: inner is `row1|row2|...` where each row is comma-separated cells like INSERT (id,col,...).
      {"BULKINSERT(", 11, MdbVerb::BulkInsert, true},
      {"PAGE_JSON(", 10, MdbVerb::PageJson, true},
      {"PAGE(", 5, MdbVerb::Page, true},
      {"USE(", 4, MdbVerb::Use, true},
      {"SUM(", 4, MdbVerb::Sum, true},
      {"AVG(", 4, MdbVerb::Avg, true},
      {"MIN(", 4, MdbVerb::Min, true},
      {"MAX(", 4, MdbVerb::Max, true},
      {"QBAL(", 5, MdbVerb::Qbal, true},
      {"SHOW PLAN(", 10, MdbVerb::ShowPlan, true},
      {"EXPLAIN WHERE(", 14, MdbVerb::ExplainWhere, true},
  };

  for (const auto& e : kEntries) {
    if (command_has_prefix_token(s, e.pfx, e.len)) {
      out->verb = e.verb;
      if (e.needs_paren) {
        const std::size_t pos = line_trimmed.find('(');
        if (pos == std::string_view::npos || !extract_open_paren(line_trimmed, pos, out)) {
          if (err) *err = "expected (...)";
          return false;
        }
      }
      return true;
    }
  }

  if (command_has_prefix_token(s, "SAVEPOINT", 9)) {
    const char* p = s + 9;
    p = skip_ws_cstr(p);
    out->verb = MdbVerb::Savepoint;
    out->tail.assign(p);
    if (out->tail.empty()) {
      if (err) *err = "SAVEPOINT: need name";
      return false;
    }
    return true;
  }

  if (command_has_prefix_token(s, "BEGIN", 5) && s[5] == '\0') {
    out->verb = MdbVerb::Begin;
    return true;
  }
  if (command_has_prefix_token(s, "COMMIT", 6) && s[6] == '\0') {
    out->verb = MdbVerb::Commit;
    return true;
  }
  if (command_has_prefix_token(s, "ROLLBACK", 8) && s[8] == '\0') {
    out->verb = MdbVerb::Rollback;
    return true;
  }

  if (command_has_prefix_token(s, "COUNT", 5) && s[5] == '\0') {
    out->verb = MdbVerb::CountBare;
    return true;
  }

  if (low.rfind("autovacuum", 0) == 0 || low.rfind("recover to ", 0) == 0 ||
      low.rfind("walsync", 0) == 0 || low.rfind("groupcommit", 0) == 0 || low.rfind("waladaptive", 0) == 0 ||
      low.rfind("segment", 0) == 0 || low.rfind("hotindex", 0) == 0 || low.rfind("writeconflict", 0) == 0 ||
      low.rfind("create schema", 0) == 0 || low.rfind("drop schema", 0) == 0 || low == "list schemas" ||
      low == "show schemas" || low.rfind("alter table", 0) == 0) {
    out->verb = MdbVerb::NotPortable;
    out->tail = trim_copy_sv(line_trimmed);
    return true;
  }

  if (err) *err = "unknown verb";
  return false;
}

}  // namespace structdb::client::mdb
