#pragma once

#include <string>
#include <string_view>
#include <vector>

namespace structdb::client::mdb {

/// Verbs aligned with newdb `dispatch.cc` phase-1/2 prefix routing (subset implemented in StructDB).
enum class MdbVerb : int {
  Unknown = 0,
  CountBare,
  CountPred,
  Page,
  PageJson,
  Where,
  WhereP,
  Find,
  FindPk,
  Sum,
  Avg,
  Min,
  Max,
  Qbal,
  UpdateWhere,
  DeleteWhere,
  SetAttrMulti,
  SetAttr,
  RenAttr,
  DeleteRow,
  DeletePk,
  BulkInsert,
  ListTables,
  ShowTables,
  ShowTxn,
  ShowSnapshot,
  Export,
  TxnIsolation,
  Begin,
  Commit,
  Rollback,
  Savepoint,
  RollbackToSavepoint,
  Help,
  CreateTable,
  Use,
  DefAttr,
  /// Append one column after initial `DEFATTR` (same `name:type` grammar as one DEFATTR pair).
  AddAttr,
  Insert,
  Update,
  DelAttr,
  DropTable,
  RenameTable,
  ResetTable,
  ShowAttr,
  ShowKey,
  SetPrimaryKey,
  /// Primary-key / row-anchor remap: JSON `{"table":"…","pairs":[[old,new],…]}` in `CONFIRM_REORDER(…)`.
  ConfirmReorder,
  ShowLog,
  ShowTuning,
  ShowTuningJson,
  ShowStorage,
  ShowStorageJson,
  Vacuum,
  Scan,
  ImportDir,
  BulkInsertFast,
  ReleaseSavepoint,
  Exit,
  ShowPlan,
  ExplainWhere,
  /// newdb-only / heap-only verbs: dispatch prints `[NOT_SUPPORTED]` with `tail` as the matched prefix.
  NotPortable,
};

struct MdbParsedLine {
  MdbVerb verb{MdbVerb::Unknown};
  /// Inner of first `(...)` when applicable; may be empty for bare verbs.
  std::string_view paren_inner;
  /// Position of opening `(` for commands that need it (when `has_paren`).
  bool has_paren{false};
  std::size_t open_paren{0};
  /// Trailing tokens after the verb for multi-word commands (e.g. `EXPORT JSON path`).
  std::string tail;
};

/// Classifies a trimmed script line (non-empty, not `#`). On success sets `out`; on failure sets `err`.
bool mdb_parse_command_line(std::string_view line_trimmed, MdbParsedLine* out, std::string* err);

}  // namespace structdb::client::mdb
